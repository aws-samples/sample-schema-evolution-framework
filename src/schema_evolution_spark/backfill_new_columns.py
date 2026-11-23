"""Glue job: backfill newly-added Silver columns from the matching Bronze table.

Issues a Spark MERGE INTO to populate NULL-filled Silver columns from the
corresponding Bronze Iceberg table.  All inputs are passed as explicit Glue job
parameters — no YAML config or plan file is read at runtime.  The caller
(human or automation) supplies the columns and merge keys derived from the
migration plan + schema config.

This job is **manually triggered** from the Glue console or AWS CLI whenever
env-gated add_column migration plans need to be backfilled after DDL has been
applied by ``schema-evolution-apply``.  It is NOT triggered by CI/CD
(``schema-evolution-apply`` only launches the type-change backfill job).

Triggering manually from AWS CLI
---------------------------------
    aws glue start-job-run \\
      --job-name dp-qa-glue-schema-evolution-backfill-env-columns \\
      --arguments '{
        "--environment":  "qa",
        "--database":     "dp_qa_silver",
        "--table":        "contracts",
        "--new_columns":  "STATUS_CODE,PRIORITY_FLAG,ARCHIVED_FLAG",
        "--merge_keys":   "PARTNER_ID,PARTNER_SITE_CODE",
        "--account_id":   "222222222222"
      }'

Parameters
----------
--environment   : dev | qa | prod
--database      : Silver database name (e.g. dp_qa_silver)
--table         : Table name (e.g. contracts)
--new_columns   : Comma-separated list of columns to backfill from Bronze
--merge_keys    : Comma-separated list of join key columns
--account_id    : AWS datalake account ID
--region        : AWS region (default: us-west-2)
--bucket_prefix : S3 Tables bucket name prefix (default: dp)
--dry_run       : Set to 'true' to print MERGE SQL without executing
--silver_catalog: Spark catalog name for Silver (default: silver_catalog)
--bronze_catalog: Spark catalog name for Bronze (default: s3tables_bronze)
"""

from __future__ import annotations

import argparse
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Spark helpers
# ---------------------------------------------------------------------------


def _get_spark(  # pragma: no cover
    silver_catalog: str,
    bronze_catalog: str,
    silver_bucket: str,
    bronze_bucket: str,
    account_id: str,
    region: str,
) -> object:
    """Build or retrieve a SparkSession with both Iceberg catalogs configured."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        logger.error("PySpark is not available. Run this script inside Glue or a local Spark environment.")
        sys.exit(1)

    spark = SparkSession.builder.appName("backfill_new_columns").getOrCreate()

    for catalog, bucket in [(silver_catalog, silver_bucket), (bronze_catalog, bronze_bucket)]:
        spark.conf.set(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        spark.conf.set(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
        spark.conf.set(f"spark.sql.catalog.{catalog}.uri", f"https://s3tables.{region}.amazonaws.com/iceberg")
        spark.conf.set(
            f"spark.sql.catalog.{catalog}.warehouse",
            f"arn:aws:s3tables:{region}:{account_id}:bucket/{bucket}",
        )
        spark.conf.set(f"spark.sql.catalog.{catalog}.rest.sigv4-enabled", "true")
        spark.conf.set(f"spark.sql.catalog.{catalog}.rest.signing-name", "s3tables")
        spark.conf.set(f"spark.sql.catalog.{catalog}.rest.signing-region", region)

    return spark


def _bronze_columns(spark: object, fqn: str) -> set[str]:
    """Return the set of lower-cased column names present in the Bronze table."""
    try:
        return {f.name.lower() for f in spark.table(fqn).schema.fields}  # type: ignore[union-attr]
    except Exception as e:
        logger.warning("Could not inspect Bronze table %s: %s", fqn, e)
        return set()


def build_merge_sql(
    silver_catalog: str,
    silver_db: str,
    bronze_catalog: str,
    bronze_db: str,
    table: str,
    merge_keys: list[str],
    new_columns: list[str],
    load_type: str = "merge",
    dedup_col: str = "dms_change_seq",
) -> str:
    """Generate the MERGE INTO SQL statement.

    Always deduplicates Bronze on ``dedup_col`` (default: dms_change_seq) to
    take the latest CDC record per merge key before joining to Silver.

    For SCD2 tables (``load_type='scd2'``), the join also filters Silver to
    ``_is_current = true`` so only the current version of each entity is updated.
    Historical Silver rows remain unchanged.
    """
    select_cols = ", ".join(merge_keys + new_columns)
    partition_cols = ", ".join(f"`{k}`" for k in merge_keys)
    join_cond = " AND ".join(f"s.{k} = b.{k}" for k in merge_keys)
    if load_type == "scd2":
        join_cond += " AND s._is_current = true"
    set_clause = ",\n        ".join(f"s.{c} = b.{c}" for c in new_columns)

    return (  # nosec B608
        f"MERGE INTO `{silver_catalog}`.`{silver_db}`.`{table}` AS s\n"  # nosec B608
        f"USING (\n"
        f"    SELECT {select_cols}\n"
        f"    FROM (\n"
        f"        SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition_cols} ORDER BY `{dedup_col}` DESC) AS _rn\n"
        f"        FROM `{bronze_catalog}`.`{bronze_db}`.`{table}`\n"
        f"    ) WHERE _rn = 1\n"
        f") AS b\n"
        f"ON {join_cond}\n"
        f"WHEN MATCHED THEN UPDATE SET\n"
        f"    {set_clause}"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:  # noqa: C901
    """Parse arguments and run the Silver column backfill MERGE INTO."""
    parser = argparse.ArgumentParser(description="Backfill env-gated add_column Silver values from Bronze.")
    parser.add_argument("--environment", required=True, help="Target environment: dev | qa | prod")
    parser.add_argument("--database", required=True, help="Silver database name (e.g. dp_qa_silver)")
    parser.add_argument("--table", required=True, help="Table name (e.g. contracts)")
    parser.add_argument("--new_columns", required=True, help="Comma-separated columns to backfill from Bronze")
    parser.add_argument("--merge_keys", required=True, help="Comma-separated join key column names")
    parser.add_argument("--account_id", required=True, help="AWS datalake account ID")
    parser.add_argument("--region", default="us-west-2")
    parser.add_argument("--bucket_prefix", default="dp")
    parser.add_argument("--silver_catalog", default="silver_catalog")
    parser.add_argument("--bronze_catalog", default="s3tables_bronze")
    parser.add_argument("--dry_run", default="false", help="Set to 'true' to print MERGE SQL without executing")
    parser.add_argument(
        "--load_type",
        default="merge",
        choices=["merge", "scd2"],
        help="Load type of the Silver table. Use 'scd2' to restrict MERGE to _is_current=true rows only (default: merge)",
    )
    parser.add_argument(
        "--dedup_col",
        default="dms_change_seq",
        help="Bronze column used to deduplicate CDC rows per merge key (default: dms_change_seq)",
    )
    parser.add_argument(
        "--force_partial",
        action="store_true",
        help="Proceed with MERGE even if Bronze does not cover all Silver NULL rows (accepts residual NULLs)",
    )
    args, _ = (
        parser.parse_known_args()
    )  # parse_known_args ignores Glue-injected system args (--JOB_ID, --JOB_RUN_ID, etc.)

    env = args.environment
    dry_run = args.dry_run.lower() == "true"
    silver_db = args.database
    table = args.table
    new_cols = [c.strip() for c in args.new_columns.split(",") if c.strip()]
    merge_keys = [k.strip() for k in args.merge_keys.split(",") if k.strip()]

    if not new_cols:
        logger.info("No columns specified — nothing to backfill.")
        return

    logger.info("Columns to backfill (%d): %s", len(new_cols), new_cols)
    logger.info("Merge keys for %s (env=%s): %s", table, env, merge_keys)
    dedup_col = args.dedup_col

    # ---- 3. Derive bucket / bronze db names ---------------------------------
    silver_bucket = f"{args.bucket_prefix}-{env}-silver-tables"
    bronze_bucket = f"{args.bucket_prefix}-{env}-bronze-tables"
    bronze_db = silver_db.replace("_silver", "_bronze")

    logger.info(
        "Catalogs: silver=%s (%s / %s)  bronze=%s (%s / %s)",
        args.silver_catalog,
        silver_db,
        silver_bucket,
        args.bronze_catalog,
        bronze_db,
        bronze_bucket,
    )

    # ---- 4. Check Bronze schema — skip columns not present in Bronze --------
    if not dry_run:
        spark = _get_spark(
            silver_catalog=args.silver_catalog,
            bronze_catalog=args.bronze_catalog,
            silver_bucket=silver_bucket,
            bronze_bucket=bronze_bucket,
            account_id=args.account_id,
            region=args.region,
        )
        bronze_fqn = f"`{args.bronze_catalog}`.`{bronze_db}`.`{table}`"
        bronze_cols = _bronze_columns(spark, bronze_fqn)

        eligible = [c for c in new_cols if c.lower() in bronze_cols]
        skipped = [c for c in new_cols if c.lower() not in bronze_cols]

        if skipped:
            logger.warning(
                "Skipping %d column(s) not found in Bronze table %s: %s",
                len(skipped),
                bronze_fqn,
                skipped,
            )
        if not eligible:
            logger.warning("No eligible columns remain after Bronze schema check — nothing to backfill.")
            return

        if dedup_col.lower() not in bronze_cols:
            if dedup_col.lower() == "dms_change_seq" and "dms_commit_ts" in bronze_cols:
                logger.warning(
                    "Dedup column '%s' not present in Bronze table %s; falling back to 'dms_commit_ts'.",
                    dedup_col,
                    bronze_fqn,
                )
                dedup_col = "dms_commit_ts"
            else:
                logger.error(
                    "Dedup column '%s' not found in Bronze table %s. Available columns include: %s",
                    dedup_col,
                    bronze_fqn,
                    sorted(bronze_cols),
                )
                return
    else:
        eligible = new_cols
        spark = None

    # ---- 5. Build and run MERGE INTO ----------------------------------------
    sql = build_merge_sql(
        silver_catalog=args.silver_catalog,
        silver_db=silver_db,
        bronze_catalog=args.bronze_catalog,
        bronze_db=bronze_db,
        table=table,
        merge_keys=merge_keys,
        new_columns=eligible,
        load_type=args.load_type,
        dedup_col=dedup_col,
    )

    logger.info("MERGE SQL:\n%s", sql)

    if dry_run:
        logger.info("[dry_run=true] SQL printed above — no changes applied.")
        return

    logger.info("Executing MERGE INTO for %d column(s): %s", len(eligible), eligible)

    silver_fqn = f"`{args.silver_catalog}`.`{silver_db}`.`{table}`"
    merge_key_cols = ", ".join(f"`{k}`" for k in merge_keys)

    # ---- Step 1: Check Bronze deduped non-null count (fast early exit) ------
    null_check = " OR ".join(f"`{c}` IS NULL" for c in eligible)  # nosec B608
    silver_null_check = " OR ".join(f"s.`{c}` IS NULL" for c in eligible)  # qualified for join queries  # nosec B608
    is_current_filter = "WHERE _is_current = true" if args.load_type == "scd2" else ""

    bronze_fqn_deduped = (  # nosec B608
        f"(SELECT * FROM ("  # nosec B608
        f"  SELECT *, ROW_NUMBER() OVER (PARTITION BY {merge_key_cols} ORDER BY `{dedup_col}` DESC) AS _rn"
        f"  FROM `{args.bronze_catalog}`.`{bronze_db}`.`{table}`"
        f") WHERE _rn = 1)"
    )

    bronze_deduped_count = spark.sql(  # type: ignore[union-attr]  # nosec B608
        f"SELECT COUNT(*) AS cnt FROM ("  # nosec B608
        f"  SELECT {merge_key_cols}, ROW_NUMBER() OVER (PARTITION BY {merge_key_cols} ORDER BY `{dedup_col}` DESC) AS _rn"
        f"  FROM `{args.bronze_catalog}`.`{bronze_db}`.`{table}`"
        f") WHERE _rn = 1"
    ).collect()[0]["cnt"]

    bronze_null_in_source = spark.sql(  # type: ignore[union-attr]  # nosec B608
        f"SELECT COUNT(*) AS cnt FROM {bronze_fqn_deduped} WHERE {' OR '.join(f'`{c}` IS NULL' for c in eligible)}"  # nosec B608
    ).collect()[0]["cnt"]

    bronze_non_null = bronze_deduped_count - bronze_null_in_source
    logger.info(
        "Bronze deduped: total=%d  source_nulls=%d  non_null_available=%d",
        bronze_deduped_count,
        bronze_null_in_source,
        bronze_non_null,
    )

    if bronze_non_null == 0:
        logger.info(
            "All %d deduped Bronze rows have NULL for new column(s) in source — nothing to backfill. Skipping MERGE. ✓",
            bronze_deduped_count,
        )
        return

    # ---- Step 2: Check Silver NULL count and Bronze/Silver match ------------
    total_before = spark.sql(f"SELECT COUNT(*) AS cnt FROM {silver_fqn}").collect()[0]["cnt"]  # type: ignore[union-attr]  # nosec B608
    rows_with_nulls = spark.sql(f"SELECT COUNT(*) AS cnt FROM {silver_fqn} WHERE {null_check}").collect()[0]["cnt"]  # type: ignore[union-attr]  # nosec B608
    dupes_before = spark.sql(  # type: ignore[union-attr]  # nosec B608
        f"SELECT COUNT(*) AS cnt FROM ("  # nosec B608
        f"  SELECT {merge_key_cols}, COUNT(*) AS n FROM {silver_fqn} GROUP BY {merge_key_cols} HAVING n > 1"
        f") dupe_check"
    ).collect()[0]["cnt"]

    bronze_matching_silver = spark.sql(  # type: ignore[union-attr]  # nosec B608
        f"SELECT COUNT(*) AS cnt FROM {silver_fqn} s"  # nosec B608
        f"  JOIN {bronze_fqn_deduped} b ON {' AND '.join(f's.`{k}` = b.`{k}`' for k in merge_keys)}"
        f"  {is_current_filter.replace('WHERE', 'AND') if is_current_filter else ''}"
        f"  WHERE {silver_null_check}"
    ).collect()[0]["cnt"]

    logger.info(
        "Silver state: total=%d  rows_with_nulls=%d  bronze_matching_silver_nulls=%d  duplicate_pk_groups=%d",
        total_before,
        rows_with_nulls,
        bronze_matching_silver,
        dupes_before,
    )

    if bronze_matching_silver < rows_with_nulls:
        logger.error(
            "ABORTING: Bronze covers only %d of %d Silver rows with NULLs — "
            "%d Silver row(s) have no matching Bronze record. "
            "Run with --force_partial true to proceed anyway.",
            bronze_matching_silver,
            rows_with_nulls,
            rows_with_nulls - bronze_matching_silver,
        )
        if not getattr(args, "force_partial", False):
            return
    else:
        logger.info("Bronze matches all Silver NULL rows (%d). ✓", bronze_matching_silver)

    # Count Bronze rows that match Silver NULLs AND have a non-null value to provide
    non_null_filter = " AND ".join(f"b.`{c}` IS NOT NULL" for c in eligible)  # nosec B608
    rows_that_will_update = spark.sql(  # type: ignore[union-attr]  # nosec B608
        f"SELECT COUNT(*) AS cnt FROM {silver_fqn} s"  # nosec B608
        f"  JOIN {bronze_fqn_deduped} b ON {' AND '.join(f's.`{k}` = b.`{k}`' for k in merge_keys)}"
        f"  {is_current_filter.replace('WHERE', 'AND') if is_current_filter else ''}"
        f"  WHERE ({silver_null_check}) AND ({non_null_filter})"
    ).collect()[0]["cnt"]

    logger.info("Rows that will actually be updated (Silver NULL + Bronze non-null): %d", rows_that_will_update)

    if rows_that_will_update == 0:
        logger.info(
            "No rows to update — all Silver NULLs have NULL in Bronze source too. Skipping MERGE. ✓",
        )
        return

    if dupes_before > 0:
        logger.warning(
            "%d duplicate PK group(s) in Silver BEFORE merge — expected for SCD2 tables.",
            dupes_before,
        )

    # ---- Execute MERGE ------------------------------------------------------
    spark.sql(sql)  # type: ignore[union-attr]

    # ---- Post-merge checks --------------------------------------------------
    total_after = spark.sql(f"SELECT COUNT(*) AS cnt FROM {silver_fqn}").collect()[0]["cnt"]  # type: ignore[union-attr]  # nosec B608
    rows_still_null = spark.sql(f"SELECT COUNT(*) AS cnt FROM {silver_fqn} WHERE {null_check}").collect()[0]["cnt"]  # type: ignore[union-attr]  # nosec B608
    dupes_after = spark.sql(  # type: ignore[union-attr]  # nosec B608
        f"SELECT COUNT(*) AS cnt FROM ("  # nosec B608
        f"  SELECT {merge_key_cols}, COUNT(*) AS n FROM {silver_fqn} GROUP BY {merge_key_cols} HAVING n > 1"
        f") dupe_check"
    ).collect()[0]["cnt"]
    rows_updated = rows_with_nulls - rows_still_null

    logger.info(
        "Post-merge Silver state: total_rows=%d  rows_updated=%d  rows_still_null=%d  "
        "bronze_source_nulls=%d  duplicate_pk_groups=%d",
        total_after,
        rows_updated,
        rows_still_null,
        bronze_null_in_source,
        dupes_after,
    )

    # ---- Integrity assertions -----------------------------------------------
    if total_after != total_before:
        logger.error(
            "ROW COUNT CHANGED: before=%d after=%d — MERGE introduced or deleted rows. Investigate immediately.",
            total_before,
            total_after,
        )
    else:
        logger.info("Row count unchanged (%d) — no rows inserted or deleted. ✓", total_after)

    if dupes_after > dupes_before:
        logger.error(
            "DUPLICATE PKs INTRODUCED: before=%d groups after=%d groups — backfill may have corrupted data.",
            dupes_before,
            dupes_after,
        )
    else:
        logger.info("No new duplicate PKs introduced. ✓")

    logger.info(
        "Backfill complete for table=%s env=%s — columns=%s",
        table,
        env,
        eligible,
    )


if __name__ == "__main__":
    main()
