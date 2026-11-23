"""Schema Evolution Backfill Glue Job.

Handles incompatible type changes that require Spark for row-level operations.
Triggered by ``schema-evolution-apply`` (schema_evolution.apply.trigger_backfill)
when a plan contains a change that needs a data rewrite.

Strategy is chosen per table based on whether a changed column backs an active
partition field and whether the type change is an Iceberg-promotable widening:

  * Unpartitioned column, or promotable widening (int->long, float->double,
    decimal precision increase): applied IN PLACE.
      - promotable: ``ALTER COLUMN ... TYPE`` (keeps the column id)
      - non-promotable: rename old -> temp, add new (target type), CAST temp ->
        new, drop temp.
  * Partition-source column with a NON-promotable change (e.g. date->timestamp):
    a full table REBUILD (CTAS) is required. Iceberg retains historical
    partition specs forever and binds them by column name, so once the column id
    changes (which a non-promotable type change forces), the column name is
    permanently poisoned for that table lineage and no in-place ordering works.
    Verified empirically against S3 Tables. The rebuild creates a fresh table
    with the target type + desired partition spec and swaps it in atomically.

Required Glue Job Parameters:
    --mode           Must be 'apply'
    --catalog        Iceberg catalog name (e.g. s3tables)
    --database       Target database/namespace
    --table          Target table name
    --changes_json   JSON array of incompatible changes
    --region         AWS region
    --account_id     AWS account ID
    --target_bucket  S3 Tables bucket name
"""

import json
import logging
import sys
from datetime import UTC

from schema_evolution_spark.iceberg_helpers import configure_iceberg_catalog

logger = logging.getLogger(__name__)

# Iceberg-supported in-place type promotions (widening). Anything not covered
# here (notably date->timestamp) forces a physical column swap.
_PROMOTABLE: set[tuple[str, str]] = {
    ("int", "long"),
    ("int", "bigint"),
    ("integer", "long"),
    ("integer", "bigint"),
    ("float", "double"),
}


def _norm_type(t: str) -> str:
    return t.strip().lower()


def is_promotable_type_change(current_type: str, target_type: str) -> bool:
    """Return True if Iceberg can widen current_type -> target_type in place.

    Covers int->long, float->double, and decimal precision increases (same
    scale, larger-or-equal precision). Everything else (e.g. date->timestamp) is
    non-promotable and requires a physical column swap / rebuild.
    """
    cur = _norm_type(current_type)
    tgt = _norm_type(target_type)
    if cur == tgt:
        return True
    if (cur, tgt) in _PROMOTABLE:
        return True
    if cur.startswith("decimal(") and tgt.startswith("decimal("):
        import re

        cm = re.match(r"decimal\((\d+),\s*(\d+)\)", cur)
        tm = re.match(r"decimal\((\d+),\s*(\d+)\)", tgt)
        if cm and tm:
            cp, cs = int(cm.group(1)), int(cm.group(2))
            tp, ts = int(tm.group(1)), int(tm.group(2))
            return cs == ts and tp >= cp
    return False


def _split_top_level_commas(clause: str) -> list[str]:
    """Split on commas not nested inside parentheses (e.g. bucket(16, col))."""
    parts: list[str] = []
    depth = 0
    buf = ""
    for ch in clause:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append(buf)
            buf = ""
        else:
            buf += ch
    if buf.strip():
        parts.append(buf)
    return parts


def _parse_fqn(fqn: str) -> tuple[str, str, str]:
    """Split a ``catalog.`db`.`table``` identifier into its three parts,
    honouring backtick quoting so dots inside quoted names are preserved."""
    raw = fqn.strip()
    out: list[str] = []
    buf = ""
    in_tick = False
    for ch in raw:
        if ch == "`":
            in_tick = not in_tick
            continue
        if ch == "." and not in_tick:
            out.append(buf)
            buf = ""
            continue
        buf += ch
    out.append(buf)
    if len(out) != 3:
        raise ValueError(f"Cannot parse catalog.db.table from {fqn!r} (got {out})")
    return out[0], out[1], out[2]


def _s3tables_delete_table(spark, fqn: str) -> None:  # pragma: no cover
    """Drop an S3 Tables table via the S3 Tables ``DeleteTable`` API.

    Spark's ``DROP TABLE ... PURGE`` does not propagate ``purge=true`` to the
    Iceberg REST catalog backing S3 Tables, so the drop fails with
    ``BadRequestException: S3 Tables only supports dropping tables with purge
    enabled`` and the table survives. We therefore drop via the native S3 Tables
    API, resolving the table-bucket ARN from the catalog's ``warehouse`` conf and
    using the database as the namespace. A not-found table is treated as success.
    """
    import boto3
    from botocore.exceptions import ClientError

    catalog, database, table = _parse_fqn(fqn)
    warehouse = spark.conf.get(f"spark.sql.catalog.{catalog}.warehouse")
    region = spark.conf.get(f"spark.sql.catalog.{catalog}.rest.signing-region", "us-west-2")
    client = boto3.client("s3tables", region_name=region)
    try:
        client.delete_table(tableBucketARN=warehouse, namespace=database, name=table)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NotFoundException", "ResourceNotFoundException"):
            return
        raise


def _drop_table_if_exists(spark, fqn: str) -> None:  # pragma: no cover
    """Drop a table if it exists, tolerating a missing table.

    Uses the S3 Tables ``DeleteTable`` API rather than Spark ``DROP TABLE ...
    PURGE`` because the REST catalog backing S3 Tables does not honour the Spark
    ``PURGE`` keyword (it receives ``purge=false`` and rejects the drop)."""
    _s3tables_delete_table(spark, fqn)


def _partition_fields(spark, full_table_name: str) -> list[dict]:  # pragma: no cover
    """Return the table's active partition fields as source-column descriptors.

    Parses the ``PARTITIONED BY (...)`` clause of ``SHOW CREATE TABLE`` — the
    authoritative, stable rendering of the active partition spec on S3 Tables
    (DESCRIBE EXTENDED's partition section is unreliable/format-variant). Each
    descriptor has: source (lower-cased source column), transform, and expr (the
    raw clause element, e.g. ``months(ts)`` or a bare identity column).
    """
    import re

    rows = spark.sql(f"SHOW CREATE TABLE {full_table_name}").collect()
    ddl = "\n".join((r[0] or "") for r in rows)

    m = re.search(r"PARTITIONED BY \((.*?)\)\s*(?:\n|$)", ddl, re.DOTALL)
    if not m or not m.group(1).strip():
        return []

    fields: list[dict] = []
    for raw in _split_top_level_commas(m.group(1).strip()):
        expr = raw.strip()
        tm = re.match(r"(\w+)\s*\((.*)\)", expr)
        if tm:
            transform = tm.group(1).lower()
            source = tm.group(2).split(",")[-1].strip()  # bucket(16, col) -> col
        else:
            transform = "identity"
            source = expr
        source = source.strip("`").strip().lower()
        if source:
            fields.append({"source": source, "transform": transform, "expr": expr})
    return fields


def _partition_source_columns(spark, full_table_name: str) -> set[str]:  # pragma: no cover
    """Return lower-cased source column names backing the table's partition fields."""
    return {f["source"] for f in _partition_fields(spark, full_table_name)}


def _partition_clause(transform: str, column: str, param: int | None) -> str:
    """Build a Spark partition-field expression, e.g. 'month(`col`)' or 'bucket(16, `col`)'."""
    transform = (transform or "identity").lower()
    if transform == "identity":
        return f"`{column}`"
    if transform in ("bucket", "truncate"):
        if param is None:
            raise ValueError(f"Partition transform '{transform}' requires a param for column '{column}'")
        return f"{transform}({param}, `{column}`)"
    return f"{transform}(`{column}`)"


def _column_types(spark, full_table_name: str) -> dict[str, str]:  # pragma: no cover
    """Return {lower_col_name: data_type} for the table's top-level columns."""
    rows = spark.sql(f"DESCRIBE {full_table_name}").collect()
    types: dict[str, str] = {}
    for row in rows:
        name = (row["col_name"] or "").strip()
        if not name or name.startswith("#"):
            break
        types[name.lower()] = (row["data_type"] or "").strip()
    return types


def _requires_rebuild(spark, full_table_name: str, changes: list[dict]) -> list[dict]:
    """Return the changes that force a full table rebuild.

    A change forces a rebuild when either:

    1. The column backs an active partition field AND the type change is not an
       Iceberg-promotable widening. Such a change cannot be applied in place
       because the physical column swap orphans the retained partition spec and
       permanently poisons the column name. This ALWAYS forces a rebuild and
       cannot be opted out of.
    2. The change requests a ``new_partition`` that differs from the table's
       current partitioning. Iceberg partition evolution is metadata-only and
       does not rewrite existing data files, so a CTAS rebuild is required to
       physically re-cluster the existing rows under the new partition spec.
       This can be opted out of with ``new_partition.rebuild = false`` (metadata
       only re-partition), UNLESS case 1 also applies to the change.
     3. A ``set_sort_order`` change rebuilds by default so the write sort order is
         materialized across the full table contents. ``rebuild: false`` opts out
         to make the metadata-only behavior explicit.
    """
    fields = _partition_fields(spark, full_table_name)
    partition_sources = {f["source"].lower() for f in fields}
    current_transforms = {f["source"].lower(): f["transform"].lower() for f in fields}
    forcing = []
    for c in changes:
        if c.get("change_type") == "rebuild_table":
            forcing.append(c)
            continue
        if c.get("change_type") == "set_sort_order" and c.get("rebuild") is not False:
            forcing.append(c)
            continue
        col = c["column_name"].lower()
        type_change_on_partition = col in partition_sources and not is_promotable_type_change(
            c.get("current_type", ""), c.get("target_type", "")
        )
        repartition = _new_partition_differs(c, col, current_transforms)
        # A non-promotable type change on a partition-source column always forces
        # a rebuild (the opt-out only applies to a pure metadata re-partition).
        opted_out = _repartition_rebuild_opt_out(c) and not type_change_on_partition
        if type_change_on_partition or (repartition and not opted_out):
            forcing.append(c)
    return forcing


def _repartition_rebuild_opt_out(change: dict) -> bool:
    """True if the change's ``new_partition`` explicitly opts out of a rebuild.

    ``new_partition.rebuild = false`` requests a metadata-only Iceberg partition
    evolution (drop old field, add new field) instead of a CTAS rebuild. Existing
    data files keep the old physical layout; only new writes use the new spec.
    """
    np = change.get("new_partition")
    return bool(np) and np.get("rebuild") is False


def _new_partition_differs(change: dict, col: str, current_transforms: dict[str, str]) -> bool:
    """True if the change's ``new_partition`` differs from the column's current spec.

    Returns False when there is no ``new_partition`` instruction or when the
    requested transform already matches the active partition field for the column
    (so no needless rebuild is triggered).
    """
    np = change.get("new_partition")
    if not np:
        return False
    requested = (np.get("transform") or "identity").lower()
    # Iceberg renders the month transform as "month"/"months"; normalize.
    current = current_transforms.get(col)
    if current is None:
        return True
    return not (current == requested or current == f"{requested}s" or f"{current}s" == requested)


def _resolve_source_column(current_cols: set[str], real_col: str) -> str | None:
    """Find the column holding the source data for real_col.

    Prefers the real column name, then a prior repair/backfill staging column
    (so a table left half-migrated by an older run can still be rebuilt).
    """
    for candidate in (real_col, f"__repair_stage_{real_col}", f"__backfill_tmp_{real_col}"):
        if candidate.lower() in current_cols:
            return candidate
    return None


def _table_format_version(spark, full_table_name: str, default: str = "3") -> str:  # pragma: no cover
    """Return the Iceberg ``format-version`` of an existing table.

    Reads ``SHOW TBLPROPERTIES``; falls back to ``default`` if the property is
    absent. Used so a rebuild preserves the source table's format version (e.g.
    a v2 table is not silently upgraded to v3)."""
    try:
        rows = spark.sql(f"SHOW TBLPROPERTIES {full_table_name}").collect()
    except Exception:
        return default
    for r in rows:
        d = r.asDict()
        if str(d.get("key", "")).strip() == "format-version":
            return str(d.get("value", default)).strip()
    return default


def _is_repartition_only(change: dict) -> bool:
    """True if the change only re-partitions (no column value rewrite).

    A repartition-only change carries a ``new_partition`` instruction but no
    ``target_type`` (it is not a type change), so the rebuild projects the column
    as-is and only uses its ``new_partition`` to define the rebuilt spec. The
    ``repartition`` change_type is the canonical form, but any change with a
    ``new_partition`` and no ``target_type`` is treated the same way.
    """
    return bool(change.get("new_partition")) and not change.get("target_type")


def _rewrites_column_values(change: dict) -> bool:
    """True if the change casts/transforms a column's values during a rebuild.

    Only type changes (those carrying a ``target_type``) rewrite values. Table-level
    or metadata-only changes that can ride along in the same plan -- e.g.
    ``set_sort_order`` (pseudo column ``__sort_order__``), ``rebuild_table``,
    ``set_column_nullability``, and repartition-only changes -- are projected
    as-is and must not be resolved as source columns.
    """
    if change.get("change_type") in ("rebuild_table", "set_sort_order"):
        return False
    return bool(change.get("target_type")) and not _is_repartition_only(change)


def _describe_change(change: dict) -> str:
    """One-line human summary of a change for logging.

    Tolerant of repartition-only changes (which carry no ``target_type``) so the
    job's startup summary never raises on a non-type change.
    """
    col = change["column_name"]
    if _is_repartition_only(change):
        transform = (change.get("new_partition") or {}).get("transform", "identity")
        return f"{col}: repartition -> {transform}"
    return f"{col}: {change.get('current_type', '?')} -> {change.get('target_type', '?')}"


def _partition_by_column(entry: tuple[str, str] | str) -> str:
    """Return the source column name from a partition_by entry.

    Entries are either a bare column string (identity) or a ``(column, transform)``
    tuple, as produced by ``_partition_by_for_rebuild``.
    """
    return entry[0] if isinstance(entry, tuple) else entry


def _assert_no_silent_partition_drop(
    full_table_name: str,
    source_partition_cols: set[str],
    rebuilt_partition_cols: set[str],
    changed_cols: set[str],
    staging_cols: set[str],
) -> None:
    """Fail if a rebuild would silently drop a partition column.

    A single-column partition scheme may be fully replaced by the plan's
    ``new_partition`` (e.g. ``identity(period_key)`` ->
    ``month(period_end_date)``); that intentional swap is allowed. But if
    the source has 2+ partition columns and the rebuilt spec drops one that the
    plan neither changes nor re-declares via ``new_partition``, partitioning would
    be silently lost -- multi-column re-partitioning during a rebuild is not
    supported, so raise instead. All column sets are lower-cased.
    """
    dropped = sorted(
        col
        for col in source_partition_cols
        if col not in rebuilt_partition_cols and col not in changed_cols and col not in staging_cols
    )
    if dropped and len(source_partition_cols) > 1:
        raise RuntimeError(
            f"Rebuild of {full_table_name} would silently drop partition column(s) {dropped} "
            f"from a multi-column partition scheme {sorted(source_partition_cols)}: they are not changed by the "
            "plan and no new_partition re-declares them. Multi-column re-partitioning during a rebuild is not "
            "supported. Add a new_partition instruction for each partition column to be kept."
        )


def _partition_by_for_rebuild(changes: list[dict], all_partition_specs: list[tuple[str, str]]) -> list:
    """Build the partition_by list for the rebuilt table.

    Starts from the table's existing (non-forcing) partition fields, then applies
    each change's new_partition instruction for the changed column. Iceberg's
    SHOW CREATE TABLE may render time transforms in plural form (``days``), while
    the shared table creator accepts canonical singular transform names.
    """
    time_transform_aliases = {"years": "year", "months": "month", "days": "day", "hours": "hour"}
    spec: dict[str, tuple[str, str] | str] = {}
    for source, transform in all_partition_specs:
        transform = time_transform_aliases.get(transform.lower(), transform.lower())
        spec[source.lower()] = source if transform == "identity" else (source, transform)
    for c in changes:
        col = c["column_name"]
        np = c.get("new_partition")
        if np:
            transform = np.get("transform", "identity").lower()
            transform = time_transform_aliases.get(transform, transform)
            spec[col.lower()] = col if transform == "identity" else (col, transform)
    return list(spec.values())


def rebuild_table(  # noqa: C901  # pragma: no cover
    spark,
    full_table_name: str,
    changes: list[dict],
    catalog: str,
    database: str,
    table: str,
    replace_partitioning: bool = False,
    sort_order: list[tuple[str, str]] | None = None,
    target_format_version: str | None = None,
) -> list[str]:
    """Rebuild a table via CTAS to apply non-promotable type changes on a
    partitioned column, then swap the rebuilt table in atomically.

    The original table is never dropped until the rebuild is fully populated and
    row-count-verified, so a failure leaves the original intact. Idempotent: a
    leftover ``*_rebuild`` table from a prior failed run is dropped first.

    When ``replace_partitioning`` is True the table's existing partition fields are
    NOT carried over; the rebuilt table is partitioned solely by the ``changes'``
    ``new_partition`` instructions. Use this to fully replace a partition scheme
    (e.g. ``identity(period_key)`` -> ``month(period_end_date)``).
    """
    from schema_evolution_spark.iceberg_helpers import create_table_like_df as _create_table_if_not_exists

    rebuild_name = f"{table}_rebuild"
    rebuild_fqn = f"{catalog}.`{database}`.`{rebuild_name}`"

    current_cols = set(_column_types(spark, full_table_name).keys())
    # A repartition-only change (new_partition, no type change) re-clusters the
    # rebuilt table but does not rewrite the column's values, so it is NOT cast or
    # staged -- it is projected as-is and only contributes its new_partition to the
    # rebuilt spec (via _partition_by_for_rebuild, which reads every change).
    cast_changes = [c for c in changes if _rewrites_column_values(c)]
    change_by_col = {c["column_name"].lower(): c for c in cast_changes}
    source_df_cols = spark.table(full_table_name).columns

    # Columns produced by prior partial runs that must NOT be carried forward.
    staging_cols: set[str] = set()
    for c in cast_changes:
        staging_cols.add(f"__repair_stage_{c['column_name']}".lower())
        staging_cols.add(f"__backfill_tmp_{c['column_name']}".lower())

    def _cast_expr(c: dict) -> str:
        src = _resolve_source_column(current_cols, c["column_name"])
        if src is None:
            raise RuntimeError(f"Rebuild: no source column found for '{c['column_name']}' in {full_table_name}")
        transformation = c.get("transformation")
        if transformation and transformation.lower() != "none":
            return transformation.replace(c["column_name"], f"`{src}`")
        return f"CAST(`{src}` AS {c['target_type']})"

    # Project every real (non-staging) source column, casting the changed ones.
    select_exprs: list[str] = []
    projected: set[str] = set()
    changed: list[str] = []
    for actual in source_df_cols:
        low = actual.lower()
        if low in staging_cols:
            continue
        if low in change_by_col:
            c = change_by_col[low]
            select_exprs.append(f"{_cast_expr(c)} AS `{c['column_name']}`")
            changed.append(c["column_name"])
            projected.add(c["column_name"].lower())
        else:
            select_exprs.append(f"`{actual}`")
            projected.add(low)

    # A changed column whose real name is absent (data only in a staging column)
    # is not in source_df_cols under its real name; emit it explicitly.
    for c in cast_changes:
        if c["column_name"].lower() not in projected:
            select_exprs.append(f"{_cast_expr(c)} AS `{c['column_name']}`")
            changed.append(c["column_name"])
            projected.add(c["column_name"].lower())

    logger.info("Rebuild: dropping any stale rebuild table %s", rebuild_fqn)
    _drop_table_if_exists(spark, rebuild_fqn)

    # Existing partition fields NOT being changed carry over; changed ones use
    # new_partition. Fields sourced from a staging column of a changed column
    # (e.g. an identity spec left on __backfill_tmp_/__repair_stage_ by a prior
    # partial run) are dropped -- they are superseded by the change's new_partition.
    # When replace_partitioning is set, NO existing field carries over.
    existing_specs = (
        []
        if replace_partitioning
        else [
            (f["source"], f["transform"])
            for f in _partition_fields(spark, full_table_name)
            if f["source"].lower() not in change_by_col and f["source"].lower() not in staging_cols
        ]
    )
    partition_by = _partition_by_for_rebuild(changes, existing_specs)
    logger.info("Rebuild: partition_by = %s", partition_by)

    # Guard: multi-column re-partitioning during a rebuild is not supported.
    # A single-column partition scheme may be fully replaced by the plan's
    # new_partition (e.g. identity(period_key) -> month(period_end_date));
    # that intentional swap is allowed. But if the source has 2+ partition columns
    # and the rebuilt spec would drop one that the plan neither changes nor
    # re-declares via new_partition, we would silently lose partitioning -- fail
    # loudly instead.
    rebuilt_partition_cols = {_partition_by_column(p).lower() for p in partition_by}
    source_partition_cols = {f["source"].lower() for f in _partition_fields(spark, full_table_name)}
    _assert_no_silent_partition_drop(
        full_table_name, source_partition_cols, rebuilt_partition_cols, set(change_by_col), staging_cols
    )

    select_clause = ", ".join(select_exprs)
    projected_df = spark.sql(f"SELECT {select_clause} FROM {full_table_name}")  # nosec B608
    source_format_version = _table_format_version(spark, full_table_name)
    format_version = target_format_version or source_format_version
    if format_version not in {"2", "3"}:
        raise ValueError(f"Unsupported target format-version '{format_version}'; expected '2' or '3'")
    logger.info("Rebuild: creating format-version %s (source was %s)", format_version, source_format_version)
    _create_table_if_not_exists(
        spark,
        rebuild_fqn,
        projected_df,
        partition_by=partition_by or None,
        sort_order=sort_order,
        format_version=format_version,
    )

    logger.info("Rebuild: inserting rows into %s", rebuild_fqn)
    spark.sql(f"INSERT INTO {rebuild_fqn} SELECT {select_clause} FROM {full_table_name}")  # nosec B608

    # Capture the source's snapshot state immediately after the copy so we can
    # detect a concurrent writer (e.g. a CDC/ETL job) committing to the source
    # between the copy and the swap. Any such write would be silently lost at the
    # swap, so we abort instead (leaving the original intact). This is defense in
    # depth: upstream writers should be paused for the migration regardless.
    source_snapshot_after_copy = _current_snapshot_id(spark, full_table_name)

    src_count = spark.table(full_table_name).count()
    dst_count = spark.table(rebuild_fqn).count()
    if src_count != dst_count:
        raise RuntimeError(
            f"Rebuild row-count mismatch for {full_table_name}: source={src_count} rebuilt={dst_count}. "
            f"Leaving original intact; rebuilt table {rebuild_fqn} kept for inspection."
        )
    logger.info("Rebuild: row-count parity OK (%d rows)", src_count)

    # Final concurrent-write guard, right before the (non-atomic) swap: if a data
    # write committed to the source since the copy, abort. Compaction / snapshot
    # expiration performed by S3 Tables background maintenance ("replace") is not a
    # concurrent writer and does not trip this guard.
    _assert_source_unchanged(spark, full_table_name, source_snapshot_after_copy, rebuild_fqn)

    logger.info("Rebuild: swapping %s -> %s", rebuild_name, table)
    _s3tables_delete_table(spark, full_table_name)
    spark.sql(f"ALTER TABLE {rebuild_fqn} RENAME TO `{database}`.`{table}`")
    logger.info("Rebuild complete for %s (columns rebuilt: %s)", full_table_name, changed)
    return changed


_DATA_WRITE_OPERATIONS = frozenset({"append", "overwrite", "delete"})


def _current_snapshot_id(spark, full_table_name: str) -> int | None:  # pragma: no cover
    """Return the table's current (latest) Iceberg snapshot id, or None.

    Reads the ``.snapshots`` metadata table. None means the table has no
    snapshots (e.g. freshly created, no data), which the guard treats as
    "unknown" rather than "changed".
    """
    try:
        rows = spark.sql(
            f"SELECT snapshot_id, operation FROM {full_table_name}.snapshots "  # nosec B608
            "ORDER BY committed_at DESC LIMIT 1"
        ).collect()
    except Exception as e:  # noqa: BLE001
        logger.warning("Could not read snapshot id for %s: %s", full_table_name, str(e).splitlines()[0])
        return None
    if not rows:
        return None
    return rows[0]["snapshot_id"]


def _assert_source_unchanged(
    spark, full_table_name: str, expected_snapshot_id: int | None, rebuild_fqn: str
) -> None:  # pragma: no cover
    """Abort the rebuild if a data write committed to the source since the copy.

    Compares the source's current snapshot id against the one captured after the
    copy. A change caused by a data-write operation (append/overwrite/delete)
    means a concurrent writer raced the rebuild -- those rows are not in the
    rebuilt table and would be lost at the swap, so we abort with the original
    intact. A change caused only by S3 Tables maintenance ("replace" / compaction)
    is not a concurrent writer and is allowed.
    """
    if expected_snapshot_id is None:
        return
    try:
        rows = spark.sql(
            f"SELECT snapshot_id, operation FROM {full_table_name}.snapshots "  # nosec B608
            "ORDER BY committed_at DESC LIMIT 1"
        ).collect()
    except Exception as e:  # noqa: BLE001
        logger.warning("Could not re-read snapshot id for %s: %s", full_table_name, str(e).splitlines()[0])
        return
    if not rows:
        return
    current_id = rows[0]["snapshot_id"]
    current_op = (rows[0]["operation"] or "").lower()
    if current_id != expected_snapshot_id and current_op in _DATA_WRITE_OPERATIONS:
        raise RuntimeError(
            f"Concurrent write detected on {full_table_name} during rebuild: source snapshot changed "
            f"from {expected_snapshot_id} to {current_id} (operation '{current_op}'). Aborting swap to avoid "
            f"data loss; original left intact and rebuilt table {rebuild_fqn} kept for inspection. "
            "Pause upstream writers (CDC/ETL DAG) and re-run the migration."
        )


def _apply_in_place(  # noqa: C901  # pragma: no cover
    spark, full_table_name: str, change: dict, actual_cols: dict[str, str], max_retries: int, retry_wait: int
) -> str:
    """Apply a single non-partition-forcing change in place, returning the column."""
    import time as _time

    col = change["column_name"]
    target_type = change["target_type"]
    transformation = change.get("transformation")
    real_col = actual_cols.get(col.lower()) or col

    # Promotable widening: single metadata-only ALTER COLUMN, keeps the column id.
    if is_promotable_type_change(change.get("current_type", ""), target_type):
        logger.info("  Promotable widening '%s' -> %s (ALTER COLUMN)", real_col, target_type)
        spark.sql(f"ALTER TABLE {full_table_name} ALTER COLUMN `{real_col}` TYPE {target_type}")
        return real_col

    # Non-promotable, unpartitioned: rename -> add -> cast -> drop.
    temp_col = f"__backfill_tmp_{real_col}"
    df_desc = spark.sql(f"DESCRIBE {full_table_name}")
    current_cols = {row["col_name"].lower(): row["col_name"] for row in df_desc.collect()}
    temp_exists = temp_col.lower() in current_cols
    original_exists = real_col.lower() in current_cols

    if temp_exists and not original_exists:
        logger.info("  Temp '%s' already exists and '%s' gone — skipping rename", temp_col, real_col)
    elif not original_exists and not temp_exists:
        logger.info("  Neither '%s' nor '%s' exist — skipping rename (already done?)", real_col, temp_col)
    else:
        logger.info("  Renaming '%s' -> '%s'", real_col, temp_col)
        spark.sql(f"ALTER TABLE {full_table_name} RENAME COLUMN `{real_col}` TO `{temp_col}`")

    df_desc = spark.sql(f"DESCRIBE {full_table_name}")
    current_cols = {row["col_name"].lower(): row["col_name"] for row in df_desc.collect()}
    temp_exists = temp_col.lower() in current_cols

    if real_col.lower() in current_cols:
        logger.info("  Column '%s' already exists — skipping add", real_col)
    else:
        logger.info("  Adding '%s' as %s", real_col, target_type)
        spark.sql(f"ALTER TABLE {full_table_name} ADD COLUMN `{real_col}` {target_type}")

    if not temp_exists:
        logger.info("  Temp column '%s' not found — skipping UPDATE (already done?)", temp_col)
    else:
        if transformation and transformation.lower() != "none":
            expr = transformation.replace(col, f"`{temp_col}`")
        else:
            expr = f"CAST(`{temp_col}` AS {target_type})"
            logger.warning("  No custom transformation - using CAST for '%s'", real_col)

        update_sql = f"UPDATE {full_table_name} SET `{real_col}` = {expr}"  # nosec B608 - table names from Glue job params
        for attempt in range(max_retries + 1):
            try:
                logger.info("  UPDATE %s SET `%s` = %s", full_table_name, real_col, expr)
                spark.sql(update_sql)
                break
            except Exception as update_err:
                if "ValidationException" in str(update_err) and attempt < max_retries:
                    logger.warning(
                        "  Write conflict on UPDATE, retrying in %ds (%d/%d)...", retry_wait, attempt + 1, max_retries
                    )
                    _time.sleep(retry_wait)
                else:
                    raise

    df_desc = spark.sql(f"DESCRIBE {full_table_name}")
    current_cols = {row["col_name"].lower(): row["col_name"] for row in df_desc.collect()}
    if temp_col.lower() not in current_cols:
        logger.info("  Temp column '%s' already absent — skipping drop", temp_col)
    else:
        logger.info("  Dropping temp column '%s'", temp_col)
        spark.sql(f"ALTER TABLE {full_table_name} DROP COLUMN `{temp_col}`")

    return real_col


def apply_backfill(  # noqa: C901
    spark,
    full_table_name: str,
    changes: list[dict],
    max_retries: int = 3,
    retry_wait: int = 30,
    catalog: str | None = None,
    database: str | None = None,
    table: str | None = None,
    target_format_version: str | None = None,
) -> list[str]:
    """Apply incompatible type changes, choosing in-place vs rebuild per table.

    If any change is a non-promotable type change on a partition-source column,
    the whole table is rebuilt (CTAS) once, applying all changes together.
    Otherwise each change is applied in place. Returns backfilled column names.
    """
    seen_cols: set[str] = set()
    requested_formats = {str(c["target_format_version"]) for c in changes if c.get("target_format_version")}
    if len(requested_formats) > 1:
        raise RuntimeError(f"Plan for {full_table_name} requests multiple target format-versions: {requested_formats}")
    plan_target_format = next(iter(requested_formats), None)
    for c in changes:
        col = c["column_name"].lower()
        if col in seen_cols:
            raise RuntimeError(
                f"Plan for {full_table_name} contains multiple changes targeting column "
                f"'{c['column_name']}'. Each column may appear at most once per plan."
            )
        seen_cols.add(col)

    rebuild_forcing = _requires_rebuild(spark, full_table_name, changes)
    if rebuild_forcing:
        if not (catalog and database and table):
            raise RuntimeError(
                f"{full_table_name} needs a table rebuild for partition-source column(s) "
                f"{sorted(c['column_name'] for c in rebuild_forcing)} but catalog/database/table were not provided."
            )
        missing = [
            c["column_name"]
            for c in rebuild_forcing
            if c.get("change_type") not in ("set_sort_order", "rebuild_table") and not c.get("new_partition")
        ]
        if missing:
            raise RuntimeError(
                f"Type change on partition-source column(s) {sorted(missing)} in {full_table_name} requires a "
                '"new_partition" instruction (e.g. {"transform": "month"}) so the rebuilt table can be partitioned.'
            )
        logger.info(
            "Rebuild path selected for %s (forced by %s)", full_table_name, [c["column_name"] for c in rebuild_forcing]
        )
        # When any change carries a new_partition, the plan's new_partition
        # instruction(s) define the rebuilt table's partitioning authoritatively;
        # the old spec (e.g. a different partition column) is fully replaced so we
        # do not end up partitioning by both the old and new columns.
        replace_partitioning = any(c.get("new_partition") for c in rebuild_forcing)
        sort_order = None
        for c in rebuild_forcing:
            if c.get("change_type") in ("set_sort_order", "rebuild_table"):
                sort_fields = c.get("sort_fields") or []
                if sort_fields:
                    sort_order = [(f["column"], f["direction"]) for f in sort_fields]
                break
        rebuild_kwargs = {}
        if target_format_version or plan_target_format:
            rebuild_kwargs["target_format_version"] = target_format_version or plan_target_format
        return rebuild_table(
            spark,
            full_table_name,
            changes,
            catalog,
            database,
            table,
            replace_partitioning=replace_partitioning,
            **rebuild_kwargs,
            **({"sort_order": sort_order} if sort_order is not None else {}),
        )

    # In-place path (Spark-dependent dispatch loop).
    return _apply_in_place_all(spark, full_table_name, changes, max_retries, retry_wait)  # pragma: no cover


def _apply_in_place_all(  # pragma: no cover
    spark, full_table_name: str, changes: list[dict], max_retries: int, retry_wait: int
) -> list[str]:
    """Apply each change in place; handle the metadata-only re-partition opt-out.

    Spark-dependent dispatch loop extracted from ``apply_backfill`` so the pure
    routing logic there stays unit-testable.
    """
    df = spark.sql(f"DESCRIBE {full_table_name}")
    actual_cols = {row["col_name"].lower(): row["col_name"] for row in df.collect()}
    current_transforms = {
        f["source"].lower(): f["transform"].lower() for f in _partition_fields(spark, full_table_name)
    }
    backfilled = []
    for change in changes:
        real_col = actual_cols.get(change["column_name"].lower(), change["column_name"])
        try:
            done = _apply_in_place(spark, full_table_name, change, actual_cols, max_retries, retry_wait)
            actual_cols[done.lower()] = done
            backfilled.append(done)
            logger.info("  Backfill complete for '%s'", done)
            # Metadata-only re-partition opt-out: evolve the partition spec without
            # rewriting existing data (new writes use the new spec, old files keep
            # their layout). Only reached when _requires_rebuild did not force a
            # rebuild for this change (i.e. new_partition.rebuild == false).
            if _repartition_rebuild_opt_out(change) and _new_partition_differs(
                change, change["column_name"].lower(), current_transforms
            ):
                _evolve_partition_in_place(spark, full_table_name, change, current_transforms)
        except Exception as e:
            logger.error("  Backfill FAILED for '%s': %s", real_col, e)
            raise RuntimeError(f"Backfill failed for column '{real_col}': {e}") from e
    return backfilled


def _evolve_partition_in_place(
    spark, full_table_name: str, change: dict, current_transforms: dict[str, str]
) -> None:  # pragma: no cover
    """Apply a metadata-only Iceberg partition evolution (no data rewrite).

    Drops every existing partition field and adds the change's ``new_partition``
    for its column. Existing data files retain their old physical partitioning;
    only new writes use the new spec. Used for the ``new_partition.rebuild=false``
    opt-out.
    """
    col = change["column_name"]
    np = change["new_partition"]
    transform = (np.get("transform") or "identity").lower()
    new_field = col if transform == "identity" else f"{transform}({col})"

    other_sources = [s for s in current_transforms if s != col.lower()]
    if other_sources:
        raise RuntimeError(
            f"Metadata-only re-partition (new_partition.rebuild=false) is only supported for "
            f"single-column-partitioned tables. {full_table_name} is also partitioned by {sorted(other_sources)}; "
            "dropping those fields would silently change unrelated partitioning. Use a full rebuild "
            "(new_partition.rebuild=true) instead."
        )

    for source, cur_transform in current_transforms.items():
        old_field = source if cur_transform == "identity" else f"{cur_transform}({source})"
        logger.info("  Metadata re-partition: DROP PARTITION FIELD %s", old_field)
        spark.sql(f"ALTER TABLE {full_table_name} DROP PARTITION FIELD {old_field}")

    logger.info("  Metadata re-partition: ADD PARTITION FIELD %s", new_field)
    spark.sql(f"ALTER TABLE {full_table_name} ADD PARTITION FIELD {new_field}")


def _update_tracker(
    spark, catalog: str, table_name: str, plan_name: str, status: str, notes: str = ""
) -> None:  # pragma: no cover
    """Update the tracker table via Spark SQL (MERGE/upsert). Preserves existing notes."""
    view = _tracker_source_view(spark, table_name, plan_name, status, notes)
    spark.sql(_tracker_merge_sql(view, catalog, bool(notes)))


def _tracker_source_view(spark, table_name: str, plan_name: str, status: str, notes: str) -> str:
    """Register the tracker row as a temp view and return the view's name.

    The values (table and plan names from job arguments, and the error text in
    ``notes``) travel as DataFrame data, never as SQL text, so no value can
    change the MERGE statement.
    """
    import uuid
    from datetime import datetime

    applied_at = datetime.now(UTC).isoformat() if status == "applied" else ""
    view = f"tracker_update_{uuid.uuid4().hex}"
    spark.createDataFrame(
        [(table_name, plan_name, status, applied_at, notes or "")],
        "table_name string, plan_name string, status string, applied_at string, notes string",
    ).createOrReplaceTempView(view)
    return view


def _tracker_merge_sql(view: str, catalog: str, append_notes: bool) -> str:
    """MERGE statement that copies the row in *view* into the plan tracker.

    Only identifiers are interpolated: *view* is generated by
    ``_tracker_source_view`` and *catalog* is the Glue job's configured catalog.
    On failure the error is appended to the existing notes; on success the
    existing notes (which hold the Glue run ID) are kept.
    """
    tracker_fqn = f"{catalog}.`schema_evolution_metadata`.`plan_tracker`"
    notes_expr = "concat(t.notes, '; ', s.notes)" if append_notes else "t.notes"
    return (
        f"MERGE INTO {tracker_fqn} t USING {view} s "  # nosec B608 - identifiers only; values come from the view
        "ON t.table_name = s.table_name AND t.plan_name = s.plan_name "
        "WHEN MATCHED THEN UPDATE SET t.status = s.status, t.applied_at = s.applied_at, "
        f"t.notes = {notes_expr}"
    )
def main() -> None:  # pragma: no cover
    """Glue job entry point. Parses args and runs backfill."""
    from awsglue.utils import getResolvedOptions
    from pyspark.sql import SparkSession

    logging.basicConfig(level=logging.INFO)
    logger.setLevel(logging.INFO)

    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "mode",
            "catalog",
            "database",
            "table",
            "changes_json",
            "region",
            "account_id",
            "target_bucket",
            "tracker_table_name",
            "tracker_plan_name",
        ],
    )

    mode = args["mode"].lower()
    if mode != "apply":
        raise ValueError(f"Invalid mode: '{mode}'. This job only supports 'apply'.")

    catalog = args["catalog"]
    region = args["region"]
    account_id = args["account_id"]
    target_bucket = args["target_bucket"]
    database = args["database"]
    table = args["table"]
    tracker_table_name = args["tracker_table_name"]
    tracker_plan_name = args["tracker_plan_name"]

    changes = json.loads(args["changes_json"])
    if not changes:
        logger.info("No changes to backfill - exiting")
        return

    # Create the Spark session first: _update_tracker needs it to record a
    # terminal status, so it must exist before the guarded block below.
    spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()

    # Pin the session timezone. This job rewrites column values through CAST, and
    # any cast between a zone-less and a zone-aware timestamp is interpreted in
    # session-local time - so a non-UTC session would silently shift every value
    # it migrates by the offset. Glue already defaults to UTC; this stops the
    # correctness of a data rewrite resting on an unstated default.
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    full_table_name = f"{catalog}.`{database}`.`{table}`"

    # Everything that can fail is inside this block so ANY error (including the
    # per-change summary log, catalog configuration, or planning) records a
    # 'failed' tracker status instead of leaving the plan stuck at 'backfilling'.
    try:
        logger.info("=" * 80)
        logger.info("Schema Evolution Backfill Job")
        logger.info("Catalog: %s  Database: %s  Table: %s", catalog, database, table)
        logger.info("Changes: %d change(s)", len(changes))
        for c in changes:
            logger.info("  %s", _describe_change(c))
        logger.info("=" * 80)

        configure_iceberg_catalog(
            spark=spark,
            catalog=catalog,
            region=region,
            account_id=account_id,
            table_bucket=target_bucket,
        )

        backfilled = apply_backfill(spark, full_table_name, changes, catalog=catalog, database=database, table=table)
        logger.info("Backfill complete: %s", backfilled)

        # Update tracker to applied via Spark SQL
        _update_tracker(spark, catalog, tracker_table_name, tracker_plan_name, "applied")
        logger.info("Tracker updated: %s/%s -> applied", tracker_table_name, tracker_plan_name)

    except Exception as e:
        logger.error("Backfill failed: %s", e)
        _update_tracker(spark, catalog, tracker_table_name, tracker_plan_name, "failed", notes=str(e))
        logger.info("Tracker updated: %s/%s -> failed", tracker_table_name, tracker_plan_name)
        raise


if __name__ == "__main__":  # pragma: no cover
    main()
