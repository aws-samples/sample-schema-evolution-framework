"""Spark-side Iceberg helpers used by the backfill job.

Everything here targets Iceberg tables on Amazon S3 Tables through the
Iceberg REST catalog endpoint.

PySpark is imported lazily inside each function so the pure-Python routing logic
in the backfill job stays importable (and unit-testable) without a JVM.
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


# ---------------------------------------------------------------------------
# Catalog configuration
# ---------------------------------------------------------------------------


def configure_iceberg_catalog(
    *,
    spark: Any,
    catalog: str,
    region: str,
    account_id: str,
    table_bucket: str,
) -> None:
    """Register an S3 Tables REST catalog on a running Spark session.

    ``spark.sql.extensions`` is a static setting and cannot be changed at
    runtime on AWS Glue. Set it on the job instead::

        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
    """
    try:
        current_ext = spark.conf.get("spark.sql.extensions")
    except Exception:  # noqa: BLE001 - conf lookup is best-effort
        current_ext = None
    if current_ext and ICEBERG_EXTENSIONS not in current_ext:
        logger.warning(
            "spark.sql.extensions does not include IcebergSparkSessionExtensions; "
            "ALTER TABLE / MERGE statements may fail. Set it on the Glue job."
        )

    conf = spark.conf
    conf.set(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
    conf.set(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
    conf.set(f"spark.sql.catalog.{catalog}.uri", f"https://s3tables.{region}.amazonaws.com/iceberg")
    conf.set(
        f"spark.sql.catalog.{catalog}.warehouse",
        f"arn:aws:s3tables:{region}:{account_id}:bucket/{table_bucket}",
    )
    conf.set(f"spark.sql.catalog.{catalog}.rest.sigv4-enabled", "true")
    conf.set(f"spark.sql.catalog.{catalog}.rest.signing-name", "s3tables")
    conf.set(f"spark.sql.catalog.{catalog}.rest.signing-region", region)
    # S3FileIO resilience for large rewrites (throttling / connection-pool exhaustion).
    conf.set(f"spark.sql.catalog.{catalog}.s3.retry.num-retries", "32")
    conf.set(f"spark.sql.catalog.{catalog}.s3.retry.min-wait-ms", "2000")
    conf.set(f"spark.sql.catalog.{catalog}.s3.retry.max-wait-ms", "60000")
    conf.set("spark.sql.caseSensitive", "false")


# ---------------------------------------------------------------------------
# Table helpers
# ---------------------------------------------------------------------------


def format_field_to_sql(field: Any, *, force_nullable: bool = False) -> str:
    """Render a StructField as a backtick-escaped, lower-cased column definition."""
    col_name = field.name.lower().replace("`", "``")
    sql_type = field.dataType.simpleString()
    not_null = " NOT NULL" if not force_nullable and not field.nullable else ""
    return f"`{col_name}` {sql_type}{not_null}"


_PARAMETERIZED_TRANSFORM_RE = re.compile(r"^(bucket|truncate)\[(\d+)\]$", re.IGNORECASE)
_TIME_TRANSFORMS = frozenset({"year", "month", "day", "hour"})


def render_partition_transform(transform: str, col_name: str) -> str:
    """Render ``month`` / ``bucket[16]`` style transforms as Iceberg DDL."""
    normalized = transform.strip().lower()
    match = _PARAMETERIZED_TRANSFORM_RE.match(normalized)
    if match and int(match.group(2)) > 0:
        return f"{match.group(1)}({match.group(2)}, `{col_name}`)"
    if normalized in _TIME_TRANSFORMS:
        return f"{normalized}(`{col_name}`)"
    raise ValueError(
        f"Invalid Iceberg partition transform '{transform}' for column '{col_name}'. "
        f"Allowed: {sorted(_TIME_TRANSFORMS)} plus bucket[N] / truncate[N]."
    )


def create_iceberg_table(
    spark: Any,
    full_table_name: str,
    schema: Any,
    *,
    format_version: str = "3",
    partition_by: list[str | tuple[str, str]] | None = None,
    sort_order: list[tuple[str, str]] | None = None,
) -> None:
    """``CREATE TABLE IF NOT EXISTS`` an Iceberg table from an explicit schema.

    Args:
        spark: Active Spark session.
        full_table_name: ``catalog.database.table``.
        schema: StructType. Non-nullable fields become ``NOT NULL``.
        format_version: Iceberg format version, ``"2"`` or ``"3"``.
        partition_by: Column names (identity) or ``(column, transform)`` tuples.
        sort_order: ``(column, "asc"|"desc")`` tuples, applied with
            ``ALTER TABLE ... WRITE ORDERED BY`` after creation.
    """
    if format_version not in ("2", "3"):
        raise ValueError(f"Unsupported Iceberg format_version {format_version!r}; expected '2' or '3'")

    fields_ddl = ", ".join(format_field_to_sql(f) for f in schema.fields)

    partition_clause = ""
    if partition_by:
        exprs = [
            render_partition_transform(spec[1], spec[0]) if isinstance(spec, tuple) else f"`{spec}`"
            for spec in partition_by
        ]
        partition_clause = f"PARTITIONED BY ({', '.join(exprs)})"

    ordered: list[str] = []
    for col_name, direction in sort_order or []:
        if direction.upper() not in ("ASC", "DESC"):
            raise ValueError(f"Invalid sort direction '{direction}' for column '{col_name}'.")
        ordered.append(f"`{col_name}` {direction.upper()} NULLS LAST")

    # snappy is set explicitly: some Glue versions default to zstd, which the
    # S3 Tables REST catalog has rejected on commit.
    tblproperties = ", ".join(
        [
            f"'format-version' = '{format_version}'",
            "'write.delete.mode' = 'merge-on-read'",
            "'write.update.mode' = 'merge-on-read'",
            "'write.merge.mode' = 'merge-on-read'",
            "'write.parquet.compression-codec' = 'snappy'",
        ]
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {full_table_name} ({fields_ddl}) "
        f"USING iceberg {partition_clause} TBLPROPERTIES ({tblproperties})"
    )
    if ordered:
        spark.sql(f"ALTER TABLE {full_table_name} WRITE ORDERED BY ({', '.join(ordered)})")


def create_table_like_df(
    spark: Any,
    full_table_name: str,
    df: Any,
    partition_by: list[str | tuple[str, str]] | None = None,
    sort_order: list[tuple[str, str]] | None = None,
    format_version: str = "3",
) -> None:
    """Create an Iceberg table using a DataFrame's schema."""
    create_iceberg_table(
        spark,
        full_table_name,
        df.schema,
        format_version=format_version,
        partition_by=partition_by,
        sort_order=sort_order,
    )
