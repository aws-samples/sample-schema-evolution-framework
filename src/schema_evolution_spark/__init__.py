"""Spark-side components of the schema evolution toolkit.

- ``schema_evolution_backfill``: Glue job for changes that rewrite data
  (incompatible type changes, re-partitioning, sort-order rebuilds).
- ``backfill_new_columns``: manual Glue job that fills environment-gated
  columns added by a plan (MERGE INTO Silver from Bronze).
- ``iceberg_helpers``: shared Spark/Iceberg helpers for the above.

These modules need PySpark (provided by AWS Glue at runtime). Install the
``spark`` extra locally to run their tests.
"""
