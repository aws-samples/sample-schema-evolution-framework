"""Tests for schema_evolution_backfill routing: in-place vs rebuild.

Non-promotable type change (date->timestamp) on a partition-source column cannot
be done in place (Iceberg retains partition specs and poisons the column name),
so the job must take the rebuild path. Promotable widenings and unpartitioned
columns stay in place.
"""

from unittest.mock import MagicMock

import pytest

from schema_evolution_spark.schema_evolution_backfill import (
    _assert_no_silent_partition_drop,
    _assert_source_unchanged,
    _current_snapshot_id,
    _describe_change,
    _evolve_partition_in_place,
    _is_repartition_only,
    _new_partition_differs,
    _parse_fqn,
    _partition_by_column,
    _partition_by_for_rebuild,
    _partition_clause,
    _partition_fields,
    _requires_rebuild,
    _resolve_source_column,
    _rewrites_column_values,
    apply_backfill,
    is_promotable_type_change,
)


def _row(col_name: str, data_type: str = "", comment: str = ""):
    r = MagicMock()
    r.__getitem__ = lambda _self, k: {"col_name": col_name, "data_type": data_type, "comment": comment}[k]
    return r


def _show_create_rows(columns, partitioning):
    """Build SHOW CREATE TABLE output rows (single column, index 0)."""
    lines = ["CREATE TABLE s3tables.db.tbl ("]
    lines += [f"  {name} {dtype.upper()}," for name, dtype in columns]
    if partitioning:
        lines.append(f"PARTITIONED BY ({', '.join(partitioning)})")
    lines.append("TBLPROPERTIES (")
    lines.append("  'format-version' = '3')")
    rows = []
    for ln in lines:
        r = MagicMock()
        r.__getitem__ = lambda _self, _k, _v=ln: _v
        rows.append(r)
    return rows


# --- promotability ---------------------------------------------------------


def test_promotable_int_to_long():
    assert is_promotable_type_change("int", "long") is True
    assert is_promotable_type_change("integer", "bigint") is True


def test_promotable_float_to_double():
    assert is_promotable_type_change("float", "double") is True


def test_promotable_decimal_precision_increase():
    assert is_promotable_type_change("decimal(10,2)", "decimal(20,2)") is True
    # scale change is NOT promotable
    assert is_promotable_type_change("decimal(10,2)", "decimal(20,4)") is False


def test_date_to_timestamp_not_promotable():
    assert is_promotable_type_change("date", "timestamp") is False


def test_naive_to_zoned_timestamp_not_promotable():
    """timestamp_ntz -> timestamp must take the physical column-swap path.

    Iceberg cannot promote a zone-less timestamp to a zone-aware one, so this
    has to route to rename/add/CAST/drop rather than a metadata-only ALTER. The
    distinct type names matter: is_promotable_type_change short-circuits on
    cur == tgt, so writing "timestamp" for both sides of this repair would make
    it a silent no-op that leaves the column naive.
    """
    assert is_promotable_type_change("timestamp_ntz", "timestamp") is False
    assert is_promotable_type_change("timestamp", "timestamp_ntz") is False


def test_same_type_is_promotable_noop():
    assert is_promotable_type_change("string", "string") is True


# --- rebuild routing -------------------------------------------------------


def _spark_partitioned_on(source_col, transform="identity", col_type="date"):
    part = source_col if transform == "identity" else f"{transform}({source_col})"
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = _show_create_rows(
        columns=[("id", "int"), (source_col, col_type)],
        partitioning=[part],
    )
    return spark


def test_requires_rebuild_for_partitioned_nonpromotable():
    spark = _spark_partitioned_on("period_end_date")
    changes = [{"column_name": "period_end_date", "current_type": "date", "target_type": "timestamp"}]
    forcing = _requires_rebuild(spark, "cat.db.tbl", changes)
    assert [c["column_name"] for c in forcing] == ["period_end_date"]


def test_no_rebuild_for_partitioned_promotable():
    spark = _spark_partitioned_on("k", col_type="int")
    changes = [{"column_name": "k", "current_type": "int", "target_type": "long"}]
    assert _requires_rebuild(spark, "cat.db.tbl", changes) == []


def test_no_rebuild_for_unpartitioned_nonpromotable():
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = _show_create_rows(columns=[("some_date", "date")], partitioning=[])
    changes = [{"column_name": "some_date", "current_type": "date", "target_type": "timestamp"}]
    assert _requires_rebuild(spark, "cat.db.tbl", changes) == []


def test_apply_backfill_requires_new_partition_for_rebuild_case():
    """Rebuild-forcing change without new_partition must fail fast, no mutation."""
    spark = _spark_partitioned_on("period_end_date")
    changes = [
        {
            "column_name": "period_end_date",
            "current_type": "date",
            "target_type": "timestamp",
            "transformation": "CAST(period_end_date AS TIMESTAMP)",
        }
    ]
    with pytest.raises(RuntimeError, match="new_partition"):
        apply_backfill(spark, "cat.`db`.`tbl`", changes, catalog="s3tables", database="db", table="tbl")
    issued = [c.args[0] for c in spark.sql.call_args_list]
    assert not any(q.startswith("ALTER TABLE") or q.startswith("DROP TABLE") for q in issued)


def test_apply_backfill_requires_table_ident_for_rebuild_case():
    """Rebuild-forcing change without catalog/db/table cannot proceed."""
    spark = _spark_partitioned_on("period_end_date")
    changes = [
        {
            "column_name": "period_end_date",
            "current_type": "date",
            "target_type": "timestamp",
            "new_partition": {"transform": "month", "param": None},
        }
    ]
    with pytest.raises(RuntimeError, match="catalog/database/table"):
        apply_backfill(spark, "cat.`db`.`tbl`", changes)


def test_apply_backfill_routes_rebuild_forcing_to_rebuild(monkeypatch):
    """A rebuild-forcing change with new_partition + idents calls rebuild_table."""
    import schema_evolution_spark.schema_evolution_backfill as mod

    spark = _spark_partitioned_on("period_end_date")
    called = {}

    def fake_rebuild(spark_, fqn, changes_, catalog, database, table, replace_partitioning=False):
        called["args"] = (fqn, catalog, database, table)
        return ["period_end_date"]

    monkeypatch.setattr(mod, "rebuild_table", fake_rebuild)
    changes = [
        {
            "column_name": "period_end_date",
            "current_type": "date",
            "target_type": "timestamp",
            "new_partition": {"transform": "month", "param": None},
        }
    ]
    result = apply_backfill(
        spark, "cat.`db`.`tbl`", changes, catalog="s3tables", database="dp_dev_gold", table="orders"
    )
    assert result == ["period_end_date"]
    assert called["args"] == ("cat.`db`.`tbl`", "s3tables", "dp_dev_gold", "orders")


# --- repartition-only change (no type change) ------------------------------


def test_is_repartition_only_true_for_new_partition_without_type_change():
    assert _is_repartition_only({"column_name": "posting_date", "new_partition": {"transform": "month"}}) is True


def test_is_repartition_only_false_for_type_change_with_new_partition():
    change = {"column_name": "x", "target_type": "timestamp", "new_partition": {"transform": "month"}}
    assert _is_repartition_only(change) is False


def test_is_repartition_only_false_without_new_partition():
    assert _is_repartition_only({"column_name": "x", "target_type": "timestamp"}) is False


def test_requires_rebuild_for_repartition_onto_new_column():
    """A repartition change onto a column that is not the current partition source
    (e.g. move order_costs from month(period_end_date) to month(posting_date))
    forces a rebuild so existing data is physically re-clustered."""
    spark = _spark_partitioned_on("period_end_date", transform="month")
    changes = [{"column_name": "posting_date", "new_partition": {"transform": "month", "param": None}}]
    forcing = _requires_rebuild(spark, "cat.db.tbl", changes)
    assert [c["column_name"] for c in forcing] == ["posting_date"]


def test_apply_backfill_routes_repartition_only_to_rebuild_with_replace(monkeypatch):
    """A repartition-only change routes to rebuild_table with replace_partitioning
    True so the old partition scheme is fully replaced by the new key."""
    import schema_evolution_spark.schema_evolution_backfill as mod

    spark = _spark_partitioned_on("period_end_date", transform="month")
    called = {}

    def fake_rebuild(spark_, fqn, changes_, catalog, database, table, replace_partitioning=False):
        called["replace"] = replace_partitioning
        called["cols"] = [c["column_name"] for c in changes_]
        return ["posting_date"]

    monkeypatch.setattr(mod, "rebuild_table", fake_rebuild)
    changes = [{"column_name": "posting_date", "new_partition": {"transform": "month", "param": None}}]
    result = apply_backfill(
        spark, "cat.`db`.`tbl`", changes, catalog="s3tables", database="dp_dev_gold", table="order_costs"
    )
    assert result == ["posting_date"]
    assert called["replace"] is True
    assert called["cols"] == ["posting_date"]


def test_apply_backfill_routes_sort_order_rebuild_to_rebuild(monkeypatch):
    """A sort-order change marked rebuild=True routes through the CTAS rebuild path."""
    import schema_evolution_spark.schema_evolution_backfill as mod

    spark = _spark_partitioned_on("period_end_date", transform="month")
    called = {}

    def fake_rebuild(spark_, fqn, changes_, catalog, database, table, replace_partitioning=False, sort_order=None):
        called["replace"] = replace_partitioning
        called["sort_order"] = sort_order
        called["cols"] = [c["column_name"] for c in changes_]
        return ["__sort_order__"]

    monkeypatch.setattr(mod, "rebuild_table", fake_rebuild)
    changes = [
        {
            "change_type": "set_sort_order",
            "column_name": "__sort_order__",
            "sort_fields": [{"column": "order_id", "direction": "asc", "rebuild": True}],
        }
    ]
    result = apply_backfill(
        spark, "cat.`db`.`tbl`", changes, catalog="s3tables", database="dp_dev_gold", table="projects"
    )
    assert result == ["__sort_order__"]
    assert called["replace"] is False
    assert called["sort_order"] == [("order_id", "asc")]


def test_apply_backfill_routes_sort_order_without_flag_to_rebuild(monkeypatch):
    """Sort-order changes rebuild by default, matching partition evolution."""
    import schema_evolution_spark.schema_evolution_backfill as mod

    spark = _spark_partitioned_on("period_end_date", transform="month")
    monkeypatch.setattr(mod, "rebuild_table", lambda *args, **kwargs: ["__sort_order__"])
    changes = [{"change_type": "set_sort_order", "column_name": "__sort_order__", "sort_fields": []}]
    assert apply_backfill(
        spark, "cat.`db`.`tbl`", changes, catalog="s3tables", database="dp_dev_gold", table="projects"
    ) == ["__sort_order__"]


def test_apply_backfill_routes_standalone_rebuild_to_rebuild(monkeypatch):
    """An explicit rebuild_table change rebuilds without changing partitioning."""
    import schema_evolution_spark.schema_evolution_backfill as mod

    spark = _spark_partitioned_on("period_end_date", transform="month")
    called = {}

    def fake_rebuild(spark_, fqn, changes_, catalog, database, table, replace_partitioning=False, sort_order=None):
        called["replace"] = replace_partitioning
        called["sort_order"] = sort_order
        return []

    monkeypatch.setattr(mod, "rebuild_table", fake_rebuild)
    changes = [
        {
            "change_type": "rebuild_table",
            "column_name": "__table__",
            "sort_fields": [{"column": "order_id", "direction": "asc"}],
        }
    ]
    assert (
        apply_backfill(spark, "cat.`db`.`tbl`", changes, catalog="s3tables", database="dp_dev_gold", table="projects")
        == []
    )
    assert called == {"replace": False, "sort_order": [("order_id", "asc")]}


def test_apply_backfill_forwards_target_format_version(monkeypatch):
    """A rebuild plan can explicitly request a target Iceberg format version."""
    import schema_evolution_spark.schema_evolution_backfill as mod

    spark = _spark_partitioned_on("period_end_date", transform="month")
    called = {}

    def fake_rebuild(
        spark_, fqn, changes_, catalog, database, table, replace_partitioning=False, sort_order=None, **kwargs
    ):
        called["target_format_version"] = kwargs.get("target_format_version")
        return []

    monkeypatch.setattr(mod, "rebuild_table", fake_rebuild)
    changes = [
        {
            "change_type": "rebuild_table",
            "column_name": "__table__",
            "target_format_version": "2",
        }
    ]
    assert (
        apply_backfill(spark, "cat.`db`.`tbl`", changes, catalog="s3tables", database="dp_dev_gold", table="projects")
        == []
    )
    assert called == {"target_format_version": "2"}


def test_describe_change_repartition_only_has_no_target_type():
    """Regression: the job startup summary must not require target_type on a
    repartition change (a KeyError there crashed the Glue job at startup)."""
    change = {"column_name": "posting_date", "new_partition": {"transform": "month", "param": None}}
    assert _describe_change(change) == "posting_date: repartition -> month"


def test_describe_change_type_change():
    change = {"column_name": "period_end_date", "current_type": "date", "target_type": "timestamp"}
    assert _describe_change(change) == "period_end_date: date -> timestamp"


def test_describe_change_type_change_missing_current_type():
    assert _describe_change({"column_name": "x", "target_type": "timestamp"}) == "x: ? -> timestamp"


def test_partition_by_for_rebuild_uses_repartition_new_partition():
    """A repartition-only change contributes its new_partition to the rebuilt spec."""
    part = _partition_by_for_rebuild(
        [{"column_name": "posting_date", "new_partition": {"transform": "month", "param": None}}],
        [],
    )
    assert part == [("posting_date", "month")]


# --- partition parsing (unchanged helpers) ---------------------------------


def test_partition_fields_parses_transform_and_source():
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = _show_create_rows(
        columns=[("period_end_date", "date")],
        partitioning=["months(period_end_date)"],
    )
    fields = _partition_fields(spark, "cat.db.tbl")
    assert fields == [
        {"source": "period_end_date", "transform": "months", "expr": "months(period_end_date)"}
    ]


def test_partition_fields_parses_identity_bare_column():
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = _show_create_rows(
        columns=[("period_end_date", "date")],
        partitioning=["period_end_date"],
    )
    fields = _partition_fields(spark, "cat.db.tbl")
    assert fields == [{"source": "period_end_date", "transform": "identity", "expr": "period_end_date"}]


def test_partition_fields_parses_bucket_transform():
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = _show_create_rows(
        columns=[("region", "string")],
        partitioning=["bucket(16, region)"],
    )
    fields = _partition_fields(spark, "cat.db.tbl")
    assert fields == [{"source": "region", "transform": "bucket", "expr": "bucket(16, region)"}]


def test_partition_fields_parses_multiple_fields_split_on_top_level_comma():
    """Multiple partition fields split on top-level commas only (not the comma
    inside bucket(16, region))."""
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = _show_create_rows(
        columns=[("period_end_date", "date"), ("region", "string")],
        partitioning=["month(period_end_date), bucket(16, region)"],
    )
    fields = _partition_fields(spark, "cat.db.tbl")
    assert fields == [
        {"source": "period_end_date", "transform": "month", "expr": "month(period_end_date)"},
        {"source": "region", "transform": "bucket", "expr": "bucket(16, region)"},
    ]


def test_parse_fqn_backtick_quoted():
    assert _parse_fqn("s3tables.`dp_dev_gold`.`orders`") == (
        "s3tables",
        "dp_dev_gold",
        "orders",
    )


def test_parse_fqn_unquoted():
    assert _parse_fqn("cat.db.tbl") == ("cat", "db", "tbl")


def test_parse_fqn_rejects_non_three_part():
    with pytest.raises(ValueError):
        _parse_fqn("db.tbl")


def _existing_specs_for_rebuild(partition_fields, change_by_col, staging_cols):
    """Mirror the caller-side filtering in rebuild_table (lines ~349-357)."""
    return [
        (f["source"], f["transform"])
        for f in partition_fields
        if f["source"].lower() not in change_by_col and f["source"].lower() not in staging_cols
    ]


def test_rebuild_drops_staging_sourced_identity_spec():
    """QA case: an identity spec left on a __backfill_tmp_ staging column of a
    changed column must NOT carry over; the change's month() new_partition wins.
    """
    change = {
        "column_name": "period_end_date",
        "current_type": "date",
        "target_type": "timestamp",
        "transformation": "none",
        "new_partition": {"transform": "month", "param": None},
    }
    changes = [change]
    change_by_col = {"period_end_date"}
    staging_cols = {
        "__repair_stage_period_end_date",
        "__backfill_tmp_period_end_date",
    }
    partition_fields = [
        {"source": "__backfill_tmp_period_end_date", "transform": "identity"},
    ]
    existing = _existing_specs_for_rebuild(partition_fields, change_by_col, staging_cols)
    assert existing == []
    partition_by = _partition_by_for_rebuild(changes, existing)
    assert partition_by == [("period_end_date", "month")]


def test_rebuild_carries_over_unrelated_partition_field():
    """An unrelated partition field (not a staging column, not changed) carries over."""
    change = {
        "column_name": "period_end_date",
        "current_type": "date",
        "target_type": "timestamp",
        "transformation": "none",
        "new_partition": {"transform": "month", "param": None},
    }
    change_by_col = {"period_end_date"}
    staging_cols = {"__backfill_tmp_period_end_date"}
    partition_fields = [
        {"source": "region", "transform": "identity"},
        {"source": "__backfill_tmp_period_end_date", "transform": "identity"},
    ]
    existing = _existing_specs_for_rebuild(partition_fields, change_by_col, staging_cols)
    assert existing == [("region", "identity")]
    partition_by = _partition_by_for_rebuild([change], existing)
    assert partition_by == ["region", ("period_end_date", "month")]


def test_replace_partitioning_drops_all_existing_specs():
    """order_costs case: replace identity(period_key) with
    month(period_end_date). With replace_partitioning the old spec is
    dropped even though period_key is neither changed nor a staging col.
    """
    change = {
        "column_name": "period_end_date",
        "current_type": "timestamp",
        "target_type": "timestamp",
        "transformation": "none",
        "new_partition": {"transform": "month", "param": None},
    }
    # replace_partitioning => existing_specs is unconditionally empty
    existing = []
    partition_by = _partition_by_for_rebuild([change], existing)
    assert partition_by == [("period_end_date", "month")]


def test_new_partition_differs_when_no_current_spec_for_column():
    change = {"new_partition": {"transform": "month"}}
    assert _new_partition_differs(change, "period_end_date", {"period_key": "identity"}) is True


def test_new_partition_differs_false_without_new_partition():
    assert _new_partition_differs({}, "col", {"col": "identity"}) is False


def test_new_partition_differs_false_when_already_matching_month():
    change = {"new_partition": {"transform": "month"}}
    assert _new_partition_differs(change, "period_end_date", {"period_end_date": "month"}) is False


def test_new_partition_differs_false_when_iceberg_renders_months():
    change = {"new_partition": {"transform": "month"}}
    assert _new_partition_differs(change, "period_end_date", {"period_end_date": "months"}) is False


def test_requires_rebuild_triggered_by_repartition_of_other_column():
    """order_costs: period_end_date is NOT the active partition source
    (period_key is), but a new_partition:month must still force a rebuild.
    """
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = _show_create_rows(
        columns=[("period_key", "int"), ("period_end_date", "timestamp")],
        partitioning=["period_key"],
    )
    changes = [
        {
            "column_name": "period_end_date",
            "current_type": "timestamp",
            "target_type": "timestamp",
            "new_partition": {"transform": "month", "param": None},
        }
    ]
    forcing = _requires_rebuild(spark, "cat.db.tbl", changes)
    assert [c["column_name"] for c in forcing] == ["period_end_date"]


def test_requires_rebuild_not_triggered_when_partition_already_matches():
    """Idempotency: re-running the order_costs plan on an already-repartitioned
    table (month spec present) does NOT force another rebuild.
    """
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = _show_create_rows(
        columns=[("period_end_date", "timestamp")],
        partitioning=["months(period_end_date)"],
    )
    changes = [
        {
            "column_name": "period_end_date",
            "current_type": "timestamp",
            "target_type": "timestamp",
            "new_partition": {"transform": "month", "param": None},
        }
    ]
    assert _requires_rebuild(spark, "cat.db.tbl", changes) == []


def test_repartition_rebuild_opt_out_skips_rebuild():
    """new_partition.rebuild=false => a re-partition is NOT rebuild-forcing
    (it will be applied as a metadata-only Iceberg partition evolution instead).
    """
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = _show_create_rows(
        columns=[("period_key", "int"), ("period_end_date", "timestamp")],
        partitioning=["period_key"],
    )
    changes = [
        {
            "column_name": "period_end_date",
            "current_type": "timestamp",
            "target_type": "timestamp",
            "new_partition": {"transform": "month", "param": None, "rebuild": False},
        }
    ]
    assert _requires_rebuild(spark, "cat.db.tbl", changes) == []


def test_opt_out_ignored_when_type_change_poisons_partition_source():
    """Safety: the opt-out cannot skip a rebuild that is mandatory because a
    non-promotable type change lands on an active partition-source column.
    """
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = _show_create_rows(
        columns=[("period_end_date", "date")],
        partitioning=["period_end_date"],
    )
    changes = [
        {
            "column_name": "period_end_date",
            "current_type": "date",
            "target_type": "timestamp",
            "new_partition": {"transform": "month", "param": None, "rebuild": False},
        }
    ]
    forcing = _requires_rebuild(spark, "cat.db.tbl", changes)
    assert [c["column_name"] for c in forcing] == ["period_end_date"]


def _snapshot_row(snapshot_id, operation):
    r = MagicMock()
    r.__getitem__ = lambda _self, k: {"snapshot_id": snapshot_id, "operation": operation}[k]
    return r


def _spark_returning_snapshots(rows):
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = rows
    return spark


def test_current_snapshot_id_returns_latest():
    spark = _spark_returning_snapshots([_snapshot_row(111, "append")])
    assert _current_snapshot_id(spark, "cat.db.tbl") == 111


def test_current_snapshot_id_none_when_no_snapshots():
    spark = _spark_returning_snapshots([])
    assert _current_snapshot_id(spark, "cat.db.tbl") is None


def test_assert_source_unchanged_ok_when_same_snapshot():
    spark = _spark_returning_snapshots([_snapshot_row(111, "append")])
    # Must not raise.
    _assert_source_unchanged(spark, "cat.db.tbl", 111, "cat.db.tbl_rebuild")


def test_assert_source_unchanged_aborts_on_concurrent_data_write():
    spark = _spark_returning_snapshots([_snapshot_row(222, "append")])
    with pytest.raises(RuntimeError, match="Concurrent write detected"):
        _assert_source_unchanged(spark, "cat.db.tbl", 111, "cat.db.tbl_rebuild")


def test_assert_source_unchanged_allows_compaction_replace():
    """S3 Tables background maintenance (compaction => 'replace') is not a
    concurrent writer and must not abort the swap even though the id changed.
    """
    spark = _spark_returning_snapshots([_snapshot_row(222, "replace")])
    _assert_source_unchanged(spark, "cat.db.tbl", 111, "cat.db.tbl_rebuild")


def test_assert_source_unchanged_skips_when_expected_none():
    spark = _spark_returning_snapshots([_snapshot_row(222, "append")])
    # expected None => guard is a no-op (cannot compare).
    _assert_source_unchanged(spark, "cat.db.tbl", None, "cat.db.tbl_rebuild")


# --- duplicate-column guard ------------------------------------------------


def test_apply_backfill_rejects_duplicate_column_changes():
    """Two changes targeting the same column (any case) must fail fast, no mutation."""
    spark = _spark_partitioned_on("period_end_date")
    changes = [
        {"column_name": "amount", "current_type": "int", "target_type": "long"},
        {"column_name": "AMOUNT", "current_type": "long", "target_type": "double"},
    ]
    with pytest.raises(RuntimeError, match="multiple changes targeting column"):
        apply_backfill(spark, "cat.`db`.`tbl`", changes, catalog="s3tables", database="db", table="tbl")
    issued = [c.args[0] for c in spark.sql.call_args_list]
    assert not any(q.startswith("ALTER TABLE") or q.startswith("DROP TABLE") for q in issued)


# --- _partition_clause -----------------------------------------------------


def test_partition_clause_identity_quotes_column():
    assert _partition_clause("identity", "period_end_date", None) == "`period_end_date`"


def test_partition_clause_month_transform():
    assert _partition_clause("month", "period_end_date", None) == "month(`period_end_date`)"


@pytest.mark.parametrize(
    ("iceberg_transform", "expected_transform"),
    [("years", "year"), ("months", "month"), ("days", "day"), ("hours", "hour")],
)
def test_partition_by_for_rebuild_normalizes_plural_iceberg_time_transforms(iceberg_transform, expected_transform):
    assert _partition_by_for_rebuild([], [("snapshot_date", iceberg_transform)]) == [
        ("snapshot_date", expected_transform)
    ]


def test_partition_clause_bucket_requires_param():
    assert _partition_clause("bucket", "id", 16) == "bucket(16, `id`)"
    with pytest.raises(ValueError, match="param"):
        _partition_clause("bucket", "id", None)


def test_partition_clause_truncate_requires_param():
    assert _partition_clause("truncate", "name", 4) == "truncate(4, `name`)"
    with pytest.raises(ValueError, match="param"):
        _partition_clause("truncate", "name", None)


# --- _resolve_source_column ------------------------------------------------


def test_resolve_source_column_prefers_real_name():
    cols = {"period_end_date", "id"}
    assert _resolve_source_column(cols, "period_end_date") == "period_end_date"


def test_resolve_source_column_falls_back_to_staging_column():
    cols = {"__backfill_tmp_period_end_date", "id"}
    assert _resolve_source_column(cols, "period_end_date") == "__backfill_tmp_period_end_date"


def test_resolve_source_column_none_when_absent():
    cols = {"id", "name"}
    assert _resolve_source_column(cols, "period_end_date") is None


# --- metadata-only re-partition multi-column guard -------------------------


def test_evolve_partition_in_place_single_column_ok():
    spark = MagicMock()
    change = {"column_name": "period_end_date", "new_partition": {"transform": "month", "param": None}}
    _evolve_partition_in_place(spark, "cat.`db`.`tbl`", change, {"period_end_date": "identity"})
    issued = [c.args[0] for c in spark.sql.call_args_list]
    assert any("DROP PARTITION FIELD period_end_date" in q for q in issued)
    assert any("ADD PARTITION FIELD month(period_end_date)" in q for q in issued)


def test_evolve_partition_in_place_rejects_multi_column_table():
    """Metadata-only re-partition must refuse a multi-column partition spec so it
    does not silently drop unrelated partition fields."""
    spark = MagicMock()
    change = {"column_name": "period_end_date", "new_partition": {"transform": "month", "param": None}}
    current = {"period_end_date": "identity", "region": "identity"}
    with pytest.raises(RuntimeError, match="single-column-partitioned"):
        _evolve_partition_in_place(spark, "cat.`db`.`tbl`", change, current)
    issued = [c.args[0] for c in spark.sql.call_args_list]
    assert not any("ALTER TABLE" in q for q in issued)


# --- rebuild multi-column partition guard ----------------------------------


def test_partition_by_column_extracts_name():
    assert _partition_by_column("region") == "region"
    assert _partition_by_column(("period_end_date", "month")) == "period_end_date"


def test_no_silent_partition_drop_allows_single_column_repartition():
    """identity(period_key) -> month(period_end_date): the whole
    single-column scheme is intentionally replaced, so this is allowed."""
    _assert_no_silent_partition_drop(
        "cat.db.tbl",
        source_partition_cols={"period_key"},
        rebuilt_partition_cols={"period_end_date"},
        changed_cols={"period_end_date"},
        staging_cols=set(),
    )


def test_no_silent_partition_drop_allows_carried_over_multi_column():
    """Multi-column scheme where every column is preserved in the rebuilt spec."""
    _assert_no_silent_partition_drop(
        "cat.db.tbl",
        source_partition_cols={"period_end_date", "region"},
        rebuilt_partition_cols={"period_end_date", "region"},
        changed_cols={"period_end_date"},
        staging_cols=set(),
    )


def test_no_silent_partition_drop_blocks_multi_column_loss():
    """Multi-column scheme where an unrelated partition column would be dropped."""
    with pytest.raises(RuntimeError, match="Multi-column re-partitioning"):
        _assert_no_silent_partition_drop(
            "cat.db.tbl",
            source_partition_cols={"period_end_date", "region"},
            rebuilt_partition_cols={"period_end_date"},
            changed_cols={"period_end_date"},
            staging_cols=set(),
        )


def test_no_silent_partition_drop_ignores_staging_columns():
    """A dropped partition column that is a staging artifact of a changed column
    is expected and must not trip the guard."""
    _assert_no_silent_partition_drop(
        "cat.db.tbl",
        source_partition_cols={"__backfill_tmp_period_end_date", "period_end_date"},
        rebuilt_partition_cols={"period_end_date"},
        changed_cols={"period_end_date"},
        staging_cols={"__backfill_tmp_period_end_date"},
    )


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        ({"change_type": "set_sort_order", "column_name": "__sort_order__", "sort_fields": []}, False),
        ({"change_type": "rebuild_table", "column_name": "__rebuild__"}, False),
        ({"change_type": "set_column_nullability", "column_name": "first_name", "target_not_null": False}, False),
        ({"change_type": "repartition", "column_name": "d", "new_partition": {"transform": "month"}}, False),
        ({"change_type": "incompatible_type_change", "column_name": "x", "target_type": "int"}, True),
        (
            {
                "change_type": "incompatible_type_change",
                "column_name": "d",
                "target_type": "timestamp",
                "new_partition": {"transform": "month"},
            },
            True,
        ),
    ],
)
def test_rewrites_column_values(change, expected):
    """Only type changes are cast during a rebuild; the __sort_order__ pseudo column
    (and other metadata-only changes) must never be resolved as a source column.

    Regression: QA skills_ratings/census sort-order rebuilds failed with
    "Rebuild: no source column found for '__sort_order__'".
    """
    assert _rewrites_column_values(change) is expected


def test_tracker_update_keeps_values_out_of_sql() -> None:
    """Table/plan names and error text go into a DataFrame, not the MERGE text,
    so a quote or backslash in an error message can't alter the statement."""
    from schema_evolution_spark.schema_evolution_backfill import _tracker_merge_sql, _tracker_source_view

    spark = MagicMock()
    hostile = "boom'); DROP TABLE x; --\\'"
    view = _tracker_source_view(spark, "demo_customers", "p1.json", "failed", hostile)

    rows, schema = spark.createDataFrame.call_args.args
    assert rows == [("demo_customers", "p1.json", "failed", "", hostile)]
    assert "notes string" in schema
    spark.createDataFrame.return_value.createOrReplaceTempView.assert_called_once_with(view)

    sql = _tracker_merge_sql(view, "s3tablescatalog", append_notes=True)
    assert hostile not in sql and "demo_customers" not in sql
    assert f"USING {view} s" in sql
    assert "t.notes = concat(t.notes, '; ', s.notes)" in sql
    assert _tracker_merge_sql(view, "c", append_notes=False).endswith("t.notes = t.notes")
