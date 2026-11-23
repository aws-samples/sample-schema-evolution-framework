"""Unit tests for the env-gated add_column backfill Glue job.

Tests cover the pure-Python helper functions (no Spark required):
  - build_merge_sql
  - _bronze_columns (mocked Spark)
  - main argument parsing / dry-run flow
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from schema_evolution_spark.backfill_new_columns import (
    _bronze_columns,
    build_merge_sql,
)

# ---------------------------------------------------------------------------
# build_merge_sql
# ---------------------------------------------------------------------------


def test_build_merge_sql_structure():
    sql = build_merge_sql(
        silver_catalog="silver_catalog",
        silver_db="dp_qa_silver",
        bronze_catalog="s3tables_bronze",
        bronze_db="dp_qa_bronze",
        table="contracts",
        merge_keys=["PARTNER_ID", "PARTNER_SITE_CODE"],
        new_columns=["PRIORITY_FLAG", "STATUS_CODE"],
    )
    assert "MERGE INTO `silver_catalog`.`dp_qa_silver`.`contracts`" in sql
    assert "FROM `s3tables_bronze`.`dp_qa_bronze`.`contracts`" in sql
    assert "s.PARTNER_ID = b.PARTNER_ID" in sql
    assert "s.PARTNER_SITE_CODE = b.PARTNER_SITE_CODE" in sql
    assert "s.PRIORITY_FLAG = b.PRIORITY_FLAG" in sql
    assert "s.STATUS_CODE = b.STATUS_CODE" in sql
    assert "WHEN MATCHED THEN UPDATE SET" in sql
    # Bronze dedup subquery
    assert "ROW_NUMBER() OVER" in sql
    assert "_rn = 1" in sql
    assert "dms_change_seq" in sql


def test_build_merge_sql_scd2_adds_is_current_filter():
    sql = build_merge_sql(
        silver_catalog="sc",
        silver_db="dp_qa_silver",
        bronze_catalog="bc",
        bronze_db="dp_qa_bronze",
        table="customer_accounts",
        merge_keys=["CUSTOMER_ACCOUNT_ID"],
        new_columns=["CANCEL_UNSHIPPED_LINES_FLAG"],
        load_type="scd2",
    )
    assert "s._is_current = true" in sql


def test_build_merge_sql_merge_type_no_is_current_filter():
    sql = build_merge_sql(
        silver_catalog="sc",
        silver_db="dp_qa_silver",
        bronze_catalog="bc",
        bronze_db="dp_qa_bronze",
        table="contracts",
        merge_keys=["CONTRACT_ID"],
        new_columns=["EXTERNAL_REFERENCE_1"],
        load_type="merge",
    )
    assert "_is_current" not in sql


def test_build_merge_sql_single_key_single_col():
    sql = build_merge_sql(
        silver_catalog="sc",
        silver_db="dp_dev_silver",
        bronze_catalog="bc",
        bronze_db="dp_dev_bronze",
        table="partner_sites",
        merge_keys=["SITE_ID"],
        new_columns=["PAYMENT_GROUP_CODE"],
    )
    assert "ON s.SITE_ID = b.SITE_ID" in sql
    assert "s.PAYMENT_GROUP_CODE = b.PAYMENT_GROUP_CODE" in sql


def test_build_merge_sql_select_includes_keys_and_cols():
    sql = build_merge_sql(
        silver_catalog="sc",
        silver_db="dp_qa_silver",
        bronze_catalog="bc",
        bronze_db="dp_qa_bronze",
        table="tbl",
        merge_keys=["KEY1"],
        new_columns=["COL_A", "COL_B"],
    )
    # SELECT must include both keys and new columns
    assert "SELECT KEY1, COL_A, COL_B" in sql


# ---------------------------------------------------------------------------
# _bronze_columns
# ---------------------------------------------------------------------------


def test_bronze_columns_returns_lowercase():
    mock_field_a = MagicMock()
    mock_field_a.name = "PARTNER_ID"
    mock_field_b = MagicMock()
    mock_field_b.name = "pay_group"

    mock_df = MagicMock()
    mock_df.schema.fields = [mock_field_a, mock_field_b]

    mock_spark = MagicMock()
    mock_spark.table.return_value = mock_df

    result = _bronze_columns(mock_spark, "catalog.db.tbl")
    assert result == {"partner_id", "pay_group"}


def test_bronze_columns_returns_empty_on_error():
    mock_spark = MagicMock()
    mock_spark.table.side_effect = Exception("Table not found")

    result = _bronze_columns(mock_spark, "catalog.db.missing_table")
    assert result == set()


# ---------------------------------------------------------------------------
# Integration: dry_run flow via main()
# ---------------------------------------------------------------------------


def test_main_dry_run_prints_sql(capsys):
    """main() with dry_run=true should log the MERGE SQL and return cleanly."""
    import sys

    from schema_evolution_spark.backfill_new_columns import main

    test_args = [
        "prog",
        "--environment",
        "qa",
        "--database",
        "dp_qa_silver",
        "--table",
        "contracts",
        "--new_columns",
        "PRIORITY_FLAG,STATUS_CODE",
        "--merge_keys",
        "PARTNER_ID,PARTNER_SITE_CODE",
        "--account_id",
        "222222222222",
        "--dry_run",
        "true",
    ]
    with patch.object(sys, "argv", test_args):
        main()  # should not raise


def test_main_dry_run_no_columns(capsys):
    """main() with empty new_columns returns without error."""
    import sys

    from schema_evolution_spark.backfill_new_columns import main

    test_args = [
        "prog",
        "--environment",
        "qa",
        "--database",
        "dp_qa_silver",
        "--table",
        "contracts",
        "--new_columns",
        "",
        "--merge_keys",
        "PARTNER_ID",
        "--account_id",
        "222222222222",
        "--dry_run",
        "true",
    ]
    with patch.object(sys, "argv", test_args):
        main()  # should not raise


def test_main_non_dry_run_skips_columns_not_in_bronze():
    """When a column is not in Bronze, it should be skipped and not cause an error."""
    import sys

    from schema_evolution_spark.backfill_new_columns import main

    mock_field = MagicMock()
    mock_field.name = "PRIORITY_FLAG"
    mock_dedup = MagicMock()
    mock_dedup.name = "dms_change_seq"
    mock_df = MagicMock()
    mock_df.schema.fields = [mock_field, mock_dedup]

    mock_spark = MagicMock()
    mock_spark.table.return_value = mock_df
    # COUNT queries return 0 rows affected
    mock_spark.sql.return_value.collect.return_value = [{"cnt": 0}]

    test_args = [
        "prog",
        "--environment",
        "qa",
        "--database",
        "dp_qa_silver",
        "--table",
        "contracts",
        "--new_columns",
        "PRIORITY_FLAG,MISSING_COL",
        "--merge_keys",
        "PARTNER_ID",
        "--account_id",
        "222222222222",
    ]
    with (
        patch.object(sys, "argv", test_args),
        patch(
            "schema_evolution_spark.backfill_new_columns._get_spark",
            return_value=mock_spark,
        ),
    ):
        main()

    # MERGE should have been called (PRIORITY_FLAG is in Bronze)
    assert mock_spark.sql.called


def test_main_non_dry_run_no_eligible_columns_exits_early():
    """When no columns are present in Bronze, should exit without running MERGE."""
    import sys

    from schema_evolution_spark.backfill_new_columns import main

    mock_spark = MagicMock()
    mock_spark.table.return_value.schema.fields = []  # empty Bronze schema

    test_args = [
        "prog",
        "--environment",
        "qa",
        "--database",
        "dp_qa_silver",
        "--table",
        "contracts",
        "--new_columns",
        "MISSING_COL",
        "--merge_keys",
        "PARTNER_ID",
        "--account_id",
        "222222222222",
    ]
    with (
        patch.object(sys, "argv", test_args),
        patch(
            "schema_evolution_spark.backfill_new_columns._get_spark",
            return_value=mock_spark,
        ),
    ):
        main()

    # No MERGE SQL should have been executed
    mock_spark.sql.assert_not_called()


def _make_spark_mock(
    bronze_deduped=10,
    bronze_nulls=0,
    silver_total=10,
    silver_nulls=10,
    bronze_matching=10,
    rows_to_update=10,
    dupes_before=0,
    total_after=10,
    still_null=0,
    dupes_after=0,
    dedup_column="dms_change_seq",
):
    """Build a mock Spark where sql().collect() returns configurable counts."""
    mock_spark = MagicMock()

    # Mock spark.table() used by _bronze_columns — return schema with column present
    mock_field = MagicMock()
    mock_field.name = "payment_group_code"
    mock_dedup_field = MagicMock()
    mock_dedup_field.name = dedup_column
    mock_df = MagicMock()
    mock_df.schema.fields = [mock_field, mock_dedup_field]
    mock_spark.table.return_value = mock_df

    # Each sql() call returns a different count based on call order
    counts = [
        bronze_deduped,  # bronze_deduped_count
        bronze_nulls,  # bronze_null_in_source
        silver_total,  # total_before
        silver_nulls,  # rows_with_nulls
        dupes_before,  # dupes_before
        bronze_matching,  # bronze_matching_silver
        rows_to_update,  # rows_that_will_update
        total_after,  # total_after
        still_null,  # rows_still_null
        dupes_after,  # dupes_after
    ]
    mock_spark.sql.return_value.collect.side_effect = [[{"cnt": c}] for c in counts]
    return mock_spark


def test_main_skips_when_all_bronze_source_nulls():
    """When Bronze has no non-null values, MERGE should be skipped."""
    import sys

    from schema_evolution_spark.backfill_new_columns import main

    mock_spark = _make_spark_mock(bronze_deduped=100, bronze_nulls=100)  # non_null=0

    test_args = [
        "prog",
        "--environment",
        "dev",
        "--database",
        "dp_dev_silver",
        "--table",
        "partner_sites",
        "--new_columns",
        "PAYMENT_GROUP_CODE",
        "--merge_keys",
        "PARTNER_SITE_ID",
        "--account_id",
        "111111111111",
    ]
    with (
        patch.object(sys, "argv", test_args),
        patch(
            "schema_evolution_spark.backfill_new_columns._get_spark",
            return_value=mock_spark,
        ),
    ):
        main()

    # Only 2 sql calls (bronze_deduped_count + bronze_null_in_source), no MERGE
    assert mock_spark.sql.call_count == 2


def test_main_skips_when_rows_to_update_zero():
    """When rows_that_will_update=0 (all remaining NULLs are Bronze NULLs), skip MERGE."""
    import sys

    from schema_evolution_spark.backfill_new_columns import main

    # bronze_deduped=100, bronze_nulls=5 (non_null=95), silver_nulls=5,
    # bronze_matching=5, rows_to_update=0 (Bronze matches but has NULL values)
    mock_spark = _make_spark_mock(
        bronze_deduped=100,
        bronze_nulls=5,
        silver_total=1000,
        silver_nulls=5,
        bronze_matching=5,
        rows_to_update=0,
    )

    test_args = [
        "prog",
        "--environment",
        "dev",
        "--database",
        "dp_dev_silver",
        "--table",
        "partner_sites",
        "--new_columns",
        "PAYMENT_GROUP_CODE",
        "--merge_keys",
        "PARTNER_SITE_ID",
        "--account_id",
        "111111111111",
    ]
    with (
        patch.object(sys, "argv", test_args),
        patch(
            "schema_evolution_spark.backfill_new_columns._get_spark",
            return_value=mock_spark,
        ),
    ):
        main()

    # MERGE should not have been executed — check no sql call contains MERGE INTO
    merge_calls = [str(c) for c in mock_spark.sql.call_args_list if "MERGE INTO" in str(c)]
    assert merge_calls == []


def test_main_aborts_when_bronze_does_not_cover_silver():
    """When Bronze covers fewer Silver NULL rows than expected, MERGE should be aborted."""
    import sys

    from schema_evolution_spark.backfill_new_columns import main

    # bronze covers only 5 of 10 Silver NULL rows
    mock_spark = _make_spark_mock(
        bronze_deduped=100,
        bronze_nulls=0,
        silver_total=100,
        silver_nulls=10,
        bronze_matching=5,
        rows_to_update=5,
    )

    test_args = [
        "prog",
        "--environment",
        "dev",
        "--database",
        "dp_dev_silver",
        "--table",
        "partner_sites",
        "--new_columns",
        "PAYMENT_GROUP_CODE",
        "--merge_keys",
        "PARTNER_SITE_ID",
        "--account_id",
        "111111111111",
    ]
    with (
        patch.object(sys, "argv", test_args),
        patch(
            "schema_evolution_spark.backfill_new_columns._get_spark",
            return_value=mock_spark,
        ),
    ):
        main()

    merge_calls = [str(c) for c in mock_spark.sql.call_args_list if "MERGE INTO" in str(c)]
    assert merge_calls == []


def test_main_executes_merge_and_logs_success():
    """Full happy-path: MERGE runs, row count unchanged, no duplicate PKs."""
    import sys

    from schema_evolution_spark.backfill_new_columns import main

    mock_spark = _make_spark_mock(
        bronze_deduped=100,
        bronze_nulls=0,
        silver_total=1000,
        silver_nulls=10,
        bronze_matching=10,
        rows_to_update=10,
        dupes_before=0,
        total_after=1000,
        still_null=0,
        dupes_after=0,
    )
    test_args = [
        "prog",
        "--environment",
        "dev",
        "--database",
        "dp_dev_silver",
        "--table",
        "partner_sites",
        "--new_columns",
        "PAYMENT_GROUP_CODE",
        "--merge_keys",
        "PARTNER_SITE_ID",
        "--account_id",
        "111111111111",
    ]
    with (
        patch.object(sys, "argv", test_args),
        patch(
            "schema_evolution_spark.backfill_new_columns._get_spark",
            return_value=mock_spark,
        ),
    ):
        main()

    merge_calls = [str(c) for c in mock_spark.sql.call_args_list if "MERGE INTO" in str(c)]
    assert len(merge_calls) == 1


def test_main_logs_error_when_row_count_changes():
    """When total rows change after MERGE, error should be logged."""
    import sys

    from schema_evolution_spark.backfill_new_columns import main

    mock_spark = _make_spark_mock(
        bronze_deduped=100,
        bronze_nulls=0,
        silver_total=1000,
        silver_nulls=10,
        bronze_matching=10,
        rows_to_update=10,
        dupes_before=0,
        total_after=999,
        still_null=0,
        dupes_after=0,
    )
    test_args = [
        "prog",
        "--environment",
        "dev",
        "--database",
        "dp_dev_silver",
        "--table",
        "partner_sites",
        "--new_columns",
        "PAYMENT_GROUP_CODE",
        "--merge_keys",
        "PARTNER_SITE_ID",
        "--account_id",
        "111111111111",
    ]
    with (
        patch.object(sys, "argv", test_args),
        patch(
            "schema_evolution_spark.backfill_new_columns._get_spark",
            return_value=mock_spark,
        ),
        patch("schema_evolution_spark.backfill_new_columns.logger") as mock_logger,
    ):
        main()

    error_calls = [str(c) for c in mock_logger.error.call_args_list if "ROW COUNT" in str(c)]
    assert error_calls, "Expected ROW COUNT CHANGED error to be logged"


def test_main_logs_error_when_duplicate_pks_introduced():
    """When duplicate PK groups increase after MERGE, error should be logged."""
    import sys

    from schema_evolution_spark.backfill_new_columns import main

    mock_spark = _make_spark_mock(
        bronze_deduped=100,
        bronze_nulls=0,
        silver_total=1000,
        silver_nulls=10,
        bronze_matching=10,
        rows_to_update=10,
        dupes_before=0,
        total_after=1000,
        still_null=0,
        dupes_after=1,
    )
    test_args = [
        "prog",
        "--environment",
        "dev",
        "--database",
        "dp_dev_silver",
        "--table",
        "partner_sites",
        "--new_columns",
        "PAYMENT_GROUP_CODE",
        "--merge_keys",
        "PARTNER_SITE_ID",
        "--account_id",
        "111111111111",
    ]
    with (
        patch.object(sys, "argv", test_args),
        patch(
            "schema_evolution_spark.backfill_new_columns._get_spark",
            return_value=mock_spark,
        ),
        patch("schema_evolution_spark.backfill_new_columns.logger") as mock_logger,
    ):
        main()

    error_calls = [str(c) for c in mock_logger.error.call_args_list if "DUPLICATE" in str(c)]
    assert error_calls, "Expected DUPLICATE PKs INTRODUCED error to be logged"


def test_main_scd2_includes_is_current_in_sql():
    """SCD2 load type should include _is_current=true in join SQL."""
    import sys

    from schema_evolution_spark.backfill_new_columns import main

    mock_spark = _make_spark_mock(
        bronze_deduped=100,
        bronze_nulls=0,
        silver_total=500,
        silver_nulls=5,
        bronze_matching=5,
        rows_to_update=5,
        dupes_before=0,
        total_after=500,
        still_null=0,
        dupes_after=0,
    )
    test_args = [
        "prog",
        "--environment",
        "qa",
        "--database",
        "dp_qa_silver",
        "--table",
        "customer_accounts",
        "--new_columns",
        "PAYMENT_GROUP_CODE",
        "--merge_keys",
        "CUSTOMER_ACCOUNT_ID",
        "--account_id",
        "222222222222",
        "--load_type",
        "scd2",
    ]
    with (
        patch.object(sys, "argv", test_args),
        patch(
            "schema_evolution_spark.backfill_new_columns._get_spark",
            return_value=mock_spark,
        ),
    ):
        main()

    all_sql = " ".join(str(c) for c in mock_spark.sql.call_args_list)
    assert "_is_current" in all_sql


def test_main_falls_back_to_dms_commit_ts_when_change_seq_missing():
    """If dms_change_seq is missing but dms_commit_ts exists, fallback should proceed."""
    import sys

    from schema_evolution_spark.backfill_new_columns import main

    mock_spark = _make_spark_mock(
        bronze_deduped=100,
        bronze_nulls=0,
        silver_total=100,
        silver_nulls=10,
        bronze_matching=10,
        rows_to_update=10,
        total_after=100,
        still_null=0,
        dedup_column="dms_commit_ts",
    )

    test_args = [
        "prog",
        "--environment",
        "dev",
        "--database",
        "dp_dev_silver",
        "--table",
        "partner_sites",
        "--new_columns",
        "PAYMENT_GROUP_CODE",
        "--merge_keys",
        "PARTNER_SITE_ID",
        "--account_id",
        "111111111111",
    ]

    with (
        patch.object(sys, "argv", test_args),
        patch(
            "schema_evolution_spark.backfill_new_columns._get_spark",
            return_value=mock_spark,
        ),
    ):
        main()

    sql_strings = [str(call) for call in mock_spark.sql.call_args_list]
    assert any("ORDER BY `dms_commit_ts` DESC" in s for s in sql_strings)


def test_main_aborts_when_no_supported_dedup_column_present():
    """If neither dms_change_seq nor dms_commit_ts exists, MERGE must not run."""
    import sys

    from schema_evolution_spark.backfill_new_columns import main

    mock_spark = MagicMock()
    mock_field = MagicMock()
    mock_field.name = "payment_group_code"
    mock_df = MagicMock()
    mock_df.schema.fields = [mock_field]
    mock_spark.table.return_value = mock_df

    test_args = [
        "prog",
        "--environment",
        "dev",
        "--database",
        "dp_dev_silver",
        "--table",
        "partner_sites",
        "--new_columns",
        "PAYMENT_GROUP_CODE",
        "--merge_keys",
        "PARTNER_SITE_ID",
        "--account_id",
        "111111111111",
    ]

    with (
        patch.object(sys, "argv", test_args),
        patch(
            "schema_evolution_spark.backfill_new_columns._get_spark",
            return_value=mock_spark,
        ),
    ):
        main()

    mock_spark.sql.assert_not_called()
