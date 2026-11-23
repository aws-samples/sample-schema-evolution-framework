"""Unit tests for schema_evolution.apply — no AWS needed."""

import argparse
import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from schema_evolution.apply import (
    PartitionSourceColumnError,
    _rename_source_for_plans,
    apply_ddl,
    apply_migrations,
    apply_table_rename,
    check_backfill_partition_safety,
    list_plan_files,
    partition_source_columns,
    read_plan_json_local,
    read_plan_metadata_local,
    read_tracker,
    trigger_backfill,
)


def test_list_plan_files_empty():
    with tempfile.TemporaryDirectory() as d:
        assert list_plan_files(d) == {}


def test_list_plan_files():
    with tempfile.TemporaryDirectory() as d:
        layer_dir = os.path.join(d, "bronze", "my_table")
        os.makedirs(layer_dir)
        for name in ["20260101T000000_a.json", "20260102T000000_b.json"]:
            with open(os.path.join(layer_dir, name), "w") as f:
                json.dump({"changes": []}, f)
        # non-json file should be ignored
        with open(os.path.join(layer_dir, "readme.txt"), "w") as f:
            f.write("ignore me")

        result = list_plan_files(d)
        assert ("bronze", "my_table") in result
        assert len(result[("bronze", "my_table")]) == 2
        assert result[("bronze", "my_table")][0] == "20260101T000000_a.json"


def test_read_plan_json_local():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({"changes": [{"change_type": "add_column", "column_name": "x", "target_type": "string"}]}, f)
        f.flush()
        changes = read_plan_json_local(f.name)
    assert len(changes) == 1
    assert changes[0]["column_name"] == "x"
    os.unlink(f.name)


def test_read_plan_metadata_local():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({"metadata": {"layer": "gold", "table": "mytable"}, "changes": []}, f)
        f.flush()
        meta = read_plan_metadata_local(f.name)
    assert meta["layer"] == "gold"
    os.unlink(f.name)


def _make_migrations_dir(
    tmpdir: str,
    layer: str,
    table: str,
    plan_name: str,
    changes: list,
    environments: list[str] | None = None,
) -> str:
    """Write a plan JSON into a migrations directory structure and return the dir path."""
    table_dir = os.path.join(tmpdir, layer, table)
    os.makedirs(table_dir)
    metadata: dict = {"layer": layer, "table": table}
    if environments is not None:
        metadata["environments"] = environments
    with open(os.path.join(table_dir, plan_name), "w") as f:
        json.dump({"metadata": metadata, "changes": changes}, f)
    return tmpdir


def _make_args(migrations_dir: str, environment: str = "dev") -> argparse.Namespace:
    return argparse.Namespace(
        migrations_dir=migrations_dir,
        environment=environment,
        region="us-west-2",
        account_id="111111111111",
        bucket_prefix="dp",
        glue_job_name="dp-dev-glue-schema-evolution-backfill",
    )


@patch("schema_evolution.apply.load_catalog")
@patch("schema_evolution.apply.ensure_tracker")
@patch("schema_evolution.apply.read_tracker", return_value={})
@patch("schema_evolution.apply.wait_for_table", return_value=True)
@patch("schema_evolution.apply.batch_upsert_tracker")
@patch("schema_evolution.apply.apply_ddl", return_value=(True, None))
def test_apply_migrations_uses_environment_for_namespace(
    mock_ddl, mock_upsert, mock_wait, mock_tracker, mock_ensure, mock_catalog
):
    """Namespace passed to apply_ddl must be derived from --environment."""
    mock_cat = MagicMock()
    mock_catalog.return_value = mock_cat

    with tempfile.TemporaryDirectory() as tmpdir:
        changes = [{"change_type": "rename_column", "column_name": "cost_center_name", "previous_name": "cost_center"}]
        _make_migrations_dir(
            tmpdir,
            "gold",
            "global_skills_finder",
            "20260713T000000_plan.json",
            changes,
            environments=["qa"],
        )

        args = _make_args(tmpdir, environment="qa")
        apply_migrations(args)

    # namespace must be built from the deployed environment ("qa")
    ns_used = mock_ddl.call_args[0][1]
    assert ns_used == "dp_qa_gold"


@patch("schema_evolution.apply.load_catalog")
@patch("schema_evolution.apply.ensure_tracker")
@patch("schema_evolution.apply.read_tracker", return_value={})
@patch("schema_evolution.apply.wait_for_table", return_value=True)
@patch("schema_evolution.apply.batch_upsert_tracker")
def test_apply_migrations_ns_used_for_empty_plan(mock_upsert, mock_wait, mock_tracker, mock_ensure, mock_catalog):
    """ns is assigned on line 530 even when changes list is empty (status: applied, notes: Empty plan)."""
    mock_cat = MagicMock()
    mock_catalog.return_value = mock_cat

    with tempfile.TemporaryDirectory() as tmpdir:
        _make_migrations_dir(
            tmpdir,
            "gold",
            "global_skills_finder",
            "20260713T000000_plan.json",
            [],
            environments=["prod"],
        )

        args = _make_args(tmpdir, environment="prod")
        result = apply_migrations(args)

    assert result is True
    # empty plan → recorded as applied with no DDL calls needed
    upsert_rows = mock_upsert.call_args[0][2]
    assert upsert_rows[0]["status"] == "applied"
    assert upsert_rows[0]["notes"] == "Empty plan"


# ---------------------------------------------------------------------------
# Partition-source backfill guard
# ---------------------------------------------------------------------------

_INCOMPATIBLE = {
    "change_type": "incompatible_type_change",
    "column_name": "period_end_date",
    "current_type": "date",
    "target_type": "timestamp",
}

_INCOMPATIBLE_WITH_REPARTITION = {
    **_INCOMPATIBLE,
    "new_partition": {"transform": "month", "param": None},
}

_REPARTITION = {
    "change_type": "repartition",
    "column_name": "posting_date",
    "new_partition": {"transform": "month", "param": None},
}


def test_apply_ddl_repartition_needs_backfill():
    """A repartition change re-clusters existing data, so it routes to the Glue
    rebuild job (BACKFILL_NEEDED), never metadata-only DDL."""
    success, error = apply_ddl(MagicMock(), "dp_dev_gold", "order_costs", _REPARTITION)
    assert success is False
    assert error == "BACKFILL_NEEDED"


def _mock_table_with_fields(field_specs):
    """Build a mock Iceberg table whose schema().fields returns the given specs.

    field_specs: list of (name, required_bool).
    Returns (table, update_ctx) where update_ctx is the enter-value of
    update_schema() so assertions can inspect calls.
    """
    fields = []
    for name, required in field_specs:
        f = MagicMock()
        f.name = name
        f.required = required
        f.field_type = "string"
        fields.append(f)

    table = MagicMock()
    table.schema.return_value.fields = fields

    update_ctx = MagicMock()
    table.update_schema.return_value.__enter__ = MagicMock(return_value=update_ctx)
    table.update_schema.return_value.__exit__ = MagicMock(return_value=False)
    return table, update_ctx


def test_apply_ddl_set_column_not_null():
    """Enabling NOT NULL (optional -> required) uses allow_incompatible_changes."""
    table, update_ctx = _mock_table_with_fields([("id", False)])
    cat = MagicMock()
    cat.load_table.return_value = table

    change = {"change_type": "set_column_nullability", "column_name": "id", "target_not_null": True}
    success, error = apply_ddl(cat, "dp_dev_gold", "account_types", change)

    assert success is True
    assert error is None
    table.update_schema.assert_called_once_with(allow_incompatible_changes=True)
    update_ctx.update_column.assert_called_once_with("id", required=True)


def test_apply_ddl_set_column_nullable():
    """Disabling NOT NULL (required -> optional) uses make_column_optional."""
    table, update_ctx = _mock_table_with_fields([("id", True)])
    cat = MagicMock()
    cat.load_table.return_value = table

    change = {"change_type": "set_column_nullability", "column_name": "id", "target_not_null": False}
    success, error = apply_ddl(cat, "dp_dev_gold", "account_types", change)

    assert success is True
    assert error is None
    table.update_schema.assert_called_once_with()
    update_ctx.make_column_optional.assert_called_once_with("id")


def test_apply_ddl_set_column_nullability_idempotent():
    """No-op when the column is already in the target required state."""
    table, update_ctx = _mock_table_with_fields([("id", True)])
    cat = MagicMock()
    cat.load_table.return_value = table

    change = {"change_type": "set_column_nullability", "column_name": "id", "target_not_null": True}
    success, error = apply_ddl(cat, "dp_dev_gold", "account_types", change)

    assert success is True
    assert error is None
    table.update_schema.assert_not_called()


def test_apply_ddl_set_column_nullability_missing_flag():
    """Missing target_not_null returns an error."""
    table, _ = _mock_table_with_fields([("id", False)])
    cat = MagicMock()
    cat.load_table.return_value = table

    change = {"change_type": "set_column_nullability", "column_name": "id"}
    success, error = apply_ddl(cat, "dp_dev_gold", "account_types", change)

    assert success is False
    assert error is not None
    assert "target_not_null" in error


def test_apply_ddl_set_column_nullability_missing_column():
    """Nullability change on an absent column is a safe no-op."""
    table, update_ctx = _mock_table_with_fields([("other", True)])
    cat = MagicMock()
    cat.load_table.return_value = table

    change = {"change_type": "set_column_nullability", "column_name": "id", "target_not_null": True}
    success, error = apply_ddl(cat, "dp_dev_gold", "account_types", change)

    assert success is True
    assert error is None
    table.update_schema.assert_not_called()


def test_apply_ddl_standalone_rebuild_needs_backfill():
    """An explicit rebuild_table plan change routes to the Glue rebuild job."""
    change = {"change_type": "rebuild_table", "column_name": "__table__"}
    success, error = apply_ddl(MagicMock(), "dp_dev_gold", "order_costs", change)
    assert success is False
    assert error == "BACKFILL_NEEDED"


def test_apply_table_rename_uses_version_token():
    s3tables = MagicMock()
    s3tables.get_table.return_value = {"versionToken": "v123"}

    success, error = apply_table_rename(
        s3tables,
        "arn:aws:s3tables:us-west-2:123:bucket/dp-dev-gold-tables",
        "dp_dev_gold",
        "old_name",
        "new_name",
    )

    assert success is True
    assert error is None
    s3tables.rename_table.assert_called_once_with(
        tableBucketARN="arn:aws:s3tables:us-west-2:123:bucket/dp-dev-gold-tables",
        namespace="dp_dev_gold",
        name="old_name",
        newName="new_name",
        versionToken="v123",
    )


def test_apply_table_rename_is_idempotent_when_target_exists():
    from botocore.exceptions import ClientError

    s3tables = MagicMock()
    s3tables.get_table.side_effect = [
        ClientError({"Error": {"Code": "NotFoundException", "Message": "missing"}}, "GetTable"),
        {"name": "new_name"},
    ]

    success, error = apply_table_rename(s3tables, "bucket-arn", "namespace", "old_name", "new_name")

    assert success is True
    assert error is None
    s3tables.rename_table.assert_not_called()


def test_apply_table_rename_rejects_same_name():
    s3tables = MagicMock()

    success, error = apply_table_rename(s3tables, "bucket-arn", "namespace", "same", "SAME")

    assert success is False
    assert error is not None
    assert "different" in error
    s3tables.get_table.assert_not_called()


def test_apply_table_rename_rejects_empty_source_or_destination():
    s3tables = MagicMock()

    assert apply_table_rename(s3tables, "bucket", "ns", "", "new")[0] is False
    assert apply_table_rename(s3tables, "bucket", "ns", "old", "")[0] is False
    s3tables.get_table.assert_not_called()


def test_apply_table_rename_returns_non_not_found_source_error():
    s3tables = MagicMock()
    s3tables.get_table.side_effect = RuntimeError("access denied")

    success, error = apply_table_rename(s3tables, "bucket", "ns", "old", "new")

    assert success is False
    assert error == "access denied"
    s3tables.rename_table.assert_not_called()


def test_apply_table_rename_reports_missing_source_and_destination():
    from botocore.exceptions import ClientError

    missing = ClientError({"Error": {"Code": "NotFoundException", "Message": "missing"}}, "GetTable")
    s3tables = MagicMock()
    s3tables.get_table.side_effect = [missing, missing]

    success, error = apply_table_rename(s3tables, "bucket", "ns", "old", "new")

    assert success is False
    assert error is not None
    assert "were not found" in error
    s3tables.rename_table.assert_not_called()


def test_apply_table_rename_omits_missing_version_token_and_returns_api_error():
    s3tables = MagicMock()
    s3tables.get_table.return_value = {"name": "old"}
    s3tables.rename_table.side_effect = RuntimeError("rename rejected")

    success, error = apply_table_rename(s3tables, "bucket", "ns", "old", "new")

    assert success is False
    assert error == "rename rejected"
    s3tables.rename_table.assert_called_once_with(
        tableBucketARN="bucket",
        namespace="ns",
        name="old",
        newName="new",
    )


def test_rename_source_for_plans_reads_local_plan_and_ignores_non_rename():
    with tempfile.TemporaryDirectory() as tmpdir:
        _make_migrations_dir(
            tmpdir,
            "gold",
            "new_name",
            "rename.json",
            [{"change_type": "rename_table", "column_name": "__table__", "previous_name": " old_name "}],
        )
        _make_migrations_dir(
            tmpdir,
            "silver",
            "another_table",
            "add.json",
            [{"change_type": "add_column", "column_name": "value", "target_type": "string"}],
        )

        source = _rename_source_for_plans(
            tmpdir,
            "gold",
            "new_name",
            ["rename.json"],
            from_s3=False,
        )
        no_source = _rename_source_for_plans(
            tmpdir,
            "silver",
            "another_table",
            ["add.json"],
            from_s3=False,
        )

    assert source == "old_name"
    assert no_source is None


def test_rename_source_for_plans_skips_unreadable_plan():
    with tempfile.TemporaryDirectory() as tmpdir:
        source = _rename_source_for_plans(
            tmpdir,
            "gold",
            "new_name",
            ["missing.json"],
            from_s3=False,
        )

    assert source is None


def test_rename_source_for_plans_reads_s3_plan():
    with patch(
        "schema_evolution.apply.read_plan_json_s3",
        return_value=[{"change_type": "rename_table", "column_name": "__table__", "previous_name": "old_name"}],
    ) as read_plan:
        source = _rename_source_for_plans(
            "s3://amzn-s3-demo-bucket/migrations",
            "gold",
            "new_name",
            ["rename.json"],
            from_s3=True,
            s3_client=MagicMock(),
        )

    assert source == "old_name"
    read_plan.assert_called_once()


@pytest.mark.parametrize(
    "changes",
    [
        [
            {"change_type": "rename_table", "column_name": "__table__", "previous_name": "old_name"},
            {"change_type": "add_column", "column_name": "value", "target_type": "string"},
        ],
        [{"change_type": "rename_table", "column_name": "not_table", "previous_name": "old_name"}],
        [{"change_type": "rename_table", "column_name": "__table__"}],
        [{"change_type": "rename_table", "column_name": "__table__", "previous_name": "new_name"}],
    ],
)
def test_rename_source_for_plans_rejects_invalid_rename(changes):
    with tempfile.TemporaryDirectory() as tmpdir:
        _make_migrations_dir(tmpdir, "gold", "new_name", "rename.json", changes)

        with pytest.raises(ValueError):
            _rename_source_for_plans(
                tmpdir,
                "gold",
                "new_name",
                ["rename.json"],
                from_s3=False,
            )


def test_rename_source_for_plans_rejects_multiple_sources():
    with tempfile.TemporaryDirectory() as tmpdir:
        table_dir = os.path.join(tmpdir, "gold", "new_name")
        os.makedirs(table_dir)
        for plan_name, previous_name in (("first.json", "old_one"), ("second.json", "old_two")):
            with open(os.path.join(table_dir, plan_name), "w") as plan_file:
                json.dump(
                    {
                        "changes": [
                            {
                                "change_type": "rename_table",
                                "column_name": "__table__",
                                "previous_name": previous_name,
                            }
                        ]
                    },
                    plan_file,
                )

        with pytest.raises(ValueError, match="multiple rename sources"):
            _rename_source_for_plans(
                tmpdir,
                "gold",
                "new_name",
                ["first.json", "second.json"],
                from_s3=False,
            )


def test_apply_migrations_skips_missing_destination_without_rename_plan():
    catalog = MagicMock()
    catalog.load_table.side_effect = RuntimeError("not found")

    with (
        patch("schema_evolution.apply.load_catalog", return_value=catalog),
        patch("schema_evolution.apply.ensure_tracker"),
        patch("schema_evolution.apply.read_tracker_snapshot", return_value={}),
        patch("schema_evolution.apply.batch_upsert_tracker") as upsert,
        patch("schema_evolution.apply.boto3.client"),
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        _make_migrations_dir(
            tmpdir,
            "gold",
            "new_name",
            "20260930T000000_add.json",
            [{"change_type": "add_column", "column_name": "value", "target_type": "string"}],
        )
        result = apply_migrations(_make_args(tmpdir, environment="prod"))

    assert result is True
    # The plan is recorded as waiting so the tracker shows why it has not run.
    row = upsert.call_args.args[2][0]
    assert row["status"] == "waiting_for_table"
    assert "does not exist yet" in row["notes"]


def test_apply_migrations_skips_rename_when_source_table_is_missing():
    catalog = MagicMock()
    catalog.load_table.side_effect = RuntimeError("not found")

    with (
        patch("schema_evolution.apply.load_catalog", return_value=catalog),
        patch("schema_evolution.apply.ensure_tracker"),
        patch("schema_evolution.apply.read_tracker_snapshot", return_value={}),
        patch("schema_evolution.apply.batch_upsert_tracker") as upsert,
        patch("schema_evolution.apply.boto3.client"),
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        _make_migrations_dir(
            tmpdir,
            "gold",
            "new_name",
            "20260930T000000_rename.json",
            [{"change_type": "rename_table", "column_name": "__table__", "previous_name": "old_name"}],
        )
        result = apply_migrations(_make_args(tmpdir, environment="prod"))

    assert result is True
    assert catalog.load_table.call_count == 2
    row = upsert.call_args.args[2][0]
    assert row["status"] == "waiting_for_table"
    assert "Neither rename source" in row["notes"]


def test_apply_migrations_fails_malformed_rename_when_destination_missing():
    catalog = MagicMock()
    catalog.load_table.side_effect = RuntimeError("destination missing")

    with (
        patch("schema_evolution.apply.load_catalog", return_value=catalog),
        patch("schema_evolution.apply.ensure_tracker"),
        patch("schema_evolution.apply.read_tracker_snapshot", return_value={}),
        patch("schema_evolution.apply.batch_upsert_tracker"),
        patch("schema_evolution.apply.boto3.client"),
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        _make_migrations_dir(
            tmpdir,
            "gold",
            "new_name",
            "20260930T000000_rename.json",
            [{"change_type": "rename_table", "column_name": "__table__", "previous_name": "new_name"}],
        )
        result = apply_migrations(_make_args(tmpdir, environment="prod"))

    assert result is False


def test_apply_migrations_records_native_rename_api_error():
    catalog = MagicMock()

    def load_table(fqn):
        if fqn == "dp_prod_gold.new_name":
            raise RuntimeError("destination missing")
        return MagicMock()

    catalog.load_table.side_effect = load_table
    s3tables = MagicMock()
    s3tables.get_table.return_value = {"versionToken": "v123"}
    s3tables.rename_table.side_effect = RuntimeError("rename rejected")
    glue = MagicMock()

    with (
        patch("schema_evolution.apply.load_catalog", return_value=catalog),
        patch("schema_evolution.apply.ensure_tracker"),
        patch("schema_evolution.apply.read_tracker", return_value={}),
        patch("schema_evolution.apply.wait_for_table", return_value=True),
        patch("schema_evolution.apply.batch_upsert_tracker") as upsert,
        patch(
            "schema_evolution.apply.boto3.client",
            side_effect=lambda service, **kwargs: {"glue": glue, "s3tables": s3tables}[service],
        ),
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        _make_migrations_dir(
            tmpdir,
            "gold",
            "new_name",
            "20260930T000000_rename.json",
            [{"change_type": "rename_table", "column_name": "__table__", "previous_name": "old_name"}],
        )
        result = apply_migrations(_make_args(tmpdir, environment="prod"))

    assert result is False
    assert upsert.call_args.args[2][0]["status"] == "failed"
    assert "rename rejected" in upsert.call_args.args[2][0]["notes"]


def test_apply_migrations_renames_table_to_destination_plan_name():
    catalog = MagicMock()

    def load_table(fqn):
        if fqn == "dp_prod_gold.new_name":
            raise ValueError("destination not created yet")
        return MagicMock()

    catalog.load_table.side_effect = load_table
    s3tables = MagicMock()
    s3tables.get_table.return_value = {"versionToken": "v123"}
    glue = MagicMock()

    with (
        patch("schema_evolution.apply.load_catalog", return_value=catalog),
        patch("schema_evolution.apply.ensure_tracker"),
        patch("schema_evolution.apply.read_tracker", return_value={}),
        patch("schema_evolution.apply.wait_for_table", return_value=True),
        patch("schema_evolution.apply.batch_upsert_tracker") as upsert,
        patch(
            "schema_evolution.apply.boto3.client",
            side_effect=lambda service, **kwargs: {"glue": glue, "s3tables": s3tables}[service],
        ),
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        _make_migrations_dir(
            tmpdir,
            "gold",
            "new_name",
            "20260930T000000_rename.json",
            [{"change_type": "rename_table", "column_name": "__table__", "previous_name": "old_name"}],
            environments=["prod"],
        )

        result = apply_migrations(_make_args(tmpdir, environment="prod"))

    assert result is True
    s3tables.rename_table.assert_called_once()
    assert upsert.call_args.args[1] == "new_name"
    assert upsert.call_args.args[2][0]["status"] == "applied"


def test_apply_migrations_rejects_mixed_rename_plan_before_mutations():
    catalog = MagicMock()
    s3tables = MagicMock()
    glue = MagicMock()

    with (
        patch("schema_evolution.apply.load_catalog", return_value=catalog),
        patch("schema_evolution.apply.ensure_tracker"),
        patch("schema_evolution.apply.read_tracker", return_value={}),
        patch("schema_evolution.apply.wait_for_table", return_value=True),
        patch("schema_evolution.apply.batch_upsert_tracker") as upsert,
        patch("schema_evolution.apply.apply_ddl") as apply_ddl_mock,
        patch(
            "schema_evolution.apply.boto3.client",
            side_effect=lambda service, **kwargs: {"glue": glue, "s3tables": s3tables}[service],
        ),
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        _make_migrations_dir(
            tmpdir,
            "gold",
            "new_name",
            "20260930T000000_mixed.json",
            [
                {"change_type": "rename_table", "column_name": "__table__", "previous_name": "old_name"},
                {"change_type": "add_column", "column_name": "new_col", "target_type": "string"},
            ],
            environments=["prod"],
        )

        result = apply_migrations(_make_args(tmpdir, environment="prod"))

    assert result is False
    assert upsert.call_args.args[2][0]["status"] == "failed"
    s3tables.rename_table.assert_not_called()
    apply_ddl_mock.assert_not_called()


def test_apply_ddl_sort_order_rebuild_defaults_to_true():
    """Sort-order changes rebuild unless the plan explicitly opts out."""
    change = {"change_type": "set_sort_order", "column_name": "__sort_order__", "sort_fields": []}
    success, error = apply_ddl(MagicMock(), "dp_dev_gold", "projects", change)
    assert success is False
    assert error == "BACKFILL_NEEDED"


@patch("schema_evolution.apply.get_table_partition_spec")
def test_trigger_backfill_includes_repartition_change(mock_spec):
    """A repartition change must be forwarded to the Glue job payload."""
    mock_spec.return_value = []
    glue = MagicMock()
    glue.start_job_run.return_value = {"JobRunId": "jr_repart_only"}
    run_id = trigger_backfill(
        glue,
        "job",
        [_REPARTITION],
        "s3tables",
        "dp_dev_gold",
        "order_costs",
        "us-west-2",
        "111111111111",
        "dp-dev-gold-tables",
        plan_name="p",
        table=MagicMock(),
    )
    assert run_id == "jr_repart_only"
    sent = json.loads(glue.start_job_run.call_args.kwargs["Arguments"]["--changes_json"])
    assert [c["change_type"] for c in sent] == ["repartition"]
    assert sent[0]["column_name"] == "posting_date"


@patch("schema_evolution.apply.get_table_sort_order", return_value=[{"column": "order_id", "direction": "asc"}])
@patch("schema_evolution.apply.get_table_partition_spec", return_value=[])
def test_trigger_backfill_preserves_sort_order_for_standalone_rebuild(mock_spec, mock_sort):
    """A standalone rebuild carries the table's current sort order to the Glue job."""
    glue = MagicMock()
    glue.start_job_run.return_value = {"JobRunId": "jr_rebuild"}
    change = {"change_type": "rebuild_table", "column_name": "__table__"}
    trigger_backfill(
        glue,
        "job",
        [change],
        "s3tables",
        "dp_dev_gold",
        "order_costs",
        "us-west-2",
        "111111111111",
        "dp-dev-gold-tables",
        table=MagicMock(),
    )
    sent = json.loads(glue.start_job_run.call_args.kwargs["Arguments"]["--changes_json"])
    assert sent == [
        {
            "change_type": "rebuild_table",
            "column_name": "__table__",
            "sort_fields": [{"column": "order_id", "direction": "asc"}],
        }
    ]


@patch("schema_evolution.apply.get_table_partition_spec")
def test_check_backfill_ignores_repartition_change(mock_spec):
    """A repartition change targets a non-partition column (the new key), so the
    incompatible-type-change partition-source guard must not flag it."""
    mock_spec.return_value = [{"column": "period_end_date", "transform": "month", "transform_param": None}]
    assert check_backfill_partition_safety(MagicMock(), [_REPARTITION]) == []


@patch("schema_evolution.apply.get_table_partition_spec")
def test_partition_source_columns_lowercased(mock_spec):
    mock_spec.return_value = [
        {"column": "Period_End_Date", "transform": "identity", "transform_param": None},
        {"column": "REGION", "transform": "identity", "transform_param": None},
    ]
    assert partition_source_columns(MagicMock()) == {"period_end_date", "region"}


@patch("schema_evolution.apply.get_table_partition_spec")
def test_check_backfill_flags_partition_source(mock_spec):
    mock_spec.return_value = [{"column": "period_end_date", "transform": "month", "transform_param": None}]
    blockers = check_backfill_partition_safety(MagicMock(), [_INCOMPATIBLE])
    assert blockers == ["period_end_date"]


@patch("schema_evolution.apply.get_table_partition_spec")
def test_check_backfill_allows_partition_source_with_repartition(mock_spec):
    mock_spec.return_value = [{"column": "period_end_date", "transform": "identity", "transform_param": None}]
    assert check_backfill_partition_safety(MagicMock(), [_INCOMPATIBLE_WITH_REPARTITION]) == []


@patch("schema_evolution.apply.get_table_partition_spec")
def test_check_backfill_safe_when_not_partitioned(mock_spec):
    mock_spec.return_value = []
    assert check_backfill_partition_safety(MagicMock(), [_INCOMPATIBLE]) == []


@patch("schema_evolution.apply.get_table_partition_spec")
def test_check_backfill_ignores_non_incompatible_changes(mock_spec):
    mock_spec.return_value = [{"column": "region", "transform": "identity", "transform_param": None}]
    changes = [{"change_type": "add_column", "column_name": "region", "target_type": "string"}]
    assert check_backfill_partition_safety(MagicMock(), changes) == []


@patch("schema_evolution.apply.get_table_partition_spec")
def test_trigger_backfill_refuses_partition_source(mock_spec):
    mock_spec.return_value = [{"column": "period_end_date", "transform": "identity", "transform_param": None}]
    glue = MagicMock()
    with pytest.raises(PartitionSourceColumnError):
        trigger_backfill(
            glue,
            "job",
            [_INCOMPATIBLE],
            "s3tables",
            "dp_dev_gold",
            "orders",
            "us-west-2",
            "111111111111",
            "dp-dev-gold-tables",
            plan_name="p",
            table=MagicMock(),
        )
    glue.start_job_run.assert_not_called()


@patch("schema_evolution.apply.get_table_partition_spec")
def test_trigger_backfill_allows_partition_source_with_repartition(mock_spec):
    mock_spec.return_value = [{"column": "period_end_date", "transform": "identity", "transform_param": None}]
    glue = MagicMock()
    glue.start_job_run.return_value = {"JobRunId": "jr_repart"}
    run_id = trigger_backfill(
        glue,
        "job",
        [_INCOMPATIBLE_WITH_REPARTITION],
        "s3tables",
        "dp_dev_gold",
        "orders",
        "us-west-2",
        "111111111111",
        "dp-dev-gold-tables",
        plan_name="p",
        table=MagicMock(),
    )
    assert run_id == "jr_repart"
    glue.start_job_run.assert_called_once()


@patch("schema_evolution.apply.get_table_partition_spec")
def test_trigger_backfill_proceeds_when_safe(mock_spec):
    mock_spec.return_value = []
    glue = MagicMock()
    glue.start_job_run.return_value = {"JobRunId": "jr_123"}
    run_id = trigger_backfill(
        glue,
        "job",
        [_INCOMPATIBLE],
        "s3tables",
        "dp_dev_gold",
        "orders",
        "us-west-2",
        "111111111111",
        "dp-dev-gold-tables",
        plan_name="p",
        table=MagicMock(),
    )
    assert run_id == "jr_123"
    glue.start_job_run.assert_called_once()


def test_trigger_backfill_skips_guard_when_table_none():
    """Backwards compatible: no table supplied means no partition check (legacy callers)."""
    glue = MagicMock()
    glue.start_job_run.return_value = {"JobRunId": "jr_legacy"}
    run_id = trigger_backfill(
        glue,
        "job",
        [_INCOMPATIBLE],
        "s3tables",
        "dp_dev_gold",
        "orders",
        "us-west-2",
        "111111111111",
        "dp-dev-gold-tables",
        plan_name="p",
    )
    assert run_id == "jr_legacy"


def _mock_catalog_with_tracker_rows(rows, include_notes=True):
    """Build a mock catalog whose tracker scan returns the given rows as arrow."""
    import pyarrow as pa

    fields = [
        ("table_name", [r["table_name"] for r in rows]),
        ("plan_name", [r["plan_name"] for r in rows]),
        ("status", [r["status"] for r in rows]),
    ]
    if include_notes:
        fields.append(("notes", [r.get("notes") for r in rows]))
    arrow = pa.table({name: vals for name, vals in fields})

    tbl = MagicMock()
    tbl.scan.return_value.to_arrow.return_value = arrow
    cat = MagicMock()
    cat.load_table.return_value = tbl
    return cat


def test_read_tracker_returns_status_and_notes():
    cat = _mock_catalog_with_tracker_rows(
        [
            {
                "table_name": "orders",
                "plan_name": "p1.json",
                "status": "failed",
                "notes": "Concurrent write detected on ... Pause upstream writers",
            }
        ]
    )
    result = read_tracker(cat, "orders")
    assert result["p1.json"]["status"] == "failed"
    assert "Concurrent write detected" in result["p1.json"]["notes"]


def test_read_tracker_notes_none_when_column_absent():
    cat = _mock_catalog_with_tracker_rows(
        [{"table_name": "orders", "plan_name": "p1.json", "status": "applied"}],
        include_notes=False,
    )
    result = read_tracker(cat, "orders")
    assert result["p1.json"]["status"] == "applied"
    assert result["p1.json"]["notes"] is None


# ---------------------------------------------------------------------------
# S3 plan source
# ---------------------------------------------------------------------------


def _s3_paginator(keys: list[str]):
    """Build a mock S3 client whose list_objects_v2 paginator yields the given keys."""
    client = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [{"Contents": [{"Key": k} for k in keys]}]
    client.get_paginator.return_value = paginator
    return client


def test_is_s3_uri():
    from schema_evolution.apply import is_s3_uri

    assert is_s3_uri("s3://amzn-s3-demo-bucket/migrations")
    assert not is_s3_uri("migrations/")
    assert not is_s3_uri("/abs/migrations")


def test_list_plan_files_s3_groups_and_sorts():
    from schema_evolution.apply import list_plan_files_s3

    client = _s3_paginator(
        [
            "migrations/gold/order_costs/20260720T120000_repartition.json",
            "migrations/gold/order_costs/20260717T120000_types.json",
            "migrations/gold/orders/20260717T120000_types.json",
            "migrations/gold/orders/notes.txt",  # ignored
            "migrations/gold/orders/extra/deep.json",  # wrong depth, ignored
        ]
    )
    result = list_plan_files_s3("s3://amzn-s3-demo-bucket/migrations", client)
    assert result[("gold", "order_costs")] == [
        "20260717T120000_types.json",
        "20260720T120000_repartition.json",
    ]
    assert result[("gold", "orders")] == ["20260717T120000_types.json"]
    # non-json and wrong-depth entries excluded
    assert all(not p.endswith(".txt") for plans in result.values() for p in plans)


def test_read_plan_json_s3():
    from schema_evolution.apply import read_plan_json_s3

    client = MagicMock()
    body = MagicMock()
    body.read.return_value = json.dumps(
        {"changes": [{"change_type": "add_column", "column_name": "x", "target_type": "string"}]}
    ).encode()
    client.get_object.return_value = {"Body": body}

    changes = read_plan_json_s3("s3://amzn-s3-demo-bucket/migrations", "gold", "orders", "p.json", client)
    assert changes[0]["column_name"] == "x"
    key = client.get_object.call_args.kwargs["Key"]
    assert key == "migrations/gold/orders/p.json"


# ---------------------------------------------------------------------------
# Dead-backfill self-heal
# ---------------------------------------------------------------------------


def test_extract_run_id():
    from schema_evolution.apply import _extract_run_id

    assert _extract_run_id("Glue job run: jr_abc123") == "jr_abc123"
    assert _extract_run_id("Auto-reset by schema-evolution-apply: Glue run jr_x FAILED") is None
    assert _extract_run_id(None) is None
    assert _extract_run_id("no run here") is None


def test_heal_dead_backfills_resets_terminal_run():
    """A backfilling row whose Glue run is FAILED is reset to failed."""
    from schema_evolution.apply import _heal_dead_backfills

    glue = MagicMock()
    glue.get_job_run.return_value = {"JobRun": {"JobRunState": "FAILED", "ErrorMessage": "OOM"}}
    catalog = MagicMock()
    tracker = {"p1.json": {"status": "backfilling", "notes": "Glue job run: jr_dead"}}

    with patch("schema_evolution.apply.batch_upsert_tracker") as mock_upsert:
        healed = _heal_dead_backfills(
            glue, catalog, "dp-prod-glue-schema-evolution-backfill", "order_costs", tracker
        )

    assert healed is True
    rows = mock_upsert.call_args[0][2]
    assert rows[0]["plan_name"] == "p1.json"
    assert rows[0]["status"] == "failed"
    assert "jr_dead" in rows[0]["notes"]
    assert "FAILED" in rows[0]["notes"]


def test_heal_dead_backfills_ignores_running_run():
    """A backfilling row whose Glue run is still RUNNING is left untouched."""
    from schema_evolution.apply import _heal_dead_backfills

    glue = MagicMock()
    glue.get_job_run.return_value = {"JobRun": {"JobRunState": "RUNNING"}}
    catalog = MagicMock()
    tracker = {"p1.json": {"status": "backfilling", "notes": "Glue job run: jr_live"}}

    with patch("schema_evolution.apply.batch_upsert_tracker") as mock_upsert:
        healed = _heal_dead_backfills(
            glue, catalog, "dp-prod-glue-schema-evolution-backfill", "order_costs", tracker
        )

    assert healed is False
    mock_upsert.assert_not_called()


def test_heal_dead_backfills_skips_rows_without_run_id():
    """A backfilling row with no recorded run id is left for the bounded wait."""
    from schema_evolution.apply import _heal_dead_backfills

    glue = MagicMock()
    catalog = MagicMock()
    tracker = {"p1.json": {"status": "backfilling", "notes": None}}

    with patch("schema_evolution.apply.batch_upsert_tracker") as mock_upsert:
        healed = _heal_dead_backfills(glue, catalog, "job", "order_costs", tracker)

    assert healed is False
    glue.get_job_run.assert_not_called()
    mock_upsert.assert_not_called()


def test_heal_dead_backfills_swallows_glue_error():
    """If get_job_run raises, the row is left untouched (no reset, no crash)."""
    from schema_evolution.apply import _heal_dead_backfills

    glue = MagicMock()
    glue.get_job_run.side_effect = RuntimeError("throttled")
    catalog = MagicMock()
    tracker = {"p1.json": {"status": "backfilling", "notes": "Glue job run: jr_x"}}

    with patch("schema_evolution.apply.batch_upsert_tracker") as mock_upsert:
        healed = _heal_dead_backfills(glue, catalog, "job", "order_costs", tracker)

    assert healed is False
    mock_upsert.assert_not_called()


# ---------------------------------------------------------------------------
# apply_migrations S3 plan source branch
# ---------------------------------------------------------------------------


@patch("schema_evolution.apply.load_catalog")
@patch("schema_evolution.apply.ensure_tracker")
@patch("schema_evolution.apply.read_tracker", return_value={})
@patch("schema_evolution.apply.wait_for_table", return_value=True)
@patch("schema_evolution.apply.batch_upsert_tracker")
@patch("schema_evolution.apply.apply_ddl", return_value=(True, None))
@patch("schema_evolution.apply.read_plan_json_s3")
@patch("schema_evolution.apply.read_plan_metadata_s3")
@patch("schema_evolution.apply.list_plan_files_s3")
@patch("schema_evolution.apply.boto3")
def test_apply_migrations_reads_plans_from_s3(
    mock_boto3,
    mock_list_s3,
    mock_read_meta_s3,
    mock_read_s3,
    mock_ddl,
    mock_upsert,
    mock_wait,
    mock_tracker,
    mock_ensure,
    mock_catalog,
):
    """When migrations_dir is an s3:// uri, plans are listed/read from S3."""
    mock_catalog.return_value = MagicMock()
    mock_list_s3.return_value = {("gold", "orders"): ["p1.json"]}
    mock_read_meta_s3.return_value = {"layer": "gold", "table": "orders"}
    mock_read_s3.return_value = [{"change_type": "add_column", "column_name": "x", "target_type": "string"}]

    args = _make_args("s3://amzn-s3-demo-bucket/migrations", environment="prod")
    result = apply_migrations(args)

    assert result is True
    mock_list_s3.assert_called_once()
    mock_read_s3.assert_called_once()
    # applied via metadata DDL, recorded applied
    assert mock_upsert.call_args[0][2][0]["status"] == "applied"


@patch("schema_evolution.apply.load_catalog")
@patch("schema_evolution.apply.ensure_tracker")
@patch("schema_evolution.apply.read_tracker", return_value={})
@patch("schema_evolution.apply.wait_for_table", return_value=True)
@patch("schema_evolution.apply.batch_upsert_tracker")
@patch("schema_evolution.apply.read_plan_json_s3", side_effect=RuntimeError("boom"))
@patch("schema_evolution.apply.read_plan_metadata_s3")
@patch("schema_evolution.apply.list_plan_files_s3")
@patch("schema_evolution.apply.boto3")
def test_apply_migrations_s3_read_failure_marks_failed(
    mock_boto3,
    mock_list_s3,
    mock_read_meta_s3,
    mock_read_s3,
    mock_upsert,
    mock_wait,
    mock_tracker,
    mock_ensure,
    mock_catalog,
):
    """A failed S3 plan read records the plan as failed (not a crash)."""
    mock_catalog.return_value = MagicMock()
    mock_list_s3.return_value = {("gold", "orders"): ["p1.json"]}
    mock_read_meta_s3.return_value = {"layer": "gold", "table": "orders"}

    args = _make_args("s3://amzn-s3-demo-bucket/migrations", environment="prod")
    result = apply_migrations(args)

    assert result is False
    row = mock_upsert.call_args[0][2][0]
    assert row["status"] == "failed"
    assert "Plan read failed" in row["notes"]


# ---------------------------------------------------------------------------
# CLI entrypoints
# ---------------------------------------------------------------------------


def test_build_args_defaults_glue_job_name():
    from schema_evolution.apply import _build_args_from_argv

    args = _build_args_from_argv(["--account_id", "123", "--environment", "qa"])
    assert args.glue_job_name == "dp-qa-glue-schema-evolution-backfill"
    assert args.bucket_prefix == "dp"
    assert args.migrations_dir == "migrations/"


def test_main_exits_nonzero_on_failure():
    from schema_evolution import apply as apply_mod

    with (
        patch.object(apply_mod, "_build_args_from_argv", return_value=MagicMock()),
        patch.object(apply_mod, "apply_migrations", return_value=False),
        pytest.raises(SystemExit) as exc,
    ):
        apply_mod.main()
    assert exc.value.code == 1


def test_main_succeeds_on_success(capsys):
    from schema_evolution import apply as apply_mod

    with (
        patch.object(apply_mod, "_build_args_from_argv", return_value=MagicMock()),
        patch.object(apply_mod, "apply_migrations", return_value=True),
    ):
        apply_mod.main()  # no SystemExit on success


def test_list_plan_files_s3_default_client():
    """Covers the boto3 default-client branch when no s3_client is passed."""
    from schema_evolution.apply import list_plan_files_s3

    with patch("schema_evolution.apply.boto3") as mock_boto3:
        paginator = MagicMock()
        paginator.paginate.return_value = [{"Contents": []}]
        mock_boto3.client.return_value.get_paginator.return_value = paginator
        result = list_plan_files_s3("s3://amzn-s3-demo-bucket/migrations")

    assert result == {}
    mock_boto3.client.assert_called_once_with("s3")


def test_read_plan_json_s3_default_client():
    """Covers the boto3 default-client branch for read_plan_json_s3."""
    from schema_evolution.apply import read_plan_json_s3

    with patch("schema_evolution.apply.boto3") as mock_boto3:
        body = MagicMock()
        body.read.return_value = json.dumps({"changes": []}).encode()
        mock_boto3.client.return_value.get_object.return_value = {"Body": body}
        changes = read_plan_json_s3("s3://amzn-s3-demo-bucket/migrations", "gold", "t", "p.json")

    assert changes == []
    mock_boto3.client.assert_called_once_with("s3")


def test_read_plan_metadata_s3_default_client():
    """Covers the boto3 default-client branch for read_plan_metadata_s3."""
    from schema_evolution.apply import read_plan_metadata_s3

    with patch("schema_evolution.apply.boto3") as mock_boto3:
        body = MagicMock()
        body.read.return_value = json.dumps({"metadata": {"layer": "gold", "table": "t"}}).encode()
        mock_boto3.client.return_value.get_object.return_value = {"Body": body}
        metadata = read_plan_metadata_s3("s3://amzn-s3-demo-bucket/migrations", "gold", "t", "p.json")

    assert metadata["layer"] == "gold"
    mock_boto3.client.assert_called_once_with("s3")


@patch("schema_evolution.apply.load_catalog")
@patch("schema_evolution.apply.ensure_tracker")
@patch("schema_evolution.apply.wait_for_table", return_value=True)
@patch("schema_evolution.apply.batch_upsert_tracker")
@patch("schema_evolution.apply.read_plan_json_local", side_effect=FileNotFoundError("gone"))
def test_apply_migrations_local_plan_not_found(mock_read, mock_upsert, mock_wait, mock_ensure, mock_catalog):
    """A local plan whose file cannot be located is marked failed."""
    mock_catalog.return_value = MagicMock()

    with (
        patch("schema_evolution.apply.read_tracker", return_value={}),
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        # plan listed but removed before it is read
        _make_migrations_dir(
            tmpdir,
            "gold",
            "orders",
            "20260101T000000_p.json",
            [{"change_type": "x"}],
            environments=["prod"],
        )
        args = _make_args(tmpdir, environment="prod")
        result = apply_migrations(args)

    assert result is False
    row = mock_upsert.call_args[0][2][0]
    assert row["status"] == "failed"
    assert "not found" in row["notes"].lower()


@patch("schema_evolution.apply.load_catalog")
@patch("schema_evolution.apply.ensure_tracker")
@patch("schema_evolution.apply.wait_for_table", return_value=True)
@patch("schema_evolution.apply.batch_upsert_tracker")
def test_apply_migrations_fails_if_plan_env_metadata_invalid(mock_upsert, mock_wait, mock_ensure, mock_catalog):
    """Ambiguous env metadata must fail fast to prevent wrong-env writes."""
    from schema_evolution.apply import ApplyResult

    mock_catalog.return_value = MagicMock()

    with tempfile.TemporaryDirectory() as tmpdir:
        table_dir = os.path.join(tmpdir, "gold", "orders")
        os.makedirs(table_dir)
        with open(os.path.join(table_dir, "20260101T000000_p.json"), "w") as f:
            json.dump(
                {
                    "metadata": {
                        "table": "orders",
                        "environment": "dev",
                        "environments": ["prod"],
                    },
                    "changes": [{"change_type": "x"}],
                },
                f,
            )

        args = _make_args(tmpdir, environment="prod")
        result = apply_migrations(args, return_details=True)

    assert isinstance(result, ApplyResult)
    assert result.success is False
    assert result.pending_work is False
    mock_upsert.assert_not_called()


class TestPlanTargetsEnvironment:
    """Resolution of which environments a plan applies to."""

    def test_absent_env_key_targets_all_environments(self) -> None:
        from schema_evolution.apply import plan_targets_environment

        for env in ("dev", "qa", "prod"):
            assert plan_targets_environment({"table": "t"}, env, "gold") is True

    def test_string_env_value(self) -> None:
        from schema_evolution.apply import plan_targets_environment

        meta = {"table": "t", "environments": "qa"}
        assert plan_targets_environment(meta, "qa", "gold") is True
        assert plan_targets_environment(meta, "dev", "gold") is False

    def test_list_env_value(self) -> None:
        from schema_evolution.apply import plan_targets_environment

        meta = {"table": "t", "environments": ["dev", "qa"]}
        assert plan_targets_environment(meta, "dev", "gold") is True
        assert plan_targets_environment(meta, "qa", "gold") is True
        assert plan_targets_environment(meta, "prod", "gold") is False

    def test_singular_environment_alias(self) -> None:
        from schema_evolution.apply import plan_targets_environment

        meta = {"table": "t", "environment": ["prod"]}
        assert plan_targets_environment(meta, "prod", "gold") is True
        assert plan_targets_environment(meta, "dev", "gold") is False

    def test_case_and_whitespace_insensitive(self) -> None:
        from schema_evolution.apply import plan_targets_environment

        meta = {"table": "t", "environments": [" DEV "]}
        assert plan_targets_environment(meta, "dev", "gold") is True

    def test_both_env_keys_raises(self) -> None:
        from schema_evolution.apply import plan_targets_environment

        meta = {"table": "t", "environment": "dev", "environments": ["dev"]}
        with pytest.raises(ValueError, match="must not set both"):
            plan_targets_environment(meta, "dev", "gold")

    def test_empty_env_list_raises(self) -> None:
        from schema_evolution.apply import plan_targets_environment

        with pytest.raises(ValueError, match="must not be empty"):
            plan_targets_environment({"table": "t", "environments": []}, "dev", "gold")

    def test_wrong_env_type_raises(self) -> None:
        from schema_evolution.apply import plan_targets_environment

        with pytest.raises(ValueError, match="must be a string or a list"):
            plan_targets_environment({"table": "t", "environments": 3}, "dev", "gold")

    def test_non_string_env_entry_raises(self) -> None:
        from schema_evolution.apply import plan_targets_environment

        with pytest.raises(ValueError, match="entries must be strings"):
            plan_targets_environment({"table": "t", "environments": ["dev", 7]}, "dev", "gold")

    def test_layer_mismatch_raises(self) -> None:
        from schema_evolution.apply import plan_targets_environment

        with pytest.raises(ValueError, match="does not match"):
            plan_targets_environment({"table": "t", "layer": "silver"}, "dev", "gold")

    def test_matching_layer_is_allowed(self) -> None:
        from schema_evolution.apply import plan_targets_environment

        assert plan_targets_environment({"table": "t", "layer": "Gold"}, "dev", "gold") is True

    def test_legacy_database_key_still_pins_environment(self) -> None:
        """Old-format plans (still in S3 until each env redeploys) keep their pinning."""
        from schema_evolution.apply import plan_targets_environment

        meta = {"table": "t", "database": "dp_dev_bronze"}
        assert plan_targets_environment(meta, "dev", "bronze") is True
        assert plan_targets_environment(meta, "qa", "bronze") is False
        assert plan_targets_environment(meta, "prod", "bronze") is False

    def test_new_env_key_overrides_legacy_database(self) -> None:
        from schema_evolution.apply import plan_targets_environment

        meta = {"table": "t", "database": "dp_dev_gold", "environments": ["prod"]}
        assert plan_targets_environment(meta, "prod", "gold") is True
        assert plan_targets_environment(meta, "dev", "gold") is False

    def test_legacy_database_layer_mismatch_raises(self) -> None:
        from schema_evolution.apply import plan_targets_environment

        with pytest.raises(ValueError, match="targets layer"):
            plan_targets_environment({"table": "t", "database": "dp_dev_silver"}, "dev", "gold")

    def test_legacy_database_malformed_raises(self) -> None:
        from schema_evolution.apply import plan_targets_environment

        with pytest.raises(ValueError, match="is not of the form"):
            plan_targets_environment({"table": "t", "database": "nonsense"}, "dev", "gold")


@patch("schema_evolution.apply.load_catalog")
@patch("schema_evolution.apply.ensure_tracker")
@patch("schema_evolution.apply.wait_for_table", return_value=True)
@patch("schema_evolution.apply.batch_upsert_tracker")
@patch("schema_evolution.apply.read_plan_metadata_local", side_effect=RuntimeError("bad metadata"))
def test_apply_migrations_fails_if_plan_metadata_unreadable(
    mock_read_meta,
    mock_upsert,
    mock_wait,
    mock_ensure,
    mock_catalog,
):
    """Unreadable metadata must fail fast to prevent wrong-env writes."""
    from schema_evolution.apply import ApplyResult

    mock_catalog.return_value = MagicMock()

    with tempfile.TemporaryDirectory() as tmpdir:
        _make_migrations_dir(
            tmpdir,
            "gold",
            "orders",
            "20260101T000000_p.json",
            [{"change_type": "x"}],
            environments=["prod"],
        )

        args = _make_args(tmpdir, environment="prod")
        result = apply_migrations(args, return_details=True)

    assert isinstance(result, ApplyResult)
    assert result.success is False
    assert result.pending_work is False
    mock_read_meta.assert_called_once()
    mock_upsert.assert_not_called()


@patch("schema_evolution.apply.load_catalog")
@patch("schema_evolution.apply.ensure_tracker")
@patch("schema_evolution.apply.wait_for_table", return_value=True)
@patch("schema_evolution.apply.batch_upsert_tracker")
def test_apply_migrations_flags_pending_when_backfilling(mock_upsert, mock_wait, mock_ensure, mock_catalog):
    """An in-flight (backfilling) plan makes the pass report pending_work, with
    no new actionable plans."""
    from schema_evolution.apply import ApplyResult

    mock_catalog.return_value = MagicMock()

    with (
        patch(
            "schema_evolution.apply.read_tracker",
            return_value={"20260101T000000_p.json": {"status": "backfilling", "notes": "Glue job run: jr_x"}},
        ),
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        _make_migrations_dir(
            tmpdir,
            "gold",
            "orders",
            "20260101T000000_p.json",
            [{"change_type": "x"}],
            environments=["prod"],
        )
        args = _make_args(tmpdir, environment="prod")
        result = apply_migrations(args, return_details=True)

    assert isinstance(result, ApplyResult)
    assert result.pending_work is True


class TestTimestampTypeMapping:
    """``timestamp`` must add a tz-*aware* column.

    Everywhere else in the platform "timestamp" means tz-aware: Gold schema
    contracts map it to Spark TimestampType, and Spark writes that to Iceberg as
    timestamptz. When this map used pyiceberg's tz-naive TimestampType instead, a
    migration-added column silently disagreed with every Spark-written column
    beside it - and Athena then coerced across the two using the session time
    zone, shifting joins and filters by the offset.
    """

    @staticmethod
    def _added_type(target_type: str):
        table, update_ctx = _mock_table_with_fields([("id", False)])
        cat = MagicMock()
        cat.load_table.return_value = table

        change = {"change_type": "add_column", "column_name": "event_at", "target_type": target_type}
        success, error = apply_ddl(cat, "dp_dev_gold", "some_table", change)
        assert success is True, error
        return update_ctx.add_column.call_args[0][1]

    def test_timestamp_is_timezone_aware(self):
        from pyiceberg.types import TimestamptzType

        assert isinstance(self._added_type("timestamp"), TimestamptzType)

    def test_timestamptz_alias(self):
        from pyiceberg.types import TimestamptzType

        assert isinstance(self._added_type("timestamptz"), TimestamptzType)

    def test_timestamp_tz_alias(self):
        from pyiceberg.types import TimestamptzType

        assert isinstance(self._added_type("timestamp_tz"), TimestamptzType)

    def test_timestamp_ntz_opts_into_naive(self):
        """A genuinely zone-less column still has to be expressible."""
        from pyiceberg.types import TimestampType, TimestamptzType

        added = self._added_type("timestamp_ntz")
        assert isinstance(added, TimestampType)
        assert not isinstance(added, TimestamptzType)


class TestContractTypeCoverage:
    """Every type a Gold schema contract can declare must be addable here.

    ``date`` is mapped by schema_utils to Spark DateType and is used by the HR
    workforce contracts, but was missing from the add_column type map - so a
    plan targeting a date column failed with "Unrecognized type: date". The gap
    survived because date had only ever appeared as a ``current_type`` on the
    source side of a type change, never as an ``add_column`` target.
    """

    @staticmethod
    def _add(target_type: str):
        table, update_ctx = _mock_table_with_fields([("id", False)])
        cat = MagicMock()
        cat.load_table.return_value = table
        change = {"change_type": "add_column", "column_name": "c", "target_type": target_type}
        return apply_ddl(cat, "dp_dev_gold", "t", change), update_ctx

    def test_date_maps_to_date_type(self):
        from pyiceberg.types import DateType

        (success, error), ctx = self._add("date")
        assert success is True, error
        assert isinstance(ctx.add_column.call_args[0][1], DateType)

    @pytest.mark.parametrize(
        "contract_type",
        ["string", "int", "long", "double", "float", "boolean", "date", "timestamp"],
    )
    def test_gold_contract_types_are_all_addable(self, contract_type):
        (success, error), _ = self._add(contract_type)
        assert success is True, f"{contract_type}: {error}"

    def test_unknown_type_still_rejected(self):
        (success, error), _ = self._add("nonsense")
        assert success is False
        assert "Unrecognized type" in (error or "")


def test_drop_then_add_repairs_column_type_in_order():
    """The response_date_day repair: drop the naive column, re-add it tz-aware.

    Used instead of incompatible_type_change because the column holds no data -
    Iceberg cannot promote timestamp to timestamptz, so the alternative routes to
    the backfill job and rewrites 15.8M rows to migrate nothing.

    Guards the two things that make it work: changes apply in file order, and
    apply_ddl re-loads the table per change so the add sees the post-drop schema
    (otherwise it would hit the "already exists" guard and skip, leaving the
    column naive).
    """
    from pyiceberg.types import TimestamptzType

    present = [("response_date_day", False)]
    absent: list[tuple[str, bool]] = []
    state = {"fields": present}

    dropped = MagicMock()
    added = MagicMock()

    def load_table(_fqn):
        table, update_ctx = _mock_table_with_fields(state["fields"])
        update_ctx.delete_column = dropped
        update_ctx.add_column = added
        return table

    cat = MagicMock()
    cat.load_table.side_effect = load_table

    ok, err = apply_ddl(
        cat,
        "dp_dev_gold",
        "order_approvals",
        {"change_type": "drop_column", "column_name": "response_date_day"},
    )
    assert (ok, err) == (True, None)
    dropped.assert_called_once()

    # the drop happened, so the next change sees a table without the column
    state["fields"] = absent

    ok, err = apply_ddl(
        cat,
        "dp_dev_gold",
        "order_approvals",
        {"change_type": "add_column", "column_name": "response_date_day", "target_type": "timestamptz"},
    )
    assert (ok, err) == (True, None)
    added.assert_called_once()
    assert isinstance(added.call_args[0][1], TimestamptzType)


def test_add_column_skips_when_already_present():
    """Why order matters: re-adding without dropping first is a silent no-op."""
    table, update_ctx = _mock_table_with_fields([("response_date_day", False)])
    cat = MagicMock()
    cat.load_table.return_value = table

    ok, err = apply_ddl(
        cat,
        "dp_dev_gold",
        "order_approvals",
        {"change_type": "add_column", "column_name": "response_date_day", "target_type": "timestamptz"},
    )
    assert (ok, err) == (True, None)
    update_ctx.add_column.assert_not_called()


@patch("schema_evolution.apply.load_catalog")
@patch("schema_evolution.apply.ensure_tracker")
@patch("schema_evolution.apply.batch_upsert_tracker")
@patch("schema_evolution.apply.apply_ddl", return_value=(True, None))
def test_apply_migrations_heals_dead_backfill_before_lock_wait(mock_ddl, mock_upsert, mock_ensure, mock_catalog):
    """A plan stuck at 'backfilling' behind a dead Glue run must not lock the table.

    Regression: QA census plan stayed 'backfilling' after its Glue run FAILED, so
    every deploy waited out the lock and failed with 'Table locked - skipping'.
    """
    mock_catalog.return_value = MagicMock()
    stuck = {"20260101T000000_p.json": {"status": "backfilling", "notes": "Glue job run: jr_dead"}}
    healed = {"20260101T000000_p.json": {"status": "failed", "notes": "Auto-reset"}}

    with (
        patch("schema_evolution.apply.read_tracker", side_effect=[stuck, healed]),
        patch("schema_evolution.apply._heal_dead_backfills", return_value=True) as mock_heal,
        patch("schema_evolution.apply.wait_for_table", return_value=True) as mock_wait,
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        _make_migrations_dir(
            tmpdir,
            "gold",
            "census",
            "20260101T000000_p.json",
            [{"change_type": "set_column_nullability", "column_name": "first_name", "target_not_null": False}],
            environments=["qa"],
        )
        args = _make_args(tmpdir, environment="qa")
        result = apply_migrations(args)

    assert result is True
    mock_heal.assert_called_once()
    # After healing nothing is in flight, so there is no lock to wait on.
    mock_wait.assert_not_called()
    row = mock_upsert.call_args[0][2][0]
    assert row["plan_name"] == "20260101T000000_p.json"
    assert row["status"] == "applied"


class TestWriteTrackerRowsRetry:
    """Tracker commits must retry on S3 Tables optimistic-concurrency conflicts."""

    @staticmethod
    def _rows() -> list[dict]:
        return [{"table_name": "t1", "plan_name": "p1.json", "status": "applied"}]

    @patch("schema_evolution.apply.time.sleep")
    def test_retries_on_commit_failed_then_succeeds(self, mock_sleep: MagicMock) -> None:
        from pyiceberg.exceptions import CommitFailedException

        from schema_evolution.apply import write_tracker_rows

        tbl = MagicMock()
        # Real server message: does not contain the class name.
        tbl.overwrite.side_effect = [
            CommitFailedException("resource_conflict: The resource was modified by a concurrent request."),
            None,
        ]
        catalog = MagicMock()
        catalog.load_table.return_value = tbl

        write_tracker_rows(catalog, self._rows(), max_retries=3)

        assert tbl.overwrite.call_count == 2
        assert catalog.load_table.call_count == 2  # reloads fresh metadata
        mock_sleep.assert_called_once()

    @patch("schema_evolution.apply.time.sleep")
    def test_raises_after_exhausting_retries(self, mock_sleep: MagicMock) -> None:
        from pyiceberg.exceptions import CommitFailedException

        from schema_evolution.apply import write_tracker_rows

        tbl = MagicMock()
        tbl.overwrite.side_effect = CommitFailedException("resource_conflict")
        catalog = MagicMock()
        catalog.load_table.return_value = tbl

        with pytest.raises(CommitFailedException):
            write_tracker_rows(catalog, self._rows(), max_retries=2)
        assert tbl.overwrite.call_count == 3

    def test_does_not_retry_other_errors(self) -> None:
        from schema_evolution.apply import write_tracker_rows

        tbl = MagicMock()
        tbl.overwrite.side_effect = ValueError("boom")
        catalog = MagicMock()
        catalog.load_table.return_value = tbl

        with pytest.raises(ValueError):
            write_tracker_rows(catalog, self._rows())
        assert tbl.overwrite.call_count == 1


# ---------------------------------------------------------------------------
# A backfill still running is not a failure
# ---------------------------------------------------------------------------


@patch("schema_evolution.apply.load_catalog")
@patch("schema_evolution.apply.ensure_tracker")
@patch("schema_evolution.apply.batch_upsert_tracker")
@patch("schema_evolution.apply.apply_ddl", return_value=(True, None))
@patch("schema_evolution.apply.wait_for_table", return_value=False)
def test_running_backfill_skips_table_without_failing(mock_wait, mock_ddl, mock_upsert, _ensure, mock_catalog, caplog):
    """A table still backfilling after the wait is skipped: the run succeeds,
    reports pending work, applies nothing to that table and names it in the log."""
    mock_catalog.return_value = MagicMock()
    tracker = {"20260101T000000_inflight.json": {"status": "backfilling", "notes": "Glue job run: jr_abc"}}
    with (
        patch("schema_evolution.apply.read_tracker", return_value=tracker),
        patch("schema_evolution.apply._heal_dead_backfills", return_value=False),
        tempfile.TemporaryDirectory() as tmpdir,
        caplog.at_level("WARNING", logger="schema_evolution.apply"),
    ):
        _make_migrations_dir(tmpdir, "gold", "orders", "20260202T000000_new.json", [{"change_type": "x"}])
        result = apply_migrations(_make_args(tmpdir, environment="prod"), return_details=True)

    mock_wait.assert_called_once()
    mock_ddl.assert_not_called()
    mock_upsert.assert_not_called()  # the queued plan is not marked failed
    assert result.success is True
    assert result.pending_work is True
    assert "jr_abc" in caplog.text
    assert "gold/orders" in caplog.text


@patch("schema_evolution.apply.load_catalog")
@patch("schema_evolution.apply.ensure_tracker")
@patch("schema_evolution.apply.batch_upsert_tracker")
@patch("schema_evolution.apply.apply_ddl", return_value=(True, None))
@patch("schema_evolution.apply.wait_for_table", return_value=True)
def test_backfill_finished_during_wait_applies_next_plan(mock_wait, mock_ddl, mock_upsert, _ensure, mock_catalog):
    """If the backfill finishes within the wait, the table's next plan applies in the same run."""
    mock_catalog.return_value = MagicMock()
    tracker = {"20260101T000000_inflight.json": {"status": "backfilling", "notes": "Glue job run: jr_abc"}}
    with (
        patch("schema_evolution.apply.read_tracker", return_value=tracker),
        patch("schema_evolution.apply._heal_dead_backfills", return_value=False),
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        _make_migrations_dir(
            tmpdir,
            "gold",
            "orders",
            "20260202T000000_new.json",
            [{"change_type": "add_column", "column_name": "c", "target_type": "string"}],
        )
        result = apply_migrations(_make_args(tmpdir, environment="prod"), return_details=True)

    assert mock_ddl.call_count == 1
    assert result.success is True

