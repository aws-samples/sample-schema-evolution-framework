"""Tests for the tracker diff, checksums, parallelism and related fixes in apply.py."""

import argparse
import json
import os
import tempfile
import threading
import time
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from schema_evolution.apply import (
    ApplyResult,
    _build_args_from_argv,
    apply_ddl,
    apply_migrations,
    batch_upsert_tracker,
    ensure_tracker,
    plan_checksum,
    read_tracker,
    read_tracker_snapshot,
    write_tracker_rows,
)

ADD_X = [{"change_type": "add_column", "column_name": "x", "target_type": "string"}]
ADD_Y = [{"change_type": "add_column", "column_name": "y", "target_type": "string"}]


def _write_plan(root: str, layer: str, table: str, plan: str, changes: list, environments=None) -> None:
    table_dir = os.path.join(root, layer, table)
    os.makedirs(table_dir, exist_ok=True)
    metadata: dict = {"layer": layer, "table": table}
    if environments is not None:
        metadata["environments"] = environments
    with open(os.path.join(table_dir, plan), "w") as f:
        json.dump({"metadata": metadata, "changes": changes}, f)


def _args(root: str, **overrides) -> argparse.Namespace:
    values = {
        "migrations_dir": root,
        "environment": "dev",
        "region": "us-west-2",
        "account_id": "111111111111",
        "bucket_prefix": "dp",
        "glue_job_name": "dp-dev-glue-schema-evolution-backfill",
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def _applied(changes: list | None = None, checksum: str | None = "unset") -> dict:
    return {
        "status": "applied",
        "notes": "",
        "applied_at": "2026-01-01T00:00:00+00:00",
        "checksum": plan_checksum(changes or []) if checksum == "unset" else checksum,
    }


# ---------------------------------------------------------------------------
# Tracker reads
# ---------------------------------------------------------------------------


def _tracker_catalog(rows: list[dict]) -> MagicMock:
    columns = ["table_name", "plan_name", "status", "notes", "applied_at", "checksum"]
    arrow = pa.table({c: [r.get(c) for r in rows] for c in columns})
    tbl = MagicMock()
    tbl.scan.return_value.to_arrow.return_value = arrow
    catalog = MagicMock()
    catalog.load_table.return_value = tbl
    return catalog


def test_read_tracker_snapshot_groups_rows_by_table():
    catalog = _tracker_catalog(
        [
            {"table_name": "a", "plan_name": "p1.json", "status": "applied", "checksum": "abc"},
            {"table_name": "a", "plan_name": "p2.json", "status": "failed", "notes": "boom"},
            {"table_name": "b", "plan_name": "p1.json", "status": "backfilling"},
        ]
    )

    snapshot = read_tracker_snapshot(catalog)

    assert set(snapshot) == {"a", "b"}
    assert snapshot["a"]["p1.json"]["checksum"] == "abc"
    assert snapshot["a"]["p2.json"]["notes"] == "boom"
    assert snapshot["b"]["p1.json"]["status"] == "backfilling"
    # One unfiltered scan for the whole layer.
    catalog.load_table.return_value.scan.assert_called_once_with()


def test_read_tracker_uses_snapshot_without_touching_catalog():
    catalog = MagicMock()
    snapshot = {"a": {"p1.json": {"status": "applied"}}}

    assert read_tracker(catalog, "a", snapshot=snapshot) == {"p1.json": {"status": "applied"}}
    assert read_tracker(catalog, "missing", snapshot=snapshot) == {}
    catalog.load_table.assert_not_called()


def test_batch_upsert_preserves_applied_at_and_checksum():
    catalog = MagicMock()
    batch_upsert_tracker(
        catalog,
        "a",
        [{"plan_name": "p1.json", "status": "applied", "applied_at": "2026-01-01", "checksum": "abc"}],
    )
    written = catalog.load_table.return_value.overwrite.call_args.args[0].to_pylist()[0]
    assert written["applied_at"] == "2026-01-01"
    assert written["checksum"] == "abc"


def test_ensure_tracker_adds_checksum_column_to_old_tracker():
    field = MagicMock()
    field.name = "status"
    tbl = MagicMock()
    tbl.schema.return_value.fields = [field]
    catalog = MagicMock()
    catalog.load_table.return_value = tbl

    ensure_tracker(catalog)

    update = tbl.update_schema.return_value.__enter__.return_value
    assert update.add_column.call_args.args[0] == "checksum"


def test_plan_checksum_ignores_key_order_and_formatting():
    a = [{"change_type": "add_column", "column_name": "x", "target_type": "string"}]
    b = [{"target_type": "string", "column_name": "x", "change_type": "add_column"}]
    assert plan_checksum(a) == plan_checksum(b)
    assert plan_checksum(a) != plan_checksum(ADD_Y)


# ---------------------------------------------------------------------------
# Diff before touching tables
# ---------------------------------------------------------------------------


@patch("schema_evolution.apply.boto3")
@patch("schema_evolution.apply.ensure_tracker")
@patch("schema_evolution.apply.load_catalog")
def test_fully_applied_tables_are_not_loaded(mock_load_catalog, _ensure, _boto3):
    catalog = MagicMock()
    mock_load_catalog.return_value = catalog
    snapshot = {
        "t1": {"p1.json": _applied(ADD_X)},
        "t2": {"p1.json": _applied(ADD_X)},
    }

    with (
        tempfile.TemporaryDirectory() as root,
        patch("schema_evolution.apply.read_tracker_snapshot", return_value=snapshot) as mock_snapshot,
        patch("schema_evolution.apply.read_plan_metadata_local") as mock_meta,
        patch("schema_evolution.apply.batch_upsert_tracker") as mock_upsert,
    ):
        _write_plan(root, "gold", "t1", "p1.json", ADD_X)
        _write_plan(root, "gold", "t2", "p1.json", ADD_X)
        result = apply_migrations(_args(root), return_details=True)

    assert result == ApplyResult(success=True, pending_work=False)
    mock_snapshot.assert_called_once()
    mock_meta.assert_not_called()
    catalog.load_table.assert_not_called()
    mock_upsert.assert_not_called()


@patch("schema_evolution.apply.boto3")
@patch("schema_evolution.apply.ensure_tracker")
@patch("schema_evolution.apply.load_catalog")
def test_only_unapplied_plan_runs_and_records_checksum(mock_load_catalog, _ensure, _boto3):
    mock_load_catalog.return_value = MagicMock()
    snapshot = {"t1": {"p1.json": _applied(ADD_X)}}

    with (
        tempfile.TemporaryDirectory() as root,
        patch("schema_evolution.apply.read_tracker_snapshot", return_value=snapshot),
        patch("schema_evolution.apply.apply_ddl", return_value=(True, None)) as mock_ddl,
        patch("schema_evolution.apply.batch_upsert_tracker") as mock_upsert,
    ):
        _write_plan(root, "gold", "t1", "p1.json", ADD_X)
        _write_plan(root, "gold", "t1", "p2.json", ADD_Y)
        assert apply_migrations(_args(root)) is True

    assert mock_ddl.call_count == 1
    assert mock_ddl.call_args.args[3]["column_name"] == "y"
    row = mock_upsert.call_args.args[2][0]
    assert row["plan_name"] == "p2.json"
    assert row["status"] == "applied"
    assert row["checksum"] == plan_checksum(ADD_Y)


# ---------------------------------------------------------------------------
# Checksums
# ---------------------------------------------------------------------------


def _run_with_snapshot(root: str, snapshot: dict, **arg_overrides):
    with (
        patch("schema_evolution.apply.boto3"),
        patch("schema_evolution.apply.ensure_tracker"),
        patch("schema_evolution.apply.load_catalog", return_value=MagicMock()),
        patch("schema_evolution.apply.read_tracker_snapshot", return_value=snapshot),
        patch("schema_evolution.apply.apply_ddl", return_value=(True, None)) as mock_ddl,
        patch("schema_evolution.apply.batch_upsert_tracker") as mock_upsert,
        patch("schema_evolution.apply.write_tracker_rows") as mock_write,
    ):
        result = apply_migrations(_args(root, **arg_overrides))
    _run_with_snapshot.last_write = mock_write  # type: ignore[attr-defined]
    return result, mock_ddl, mock_upsert


def test_checksum_baseline_recorded_for_rows_without_one():
    with tempfile.TemporaryDirectory() as root:
        _write_plan(root, "gold", "t1", "p1.json", ADD_X)
        snapshot = {"t1": {"p1.json": _applied(checksum=None)}}
        result, mock_ddl, mock_upsert = _run_with_snapshot(root, snapshot, checksum_mode="fail")

    assert result is True
    mock_ddl.assert_not_called()
    mock_upsert.assert_not_called()
    write = _run_with_snapshot.last_write  # type: ignore[attr-defined]
    write.assert_called_once()
    row = write.call_args.args[1][0]
    assert row == {
        "table_name": "t1",
        "plan_name": "p1.json",
        "status": "applied",
        "notes": "",
        "applied_at": "2026-01-01T00:00:00+00:00",
        "checksum": plan_checksum(ADD_X),
    }


def test_checksum_drift_fails_before_any_change():
    with tempfile.TemporaryDirectory() as root:
        _write_plan(root, "gold", "t1", "p1.json", ADD_Y)  # edited after apply
        _write_plan(root, "gold", "t1", "p2.json", ADD_X)  # pending
        snapshot = {"t1": {"p1.json": _applied(ADD_X)}}
        result, mock_ddl, mock_upsert = _run_with_snapshot(root, snapshot, checksum_mode="fail")

    assert result is False
    mock_ddl.assert_not_called()
    mock_upsert.assert_not_called()


def test_checksum_drift_warn_mode_continues():
    with tempfile.TemporaryDirectory() as root:
        _write_plan(root, "gold", "t1", "p1.json", ADD_Y)
        _write_plan(root, "gold", "t1", "p2.json", ADD_X)
        snapshot = {"t1": {"p1.json": _applied(ADD_X)}}
        result, mock_ddl, _upsert = _run_with_snapshot(root, snapshot, checksum_mode="warn")

    assert result is True
    assert mock_ddl.call_count == 1


def test_checksum_skipped_for_s3_plan_source():
    snapshot = {"t1": {"p1.json": _applied(checksum="stale")}}
    with (
        patch("schema_evolution.apply.boto3"),
        patch("schema_evolution.apply.ensure_tracker"),
        patch("schema_evolution.apply.load_catalog", return_value=MagicMock()),
        patch("schema_evolution.apply.read_tracker_snapshot", return_value=snapshot),
        patch("schema_evolution.apply.list_plan_files_s3", return_value={("gold", "t1"): ["p1.json"]}),
        patch("schema_evolution.apply.read_plan_json_s3") as mock_read,
        patch("schema_evolution.apply.batch_upsert_tracker") as mock_upsert,
    ):
        result = apply_migrations(_args("s3://amzn-s3-demo-bucket/migrations", checksum_mode="fail"))

    assert result is True
    mock_read.assert_not_called()
    mock_upsert.assert_not_called()


def test_invalid_checksum_mode_rejected():
    with tempfile.TemporaryDirectory() as root:
        _write_plan(root, "gold", "t1", "p1.json", ADD_X)
        with pytest.raises(ValueError, match="checksum_mode"):
            _run_with_snapshot(root, {}, checksum_mode="nope")


# ---------------------------------------------------------------------------
# Bug fixes
# ---------------------------------------------------------------------------


def test_same_table_name_in_two_layers_reads_each_layers_plan():
    """A plan is read from its own layer, never another layer's same-named file."""
    with tempfile.TemporaryDirectory() as root:
        _write_plan(root, "gold", "projects", "p1.json", ADD_X)
        _write_plan(root, "silver", "projects", "p1.json", ADD_Y)
        _result, mock_ddl, _upsert = _run_with_snapshot(root, {})

    applied = {(call.args[1], call.args[3]["column_name"]) for call in mock_ddl.call_args_list}
    assert applied == {("dp_dev_gold", "x"), ("dp_dev_silver", "y")}


def test_plan_read_failure_stops_later_plans_for_table():
    with tempfile.TemporaryDirectory() as root:
        _write_plan(root, "gold", "t1", "p1.json", ADD_X)
        _write_plan(root, "gold", "t1", "p2.json", ADD_Y)
        with patch(
            "schema_evolution.apply.read_plan_json_local",
            side_effect=[RuntimeError("bad json"), ADD_Y],
        ):
            result, mock_ddl, mock_upsert = _run_with_snapshot(root, {})

    assert result is False
    mock_ddl.assert_not_called()
    rows = mock_upsert.call_args.args[2]
    assert [r["plan_name"] for r in rows] == ["p1.json"]
    assert rows[0]["status"] == "failed"


def test_rename_with_neither_column_present_is_recorded_as_skipped():
    field = MagicMock()
    field.name = "other"
    table = MagicMock()
    table.schema.return_value.fields = [field]
    catalog = MagicMock()
    catalog.load_table.return_value = table

    ok, note = apply_ddl(
        catalog,
        "dp_dev_gold",
        "t1",
        {"change_type": "rename_column", "column_name": "new", "previous_name": "old"},
    )

    assert ok is True
    assert note is not None and note.startswith("SKIPPED: ")
    table.update_schema.assert_not_called()


def test_skip_note_is_written_to_tracker():
    with tempfile.TemporaryDirectory() as root:
        _write_plan(root, "gold", "t1", "p1.json", ADD_X)
        with patch("schema_evolution.apply.apply_ddl", return_value=(True, "SKIPPED: nothing renamed")):
            with (
                patch("schema_evolution.apply.boto3"),
                patch("schema_evolution.apply.ensure_tracker"),
                patch("schema_evolution.apply.load_catalog", return_value=MagicMock()),
                patch("schema_evolution.apply.read_tracker_snapshot", return_value={}),
                patch("schema_evolution.apply.batch_upsert_tracker") as mock_upsert,
            ):
                assert apply_migrations(_args(root)) is True

    row = mock_upsert.call_args.args[2][0]
    assert row["status"] == "applied"
    assert "SKIPPED: nothing renamed" in row["notes"]


def test_waiting_for_table_is_not_rewritten_when_unchanged():
    catalog = MagicMock()
    catalog.load_table.side_effect = RuntimeError("not found")
    reason = "Table 'dp_dev_gold.t1' does not exist yet"
    snapshot = {"t1": {"p1.json": {"status": "waiting_for_table", "notes": reason}}}

    with (
        tempfile.TemporaryDirectory() as root,
        patch("schema_evolution.apply.boto3"),
        patch("schema_evolution.apply.ensure_tracker"),
        patch("schema_evolution.apply.load_catalog", return_value=catalog),
        patch("schema_evolution.apply.read_tracker_snapshot", return_value=snapshot),
        patch("schema_evolution.apply.batch_upsert_tracker") as mock_upsert,
    ):
        _write_plan(root, "gold", "t1", "p1.json", ADD_X)
        assert apply_migrations(_args(root)) is True

    mock_upsert.assert_not_called()


def test_waiting_plan_runs_once_table_exists():
    snapshot = {"t1": {"p1.json": {"status": "waiting_for_table", "notes": "missing"}}}
    with tempfile.TemporaryDirectory() as root:
        _write_plan(root, "gold", "t1", "p1.json", ADD_X)
        result, mock_ddl, mock_upsert = _run_with_snapshot(root, snapshot)

    assert result is True
    assert mock_ddl.call_count == 1
    assert mock_upsert.call_args.args[2][0]["status"] == "applied"


# ---------------------------------------------------------------------------
# Parallelism
# ---------------------------------------------------------------------------


def test_parallel_tables_apply_all_and_serialize_tracker_writes():
    active = 0
    max_active = 0
    guard = threading.Lock()
    written: list[str] = []

    def fake_upsert(_catalog, table_name, _rows):
        nonlocal active, max_active
        with guard:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.01)
        with guard:
            active -= 1
            written.append(table_name)

    with tempfile.TemporaryDirectory() as root:
        for i in range(8):
            _write_plan(root, "gold", f"t{i}", "p1.json", ADD_X)
        with (
            patch("schema_evolution.apply.boto3"),
            patch("schema_evolution.apply.ensure_tracker"),
            patch("schema_evolution.apply.load_catalog", return_value=MagicMock()),
            patch("schema_evolution.apply.read_tracker_snapshot", return_value={}),
            patch("schema_evolution.apply.apply_ddl", return_value=(True, None)),
            patch("schema_evolution.apply.batch_upsert_tracker", side_effect=fake_upsert),
        ):
            result = apply_migrations(_args(root, max_workers=4), return_details=True)

    assert result == ApplyResult(success=True, pending_work=True)
    assert sorted(written) == sorted(f"t{i}" for i in range(8))
    assert max_active == 1


def test_parallel_failure_in_one_table_does_not_hide_others():
    def ddl(_catalog, _ns, table_name, _change):
        return (False, "boom") if table_name == "t0" else (True, None)

    with tempfile.TemporaryDirectory() as root:
        for i in range(3):
            _write_plan(root, "gold", f"t{i}", "p1.json", ADD_X)
        with patch("schema_evolution.apply.apply_ddl", side_effect=ddl):
            with (
                patch("schema_evolution.apply.boto3"),
                patch("schema_evolution.apply.ensure_tracker"),
                patch("schema_evolution.apply.load_catalog", return_value=MagicMock()),
                patch("schema_evolution.apply.read_tracker_snapshot", return_value={}),
                patch("schema_evolution.apply.batch_upsert_tracker") as mock_upsert,
            ):
                result = apply_migrations(_args(root, max_workers=3))

    assert result is False
    statuses = {c.args[1]: c.args[2][0]["status"] for c in mock_upsert.call_args_list}
    assert statuses == {"t0": "failed", "t1": "applied", "t2": "applied"}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def test_cli_defaults_for_workers_and_checksum_mode(monkeypatch):
    monkeypatch.delenv("SCHEMA_EVOLUTION_CHECKSUM_MODE", raising=False)
    args = _build_args_from_argv(["--account_id", "1", "--environment", "dev"])
    assert args.max_workers == 4
    assert args.checksum_mode == "fail"


def test_cli_checksum_mode_from_env(monkeypatch):
    monkeypatch.setenv("SCHEMA_EVOLUTION_CHECKSUM_MODE", "warn")
    args = _build_args_from_argv(["--account_id", "1", "--environment", "dev"])
    assert args.checksum_mode == "warn"


# ---------------------------------------------------------------------------
# Real PyIceberg catalog (SQLite) - tracker writes after a schema change
# ---------------------------------------------------------------------------


def _sql_catalog(tmp_path):
    from pyiceberg.catalog.sql import SqlCatalog

    return SqlCatalog("test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}")


def _rows(catalog) -> list[dict]:
    tbl = catalog.load_table("schema_evolution_metadata.plan_tracker")
    return sorted(tbl.scan().to_arrow().to_pylist(), key=lambda r: (r["table_name"], r["plan_name"]))


def test_tracker_write_after_checksum_column_added(tmp_path):
    """Regression: the first write after adding 'checksum' failed in dev.

    pyiceberg's upsert read existing rows with the snapshot's (pre-checksum)
    schema and failed to cast. The tracker must accept the 7-column write
    straight after ensure_tracker evolves an old 6-column table.
    """
    import pyiceberg.schema as sch
    from pyiceberg.types import NestedField, StringType

    catalog = _sql_catalog(tmp_path)
    catalog.create_namespace("schema_evolution_metadata")
    names = ["table_name", "plan_name", "status", "created_at", "applied_at", "notes"]
    old = sch.Schema(*[NestedField(i + 1, n, StringType(), required=i < 3) for i, n in enumerate(names)])
    tbl = catalog.create_table("schema_evolution_metadata.plan_tracker", schema=old)
    tbl.append(
        pa.table(
            {
                "table_name": ["a", "a", "b"],
                "plan_name": ["p1.json", "p2.json", "p1.json"],
                "status": ["applied", "applied", "applied"],
                "created_at": ["c", "c", "c"],
                "applied_at": ["t1", "t2", "t3"],
                "notes": ["", "", ""],
            },
            schema=tbl.schema().as_arrow(),
        )
    )

    ensure_tracker(catalog)
    batch_upsert_tracker(
        catalog, "a", [{"plan_name": "p1.json", "status": "applied", "applied_at": "t1", "checksum": "abc"}]
    )

    rows = _rows(catalog)
    assert [(r["table_name"], r["plan_name"], r["checksum"]) for r in rows] == [
        ("a", "p1.json", "abc"),
        ("a", "p2.json", None),
        ("b", "p1.json", None),
    ]
    assert rows[0]["applied_at"] == "t1"


def test_tracker_write_replaces_rows_without_duplicates(tmp_path):
    catalog = _sql_catalog(tmp_path)
    ensure_tracker(catalog)

    batch_upsert_tracker(catalog, "a", [{"plan_name": "p1.json", "status": "backfilling", "notes": "run 1"}])
    batch_upsert_tracker(catalog, "a", [{"plan_name": "p1.json", "status": "applied", "checksum": "abc"}])
    batch_upsert_tracker(catalog, "b", [{"plan_name": "p1.json", "status": "failed"}])

    rows = _rows(catalog)
    assert [(r["table_name"], r["plan_name"], r["status"]) for r in rows] == [
        ("a", "p1.json", "applied"),
        ("b", "p1.json", "failed"),
    ]
    assert read_tracker_snapshot(catalog)["a"]["p1.json"]["checksum"] == "abc"


def test_checksum_baselines_are_one_write_per_layer():
    with tempfile.TemporaryDirectory() as root:
        for table in ("t1", "t2", "t3"):
            _write_plan(root, "gold", table, "p1.json", ADD_X)
        _write_plan(root, "silver", "s1", "p1.json", ADD_X)
        snapshot = {t: {"p1.json": _applied(checksum=None)} for t in ("t1", "t2", "t3", "s1")}
        result, _ddl, mock_upsert = _run_with_snapshot(root, snapshot, checksum_mode="fail")

    assert result is True
    mock_upsert.assert_not_called()
    write = _run_with_snapshot.last_write  # type: ignore[attr-defined]
    written = sorted(sorted(r["table_name"] for r in call.args[1]) for call in write.call_args_list)
    assert written == [["s1"], ["t1", "t2", "t3"]]


def test_write_tracker_rows_spans_tables_in_one_commit(tmp_path):
    catalog = _sql_catalog(tmp_path)
    ensure_tracker(catalog)
    batch_upsert_tracker(catalog, "a", [{"plan_name": "p1.json", "status": "failed"}])
    tbl = catalog.load_table("schema_evolution_metadata.plan_tracker")
    snapshots_before = len(tbl.metadata.snapshots)

    write_tracker_rows(
        catalog,
        [
            {"table_name": "a", "plan_name": "p1.json", "status": "applied", "checksum": "x"},
            {"table_name": "b", "plan_name": "p1.json", "status": "applied", "checksum": "y"},
        ],
    )

    rows = _rows(catalog)
    assert [(r["table_name"], r["status"], r["checksum"]) for r in rows] == [
        ("a", "applied", "x"),
        ("b", "applied", "y"),
    ]
    tbl = catalog.load_table("schema_evolution_metadata.plan_tracker")
    # overwrite = delete + append, at most two snapshots, regardless of table count
    assert len(tbl.metadata.snapshots) - snapshots_before <= 2
