"""Tests for schema_evolution.validate (offline PR/MR checks)."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from schema_evolution.validate import is_valid_type, main, validate_migrations

EXAMPLES = Path(__file__).resolve().parents[2] / "examples" / "migrations"


def _plan(root: Path, layer: str, table: str, name: str, changes: list, meta: dict | None = None, **extra) -> Path:
    """Write a plan under root/layer/table. *meta* overrides metadata keys (even table/layer)."""
    d = root / layer / table
    d.mkdir(parents=True, exist_ok=True)
    p = d / name
    metadata = {"layer": layer, "table": table, **extra, **(meta or {})}
    p.write_text(json.dumps({"metadata": metadata, "changes": changes}))
    return p


ADD = {"change_type": "add_column", "column_name": "c", "target_type": "string"}


def test_bundled_examples_are_valid() -> None:
    report = validate_migrations(str(EXAMPLES))
    assert report.ok, report.errors
    assert report.plans_checked >= 5


def test_valid_plan_passes(tmp_path: Path) -> None:
    _plan(tmp_path, "silver", "t", "20260101T000000_add.json", [ADD])
    report = validate_migrations(str(tmp_path))
    assert report.ok and not report.warnings


@pytest.mark.parametrize(
    ("changes", "fragment"),
    [
        ([], "non-empty list"),
        ([{"change_type": "nope", "column_name": "c"}], "unknown change_type"),
        ([{"change_type": "add_column", "column_name": "c"}], "missing 'target_type'"),
        ([{"change_type": "add_column", "column_name": "c", "target_type": "varchar"}], "unrecognized target_type"),
        ([{"change_type": "rename_column", "column_name": "c"}], "missing 'previous_name'"),
        ([ADD, {"change_type": "drop_column", "column_name": "C"}], "more than once"),
        ([{"change_type": "set_sort_order", "column_name": "x", "sort_fields": [{"column": "a", "direction": "asc"}]}],
         "must be '__sort_order__'"),
        ([{"change_type": "add_partition_field", "column_name": "c", "transform": "bucket"}], "integer parameter"),
        ([{"change_type": "repartition", "column_name": "c", "new_partition": {"transform": "weekly"}}],
         "unknown partition transform"),
        ([{"change_type": "set_column_nullability", "column_name": "c", "target_not_null": "yes"}], "true or false"),
        ([{"change_type": "rename_table", "column_name": "__table__", "previous_name": "old"}, ADD],
         "only change in its plan"),
        ([{"change_type": "rebuild_table", "column_name": "__table__", "target_format_version": "4"}],
         "target_format_version"),
    ],
)  # fmt: skip
def test_invalid_changes_are_reported(tmp_path: Path, changes: list, fragment: str) -> None:
    _plan(tmp_path, "silver", "t", "20260101T000000_x.json", changes)
    report = validate_migrations(str(tmp_path))
    assert not report.ok
    assert any(fragment in e for e in report.errors), report.errors


def test_layout_and_metadata_errors(tmp_path: Path) -> None:
    _plan(tmp_path, "platinum", "t", "20260101T000000_a.json", [ADD])
    _plan(tmp_path, "gold", "t", "20260101T000000_b.json", [ADD], meta={"table": "other"})
    _plan(tmp_path, "gold", "u", "20260101T000000_c.json", [ADD], meta={"layer": "silver"})
    _plan(tmp_path, "gold", "v", "20260101T000000_d.json", [ADD], environments=[])
    (tmp_path / "gold" / "w").mkdir(parents=True)
    (tmp_path / "gold" / "w" / "20260101T000000_e.json").write_text("{not json")
    (tmp_path / "loose.json").write_text("{}")
    errors = "\n".join(validate_migrations(str(tmp_path)).errors)
    assert "unknown layer 'platinum'" in errors
    assert "must equal its directory 't'" in errors
    assert "does not match" in errors
    assert "must not be empty" in errors
    assert "not valid JSON" in errors
    assert "<layer>/<table>/<name>.json" in errors


def test_name_without_timestamp_is_a_warning(tmp_path: Path) -> None:
    _plan(tmp_path, "gold", "t", "add_col.json", [ADD])
    report = validate_migrations(str(tmp_path))
    assert report.ok
    assert any("UTC timestamp" in w for w in report.warnings)


@pytest.mark.parametrize("t", ["string", "LONG", "decimal(18,2)", "decimal(20, 4)", "timestamp_ntz", "date"])
def test_valid_types(t: str) -> None:
    assert is_valid_type(t)


@pytest.mark.parametrize("t", ["varchar", "decimal", "map<string,int>", 5, None])
def test_invalid_types(t: object) -> None:
    assert not is_valid_type(t)


def test_main_exit_codes(tmp_path: Path) -> None:
    _plan(tmp_path, "gold", "t", "20260101T000000_a.json", [ADD])
    with pytest.raises(SystemExit) as ok:
        main(["--migrations_dir", str(tmp_path)])
    assert ok.value.code == 0
    _plan(tmp_path, "gold", "t", "20260101T000001_b.json", [{"change_type": "nope", "column_name": "c"}])
    with pytest.raises(SystemExit) as bad:
        main(["--migrations_dir", str(tmp_path)])
    assert bad.value.code == 1


# --- --base_ref: merged plans are immutable -----------------------------------

def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@example.com", *args],
        cwd=repo,
        check=True,
        capture_output=True,
    )


@pytest.fixture
def repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    if shutil.which("git") is None:
        pytest.skip("git not installed")
    _git(tmp_path, "init", "-q", "-b", "main")
    _plan(tmp_path / "migrations", "gold", "t", "20260101T000000_a.json", [ADD])
    _git(tmp_path, "add", ".")
    _git(tmp_path, "commit", "-q", "-m", "base")
    _git(tmp_path, "checkout", "-q", "-b", "feature")
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_new_plan_on_branch_passes(repo: Path) -> None:
    _plan(repo / "migrations", "gold", "t", "20260102T000000_b.json", [ADD])
    report = validate_migrations("migrations", base_ref="main")
    assert report.ok, report.errors


def test_metadata_only_edit_is_allowed(repo: Path) -> None:
    _plan(repo / "migrations", "gold", "t", "20260101T000000_a.json", [ADD], environments=["dev"])
    assert validate_migrations("migrations", base_ref="main").ok


def test_edited_merged_plan_fails(repo: Path) -> None:
    edited = dict(ADD, target_type="long")
    _plan(repo / "migrations", "gold", "t", "20260101T000000_a.json", [edited])
    errors = validate_migrations("migrations", base_ref="main").errors
    assert any("'changes' edited" in e for e in errors), errors


def test_deleted_merged_plan_fails(repo: Path) -> None:
    os.remove(repo / "migrations" / "gold" / "t" / "20260101T000000_a.json")
    errors = validate_migrations("migrations", base_ref="main").errors
    assert any("deleted" in e for e in errors), errors


def test_unknown_base_ref_is_an_error(repo: Path) -> None:
    errors = validate_migrations("migrations", base_ref="origin/does-not-exist").errors
    assert any("not a known commit" in e for e in errors), errors
