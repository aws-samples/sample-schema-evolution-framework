"""Tests for the env-gated backfill trigger CLI helpers.

``find_plans`` previously filtered on ``metadata.database``. It now resolves
targeting through ``plan_targets_environment``, so these tests pin the
behaviour that a plan with no environment key reaches every environment while a
scoped plan reaches only its own.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from schema_evolution import trigger_backfill as tb

ADD_COLUMN = [{"change_type": "add_column", "column_name": "new_col", "target_type": "string"}]


def _write_plan(root: Path, layer: str, table: str, name: str, metadata: dict, changes: list) -> Path:
    table_dir = root / layer / table
    table_dir.mkdir(parents=True, exist_ok=True)
    path = table_dir / name
    payload = {"metadata": {"table": table, **metadata}, "changes": changes}
    path.write_text(json.dumps(payload))
    return path


@pytest.fixture
def migrations_root(tmp_path: Path):
    root = tmp_path / "migrations"
    root.mkdir()
    with patch.object(tb, "MIGRATIONS_ROOT", root):
        yield root


class TestPlanLayer:
    def test_derives_layer_from_path(self, migrations_root: Path) -> None:
        p = _write_plan(migrations_root, "silver", "t", "p.json", {"layer": "silver"}, ADD_COLUMN)
        assert tb.plan_layer(p) == "silver"


class TestGetDatabase:
    def test_composes_from_environment_and_layer(self, migrations_root: Path) -> None:
        p = _write_plan(migrations_root, "gold", "t", "p.json", {"layer": "gold"}, ADD_COLUMN)
        assert tb.get_database(p, "qa") == "dp_qa_gold"
        assert tb.get_database(p, "prod", "dp") == "dp_prod_gold"

    def test_honours_custom_prefix(self, migrations_root: Path) -> None:
        p = _write_plan(migrations_root, "bronze", "t", "p.json", {"layer": "bronze"}, ADD_COLUMN)
        assert tb.get_database(p, "dev", "acme") == "acme_dev_bronze"


class TestFindPlans:
    def test_scoped_plan_matches_only_its_environment(self, migrations_root: Path) -> None:
        _write_plan(
            migrations_root, "silver", "resources", "p.json", {"layer": "silver", "environments": ["qa"]}, ADD_COLUMN
        )
        assert len(tb.find_plans("qa")) == 1
        assert tb.find_plans("dev") == []
        assert tb.find_plans("prod") == []

    def test_plan_without_environments_matches_every_environment(self, migrations_root: Path) -> None:
        _write_plan(migrations_root, "gold", "order_costs", "p.json", {"layer": "gold"}, ADD_COLUMN)
        for env in ("dev", "qa", "prod"):
            assert len(tb.find_plans(env)) == 1, env

    def test_list_of_environments(self, migrations_root: Path) -> None:
        _write_plan(
            migrations_root,
            "silver",
            "t",
            "p.json",
            {"layer": "silver", "environments": ["dev", "qa"]},
            ADD_COLUMN,
        )
        assert len(tb.find_plans("dev")) == 1
        assert len(tb.find_plans("qa")) == 1
        assert tb.find_plans("prod") == []

    def test_legacy_database_key_still_scopes(self, migrations_root: Path) -> None:
        _write_plan(migrations_root, "bronze", "t", "p.json", {"database": "dp_dev_bronze"}, ADD_COLUMN)
        assert len(tb.find_plans("dev")) == 1
        assert tb.find_plans("qa") == []

    def test_table_filter(self, migrations_root: Path) -> None:
        _write_plan(migrations_root, "gold", "wanted", "p.json", {"layer": "gold"}, ADD_COLUMN)
        _write_plan(migrations_root, "gold", "other", "p.json", {"layer": "gold"}, ADD_COLUMN)
        found = tb.find_plans("dev", table_filter="wanted")
        assert len(found) == 1
        assert found[0].parent.name == "wanted"

    def test_plans_without_add_column_are_ignored(self, migrations_root: Path) -> None:
        _write_plan(
            migrations_root,
            "gold",
            "t",
            "p.json",
            {"layer": "gold"},
            [{"change_type": "drop_column", "column_name": "gone"}],
        )
        assert tb.find_plans("dev") == []

    def test_malformed_plan_is_skipped_not_fatal(self, migrations_root: Path) -> None:
        good = _write_plan(migrations_root, "gold", "good", "p.json", {"layer": "gold"}, ADD_COLUMN)
        bad_dir = migrations_root / "gold" / "bad"
        bad_dir.mkdir(parents=True, exist_ok=True)
        (bad_dir / "p.json").write_text("{not json")

        found = tb.find_plans("dev")
        assert found == [good]

    def test_layer_mismatch_is_skipped_not_fatal(self, migrations_root: Path) -> None:
        """A plan whose declared layer contradicts its path is skipped with a warning."""
        _write_plan(migrations_root, "gold", "t", "p.json", {"layer": "silver"}, ADD_COLUMN)
        assert tb.find_plans("dev") == []


class TestGetLoadType:
    """Silver load_type lives in the sibling transformations file."""

    def test_reads_sibling_transformations_file(self, tmp_path: Path) -> None:
        (tmp_path / "schemas").mkdir()
        (tmp_path / "transformations").mkdir()
        schema = tmp_path / "schemas" / "t.yaml"
        schema.write_text("fields: []\n")
        (tmp_path / "transformations" / "t.yaml").write_text("source: s\nload_type: scd2\n")
        assert tb.get_load_type(schema) == "scd2"

    def test_falls_back_to_schema_load_type(self, tmp_path: Path) -> None:
        (tmp_path / "schemas").mkdir()
        schema = tmp_path / "schemas" / "t.yaml"
        schema.write_text("load_type: truncate\nfields: []\n")
        assert tb.get_load_type(schema) == "truncate"

    def test_defaults_to_merge(self, tmp_path: Path) -> None:
        (tmp_path / "schemas").mkdir()
        schema = tmp_path / "schemas" / "t.yaml"
        schema.write_text("fields: []\n")
        assert tb.get_load_type(schema) == "merge"
