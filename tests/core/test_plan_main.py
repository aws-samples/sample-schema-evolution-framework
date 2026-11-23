"""Tests for plan.py main() CLI entrypoint with mocked AWS and filesystem."""

import json
import os
from unittest.mock import MagicMock, patch

import pytest
import yaml


@pytest.fixture
def config_file(tmp_path):
    """Create a minimal config YAML for testing."""
    config = {
        "fields": [
            {"table_column": "id", "table_data_type": "long", "source_field": "ID", "source_data_type": "long"},
            {"table_column": "name", "table_data_type": "string", "source_field": "NAME", "source_data_type": "string"},
        ],
        "partition_strategy": {
            "partition_by": [{"column": "id"}],
            "sort_order": [{"column": "id", "direction": "asc"}],
        },
        "table_properties": {"write.target-file-size-bytes": "134217728"},
    }
    path = tmp_path / "config.yaml"
    with open(path, "w") as f:
        yaml.dump(config, f)
    return str(path)


def _make_mock_table():
    """Create a mock table that matches the config (no changes expected)."""
    table = MagicMock()
    # Schema fields
    f1 = MagicMock()
    f1.name = "id"
    f1.required = False
    f1.field_type = MagicMock()
    f1.field_type.__str__ = lambda self: "long"
    f2 = MagicMock()
    f2.name = "name"
    f2.required = False
    f2.field_type = MagicMock()
    f2.field_type.__str__ = lambda self: "string"
    table.schema.return_value.fields = [f1, f2]
    # Properties
    table.properties = {"write.target-file-size-bytes": "134217728"}
    return table


def _make_mock_table_with_changes():
    """Create a mock table that differs from config (changes expected)."""
    table = MagicMock()
    # Only 'id' column exists, 'name' is missing → triggers add_column
    f1 = MagicMock()
    f1.name = "id"
    f1.required = False
    f1.field_type = MagicMock()
    f1.field_type.__str__ = lambda self: "long"
    table.schema.return_value.fields = [f1]
    # No properties → triggers set_table_properties
    table.properties = {}
    return table


@patch("schema_evolution.plan.load_catalog")
@patch(
    "schema_evolution.plan.get_table_partition_spec",
    return_value=[{"column": "id", "transform": "identity", "transform_param": None}],
)
@patch("schema_evolution.plan.get_table_sort_order", return_value=[{"column": "id", "direction": "asc"}])
def test_main_no_changes(mock_sort, mock_part, mock_catalog, config_file, tmp_path):
    """When table matches config, no plan file is written."""
    table = _make_mock_table()
    mock_catalog.return_value.load_table.return_value = table

    migrations_dir = str(tmp_path / "migrations")
    test_args = [
        "schema-evolution-plan",
        "--account_id",
        "123456789012",
        "--config_file",
        config_file,
        "--layer",
        "silver",
        "--table",
        "test_table",
        "--environment",
        "dev",
        "--migrations_dir",
        migrations_dir,
    ]
    with patch("sys.argv", test_args):
        from schema_evolution.plan import main

        main()

    # No migrations dir created since no changes
    if os.path.exists(migrations_dir):
        files = list(os.walk(migrations_dir))
        json_files = [f for _, _, fnames in files for f in fnames if f.endswith(".json")]
        assert json_files == []


@patch("schema_evolution.plan.load_catalog")
@patch("schema_evolution.plan.get_table_partition_spec", return_value=[])
@patch("schema_evolution.plan.get_table_sort_order", return_value=[])
def test_main_with_changes_writes_plan(mock_sort, mock_part, mock_catalog, config_file, tmp_path):
    """When table differs from config, a plan file is written."""
    table = _make_mock_table_with_changes()
    mock_catalog.return_value.load_table.return_value = table

    migrations_dir = str(tmp_path / "migrations")
    test_args = [
        "schema-evolution-plan",
        "--account_id",
        "123456789012",
        "--config_file",
        config_file,
        "--layer",
        "silver",
        "--table",
        "test_table",
        "--environment",
        "dev",
        "--migrations_dir",
        migrations_dir,
    ]
    with patch("sys.argv", test_args):
        from schema_evolution.plan import main

        main()

    # Plan file should exist
    assert os.path.exists(migrations_dir)
    json_files = []
    for root, _, fnames in os.walk(migrations_dir):
        for fname in fnames:
            if fname.endswith(".json"):
                json_files.append(os.path.join(root, fname))
    assert len(json_files) == 1

    with open(json_files[0]) as f:
        plan = json.load(f)
    assert plan["metadata"]["layer"] == "silver"
    assert "environments" not in plan["metadata"]
    assert plan["metadata"]["table"] == "test_table"
    # Should have add_column for 'name', partition changes, sort order, and property set
    change_types = [c["change_type"] for c in plan["changes"]]
    assert "add_column" in change_types


@patch("schema_evolution.plan.load_catalog")
def test_main_table_not_found(mock_catalog, config_file, tmp_path):
    """When table doesn't exist, main() returns early without error."""
    mock_catalog.return_value.load_table.side_effect = Exception("table not found")

    migrations_dir = str(tmp_path / "migrations")
    test_args = [
        "schema-evolution-plan",
        "--account_id",
        "123456789012",
        "--config_file",
        config_file,
        "--layer",
        "silver",
        "--table",
        "missing_table",
        "--environment",
        "dev",
        "--migrations_dir",
        migrations_dir,
    ]
    with patch("sys.argv", test_args):
        from schema_evolution.plan import main

        main()  # Should not raise

    # No plan file should exist
    assert not os.path.exists(migrations_dir)


@pytest.fixture
def env_gated_config_file(tmp_path):
    """Config YAML with one env-gated add_column and one non-gated field."""
    config = {
        "fields": [
            {
                "table_column": "id",
                "table_data_type": "long",
                "source_field": "ID",
                "source_data_type": "long",
            },
            {
                "table_column": "env_only_col",
                "table_data_type": "string",
                "source_field": "env_only_col",
                "source_data_type": "string",
                "environments": ["qa"],
            },
        ],
        "partition_strategy": {
            "partition_by": [{"column": "id"}],
            "sort_order": [{"column": "id", "direction": "asc"}],
        },
        "table_properties": {"write.target-file-size-bytes": "134217728"},
    }
    path = tmp_path / "env_config.yaml"
    with open(path, "w") as f:
        yaml.dump(config, f)
    return str(path)


@patch("schema_evolution.plan.load_catalog")
def test_only_env_additions_writes_plan_with_env_gated_col(mock_catalog, env_gated_config_file, tmp_path):
    """--only_env_additions should write a plan containing only the env-gated add_column."""
    mock_table = MagicMock()
    # Live table only has 'id' — 'env_only_col' is missing → add_column
    f1 = MagicMock()
    f1.name = "id"
    f1.field_type = MagicMock()
    f1.field_type.__str__ = lambda self: "long"
    mock_table.schema.return_value.fields = [f1]
    mock_table.properties = {"write.target-file-size-bytes": "134217728"}
    mock_catalog.return_value.load_table.return_value = mock_table

    migrations_dir = str(tmp_path / "migrations")
    test_args = [
        "schema-evolution-plan",
        "--account_id",
        "123456789012",
        "--config_file",
        env_gated_config_file,
        "--layer",
        "silver",
        "--table",
        "test_table",
        "--environment",
        "qa",
        "--migrations_dir",
        migrations_dir,
        "--only_env_additions",
    ]
    with patch("sys.argv", test_args):
        from schema_evolution.plan import main

        main()

    plan_files = list(os.walk(migrations_dir))
    json_files = [f for root, _, files in plan_files for f in files if f.endswith(".json")]
    assert len(json_files) == 1

    plan_path = os.path.join(migrations_dir, "silver", "test_table", json_files[0])
    with open(plan_path) as f:
        plan = json.load(f)

    changes = plan["changes"]
    assert len(changes) == 1
    assert changes[0]["change_type"] == "add_column"
    assert changes[0]["column_name"] == "env_only_col"


@patch("schema_evolution.plan.load_catalog")
def test_only_env_additions_no_env_gated_cols_writes_nothing(mock_catalog, config_file, tmp_path):
    """--only_env_additions with no env-gated columns should write no plan."""
    mock_table = MagicMock()
    # Missing 'name' column — would normally produce an add_column
    f1 = MagicMock()
    f1.name = "id"
    f1.field_type = MagicMock()
    f1.field_type.__str__ = lambda self: "long"
    mock_table.schema.return_value.fields = [f1]
    mock_table.properties = {}
    mock_catalog.return_value.load_table.return_value = mock_table

    migrations_dir = str(tmp_path / "migrations")
    test_args = [
        "schema-evolution-plan",
        "--account_id",
        "123456789012",
        "--config_file",
        config_file,
        "--layer",
        "silver",
        "--table",
        "test_table",
        "--environment",
        "dev",
        "--migrations_dir",
        migrations_dir,
        "--only_env_additions",
    ]
    with patch("sys.argv", test_args):
        from schema_evolution.plan import main

        main()

    # No plan file should be written — 'name' is not env-gated
    assert not os.path.exists(migrations_dir)
