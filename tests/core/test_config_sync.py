"""Unit tests for schema_evolution.config_sync — find, normalize, load, and check functions."""

import pytest
import yaml

from schema_evolution.config_sync import (
    _normalize_field,
    check_config_sync,
    find_config_file,
    load_config_fields,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# find_config_file
# ---------------------------------------------------------------------------


class TestFindConfigFile:
    """Tests for find_config_file filesystem search patterns."""

    def test_pattern1_direct_schemas(self, tmp_path):
        """Pattern 1: config/<layer>/<source>/schemas/<table>.yaml"""
        schema_dir = tmp_path / "bronze" / "erp" / "schemas"
        schema_dir.mkdir(parents=True)
        target = schema_dir / "invoice_lines.yaml"
        target.write_text("fields: []")

        result = find_config_file(str(tmp_path), "bronze", "erp", "invoice_lines")
        assert result == str(target)

    def test_pattern2_nested_schemas(self, tmp_path):
        """Pattern 2: config/<layer>/<source>/**/schemas/<table>.yaml"""
        nested = tmp_path / "gold" / "finance" / "finance" / "schemas"
        nested.mkdir(parents=True)
        target = nested / "budget_report.yaml"
        target.write_text("fields: []")

        result = find_config_file(str(tmp_path), "gold", "finance", "budget_report")
        assert result == str(target)

    def test_pattern3_data_products(self, tmp_path):
        """Pattern 3: config/<layer>/<source>/**/data_products/<table>.yaml"""
        dp_dir = tmp_path / "gold" / "finance" / "reporting" / "data_products"
        dp_dir.mkdir(parents=True)
        target = dp_dir / "kpi_summary.yaml"
        target.write_text("fields: []")

        result = find_config_file(str(tmp_path), "gold", "finance", "kpi_summary")
        assert result == str(target)

    def test_pattern4_silver_falls_back_to_bronze(self, tmp_path):
        """Silver layer falls back to bronze when not found directly."""
        bronze_dir = tmp_path / "bronze" / "erp" / "schemas"
        bronze_dir.mkdir(parents=True)
        target = bronze_dir / "invoice_lines.yaml"
        target.write_text("fields: []")

        # No silver directory exists — should fall back to bronze
        result = find_config_file(str(tmp_path), "silver", "erp", "invoice_lines")
        assert result == str(target)

    def test_not_found_returns_none(self, tmp_path):
        """Returns None when no config file matches."""
        result = find_config_file(str(tmp_path), "bronze", "erp", "nonexistent")
        assert result is None

    def test_case_insensitive_table_name(self, tmp_path):
        """Table name match is case-insensitive."""
        schema_dir = tmp_path / "bronze" / "erp" / "schemas"
        schema_dir.mkdir(parents=True)
        target = schema_dir / "Invoice_Lines.yaml"
        target.write_text("fields: []")

        result = find_config_file(str(tmp_path), "bronze", "erp", "invoice_lines")
        assert result == str(target)

    def test_silver_fallback_not_triggered_for_bronze(self, tmp_path):
        """Bronze layer does NOT fall back — only silver does."""
        # Create nothing; should return None (no infinite recursion)
        result = find_config_file(str(tmp_path), "bronze", "erp", "missing")
        assert result is None

    def test_pattern1_takes_priority_over_nested(self, tmp_path):
        """Direct schemas/ match (pattern 1) is returned before nested (pattern 2)."""
        direct = tmp_path / "gold" / "finance" / "schemas"
        direct.mkdir(parents=True)
        direct_file = direct / "report.yaml"
        direct_file.write_text("fields: []")

        nested = tmp_path / "gold" / "finance" / "sub" / "schemas"
        nested.mkdir(parents=True)
        (nested / "report.yaml").write_text("fields: []")

        result = find_config_file(str(tmp_path), "gold", "finance", "report")
        assert result == str(direct_file)


# ---------------------------------------------------------------------------
# _normalize_field
# ---------------------------------------------------------------------------


class TestNormalizeField:
    """Tests for _normalize_field dict transformation."""

    def test_maps_technical_name_to_table_column(self):
        field = {"technical_name": "customer_id", "data_type": "integer"}
        result = _normalize_field(field)
        assert result["table_column"] == "customer_id"
        assert result["table_data_type"] == "integer"

    def test_preserves_existing_table_column(self):
        field = {"technical_name": "cust_id", "table_column": "customer_id", "table_data_type": "string"}
        result = _normalize_field(field)
        # table_column already present — should not overwrite
        assert result["table_column"] == "customer_id"

    def test_missing_data_type_defaults_to_string(self):
        field = {"technical_name": "status"}
        result = _normalize_field(field)
        assert result["table_data_type"] == "string"

    def test_field_without_technical_name_unchanged(self):
        field = {"table_column": "id", "table_data_type": "long"}
        result = _normalize_field(field)
        assert result == {"table_column": "id", "table_data_type": "long"}

    def test_does_not_mutate_original(self):
        field = {"technical_name": "col", "data_type": "double"}
        _normalize_field(field)
        # Original dict should not have table_column injected
        assert "table_column" not in field


# ---------------------------------------------------------------------------
# load_config_fields
# ---------------------------------------------------------------------------


class TestLoadConfigFields:
    """Tests for load_config_fields YAML parsing + normalization."""

    def test_loads_and_normalizes_fields(self, tmp_path):
        config = {
            "fields": [
                {"table_column": "id", "table_data_type": "long"},
                {"technical_name": "name", "data_type": "string"},
            ],
        }
        path = tmp_path / "table.yaml"
        path.write_text(yaml.dump(config))

        result = load_config_fields(str(path))
        assert len(result) == 2
        assert result[0]["table_column"] == "id"
        # Second field was normalized from technical_name
        assert result[1]["table_column"] == "name"
        assert result[1]["table_data_type"] == "string"

    def test_empty_fields_returns_empty_list(self, tmp_path):
        path = tmp_path / "empty.yaml"
        path.write_text(yaml.dump({"fields": []}))

        result = load_config_fields(str(path))
        assert result == []

    def test_missing_fields_key_returns_empty_list(self, tmp_path):
        path = tmp_path / "no_fields.yaml"
        path.write_text(yaml.dump({"target_table": "t"}))

        result = load_config_fields(str(path))
        assert result == []


# ---------------------------------------------------------------------------
# check_config_sync
# ---------------------------------------------------------------------------


class TestCheckConfigSync:
    """Tests for check_config_sync comparison logic."""

    def test_all_in_sync(self):
        config_fields = [
            {"table_column": "id", "table_data_type": "long"},
            {"table_column": "name", "table_data_type": "string"},
        ]
        table_schema = {"id": "long", "name": "string"}

        result = check_config_sync(config_fields, table_schema)
        assert result["in_sync"] is True
        assert result["missing_in_table"] == []
        assert result["missing_in_config"] == []
        assert result["type_mismatches"] == []

    def test_missing_columns_in_table(self):
        config_fields = [
            {"table_column": "id", "table_data_type": "long"},
            {"table_column": "new_col", "table_data_type": "string"},
        ]
        table_schema = {"id": "long"}

        result = check_config_sync(config_fields, table_schema)
        assert result["in_sync"] is False
        assert "new_col" in result["missing_in_table"]

    def test_missing_columns_in_config(self):
        config_fields = [{"table_column": "id", "table_data_type": "long"}]
        table_schema = {"id": "long", "extra_col": "string"}

        result = check_config_sync(config_fields, table_schema)
        assert result["in_sync"] is False
        assert "extra_col" in result["missing_in_config"]

    def test_type_mismatch(self):
        config_fields = [{"table_column": "amount", "table_data_type": "double"}]
        table_schema = {"amount": "string"}

        result = check_config_sync(config_fields, table_schema)
        assert result["in_sync"] is False
        assert len(result["type_mismatches"]) == 1
        assert result["type_mismatches"][0]["column"] == "amount"
        assert result["type_mismatches"][0]["config_type"] == "double"
        assert result["type_mismatches"][0]["table_type"] == "string"

    def test_combination_of_issues(self):
        config_fields = [
            {"table_column": "id", "table_data_type": "long"},
            {"table_column": "amount", "table_data_type": "double"},
            {"table_column": "only_config", "table_data_type": "string"},
        ]
        table_schema = {"id": "long", "amount": "string", "only_table": "integer"}

        result = check_config_sync(config_fields, table_schema)
        assert result["in_sync"] is False
        assert "only_config" in result["missing_in_table"]
        assert "only_table" in result["missing_in_config"]
        assert len(result["type_mismatches"]) == 1

    def test_case_insensitive_column_match(self):
        config_fields = [{"table_column": "ID", "table_data_type": "long"}]
        table_schema = {"id": "long"}

        result = check_config_sync(config_fields, table_schema)
        assert result["in_sync"] is True

    def test_equivalent_types_are_in_sync(self):
        """bigint and long are equivalent after normalization."""
        config_fields = [{"table_column": "id", "table_data_type": "bigint"}]
        table_schema = {"id": "long"}

        result = check_config_sync(config_fields, table_schema)
        assert result["in_sync"] is True
        assert result["type_mismatches"] == []

    def test_empty_config_and_table(self):
        result = check_config_sync([], {})
        assert result["in_sync"] is True
