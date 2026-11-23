"""Unit tests for check_metadata_sync (mocked PyIceberg table)."""

from unittest.mock import MagicMock, patch

from schema_evolution.config_sync import check_metadata_sync

# Note: check_metadata_sync does a local import from schema_evolution.plan,
# so we patch at the definition site (schema_evolution.plan).


@patch("schema_evolution.plan.get_table_partition_spec", return_value=[])
@patch("schema_evolution.plan.get_table_sort_order", return_value=[])
@patch("schema_evolution.plan.get_table_properties", return_value={})
def test_metadata_in_sync_when_yaml_empty(mock_props, mock_sort, mock_part):
    yaml_data = {}  # no partition_strategy, no table_properties
    table = MagicMock()
    result = check_metadata_sync(yaml_data, table)
    assert result["metadata_in_sync"] is True
    assert result["partition_drift"] == []
    assert result["sort_order_drift"] == []
    assert result["property_drift"] == []


@patch("schema_evolution.plan.get_table_partition_spec", return_value=[])
@patch("schema_evolution.plan.get_table_sort_order", return_value=[])
@patch("schema_evolution.plan.get_table_properties", return_value={})
def test_partition_drift_detected(mock_props, mock_sort, mock_part):
    yaml_data = {"partition_strategy": {"partition_by": [{"column": "date", "transform": "month"}]}}
    table = MagicMock()
    result = check_metadata_sync(yaml_data, table)
    assert result["metadata_in_sync"] is False
    assert len(result["partition_drift"]) == 1
    assert "Missing partition field" in result["partition_drift"][0]


@patch(
    "schema_evolution.plan.get_table_partition_spec",
    return_value=[{"column": "date", "transform": "month", "transform_param": None}],
)
@patch("schema_evolution.plan.get_table_sort_order", return_value=[])
@patch("schema_evolution.plan.get_table_properties", return_value={})
def test_partition_in_sync(mock_props, mock_sort, mock_part):
    yaml_data = {"partition_strategy": {"partition_by": [{"column": "date", "transform": "month"}]}}
    table = MagicMock()
    result = check_metadata_sync(yaml_data, table)
    assert result["partition_drift"] == []


@patch("schema_evolution.plan.get_table_partition_spec", return_value=[])
@patch(
    "schema_evolution.plan.get_table_sort_order",
    return_value=[{"column": "id", "direction": "desc"}],
)
@patch("schema_evolution.plan.get_table_properties", return_value={})
def test_sort_order_drift_detected(mock_props, mock_sort, mock_part):
    yaml_data = {"partition_strategy": {"sort_order": [{"column": "id", "direction": "asc"}]}}
    table = MagicMock()
    result = check_metadata_sync(yaml_data, table)
    assert result["metadata_in_sync"] is False
    assert len(result["sort_order_drift"]) == 1
    assert "Sort order mismatch" in result["sort_order_drift"][0]


@patch("schema_evolution.plan.get_table_partition_spec", return_value=[])
@patch(
    "schema_evolution.plan.get_table_sort_order",
    return_value=[{"column": "id", "direction": "asc"}],
)
@patch("schema_evolution.plan.get_table_properties", return_value={})
def test_sort_order_in_sync(mock_props, mock_sort, mock_part):
    yaml_data = {"partition_strategy": {"sort_order": [{"column": "id", "direction": "asc"}]}}
    table = MagicMock()
    result = check_metadata_sync(yaml_data, table)
    assert result["sort_order_drift"] == []
    assert result["metadata_in_sync"] is True


@patch("schema_evolution.plan.get_table_partition_spec", return_value=[])
@patch("schema_evolution.plan.get_table_sort_order", return_value=[])
@patch("schema_evolution.plan.get_table_properties", return_value={"key": "old"})
def test_property_drift_detected(mock_props, mock_sort, mock_part):
    yaml_data = {"table_properties": {"key": "new"}}
    table = MagicMock()
    result = check_metadata_sync(yaml_data, table)
    assert result["metadata_in_sync"] is False
    assert len(result["property_drift"]) == 1
    assert "Property 'key'" in result["property_drift"][0]


@patch("schema_evolution.plan.get_table_partition_spec", return_value=[])
@patch("schema_evolution.plan.get_table_sort_order", return_value=[])
@patch("schema_evolution.plan.get_table_properties", return_value={"key": "value"})
def test_property_in_sync(mock_props, mock_sort, mock_part):
    yaml_data = {"table_properties": {"key": "value"}}
    table = MagicMock()
    result = check_metadata_sync(yaml_data, table)
    assert result["property_drift"] == []


@patch(
    "schema_evolution.plan.get_table_partition_spec",
    return_value=[{"column": "old_col", "transform": "identity", "transform_param": None}],
)
@patch("schema_evolution.plan.get_table_sort_order", return_value=[])
@patch("schema_evolution.plan.get_table_properties", return_value={})
def test_extra_partition_field_drift(mock_props, mock_sort, mock_part):
    yaml_data = {}  # No partition_strategy means desired is empty
    table = MagicMock()
    result = check_metadata_sync(yaml_data, table)
    assert result["metadata_in_sync"] is False
    assert len(result["partition_drift"]) == 1
    assert "Extra partition field" in result["partition_drift"][0]


@patch("schema_evolution.plan.get_table_partition_spec", return_value=[])
@patch("schema_evolution.plan.get_table_sort_order", return_value=[])
@patch("schema_evolution.plan.get_table_properties", return_value={})
def test_metadata_all_in_sync_with_matching_config(mock_props, mock_sort, mock_part):
    """When YAML specifies empty-equivalent config and table is empty, all in sync."""
    yaml_data = {"partition_strategy": {}, "table_properties": {}}
    table = MagicMock()
    result = check_metadata_sync(yaml_data, table)
    assert result["metadata_in_sync"] is True
    assert result["partition_drift"] == []
    assert result["sort_order_drift"] == []
    assert result["property_drift"] == []
