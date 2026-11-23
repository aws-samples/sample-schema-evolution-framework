"""Unit tests for schema evolution metadata apply handlers (mocked PyIceberg)."""

from unittest.mock import MagicMock, patch

from schema_evolution.apply import (
    apply_add_partition_field,
    apply_drop_partition_field,
    apply_set_sort_order,
    apply_set_table_properties,
    apply_unset_table_properties,
)


def _make_mock_table(partition_fields=None, sort_fields=None, properties=None):
    """Create a mock PyIceberg table with configurable metadata."""
    table = MagicMock()

    # Mock spec
    spec = MagicMock()
    spec.fields = partition_fields or []
    table.spec.return_value = spec

    # Mock schema for field lookups
    schema = MagicMock()
    table.schema.return_value = schema

    # Mock sort order
    sort_order = MagicMock()
    sort_order.is_unsorted = sort_fields is None or len(sort_fields) == 0
    sort_order.fields = sort_fields or []
    table.sort_order.return_value = sort_order

    # Mock properties
    table.properties = properties or {}

    # Mock context managers for updates
    table.update_spec.return_value.__enter__ = MagicMock()
    table.update_spec.return_value.__exit__ = MagicMock(return_value=False)
    table.update_sort_order.return_value.__enter__ = MagicMock()
    table.update_sort_order.return_value.__exit__ = MagicMock(return_value=False)
    # Properties are managed via a transaction context manager
    table.transaction.return_value.__enter__ = MagicMock()
    table.transaction.return_value.__exit__ = MagicMock(return_value=False)

    return table


# --- apply_add_partition_field ---


@patch("schema_evolution.apply.get_table_partition_spec", return_value=[])
def test_apply_add_partition_field_month(mock_spec):
    table = _make_mock_table()
    change = {"column_name": "event_date", "transform": "month", "transform_param": None}
    success, error = apply_add_partition_field(table, change)
    assert success is True
    assert error is None
    table.update_spec.assert_called_once()


@patch("schema_evolution.apply.get_table_partition_spec", return_value=[])
def test_apply_add_partition_field_bucket(mock_spec):
    table = _make_mock_table()
    change = {"column_name": "user_id", "transform": "bucket", "transform_param": 16}
    success, error = apply_add_partition_field(table, change)
    assert success is True
    assert error is None


@patch("schema_evolution.apply.get_table_partition_spec", return_value=[])
def test_apply_add_partition_field_identity(mock_spec):
    table = _make_mock_table()
    change = {"column_name": "region", "transform": "identity", "transform_param": None}
    success, error = apply_add_partition_field(table, change)
    assert success is True
    assert error is None


@patch("schema_evolution.apply.get_table_partition_spec", return_value=[])
def test_apply_add_partition_field_truncate(mock_spec):
    table = _make_mock_table()
    change = {"column_name": "name", "transform": "truncate", "transform_param": 10}
    success, error = apply_add_partition_field(table, change)
    assert success is True
    assert error is None


@patch("schema_evolution.apply.get_table_partition_spec", return_value=[])
def test_apply_add_partition_field_year(mock_spec):
    table = _make_mock_table()
    change = {"column_name": "created_at", "transform": "year", "transform_param": None}
    success, error = apply_add_partition_field(table, change)
    assert success is True
    assert error is None
    table.update_spec.assert_called_once()


@patch("schema_evolution.apply.get_table_partition_spec", return_value=[])
def test_apply_add_partition_field_day(mock_spec):
    table = _make_mock_table()
    change = {"column_name": "event_ts", "transform": "day", "transform_param": None}
    success, error = apply_add_partition_field(table, change)
    assert success is True
    assert error is None
    table.update_spec.assert_called_once()


@patch("schema_evolution.apply.get_table_partition_spec", return_value=[])
def test_apply_add_partition_field_hour(mock_spec):
    table = _make_mock_table()
    change = {"column_name": "log_time", "transform": "hour", "transform_param": None}
    success, error = apply_add_partition_field(table, change)
    assert success is True
    assert error is None
    table.update_spec.assert_called_once()


@patch("schema_evolution.apply.get_table_partition_spec", return_value=[])
def test_apply_add_partition_field_unsupported_transform(mock_spec):
    table = _make_mock_table()
    change = {"column_name": "col", "transform": "unknown_xform", "transform_param": None}
    success, error = apply_add_partition_field(table, change)
    assert success is False
    assert error is not None and "Unsupported partition transform" in error


@patch("schema_evolution.apply.get_table_partition_spec", side_effect=RuntimeError("catalog error"))
def test_apply_add_partition_field_exception(mock_spec):
    table = _make_mock_table()
    change = {"column_name": "col", "transform": "month", "transform_param": None}
    success, error = apply_add_partition_field(table, change)
    assert success is False
    assert error is not None and "catalog error" in error


@patch(
    "schema_evolution.apply.get_table_partition_spec",
    return_value=[{"column": "DATE", "transform": "month", "transform_param": None}],
)
def test_apply_add_partition_field_already_exists(mock_spec):
    table = _make_mock_table()
    change = {"column_name": "date", "transform": "month", "transform_param": None}
    success, error = apply_add_partition_field(table, change)
    assert success is True
    table.update_spec.assert_not_called()


# --- apply_drop_partition_field ---


@patch(
    "schema_evolution.apply.get_table_partition_spec",
    return_value=[{"column": "date", "transform": "month", "transform_param": None}],
)
def test_apply_drop_partition_field(mock_spec):
    # Mock a partition field to drop
    pf = MagicMock()
    pf.source_id = 1
    pf.name = "date_month"
    source_field = MagicMock()
    source_field.name = "date"

    table = _make_mock_table(partition_fields=[pf])
    table.schema.return_value.find_field.return_value = source_field

    change = {"column_name": "date", "transform": "month", "transform_param": None}
    success, error = apply_drop_partition_field(table, change)
    assert success is True


@patch("schema_evolution.apply.get_table_partition_spec", return_value=[])
def test_apply_drop_partition_field_not_exists(mock_spec):
    table = _make_mock_table()
    change = {"column_name": "nonexistent", "transform": "identity", "transform_param": None}
    success, error = apply_drop_partition_field(table, change)
    assert success is True
    table.update_spec.assert_not_called()


@patch("schema_evolution.apply.get_table_partition_spec", side_effect=RuntimeError("drop error"))
def test_apply_drop_partition_field_exception(mock_spec):
    table = _make_mock_table()
    change = {"column_name": "col", "transform": "month", "transform_param": None}
    success, error = apply_drop_partition_field(table, change)
    assert success is False
    assert error is not None and "drop error" in error


# --- apply_set_sort_order ---


@patch("schema_evolution.apply.get_table_sort_order", return_value=[])
def test_apply_set_sort_order(mock_sort):
    table = _make_mock_table()
    change = {"sort_fields": [{"column": "id", "direction": "asc"}]}
    success, error = apply_set_sort_order(table, change)
    assert success is True
    table.update_sort_order.assert_called_once()


@patch("schema_evolution.apply.get_table_sort_order", return_value=[{"column": "id", "direction": "asc"}])
def test_apply_set_sort_order_already_matches(mock_sort):
    table = _make_mock_table()
    change = {"sort_fields": [{"column": "id", "direction": "asc"}]}
    success, error = apply_set_sort_order(table, change)
    assert success is True
    table.update_sort_order.assert_not_called()


@patch("schema_evolution.apply.get_table_sort_order", return_value=[{"column": "id", "direction": "asc"}])
def test_apply_set_sort_order_empty_unsorts(mock_sort):
    table = _make_mock_table()
    change = {"sort_fields": []}
    success, error = apply_set_sort_order(table, change)
    assert success is True
    table.update_sort_order.assert_called_once()


@patch("schema_evolution.apply.get_table_sort_order", return_value=[{"column": "a", "direction": "asc"}])
def test_apply_set_sort_order_different_direction(mock_sort):
    table = _make_mock_table()
    change = {"sort_fields": [{"column": "a", "direction": "desc"}]}
    success, error = apply_set_sort_order(table, change)
    assert success is True
    table.update_sort_order.assert_called_once()


@patch("schema_evolution.apply.get_table_sort_order", side_effect=RuntimeError("sort error"))
def test_apply_set_sort_order_exception(mock_sort):
    table = _make_mock_table()
    change = {"sort_fields": [{"column": "id", "direction": "asc"}]}
    success, error = apply_set_sort_order(table, change)
    assert success is False
    assert error is not None and "sort error" in error


# --- apply_set_table_properties ---


def test_apply_set_table_properties():
    table = _make_mock_table()
    change = {"properties": {"write.spark.accept-any-schema": "true"}}
    success, error = apply_set_table_properties(table, change)
    assert success is True
    table.transaction.assert_called_once()


def test_apply_set_table_properties_empty():
    table = _make_mock_table()
    change = {"properties": {}}
    success, error = apply_set_table_properties(table, change)
    assert success is True
    table.transaction.assert_not_called()


def test_apply_set_table_properties_multiple():
    table = _make_mock_table()
    change = {"properties": {"key1": "val1", "key2": "val2"}}
    success, error = apply_set_table_properties(table, change)
    assert success is True
    table.transaction.assert_called_once()


def test_apply_set_table_properties_exception():
    table = _make_mock_table()
    table.transaction.return_value.__enter__ = MagicMock(side_effect=RuntimeError("props error"))
    change = {"properties": {"key1": "val1"}}
    success, error = apply_set_table_properties(table, change)
    assert success is False
    assert error is not None and "props error" in error


# --- apply_unset_table_properties ---


def test_apply_unset_table_properties():
    table = _make_mock_table()
    change = {"property_keys": ["old.key"]}
    success, error = apply_unset_table_properties(table, change)
    assert success is True
    table.transaction.assert_called_once()


def test_apply_unset_table_properties_empty():
    table = _make_mock_table()
    change = {"property_keys": []}
    success, error = apply_unset_table_properties(table, change)
    assert success is True
    table.transaction.assert_not_called()


def test_apply_unset_table_properties_multiple():
    table = _make_mock_table()
    change = {"property_keys": ["key1", "key2", "key3"]}
    success, error = apply_unset_table_properties(table, change)
    assert success is True
    table.transaction.assert_called_once()


def test_apply_unset_table_properties_exception():
    table = _make_mock_table()
    table.transaction.return_value.__enter__ = MagicMock(side_effect=RuntimeError("unset error"))
    change = {"property_keys": ["key1"]}
    success, error = apply_unset_table_properties(table, change)
    assert success is False
    assert error is not None and "unset error" in error


# --- apply_ddl dispatch for metadata change types ---

from schema_evolution.apply import apply_ddl  # noqa: E402


def _dispatch_catalog():
    """Catalog whose loaded table has an empty schema (so _resolve_col is a no-op)."""
    catalog = MagicMock()
    table = MagicMock()
    table.schema.return_value.fields = []
    catalog.load_table.return_value = table
    return catalog


@patch("schema_evolution.apply.apply_add_partition_field", return_value=(True, None))
def test_apply_ddl_dispatch_add_partition_field(mock_fn):
    ok, err = apply_ddl(_dispatch_catalog(), "db", "t", {"change_type": "add_partition_field", "column_name": "c"})
    assert ok is True and err is None
    mock_fn.assert_called_once()


@patch("schema_evolution.apply.apply_drop_partition_field", return_value=(True, None))
def test_apply_ddl_dispatch_drop_partition_field(mock_fn):
    ok, err = apply_ddl(_dispatch_catalog(), "db", "t", {"change_type": "drop_partition_field", "column_name": "c"})
    assert ok is True and err is None
    mock_fn.assert_called_once()


@patch("schema_evolution.apply.apply_set_sort_order", return_value=(True, None))
def test_apply_ddl_dispatch_set_sort_order(mock_fn):
    ok, err = apply_ddl(
        _dispatch_catalog(),
        "db",
        "t",
        {"change_type": "set_sort_order", "column_name": "__sort_order__", "rebuild": False},
    )
    assert ok is True and err is None
    mock_fn.assert_called_once()


@patch("schema_evolution.apply.apply_set_table_properties", return_value=(True, None))
def test_apply_ddl_dispatch_set_table_properties(mock_fn):
    ok, err = apply_ddl(
        _dispatch_catalog(), "db", "t", {"change_type": "set_table_properties", "column_name": "__table_properties__"}
    )
    assert ok is True and err is None
    mock_fn.assert_called_once()


@patch("schema_evolution.apply.apply_unset_table_properties", return_value=(True, None))
def test_apply_ddl_dispatch_unset_table_properties(mock_fn):
    ok, err = apply_ddl(
        _dispatch_catalog(), "db", "t", {"change_type": "unset_table_properties", "column_name": "__table_properties__"}
    )
    assert ok is True and err is None
    mock_fn.assert_called_once()


def test_apply_ddl_unknown_change_type():
    ok, err = apply_ddl(_dispatch_catalog(), "db", "t", {"change_type": "bogus", "column_name": "c"})
    assert ok is False
    assert err is not None and "Unknown change type" in err


# --- apply_ddl dispatch routing for metadata change types ---


@patch("schema_evolution.apply.apply_add_partition_field", return_value=(True, None))
def test_apply_ddl_routes_add_partition_field(mock_handler):
    from schema_evolution.apply import apply_ddl

    catalog = MagicMock()
    change = {"change_type": "add_partition_field", "column_name": "user_id", "transform": "bucket"}
    success, error = apply_ddl(catalog, "ns", "tbl", change)
    assert success is True
    mock_handler.assert_called_once()


@patch("schema_evolution.apply.apply_drop_partition_field", return_value=(True, None))
def test_apply_ddl_routes_drop_partition_field(mock_handler):
    from schema_evolution.apply import apply_ddl

    catalog = MagicMock()
    change = {"change_type": "drop_partition_field", "column_name": "region", "transform": "identity"}
    success, error = apply_ddl(catalog, "ns", "tbl", change)
    assert success is True
    mock_handler.assert_called_once()


@patch("schema_evolution.apply.apply_set_sort_order", return_value=(True, None))
def test_apply_ddl_routes_set_sort_order(mock_handler):
    from schema_evolution.apply import apply_ddl

    catalog = MagicMock()
    change = {
        "change_type": "set_sort_order",
        "column_name": "__sort_order__",
        "sort_fields": [],
        "rebuild": False,
    }
    success, error = apply_ddl(catalog, "ns", "tbl", change)
    assert success is True
    mock_handler.assert_called_once()


@patch("schema_evolution.apply.apply_set_table_properties", return_value=(True, None))
def test_apply_ddl_routes_set_table_properties(mock_handler):
    from schema_evolution.apply import apply_ddl

    catalog = MagicMock()
    change = {"change_type": "set_table_properties", "column_name": "__table_properties__", "properties": {"k": "v"}}
    success, error = apply_ddl(catalog, "ns", "tbl", change)
    assert success is True
    mock_handler.assert_called_once()


@patch("schema_evolution.apply.apply_unset_table_properties", return_value=(True, None))
def test_apply_ddl_routes_unset_table_properties(mock_handler):
    from schema_evolution.apply import apply_ddl

    catalog = MagicMock()
    change = {"change_type": "unset_table_properties", "column_name": "__table_properties__", "property_keys": ["k"]}
    success, error = apply_ddl(catalog, "ns", "tbl", change)
    assert success is True
    mock_handler.assert_called_once()
