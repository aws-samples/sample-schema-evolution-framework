"""Additional coverage tests for plan.py: get_table_partition_spec, get_table_sort_order,
_serialize_change, and write_plan."""

import json
import os
from unittest.mock import MagicMock

from schema_evolution import ChangeType, SchemaChange
from schema_evolution.plan import (
    _serialize_change,
    get_table_partition_spec,
    get_table_sort_order,
    write_plan,
)

# --- get_table_partition_spec ---


def _mock_transform(cls_name: str, **attrs):
    """Create a mock transform that passes isinstance checks via __class__.__name__."""
    from pyiceberg.transforms import (
        BucketTransform,
        DayTransform,
        HourTransform,
        IdentityTransform,
        MonthTransform,
        TruncateTransform,
        YearTransform,
    )

    class_map = {
        "IdentityTransform": IdentityTransform,
        "YearTransform": YearTransform,
        "MonthTransform": MonthTransform,
        "DayTransform": DayTransform,
        "HourTransform": HourTransform,
        "BucketTransform": BucketTransform,
        "TruncateTransform": TruncateTransform,
    }
    # For identity/time transforms, instantiate them directly
    if cls_name in ("IdentityTransform", "YearTransform", "MonthTransform", "DayTransform", "HourTransform"):
        return class_map[cls_name]()
    elif cls_name == "BucketTransform":
        return BucketTransform(attrs.get("num_buckets", 16))
    elif cls_name == "TruncateTransform":
        return TruncateTransform(attrs.get("width", 10))
    # Fallback: unknown transform
    m = MagicMock()
    m.__class__ = type("UnknownTransform", (), {})
    return m


def _make_partition_table(spec_fields: list) -> MagicMock:
    """Build a mock table with given partition spec fields."""
    table = MagicMock()
    spec = MagicMock()
    spec.fields = spec_fields
    table.spec.return_value = spec

    schema = MagicMock()

    def find_field(source_id):
        f = MagicMock()
        f.name = f"col_{source_id}"
        return f

    schema.find_field = find_field
    table.schema.return_value = schema
    return table


def test_get_table_partition_spec_identity():
    field = MagicMock()
    field.source_id = 1
    field.transform = _mock_transform("IdentityTransform")
    table = _make_partition_table([field])
    result = get_table_partition_spec(table)
    assert result == [{"column": "col_1", "transform": "identity", "transform_param": None}]


def test_get_table_partition_spec_year():
    field = MagicMock()
    field.source_id = 2
    field.transform = _mock_transform("YearTransform")
    table = _make_partition_table([field])
    result = get_table_partition_spec(table)
    assert result == [{"column": "col_2", "transform": "year", "transform_param": None}]


def test_get_table_partition_spec_month():
    field = MagicMock()
    field.source_id = 3
    field.transform = _mock_transform("MonthTransform")
    table = _make_partition_table([field])
    result = get_table_partition_spec(table)
    assert result == [{"column": "col_3", "transform": "month", "transform_param": None}]


def test_get_table_partition_spec_day():
    field = MagicMock()
    field.source_id = 4
    field.transform = _mock_transform("DayTransform")
    table = _make_partition_table([field])
    result = get_table_partition_spec(table)
    assert result == [{"column": "col_4", "transform": "day", "transform_param": None}]


def test_get_table_partition_spec_hour():
    field = MagicMock()
    field.source_id = 5
    field.transform = _mock_transform("HourTransform")
    table = _make_partition_table([field])
    result = get_table_partition_spec(table)
    assert result == [{"column": "col_5", "transform": "hour", "transform_param": None}]


def test_get_table_partition_spec_bucket():
    field = MagicMock()
    field.source_id = 6
    field.transform = _mock_transform("BucketTransform", num_buckets=32)
    table = _make_partition_table([field])
    result = get_table_partition_spec(table)
    assert result == [{"column": "col_6", "transform": "bucket", "transform_param": 32}]


def test_get_table_partition_spec_truncate():
    field = MagicMock()
    field.source_id = 7
    field.transform = _mock_transform("TruncateTransform", width=5)
    table = _make_partition_table([field])
    result = get_table_partition_spec(table)
    assert result == [{"column": "col_7", "transform": "truncate", "transform_param": 5}]


def test_get_table_partition_spec_unknown_transform():
    field = MagicMock()
    field.source_id = 8
    field.transform = _mock_transform("UnknownTransform")
    table = _make_partition_table([field])
    result = get_table_partition_spec(table)
    # Unknown transform falls through to str(transform)
    assert result[0]["column"] == "col_8"
    assert result[0]["transform_param"] is None


def test_get_table_partition_spec_multiple_fields():
    f1 = MagicMock()
    f1.source_id = 1
    f1.transform = _mock_transform("IdentityTransform")
    f2 = MagicMock()
    f2.source_id = 2
    f2.transform = _mock_transform("MonthTransform")
    table = _make_partition_table([f1, f2])
    result = get_table_partition_spec(table)
    assert len(result) == 2


# --- get_table_sort_order ---


def test_get_table_sort_order_unsorted():
    table = MagicMock()
    sort_order = MagicMock()
    sort_order.is_unsorted = True
    table.sort_order.return_value = sort_order
    assert get_table_sort_order(table) == []


def test_get_table_sort_order_asc():
    from pyiceberg.table.sorting import SortDirection

    table = MagicMock()
    sort_order = MagicMock()
    sort_order.is_unsorted = False

    field = MagicMock()
    field.source_id = 1
    field.direction = SortDirection.ASC
    sort_order.fields = [field]
    table.sort_order.return_value = sort_order

    schema = MagicMock()
    source_field = MagicMock()
    source_field.name = "event_date"
    schema.find_field.return_value = source_field
    table.schema.return_value = schema

    result = get_table_sort_order(table)
    assert result == [{"column": "event_date", "direction": "asc"}]


def test_get_table_sort_order_desc():
    from pyiceberg.table.sorting import SortDirection

    table = MagicMock()
    sort_order = MagicMock()
    sort_order.is_unsorted = False

    field = MagicMock()
    field.source_id = 2
    field.direction = SortDirection.DESC
    sort_order.fields = [field]
    table.sort_order.return_value = sort_order

    schema = MagicMock()
    source_field = MagicMock()
    source_field.name = "updated_at"
    schema.find_field.return_value = source_field
    table.schema.return_value = schema

    result = get_table_sort_order(table)
    assert result == [{"column": "updated_at", "direction": "desc"}]


def test_get_table_sort_order_multiple():
    from pyiceberg.table.sorting import SortDirection

    table = MagicMock()
    sort_order = MagicMock()
    sort_order.is_unsorted = False

    f1 = MagicMock()
    f1.source_id = 1
    f1.direction = SortDirection.ASC
    f2 = MagicMock()
    f2.source_id = 2
    f2.direction = SortDirection.DESC
    sort_order.fields = [f1, f2]
    table.sort_order.return_value = sort_order

    schema = MagicMock()

    def find_field(sid):
        m = MagicMock()
        m.name = f"col_{sid}"
        return m

    schema.find_field = find_field
    table.schema.return_value = schema

    result = get_table_sort_order(table)
    assert len(result) == 2
    assert result[0] == {"column": "col_1", "direction": "asc"}
    assert result[1] == {"column": "col_2", "direction": "desc"}


# --- _serialize_change ---


def test_serialize_add_partition_field():
    change = SchemaChange(
        change_type=ChangeType.ADD_PARTITION_FIELD,
        column_name="date",
        transform="month",
        transform_param=None,
    )
    result = _serialize_change(change)
    assert result == {"change_type": "add_partition_field", "column_name": "date", "transform": "month"}


def test_serialize_add_partition_field_with_param():
    change = SchemaChange(
        change_type=ChangeType.ADD_PARTITION_FIELD,
        column_name="user_id",
        transform="bucket",
        transform_param=16,
    )
    result = _serialize_change(change)
    assert result["transform"] == "bucket"
    assert result["transform_param"] == 16


def test_serialize_drop_partition_field():
    change = SchemaChange(
        change_type=ChangeType.DROP_PARTITION_FIELD,
        column_name="old_col",
        transform="identity",
    )
    result = _serialize_change(change)
    assert result["change_type"] == "drop_partition_field"
    assert result["transform"] == "identity"


def test_serialize_set_sort_order():
    change = SchemaChange(
        change_type=ChangeType.SET_SORT_ORDER,
        column_name="__sort_order__",
        sort_fields=[{"column": "id", "direction": "asc"}],
    )
    result = _serialize_change(change)
    assert result["change_type"] == "set_sort_order"
    assert result["sort_fields"] == [{"column": "id", "direction": "asc"}]


def test_serialize_set_table_properties():
    change = SchemaChange(
        change_type=ChangeType.SET_TABLE_PROPERTIES,
        column_name="__table_properties__",
        properties={"key": "value"},
    )
    result = _serialize_change(change)
    assert result["change_type"] == "set_table_properties"
    assert result["properties"] == {"key": "value"}


def test_serialize_unset_table_properties():
    change = SchemaChange(
        change_type=ChangeType.UNSET_TABLE_PROPERTIES,
        column_name="__table_properties__",
        property_keys=["old_key", "stale_key"],
    )
    result = _serialize_change(change)
    assert result["change_type"] == "unset_table_properties"
    assert result["property_keys"] == ["old_key", "stale_key"]


def test_serialize_column_change_with_all_fields():
    change = SchemaChange(
        change_type=ChangeType.RENAME_COLUMN,
        column_name="new_name",
        previous_name="old_name",
    )
    result = _serialize_change(change)
    assert result["change_type"] == "rename_column"
    assert result["previous_name"] == "old_name"


def test_serialize_incompatible_type_change():
    change = SchemaChange(
        change_type=ChangeType.INCOMPATIBLE_TYPE_CHANGE,
        column_name="col",
        current_type="int",
        target_type="string",
        transformation="CAST(`col` AS STRING)",
    )
    result = _serialize_change(change)
    assert result["transformation"] == "CAST(`col` AS STRING)"
    assert result["current_type"] == "int"
    assert result["target_type"] == "string"


# --- write_plan ---


def test_write_plan_creates_file(tmp_path):
    changes = [
        SchemaChange(change_type=ChangeType.ADD_COLUMN, column_name="new_col", target_type="string"),
        SchemaChange(
            change_type=ChangeType.ADD_PARTITION_FIELD,
            column_name="date",
            transform="month",
        ),
    ]
    output_path = str(tmp_path / "migrations" / "silver" / "test_table" / "plan.json")
    result = write_plan(changes, output_path, "test_table", layer="silver")
    assert result == output_path
    assert os.path.exists(output_path)

    with open(output_path) as f:
        plan = json.load(f)
    assert plan["metadata"]["layer"] == "silver"
    assert plan["metadata"]["table"] == "test_table"
    assert len(plan["changes"]) == 2
    assert plan["changes"][0]["change_type"] == "add_column"
    assert plan["changes"][1]["change_type"] == "add_partition_field"
