"""Unit tests for PyIceberg metadata extractors and plan serialization.

Covers get_table_partition_spec, get_table_sort_order, get_table_properties,
_serialize_change, and write_plan (mocked PyIceberg objects + tmp files).
"""

import json
from unittest.mock import MagicMock

from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    TruncateTransform,
    YearTransform,
)

from schema_evolution import ChangeType, SchemaChange
from schema_evolution.plan import (
    get_table_partition_spec,
    get_table_properties,
    get_table_sort_order,
    write_plan,
)


def _mock_table_with_partition_fields(fields):
    """fields: list of (source_id, column_name, transform_instance)."""
    table = MagicMock()

    spec_fields = []
    id_to_name = {}
    for source_id, name, transform in fields:
        pf = MagicMock()
        pf.source_id = source_id
        pf.transform = transform
        spec_fields.append(pf)
        id_to_name[source_id] = name
    table.spec.return_value.fields = spec_fields

    def _find_field(source_id):
        sf = MagicMock()
        sf.name = id_to_name[source_id]
        return sf

    table.schema.return_value.find_field.side_effect = _find_field
    return table


# --- get_table_partition_spec ---


def test_get_table_partition_spec_all_transforms():
    table = _mock_table_with_partition_fields(
        [
            (1, "region", IdentityTransform()),
            (2, "created_at", YearTransform()),
            (3, "created_at", MonthTransform()),
            (4, "created_at", DayTransform()),
            (5, "created_at", HourTransform()),
            (6, "user_id", BucketTransform(16)),
            (7, "name", TruncateTransform(10)),
        ]
    )
    result = get_table_partition_spec(table)
    assert result == [
        {"column": "region", "transform": "identity", "transform_param": None},
        {"column": "created_at", "transform": "year", "transform_param": None},
        {"column": "created_at", "transform": "month", "transform_param": None},
        {"column": "created_at", "transform": "day", "transform_param": None},
        {"column": "created_at", "transform": "hour", "transform_param": None},
        {"column": "user_id", "transform": "bucket", "transform_param": 16},
        {"column": "name", "transform": "truncate", "transform_param": 10},
    ]


def test_get_table_partition_spec_empty():
    table = _mock_table_with_partition_fields([])
    assert get_table_partition_spec(table) == []


# --- get_table_sort_order ---


def test_get_table_sort_order_unsorted():
    table = MagicMock()
    table.sort_order.return_value.is_unsorted = True
    assert get_table_sort_order(table) == []


def test_get_table_sort_order_asc_and_desc():
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

    id_to_name = {1: "period_key", 2: "event_ts"}

    def _find_field(source_id):
        sf = MagicMock()
        sf.name = id_to_name[source_id]
        return sf

    table.schema.return_value.find_field.side_effect = _find_field

    result = get_table_sort_order(table)
    assert result == [
        {"column": "period_key", "direction": "asc"},
        {"column": "event_ts", "direction": "desc"},
    ]


# --- get_table_properties ---


def test_get_table_properties_returns_dict():
    table = MagicMock()
    table.properties = {"write.spark.accept-any-schema": "true", "k": "v"}
    assert get_table_properties(table) == {"write.spark.accept-any-schema": "true", "k": "v"}


# --- write_plan / _serialize_change ---


def test_write_plan_serializes_all_change_types(tmp_path):
    changes = [
        SchemaChange(ChangeType.ADD_COLUMN, "new_col", target_type="string"),
        SchemaChange(ChangeType.DROP_COLUMN, "old_col"),
        SchemaChange(ChangeType.RENAME_COLUMN, "new_name", previous_name="old_name"),
        SchemaChange(ChangeType.COMPATIBLE_TYPE_CHANGE, "id", current_type="int", target_type="long"),
        SchemaChange(ChangeType.ADD_PARTITION_FIELD, "user_id", transform="bucket", transform_param=16),
        SchemaChange(ChangeType.DROP_PARTITION_FIELD, "region", transform="identity"),
        SchemaChange(
            ChangeType.SET_SORT_ORDER,
            "__sort_order__",
            sort_fields=[{"column": "id", "direction": "asc"}],
        ),
        SchemaChange(
            ChangeType.SET_TABLE_PROPERTIES,
            "__table_properties__",
            properties={"k": "v"},
        ),
        SchemaChange(
            ChangeType.UNSET_TABLE_PROPERTIES,
            "__table_properties__",
            property_keys=["old.key"],
        ),
    ]

    output_path = str(tmp_path / "migrations" / "gold" / "tbl" / "plan.json")
    returned = write_plan(changes, output_path, table="tbl", layer="gold")
    assert returned == output_path

    with open(output_path) as f:
        plan = json.load(f)

    assert plan["metadata"] == {"layer": "gold", "table": "tbl"}
    serialized = plan["changes"]
    assert len(serialized) == len(changes)

    by_type = {c["change_type"]: c for c in serialized}

    assert by_type["add_column"]["target_type"] == "string"
    assert by_type["rename_column"]["previous_name"] == "old_name"
    assert by_type["compatible_type_change"]["current_type"] == "int"
    assert by_type["add_partition_field"]["transform"] == "bucket"
    assert by_type["add_partition_field"]["transform_param"] == 16
    assert by_type["set_sort_order"]["sort_fields"] == [{"column": "id", "direction": "asc"}]
    assert by_type["set_table_properties"]["properties"] == {"k": "v"}
    assert by_type["unset_table_properties"]["property_keys"] == ["old.key"]
