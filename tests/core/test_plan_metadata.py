"""Unit tests for schema evolution metadata parsing and diffing functions."""

import pytest

from schema_evolution import ChangeType
from schema_evolution.plan import (
    correlate_repartition_with_type_change,
    diff_partition_spec,
    diff_sort_order,
    diff_table_properties,
    parse_partition_spec,
    parse_sort_order,
    parse_table_properties,
)

# --- parse_partition_spec ---


def test_parse_partition_spec_month_transform():
    yaml_data = {"partition_strategy": {"partition_by": [{"column": "event_date", "transform": "month"}]}}
    result = parse_partition_spec(yaml_data)
    assert result == [{"column": "event_date", "transform": "month", "transform_param": None, "rebuild": None}]


def test_parse_partition_spec_bucket_transform():
    yaml_data = {"partition_strategy": {"partition_by": [{"column": "user_id", "transform": "bucket", "param": 16}]}}
    result = parse_partition_spec(yaml_data)
    assert result == [{"column": "user_id", "transform": "bucket", "transform_param": 16, "rebuild": None}]


def test_parse_partition_spec_truncate_transform():
    yaml_data = {"partition_strategy": {"partition_by": [{"column": "city", "transform": "truncate", "param": 3}]}}
    result = parse_partition_spec(yaml_data)
    assert result == [{"column": "city", "transform": "truncate", "transform_param": 3, "rebuild": None}]


def test_parse_partition_spec_identity():
    yaml_data = {"partition_strategy": {"partition_by": [{"column": "region", "transform": "identity"}]}}
    result = parse_partition_spec(yaml_data)
    assert result == [{"column": "region", "transform": "identity", "transform_param": None, "rebuild": None}]


def test_parse_partition_spec_all_time_transforms():
    yaml_data = {
        "partition_strategy": {
            "partition_by": [
                {"column": "ts", "transform": "year"},
                {"column": "ts", "transform": "month"},
                {"column": "ts", "transform": "day"},
                {"column": "ts", "transform": "hour"},
            ]
        }
    }
    result = parse_partition_spec(yaml_data)
    assert len(result) == 4
    assert all(r["transform_param"] is None for r in result)


def test_parse_partition_spec_empty_when_absent():
    assert parse_partition_spec({}) == []
    assert parse_partition_spec({"partition_strategy": {}}) == []
    assert parse_partition_spec({"partition_strategy": {"partition_by": []}}) == []


def test_parse_partition_spec_invalid_transform_raises():
    yaml_data = {"partition_strategy": {"partition_by": [{"column": "x", "transform": "invalid"}]}}
    with pytest.raises(ValueError, match="Invalid partition transform 'invalid'"):
        parse_partition_spec(yaml_data)


def test_parse_partition_spec_bucket_missing_param_raises():
    yaml_data = {"partition_strategy": {"partition_by": [{"column": "x", "transform": "bucket"}]}}
    with pytest.raises(ValueError, match="requires a 'param' value"):
        parse_partition_spec(yaml_data)


def test_parse_partition_spec_case_insensitive():
    yaml_data = {"partition_strategy": {"partition_by": [{"column": "X", "transform": "MONTH"}]}}
    result = parse_partition_spec(yaml_data)
    assert result[0]["transform"] == "month"


def test_parse_partition_spec_bucket_shorthand():
    """bucket[64] shorthand notation should be parsed as transform=bucket, param=64."""
    yaml_data = {"partition_strategy": {"partition_by": [{"column": "ENTRY_HEADER_ID", "transform": "bucket[64]"}]}}
    result = parse_partition_spec(yaml_data)
    assert result == [{"column": "ENTRY_HEADER_ID", "transform": "bucket", "transform_param": 64, "rebuild": None}]


def test_parse_partition_spec_truncate_shorthand():
    """truncate[10] shorthand notation should be parsed as transform=truncate, param=10."""
    yaml_data = {"partition_strategy": {"partition_by": [{"column": "name", "transform": "truncate[10]"}]}}
    result = parse_partition_spec(yaml_data)
    assert result == [{"column": "name", "transform": "truncate", "transform_param": 10, "rebuild": None}]


def test_parse_partition_spec_bucket_shorthand_case_insensitive():
    """BUCKET[32] shorthand should be normalised to bucket, param=32."""
    yaml_data = {"partition_strategy": {"partition_by": [{"column": "id", "transform": "BUCKET[32]"}]}}
    result = parse_partition_spec(yaml_data)
    assert result[0]["transform"] == "bucket"
    assert result[0]["transform_param"] == 32


# --- diff_partition_spec ---


def test_diff_partition_spec_no_changes():
    spec = [{"column": "date", "transform": "month", "transform_param": None}]
    assert diff_partition_spec(spec, spec) == []


def test_diff_partition_spec_add_field():
    desired = [{"column": "date", "transform": "month", "transform_param": None}]
    actual = []
    changes = diff_partition_spec(desired, actual)
    assert len(changes) == 1
    assert changes[0].change_type == ChangeType.ADD_PARTITION_FIELD
    assert changes[0].column_name == "date"
    assert changes[0].transform == "month"


def test_diff_partition_spec_drop_field():
    desired = []
    actual = [{"column": "old", "transform": "identity", "transform_param": None}]
    changes = diff_partition_spec(desired, actual)
    assert len(changes) == 1
    assert changes[0].change_type == ChangeType.DROP_PARTITION_FIELD


def test_diff_partition_spec_transform_change():
    desired = [{"column": "date", "transform": "day", "transform_param": None}]
    actual = [{"column": "date", "transform": "month", "transform_param": None}]
    changes = diff_partition_spec(desired, actual)
    assert len(changes) == 2  # drop old + add new
    types = {c.change_type for c in changes}
    assert ChangeType.ADD_PARTITION_FIELD in types
    assert ChangeType.DROP_PARTITION_FIELD in types


def test_diff_partition_spec_case_insensitive():
    desired = [{"column": "DATE", "transform": "month", "transform_param": None}]
    actual = [{"column": "date", "transform": "month", "transform_param": None}]
    assert diff_partition_spec(desired, actual) == []


# --- parse_sort_order ---


def test_parse_sort_order_basic():
    yaml_data = {"partition_strategy": {"sort_order": [{"column": "id", "direction": "asc"}]}}
    result = parse_sort_order(yaml_data)
    assert result == [{"column": "id", "direction": "asc"}]


def test_parse_sort_order_multiple():
    yaml_data = {
        "partition_strategy": {
            "sort_order": [
                {"column": "date", "direction": "desc"},
                {"column": "id", "direction": "asc"},
            ]
        }
    }
    result = parse_sort_order(yaml_data)
    assert len(result) == 2
    assert result[0]["direction"] == "desc"
    assert result[1]["direction"] == "asc"


def test_parse_sort_order_empty_when_absent():
    assert parse_sort_order({}) == []
    assert parse_sort_order({"partition_strategy": {}}) == []


def test_parse_sort_order_invalid_direction_raises():
    yaml_data = {"partition_strategy": {"sort_order": [{"column": "x", "direction": "up"}]}}
    with pytest.raises(ValueError, match="Invalid sort direction 'up'"):
        parse_sort_order(yaml_data)


def test_parse_sort_order_rebuild_flag():
    yaml_data = {"partition_strategy": {"sort_order": [{"column": "id", "direction": "asc", "rebuild": True}]}}
    result = parse_sort_order(yaml_data)
    assert result == [{"column": "id", "direction": "asc", "rebuild": True}]


# --- diff_sort_order ---


def test_diff_sort_order_no_change():
    sort = [{"column": "id", "direction": "asc"}]
    assert diff_sort_order(sort, sort) == []


def test_diff_sort_order_different():
    desired = [{"column": "id", "direction": "asc"}]
    actual = [{"column": "id", "direction": "desc"}]
    changes = diff_sort_order(desired, actual)
    assert len(changes) == 1
    assert changes[0].change_type == ChangeType.SET_SORT_ORDER
    assert changes[0].sort_fields == [{"column": "id", "direction": "asc"}]
    assert changes[0].rebuild is True


def test_diff_sort_order_rebuild_false_is_metadata_only():
    desired = [{"column": "id", "direction": "asc", "rebuild": False}]
    actual = [{"column": "id", "direction": "desc"}]
    changes = diff_sort_order(desired, actual)
    assert changes[0].rebuild is False


def test_diff_sort_order_empty_desired():
    desired = []
    actual = [{"column": "id", "direction": "asc"}]
    changes = diff_sort_order(desired, actual)
    assert len(changes) == 1
    assert changes[0].sort_fields == []


def test_diff_sort_order_case_insensitive():
    desired = [{"column": "ID", "direction": "ASC"}]
    actual = [{"column": "id", "direction": "asc"}]
    assert diff_sort_order(desired, actual) == []


# --- parse_table_properties ---


def test_parse_table_properties_basic():
    yaml_data = {"table_properties": {"write.spark.accept-any-schema": "true", "key": "value"}}
    result = parse_table_properties(yaml_data)
    assert result == {"write.spark.accept-any-schema": "true", "key": "value"}


def test_parse_table_properties_coerces_values():
    yaml_data = {"table_properties": {"num": 42, "flag": True, "ratio": 3.14}}
    result = parse_table_properties(yaml_data)
    assert result == {"num": "42", "flag": "True", "ratio": "3.14"}


def test_parse_table_properties_empty_when_absent():
    assert parse_table_properties({}) == {}
    assert parse_table_properties({"table_properties": {}}) == {}


# --- diff_table_properties ---


def test_diff_table_properties_no_change():
    props = {"key": "value"}
    assert diff_table_properties(props, props) == []


def test_diff_table_properties_new_property():
    desired = {"new_key": "new_value"}
    actual = {}
    changes = diff_table_properties(desired, actual)
    assert len(changes) == 1
    assert changes[0].change_type == ChangeType.SET_TABLE_PROPERTIES
    assert changes[0].properties == {"new_key": "new_value"}


def test_diff_table_properties_changed_value():
    desired = {"key": "new"}
    actual = {"key": "old"}
    changes = diff_table_properties(desired, actual)
    assert len(changes) == 1
    assert changes[0].properties == {"key": "new"}


def test_diff_table_properties_ignores_extra_actual():
    desired = {"key": "val"}
    actual = {"key": "val", "extra": "ignored"}
    assert diff_table_properties(desired, actual) == []


# --- get_table_partition_spec / get_table_sort_order (PyIceberg extraction) ---

from unittest.mock import MagicMock  # noqa: E402

from schema_evolution.plan import (  # noqa: E402
    _serialize_change,
    get_table_partition_spec,
    get_table_sort_order,
)


def _named(name):
    """A mock whose `.name` attribute is set (MagicMock(name=...) is special)."""
    m = MagicMock()
    m.name = name
    return m


def _mock_partition_table(fields):
    """fields: list of (column_name, transform_instance)."""
    table = MagicMock()
    spec = MagicMock()
    pfs = []
    names = {}
    for i, (col, tr) in enumerate(fields, start=1):
        pf = MagicMock()
        pf.source_id = i
        pf.transform = tr
        pfs.append(pf)
        names[i] = col
    spec.fields = pfs
    table.spec.return_value = spec
    schema = MagicMock()
    schema.find_field.side_effect = lambda sid: _named(names[sid])
    table.schema.return_value = schema
    return table


def test_get_table_partition_spec_all_transforms():
    from pyiceberg.transforms import (
        BucketTransform,
        DayTransform,
        HourTransform,
        IdentityTransform,
        MonthTransform,
        TruncateTransform,
        YearTransform,
    )

    table = _mock_partition_table(
        [
            ("a", IdentityTransform()),
            ("b", YearTransform()),
            ("c", MonthTransform()),
            ("d", DayTransform()),
            ("e", HourTransform()),
            ("f", BucketTransform(16)),
            ("g", TruncateTransform(4)),
        ]
    )
    result = get_table_partition_spec(table)
    assert result == [
        {"column": "a", "transform": "identity", "transform_param": None},
        {"column": "b", "transform": "year", "transform_param": None},
        {"column": "c", "transform": "month", "transform_param": None},
        {"column": "d", "transform": "day", "transform_param": None},
        {"column": "e", "transform": "hour", "transform_param": None},
        {"column": "f", "transform": "bucket", "transform_param": 16},
        {"column": "g", "transform": "truncate", "transform_param": 4},
    ]


def test_get_table_partition_spec_unknown_transform_falls_back_to_str():
    from pyiceberg.transforms import VoidTransform

    table = _mock_partition_table([("h", VoidTransform())])
    result = get_table_partition_spec(table)
    assert result[0]["column"] == "h"
    assert result[0]["transform_param"] is None
    assert result[0]["transform"]  # non-empty string fallback


def test_get_table_partition_spec_empty():
    table = _mock_partition_table([])
    assert get_table_partition_spec(table) == []


def _mock_sorted_table(sort_fields):
    """sort_fields: list of (column_name, SortDirection)."""
    table = MagicMock()
    so = MagicMock()
    so.is_unsorted = len(sort_fields) == 0
    fs = []
    names = {}
    for i, (col, direction) in enumerate(sort_fields, start=1):
        f = MagicMock()
        f.source_id = i
        f.direction = direction
        fs.append(f)
        names[i] = col
    so.fields = fs
    table.sort_order.return_value = so
    schema = MagicMock()
    schema.find_field.side_effect = lambda sid: _named(names[sid])
    table.schema.return_value = schema
    return table


def test_get_table_sort_order_asc_and_desc():
    from pyiceberg.table.sorting import SortDirection

    table = _mock_sorted_table([("id", SortDirection.ASC), ("ts", SortDirection.DESC)])
    result = get_table_sort_order(table)
    assert result == [
        {"column": "id", "direction": "asc"},
        {"column": "ts", "direction": "desc"},
    ]


def test_get_table_sort_order_unsorted_returns_empty():
    table = _mock_sorted_table([])
    assert get_table_sort_order(table) == []


# --- _serialize_change for metadata change types ---


def test_serialize_change_partition_and_sort_and_props():
    from schema_evolution import ChangeType, SchemaChange

    add = _serialize_change(
        SchemaChange(ChangeType.ADD_PARTITION_FIELD, "user_id", transform="bucket", transform_param=16)
    )
    assert add == {
        "change_type": "add_partition_field",
        "column_name": "user_id",
        "transform": "bucket",
        "transform_param": 16,
    }

    sort = _serialize_change(
        SchemaChange(ChangeType.SET_SORT_ORDER, "__sort_order__", sort_fields=[{"column": "id", "direction": "asc"}])
    )
    assert sort["change_type"] == "set_sort_order"
    assert sort["sort_fields"] == [{"column": "id", "direction": "asc"}]

    set_props = _serialize_change(
        SchemaChange(ChangeType.SET_TABLE_PROPERTIES, "__table_properties__", properties={"k": "v"})
    )
    assert set_props["properties"] == {"k": "v"}

    unset_props = _serialize_change(
        SchemaChange(ChangeType.UNSET_TABLE_PROPERTIES, "__table_properties__", property_keys=["k"])
    )
    assert unset_props["property_keys"] == ["k"]


# --- correlate_repartition_with_type_change ---


def _incompat(col, current="date", target="timestamp"):
    from schema_evolution import ChangeType, SchemaChange

    return SchemaChange(
        ChangeType.INCOMPATIBLE_TYPE_CHANGE,
        col,
        current_type=current,
        target_type=target,
        transformation=f"CAST({col} AS TIMESTAMP)",
    )


def _add_part(col, transform="month", param=None):
    from schema_evolution import ChangeType, SchemaChange

    return SchemaChange(ChangeType.ADD_PARTITION_FIELD, col, transform=transform, transform_param=param)


def _drop_part(col, transform="identity", param=None):
    from schema_evolution import ChangeType, SchemaChange

    return SchemaChange(ChangeType.DROP_PARTITION_FIELD, col, transform=transform, transform_param=param)


def test_correlate_folds_repartition_into_type_change():
    """date->timestamp on period_end_date + partition diff that adds
    month(period_end_date) and drops identity(period_key) =>
    the type change gets new_partition:month and the add/drop are removed.
    """
    type_changes = [_incompat("period_end_date")]
    partition_changes = [
        _drop_part("period_key"),
        _add_part("period_end_date", "month"),
    ]
    desired = [{"column": "period_end_date", "transform": "month", "transform_param": None, "rebuild": None}]

    tcs, pcs = correlate_repartition_with_type_change(type_changes, partition_changes, desired)

    assert tcs[0].new_partition == {"transform": "month", "param": None}
    assert pcs == []  # add + drop both removed (rebuild replaces partitioning)


def test_correlate_metadata_only_preserves_unrelated_drop():
    """rebuild=false correlation touches only its own column: an unrelated
    drop_partition_field must survive (not wiped like the rebuild case)."""
    type_changes = [_incompat("period_end_date")]
    partition_changes = [
        _drop_part("region"),
        _add_part("period_end_date", "month"),
    ]
    desired = [{"column": "period_end_date", "transform": "month", "transform_param": None, "rebuild": False}]

    tcs, pcs = correlate_repartition_with_type_change(type_changes, partition_changes, desired)

    assert tcs[0].new_partition == {"transform": "month", "param": None, "rebuild": False}
    # add for correlated col removed; unrelated region drop preserved.
    assert len(pcs) == 1
    assert pcs[0].column_name.lower() == "region"


def test_correlate_respects_rebuild_false_flag():
    type_changes = [_incompat("period_end_date")]
    partition_changes = [_add_part("period_end_date", "month")]
    desired = [{"column": "period_end_date", "transform": "month", "transform_param": None, "rebuild": False}]

    tcs, pcs = correlate_repartition_with_type_change(type_changes, partition_changes, desired)

    assert tcs[0].new_partition == {"transform": "month", "param": None, "rebuild": False}
    assert pcs == []


def test_correlate_noop_when_type_change_column_not_partitioned():
    """A partition change on a different column than the type change is left as a
    standalone metadata-only add/drop (not folded)."""
    from schema_evolution import ChangeType

    type_changes = [_incompat("some_other_col")]
    partition_changes = [_add_part("region", "identity")]
    desired = [{"column": "region", "transform": "identity", "transform_param": None, "rebuild": None}]

    tcs, pcs = correlate_repartition_with_type_change(type_changes, partition_changes, desired)

    assert tcs[0].new_partition is None
    assert [c.change_type for c in pcs] == [ChangeType.ADD_PARTITION_FIELD]


def test_correlate_noop_without_partition_change():
    """A pure type change with no partition diff is untouched."""
    type_changes = [_incompat("some_date")]
    tcs, pcs = correlate_repartition_with_type_change(type_changes, [], [])
    assert tcs[0].new_partition is None
    assert pcs == []


def test_correlate_type_change_on_unchanged_partition_column():
    """date->timestamp on a column that is already partitioned by month(col) and
    stays that way: no partition diff, but the backfill still needs
    new_partition, or apply refuses to touch a partition-source column."""
    type_changes = [_incompat("signup_date")]
    desired = [{"column": "signup_date", "transform": "month", "transform_param": None, "rebuild": None}]

    tcs, pcs = correlate_repartition_with_type_change(type_changes, [], desired)

    assert tcs[0].new_partition == {"transform": "month", "param": None}
    assert pcs == []
