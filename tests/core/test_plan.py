"""Unit tests for schema_evolution.plan — no AWS needed."""

from schema_evolution import ChangeType, is_compatible_type_change, normalize_type, types_match
from schema_evolution.plan import classify_changes


def test_normalize_type_aliases():
    assert normalize_type("bigint") == "long"
    assert normalize_type("int") == "integer"
    assert normalize_type("  STRING  ") == "string"
    assert normalize_type("decimal( 15 , 0 )") == "decimal(15,0)"


def test_types_match():
    assert types_match("bigint", "long")
    assert types_match("INT", "integer")
    assert types_match("decimal(15,0)", "decimal( 15, 0 )")
    assert not types_match("string", "long")


def test_is_compatible_type_change():
    assert is_compatible_type_change("integer", "long")
    assert is_compatible_type_change("float", "double")
    assert is_compatible_type_change("decimal(15,0)", "decimal(20,0)")
    assert not is_compatible_type_change("decimal(15,0)", "decimal(15,2)")
    assert not is_compatible_type_change("string", "long")
    assert not is_compatible_type_change("decimal(20,0)", "decimal(15,0)")


def test_classify_add_column():
    fields = [{"table_column": "new_col", "table_data_type": "string"}]
    schema = {"existing_col": "string"}
    changes = classify_changes(fields, schema)
    assert len(changes) == 2  # add new_col + drop existing_col
    add = [c for c in changes if c.change_type == ChangeType.ADD_COLUMN]
    assert len(add) == 1
    assert add[0].column_name == "new_col"


def test_classify_drop_column():
    fields = []
    schema = {"old_col": "string"}
    changes = classify_changes(fields, schema)
    assert len(changes) == 1
    assert changes[0].change_type == ChangeType.DROP_COLUMN
    assert changes[0].column_name == "old_col"


def test_classify_compatible_type_change():
    fields = [{"table_column": "id", "table_data_type": "decimal(20,0)"}]
    schema = {"id": "decimal(15,0)"}
    changes = classify_changes(fields, schema)
    assert len(changes) == 1
    assert changes[0].change_type == ChangeType.COMPATIBLE_TYPE_CHANGE


def test_classify_incompatible_type_change():
    fields = [{"table_column": "id", "table_data_type": "string"}]
    schema = {"id": "integer"}
    changes = classify_changes(fields, schema)
    assert len(changes) == 1
    assert changes[0].change_type == ChangeType.INCOMPATIBLE_TYPE_CHANGE


def test_classify_rename():
    fields = [{"table_column": "new_name", "table_data_type": "string", "previous_table_column": "old_name"}]
    schema = {"old_name": "string"}
    changes = classify_changes(fields, schema)
    assert len(changes) == 1
    assert changes[0].change_type == ChangeType.RENAME_COLUMN
    assert changes[0].previous_name == "old_name"


def test_classify_no_changes():
    fields = [{"table_column": "id", "table_data_type": "string"}]
    schema = {"id": "string"}
    changes = classify_changes(fields, schema)
    assert len(changes) == 0


def test_classify_make_column_not_null():
    """Field with not_null=True on a currently-nullable column emits a nullability change."""
    fields = [{"table_column": "id", "table_data_type": "string", "not_null": True}]
    schema = {"id": "string"}
    table_required = {"id": False}
    changes = classify_changes(fields, schema, table_required=table_required)
    assert len(changes) == 1
    assert changes[0].change_type == ChangeType.SET_COLUMN_NULLABILITY
    assert changes[0].column_name == "id"
    assert changes[0].target_not_null is True


def test_classify_make_column_nullable():
    """Field without not_null on a currently-required column emits a nullability change."""
    fields = [{"table_column": "id", "table_data_type": "string"}]
    schema = {"id": "string"}
    table_required = {"id": True}
    changes = classify_changes(fields, schema, table_required=table_required)
    assert len(changes) == 1
    assert changes[0].change_type == ChangeType.SET_COLUMN_NULLABILITY
    assert changes[0].target_not_null is False


def test_classify_nullability_no_change_when_matching():
    """No nullability change when not_null matches the table's required state."""
    fields = [{"table_column": "id", "table_data_type": "string", "not_null": True}]
    schema = {"id": "string"}
    table_required = {"id": True}
    changes = classify_changes(fields, schema, table_required=table_required)
    assert len(changes) == 0


def test_classify_nullability_ignored_when_required_map_absent():
    """Without table_required, nullability is not diffed (backward compatible)."""
    fields = [{"table_column": "id", "table_data_type": "string", "not_null": True}]
    schema = {"id": "string"}
    changes = classify_changes(fields, schema)
    assert len(changes) == 0


def test_classify_type_and_nullability_change_together():
    """A column can emit both a type change and a nullability change."""
    fields = [{"table_column": "id", "table_data_type": "decimal(20,0)", "not_null": True}]
    schema = {"id": "decimal(15,0)"}
    table_required = {"id": False}
    changes = classify_changes(fields, schema, table_required=table_required)
    types = {c.change_type for c in changes}
    assert ChangeType.COMPATIBLE_TYPE_CHANGE in types
    assert ChangeType.SET_COLUMN_NULLABILITY in types
    assert len(changes) == 2
