"""Schema Evolution Plan Generator — compares config YAML vs Iceberg table schema."""

import argparse
import json
import logging
import os
from datetime import UTC, datetime

from schema_evolution import (
    LAYER_METADATA_COLUMNS,
    PARAMETERIZED_TRANSFORMS,
    VALID_TRANSFORMS,
    ChangeType,
    SchemaChange,
    is_compatible_type_change,
    load_catalog,
    parse_previous_names,
    types_match,
)
from schema_evolution.config_sync import _normalize_field, filter_fields_by_environment

logger = logging.getLogger(__name__)


def get_table_schema(catalog, namespace: str, table_name: str) -> dict:
    """Get Iceberg table schema as {col_lower: type_string}."""
    fqn = f"{namespace}.{table_name}"
    table = catalog.load_table(fqn)
    return {f.name.lower(): str(f.field_type).lower() for f in table.schema().fields}


def classify_changes(  # noqa: C901
    fields: list, table_schema: dict, layer: str = "", table_required: dict[str, bool] | None = None
) -> list[SchemaChange]:
    """Compare config fields against table schema and classify changes.

    Args:
        fields: Config field definitions from the schema YAML.
        table_schema: Live table schema as {col_lower: type_string}.
        layer: Data layer (bronze/silver/gold) for metadata column exclusion.
        table_required: Optional {col_lower: required_bool} map of the live
            table's per-column nullability. When provided, a mismatch between a
            field's ``not_null`` flag and the table's required state emits a
            SET_COLUMN_NULLABILITY change. When None, nullability is not diffed
            (backward-compatible behavior).

    Returns:
        List of SchemaChange operations.
    """
    changes = []
    table_cols = {k.lower(): v for k, v in table_schema.items()}
    table_required = {k.lower(): v for k, v in (table_required or {}).items()}
    accounted: set[str] = set()

    for fld in fields:
        col = fld["table_column"]
        target_type = fld["table_data_type"]
        previous_names = parse_previous_names(fld.get("previous_table_column"))
        col_lower = col.lower()

        for prev in previous_names:
            if prev.lower() in table_cols:
                accounted.add(prev.lower())

        if col_lower in table_cols:
            accounted.add(col_lower)
            current_type = table_cols[col_lower]
            if not types_match(current_type, target_type):
                if is_compatible_type_change(current_type, target_type):
                    changes.append(
                        SchemaChange(
                            ChangeType.COMPATIBLE_TYPE_CHANGE, col, current_type=current_type, target_type=target_type
                        )
                    )
                else:
                    changes.append(
                        SchemaChange(
                            ChangeType.INCOMPATIBLE_TYPE_CHANGE,
                            col,
                            current_type=current_type,
                            target_type=target_type,
                            transformation=fld.get("transformation"),
                        )
                    )
            # Nullability diff (only when the live required state is known)
            if col_lower in table_required:
                desired_not_null = bool(fld.get("not_null", False))
                current_not_null = bool(table_required[col_lower])
                if desired_not_null != current_not_null:
                    changes.append(
                        SchemaChange(
                            ChangeType.SET_COLUMN_NULLABILITY,
                            col,
                            target_not_null=desired_not_null,
                        )
                    )
        elif previous_names:
            found = next((p for p in previous_names if p.lower() in table_cols), None)
            if found:
                changes.append(SchemaChange(ChangeType.RENAME_COLUMN, col, previous_name=found))
            else:
                changes.append(SchemaChange(ChangeType.ADD_COLUMN, col, target_type=target_type))
        else:
            changes.append(SchemaChange(ChangeType.ADD_COLUMN, col, target_type=target_type))

    config_cols = {f["table_column"].lower() for f in fields}
    metadata_cols = {c.lower() for c in LAYER_METADATA_COLUMNS.get(layer, set())}
    for tc in table_cols:
        if tc not in accounted and tc not in config_cols:
            if tc in metadata_cols:
                continue
            changes.append(SchemaChange(ChangeType.DROP_COLUMN, tc))

    return changes


def parse_partition_spec(yaml_data: dict) -> list[dict]:
    """Parse partition_strategy.partition_by from YAML into field descriptors.

    Returns a list of dicts with keys: column (str), transform (str), transform_param (int | None).
    Raises ValueError for unrecognized transforms or missing params on parameterized transforms.
    """
    partition_strategy = yaml_data.get("partition_strategy", {})
    if not partition_strategy:
        return []
    partition_by = partition_strategy.get("partition_by", [])
    if not partition_by:
        return []

    result = []
    for entry in partition_by:
        column = entry["column"]
        transform = entry.get("transform", "identity").lower()
        param = entry.get("param")

        # Support shorthand notation e.g. "bucket[64]" → transform=bucket, param=64
        if "[" in transform:
            import contextlib

            bracket_pos = transform.index("[")
            with contextlib.suppress(ValueError, IndexError):
                param = int(transform[bracket_pos + 1 : transform.index("]")])
            transform = transform[:bracket_pos]

        if transform not in VALID_TRANSFORMS:
            raise ValueError(
                f"Invalid partition transform '{transform}' for column '{column}'. "
                f"Supported transforms: {sorted(VALID_TRANSFORMS)}"
            )

        if transform in PARAMETERIZED_TRANSFORMS and param is None:
            raise ValueError(f"Partition transform '{transform}' for column '{column}' requires a 'param' value.")

        result.append(
            {
                "column": column,
                "transform": transform,
                "transform_param": int(param) if param is not None else None,
                # Optional: rebuild=false requests a metadata-only re-partition
                # (no data rewrite) when this partition change is correlated with a
                # type change on the same column. Default (unset) => physical rebuild.
                "rebuild": entry.get("rebuild"),
            }
        )

    return result


def get_table_partition_spec(table) -> list[dict]:
    """Extract current partition spec from a loaded PyIceberg table.

    Returns same format as parse_partition_spec output: list of dicts with
    column, transform, transform_param keys.
    """
    from pyiceberg.transforms import (
        BucketTransform,
        DayTransform,
        HourTransform,
        IdentityTransform,
        MonthTransform,
        TruncateTransform,
        YearTransform,
    )

    spec = table.spec()
    schema = table.schema()
    result = []

    for field in spec.fields:
        source_field = schema.find_field(field.source_id)
        column_name = source_field.name

        transform = field.transform
        if isinstance(transform, IdentityTransform):
            t_name, t_param = "identity", None
        elif isinstance(transform, YearTransform):
            t_name, t_param = "year", None
        elif isinstance(transform, MonthTransform):
            t_name, t_param = "month", None
        elif isinstance(transform, DayTransform):
            t_name, t_param = "day", None
        elif isinstance(transform, HourTransform):
            t_name, t_param = "hour", None
        elif isinstance(transform, BucketTransform):
            t_name, t_param = "bucket", transform.num_buckets
        elif isinstance(transform, TruncateTransform):
            t_name, t_param = "truncate", transform.width
        else:
            t_name, t_param = str(transform), None

        result.append({"column": column_name, "transform": t_name, "transform_param": t_param})

    return result


def diff_partition_spec(desired: list[dict], actual: list[dict]) -> list[SchemaChange]:
    """Produce add/drop partition field changes by comparing desired vs actual.

    Compares fields by (column_name_upper, transform, transform_param) tuple.
    """

    def _key(field: dict) -> tuple:
        return (field["column"].upper(), field["transform"], field.get("transform_param"))

    desired_set = {_key(f) for f in desired}
    actual_set = {_key(f) for f in actual}

    changes = []

    # Fields to drop (in actual but not in desired)
    for field in actual:
        if _key(field) not in desired_set:
            changes.append(
                SchemaChange(
                    change_type=ChangeType.DROP_PARTITION_FIELD,
                    column_name=field["column"],
                    transform=field["transform"],
                    transform_param=field.get("transform_param"),
                )
            )

    # Fields to add (in desired but not in actual)
    for field in desired:
        if _key(field) not in actual_set:
            changes.append(
                SchemaChange(
                    change_type=ChangeType.ADD_PARTITION_FIELD,
                    column_name=field["column"],
                    transform=field["transform"],
                    transform_param=field.get("transform_param"),
                )
            )

    return changes


def correlate_repartition_with_type_change(
    type_changes: list[SchemaChange],
    partition_changes: list[SchemaChange],
    desired_partition: list[dict],
) -> tuple[list[SchemaChange], list[SchemaChange]]:
    """Fold a config-driven re-partition into a same-column type change.

    When a column has an ``incompatible_type_change`` AND the desired partition
    spec uses that column (whether the partition diff adds it or it is already a
    partition field), the backfill must drop and re-add the partition field on the
    new column, so the type change needs a ``new_partition`` instruction. When the
    partition diff adds the field, the re-partition must also physically re-cluster
    the existing data. Iceberg partition evolution (the standalone
    ``add_partition_field`` / ``drop_partition_field`` changes) is metadata-only
    and does not rewrite data, so instead we attach a ``new_partition`` instruction
    to the type change (which routes to the rebuild/CTAS path) and drop the now
    redundant add/drop partition changes for that column.

    A desired partition entry may set ``rebuild: false`` to keep the metadata-only
    behavior (no data rewrite); in that case ``new_partition.rebuild`` is set to
    False and the backfill applies a metadata-only partition evolution.

    Returns the (possibly modified) ``(type_changes, partition_changes)``.
    """
    desired_by_col = {d["column"].lower(): d for d in desired_partition}

    correlated_cols: set[str] = set()
    for tc in type_changes:
        if tc.change_type != ChangeType.INCOMPATIBLE_TYPE_CHANGE:
            continue
        col = tc.column_name.lower()
        # Correlate whether the partition field on this column is being added
        # or already exists unchanged: either way the column backs a partition
        # after the change, so the backfill needs new_partition to re-add it.
        if col not in desired_by_col:
            continue
        desired = desired_by_col[col]
        new_partition: dict = {"transform": desired["transform"], "param": desired.get("transform_param")}
        if desired.get("rebuild") is False:
            new_partition["rebuild"] = False
        tc.new_partition = new_partition
        correlated_cols.add(col)

    if not correlated_cols:
        return type_changes, partition_changes

    # A correlated change that triggers a full rebuild replaces the table's
    # partitioning wholesale (rebuild_table runs with replace_partitioning=True),
    # so every standalone drop_partition_field is superseded and must not be
    # applied as separate metadata DDL. A metadata-only correlation
    # (new_partition.rebuild=false) touches only its own column, so unrelated
    # partition changes must survive.
    any_rebuild = any(
        tc.new_partition and tc.new_partition.get("rebuild") is not False
        for tc in type_changes
        if tc.column_name.lower() in correlated_cols
    )
    remaining_partition_changes = [
        c
        for c in partition_changes
        if not (
            (c.change_type == ChangeType.ADD_PARTITION_FIELD and c.column_name.lower() in correlated_cols)
            or (c.change_type == ChangeType.DROP_PARTITION_FIELD and c.column_name.lower() in correlated_cols)
            or (any_rebuild and c.change_type == ChangeType.DROP_PARTITION_FIELD)
        )
    ]
    return type_changes, remaining_partition_changes


def parse_sort_order(yaml_data: dict) -> list[dict]:
    """Parse partition_strategy.sort_order from YAML into sort field descriptors.

    Returns list of dicts with keys: column (str), direction (str: "asc" or "desc").
    A per-field ``rebuild`` flag is preserved so callers can decide whether the
    new sort order must be materialized by a table rebuild. Raises ValueError for
    invalid direction values. Returns empty list when section is absent.
    """
    partition_strategy = yaml_data.get("partition_strategy", {})
    if not partition_strategy:
        return []
    sort_order = partition_strategy.get("sort_order", [])
    if not sort_order:
        return []

    valid_directions = {"asc", "desc"}
    result = []
    for entry in sort_order:
        column = entry["column"]
        direction = entry["direction"].lower()
        if direction not in valid_directions:
            raise ValueError(f"Invalid sort direction '{direction}' for column '{column}'. Valid directions: asc, desc")
        parsed = {"column": column, "direction": direction}
        if "rebuild" in entry:
            parsed["rebuild"] = bool(entry["rebuild"])
        result.append(parsed)

    return result


def get_table_sort_order(table) -> list[dict]:
    """Extract current sort order from a loaded PyIceberg table.

    Returns list of dicts with keys: column (str), direction (str: "asc" or "desc").
    Returns empty list if table has no sort order (unsorted).
    """
    from pyiceberg.table.sorting import SortDirection

    sort_order = table.sort_order()
    if sort_order.is_unsorted:
        return []

    schema = table.schema()
    result = []
    for field in sort_order.fields:
        source_field = schema.find_field(field.source_id)
        direction = "asc" if field.direction == SortDirection.ASC else "desc"
        result.append({"column": source_field.name, "direction": direction})

    return result


def diff_sort_order(desired: list[dict], actual: list[dict]) -> list[SchemaChange]:
    """Produce set_sort_order change if desired differs from actual.

    Returns a list with a single SET_SORT_ORDER change if they differ,
    or empty list if they match.
    """

    def _normalize(fields: list[dict]) -> list[tuple]:
        return [(f["column"].upper(), f["direction"].lower()) for f in fields]

    if _normalize(desired) == _normalize(actual):
        return []

    # Match partition evolution: rebuilding is the default because a metadata
    # update affects only future writes. Every sort field must explicitly opt
    # out for the migration to remain metadata-only.
    desired_rebuild = not desired or any(f.get("rebuild") is not False for f in desired)
    return [
        SchemaChange(
            change_type=ChangeType.SET_SORT_ORDER,
            column_name="__sort_order__",
            sort_fields=[{"column": f["column"], "direction": f["direction"]} for f in desired],
            rebuild=desired_rebuild,
        )
    ]


def parse_table_properties(yaml_data: dict) -> dict[str, str]:
    """Parse table_properties from YAML into key-value pairs.

    All values are coerced to strings (matching Iceberg table property semantics).
    Returns empty dict when section is absent.
    """
    props = yaml_data.get("table_properties", {})
    if not props:
        return {}
    return {str(k): str(v) for k, v in props.items()}


def get_table_properties(table) -> dict[str, str]:
    """Extract current table properties from a loaded PyIceberg table.

    Returns all key-value properties as a dict.
    """
    return dict(table.properties)


def diff_table_properties(desired: dict[str, str], actual: dict[str, str]) -> list[SchemaChange]:
    """Produce set/unset property changes by comparing desired vs actual.

    Only diffs properties explicitly declared in desired dict.
    Properties not mentioned in desired are NOT unset (ignored).
    """
    changes = []

    # Properties to set (in desired with different value or not in actual)
    props_to_set = {}
    for key, value in desired.items():
        if key not in actual or actual[key] != value:
            props_to_set[key] = value

    if props_to_set:
        changes.append(
            SchemaChange(
                change_type=ChangeType.SET_TABLE_PROPERTIES,
                column_name="__table_properties__",
                properties=props_to_set,
            )
        )

    # NOTE: Per requirement 8.4, we do NOT unset properties that are in actual
    # but not in desired. Only properties explicitly declared in the YAML are managed.
    # To unset a property, it must be explicitly listed in a separate "unset_properties"
    # YAML section (future enhancement) or handled manually.

    return changes


def _serialize_change(c: SchemaChange) -> dict:
    """Serialize a SchemaChange to a JSON-ready dict."""
    entry: dict = {"change_type": c.change_type.value, "column_name": c.column_name}
    optional = (
        ("previous_name", c.previous_name),
        ("current_type", c.current_type),
        ("target_type", c.target_type),
        ("transformation", c.transformation),
        ("target_not_null", c.target_not_null),
        ("new_partition", c.new_partition),
        ("transform", c.transform),
        ("transform_param", c.transform_param),
        ("sort_fields", c.sort_fields),
        ("rebuild", c.rebuild),
        ("properties", c.properties),
        ("property_keys", c.property_keys),
    )
    for key, value in optional:
        if value is not None and value != "":
            entry[key] = value
    return entry


def write_plan(
    changes: list[SchemaChange],
    output_path: str,
    table: str,
    layer: str | None = None,
    environments: str | list[str] | None = None,
) -> str:
    """Write plan JSON to local filesystem.

    Args:
        changes: Schema changes to serialize.
        output_path: Destination path for the plan JSON.
        table: Table the plan targets.
        layer: Optional layer (``bronze``/``silver``/``gold``). Validated at
            apply time against the plan's directory.
        environments: Optional environment name or list of names. Omit to
            target every environment.

    Returns:
        The path written.
    """
    metadata: dict[str, object] = {"table": table}
    if layer is not None:
        metadata["layer"] = layer
    if environments is not None:
        metadata["environments"] = environments

    plan = {
        "metadata": metadata,
        "changes": [_serialize_change(c) for c in changes],
    }

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(plan, f, indent=2)
    return output_path


def main():
    """CLI entrypoint for generating migration plans."""
    import yaml

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Generate migration plan from config YAML")
    parser.add_argument("--region", default="us-west-2")
    parser.add_argument("--account_id", required=True)
    parser.add_argument("--config_file", required=True)
    parser.add_argument("--layer", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--database", default=None)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--bucket_prefix", default="dp")
    parser.add_argument("--migrations_dir", default="migrations/")
    parser.add_argument(
        "--only_env_additions",
        action="store_true",
        default=False,
        help=(
            "Only emit add_column changes for fields that carry an 'environments:' "
            "gate in the config YAML.  Suppresses type changes, drops, and additions "
            "for non-env-gated columns so you can ship env-specific column additions "
            "as a standalone plan without touching unrelated schema drift."
        ),
    )
    args = parser.parse_args()

    database = args.database or f"{args.bucket_prefix}_{args.environment}_{args.layer}"

    with open(args.config_file) as f:
        yaml_data = yaml.safe_load(f)
    raw_fields = yaml_data.get("fields", [])
    fields = [_normalize_field(f) for f in raw_fields]

    # Collect env-gated column names from the raw (unfiltered) field list so we
    # can identify which add_column changes are env-specific when --only_env_additions
    # is requested.  A field is env-gated if it has an 'environments' key.
    env_gated_cols: set[str] = {
        _normalize_field(f)["table_column"].lower() for f in raw_fields if f.get("environments") is not None
    }

    # Filter fields by environment: keep only those active in the target environment
    fields = filter_fields_by_environment(fields, args.environment)
    logger.info(
        "Loaded %d fields from %s (filtered to %d for environment %s)",
        len(raw_fields),
        args.config_file,
        len(fields),
        args.environment,
    )

    target_bucket = f"{args.bucket_prefix}-{args.environment}-{args.layer}-tables"
    catalog = load_catalog(args.region, args.account_id, target_bucket)

    fqn = f"{database}.{args.table}"
    try:
        table = catalog.load_table(fqn)
    except Exception as e:
        logger.error("Table '%s' not found: %s", fqn, e)
        return

    table_schema = {f.name.lower(): str(f.field_type).lower() for f in table.schema().fields}
    table_required = {f.name.lower(): f.required for f in table.schema().fields}

    changes = classify_changes(fields, table_schema, layer=args.layer, table_required=table_required)

    # Metadata diffing: partition, sort order, table properties
    desired_partition = parse_partition_spec(yaml_data)
    actual_partition = get_table_partition_spec(table)
    partition_changes = diff_partition_spec(desired_partition, actual_partition)

    # Correlate a partition change with an incompatible type change on the same
    # column so a config-driven re-partition that needs a physical re-cluster is
    # applied by the rebuild (CTAS) path atomically, instead of a separate
    # metadata-only add/drop that would leave existing data in the old layout.
    changes, partition_changes = correlate_repartition_with_type_change(changes, partition_changes, desired_partition)
    changes.extend(partition_changes)

    desired_sort = parse_sort_order(yaml_data)
    actual_sort = get_table_sort_order(table)
    changes.extend(diff_sort_order(desired_sort, actual_sort))

    desired_props = parse_table_properties(yaml_data)
    actual_props = get_table_properties(table)
    changes.extend(diff_table_properties(desired_props, actual_props))

    if not changes:
        logger.info("No changes detected")
        return

    if args.only_env_additions:
        before = len(changes)
        changes = [
            c for c in changes if c.change_type == ChangeType.ADD_COLUMN and c.column_name.lower() in env_gated_cols
        ]
        logger.info(
            "--only_env_additions: kept %d add_column change(s) for env-gated columns (suppressed %d other change(s))",
            len(changes),
            before - len(changes),
        )
        if not changes:
            logger.info("No env-gated add_column changes detected — nothing to write.")
            return

    logger.info("Detected %d change(s):", len(changes))
    for c in changes:
        logger.info("  [%s] %s", c.change_type.value.upper(), c.column_name)

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    output_path = os.path.join(args.migrations_dir, args.layer, args.table, f"{timestamp}_auto_plan.json")
    # Env-gated additions only exist in the environment they were detected in.
    # Everything else is config-driven desired state that applies to all envs,
    # so the plan omits the environments key.
    plan_environments = [args.environment] if args.only_env_additions else None
    write_plan(changes, output_path, args.table, layer=args.layer, environments=plan_environments)
    logger.info("Plan written to %s", output_path)


if __name__ == "__main__":
    main()
