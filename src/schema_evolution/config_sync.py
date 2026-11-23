"""Config Sync Check — compares local YAML config against live Iceberg table schema.

Writes sync status to SSM Parameter Store.
Publishes to SNS topic if out of sync.

Usage:
    schema-evolution-config-sync \
        --region us-west-2 \
        --account_id 444444444444 \
        --environment dev \
        --layer bronze \
        --source erp \
        --table invoice_lines \
        --config_dir config/
"""

import argparse
import glob
import json
import logging
import os

import boto3
import yaml

from schema_evolution import load_catalog, types_match

logger = logging.getLogger(__name__)


def find_config_file(config_dir: str, layer: str, source: str, table_name: str) -> str | None:
    """Find schema config YAML (case-insensitive table name match).

    Search order:
      1. config/<layer>/<source>/schemas/<table>.yaml
      2. config/<layer>/<source>/**/schemas/<table>.yaml  (nested like gold/finance/finance/schemas/)
      3. For silver: falls back to config/bronze/<source>/schemas/<table>.yaml
    """
    # Pattern 1: direct schemas folder
    pattern1 = os.path.join(config_dir, layer, source, "schemas", "*.yaml")
    for path in glob.glob(pattern1):
        if os.path.splitext(os.path.basename(path))[0].lower() == table_name.lower():
            return path

    # Pattern 2: nested schemas folder (gold domain structure)
    pattern2 = os.path.join(config_dir, layer, source, "**", "schemas", "*.yaml")
    for path in glob.glob(pattern2, recursive=True):
        if os.path.splitext(os.path.basename(path))[0].lower() == table_name.lower():
            return path

    # Pattern 3: nested data_products folder (gold data products)
    pattern3 = os.path.join(config_dir, layer, source, "**", "data_products", "*.yaml")
    for path in glob.glob(pattern3, recursive=True):
        if os.path.splitext(os.path.basename(path))[0].lower() == table_name.lower():
            return path

    # Pattern 4: silver falls back to bronze
    if layer == "silver":
        pattern4 = os.path.join(config_dir, "bronze", source, "schemas", "*.yaml")
        for path in glob.glob(pattern4):
            if os.path.splitext(os.path.basename(path))[0].lower() == table_name.lower():
                return path

    return None


def _normalize_field(field: dict) -> dict:
    """Normalize field dict to use ``table_column``/``table_data_type`` keys.

    Data product contract YAMLs use ``technical_name``/``data_type`` while
    schema YAMLs use ``table_column``/``table_data_type``.  This function
    maps the contract format into the schema format so downstream
    comparison logic works with either source.
    """
    if "technical_name" in field and "table_column" not in field:
        field = dict(field)
        field["table_column"] = field["technical_name"]
        field["table_data_type"] = field.get("data_type", "string")
    return field


def load_config_fields(config_path: str) -> list[dict]:
    """Load and normalize field definitions from a config YAML file."""
    with open(config_path) as f:
        data = yaml.safe_load(f)
    return [_normalize_field(f) for f in data.get("fields", [])]


def filter_fields_by_environment(fields: list[dict], environment: str | None = None) -> list[dict]:
    """Filter fields to include only those active in the given environment.

    Fields with no 'environments' key are included in all environments (backward compat).
    Fields with 'environments' key are included only if environment matches.

    Args:
        fields: List of field dicts (may contain 'environments' key)
        environment: Target environment (dev/qa/prod) or None to return all fields
    """
    if environment is None:
        return fields

    kept = []
    for field in fields:
        # If no environments key, include in all environments
        field_envs = field.get("environments")
        if field_envs is None or environment in field_envs:
            kept.append(field)
    return kept


def get_table_schema(catalog, namespace: str, table_name: str) -> dict[str, str]:
    """Retrieve the current Iceberg table schema as a column-to-type mapping."""
    fqn = f"{namespace}.{table_name}"
    table = catalog.load_table(fqn)
    return {f.name.lower(): str(f.field_type).lower() for f in table.schema().fields}


def check_config_sync(config_fields: list[dict], table_schema: dict[str, str]) -> dict:
    """Compare config fields against table schema."""
    config_cols = {f["table_column"].lower(): f["table_data_type"] for f in config_fields}
    table_cols = {k.lower(): v for k, v in table_schema.items()}

    missing_in_table = [col for col in config_cols if col not in table_cols]
    missing_in_config = [col for col in table_cols if col not in config_cols]
    type_mismatches = []

    for col, config_type in config_cols.items():
        if col in table_cols and not types_match(config_type, table_cols[col]):
            type_mismatches.append(
                {
                    "column": col,
                    "config_type": config_type,
                    "table_type": table_cols[col],
                }
            )

    in_sync = not missing_in_table and not missing_in_config and not type_mismatches
    return {
        "in_sync": in_sync,
        "missing_in_table": missing_in_table,
        "missing_in_config": missing_in_config,
        "type_mismatches": type_mismatches,
    }


def check_metadata_sync(yaml_data: dict, table) -> dict:
    """Compare partition spec, sort order, and table properties from YAML against actual table.

    Args:
        yaml_data: Full parsed YAML config dict (includes partition_strategy, table_properties).
        table: Loaded PyIceberg table object.

    Returns:
        Dict with keys: partition_drift, sort_order_drift, property_drift, metadata_in_sync.
    """
    from schema_evolution.plan import (
        diff_partition_spec,
        diff_sort_order,
        diff_table_properties,
        get_table_partition_spec,
        get_table_properties,
        get_table_sort_order,
        parse_partition_spec,
        parse_sort_order,
        parse_table_properties,
    )

    # Partition drift
    desired_partition = parse_partition_spec(yaml_data)
    actual_partition = get_table_partition_spec(table)
    partition_changes = diff_partition_spec(desired_partition, actual_partition)
    partition_drift = []
    for change in partition_changes:
        if change.change_type.value == "add_partition_field":
            partition_drift.append(f"Missing partition field: {change.transform}({change.column_name})")
        elif change.change_type.value == "drop_partition_field":
            partition_drift.append(f"Extra partition field: {change.transform}({change.column_name})")

    # Sort order drift
    desired_sort = parse_sort_order(yaml_data)
    actual_sort = get_table_sort_order(table)
    sort_changes = diff_sort_order(desired_sort, actual_sort)
    sort_order_drift = []
    if sort_changes:
        desired_desc = ", ".join(f"{f['column']} {f['direction']}" for f in desired_sort) or "unsorted"
        actual_desc = ", ".join(f"{f['column']} {f['direction']}" for f in actual_sort) or "unsorted"
        sort_order_drift.append(f"Sort order mismatch: config=[{desired_desc}], table=[{actual_desc}]")

    # Property drift
    desired_props = parse_table_properties(yaml_data)
    actual_props = get_table_properties(table)
    prop_changes = diff_table_properties(desired_props, actual_props)
    property_drift = []
    for change in prop_changes:
        if change.properties:
            for key, value in change.properties.items():
                actual_val = actual_props.get(key, "<not set>")
                property_drift.append(f"Property '{key}': config='{value}', table='{actual_val}'")

    metadata_in_sync = not partition_drift and not sort_order_drift and not property_drift

    return {
        "metadata_in_sync": metadata_in_sync,
        "partition_drift": partition_drift,
        "sort_order_drift": sort_order_drift,
        "property_drift": property_drift,
    }


def write_ssm_status(region: str, environment: str, layer: str, table_name: str, result: dict) -> None:
    """Write sync status to SSM Parameter Store."""
    ssm = boto3.client("ssm", region_name=region)
    param_name = f"/dp/{environment}/schema-evolution/{layer}/{table_name}/config-sync"
    value = json.dumps(
        {
            "in_sync": result["in_sync"],
            "missing_in_table": result["missing_in_table"],
            "missing_in_config": result["missing_in_config"],
            "type_mismatches": result["type_mismatches"],
        }
    )
    ssm.put_parameter(
        Name=param_name,
        Value=value,
        Type="String",
        Overwrite=True,
    )
    logger.info("SSM: %s -> %s", param_name, "in_sync" if result["in_sync"] else "out_of_sync")


def publish_sns_alert(
    region: str, sns_topic_arn: str, environment: str, layer: str, table_name: str, result: dict
) -> None:
    """Publish out-of-sync alert to SNS topic."""
    sns = boto3.client("sns", region_name=region)
    message = {
        "environment": environment,
        "layer": layer,
        "table": table_name,
        "missing_in_table": result["missing_in_table"],
        "missing_in_config": result["missing_in_config"],
        "type_mismatches": result["type_mismatches"],
    }
    sns.publish(
        TopicArn=sns_topic_arn,
        Subject=f"Schema out of sync: {layer}/{table_name} ({environment})",
        Message=json.dumps(message, indent=2),
    )
    logger.info("SNS alert published to %s", sns_topic_arn)


def main():  # noqa: C901
    """CLI entrypoint for checking config-to-schema sync."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Check config YAML vs Iceberg table schema sync")
    parser.add_argument("--region", required=True)
    parser.add_argument("--account_id", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--layer", required=True, help="bronze/silver/gold")
    parser.add_argument("--source", required=True, help="Source name (e.g. erp, crm)")
    parser.add_argument("--table", required=True, help="Table name (case-insensitive)")
    parser.add_argument("--database", default=None, help="Iceberg namespace (defaults to table)")
    parser.add_argument("--bucket_prefix", default="dp")
    parser.add_argument("--config_dir", default="config/")
    parser.add_argument("--sns_topic_arn", default=None, help="SNS topic ARN for out-of-sync alerts (optional)")
    args = parser.parse_args()

    database = args.database or args.table

    # Find config file
    config_path = find_config_file(args.config_dir, args.layer, args.source, args.table)
    if not config_path:
        logger.error("Config file not found: config/%s/%s/schemas/%s.yaml", args.layer, args.source, args.table)
        exit(1)

    logger.info("Config file: %s", config_path)
    fields = load_config_fields(config_path)
    # Filter fields by environment: keep only those active in the target environment
    fields = filter_fields_by_environment(fields, args.environment)
    logger.info(
        "Config fields: %d (filtered to %d for environment %s)",
        len(load_config_fields(config_path)),
        len(fields),
        args.environment,
    )

    # Load Iceberg table schema
    target_bucket = f"{args.bucket_prefix}-{args.environment}-{args.layer}-tables"
    catalog = load_catalog(args.region, args.account_id, target_bucket)

    try:
        table_schema = get_table_schema(catalog, database, args.table)
    except Exception as e:
        logger.error("Table '%s.%s' not found: %s", database, args.table, e)
        exit(1)

    logger.info("Table columns: %d", len(table_schema))

    # Compare columns
    result = check_config_sync(fields, table_schema)

    # Load full YAML for metadata sync
    with open(config_path) as f:
        full_yaml = yaml.safe_load(f)

    # Reload table object for metadata checks
    fqn = f"{database}.{args.table}"
    table_obj = catalog.load_table(fqn)
    metadata_result = check_metadata_sync(full_yaml, table_obj)

    if not metadata_result["metadata_in_sync"]:
        result["in_sync"] = False
        if metadata_result["partition_drift"]:
            logger.warning("  Partition drift:")
            for d in metadata_result["partition_drift"]:
                logger.warning("    %s", d)
        if metadata_result["sort_order_drift"]:
            logger.warning("  Sort order drift:")
            for d in metadata_result["sort_order_drift"]:
                logger.warning("    %s", d)
        if metadata_result["property_drift"]:
            logger.warning("  Property drift:")
            for d in metadata_result["property_drift"]:
                logger.warning("    %s", d)

    # Write to SSM Parameter Store
    write_ssm_status(args.region, args.environment, args.layer, args.table, result)

    if result["in_sync"]:
        logger.info("Config and table are IN SYNC")
    else:
        logger.warning("Config and table are OUT OF SYNC:")
        if result["missing_in_table"]:
            logger.warning("  Missing in table: %s", result["missing_in_table"])
        if result["missing_in_config"]:
            logger.warning("  Missing in config: %s", result["missing_in_config"])
        if result["type_mismatches"]:
            for m in result["type_mismatches"]:
                logger.warning(
                    "  Type mismatch: %s (config: %s, table: %s)", m["column"], m["config_type"], m["table_type"]
                )

        # Publish SNS alert if topic provided
        if args.sns_topic_arn:
            publish_sns_alert(args.region, args.sns_topic_arn, args.environment, args.layer, args.table, result)

        exit(1)


if __name__ == "__main__":
    main()
