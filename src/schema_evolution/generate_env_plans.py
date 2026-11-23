#!/usr/bin/env python3
"""Generate env-scoped schema migration plans for all env-gated config fields.

Scans every schema config YAML under config/{layer}/{source}/schemas/ and looks
for fields that carry an ``environments:`` key.  For each table that has at least
one env-gated field, the script runs the plan generator with ``--only_env_additions``
for every relevant environment (dev / qa / prod), writing plan JSON files to
``migrations/{layer}/{table}/``.

Account IDs come from ``--account_id`` (one account for every requested
environment) or from ``<accounts_dir>/{env}.yaml`` files containing either a
top-level ``account_id:`` key or ``accounts.datalake.account_id``.

Only environments that actually have env-gated column additions for a given table
are processed (e.g. a table with ``environments: [dev, prod]`` columns will get
plans for dev and prod, but NOT qa).

Usage
-----
    # generate plans for all env-gated tables, all layers, all envs
    schema-evolution-generate-env-plans --account_id 111111111111 --envs dev

    # dry run — shows what plans would be generated without touching Iceberg
    schema-evolution-generate-env-plans --dry_run

    # only silver, only dev + qa
    schema-evolution-generate-env-plans --layers silver --envs dev qa

    # single table
    schema-evolution-generate-env-plans --table partner_sites

Output
------
For each (table, env) combination a plan file is written to:
    migrations/{layer}/{table}/{timestamp}_auto_plan.json

The file only contains add_column changes for the env-gated columns active in
that environment.  Commit the resulting JSON files alongside your YAML changes
so your deploy pipeline applies the DDL (``schema-evolution-apply``) on next merge.
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import sys
from pathlib import Path

import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

VALID_ENVS = ["dev", "qa", "prod"]


# ---------------------------------------------------------------------------
# Account config
# ---------------------------------------------------------------------------


def load_account_id(accounts_dir: str, env: str) -> str | None:
    """Read the account_id for *env* from ``<accounts_dir>/{env}.yaml``.

    Accepts a top-level ``account_id`` key or ``accounts.datalake.account_id``.
    """
    path = os.path.join(accounts_dir, f"{env}.yaml")
    if not os.path.isfile(path):
        logger.warning("Account config not found: %s", path)
        return None
    with open(path) as f:
        data = yaml.safe_load(f)
    if isinstance(data, dict) and data.get("account_id"):
        return str(data["account_id"])
    try:
        return str(data["accounts"]["datalake"]["account_id"])
    except (KeyError, TypeError):
        logger.warning("Could not read account_id or accounts.datalake.account_id from %s", path)
        return None


# ---------------------------------------------------------------------------
# Config YAML scanning
# ---------------------------------------------------------------------------


def find_schema_yamls(config_root: str, layers: list[str]) -> list[tuple[str, str, str, str]]:
    """Yield (yaml_path, layer, source, table_name) for all schema config YAMLs.

    Supports two layout patterns:
      config/{layer}/{source}/schemas/{table}.yaml
      config/{layer}/{source}/**/schemas/{table}.yaml  (nested, e.g. gold domains)
    """
    results = []
    for layer in layers:
        layer_root = os.path.join(config_root, layer)
        if not os.path.isdir(layer_root):
            continue
        for source in os.listdir(layer_root):
            source_path = os.path.join(layer_root, source)
            if not os.path.isdir(source_path):
                continue
            for yaml_path in glob.glob(os.path.join(source_path, "**", "schemas", "*.yaml"), recursive=True):
                table_name = Path(yaml_path).stem
                results.append((yaml_path, layer, source, table_name))
    return sorted(results)


def envs_with_gated_columns(yaml_path: str) -> dict[str, list[str]]:
    """Return a mapping of env -> [column_names] for env-gated fields in a YAML.

    Only fields that have an ``environments:`` key are included.
    A column gated to ``[dev, prod]`` will appear under both 'dev' and 'prod'.
    Returns an empty dict if the YAML has no env-gated fields.
    """
    with open(yaml_path) as f:
        data = yaml.safe_load(f)

    env_cols: dict[str, list[str]] = {}
    for field in data.get("fields", []):
        envs = field.get("environments")
        if envs is None:
            continue
        col = field.get("table_column") or field.get("technical_name", "")
        for env in envs:
            env_cols.setdefault(env, []).append(col)
    return env_cols


# ---------------------------------------------------------------------------
# Plan generation (calls schema_evolution.plan internals directly)
# ---------------------------------------------------------------------------


def generate_plan_for_env(
    *,
    yaml_path: str,
    layer: str,
    table: str,
    env: str,
    account_id: str,
    migrations_dir: str,
    region: str = "us-west-2",
    bucket_prefix: str = "dp",
    dry_run: bool = False,
) -> bool:
    """Call schema_evolution plan generation for one (table, env) pair.

    Returns True on success, False on error/skip.
    """
    # Import here so the script can still list tables / dry-run without
    # PyIceberg installed (catalog is only needed when dry_run=False).
    import yaml as _yaml

    from schema_evolution import ChangeType, load_catalog, target_bucket_for_layer
    from schema_evolution.config_sync import _normalize_field, filter_fields_by_environment
    from schema_evolution.plan import (
        classify_changes,
        correlate_repartition_with_type_change,
        diff_partition_spec,
        diff_sort_order,
        diff_table_properties,
        get_table_partition_spec,
        get_table_sort_order,
        parse_partition_spec,
        parse_sort_order,
        parse_table_properties,
        write_plan,
    )

    with open(yaml_path) as f:
        yaml_data = _yaml.safe_load(f)

    raw_fields = yaml_data.get("fields", [])
    all_fields = [_normalize_field(f) for f in raw_fields]

    # Columns that carry an environments: key (env-gated, any env)
    env_gated_cols: set[str] = {f["table_column"].lower() for f in all_fields if f.get("environments") is not None}

    # Fields active for this environment
    fields = filter_fields_by_environment(all_fields, env)

    database = f"{bucket_prefix}_{env}_{layer}"
    target_bucket = target_bucket_for_layer(layer, bucket_prefix, env)

    if dry_run:
        gated_active = [f["table_column"] for f in fields if f["table_column"].lower() in env_gated_cols]
        logger.info(
            "[DRY RUN] Would generate plan: table=%s env=%s layer=%s account=%s  env-gated columns → %s",
            table,
            env,
            layer,
            account_id,
            gated_active,
        )
        return True

    # --- Connect to live Iceberg catalog ---
    try:
        catalog = load_catalog(region, account_id, target_bucket)
    except Exception as e:
        logger.error("Cannot load catalog for %s/%s (account=%s): %s", env, table, account_id, e)
        return False

    fqn = f"{database}.{table}"
    try:
        iceberg_table = catalog.load_table(fqn)
    except Exception as e:
        logger.warning("Table %s not found in %s (env=%s) — skipping: %s", fqn, target_bucket, env, e)
        return False

    table_schema = {f.name.lower(): str(f.field_type).lower() for f in iceberg_table.schema().fields}

    changes = classify_changes(fields, table_schema, layer=layer)

    # Partition / sort / properties diffs
    desired_partition = parse_partition_spec(yaml_data)
    actual_partition = get_table_partition_spec(iceberg_table)
    partition_changes = diff_partition_spec(desired_partition, actual_partition)
    changes, partition_changes = correlate_repartition_with_type_change(changes, partition_changes, desired_partition)
    changes.extend(partition_changes)
    changes.extend(diff_sort_order(parse_sort_order(yaml_data), get_table_sort_order(iceberg_table)))
    changes.extend(diff_table_properties(parse_table_properties(yaml_data), dict(iceberg_table.properties)))

    # --- Filter to ONLY env-gated add_column changes ---
    env_addition_changes = [
        c for c in changes if c.change_type == ChangeType.ADD_COLUMN and c.column_name.lower() in env_gated_cols
    ]

    if not env_addition_changes:
        logger.info("No env-gated add_column changes for table=%s env=%s — skipping.", table, env)
        return None  # distinguish "skipped cleanly" from "wrote a plan" (True)

    logger.info(
        "Writing plan: table=%s env=%s  %d add_column change(s): %s",
        table,
        env,
        len(env_addition_changes),
        [c.column_name for c in env_addition_changes],
    )

    from datetime import UTC, datetime

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    output_path = os.path.join(migrations_dir, layer, table, f"{timestamp}_auto_plan.json")
    write_plan(env_addition_changes, output_path, table, layer=layer, environments=[env])
    logger.info("Plan written → %s", output_path)
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:  # noqa: C901
    """Scan config YAMLs for env-gated columns and generate migration plans."""
    parser = argparse.ArgumentParser(
        description="Generate env-scoped plans for all config YAMLs with environment-gated fields."
    )
    parser.add_argument(
        "--config_root",
        default="config",
        help="Root of config directory (default: config)",
    )
    parser.add_argument(
        "--accounts_dir",
        default="accounts",
        help="Directory containing {env}.yaml account configs (default: accounts)",
    )
    parser.add_argument(
        "--account_id",
        default=None,
        help="Account that owns the S3 Tables buckets. Overrides --accounts_dir for every env.",
    )
    parser.add_argument(
        "--migrations_dir",
        default="migrations",
        help="Output directory for plan files (default: migrations)",
    )
    parser.add_argument(
        "--layers",
        nargs="+",
        default=["silver"],
        metavar="LAYER",
        help="Layers to scan (default: silver).  Use 'silver gold' for both.",
    )
    parser.add_argument(
        "--envs",
        nargs="+",
        default=VALID_ENVS,
        metavar="ENV",
        choices=VALID_ENVS,
        help="Environments to generate plans for (default: dev qa prod)",
    )
    parser.add_argument(
        "--table",
        default=None,
        help="Limit to a single table name (e.g. partner_sites)",
    )
    parser.add_argument(
        "--region",
        default="us-west-2",
    )
    parser.add_argument(
        "--bucket_prefix",
        default="dp",
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        default=False,
        help="Print what would be generated without connecting to Iceberg.",
    )
    args = parser.parse_args()

    # --- Load account IDs for every requested env ---
    account_ids: dict[str, str] = {}
    for env in args.envs:
        aid = args.account_id or load_account_id(args.accounts_dir, env)
        if aid:
            account_ids[env] = aid
        else:
            logger.warning("Skipping env=%s — no datalake account_id found.", env)

    if not account_ids:
        logger.error("No account IDs resolved — aborting.")
        sys.exit(1)

    # --- Scan all schema YAMLs ---
    all_yamls = find_schema_yamls(args.config_root, args.layers)
    if args.table:
        all_yamls = [(p, layer_name, s, t) for p, layer_name, s, t in all_yamls if t.lower() == args.table.lower()]
        if not all_yamls:
            logger.error("No config YAML found for table=%s in layers=%s", args.table, args.layers)
            sys.exit(1)

    logger.info("Scanning %d config YAML(s) across layers=%s ...", len(all_yamls), args.layers)

    total_planned = 0
    total_skipped = 0
    errors = []

    for yaml_path, layer, source, table in all_yamls:
        env_cols = envs_with_gated_columns(yaml_path)
        if not env_cols:
            continue  # No env-gated columns in this YAML — skip silently

        logger.info("─" * 80)
        logger.info("table=%s  layer=%s  source=%s", table, layer, source)
        logger.info("  env-gated columns: %s", {e: cols for e, cols in env_cols.items()})

        for env in args.envs:
            if env not in env_cols:
                # This env has no gated additions for this table — nothing to do
                logger.debug("  env=%s: no env-gated columns — skip", env)
                continue

            if env not in account_ids:
                logger.warning("  env=%s: no account_id available — skip", env)
                total_skipped += 1
                continue

            ok = generate_plan_for_env(
                yaml_path=yaml_path,
                layer=layer,
                table=table,
                env=env,
                account_id=account_ids[env],
                migrations_dir=args.migrations_dir,
                region=args.region,
                bucket_prefix=args.bucket_prefix,
                dry_run=args.dry_run,
            )
            if ok is True:
                total_planned += 1
            elif ok is False:
                total_skipped += 1
                errors.append(f"{table}/{env}")
            # ok is None → cleanly skipped (column already exists), not counted

    logger.info("═" * 80)
    logger.info(
        "Done. Plans generated: %d  |  Skipped/errored: %d",
        total_planned,
        total_skipped,
    )
    if errors:
        logger.warning("Tables that could not be planned: %s", errors)
        logger.warning(
            "Possible causes:"
            "\n  1. Cross-account access — run this script separately for each env with appropriate AWS credentials:"
            "\n     AWS_PROFILE=<qa-profile> schema-evolution-generate-env-plans --envs qa"
            "\n     AWS_PROFILE=<prod-profile> schema-evolution-generate-env-plans --envs prod"
            "\n  2. Tables don't exist yet in that env's Silver catalog"
            "\n  3. Insufficient permissions"
        )

    # Only fail if NO plans were generated at all
    if total_planned == 0 and not args.dry_run:
        sys.exit(1)


if __name__ == "__main__":
    main()
