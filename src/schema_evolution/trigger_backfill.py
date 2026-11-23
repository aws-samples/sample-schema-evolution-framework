#!/usr/bin/env python3
"""Trigger env-gated add_column backfill Glue jobs from migration plan files.

Reads every migration plan JSON under migrations/ that matches the target
environment, extracts new_columns and merge_keys, then calls
``aws glue start-job-run`` for each table.

Usage
-----
    # Trigger all QA backfills
    schema-evolution-trigger-backfill \\
        --environment qa \\
        --account_id 222222222222

    # Dry-run: print Glue CLI commands without executing
    schema-evolution-trigger-backfill \\
        --environment qa \\
        --account_id 222222222222 \\
        --dry_run

    # Single table
    schema-evolution-trigger-backfill \\
        --environment qa \\
        --account_id 222222222222 \\
        --table contracts
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import boto3
import yaml

from schema_evolution.apply import plan_targets_environment

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

GLUE_JOB_NAME = "{prefix}-{env}-glue-schema-evolution-backfill-env-columns"
MIGRATIONS_ROOT = Path("migrations")
CONFIG_ROOT = Path("config")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def plan_layer(plan_path: Path) -> str:
    """Return the layer a plan file lives under (``migrations/<layer>/<table>/``)."""
    return plan_path.relative_to(MIGRATIONS_ROOT).parts[0]


def find_plans(environment: str, table_filter: str | None = None) -> list[Path]:
    """Return plan JSON files that target the given environment.

    A plan with no ``environments`` key targets every environment; see
    :func:`schema_evolution.apply.plan_targets_environment`.
    """
    plans = []
    for path in sorted(MIGRATIONS_ROOT.glob("**/*.json")):
        try:
            data = json.loads(path.read_text())
            metadata = data.get("metadata", {})
            if not plan_targets_environment(metadata, environment, plan_layer(path)):
                continue
            if table_filter and metadata.get("table") != table_filter:
                continue
            changes = [c for c in data.get("changes", []) if c.get("change_type") == "add_column"]
            if changes:
                plans.append(path)
        except Exception as e:
            logger.warning("Skipping %s: %s", path, e)
    return plans


def get_add_columns(plan_path: Path) -> list[str]:
    """Return add_column column names from a plan JSON."""
    data = json.loads(plan_path.read_text())
    return [c["column_name"] for c in data.get("changes", []) if c.get("change_type") == "add_column"]


def get_table_name(plan_path: Path) -> str:
    """Return the table name from a plan JSON."""
    return json.loads(plan_path.read_text())["metadata"]["table"]


def get_database(plan_path: Path, environment: str, bucket_prefix: str = "dp") -> str:
    """Derive the target database for a plan.

    Plans no longer carry a database name; it is composed from the environment
    being deployed and the layer the plan lives under, matching
    ``schema_evolution.apply``.

    Args:
        plan_path: Path to the plan JSON.
        environment: Target environment (``dev``/``qa``/``prod``).
        bucket_prefix: Project prefix (default ``dp``).

    Returns:
        Database name, e.g. ``dp_dev_silver``.
    """
    return f"{bucket_prefix}_{environment}_{plan_layer(plan_path)}"


def find_config_yaml(table_name: str) -> Path | None:
    """Find the schema config YAML for a table by searching config/*/schemas/."""
    matches = list(CONFIG_ROOT.glob(f"**/schemas/{table_name}.yaml"))
    if not matches:
        return None
    return matches[0]


def get_merge_keys(config_path: Path, environment: str) -> list[str]:
    """Extract merge key column names from a schema config YAML."""
    data = yaml.safe_load(config_path.read_text())
    keys = []
    for field in data.get("fields", []):
        if not field.get("merge_key"):
            continue
        envs = field.get("environments")
        if envs is not None and environment not in envs:
            continue
        keys.append(field["table_column"])
    return keys


def get_load_type(config_path: Path) -> str:
    """Return a table's load_type (default: merge).

    Silver tables declare ``load_type`` in the sibling transformations file
    (``.../transformations/<table>.yaml``); Bronze/Gold schema files carry it
    directly.
    """
    transformations_path = config_path.parent.parent / "transformations" / config_path.name
    for path in (transformations_path, config_path):
        if path.exists():
            load_type = (yaml.safe_load(path.read_text()) or {}).get("load_type")
            if load_type:
                return load_type
    return "merge"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:  # noqa: C901
    """Parse arguments and trigger Glue job runs for each migration plan."""
    parser = argparse.ArgumentParser(description="Trigger backfill Glue jobs for env-gated migration plans.")
    parser.add_argument("--environment", required=True, help="Target environment: dev | qa | prod")
    parser.add_argument("--account_id", required=True, help="Account that owns the S3 Tables buckets")
    parser.add_argument("--table", help="Only trigger for this table (optional)")
    parser.add_argument("--region", default="us-west-2")
    parser.add_argument("--bucket_prefix", default="dp")
    parser.add_argument("--dry_run", action="store_true", help="Print Glue commands without executing")
    parser.add_argument(
        "--stagger_seconds",
        type=int,
        default=5,
        help="Seconds to wait between starting each job (default: 5). Prevents ConcurrentRunsExceededException.",
    )
    args = parser.parse_args()

    env = args.environment
    job_name = GLUE_JOB_NAME.format(prefix=args.bucket_prefix, env=env)

    plans = find_plans(env, table_filter=args.table)
    if not plans:
        logger.info("No matching migration plans found for environment=%s", env)
        return

    logger.info("Found %d plan(s) for environment=%s", len(plans), env)

    errors: list[str] = []
    triggered: list[str] = []

    glue = None if args.dry_run else boto3.client("glue", region_name=args.region)

    for plan_path in plans:
        table = get_table_name(plan_path)
        database = get_database(plan_path, env, args.bucket_prefix)
        new_cols = get_add_columns(plan_path)

        config_path = find_config_yaml(table)
        if config_path is None:
            logger.error("No schema config YAML found for table=%s — skipping", table)
            errors.append(table)
            continue

        merge_keys = get_merge_keys(config_path, environment=env)
        if not merge_keys:
            logger.error("No merge keys found for table=%s env=%s — skipping", table, env)
            errors.append(table)
            continue

        load_type = get_load_type(config_path)

        arguments = {
            "--environment": env,
            "--database": database,
            "--table": table,
            "--new_columns": ",".join(new_cols),
            "--merge_keys": ",".join(merge_keys),
            "--account_id": args.account_id,
            "--region": args.region,
            "--bucket_prefix": args.bucket_prefix,
            "--load_type": load_type,
        }

        logger.info(
            "─" * 60 + "\n  table=%s\n  load_type=%s\n  new_columns=%s\n  merge_keys=%s",
            table,
            load_type,
            new_cols,
            merge_keys,
        )

        if args.dry_run:
            args_json = json.dumps(arguments, indent=4)
            logger.info(
                "[dry_run] aws glue start-job-run --job-name %s --arguments '%s'",
                job_name,
                args_json,
            )
        else:
            if glue is None:  # created whenever --dry_run is off; narrows the type
                raise RuntimeError("Glue client not initialised")
            try:
                resp = glue.start_job_run(JobName=job_name, Arguments=arguments)
                run_id = resp["JobRunId"]
                logger.info("✅ Started: table=%s  JobRunId=%s", table, run_id)
                triggered.append(f"{table} → {run_id}")
                if args.stagger_seconds > 0 and plan_path != plans[-1]:
                    logger.info("Waiting %ds before next job start...", args.stagger_seconds)
                    time.sleep(args.stagger_seconds)
            except glue.exceptions.ConcurrentRunsExceededException:
                logger.error(
                    "❌ ConcurrentRunsExceededException for table=%s — too many jobs running. "
                    "Increase --stagger_seconds or wait and retry.",
                    table,
                )
                errors.append(table)
            except Exception as e:
                logger.error("❌ Failed to start job for table=%s: %s", table, e)
                errors.append(table)

    logger.info("═" * 60)
    logger.info("Done. Triggered: %d  |  Errors: %d", len(triggered), len(errors))
    if triggered:
        for t in triggered:
            logger.info("  ✅ %s", t)
    if errors:
        logger.warning("  ❌ Failed: %s", errors)
        sys.exit(1)


if __name__ == "__main__":
    main()
