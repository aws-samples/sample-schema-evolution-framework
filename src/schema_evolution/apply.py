"""Schema Evolution Apply — batch upsert version (v5).

Discovers plan files in migrations/, compares against Iceberg tracker,
applies DDL via PyIceberg, triggers Glue for backfills, batch upserts results.
"""

import argparse
import hashlib
import json
import logging
import os
import re
import secrets
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import boto3
import pyarrow as pa
from pyiceberg.exceptions import CommitFailedException

from schema_evolution import load_catalog, target_bucket_for_layer
from schema_evolution.lock import DEFAULT_TTL_SECONDS, LockNotAcquiredError, S3Lock, lock_uri_for
from schema_evolution.plan import get_table_partition_spec, get_table_sort_order

logger = logging.getLogger(__name__)

TRACKING_NAMESPACE = "schema_evolution_metadata"
TRACKING_TABLE = "plan_tracker"
WAIT_POLL_INTERVAL = 30
WAIT_MAX_RETRIES = 10

# Tracker statuses. ``applied`` is terminal. ``failed`` and ``waiting_for_table``
# are retried on the next pass. ``backfilling``/``in-progress`` lock the table
# until the backfill Glue job records a terminal state.
STATUS_APPLIED = "applied"
STATUS_FAILED = "failed"
STATUS_WAITING_FOR_TABLE = "waiting_for_table"
ACTIONABLE_STATUSES = frozenset({STATUS_FAILED, STATUS_WAITING_FOR_TABLE})
IN_FLIGHT_STATUSES = frozenset({"in-progress", "backfilling"})

# Default number of tables processed concurrently by the CLI. Plans for one table
# always run in order on a single worker; only independent tables overlap.
DEFAULT_MAX_WORKERS = 4

CHECKSUM_MODES = ("fail", "warn", "off")
CHECKSUM_MODE_ENV_VAR = "SCHEMA_EVOLUTION_CHECKSUM_MODE"

# A DDL handler that succeeds without changing anything noteworthy may return
# ``(True, "SKIPPED: ...")``; the note is recorded on the tracker row.
SKIPPED_PREFIX = "SKIPPED: "


# ---------------------------------------------------------------------------
# Plan File Discovery
# ---------------------------------------------------------------------------


def list_plan_files(migrations_dir: str) -> dict:
    """List migration plan files grouped by layer and table."""
    tables: dict[tuple[str, str], list[str]] = {}
    if not os.path.isdir(migrations_dir):
        return tables
    for layer in os.listdir(migrations_dir):
        layer_path = os.path.join(migrations_dir, layer)
        if not os.path.isdir(layer_path):
            continue
        for table_name in os.listdir(layer_path):
            table_path = os.path.join(layer_path, table_name)
            if not os.path.isdir(table_path):
                continue
            for filename in os.listdir(table_path):
                if filename.endswith(".json"):
                    tables.setdefault((layer, table_name), []).append(filename)
    for t in tables:
        tables[t].sort()
    return tables


def read_plan_json_local(filepath: str) -> list:
    """Read the changes list from a local plan JSON file."""
    with open(filepath) as f:
        return json.load(f).get("changes", [])


def read_plan_metadata_local(filepath: str) -> dict:
    """Read the metadata dict from a local plan JSON file."""
    with open(filepath) as f:
        return json.load(f).get("metadata", {})


def plan_checksum(changes: list) -> str:
    """Return a stable SHA-256 checksum of a plan's ``changes`` list.

    The checksum covers only ``changes`` (the DDL that is executed), serialized
    canonically, so reformatting a file or editing its ``metadata`` (for example
    replacing the deprecated ``database`` key with ``environments``) does not
    register as drift.

    Args:
        changes: The plan's ``changes`` list.

    Returns:
        Hex-encoded SHA-256 digest.
    """
    canonical = json.dumps(changes, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _legacy_database_targets_environment(legacy_db: str, environment: str, layer: str) -> bool:
    """Resolve a deprecated ``metadata.database`` value to an environment match.

    Args:
        legacy_db: Value of ``metadata.database`` (``{prefix}_{env}_{layer}``).
        environment: Environment currently being deployed.
        layer: Layer the plan file lives under.

    Returns:
        ``True`` when *legacy_db* pins the plan to *environment*.

    Raises:
        ValueError: If the value is malformed or targets a different layer.
    """
    match = re.fullmatch(r"(?P<prefix>.+)_(?P<env>[^_]+)_(?P<layer>[^_]+)", legacy_db.strip())
    if not match:
        raise ValueError(f"legacy metadata.database '{legacy_db}' is not of the form '{{prefix}}_{{env}}_{{layer}}'")
    if match.group("layer").lower() != layer.lower():
        raise ValueError(
            f"legacy metadata.database '{legacy_db}' targets layer "
            f"'{match.group('layer')}' but the plan lives under '{layer}'"
        )
    logger.warning(
        "Plan metadata uses deprecated 'database' key ('%s'); "
        "replace it with 'layer' + 'environments' (omit 'environments' to target all envs).",
        legacy_db,
    )
    return environment.strip().lower() == match.group("env").lower()


def _normalize_environment_list(raw: object, key: str) -> list[str]:
    """Normalize an env metadata value to a lowercase list of names.

    Args:
        raw: Either a single environment name or a list of names.
        key: Metadata key the value came from, for error messages.

    Returns:
        Lowercased, whitespace-stripped environment names.

    Raises:
        ValueError: If the value or an entry has the wrong type, or is empty.
    """
    if isinstance(raw, str):
        allowed: list[object] = [raw]
    elif isinstance(raw, list):
        allowed = list(raw)
    else:
        raise ValueError(f"metadata.{key} must be a string or a list of strings, got {type(raw).__name__}")

    normalized: list[str] = []
    for item in allowed:
        if not isinstance(item, str):
            raise ValueError(f"metadata.{key} entries must be strings, got {type(item).__name__}")
        normalized.append(item.strip().lower())

    if not normalized:
        raise ValueError(f"metadata.{key} must not be empty; omit the key to target all environments")
    return normalized


def plan_targets_environment(metadata: dict, environment: str, layer: str) -> bool:
    """Decide whether a plan applies to ``environment``.

    Resolution order:

    1. ``metadata.environments`` (alias ``metadata.environment``) -- a single
       environment name or a list of names. The plan applies only to those.
    2. Legacy ``metadata.database`` (``{prefix}_{env}_{layer}``) -- pins the plan
       to that single environment. Deprecated, still honoured so plan trees
       synced to S3 before this change keep their original targeting instead of
       silently widening to every environment.
    3. Otherwise the plan applies to **every** environment.

    ``metadata.layer``, when present, must match the layer the plan lives under
    so a plan cannot be silently applied against the wrong layer.

    Args:
        metadata: The plan's ``metadata`` dict.
        environment: Environment currently being deployed (e.g. ``dev``).
        layer: Layer the plan file lives under (``bronze``/``silver``/``gold``).

    Returns:
        ``True`` when the plan should be applied to this environment.

    Raises:
        ValueError: If both env keys are set, an env value has the wrong type,
            or ``metadata.layer``/``metadata.database`` contradicts *layer*.
    """
    declared_layer = metadata.get("layer")
    if declared_layer is not None and str(declared_layer).strip().lower() != layer.lower():
        raise ValueError(f"metadata.layer '{declared_layer}' does not match the plan's directory layer '{layer}'")

    has_plural = "environments" in metadata
    has_singular = "environment" in metadata
    if has_plural and has_singular:
        raise ValueError("metadata must not set both 'environments' and 'environment'")

    if not (has_plural or has_singular):
        legacy_db = metadata.get("database")
        if legacy_db:
            return _legacy_database_targets_environment(str(legacy_db), environment, layer)
        return True

    key = "environments" if has_plural else "environment"
    raw = metadata.get(key)
    if raw is None:
        return True

    return environment.strip().lower() in _normalize_environment_list(raw, key)


# ---------------------------------------------------------------------------
# S3 Plan Source
# ---------------------------------------------------------------------------
#
# Plans are normally read from the local repo checkout. ``--migrations_dir`` may
# also be an ``s3://`` prefix holding the same ``migrations/`` tree, for runs
# that have no checkout. Both paths yield the identical
# grouped/sorted structure and per-plan ``changes`` list, so ``apply_migrations``
# is agnostic to the source.


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split ``s3://bucket/prefix`` into ``(bucket, prefix)`` (prefix has no leading slash)."""
    rest = uri[len("s3://") :]
    bucket, _, prefix = rest.partition("/")
    return bucket, prefix.strip("/")


def list_plan_files_s3(s3_uri: str, s3_client=None) -> dict:
    """List migration plan files under an ``s3://`` prefix, grouped by (layer, table).

    Mirrors :func:`list_plan_files` but reads keys from S3. Expects the same
    ``<prefix>/<layer>/<table>/<plan>.json`` layout the local ``migrations/`` dir
    uses. Returns ``{(layer, table): [sorted plan filenames]}``.
    """
    if s3_client is None:
        s3_client = boto3.client("s3")

    bucket, prefix = _parse_s3_uri(s3_uri)
    base = f"{prefix}/" if prefix else ""
    tables: dict[tuple[str, str], list[str]] = {}

    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=base):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
            rel = key[len(base) :] if base else key
            parts = rel.split("/")
            # Expect exactly layer/table/plan.json
            if len(parts) != 3:
                continue
            layer, table_name, filename = parts
            tables.setdefault((layer, table_name), []).append(filename)

    for t in tables:
        tables[t].sort()
    return tables


def read_plan_json_s3(s3_uri: str, layer: str, table_name: str, plan_name: str, s3_client=None) -> list:
    """Read the changes list for a single plan file from S3."""
    if s3_client is None:
        s3_client = boto3.client("s3")
    bucket, prefix = _parse_s3_uri(s3_uri)
    key = "/".join(p for p in (prefix, layer, table_name, plan_name) if p)
    body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body).get("changes", [])


def read_plan_metadata_s3(s3_uri: str, layer: str, table_name: str, plan_name: str, s3_client=None) -> dict:
    """Read the metadata dict for a single plan file from S3."""
    if s3_client is None:
        s3_client = boto3.client("s3")
    bucket, prefix = _parse_s3_uri(s3_uri)
    key = "/".join(p for p in (prefix, layer, table_name, plan_name) if p)
    body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body).get("metadata", {})


def _rename_source_for_plans(
    migrations_dir: str,
    layer: str,
    table_name: str,
    plan_files: list[str],
    *,
    from_s3: bool,
    s3_client=None,
) -> str | None:
    """Return the source table for a standalone rename plan targeting table_name."""
    sources: set[str] = set()
    for plan_name in plan_files:
        changes: list[dict] | None = None
        try:
            if from_s3:
                changes = read_plan_json_s3(migrations_dir, layer, table_name, plan_name, s3_client)
            else:
                path = os.path.join(migrations_dir, layer, table_name, plan_name)
                changes = read_plan_json_local(path)
        except Exception as exc:
            logger.debug("Could not read candidate rename plan %s: %s", plan_name, exc)

        if changes is None:
            # The normal plan-read path records the failure in the tracker.
            continue

        rename_changes = [c for c in changes if c.get("change_type") == "rename_table"]
        for change in rename_changes:
            if len(changes) != 1 or change.get("column_name") != "__table__":
                raise ValueError("rename_table must be the only change in its plan and use column_name '__table__'")
            source = change.get("previous_name")
            if not isinstance(source, str) or not source.strip() or source.lower() == table_name.lower():
                raise ValueError("rename_table requires a different source table in 'previous_name'")
            sources.add(source.strip())

    if len(sources) > 1:
        raise ValueError(
            f"Plans for destination table '{table_name}' specify multiple rename sources: {sorted(sources)}"
        )
    return next(iter(sources), None)


def is_s3_uri(path: str) -> bool:
    """Return True if ``path`` is an ``s3://`` URI."""
    return path.startswith("s3://")


# ---------------------------------------------------------------------------
# Tracker
# ---------------------------------------------------------------------------


def ensure_tracker(catalog) -> None:
    """Create the migration tracker table, or add columns missing from an older one."""
    import pyiceberg.schema as sch
    from pyiceberg.types import NestedField, StringType

    with suppress(Exception):
        catalog.create_namespace(TRACKING_NAMESPACE)

    fqn = f"{TRACKING_NAMESPACE}.{TRACKING_TABLE}"
    try:
        tbl = catalog.load_table(fqn)
    except Exception:
        schema = sch.Schema(
            NestedField(1, "table_name", StringType(), required=True),
            NestedField(2, "plan_name", StringType(), required=True),
            NestedField(3, "status", StringType(), required=True),
            NestedField(4, "created_at", StringType(), required=False),
            NestedField(5, "applied_at", StringType(), required=False),
            NestedField(6, "notes", StringType(), required=False),
            NestedField(7, "checksum", StringType(), required=False),
        )
        catalog.create_table(fqn, schema=schema)
        logger.info("Created tracker table: %s", fqn)
        return

    # Trackers created before checksums existed lack the column. Adding an
    # optional column is metadata-only, and the backfill job's MERGE names its
    # columns explicitly, so it is unaffected.
    if "checksum" not in {f.name for f in tbl.schema().fields}:
        with tbl.update_schema() as update:
            update.add_column("checksum", StringType())
        logger.info("Added 'checksum' column to tracker table: %s", fqn)


def _tracker_rows_from_arrow(df) -> dict[str, dict[str, dict]]:
    """Convert a tracker Arrow table to ``{table_name: {plan_name: info}}``."""
    has_notes = "notes" in df.column_names
    has_checksum = "checksum" in df.column_names
    has_applied_at = "applied_at" in df.column_names
    result: dict[str, dict[str, dict]] = {}
    for i in range(len(df)):
        table_name = df.column("table_name")[i].as_py()
        result.setdefault(table_name, {})[df.column("plan_name")[i].as_py()] = {
            "status": df.column("status")[i].as_py(),
            "notes": df.column("notes")[i].as_py() if has_notes else None,
            "checksum": (df.column("checksum")[i].as_py() or None) if has_checksum else None,
            "applied_at": (df.column("applied_at")[i].as_py() or None) if has_applied_at else None,
        }
    return result


def read_tracker_snapshot(catalog) -> dict[str, dict[str, dict]]:
    """Read every tracker row for a layer in one scan.

    One scan per layer per pass replaces one scan per table, which is the main
    cost of a pass that has nothing to do.

    Returns:
        ``{table_name: {plan_name: {"status", "notes", "checksum"}}}``.
    """
    tbl = catalog.load_table(f"{TRACKING_NAMESPACE}.{TRACKING_TABLE}")
    return _tracker_rows_from_arrow(tbl.scan().to_arrow())


def read_tracker(catalog, table_name: str, snapshot: dict[str, dict[str, dict]] | None = None) -> dict:
    """Read tracker status for a given table.

    Returns a mapping of ``plan_name -> {"status": ..., "notes": ..., "checksum": ...}``.
    ``notes`` carries free-text context and, for failed plans, the error message
    recorded by the backfill job (e.g. a concurrent-write abort reason), so callers
    can surface *why* a plan failed without querying the tracker table directly.

    Args:
        catalog: PyIceberg catalog for the layer.
        table_name: Table whose plans to return.
        snapshot: Optional result of :func:`read_tracker_snapshot`. When given, no
            scan is made. Omit it to get a fresh read (e.g. when polling a lock).
    """
    if snapshot is not None:
        return dict(snapshot.get(table_name, {}))

    from pyiceberg.expressions import EqualTo

    fqn = f"{TRACKING_NAMESPACE}.{TRACKING_TABLE}"
    tbl = catalog.load_table(fqn)
    df = tbl.scan(row_filter=EqualTo("table_name", table_name)).to_arrow()
    return _tracker_rows_from_arrow(df).get(table_name, {})


def apply_table_rename(
    s3tables, table_bucket_arn: str, namespace: str, source: str, target: str
) -> tuple[bool, str | None]:
    """Rename an S3 Tables Iceberg table, treating an already-completed rename as success."""
    if not source or not target:
        return False, "rename_table requires source and destination table names"
    if source.lower() == target.lower():
        return False, "rename_table source and destination must be different"

    def _error_code(error: Exception) -> str:
        return str(getattr(error, "response", {}).get("Error", {}).get("Code", ""))

    try:
        source_table = s3tables.get_table(
            tableBucketARN=table_bucket_arn,
            namespace=namespace,
            name=source,
        )
    except Exception as error:
        if _error_code(error) not in {"NotFoundException", "ResourceNotFoundException"}:
            return False, str(error)
        try:
            s3tables.get_table(
                tableBucketARN=table_bucket_arn,
                namespace=namespace,
                name=target,
            )
            logger.info("Table rename %s -> %s already completed", source, target)
            return True, None
        except Exception as target_error:
            return False, f"Source table '{source}' and destination table '{target}' were not found: {target_error}"

    request = {
        "tableBucketARN": table_bucket_arn,
        "namespace": namespace,
        "name": source,
        "newName": target,
    }
    version_token = source_table.get("versionToken")
    if version_token:
        request["versionToken"] = version_token
    try:
        s3tables.rename_table(**request)
        logger.info("Renamed Iceberg table %s.%s -> %s", namespace, source, target)
        return True, None
    except Exception as error:
        return False, str(error)


def batch_upsert_tracker(catalog, table_name: str, rows: list[dict], max_retries: int = 6) -> None:
    """Write tracker rows for one table. See :func:`write_tracker_rows`."""
    write_tracker_rows(catalog, [{**r, "table_name": table_name} for r in rows], max_retries=max_retries)


def write_tracker_rows(catalog, rows: list[dict], max_retries: int = 6) -> None:
    """Replace tracker rows, keyed by ``(table_name, plan_name)``, in one commit.

    Rows may span several tables, so callers with many rows (e.g. checksum
    baselines) should batch them: each commit to the tracker costs several
    seconds on S3 Tables, and one commit per table made the first checksum
    baseline outlast the deploy job's timeout.

    Each row is written whole. Callers updating an existing row (e.g. healing a
    dead backfill) must pass its ``checksum`` and ``applied_at`` through or they
    are cleared/reset.
    """
    if not rows:
        return
    now = datetime.now(UTC).isoformat()
    schema = pa.schema(
        [
            pa.field("table_name", pa.string(), nullable=False),
            pa.field("plan_name", pa.string(), nullable=False),
            pa.field("status", pa.string(), nullable=False),
            pa.field("created_at", pa.string(), nullable=True),
            pa.field("applied_at", pa.string(), nullable=True),
            pa.field("notes", pa.string(), nullable=True),
            pa.field("checksum", pa.string(), nullable=True),
        ]
    )
    batch = pa.table(
        {
            "table_name": [r["table_name"] for r in rows],
            "plan_name": [r["plan_name"] for r in rows],
            "status": [r["status"] for r in rows],
            "created_at": [now] * len(rows),
            "applied_at": [r.get("applied_at") or (now if r["status"] == STATUS_APPLIED else "") for r in rows],
            "notes": [r.get("notes", "") or "" for r in rows],
            "checksum": [r.get("checksum") for r in rows],
        },
        schema=schema,
    )

    from pyiceberg.expressions import And, EqualTo, In, Or

    plans_by_table: dict[str, list[str]] = {}
    for r in rows:
        plans_by_table.setdefault(r["table_name"], []).append(r["plan_name"])
    per_table = [And(EqualTo("table_name", t), In("plan_name", plans)) for t, plans in plans_by_table.items()]
    key_filter = per_table[0] if len(per_table) == 1 else Or(*per_table)

    for attempt in range(max_retries + 1):
        try:
            fqn = f"{TRACKING_NAMESPACE}.{TRACKING_TABLE}"
            tbl = catalog.load_table(fqn)
            # Replace the keyed rows in one commit. Not ``upsert``: pyiceberg's
            # upsert reads existing rows with the current *snapshot's* schema,
            # so right after ensure_tracker adds a column (a metadata-only
            # change, no new snapshot) the read lacks it and the cast fails.
            # Rows are always written whole, so delete+append has the same result.
            tbl.overwrite(batch, overwrite_filter=key_filter)
            for r in rows:
                logger.info("Tracker: %s/%s -> %s", r["table_name"], r["plan_name"], r["status"])
            return
        except CommitFailedException:
            # Parallel table workers all commit to the one tracker table, so
            # optimistic-concurrency conflicts (S3 Tables 409) are expected.
            # Match on type: str(e) is only the server message, which does not
            # contain "CommitFailed". Jitter keeps workers from retrying in lockstep.
            if attempt >= max_retries:
                raise
            wait = 2**attempt + secrets.SystemRandom().uniform(0, 1)
            logger.warning("Tracker commit conflict, retrying in %.1fs (%d/%d)...", wait, attempt + 1, max_retries)
            time.sleep(wait)


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------


def apply_add_partition_field(table, change: dict) -> tuple[bool, str | None]:
    """Add a partition field to an Iceberg table."""
    from pyiceberg.transforms import (
        BucketTransform,
        DayTransform,
        HourTransform,
        IdentityTransform,
        MonthTransform,
        TruncateTransform,
        YearTransform,
    )

    column = change["column_name"]
    transform = change["transform"]
    param = change.get("transform_param")

    try:
        # Check if field already exists
        current_spec = get_table_partition_spec(table)
        key = (column.upper(), transform, param)
        existing = {(f["column"].upper(), f["transform"], f.get("transform_param")) for f in current_spec}
        if key in existing:
            logger.info("Partition field %s(%s) already exists, skipping", transform, column)
            return True, None

        with table.update_spec() as update:
            if transform == "identity":
                update.add_field(column, IdentityTransform(), f"{column}_identity")
            elif transform == "year":
                update.add_field(column, YearTransform(), f"{column}_year")
            elif transform == "month":
                update.add_field(column, MonthTransform(), f"{column}_month")
            elif transform == "day":
                update.add_field(column, DayTransform(), f"{column}_day")
            elif transform == "hour":
                update.add_field(column, HourTransform(), f"{column}_hour")
            elif transform == "bucket":
                update.add_field(column, BucketTransform(param), f"{column}_bucket")
            elif transform == "truncate":
                update.add_field(column, TruncateTransform(param), f"{column}_truncate")
            else:
                return False, f"Unsupported partition transform: {transform}"

        return True, None
    except Exception as e:
        return False, str(e)


def apply_drop_partition_field(table, change: dict) -> tuple[bool, str | None]:
    """Drop a partition field from an Iceberg table."""
    column = change["column_name"]
    transform = change["transform"]
    param = change.get("transform_param")

    try:
        current_spec = get_table_partition_spec(table)
        key = (column.upper(), transform, param)
        existing = {(f["column"].upper(), f["transform"], f.get("transform_param")) for f in current_spec}
        if key not in existing:
            logger.info("Partition field %s(%s) doesn't exist, skipping drop", transform, column)
            return True, None

        # Find the partition field to remove by matching source column
        with table.update_spec() as update:
            for field in table.spec().fields:
                source = table.schema().find_field(field.source_id)
                if source.name.upper() == column.upper():
                    update.remove_field(field.name)
                    break

        return True, None
    except Exception as e:
        return False, str(e)


def apply_set_sort_order(table, change: dict) -> tuple[bool, str | None]:
    """Set the write sort order on an Iceberg table."""
    sort_fields = change.get("sort_fields", [])

    try:
        # Check if already matches
        current = get_table_sort_order(table)
        desired_normalized = [(f["column"].upper(), f["direction"].lower()) for f in sort_fields]
        actual_normalized = [(f["column"].upper(), f["direction"].lower()) for f in current]
        if desired_normalized == actual_normalized:
            logger.info("Sort order already matches, skipping")
            return True, None

        from pyiceberg.transforms import IdentityTransform

        with table.update_sort_order() as update:
            for field in sort_fields:
                if field["direction"] == "asc":
                    update.asc(field["column"], IdentityTransform())
                else:
                    update.desc(field["column"], IdentityTransform())

        return True, None
    except Exception as e:
        return False, str(e)


def apply_set_table_properties(table, change: dict) -> tuple[bool, str | None]:
    """Set table properties on an Iceberg table."""
    properties = change.get("properties", {})
    if not properties:
        return True, None

    try:
        with table.transaction() as txn:
            txn.set_properties(properties)

        return True, None
    except Exception as e:
        return False, str(e)


def apply_unset_table_properties(table, change: dict) -> tuple[bool, str | None]:
    """Remove table properties from an Iceberg table."""
    keys = change.get("property_keys", [])
    if not keys:
        return True, None

    try:
        with table.transaction() as txn:
            txn.remove_properties(*keys)

        return True, None
    except Exception as e:
        return False, str(e)


def apply_ddl(catalog, namespace: str, table_name: str, change: dict) -> tuple[bool, str | None]:  # noqa: C901
    """Apply a single DDL schema change to an Iceberg table."""
    from pyiceberg.types import (
        BooleanType,
        DateType,
        DecimalType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        StringType,
        TimestampType,
        TimestamptzType,
    )

    # "timestamp" means timestamp *with* time zone, matching the rest of the
    # platform rather than pyiceberg's own naming.
    #
    # Everywhere else "timestamp" resolves to a tz-aware column: schema files
    # map it to Spark TimestampType, and Spark writes that to Iceberg as
    # timestamptz. Mapping it here to pyiceberg's TimestampType - which is
    # tz-*naive* - would make a migration-added column silently disagree with
    # every Spark-written column beside it, and with the schema that declared it.
    # Comparing a naive column to a tz-aware one makes Athena coerce using the
    # session time zone, so a join or filter silently shifts by the offset.
    #
    # timestamp_ntz remains available for a genuinely zone-less column.
    type_map = {
        "string": StringType,
        "long": LongType,
        "bigint": LongType,
        "int": IntegerType,
        "integer": IntegerType,
        "double": DoubleType,
        "float": FloatType,
        "boolean": BooleanType,
        # "date" is a first-class Gold contract type (schema_utils maps it to
        # Spark DateType), so it must be addressable here too. Without it an
        # add_column plan targeting a date column fails with "Unrecognized
        # type: date" - the gap went unnoticed because date had only ever
        # appeared as a current_type on the source side of a type change.
        "date": DateType,
        "timestamp": TimestamptzType,
        "timestamptz": TimestamptzType,
        "timestamp_tz": TimestamptzType,
        "timestamp_ntz": TimestampType,
    }

    def _to_iceberg_type(type_str: str):
        low = type_str.strip().lower()
        if low.startswith("decimal"):
            m = re.match(r"decimal\((\d+),(\d+)\)", low)
            if m:
                return DecimalType(int(m.group(1)), int(m.group(2)))
        cls = type_map.get(low)
        return cls() if cls else None

    def _resolve_col(schema_fields, col_name: str) -> str:
        """Case-insensitive column name resolution against actual Iceberg schema."""
        lookup = {f.name.lower(): f.name for f in schema_fields}
        return lookup.get(col_name.lower(), col_name)

    fqn = f"{namespace}.{table_name}"
    ct = change["change_type"]
    col = change["column_name"]

    try:
        table = catalog.load_table(fqn)
        real_col = _resolve_col(table.schema().fields, col)

        if ct == "add_column":
            iceberg_type = _to_iceberg_type(change["target_type"])
            if not iceberg_type:
                return False, f"Unrecognized type: {change['target_type']}"
            lookup = {f.name.lower() for f in table.schema().fields}
            if col.lower() in lookup:
                logger.info(" Column '%s' already exists — skipping add", col)
                return True, None
            with table.update_schema() as update:
                update.add_column(col, iceberg_type)
            return True, None
        elif ct == "drop_column":
            lookup = {f.name.lower() for f in table.schema().fields}
            if col.lower() not in lookup:
                logger.info(" Column '%s' already absent — skipping drop", col)
                return True, None
            with table.update_schema() as update:
                update.delete_column(real_col)
            return True, None
        elif ct == "rename_column":
            prev = change.get("previous_name", col)
            lookup = {f.name.lower() for f in table.schema().fields}
            # Already renamed — target exists
            if col.lower() in lookup:
                logger.info(" Column '%s' already exists — skipping rename", col)
                return True, None
            # Neither source nor target exists. This is normal when the table was
            # created from a newer contract (or a later plan drops the column),
            # so do not block the table - but return a note so the tracker
            # records that nothing was renamed instead of a silent success.
            if prev.lower() not in lookup:
                logger.warning(" Neither '%s' nor '%s' exists — rename skipped", prev, col)
                return True, f"{SKIPPED_PREFIX}neither '{prev}' nor '{col}' exists; nothing renamed"
            real_prev = _resolve_col(table.schema().fields, prev)
            with table.update_schema() as update:
                update.rename_column(real_prev, col)
            return True, None
        elif ct == "compatible_type_change":
            iceberg_type = _to_iceberg_type(change["target_type"])
            if not iceberg_type:
                return False, f"Unrecognized type: {change['target_type']}"
            # Already at target type — skip
            current = {f.name.lower(): str(f.field_type).lower() for f in table.schema().fields}
            from schema_evolution import types_match as _types_match

            if col.lower() in current and _types_match(current[col.lower()], change["target_type"]):
                logger.info(" Column '%s' already at target type '%s' — skipping", col, change["target_type"])
                return True, None
            with table.update_schema() as update:
                update.update_column(real_col, iceberg_type)
            return True, None
        elif ct == "incompatible_type_change":
            return False, "BACKFILL_NEEDED"
        elif ct == "set_column_nullability":
            target_not_null = change.get("target_not_null")
            if target_not_null is None:
                return False, "set_column_nullability requires 'target_not_null'"
            # Resolve current required state (idempotency check)
            current_required = {f.name.lower(): f.required for f in table.schema().fields}
            if col.lower() not in current_required:
                logger.info(" Column '%s' not found — skipping nullability change", col)
                return True, None
            if bool(current_required[col.lower()]) == bool(target_not_null):
                logger.info(
                    " Column '%s' already %s — skipping nullability change",
                    col,
                    "NOT NULL" if target_not_null else "nullable",
                )
                return True, None
            if target_not_null:
                # optional -> required is an incompatible change in Iceberg;
                # it asserts required without validating existing data files.
                with table.update_schema(allow_incompatible_changes=True) as update:
                    update.update_column(real_col, required=True)
            else:
                # required -> optional is always safe.
                with table.update_schema() as update:
                    update.make_column_optional(real_col)
            return True, None
        elif ct == "repartition":
            # A repartition change physically re-clusters existing data under a
            # new partition spec, so it is applied by the Glue rebuild job (like a
            # backfill), not metadata-only DDL. A metadata-only re-partition (no
            # data rewrite) is expressed instead as bare add/drop_partition_field
            # changes, handled below.
            return False, "BACKFILL_NEEDED"
        elif ct == "rebuild_table":
            logger.info("Standalone rebuild requested for %s.%s", namespace, table_name)
            return False, "BACKFILL_NEEDED"
        elif ct == "add_partition_field":
            return apply_add_partition_field(table, change)
        elif ct == "drop_partition_field":
            return apply_drop_partition_field(table, change)
        elif ct == "set_sort_order":
            if change.get("rebuild") is not False:
                logger.info(
                    "Sort-order rebuild requested for %s.%s; backfill will rewrite the table", namespace, table_name
                )
                return False, "BACKFILL_NEEDED"
            return apply_set_sort_order(table, change)
        elif ct == "set_table_properties":
            return apply_set_table_properties(table, change)
        elif ct == "unset_table_properties":
            return apply_unset_table_properties(table, change)
        else:
            return False, f"Unknown change type: {ct}"
    except Exception as e:
        return False, str(e)


# ---------------------------------------------------------------------------
# Glue Backfill Trigger
# ---------------------------------------------------------------------------


class PartitionSourceColumnError(RuntimeError):
    """Raised when a type-change backfill targets a partition-source column
    without specifying how to re-partition the new column.

    The backfill pattern renames the source column to a temp column and later
    drops the temp column. Iceberg forbids dropping a column that an active
    partition field references, which leaves the table half-migrated. A change
    on a partition-source column must therefore carry a 'new_partition'
    instruction so the tooling can drop the old partition field, swap the
    column, and re-add the partition on the new column.
    """


def partition_source_columns(table) -> set[str]:
    """Return the lower-cased set of column names referenced by any partition field."""
    return {f["column"].lower() for f in get_table_partition_spec(table)}


def check_backfill_partition_safety(table, changes: list) -> list[str]:
    """Return backfill columns that are partition sources but lack re-partitioning.

    An empty list means the backfill is safe to run. A non-empty list names the
    partition-source columns whose type change does not specify a 'new_partition'
    block; those must declare how to partition the new column before the
    rename-add-cast-drop backfill can succeed.
    """
    partition_cols = partition_source_columns(table)
    return [
        c["column_name"]
        for c in changes
        if c["change_type"] == "incompatible_type_change"
        and c["column_name"].lower() in partition_cols
        and not c.get("new_partition")
    ]


def trigger_backfill(
    glue,
    glue_job_name: str,
    changes: list,
    catalog_name: str,
    namespace: str,
    table_name: str,
    region: str,
    account_id: str,
    target_bucket: str,
    plan_name: str = "",
    table=None,
) -> str:
    """Trigger a Glue backfill job for the given schema changes.

    Raises PartitionSourceColumnError if a backfill column is an active partition
    source but the change does not specify a 'new_partition' re-partition
    instruction, so we never launch a job that would strand the table.
    """
    backfill_changes = [
        c
        for c in changes
        if c["change_type"] in ("incompatible_type_change", "repartition", "rebuild_table")
        or (c["change_type"] == "set_sort_order" and c.get("rebuild") is not False)
    ]

    if table is not None:
        for change in backfill_changes:
            if change["change_type"] == "rebuild_table" and "sort_fields" not in change:
                change["sort_fields"] = get_table_sort_order(table)
        blockers = check_backfill_partition_safety(table, backfill_changes)
        if blockers:
            raise PartitionSourceColumnError(
                "Cannot backfill partition-source column(s) "
                f"{sorted(blockers)} on {namespace}.{table_name}: they back an active "
                "partition field but the change does not specify a 'new_partition' "
                "instruction describing how to partition the new column."
            )

    response = glue.start_job_run(
        JobName=glue_job_name,
        Arguments={
            "--mode": "apply",
            "--catalog": catalog_name,
            "--database": namespace,
            "--table": table_name,
            "--changes_json": json.dumps(backfill_changes),
            "--region": region,
            "--account_id": account_id,
            "--target_bucket": target_bucket,
            "--tracker_table_name": table_name,
            "--tracker_plan_name": plan_name,
        },
    )
    run_id = response["JobRunId"]
    logger.info("Triggered Glue backfill: %s (run: %s)", glue_job_name, run_id)
    return run_id


def wait_for_table(catalog, table_name: str) -> bool:
    """Poll until all in-progress migrations for the table complete."""
    for attempt in range(WAIT_MAX_RETRIES):
        tracker = read_tracker(catalog, table_name)
        blocked = [p for p, info in tracker.items() if info["status"] in ("in-progress", "backfilling")]
        if not blocked:
            return True
        logger.info("Table '%s' blocked - waiting (%d/%d)...", table_name, attempt + 1, WAIT_MAX_RETRIES)
        time.sleep(WAIT_POLL_INTERVAL)
    return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


@dataclass
class ApplyResult:
    """Outcome of a single :func:`apply_migrations` pass.

    Attributes:
        success: True if no plan failed during the pass.
        pending_work: True if at least one table still has un-applied or
            in-flight (``backfilling``) plans that a later run will pick up.
            False means the tracker is fully drained -- nothing left to apply.
    """

    success: bool
    pending_work: bool


@dataclass
class _RunContext:
    """Shared, read-mostly state for one apply pass."""

    args: Any
    glue: Any
    plans_source: str
    from_s3: bool
    s3_client: Any
    catalogs: dict[str, Any] = field(default_factory=dict)
    # PyIceberg commits to the per-layer tracker table conflict when two
    # workers upsert at once. Serializing tracker writes per layer removes
    # those self-inflicted conflicts; the commit retry still covers writers
    # outside this process (the backfill job).
    tracker_locks: dict[str, threading.Lock] = field(default_factory=dict)


@dataclass
class _TableWork:
    """A table that has unsettled plans, computed before any table is loaded."""

    layer: str
    table_name: str
    plans: list[str]
    tracker: dict
    has_in_flight: bool


@dataclass
class _TableOutcome:
    """Result of processing one table."""

    success: bool = True
    pending_work: bool = False
    abort_message: str | None = None
    # "layer/table" when the table was skipped because its backfill is still running.
    still_backfilling: str | None = None


def _plan_path(plans_source: str, layer: str, table_name: str, plan_name: str) -> str:
    """Return the local path of a plan file, always scoped to its layer."""
    return os.path.join(plans_source, layer, table_name, plan_name)


def _read_plan_changes(ctx: _RunContext, layer: str, table_name: str, plan_name: str) -> list:
    """Read a plan's ``changes`` from the run's plan source (local or S3)."""
    if ctx.from_s3:
        return read_plan_json_s3(ctx.plans_source, layer, table_name, plan_name, ctx.s3_client)
    return read_plan_json_local(_plan_path(ctx.plans_source, layer, table_name, plan_name))


def _read_plan_meta(ctx: _RunContext, layer: str, table_name: str, plan_name: str) -> dict:
    """Read a plan's ``metadata`` from the run's plan source (local or S3)."""
    if ctx.from_s3:
        return read_plan_metadata_s3(ctx.plans_source, layer, table_name, plan_name, ctx.s3_client)
    return read_plan_metadata_local(_plan_path(ctx.plans_source, layer, table_name, plan_name))


def _validate_checksums(
    plans_source: str, layer: str, table_name: str, plan_files: list[str], tracker: dict
) -> tuple[list[str], list[dict]]:
    """Compare applied plans on disk against the checksums recorded at apply time.

    Applied rows written before checksums existed have none; they are returned
    as baseline rows that record the current file's checksum.

    Returns:
        ``(drifted, baseline_rows)``: descriptions of applied plans whose
        ``changes`` no longer match, and tracker rows to upsert as a baseline.
    """
    drifted: list[str] = []
    baseline: list[dict] = []
    for plan_name in plan_files:
        info = tracker.get(plan_name)
        if not info or info.get("status") != STATUS_APPLIED:
            continue
        try:
            actual = plan_checksum(read_plan_json_local(_plan_path(plans_source, layer, table_name, plan_name)))
        except Exception as exc:
            drifted.append(f"{layer}/{table_name}/{plan_name} (unreadable: {exc})")
            continue
        recorded = info.get("checksum")
        if not recorded:
            baseline.append(
                {
                    "plan_name": plan_name,
                    "status": STATUS_APPLIED,
                    "notes": info.get("notes") or "",
                    "applied_at": info.get("applied_at"),
                    "checksum": actual,
                }
            )
        elif recorded != actual:
            drifted.append(f"{layer}/{table_name}/{plan_name}")
    return drifted, baseline


def _record_waiting(ctx: _RunContext, work: _TableWork, reason: str) -> None:
    """Record ``waiting_for_table`` for plans whose table does not exist yet.

    Writes only rows whose status or reason changed, so a table that stays
    missing costs one tracker commit, not one per pass.
    """
    rows = [
        {"plan_name": plan_name, "status": STATUS_WAITING_FOR_TABLE, "notes": reason}
        for plan_name in work.plans
        if work.tracker.get(plan_name, {}).get("status") not in IN_FLIGHT_STATUSES
        and (
            work.tracker.get(plan_name, {}).get("status") != STATUS_WAITING_FOR_TABLE
            or work.tracker.get(plan_name, {}).get("notes") != reason
        )
    ]
    if rows:
        with ctx.tracker_locks[work.layer]:
            batch_upsert_tracker(ctx.catalogs[work.layer], work.table_name, rows)


def _missing_table_reason(ctx: _RunContext, work: _TableWork, namespace: str) -> str | None:
    """Return why the table cannot be migrated yet, or ``None`` if it can.

    Raises:
        ValueError: If a ``rename_table`` plan for this table is malformed.
    """
    catalog = ctx.catalogs[work.layer]
    try:
        catalog.load_table(f"{namespace}.{work.table_name}")
        return None
    except Exception:
        pass

    rename_source = _rename_source_for_plans(
        ctx.plans_source,
        work.layer,
        work.table_name,
        work.plans,
        from_s3=ctx.from_s3,
        s3_client=ctx.s3_client,
    )
    if rename_source is None:
        return f"Table '{namespace}.{work.table_name}' does not exist yet"
    try:
        catalog.load_table(f"{namespace}.{rename_source}")
        return None
    except Exception:
        return (
            f"Neither rename source '{namespace}.{rename_source}' nor destination "
            f"'{namespace}.{work.table_name}' exists"
        )


def _apply_plan_changes(  # noqa: C901
    ctx: _RunContext,
    work: _TableWork,
    namespace: str,
    target_bucket: str,
    plan_name: str,
    changes: list,
) -> tuple[dict, bool]:
    """Apply one plan's changes and return ``(tracker_row, stop_table)``."""
    args = ctx.args
    catalog = ctx.catalogs[work.layer]
    checksum = plan_checksum(changes)

    if not changes:
        return {"plan_name": plan_name, "status": STATUS_APPLIED, "notes": "Empty plan", "checksum": checksum}, False

    rename_changes = [c for c in changes if c.get("change_type") == "rename_table"]
    if rename_changes and (len(changes) != 1 or rename_changes[0].get("column_name") != "__table__"):
        row = {
            "plan_name": plan_name,
            "status": STATUS_FAILED,
            "notes": "rename_table must be the only change in its plan and use '__table__'",
            "checksum": checksum,
        }
        return row, True

    needs_backfill = False
    error_notes: list[str] = []
    skip_notes: list[str] = []

    for change in changes:
        ct = change["change_type"]
        col = change["column_name"]
        logger.info("    [%s] %s.%s %s", ct.upper(), work.layer, work.table_name, col)

        if ct == "rename_table":
            s3tables = boto3.client("s3tables", region_name=args.region)
            table_bucket_arn = f"arn:aws:s3tables:{args.region}:{args.account_id}:bucket/{target_bucket}"
            success, error = apply_table_rename(
                s3tables, table_bucket_arn, namespace, change.get("previous_name"), work.table_name
            )
        else:
            success, error = apply_ddl(catalog, namespace, work.table_name, change)

        if success:
            if error and error.startswith(SKIPPED_PREFIX):
                skip_notes.append(f"{col}: {error}")
            logger.info("    ok %s", col)
        elif error == "BACKFILL_NEEDED":
            logger.info("    -> backfill needed")
            needs_backfill = True
        else:
            logger.error("    x %s: %s", col, error)
            error_notes.append(f"{col}: {error}")

    if error_notes:
        return {
            "plan_name": plan_name,
            "status": STATUS_FAILED,
            "notes": "; ".join(error_notes),
            "checksum": checksum,
        }, True

    if needs_backfill:
        try:
            backfill_table = catalog.load_table(f"{namespace}.{work.table_name}")
            run_id = trigger_backfill(
                ctx.glue,
                args.glue_job_name,
                changes,
                "s3tables",
                namespace,
                work.table_name,
                args.region,
                args.account_id,
                target_bucket,
                plan_name=plan_name,
                table=backfill_table,
            )
            row = {
                "plan_name": plan_name,
                "status": "backfilling",
                "notes": f"Glue job run: {run_id}",
                "checksum": checksum,
            }
        except Exception as e:
            logger.error("  Backfill trigger failed: %s", e)
            row = {
                "plan_name": plan_name,
                "status": STATUS_FAILED,
                "notes": f"Backfill trigger failed: {e}",
                "checksum": checksum,
            }
        # A backfill locks the table until the Glue job finishes.
        return row, True

    return {
        "plan_name": plan_name,
        "status": STATUS_APPLIED,
        "notes": "; ".join(skip_notes),
        "checksum": checksum,
    }, False


def _process_table(ctx: _RunContext, work: _TableWork) -> _TableOutcome:  # noqa: C901
    """Apply the next actionable plans for one table, in filename order."""
    args = ctx.args
    layer, table_name = work.layer, work.table_name
    catalog = ctx.catalogs[layer]
    target_bucket = target_bucket_for_layer(layer, args.bucket_prefix, args.environment)
    namespace = f"{args.bucket_prefix}_{args.environment}_{layer}"
    outcome = _TableOutcome()

    logger.info("=" * 60)
    logger.info("Processing: %s/%s (%d unsettled plans)", layer, table_name, len(work.plans))

    try:
        reason = _missing_table_reason(ctx, work, namespace)
    except ValueError as exc:
        return _TableOutcome(success=False, abort_message=f"Invalid rename_table plan for {layer}/{table_name}: {exc}")
    if reason is not None:
        logger.warning("  %s - skipping", reason)
        _record_waiting(ctx, work, reason)
        return outcome

    tracker = work.tracker
    # A plan left at ``backfilling`` behind a Glue run that already died (e.g.
    # the job's own tracker update lost a commit race) would lock this table
    # forever. Reset such rows to ``failed`` so they are retried and the lock
    # wait below only waits on genuinely in-flight backfills.
    if work.has_in_flight:
        with ctx.tracker_locks[layer]:
            healed = _heal_dead_backfills(ctx.glue, catalog, args.glue_job_name, table_name, tracker)
        if healed:
            tracker = read_tracker(catalog, table_name)

    locked = any(info.get("status") in IN_FLIGHT_STATUSES for info in tracker.values())
    if locked:
        # A later run picks the table up once the Glue job reaches a terminal state.
        outcome.pending_work = True

    actionable = [pf for pf in work.plans if pf not in tracker or tracker[pf]["status"] in ACTIONABLE_STATUSES]
    if not actionable:
        logger.info("  No new or failed plans - skipping")
        return outcome

    # There is real work for this table; until it is all applied, signal that
    # another run may be needed.
    outcome.pending_work = True

    for plan_name in actionable:
        prior = tracker.get(plan_name)
        if prior and prior.get("status") == STATUS_FAILED and prior.get("notes"):
            logger.warning("  Retrying previously failed plan '%s'; prior failure: %s", plan_name, prior["notes"])

    if locked and not wait_for_table(catalog, table_name):
        # A backfill still running is normal, not a failure: leave the table's
        # remaining plans for the next run. Dead runs were already healed above,
        # so anything still in flight here is genuinely running.
        run_ids = [
            _extract_run_id(info.get("notes")) or "unknown"
            for info in read_tracker(catalog, table_name).values()
            if info.get("status") in IN_FLIGHT_STATUSES
        ]
        logger.warning(
            "  Backfill still running (Glue run %s) - %d remaining plan(s) apply on the next run",
            ", ".join(run_ids),
            len(actionable),
        )
        outcome.still_backfilling = f"{layer}/{table_name}"
        return outcome

    results: list[dict] = []
    for plan_name in actionable:
        logger.info("  Applying: %s/%s/%s", layer, table_name, plan_name)
        try:
            changes = _read_plan_changes(ctx, layer, table_name, plan_name)
        except FileNotFoundError:
            results.append({"plan_name": plan_name, "status": STATUS_FAILED, "notes": "Plan file not found"})
            outcome.success = False
            break
        except Exception as e:
            results.append({"plan_name": plan_name, "status": STATUS_FAILED, "notes": f"Plan read failed: {e}"})
            outcome.success = False
            break

        row, stop = _apply_plan_changes(ctx, work, namespace, target_bucket, plan_name, changes)
        results.append(row)
        if row["status"] == STATUS_FAILED:
            outcome.success = False
        if stop:
            # Later plans may depend on this one, so never skip past a failure
            # or an in-flight backfill.
            break

    if results:
        with ctx.tracker_locks[layer]:
            batch_upsert_tracker(catalog, table_name, results)
    return outcome


def _collect_table_work(
    ctx: _RunContext, all_plans: dict, checksum_mode: str
) -> tuple[list[_TableWork], list[str], str | None]:
    """Diff plan files against the tracker without loading any data table.

    Reads each layer's tracker once, keeps only plans that are not ``applied``
    (plus tables with in-flight backfills), and applies environment targeting
    to those plans only.

    Returns:
        ``(work_items, drifted_plans, abort_message)``.
    """
    snapshots: dict[str, dict] = {}
    work_items: list[_TableWork] = []
    drifted: list[str] = []
    # Checksum baselines are written once per layer (one commit each), not once
    # per table: on S3 Tables each tracker commit takes seconds.
    baselines: dict[str, list[dict]] = {}

    result = _collect_table_work_inner(ctx, all_plans, checksum_mode, snapshots, work_items, drifted, baselines)
    for layer, rows in baselines.items():
        if rows:
            logger.info("Recording baseline checksums for %d applied plan(s) in %s", len(rows), layer)
            write_tracker_rows(ctx.catalogs[layer], rows)
    return result


def _collect_table_work_inner(  # noqa: C901
    ctx: _RunContext,
    all_plans: dict,
    checksum_mode: str,
    snapshots: dict[str, dict],
    work_items: list[_TableWork],
    drifted: list[str],
    baselines: dict[str, list[dict]],
) -> tuple[list[_TableWork], list[str], str | None]:
    """Body of :func:`_collect_table_work`; appends to the passed collections."""
    args = ctx.args
    for (layer, table_name), plan_files in sorted(all_plans.items()):
        if layer not in ctx.catalogs:
            bucket = target_bucket_for_layer(layer, args.bucket_prefix, args.environment)
            catalog = load_catalog(args.region, args.account_id, bucket)
            ensure_tracker(catalog)
            ctx.catalogs[layer] = catalog
            ctx.tracker_locks[layer] = threading.Lock()
            snapshots[layer] = read_tracker_snapshot(catalog)
        catalog = ctx.catalogs[layer]
        tracker = read_tracker(catalog, table_name, snapshot=snapshots[layer])

        if checksum_mode != "off":
            table_drift, baseline = _validate_checksums(ctx.plans_source, layer, table_name, plan_files, tracker)
            drifted.extend(table_drift)
            baselines.setdefault(layer, []).extend({**row, "table_name": table_name} for row in baseline)

        unsettled = [pf for pf in plan_files if tracker.get(pf, {}).get("status") != STATUS_APPLIED]
        has_in_flight = any(info.get("status") in IN_FLIGHT_STATUSES for info in tracker.values())
        if not unsettled and not has_in_flight:
            continue

        # Skip plans that declare a different environment. A plan with no
        # environment key applies to every environment.
        applicable: list[str] = []
        for pf in unsettled:
            try:
                plan_meta = _read_plan_meta(ctx, layer, table_name, pf)
            except Exception as exc:
                return (
                    work_items,
                    drifted,
                    f"Plan '{pf}' for {layer}/{table_name} has unreadable metadata: {exc}. "
                    "Refusing to continue to avoid cross-environment writes.",
                )
            # Malformed env/layer metadata is fail-closed: we cannot know which
            # environment was intended, so refuse rather than guess.
            try:
                applies = plan_targets_environment(plan_meta, args.environment, layer)
            except ValueError as exc:
                return (
                    work_items,
                    drifted,
                    f"Plan '{pf}' for {layer}/{table_name} has invalid metadata: {exc}. "
                    "Refusing to continue to avoid cross-environment writes.",
                )
            if applies:
                applicable.append(pf)
            else:
                logger.debug(
                    "  Skipping %s - plan targets %s, current env is '%s'",
                    pf,
                    plan_meta.get("environments", plan_meta.get("environment")),
                    args.environment,
                )

        if not applicable and not has_in_flight:
            continue
        work_items.append(_TableWork(layer, table_name, sorted(applicable), tracker, has_in_flight))

    return work_items, drifted, None


def _run_pass(ctx: _RunContext, all_plans: dict, checksum_mode: str) -> tuple[bool, bool]:
    """Diff, validate and apply one pass. Returns ``(success, pending_work)``."""
    work_items, drifted, abort_message = _collect_table_work(ctx, all_plans, checksum_mode)
    if abort_message:
        logger.error(abort_message)
        return False, False

    if drifted:
        message = (
            f"{len(drifted)} applied plan(s) were edited after being applied: {', '.join(drifted)}. "
            "Applied plans are immutable: revert the edit and add a new plan instead. "
            f"To accept the edit, run once with {CHECKSUM_MODE_ENV_VAR}=warn."
        )
        if checksum_mode == "fail":
            logger.error(message)
            return False, False
        logger.warning(message)

    logger.info("Found plans for %d table(s); %d have unsettled plans", len(all_plans), len(work_items))

    max_workers = max(1, int(getattr(ctx.args, "max_workers", 1) or 1))
    if max_workers == 1 or len(work_items) <= 1:
        outcomes = [_process_table(ctx, work) for work in work_items]
    else:
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="schema-evo") as pool:
            outcomes = list(pool.map(lambda work: _process_table(ctx, work), work_items))

    success = True
    pending_work = False
    still_backfilling: list[str] = []
    for outcome in outcomes:
        if outcome.abort_message:
            logger.error(outcome.abort_message)
        success = success and outcome.success and outcome.abort_message is None
        pending_work = pending_work or outcome.pending_work
        if outcome.still_backfilling:
            still_backfilling.append(outcome.still_backfilling)
    if still_backfilling:
        logger.warning(
            "Backfill still running for %d table(s): %s. Their remaining plans apply on the next run.",
            len(still_backfilling),
            ", ".join(sorted(still_backfilling)),
        )
    return success, pending_work


_LOCK_BUSY = object()


def _acquire_run_lock(args, s3_client) -> "S3Lock | object | None":
    """Take the per-environment run lock named by ``args.lock_uri``.

    Returns the held :class:`S3Lock`, ``None`` when locking is disabled (no
    ``lock_uri``), or ``_LOCK_BUSY`` when another live run holds it.
    """
    lock_uri = getattr(args, "lock_uri", None)
    if not lock_uri:
        return None
    client = s3_client or boto3.client("s3", region_name=args.region)
    lock = S3Lock.create(
        client,
        lock_uri,
        owner=str(getattr(args, "lock_owner", None) or "schema-evolution-apply"),
        ttl_seconds=int(getattr(args, "lock_ttl_seconds", None) or DEFAULT_TTL_SECONDS),
    )
    wait_seconds = float(getattr(args, "lock_wait_seconds", 0) or 0)
    try:
        lock.acquire(wait_seconds=wait_seconds)
    except LockNotAcquiredError as exc:
        logger.error("%s after waiting %.0fs", exc, wait_seconds)
        return _LOCK_BUSY
    return lock


def apply_migrations(args, return_details: bool = False) -> bool | ApplyResult:
    """Apply all pending schema migrations for the configured environment.

    A pass works like a traditional migration runner (Flyway, Alembic):

    1. List plan files and read each layer's tracker once.
    2. Diff the two. Tables whose plans are all ``applied`` are skipped without
       loading the table or reading any plan file.
    3. Optionally validate that applied plans were not edited (checksums).
    4. Process the remaining tables, up to ``args.max_workers`` at a time. Plans
       for one table always run in filename order on a single worker.

    Args:
        args: Parsed CLI/namespace args (region, account_id, migrations_dir,
            environment, bucket_prefix, glue_job_name, and optionally
            max_workers and checksum_mode). ``migrations_dir`` may be a local
            path or an ``s3://`` prefix.
        return_details: When True, return an :class:`ApplyResult` (success +
            pending_work). When False (default, preserves the original CLI
            contract), return just the ``success`` bool.

    A table whose backfill is still running is polled for up to
    ``WAIT_POLL_INTERVAL * WAIT_MAX_RETRIES`` seconds. If it is still running
    after that, the table is skipped without counting as a failure; its
    remaining plans apply on the next run.
    """
    overall_start = time.time()

    # Plans come from either a local checkout or an s3:// prefix. Both yield the
    # same grouped, sorted {(layer, table): [plan_names]} structure.
    plans_source = args.migrations_dir
    from_s3 = is_s3_uri(plans_source)
    s3_client = boto3.client("s3", region_name=args.region) if from_s3 else None
    all_plans = list_plan_files_s3(plans_source, s3_client) if from_s3 else list_plan_files(plans_source)

    if not all_plans:
        logger.info("No migration plans found")
        return ApplyResult(success=True, pending_work=False) if return_details else True

    # Checksums are validated against a local checkout only, so a pass reading
    # plans from S3 does not GET every plan file.
    checksum_mode = "off" if from_s3 else getattr(args, "checksum_mode", "off")
    if checksum_mode not in CHECKSUM_MODES:
        raise ValueError(f"checksum_mode must be one of {CHECKSUM_MODES}, got {checksum_mode!r}")

    def finish(success: bool, pending_work: bool) -> bool | ApplyResult:
        logger.info("Total: %.2fs", time.time() - overall_start)
        return ApplyResult(success=success, pending_work=pending_work) if return_details else success

    lock = _acquire_run_lock(args, s3_client)
    if lock is _LOCK_BUSY:
        # Another run held the lock for the whole wait; fail so the deploy shows it.
        return finish(False, True)
    heartbeat = lock.start_heartbeat() if isinstance(lock, S3Lock) else None

    try:
        ctx = _RunContext(
            args=args,
            glue=boto3.client("glue", region_name=args.region),
            plans_source=plans_source,
            from_s3=from_s3,
            s3_client=s3_client,
        )
        return finish(*_run_pass(ctx, all_plans, checksum_mode))
    finally:
        if isinstance(lock, S3Lock):
            if heartbeat is not None:
                heartbeat.set()
            lock.release()


_TERMINAL_GLUE_STATES = {"FAILED", "TIMEOUT", "STOPPED", "ERROR"}
_RUN_ID_RE = re.compile(r"Glue job run:\s*(\S+)")


def _extract_run_id(notes: str | None) -> str | None:
    """Pull the Glue ``JobRunId`` recorded in a tracker note when the backfill started."""
    if not notes:
        return None
    m = _RUN_ID_RE.search(notes)
    return m.group(1) if m else None


def _heal_dead_backfills(glue, catalog, glue_job_name: str, table_name: str, tracker: dict) -> bool:
    """Reset ``backfilling`` rows whose Glue run has terminally died to ``failed``.

    Detection uses the ground truth of the Glue run state (``get_job_run``), not a
    single event, so it also catches STOPPED runs and missed/throttled events. A
    row is only reset when the run is terminal-failed AND the tracker was never
    moved off ``backfilling`` (idempotent: if the job already recorded ``failed``
    or ``applied``, we do nothing).

    Returns True if any row was reset.
    """
    resets: list[dict] = []
    for plan_name, info in tracker.items():
        if info.get("status") not in ("backfilling", "in-progress"):
            continue
        run_id = _extract_run_id(info.get("notes"))
        if not run_id:
            # No run id to check; leave it for the bounded wait to time out.
            continue
        try:
            run = glue.get_job_run(JobName=glue_job_name, RunId=run_id)["JobRun"]
        except Exception as e:
            logger.warning("Could not fetch Glue run %s for %s/%s: %s", run_id, table_name, plan_name, e)
            continue

        state = run.get("JobRunState")
        if state in _TERMINAL_GLUE_STATES:
            err = run.get("ErrorMessage") or "Glue run ended without recording a tracker status"
            logger.warning(
                "Healing stuck plan %s/%s: Glue run %s is %s -> resetting tracker to 'failed'",
                table_name,
                plan_name,
                run_id,
                state,
            )
            resets.append(
                {
                    "plan_name": plan_name,
                    "status": "failed",
                    "notes": f"Auto-reset by schema-evolution-apply: Glue run {run_id} {state}: {err}",
                    # Upsert rewrites the row; keep the recorded checksum.
                    "checksum": info.get("checksum"),
                }
            )

    if resets:
        batch_upsert_tracker(catalog, table_name, resets)
        return True
    return False


def _build_args_from_argv(argv=None):
    """Parse CLI args for the apply entrypoint."""
    parser = argparse.ArgumentParser(description="Apply schema migrations (batch upsert)")
    parser.add_argument("--region", default="us-west-2")
    parser.add_argument("--account_id", required=True)
    parser.add_argument(
        "--migrations_dir",
        default="migrations/",
        help="Local migrations directory or an s3:// prefix (e.g. s3://amzn-s3-demo-bucket/migrations).",
    )
    parser.add_argument("--environment", required=True)
    parser.add_argument("--bucket_prefix", default="dp")
    parser.add_argument(
        "--glue_job_name",
        default=None,
        help="Backfill Glue job name (default: {bucket_prefix}-{environment}-glue-schema-evolution-backfill)",
    )
    parser.add_argument(
        "--max_workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help="Tables processed concurrently. Plans for one table always run in order.",
    )
    parser.add_argument(
        "--checksum_mode",
        choices=CHECKSUM_MODES,
        default=os.environ.get(CHECKSUM_MODE_ENV_VAR, "fail"),
        help=(
            "What to do when an applied plan's changes were edited: fail (default), "
            f"warn, or off. Defaults to ${CHECKSUM_MODE_ENV_VAR} when set. "
            "Only checked for local plan directories."
        ),
    )
    parser.add_argument(
        "--lock_uri",
        default=None,
        help=(
            "s3:// object used as the per-environment run lock (default: "
            "s3://{bucket_prefix}-{environment}-schema-evolution-{account_id}-{region}/locks/{environment}.lock). "
            "Pass 'none' to disable."
        ),
    )
    parser.add_argument(
        "--lock_wait_seconds",
        type=float,
        default=900,
        help="How long to wait for another run's lock before failing (default 900).",
    )
    args = parser.parse_args(argv)
    if args.lock_uri is None:
        args.lock_uri = lock_uri_for(args.account_id, args.region, args.environment, args.bucket_prefix)
    elif args.lock_uri.lower() == "none":
        args.lock_uri = None
    args.lock_owner = "schema-evolution-cli"
    if args.glue_job_name is None:
        args.glue_job_name = f"{args.bucket_prefix}-{args.environment}-glue-schema-evolution-backfill"
    return args


def main():
    """CLI entrypoint for a single apply pass (deploy-time; local plan files)."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s")
    args = _build_args_from_argv()

    logger.info("Schema Evolution — Apply Migrations")
    success = apply_migrations(args)
    if not success:
        logger.error("Some migrations failed")
        exit(1)
    logger.info("All migrations applied successfully")


if __name__ == "__main__":
    main()
