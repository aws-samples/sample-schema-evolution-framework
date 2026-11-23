"""Offline validation of migration plan files, for pull/merge request checks.

Needs no AWS access. Catches the mistakes that would otherwise only surface when
``schema-evolution-apply`` runs against an environment:

- files outside ``migrations/<layer>/<table>/<name>.json`` or with an unknown layer
- invalid JSON, a ``metadata.table`` that does not match the directory, or a
  malformed ``metadata.layer`` / ``metadata.environments``
- an empty or missing ``changes`` list, unknown ``change_type`` values, missing
  required keys, unrecognized types or partition transforms
- the same column changed twice in one plan, or ``rename_table`` mixed with
  other changes
- with ``--base_ref``: plans that exist on the base branch and were **edited**
  (their ``changes`` checksum differs) or **deleted**. Merged plans may already
  be applied somewhere and must never change; add a new plan instead.

Usage::

    schema-evolution-validate --migrations_dir migrations/ --base_ref origin/main

Exit code 0 when every plan is valid, 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess  # nosec B404 - fixed git argument lists, no shell
import sys
from dataclasses import dataclass, field

from schema_evolution import PARAMETERIZED_TRANSFORMS, VALID_TRANSFORMS, ChangeType
from schema_evolution.apply import plan_checksum, plan_targets_environment

logger = logging.getLogger(__name__)

LAYERS = ("bronze", "silver", "gold")
TIMESTAMP_PREFIX = re.compile(r"^\d{8}T\d{6}_")
TABLE_PSEUDO_COLUMNS = {"__table__", "__sort_order__", "__table_properties__"}

# Types apply_ddl and the backfill job understand.
_SIMPLE_TYPES = {
    "string",
    "int",
    "integer",
    "long",
    "bigint",
    "float",
    "double",
    "boolean",
    "date",
    "timestamp",
    "timestamptz",
    "timestamp_tz",
    "timestamp_ntz",
}
_DECIMAL = re.compile(r"^decimal\(\s*\d+\s*,\s*\d+\s*\)$")

# change_type -> keys that must be present (besides change_type / column_name).
REQUIRED_KEYS: dict[str, tuple[str, ...]] = {
    "add_column": ("target_type",),
    "drop_column": (),
    "rename_column": ("previous_name",),
    "compatible_type_change": ("target_type",),
    "incompatible_type_change": ("current_type", "target_type"),
    "set_column_nullability": ("target_not_null",),
    "add_partition_field": ("transform",),
    "drop_partition_field": ("transform",),
    "repartition": ("new_partition",),
    "rebuild_table": (),
    "set_sort_order": ("sort_fields",),
    "set_table_properties": ("properties",),
    "unset_table_properties": ("property_keys",),
    "rename_table": ("previous_name",),
}
PSEUDO_COLUMN_FOR: dict[str, str] = {
    "rebuild_table": "__table__",
    "rename_table": "__table__",
    "set_sort_order": "__sort_order__",
    "set_table_properties": "__table_properties__",
    "unset_table_properties": "__table_properties__",
}


@dataclass
class ValidationReport:
    """Errors (fail the check) and warnings (printed only)."""

    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    plans_checked: int = 0

    @property
    def ok(self) -> bool:
        return not self.errors


def is_valid_type(type_str: object) -> bool:
    """Return True if *type_str* is a type the toolkit can create."""
    if not isinstance(type_str, str):
        return False
    low = type_str.strip().lower()
    return low in _SIMPLE_TYPES or bool(_DECIMAL.match(low))


def _check_transform(where: str, transform: object, param: object, report: ValidationReport) -> None:
    if not isinstance(transform, str) or transform.lower() not in VALID_TRANSFORMS:
        report.errors.append(f"{where}: unknown partition transform {transform!r} (valid: {sorted(VALID_TRANSFORMS)})")
        return
    if transform.lower() in PARAMETERIZED_TRANSFORMS and not isinstance(param, int):
        report.errors.append(f"{where}: transform '{transform}' needs an integer parameter")


def _check_change(where: str, change: object, report: ValidationReport) -> str | None:
    """Validate one change. Returns its column name (lower-cased) or None."""
    if not isinstance(change, dict):
        report.errors.append(f"{where}: each change must be a JSON object")
        return None
    ct = change.get("change_type")
    col = change.get("column_name")
    if ct not in {c.value for c in ChangeType}:
        report.errors.append(f"{where}: unknown change_type {ct!r}")
        return None
    if not isinstance(col, str) or not col.strip():
        report.errors.append(f"{where}: [{ct}] missing column_name")
        return None

    for key in REQUIRED_KEYS[ct]:
        if change.get(key) in (None, "", [], {}):
            report.errors.append(f"{where}: [{ct}] {col}: missing '{key}'")

    pseudo = PSEUDO_COLUMN_FOR.get(ct)
    if pseudo and col != pseudo:
        report.errors.append(f"{where}: [{ct}] column_name must be '{pseudo}', got {col!r}")
    if not pseudo and col in TABLE_PSEUDO_COLUMNS:
        report.errors.append(f"{where}: [{ct}] {col!r} is reserved for table-level changes")

    for key in ("target_type", "current_type"):
        if change.get(key) is not None and not is_valid_type(change[key]):
            report.errors.append(f"{where}: [{ct}] {col}: unrecognized {key} {change[key]!r}")

    if ct in ("add_partition_field", "drop_partition_field") and change.get("transform"):
        _check_transform(f"{where}: [{ct}] {col}", change["transform"], change.get("transform_param"), report)
    new_partition = change.get("new_partition")
    if new_partition is not None:
        if not isinstance(new_partition, dict):
            report.errors.append(f"{where}: [{ct}] {col}: new_partition must be an object")
        else:
            _check_transform(
                f"{where}: [{ct}] {col}: new_partition",
                new_partition.get("transform", "identity"),
                new_partition.get("param"),
                report,
            )
    if ct == "set_column_nullability" and not isinstance(change.get("target_not_null"), bool):
        report.errors.append(f"{where}: [{ct}] {col}: target_not_null must be true or false")
    if ct == "set_sort_order":
        for sf in change.get("sort_fields") or []:
            if not isinstance(sf, dict) or not sf.get("column") or str(sf.get("direction", "")).lower() not in (
                "asc",
                "desc",
            ):
                report.errors.append(f"{where}: [{ct}] sort_fields entries need 'column' and direction asc|desc")
                break
    if ct == "rebuild_table" and change.get("target_format_version") not in (None, "2", "3", 2, 3):
        report.errors.append(f"{where}: [{ct}] target_format_version must be '2' or '3'")
    return col.lower()


def validate_plan(path: str, layer: str, table: str, report: ValidationReport) -> None:
    """Validate one plan file located at ``<migrations>/<layer>/<table>/``."""
    where = path
    name = os.path.basename(path)
    if not TIMESTAMP_PREFIX.match(name):
        report.warnings.append(
            f"{where}: name should start with a UTC timestamp (YYYYMMDDTHHMMSS_) so plans apply in a clear order"
        )
    try:
        with open(path) as f:
            plan = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        report.errors.append(f"{where}: not valid JSON ({exc})")
        return
    if not isinstance(plan, dict):
        report.errors.append(f"{where}: top level must be a JSON object with 'metadata' and 'changes'")
        return

    metadata = plan.get("metadata")
    if not isinstance(metadata, dict):
        report.errors.append(f"{where}: missing 'metadata' object")
        metadata = {}
    if metadata.get("table") != table:
        report.errors.append(f"{where}: metadata.table {metadata.get('table')!r} must equal its directory '{table}'")
    try:
        plan_targets_environment(metadata, "dev", layer)
    except ValueError as exc:
        report.errors.append(f"{where}: {exc}")

    changes = plan.get("changes")
    if not isinstance(changes, list) or not changes:
        report.errors.append(f"{where}: 'changes' must be a non-empty list")
        return

    seen: set[str] = set()
    for idx, change in enumerate(changes):
        col = _check_change(f"{where} changes[{idx}]", change, report)
        if col is None:
            continue
        if col in seen:
            report.errors.append(f"{where}: column '{col}' is changed more than once; use one change per column")
        seen.add(col)

    if any(isinstance(c, dict) and c.get("change_type") == "rename_table" for c in changes) and len(changes) != 1:
        report.errors.append(f"{where}: rename_table must be the only change in its plan")
    report.plans_checked += 1


def _git(*args: str) -> str | None:
    try:
        return subprocess.run(  # nosec B603 B607
            ["git", *args], check=True, capture_output=True, text=True
        ).stdout
    except (OSError, subprocess.CalledProcessError):
        return None


def check_against_base(migrations_dir: str, base_ref: str, report: ValidationReport) -> None:
    """Fail if a plan that exists on *base_ref* was edited or deleted."""
    if _git("rev-parse", "--verify", f"{base_ref}^{{commit}}") is None:
        report.errors.append(
            f"--base_ref {base_ref!r} is not a known commit. Fetch it first "
            "(for example 'git fetch origin main', or use fetch-depth: 0 in CI)."
        )
        return
    toplevel = (_git("rev-parse", "--show-toplevel") or "").strip()
    if not toplevel:
        report.errors.append("--base_ref needs a git checkout, but this is not a git repository")
        return
    rel_dir = os.path.relpath(os.path.realpath(migrations_dir), os.path.realpath(toplevel))
    listing = _git("ls-tree", "-r", "--name-only", base_ref, "--", rel_dir) or ""
    for rel_path in sorted(p for p in listing.splitlines() if p.endswith(".json")):
        local = os.path.join(toplevel, rel_path)
        if not os.path.isfile(local):
            report.errors.append(
                f"{rel_path}: deleted, but it exists on {base_ref}. A merged plan may already be applied; keep it"
            )
            continue
        base_content = _git("show", f"{base_ref}:{rel_path}")
        try:
            base_changes = json.loads(base_content or "").get("changes", [])
            with open(local) as f:
                head_changes = json.load(f).get("changes", [])
        except (json.JSONDecodeError, OSError, AttributeError):
            continue  # invalid JSON is reported by validate_plan
        if plan_checksum(base_changes) != plan_checksum(head_changes):
            report.errors.append(
                f"{rel_path}: 'changes' edited, but the plan exists on {base_ref}. "
                "Merged plans are immutable: revert the edit and add a new plan"
            )


def validate_migrations(migrations_dir: str, base_ref: str | None = None) -> ValidationReport:
    """Validate every plan under *migrations_dir*."""
    report = ValidationReport()
    if not os.path.isdir(migrations_dir):
        report.errors.append(f"{migrations_dir}: directory not found")
        return report

    for root, _dirs, files in os.walk(migrations_dir):
        for name in sorted(files):
            if name.startswith("."):
                continue
            path = os.path.join(root, name)
            parts = os.path.relpath(path, migrations_dir).split(os.sep)
            if not name.endswith(".json"):
                report.warnings.append(f"{path}: not a .json file; ignored by apply")
                continue
            if len(parts) != 3:
                report.errors.append(f"{path}: plans must live at <layer>/<table>/<name>.json")
                continue
            layer, table, _ = parts
            if layer not in LAYERS:
                report.errors.append(f"{path}: unknown layer '{layer}' (valid: {', '.join(LAYERS)})")
                continue
            validate_plan(path, layer, table, report)

    if base_ref:
        check_against_base(migrations_dir, base_ref, report)
    return report


def main(argv: list[str] | None = None) -> None:
    """CLI entrypoint."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description="Validate migration plan files (offline, no AWS access).")
    parser.add_argument("--migrations_dir", default="migrations/")
    parser.add_argument(
        "--base_ref",
        default=None,
        help="Git ref of the target branch (e.g. origin/main). Fails if plans that exist there were edited or deleted.",
    )
    args = parser.parse_args(argv)

    report = validate_migrations(args.migrations_dir, args.base_ref)
    for w in report.warnings:
        logger.warning("WARNING %s", w)
    for e in report.errors:
        logger.error("ERROR   %s", e)
    logger.info(
        "Checked %d plan(s): %d error(s), %d warning(s)", report.plans_checked, len(report.errors), len(report.warnings)
    )
    sys.exit(0 if report.ok else 1)


if __name__ == "__main__":
    main()
