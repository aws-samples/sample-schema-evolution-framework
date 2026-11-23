"""Keep examples/ valid: every sample plan and schema file must parse."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from schema_evolution import ChangeType
from schema_evolution.apply import plan_targets_environment
from schema_evolution.config_sync import _normalize_field
from schema_evolution.plan import classify_changes, parse_partition_spec, parse_sort_order

EXAMPLES = Path(__file__).resolve().parents[2] / "examples"
PLANS = sorted((EXAMPLES / "migrations").glob("*/*/*.json"))
VALID_TYPES = {c.value for c in ChangeType}


@pytest.mark.parametrize("plan_path", PLANS, ids=lambda p: f"{p.parent.parent.name}/{p.parent.name}/{p.name}")
def test_example_plan_is_valid(plan_path: Path) -> None:
    layer, table = plan_path.parent.parent.name, plan_path.parent.name
    plan = json.loads(plan_path.read_text())
    assert plan["metadata"]["table"] == table
    # Raises on a layer mismatch or malformed environments.
    plan_targets_environment(plan["metadata"], "dev", layer)
    assert plan["changes"], "plan has no changes"
    for change in plan["changes"]:
        assert change["change_type"] in VALID_TYPES
        assert change["column_name"]


def test_example_plans_exist() -> None:
    assert len(PLANS) >= 4


def test_example_environment_gate() -> None:
    plan = json.loads((EXAMPLES / "migrations/silver/demo_customers/20260103T090000_beta_segment_dev_qa.json").read_text())
    assert plan_targets_environment(plan["metadata"], "dev", "silver")
    assert not plan_targets_environment(plan["metadata"], "prod", "silver")


def test_example_schema_file_classifies_against_an_older_table() -> None:
    data = yaml.safe_load((EXAMPLES / "config/silver/crm/schemas/demo_customers.yaml").read_text())
    fields = [_normalize_field(f) for f in data["fields"]]
    live_table = {
        "customer_id": "long",
        "full_name": "string",
        "email_host": "string",
        "lifetime_value": "decimal(18,2)",
        "signup_date": "date",
        "beta_segment": "string",
        "legacy_score": "double",
    }
    changes = {(c.change_type, c.column_name) for c in classify_changes(fields, live_table, layer="silver")}
    assert (ChangeType.ADD_COLUMN, "loyalty_tier") in changes
    assert (ChangeType.RENAME_COLUMN, "email_domain") in changes
    assert (ChangeType.COMPATIBLE_TYPE_CHANGE, "lifetime_value") in changes
    assert (ChangeType.INCOMPATIBLE_TYPE_CHANGE, "signup_date") in changes
    assert (ChangeType.DROP_COLUMN, "legacy_score") in changes
    assert parse_partition_spec(data)[0]["transform"] == "month"
    assert parse_sort_order(data) == [{"column": "customer_id", "direction": "asc"}]


TOOLKIT = EXAMPLES.parent
STARTER = EXAMPLES / "project-repo"


@pytest.mark.parametrize(
    ("template", "copy"),
    [
        ("ci/github/schema-migrations.yml", ".github/workflows/schema-migrations.yml"),
        ("ci/github/schema-migrations-apply.yml", ".github/workflows/schema-migrations-apply.yml"),
        ("ci/gitlab/schema-migrations.gitlab-ci.yml", "ci/schema-migrations.gitlab-ci.yml"),
    ],
)
def test_starter_pipeline_files_match_ci_templates(template: str, copy: str) -> None:
    """examples/project-repo/ must ship the same pipeline files as ci/."""
    assert (STARTER / copy).read_text() == (TOOLKIT / template).read_text(), f"update {STARTER / copy} from {template}"


def test_starter_includes_gitlab_pipeline_and_empty_layers() -> None:
    root = yaml.safe_load((STARTER / ".gitlab-ci.yml").read_text())
    assert {"local": "ci/schema-migrations.gitlab-ci.yml"} in root["include"]
    for layer in ("bronze", "silver", "gold"):
        assert [p.name for p in (STARTER / "migrations" / layer).iterdir()] == [".gitkeep"]
