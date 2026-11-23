"""Synth-level checks for infra/stacks/schema_evolution_stack.py."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

cdk = pytest.importorskip("aws_cdk")
from aws_cdk import assertions  # noqa: E402

_INFRA = Path(__file__).resolve().parents[2] / "infra"
if str(_INFRA) not in sys.path:
    sys.path.insert(0, str(_INFRA))

from stacks.schema_evolution_stack import (  # noqa: E402
    SchemaEvolutionConfig,
    SchemaEvolutionStack,
)

ACCOUNT = "333333333333"
REGION = "us-west-2"


def _template(env: str = "prod", **overrides) -> assertions.Template:
    app = cdk.App()
    stack = SchemaEvolutionStack(
        app,
        f"dp-{env}-schema-evolution",
        config=SchemaEvolutionConfig(env_name=env, **overrides),
        env=cdk.Environment(account=ACCOUNT, region=REGION),
    )
    return assertions.Template.from_stack(stack)


def _applier_statements(t: assertions.Template) -> dict[str, dict]:
    (policy,) = t.find_resources(
        "AWS::IAM::ManagedPolicy",
        {"Properties": {"ManagedPolicyName": "dp-prod-schema-evolution-applier"}},
    ).values()
    return {st["Sid"]: st for st in policy["Properties"]["PolicyDocument"]["Statement"]}


def test_no_lambda_or_eventbridge() -> None:
    t = _template()
    t.resource_count_is("AWS::Lambda::Function", 0)
    t.resource_count_is("AWS::Events::Rule", 0)


def test_artifacts_bucket_is_private_and_encrypted() -> None:
    _template().has_resource_properties(
        "AWS::S3::Bucket",
        {
            "BucketName": f"dp-prod-schema-evolution-{ACCOUNT}-{REGION}",
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True,
                "BlockPublicPolicy": True,
                "IgnorePublicAcls": True,
                "RestrictPublicBuckets": True,
            },
            "BucketEncryption": assertions.Match.any_value(),
            "VersioningConfiguration": {"Status": "Enabled"},
        },
    )


def test_both_glue_jobs_are_spark_with_iceberg() -> None:
    t = _template()
    jobs = t.find_resources("AWS::Glue::Job")
    names = sorted(j["Properties"]["Name"] for j in jobs.values())
    assert names == [
        "dp-prod-glue-schema-evolution-backfill",
        "dp-prod-glue-schema-evolution-backfill-env-columns",
    ]
    for job in jobs.values():
        props = job["Properties"]
        assert props["Command"]["Name"] == "glueetl"
        assert props["DefaultArguments"]["--datalake-formats"] == "iceberg"
        assert "IcebergSparkSessionExtensions" in props["DefaultArguments"]["--conf"]
        assert "--extra-py-files" in props["DefaultArguments"]


def test_applier_policy_scopes() -> None:
    sts = _applier_statements(_template())
    assert set(sts) == {"S3TablesApply", "GlueBackfillControl", "MigrationRunLock"}
    resources = sts["S3TablesApply"]["Resource"]
    for layer in ("bronze", "silver", "gold"):
        assert f"arn:aws:s3tables:{REGION}:{ACCOUNT}:bucket/dp-prod-{layer}-tables" in resources
    assert "s3tables:RenameTable" in sts["S3TablesApply"]["Action"]
    assert sts["GlueBackfillControl"]["Resource"] == (
        f"arn:aws:glue:{REGION}:{ACCOUNT}:job/dp-prod-glue-schema-evolution-backfill"
    )


def test_lock_permission_matches_cli_default() -> None:
    """The policy must grant exactly the lock object the CLI uses by default."""
    from schema_evolution.lock import lock_uri_for

    lock_key = lock_uri_for(ACCOUNT, REGION, "prod").removeprefix(f"s3://dp-prod-schema-evolution-{ACCOUNT}-{REGION}/")
    resource = _applier_statements(_template())["MigrationRunLock"]["Resource"]
    assert resource["Fn::Join"][1][-1] == f"/{lock_key}"


def test_kms_statement_only_when_keys_given() -> None:
    assert "S3TablesKms" not in _applier_statements(_template())
    key = f"arn:aws:kms:{REGION}:{ACCOUNT}:key/00000000-0000-0000-0000-000000000000"
    assert "S3TablesKms" in _applier_statements(_template(table_bucket_kms_key_arns=[key]))


def test_custom_prefix_flows_everywhere() -> None:
    t = _template(prefix="acme")
    t.has_resource_properties("AWS::Glue::Job", {"Name": "acme-prod-glue-schema-evolution-backfill"})
    t.has_resource_properties("AWS::IAM::ManagedPolicy", {"ManagedPolicyName": "acme-prod-schema-evolution-applier"})


def test_buckets_deny_insecure_transport_with_boolean_condition() -> None:
    """Both buckets deny non-TLS requests, with the condition as a JSON boolean:
    cfn-guard's S3_BUCKET_SSL_REQUESTS_ONLY doesn't accept the string "false"."""
    policies = _template().find_resources("AWS::S3::BucketPolicy")
    assert len(policies) == 2
    for policy in policies.values():
        denies = [s for s in policy["Properties"]["PolicyDocument"]["Statement"] if s["Effect"] == "Deny"]
        assert [s["Condition"] for s in denies] == [{"Bool": {"aws:SecureTransport": False}}]


def test_artifacts_bucket_logs_to_access_logs_bucket() -> None:
    t = _template()
    buckets = t.find_resources("AWS::S3::Bucket")
    assert len(buckets) == 2
    (logs_id,) = [k for k, v in buckets.items() if "BucketName" not in v["Properties"]]
    t.has_resource_properties(
        "AWS::S3::Bucket",
        {
            "BucketName": f"dp-prod-schema-evolution-{ACCOUNT}-{REGION}",
            "LoggingConfiguration": {"DestinationBucketName": {"Ref": logs_id}, "LogFilePrefix": "artifacts/"},
        },
    )
    logs = buckets[logs_id]
    # Log delivery is granted by bucket policy, not the legacy ACL.
    assert "AccessControl" not in logs["Properties"]
    assert logs["Properties"]["OwnershipControls"] == {"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]}
    assert logs["Metadata"]["guard"] == {"SuppressedRules": ["S3_BUCKET_LOGGING_ENABLED"]}
    assert logs["Metadata"]["checkov"]["skip"][0]["id"] == "CKV_AWS_18"
