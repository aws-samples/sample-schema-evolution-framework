#!/usr/bin/env python3
"""CDK app for the schema evolution toolkit.

Usage (from the infra/ directory):

    cdk deploy -c env=dev
    cdk deploy -c env=prod -c prefix=acme -c kms_key_arns=arn:aws:kms:...:key/...,arn:...

Context keys:
    env            dev | qa | prod (required)
    prefix         resource prefix, must match your S3 Tables bucket names (default: dp)
    glue_version   AWS Glue version for the Spark jobs (default: 5.1)
    kms_key_arns   comma-separated CMK ARNs protecting the table buckets (optional)
    backfill_workers / backfill_worker_type   sizing for the backfill job (optional)
    account / region   target account and region (default: from your AWS profile,
                   i.e. CDK_DEFAULT_ACCOUNT / CDK_DEFAULT_REGION)

The region must be the one your S3 Tables buckets live in.
"""

from __future__ import annotations

import os

import aws_cdk as cdk

from stacks.schema_evolution_stack import GlueJobSizing, SchemaEvolutionConfig, SchemaEvolutionStack

app = cdk.App()

env_name = app.node.try_get_context("env")
if env_name not in ("dev", "qa", "prod"):
    raise SystemExit("Pass the target environment: cdk <command> -c env=dev|qa|prod")

prefix = app.node.try_get_context("prefix") or "dp"
kms_ctx = app.node.try_get_context("kms_key_arns") or ""
kms_key_arns = [k.strip() for k in kms_ctx.split(",") if k.strip()] if isinstance(kms_ctx, str) else list(kms_ctx)

config = SchemaEvolutionConfig(
    env_name=env_name,
    prefix=prefix,
    glue_version=app.node.try_get_context("glue_version") or "5.1",
    backfill=GlueJobSizing(
        worker_type=app.node.try_get_context("backfill_worker_type") or "G.1X",
        number_of_workers=int(app.node.try_get_context("backfill_workers") or 2),
    ),
    table_bucket_kms_key_arns=kms_key_arns,
)

SchemaEvolutionStack(
    app,
    f"{prefix}-{env_name}-schema-evolution",
    config=config,
    env=cdk.Environment(
        account=app.node.try_get_context("account") or os.environ.get("CDK_DEFAULT_ACCOUNT"),
        region=app.node.try_get_context("region") or os.environ.get("CDK_DEFAULT_REGION"),
    ),
    description="Schema evolution toolkit: backfill Glue jobs and applier policy",
)

app.synth()
