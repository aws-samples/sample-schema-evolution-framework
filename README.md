# Schema Evolution Toolkit

Declarative, version-controlled schema migrations for Apache Iceberg tables on Amazon S3 Tables.

You commit small JSON migration plans next to your code. A CLI applies them at deploy time with PyIceberg. Changes that need a data rewrite are handed to an AWS Glue (Spark) job. A per-layer Iceberg tracker table records what ran, so every environment converges on the same schema.

## What it does

| Capability | How |
|---|---|
| Add, drop or rename columns | Metadata-only PyIceberg DDL, sub-second, idempotent |
| Widen types (int to long, float to double, decimal precision) | Metadata-only `update_column` |
| Incompatible type changes (for example date to timestamp, int to string) | Glue backfill job: rename, add, cast, drop. Partitioned columns get a full rebuild and an atomic swap |
| Nullability (`NOT NULL` on or off) | Metadata-only |
| Partition and sort-order evolution, re-partitioning existing data | Metadata-only, or a Glue rebuild |
| Rename tables, set or unset table properties | S3 Tables API / PyIceberg |
| Per-environment plans | `metadata.environments` in the plan |
| Plan generation from a schema YAML | `schema-evolution-plan` diffs the YAML against the live table |
| Safety | S3 run lock, SHA-256 checksums of applied plans, idempotent DDL, self-healing of dead backfills |

## Repository layout

```
schema-evolution-toolkit/
├── src/
│   ├── schema_evolution/          # Core library + CLIs (pure Python, PyIceberg)
│   └── schema_evolution_spark/    # Glue/Spark backfill jobs + helpers
├── infra/                         # AWS CDK app (Glue jobs, bucket, CI policy)
├── ci/                            # Pipelines: github/ (Actions) and gitlab/ (GitLab CI)
├── examples/                      # Sample schema YAML, migration plans, account file
│   └── project-repo/              # Starter files to copy into your project repository
├── tests/                         # core / spark / infra test suites
└── docs/
    ├── installation.md            # THE setup guide: AWS setup, pipeline setup, daily use
    ├── architecture.md            # How the pieces fit together
    ├── plan-reference.md          # Plan JSON + schema YAML reference
    └── operations.md              # Runbooks, troubleshooting, limitations
```

## Getting started

**Follow [docs/installation.md](docs/installation.md).** Each step starts with a check, so you can skip what is already set up:

1. How it works.
2. AWS setup, once per environment: deploy the stack and create the role the pipeline uses.
3. Try it on a test table from your laptop: create a table, generate a plan, apply it.
4. Connect your Git repository (GitHub Actions or GitLab CI), and test the PR/MR check step by step.
5. Daily use: write a plan, open a PR/MR, merge, release.
6. Troubleshooting and uninstall.

To only check that the code works on your machine (no AWS):

```bash
cd schema-evolution-toolkit
python3 -m venv .venv && source .venv/bin/activate   # Python 3.11+
pip install -e ".[dev]"
pytest
```

To share the toolkit as a zip (for example for a security scan), build it with `make archive`. It packages only committed files, so local build output such as `infra/cdk.out/` (which contains your account ID and bucket names) and `.venv/` stay out.

## Conventions

The toolkit derives every name from a resource prefix (default `dp`, set with `--bucket_prefix` or `-c prefix=`) and the environment:

| Thing | Name |
|---|---|
| S3 Tables bucket per layer | `{prefix}-{env}-{bronze,silver,gold}-tables` |
| Namespace (database) per layer | `{prefix}_{env}_{layer}` |
| Plan tracker | `schema_evolution_metadata.plan_tracker` in each layer bucket |
| Artifacts bucket (run lock, Spark UI logs) | `{prefix}-{env}-schema-evolution-{account}-{region}` |
| Access logs bucket (S3 server access logs of the artifacts bucket, 90 days) | CDK-generated name |
| Backfill Glue job | `{prefix}-{env}-glue-schema-evolution-backfill` |
| Env-column backfill Glue job | `{prefix}-{env}-glue-schema-evolution-backfill-env-columns` |
| CI managed policy | `{prefix}-{env}-schema-evolution-applier` |

## CLI commands

| Command | Purpose |
|---|---|
| `schema-evolution-validate` | Check plans offline in a PR/MR; blocks edits to merged plans |
| `schema-evolution-apply` | Apply pending plans (run by the pipeline). Waits for the run lock |
| `schema-evolution-plan` | Diff a schema YAML against the live table and write a plan |
| `schema-evolution-config-sync` | Report schema YAML vs. table drift (SSM status + optional SNS) |
| `schema-evolution-generate-env-plans` | Write add-column plans for environment-gated fields |
| `schema-evolution-trigger-backfill` | Start the env-column backfill job for pending env-gated plans |

Run any command with `--help` for its options.

## Requirements

- Python 3.11+ (AWS Glue 5.x runs 3.11)
- An AWS account with Amazon S3 Tables
- For `infra/`: Node.js 18+ and the AWS CDK CLI v2
