# Starter project repository

This folder shows what **your project repository** should contain to run schema migrations. Copy it into your repository instead of assembling the files by hand.

```
project-repo/
├── migrations/                     # your plan files go here, one folder per layer
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── .github/workflows/              # GitHub only
│   ├── schema-migrations.yml
│   └── schema-migrations-apply.yml
├── .gitlab-ci.yml                  # GitLab only
└── ci/schema-migrations.gitlab-ci.yml   # GitLab only
```

## Copy it

Run these from the root of your project repository. `TOOLKIT_DIR` is where you cloned the toolkit.

**GitHub:**

```bash
cp -R "$TOOLKIT_DIR/examples/project-repo/migrations" .
mkdir -p .github/workflows
cp "$TOOLKIT_DIR/examples/project-repo/.github/workflows/"*.yml .github/workflows/
```

**GitLab:**

```bash
cp -R "$TOOLKIT_DIR/examples/project-repo/migrations" .
cp -R "$TOOLKIT_DIR/examples/project-repo/ci" .
# Only if your repository has no .gitlab-ci.yml yet:
cp "$TOOLKIT_DIR/examples/project-repo/.gitlab-ci.yml" .
```

If you already have a `.gitlab-ci.yml`, add `- local: ci/schema-migrations.gitlab-ci.yml` to its `include:` section instead.

## Then change these values

The pipeline files work as they are. Check these three values and change them only if yours differ:

| Value | Default | GitHub: where | GitLab: where |
|---|---|---|---|
| AWS region | `us-west-2` | `aws_region` in `.github/workflows/schema-migrations-apply.yml` | `SCHEMA_EVOLUTION_REGION` in `ci/schema-migrations.gitlab-ci.yml` |
| Resource prefix | `dp` | `prefix` in `.github/workflows/schema-migrations-apply.yml` | `SCHEMA_EVOLUTION_PREFIX` in `ci/schema-migrations.gitlab-ci.yml` |
| Toolkit source | none | Repository variable `SCHEMA_EVOLUTION_TOOLKIT` | `SCHEMA_EVOLUTION_TOOLKIT` in `ci/schema-migrations.gitlab-ci.yml`, or a CI/CD variable with that name |

Account IDs and role ARNs are not in any file. You enter them in GitHub or GitLab settings (step 4.3 of [docs/installation.md](../../docs/installation.md)).

## Add your first plan

The `migrations/` folders are empty on purpose, so nothing is applied when you first merge. Add a plan as described in Part 5 of the setup guide. `examples/migrations/` in the toolkit has a sample for each kind of change.
