# Setup guide

This guide takes you from nothing to schema changes that run through pull requests (GitHub) or merge requests (GitLab). Each step starts with a check. If the check passes, that step is already done, and you can skip it.

| Part | What you do | Needs |
|---|---|---|
| [1. How it works](#part-1-how-it-works) | Read, 2 minutes | Nothing |
| [2. Set up AWS](#part-2-set-up-aws-once-per-environment) | Deploy the toolkit's stack and a role for the pipeline | Your laptop, AWS access |
| [3. Try it on a test table](#part-3-try-it-on-a-test-table) | Create a table, generate a plan, apply it | Part 2 |
| [4. Connect your Git repository](#part-4-connect-your-git-repository) | Add the pipeline, so PRs/MRs check and apply plans | Part 2 |
| [5. Make a schema change](#part-5-make-a-schema-change) | Daily work | Part 4 |
| [6. Troubleshooting](#part-6-troubleshooting-and-uninstall) | Fix common errors, uninstall | |

---

## Part 1. How it works

You describe a schema change in a small JSON file, called a **plan**, and commit it to a `migrations/` folder. A pipeline checks the plan when you open a PR/MR, and applies it to your Iceberg tables when you merge:

```
add a plan  ->  open PR/MR  ->  validate (no AWS)
            ->  merge to main  ->  apply to dev
            ->  release tag    ->  apply to qa, then prod (after approval)
```

Most changes, such as adding or renaming a column, only edit table metadata and finish in seconds. Changes that need the data rewritten, such as changing a column from `date` to `timestamp`, are handed to an AWS Glue job. A tracker table records which plans ran in each environment, so dev, qa and prod end up with the same schema.

**Where the files live.** The plans and the pipeline go in **your project repository**, the one your team works in. The toolkit is installed into the pipeline with `pip`. For a quick test, you can also keep the plans in the toolkit repository itself; Part 4 covers both.

### Values you will need

Wherever this guide shows a word in angle brackets, such as `<ACCOUNT_ID>`, **replace the whole thing, brackets included, with your own value**. For example, `arn:aws:iam::<ACCOUNT_ID>:role/...` becomes `arn:aws:iam::111111111111:role/...`. Most commands use shell variables instead (`$ACCOUNT`, `$REGION`, `$ENV`), which you set once in step 2.3.

| Placeholder | Meaning | Example |
|---|---|---|
| `<ACCOUNT_ID>` | AWS account that holds an environment's tables (one per environment) | `111111111111` |
| `<REGION>` | Region of those tables | `us-west-2` |
| `<env>` | Environment: `dev`, `qa` or `prod` | `dev` |
| `<prefix>` | Start of your table bucket names, `<prefix>-<env>-<layer>-tables` | `dp` |

---

## Part 2. Set up AWS (once per environment)

Do this part for **dev** first. Repeat it for qa and prod when you are ready to promote changes there, each time in that environment's AWS account.

### 2.1 Tools on your laptop

**Check:** `python3 --version`, `aws --version`, `node --version`.

| Tool | Version |
|---|---|
| Python | 3.11 or 3.12 |
| AWS CLI v2 | **2.22.10 or newer.** Older versions have no `s3tables` command and fail with `Invalid choice` |
| Node.js | 22 or 24 (for the AWS CDK) |
| Git | any |

To upgrade the AWS CLI on macOS: `brew upgrade awscli`, or download and run `https://awscli.amazonaws.com/AWSCLIV2.pkg`.

Your AWS role needs permission to create CloudFormation stacks, IAM roles, Glue jobs, S3 buckets and S3 Tables. Start with a non-production account.

### 2.2 Install the toolkit

**Check:** in the toolkit folder, `source .venv/bin/activate && schema-evolution-validate --help` prints a usage message.

```bash
git clone <toolkit repository URL> schema-evolution-toolkit
cd schema-evolution-toolkit
python3 -m venv .venv
source .venv/bin/activate          # run this again in every new terminal
pip install --upgrade pip
pip install -e ".[dev]"
pytest                             # about 480 tests, no AWS needed; all should pass
```

### 2.3 Log in to AWS and set the variables

Log in the way your company does it (SSO profile, Isengard, or access keys), then run:

```bash
aws sts get-caller-identity          # "Account" must be this environment's account
export REGION=us-west-2              # region of your tables
export ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export ENV=dev                       # dev, qa or prod
echo "$ACCOUNT / $REGION / $ENV"     # all three must be filled in
```

The rest of the guide uses these variables. They last only for this terminal. In a new terminal, or after your credentials expire and you log in again, run this block again.

### 2.4 Table buckets

The toolkit changes **existing** tables. It expects one S3 table bucket, with one namespace, per layer: bucket `<prefix>-<env>-<layer>-tables`, namespace `<prefix>_<env>_<layer>`.

**Check:** this lists `dp-dev-bronze-tables`, `dp-dev-silver-tables` and `dp-dev-gold-tables` (with your prefix and environment):

```bash
aws s3tables list-table-buckets --region $REGION --query 'tableBuckets[].name' --output text
```

If they are missing, create them:

```bash
for LAYER in bronze silver gold; do
  aws s3tables create-table-bucket --name dp-$ENV-$LAYER-tables --region $REGION
  aws s3tables create-namespace \
    --table-bucket-arn arn:aws:s3tables:$REGION:$ACCOUNT:bucket/dp-$ENV-$LAYER-tables \
    --namespace dp_${ENV}_$LAYER --region $REGION
done
```

If your buckets use another naming scheme, use a prefix that matches, for example `acme` for `acme-dev-silver-tables`. Replace `dp` with it here and wherever this guide uses `dp`, and pass it to the commands as shown in their notes.

### 2.5 Deploy the toolkit's stack

The stack holds what the toolkit needs in AWS: two Glue jobs that rewrite data, their IAM role, a bucket for the run lock and Spark logs, a second bucket for that bucket's S3 access logs, and a policy for the pipeline (`dp-<env>-schema-evolution-applier`).

**Check:** this prints `CREATE_COMPLETE` or `UPDATE_COMPLETE`:

```bash
aws cloudformation describe-stacks --stack-name dp-$ENV-schema-evolution --region $REGION \
  --query 'Stacks[0].StackStatus' --output text
```

If not, deploy it. Run the commands from the toolkit's `infra/` folder; anywhere else, CDK stops with `--app is required`. If the folder path has spaces, quote it: `cd "/path/with spaces/schema-evolution-toolkit/infra"`.

```bash
cd infra
npx aws-cdk@latest bootstrap aws://$ACCOUNT/$REGION -c env=$ENV   # once per account and region
npx aws-cdk@latest deploy -c env=$ENV -c region=$REGION
cd ..
```

`deploy` ends with a ✅ next to `dp-<env>-schema-evolution`. Optional settings, added as `-c key=value` to both commands:

| Setting | Default | Meaning |
|---|---|---|
| `prefix` | `dp` | Must match your table bucket names |
| `kms_key_arns` | none | Comma-separated KMS key ARNs, if the table buckets use customer-managed keys |
| `backfill_workers` / `backfill_worker_type` | `2` / `G.1X` | Size of the Glue job |

### 2.6 Role for the pipeline

The pipeline logs in to AWS with **OIDC**: GitHub or GitLab hands the job a signed token, and AWS swaps it for temporary credentials. No keys are stored anywhere. You need an identity provider and a role in each AWS account.

You can skip this step for now if you only want to try the toolkit from your laptop (Part 3).

**Check:** this prints the role's ARN:

```bash
aws iam get-role --role-name schema-migrations-$ENV --query Role.Arn --output text
```

If not, run the block for your platform.

<details>
<summary><b>GitHub</b></summary>

Set your repository: the part of its URL after `github.com/`.

```bash
export GIT_REPO=acme/my-data-platform        # change this

# Identity provider, once per account. "EntityAlreadyExists" is fine.
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com --client-id-list sts.amazonaws.com

# Trust policy: only jobs in this repository running in the GitHub environment $ENV may use the role.
cat > trust.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Federated": "arn:aws:iam::$ACCOUNT:oidc-provider/token.actions.githubusercontent.com"},
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {
      "StringEquals": {
        "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
        "token.actions.githubusercontent.com:sub": "repo:$GIT_REPO:environment:$ENV"
      }
    }
  }]
}
EOF
```

</details>

<details>
<summary><b>GitLab</b></summary>

Set your GitLab host and project path: the part of the project URL after the host. For `https://gitlab.com/data/my-data-platform`, that is `gitlab.com` and `data/my-data-platform`.

```bash
export GITLAB_HOST=gitlab.com                # change for self-managed GitLab
export GIT_REPO=data/my-data-platform        # change this
if [ "$ENV" = dev ]; then GITLAB_REF="ref_type:branch:ref:main"; else GITLAB_REF="ref_type:tag:ref:v*"; fi

# Identity provider, once per account. "EntityAlreadyExists" is fine.
aws iam create-open-id-connect-provider \
  --url https://$GITLAB_HOST --client-id-list sts.amazonaws.com

# Trust policy: dev may be used only from the main branch; qa and prod only from release tags (v*).
cat > trust.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Federated": "arn:aws:iam::$ACCOUNT:oidc-provider/$GITLAB_HOST"},
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {
      "StringEquals": {"$GITLAB_HOST:aud": "sts.amazonaws.com"},
      "StringLike": {"$GITLAB_HOST:sub": "project_path:$GIT_REPO:$GITLAB_REF"}
    }
  }]
}
EOF
```

For self-managed GitLab, AWS must be able to download `https://<host>/.well-known/openid-configuration` from the internet. If that URL needs a login or a VPN, OIDC can't work; ask your GitLab administrators how CI jobs get AWS credentials.

</details>

Then, for either platform, create the role:

```bash
cat trust.json        # check the account ID and repository before continuing

APPLIER_POLICY_ARN=$(aws cloudformation describe-stacks --stack-name dp-$ENV-schema-evolution --region $REGION \
  --query "Stacks[0].Outputs[?OutputKey=='ApplierPolicyArn'].OutputValue" --output text)
aws iam create-role --role-name schema-migrations-$ENV --assume-role-policy-document file://trust.json
aws iam attach-role-policy --role-name schema-migrations-$ENV --policy-arn "$APPLIER_POLICY_ARN"
aws iam get-role --role-name schema-migrations-$ENV --query Role.Arn --output text
```

**Keep the role ARN** that the last line prints. Part 4 needs it. To fix a wrong trust policy later, edit `trust.json` and run `aws iam update-assume-role-policy --role-name schema-migrations-$ENV --policy-document file://trust.json`.

**Only if your table buckets use Lake Formation or customer-managed KMS keys:** give both this role and the Glue role `dp-<env>-role-schema-evolution-glue` access. In Lake Formation, grant ALTER, DESCRIBE, SELECT, INSERT and DELETE on the tables, and CREATE_TABLE on each namespace (for the tracker table). In KMS, allow both roles in the key policy.

---

## Part 3. Try it on a test table

This part shows what the toolkit does before any pipeline exists. You create a small table, let the toolkit work out the changes it needs, then apply them. It runs from your laptop against **dev**, with the variables from step 2.3. Allow about 20 minutes, most of it waiting for a Glue job.

### 3.1 Open Athena

You use Athena, the AWS SQL console, to create the table and look at the results. Two one-time settings:

- **S3 console → Table buckets**: if you see **Enable integration**, click it. This is per region, and makes table buckets visible in Athena.
- **Athena → Workgroups**: your workgroup needs engine version 3 and a query result location in the same region.

In the Athena query editor, check the region at the top right, then set:

| Box | Value |
|---|---|
| Data source | `AwsDataCatalog` |
| Catalog | `s3tablescatalog/dp-dev-silver-tables` |
| Database | `dp_dev_silver` |

### 3.2 Create the test table

Create a table named `demo_customers` in the silver layer. It is the "before" state: an `email_host` column you will rename, a `decimal(18,2)` column you will widen, a `date` column you will change to `timestamp`, and a `legacy_score` column you will drop.

Run the two statements one at a time (Athena runs one statement per query):

```sql
CREATE TABLE dp_dev_silver.demo_customers (
  customer_id    bigint,
  full_name      string,
  email_host     string,
  lifetime_value decimal(18,2),
  signup_date    date,
  legacy_score   double
)
PARTITIONED BY (month(signup_date))
TBLPROPERTIES ('table_type' = 'ICEBERG');
```

```sql
INSERT INTO dp_dev_silver.demo_customers VALUES
  (1, 'Test User One',   'example.com', CAST(1250.00 AS decimal(18,2)), DATE '2024-01-15', 0.42),
  (2, 'Test User Two',   'example.org', CAST(310.50  AS decimal(18,2)), DATE '2024-02-03', 0.87),
  (3, 'Test User Three', 'example.net', CAST(0.00    AS decimal(18,2)), DATE '2024-02-20', NULL);
```

Keep the names lowercase, and don't add a `LOCATION` clause.

### 3.3 Generate a plan from a schema file

You can write plans by hand, or describe what the table **should** look like in a YAML file and let the toolkit work out the plan. The toolkit has a sample YAML for this table, `examples/config/silver/crm/schemas/demo_customers.yaml`. Open it: it lists the columns the table should end up with, for example `email_domain` with `previous_table_column: email_host` (a rename), and `signup_date` as `timestamp`.

From the toolkit folder, compare it with the live table:

```bash
schema-evolution-plan --region $REGION --account_id $ACCOUNT --environment dev \
  --config_file examples/config/silver/crm/schemas/demo_customers.yaml \
  --layer silver --table demo_customers --migrations_dir /tmp/plan-test/
```

The log lists each difference it found, such as `[ADD_COLUMN] loyalty_tier`, `[RENAME_COLUMN] email_domain` and `[DROP_COLUMN] legacy_score`, then `Plan written to /tmp/plan-test/silver/demo_customers/<timestamp>_auto_plan.json`. Open that file: this is what a plan looks like. Nothing has changed in the table yet. `--migrations_dir /tmp/plan-test/` keeps the file out of your repository; in real use you'd leave it off, so the plan lands in `migrations/`, and commit it.

### 3.4 Apply plans

Now apply changes for real. Instead of the generated file, use the toolkit's hand-written sample plans for this table. They make the same kind of changes, split into small steps so you can watch each one:

```bash
rm -rf /tmp/try && mkdir -p /tmp/try/silver
cp -r examples/migrations/silver/demo_customers /tmp/try/silver/
schema-evolution-validate --migrations_dir /tmp/try
schema-evolution-apply --region $REGION --account_id $ACCOUNT --environment dev --migrations_dir /tmp/try
```

The plans run in file-name order:

1. Add `loyalty_tier`, rename `email_host` to `email_domain`, widen `lifetime_value`, drop `legacy_score`. These finish in seconds.
2. Change `signup_date` from `date` to `timestamp`. This needs the data rewritten, so it starts the Glue job `dp-dev-glue-schema-evolution-backfill`, and apply stops for this table.

Wait until that job shows **Succeeded** in the **Glue console → ETL jobs → Job run monitoring** (usually a few minutes). Then run the same `schema-evolution-apply` command again; it applies the remaining plans.

Check the result in Athena:

```sql
DESCRIBE dp_dev_silver.demo_customers;
```

```sql
SELECT table_name, plan_name, status, notes
FROM schema_evolution_metadata.plan_tracker
WHERE table_name = 'demo_customers'
ORDER BY plan_name;
```

The second query reads the tracker table, which the toolkit created on the first apply. Every plan should show `applied`.

### 3.5 Clean up

```sql
DROP TABLE dp_dev_silver.demo_customers;
```

```sql
DELETE FROM schema_evolution_metadata.plan_tracker WHERE table_name = 'demo_customers';
```

---

## Part 4. Connect your Git repository

Here you add the pipeline, so that PRs/MRs check plans and merges apply them. You need the role ARN from step 2.6.

**Check:** if your repository already has `.github/workflows/schema-migrations.yml` (GitHub) or includes `schema-migrations.gitlab-ci.yml` from `.gitlab-ci.yml` (GitLab), and a `migrations/` folder, the pipeline is in place. Skip to step 4.3 and check the settings.

### 4.1 Choose where the plans live

| Option | Use it when | The pipeline installs the toolkit with |
|---|---|---|
| **A. Your project repository** (normal use) | Your team has its own repository | A Git URL or package name; see step 4.3 |
| **B. The toolkit repository** | You want a quick test, or the toolkit is your only repository | `.`, the repository itself |

### 4.2 Add the pipeline files

Run these from the **root of the repository** you chose. `TOOLKIT_DIR` is where you cloned the toolkit; check it is set with `ls "$TOOLKIT_DIR/ci"`.

<details>
<summary><b>GitHub, option A (project repository)</b></summary>

```bash
export TOOLKIT_DIR="$HOME/code/schema-evolution-toolkit"   # change to your clone's path
cp -R "$TOOLKIT_DIR/examples/project-repo/migrations" .
mkdir -p .github/workflows
cp "$TOOLKIT_DIR/examples/project-repo/.github/workflows/"*.yml .github/workflows/
```

</details>

<details>
<summary><b>GitHub, option B (toolkit repository)</b></summary>

```bash
mkdir -p .github/workflows migrations/bronze migrations/silver migrations/gold
touch migrations/bronze/.gitkeep migrations/silver/.gitkeep migrations/gold/.gitkeep
cp ci/github/*.yml .github/workflows/
```

In step 4.3, set `SCHEMA_EVOLUTION_TOOLKIT` to `.`.

</details>

<details>
<summary><b>GitLab, option A (project repository)</b></summary>

```bash
export TOOLKIT_DIR="$HOME/code/schema-evolution-toolkit"   # change to your clone's path
cp -R "$TOOLKIT_DIR/examples/project-repo/migrations" .
cp -R "$TOOLKIT_DIR/examples/project-repo/ci" .
```

GitLab reads only the root `.gitlab-ci.yml`. If your repository has none, copy the starter's:

```bash
cp "$TOOLKIT_DIR/examples/project-repo/.gitlab-ci.yml" .
```

If it already has one, add this to it instead (merge it into an existing `include:` list), and if it defines `stages:`, add `validate` and `migrate` to them:

```yaml
include:
  - local: ci/schema-migrations.gitlab-ci.yml
```

</details>

<details>
<summary><b>GitLab, option B (toolkit repository)</b></summary>

```bash
mkdir -p migrations/bronze migrations/silver migrations/gold
touch migrations/bronze/.gitkeep migrations/silver/.gitkeep migrations/gold/.gitkeep
cat > .gitlab-ci.yml <<'EOF'
include:
  - local: ci/gitlab/schema-migrations.gitlab-ci.yml

variables:
  SCHEMA_EVOLUTION_TOOLKIT: "."   # install the toolkit from this repository
EOF
```

`SCHEMA_EVOLUTION_TOOLKIT` is set here, so skip it in step 4.3.

</details>

The pipeline files assume region `us-west-2` and prefix `dp`. If yours differ, change `aws_region` and `prefix` near the top of `schema-migrations-apply.yml` (GitHub), or `SCHEMA_EVOLUTION_REGION` and `SCHEMA_EVOLUTION_PREFIX` in the GitLab pipeline file.

Commit and push:

```bash
git add migrations .github                    # GitHub
git add migrations .gitlab-ci.yml ci          # GitLab
git commit -m "ci: add schema migrations pipeline"
git push
```

### 4.3 Settings in GitHub or GitLab

These settings tell the pipeline which AWS account and role to use, and where to install the toolkit from. Account IDs and role ARNs never go in a file.

**Where to install the toolkit from** (`SCHEMA_EVOLUTION_TOOLKIT`), for option A:

| Toolkit is | Value |
|---|---|
| In its own Git repository (pin a tag) | `schema-evolution-toolkit @ git+https://<host>/<owner>/schema-evolution-toolkit.git@v1.0.0` |
| Published to a private package index | `schema-evolution-toolkit==1.0.0` |

For the Git option, create the tag once in the toolkit repository: `git tag v1.0.0 && git push origin v1.0.0`. If the repository is private, the URL needs a read-only token, `https://oauth2:<token>@<host>/...`; store that value as a secret or masked variable.

<details>
<summary><b>GitHub settings</b></summary>

In your repository:

1. **Settings → Secrets and variables → Actions → Variables → New repository variable**: `SCHEMA_EVOLUTION_TOOLKIT` with the value above.
2. **Settings → Environments → New environment**, named `dev`. Under **Environment variables** add `AWS_ACCOUNT_ID` (the account ID) and `AWS_ROLE_ARN` (the role ARN from step 2.6).
3. **Settings → Branches → Add rule** for `main`: tick **Require status checks to pass** and select `validate`. A PR with a broken plan then can't be merged. Tick **Require a pull request before merging** to also require an approval.

</details>

<details>
<summary><b>GitLab settings</b></summary>

In your project:

1. **Settings → CI/CD → Variables → Add variable**:

   | Key | Value | Environment scope | Protect |
   |---|---|---|---|
   | `SCHEMA_EVOLUTION_TOOLKIT` (option A only) | value above | `*` | **No**. MR pipelines run on unprotected branches and can't read protected variables |
   | `AWS_ACCOUNT_ID` | the account ID | `dev` | Yes |
   | `AWS_ROLE_ARN` | the role ARN from step 2.6 | `dev` | Yes |

   If the scope box doesn't offer `dev`, type it and create it.
2. **Settings → Merge requests**: tick **Pipelines must succeed**, so an MR with a broken plan can't be merged. Add an approval rule there to also require a reviewer.
3. **Settings → CI/CD → Runners**: check that a runner is available. Without one, no job starts.

</details>

### 4.4 Test the PR/MR check, step by step

This test proves three things: a valid plan passes, a broken plan blocks the merge, and a merged plan can't be edited. Do the steps in order, in a terminal at the root of your repository.

**Before you start**, make sure that:
- the pipeline files from step 4.2 are on `main` in the remote repository,
- the merge protection from step 4.3 is on (**Pipelines must succeed** on GitLab, **Require status checks to pass** on GitHub),
- the toolkit's virtual environment is active (`source <toolkit folder>/.venv/bin/activate`), so you can run the check locally.

**A. A valid plan passes**

1. Get the latest `main`:
   ```bash
   git checkout main && git pull
   ```
2. Create a test branch:
   ```bash
   git checkout -b test/schema-check
   ```
3. Add a plan that adds a `test_flag` column to the test table from Part 3. The file name starts with the current UTC time, so it sorts after any plan already in the folder:
   ```bash
   mkdir -p migrations/silver/demo_customers
   PLAN=migrations/silver/demo_customers/$(date -u +%Y%m%dT%H%M%S)_add_test_flag.json
   cat > "$PLAN" <<'EOF'
   {
     "metadata": {"layer": "silver", "table": "demo_customers"},
     "changes": [
       {"change_type": "add_column", "column_name": "test_flag", "target_type": "boolean"}
     ]
   }
   EOF
   echo "$PLAN"
   ```
4. Run the check locally:
   ```bash
   schema-evolution-validate --migrations_dir migrations/
   ```
   Expected: `Checked 1 plan(s): 0 error(s), 0 warning(s)`.
5. Commit and push the branch:
   ```bash
   git add migrations
   git commit -m "test: add test_flag to demo_customers"
   git push -u origin test/schema-check
   ```
6. Open a PR/MR from `test/schema-check` into `main`. The `git push` output usually prints a link that creates it.
7. On the PR/MR page, wait for the check: `validate` on GitHub, `schema-migrations:validate` on GitLab.
   Expected: it passes, and the merge button is available, or waits for an approval if you added an approval rule. **Don't merge yet.**

**B. A broken plan blocks the merge**

8. Break the plan on purpose with a misspelled change type:
   ```bash
   sed -i.bak 's/"add_column"/"add_colum"/' "$PLAN"
   rm migrations/silver/demo_customers/*.bak
   git commit -am "test: broken plan" && git push
   ```
9. On the PR/MR page, wait for the check.
   Expected: it fails. The job log shows `unknown change_type 'add_colum'`, and the merge button is blocked.
10. Undo the break:
    ```bash
    git revert --no-edit HEAD && git push
    ```
11. Expected: the check passes again.

**C. Approve and merge**

12. If you added an approval rule, ask a reviewer to approve the PR/MR. You usually can't approve your own.
13. Merge the PR/MR.
14. Open the pipeline that runs on `main` (GitHub: **Actions** tab; GitLab: **Build → Pipelines**). The `apply-dev` job runs.
    Expected: the log ends with `All migrations applied successfully`. If the test table still exists, it now has a `test_flag` column. If you dropped it in step 3.5, the plan is recorded as `waiting_for_table`, which is also fine.
    If the job fails while logging in to AWS, recheck the role's trust policy and the `AWS_ROLE_ARN` setting (step 2.6, step 4.3).

**D. A merged plan can't be edited**

15. Start a new branch from the updated `main`:
    ```bash
    git checkout main && git pull
    git checkout -b test/edit-merged-plan
    ```
16. Change the merged plan (`ls migrations/silver/demo_customers/` shows its name if `$PLAN` is no longer set):
    ```bash
    sed -i.bak 's/test_flag/test_flag_2/' "$PLAN"
    rm migrations/silver/demo_customers/*.bak
    git commit -am "test: edit a merged plan" && git push -u origin test/edit-merged-plan
    ```
17. Open a PR/MR from `test/edit-merged-plan` into `main`.
    Expected: the check fails with `'changes' edited, but the plan exists on main`. Changes to an applied table always go in a new plan. (For a merged plan that **failed** and was never applied, see "Fixing a merged plan that failed" in [operations.md](operations.md).)
18. Close this PR/MR without merging, and delete both test branches.

### 4.5 Before you use qa and prod

Repeat Part 2 in the qa and prod accounts (with `export ENV=qa` / `prod`), then add the same settings for those environments:

- **GitHub:** create the `qa` and `prod` environments with their own `AWS_ACCOUNT_ID` and `AWS_ROLE_ARN`. In `prod`, tick **Required reviewers** and add who approves production changes.
- **GitLab:** add `AWS_ACCOUNT_ID` and `AWS_ROLE_ARN` again with scopes `qa` and `prod`. Under **Settings → Repository → Protected tags**, protect `v*` so only maintainers can create release tags. Under **Settings → CI/CD → Protected environments**, protect `prod` and choose who may run it; the prod job is manual.

---

## Part 5. Make a schema change

### 5.1 Write a plan

One file per change set, at `migrations/<layer>/<table>/<UTC timestamp>_<short description>.json`. For example, to add a column to `silver.customers`:

```bash
mkdir -p migrations/silver/customers
cat > migrations/silver/customers/20260301T090000_add_loyalty_tier.json <<'EOF'
{
  "metadata": {"layer": "silver", "table": "customers"},
  "changes": [
    {"change_type": "add_column", "column_name": "loyalty_tier", "target_type": "string"}
  ]
}
EOF
```

Or generate it from a schema YAML file, as in step 3.3, leaving off `--migrations_dir` so it lands in `migrations/`. Read a generated plan before committing it, especially any `drop_column`. Apply it in dev, from your laptop or the pipeline, **before** you merge it to the branch that deploys qa and prod. A merged plan can't be edited, so a mistake found later needs the procedure in [operations.md](operations.md).

All change types are in [plan-reference.md](plan-reference.md), and `examples/migrations/` has a sample of each kind. Rules:

- **One plan per table per change set.** Changes that rewrite data (type changes, re-partitioning) for the same table belong in **one** plan, so they run in one Glue job.
- **Never edit or delete a plan once it is merged.** It may already be applied somewhere. Add a new plan instead; `validate` enforces this.
- To apply a plan only in some environments, add `"environments": ["dev", "qa"]` to `metadata`.

To run the same check as the pipeline before you push (no AWS needed):

```bash
schema-evolution-validate --migrations_dir migrations/ --base_ref origin/main
```

### 5.2 Open a PR/MR, merge, release

| Step | What runs | What you do |
|---|---|---|
| Open the PR/MR | `validate` | Fix anything it reports |
| Merge to `main` | `apply-dev` | Check the job log |
| Create a release tag, e.g. `v1.4.0` | `apply-qa`, then `apply-prod` | Approve prod (GitHub), or click **Run** on the prod job (GitLab) |

### 5.3 Changes that rewrite data

For an incompatible type change, re-partitioning or a table rebuild, apply starts a Glue job and doesn't wait for it:

- The pipeline still succeeds, and the plan shows `backfilling`.
- The next pipeline continues: once the job has finished, the table's later plans apply. If it is still running, apply waits up to 5 minutes, logs `Backfill still running (Glue run ...)`, and succeeds.
- If the job failed, the next pipeline fails and retries the plan. Fix the cause and re-run.

ETL jobs that run right after a deploy may see a table whose later plans haven't applied yet, because its backfill is still running. Run the migration job before jobs that start ETL.

### 5.4 See what was applied

In Athena, with **Catalog** set to `s3tablescatalog/<prefix>-<env>-<layer>-tables`:

```sql
SELECT table_name, plan_name, status, applied_at, notes
FROM schema_evolution_metadata.plan_tracker
ORDER BY created_at DESC;
```

| Status | Meaning |
|---|---|
| `applied` | Done |
| `backfilling` | A Glue job is rewriting data; `notes` has the run ID |
| `waiting_for_table` | The table doesn't exist yet; the plan applies once it does |
| `failed` | `notes` has the error; retried on the next run |

---

## Part 6. Troubleshooting and uninstall

| Problem | Fix |
|---|---|
| `aws: error: argument command: Invalid choice` | AWS CLI older than 2.22.10 (step 2.1) |
| `ExpiredToken` | Your AWS credentials expired. Log in again and rerun the block in step 2.3 |
| A path starts with `/examples/...` or a command gets empty values | A variable isn't set in this terminal. Rerun step 2.3, or set `TOOLKIT_DIR` (step 4.2) |
| CDK: `--app is required` | Run CDK from the toolkit's `infra/` folder |
| CDK: `Pass the target environment` | Add `-c env=$ENV` to the command |
| CDK: `SSM parameter /cdk-bootstrap/... not found` | The account isn't bootstrapped in this region, usually because `$REGION` was empty. Rerun the `bootstrap` line in step 2.5 |
| Table bucket not visible in Athena | Use the **Catalog** box, not Data source; enable the integration (step 3.1); check the region |
| `pytest` or a command says the toolkit isn't installed | `source .venv/bin/activate` in the toolkit folder |
| Pipeline: `Not authorized to perform sts:AssumeRoleWithWebIdentity` | The trust policy doesn't match the repository, branch or environment. Check `trust.json` (step 2.6). On GitHub, newer repositories can include IDs in the `sub` claim; see "Claims and scoping permissions" in the [configure-aws-credentials README](https://github.com/aws-actions/configure-aws-credentials) |
| Pipeline: `pip install` fails | `SCHEMA_EVOLUTION_TOOLKIT` is empty or wrong, is a protected GitLab variable used on an MR branch, or the URL lacks a token for a private repository (step 4.3) |
| `validate`: `'changes' edited, but the plan exists on main` | Revert your edit to that file and put the change in a new plan |
| `validate`: `--base_ref ... is not a known commit` | The pipeline cloned too little history. Keep `fetch-depth: 0` (GitHub) or `GIT_DEPTH: 0` (GitLab) in the pipeline files |
| Apply: `LockNotAcquiredError` | Another apply on the same environment held the lock for 15 minutes. Wait and re-run |
| Apply: `AccessDenied` on S3 Tables | Lake Formation grants or KMS key policy (end of step 2.6) |

More cases: [operations.md](operations.md). How it works inside: [architecture.md](architecture.md).

**Uninstall** an environment:

```bash
cd infra && npx aws-cdk@latest destroy -c env=$ENV
```

The artifacts bucket and its access logs bucket are kept on purpose (`RETAIN`), and the tracker tables stay in your table buckets. To start completely fresh, delete both buckets and drop `schema_evolution_metadata.plan_tracker` in each layer. Also delete the role `schema-migrations-<env>` and the OIDC provider from step 2.6.
