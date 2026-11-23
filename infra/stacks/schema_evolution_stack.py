"""CDK stack: schema evolution backfill Glue jobs.

Deploy this stack in the AWS account that owns the S3 Tables buckets
(``{prefix}-{env}-{bronze,silver,gold}-tables``).

Resources:
- Artifacts bucket ``{prefix}-{env}-schema-evolution-{account}-{region}``:
  the run lock (``locks/``) and Spark UI logs.
- Access logs bucket (CDK-generated name): S3 server access logs of the
  artifacts bucket, kept 90 days.
- Glue job ``{prefix}-{env}-glue-schema-evolution-backfill``: rewrites data for
  changes that cannot be done as metadata-only DDL. Launched by
  ``schema-evolution-apply``.
- Glue job ``{prefix}-{env}-glue-schema-evolution-backfill-env-columns``:
  manually triggered MERGE that fills environment-gated columns.
- Managed policy ``{prefix}-{env}-schema-evolution-applier``: everything
  ``schema-evolution-apply`` needs. Attach it to your CI/deploy role.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

import aws_cdk as cdk
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_assets as s3_assets
from constructs import Construct

_TOOLKIT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_SRC_DIR = os.path.join(_TOOLKIT_ROOT, "src")
_SPARK_PKG = os.path.join(_SRC_DIR, "schema_evolution_spark")

ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
LAYERS = ("bronze", "silver", "gold")


@dataclass
class GlueJobSizing:
    """Compute settings for one Glue job."""

    worker_type: str = "G.1X"
    number_of_workers: int = 2
    timeout_minutes: int = 120
    max_concurrent_runs: int = 10


@dataclass
class SchemaEvolutionConfig:
    """Stack inputs. Everything except ``env_name`` has a default."""

    env_name: str
    prefix: str = "dp"
    glue_version: str = "5.1"
    backfill: GlueJobSizing = field(default_factory=GlueJobSizing)
    env_columns_backfill: GlueJobSizing = field(default_factory=lambda: GlueJobSizing(max_concurrent_runs=15))
    # Customer-managed KMS keys protecting the S3 Tables buckets, if any.
    table_bucket_kms_key_arns: list[str] = field(default_factory=list)


class SchemaEvolutionStack(cdk.Stack):
    """Backfill Glue jobs + applier policy for the schema evolution toolkit."""

    def __init__(self, scope: Construct, construct_id: str, *, config: SchemaEvolutionConfig, **kwargs: object) -> None:
        super().__init__(scope, construct_id, **kwargs)
        # Grant S3 log delivery through a bucket policy, not the legacy
        # LogDeliveryWrite ACL (which would require ACLs to stay enabled).
        self.node.set_context("@aws-cdk/aws-s3:serverAccessLogsUseBucketPolicy", True)
        self.config = config
        prefix, env_name = config.prefix, config.env_name
        self.backfill_job_name = f"{prefix}-{env_name}-glue-schema-evolution-backfill"
        self.env_columns_job_name = f"{prefix}-{env_name}-glue-schema-evolution-backfill-env-columns"
        self.table_bucket_arns = [
            f"arn:aws:s3tables:{self.region}:{self.account}:bucket/{prefix}-{env_name}-{layer}-tables"
            for layer in LAYERS
        ]

        # Literal name (not a token) so it matches the CLI's default lock location
        # (schema_evolution.lock.artifacts_bucket_for) exactly.
        self.artifacts_bucket_name = f"{prefix}-{env_name}-schema-evolution-{self.account}-{self.region}"
        self.access_logs_bucket = self._build_access_logs_bucket()
        self.artifacts_bucket = s3.Bucket(
            self,
            "ArtifactsBucket",
            bucket_name=self.artifacts_bucket_name,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            server_access_logs_bucket=self.access_logs_bucket,
            server_access_logs_prefix="artifacts/",
            versioned=True,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(id="ExpireSparkUiLogs", prefix="spark-ui/", expiration=cdk.Duration.days(30)),
                s3.LifecycleRule(id="ExpireOldVersions", noncurrent_version_expiration=cdk.Duration.days(30)),
            ],
        )

        _deny_insecure_transport(self.artifacts_bucket)
        glue_role = self._build_glue_role()
        self._build_glue_jobs(glue_role)
        self._build_applier_policy()

        cdk.Tags.of(self).add("Environment", env_name)
        cdk.Tags.of(self).add("Component", "schema-evolution")

    def _build_access_logs_bucket(self) -> s3.Bucket:
        """Target bucket for the artifacts bucket's S3 server access logs.

        CDK generates the name, so it is not predictable. S3 log delivery only
        supports SSE-S3 encryption. The bucket does not log its own access:
        that would need yet another bucket, and so on.
        """
        bucket = s3.Bucket(
            self,
            "AccessLogsBucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            object_ownership=s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
            versioned=True,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="ExpireAccessLogs",
                    expiration=cdk.Duration.days(90),
                    noncurrent_version_expiration=cdk.Duration.days(1),
                ),
            ],
        )
        _deny_insecure_transport(bucket)
        reason = "This bucket is the server access log target; logging it would need another log bucket."
        cfn = bucket.node.default_child
        cfn.add_metadata("checkov", {"skip": [{"id": "CKV_AWS_18", "comment": reason}]})
        cfn.add_metadata("guard", {"SuppressedRules": ["S3_BUCKET_LOGGING_ENABLED"]})
        cfn.add_metadata("cdk_nag", {"rules_to_suppress": [{"id": "AwsSolutions-S1", "reason": reason}]})
        return bucket

    # ------------------------------------------------------------------
    # IAM helpers
    # ------------------------------------------------------------------

    def _s3tables_statement(self, sid: str, extra_actions: list[str]) -> iam.PolicyStatement:
        """Control + data plane access to the three layer table buckets.

        Data plane (Get/PutTableData) is needed because PyIceberg and Spark read
        and commit Iceberg metadata files in S3 Tables managed storage.
        CreateNamespace/CreateTable cover the per-layer plan tracker
        (``schema_evolution_metadata.plan_tracker``).
        """
        return iam.PolicyStatement(
            sid=sid,
            actions=[
                "s3tables:GetTableBucket",
                "s3tables:GetNamespace",
                "s3tables:CreateNamespace",
                "s3tables:ListNamespaces",
                "s3tables:GetTable",
                "s3tables:ListTables",
                "s3tables:CreateTable",
                "s3tables:GetTableMetadataLocation",
                "s3tables:UpdateTableMetadataLocation",
                "s3tables:GetTableData",
                "s3tables:PutTableData",
                *extra_actions,
            ],
            resources=[arn for base in self.table_bucket_arns for arn in (base, f"{base}/*")],
        )

    def _kms_statement(self, sid: str) -> iam.PolicyStatement | None:
        if not self.config.table_bucket_kms_key_arns:
            return None
        return iam.PolicyStatement(
            sid=sid,
            actions=["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"],
            resources=list(self.config.table_bucket_kms_key_arns),
        )

    def _build_glue_role(self) -> iam.Role:
        role = iam.Role(
            self,
            "GlueJobRole",
            role_name=f"{self.config.prefix}-{self.config.env_name}-role-schema-evolution-glue",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            description="Execution role for the schema-evolution backfill Glue jobs",
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")],
        )
        # A rebuild swaps the rebuilt table in: delete the original, rename the copy.
        role.add_to_policy(
            self._s3tables_statement("S3TablesBackfill", ["s3tables:DeleteTable", "s3tables:RenameTable"])
        )
        kms = self._kms_statement("S3TablesKms")
        if kms:
            role.add_to_policy(kms)
        self.artifacts_bucket.grant_read_write(role, "spark-ui/*")
        return role

    # ------------------------------------------------------------------
    # Glue jobs
    # ------------------------------------------------------------------

    def _build_glue_jobs(self, role: iam.Role) -> None:
        # Both packages are shipped as one zip on --extra-py-files so the job
        # scripts can import schema_evolution_spark.*.
        py_files = s3_assets.Asset(
            self,
            "SparkPackageZip",
            path=_SRC_DIR,
            exclude=["**/__pycache__", "**/*.pyc", "*.egg-info", "**/*.egg-info"],
        )
        py_files.grant_read(role)

        jobs = [
            (
                "BackfillJob",
                self.backfill_job_name,
                "schema_evolution_backfill.py",
                "Rewrites data for schema changes that cannot be metadata-only (type swap, rebuild, re-partition)",
                self.config.backfill,
            ),
            (
                "EnvColumnsBackfillJob",
                self.env_columns_job_name,
                "backfill_new_columns.py",
                "Manually triggered: MERGE INTO Silver from Bronze for environment-gated add_column plans",
                self.config.env_columns_backfill,
            ),
        ]
        for construct_id, job_name, script, description, sizing in jobs:
            script_asset = s3_assets.Asset(self, f"{construct_id}Script", path=os.path.join(_SPARK_PKG, script))
            script_asset.grant_read(role)
            glue.CfnJob(
                self,
                construct_id,
                name=job_name,
                description=description,
                role=role.role_arn,
                glue_version=self.config.glue_version,
                command=glue.CfnJob.JobCommandProperty(
                    name="glueetl",
                    python_version="3",
                    script_location=script_asset.s3_object_url,
                ),
                worker_type=sizing.worker_type,
                number_of_workers=sizing.number_of_workers,
                timeout=sizing.timeout_minutes,
                max_retries=0,
                execution_property=glue.CfnJob.ExecutionPropertyProperty(
                    max_concurrent_runs=sizing.max_concurrent_runs
                ),
                default_arguments={
                    "--job-language": "python",
                    "--datalake-formats": "iceberg",
                    "--conf": f"spark.sql.extensions={ICEBERG_EXTENSIONS}",
                    "--extra-py-files": py_files.s3_object_url,
                    "--enable-metrics": "true",
                    "--enable-continuous-cloudwatch-log": "true",
                    "--enable-spark-ui": "true",
                    "--spark-event-logs-path": f"s3://{self.artifacts_bucket_name}/spark-ui/",
                    "--job-bookmark-option": "job-bookmark-disable",
                },
            )

    # ------------------------------------------------------------------
    # Applier policy (attach to the role that runs schema-evolution-apply)
    # ------------------------------------------------------------------

    def _build_applier_policy(self) -> None:
        prefix, env_name = self.config.prefix, self.config.env_name
        statements = [
            # RenameTable serves rename_table plans.
            self._s3tables_statement("S3TablesApply", ["s3tables:RenameTable"]),
            iam.PolicyStatement(
                sid="GlueBackfillControl",
                actions=["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns"],
                resources=[f"arn:aws:glue:{self.region}:{self.account}:job/{self.backfill_job_name}"],
            ),
            iam.PolicyStatement(
                sid="MigrationRunLock",
                actions=["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                resources=[self.artifacts_bucket.arn_for_objects(f"locks/{env_name}.lock")],
            ),
        ]
        kms = self._kms_statement("S3TablesKms")
        if kms:
            statements.append(kms)

        policy = iam.ManagedPolicy(
            self,
            "ApplierPolicy",
            managed_policy_name=f"{prefix}-{env_name}-schema-evolution-applier",
            description="Permissions for schema-evolution-apply; attach to the CI/deploy role",
            statements=statements,
        )

        cdk.CfnOutput(self, "ArtifactsBucketName", value=self.artifacts_bucket.bucket_name)
        cdk.CfnOutput(self, "ApplierPolicyArn", value=policy.managed_policy_arn)
        cdk.CfnOutput(self, "BackfillJobName", value=self.backfill_job_name)
        cdk.CfnOutput(self, "EnvColumnsBackfillJobName", value=self.env_columns_job_name)


def _deny_insecure_transport(bucket: s3.Bucket) -> None:
    """Deny any request to *bucket* that does not use TLS.

    Used instead of ``enforce_ssl=True``, which writes the condition value as the
    string ``"false"``. IAM treats both the same, but policy scanners such as
    cfn-guard's S3_BUCKET_SSL_REQUESTS_ONLY only accept the boolean.
    """
    bucket.add_to_resource_policy(
        iam.PolicyStatement(
            sid="DenyInsecureTransport",
            effect=iam.Effect.DENY,
            principals=[iam.AnyPrincipal()],
            actions=["s3:*"],
            resources=[bucket.bucket_arn, bucket.arn_for_objects("*")],
            conditions={"Bool": {"aws:SecureTransport": False}},
        )
    )
