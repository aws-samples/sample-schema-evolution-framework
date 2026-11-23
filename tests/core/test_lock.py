"""Tests for the S3 conditional-write run lock and its use in apply_migrations."""

import argparse
import io
import json
import os
import tempfile
import threading
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from schema_evolution.apply import ApplyResult, _build_args_from_argv, apply_migrations
from schema_evolution.lock import LockNotAcquiredError, S3Lock, lock_uri_for

URI = "s3://amzn-s3-demo-bucket/schema-evolution/locks/dev.lock"


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "op")


class FakeS3:
    """In-memory S3 with conditional PutObject/DeleteObject semantics."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], tuple[bytes, str]] = {}
        self._counter = 0
        self._mutex = threading.Lock()

    def _etag(self) -> str:
        self._counter += 1
        return f'"etag-{self._counter}"'

    def put_object(self, *, Bucket, Key, Body, IfNoneMatch=None, IfMatch=None, ContentType=None):  # noqa: N803
        with self._mutex:
            current = self.objects.get((Bucket, Key))
            if IfNoneMatch == "*" and current is not None:
                raise _client_error("PreconditionFailed")
            if IfMatch is not None:
                if current is None:
                    raise _client_error("NoSuchKey")
                if current[1] != IfMatch:
                    raise _client_error("PreconditionFailed")
            etag = self._etag()
            self.objects[(Bucket, Key)] = (Body, etag)
            return {"ETag": etag}

    def get_object(self, *, Bucket, Key):  # noqa: N803
        current = self.objects.get((Bucket, Key))
        if current is None:
            raise _client_error("NoSuchKey")
        return {"Body": io.BytesIO(current[0]), "ETag": current[1]}

    def delete_object(self, *, Bucket, Key, IfMatch=None):  # noqa: N803
        with self._mutex:
            current = self.objects.get((Bucket, Key))
            if current is None:
                raise _client_error("NoSuchKey")
            if IfMatch is not None and current[1] != IfMatch:
                raise _client_error("PreconditionFailed")
            del self.objects[(Bucket, Key)]

    def body(self) -> dict:
        return json.loads(self.objects[("amzn-s3-demo-bucket", "schema-evolution/locks/dev.lock")][0])

    def plant(self, owner: str, expires_at: datetime) -> None:
        body = json.dumps({"owner": owner, "expires_at": expires_at.isoformat()}).encode()
        self.objects[("amzn-s3-demo-bucket", "schema-evolution/locks/dev.lock")] = (body, self._etag())


# ---------------------------------------------------------------------------
# S3Lock
# ---------------------------------------------------------------------------


def test_acquire_and_release():
    s3 = FakeS3()
    lock = S3Lock.create(s3, URI, owner="ci")

    lock.acquire()
    assert s3.body()["owner"].startswith("ci@")
    lock.release()
    assert s3.objects == {}


def test_second_holder_is_refused_while_lock_is_live():
    s3 = FakeS3()
    first = S3Lock.create(s3, URI, owner="ci")
    first.acquire()

    second = S3Lock.create(s3, URI, owner="lambda")
    assert second.try_acquire() is False
    with pytest.raises(LockNotAcquiredError):
        second.acquire(wait_seconds=0)


def test_expired_lock_is_taken_over():
    s3 = FakeS3()
    s3.plant("crashed", datetime.now(UTC) - timedelta(minutes=1))

    lock = S3Lock.create(s3, URI, owner="lambda")
    assert lock.try_acquire() is True
    assert s3.body()["owner"].startswith("lambda@")


def test_unreadable_lock_body_is_treated_as_expired():
    s3 = FakeS3()
    s3.objects[("amzn-s3-demo-bucket", "schema-evolution/locks/dev.lock")] = (b"not json", '"x"')

    assert S3Lock.create(s3, URI, owner="ci").try_acquire() is True


def test_release_does_not_delete_a_lock_taken_over_by_someone_else():
    s3 = FakeS3()
    first = S3Lock.create(s3, URI, owner="ci", ttl_seconds=1)
    first.acquire()
    s3.plant("other", datetime.now(UTC) + timedelta(minutes=5))

    first.release()  # must not raise or delete the other holder's lock

    assert s3.body()["owner"] == "other"


def test_refresh_extends_expiry_and_detects_loss():
    s3 = FakeS3()
    lock = S3Lock.create(s3, URI, owner="ci", ttl_seconds=60)
    lock.acquire()
    before = s3.body()["expires_at"]

    assert lock.refresh() is True
    assert s3.body()["expires_at"] >= before

    s3.plant("other", datetime.now(UTC) + timedelta(minutes=5))
    assert lock.refresh() is False


def test_heartbeat_refreshes_until_stopped():
    s3 = FakeS3()
    lock = S3Lock.create(s3, URI, owner="ci", ttl_seconds=60)
    lock.acquire()
    first_etag = s3.objects[("amzn-s3-demo-bucket", "schema-evolution/locks/dev.lock")][1]

    stop = lock.start_heartbeat(interval_seconds=0.01)
    threading.Event().wait(0.1)
    stop.set()

    assert s3.objects[("amzn-s3-demo-bucket", "schema-evolution/locks/dev.lock")][1] != first_etag
    lock.release()
    assert s3.objects == {}


def test_conflict_is_reported_as_not_acquired():
    s3 = MagicMock()
    s3.put_object.side_effect = _client_error("ConditionalRequestConflict")

    assert S3Lock.create(s3, URI, owner="ci").try_acquire() is False


def test_unexpected_error_is_raised():
    s3 = MagicMock()
    s3.put_object.side_effect = _client_error("AccessDenied")

    with pytest.raises(ClientError):
        S3Lock.create(s3, URI, owner="ci").try_acquire()


def test_invalid_uri_rejected():
    with pytest.raises(ValueError):
        S3Lock.create(FakeS3(), "bucket/key", owner="ci").try_acquire()


def test_lock_uri_for():
    assert lock_uri_for("123", "us-west-2", "qa") == (
        "s3://dp-qa-schema-evolution-123-us-west-2/locks/qa.lock"
    )


# ---------------------------------------------------------------------------
# apply_migrations integration
# ---------------------------------------------------------------------------


def _args(root: str, **overrides) -> argparse.Namespace:
    values = {
        "migrations_dir": root,
        "environment": "dev",
        "region": "us-west-2",
        "account_id": "111111111111",
        "bucket_prefix": "dp",
        "glue_job_name": "dp-dev-glue-schema-evolution-backfill",
        "lock_uri": URI,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def _write_plan(root: str) -> None:
    table_dir = os.path.join(root, "gold", "t1")
    os.makedirs(table_dir)
    with open(os.path.join(table_dir, "p1.json"), "w") as f:
        json.dump(
            {
                "metadata": {"layer": "gold", "table": "t1"},
                "changes": [{"change_type": "add_column", "column_name": "x", "target_type": "string"}],
            },
            f,
        )


def _run(s3: FakeS3, **overrides):
    boto3_mock = MagicMock()
    boto3_mock.client.side_effect = lambda service, **_kw: s3 if service == "s3" else MagicMock()
    with (
        tempfile.TemporaryDirectory() as root,
        patch("schema_evolution.apply.boto3", boto3_mock),
        patch("schema_evolution.apply.ensure_tracker"),
        patch("schema_evolution.apply.load_catalog", return_value=MagicMock()),
        patch("schema_evolution.apply.read_tracker_snapshot", return_value={}),
        patch("schema_evolution.apply.apply_ddl", return_value=(True, None)) as mock_ddl,
        patch("schema_evolution.apply.batch_upsert_tracker"),
    ):
        _write_plan(root)
        result = apply_migrations(_args(root, **overrides), return_details=True)
    return result, mock_ddl


def test_apply_takes_and_releases_lock():
    s3 = FakeS3()
    result, mock_ddl = _run(s3)

    assert result == ApplyResult(success=True, pending_work=True)
    assert mock_ddl.call_count == 1
    assert s3.objects == {}


def test_cli_pass_fails_when_lock_stays_held():
    s3 = FakeS3()
    s3.plant("other-deploy", datetime.now(UTC) + timedelta(minutes=10))

    result, mock_ddl = _run(s3, lock_wait_seconds=0)

    assert result.success is False
    mock_ddl.assert_not_called()


def test_lock_released_when_pass_raises():
    s3 = FakeS3()
    boto3_mock = MagicMock()
    boto3_mock.client.side_effect = lambda service, **_kw: s3 if service == "s3" else MagicMock()
    with (
        tempfile.TemporaryDirectory() as root,
        patch("schema_evolution.apply.boto3", boto3_mock),
        patch("schema_evolution.apply.ensure_tracker", side_effect=RuntimeError("catalog down")),
        patch("schema_evolution.apply.load_catalog", return_value=MagicMock()),
    ):
        _write_plan(root)
        with pytest.raises(RuntimeError):
            apply_migrations(_args(root))

    assert s3.objects == {}


def test_no_lock_uri_means_no_lock_calls():
    s3 = FakeS3()
    s3.plant("someone", datetime.now(UTC) + timedelta(minutes=10))

    result, mock_ddl = _run(s3, lock_uri=None)

    # The planted lock is ignored and left alone when locking is disabled.
    assert result.success is True
    assert mock_ddl.call_count == 1
    assert s3.body()["owner"] == "someone"


def test_cli_derives_lock_uri_and_allows_disabling():
    args = _build_args_from_argv(["--account_id", "123", "--environment", "qa"])
    assert args.lock_uri == "s3://dp-qa-schema-evolution-123-us-west-2/locks/qa.lock"
    assert args.lock_wait_seconds == 900

    args = _build_args_from_argv(["--account_id", "123", "--environment", "qa", "--lock_uri", "none"])
    assert args.lock_uri is None
