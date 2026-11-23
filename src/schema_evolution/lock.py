"""Run lock for schema migrations, backed by an S3 object and conditional writes.

Two deploys running ``schema-evolution-apply`` at once share one tracker per
layer. Their DDL is idempotent, but both could start a backfill Glue job for the
same table. This lock lets only one pass run per environment.

S3 conditional requests make this safe without extra infrastructure:

- Acquire: ``PutObject`` with ``If-None-Match: *`` succeeds only if no lock exists.
- Take over an expired lock: ``PutObject`` with ``If-Match: <etag>`` succeeds only
  if nobody else replaced it first.
- Release: ``DeleteObject`` with ``If-Match: <etag>`` deletes only our own lock.

The lock carries an expiry, so a holder that crashes (e.g. a CI job killed mid-run)
blocks others for at most ``ttl_seconds``.
"""

import json
import logging
import socket
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Refreshed by a heartbeat while the pass runs, so a live holder
# never loses the lock, while a crashed holder releases it within 20 minutes.
DEFAULT_TTL_SECONDS = 20 * 60

_PRECONDITION_CODES = {"PreconditionFailed", "412"}
_CONFLICT_CODES = {"ConditionalRequestConflict", "409", "OperationAborted"}
_NOT_FOUND_CODES = {"NoSuchKey", "404", "NotFound"}


class LockNotAcquiredError(RuntimeError):
    """Raised when the lock is held by another live run."""


def _error_code(error: ClientError) -> str:
    return str(error.response.get("Error", {}).get("Code", ""))


def artifacts_bucket_for(account_id: str, region: str, environment: str, bucket_prefix: str = "dp") -> str:
    """Return the artifacts bucket name created by the toolkit's CDK stack."""
    return f"{bucket_prefix}-{environment}-schema-evolution-{account_id}-{region}"


def lock_uri_for(account_id: str, region: str, environment: str, bucket_prefix: str = "dp") -> str:
    """Return the default lock location in the toolkit's artifacts bucket."""
    bucket = artifacts_bucket_for(account_id, region, environment, bucket_prefix)
    return f"s3://{bucket}/locks/{environment}.lock"


def _split_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"lock URI must start with s3://, got {uri!r}")
    bucket, _, key = uri[len("s3://") :].partition("/")
    if not bucket or not key:
        raise ValueError(f"lock URI must include a bucket and key, got {uri!r}")
    return bucket, key


@dataclass
class S3Lock:
    """An S3-object lock. Use :meth:`acquire` and :meth:`release`, or ``with``."""

    s3_client: object
    uri: str
    owner: str
    ttl_seconds: int = DEFAULT_TTL_SECONDS
    _etag: str | None = None
    # Serializes refresh (heartbeat thread) and release (caller) on ``_etag``.
    _mutex: threading.Lock = field(default_factory=threading.Lock, repr=False)

    @classmethod
    def create(cls, s3_client: object, uri: str, owner: str, ttl_seconds: int = DEFAULT_TTL_SECONDS) -> "S3Lock":
        """Build a lock with a unique owner id (``owner`` plus host and a random suffix)."""
        unique = f"{owner}@{socket.gethostname()}#{uuid.uuid4().hex[:8]}"
        return cls(s3_client=s3_client, uri=uri, owner=unique, ttl_seconds=ttl_seconds)

    def _body(self) -> bytes:
        now = datetime.now(UTC)
        return json.dumps(
            {
                "owner": self.owner,
                "acquired_at": now.isoformat(),
                "expires_at": (now + timedelta(seconds=self.ttl_seconds)).isoformat(),
            }
        ).encode("utf-8")

    def _read_current(self) -> tuple[dict, str] | None:
        """Return ``(body, etag)`` of the current lock, or ``None`` if absent."""
        bucket, key = _split_uri(self.uri)
        try:
            resp = self.s3_client.get_object(Bucket=bucket, Key=key)  # type: ignore[attr-defined]
        except ClientError as error:
            if _error_code(error) in _NOT_FOUND_CODES:
                return None
            raise
        try:
            body = json.loads(resp["Body"].read() or b"{}")
        except ValueError:
            body = {}
        return body, resp["ETag"]

    def try_acquire(self) -> bool:
        """Try once to take the lock. Returns True on success."""
        bucket, key = _split_uri(self.uri)
        try:
            resp = self.s3_client.put_object(  # type: ignore[attr-defined]
                Bucket=bucket, Key=key, Body=self._body(), IfNoneMatch="*", ContentType="application/json"
            )
            self._etag = resp["ETag"]
            logger.info("Acquired migration lock %s as %s", self.uri, self.owner)
            return True
        except ClientError as error:
            if _error_code(error) in _CONFLICT_CODES:
                return False
            if _error_code(error) not in _PRECONDITION_CODES:
                raise

        current = self._read_current()
        if current is None:
            # Released between our write and our read; let the caller retry.
            return False
        body, etag = current
        expires_at = body.get("expires_at")
        try:
            expired = expires_at is None or datetime.fromisoformat(expires_at) <= datetime.now(UTC)
        except ValueError:
            expired = True
        if not expired:
            logger.info("Migration lock %s held by %s until %s", self.uri, body.get("owner"), expires_at)
            return False

        logger.warning("Taking over expired migration lock %s from %s", self.uri, body.get("owner"))
        try:
            resp = self.s3_client.put_object(  # type: ignore[attr-defined]
                Bucket=bucket, Key=key, Body=self._body(), IfMatch=etag, ContentType="application/json"
            )
        except ClientError as error:
            if _error_code(error) in _PRECONDITION_CODES | _CONFLICT_CODES | _NOT_FOUND_CODES:
                return False
            raise
        self._etag = resp["ETag"]
        return True

    def acquire(self, wait_seconds: float = 0, poll_seconds: float = 15) -> None:
        """Take the lock, polling for up to ``wait_seconds``.

        Raises:
            LockNotAcquiredError: If the lock is still held after waiting.
        """
        deadline = time.monotonic() + wait_seconds
        while True:
            if self.try_acquire():
                return
            if time.monotonic() >= deadline:
                raise LockNotAcquiredError(f"Migration lock {self.uri} is held by another run")
            time.sleep(poll_seconds)

    def refresh(self) -> bool:
        """Extend the expiry of a lock we hold. Returns False if it was lost."""
        with self._mutex:
            return self._refresh_locked()

    def _refresh_locked(self) -> bool:
        if self._etag is None:
            return False
        bucket, key = _split_uri(self.uri)
        try:
            resp = self.s3_client.put_object(  # type: ignore[attr-defined]
                Bucket=bucket, Key=key, Body=self._body(), IfMatch=self._etag, ContentType="application/json"
            )
        except ClientError as error:
            if _error_code(error) in _PRECONDITION_CODES | _NOT_FOUND_CODES:
                logger.error("Lost migration lock %s to another run", self.uri)
                self._etag = None
                return False
            if _error_code(error) in _CONFLICT_CODES:
                return True  # transient; the next heartbeat retries
            raise
        self._etag = resp["ETag"]
        return True

    def start_heartbeat(self, interval_seconds: float | None = None) -> threading.Event:
        """Refresh the lock in a daemon thread until the returned event is set.

        Keeps a long pass (e.g. one waiting on locked tables) from outliving the
        TTL and letting a second run in.
        """
        stop = threading.Event()
        interval = interval_seconds if interval_seconds is not None else max(1.0, self.ttl_seconds / 3)

        def _beat() -> None:
            while not stop.wait(interval):
                try:
                    if not self.refresh():
                        return
                except Exception as exc:  # keep beating through transient errors
                    logger.warning("Migration lock heartbeat failed: %s", exc)

        threading.Thread(target=_beat, name="schema-evo-lock", daemon=True).start()
        return stop

    def release(self) -> None:
        """Delete the lock if we still own it. Never raises on a lost lock."""
        with self._mutex:
            self._release_locked()

    def _release_locked(self) -> None:
        if self._etag is None:
            return
        bucket, key = _split_uri(self.uri)
        try:
            self.s3_client.delete_object(Bucket=bucket, Key=key, IfMatch=self._etag)  # type: ignore[attr-defined]
            logger.info("Released migration lock %s", self.uri)
        except ClientError as error:
            if _error_code(error) in _PRECONDITION_CODES | _NOT_FOUND_CODES:
                logger.warning("Migration lock %s was taken over before release", self.uri)
            else:
                raise
        finally:
            self._etag = None
