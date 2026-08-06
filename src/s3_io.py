"""Thin boto3 wrappers for S3 partition I/O.

All S3 access in the pipeline goes through this module (research.md, decision 1) —
DuckDB never talks to S3 directly, it only ever reads/writes local files. Credentials
always come from the environment / AWS profile chain locally, or an OIDC-assumed role
in GitHub Actions — never hardcoded (constitution Principle V).
"""

from __future__ import annotations

from pathlib import Path

import boto3

_DELETE_BATCH_SIZE = 1000  # S3 DeleteObjects API limit per request


def _client():
    return boto3.client("s3")


def list_partition_keys(bucket: str, prefix: str) -> list[str]:
    """Return every object key under `prefix` in `bucket` (paginated)."""
    client = _client()
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def download_partition(bucket: str, prefix: str, local_dir: Path) -> list[Path]:
    """Download every object under `prefix` into `local_dir`, preserving relative paths.

    Returns the local file paths written. An empty result (no error) means the
    partition does not exist yet — a date/channel that was never processed is a valid
    state, not a failure (spec.md Edge Cases).
    """
    local_dir = Path(local_dir)
    client = _client()
    written: list[Path] = []
    for key in list_partition_keys(bucket, prefix):
        relative = key[len(prefix) :].lstrip("/")
        if not relative:
            continue
        destination = local_dir / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        client.download_file(bucket, key, str(destination))
        written.append(destination)
    return written


def upload_partition(local_dir: Path, bucket: str, prefix: str) -> list[str]:
    """Delete-then-write: replace every object under `prefix` with `local_dir`'s contents.

    Implements the idempotency mechanic mandated by the constitution (Principle I):
    delete the objects already in the partition, then upload the new ones — never
    append to an existing partition.
    """
    local_dir = Path(local_dir)
    client = _client()

    existing_keys = list_partition_keys(bucket, prefix)
    for start in range(0, len(existing_keys), _DELETE_BATCH_SIZE):
        batch = existing_keys[start : start + _DELETE_BATCH_SIZE]
        client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in batch]},
        )

    uploaded_keys: list[str] = []
    for path in sorted(local_dir.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(local_dir).as_posix()
        key = f"{prefix.rstrip('/')}/{relative}"
        client.upload_file(str(path), bucket, key)
        uploaded_keys.append(key)
    return uploaded_keys
