"""Integration test: bronze ingestion round-trips through real S3 (FR-002, FR-004).

Only runs in GitHub Actions, where BUCKET_NAME/AWS_REGION and OIDC-assumed credentials
are present (tests/conftest.py auto-skips this file otherwise).
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime
from pathlib import Path

import boto3
import duckdb
import pytest

from src import s3_io
from src.bronze.ingest import ingest_to_local

pytestmark = pytest.mark.integration

BUCKET_NAME = os.environ.get("BUCKET_NAME", "")
TEST_PREFIX = f"_integration_tests/{uuid.uuid4().hex[:8]}"


@pytest.fixture(autouse=True)
def _cleanup_s3():
    yield
    keys = s3_io.list_partition_keys(BUCKET_NAME, TEST_PREFIX)
    if keys:
        boto3.client("s3").delete_objects(
            Bucket=BUCKET_NAME, Delete={"Objects": [{"Key": key} for key in keys]}
        )


def test_bronze_partition_round_trips_through_s3(tmp_path: Path) -> None:
    events = [
        {
            "timestamp": "2026-08-01T10:00:00",
            "canal": "Canal1",
            "id_hogar_panelista": "hogar-0001",
            "universo_total": 100_000,
        },
        {
            "timestamp": "2026-08-01T10:01:00",
            "canal": "Canal2",
            "id_hogar_panelista": "hogar-0002",
            "universo_total": 100_000,
        },
    ]
    local_dir = tmp_path / "bronze_local"
    summary = ingest_to_local(events, local_dir, procesado_en=datetime(2026, 8, 6, 12, 0, 0))
    assert summary.accepted == 2

    partition_prefix = f"{TEST_PREFIX}/bronze/fecha=2026-08-01"
    uploaded = s3_io.upload_partition(local_dir / "fecha=2026-08-01", BUCKET_NAME, partition_prefix)
    assert uploaded

    download_dir = tmp_path / "bronze_downloaded"
    downloaded = s3_io.download_partition(BUCKET_NAME, partition_prefix, download_dir)
    assert downloaded

    result = duckdb.sql(f"SELECT count(*) FROM read_parquet('{download_dir.as_posix()}/*.parquet')").fetchone()
    assert result[0] == 2


def test_reuploading_the_same_partition_does_not_duplicate(tmp_path: Path) -> None:
    events = [
        {
            "timestamp": "2026-08-01T10:00:00",
            "canal": "Canal1",
            "id_hogar_panelista": "hogar-0001",
            "universo_total": 100_000,
        },
    ]
    local_dir = tmp_path / "bronze_local"
    ingest_to_local(events, local_dir, procesado_en=datetime(2026, 8, 6, 12, 0, 0))

    partition_prefix = f"{TEST_PREFIX}/bronze/fecha=2026-08-01"
    s3_io.upload_partition(local_dir / "fecha=2026-08-01", BUCKET_NAME, partition_prefix)
    s3_io.upload_partition(local_dir / "fecha=2026-08-01", BUCKET_NAME, partition_prefix)

    keys = s3_io.list_partition_keys(BUCKET_NAME, partition_prefix)
    assert len(keys) == 1
