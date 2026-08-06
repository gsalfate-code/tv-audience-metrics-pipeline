"""Integration test: silver cleaning round-trips through real S3 (FR-003, FR-015).

Only runs in GitHub Actions (tests/conftest.py auto-skips this file otherwise).
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
from src.silver.clean import clean_partition

pytestmark = pytest.mark.integration

BUCKET_NAME = os.environ.get("BUCKET_NAME", "")
TEST_PREFIX = f"_integration_tests/{uuid.uuid4().hex[:8]}"
PROCESADO_EN = datetime(2026, 8, 6, 12, 0, 0)


@pytest.fixture(autouse=True)
def _cleanup_s3():
    yield
    keys = s3_io.list_partition_keys(BUCKET_NAME, TEST_PREFIX)
    if keys:
        boto3.client("s3").delete_objects(
            Bucket=BUCKET_NAME, Delete={"Objects": [{"Key": key} for key in keys]}
        )


def test_silver_partition_round_trips_through_s3(tmp_path: Path) -> None:
    # Arrange: a bronze partition already sitting in S3, with one duplicate natural key.
    events = [
        {
            "timestamp": "2026-08-01T10:00:00",
            "canal": "Canal1",
            "id_hogar_panelista": "hogar-0001",
            "universo_total": 100_000,
        },
        {
            "timestamp": "2026-08-01T10:00:00",
            "canal": "Canal1",
            "id_hogar_panelista": "hogar-0001",
            "universo_total": 150_000,
        },
        {
            "timestamp": "2026-08-01T10:05:00",
            "canal": "Canal2",
            "id_hogar_panelista": "hogar-0002",
            "universo_total": 100_000,
        },
    ]
    bronze_local = tmp_path / "bronze_local"
    ingest_to_local(events, bronze_local, procesado_en=PROCESADO_EN)
    bronze_prefix = f"{TEST_PREFIX}/bronze/fecha=2026-08-01"
    s3_io.upload_partition(bronze_local / "fecha=2026-08-01", BUCKET_NAME, bronze_prefix)

    # Act: a job downloads the bronze partition fresh from S3 and cleans it.
    bronze_downloaded = tmp_path / "bronze_downloaded"
    s3_io.download_partition(BUCKET_NAME, bronze_prefix, bronze_downloaded)

    silver_local = tmp_path / "silver_local"
    summary = clean_partition(bronze_downloaded, silver_local, procesado_en=PROCESADO_EN)
    assert summary.output_rows == 2  # duplicate natural key collapsed to 1 + 1 other row

    for canal in ("Canal1", "Canal2"):
        silver_prefix = f"{TEST_PREFIX}/silver/fecha=2026-08-01/canal={canal}"
        s3_io.upload_partition(silver_local / "fecha=2026-08-01" / f"canal={canal}", BUCKET_NAME, silver_prefix)

    # Assert: reading back from S3 shows the deduplicated result.
    silver_downloaded = tmp_path / "silver_downloaded"
    s3_io.download_partition(BUCKET_NAME, f"{TEST_PREFIX}/silver/fecha=2026-08-01/canal=Canal1", silver_downloaded)
    result = duckdb.sql(
        f"SELECT universo_total FROM read_parquet('{silver_downloaded.as_posix()}/*.parquet')"
    ).fetchall()
    assert result == [(150_000,)]
