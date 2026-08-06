"""Integration test: gold aggregation round-trips through real S3 (SC-004).

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
from src.gold.metrics import compute_metrics

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


def _write_and_upload_silver_fixture(tmp_path: Path) -> None:
    con = duckdb.connect()
    con.execute(
        "CREATE TEMP TABLE t (id_hogar_panelista VARCHAR, canal VARCHAR, timestamp TIMESTAMP, universo_total BIGINT)"
    )
    con.execute(
        "INSERT INTO t VALUES (?, ?, ?, ?), (?, ?, ?, ?)",
        [
            "hogar-0001", "Canal1", datetime(2026, 8, 1, 10, 5), 100_000,
            "hogar-0002", "Canal2", datetime(2026, 8, 1, 10, 10), 100_000,
        ],
    )
    for canal in ("Canal1", "Canal2"):
        local_dir = tmp_path / "silver_local" / f"canal={canal}"
        local_dir.mkdir(parents=True, exist_ok=True)
        con.sql(
            f"COPY (SELECT * FROM t WHERE canal = '{canal}') "
            f"TO '{(local_dir / 'part-0000.parquet').as_posix()}' (FORMAT PARQUET)"
        )
        s3_io.upload_partition(local_dir, BUCKET_NAME, f"{TEST_PREFIX}/silver/fecha=2026-08-01/canal={canal}")


def test_gold_report_round_trips_and_shares_sum_to_100(tmp_path: Path) -> None:
    _write_and_upload_silver_fixture(tmp_path)

    silver_downloaded = tmp_path / "silver_downloaded"
    for canal in ("Canal1", "Canal2"):
        s3_io.download_partition(
            BUCKET_NAME,
            f"{TEST_PREFIX}/silver/fecha=2026-08-01/canal={canal}",
            silver_downloaded / f"canal={canal}",
        )

    gold_local = tmp_path / "gold_local"
    summary = compute_metrics(silver_downloaded, gold_local, procesado_en=PROCESADO_EN, canales=["Canal1", "Canal2"])
    assert summary.output_rows == 24 * 2

    for canal in ("Canal1", "Canal2"):
        s3_io.upload_partition(
            gold_local / "fecha=2026-08-01" / f"canal={canal}",
            BUCKET_NAME,
            f"{TEST_PREFIX}/gold/fecha=2026-08-01/canal={canal}",
        )

    gold_downloaded = tmp_path / "gold_downloaded"
    for canal in ("Canal1", "Canal2"):
        s3_io.download_partition(
            BUCKET_NAME,
            f"{TEST_PREFIX}/gold/fecha=2026-08-01/canal={canal}",
            gold_downloaded / f"canal={canal}",
        )

    total_share = duckdb.sql(
        f"SELECT sum(share_pct) FROM read_parquet('{gold_downloaded.as_posix()}/*/*.parquet') "
        "WHERE franja_horaria = 10"
    ).fetchone()[0]
    assert total_share == pytest.approx(100.0)
