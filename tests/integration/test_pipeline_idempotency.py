"""Integration test: full bronze -> silver -> gold pipeline is idempotent against real S3 (SC-002).

Runs the pipeline twice over the same date range against the real bucket and compares
the logical content of each layer (never raw Parquet bytes — research.md, decision 5).
Only runs in GitHub Actions (tests/conftest.py auto-skips this file otherwise).
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime
from pathlib import Path

import boto3
import pytest

from src import s3_io
from src.bronze.ingest import ingest_to_local
from src.generator.events import generate_events
from src.gold.metrics import compute_metrics
from src.silver.clean import clean_partition
from tests.integration._hash_helpers import hash_partition_content, row_count

pytestmark = pytest.mark.integration

BUCKET_NAME = os.environ.get("BUCKET_NAME", "")
TEST_PREFIX = f"_integration_tests/{uuid.uuid4().hex[:8]}"
PROCESADO_EN = datetime(2026, 8, 6, 12, 0, 0)
CANALES = ["Canal1", "Canal2", "Canal3"]
FECHA = date(2026, 8, 1)


@pytest.fixture(autouse=True)
def _cleanup_s3():
    yield
    keys = s3_io.list_partition_keys(BUCKET_NAME, TEST_PREFIX)
    if keys:
        boto3.client("s3").delete_objects(
            Bucket=BUCKET_NAME, Delete={"Objects": [{"Key": key} for key in keys]}
        )


def _run_pipeline_against_s3(work_dir: Path) -> None:
    events = generate_events(seed=7, fecha=FECHA, canales=CANALES, num_hogares=20)

    bronze_local = work_dir / "bronze_local"
    ingest_to_local(events, bronze_local, procesado_en=PROCESADO_EN)
    bronze_prefix = f"{TEST_PREFIX}/bronze/fecha={FECHA.isoformat()}"
    s3_io.upload_partition(bronze_local / f"fecha={FECHA.isoformat()}", BUCKET_NAME, bronze_prefix)

    bronze_downloaded = work_dir / "bronze_downloaded"
    s3_io.download_partition(BUCKET_NAME, bronze_prefix, bronze_downloaded)

    silver_local = work_dir / "silver_local"
    clean_partition(bronze_downloaded, silver_local, procesado_en=PROCESADO_EN)
    for canal in CANALES:
        s3_io.upload_partition(
            silver_local / f"fecha={FECHA.isoformat()}" / f"canal={canal}",
            BUCKET_NAME,
            f"{TEST_PREFIX}/silver/fecha={FECHA.isoformat()}/canal={canal}",
        )

    silver_downloaded = work_dir / "silver_downloaded"
    for canal in CANALES:
        s3_io.download_partition(
            BUCKET_NAME,
            f"{TEST_PREFIX}/silver/fecha={FECHA.isoformat()}/canal={canal}",
            silver_downloaded / f"canal={canal}",
        )

    gold_local = work_dir / "gold_local"
    compute_metrics(silver_downloaded, gold_local, procesado_en=PROCESADO_EN, canales=CANALES)
    for canal in CANALES:
        s3_io.upload_partition(
            gold_local / f"fecha={FECHA.isoformat()}" / f"canal={canal}",
            BUCKET_NAME,
            f"{TEST_PREFIX}/gold/fecha={FECHA.isoformat()}/canal={canal}",
        )


def _download_all_layers(work_dir: Path) -> dict[str, Path]:
    downloaded = {}
    for layer in ("bronze", "silver", "gold"):
        destination = work_dir / f"{layer}_final"
        s3_io.download_partition(BUCKET_NAME, f"{TEST_PREFIX}/{layer}", destination)
        downloaded[layer] = destination
    return downloaded


def test_pipeline_is_idempotent_when_rerun_against_real_s3(tmp_path: Path) -> None:
    _run_pipeline_against_s3(tmp_path / "run1")
    after_first_run = _download_all_layers(tmp_path / "check1")
    keys_after_first_run = {
        layer: len(s3_io.list_partition_keys(BUCKET_NAME, f"{TEST_PREFIX}/{layer}")) for layer in ("bronze", "silver", "gold")
    }

    _run_pipeline_against_s3(tmp_path / "run2")  # re-run over the same date range
    after_second_run = _download_all_layers(tmp_path / "check2")
    keys_after_second_run = {
        layer: len(s3_io.list_partition_keys(BUCKET_NAME, f"{TEST_PREFIX}/{layer}")) for layer in ("bronze", "silver", "gold")
    }

    for layer in ("bronze", "silver", "gold"):
        assert hash_partition_content(after_first_run[layer]) == hash_partition_content(after_second_run[layer]), (
            f"{layer} logical content changed after re-running the pipeline"
        )
        assert row_count(after_first_run[layer]) == row_count(after_second_run[layer])
        # delete-then-write per partition means the object count must not grow (SC-002)
        assert keys_after_first_run[layer] == keys_after_second_run[layer]
