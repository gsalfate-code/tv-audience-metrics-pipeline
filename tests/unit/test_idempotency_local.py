"""Unit test: bronze -> silver -> gold end-to-end idempotency, without touching AWS (FR-008).

Exercises the full chain twice and compares logical content via DuckDB hashing
(research.md, decision 5) — never raw Parquet bytes, per the constitution's testing
gate.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from src.bronze.ingest import ingest_to_local
from src.generator.events import generate_events
from src.gold.metrics import compute_metrics
from src.silver.clean import clean_partition
from tests.integration._hash_helpers import hash_partition_content, row_count

PROCESADO_EN = datetime(2026, 8, 6, 12, 0, 0)
CANALES = ["Canal1", "Canal2", "Canal3"]


def _run_full_pipeline(root: Path) -> None:
    events = generate_events(seed=7, fecha=date(2026, 8, 1), canales=CANALES, num_hogares=20)

    bronze_root = root / "bronze"
    ingest_to_local(events, bronze_root, procesado_en=PROCESADO_EN)

    silver_root = root / "silver"
    clean_partition(bronze_root / "fecha=2026-08-01", silver_root, procesado_en=PROCESADO_EN)

    gold_root = root / "gold"
    compute_metrics(silver_root / "fecha=2026-08-01", gold_root, procesado_en=PROCESADO_EN, canales=CANALES)


def test_full_pipeline_is_idempotent_across_independent_runs(tmp_path: Path) -> None:
    run_1 = tmp_path / "run1"
    run_2 = tmp_path / "run2"
    _run_full_pipeline(run_1)
    _run_full_pipeline(run_2)

    for layer in ("bronze", "silver", "gold"):
        hash_1 = hash_partition_content(run_1 / layer)
        hash_2 = hash_partition_content(run_2 / layer)
        assert hash_1 == hash_2, f"{layer} content differs between independent runs"
        assert row_count(run_1 / layer) == row_count(run_2 / layer)


def test_rerunning_into_the_same_local_root_does_not_duplicate_rows(tmp_path: Path) -> None:
    root = tmp_path / "run"
    _run_full_pipeline(root)
    rows_after_first_run = {layer: row_count(root / layer) for layer in ("bronze", "silver", "gold")}

    _run_full_pipeline(root)  # re-run into the same local root
    rows_after_second_run = {layer: row_count(root / layer) for layer in ("bronze", "silver", "gold")}

    assert rows_after_first_run == rows_after_second_run
