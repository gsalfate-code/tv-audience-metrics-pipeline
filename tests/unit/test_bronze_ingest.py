"""Unit tests for bronze schema validation and ingestion (FR-002, FR-004)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb

from src.bronze.ingest import ingest_to_local

PROCESADO_EN = datetime(2026, 8, 6, 12, 0, 0)


def test_valid_events_are_written_to_bronze_partition(tmp_path: Path, sample_raw_events: list[dict]) -> None:
    summary = ingest_to_local(sample_raw_events, tmp_path, procesado_en=PROCESADO_EN)

    assert summary.accepted == 3
    assert summary.rejected == 0

    result = duckdb.sql(
        f"SELECT canal, id_hogar_panelista, universo_total "
        f"FROM read_parquet('{tmp_path.as_posix()}/fecha=2026-08-01/*.parquet') "
        f"ORDER BY canal, id_hogar_panelista"
    ).fetchall()
    assert result == [
        ("Canal1", "hogar-0001", 100_000),
        ("Canal1", "hogar-0001", 100_000),
        ("Canal2", "hogar-0002", 100_000),
    ]


def test_invalid_events_are_rejected_without_stopping_the_batch(tmp_path: Path) -> None:
    events = [
        {
            "timestamp": "2026-08-01T10:00:00",
            "canal": "Canal1",
            "id_hogar_panelista": "hogar-0001",
            "universo_total": 100_000,
        },
        {
            "timestamp": "2026-08-01T10:01:00",
            "canal": "",
            "id_hogar_panelista": "hogar-0002",
            "universo_total": 100_000,
        },
        {
            "timestamp": None,
            "canal": "Canal1",
            "id_hogar_panelista": "hogar-0003",
            "universo_total": 100_000,
        },
    ]

    summary = ingest_to_local(events, tmp_path, procesado_en=PROCESADO_EN)

    assert summary.accepted == 1
    assert summary.rejected == 2
    assert len(summary.rejected_reasons) == 2

    result = duckdb.sql(
        f"SELECT count(*) FROM read_parquet('{tmp_path.as_posix()}/fecha=2026-08-01/*.parquet')"
    ).fetchone()
    assert result[0] == 1


def test_events_partition_by_their_own_date(tmp_path: Path) -> None:
    events = [
        {
            "timestamp": "2026-08-01T23:59:00",
            "canal": "Canal1",
            "id_hogar_panelista": "hogar-0001",
            "universo_total": 100_000,
        },
        {
            "timestamp": "2026-08-02T00:01:00",
            "canal": "Canal1",
            "id_hogar_panelista": "hogar-0002",
            "universo_total": 100_000,
        },
    ]

    summary = ingest_to_local(events, tmp_path, procesado_en=PROCESADO_EN)

    assert summary.accepted == 2
    assert (tmp_path / "fecha=2026-08-01").exists()
    assert (tmp_path / "fecha=2026-08-02").exists()


def test_empty_batch_produces_no_partitions(tmp_path: Path) -> None:
    summary = ingest_to_local([], tmp_path, procesado_en=PROCESADO_EN)

    assert summary.accepted == 0
    assert summary.rejected == 0
    assert list(tmp_path.iterdir()) == []
