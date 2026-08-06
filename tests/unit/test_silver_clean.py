"""Unit tests for silver deduplication/typing/null-handling (FR-003, FR-015)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb

from src.silver.clean import clean_partition

PROCESADO_EN = datetime(2026, 8, 6, 12, 0, 0)


def _write_bronze_fixture(bronze_dir: Path, rows: list[dict]) -> None:
    """Write a bronze-shaped Parquet file directly, bypassing bronze validation, so
    silver's own null-handling and dedup logic can be exercised in isolation."""
    bronze_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(
        "CREATE TEMP TABLE bronze_rows "
        "(timestamp TIMESTAMP, canal VARCHAR, id_hogar_panelista VARCHAR, universo_total BIGINT)"
    )
    for row in rows:
        con.execute(
            "INSERT INTO bronze_rows VALUES (?, ?, ?, ?)",
            [row["timestamp"], row.get("canal"), row.get("id_hogar_panelista"), row.get("universo_total")],
        )
    con.sql(f"COPY bronze_rows TO '{(bronze_dir / 'part-0000.parquet').as_posix()}' (FORMAT PARQUET)")


def test_duplicate_natural_key_resolves_deterministically(tmp_path: Path) -> None:
    bronze_dir = tmp_path / "bronze"
    silver_root = tmp_path / "silver"
    _write_bronze_fixture(
        bronze_dir,
        [
            {
                "timestamp": datetime(2026, 8, 1, 10, 0),
                "canal": "Canal1",
                "id_hogar_panelista": "hogar-0001",
                "universo_total": 100_000,
            },
            {
                "timestamp": datetime(2026, 8, 1, 10, 0),
                "canal": "Canal1",
                "id_hogar_panelista": "hogar-0001",
                "universo_total": 150_000,
            },
        ],
    )

    summary = clean_partition(bronze_dir, silver_root, procesado_en=PROCESADO_EN)

    assert summary.output_rows == 1
    result = duckdb.sql(
        f"SELECT universo_total FROM read_parquet('{silver_root.as_posix()}/fecha=2026-08-01/canal=Canal1/*.parquet')"
    ).fetchall()
    assert result == [(150_000,)]


def test_resolution_is_stable_across_independent_runs(tmp_path: Path) -> None:
    """Determinism check: the winning row must not depend on row order or run count."""
    bronze_dir = tmp_path / "bronze"
    silver_root_1 = tmp_path / "silver1"
    silver_root_2 = tmp_path / "silver2"
    rows = [
        {
            "timestamp": datetime(2026, 8, 1, 10, 0),
            "canal": "Canal1",
            "id_hogar_panelista": "hogar-0001",
            "universo_total": 150_000,
        },
        {
            "timestamp": datetime(2026, 8, 1, 10, 0),
            "canal": "Canal1",
            "id_hogar_panelista": "hogar-0001",
            "universo_total": 100_000,
        },
    ]
    _write_bronze_fixture(bronze_dir, rows)

    summary_1 = clean_partition(bronze_dir, silver_root_1, procesado_en=PROCESADO_EN)
    summary_2 = clean_partition(bronze_dir, silver_root_2, procesado_en=PROCESADO_EN)

    result_1 = duckdb.sql(
        f"SELECT universo_total FROM read_parquet('{silver_root_1.as_posix()}/fecha=2026-08-01/canal=Canal1/*.parquet')"
    ).fetchall()
    result_2 = duckdb.sql(
        f"SELECT universo_total FROM read_parquet('{silver_root_2.as_posix()}/fecha=2026-08-01/canal=Canal1/*.parquet')"
    ).fetchall()
    assert result_1 == result_2 == [(150_000,)]
    assert summary_1.output_rows == summary_2.output_rows == 1


def test_rows_with_null_required_fields_are_excluded(tmp_path: Path) -> None:
    bronze_dir = tmp_path / "bronze"
    silver_root = tmp_path / "silver"
    _write_bronze_fixture(
        bronze_dir,
        [
            {
                "timestamp": datetime(2026, 8, 1, 10, 0),
                "canal": "Canal1",
                "id_hogar_panelista": "hogar-0001",
                "universo_total": 100_000,
            },
            {
                "timestamp": datetime(2026, 8, 1, 10, 1),
                "canal": None,
                "id_hogar_panelista": "hogar-0002",
                "universo_total": 100_000,
            },
        ],
    )

    summary = clean_partition(bronze_dir, silver_root, procesado_en=PROCESADO_EN)

    assert summary.output_rows == 1
    assert summary.excluded_null_rows == 1


def test_partitions_by_fecha_and_canal(tmp_path: Path) -> None:
    bronze_dir = tmp_path / "bronze"
    silver_root = tmp_path / "silver"
    _write_bronze_fixture(
        bronze_dir,
        [
            {
                "timestamp": datetime(2026, 8, 1, 10, 0),
                "canal": "Canal1",
                "id_hogar_panelista": "hogar-0001",
                "universo_total": 100_000,
            },
            {
                "timestamp": datetime(2026, 8, 1, 10, 0),
                "canal": "Canal2",
                "id_hogar_panelista": "hogar-0002",
                "universo_total": 100_000,
            },
        ],
    )

    clean_partition(bronze_dir, silver_root, procesado_en=PROCESADO_EN)

    assert (silver_root / "fecha=2026-08-01" / "canal=Canal1").exists()
    assert (silver_root / "fecha=2026-08-01" / "canal=Canal2").exists()


def test_canal_filter_scopes_reprocessing_to_one_channel(tmp_path: Path) -> None:
    bronze_dir = tmp_path / "bronze"
    silver_root = tmp_path / "silver"
    _write_bronze_fixture(
        bronze_dir,
        [
            {
                "timestamp": datetime(2026, 8, 1, 10, 0),
                "canal": "Canal1",
                "id_hogar_panelista": "hogar-0001",
                "universo_total": 100_000,
            },
            {
                "timestamp": datetime(2026, 8, 1, 10, 0),
                "canal": "Canal2",
                "id_hogar_panelista": "hogar-0002",
                "universo_total": 100_000,
            },
        ],
    )

    summary = clean_partition(bronze_dir, silver_root, procesado_en=PROCESADO_EN, canal_filter="Canal1")

    assert summary.output_rows == 1
    assert (silver_root / "fecha=2026-08-01" / "canal=Canal1").exists()
    assert not (silver_root / "fecha=2026-08-01" / "canal=Canal2").exists()
