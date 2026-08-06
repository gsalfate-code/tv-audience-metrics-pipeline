"""Unit tests for Rating%/Share% aggregation (FR-005, FR-006, FR-007, FR-014)."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb
import pytest

from src.gold.metrics import compute_metrics

PROCESADO_EN = datetime(2026, 8, 6, 12, 0, 0)
UNIVERSO_TOTAL = 100_000


def _write_silver_fixture(silver_date_dir: Path, canal: str, rows: list[dict]) -> None:
    partition_dir = silver_date_dir / f"canal={canal}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(
        "CREATE TEMP TABLE t (id_hogar_panelista VARCHAR, canal VARCHAR, timestamp TIMESTAMP, universo_total BIGINT)"
    )
    for row in rows:
        con.execute(
            "INSERT INTO t VALUES (?, ?, ?, ?)",
            [row["id_hogar_panelista"], canal, row["timestamp"], row["universo_total"]],
        )
    con.sql(f"COPY t TO '{(partition_dir / 'part-0000.parquet').as_posix()}' (FORMAT PARQUET)")


def _build_two_channel_fixture(silver_date_dir: Path) -> None:
    _write_silver_fixture(
        silver_date_dir,
        "Canal1",
        [
            {"id_hogar_panelista": "hogar-0001", "timestamp": datetime(2026, 8, 1, 10, 5), "universo_total": UNIVERSO_TOTAL},
            {"id_hogar_panelista": "hogar-0002", "timestamp": datetime(2026, 8, 1, 10, 30), "universo_total": UNIVERSO_TOTAL},
            {"id_hogar_panelista": "hogar-0001", "timestamp": datetime(2026, 8, 1, 9, 5), "universo_total": UNIVERSO_TOTAL},
        ],
    )
    _write_silver_fixture(
        silver_date_dir,
        "Canal2",
        [
            {"id_hogar_panelista": "hogar-0003", "timestamp": datetime(2026, 8, 1, 10, 45), "universo_total": UNIVERSO_TOTAL},
        ],
    )


def _read_gold(gold_root: Path, canal: str) -> list[tuple]:
    return duckdb.sql(
        f"SELECT franja_horaria, audiencia_canal, audiencia_total_franja, rating_pct, share_pct "
        f"FROM read_parquet('{gold_root.as_posix()}/fecha=2026-08-01/canal={canal}/*.parquet') "
        f"ORDER BY franja_horaria"
    ).fetchall()


def test_rating_and_share_are_computed_correctly_for_a_franja_with_audience(tmp_path: Path) -> None:
    silver_date_dir = tmp_path / "silver"
    gold_root = tmp_path / "gold"
    _build_two_channel_fixture(silver_date_dir)

    compute_metrics(silver_date_dir, gold_root, procesado_en=PROCESADO_EN, canales=["Canal1", "Canal2"])

    canal1_rows = {row[0]: row for row in _read_gold(gold_root, "Canal1")}
    canal2_rows = {row[0]: row for row in _read_gold(gold_root, "Canal2")}

    franja_10_c1 = canal1_rows[10]
    franja_10_c2 = canal2_rows[10]

    # audiencia_total_franja(10) = 3 distinct households across both channels
    assert franja_10_c1[1] == 2  # audiencia_canal Canal1
    assert franja_10_c1[2] == 3  # audiencia_total_franja
    assert franja_10_c1[3] == pytest.approx(2 / UNIVERSO_TOTAL * 100)
    assert franja_10_c1[4] == pytest.approx(2 / 3 * 100)

    assert franja_10_c2[1] == 1
    assert franja_10_c2[3] == pytest.approx(1 / UNIVERSO_TOTAL * 100)
    assert franja_10_c2[4] == pytest.approx(1 / 3 * 100)

    # Share% across all channels sums to 100 for a franja with audience (SC-004)
    assert franja_10_c1[4] + franja_10_c2[4] == pytest.approx(100.0)


def test_franja_without_any_audience_has_zero_rating_and_null_share(tmp_path: Path) -> None:
    silver_date_dir = tmp_path / "silver"
    gold_root = tmp_path / "gold"
    _build_two_channel_fixture(silver_date_dir)

    compute_metrics(silver_date_dir, gold_root, procesado_en=PROCESADO_EN, canales=["Canal1", "Canal2"])

    canal1_rows = {row[0]: row for row in _read_gold(gold_root, "Canal1")}
    franja_11 = canal1_rows[11]  # no events in hour 11 in the fixture

    assert franja_11[1] == 0  # audiencia_canal
    assert franja_11[2] == 0  # audiencia_total_franja
    assert franja_11[3] == 0.0  # rating_pct
    assert franja_11[4] is None  # share_pct indefinido, not 0/0


def test_channel_with_no_audience_in_one_franja_still_appears_explicitly(tmp_path: Path) -> None:
    silver_date_dir = tmp_path / "silver"
    gold_root = tmp_path / "gold"
    _build_two_channel_fixture(silver_date_dir)

    compute_metrics(silver_date_dir, gold_root, procesado_en=PROCESADO_EN, canales=["Canal1", "Canal2"])

    canal2_rows = {row[0]: row for row in _read_gold(gold_root, "Canal2")}
    franja_9 = canal2_rows[9]  # Canal1 has audience in hour 9 (see fixture), Canal2 does not

    assert franja_9[1] == 0
    assert franja_9[3] == 0.0
    assert franja_9[4] == 0.0  # audiencia_total_franja > 0 here, so share is defined and 0


def test_all_24_time_bands_are_present_per_channel(tmp_path: Path) -> None:
    silver_date_dir = tmp_path / "silver"
    gold_root = tmp_path / "gold"
    _build_two_channel_fixture(silver_date_dir)

    compute_metrics(silver_date_dir, gold_root, procesado_en=PROCESADO_EN, canales=["Canal1", "Canal2"])

    canal1_rows = _read_gold(gold_root, "Canal1")
    assert sorted(row[0] for row in canal1_rows) == list(range(24))


def test_canal_filter_scopes_output_to_one_channel(tmp_path: Path) -> None:
    silver_date_dir = tmp_path / "silver"
    gold_root = tmp_path / "gold"
    _build_two_channel_fixture(silver_date_dir)

    compute_metrics(
        silver_date_dir, gold_root, procesado_en=PROCESADO_EN, canales=["Canal1", "Canal2"], canal_filter="Canal1"
    )

    assert (gold_root / "fecha=2026-08-01" / "canal=Canal1").exists()
    assert not (gold_root / "fecha=2026-08-01" / "canal=Canal2").exists()
