"""Gold layer: Rating%/Share% aggregation by canal + franja horaria (FR-005, FR-006, FR-007).

Reads one already-downloaded silver date-partition (a local directory containing
`canal=<canal>/*.parquet` subdirectories — see src/s3_io.py) and writes
`gold/fecha=YYYY-MM-DD/canal=<canal>/` with one row per (fecha, canal, franja_horaria),
including the 24 hourly bands with zero audience (FR-014).
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date as date_cls
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

FRANJAS_POR_DIA = 24


@dataclass
class MetricsSummary:
    """Outcome of one `compute_metrics` call, safe to log without leaking data."""

    output_rows: int
    canales: list[str]


def compute_metrics(
    silver_date_dir: Path,
    gold_root: Path,
    *,
    procesado_en: datetime,
    canales: list[str] | None = None,
    canal_filter: str | None = None,
) -> MetricsSummary:
    """Aggregate one silver date-partition into the gold Rating%/Share% report.

    `canales` is the definitive channel list for the day (so a channel with zero
    audience the whole day still gets 24 zero-rows, per FR-014); if omitted, it
    defaults to whatever channels are actually present in the silver data.
    `canal_filter`, if given, restricts the write to a single channel's partition
    (spec.md Historia 2, escenario 2; FR-009).

    Rating% = audiencia_canal / universo_total. Share% = audiencia_canal /
    audiencia_total_franja (distinct households tuning ANY channel in that hour);
    Share% is NULL — not 0/0 — when nobody tuned in during that hour at all
    (spec.md Edge Cases).
    """
    silver_date_dir = Path(silver_date_dir)
    gold_root = Path(gold_root)
    parquet_glob = (silver_date_dir / "canal=*" / "*.parquet").as_posix()

    con = duckdb.connect()
    con.sql(
        f"""
        CREATE TEMP TABLE silver_rows AS
        SELECT
            id_hogar_panelista,
            canal,
            timestamp,
            universo_total,
            CAST(timestamp AS DATE) AS fecha,
            EXTRACT(hour FROM timestamp) AS franja_horaria
        FROM read_parquet('{parquet_glob}')
        """
    )

    if con.sql("SELECT count(*) FROM silver_rows").fetchone()[0] == 0:
        return MetricsSummary(output_rows=0, canales=[])

    if canales is not None:
        resolved_canales = canales
    else:
        resolved_canales = [
            row[0]
            for row in con.sql("SELECT DISTINCT canal FROM silver_rows ORDER BY canal").fetchall()
        ]
    if canal_filter is not None:
        resolved_canales = [c for c in resolved_canales if c == canal_filter]

    con.sql(
        """
        CREATE TEMP TABLE universo_por_franja AS
        SELECT fecha, franja_horaria, MAX(universo_total) AS universo_total
        FROM silver_rows GROUP BY fecha, franja_horaria
        """
    )
    con.sql(
        """
        CREATE TEMP TABLE audiencia_total AS
        SELECT fecha, franja_horaria, COUNT(DISTINCT id_hogar_panelista) AS audiencia_total_franja
        FROM silver_rows GROUP BY fecha, franja_horaria
        """
    )
    con.sql(
        """
        CREATE TEMP TABLE audiencia_canal AS
        SELECT fecha, canal, franja_horaria, COUNT(DISTINCT id_hogar_panelista) AS audiencia_canal
        FROM silver_rows GROUP BY fecha, canal, franja_horaria
        """
    )
    con.sql(
        f"""
        CREATE TEMP TABLE grid AS
        SELECT f.fecha, c.canal, h.franja_horaria
        FROM (SELECT DISTINCT fecha FROM silver_rows) f
        CROSS JOIN (SELECT UNNEST(?) AS canal) c
        CROSS JOIN (SELECT range AS franja_horaria FROM range({FRANJAS_POR_DIA})) h
        """,
        params=[resolved_canales],
    )

    con.sql(
        """
        CREATE TEMP TABLE gold_rows AS
        SELECT
            g.fecha,
            g.canal,
            g.franja_horaria,
            COALESCE(ac.audiencia_canal, 0) AS audiencia_canal,
            COALESCE(atf.audiencia_total_franja, 0) AS audiencia_total_franja,
            COALESCE(uf.universo_total, 0) AS universo_total,
            CASE WHEN COALESCE(uf.universo_total, 0) > 0
                 THEN COALESCE(ac.audiencia_canal, 0) * 100.0 / uf.universo_total
                 ELSE 0.0 END AS rating_pct,
            CASE WHEN COALESCE(atf.audiencia_total_franja, 0) > 0
                 THEN COALESCE(ac.audiencia_canal, 0) * 100.0 / atf.audiencia_total_franja
                 ELSE NULL END AS share_pct
        FROM grid g
        LEFT JOIN audiencia_canal ac
            ON ac.fecha = g.fecha AND ac.canal = g.canal AND ac.franja_horaria = g.franja_horaria
        LEFT JOIN audiencia_total atf
            ON atf.fecha = g.fecha AND atf.franja_horaria = g.franja_horaria
        LEFT JOIN universo_por_franja uf
            ON uf.fecha = g.fecha AND uf.franja_horaria = g.franja_horaria
        """
    )

    partitions = con.sql(
        "SELECT DISTINCT fecha, canal FROM gold_rows ORDER BY fecha, canal"
    ).fetchall()
    for fecha, canal in partitions:
        partition_dir = gold_root / f"fecha={fecha.isoformat()}" / f"canal={canal}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        output_path = (partition_dir / "part-0000.parquet").as_posix()
        con.sql(
            "COPY (SELECT fecha, canal, franja_horaria, audiencia_canal, audiencia_total_franja, "
            "universo_total, rating_pct, share_pct, CAST(? AS TIMESTAMP) AS procesado_en "
            f"FROM gold_rows WHERE fecha = ? AND canal = ?) TO '{output_path}' (FORMAT PARQUET)",
            params=[procesado_en, fecha, canal],
        )

    output_rows = con.sql("SELECT count(*) FROM gold_rows").fetchone()[0]
    canales_escritos = sorted({canal for _fecha, canal in partitions})
    return MetricsSummary(output_rows=output_rows, canales=canales_escritos)


def _parse_args() -> argparse.Namespace:
    from src.generator.events import DEFAULT_CANALES

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bucket", required=True, help="S3 bucket name (from the BUCKET_NAME repo variable)"
    )
    parser.add_argument("--start-date", required=True, type=str, help="YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, help="YYYY-MM-DD; defaults to --start-date")
    parser.add_argument(
        "--canal", type=str, default=None, help="Reprocess only this channel's partition"
    )
    parser.add_argument(
        "--canales",
        type=str,
        default=",".join(DEFAULT_CANALES),
        help="Definitive channel list for the day, so a dark channel still gets 24 zero-rows "
        "(FR-014)",
    )
    parser.add_argument(
        "--work-dir", type=str, default="_work/gold", help="Local scratch directory"
    )
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint for the `gold` workflow job: for each date in range, download
    `silver/fecha=.../` from S3, aggregate it locally, and upload `gold/fecha=.../canal=.../`
    back (FR-005, FR-006, FR-007, FR-009, FR-014)."""
    args = _parse_args()
    # Local import: keep compute_metrics usable without boto3 in unit tests.
    from src import s3_io

    start_date = date_cls.fromisoformat(args.start_date)
    end_date = date_cls.fromisoformat(args.end_date) if args.end_date else start_date
    canales = [c.strip() for c in args.canales.split(",") if c.strip()]
    # Captured once, outside any transformation logic (constitution Principle I).
    procesado_en = datetime.now(timezone.utc)

    work_dir = Path(args.work_dir)

    fecha = start_date
    while fecha <= end_date:
        silver_local = work_dir / f"silver_fecha={fecha.isoformat()}"
        s3_io.download_partition(args.bucket, f"silver/fecha={fecha.isoformat()}", silver_local)

        gold_out = work_dir / "gold_out"
        summary = compute_metrics(
            silver_local,
            gold_out,
            procesado_en=procesado_en,
            canales=canales,
            canal_filter=args.canal,
        )

        for canal in summary.canales:
            partition_local = gold_out / f"fecha={fecha.isoformat()}" / f"canal={canal}"
            s3_io.upload_partition(
                partition_local, args.bucket, f"gold/fecha={fecha.isoformat()}/canal={canal}"
            )

        print(
            f"[gold] {fecha.isoformat()}: "
            f"output_rows={summary.output_rows} canales={summary.canales}"
        )
        fecha += timedelta(days=1)


if __name__ == "__main__":
    main()
