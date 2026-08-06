"""Silver layer: deduplication, typing, and null handling (FR-003, FR-015).

Reads one already-downloaded bronze date-partition (a flat directory of Parquet
files — see src/s3_io.py) and writes a cleaned, deduplicated
`silver/fecha=YYYY-MM-DD/canal=<canal>/` layout.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date as date_cls
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb


@dataclass
class CleanSummary:
    """Outcome of one `clean_partition` call, safe to log without leaking data."""

    output_rows: int
    excluded_null_rows: int
    canales: list[str]


def clean_partition(
    bronze_partition_dir: Path,
    silver_root: Path,
    *,
    procesado_en: datetime,
    canal_filter: str | None = None,
) -> CleanSummary:
    """Dedup + clean one bronze date-partition and write it to `silver_root`.

    Deduplication (FR-003, FR-015): rows sharing the natural key
    (`id_hogar_panelista`, `canal`, `timestamp`) collapse to one, keeping the row with
    the largest `universo_total`. That rule only depends on the data itself — never on
    file or row read order — so it is deterministic across repeated runs (constitution
    Principle I).

    `canal_filter`, if given, restricts the write to a single channel's partition, so a
    reprocess of one fecha+canal never touches the other channels already written for
    that date (spec.md Historia 2, escenario 2; FR-009).
    """
    bronze_partition_dir = Path(bronze_partition_dir)
    silver_root = Path(silver_root)
    parquet_glob = (bronze_partition_dir / "*.parquet").as_posix()

    con = duckdb.connect()
    con.sql(
        f"""
        CREATE TEMP TABLE bronze_rows AS
        SELECT timestamp, canal, id_hogar_panelista, universo_total
        FROM read_parquet('{parquet_glob}')
        """
    )

    total_rows = con.sql("SELECT count(*) FROM bronze_rows").fetchone()[0]
    con.sql(
        """
        CREATE TEMP TABLE valid_rows AS
        SELECT * FROM bronze_rows
        WHERE timestamp IS NOT NULL AND canal IS NOT NULL
          AND id_hogar_panelista IS NOT NULL AND universo_total IS NOT NULL
        """
    )
    valid_row_count = con.sql("SELECT count(*) FROM valid_rows").fetchone()[0]
    excluded_null_rows = total_rows - valid_row_count

    con.sql(
        """
        CREATE TEMP TABLE deduped AS
        SELECT
            id_hogar_panelista,
            canal,
            timestamp,
            MAX(universo_total) AS universo_total,
            CAST(timestamp AS DATE) AS fecha
        FROM valid_rows
        GROUP BY id_hogar_panelista, canal, timestamp
        """
    )

    if canal_filter is not None:
        con.sql("DELETE FROM deduped WHERE canal != ?", params=[canal_filter])

    output_rows = con.sql("SELECT count(*) FROM deduped").fetchone()[0]
    partitions = con.sql(
        "SELECT DISTINCT fecha, canal FROM deduped ORDER BY fecha, canal"
    ).fetchall()

    for fecha, canal in partitions:
        partition_dir = silver_root / f"fecha={fecha.isoformat()}" / f"canal={canal}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        output_path = (partition_dir / "part-0000.parquet").as_posix()
        con.sql(
            "COPY (SELECT id_hogar_panelista, canal, timestamp, universo_total, "
            f"CAST(? AS TIMESTAMP) AS procesado_en FROM deduped WHERE fecha = ? AND canal = ?) "
            f"TO '{output_path}' (FORMAT PARQUET)",
            params=[procesado_en, fecha, canal],
        )

    canales = sorted({canal for _fecha, canal in partitions})
    return CleanSummary(
        output_rows=output_rows, excluded_null_rows=excluded_null_rows, canales=canales
    )


def _parse_args() -> argparse.Namespace:
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
        "--work-dir", type=str, default="_work/silver", help="Local scratch directory"
    )
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint for the `silver` workflow job: for each date in range, download
    `bronze/fecha=.../` from S3, clean it locally, and upload `silver/fecha=.../canal=.../`
    back (FR-003, FR-009, FR-015)."""
    args = _parse_args()
    # Local import: keep clean_partition usable without boto3 in unit tests.
    from src import s3_io

    start_date = date_cls.fromisoformat(args.start_date)
    end_date = date_cls.fromisoformat(args.end_date) if args.end_date else start_date
    # Captured once, outside any transformation logic (constitution Principle I).
    procesado_en = datetime.now(timezone.utc)

    work_dir = Path(args.work_dir)

    fecha = start_date
    while fecha <= end_date:
        bronze_local = work_dir / f"bronze_fecha={fecha.isoformat()}"
        s3_io.download_partition(args.bucket, f"bronze/fecha={fecha.isoformat()}", bronze_local)

        silver_out = work_dir / "silver_out"
        summary = clean_partition(
            bronze_local, silver_out, procesado_en=procesado_en, canal_filter=args.canal
        )

        for canal in summary.canales:
            partition_local = silver_out / f"fecha={fecha.isoformat()}" / f"canal={canal}"
            s3_io.upload_partition(
                partition_local, args.bucket, f"silver/fecha={fecha.isoformat()}/canal={canal}"
            )

        print(
            f"[silver] {fecha.isoformat()}: output_rows={summary.output_rows} "
            f"excluded_null_rows={summary.excluded_null_rows} canales={summary.canales}"
        )
        fecha += timedelta(days=1)


if __name__ == "__main__":
    main()
