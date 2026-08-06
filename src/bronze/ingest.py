"""Bronze layer: schema validation + raw-to-Parquet normalization (FR-002, FR-004).

No business logic beyond schema validation lives here — deduplication, typing beyond
raw-to-Parquet, null-handling rules, and metric calculation all happen in later layers
(constitution Principle III: bronze stays a faithful, unopinionated copy of the source).
"""

from __future__ import annotations

import argparse
import json
import tempfile
from dataclasses import dataclass, field
from datetime import date as date_cls
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

from src.schema import validate_raw_event


@dataclass
class IngestSummary:
    """Outcome of one `ingest_to_local` call, safe to log without leaking data."""

    accepted: int
    rejected: int
    rejected_reasons: list[str] = field(default_factory=list)


def ingest_to_local(
    events: list[dict], local_root: Path, *, procesado_en: datetime
) -> IngestSummary:
    """Validate `events` and write the valid ones as Parquet under local_root/fecha=YYYY-MM-DD/.

    Partitions by the date derived from each event's own `timestamp` (business data),
    so a batch spanning multiple dates still lands in the right partitions. Invalid
    events are counted and skipped — the rest of the batch always proceeds (FR-004).
    `procesado_en` is audit metadata supplied by the caller (e.g. the CLI, captured
    once at the start of the run) — never computed here via `now()` (constitution
    Principle I).
    """
    local_root = Path(local_root)

    valid_rows: list[dict] = []
    rejected_reasons: list[str] = []
    for event in events:
        reason = validate_raw_event(event)
        if reason is None:
            valid_rows.append(event)
        else:
            rejected_reasons.append(reason)

    if not valid_rows:
        return IngestSummary(
            accepted=0, rejected=len(rejected_reasons), rejected_reasons=rejected_reasons
        )

    with tempfile.TemporaryDirectory() as tmp_dir:
        ndjson_path = Path(tmp_dir) / "valid_events.jsonl"
        with ndjson_path.open("w", encoding="utf-8") as handle:
            for row in valid_rows:
                handle.write(json.dumps(row, default=str))
                handle.write("\n")

        con = duckdb.connect()
        con.sql(
            f"""
            CREATE TEMP TABLE valid_events AS
            SELECT
                CAST(timestamp AS TIMESTAMP) AS timestamp,
                CAST(canal AS VARCHAR) AS canal,
                CAST(id_hogar_panelista AS VARCHAR) AS id_hogar_panelista,
                CAST(universo_total AS BIGINT) AS universo_total,
                CAST(timestamp AS DATE) AS fecha,
                CAST(? AS TIMESTAMP) AS procesado_en
            FROM read_json_auto('{ndjson_path.as_posix()}')
            """,
            params=[procesado_en],
        )

        fechas = [
            row[0]
            for row in con.sql("SELECT DISTINCT fecha FROM valid_events ORDER BY fecha").fetchall()
        ]
        for fecha in fechas:
            partition_dir = local_root / f"fecha={fecha.isoformat()}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            output_path = (partition_dir / "part-0000.parquet").as_posix()
            con.sql(
                "COPY (SELECT timestamp, canal, id_hogar_panelista, universo_total, procesado_en "
                f"FROM valid_events WHERE fecha = ?) TO '{output_path}' (FORMAT PARQUET)",
                params=[fecha],
            )

    return IngestSummary(
        accepted=len(valid_rows), rejected=len(rejected_reasons), rejected_reasons=rejected_reasons
    )


def _read_raw_events(raw_partition_dir: Path) -> list[dict]:
    events_path = raw_partition_dir / "events.jsonl"
    if not events_path.exists():
        return []
    with events_path.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bucket", required=True, help="S3 bucket name (from the BUCKET_NAME repo variable)"
    )
    parser.add_argument("--start-date", required=True, type=str, help="YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, help="YYYY-MM-DD; defaults to --start-date")
    parser.add_argument(
        "--work-dir", type=str, default="_work/bronze", help="Local scratch directory"
    )
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint for the `bronze` workflow job: for each date in range, download
    `raw/fecha=.../` from S3, ingest it locally, and upload `bronze/fecha=.../` back
    (FR-002, FR-004, FR-009)."""
    args = _parse_args()
    from src import s3_io  # local import: keep ingest_to_local usable without boto3 in unit tests

    start_date = date_cls.fromisoformat(args.start_date)
    end_date = date_cls.fromisoformat(args.end_date) if args.end_date else start_date
    # Captured once, outside any transformation logic, and passed through as plain
    # metadata — never computed via now() inside ingest_to_local (constitution Principle I).
    procesado_en = datetime.now(timezone.utc)

    work_dir = Path(args.work_dir)
    total_accepted = 0
    total_rejected = 0

    fecha = start_date
    while fecha <= end_date:
        raw_local = work_dir / f"raw_fecha={fecha.isoformat()}"
        s3_io.download_partition(args.bucket, f"raw/fecha={fecha.isoformat()}", raw_local)
        events = _read_raw_events(raw_local)

        bronze_out = work_dir / "bronze_out"
        summary = ingest_to_local(events, bronze_out, procesado_en=procesado_en)

        partition_local = bronze_out / f"fecha={fecha.isoformat()}"
        if partition_local.exists():
            s3_io.upload_partition(
                partition_local, args.bucket, f"bronze/fecha={fecha.isoformat()}"
            )

        total_accepted += summary.accepted
        total_rejected += summary.rejected
        print(
            f"[bronze] {fecha.isoformat()}: accepted={summary.accepted} rejected={summary.rejected}"
        )

        fecha += timedelta(days=1)

    print(f"[bronze] done: total_accepted={total_accepted} total_rejected={total_rejected}")


if __name__ == "__main__":
    main()
