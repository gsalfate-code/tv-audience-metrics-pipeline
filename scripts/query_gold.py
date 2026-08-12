"""Explore the gold layer (Rating%/Share%) locally with DuckDB.

Downloads `gold/fecha=.../canal=.../*.parquet` for a date range via `src/s3_io.py`
(boto3, credentials from the environment/profile chain — never hardcoded, same as the
pipeline jobs; research.md decision 1) into a local scratch dir, then runs a query
against it. `fecha` and `canal` are real columns inside the parquet files (data-model.md
§4), so a plain recursive glob is enough — no hive_partitioning parsing needed.

Examples:
    python scripts/query_gold.py --bucket my-bucket --start-date 2026-08-01
    python scripts/query_gold.py --bucket my-bucket --start-date 2026-08-01 \\
        --end-date 2026-08-07 --query resumen_canal
    python scripts/query_gold.py --bucket my-bucket --start-date 2026-08-01 \\
        --sql "SELECT canal, MAX(rating_pct) FROM gold GROUP BY canal"
    python scripts/query_gold.py --list-queries
"""

from __future__ import annotations

import argparse
import os
from datetime import date as date_cls
from datetime import timedelta
from pathlib import Path

import duckdb

PRESET_QUERIES: dict[str, str] = {
    "rating_share": """
        SELECT fecha, franja_horaria, canal, rating_pct, share_pct
        FROM gold
        ORDER BY fecha, franja_horaria, canal
    """,
    "resumen_canal": """
        SELECT
            fecha,
            canal,
            ROUND(AVG(rating_pct), 2) AS rating_prom,
            ROUND(AVG(share_pct), 2) AS share_prom,
            SUM(audiencia_canal) AS audiencia_acumulada_dia
        FROM gold
        GROUP BY fecha, canal
        ORDER BY fecha, rating_prom DESC
    """,
    "top_franja": """
        SELECT fecha, franja_horaria, canal, rating_pct, share_pct
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY fecha, franja_horaria ORDER BY rating_pct DESC
            ) AS rn
            FROM gold
        )
        WHERE rn = 1
        ORDER BY fecha, franja_horaria
    """,
    "cobertura": """
        -- Franjas sin ninguna sintonización (FR-014: deben existir igual, con audiencia 0)
        SELECT fecha, franja_horaria, SUM(audiencia_total_franja) AS audiencia_total
        FROM gold
        GROUP BY fecha, franja_horaria
        HAVING SUM(audiencia_total_franja) = 0
        ORDER BY fecha, franja_horaria
    """,
    "share_check": """
        -- Invariante SC-004: la suma de share_pct por franja debe dar ~100
        SELECT fecha, franja_horaria, ROUND(SUM(share_pct), 2) AS suma_share_pct
        FROM gold
        GROUP BY fecha, franja_horaria
        ORDER BY fecha, franja_horaria
    """,
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--bucket",
        default=os.environ.get("BUCKET_NAME"),
        help="S3 bucket name (defaults to $BUCKET_NAME)",
    )
    parser.add_argument("--start-date", type=str, help="YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, help="YYYY-MM-DD; defaults to --start-date")
    parser.add_argument("--canal", type=str, default=None, help="Restrict to a single channel")
    parser.add_argument(
        "--query",
        choices=sorted(PRESET_QUERIES),
        default="rating_share",
        help="Named preset query to run (default: rating_share)",
    )
    parser.add_argument(
        "--sql",
        type=str,
        default=None,
        help="Custom SQL to run instead of a preset; query the `gold` view/table name",
    )
    parser.add_argument(
        "--work-dir", type=str, default="_work/query_gold", help="Local scratch directory"
    )
    parser.add_argument(
        "--region",
        type=str,
        default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"),
        help="AWS region for the bucket (defaults to $AWS_REGION/$AWS_DEFAULT_REGION)",
    )
    parser.add_argument(
        "--list-queries", action="store_true", help="Print available preset queries and exit"
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    if args.list_queries:
        for name, sql in sorted(PRESET_QUERIES.items()):
            print(f"{name}:{sql}")
        return

    if not args.bucket:
        raise SystemExit("--bucket is required (or set $BUCKET_NAME)")
    if not args.start_date:
        raise SystemExit("--start-date is required")
    if not args.region:
        raise SystemExit(
            "No AWS region set. Pass --region or export AWS_REGION/AWS_DEFAULT_REGION "
            "(e.g. `export AWS_DEFAULT_REGION=us-east-1`)."
        )
    os.environ.setdefault("AWS_DEFAULT_REGION", args.region)

    from botocore.exceptions import ClientError, NoCredentialsError

    from src import s3_io

    start_date = date_cls.fromisoformat(args.start_date)
    end_date = date_cls.fromisoformat(args.end_date) if args.end_date else start_date

    work_dir = Path(args.work_dir)
    fecha = start_date
    try:
        while fecha <= end_date:
            prefix = f"gold/fecha={fecha.isoformat()}"
            if args.canal:
                prefix += f"/canal={args.canal}"
            s3_io.download_partition(args.bucket, prefix, work_dir / f"fecha={fecha.isoformat()}")
            fecha += timedelta(days=1)
    except NoCredentialsError:
        raise SystemExit(
            "No AWS credentials found. Export AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY "
            "(or configure an AWS profile) before running this script."
        )
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        raise SystemExit(f"S3 request failed ({code}): {exc}")

    parquet_glob = (work_dir / "**" / "*.parquet").as_posix()
    con = duckdb.connect()
    con.sql(f"CREATE VIEW gold AS SELECT * FROM read_parquet('{parquet_glob}')")

    if con.sql("SELECT count(*) FROM gold").fetchone()[0] == 0:
        print(f"No hay datos en gold para {start_date}..{end_date} (bucket={args.bucket}).")
        return

    sql = args.sql if args.sql else PRESET_QUERIES[args.query]
    con.sql(sql).show(max_rows=200)


if __name__ == "__main__":
    main()
