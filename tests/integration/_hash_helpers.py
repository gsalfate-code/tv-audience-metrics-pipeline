"""Test helper: deterministic logical-content hash for idempotency checks.

Reads a Parquet partition tree via DuckDB, orders rows, and hashes their
concatenation. Comparing these hashes across two runs — not raw Parquet bytes — is
what the constitution's idempotency test requires (research.md, decision 5): two
logically identical writes can still differ in file metadata, row-group order, or
compression.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

DEFAULT_EXCLUDED_COLUMNS = ("procesado_en",)


def hash_partition_content(local_dir: Path, *, exclude_columns: tuple[str, ...] = DEFAULT_EXCLUDED_COLUMNS) -> str:
    """Return a deterministic hash of every row under `local_dir`'s Parquet files.

    Excludes `exclude_columns` (process metadata, never part of the idempotency
    contract — constitution Principle I) before ordering rows and hashing their
    concatenation. Recurses into subdirectories, so it works equally on a single
    partition or a whole downloaded layer (e.g. all `canal=` subdirs of one date).
    """
    local_dir = Path(local_dir)
    glob = (local_dir / "**" / "*.parquet").as_posix()

    con = duckdb.connect()
    all_columns = [row[0] for row in con.sql(f"DESCRIBE SELECT * FROM read_parquet('{glob}')").fetchall()]
    columns = [c for c in all_columns if c not in exclude_columns]
    if not columns:
        raise ValueError("no columns left to hash after excluding audit columns")

    concat_args = ", ".join(f'"{c}"' for c in columns)
    result = con.sql(
        f"""
        SELECT md5(string_agg(row_repr, '|' ORDER BY row_repr))
        FROM (
            SELECT concat_ws(chr(1), {concat_args}) AS row_repr
            FROM read_parquet('{glob}')
        )
        """
    ).fetchone()
    return result[0]


def row_count(local_dir: Path) -> int:
    """Return the total number of rows under `local_dir`'s Parquet files."""
    local_dir = Path(local_dir)
    glob = (local_dir / "**" / "*.parquet").as_posix()
    return duckdb.sql(f"SELECT count(*) FROM read_parquet('{glob}')").fetchone()[0]
