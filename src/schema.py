"""Shared schema definitions for the TV audience metrics pipeline.

Single source of truth for column names/types across bronze, silver, and gold
(constitution Principle II: no duplicated schema definitions between layers).
No Pydantic or other validation library — a `dataclass` plus explicit checks is
enough for this POC's needs (research.md, decision 4).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True)
class RawEvent:
    """One row of the bronze layer: a tuning event exactly as generated/ingested."""

    timestamp: datetime
    canal: str
    id_hogar_panelista: str
    universo_total: int


@dataclass(frozen=True)
class CleanEvent:
    """One row of the silver layer: deduplicated, typed, non-null."""

    timestamp: datetime
    canal: str
    id_hogar_panelista: str
    universo_total: int


@dataclass(frozen=True)
class GoldMetricRow:
    """One row of the gold layer: Rating%/Share% for a fecha + canal + franja_horaria."""

    fecha: date
    canal: str
    franja_horaria: int
    audiencia_canal: int
    audiencia_total_franja: int
    universo_total: int
    rating_pct: float
    share_pct: float | None


# DuckDB column types shared by bronze/silver DDL and CSV/JSON ingestion.
RAW_EVENT_COLUMNS: dict[str, str] = {
    "timestamp": "TIMESTAMP",
    "canal": "VARCHAR",
    "id_hogar_panelista": "VARCHAR",
    "universo_total": "BIGINT",
}

# Process metadata column: never part of a natural key, partition key, or the
# idempotency hash (constitution Principle I).
AUDIT_COLUMN = "procesado_en"


def validate_raw_event(row: dict) -> str | None:
    """Check `row` against the bronze schema.

    Returns None if the row is valid, otherwise a short human-readable rejection
    reason. Never raises: bronze must be able to reject a single bad row without
    stopping the rest of the batch (FR-004).
    """
    for field in ("timestamp", "canal", "id_hogar_panelista", "universo_total"):
        if row.get(field) in (None, ""):
            return f"missing required field: {field}"

    if not isinstance(row["canal"], str) or not row["canal"].strip():
        return "canal must be a non-empty string"

    if not isinstance(row["id_hogar_panelista"], str) or not row["id_hogar_panelista"].strip():
        return "id_hogar_panelista must be a non-empty string"

    try:
        universo_total = int(row["universo_total"])
    except (TypeError, ValueError):
        return "universo_total must be an integer"
    if universo_total <= 0:
        return "universo_total must be a positive integer"

    timestamp = row["timestamp"]
    if isinstance(timestamp, datetime):
        pass
    elif isinstance(timestamp, str):
        try:
            datetime.fromisoformat(timestamp)
        except ValueError:
            return "timestamp is not a parseable ISO-8601 datetime"
    else:
        return "timestamp must be a datetime or ISO-8601 string"

    return None


def franja_horaria_from_timestamp(timestamp: datetime) -> int:
    """Truncate a business timestamp to its hour-of-day time band (research.md, decision 6)."""
    return timestamp.hour


def partition_fecha(timestamp: datetime) -> date:
    """Derive the bronze/silver/gold partition date from a business timestamp."""
    return timestamp.date()
