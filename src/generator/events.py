"""Reproducible generator of simulated per-minute tuning events.

Given the same seed and parameters, this always produces the same set of events
(FR-001). Uses a seeded `random.Random` instance, consumed in a fixed iteration
order — never the unseeded `random` module and never wall-clock time (constitution
Principle I: no non-deterministic functions inside pipeline logic).
"""

from __future__ import annotations

import argparse
import json
import random
from datetime import date as date_cls
from datetime import datetime, timedelta
from pathlib import Path

DEFAULT_CANALES = ["Canal1", "Canal2", "Canal3", "Canal4", "Canal5"]
DEFAULT_NUM_HOGARES = 200
DEFAULT_UNIVERSO_TOTAL = 100_000
DEFAULT_PROB_SINTONIZANDO = 0.35  # probability a household is watching TV in a given minute
MINUTES_PER_DAY = 24 * 60


def generate_events(
    seed: int,
    fecha: date_cls,
    canales: list[str] = DEFAULT_CANALES,
    num_hogares: int = DEFAULT_NUM_HOGARES,
    universo_total: int = DEFAULT_UNIVERSO_TOTAL,
    prob_sintonizando: float = DEFAULT_PROB_SINTONIZANDO,
) -> list[dict]:
    """Deterministically generate one day of per-minute tuning events.

    Iterates households then minutes, in a fixed order, consuming the seeded RNG in
    the same sequence every time — this is the only place randomness enters the
    pipeline, and it never depends on wall-clock time.
    """
    rng = random.Random(seed)
    hogares = [f"hogar-{i:04d}" for i in range(num_hogares)]
    day_start = datetime(fecha.year, fecha.month, fecha.day)

    events: list[dict] = []
    for hogar in hogares:
        for minute in range(MINUTES_PER_DAY):
            if rng.random() >= prob_sintonizando:
                continue
            canal = rng.choice(canales)
            timestamp = day_start + timedelta(minutes=minute)
            events.append(
                {
                    "timestamp": timestamp.isoformat(),
                    "canal": canal,
                    "id_hogar_panelista": hogar,
                    "universo_total": universo_total,
                }
            )
    return events


def write_events(events: list[dict], out_dir: Path) -> Path:
    """Write events as newline-delimited JSON to `out_dir/events.jsonl`."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    destination = out_dir / "events.jsonl"
    with destination.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event, sort_keys=True))
            handle.write("\n")
    return destination


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument(
        "--date", type=str, help="YYYY-MM-DD; shorthand for --start-date/--end-date on the same day"
    )
    parser.add_argument("--start-date", type=str, help="YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, help="YYYY-MM-DD; defaults to --start-date")
    parser.add_argument("--out", type=str, required=True, help="Local output directory")
    parser.add_argument("--canales", type=str, default=",".join(DEFAULT_CANALES))
    parser.add_argument("--num-hogares", type=int, default=DEFAULT_NUM_HOGARES)
    parser.add_argument("--universo-total", type=int, default=DEFAULT_UNIVERSO_TOTAL)
    parser.add_argument(
        "--bucket",
        type=str,
        default=None,
        help="If set, also upload each date's events to s3://<bucket>/raw/fecha=YYYY-MM-DD/ "
        "(the pre-bronze landing prefix the bronze job downloads from)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if args.date:
        start_date = end_date = date_cls.fromisoformat(args.date)
    else:
        if not args.start_date:
            raise SystemExit("Provide either --date or --start-date")
        start_date = date_cls.fromisoformat(args.start_date)
        end_date = date_cls.fromisoformat(args.end_date) if args.end_date else start_date

    canales = [c.strip() for c in args.canales.split(",") if c.strip()]

    fecha = start_date
    while fecha <= end_date:
        events = generate_events(
            seed=args.seed,
            fecha=fecha,
            canales=canales,
            num_hogares=args.num_hogares,
            universo_total=args.universo_total,
        )
        out_dir = Path(args.out) / f"fecha={fecha.isoformat()}"
        destination = write_events(events, out_dir)
        print(f"Generated {len(events)} events for {fecha.isoformat()} -> {destination}")

        if args.bucket:
            # Local import: keep the generator importable with no AWS SDK for local-only use.
            from src import s3_io

            s3_io.upload_partition(out_dir, args.bucket, f"raw/fecha={fecha.isoformat()}")
            print(
                f"Uploaded raw events for {fecha.isoformat()} to "
                f"s3://{args.bucket}/raw/fecha={fecha.isoformat()}"
            )

        fecha += timedelta(days=1)


if __name__ == "__main__":
    main()
