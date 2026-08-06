"""Unit tests for the reproducible tuning-event generator (FR-001)."""

from __future__ import annotations

from datetime import date

from src.generator.events import generate_events, write_events


def test_same_seed_produces_identical_events() -> None:
    run_1 = generate_events(seed=42, fecha=date(2026, 8, 1), num_hogares=10)
    run_2 = generate_events(seed=42, fecha=date(2026, 8, 1), num_hogares=10)

    assert run_1 == run_2
    assert len(run_1) > 0


def test_different_seed_produces_different_events() -> None:
    run_1 = generate_events(seed=42, fecha=date(2026, 8, 1), num_hogares=10)
    run_2 = generate_events(seed=43, fecha=date(2026, 8, 1), num_hogares=10)

    assert run_1 != run_2


def test_generated_events_are_well_formed() -> None:
    events = generate_events(seed=1, fecha=date(2026, 8, 1), num_hogares=5)

    assert events
    for event in events:
        assert set(event) == {"timestamp", "canal", "id_hogar_panelista", "universo_total"}
        assert event["universo_total"] > 0
        assert event["canal"]
        assert event["id_hogar_panelista"]
        assert event["timestamp"].startswith("2026-08-01T")


def test_write_events_is_byte_for_byte_reproducible(tmp_path) -> None:
    events = generate_events(seed=42, fecha=date(2026, 8, 1), num_hogares=5)

    path_1 = write_events(events, tmp_path / "run1")
    path_2 = write_events(events, tmp_path / "run2")

    assert path_1.read_text() == path_2.read_text()
