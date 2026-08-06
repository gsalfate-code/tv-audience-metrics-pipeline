"""Shared pytest fixtures.

Tests under tests/unit never touch AWS (constitution Principle IV). Tests marked
`integration` are the only ones allowed to hit real S3, and they only run when the AWS
environment is actually configured — i.e. inside the GitHub Actions workflow. Running
`pytest` locally with no extra configuration silently skips them instead of failing.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

REQUIRED_INTEGRATION_ENV_VARS = ("BUCKET_NAME", "AWS_REGION")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    missing = [name for name in REQUIRED_INTEGRATION_ENV_VARS if not os.environ.get(name)]
    if not missing:
        return
    skip_marker = pytest.mark.skip(
        reason=f"integration tests require {', '.join(missing)} (only set in GitHub Actions)"
    )
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_marker)


@pytest.fixture
def sample_raw_events() -> list[dict]:
    """A small, fixed set of raw tuning events covering the common cases."""
    return [
        {
            "timestamp": "2026-08-01T10:00:00",
            "canal": "Canal1",
            "id_hogar_panelista": "hogar-0001",
            "universo_total": 100_000,
        },
        {
            "timestamp": "2026-08-01T10:00:00",
            "canal": "Canal2",
            "id_hogar_panelista": "hogar-0002",
            "universo_total": 100_000,
        },
        {
            "timestamp": "2026-08-01T10:01:00",
            "canal": "Canal1",
            "id_hogar_panelista": "hogar-0001",
            "universo_total": 100_000,
        },
    ]


@pytest.fixture
def partition_dir(tmp_path: Path):
    """Factory for a local directory shaped like a Hive partition (fecha=.../canal=...)."""

    def _make(*, fecha: str, canal: str | None = None) -> Path:
        parts = [f"fecha={fecha}"]
        if canal is not None:
            parts.append(f"canal={canal}")
        directory = tmp_path.joinpath(*parts)
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    return _make
