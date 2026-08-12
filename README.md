# 📺 TV Audience Metrics Pipeline

[![Pipeline](https://img.shields.io/github/actions/workflow/status/gsalfate-code/tv-audience-metrics-pipeline/pipeline.yml?branch=main&label=pipeline&logo=githubactions&logoColor=white)](../../actions/workflows/pipeline.yml)
[![Python](https://img.shields.io/badge/python-3.12-3776AB?logo=python&logoColor=white)](pyproject.toml)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.0-FFF000?logo=duckdb&logoColor=black)](https://duckdb.org)
[![AWS S3](https://img.shields.io/badge/AWS-S3-FF9900?logo=amazons3&logoColor=white)](infra/bootstrap.sh)
[![License](https://img.shields.io/badge/status-POC-lightgrey)](DOCUMENTATION.md)

A small, deterministic data pipeline that simulates TV tuning events and turns
them into daily **Rating%** and **Share%** metrics, using a Medallion
architecture (**Bronze → Silver → Gold**) on top of **DuckDB** and **AWS S3**,
orchestrated entirely by **GitHub Actions**.

> Built as a proof of concept for simple, idempotent, reproducible data
> engineering — no warehouse, no cluster, no long-lived AWS credentials.

---

## How it flows

```mermaid
flowchart LR
    subgraph Generate
        G["🎲 generator.events\nsimulated tuning events"]
    end
    subgraph Bronze
        B["🥉 bronze.ingest\nvalidate & normalize"]
    end
    subgraph Silver
        S["🥈 silver.clean\ndedupe & clean"]
    end
    subgraph Gold
        Gd["🥇 gold.metrics\nRating% / Share%"]
    end

    G -->|raw/fecha=.../| B -->|bronze/fecha=.../| S -->|silver/fecha=.../| Gd -->|gold/fecha=.../| Out["📊 audience metrics"]

    style G fill:#8b8b8b20,stroke:#8b8b8b
    style B fill:#cd7f3220,stroke:#cd7f32
    style S fill:#c0c0c020,stroke:#a8a8a8
    style Gd fill:#ffd70020,stroke:#d4af37
```

Each stage reads only from the previous one, writes to its own S3 prefix, and
is safe to re-run: same input → byte-identical output (see
[determinism & idempotency](DOCUMENTATION.md#5-determinismo-e-idempotencia)).

---

## Quickstart

Requirements: **Python 3.12**.

```bash
pip install -e ".[dev]"
```

Run the unit tests — no AWS involved, everything runs against local temp files:

```bash
pytest tests/unit -v
```

Running plain `pytest` also collects `tests/integration/`, but those
auto-skip locally unless `BUCKET_NAME` and `AWS_REGION` are set — see
[`tests/integration/README.md`](tests/integration/README.md).

Try the reproducible event generator on its own:

```bash
python -m src.generator.events --seed 42 --date 2026-08-01 --out /tmp/eventos
```

---

## Running the pipeline for real

The pipeline only ever runs in **GitHub Actions** against a real S3 bucket,
authenticated via OIDC — never with local long-lived AWS keys (constitution
Principle VII). One-time setup:

1. Run [`infra/bootstrap.sh`](infra/bootstrap.sh) once against your AWS account
   (see the script header for required environment variables) to create the
   OIDC provider, IAM role, and S3 bucket.
2. Configure the repo's `ROLE_ARN` secret and `BUCKET_NAME` / `AWS_REGION`
   variables in GitHub with the values `bootstrap.sh` prints.
3. Trigger [`.github/workflows/pipeline.yml`](.github/workflows/pipeline.yml)
   via `workflow_dispatch`, or let the daily `schedule` trigger run it.

Full walkthrough: [`specs/001-audience-metrics-poc/quickstart.md`](specs/001-audience-metrics-poc/quickstart.md).

---

## Learn more

| Resource | What's in it |
|---|---|
| [`DOCUMENTATION.md`](DOCUMENTATION.md) | Consolidated technical reference — architecture, data model, module reference, security, CI/CD |
| [`specs/001-audience-metrics-poc/`](specs/001-audience-metrics-poc/) | Full spec, plan, and design docs |
| [`specs/001-audience-metrics-poc/quickstart.md`](specs/001-audience-metrics-poc/quickstart.md) | End-to-end validation guide (local + real AWS) |
| [`tests/integration/README.md`](tests/integration/README.md) | How integration tests exercise real S3 |
