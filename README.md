# tv-audience-metrics-pipeline

POC: TV audience metrics pipeline (Rating% &amp; Share%) using Medallion architecture (Bronze→Silver→Gold), DuckDB, AWS S3, and GitHub Actions. Simple, deterministic, and idempotent data engineering.

See [`DOCUMENTATION.md`](DOCUMENTATION.md) for the consolidated technical reference
(architecture, data model, module reference, security, CI/CD). See
[`specs/001-audience-metrics-poc/`](specs/001-audience-metrics-poc/) for the full
spec, plan, and design docs, and
[`specs/001-audience-metrics-poc/quickstart.md`](specs/001-audience-metrics-poc/quickstart.md)
for the end-to-end validation guide (local + real AWS).

## Local development

Requirements: Python 3.12.

```bash
pip install -e ".[dev]"
```

Run the unit tests (no AWS involved — they run entirely against local temp files):

```bash
pytest tests/unit -v
```

Running `pytest` (or `pytest tests/`) with no arguments also collects
`tests/integration/`, but those tests auto-skip locally unless `BUCKET_NAME` and
`AWS_REGION` are set — see [`tests/integration/README.md`](tests/integration/README.md).

Try the reproducible event generator on its own:

```bash
python -m src.generator.events --seed 42 --date 2026-08-01 --out /tmp/eventos
```

## Running the pipeline for real

The pipeline itself only ever runs in GitHub Actions against a real S3 bucket,
authenticated via OIDC — never with local long-lived AWS keys (constitution Principle
VII). One-time setup:

1. Run `infra/bootstrap.sh` once against your AWS account (see the script header for
   required environment variables) to create the OIDC provider, IAM role, and S3 bucket.
2. Configure the repo's `ROLE_ARN` secret and `BUCKET_NAME`/`AWS_REGION` variables in
   GitHub with the values `bootstrap.sh` prints.
3. Trigger `.github/workflows/pipeline.yml` via `workflow_dispatch`, or let the daily
   `schedule` trigger run it.

Full walkthrough: [`specs/001-audience-metrics-poc/quickstart.md`](specs/001-audience-metrics-poc/quickstart.md).
