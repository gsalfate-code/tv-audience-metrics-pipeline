# Integration tests

Every test in this directory is marked `@pytest.mark.integration` (set at module level
via `pytestmark`) and talks to a **real S3 bucket**. Per the project constitution
(Principle IV), integration tests never run against mocked or local AWS — only against
the real bucket, and only from GitHub Actions.

## Requirements to run these tests

- `BUCKET_NAME` and `AWS_REGION` environment variables set
- AWS credentials in the environment (in CI: the OIDC-assumed role from
  `.github/workflows/pipeline.yml`; locally: your own AWS profile, if you really want to
  run them against a real bucket)

`tests/conftest.py` checks for `BUCKET_NAME`/`AWS_REGION` at collection time and
auto-skips every `integration`-marked test when they are absent — so plain `pytest` or
`pytest tests/` locally always runs cleanly, with **zero** AWS calls, and these tests
show up as `SKIPPED` rather than failing.

Each test creates its data under a disposable `_integration_tests/<random-id>/` prefix
and deletes it in an `autouse` fixture teardown, so runs never interfere with each other
or with the pipeline's real `bronze/`, `silver/`, `gold/` data.
