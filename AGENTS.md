# Repository Instructions For Codex

## Project Overview

This repository is `GPT-Trade`, published as `edgar-tools`: a Python toolkit for
SEC EDGAR filing collection, parsing, monitoring, S3 upload, and database-backed
ETL workflows.

Use the refactored EDGAR architecture for new work. The original modules and
scripts remain for compatibility, but `*_new.py` scripts and the newer classes
under `edgar/` are the preferred path.

Primary capabilities:

- Fetch and list SEC filings such as 10-K, 10-Q, and 8-K.
- Validate CIKs, accession numbers, SEC URLs, and configuration.
- Parse filing documents and manage local processing state.
- Upload filing artifacts to S3 when configured.
- Run GPT-Trader ETL workflows with SQLAlchemy-backed persistence.
- Monitor jobs, progress, performance, and SLA-style processing metrics.

## Architecture

- `edgar/`: core EDGAR toolkit. Prefer `client_new.py`, `s3_manager.py`,
  `filing_processor.py`, `config_manager.py`, `urls.py`, `parser.py`, and
  `state.py` for new work.
- `gpt_trader/`: database-backed ETL platform, configuration, models,
  monitoring, and filing processors.
- `orchestration/`: scheduling, resource management, progress, performance, and
  higher-level orchestration helpers.
- `scripts/`: command-line entry points. Prefer `fetch_10k_new.py`,
  `list_files_new.py`, `monitor_new.py`, and `gpt_trader_cli.py`.
- `database/`: SQL schema, SQLAlchemy models, Alembic config, and migrations.
- `config/` and `scripts/config/`: checked-in example or local JSON
  configuration references.
- `tests/`: pytest suite with unit, integration, parser, ETL, database, and
  refactored-component coverage.
- `CLAUDEmd/`: migration and cleanup notes for the refactored architecture.

Do not introduce a `src/` layout for this project. Keep package code in the
existing top-level packages unless a separate migration is explicitly requested.

## Commands

Setup:

```bash
python -m venv venv
venv\Scripts\activate
pip install -e ".[dev,test]"
```

Alternative dependency install:

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

Common development commands:

```bash
make test
make test-fast
make test-unit
make test-integration
make test-cov
make lint
make format-check
make type-check
make security
make pre-commit
make qa
```

Direct pytest commands:

```bash
python -m pytest tests/ -v
python -m pytest tests/test_refactored_components.py -v
python -m pytest tests/test_parser.py -v
python run_tests.py --type fast --no-coverage
```

Useful CLI commands:

```bash
python scripts/fetch_10k_new.py 0000320193 --verbose
python scripts/list_files_new.py 0000320193 --json --output files.json
python scripts/monitor_new.py 0000320193 --bucket my-bucket --dry-run
python scripts/gpt_trader_cli.py init-db
python scripts/gpt_trader_cli.py config create-sample
python scripts/gpt_trader_cli.py run-etl --ticker AAPL
```

Some Makefile targets use Unix-style shell commands. On Windows, prefer the
direct Python commands when a Make target is not available in the current shell.

## Configuration

Set a compliant SEC user agent before making SEC requests:

```bash
set SEC_USER_AGENT=YourApp (contact@example.com)
```

Common EDGAR settings:

- `SEC_USER_AGENT`: required for SEC access.
- `EDGAR_RATE_LIMIT`: default request rate limit, commonly `6.0`.
- `EDGAR_TIMEOUT`, `EDGAR_MAX_RETRIES`, `EDGAR_NUM_WORKERS`.
- `EDGAR_FORM_TYPES`, for example `10-K,10-Q`.
- `EDGAR_S3_REGION`, `EDGAR_S3_PREFIX`, and AWS credentials for S3 workflows.
- `EDGAR_LOG_LEVEL`.

GPT-Trader/database settings commonly include:

- `DATABASE_URL` for PostgreSQL or SQLite.
- `S3_BUCKET` for upload workflows.
- JSON config files created or referenced by `scripts/gpt_trader_cli.py`.

Keep real credentials, private bucket names, production database URLs, `.env`
files, downloaded filings, generated reports, logs, local databases, coverage
outputs, and build artifacts out of commits unless the user explicitly asks for
a tracked fixture or example. Prefer small synthetic fixtures in `tests/`.

## Testing

- Add or update tests when changing parsing, validation, configuration,
  database, ETL, S3, orchestration, or CLI behavior.
- Use mocked SEC, S3, and database dependencies unless an integration test is
  explicitly required.
- Keep tests deterministic and avoid live network access in default test runs.
- Respect existing pytest markers: `unit`, `integration`, `slow`, and `network`.
- Coverage is configured primarily for `edgar`; do not assume GPT-Trader changes
  are covered unless targeted tests are added.

## Workflow Cautions

- Preserve SEC compliance: require a meaningful user agent, respect rate limits,
  and avoid unnecessary live SEC traffic.
- Prefer dependency injection, explicit configuration, context managers, and
  resource cleanup over hidden global state.
- Do not add live trading, broker execution, account access, or financial advice
  behavior unless the user explicitly scopes that work.
- Do not store secrets, AWS credentials, real account data, or private
  production settings in the repository.
- Keep legacy compatibility unless the user explicitly asks to remove it.
- Keep changes focused and avoid broad cleanup unrelated to the task.
- Do not create commits, tags, pushes, releases, or pull requests unless the user
  explicitly requests them.
