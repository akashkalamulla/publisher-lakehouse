# Publisher Lakehouse

Publisher Lakehouse is being built as a deduplicated academic-metadata
ingestion platform. ScienceDirect is the Phase 1 pilot; Springer and later
lakehouse layers remain out of scope for this phase.

Phase 1 turns the working `CABIACQ.py` script into an ingestion pipeline with
deduplication **without changing a single extracted value**. The per-journal
JSON it produces must stay byte-for-byte identical to what the script produces
for the same input.

## Implementation status

The project follows the review gates in
`claude_code_prompt_phase1_sciencedirect_v2.md`.

| Gate | Scope | Status |
|---|---|---|
| 1 | Packaging, `settings.py`, `common/` | Complete |
| 2 | `inputs/loader.py`, `scripts/migrate_url_file.py` | Complete |
| 3 | `manifest/` — models, Alembic, repository, policy | Complete, verified against PostgreSQL 16 |
| 4 | `ingestion/browser/`, `sciencedirect/extract.py` | Complete, tagged `gate4` |
| 5a | `schemas/article.py`, `sciencedirect/record.py`, `writers/` | Complete, tagged `gate5a` — 8/8 golden comparisons exact |
| 5b | `base.py`, `scraper.py`, `pipeline.py`, `cli.py` | Complete, tagged `gate5b` |
| 6 | `export/journal_json.py` + regression test | Not started |

The ingestion and CLI commands are intentionally unavailable until their review
gates are approved. `CABIACQ.py` remains the working scraper and is untouched.

Suite: 223 tests (214 offline, 9 requiring `PL_TEST_DATABASE_URL`).

## Lakehouse platform (gate 7a)

Use Docker Desktop with the WSL2 backend. Allocate at least 4 GB to Docker;
plan for 8 GB when Airflow is added. This gate lands the Phase 1 JSONL and
compressed HTML unchanged and verifies Delta on object storage. It does not
create a bronze Delta table from those files.

Set these four variables in `.env` (choose your own credentials, with a secret
of at least eight characters): `S3_ENDPOINT_URL=http://localhost:9000`,
`S3_ACCESS_KEY`, `S3_SECRET_KEY`, and `LAKE_BUCKET=lakehouse`. The local object
store (RustFS) uses the same S3 keys as its root credentials. The Spark service
uses `http://objectstore:9000` on the Compose network.

```powershell
docker compose up -d objectstore
publisher-lakehouse lake init
publisher-lakehouse lake land --publisher sciencedirect
docker compose build spark
docker compose run --rm --service-ports spark python -m publisher_lakehouse.transform.smoke
```

The RustFS console is at
http://127.0.0.1:9001/rustfs/console/index.html; log in with the S3 keys. The
Spark UI is at http://localhost:4040 while a job started with
`--service-ports` is running. `docker compose down` keeps the lake's named
volume. `docker compose down -v` deletes `lake-data` and the whole lake.

Do not run `lake land` while `ingest run` is active: a JSONL part may still be
growing. A later `lake land` replaces any changed part. Repeated landing runs
report unchanged files and upload no bytes when the local files are stable.

### Gate 3 is blocked

The PostgreSQL server on `localhost:5432` is running, but the password in
`.env` is still the template placeholder, so connections are rejected. The
brief forbids falling back to SQLite, creating a database, or rewriting the
URL, so the migration has not been run. See `DEVIATIONS.md`.

To unblock, put the real password in `.env` and run:

```powershell
& .\.venv\Scripts\python.exe -m alembic upgrade head
psql -d publisher_lakehouse -c "\d ingestion_manifest"
```

All three timestamp columns must report `timestamp with time zone`.

## Configuration

Two separate domains, deliberately:

- **`.env`** — environment and secrets, read by `pydantic-settings`.
  `DATABASE_URL` has **no default**: a missing or wrong URL fails the run
  rather than quietly writing the manifest somewhere else.
- **`config/config.ini`** — operator behaviour, read by `configparser` into
  frozen models with `extra="forbid"`, so a new config key requires a matching
  field.

Browser timing constants are **not** configuration. They are tuned against
live anti-bot behaviour and stay as module constants. `force_refetch` is
CLI-only.

Importing any module reads neither file and creates no directories; settings
load explicitly at the application boundary.

## How to run

After each journal completes, the pipeline writes per-journal output to:

    data/exports/<publisher>/<journal_slug>/<journal_slug>.json
    data/exports/<publisher>/<journal_slug>/<journal_slug>_html.zip

To rebuild all exports from bronze without re-scraping:

    publisher-lakehouse export run --publisher sciencedirect

## Where the logs go

**Structured JSON logs go to stderr.** The browser layer prints
human-readable progress to stdout, so stdout is not machine-readable. Anything
that parses this run's logs reads stderr:

```powershell
& .\.venv\Scripts\python.exe -m publisher_lakehouse.cli ingest run 2> run.jsonl
```

Errors appended by the frozen modules are collected in memory and emitted as
structured `legacy_error` events at the end of ingestion. The
legacy-compatible `errors.txt` renderer remains deferred to Gate 6.

## The URL model

```
journal URL   (input: .../journal/<slug>/issues, or .../bookseries/<slug>/volumes)
    └── issue URL   (discovered: .../vol/73/issue/3)
            └── article URL   ← the deduplication unit
```

Journal and issue pages are **discovery** pages and are re-fetched every run
(`journal_days = 0`, `issue_days = 0`), or a new issue is never noticed.
Articles are the expensive, stable thing dedup protects: each costs a browser
navigation, an ARP API call, and a 3–7 second delay.

## Input file

`inputs/url_details.csv` is the input of record:

```csv
publisher,url,enabled,note
sciencedirect,https://www.sciencedirect.com/journal/harmful-algae/issues,true,
sciencedirect,https://www.sciencedirect.com/bookseries/advances-in-parasitology/volumes,true,bookseries
```

It was generated from the legacy `urlDetails.txt` (85 URLs, despite the name
"74") and the row count is asserted, not trusted:

```powershell
& .\.venv\Scripts\python.exe scripts\migrate_url_file.py
```

Nothing in config may reduce coverage: every enabled row is visited every run.
The only exception will be `--limit N` on the CLI, which logs a loud warning
that it is truncating.

## Run counters

Emitted as one structured log line at the end of a run. **Every discovered
article URL must land in exactly one bucket**, and the run fails loudly if the
buckets do not sum to `articles_discovered`.

| Counter | Meaning |
|---|---|
| `input_total` | Rows in the input file for this publisher |
| `dup_in_file` | Gate 0: rows that normalised to a URL already in the file |
| `skipped_in_progress` | Journals whose latest issue is still "In progress" |
| `articles_discovered` | Article URLs found across all issue pages |
| `dup_in_run_url` | Gate 1: article URL already seen this run |
| `dup_in_run_doi` | Gate 3: DOI already seen this run, under a different URL |
| `skipped_manifest` | Gate 2: collected recently enough, inside the refresh window |
| `fetched` | Articles actually fetched |
| `fetch_failed` | Fetch raised or timed out |
| `blocked_gave_up` | Still blocked after the browser-restart retries |
| `parse_failed` | Fetched but could not be parsed |
| `unchanged_payload` | Gate 4: re-fetched, payload hash identical, no bronze row |
| `bronze_written` | Bronze JSONL records written |

A `blocked` manifest status **always** re-fetches, whatever the refresh
window. A Cloudflare block is not a successful collection; recording it as one
would suppress that article for 90 days and surface as a short QA diff months
later.

## Ordering guarantee

Bronze is written **before** the manifest row is staged. The manifest is a
claim that a URL is already collected — written first, a crash between the two
would make the next run skip a URL whose data was never stored, silently and
permanently.

## The three-run acceptance test

Not fully runnable until Gate 6 supplies the export comparison. The complete
acceptance sequence will be:

1. `ingest run --publisher sciencedirect --limit 3` completes against the real
   site and produces bronze JSONL; `export json` over that run produces files
   byte-identical to `CABIACQ.py`'s output for the same journals.
2. `ingest run` **immediately again** re-fetches journal and issue pages and
   fetches **zero** articles; `skipped_manifest` equals the discovered article
   count.
3. A third run with `--force-refetch` fetches every article again but writes
   **zero** bronze rows, because the payload hashes match.

Counters must sum exactly to `articles_discovered` on all three runs.

## Tests

No network calls, no browser, no live PostgreSQL by default.

```powershell
& .\.venv\Scripts\python.exe -m pip install -e '.[test]'
& .\.venv\Scripts\python.exe -m pytest
```

The manifest integration tests are skipped unless `PL_TEST_DATABASE_URL` is
set. It must point at a **separate** database — the tests drop and recreate the
schema:

```powershell
$env:PL_TEST_DATABASE_URL = 'postgresql+psycopg://USER:PASSWORD@localhost:5432/publisher_lakehouse_test'
& .\.venv\Scripts\python.exe -m pytest tests\test_manifest_repository_pg.py
```

SQLite appears only as a test-only convenience for exercising the second
upsert dialect. It is not a fallback for the working database.
