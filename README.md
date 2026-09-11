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
| 1 | Packaging, `settings.py`, `common/` | Complete, fixes applied |
| 2 | `inputs/loader.py`, `scripts/migrate_url_file.py` | Complete |
| 3 | `manifest/` — models, Alembic, repository, policy | Code complete; **not yet run against PostgreSQL** |
| 4 | `ingestion/browser/`, `sciencedirect/extract.py` | Not started |
| 5 | `base.py`, `scraper.py`, `pipeline.py`, writers, `cli.py` | Not started |
| 6 | `export/journal_json.py` + regression test | Not started |

The ingestion, browser, export, and CLI commands are intentionally unavailable
until their review gates are approved. `CABIACQ.py` remains the working
scraper and is untouched.

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

## Where the logs go

**Structured JSON logs go to stderr.** The browser layer prints
human-readable progress to stdout, so stdout is not machine-readable. Anything
that parses this run's logs reads stderr:

```powershell
& .\.venv\Scripts\python.exe -m publisher_lakehouse.cli ingest run 2> run.jsonl
```

Errors are additionally collected in memory during the run and rendered to a
legacy-compatible `errors.txt` at the end, one physical line per event with
embedded newlines escaped.

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

Not yet runnable (gates 4–6 outstanding). It will be:

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
