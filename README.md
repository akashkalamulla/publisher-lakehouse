# Publisher Lakehouse

Publisher Lakehouse is a deduplicated academic-metadata ingestion and analytics
platform. ScienceDirect is the Phase 1 pilot, with bronze, silver, gold, and
serving layers in place. Springer remains out of scope for this phase.

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
| 6 | `export/journal_export.py`, inline per-journal export in `pipeline.py`, `tests/test_journal_export.py` (including `test_matches_golden_shape`) | Complete before `gate7a`; no `gate6` tag |
| 7a | RustFS, Spark, Delta smoke test, file landing | Complete, tagged `gate7a` |
| 7b | Append-only bronze Delta article table | Complete, tagged `gate7b` |
| 8 | Current-state silver articles and data-quality tables | Complete, tagged `gate8` |
| 9 | Gold article star schema and weighted author bridge | Complete, tagged `gate9` |
| 10a | Postgres serving warehouse and atomic publish | Complete, tagged `gate10a`; live verification passed |
| 10b | Provisioned Grafana dashboards and API checks | Complete |
| 11a | Marker-driven Airflow orchestration and immutable Spark jobs | Complete |
| 11b | Scheduled Windows producer, scraper health in Grafana | Complete |

The ingestion, export, and lake CLI commands are available. `CABIACQ.py` is
untouched and remains the extraction regression reference.

The host suite includes warehouse schema contracts; the manifest integration
tests still require `PL_TEST_DATABASE_URL`.

## Lakehouse platform (gate 7a)

Use Docker Desktop with the WSL2 backend. Allocate at least 4 GB to Docker;
plan for 8 GB when Airflow is added. Gate 7a lands the Phase 1 JSONL and
compressed HTML unchanged and verifies Delta on object storage. Gate 7b loads
the landed JSONL into a bronze Delta table.

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

## Bronze Delta table (gate 7b)

Build the Spark image after checkout, then load one publisher. Repeating the
load inserts no rows for keys already present. `--schema` prints the declared
table schema; each run prints its summary and the last five Delta history rows.

```powershell
docker compose build spark
docker compose run --rm spark python -m publisher_lakehouse.transform.bronze --publisher sciencedirect --schema
docker compose run --rm spark python -m publisher_lakehouse.transform.bronze --publisher sciencedirect
docker compose run --rm --no-deps spark python -m pytest -q -p no:cacheprovider tests_spark
```

The summary reports `landed` JSONL rows, `inserted` new Delta rows, `already
present` keys found before the MERGE, `table rows` after it, `raw HTML missing`
referenced objects absent from landing, `files added` by the MERGE, and the
resulting Delta `version`. `inserted + already present = landed` on every run.
Missing raw HTML produces a warning and does not prevent the load.

To inspect the Spark UI at http://localhost:4040, run this command and press
Enter when finished:

```powershell
docker compose run --rm --service-ports spark python -m publisher_lakehouse.transform.bronze --publisher sciencedirect --hold
```

The table lives at `bronze/articles/` in the lake bucket. `_delta_log/` holds
commits, and data files live beneath `publisher=<publisher>/ingest_date=<date>/`.
Each row contains the raw HTML object's `landing/raw_html/...` key, not its
HTML bytes.

## Silver layer (gate 8)

Build current, typed articles for one publisher from bronze, then rerun to
verify the article table version stays the same. Use `--sample 5` to inspect
typed columns and `--hold` with `--service-ports` to keep the Spark UI open.

```powershell
docker compose run --rm spark python -m publisher_lakehouse.transform.silver --publisher sciencedirect --sample 5
docker compose run --rm spark python -m publisher_lakehouse.transform.silver --publisher sciencedirect
docker compose run --rm --no-deps spark python -m pytest -q -p no:cacheprovider tests_spark
docker compose run --rm --service-ports spark python -m publisher_lakehouse.transform.silver --publisher sciencedirect --hold
```

The summary reports bronze rows checked, latest valid candidates, new silver
inserts, changed silver updates, unchanged candidates, newly quarantined bronze
rows, candidates with at least one soft flag, current silver rows, and the
silver Delta version. A rerun with no new or changed candidates leaves the
versions of `silver/articles` and `silver/articles_quarantine` unchanged.

The lake bucket contains three silver tables:

- `silver/articles/` has one latest valid, typed row per article. Authors stay
  nested; rich-text segments remain in bronze.
- `silver/articles_quarantine/` keeps bronze versions with a missing article
  URL, journal URL, or both titles. It never duplicates an existing bronze key.
- `silver/dq_results/` appends one result per hard or soft rule on every run,
  including no-op runs. Its version therefore advances on a rerun.

Soft flags keep the article in silver: missing or invalid DOI, no authors,
missing abstract, unparsed publication month, month range, bad reference count,
bad copyright year, and reversed numeric page range. The printed per-rule table
shows each failure count and the number of rows evaluated.

## Gold star schema (gate 9)

Build the current article star from silver and inspect four example Spark SQL
queries:

```powershell
docker compose run --rm spark python -m publisher_lakehouse.transform.gold --publisher sciencedirect --queries
docker compose run --rm spark python -m publisher_lakehouse.transform.gold --publisher sciencedirect
docker compose run --rm --no-deps spark python -m pytest -q -p no:cacheprovider tests_spark
```

| Delta table under `gold/` | Grain |
| --- | --- |
| `dim_date` | One calendar day, 2000-01-01 through 2035-12-31, plus unknown |
| `dim_article_type` | One normalized article type, plus unknown |
| `dim_journal` | One journal title version per effective interval, plus unknown |
| `dim_issue` | One publisher and issue URL, plus unknown |
| `dim_author` | One normalized author name across all three roles, plus unknown |
| `fact_article` | One current silver article per publisher |
| `bridge_article_author` | One article, role, and 1-based position |

```text
 dim_date (publication month, first seen)      dim_article_type
                  \                            /
 dim_journal ---- fact_article ---- dim_issue
                         |
             bridge_article_author ---- dim_author
```

Journal titles use SCD Type 2. A title change expires the old journal row at
the changed article's `last_changed_at` and inserts a new current row. The fact
uses the journal version valid when its article last changed, so older articles
keep the earlier title version. Each dimension includes surrogate key `-1` for
unknowns; articles without a known publication month use the unknown date.

The bridge stores authors, editors, and corporate authors separately. Its
`weight` is `1 / entries in that role on that article`; sum weights for the
`author` role to count coauthored articles without inflating totals. A rerun
with unchanged silver skips every gold MERGE and leaves all gold versions
unchanged. Every build checks foreign keys, key uniqueness, journal intervals,
fact grain, bridge counts, and weights. `silver/dq_results` is compacted once
and each later silver run appends one Parquet file.

## Serving layer (gate 10a)

The analytical warehouse is a separate Postgres database from the Windows
ingestion manifest database. Docker exposes it on `127.0.0.1:5433`, leaving
the manifest's port 5432 untouched. Set `WAREHOUSE_DB`, `WAREHOUSE_USER`,
`WAREHOUSE_PASSWORD`, and `GRAFANA_DB_PASSWORD` in the local `.env`; see
`.env.example` for placeholders. Do not reuse manifest credentials.

```powershell
docker compose up -d warehouse
docker compose build spark
docker compose run --rm spark python -m publisher_lakehouse.transform.publish --dry-run
docker compose run --rm spark python -m publisher_lakehouse.transform.publish
docker compose run --rm spark python -m publisher_lakehouse.transform.publish --force
docker compose run --rm -e PL_TEST_WAREHOUSE=1 spark python -m pytest -q -p no:cacheprovider tests_spark/test_publish.py
```

`serving` holds the seven gold tables. `serving_stage` receives Spark JDBC
writes. `ops` holds DQ results, one status row per tracked lake table, and the
publish log. The `grafana_ro` role has read access to `serving` and `ops`; it
has no stage or write access. Serving tables have primary keys and indexes,
but no foreign-key constraints. Each gold build checks integrity, and the
publisher checks serving counts and orphan keys after the swap. Omitting FK
constraints keeps the full-refresh transaction simple at this data volume.

The job plans the current versions of 11 Delta tables, stages data read with
`versionAsOf`, overwrites each precreated stage table through Spark JDBC with
`truncate=true` and batches of 1000, and swaps all live tables and status rows
in one Postgres transaction before verifying the result. A rerun skips when every
version equals the latest successful publish. `--force` republishes the same
versions; `--dry-run` shows the plan without writing. Failed swaps roll back
the live tables and record a failed attempt separately.

Connect with a GUI to `localhost:5433` using the warehouse database and role,
or run `docker compose exec warehouse psql -U <WAREHOUSE_USER> -d
<WAREHOUSE_DB>`. The init script runs only when `warehouse-data` is empty;
changing passwords later requires `ALTER ROLE` or recreating that volume.

## Dashboards (gate 10b)

Start Grafana with `docker compose up -d grafana`, then open
http://127.0.0.1:3000 and log in with `GRAFANA_ADMIN_USER` and
`GRAFANA_ADMIN_PASSWORD` from your local `.env`. Anonymous access and sign-up
are disabled. The provisioned Warehouse data source connects to Postgres as
the read-only `grafana_ro` role.

The **Publisher Lakehouse** folder contains three dashboards:

| Dashboard | What it answers |
| --- | --- |
| Corpus overview | Article, journal, author, publication-month, license, and classification questions for a selected publisher. |
| Data quality | Latest rule failures, their recent trend, quarantine counts, soft flags, and run count. |
| Pipeline health | Published Delta versions and row counts, commit ages, warehouse publish history, and scraper health. |

Dashboards are code in `docker/grafana/dashboards/`. UI saves to the provisioned
dashboards are blocked. To change a panel, edit a copy in the UI, export its
JSON, update the matching repository file, commit it, and Grafana reloads the
file within 30 seconds. Run the full live API check from the host with:

```powershell
.venv\Scripts\python.exe scripts\check_grafana.py
```

Grafana applies the admin password only when `grafana-data` is first created.
To change it later, use `grafana cli admin reset-admin-password` inside the
container, or stop Grafana and remove only the `grafana-data` volume before
starting it with the new `.env` value.

## Orchestration (gate 11a)

The producer and consumer connect through an object-store marker:

```text
Windows scraper -> lake land (marker) -> Airflow DAG -> bronze -> silver -> gold
                                                         -> publish -> warehouse -> Grafana
```

Set the five `AIRFLOW_*` values shown in `.env.example`, then start Airflow:

```powershell
docker compose up -d airflow-init
docker compose up -d airflow-apiserver airflow-scheduler airflow-dag-processor
```

Open http://127.0.0.1:8080 and log in with `AIRFLOW_ADMIN_USER` and
`AIRFLOW_ADMIN_PASSWORD` from the local `.env`. The DAG is paused when first
created. Unpause `lakehouse_sciencedirect` in the UI to enable its hourly
schedule. Use **Trigger** with a JSON configuration of `{"force": true}` to
run the four jobs when there are no pending markers.

On the Windows host, `lake land --publisher sciencedirect` uploads changed
files and then writes one `landing/_markers/sciencedirect/*.json` marker. A
run with no changed files writes no marker. The DAG lists markers without a
matching `.json.processed` object, runs all four jobs, and writes an
acknowledgement for each pending marker only after publish succeeds. A forced
run with no pending markers writes no acknowledgement. The `lake_writer`
pool has one slot, so Delta and publish jobs cannot overlap across runs.

The scheduler uses an immutable job image containing the Python package; the
development `spark` service still bind-mounts the code. After committing code
changes, rebuild the job image; see "Rebuilding the job image" below.

Docker Desktop needs at least 6 GiB of available memory for this stack; 8 GiB
or more gives Spark and Airflow more headroom. The Windows scraper remains a
host process and is never launched by Airflow.

## Producer (gate 11b)

`scripts\producer.ps1` is the Windows half of the pipeline. Task Scheduler
runs it daily; you can also run it by hand. It works in Windows PowerShell 5.1
and PowerShell 7. Each run does four things:

1. `ingest run --publisher <p> [--limit N]` scrapes with the browser.
2. A Docker preflight checks that `objectstore` and `warehouse` report
   `healthy` in `docker compose ps`. If either does not, the run skips steps 3
   and 4 and exits 3. The scraped files stay on disk, and the next run's
   `lake land` uploads them.
3. `lake land --publisher <p>` uploads changed files and writes the marker
   that the Airflow DAG picks up.
4. `ops push-scrape-health` records this run and a fresh manifest snapshot in
   the warehouse for Grafana.

Steps 3 and 4 still run after a failed ingest: whatever the scraper wrote is
durable, and the push records the run as `incomplete`.

| Parameter | Default | Meaning |
| --- | --- | --- |
| `-Publisher` | `sciencedirect` | Publisher to run |
| `-Limit` | `0` (no limit) | Only the first N enabled journals; `ingest run` logs its coverage warning |

| Exit code | Meaning |
| --- | --- |
| 0 | Success |
| 1 | Ingest failed (or the script itself failed; see `producer.log`) |
| 2 | Land failed |
| 3 | Docker unavailable; land and push skipped |
| 4 | Push failed |
| 5 | Another producer run is active |

When several steps fail, the exit code is the first match in the order 5, 3,
1, 2, 4.

Each run writes UTF-8 logs to
`logs\producer\<UTC yyyyMMddTHHmmssZ>-<publisher>\`:

| File | Contents |
| --- | --- |
| `ingest.out.log` | Scraper progress (stdout) |
| `ingest.err.log` | structlog JSON, including `ingestion_run_complete` with the run counters |
| `land.log` | `lake land` output, then its JSON log |
| `push.log` | Push summary line, then its JSON log |
| `producer.log` | Timestamped step results; the last line is the run summary |

```text
Producer sciencedirect: ingest=0 land=ok marker=written push=ok exit=0
```

`logs\producer\.lock` holds the PID and start time of the active run. A second
start exits 5 with `another producer run is active (pid N)`. If the lock's
process is gone, for example after a killed run, the next run replaces the
lock.

Run the producer by hand:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\producer.ps1 -Publisher sciencedirect
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\producer.ps1 -Publisher sciencedirect -Limit 1
```

`PL_PRODUCER_DRY_RUN=1` skips the ingest step and still runs the preflight,
land and push. It exists only to verify the script. Each dry run records an
`incomplete` run with exit code 0, because it produces no completion event.

### Scheduling

Register or update the daily task, `PublisherLakehouse-Producer`:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\register_producer_task.ps1 -At 02:00
```

`-At` is local time. Add `-PrintOnly` to print the task XML without
registering it. To unregister the task:

```powershell
Unregister-ScheduledTask -TaskName PublisherLakehouse-Producer -Confirm:$false
```

The task is set to **Run only when user is logged on**. The scraper drives a
real Chrome window, which needs your desktop session. A task set to "Run
whether user is logged on or not" runs without a desktop, so the browser
cannot work, and Windows would have to store your password. The task
therefore stores none. It runs while your session is locked, but not while
you are signed out or the computer is asleep. A missed start runs as soon as
possible after that. A second start while a run is active is ignored, and a
run is stopped after 10 hours. Battery power neither prevents nor stops a
run.

### Scraper health

`ops push-scrape-health` writes three warehouse tables. `grafana_ro` can read
them.

| Table | Contents |
| --- | --- |
| `ops.scrape_runs` | One row per run: `run_id`, `finished_at`, `ingest_exit_code`, `status`, and one column per run counter |
| `ops.scrape_status` | Manifest rows per URL type and status, with the oldest and newest `last_seen_at` |
| `ops.scrape_journals` | The manifest's journal-tier rows |

A run whose log has an `ingestion_run_complete` event is `complete`. A run
without one, such as a crash, is `incomplete` with the id
`incomplete-<log directory>`. It has no counters. Each push replaces the
publisher's snapshot rows. It reads the manifest in a read-only session. To
refresh only the snapshot:

```powershell
.venv\Scripts\python.exe -m publisher_lakehouse.cli ops push-scrape-health --publisher sciencedirect
```

The **Scraper health** row on the Pipeline health dashboard shows when the
last scrape finished, the last run's status and exit code, the last 10 runs,
manifest rows by type and status, and the journals needing attention.
`scripts\check_grafana.py` checks these panels against the manifest. The
pipeline stages only article rows in the manifest, so "Journals needing
attention" stays empty until journal rows are recorded.

### Rebuilding the job image

After committing changes under `publisher_lakehouse/`, rebuild the Airflow
job image:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\build_job_image.ps1
```

The script labels the image with `git rev-parse HEAD`. It adds `-dirty` when
the working tree has uncommitted changes. Then it prints the image's
`org.opencontainers.image.revision` label. A build that sets no `GIT_COMMIT`
is labelled `unknown`.

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
`common/logging.py` provides an `errors.txt` writer, but the pipeline and CLI
do not call it; no `errors.txt` file is written automatically.

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

The Gate 6 export comparison is implemented. The live acceptance sequence is:

1. `ingest run --publisher sciencedirect --limit 3` completes against the real
   site and produces bronze JSONL; `export run --publisher sciencedirect` produces files
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
