# Task: Phase 1 — ScienceDirect ingestion pipeline with deduplication

**Version 2.** Changes from v1: PostgreSQL is now the development target and is
already provisioned (SQLite is demoted to a test-only convenience); gate 1 is
complete and its required fixes are listed in `gate1_fixes.md`; several
gate-1 findings now constrain later gates.

---

## Context

This repo (`publisher-lakehouse`) will become a multi-publisher academic metadata
data platform. It contains two working scrapers:

- `CABIACQ.py` (~1,986 lines) — **ScienceDirect**. Async, `zendriver` browser
  automation, Cloudflare clearance, block detection with browser restart.
  Reads journal URLs from `urlDetails.txt` (85 URLs), finds each journal's
  latest issue, scrapes every article across all pagination pages, and writes
  one JSON file per journal: `<Journal_Title>_<YYYYMMDDHHMMSS>.json` containing
  `{"articles": [ {...29 keys...} ]}`.
- `cabi_springer.py` (~910 lines) — **Springer**. Sync, `requests` +
  BeautifulSoup. **Out of scope.** Do not touch it.

**ScienceDirect is the pilot.** Phase 1 turns `CABIACQ.py` into a proper
ingestion pipeline with deduplication, without changing a single extracted
value. Springer is Phase 1b and will be added against the same interfaces later.

Later phases (Delta Lake, Spark transforms, star schema, Airflow) are **out of
scope**. Do not start or scaffold them.

Read `CABIACQ.py` fully before writing any code. Read the sample outputs in
`tests/fixtures/golden/sciencedirect/` to understand the exact target shape.

---

## Environment — already provisioned by the operator

Do not install, configure, or script any of this. It exists.

- **PostgreSQL** is installed and running on `localhost:5432`.
- The database `publisher_lakehouse` has been created.
- `.env` contains a working `postgresql+psycopg://` URL, `DATA_DIR`,
  `LOG_LEVEL`, `CAPTURE_FIXTURES`. It is gitignored.
- `psycopg[binary]>=3.2` is declared in `pyproject.toml` and installed.

If a connection fails, report it and stop. Do not fall back to SQLite, do not
create a database, do not rewrite the URL.

---

## Non-negotiable constraints

1. **Do not change extraction logic.** Every `extract_*` function, plus
   `html_to_rich_text`, `_parse_arp_authors`, `_collect_authors`,
   `_collect_collaborations`, `_clean_affiliation_text`, `_decode_email`,
   `_extract_token_and_pii`, `_find_token`, `_is_affiliation_ref`,
   `get_latest_issue_url`, `_volume_issue_from_url`,
   `extract_issue_volume_pubdate`, `get_all_article_list`,
   `extract_article_data`, `get_total_pages`, and `is_blocked` moves
   **verbatim** into the new package. Same selectors, same regexes, same
   `None`-vs-`""` behaviour. You may only change a signature where it currently
   reaches for a module-level global.

2. **The browser layer is frozen.** `_solve_cf_on_page`, `start_browser`,
   `_wait_ready`, `_accept_cookies`, `setup_browser_session`, `fetch_soup`,
   `fetch_soup_with_page`, `_arp_api_xhr`, `_arp_api_browser_nav`, all
   `BROWSER_ARGS`, all delay and retry constants, and the block-recovery loop in
   `scrape_journal` move into `ingestion/browser/` **unchanged**. This code is
   the result of trial and error against a live anti-bot system. Do not refactor
   it, do not "clean it up", do not change a timeout.

3. **Output must stay byte-identical.** The per-journal JSON file produced at
   the end of Phase 1 must be byte-for-byte identical to the current output for
   the same input, including key order, `indent=2`, `ensure_ascii=False`, the
   trailing newline, the `{"articles": [...]}` wrapper, and the
   `<Journal_Title>_<YYYYMMDDHHMMSS>.json` filename convention. The
   `_json_contacts` quirk that replaces `/` with `\` in affiliations is part of
   that contract — preserve it.

4. **Bronze grain is one row per article.** The existing JSON article object is
   already at this grain. Bronze is that object plus provenance fields, one per
   line in JSONL.

5. **Write bronze before the manifest.** The manifest is a claim that a URL is
   already collected. If it is written first and the process dies, the next run
   skips a URL whose data was never stored, silently and permanently. Bronze
   write must succeed before the corresponding manifest row is staged.

6. **Nothing in config may reduce coverage.** Every enabled URL in the input
   file is visited every run. The only exception is `--limit N` on the CLI,
   which must log a loud warning that it is truncating.

7. **No Spark, no Delta, no Airflow, no MinIO, no FastAPI.** Do not create empty
   `transform/` or `serving/` packages.

---

## Known issues in the current script — handle each as specified

| Issue | Location | Action |
|---|---|---|
| `config.read()` and `folder_path = ...` execute at **import time** | lines 48–63 | **Done at gate 1.** Settings load explicitly at the boundary. |
| Rich text discarded: `_json_text()` keeps only string fragments | `JSON_FIELDS`, `_json_text` | **Additive only.** Flat fields unchanged. In **bronze only**, add `english_title_rich`, `foreign_title_rich`, `english_abstract_rich` as neutral segments. Export must exclude them. |
| `html_to_rich_text` returns `(None, plain)` when `has_formatting` is false **or** `len(runs) < 4` | line ~576 | Formatting is lost for short rich runs. **Legacy behaviour — preserve it.** Callers must pass the plain text as the `to_segments` fallback (see gate-1 fixes). |
| `JsonOutput.add_format` builds `xlsxwriter.Format` objects for a JSON writer | line ~1447 | Keep the shim. `# TODO`: removable once neutral segments are proven. Keep `XlsxWriter` in dependencies. |
| Global `error_list` + `errors.txt` in a module-level `finally` | line 53, file bottom | **Replaced at gate 1** by structlog events + `write_error_report`. Newlines are now escaped to one physical line per event — recorded in `DEVIATIONS.md`. |
| `skip_issues = "In progress"` silently skips a whole journal | `get_latest_issue_url` | **Keep the behaviour**, but record it: manifest row gets `status='skipped_in_progress'`, plus a counter. |
| `DATE` module-level timestamp | line 44 | Becomes `run_id` (local time, legacy format). Bronze also carries `run_started_at` in UTC. |
| Commented-out debug lines | `scrape_journal` | Delete. `--limit` replaces them. |
| `urlDetails.txt` named "74" but holds **85** URLs, one a **bookseries** (`/bookseries/<slug>/volumes`) | input file | Migrate all 85. The bookseries URL needs its own handling and test. Do not drop it. |

---

## URL model — three tiers

```
journal URL   (input, e.g. https://www.sciencedirect.com/journal/harmful-algae/issues)
    │          also: https://www.sciencedirect.com/bookseries/<slug>/volumes
    └── issue URL   (discovered: latest issue, e.g. .../vol/73/issue/3)
            └── article URL   ← THIS is the deduplication unit
                              e.g. .../science/article/pii/S0019570726001617
```

Journal and issue pages are **discovery** pages, re-fetched every run or a new
issue is never noticed. Articles are the expensive, stable thing dedup protects
— each costs a browser navigation, an ARP API call, and a 3–7 second delay.

The manifest has a `url_type` column (`journal` | `issue` | `article`) and the
refresh policy is looked up per type.

---

## Target structure

```
publisher-lakehouse/
├── pyproject.toml                       # includes [project.scripts]
├── .env  .env.example  .gitignore
├── README.md  DEVIATIONS.md
│
├── alembic.ini                          # sqlalchemy.url left EMPTY
├── config/config.ini
├── inputs/url_details.csv
├── data/                                # gitignored
│   ├── raw_html/sciencedirect/<run_date>/<url_hash>.html.gz
│   └── bronze/sciencedirect/ingest_date=YYYY-MM-DD/part-<run_id>.jsonl
├── migrations/                          # alembic versions
├── scripts/migrate_url_file.py
│
├── publisher_lakehouse/
│   ├── settings.py                      ✓ gate 1
│   ├── cli.py
│   ├── common/                          ✓ gate 1
│   │   ├── urls.py  hashing.py  ids.py  richtext.py  logging.py
│   ├── inputs/loader.py
│   ├── manifest/
│   │   ├── models.py  repository.py  policy.py
│   ├── schemas/article.py
│   ├── ingestion/
│   │   ├── base.py
│   │   ├── sciencedirect/
│   │   │   ├── scraper.py
│   │   │   └── extract.py               # verbatim move
│   │   ├── browser/                     # FROZEN
│   │   ├── pipeline.py
│   │   └── writers/raw_store.py  bronze_writer.py
│   └── export/journal_json.py
│
└── tests/
    ├── conftest.py
    ├── test_settings.py                 test_logging.py
    ├── test_normalize_url.py            test_payload_hash.py
    ├── test_richtext.py                 test_loader.py
    ├── test_manifest_policy.py          test_manifest_repository_pg.py
    ├── test_sciencedirect_parse.py      test_export_regression.py
    └── fixtures/
        ├── sciencedirect/html/
        └── golden/sciencedirect/
```

---

## Module contracts

### `settings.py` — complete (gate 1)

`.env` via `pydantic-settings` for environment and secrets. `config/config.ini`
via `configparser` into frozen pydantic models for operator behaviour. Importing
either reads nothing.

```ini
[DETAILS]
output_path = C:\CABIACQ_NEW1\

[paths]
url_details  = inputs/url_details.csv
raw_html_dir = data/raw_html
bronze_dir   = data/bronze

[ingestion]
bronze_flush_every   = 200
manifest_flush_every = 100

[refresh]
journal_days = 0
issue_days   = 0
article_days = 90
```

All browser timing constants stay as module constants in `ingestion/browser/`.
**Do not move them into config** — they are tuned against live anti-bot
behaviour, not operator preference. `force_refetch` is **CLI-only**.

Models use `extra="forbid"`, so any new config key requires a matching field.
That is deliberate.

### `common/` — complete (gate 1), with fixes pending

See `gate1_fixes.md`. Three constraints from it bind later gates:

- `to_segments(rich, plain_text)` takes the plain text as a fallback. Every
  caller in `sciencedirect/scraper.py` must pass it, because
  `html_to_rich_text` returns `None` for the rich value on most articles.
- `PUBLISHER_OVERRIDES` maps `sciencedirect.com → www.sciencedirect.com`.
  Phase 1b adds `rd.springer.com → link.springer.com`.
- structlog JSON goes to **stderr**, because the frozen browser code prints
  human-readable lines to stdout. Anything reading logs programmatically reads
  stderr.

### `inputs/loader.py`

Reads `inputs/url_details.csv`:

```csv
publisher,url,enabled,note
sciencedirect,https://www.sciencedirect.com/journal/harmful-algae/issues,true,
sciencedirect,https://www.sciencedirect.com/bookseries/advances-in-parasitology/volumes,true,bookseries
```

`scripts/migrate_url_file.py` converts `urlDetails.txt` (85 URLs) into this CSV
with `publisher=sciencedirect, enabled=true`, tagging the bookseries row in
`note`. Assert output row count equals input line count.

`load_tasks(publisher) -> list[UrlTask]` filters by publisher, drops
`enabled=false`, normalises every URL, applies **gate 0** (dedup within the file,
logging both raw URLs on every drop), returns
`UrlTask(url_hash, normalised_url, raw_url, publisher, url_type, note)`.

### `manifest/models.py` — PostgreSQL is the target

One table, `ingestion_manifest`:

| Column | Type | Notes |
|---|---|---|
| `url_hash` | `TEXT` PK | from normalised URL |
| `normalised_url` | `TEXT` | debugging |
| `publisher` | `TEXT` | indexed with `url_type` |
| `url_type` | `TEXT` | `journal` / `issue` / `article` |
| `doi` | `TEXT` NULL | filled after parse |
| `payload_hash` | `TEXT` NULL | |
| `first_seen_at` | `TIMESTAMP(timezone=True)` | |
| `last_seen_at` | `TIMESTAMP(timezone=True)` | |
| `last_changed_at` | `TIMESTAMP(timezone=True)` NULL | |
| `fetch_count` | `INTEGER` | |
| `last_http_status` | `INTEGER` NULL | |
| `last_run_id` | `TEXT` | |
| `status` | `TEXT` | `ok` / `failed` / `blocked` / `not_found` / `parse_error` / `skipped_in_progress` |

**Timestamps are timezone-aware and stored in UTC.** Every write uses
`datetime.now(timezone.utc)`. Postgres distinguishes aware from naive; SQLite
silently does not, so a naive value would pass unit tests and shift
`should_fetch` comparisons by the operator's UTC offset in production. Make
`should_fetch` **raise** on a naive `now` rather than comparing it.

Use SQLAlchemy Core. Index `(publisher, url_type)` and `(publisher, status)`.

### `manifest/repository.py` — dialect-aware upsert

- `load_for_publisher(publisher) -> dict[str, ManifestRow]` — one query at run
  start, held in memory. **Never query per URL.**
- `batch_upsert(rows)` — `INSERT ... ON CONFLICT (url_hash) DO UPDATE`, called
  every `manifest_flush_every` and once at the end.

The upsert construct differs per dialect and the two are **separate imports**:

```python
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
```

Dispatch on `engine.dialect.name`. Code written against only one dialect can
pass its whole suite and fail on the other.

Manifest I/O is **synchronous** and must never be called from inside the
per-article async loop. Load once before, flush in batches between journals.

### Alembic

- `alembic.ini` must leave `sqlalchemy.url` **empty**. `migrations/env.py` reads
  the URL from `EnvironmentSettings` so the database password never lands in a
  committed file.
- Generate and run the migration against **PostgreSQL**, not SQLite.
- Verify with `psql -d publisher_lakehouse -c "\d ingestion_manifest"` and paste
  the output into the gate-3 review. Confirm the timestamp columns report
  `timestamp with time zone`.

### `manifest/policy.py`

```python
def should_fetch(task, manifest_row, refresh_days, force_refetch, now) -> tuple[bool, str]:
    """Returns (fetch?, reason). Reason is logged and counted."""
```

Skip only when the row exists, `status == 'ok'`, `last_seen_at` is within
`refresh_days`, and `force_refetch` is False.

- `refresh_days == 0` means always fetch.
- `status == 'blocked'` **always** re-fetches regardless of window. A Cloudflare
  block is not a successful collection; recording it as one would suppress that
  article for 90 days and surface as a short QA diff months later.
- Naive `now` raises.

Pure function, no I/O, fully unit-tested against fabricated rows.

### `ingestion/base.py`

```python
class BaseScraper(ABC):
    publisher: str

    @abstractmethod
    async def discover_issues(self, journal_task) -> list[IssueRef]: ...

    @abstractmethod
    async def discover_articles(self, issue_ref) -> list[ArticleRef]: ...

    @abstractmethod
    def parse_article(self, html: str, context) -> dict: ...
```

`parse_article` is **synchronous**, takes HTML text, and must not touch the
network. This is what makes fixture-based testing possible and it is the single
most important line in this document.

The ARP author API complicates this: `extract_author_details` is async and needs
a live page. Split it. `parse_article` handles everything obtainable from the
article HTML alone; author enrichment stays in the scraper as
`async def enrich_authors(...)` and is merged by the scraper, not by
`parse_article`. Document the split in `DEVIATIONS.md`.

Springer (Phase 1b) is synchronous. Keep the ABC async and let Springer
implement thin sync wrappers. Do not build two parallel hierarchies.

### `ingestion/sciencedirect/scraper.py`

`discover_issues` wraps `get_latest_issue_url` plus the `"In progress"` skip.
`discover_articles` wraps `get_all_articles_with_pagination` and returns
**absolute article URLs**, not BeautifulSoup `<li>` nodes — soup fragments
cannot be serialised, resumed, or deduplicated. Extract the href and
`article_type` in one pass and carry `ArticleRef(url, url_hash, article_type)`.

Every `to_segments` call passes the plain-text fallback returned alongside the
rich value by `html_to_rich_text`.

### `ingestion/pipeline.py` — where the gates go

**Critical.** The block-recovery loop deliberately does not increment `idx` when
an article is blocked. If a dedup skip is mistaken for a block, the run either
loops forever or skips real articles. Therefore:

- **Gates 1 and 2 run before the block-recovery loop**, as a filter over the
  discovered article list. The loop then runs over the filtered list, untouched.
- **Gates 3 and 4 run after parse, inside `scrape_article`**, and must return
  `blocked=False`. They are outcomes, not failures.

```
run_id   = new_run_id()                              # local time, legacy format
started  = run_started_at()                          # UTC ISO, for bronze
tasks    = loader.load_tasks("sciencedirect")        # gate 0
manifest = repository.load_for_publisher("sciencedirect")
seen_urls: set[str] = set()
seen_dois: set[str] = set()

for journal_task in tasks:
    fetch journal page (url_type=journal, refresh_days=0)
    issue_ref = discover_issues(...)
    if in-progress: manifest status='skipped_in_progress'; count; continue
    fetch issue page (url_type=issue, refresh_days=0)
    refs = discover_articles(issue_ref)

    # ---- gates 1 + 2, BEFORE the block-recovery loop ----
    todo = []
    for ref in refs:
        assert "?" not in ref.url, "pagination URL must never become a manifest key"
        if ref.url_hash in seen_urls: count("dup_in_run_url"); continue
        seen_urls.add(ref.url_hash)
        fetch, reason = should_fetch(ref, manifest.get(ref.url_hash), ...)
        if not fetch: count(reason); continue
        todo.append(ref)

    # ---- FROZEN block-recovery loop, now over `todo` ----
    while idx < len(todo):
        ... existing timeout / block / restart logic, unchanged ...
        payload = parse_article(html, ctx)
        payload |= await enrich_authors(...)

        # gate 3 — within-run DOI duplicate
        if payload["doi"] and payload["doi"].lower() in seen_dois:
            count("dup_in_run_doi"); log both URLs; idx += 1; continue
        seen_dois.add(...)

        # gate 4 — unchanged payload
        h = payload_hash(payload)
        if row and row.payload_hash == h:
            stage manifest touch; count("unchanged"); idx += 1; continue

        bronze_writer.write(payload, run_id, started)   # BRONZE FIRST
        stage manifest update                            # THEN MANIFEST
        count("written")

    flush manifest between journals
flush bronze; flush manifest; write errors.txt; emit run summary
```

The assertion above is not decoration. `normalize_url` strips pagination query
strings, so a paginated discovery URL normalises to its base. Correct as
identity, dangerous as a fetch key: without the guard, pages 2..N would collapse
onto page 1 and vanish silently.

Counters, emitted in one structured log line at run end:
`input_total`, `dup_in_file`, `skipped_in_progress`, `articles_discovered`,
`dup_in_run_url`, `dup_in_run_doi`, `skipped_manifest`, `fetched`,
`fetch_failed`, `blocked_gave_up`, `parse_failed`, `unchanged_payload`,
`bronze_written`.

**Every discovered article URL must land in exactly one bucket.** Assert the
buckets sum to `articles_discovered` and fail the run loudly if they do not.

### `ingestion/writers/`

- `raw_store.py` — gzip article HTML to
  `data/raw_html/sciencedirect/<YYYY-MM-DD>/<url_hash>.html.gz`. When
  `CAPTURE_FIXTURES=1`, also write an uncompressed copy to
  `tests/fixtures/sciencedirect/html/` so fixtures can be captured from a real
  run and curated by hand.
- `bronze_writer.py` — buffered JSONL append to
  `data/bronze/sciencedirect/ingest_date=YYYY-MM-DD/part-<run_id>.jsonl`. One
  object per line, `ensure_ascii=False`. Each record is the article payload plus
  `run_id`, `run_started_at` (UTC), `scraped_at` (UTC), `payload_hash`,
  `url_hash`, `publisher`, and the `*_rich` segment fields. Flush every
  `bronze_flush_every`; always flush in a `finally`. The Hive-style
  `ingest_date=` partition name is deliberate — Spark reads it natively later.

### `export/journal_json.py`

The regression gate. Reads bronze JSONL for one run, groups by `journal_title`,
reproduces the current per-journal output exactly: 29 keys in current order,
`{"articles": [...]}`, `indent=2`, `ensure_ascii=False`, trailing newline,
`<Journal_Title>_<run_id>.json` under `<output_path>/out/<YYYYMMDD>/`. Reuse
`create_json_file`'s filename sanitisation verbatim. **Excludes `*_rich` and all
provenance fields.** Reuse `richtext._json_text` — do not write a second copy.

### `cli.py` (typer)

```
publisher-lakehouse ingest run --publisher sciencedirect [--force-refetch] [--limit N] [--dry-run]
publisher-lakehouse export json --publisher sciencedirect --run-id <id>
publisher-lakehouse manifest stats --publisher sciencedirect
```

`--dry-run` runs gates 0–2 and reports what it *would* fetch, with no browser
launched and no network call. Requires `[project.scripts]` in `pyproject.toml`.

---

## Tests

No network calls. No browser launched. No live Postgres required by default.

- `test_settings.py` — import purity via audit hook; loading from a **temporary**
  INI, never the operator's real `config/config.ini`; missing-section error.
- `test_normalize_url.py` — case, trailing slash, fragment, `?page=2`, tracking
  query, bookseries path, PII case preserved, bare-vs-`www` host equality,
  idempotence.
- `test_payload_hash.py` — key-order stability, volatile-key exclusion,
  author-order sensitivity.
- `test_richtext.py` — `flatten(to_segments(rich, plain)) == _json_text(rich)`;
  `flatten(to_segments(None, plain)) == plain` with **no warning**; malformed
  input falls back without raising.
- `test_logging.py` — run-id binding, error collection, stderr routing,
  `errors.txt` rendering.
- `test_manifest_policy.py` — fresh row, stale row, `status='failed'`,
  `status='blocked'` (must always re-fetch), `refresh_days=0`,
  `force_refetch=True`, naive `now` raises.
- `test_loader.py` — CSV parsing, `enabled=false`, gate-0 dedup, bookseries row
  survives, 85 rows in → 85 tasks out.
- `test_manifest_repository_pg.py` — **integration**, skipped unless
  `PL_TEST_DATABASE_URL` is set. Points at a **separate** database
  (`publisher_lakehouse_test`), never the working one. Covers: Alembic upgrade
  from empty, `batch_upsert` inserting, `batch_upsert` updating the same
  `url_hash`, and a round trip proving timestamps come back timezone-aware.
- `test_sciencedirect_parse.py` — `parse_article` against each saved HTML
  fixture. Assert specific field values, not just "not empty".
- `test_export_regression.py` — **the most important test.** Load the golden
  JSON files, build the bronze record each article would correspond to, run
  `export/journal_json.py`, compare with `json.loads` equality **and** a byte
  comparison of the serialised output. Any mismatch prints journal, article URL,
  key, expected and actual.

---

## Deliverables

1. Working code as specified.
2. `DEVIATIONS.md`, cumulative, every behaviour change with its reason.
3. `README.md`: what Phase 1 does, how to run it, which stream carries logs,
   what each counter means, and the three-run acceptance test.
4. All tests passing; the Postgres integration test passing when
   `PL_TEST_DATABASE_URL` is set.

## Acceptance criteria

- `ingest run --publisher sciencedirect --limit 3` completes against the real
  site and produces bronze JSONL.
- `export json` over that run produces files **byte-identical** to what
  `CABIACQ.py` produces for the same journals.
- Running `ingest run` **immediately again** re-fetches journal and issue pages
  and fetches **zero** articles; `skipped_manifest` equals the discovered
  article count.
- A third run with `--force-refetch` fetches every article again but writes
  **zero** bronze rows, because payload hashes match.
- Counters sum exactly to `articles_discovered` on all three runs.
- The block-recovery loop still restarts the browser and retries without
  incrementing `idx`. Prove it with a unit test that fakes three consecutive
  blocked results and asserts the same `ArticleRef` is attempted four times.
- `\d ingestion_manifest` in psql shows `timestamp with time zone` on all three
  timestamp columns.

## Work order and review gates

Stop for review after each numbered step marked **STOP**.

1. ✓ **Complete.** Packaging, `settings.py`, `common/` + tests.
   Apply `gate1_fixes.md` before proceeding. **STOP.**
2. `inputs/loader.py` + `scripts/migrate_url_file.py` + tests.
3. `manifest/` (models, Alembic against Postgres, repository, policy) + unit
   tests + the Postgres integration test. **STOP.**
4. `ingestion/browser/` (frozen move) and `sciencedirect/extract.py` (verbatim
   move). `test_sciencedirect_parse.py` must pass before anything below.
5. `ingestion/base.py`, `scraper.py`, `pipeline.py`, writers, `cli.py`. **STOP.**
6. `export/journal_json.py` + `test_export_regression.py`.

Do not skip ahead. Do not write the pipeline before the parse tests are green.
