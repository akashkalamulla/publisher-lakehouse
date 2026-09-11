# Task: Phase 1 — ScienceDirect ingestion pipeline with deduplication

## Context

This repo (`publisher-lakehouse`) will become a multi-publisher academic metadata
data platform. Today it contains two working scrapers:

- `CABIACQ.py` (~1,986 lines) — **ScienceDirect**. Async, `zendriver` browser
  automation, Cloudflare clearance, block detection with browser restart.
  Reads journal URLs from `urlDetails.txt`, finds each journal's latest issue,
  scrapes every article across all pagination pages, and writes one JSON file
  per journal: `<Journal_Title>_<YYYYMMDDHHMMSS>.json` containing
  `{"articles": [ {...29 keys...} ]}`.
- `cabi_springer.py` (~910 lines) — **Springer**. Sync, `requests` + BeautifulSoup.
  **Out of scope for this task.** Do not touch it.

**ScienceDirect is the pilot.** Phase 1 turns `CABIACQ.py` into a proper
ingestion pipeline with deduplication, without changing a single extracted
value. Springer is Phase 1b and will be added against the same interfaces later.

Later phases (Delta Lake, Spark transforms, star schema, Airflow) are **out of
scope**. Do not start or scaffold them.

Read `CABIACQ.py` fully before writing any code. Read the three sample outputs
in `tests/fixtures/golden/sciencedirect/` to understand the exact target shape.

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
   `None`-vs-`""` behaviour. You may only change a signature where it
   currently reaches for a module-level global.

2. **The browser layer is frozen.** `_solve_cf_on_page`, `start_browser`,
   `_wait_ready`, `_accept_cookies`, `setup_browser_session`, `fetch_soup`,
   `fetch_soup_with_page`, `_arp_api_xhr`, `_arp_api_browser_nav`, all
   `BROWSER_ARGS`, all delay and retry constants, and the block-recovery loop
   in `scrape_journal` move into `ingestion/browser/` **unchanged**. This code
   is the result of trial and error against a live anti-bot system. Do not
   refactor it, do not "clean it up", do not change a timeout.

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

7. **No Spark, no Delta, no Airflow, no MinIO, no FastAPI.** Do not create
   empty `transform/` or `serving/` packages.

---

## Known issues in the current script — handle each as specified

| Issue | Location | Action |
|---|---|---|
| `config.read()` and `folder_path = ...` execute at **import time** | lines 48–63 | **Fix.** Move into `settings.py`. Importing a parse module must have zero side effects — no directory creation, no file reads. This blocks all testing until fixed. |
| Rich text is discarded: `_json_text()` keeps only string fragments, so `english_title_rich` / `foreign_title_rich` never reach the output | `JSON_FIELDS`, `_json_text` | **Additive fix only.** Keep the flat `english_title` etc. exactly as they are. In **bronze only**, add `english_title_rich`, `foreign_title_rich`, `english_abstract_rich` as neutral JSON segments: `[{"text": "...", "bold": true, "italic": false, "superscript": false, "subscript": false}]`. The per-journal export must **not** include these. Record in `DEVIATIONS.md`. |
| `JsonOutput.add_format` builds `xlsxwriter.format.Format` objects purely to keep the extractors happy | line ~1447 | Keep the shim for now so `html_to_rich_text` is untouched. Add a `# TODO` noting it becomes removable once the neutral-segment converter is proven. Do not remove `xlsxwriter` from dependencies yet. |
| Global `error_list` + `errors.txt` written in a module-level `finally` | line 53, bottom of file | **Replace** with structlog JSON events carrying `run_id`. Still write `errors.txt` at run end, rendered from the collected events, same location, same one-error-per-line format. |
| `skip_issues = "In progress"` silently skips a whole journal | `get_latest_issue_url` | **Keep the behaviour**, but record it: manifest row for the journal gets `status='skipped_in_progress'`. Add a `skipped_in_progress` counter. A journal in progress for three months must be distinguishable from one that failed. |
| `DATE` module-level timestamp shared by all journals in a run | line 44 | Becomes `run_id`. The export layer still formats it as `%Y%m%d%H%M%S` for the filename. |
| Commented-out debug lines (`article_list[:3]`, hardcoded pagination test URL) | `scrape_journal` | Delete. `--limit` replaces them. |
| `74URLS.txt` is named 74 but contains **85** URLs, one of which is a **bookseries** (`/bookseries/<slug>/volumes`, not `/journal/<slug>/issues`) | input file | Migrate all 85. The bookseries URL needs its own `url_type` handling and its own test. Do not silently drop it. |

---

## URL model — three tiers

```
journal URL   (input, e.g. https://www.sciencedirect.com/journal/harmful-algae/issues)
    │          also: https://www.sciencedirect.com/bookseries/<slug>/volumes
    └── issue URL   (discovered: latest issue, e.g. .../vol/73/issue/3)
            └── article URL   ← THIS is the deduplication unit
                              e.g. .../science/article/pii/S0019570726001617
```

Journal and issue pages are **discovery** pages. They must be re-fetched every
run or a new issue is never noticed. Articles are the expensive, stable thing
that dedup protects — each one costs a browser navigation, an ARP API call, and
a 3–7 second politeness delay.

The manifest has a `url_type` column (`journal` | `issue` | `article`) and the
refresh policy is looked up per type.

---

## `normalize_url()` — get this right first

Write this and its tests **before** any other module uses it.

ScienceDirect requirements:

- Lowercase scheme and host; force `https`.
- Strip query parameters from article URLs. Issue listing URLs carry a
  meaningful `?page=N` — normalise the **base** issue URL, never the paginated
  variant. The paginated URL is never a manifest key.
- Drop fragments.
- Strip trailing slashes.
- Decode percent-encoding once, consistently.
- Handle both `/journal/<slug>/issues` and `/bookseries/<slug>/volumes`.
- Leave the PII case alone — `S0019570726001617` is case-sensitive in principle;
  do not lowercase the path.

Structure it as a generic function plus a `PUBLISHER_OVERRIDES` dict.
ScienceDirect needs no host rewriting, but **Springer will**: it serves the same
content from `rd.springer.com` and `link.springer.com`, and Phase 1b will add
`rd.springer.com → link.springer.com` to that map. Build the seam now.

`url_hash(url) -> str` is a separate function returning
`sha256(normalised).hexdigest()`.

Unit tests must cover: `http` vs `https`; trailing slash; a fragment; a
`?page=2` issue URL normalising to the same key as its base; an article URL
with a tracking query; the bookseries path shape; PII case preservation; and
idempotence (`normalize(normalize(u)) == normalize(u)`).

---

## `payload_hash()`

Do **not** hash raw HTML. ScienceDirect pages carry JWTs, recommendation
panels and render timestamps, so the raw hash changes on every fetch and the
check never fires.

Hash the parsed payload:

```python
canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
```

Exclude volatile keys before hashing: `run_id`, `scraped_at`, `source_url`,
and the `*_rich` segment fields (they are derived from the flat fields, so
including them only adds noise). Put the excluded-key list in one named constant.

Test: same dict, different key insertion order → same hash. Reordered `authors`
list → **different** hash (author order is meaningful data).

---

## Target structure

Create exactly this. Do not add directories beyond it.

```
publisher-lakehouse/
├── pyproject.toml
├── .env.example
├── .gitignore                           # must ignore data/ and .env
├── README.md
├── DEVIATIONS.md
│
├── config/config.ini
├── inputs/url_details.csv
├── data/                                # gitignored
│   ├── manifest.db
│   ├── raw_html/sciencedirect/<run_date>/<url_hash>.html.gz
│   └── bronze/sciencedirect/ingest_date=YYYY-MM-DD/part-<run_id>.jsonl
├── migrations/                          # alembic
├── scripts/migrate_url_file.py
│
├── publisher_lakehouse/
│   ├── __init__.py
│   ├── settings.py
│   ├── cli.py
│   ├── common/
│   │   ├── urls.py                      # normalize_url, url_hash, PUBLISHER_OVERRIDES
│   │   ├── hashing.py                   # canonical_json, payload_hash
│   │   ├── ids.py                       # new_run_id
│   │   ├── richtext.py                  # xlsxwriter rich list -> neutral segments
│   │   └── logging.py                   # structlog JSON, run_id bound
│   ├── inputs/loader.py
│   ├── manifest/
│   │   ├── models.py
│   │   ├── repository.py
│   │   └── policy.py
│   ├── schemas/article.py
│   ├── ingestion/
│   │   ├── base.py                      # BaseScraper ABC (async)
│   │   ├── sciencedirect/
│   │   │   ├── scraper.py               # implements BaseScraper
│   │   │   └── extract.py               # extraction functions, moved verbatim
│   │   ├── browser/                     # FROZEN — zendriver, CF, block recovery
│   │   ├── pipeline.py                  # run loop, dedup gates, counters
│   │   └── writers/
│   │       ├── raw_store.py
│   │       └── bronze_writer.py
│   └── export/
│       └── journal_json.py              # bronze -> per-journal JSON, regression gate
│
└── tests/
    ├── conftest.py
    ├── test_normalize_url.py
    ├── test_payload_hash.py
    ├── test_richtext.py
    ├── test_manifest_policy.py
    ├── test_loader.py
    ├── test_sciencedirect_parse.py
    ├── test_export_regression.py
    └── fixtures/
        ├── sciencedirect/html/          # captured by the fixture hook
        └── golden/sciencedirect/        # the 3 frozen .json files, already present
```

---

## Module contracts

### `settings.py`

Two config sources, strictly separated.

- `.env` via `pydantic-settings`: `DATABASE_URL`, `DATA_DIR`, `LOG_LEVEL`,
  `CAPTURE_FIXTURES`. Secrets and environment only.
- `config/config.ini` via `configparser` into a pydantic model: operator-tunable
  behaviour only.

```ini
[DETAILS]
output_path = D:\new_test1\

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

Keep `[DETAILS] output_path` — the export layer still writes there.
All browser timing constants (`DELAY_BETWEEN_ARTICLES`, `RETRY_MAX`,
`ARTICLE_TIMEOUT`, `MAX_BLOCK_RETRIES`, `BLOCK_RECOVERY_TIMEOUT`, …) stay as
module constants in `ingestion/browser/`. **Do not move them into config.**
They are tuned against live anti-bot behaviour, not operator preference.

`force_refetch` is **CLI-only**, never a config key.

### `inputs/loader.py`

Reads `inputs/url_details.csv`:

```csv
publisher,url,enabled,note
sciencedirect,https://www.sciencedirect.com/journal/harmful-algae/issues,true,
sciencedirect,https://www.sciencedirect.com/bookseries/advances-in-parasitology/volumes,true,bookseries
```

`scripts/migrate_url_file.py` converts the existing `74URLS.txt` (85 URLs) into
this CSV with `publisher=sciencedirect, enabled=true`, tagging the bookseries
row in `note`. Assert the output row count equals the input line count.

`load_tasks(publisher) -> list[UrlTask]` filters by publisher, drops
`enabled=false`, normalises every URL, applies **gate 0** (dedup within the file,
logging both raw URLs for every drop), and returns
`UrlTask(url_hash, normalised_url, raw_url, publisher, url_type, note)`.

### `common/richtext.py`

`to_segments(rich) -> list[dict]` converts the alternating
`[Format, str, Format, str, ...]` list from `html_to_rich_text` into neutral
JSON segments, reading `bold`, `italic`, `font_script` off each
`xlsxwriter.format.Format`. A plain string input returns a single
unformatted segment. Never raises — on any unexpected shape, fall back to a
single segment carrying `_json_text(rich)` and log a WARNING.

`flatten(segments) -> str` must reproduce `_json_text()` output exactly. Test
this as a round trip against real rich lists from the fixtures.

### `manifest/models.py`

One table, `ingestion_manifest`:

| Column | Type | Notes |
|---|---|---|
| `url_hash` | TEXT PK | from normalised URL |
| `normalised_url` | TEXT | debugging |
| `publisher` | TEXT | indexed with `url_type` |
| `url_type` | TEXT | `journal` / `issue` / `article` |
| `doi` | TEXT NULL | filled after parse |
| `payload_hash` | TEXT NULL | |
| `first_seen_at` | TIMESTAMP | |
| `last_seen_at` | TIMESTAMP | |
| `last_changed_at` | TIMESTAMP NULL | |
| `fetch_count` | INTEGER | |
| `last_http_status` | INTEGER NULL | |
| `last_run_id` | TEXT | |
| `status` | TEXT | `ok` / `failed` / `blocked` / `not_found` / `parse_error` / `skipped_in_progress` |

SQLAlchemy Core, portable types only. **No JSONB, no Postgres-specific types** —
this runs on SQLite today and Postgres later with only a `DATABASE_URL` change.
Alembic from the start.

`repository.py`:
- `load_for_publisher(publisher) -> dict[str, ManifestRow]` — one query at run
  start, held in memory. Never query per URL.
- `batch_upsert(rows)` — dialect-aware `INSERT ... ON CONFLICT DO UPDATE`,
  called every `manifest_flush_every` and once at the end.

Manifest I/O is **synchronous** and must never be called from inside the
per-article async loop. Load once before, flush in batches between journals.

### `manifest/policy.py`

```python
def should_fetch(task, manifest_row, refresh_days, force_refetch, now) -> tuple[bool, str]:
    """Returns (fetch?, reason). Reason is logged and counted."""
```

Skip only when the row exists, `status == 'ok'`, `last_seen_at` is within
`refresh_days`, and `force_refetch` is False. `refresh_days == 0` means always
fetch. `status == 'blocked'` must **always** re-fetch regardless of window — a
block is not a successful collection. Pure function, no I/O, fully unit-tested.

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

The ARP author API is the complication: `extract_author_details` is async and
needs a live page. Split it. `parse_article` handles everything obtainable from
the article HTML alone. Author enrichment stays in the scraper as
`async def enrich_authors(...)`, and its result is merged into the payload by
the scraper, not by `parse_article`. Document this split in `DEVIATIONS.md`.

Springer (Phase 1b) is synchronous and `requests`-based. Keep the ABC async and
let Springer implement the async methods as thin sync wrappers. Do not build two
parallel hierarchies.

### `ingestion/sciencedirect/scraper.py`

`discover_issues` wraps `get_latest_issue_url` + the `"In progress"` skip.
`discover_articles` wraps `get_all_articles_with_pagination` and returns
**absolute article URLs**, not BeautifulSoup `<li>` nodes. The current code
passes soup fragments through several layers, which cannot be serialised,
resumed, or deduplicated. Extract the href and the `article_type` from each node
in one pass and carry a small `ArticleRef(url, url_hash, article_type)` forward.

### `ingestion/pipeline.py` — where the gates go

**Critical.** The block-recovery loop in `scrape_journal` deliberately does not
increment `idx` when an article is blocked. If a dedup skip is mistaken for a
block, the run either loops forever or skips real articles. Therefore:

- **Gates 1 and 2 run before the block-recovery loop**, as a filter over the
  discovered article list. The loop then runs over the already-filtered list and
  its logic is completely untouched.
- **Gates 3 and 4 run after parse, inside `scrape_article`**, and must return
  `blocked=False`. They are outcomes, not failures.

```
run_id   = new_run_id()
tasks    = loader.load_tasks("sciencedirect")      # gate 0
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

        bronze_writer.write(payload, run_id)    # BRONZE FIRST
        stage manifest update                    # THEN MANIFEST
        count("written")

    flush manifest between journals
flush bronze; flush manifest; write errors.txt; emit run summary
```

Counters, emitted in one structured log line at run end:
`input_total`, `dup_in_file`, `skipped_in_progress`, `articles_discovered`,
`dup_in_run_url`, `dup_in_run_doi`, `skipped_manifest`, `fetched`,
`fetch_failed`, `blocked_gave_up`, `parse_failed`, `unchanged_payload`,
`bronze_written`.

**Every discovered article URL must land in exactly one bucket.** Assert the
buckets sum to `articles_discovered` and fail the run loudly if they do not.

### `ingestion/writers/`

- `raw_store.py` — gzip the article HTML to
  `data/raw_html/sciencedirect/<YYYY-MM-DD>/<url_hash>.html.gz`. When
  `CAPTURE_FIXTURES=1`, also write an uncompressed copy to
  `tests/fixtures/sciencedirect/html/` so fixtures can be captured from a real
  run and then curated by hand.
- `bronze_writer.py` — buffered JSONL append to
  `data/bronze/sciencedirect/ingest_date=YYYY-MM-DD/part-<run_id>.jsonl`.
  One object per line, `ensure_ascii=False`. Each record is the article payload
  plus `run_id`, `scraped_at`, `payload_hash`, `url_hash`, `publisher`, and the
  `*_rich` segment fields. Flush every `bronze_flush_every`; always flush in a
  `finally`. The Hive-style `ingest_date=` partition name is deliberate — Spark
  reads it natively in Phase 2.

### `export/journal_json.py`

The regression gate. Reads bronze JSONL for one run, groups by `journal_title`,
and reproduces the current per-journal output exactly: the 29 keys in their
current order, `{"articles": [...]}`, `indent=2`, `ensure_ascii=False`,
trailing newline, `<Journal_Title>_<run_id>.json` under
`<output_path>/out/<YYYYMMDD>/`. Reuse `create_json_file`'s filename sanitisation
verbatim. **Excludes the `*_rich` fields and all provenance fields.**

### `cli.py` (typer)

```
publisher-lakehouse ingest run --publisher sciencedirect [--force-refetch] [--limit N] [--dry-run]
publisher-lakehouse export json --publisher sciencedirect --run-id <id>
publisher-lakehouse manifest stats --publisher sciencedirect
```

`--dry-run` runs gates 0–2 and reports what it *would* fetch, with no browser
launched and no network call.

### `common/logging.py`

`structlog`, JSON to stdout, `run_id` bound globally at run start. Replace every
`print()` in moved code with a structured event. The emoji go; the information
in them stays. Collect ERROR events in memory and render `errors.txt` at run
end, same location and same one-line-per-error format as today.

---

## Tests

No network calls in any test. No browser launched in any test.

- `test_normalize_url.py` — the cases listed above. Write these **first**.
- `test_payload_hash.py` — key-order stability, volatile-key exclusion,
  author-order sensitivity.
- `test_richtext.py` — `flatten(to_segments(x)) == _json_text(x)` for real rich
  lists; plain-string input; malformed input falls back without raising.
- `test_manifest_policy.py` — table-driven: fresh row, stale row,
  `status='failed'`, `status='blocked'` (must always re-fetch),
  `refresh_days=0`, `force_refetch=True`.
- `test_loader.py` — CSV parsing, `enabled=false`, gate-0 dedup, bookseries row
  survives, 85 rows in → 85 tasks out.
- `test_sciencedirect_parse.py` — `parse_article` against each saved HTML
  fixture. Assert specific field values, not just "not empty".
- `test_export_regression.py` — **the most important test.** Load the three
  golden JSON files. For each article object, build the bronze record it would
  correspond to, run `export/journal_json.py` over it, and compare the result to
  the golden file with `json.loads` equality **and** a byte comparison of the
  serialised output. Any mismatch must print the journal, the article URL, the
  key, expected and actual.

---

## Deliverables

1. Working code as specified.
2. `DEVIATIONS.md` listing every behaviour change against the current script,
   with the reason. At minimum: the import-time side-effect fix, the
   `parse_article` / `enrich_authors` split, the `*_rich` bronze addition, the
   `error_list` → structlog change, and the soup-node → `ArticleRef` change.
3. `README.md`: what Phase 1 does, how to run it, what each counter means, and
   the three-run acceptance test.
4. All tests passing.

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

## Work order

Stop for review after steps 1, 3 and 5.

1. `pyproject.toml`, package skeleton, `settings.py` (fixing the import-time
   side effects), `common/` (urls, hashing, ids, richtext, logging) **plus their
   tests**. Stop.
2. `inputs/loader.py` + `scripts/migrate_url_file.py` + tests.
3. `manifest/` (models, Alembic migration, repository, policy) + tests. Stop.
4. `ingestion/browser/` (frozen move, no logic change) and
   `ingestion/sciencedirect/extract.py` (verbatim move).
   `test_sciencedirect_parse.py` must pass before anything below is written.
5. `ingestion/base.py`, `sciencedirect/scraper.py`, `pipeline.py`, writers,
   `cli.py`. Stop.
6. `export/journal_json.py` + `test_export_regression.py`.

Do not skip ahead. Do not write the pipeline before the parse tests are green.
