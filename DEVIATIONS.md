# Phase 1 deviations from `CABIACQ.py`

This file is cumulative and will be updated at each implementation review
gate.

## Implemented at review gate 1

- Configuration and the run timestamp are no longer evaluated while common
  modules are imported. Settings are loaded explicitly at the application
  boundary. This removes the legacy import-time `config.ini` read and frozen
  module-level `DATE` while leaving the legacy script untouched for regression
  comparison.
- The operator config retains this repository's existing
  `C:\CABIACQ_NEW1\` output location. The `D:\new_test1\` value in the brief is
  treated as an illustrative example, not as an instruction to redirect the
  user's existing output.
- Rich-text conversion accepts standalone unformatted strings between
  XlsxWriter format/text pairs. The legacy abstract builder produces this
  hybrid shape; retaining it is necessary to preserve formatting while
  `flatten()` remains identical to the old `_json_text()` behavior.

No extraction or browser behavior has changed at this gate.

## Gate-1 fixes applied

`gate1_fixes.md` is not present in this repository. The three constraints the
Phase 1 v2 brief states as binding on later gates were applied from the brief
itself:

- **`to_segments` now takes the plain text as a fallback.**
  `html_to_rich_text` returns `None` for the rich value whenever the source had
  no formatting *or* produced fewer than four runs, which is the common case.
  That is not an error, so `to_segments(None, plain)` returns one unformatted
  segment and logs **no** warning. Malformed input still falls back with a
  warning, preferring `_json_text(rich)` and using the plain text only when
  that yields nothing. The legacy `< 4 runs` behaviour of `html_to_rich_text`
  itself is untouched.
- **`PUBLISHER_OVERRIDES` maps `sciencedirect.com` to `www.sciencedirect.com`.**
  Both hosts serve the same content, so without the alias the same article has
  two manifest identities and is fetched twice. Phase 1b adds
  `rd.springer.com → link.springer.com`.
- **structlog JSON is written to stderr, not stdout.** The frozen browser layer
  prints human-readable progress to stdout. Anything that reads this run's logs
  programmatically reads stderr.

Two further changes were needed to make those work:

- The log stream is resolved per write rather than captured when
  `configure_logging()` runs. A run that redirects stderr afterwards — or a
  test that captures it — would otherwise write into a stale, possibly closed
  handle.
- `EnvironmentSettings.database_url` has **no default**. It previously
  defaulted to `sqlite:///data/manifest.db`, which is precisely the silent
  SQLite fallback the v2 brief forbids: a broken PostgreSQL connection string
  would have produced a run that looked successful while writing its manifest
  somewhere else.

The gate-1 tests were also split to match the layout in the brief:
`test_settings.py` and `test_logging.py` now exist as their own files instead
of living inside `test_normalize_url.py` and `test_richtext.py`.

## Implemented at review gate 2 — inputs

- `classify_url_type` was added to `common/urls.py` rather than to the loader.
  The pipeline needs the same journal/issue/article classification for URLs it
  discovers, not only for URLs read from the file, and a second copy would be
  a place for the two to drift.
- Unclassifiable URLs **raise** instead of defaulting to a tier. The refresh
  policy is looked up per tier, so a wrong tier either re-fetches articles that
  dedup should have skipped or freezes a discovery page that must be re-read to
  notice a new issue.
- The bookseries URL (`/bookseries/advances-in-parasitology/volumes`) is
  classified as the `journal` tier: it is a discovery page, not an article. Its
  row is tagged `note=bookseries` so its distinct handling stays visible.
- `load_tasks` logs both raw URLs on every gate-0 drop. Colliding rows are
  rarely textually identical — that is the point of normalising — so the
  operator cannot act on the drop without seeing both sides.

## Implemented at review gate 3 — manifest

- **`fetch_count` is incremented database-side**, not assigned. The upsert sets
  `fetch_count = existing + excluded`, so a caller passes `1` for a real fetch
  and `0` for a touch that records an outcome without a network round trip
  (`skipped_in_progress`, or an unchanged payload that was already counted).
  Assigning from the caller would make every staged row responsible for
  remembering the previous count.
- **`doi`, `payload_hash` and `last_changed_at` are coalesced on conflict.** A
  failed or blocked re-fetch has no DOI and no payload hash; writing those
  NULLs would erase the values from the last good collection. `last_http_status`
  is overwritten, because it describes the latest attempt and NULL is
  meaningful there.
- **`first_seen_at` is written once**, on insert, and is absent from the
  conflict assignment.
- `should_fetch` raises on a naive `last_seen_at` as well as on a naive `now`.
  The brief requires the latter; the former is the same failure arriving from
  the database side, and raising beats a `TypeError` from the comparison.
- The skip reason returned by `should_fetch` is the literal string
  `skipped_manifest`, so the pipeline can count the reason it is handed without
  a mapping table. Fetch reasons (`new_url`, `stale`, `retry_blocked`, …) are
  free-form and only logged.
- `ManifestRepository` rejects any dialect other than `postgresql` and
  `sqlite` at construction. The upsert construct is a separate import per
  dialect, so an unsupported dialect must fail immediately rather than at the
  first flush.
- The migration is **hand-written rather than autogenerated**, because the
  PostgreSQL instance could not be reached (see below). A test asserts
  `compare_metadata` finds no drift between `models.py` and the migration, so
  the two cannot diverge unnoticed.
- Tests migrate with Alembic rather than `metadata.create_all`. A schema built
  from the models would hide exactly the drift the migration is supposed to
  carry.

### Not verified at gate 3

The PostgreSQL server on `localhost:5432` is running and reachable, but the
password in `.env` is still the placeholder text from the template, so every
connection is rejected with `password authentication failed for user
"postgres"`. Per the brief this is reported rather than worked around: no
SQLite fallback, no database created, no URL rewritten.

Consequently these remain outstanding:

- the migration has not been **run** against `publisher_lakehouse`;
- `\d ingestion_manifest` output cannot be pasted into this review.

What was verified without a connection: `alembic upgrade head --sql` against
the PostgreSQL dialect emits `TIMESTAMP WITH TIME ZONE` for all three
timestamp columns, both indexes, and the `url_hash` primary key.

## Gate 4 — browser and extraction move

Verified: 44 moved function bodies are byte-identical to `CABIACQ.py`
(AST span comparison, not eyeballing), all 12 browser constants match,
`git diff CABIACQ.py` is empty.

### The editors question is closed

`extract_editor_details` is correct, not broken. Measured against the eight
captured issue pages:

- `bookseries_advances_in_parasitology_vol_131_suppl_c` returns
  David Rollinson, Russell Stothard, Cinzia Cantacessi.
- All seven journal issue pages return `[]`.

ScienceDirect journal issue pages carry no editor block; book series do.
The empty `editors` array across all 54 golden articles is therefore correct
behaviour, not silent extraction failure. `editors` must stay optional in the
record schema and must never be a QA-fail condition. It is an issue-level
value, extracted once per issue and stamped onto every article in it.

### Legacy defects preserved deliberately

These exist in `CABIACQ.py` and are reproduced unchanged. The golden files
encode the buggy output, which is what makes them regression proof.

| Defect | Disposition |
|---|---|
| `extract_reference_count` sits under the browser-required banner but only needs soup | Misfiled comment. Cosmetic. |
| `extract_corporate_author_details` returns `str \| None`, not a list, and swallows exceptions | Normalised in the schema layer by `_json_contacts`. Extractor untouched. |
| Issue publication day is never extracted; the workflow hardcodes `""` | Keep hardcoding `""`. Changing it breaks byte-identical output. Silver-layer backlog. |
| Unsafe DOI fallback dereferencing | Frozen. |
| Potentially unbound `pub_data_txt` | Frozen. |
| Formatting lost when rich text has a single run | Frozen. Present identically in golden output. |
| Over-broad funding sibling traversal | Frozen. |
| Browser fetches can return blocked or partial HTML | **Gate 5b design constraint**: block detection runs before the bronze write; the manifest records `blocked`, never `ok`. `should_fetch` already returns `retry_blocked`. |

## Gate 5a — bronze records and durable writers

### Hash scope

`BronzeArticle.with_payload_hash()` hashes only `content_payload()`: the 29
legacy content keys in their legacy order. Run id, scrape time, publisher,
URL hash, source URL, and all rich-text segment fields are outside that
payload. This keeps re-fetches stable across runs and retains the exclusion
rules established in Gate 1.

Consequently, a formatting-only change is not detected when the flattened
article content remains identical. That is the accepted cost of keeping the
existing hash contract unchanged; the rich segments remain available in the
bronze record for later consumers.

### Boundary decisions

- Extractors can return `None` for missing issue and article values.
  `IssueContext` retains those raw extractor results, while the assembler uses
  the legacy `_json_text` function at the `BronzeArticle` boundary so every
  flat content field is a string.
- `BronzeWriter` opens a run part in append mode. Re-entering a writer for the
  same publisher, date, and run id therefore continues that run instead of
  silently truncating already-flushed records.
- Non-positive writer flush sizes are rejected immediately, matching the
  validation already applied to operator ingestion settings.

### Legacy output quirks retained

The verbatim `_json_contacts` changes every `/` in an affiliation to `\`,
treats a lone string as a name-only contact, removes all-empty contacts, and
assumes every non-string contact row contains exactly three values. The
verbatim `JSON_FIELDS` also exposes `copyright` as `license_type` and
`funding_details` as `funding_status`. These behaviours were not corrected
because the golden records encode them.

## Gate 5b — scraper, pipeline, and CLI

### Pipeline boundary decisions

- `--limit` truncates the deduplicated journal-task list, not the articles in
  each discovered issue. This matches the acceptance command's use of
  `--limit 3` as a deliberately partial coverage run.
- The injected or captured run timestamp is required to be timezone-aware and
  is converted to UTC before deriving `ingest_date` and article provenance.
- The frozen browser API does not expose an HTTP status, so Gate 5b manifest
  rows leave `last_http_status` null rather than inventing `200`.
- The prompt specifies terminal manifest rows for article outcomes, but does
  not specify journal or issue rows. Gate 5b therefore stages article rows
  only. The journal `should_fetch` check honors a pre-existing journal row;
  pipeline-produced state does not create one. This is currently neutral
  because `journal_days` is zero. The issue refresh setting is likewise not a
  gate in the prescribed sequence.

### Frozen-interface workarounds

`BaseScraper` remains limited to its three abstract methods. The concrete
ScienceDirect adapter provides optional article-session start and stop hooks
so the pipeline can preserve the legacy stop / sleep / restart / warm-up
sequence without enlarging the publisher-neutral test-double contract.

`ArticleFetch` has exactly the five requested fields and therefore cannot carry
the browser page used for the ARP request. `ScienceDirectScraper` retains that
page privately and passes it through the existing
`extract_author_details(..., page=page)` parameter. The frozen author function
needed no code or data-shape adaptation, and this avoids a second navigation.

Gate 5a's `BronzeWriter` had no public flush operation, and its configured
buffer can be larger than the manifest batch: the default 200-record bronze
buffer and 100-row manifest batch could persist an `ok` claim before its
corresponding JSONL bytes. Rather than call a private method from production
code, Gate 5b makes one narrowly-scoped addition inside the Gate 5a freeze —
a public `BronzeWriter.flush()` holding the former `_flush()` body, with
`_flush()` kept as an alias for callers written against the older API. The
pipeline calls `flush()` immediately before every manifest `batch_upsert`.
Nothing else in `writers/` changed.

### Block recovery fidelity

The retry loop retains the legacy `while idx < total` control flow, retry
counters and `>` budget checks, monotonic elapsed-time cutoff, non-advancing
blocked index, browser stop / 10-second sleep / restart / session warm-up, and
the randomized inter-article delay. The only structural indirection is that
browser restart and `setup_browser_session` are encapsulated by the concrete
scraper's session-start hook so offline `BaseScraper` fakes need no browser.

### Acceptance-test limits

The three-run live acceptance sequence checks the ordinary write, fresh-
manifest skip, forced re-fetch, unchanged-payload suppression, and Gate 6
byte comparison. It does not exercise Cloudflare retry exhaustion, elapsed
block timeout, fetch timeout/exception recovery, malformed ARP contact rows,
duplicate DOI handling, partial manifest-batch failure, formatting-only rich
text changes, non-default discovery refresh windows, or UTC partition rollover.

## Inline per-journal export

The legacy scraper writes `<Title>_<YYYYMMDDHHMMSS>.json` to `output_path`
as each journal completes. The pipeline writes to
`data/exports/<publisher>/<slug>/<slug>.json` with no timestamp in the
filename — the file is overwritten on each run. Bronze JSONL retains the
full versioned history. The standalone `export run` command remains
available for rebuilding all exports from bronze.
