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
