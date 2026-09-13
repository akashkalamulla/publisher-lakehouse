"""Per-gate behaviour of the ingestion run policy.

``test_pipeline_manifest.py`` owns the write-ordering guarantees.  This module
owns the four deduplication gates, the block-recovery loop, and the two
non-terminal outcomes, one scenario per test.  Every test also asserts the
counter identity that makes the run auditable: the mutually exclusive article
outcome buckets sum exactly to ``articles_discovered``.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest

import publisher_lakehouse.ingestion.pipeline as pipeline
from publisher_lakehouse.common.urls import normalize_url, url_hash
from publisher_lakehouse.ingestion.base import ArticleProvenance
from publisher_lakehouse.ingestion.browser.constants import MAX_TIMEOUT_RETRIES
from publisher_lakehouse.manifest.models import ManifestRow

# This repository cross-imports test helpers directly rather than growing a
# conftest fixture per collaborator; see test_bronze_record.py.  Importing
# deterministic_pipeline re-registers it as an autouse fixture of this module,
# so the block-retry tests do not sleep ten seconds per retry.
from test_pipeline_manifest import (  # noqa: F401 - deterministic_pipeline is a fixture
    ARTICLE_ONE,
    ARTICLE_TWO,
    BASED_URL,
    NOW,
    PUBLISHER,
    RUN_ID,
    FakeScraper,
    RecordingRepository,
    _fresh_manifest_row,
    _record,
    _run,
    deterministic_pipeline,
)


def _article(index: int) -> str:
    return f"{BASED_URL}/science/article/pii/S{index:016d}"


ARTICLE_THREE = _article(3)
ARTICLE_FOUR = _article(4)
ARTICLE_FIVE = _article(5)
ARTICLE_SIX = _article(6)
ARTICLE_SEVEN = _article(7)

DOI_ONE = "10.1000/example.0001"
DOI_SHARED = "10.1000/example.shared"

FIRST_SEEN = NOW - timedelta(days=400)

#: The mutually exclusive terminal outcomes, spelled out here rather than read
#: from the pipeline so the assertion is independent of the code under test.
ARTICLE_OUTCOMES = (
    "dup_in_run_url",
    "dup_in_run_doi",
    "skipped_manifest",
    "fetch_failed",
    "blocked_gave_up",
    "parse_failed",
    "unchanged_payload",
    "bronze_written",
)


def _assert_counters_sum(counters: pipeline.RunCounters) -> None:
    """Every discovered article must land in exactly one outcome bucket."""

    buckets = {name: getattr(counters, name) for name in ARTICLE_OUTCOMES}
    assert sum(buckets.values()) == counters.articles_discovered, buckets


def _provenance(article_url: str) -> ArticleProvenance:
    """The provenance the pipeline builds for ``article_url`` in this run."""

    return ArticleProvenance(
        run_id=RUN_ID,
        scraped_at=NOW,
        publisher=PUBLISHER,
        url_hash=url_hash(article_url),
        source_url=article_url,
        issue_context=None,
    )


def _payload_hash_for(article_url: str, *, doi: str = "") -> str:
    """The content hash FakeScraper will produce for ``article_url``."""

    return _record(article_url, _provenance(article_url), doi=doi).payload_hash


def _stale_manifest_row(article_url: str, *, payload_hash: str) -> ManifestRow:
    """A prior collection old enough that gate 2 passes it through to gate 4."""

    return replace(
        _fresh_manifest_row(article_url),
        payload_hash=payload_hash,
        first_seen_at=FIRST_SEEN,
        last_seen_at=NOW - timedelta(days=200),
        last_changed_at=NOW - timedelta(days=200),
    )


def _manifest(*rows: ManifestRow) -> dict[str, ManifestRow]:
    return {row.url_hash: row for row in rows}


def _bronze_path(tmp_path: Path) -> Path:
    return (
        tmp_path
        / "bronze"
        / PUBLISHER
        / f"ingest_date={NOW:%Y-%m-%d}"
        / f"part-{RUN_ID}.jsonl"
    )


def _bronze_urls(tmp_path: Path) -> list[str]:
    """The article URLs actually persisted, in write order."""

    path = _bronze_path(tmp_path)
    if not path.is_file():
        return []
    return [
        json.loads(line)["article_url"]
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]


def _rows_by_hash(
    repository: RecordingRepository,
) -> dict[str, dict[str, object]]:
    return {row["url_hash"]: row for row in repository.rows}


def _fetch_count(scraper: FakeScraper, article_url: str) -> int:
    return sum(1 for call in scraper.fetch_calls if call == article_url)


def test_counters_sum_across_every_outcome_in_one_run(tmp_path: Path) -> None:
    unchanged_hash = _payload_hash_for(ARTICLE_FIVE)
    repository = RecordingRepository(
        _manifest(
            # Gate 2: collected today, inside the 90-day article window.
            _fresh_manifest_row(ARTICLE_TWO),
            # Gate 4: old enough to re-fetch, identical once re-built.
            _stale_manifest_row(ARTICLE_FIVE, payload_hash=unchanged_hash),
        )
    )
    scraper = FakeScraper(
        [
            ARTICLE_ONE,
            ARTICLE_ONE,  # gate 1
            ARTICLE_TWO,  # gate 2
            ARTICLE_THREE,
            ARTICLE_FOUR,  # gate 3, shares THREE's DOI
            ARTICLE_FIVE,  # gate 4
            ARTICLE_SIX,  # blocked past its budget
            ARTICLE_SEVEN,  # fetch raises
        ],
        blocked_urls={ARTICLE_SIX},
        fetch_error_urls={ARTICLE_SEVEN},
        dois={
            ARTICLE_ONE: DOI_ONE,
            ARTICLE_THREE: DOI_SHARED,
            ARTICLE_FOUR: DOI_SHARED,
        },
    )

    counters = _run(tmp_path, scraper, repository)

    assert counters.articles_discovered == 8
    assert counters.dup_in_run_url == 1
    assert counters.skipped_manifest == 1
    assert counters.dup_in_run_doi == 1
    assert counters.unchanged_payload == 1
    assert counters.blocked_gave_up == 1
    assert counters.fetch_failed == 1
    assert counters.parse_failed == 0
    assert counters.bronze_written == 2
    assert _bronze_urls(tmp_path) == [ARTICLE_ONE, ARTICLE_THREE]
    _assert_counters_sum(counters)


def test_gate1_duplicate_url_in_one_listing_is_fetched_once(
    tmp_path: Path,
) -> None:
    repository = RecordingRepository()
    scraper = FakeScraper([ARTICLE_ONE, ARTICLE_ONE])

    counters = _run(tmp_path, scraper, repository)

    assert counters.articles_discovered == 2
    assert counters.dup_in_run_url == 1
    assert counters.bronze_written == 1
    # The gate runs before the fetch loop, so the repeat never reaches the net.
    assert scraper.fetch_calls == [ARTICLE_ONE]
    assert scraper.build_calls == [ARTICLE_ONE]
    assert _bronze_urls(tmp_path) == [ARTICLE_ONE]
    assert [row["url_hash"] for row in repository.rows] == [url_hash(ARTICLE_ONE)]
    _assert_counters_sum(counters)


def test_gate2_article_inside_refresh_window_stages_no_manifest_row(
    tmp_path: Path,
) -> None:
    skipped_hash = url_hash(ARTICLE_ONE)
    existing = _fresh_manifest_row(ARTICLE_ONE)
    repository = RecordingRepository(_manifest(existing))
    scraper = FakeScraper([ARTICLE_ONE, ARTICLE_TWO])

    counters = _run(tmp_path, scraper, repository)

    assert counters.articles_discovered == 2
    assert counters.skipped_manifest == 1
    assert scraper.fetch_calls == [ARTICLE_TWO]
    assert scraper.build_calls == [ARTICLE_TWO]

    # A skip that touched last_seen_at would slide the refresh window forward
    # on every run, and this article would never be collected again.
    assert all(row["url_hash"] != skipped_hash for row in repository.rows)
    assert [row["url_hash"] for row in repository.rows] == [url_hash(ARTICLE_TWO)]
    assert _bronze_urls(tmp_path) == [ARTICLE_TWO]
    _assert_counters_sum(counters)


def test_gate3_second_url_with_same_doi_is_claimed_but_not_written(
    tmp_path: Path,
) -> None:
    repository = RecordingRepository()
    scraper = FakeScraper(
        [ARTICLE_ONE, ARTICLE_TWO],
        dois={ARTICLE_ONE: DOI_SHARED, ARTICLE_TWO: DOI_SHARED},
    )

    counters = _run(tmp_path, scraper, repository)

    assert counters.dup_in_run_doi == 1
    assert counters.bronze_written == 1
    # The DOI is only known after the record is built, so both are built.
    assert scraper.build_calls == [ARTICLE_ONE, ARTICLE_TWO]
    assert _bronze_urls(tmp_path) == [ARTICLE_ONE]

    duplicate = _rows_by_hash(repository)[url_hash(ARTICLE_TWO)]
    assert duplicate["status"] == "ok"
    assert duplicate["doi"] == DOI_SHARED
    _assert_counters_sum(counters)

    # An empty DOI carries no identity: two of them are not the same article.
    empty_doi_repository = RecordingRepository()
    empty_doi_scraper = FakeScraper([ARTICLE_THREE, ARTICLE_FOUR])

    empty_doi_counters = _run(tmp_path, empty_doi_scraper, empty_doi_repository)

    assert empty_doi_counters.dup_in_run_doi == 0
    assert empty_doi_counters.bronze_written == 2
    _assert_counters_sum(empty_doi_counters)


def test_gate4_unchanged_payload_writes_no_bronze_row(tmp_path: Path) -> None:
    unchanged_hash = _payload_hash_for(ARTICLE_ONE)
    existing = _stale_manifest_row(ARTICLE_ONE, payload_hash=unchanged_hash)
    repository = RecordingRepository(_manifest(existing))
    scraper = FakeScraper([ARTICLE_ONE])

    counters = _run(tmp_path, scraper, repository)

    assert counters.unchanged_payload == 1
    assert counters.bronze_written == 0
    assert scraper.build_calls == [ARTICLE_ONE]
    assert _bronze_urls(tmp_path) == []

    row = _rows_by_hash(repository)[url_hash(ARTICLE_ONE)]
    assert row["status"] == "ok"
    assert row["payload_hash"] == unchanged_hash
    # changed=False, so the last_changed_at of the earlier collection stands.
    assert row["last_changed_at"] is None
    assert row["first_seen_at"] == FIRST_SEEN
    assert row["last_seen_at"] == NOW
    _assert_counters_sum(counters)


def test_block_budget_exhausted_gives_up_and_advances(tmp_path: Path) -> None:
    repository = RecordingRepository()
    scraper = FakeScraper([ARTICLE_ONE, ARTICLE_TWO], blocked_urls={ARTICLE_ONE})

    counters = _run(tmp_path, scraper, repository)

    assert counters.blocked_gave_up == 1
    # The retry loop keeps its own copy of this budget beside the legacy code.
    assert _fetch_count(scraper, ARTICLE_ONE) == pipeline.MAX_BLOCK_RETRIES + 1
    assert scraper.build_calls == [ARTICLE_TWO]

    blocked_hash = url_hash(ARTICLE_ONE)
    blocked_rows = [row for row in repository.rows if row["url_hash"] == blocked_hash]
    assert [row["status"] for row in blocked_rows] == ["blocked"]
    assert all(row["status"] != "ok" for row in blocked_rows)

    # The loop advanced rather than hanging on the exhausted article.
    assert _rows_by_hash(repository)[url_hash(ARTICLE_TWO)]["status"] == "ok"
    assert counters.bronze_written == 1
    assert _bronze_urls(tmp_path) == [ARTICLE_TWO]
    _assert_counters_sum(counters)


def test_block_then_recovery_within_budget_writes_one_row(
    tmp_path: Path,
) -> None:
    repository = RecordingRepository()
    scraper = FakeScraper([ARTICLE_ONE], transient_blocks={ARTICLE_ONE: 2})

    counters = _run(tmp_path, scraper, repository)

    # Three fetches of one article: idx does not advance while the retry
    # budget is unspent, and the recovered attempt is not a second article.
    assert scraper.fetch_calls == [ARTICLE_ONE, ARTICLE_ONE, ARTICLE_ONE]
    assert scraper.build_calls == [ARTICLE_ONE]
    assert counters.fetched == 1
    assert counters.blocked_gave_up == 0
    assert counters.bronze_written == 1
    assert _bronze_urls(tmp_path) == [ARTICLE_ONE]
    assert _rows_by_hash(repository)[url_hash(ARTICLE_ONE)]["status"] == "ok"
    _assert_counters_sum(counters)


def test_fetch_failure_stages_failed_and_next_article_continues(
    tmp_path: Path,
) -> None:
    repository = RecordingRepository()
    scraper = FakeScraper(
        [ARTICLE_ONE, ARTICLE_TWO],
        fetch_error_urls={ARTICLE_ONE},
    )

    counters = _run(tmp_path, scraper, repository)

    assert counters.fetch_failed == 1
    assert scraper.fetch_calls == [ARTICLE_ONE, ARTICLE_TWO]
    assert scraper.build_calls == [ARTICLE_TWO]

    rows = _rows_by_hash(repository)
    assert rows[url_hash(ARTICLE_ONE)]["status"] == "failed"
    assert rows[url_hash(ARTICLE_ONE)]["normalised_url"] == normalize_url(ARTICLE_ONE)
    assert rows[url_hash(ARTICLE_TWO)]["status"] == "ok"
    assert counters.bronze_written == 1
    assert _bronze_urls(tmp_path) == [ARTICLE_TWO]
    _assert_counters_sum(counters)


def test_fetch_timeout_gives_up_after_max_retries_and_continues(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pipeline, "ARTICLE_TIMEOUT", 0.01)
    repository = RecordingRepository()
    scraper = FakeScraper([ARTICLE_ONE, ARTICLE_TWO], hanging_urls={ARTICLE_ONE})

    counters = _run(tmp_path, scraper, repository)

    assert counters.fetch_failed == 1
    assert _fetch_count(scraper, ARTICLE_ONE) == MAX_TIMEOUT_RETRIES + 1
    assert _rows_by_hash(repository)[url_hash(ARTICLE_ONE)]["status"] == "failed"

    assert counters.bronze_written == 1
    assert _bronze_urls(tmp_path) == [ARTICLE_TWO]
    _assert_counters_sum(counters)


def test_in_progress_issue_discovers_and_fetches_nothing(tmp_path: Path) -> None:
    repository = RecordingRepository()
    scraper = FakeScraper([ARTICLE_ONE, ARTICLE_TWO], in_progress=True)

    counters = _run(tmp_path, scraper, repository)

    assert counters.skipped_in_progress == 1
    # An in-progress issue is not a discovery: its listing is not yet final.
    assert counters.articles_discovered == 0
    assert scraper.fetch_calls == []
    assert scraper.build_calls == []
    assert repository.rows == []
    assert _bronze_urls(tmp_path) == []
    _assert_counters_sum(counters)
