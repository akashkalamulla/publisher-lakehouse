from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import publisher_lakehouse.ingestion.pipeline as pipeline
from publisher_lakehouse.common.urls import normalize_url, url_hash
from publisher_lakehouse.ingestion.base import (
    ArticleFetch,
    ArticleProvenance,
    IssueDiscovery,
)
from publisher_lakehouse.manifest.models import ManifestRow
from publisher_lakehouse.schemas.article import BronzeArticle


NOW = datetime(2026, 9, 13, 12, tzinfo=timezone.utc)
RUN_ID = "20260913120000"
PUBLISHER = "sciencedirect"
BASED_URL = "https://www.sciencedirect.com"
JOURNAL_TITLE = "Example Journal"
JOURNAL_URL = f"{BASED_URL}/journal/example/issues"
ISSUE_URL = f"{BASED_URL}/journal/example/vol/1/issue/1"
ARTICLE_ONE = f"{BASED_URL}/science/article/pii/S0000000000000001"
ARTICLE_TWO = f"{BASED_URL}/science/article/pii/S0000000000000002"


@dataclass(frozen=True)
class FakeNode:
    article_url: str

    def select_one(self, selector: str):
        assert selector == "a.anchor.article-content-title"
        return self

    def get(self, key: str):
        assert key == "href"
        return self.article_url.removeprefix(BASED_URL)


class FakeScraper:
    publisher = PUBLISHER

    def __init__(
        self,
        article_urls: list[str],
        *,
        blocked_urls: set[str] | None = None,
        build_error_urls: set[str] | None = None,
        transient_blocks: dict[str, int] | None = None,
        fetch_error_urls: set[str] | None = None,
        hanging_urls: set[str] | None = None,
        dois: dict[str, str] | None = None,
        in_progress: bool = False,
    ) -> None:
        self.article_urls = article_urls
        self.blocked_urls = blocked_urls or set()
        self.build_error_urls = build_error_urls or set()
        # ``blocked_urls`` is permanent.  ``transient_blocks`` is a per-URL
        # budget of blocked responses that runs out, so the recovery path can
        # be exercised without also exercising the give-up path.
        self.transient_blocks = dict(transient_blocks or {})
        self.fetch_error_urls = fetch_error_urls or set()
        self.hanging_urls = hanging_urls or set()
        self.dois = dict(dois or {})
        self.in_progress = in_progress
        self.fetch_calls: list[str] = []
        self.build_calls: list[str] = []

    async def discover(self, task) -> IssueDiscovery:
        return IssueDiscovery(
            based_url=BASED_URL,
            issue_url=ISSUE_URL,
            issue_soup=object(),
            article_nodes=[FakeNode(url) for url in self.article_urls],
            in_progress=self.in_progress,
        )

    async def fetch_article(self, node: FakeNode) -> ArticleFetch:
        article_url = node.article_url
        self.fetch_calls.append(article_url)

        if article_url in self.fetch_error_urls:
            raise RuntimeError("fixture fetch failure")
        if article_url in self.hanging_urls:
            # Never returns: the pipeline's asyncio.wait_for cancels this.
            await asyncio.Event().wait()

        blocked = article_url in self.blocked_urls
        if not blocked and self.transient_blocks.get(article_url, 0) > 0:
            self.transient_blocks[article_url] -= 1
            blocked = True

        return ArticleFetch(
            article_url=article_url,
            article_soup=object(),
            article_html=f"<html>{article_url}</html>",
            blocked=blocked,
            article_type="Research article",
        )

    async def build(
        self,
        fetch: ArticleFetch,
        discovery: IssueDiscovery,
        provenance: ArticleProvenance,
    ) -> BronzeArticle:
        self.build_calls.append(fetch.article_url)
        if fetch.article_url in self.build_error_urls:
            raise ValueError("fixture parse failure")
        return _record(
            fetch.article_url,
            provenance,
            doi=self.dois.get(fetch.article_url, ""),
        )


class RecordingRepository:
    def __init__(
        self,
        manifest: dict[str, ManifestRow] | None = None,
    ) -> None:
        self.manifest = manifest or {}
        self.batches: list[list[dict[str, object]]] = []

    def load_for_publisher(self, publisher: str) -> dict[str, ManifestRow]:
        assert publisher == PUBLISHER
        return dict(self.manifest)

    def batch_upsert(self, rows) -> int:
        batch = [dict(row) for row in rows]
        self.batches.append(batch)
        return len(batch)

    @property
    def rows(self) -> list[dict[str, object]]:
        return [row for batch in self.batches for row in batch]


@pytest.fixture(autouse=True)
def deterministic_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    async def no_sleep(delay: float) -> None:
        return None

    monkeypatch.setattr(pipeline.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(
        pipeline,
        "new_run_id",
        lambda *args, **kwargs: RUN_ID,
    )
    monkeypatch.setattr(
        pipeline,
        "build_issue_context",
        lambda *args, **kwargs: SimpleNamespace(
            journal_title=JOURNAL_TITLE,
            journal_url=JOURNAL_URL,
            volume="1",
            issue="1",
            issue_publication_year="2026",
            issue_publication_month="September",
        ),
    )


def _settings(
    tmp_path: Path,
    *,
    bronze_flush_every: int = 50,
    manifest_flush_every: int = 50,
):
    input_path = tmp_path / "urls.csv"
    input_path.write_text(
        "publisher,url,enabled,note\n"
        f"{PUBLISHER},{JOURNAL_URL},true,fixture\n",
        encoding="utf-8",
    )
    return SimpleNamespace(
        environment=SimpleNamespace(database_url="unused"),
        operator=SimpleNamespace(
            paths=SimpleNamespace(
                url_details=input_path,
                raw_html_dir=tmp_path / "raw",
                bronze_dir=tmp_path / "bronze",
                export_dir=tmp_path / "exports",
            ),
            ingestion=SimpleNamespace(
                bronze_flush_every=bronze_flush_every,
                manifest_flush_every=manifest_flush_every,
            ),
            refresh=SimpleNamespace(
                journal_days=0,
                issue_days=0,
                article_days=90,
            ),
        ),
    )


def _record(
    article_url: str,
    provenance: ArticleProvenance,
    *,
    doi: str = "",
    journal_title: str = JOURNAL_TITLE,
    journal_url: str = JOURNAL_URL,
) -> BronzeArticle:
    return BronzeArticle(
        journal_title=journal_title,
        journal_url=journal_url,
        article_url=article_url,
        english_title=f"Record for {article_url.rsplit('/', 1)[-1]}",
        article_type="Research article",
        doi=doi,
        run_id=provenance.run_id,
        scraped_at=provenance.scraped_at,
        publisher=provenance.publisher,
        url_hash=provenance.url_hash,
        source_url=provenance.source_url,
    ).with_payload_hash()


def _fresh_manifest_row(article_url: str) -> ManifestRow:
    identity = url_hash(article_url)
    return ManifestRow(
        url_hash=identity,
        normalised_url=normalize_url(article_url),
        publisher=PUBLISHER,
        url_type="article",
        doi=None,
        payload_hash="existing-payload-hash",
        first_seen_at=NOW,
        last_seen_at=NOW,
        last_changed_at=NOW,
        fetch_count=1,
        last_http_status=200,
        last_run_id="20260913110000",
        status="ok",
    )


def _run(
    tmp_path: Path,
    scraper: FakeScraper,
    repository: RecordingRepository,
    *,
    force_refetch: bool = False,
    bronze_flush_every: int = 50,
    manifest_flush_every: int = 50,
):
    return asyncio.run(
        pipeline.run_ingestion(
            PUBLISHER,
            force_refetch=force_refetch,
            settings=_settings(
                tmp_path,
                bronze_flush_every=bronze_flush_every,
                manifest_flush_every=manifest_flush_every,
            ),
            scraper=scraper,
            repository=repository,
            now=NOW,
        )
    )


def test_bronze_is_durable_before_manifest_batch_is_staged(
    tmp_path: Path,
) -> None:
    class RepositoryAbort(RuntimeError):
        pass

    class InspectingRepository(RecordingRepository):
        checked_at_call_time = False

        def batch_upsert(self, rows) -> int:
            batch = [dict(row) for row in rows]
            assert [row["status"] for row in batch] == ["ok"]

            bronze_path = (
                tmp_path
                / "bronze"
                / PUBLISHER
                / f"ingest_date={NOW:%Y-%m-%d}"
                / f"part-{RUN_ID}.jsonl"
            )
            assert bronze_path.is_file()
            payload = bronze_path.read_bytes()
            assert payload.endswith(b"\n")
            stored = json.loads(payload.decode("utf-8"))
            assert stored["article_url"] == ARTICLE_ONE

            self.checked_at_call_time = True
            raise RepositoryAbort("stop after ordering assertion")

    repository = InspectingRepository()

    with pytest.raises(RepositoryAbort, match="ordering assertion"):
        _run(
            tmp_path,
            FakeScraper([ARTICLE_ONE]),
            repository,
            bronze_flush_every=100,
            manifest_flush_every=1,
        )

    assert repository.checked_at_call_time


def test_blocked_article_never_stages_ok(tmp_path: Path) -> None:
    scraper = FakeScraper(
        [ARTICLE_ONE],
        blocked_urls={ARTICLE_ONE},
    )
    repository = RecordingRepository()

    counters = _run(tmp_path, scraper, repository)

    assert counters.blocked_gave_up == 1
    assert counters.bronze_written == 0
    assert scraper.build_calls == []
    assert len(scraper.fetch_calls) > 1
    assert [row["status"] for row in repository.rows] == ["blocked"]
    assert all(row["status"] != "ok" for row in repository.rows)


def test_build_error_stages_parse_error_and_next_article_continues(
    tmp_path: Path,
) -> None:
    scraper = FakeScraper(
        [ARTICLE_ONE, ARTICLE_TWO],
        build_error_urls={ARTICLE_ONE},
    )
    repository = RecordingRepository()

    counters = _run(tmp_path, scraper, repository)

    rows_by_url = {row["normalised_url"]: row for row in repository.rows}
    assert rows_by_url[normalize_url(ARTICLE_ONE)]["status"] == "parse_error"
    assert rows_by_url[normalize_url(ARTICLE_TWO)]["status"] == "ok"
    assert scraper.fetch_calls == [ARTICLE_ONE, ARTICLE_TWO]
    assert scraper.build_calls == [ARTICLE_ONE, ARTICLE_TWO]
    assert counters.parse_failed == 1
    assert counters.bronze_written == 1
    assert counters.article_bucket_total == counters.articles_discovered == 2


def test_force_refetch_fetches_fresh_manifest_url(tmp_path: Path) -> None:
    existing = _fresh_manifest_row(ARTICLE_ONE)
    repository = RecordingRepository({existing.url_hash: existing})
    scraper = FakeScraper([ARTICLE_ONE])

    counters = _run(
        tmp_path,
        scraper,
        repository,
        force_refetch=True,
    )

    assert scraper.fetch_calls == [ARTICLE_ONE]
    assert counters.fetched == 1
    assert counters.skipped_manifest == 0
