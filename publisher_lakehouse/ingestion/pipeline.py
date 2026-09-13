"""Deduplicating asynchronous ingestion orchestration.

The publisher adapter owns page navigation and extraction.  This module owns
the ordering-sensitive run policy: discovery, pre-loop URL gates, the legacy
block-recovery loop, durable bronze writes, and batched manifest claims.
"""

from __future__ import annotations

import asyncio
import inspect
import random
import time
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import create_engine

from publisher_lakehouse.common.ids import new_run_id
from publisher_lakehouse.common.logging import bind_run_id, get_logger
from publisher_lakehouse.common.urls import normalize_url, url_hash
from publisher_lakehouse.ingestion.base import ArticleProvenance, BaseScraper
from publisher_lakehouse.ingestion.browser.constants import (
    ARTICLE_TIMEOUT,
    BLOCK_RECOVERY_TIMEOUT,
    DELAY_BETWEEN_ARTICLES,
    MAX_TIMEOUT_RETRIES,
)
from publisher_lakehouse.ingestion.errors import error_list
from publisher_lakehouse.ingestion.sciencedirect.record import build_issue_context
from publisher_lakehouse.ingestion.writers.bronze_writer import BronzeWriter
from publisher_lakehouse.ingestion.writers.raw_store import store_raw_html
from publisher_lakehouse.inputs.loader import load_tasks_with_stats
from publisher_lakehouse.manifest.models import require_aware, utc_now
from publisher_lakehouse.manifest.policy import should_fetch
from publisher_lakehouse.manifest.repository import (
    ManifestRepository,
    new_manifest_row,
    stage_rows,
)
from publisher_lakehouse.settings import Settings, load_settings


logger = get_logger(__name__)

# Kept beside the loop, as in the legacy implementation.  This is deliberately
# not operator configuration: it is part of the tuned browser recovery policy.
MAX_BLOCK_RETRIES = 6

_ARTICLE_OUTCOME_FIELDS = (
    "dup_in_run_url",
    "dup_in_run_doi",
    "skipped_manifest",
    "fetch_failed",
    "blocked_gave_up",
    "parse_failed",
    "unchanged_payload",
    "bronze_written",
)


@dataclass(slots=True)
class RunCounters:
    """The exact counters in README's ``Run counters`` table."""

    input_total: int = 0
    dup_in_file: int = 0
    skipped_in_progress: int = 0
    articles_discovered: int = 0
    dup_in_run_url: int = 0
    dup_in_run_doi: int = 0
    skipped_manifest: int = 0
    fetched: int = 0
    fetch_failed: int = 0
    blocked_gave_up: int = 0
    parse_failed: int = 0
    unchanged_payload: int = 0
    bronze_written: int = 0

    @property
    def article_bucket_total(self) -> int:
        """Return the sum of mutually exclusive article outcomes.

        ``fetched`` is an activity counter and deliberately is not an outcome
        bucket: every fetched article subsequently lands in one of the five
        post-fetch terminal outcomes.
        """

        return sum(getattr(self, field) for field in _ARTICLE_OUTCOME_FIELDS)

    def assert_balanced(self) -> None:
        """Fail loudly unless every discovered article has one outcome."""

        if self.article_bucket_total != self.articles_discovered:
            raise RuntimeError(
                "article outcome counters do not sum to articles_discovered: "
                f"{self.article_bucket_total} != {self.articles_discovered}"
            )


@dataclass(frozen=True, slots=True)
class _ArticleCandidate:
    node: Any
    article_url: str
    normalised_url: str
    url_hash: str


def _article_url(discovery: Any, node: Any) -> str:
    """Build the pre-fetch URL using the legacy ScienceDirect expression."""

    # The publisher-neutral contract intentionally leaves listing nodes opaque.
    # ScienceDirect discovery returns BeautifulSoup nodes, so the pipeline must
    # inspect only the link needed by gates 1 and 2 before entering the loop.
    anchor = None
    if hasattr(node, "select_one"):
        anchor = node.select_one("a.anchor.article-content-title")
    if anchor is None and hasattr(node, "find"):
        anchor = node.find("a", {"class": "anchor article-content-title"})
    if anchor is None:
        raise ValueError("article listing node has no article-content-title link")

    article_link = anchor.get("href")
    if not article_link:
        raise ValueError("article-content-title link has no href")
    return f"{discovery.based_url}{article_link}"


async def _call_optional(scraper: BaseScraper, method_name: str, *args: Any) -> None:
    """Call a concrete adapter lifecycle hook without enlarging BaseScraper."""

    method = getattr(scraper, method_name, None)
    if method is None:
        return
    result = method(*args)
    if inspect.isawaitable(result):
        await result


def _flush_bronze(writer: Any) -> None:
    """Make buffered bronze bytes durable before a manifest transaction.

    The writer's configured batch can be larger than the manifest batch, so
    its own boundary cannot be the only thing that makes the bytes durable.
    An injected test writer need not implement ``flush`` at all.
    """

    flush = getattr(writer, "flush", None)
    if flush is not None:
        flush()


async def run_ingestion(
    publisher: str,
    *,
    limit: int | None = None,
    force_refetch: bool = False,
    settings: Settings | None = None,
    scraper: BaseScraper | None = None,
    repository: ManifestRepository | None = None,
    now: datetime | None = None,
    bronze_writer_factory: Callable[..., Any] | None = None,
    raw_html_store: Callable[..., Any] | None = None,
) -> RunCounters:
    """Run one publisher ingestion and return its audited counters.

    Optional collaborators are dependency-injection seams for offline tests.
    Production callers only provide ``publisher``, ``limit``, and
    ``force_refetch``.
    """

    if limit is not None and limit < 0:
        raise ValueError("limit cannot be negative")

    run_id = new_run_id()
    bind_run_id(run_id)
    settings = settings or load_settings()
    loaded_tasks = load_tasks_with_stats(
        publisher,
        settings.operator.paths.url_details,
    )

    owned_engine = None
    if repository is None:
        owned_engine = create_engine(settings.environment.database_url)
        repository = ManifestRepository(owned_engine)

    # The snapshot is intentionally loaded exactly once.  Gate decisions never
    # move during a run merely because another result has just been staged.
    manifest = repository.load_for_publisher(publisher)
    seen_article_urls: set[str] = set()
    seen_dois: dict[str, str] = {}

    if scraper is None:
        if publisher != "sciencedirect":
            raise ValueError(f"No scraper is registered for publisher {publisher!r}")
        from publisher_lakehouse.ingestion.sciencedirect.scraper import (
            ScienceDirectScraper,
        )

        scraper = ScienceDirectScraper()

    run_now = require_aware(now or utc_now(), "now").astimezone(timezone.utc)
    writer_factory = bronze_writer_factory or BronzeWriter
    raw_store = raw_html_store or store_raw_html
    writer = writer_factory(
        bronze_dir=settings.operator.paths.bronze_dir,
        publisher=publisher,
        run_id=run_id,
        ingest_date=run_now.date(),
        flush_every=settings.operator.ingestion.bronze_flush_every,
    )

    counters = RunCounters(
        input_total=loaded_tasks.input_total,
        dup_in_file=loaded_tasks.dup_in_file,
    )
    pending_manifest_rows: list[Mapping[str, Any]] = []
    legacy_error_start = len(error_list)

    def flush_manifest() -> None:
        if not pending_manifest_rows:
            return
        # A manifest row claims that the corresponding bronze bytes are safely
        # stored.  The writer's configured batch can be larger than this batch.
        _flush_bronze(writer)
        repository.batch_upsert(stage_rows(pending_manifest_rows))
        pending_manifest_rows.clear()

    def stage_manifest_row(row: Mapping[str, Any]) -> None:
        pending_manifest_rows.append(row)
        if (
            len(pending_manifest_rows)
            >= settings.operator.ingestion.manifest_flush_every
        ):
            flush_manifest()

    def outcome_row(
        candidate: _ArticleCandidate,
        *,
        status: str,
        doi: str | None = None,
        payload_hash: str | None = None,
        changed: bool = False,
        fetched: bool = True,
    ) -> dict[str, Any]:
        return new_manifest_row(
            url_hash=candidate.url_hash,
            normalised_url=candidate.normalised_url,
            publisher=publisher,
            url_type="article",
            run_id=run_id,
            status=status,
            now=run_now,
            existing=manifest.get(candidate.url_hash),
            doi=doi,
            payload_hash=payload_hash,
            last_http_status=None,
            changed=changed,
            fetched=fetched,
        )

    tasks = loaded_tasks.tasks if limit is None else loaded_tasks.tasks[:limit]

    try:
        with writer:
            for task in tasks:
                fetch_journal, journal_reason = should_fetch(
                    task,
                    manifest.get(task.url_hash),
                    settings.operator.refresh.journal_days,
                    force_refetch,
                    run_now,
                )
                if not fetch_journal:
                    logger.info(
                        "journal_skipped_manifest",
                        publisher=publisher,
                        journal_url=task.raw_url,
                        reason=journal_reason,
                    )
                    continue

                try:
                    discovery = await scraper.discover(task)
                except Exception as exc:
                    error_list.append(
                        f"discover failed | {task.raw_url} | "
                        f"{type(exc).__name__}: {exc}"
                    )
                    continue

                if discovery.in_progress:
                    counters.skipped_in_progress += 1
                    continue

                article_nodes = list(discovery.article_nodes)
                counters.articles_discovered += len(article_nodes)

                try:
                    issue_context = build_issue_context(
                        discovery.issue_soup,
                        journal_url=task.raw_url,
                        issue_url=discovery.issue_url,
                    )
                except Exception as exc:
                    # Context extraction is journal-scoped, but every affected
                    # listing node still needs a terminal article bucket.
                    counters.parse_failed += len(article_nodes)
                    error_list.append(
                        f"build_issue_context failed | {discovery.issue_url} | "
                        f"{type(exc).__name__}: {exc}"
                    )
                    continue

                # Gates 1 and 2 are intentionally complete before the retry
                # loop.  Moving either into it would alter retry counters.
                survivors: list[_ArticleCandidate] = []
                for node in article_nodes:
                    try:
                        article_url = _article_url(discovery, node)
                        normalised_article_url = normalize_url(article_url)
                        article_url_hash = url_hash(article_url)
                    except Exception as exc:
                        counters.parse_failed += 1
                        error_list.append(
                            "article URL extraction failed | "
                            f"{discovery.issue_url} | {type(exc).__name__}: {exc}"
                        )
                        continue

                    candidate = _ArticleCandidate(
                        node=node,
                        article_url=article_url,
                        normalised_url=normalised_article_url,
                        url_hash=article_url_hash,
                    )
                    if article_url_hash in seen_article_urls:
                        counters.dup_in_run_url += 1
                        continue
                    seen_article_urls.add(article_url_hash)

                    fetch_article, article_reason = should_fetch(
                        candidate,
                        manifest.get(article_url_hash),
                        settings.operator.refresh.article_days,
                        force_refetch,
                        run_now,
                    )
                    if not fetch_article:
                        counters.skipped_manifest += 1
                        logger.info(
                            "article_skipped_manifest",
                            publisher=publisher,
                            article_url=normalised_article_url,
                            reason=article_reason,
                        )
                        # Gate 2 is deliberately a read-only decision: never
                        # touch last_seen_at here or the refresh window slides.
                        continue
                    survivors.append(candidate)

                if not survivors:
                    continue

                try:
                    await _call_optional(
                        scraper,
                        "start_article_session",
                        discovery.issue_url,
                    )
                except Exception as exc:
                    error_list.append(
                        f"article browser start failed | {discovery.issue_url} | "
                        f"{type(exc).__name__}: {exc}"
                    )
                    for candidate in survivors:
                        counters.fetch_failed += 1
                        stage_manifest_row(
                            outcome_row(candidate, status="failed", fetched=False)
                        )
                    continue

                try:
                    total = len(survivors)

                    # Structurally retained from scrape_journal lines 1848-1934:
                    # a blocked idx never advances until its budget is spent.
                    idx = 0
                    block_retries = 0
                    timeout_retries = 0
                    block_start_time = None
                    MAX_BLOCK_RETRIES = 6
                    fetched_indices: set[int] = set()
                    while idx < total:
                        candidate = survivors[idx]
                        print(f"[{idx + 1}/{total}] ", end="")

                        if idx not in fetched_indices:
                            counters.fetched += 1
                            fetched_indices.add(idx)

                        try:
                            fetch = await asyncio.wait_for(
                                scraper.fetch_article(candidate.node),
                                timeout=ARTICLE_TIMEOUT,
                            )
                        except asyncio.TimeoutError:
                            timeout_retries += 1
                            error_list.append(
                                f"TIMEOUT article {idx + 1} "
                                f"(attempt {timeout_retries}/{MAX_TIMEOUT_RETRIES}) "
                                f"| {discovery.issue_url}"
                            )
                            print(
                                f"  Timeout on article {idx + 1} - "
                                f"({timeout_retries}/{MAX_TIMEOUT_RETRIES}), "
                                "restarting browser..."
                            )
                            if timeout_retries > MAX_TIMEOUT_RETRIES:
                                error_list.append(
                                    f"GAVE UP article {idx + 1} after "
                                    f"{MAX_TIMEOUT_RETRIES} timeouts | "
                                    f"{discovery.issue_url}"
                                )
                                print(
                                    f"  Gave up on article {idx + 1} after "
                                    f"{MAX_TIMEOUT_RETRIES} timeouts - skipping"
                                )
                                counters.fetch_failed += 1
                                stage_manifest_row(
                                    outcome_row(candidate, status="failed")
                                )
                                idx += 1
                                timeout_retries = 0
                            else:
                                try:
                                    await _call_optional(
                                        scraper,
                                        "stop_article_session",
                                    )
                                except Exception:
                                    pass
                                await asyncio.sleep(10)
                                await _call_optional(
                                    scraper,
                                    "start_article_session",
                                    discovery.issue_url,
                                )
                                print(
                                    f"  Browser ready - retrying article "
                                    f"{idx + 1}..."
                                )
                            continue
                        except Exception as exc:
                            counters.fetch_failed += 1
                            error_list.append(
                                f"fetch article failed | {candidate.article_url} | "
                                f"{type(exc).__name__}: {exc}"
                            )
                            stage_manifest_row(
                                outcome_row(candidate, status="failed")
                            )
                            block_retries = 0
                            block_start_time = None
                            timeout_retries = 0
                            idx += 1
                            continue

                        if fetch.blocked:
                            block_retries += 1
                            if block_retries == 1:
                                block_start_time = time.monotonic()
                            elapsed = time.monotonic() - block_start_time
                            if (
                                block_retries > MAX_BLOCK_RETRIES
                                or elapsed > BLOCK_RECOVERY_TIMEOUT
                            ):
                                reason = (
                                    f"block timeout ({elapsed:.0f}s)"
                                    if elapsed > BLOCK_RECOVERY_TIMEOUT
                                    else f"{MAX_BLOCK_RETRIES} block retries"
                                )
                                error_list.append(
                                    f"giving up on article {idx + 1} after "
                                    f"{reason} | {candidate.node}"
                                )
                                print(
                                    f"  Gave up on article {idx + 1} - {reason}"
                                )
                                counters.blocked_gave_up += 1
                                stage_manifest_row(
                                    outcome_row(candidate, status="blocked")
                                )
                                idx += 1
                                block_retries = 0
                                block_start_time = None
                                continue

                            print(
                                f"  Block on article {idx + 1} "
                                f"(retry {block_retries}/{MAX_BLOCK_RETRIES}, "
                                f"{elapsed:.0f}s elapsed) - restarting browser..."
                            )
                            try:
                                await _call_optional(
                                    scraper,
                                    "stop_article_session",
                                )
                            except Exception:
                                pass
                            await asyncio.sleep(10)
                            await _call_optional(
                                scraper,
                                "start_article_session",
                                discovery.issue_url,
                            )
                            print(
                                f"  Browser ready - retrying article {idx + 1}..."
                            )
                            continue

                        try:
                            provenance = ArticleProvenance(
                                run_id=run_id,
                                scraped_at=run_now,
                                publisher=publisher,
                                url_hash=candidate.url_hash,
                                source_url=candidate.article_url,
                                issue_context=issue_context,
                            )
                            record = await scraper.build(
                                fetch,
                                discovery,
                                provenance,
                            )
                        except Exception as exc:
                            counters.parse_failed += 1
                            error_list.append(
                                f"build article failed | {candidate.article_url} | "
                                f"{type(exc).__name__}: {exc}"
                            )
                            stage_manifest_row(
                                outcome_row(candidate, status="parse_error")
                            )
                        else:
                            doi = record.doi.strip()
                            prior_doi_url_hash = seen_dois.get(doi) if doi else None
                            if (
                                prior_doi_url_hash is not None
                                and prior_doi_url_hash != candidate.url_hash
                            ):
                                counters.dup_in_run_doi += 1
                                stage_manifest_row(
                                    outcome_row(
                                        candidate,
                                        status="ok",
                                        doi=doi,
                                        payload_hash=record.payload_hash,
                                    )
                                )
                            else:
                                if doi:
                                    seen_dois[doi] = candidate.url_hash

                                existing = manifest.get(candidate.url_hash)
                                if (
                                    existing is not None
                                    and existing.payload_hash
                                    == record.payload_hash
                                ):
                                    counters.unchanged_payload += 1
                                    stage_manifest_row(
                                        outcome_row(
                                            candidate,
                                            status="ok",
                                            doi=doi or None,
                                            payload_hash=record.payload_hash,
                                            changed=False,
                                        )
                                    )
                                else:
                                    try:
                                        raw_store(
                                            fetch.article_html,
                                            raw_html_dir=(
                                                settings.operator.paths.raw_html_dir
                                            ),
                                            publisher=publisher,
                                            run_date=run_now.date(),
                                            url_hash=candidate.url_hash,
                                        )
                                        writer.write(record)
                                    except Exception as exc:
                                        counters.parse_failed += 1
                                        error_list.append(
                                            "store article failed | "
                                            f"{candidate.article_url} | "
                                            f"{type(exc).__name__}: {exc}"
                                        )
                                        stage_manifest_row(
                                            outcome_row(
                                                candidate,
                                                status="parse_error",
                                            )
                                        )
                                    else:
                                        counters.bronze_written += 1
                                        # This successful manifest claim is
                                        # staged only after the bronze write.
                                        stage_manifest_row(
                                            outcome_row(
                                                candidate,
                                                status="ok",
                                                doi=doi or None,
                                                payload_hash=record.payload_hash,
                                                changed=(
                                                    existing is None
                                                    or existing.payload_hash
                                                    != record.payload_hash
                                                ),
                                            )
                                        )

                        block_retries = 0
                        block_start_time = None
                        timeout_retries = 0
                        idx += 1
                        if idx < total:
                            delay = random.uniform(*DELAY_BETWEEN_ARTICLES)
                            await asyncio.sleep(delay)

                finally:
                    try:
                        await _call_optional(scraper, "stop_article_session")
                    except Exception:
                        pass

            # Close the run with every partial batch accounted for.  Bronze is
            # flushed first inside flush_manifest even though its own context
            # manager would also flush on exit.
            flush_manifest()

        counters.assert_balanced()
        logger.info("ingestion_run_complete", **asdict(counters))
        return counters
    finally:
        for legacy_error in error_list[legacy_error_start:]:
            logger.error("legacy_error", error_line=legacy_error)
        if owned_engine is not None:
            owned_engine.dispose()
