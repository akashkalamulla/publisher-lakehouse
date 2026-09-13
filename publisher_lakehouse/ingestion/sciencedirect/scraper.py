"""ScienceDirect browser adapter for the publisher-neutral pipeline."""

from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import urlparse

from xlsxwriter.format import Format

from publisher_lakehouse.ingestion.base import (
    ArticleFetch,
    ArticleProvenance,
    BaseScraper,
    IssueDiscovery,
)
from publisher_lakehouse.ingestion.browser.session import (
    fetch_soup,
    fetch_soup_with_page,
    is_blocked,
    setup_browser_session,
    start_browser,
)
from publisher_lakehouse.ingestion.errors import error_list
from publisher_lakehouse.ingestion.sciencedirect.authors import (
    extract_author_details,
)
from publisher_lakehouse.ingestion.sciencedirect.extract import (
    extract_article_titles,
    extract_article_type,
    extract_main_title,
    get_all_article_list,
    get_latest_issue_url,
    get_total_pages,
)
from publisher_lakehouse.ingestion.sciencedirect.record import (
    build_bronze_article,
)
from publisher_lakehouse.inputs.loader import UrlTask
from publisher_lakehouse.schemas.article import BronzeArticle


class _FormatFactory:
    def add_format(self, properties: dict[str, Any]) -> Format:
        return Format(properties)


async def get_all_articles_with_pagination(
    browser,
    issue_url: str,
    first_soup=None,
) -> list[Any]:
    """Port of the legacy issue-pagination sequence."""

    all_articles = []

    try:
        if first_soup is None:
            first_soup = await fetch_soup(
                browser,
                issue_url,
                wait_id="react-root",
            )
        if first_soup is None:
            return []

        total_pages = get_total_pages(first_soup) or 1
        print(f"[Pagination] Total pages: {total_pages}")

        articles = get_all_article_list(first_soup) or []
        if not articles:
            error_list.append(
                f"PAGINATION PAGE EMPTY | page 1 | {issue_url}"
            )
            print("  Warning: Page 1 returned 0 articles — verify against site")
            refetch = await fetch_soup(
                browser,
                issue_url,
                wait_id="article-results-rhs-primary",
            )
            if refetch:
                articles = get_all_article_list(refetch) or []
                if not articles:
                    error_list.append(
                        "PAGINATION PAGE EMPTY AFTER REFETCH | "
                        f"page 1 | {issue_url}"
                    )
        all_articles.extend(articles)

        if total_pages == 1:
            return all_articles

        for page_num in range(2, total_pages + 1):
            page_url = f"{issue_url}?page={page_num}"
            print(f"[Pagination] Fetching page {page_num}: {page_url}")

            soup = None
            for page_attempt in range(1, 4):
                soup = await fetch_soup(
                    browser,
                    page_url,
                    wait_id="react-root",
                )
                if soup is None:
                    await asyncio.sleep(8)
                    continue

                title_tag = soup.find("title")
                title_str = (
                    title_tag.get_text(strip=True) if title_tag else ""
                )
                if is_blocked(title_str):
                    print(
                        "  Warning: Block detected on page "
                        f"{page_num} attempt {page_attempt}! "
                        f"Title='{title_str}'"
                    )
                    soup = None
                    await asyncio.sleep(8)
                    continue
                break

            if soup is None:
                error_list.append(
                    f"PAGINATION PAGE FAILED | page {page_num} | {page_url}"
                )
                print(
                    f"  Failed to fetch page {page_num} after retries — skipping"
                )
                continue

            articles = get_all_article_list(soup) or []
            if not articles:
                error_list.append(
                    f"PAGINATION PAGE EMPTY | page {page_num} | {page_url}"
                )
                print(
                    f"  Warning: Page {page_num} returned 0 articles "
                    "— verify against site"
                )
            all_articles.extend(articles)

        return all_articles

    except Exception as exc:
        error_list.append(f"get_all_articles_with_pagination | {exc}")
        return all_articles


class ScienceDirectScraper(BaseScraper):
    publisher = "sciencedirect"

    def __init__(self) -> None:
        self._based_url: str | None = None
        self._article_browser = None
        self._article_page = None
        self._article_page_url: str | None = None
        self._format_factory = _FormatFactory()

    async def discover(self, task: UrlTask) -> IssueDiscovery:
        setup_browser = None
        based_url = ""

        try:
            setup_browser = await start_browser()

            main_soup = await fetch_soup(
                setup_browser,
                task.raw_url,
                wait_id="all-issues",
            )
            if main_soup is None:
                raise RuntimeError(
                    f"journal page returned no HTML: {task.raw_url}"
                )

            parsed = urlparse(task.raw_url)
            based_url = f"{parsed.scheme}://{parsed.netloc}"
            self._based_url = based_url
            latest_issue_link = get_latest_issue_url(main_soup)

            if str(latest_issue_link) == "Skip":
                print("Latest issue is In Progress!")
                return IssueDiscovery(
                    based_url=based_url,
                    issue_url="",
                    issue_soup=None,
                    article_nodes=[],
                    in_progress=True,
                )
            if latest_issue_link is None:
                raise RuntimeError(
                    f"latest issue URL was not found: {task.raw_url}"
                )

            latest_issue_url = f"{based_url}{latest_issue_link}"
            issue_soup = await fetch_soup(
                setup_browser,
                latest_issue_url,
                wait_id="react-root",
            )
            if issue_soup is None:
                raise RuntimeError(
                    f"issue page returned no HTML: {latest_issue_url}"
                )

            main_title = extract_main_title(issue_soup)
            print(f"\nScraping started for: [{main_title}]\n")

        finally:
            if setup_browser is not None:
                try:
                    await setup_browser.stop()
                except Exception as stop_err:
                    error_list.append(
                        f"setup_browser.stop failed | {stop_err}"
                    )
                await asyncio.sleep(1.0)

        print(f"Latest issue URL: {latest_issue_url}")
        pagination_browser = await start_browser()
        try:
            article_nodes = await get_all_articles_with_pagination(
                pagination_browser,
                latest_issue_url,
                first_soup=issue_soup,
            )
        finally:
            await pagination_browser.stop()

        return IssueDiscovery(
            based_url=based_url,
            issue_url=latest_issue_url,
            issue_soup=issue_soup,
            article_nodes=article_nodes,
            in_progress=False,
        )

    async def start_article_session(self, issue_url: str) -> None:
        self._article_browser = await start_browser()
        self._article_page = None
        self._article_page_url = None
        await setup_browser_session(self._article_browser, issue_url)

    async def stop_article_session(self) -> None:
        browser = self._article_browser
        self._article_browser = None
        self._article_page = None
        self._article_page_url = None
        if browser is not None:
            try:
                await browser.stop()
            except Exception:
                pass

    async def fetch_article(self, node: Any) -> ArticleFetch:
        article_link_node = node.find(
            "a",
            {
                "class": (
                    "anchor article-content-title u-margin-xs-top "
                    "u-margin-s-bottom anchor-primary"
                )
            },
        )
        if not article_link_node:
            raise ValueError("article listing node has no article title link")

        article_link = article_link_node["href"]
        if self._based_url is None:
            raise RuntimeError("article base URL has not been set by discovery")
        article_url = f"{self._based_url}{article_link}"
        article_type = extract_article_type(node)

        if self._article_browser is None:
            raise RuntimeError("article browser session has not been started")

        article_soup, article_page, article_html = await fetch_soup_with_page(
            self._article_browser,
            article_url,
            wait_id="root",
        )
        if article_soup is None or article_page is None or article_html is None:
            raise RuntimeError(f"article page returned no HTML: {article_url}")

        self._article_page = article_page
        self._article_page_url = article_url

        title_tag = article_soup.find("title")
        title = title_tag.get_text(strip=True) if title_tag else ""
        if is_blocked(title):
            print(f"  Warning: Block detected! Title='{title}'")
            return ArticleFetch(
                article_url=article_url,
                article_soup=article_soup,
                article_html=article_html,
                blocked=True,
                article_type=article_type,
            )

        _, english_title, _, foreign_title = extract_article_titles(
            article_soup,
            self._format_factory,
        )
        blocked = (
            english_title == "Are you a robot?"
            or foreign_title == "Are you a robot?"
        )
        return ArticleFetch(
            article_url=article_url,
            article_soup=article_soup,
            article_html=article_html,
            blocked=blocked,
            article_type=article_type,
        )

    async def build(
        self,
        fetch: ArticleFetch,
        discovery: IssueDiscovery,
        provenance: ArticleProvenance,
    ) -> BronzeArticle:
        if self._article_browser is None:
            raise RuntimeError("article browser session has not been started")

        article_page = (
            self._article_page
            if self._article_page_url == fetch.article_url
            else None
        )
        author_details, corporate_details = await extract_author_details(
            self._article_browser,
            fetch.article_html,
            fetch.article_url,
            page=article_page,
        )

        return build_bronze_article(
            fetch.article_soup,
            self._format_factory,
            provenance.issue_context,
            fetch.article_type,
            author_details,
            corporate_details,
            provenance.run_id,
            provenance.scraped_at,
            provenance.publisher,
            provenance.url_hash,
            provenance.source_url,
            fetch.article_url,
        )
