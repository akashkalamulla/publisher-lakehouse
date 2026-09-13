"""Small publisher-scraper contract used by the ingestion pipeline."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from publisher_lakehouse.inputs.loader import UrlTask
from publisher_lakehouse.schemas.article import BronzeArticle


@dataclass(frozen=True)
class IssueDiscovery:
    based_url: str
    issue_url: str
    issue_soup: Any | None
    article_nodes: list[Any]
    in_progress: bool


@dataclass(frozen=True)
class ArticleFetch:
    article_url: str
    article_soup: Any
    article_html: str
    blocked: bool
    article_type: Any


@dataclass(frozen=True)
class ArticleProvenance:
    run_id: str
    scraped_at: datetime
    publisher: str
    url_hash: str
    source_url: str
    issue_context: Any


class BaseScraper(ABC):
    publisher: str

    @abstractmethod
    async def discover(self, task: UrlTask) -> IssueDiscovery:
        raise NotImplementedError

    @abstractmethod
    async def fetch_article(self, node: Any) -> ArticleFetch:
        raise NotImplementedError

    @abstractmethod
    async def build(
        self,
        fetch: ArticleFetch,
        discovery: IssueDiscovery,
        provenance: ArticleProvenance,
    ) -> BronzeArticle:
        raise NotImplementedError
