"""Publisher-neutral bronze article records."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from publisher_lakehouse.common.hashing import payload_hash
from publisher_lakehouse.common.richtext import RichTextSegment


JSON_FIELDS = [
    ("journal_title", "journal_title"),
    ("journal_url", "journal_url"),
    ("volume", "volume"),
    ("issue", "issue"),
    ("issue_url", "issue_url"),
    ("issue_publication_month", "issue_pub_month"),
    ("issue_publication_day", "issue_pub_day"),
    ("issue_publication_year", "issue_pub_year"),
    ("article_url", "article_url"),
    ("article_publication_month", "article_pub_month"),
    ("article_publication_day", "article_pub_day"),
    ("article_publication_year", "article_pub_year"),
    ("start_page", "start_page"),
    ("last_page", "last_page"),
    ("article_id", "article_id"),
    ("english_title", "english_title"),
    ("foreign_title", "foreign_title"),
    ("doi", "doi"),
    ("english_abstract", "english_abstract"),
    ("non_english_abstract", "non_english_abstract"),
    ("article_type", "article_type"),
    ("author_keyword", "author_keyword"),
    ("reference_count", "reference_count"),
    ("copyright_year", "copyright_year"),
    ("license_type", "copyright"),
    ("funding_status", "funding_details"),
]


def _json_contacts(details) -> list[dict]:
    if isinstance(details, str):
        details = [[details, "", ""]] if details else []
    contacts = []
    for name, affiliation, email in details or []:
        contact = {
            "name": (name or "").strip(),
            "affiliation": (affiliation or "").strip().replace("/", "\\"),
            "email": (email or "").strip(),
        }
        if any(contact.values()):
            contacts.append(contact)
    return contacts


class Contact(BaseModel):
    """A named person or organisation attached to an article."""

    name: str
    affiliation: str
    email: str


class BronzeArticle(BaseModel):
    """One immutable article record with content and collection provenance."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    journal_title: str = ""
    journal_url: str = ""
    volume: str = ""
    issue: str = ""
    issue_url: str = ""
    issue_publication_month: str = ""
    issue_publication_day: str = ""
    issue_publication_year: str = ""
    article_url: str = ""
    article_publication_month: str = ""
    article_publication_day: str = ""
    article_publication_year: str = ""
    start_page: str = ""
    last_page: str = ""
    article_id: str = ""
    english_title: str = ""
    foreign_title: str = ""
    doi: str = ""
    english_abstract: str = ""
    non_english_abstract: str = ""
    article_type: str = ""
    author_keyword: str = ""
    reference_count: str = ""
    copyright_year: str = ""
    license_type: str = ""
    funding_status: str = ""

    authors: list[Contact] = Field(default_factory=list)
    editors: list[Contact] = Field(default_factory=list)
    corporate_authors: list[Contact] = Field(default_factory=list)

    english_title_rich: list[RichTextSegment] = Field(default_factory=list)
    foreign_title_rich: list[RichTextSegment] = Field(default_factory=list)
    english_abstract_rich: list[RichTextSegment] = Field(default_factory=list)
    non_english_abstract_rich: list[RichTextSegment] = Field(default_factory=list)

    run_id: str
    scraped_at: datetime
    publisher: str
    url_hash: str
    source_url: str
    payload_hash: str = ""

    @field_validator("scraped_at")
    @classmethod
    def require_utc_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("scraped_at must be timezone-aware UTC")
        if value.utcoffset() != timedelta(0):
            raise ValueError("scraped_at must use UTC")
        return value

    def content_payload(self) -> dict[str, Any]:
        """Return the legacy 29-key article payload without provenance."""

        content = {key: getattr(self, key) for key, _ in JSON_FIELDS}
        content["authors"] = [contact.model_dump() for contact in self.authors]
        content["editors"] = [contact.model_dump() for contact in self.editors]
        content["corporate_authors"] = [
            contact.model_dump() for contact in self.corporate_authors
        ]
        return content

    def with_payload_hash(self) -> BronzeArticle:
        """Return an immutable copy carrying its content-only hash."""

        return self.model_copy(
            update={"payload_hash": payload_hash(self.content_payload())}
        )
