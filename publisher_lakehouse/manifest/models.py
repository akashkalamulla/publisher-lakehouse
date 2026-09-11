"""The ``ingestion_manifest`` table and its in-memory row.

Timestamps are timezone-aware and stored in UTC.  PostgreSQL distinguishes
aware from naive values; SQLite silently does not, so a naive write would pass
a SQLite unit test and then shift every ``should_fetch`` comparison by the
operator's UTC offset in production.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from sqlalchemy import (
    Column,
    DateTime,
    Index,
    Integer,
    MetaData,
    Table,
    Text,
    text,
)


metadata = MetaData()

MANIFEST_TABLE_NAME = "ingestion_manifest"

#: Every terminal state a URL can be left in by a run.
MANIFEST_STATUSES = frozenset(
    {
        "ok",
        "failed",
        "blocked",
        "not_found",
        "parse_error",
        "skipped_in_progress",
    }
)

ingestion_manifest = Table(
    MANIFEST_TABLE_NAME,
    metadata,
    Column("url_hash", Text, primary_key=True),
    Column("normalised_url", Text, nullable=False),
    Column("publisher", Text, nullable=False),
    Column("url_type", Text, nullable=False),
    Column("doi", Text, nullable=True),
    Column("payload_hash", Text, nullable=True),
    Column("first_seen_at", DateTime(timezone=True), nullable=False),
    Column("last_seen_at", DateTime(timezone=True), nullable=False),
    Column("last_changed_at", DateTime(timezone=True), nullable=True),
    Column("fetch_count", Integer, nullable=False, server_default=text("0")),
    Column("last_http_status", Integer, nullable=True),
    Column("last_run_id", Text, nullable=False),
    Column("status", Text, nullable=False),
    Index("ix_ingestion_manifest_publisher_url_type", "publisher", "url_type"),
    Index("ix_ingestion_manifest_publisher_status", "publisher", "status"),
)

MANIFEST_COLUMNS = tuple(column.name for column in ingestion_manifest.columns)


def utc_now() -> datetime:
    """The only clock the manifest is allowed to use."""

    return datetime.now(timezone.utc)


def require_aware(value: datetime, field: str) -> datetime:
    """Reject naive datetimes at the boundary instead of on comparison."""

    if not isinstance(value, datetime):
        raise TypeError(f"{field} must be a datetime, got {type(value).__name__}")
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError(
            f"{field} must be timezone-aware; use datetime.now(timezone.utc)"
        )
    return value


@dataclass(frozen=True, slots=True)
class ManifestRow:
    """One manifest record held in memory for the length of a run."""

    url_hash: str
    normalised_url: str
    publisher: str
    url_type: str
    doi: str | None
    payload_hash: str | None
    first_seen_at: datetime
    last_seen_at: datetime
    last_changed_at: datetime | None
    fetch_count: int
    last_http_status: int | None
    last_run_id: str
    status: str

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> "ManifestRow":
        return cls(**{name: row[name] for name in MANIFEST_COLUMNS})
