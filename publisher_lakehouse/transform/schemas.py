"""Declared bronze column types and object-key layout; safe to import on the host."""

from __future__ import annotations

from pathlib import PurePosixPath


CONTACT_DDL = "ARRAY<STRUCT<name: STRING, affiliation: STRING, email: STRING>>"
RICH_TEXT_DDL = (
    "ARRAY<STRUCT<text: STRING, bold: BOOLEAN, italic: BOOLEAN, "
    "superscript: BOOLEAN, subscript: BOOLEAN>>"
)

BRONZE_SOURCE_COLUMNS: list[tuple[str, str]] = [
    ("journal_title", "STRING"),
    ("journal_url", "STRING"),
    ("volume", "STRING"),
    ("issue", "STRING"),
    ("issue_url", "STRING"),
    ("issue_publication_month", "STRING"),
    ("issue_publication_day", "STRING"),
    ("issue_publication_year", "STRING"),
    ("article_url", "STRING"),
    ("article_publication_month", "STRING"),
    ("article_publication_day", "STRING"),
    ("article_publication_year", "STRING"),
    ("start_page", "STRING"),
    ("last_page", "STRING"),
    ("article_id", "STRING"),
    ("english_title", "STRING"),
    ("foreign_title", "STRING"),
    ("doi", "STRING"),
    ("english_abstract", "STRING"),
    ("non_english_abstract", "STRING"),
    ("article_type", "STRING"),
    ("author_keyword", "STRING"),
    ("reference_count", "STRING"),
    ("copyright_year", "STRING"),
    ("license_type", "STRING"),
    ("funding_status", "STRING"),
    ("authors", CONTACT_DDL),
    ("editors", CONTACT_DDL),
    ("corporate_authors", CONTACT_DDL),
    ("english_title_rich", RICH_TEXT_DDL),
    ("foreign_title_rich", RICH_TEXT_DDL),
    ("english_abstract_rich", RICH_TEXT_DDL),
    ("non_english_abstract_rich", RICH_TEXT_DDL),
    ("run_id", "STRING"),
    ("scraped_at", "TIMESTAMP"),
    ("publisher", "STRING"),
    ("url_hash", "STRING"),
    ("source_url", "STRING"),
    ("payload_hash", "STRING"),
]

BRONZE_ADDED_COLUMNS: list[tuple[str, str]] = [
    ("ingest_date", "DATE"),
    ("raw_html_key", "STRING"),
    ("_source_file", "STRING"),
    ("_loaded_at", "TIMESTAMP"),
]

BRONZE_KEY = ("publisher", "url_hash", "run_id")
BRONZE_PARTITIONS = ("publisher", "ingest_date")


def read_ddl() -> str:
    """Spark JSON read schema; timestamp parsing is checked after the read."""

    return ", ".join(
        f"{name} {'STRING' if name == 'scraped_at' else kind}"
        for name, kind in BRONZE_SOURCE_COLUMNS
    )


def table_ddl() -> str:
    """Explicit Delta table schema, including the four load-time columns."""

    return ", ".join(
        f"{name} {kind}" for name, kind in BRONZE_SOURCE_COLUMNS + BRONZE_ADDED_COLUMNS
    )


def raw_html_key(publisher: str, ingest_date: str, url_hash: str) -> str:
    """Return the Phase 1 object key for an article's compressed HTML."""

    return str(
        PurePosixPath(
            "landing", "raw_html", publisher, ingest_date, f"{url_hash}.html.gz"
        )
    )
