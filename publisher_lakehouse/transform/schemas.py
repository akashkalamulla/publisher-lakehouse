"""Declared lakehouse schemas and parsing constants; safe to import on the host."""

from __future__ import annotations

from calendar import month_abbr, month_name
from dataclasses import dataclass
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


SILVER_KEY = ("publisher", "url_hash")
SILVER_PARTITIONS = ("publisher",)
QUARANTINE_KEY = BRONZE_KEY

MONTH_LOOKUP: dict[str, int] = {
    **{month_name[number].lower(): number for number in range(1, 13)},
    **{month_abbr[number].lower(): number for number in range(1, 13)},
}
CC_LICENSE_PATTERN = (
    r"(?i)^https?://creativecommons\.org/licenses/"
    r"(by(?:-(?:nc|nd|sa)){0,2})/(\d+(?:\.\d+)?)/?$"
)

HARD_RULES = (
    "missing_article_url",
    "missing_journal_url",
    "missing_title",
)
SOFT_RULES = (
    "missing_doi",
    "invalid_doi",
    "no_authors",
    "missing_abstract",
    "unparsed_pub_month",
    "month_range",
    "bad_reference_count",
    "bad_copyright_year",
    "bad_page_range",
)
DQ_RULES = tuple((name, "quarantine") for name in HARD_RULES) + tuple(
    (name, "flag") for name in SOFT_RULES
)

SILVER_COLUMNS: list[tuple[str, str]] = [
    ("publisher", "STRING"),
    ("url_hash", "STRING"),
    ("article_url", "STRING"),
    ("source_url", "STRING"),
    ("journal_url", "STRING"),
    ("journal_title", "STRING"),
    ("issue_url", "STRING"),
    ("volume", "STRING"),
    ("issue", "STRING"),
    ("volume_num", "INT"),
    ("issue_num", "INT"),
    ("english_title", "STRING"),
    ("foreign_title", "STRING"),
    ("english_abstract", "STRING"),
    ("non_english_abstract", "STRING"),
    ("doi", "STRING"),
    ("doi_valid", "BOOLEAN"),
    ("article_type", "STRING"),
    ("article_type_key", "STRING"),
    ("keywords", "ARRAY<STRING>"),
    ("reference_count", "INT"),
    ("copyright_year", "INT"),
    ("start_page", "STRING"),
    ("last_page", "STRING"),
    ("page_count", "INT"),
    ("article_pub_year", "INT"),
    ("article_pub_month", "INT"),
    ("article_pub_day", "INT"),
    ("article_date_precision", "STRING"),
    ("article_pub_month_start", "DATE"),
    ("issue_pub_year", "INT"),
    ("issue_pub_month", "INT"),
    ("issue_pub_day", "INT"),
    ("issue_date_precision", "STRING"),
    ("issue_pub_month_start", "DATE"),
    ("license_url", "STRING"),
    ("license_code", "STRING"),
    ("has_cc_license", "BOOLEAN"),
    ("funding_statement", "STRING"),
    ("authors", CONTACT_DDL),
    ("editors", CONTACT_DDL),
    ("corporate_authors", CONTACT_DDL),
    ("author_count", "INT"),
    ("first_seen_at", "TIMESTAMP"),
    ("last_changed_at", "TIMESTAMP"),
    ("version_count", "INT"),
    ("latest_run_id", "STRING"),
    ("payload_hash", "STRING"),
    ("raw_html_key", "STRING"),
    ("ingest_date", "DATE"),
    ("dq_flags", "ARRAY<STRING>"),
    ("_row_hash", "STRING"),
    ("_silver_updated_at", "TIMESTAMP"),
]
SILVER_BUSINESS_COLUMNS = tuple(
    name for name, _ in SILVER_COLUMNS if name not in {"_row_hash", "_silver_updated_at"}
)

QUARANTINE_COLUMNS: list[tuple[str, str]] = [
    ("publisher", "STRING"),
    ("url_hash", "STRING"),
    ("run_id", "STRING"),
    ("article_url", "STRING"),
    ("journal_url", "STRING"),
    ("english_title", "STRING"),
    ("scraped_at", "TIMESTAMP"),
    ("dq_reasons", "ARRAY<STRING>"),
    ("_quarantined_at", "TIMESTAMP"),
]

DQ_RESULT_COLUMNS: list[tuple[str, str]] = [
    ("dq_run_id", "STRING"),
    ("checked_at", "TIMESTAMP"),
    ("publisher", "STRING"),
    ("table_name", "STRING"),
    ("rule", "STRING"),
    ("severity", "STRING"),
    ("failed_rows", "BIGINT"),
    ("total_rows", "BIGINT"),
]


def silver_ddl() -> str:
    return ", ".join(f"{name} {kind}" for name, kind in SILVER_COLUMNS)


def quarantine_ddl() -> str:
    return ", ".join(f"{name} {kind}" for name, kind in QUARANTINE_COLUMNS)


def dq_ddl() -> str:
    return ", ".join(f"{name} {kind}" for name, kind in DQ_RESULT_COLUMNS)


@dataclass(frozen=True)
class GoldTableSpec:
    columns: tuple[tuple[str, str], ...]
    surrogate_key: str | None
    natural_key: tuple[str, ...]
    partitions: tuple[str, ...] = ()
    foreign_keys: tuple[tuple[str, str], ...] = ()


GOLD_SPECS: dict[str, GoldTableSpec] = {
    "dim_date": GoldTableSpec(
        columns=(
            ("date_sk", "INT"), ("date", "DATE"), ("year", "INT"),
            ("quarter", "INT"), ("month", "INT"), ("month_name", "STRING"),
            ("day", "INT"), ("day_of_week", "INT"), ("iso_week", "INT"),
            ("is_month_start", "BOOLEAN"),
        ),
        surrogate_key="date_sk", natural_key=("date",),
    ),
    "dim_article_type": GoldTableSpec(
        columns=(
            ("article_type_sk", "BIGINT"), ("article_type_key", "STRING"),
            ("article_type", "STRING"), ("content_class", "STRING"),
            ("is_scholarly", "BOOLEAN"),
        ),
        surrogate_key="article_type_sk", natural_key=("article_type_key",),
    ),
    "dim_journal": GoldTableSpec(
        columns=(
            ("journal_sk", "BIGINT"), ("publisher", "STRING"),
            ("journal_url", "STRING"), ("journal_title", "STRING"),
            ("effective_from", "TIMESTAMP"), ("effective_to", "TIMESTAMP"),
            ("is_current", "BOOLEAN"), ("_attr_hash", "STRING"),
        ),
        surrogate_key="journal_sk", natural_key=("publisher", "journal_url"),
    ),
    "dim_issue": GoldTableSpec(
        columns=(
            ("issue_sk", "BIGINT"), ("publisher", "STRING"),
            ("issue_url", "STRING"), ("journal_url", "STRING"),
            ("volume", "STRING"), ("issue", "STRING"),
            ("volume_num", "INT"), ("issue_num", "INT"),
            ("issue_pub_year", "INT"), ("issue_pub_month", "INT"),
            ("issue_pub_day", "INT"), ("issue_date_precision", "STRING"),
            ("issue_pub_month_start", "DATE"),
        ),
        surrogate_key="issue_sk", natural_key=("publisher", "issue_url"),
    ),
    "dim_author": GoldTableSpec(
        columns=(
            ("author_sk", "BIGINT"), ("author_nk", "STRING"),
            ("display_name", "STRING"), ("latest_affiliation", "STRING"),
            ("latest_email", "STRING"), ("first_seen_at", "TIMESTAMP"),
        ),
        surrogate_key="author_sk", natural_key=("author_nk",),
    ),
    "fact_article": GoldTableSpec(
        columns=(
            ("article_sk", "BIGINT"), ("journal_sk", "BIGINT"),
            ("issue_sk", "BIGINT"), ("article_type_sk", "BIGINT"),
            ("pub_month_date_sk", "INT"), ("first_seen_date_sk", "INT"),
            ("article_count", "INT"), ("reference_count", "INT"),
            ("page_count", "INT"), ("author_count", "INT"),
            ("keyword_count", "INT"), ("version_count", "INT"),
            ("dq_flag_count", "INT"), ("publisher", "STRING"),
            ("url_hash", "STRING"), ("doi", "STRING"),
            ("english_title", "STRING"), ("article_url", "STRING"),
            ("pub_year", "INT"), ("pub_date_precision", "STRING"),
            ("license_code", "STRING"), ("has_cc_license", "BOOLEAN"),
            ("_row_hash", "STRING"),
        ),
        surrogate_key="article_sk", natural_key=("publisher", "url_hash"),
        partitions=("publisher",),
        foreign_keys=(
            ("journal_sk", "dim_journal"), ("issue_sk", "dim_issue"),
            ("article_type_sk", "dim_article_type"),
            ("pub_month_date_sk", "dim_date"),
            ("first_seen_date_sk", "dim_date"),
        ),
    ),
    "bridge_article_author": GoldTableSpec(
        columns=(
            ("article_sk", "BIGINT"), ("author_sk", "BIGINT"),
            ("role", "STRING"), ("author_position", "INT"),
            ("affiliation_at_publication", "STRING"),
            ("email_at_publication", "STRING"), ("weight", "DOUBLE"),
            ("publisher", "STRING"), ("_row_hash", "STRING"),
        ),
        surrogate_key=None,
        natural_key=("article_sk", "role", "author_position"),
        partitions=("publisher",),
        foreign_keys=(("article_sk", "fact_article"), ("author_sk", "dim_author")),
    ),
}

GOLD_BUILD_ORDER = tuple(GOLD_SPECS)


def gold_ddl(table_name: str) -> str:
    return ", ".join(
        f"{name} {kind}" for name, kind in GOLD_SPECS[table_name].columns
    )
