from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from publisher_lakehouse.common.richtext import _json_text, to_segments
from publisher_lakehouse.ingestion.errors import _log_missing
from publisher_lakehouse.ingestion.sciencedirect.extract import (
    extract_abstract,
    extract_article_data,
    extract_article_titles,
    extract_copyright,
    extract_copyright_year,
    extract_corporate_author_details,
    extract_doi,
    extract_editor_details,
    extract_funding_details,
    extract_issue_volume_pubdate,
    extract_keywords,
    extract_main_title,
    extract_reference_count,
)
from publisher_lakehouse.schemas.article import (
    BronzeArticle,
    Contact,
    _json_contacts,
)


@dataclass(frozen=True)
class IssueContext:
    journal_title: str | None
    journal_url: str
    volume: str | None
    issue: str | None
    issue_url: str
    issue_publication_month: str | None
    issue_publication_day: str
    issue_publication_year: str | None
    editors: Any


def build_issue_context(issue_soup, journal_url, issue_url) -> IssueContext:
    journal_title = extract_main_title(issue_soup)
    volume, issue, publication_year, publication_month = (
        extract_issue_volume_pubdate(issue_soup, issue_url)
    )
    editors = extract_editor_details(issue_soup)

    return IssueContext(
        journal_title=journal_title,
        journal_url=journal_url,
        volume=volume,
        issue=issue,
        issue_url=issue_url,
        issue_publication_month=publication_month,
        issue_publication_day="",
        issue_publication_year=publication_year,
        editors=editors,
    )


def build_bronze_article(
    article_soup,
    format_factory,
    issue_context,
    article_type,
    author_details,
    corporate_details,
    run_id,
    scraped_at: datetime,
    publisher,
    url_hash,
    source_url,
    article_url,
) -> BronzeArticle:
    article_data = extract_article_data(article_soup)
    article_pub_date = article_pub_month = article_pub_year = None
    article_start_page = article_end_page = article_id = None
    if article_data is None:
        _log_missing(
            "article_data",
            article_url,
            None,
            issue_context.journal_title,
        )
    if article_data is not None:
        article_pub_date = article_data["publication_day"]
        article_pub_month = article_data["publication_month"]
        article_pub_year = article_data["publication_year"]
        article_start_page = article_data["start_page"]
        article_end_page = article_data["end_page"]
        article_id = article_data["article_id"]

    eng_rich, english_title, for_rich, foreign_title = extract_article_titles(
        article_soup,
        format_factory,
    )
    if english_title is None:
        _log_missing(
            "english_title",
            article_url,
            None,
            issue_context.journal_title,
        )

    doi = extract_doi(article_soup)
    if doi is None:
        _log_missing(
            "doi",
            article_url,
            english_title,
            issue_context.journal_title,
        )
    english_abstract, non_english_abstract = extract_abstract(
        article_soup,
        format_factory,
    )
    key_words = extract_keywords(article_soup)
    copyright_year = extract_copyright_year(article_soup)
    copyright = extract_copyright(article_soup)
    funding_details = extract_funding_details(article_soup)
    reference_count = extract_reference_count(article_soup)

    corporate_author_details = (
        corporate_details
        if corporate_details is not None
        else extract_corporate_author_details(article_soup)
    )

    authors = [Contact(**contact) for contact in _json_contacts(author_details)]
    editors = [
        Contact(**contact) for contact in _json_contacts(issue_context.editors)
    ]
    corporate_authors = [
        Contact(**contact)
        for contact in _json_contacts(corporate_author_details)
    ]

    return BronzeArticle(
        journal_title=_json_text(issue_context.journal_title),
        journal_url=_json_text(issue_context.journal_url),
        volume=_json_text(issue_context.volume),
        issue=_json_text(issue_context.issue),
        issue_url=_json_text(issue_context.issue_url),
        issue_publication_month=_json_text(
            issue_context.issue_publication_month
        ),
        issue_publication_day=_json_text(issue_context.issue_publication_day),
        issue_publication_year=_json_text(issue_context.issue_publication_year),
        article_url=_json_text(article_url),
        article_publication_month=_json_text(article_pub_month),
        article_publication_day=_json_text(article_pub_date),
        article_publication_year=_json_text(article_pub_year),
        start_page=_json_text(article_start_page),
        last_page=_json_text(article_end_page),
        article_id=_json_text(article_id),
        english_title=_json_text(english_title),
        foreign_title=_json_text(foreign_title),
        doi=_json_text(doi),
        english_abstract=_json_text(english_abstract),
        non_english_abstract=_json_text(non_english_abstract),
        article_type=_json_text(article_type),
        author_keyword=_json_text(key_words),
        reference_count=_json_text(reference_count),
        copyright_year=_json_text(copyright_year),
        license_type=_json_text(copyright),
        funding_status=_json_text(funding_details),
        authors=authors,
        editors=editors,
        corporate_authors=corporate_authors,
        english_title_rich=to_segments(eng_rich, english_title),
        foreign_title_rich=to_segments(for_rich, foreign_title),
        english_abstract_rich=to_segments(
            english_abstract,
            english_abstract,
        ),
        non_english_abstract_rich=to_segments(
            non_english_abstract,
            non_english_abstract,
        ),
        run_id=run_id,
        scraped_at=scraped_at,
        publisher=publisher,
        url_hash=url_hash,
        source_url=source_url,
    ).with_payload_hash()
