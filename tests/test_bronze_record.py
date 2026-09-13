from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from publisher_lakehouse.ingestion.sciencedirect.record import (
    IssueContext,
    build_bronze_article,
    build_issue_context,
)
from publisher_lakehouse.schemas.article import BronzeArticle, JSON_FIELDS
from test_sciencedirect_issue_parse import (
    ISSUE_PATHS,
    _issue_url,
    _soup as _issue_soup,
)
from test_sciencedirect_parse import (
    EXTRA_NAMES,
    SELECTION_NAMES,
    FormatFactory,
    _golden_for,
    _reported_article_url,
    _soup as _article_soup,
)


NOW = datetime(2026, 9, 13, 12, tzinfo=timezone.utc)
ISSUE_PATH_BY_URL = {_issue_url(_issue_soup(path)): path for path in ISSUE_PATHS}
EXTRA_ISSUE_URLS = {
    "foreign_title_and_abstract": (
        "https://www.sciencedirect.com/journal/"
        "revue-veterinaire-clinique/vol/61/issue/2"
    ),
    "bookseries_chapter_editor": (
        "https://www.sciencedirect.com/bookseries/"
        "advances-in-parasitology/vol/131/suppl/C"
    ),
    "article_id_no_pages": (
        "https://www.sciencedirect.com/journal/"
        "the-lancet-global-health/vol/14/issue/6"
    ),
    "richtext_sub_and_superscript": (
        "https://www.sciencedirect.com/journal/pedosphere/vol/36/issue/3"
    ),
    "richtext_subscript_title": (
        "https://www.sciencedirect.com/journal/"
        "journal-of-microbiology-immunology-and-infection/vol/59/issue/3"
    ),
}


def _build_extra(fixture_name: str) -> BronzeArticle:
    article_soup = _article_soup(fixture_name)
    issue_url = EXTRA_ISSUE_URLS[fixture_name]
    issue_context = build_issue_context(
        _issue_soup(ISSUE_PATH_BY_URL[issue_url]),
        journal_url=issue_url,
        issue_url=issue_url,
    )
    article_url = _reported_article_url(article_soup)
    return build_bronze_article(
        article_soup,
        FormatFactory(),
        issue_context,
        article_type="",
        author_details=[],
        corporate_details=None,
        run_id="20260913120000",
        scraped_at=NOW,
        publisher="sciencedirect",
        url_hash="fixture-hash",
        source_url=article_url,
        article_url=article_url,
    )


@pytest.mark.parametrize("fixture_name", SELECTION_NAMES)
def test_selected_article_content_matches_golden(fixture_name: str) -> None:
    article_soup = _article_soup(fixture_name)
    golden = _golden_for(article_soup)
    issue_url = str(golden["issue_url"])
    issue_context = build_issue_context(
        _issue_soup(ISSUE_PATH_BY_URL[issue_url]),
        journal_url=golden["journal_url"],
        issue_url=issue_url,
    )
    author_details = [
        [contact["name"], contact["affiliation"], contact["email"]]
        for contact in golden["authors"]
    ]
    corporate_details = [
        [contact["name"], contact["affiliation"], contact["email"]]
        for contact in golden["corporate_authors"]
    ]

    record = build_bronze_article(
        article_soup,
        FormatFactory(),
        issue_context,
        article_type=golden["article_type"],
        author_details=author_details,
        corporate_details=corporate_details,
        run_id="20260913120000",
        scraped_at=NOW,
        publisher="sciencedirect",
        url_hash="fixture-hash",
        source_url=golden["article_url"],
        article_url=golden["article_url"],
    )

    assert list(record.content_payload()) == [
        *(field for field, _ in JSON_FIELDS),
        "authors",
        "editors",
        "corporate_authors",
    ]
    assert record.content_payload() == golden


def test_foreign_title_extra_is_populated() -> None:
    assert _build_extra("foreign_title_and_abstract").foreign_title


def test_bookseries_extra_has_expected_editor() -> None:
    record = _build_extra("bookseries_chapter_editor")
    assert "David Rollinson" in [editor.name for editor in record.editors]


def test_article_id_extra_has_no_pages() -> None:
    record = _build_extra("article_id_no_pages")
    assert record.article_id == "103974"
    assert record.start_page == ""
    assert record.last_page == ""


def test_abstract_extra_retains_subscript() -> None:
    record = _build_extra("richtext_sub_and_superscript")
    assert any(segment["subscript"] for segment in record.english_abstract_rich)


def test_title_extra_retains_both_subscript_runs() -> None:
    record = _build_extra("richtext_subscript_title")
    assert sum(
        segment["subscript"] for segment in record.english_title_rich
    ) >= 2


def test_extra_fixture_list_is_fully_covered() -> None:
    assert set(EXTRA_ISSUE_URLS) == set(EXTRA_NAMES)


def test_payload_hash_uses_content_but_not_provenance() -> None:
    base = BronzeArticle(
        english_title="Stable title",
        run_id="20260913120000",
        scraped_at=NOW,
        publisher="sciencedirect",
        url_hash="first-url-hash",
        source_url="https://example.test/original",
    ).with_payload_hash()
    repeated = base.model_copy(update={"payload_hash": ""}).with_payload_hash()
    changed_provenance = base.model_copy(
        update={
            "run_id": "20260914120000",
            "scraped_at": datetime(2026, 9, 14, 12, tzinfo=timezone.utc),
            "url_hash": "second-url-hash",
            "payload_hash": "",
        }
    ).with_payload_hash()
    changed_content = base.model_copy(
        update={"english_title": "Changed title", "payload_hash": ""}
    ).with_payload_hash()

    assert repeated.payload_hash == base.payload_hash
    assert changed_provenance.payload_hash == base.payload_hash
    assert changed_content.payload_hash != base.payload_hash


def test_article_rejects_naive_scrape_time_and_unknown_fields() -> None:
    required = {
        "run_id": "20260913120000",
        "scraped_at": datetime(2026, 9, 13, 12),
        "publisher": "sciencedirect",
        "url_hash": "fixture-hash",
        "source_url": "https://example.test/article",
    }
    with pytest.raises(ValidationError, match="timezone-aware UTC"):
        BronzeArticle(**required)

    required["scraped_at"] = datetime(
        2026, 9, 13, 17, 30, tzinfo=timezone(timedelta(hours=5, minutes=30))
    )
    with pytest.raises(ValidationError, match="must use UTC"):
        BronzeArticle(**required)

    required["scraped_at"] = NOW
    with pytest.raises(ValidationError, match="extra_forbidden"):
        BronzeArticle(**required, unexpected="value")


def test_article_and_issue_context_are_frozen() -> None:
    record = BronzeArticle(
        run_id="20260913120000",
        scraped_at=NOW,
        publisher="sciencedirect",
        url_hash="fixture-hash",
        source_url="https://example.test/article",
    )
    with pytest.raises(ValidationError, match="frozen_instance"):
        record.english_title = "replacement"

    context = IssueContext(
        "Journal", "journal", "1", "2", "issue", "May", "", "2026", []
    )
    with pytest.raises(FrozenInstanceError):
        context.issue = "3"
