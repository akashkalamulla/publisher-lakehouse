from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import pytest
import xlsxwriter
from bs4 import BeautifulSoup

from publisher_lakehouse.common.richtext import flatten, to_segments
from publisher_lakehouse.ingestion.errors import error_list
from publisher_lakehouse.ingestion.sciencedirect.extract import (
    extract_abstract,
    extract_article_data,
    extract_article_titles,
    extract_copyright,
    extract_copyright_year,
    extract_corporate_author_details,
    extract_doi,
    extract_funding_details,
    extract_keywords,
    extract_reference_count,
)


FIXTURE_ROOT = Path(__file__).parent / "fixtures"
ARTICLE_HTML_ROOT = FIXTURE_ROOT / "sciencedirect" / "html"
GOLDEN_ROOT = FIXTURE_ROOT / "golden" / "sciencedirect"


class FormatFactory:
    def add_format(self, properties):
        return xlsxwriter.format.Format(properties)


def _load_golden_articles() -> dict[str, dict[str, object]]:
    by_article_url: dict[str, dict[str, object]] = {}
    for path in sorted(GOLDEN_ROOT.glob("*.json")):
        document = json.loads(path.read_text(encoding="utf-8"))
        for article in document["articles"]:
            article_url = article["article_url"]
            assert article_url not in by_article_url
            by_article_url[article_url] = article
    return by_article_url


def _load_selection_names() -> tuple[str, ...]:
    document = json.loads(
        (ARTICLE_HTML_ROOT / "SELECTION.json").read_text(encoding="utf-8")
    )
    return tuple(record["fixture_name"] for record in document["selected"])


GOLDEN_BY_ARTICLE_URL = _load_golden_articles()
SELECTION_NAMES = _load_selection_names()
EXTRA_NAMES = (
    "foreign_title_and_abstract",
    "bookseries_chapter_editor",
    "article_id_no_pages",
    "richtext_sub_and_superscript",
    "richtext_subscript_title",
)


def _soup(fixture_name: str) -> BeautifulSoup:
    path = ARTICLE_HTML_ROOT / f"{fixture_name}.html"
    return BeautifulSoup(path.read_text(encoding="utf-8"), "lxml")


def _normalise_reported_url(url: str) -> str:
    parts = urlsplit(url)
    path = parts.path.replace("/abs/pii/", "/pii/").rstrip("/")
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), path, "", ""))


def _reported_article_url(soup: BeautifulSoup) -> str:
    canonical = soup.select_one('link[rel~="canonical"][href]')
    assert canonical is not None
    return _normalise_reported_url(str(canonical["href"]))


def _golden_for(soup: BeautifulSoup) -> dict[str, object]:
    article_url = _reported_article_url(soup)
    assert article_url in GOLDEN_BY_ARTICLE_URL
    return GOLDEN_BY_ARTICLE_URL[article_url]


def _legacy_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return "".join(fragment for fragment in value if isinstance(fragment, str))
    return str(value)


def _segments(value: object):
    rich = value if isinstance(value, list) else None
    plain = value if isinstance(value, str) else None
    return to_segments(rich, plain)


def _flatten_value(value: object) -> str:
    return flatten(_segments(value))


def _extra_soup(fixture_name: str) -> BeautifulSoup:
    soup = _soup(fixture_name)
    assert _reported_article_url(soup) not in GOLDEN_BY_ARTICLE_URL
    return soup


class TestSelectedArticlePages:
    @pytest.mark.parametrize("fixture_name", SELECTION_NAMES)
    def test_article_page_fields_match_golden(self, fixture_name: str) -> None:
        soup = _soup(fixture_name)
        golden = _golden_for(soup)

        article_data = extract_article_data(soup)
        assert article_data is not None
        assert _legacy_text(article_data["publication_month"]) == golden[
            "article_publication_month"
        ]
        assert _legacy_text(article_data["publication_day"]) == golden[
            "article_publication_day"
        ]
        assert _legacy_text(article_data["publication_year"]) == golden[
            "article_publication_year"
        ]
        assert _legacy_text(article_data["start_page"]) == golden["start_page"]
        assert _legacy_text(article_data["end_page"]) == golden["last_page"]
        assert _legacy_text(article_data["article_id"]) == golden["article_id"]

        english_rich, english_plain, foreign_rich, foreign_plain = (
            extract_article_titles(soup, FormatFactory())
        )
        assert flatten(to_segments(english_rich, english_plain)) == golden[
            "english_title"
        ]
        assert flatten(to_segments(foreign_rich, foreign_plain)) == golden[
            "foreign_title"
        ]

        english_abstract, non_english_abstract = extract_abstract(
            soup, FormatFactory()
        )
        assert _flatten_value(english_abstract) == golden["english_abstract"]
        assert _flatten_value(non_english_abstract) == golden[
            "non_english_abstract"
        ]

        assert _legacy_text(extract_doi(soup)) == golden["doi"]
        assert _legacy_text(extract_keywords(soup)) == golden["author_keyword"]
        assert _legacy_text(extract_copyright_year(soup)) == golden[
            "copyright_year"
        ]
        assert _legacy_text(extract_copyright(soup)) == golden["license_type"]
        assert _legacy_text(extract_funding_details(soup)) == golden[
            "funding_status"
        ]
        assert _legacy_text(extract_reference_count(soup)) == golden[
            "reference_count"
        ]

        corporate_author = extract_corporate_author_details(soup)
        corporate_authors = [] if corporate_author is None else [corporate_author]
        assert isinstance(corporate_authors, list)
        if not golden["corporate_authors"]:
            assert corporate_author is None
            assert corporate_authors == golden["corporate_authors"]


class TestExtraArticlePages:
    def test_foreign_title_and_abstract_contain_french_text(self) -> None:
        soup = _extra_soup("foreign_title_and_abstract")

        _, _, foreign_rich, foreign_plain = extract_article_titles(
            soup, FormatFactory()
        )
        _, non_english_abstract = extract_abstract(soup, FormatFactory())
        foreign_title = flatten(to_segments(foreign_rich, foreign_plain))
        foreign_abstract = _flatten_value(non_english_abstract)

        assert foreign_title
        assert foreign_abstract
        assert "dysplasie" in foreign_title.lower()
        assert "dysplasie" in foreign_abstract.lower()

    def test_bookseries_chapter_parses_without_error(self) -> None:
        soup = _extra_soup("bookseries_chapter_editor")
        article_url = _reported_article_url(soup)
        assert "/science/chapter/bookseries/pii/" in article_url
        error_list.clear()

        article_data = extract_article_data(soup)
        english_rich, english_plain, _, _ = extract_article_titles(
            soup, FormatFactory()
        )
        english_abstract, _ = extract_abstract(soup, FormatFactory())

        assert error_list == []
        assert flatten(to_segments(english_rich, english_plain))
        assert _flatten_value(english_abstract)
        assert article_data is not None
        assert article_data["start_page"] == "1"
        assert article_data["end_page"] == "29"

    def test_article_id_without_pages(self) -> None:
        soup = _extra_soup("article_id_no_pages")

        article_data = extract_article_data(soup)

        assert article_data is not None
        assert article_data["article_id"] == "103974"
        assert _legacy_text(article_data["start_page"]) == ""
        assert _legacy_text(article_data["end_page"]) == ""

    def test_abstract_preserves_subscript_and_superscript(self) -> None:
        soup = _extra_soup("richtext_sub_and_superscript")

        english_abstract, _ = extract_abstract(soup, FormatFactory())
        segments = _segments(english_abstract)

        assert any(segment["subscript"] for segment in segments)
        assert any(segment["superscript"] for segment in segments)

    def test_title_preserves_both_subscript_runs(self) -> None:
        soup = _extra_soup("richtext_subscript_title")

        english_rich, english_plain, _, _ = extract_article_titles(
            soup, FormatFactory()
        )
        segments = to_segments(english_rich, english_plain)

        assert [
            segment["text"] for segment in segments if segment["subscript"]
        ] == ["NDM", "KPC"]
