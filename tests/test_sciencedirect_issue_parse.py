from __future__ import annotations

import json
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from publisher_lakehouse.ingestion.sciencedirect.extract import (
    extract_article_type,
    extract_editor_details,
    extract_issue_volume_pubdate,
    extract_main_title,
    get_all_article_list,
    get_total_pages,
)


FIXTURE_ROOT = Path(__file__).parent / "fixtures"
ISSUE_HTML_ROOT = FIXTURE_ROOT / "sciencedirect" / "issue_html"
GOLDEN_ROOT = FIXTURE_ROOT / "golden" / "sciencedirect"
ISSUE_PATHS = tuple(sorted(ISSUE_HTML_ROOT.glob("*.html")))

BOOKSERIES_EDITORS = [
    ("David Rollinson", "", ""),
    ("Russell Stothard", "", ""),
    ("Cinzia Cantacessi", "", ""),
]


def _load_golden_issues() -> dict[str, dict[str, object]]:
    by_issue_url: dict[str, dict[str, object]] = {}
    compared_fields = (
        "journal_title",
        "volume",
        "issue",
        "issue_publication_month",
        "issue_publication_day",
        "issue_publication_year",
    )
    for path in sorted(GOLDEN_ROOT.glob("*.json")):
        document = json.loads(path.read_text(encoding="utf-8"))
        for article in document["articles"]:
            issue_url = article["issue_url"]
            if issue_url in by_issue_url:
                assert all(
                    by_issue_url[issue_url][field] == article[field]
                    for field in compared_fields
                )
            else:
                by_issue_url[issue_url] = article
    return by_issue_url


GOLDEN_BY_ISSUE_URL = _load_golden_issues()


def _soup(path: Path) -> BeautifulSoup:
    return BeautifulSoup(path.read_text(encoding="utf-8"), "lxml")


def _issue_url(soup: BeautifulSoup) -> str:
    canonical = soup.select_one('link[rel~="canonical"][href]')
    assert canonical is not None
    return str(canonical["href"])


@pytest.mark.parametrize("path", ISSUE_PATHS, ids=lambda path: path.stem)
def test_issue_metadata_matches_golden_or_has_expected_shape(path: Path) -> None:
    soup = _soup(path)
    issue_url = _issue_url(soup)
    journal_title = extract_main_title(soup)
    volume, issue, publication_year, publication_month = (
        extract_issue_volume_pubdate(soup, issue_url)
    )
    # The extractor has no day return slot; the legacy workflow stores it empty.
    publication_day = ""

    assert isinstance(journal_title, str) and journal_title.strip()
    assert isinstance(volume, str) and volume.strip()
    assert isinstance(publication_year, str) and publication_year.strip()

    golden = GOLDEN_BY_ISSUE_URL.get(issue_url)
    if golden is not None:
        assert journal_title == golden["journal_title"]
        assert volume == golden["volume"]
        assert issue == golden["issue"]
        assert publication_month == golden["issue_publication_month"]
        assert publication_day == golden["issue_publication_day"]
        assert publication_year == golden["issue_publication_year"]
    elif "/bookseries/" in issue_url:
        assert issue is None
        assert publication_month is None
    else:
        assert isinstance(issue, str) and issue.strip()
        assert isinstance(publication_month, str) and publication_month.strip()


@pytest.mark.parametrize("path", ISSUE_PATHS, ids=lambda path: path.stem)
def test_issue_article_list_has_an_extractable_article_type(path: Path) -> None:
    articles = get_all_article_list(_soup(path))

    assert isinstance(articles, list)
    assert articles
    article_type = extract_article_type(articles[0])
    assert isinstance(article_type, str) and article_type.strip()


@pytest.mark.parametrize("path", ISSUE_PATHS, ids=lambda path: path.stem)
def test_editor_details_match_observed_fixture_output(path: Path) -> None:
    editors = extract_editor_details(_soup(path))

    if path.stem == "bookseries_advances_in_parasitology_vol_131_suppl_c":
        assert editors == BOOKSERIES_EDITORS
    else:
        assert editors == []


@pytest.mark.parametrize("path", ISSUE_PATHS, ids=lambda path: path.stem)
def test_issue_has_no_pagination_label(path: Path) -> None:
    assert get_total_pages(_soup(path)) is None
