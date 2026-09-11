from __future__ import annotations

import hashlib

import pytest

from publisher_lakehouse.common.urls import (
    PUBLISHER_OVERRIDES,
    normalize_url,
    url_hash,
)


JOURNAL = "https://www.sciencedirect.com/journal/harmful-algae/issues"
ISSUE = "https://www.sciencedirect.com/journal/harmful-algae/vol/73/issue/3"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (
            "http://WWW.ScienceDirect.com/journal/harmful-algae/issues",
            JOURNAL,
        ),
        (f"{JOURNAL}/", JOURNAL),
        (f"{JOURNAL}#all-issues", JOURNAL),
        (f"{ISSUE}?page=2", ISSUE),
        (
            "https://www.sciencedirect.com/science/article/pii/S0019570726001617?via%3Dihub&utm_source=test",
            "https://www.sciencedirect.com/science/article/pii/S0019570726001617",
        ),
        (
            "https://www.sciencedirect.com/bookseries/advances-in-parasitology/volumes/",
            "https://www.sciencedirect.com/bookseries/advances-in-parasitology/volumes",
        ),
        (
            "https://www.sciencedirect.com/science/article/pii/SAbC123xYz",
            "https://www.sciencedirect.com/science/article/pii/SAbC123xYz",
        ),
        (
            "https://example.com/a%20path?q=hello%20world#fragment",
            "https://example.com/a%20path?q=hello%20world",
        ),
    ],
)
def test_normalize_url(raw: str, expected: str) -> None:
    assert normalize_url(raw) == expected


@pytest.mark.parametrize(
    "url",
    [
        JOURNAL,
        f"{ISSUE}?page=2#results",
        "https://www.sciencedirect.com/bookseries/advances-in-parasitology/volumes/",
        "https://example.com/a%20path?q=hello%20world",
        "https://example.com/%252Fstill-encoded/%3Fquery-marker/%23fragment-marker",
    ],
)
def test_normalize_url_is_idempotent(url: str) -> None:
    normalized = normalize_url(url)
    assert normalize_url(normalized) == normalized


def test_bare_and_www_sciencedirect_hosts_are_one_identity() -> None:
    bare = "https://sciencedirect.com/journal/harmful-algae/issues"
    assert normalize_url(bare) == JOURNAL
    assert url_hash(bare) == url_hash(JOURNAL)


def test_bare_host_override_also_applies_to_article_urls() -> None:
    article = "https://sciencedirect.com/science/article/pii/S0019570726001617"
    assert normalize_url(article) == (
        "https://www.sciencedirect.com/science/article/pii/S0019570726001617"
    )


def test_overridden_host_still_drops_sciencedirect_discovery_pagination() -> None:
    assert normalize_url("https://sciencedirect.com/journal/harmful-algae/issues?page=3") == (
        JOURNAL
    )


def test_publisher_host_override_is_a_generic_seam(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(PUBLISHER_OVERRIDES, "rd.springer.com", "link.springer.com")
    assert normalize_url("http://RD.Springer.com/article/10.1007/test/") == (
        "https://link.springer.com/article/10.1007/test"
    )


def test_url_hash_hashes_the_normalized_url() -> None:
    expected = hashlib.sha256(JOURNAL.encode("utf-8")).hexdigest()
    assert url_hash(f"http://WWW.ScienceDirect.com/journal/harmful-algae/issues/") == expected


def test_percent_encoded_unreserved_characters_are_decoded() -> None:
    assert normalize_url("https://example.com/%7Eauthor/%41rticle") == (
        "https://example.com/~author/Article"
    )


def test_double_encoded_path_is_decoded_once_and_stays_idempotent() -> None:
    url = "https://example.com/%252Fstill-encoded"
    assert normalize_url(url) == url


def test_generic_query_keeps_encoded_delimiters_distinct() -> None:
    encoded_value = normalize_url("https://example.com/search?q=a%26b")
    separate_parameter = normalize_url("https://example.com/search?q=a&b")
    assert encoded_value == "https://example.com/search?q=a%26b"
    assert separate_parameter == "https://example.com/search?q=a&b"
    assert encoded_value != separate_parameter


def test_ipv6_host_keeps_brackets_and_is_idempotent() -> None:
    normalized = normalize_url("http://[2001:DB8::1]:80/article/")
    assert normalized == "https://[2001:db8::1]/article"
    assert normalize_url(normalized) == normalized


@pytest.mark.parametrize("value", ["", "   ", "relative/path", None])
def test_normalize_url_rejects_non_absolute_urls(value) -> None:
    with pytest.raises(ValueError):
        normalize_url(value)
