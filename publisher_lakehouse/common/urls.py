"""Stable URL identities shared by publisher ingestion pipelines."""

from __future__ import annotations

import hashlib
import re
from urllib.parse import quote, unquote, urlsplit, urlunsplit


# Host aliases are intentionally publisher-specific.  Both hosts serve the same
# ScienceDirect content, so they must collapse to one manifest identity or the
# same article is fetched twice.  Phase 1b adds ``rd.springer.com:
# link.springer.com`` here.
PUBLISHER_OVERRIDES: dict[str, str] = {
    "sciencedirect.com": "www.sciencedirect.com",
}

_SCIENCEDIRECT_DISCOVERY_PATH = re.compile(
    r"^/(?:journal|bookseries)/[^/]+/(?:issues|volumes|vol)(?:/|$)",
    re.IGNORECASE,
)

# The three tiers of the URL model.  Journal and issue pages are discovery
# pages; only article URLs are a deduplication unit.
URL_TYPES = ("journal", "issue", "article")

_ARTICLE_PATH = re.compile(r"^/science/article/", re.IGNORECASE)
_ISSUE_PATH = re.compile(r"^/(?:journal|bookseries)/[^/]+/vol/", re.IGNORECASE)
_JOURNAL_PATH = re.compile(
    r"^/(?:journal|bookseries)/[^/]+(?:/(?:issues|volumes))?$",
    re.IGNORECASE,
)
_PATH_SAFE_CHARACTERS = "/:@!$&'()*+,;=-._~"
_QUERY_COMPONENT_SAFE_CHARACTERS = "/:?@!$'()*+,-._~"
_QUERY_SEPARATOR = re.compile(r"([&;])")


def _normalised_host(parts) -> str:
    host = parts.hostname
    if not host:
        raise ValueError("URL must include a host")

    host = PUBLISHER_OVERRIDES.get(host.lower(), host.lower()).lower()
    try:
        port = parts.port
    except ValueError as exc:
        raise ValueError(f"Invalid URL port: {parts.netloc}") from exc

    rendered_host = f"[{host}]" if ":" in host and not host.startswith("[") else host
    if port not in (None, 80, 443):
        return f"{rendered_host}:{port}"
    return rendered_host


def _normalised_query_components(query: str) -> str:
    components = _QUERY_SEPARATOR.split(query)
    normalized: list[str] = []
    for component in components:
        if component in {"&", ";"}:
            normalized.append(component)
            continue

        key, separator, value = component.partition("=")
        normalized.append(
            quote(unquote(key), safe=_QUERY_COMPONENT_SAFE_CHARACTERS)
        )
        if separator:
            normalized.append("=")
            normalized.append(
                quote(unquote(value), safe=_QUERY_COMPONENT_SAFE_CHARACTERS)
            )
    return "".join(normalized)


def _normalised_query(host: str, path: str, query: str) -> str:
    if host.startswith("["):
        hostname = host[1 : host.index("]")]
    else:
        hostname = host.rsplit(":", 1)[0] if ":" in host else host
    is_sciencedirect = hostname == "sciencedirect.com" or hostname.endswith(
        ".sciencedirect.com"
    )
    if not is_sciencedirect:
        return _normalised_query_components(query)

    if path.lower().startswith("/science/article/"):
        return ""
    if _SCIENCEDIRECT_DISCOVERY_PATH.match(path):
        # Discovery pagination is fetched, but only its query-free base URL is
        # an identity in the manifest.
        return ""
    return _normalised_query_components(query)


def normalize_url(url: str) -> str:
    """Return a deterministic, publisher-aware URL identity.

    The scheme is always HTTPS, host aliases are applied through
    :data:`PUBLISHER_OVERRIDES`, fragments and trailing slashes are removed,
    and percent encoding is decoded exactly once.
    """

    if not isinstance(url, str) or not url.strip():
        raise ValueError("URL must be a non-empty string")

    parts = urlsplit(url.strip())
    host = _normalised_host(parts)
    path = quote(
        unquote(parts.path).rstrip("/"),
        safe=_PATH_SAFE_CHARACTERS,
    )
    query = _normalised_query(host, path, parts.query)
    return urlunsplit(("https", host, path, query, ""))


def url_hash(url: str) -> str:
    """Return the SHA-256 hex digest of the normalized URL."""

    return hashlib.sha256(normalize_url(url).encode("utf-8")).hexdigest()


def classify_url_type(url: str) -> str:
    """Return the manifest ``url_type`` tier for a URL.

    The refresh policy is looked up per tier, so a misclassification either
    re-fetches articles that dedup should have skipped or freezes a discovery
    page that must be re-read to notice a new issue.  Unknown shapes raise
    rather than defaulting to a tier.
    """

    path = urlsplit(normalize_url(url)).path
    if _ARTICLE_PATH.match(path):
        return "article"
    if _ISSUE_PATH.match(path):
        return "issue"
    if _JOURNAL_PATH.match(path):
        return "journal"
    raise ValueError(f"Cannot classify URL into a journal/issue/article tier: {url}")
