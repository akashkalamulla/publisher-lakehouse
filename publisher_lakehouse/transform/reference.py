"""Stable, host-importable article-type classification for the gold star."""

from __future__ import annotations


ARTICLE_TYPE_GROUPS: dict[str, tuple[str, ...]] = {
    "research": (
        "research_article", "short_communication", "case_report",
        "original_article", "brief_report", "data_article",
    ),
    "review": ("review_article", "mini_review", "systematic_review"),
    "opinion": (
        "editorial", "correspondence", "letter", "commentary", "perspective",
        "viewpoint", "discussion",
    ),
    "correction": ("erratum", "correction", "retraction"),
    "other_content": ("book_review", "news", "obituary", "miscellaneous"),
    "front_back_matter": (
        "contents_list", "calendar", "editorial_board", "index", "announcement",
    ),
}

SCHOLARLY_CLASSES = frozenset({"research", "review"})
ARTICLE_TYPE_CLASS = {
    article_type_key: content_class
    for content_class, keys in ARTICLE_TYPE_GROUPS.items()
    for article_type_key in keys
}


def classify_article_type(article_type_key: str | None) -> tuple[str, bool]:
    """Return the class and scholarly flag for one normalized type key."""

    content_class = ARTICLE_TYPE_CLASS.get(article_type_key, "unclassified")
    return content_class, content_class in SCHOLARLY_CLASSES
