"""Validated records written to the publisher lakehouse."""

from publisher_lakehouse.schemas.article import (
    JSON_FIELDS,
    BronzeArticle,
    Contact,
)


__all__ = ["BronzeArticle", "Contact", "JSON_FIELDS"]
