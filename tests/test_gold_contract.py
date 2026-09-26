"""Host-only contracts for the gold model; no Spark dependency."""

from __future__ import annotations

import importlib
import re
import sys

from publisher_lakehouse.transform.reference import (
    ARTICLE_TYPE_CLASS, ARTICLE_TYPE_GROUPS, SCHOLARLY_CLASSES,
    classify_article_type,
)
from publisher_lakehouse.transform.schemas import GOLD_SPECS


def test_article_type_mapping_is_unique_valid_and_snake_case():
    allowed = {
        "research", "review", "opinion", "correction",
        "other_content", "front_back_matter",
    }
    keys = [key for group in ARTICLE_TYPE_GROUPS.values() for key in group]
    assert set(ARTICLE_TYPE_GROUPS) == allowed
    assert len(keys) == len(set(keys)) == len(ARTICLE_TYPE_CLASS)
    assert all(re.fullmatch(r"[a-z][a-z0-9]*(?:_[a-z0-9]+)*", key) for key in keys)
    assert "miscellaneous" in ARTICLE_TYPE_CLASS


def test_scholarly_flag_is_derived_from_class():
    for key, content_class in ARTICLE_TYPE_CLASS.items():
        assert classify_article_type(key) == (
            content_class, content_class in SCHOLARLY_CLASSES
        )
    assert classify_article_type("alien_material") == ("unclassified", False)


def test_every_dimension_has_surrogate_and_natural_key():
    for name, spec in GOLD_SPECS.items():
        names = [column for column, _ in spec.columns]
        assert len(names) == len(set(names)), name
        if name.startswith("dim_"):
            assert spec.surrogate_key in names
            assert spec.natural_key and set(spec.natural_key) <= set(names)


def test_fact_foreign_keys_reference_dimension_keys():
    fact = GOLD_SPECS["fact_article"]
    columns = {column for column, _ in fact.columns}
    for foreign_key, dimension_name in fact.foreign_keys:
        assert foreign_key in columns
        surrogate_key = GOLD_SPECS[dimension_name].surrogate_key
        assert foreign_key == surrogate_key or foreign_key.endswith(surrogate_key)


def test_gold_and_reference_import_without_pyspark():
    before = {name for name in sys.modules if name.startswith("pyspark")}
    importlib.import_module("publisher_lakehouse.transform.reference")
    importlib.import_module("publisher_lakehouse.transform.gold")
    assert {name for name in sys.modules if name.startswith("pyspark")} == before
