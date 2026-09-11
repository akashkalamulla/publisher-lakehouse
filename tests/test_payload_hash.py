from __future__ import annotations

import json
from datetime import datetime

import pytest

from publisher_lakehouse.common.hashing import canonical_json, payload_hash
from publisher_lakehouse.common.ids import new_run_id


def test_payload_hash_is_stable_across_key_insertion_order() -> None:
    first = {"title": "Café", "doi": "10.1/example", "authors": ["A", "B"]}
    second = {"authors": ["A", "B"], "doi": "10.1/example", "title": "Café"}
    assert payload_hash(first) == payload_hash(second)


@pytest.mark.parametrize(
    "excluded_key",
    [
        "run_id",
        "scraped_at",
        "source_url",
        "english_title_rich",
        "foreign_title_rich",
        "english_abstract_rich",
    ],
)
def test_payload_hash_excludes_volatile_and_derived_keys(excluded_key: str) -> None:
    payload = {"doi": "10.1/example", excluded_key: "first"}
    changed = {"doi": "10.1/example", excluded_key: "second"}
    assert payload_hash(payload) == payload_hash(changed)


def test_payload_hash_preserves_meaningful_author_order() -> None:
    first = {"authors": [{"name": "A"}, {"name": "B"}]}
    reordered = {"authors": [{"name": "B"}, {"name": "A"}]}
    assert payload_hash(first) != payload_hash(reordered)


def test_payload_hash_excludes_any_derived_rich_segment_field() -> None:
    first = {"doi": "10.1/example", "future_field_rich": [{"text": "first"}]}
    changed = {"doi": "10.1/example", "future_field_rich": [{"text": "second"}]}
    assert payload_hash(first) == payload_hash(changed)


def test_payload_hash_does_not_mutate_input() -> None:
    payload = {"doi": "10.1/example", "run_id": "20260911102611"}
    before = dict(payload)
    payload_hash(payload)
    assert payload == before


def test_canonical_json_is_compact_sorted_and_unicode_preserving() -> None:
    value = canonical_json({"z": 1, "a": "é"})
    assert value == '{"a":"é","z":1}'
    assert json.loads(value) == {"a": "é", "z": 1}


def test_new_run_id_matches_legacy_timestamp_format() -> None:
    assert new_run_id(datetime(2026, 9, 11, 10, 26, 11)) == "20260911102611"
