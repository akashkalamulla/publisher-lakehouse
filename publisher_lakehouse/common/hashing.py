"""Canonical parsed-payload hashing for change detection."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any


PAYLOAD_HASH_EXCLUDED_KEYS = frozenset(
    {
        "run_id",
        "scraped_at",
        "source_url",
        "english_title_rich",
        "foreign_title_rich",
        "english_abstract_rich",
    }
)


def canonical_json(payload: Mapping[str, Any]) -> str:
    """Serialize a mapping deterministically without changing list order."""

    return json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )


def payload_hash(payload: Mapping[str, Any]) -> str:
    """Hash stable parsed content while ignoring provenance and derived fields."""

    stable_payload = {
        key: value
        for key, value in payload.items()
        if key not in PAYLOAD_HASH_EXCLUDED_KEYS and not key.endswith("_rich")
    }
    canonical = canonical_json(stable_payload)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
