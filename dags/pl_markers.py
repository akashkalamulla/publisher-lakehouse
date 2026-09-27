"""Pure landing-marker names and pending-set calculation for the DAG."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from collections.abc import Iterable


MARKER_ROOT = "landing/_markers"
_MARKER_NAME = re.compile(r"\d{8}T\d{6}Z\.json\Z")


def marker_key(publisher: str, landed_at: datetime) -> str:
    """Name a completed landing batch using its UTC second."""

    if not publisher or publisher in {".", ".."} or "/" in publisher or "\\" in publisher:
        raise ValueError("Invalid publisher name")
    if landed_at.tzinfo is None or landed_at.utcoffset() is None:
        raise ValueError("Landing time must be timezone-aware")
    stamp = landed_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{MARKER_ROOT}/{publisher}/{stamp}.json"


def processed_key(key: str) -> str:
    """Return the acknowledgement sibling of one marker."""

    if not _MARKER_NAME.fullmatch(key.rsplit("/", 1)[-1]):
        raise ValueError("Not a landing marker key")
    return key + ".processed"


def pending_marker_keys(keys: Iterable[str], publisher: str) -> list[str]:
    """Return sorted marker objects without processed siblings."""

    prefix = f"{MARKER_ROOT}/{publisher}/"
    all_keys = set(keys)
    markers = (
        key for key in all_keys
        if key.startswith(prefix)
        and "/" not in key[len(prefix):]
        and _MARKER_NAME.fullmatch(key[len(prefix):])
    )
    return sorted(key for key in markers if processed_key(key) not in all_keys)
