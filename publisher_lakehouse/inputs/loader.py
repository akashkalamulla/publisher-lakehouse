"""Read the operator's URL list into normalised ingestion tasks.

This module owns **gate 0** of deduplication: two rows in the input file that
normalise to the same identity are collapsed to one task, so the same journal
is never scraped twice in a run because someone pasted a bare-host or
trailing-slash variant of a URL already in the file.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from publisher_lakehouse.common.logging import get_logger
from publisher_lakehouse.common.urls import classify_url_type, normalize_url, url_hash


logger = get_logger(__name__)

REQUIRED_COLUMNS = ("publisher", "url", "enabled", "note")

_TRUE_VALUES = frozenset({"true", "1", "yes", "y"})
_FALSE_VALUES = frozenset({"false", "0", "no", "n"})


@dataclass(frozen=True, slots=True)
class UrlTask:
    """One enabled, normalised URL to visit for a publisher."""

    url_hash: str
    normalised_url: str
    raw_url: str
    publisher: str
    url_type: str
    note: str


@dataclass(frozen=True, slots=True)
class LoadedTasks:
    """Tasks plus the gate-0 counters the run summary reports."""

    tasks: list[UrlTask]
    input_total: int
    dup_in_file: int
    disabled: int


def _parse_enabled(value: str | None, *, line: int) -> bool:
    text = (value or "").strip().lower()
    if text in _TRUE_VALUES:
        return True
    if text in _FALSE_VALUES:
        return False
    raise ValueError(
        f"{REQUIRED_COLUMNS[2]!r} on row {line} must be true or false, got {value!r}"
    )


def load_tasks_with_stats(
    publisher: str,
    path: str | Path,
) -> LoadedTasks:
    """Load tasks for one publisher and report the gate-0 counters."""

    csv_path = Path(path)
    if not csv_path.is_file():
        raise FileNotFoundError(f"URL input file not found: {csv_path}")

    tasks: list[UrlTask] = []
    by_hash: dict[str, UrlTask] = {}
    input_total = 0
    dup_in_file = 0
    disabled = 0

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        missing = [name for name in REQUIRED_COLUMNS if name not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(
                f"{csv_path} is missing column(s): {', '.join(missing)}"
            )

        for line, row in enumerate(reader, start=2):
            row_publisher = (row.get("publisher") or "").strip()
            if row_publisher != publisher:
                continue

            input_total += 1
            if not _parse_enabled(row.get("enabled"), line=line):
                disabled += 1
                logger.info(
                    "input_url_disabled",
                    publisher=publisher,
                    raw_url=(row.get("url") or "").strip(),
                    line=line,
                )
                continue

            raw_url = (row.get("url") or "").strip()
            normalised_url = normalize_url(raw_url)
            identity = url_hash(raw_url)

            existing = by_hash.get(identity)
            if existing is not None:
                dup_in_file += 1
                # Both raw URLs are logged: the operator needs to see which two
                # lines collided, since they are rarely textually identical.
                logger.warning(
                    "input_url_duplicate_dropped",
                    publisher=publisher,
                    normalised_url=normalised_url,
                    kept_raw_url=existing.raw_url,
                    dropped_raw_url=raw_url,
                    line=line,
                )
                continue

            task = UrlTask(
                url_hash=identity,
                normalised_url=normalised_url,
                raw_url=raw_url,
                publisher=publisher,
                url_type=classify_url_type(normalised_url),
                note=(row.get("note") or "").strip(),
            )
            by_hash[identity] = task
            tasks.append(task)

    logger.info(
        "input_tasks_loaded",
        publisher=publisher,
        input_total=input_total,
        dup_in_file=dup_in_file,
        disabled=disabled,
        tasks=len(tasks),
    )
    return LoadedTasks(
        tasks=tasks,
        input_total=input_total,
        dup_in_file=dup_in_file,
        disabled=disabled,
    )


def load_tasks(publisher: str, path: str | Path) -> list[UrlTask]:
    """Load the enabled, deduplicated tasks for one publisher."""

    return load_tasks_with_stats(publisher, path).tasks
