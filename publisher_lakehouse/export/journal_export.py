"""Write legacy-compatible journal exports from immutable bronze artifacts."""

from __future__ import annotations

import json
import re
import zipfile
from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import date
from pathlib import Path
from typing import Any

from publisher_lakehouse.ingestion.writers.raw_store import read_raw_html
from publisher_lakehouse.schemas.article import BronzeArticle


_PARTITION_PREFIX = "ingest_date="
_PART_PREFIX = "part-"
_WINDOWS_ILLEGAL = re.compile(r'[<>:"/\\|?*]')
_CONTROL_CHARACTERS = re.compile(r"[\x00-\x1f\x7f]")
_WHITESPACE_OR_UNDERSCORES = re.compile(r"[_\s]+")
_ZIP_TIMESTAMP = (1980, 1, 1, 0, 0, 0)

Record = Mapping[str, Any] | BronzeArticle
ManifestRecord = Mapping[str, Any] | Any


def _record_value(record: Record, field: str) -> Any:
    if isinstance(record, BronzeArticle):
        return getattr(record, field)
    return record[field]


def _partition_key(path: Path) -> tuple[date, str]:
    """Return the ordering key encoded in a bronze partition path.

    The key deliberately uses the ingest-date directory and run-id filename;
    filesystem mtimes are not a reliable representation of collection order.
    """

    partition = path.parent.name
    if not partition.startswith(_PARTITION_PREFIX):
        raise ValueError(f"Unexpected bronze partition name: {partition!r}")
    try:
        ingest_date = date.fromisoformat(partition.removeprefix(_PARTITION_PREFIX))
    except ValueError as exc:
        raise ValueError(f"Invalid bronze ingest date in {path}") from exc

    if path.suffix != ".jsonl" or not path.stem.startswith(_PART_PREFIX):
        raise ValueError(f"Unexpected bronze part filename: {path.name!r}")
    run_id = path.stem.removeprefix(_PART_PREFIX)
    if not run_id:
        raise ValueError(f"Bronze part filename has no run id: {path.name!r}")
    return ingest_date, run_id


def load_latest_bronze_records(
    publisher: str,
    bronze_root: str | Path = Path("data/bronze"),
) -> dict[str, dict[str, Any]]:
    """Load the newest bronze row for every URL hash of ``publisher``.

    Within a part, a later duplicate line replaces an earlier one. Across
    parts, freshness is determined solely by ``ingest_date`` and ``run_id``.
    """

    root = Path(bronze_root) / publisher
    latest: dict[str, tuple[tuple[date, str], dict[str, Any]]] = {}
    paths = sorted(root.glob("ingest_date=*/part-*.jsonl"))

    for path in paths:
        partition_key = _partition_key(path)
        with path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        f"Invalid JSON in bronze file {path} line {line_number}"
                    ) from exc
                if not isinstance(record, dict):
                    raise ValueError(
                        f"Bronze file {path} line {line_number} is not an object"
                    )
                try:
                    identity = record["url_hash"]
                except KeyError as exc:
                    raise ValueError(
                        f"Bronze file {path} line {line_number} has no url_hash"
                    ) from exc
                if not isinstance(identity, str) or not identity:
                    raise ValueError(
                        f"Bronze file {path} line {line_number} has an invalid url_hash"
                    )

                existing = latest.get(identity)
                if existing is None or partition_key >= existing[0]:
                    latest[identity] = (partition_key, record)

    return {identity: record for identity, (_, record) in latest.items()}


def group_by_journal(
    records: Mapping[str, Record] | Iterable[Record],
) -> dict[str, list[Record]]:
    """Group records by their literal ``journal_url`` field."""

    values = records.values() if isinstance(records, Mapping) else records
    grouped: dict[str, list[Record]] = defaultdict(list)
    for record in values:
        journal_url = _record_value(record, "journal_url")
        if not isinstance(journal_url, str) or not journal_url:
            raise ValueError("Bronze record has an invalid journal_url")
        grouped[journal_url].append(record)
    return dict(grouped)


def slugify(journal_title: str) -> str:
    """Return the legacy Windows-safe, deterministic journal directory name."""

    if not journal_title or not str(journal_title).strip():
        return "untitled"

    slug = str(journal_title).strip()
    slug = _WINDOWS_ILLEGAL.sub("_", slug)
    slug = _CONTROL_CHARACTERS.sub("_", slug)
    slug = _WHITESPACE_OR_UNDERSCORES.sub("_", slug).strip("_")
    return slug[:150] or "untitled"


def _journal_slug(records: Iterable[Record]) -> str:
    titles = {str(_record_value(record, "journal_title")) for record in records}
    if len(titles) != 1:
        raise ValueError("A journal export must contain one journal_title")
    return slugify(titles.pop())


def _content_payload(record: Record) -> dict[str, Any]:
    if isinstance(record, BronzeArticle):
        return record.content_payload()
    return BronzeArticle.model_validate(record).content_payload()


def write_journal_json(
    journal_url: str,
    records: Iterable[Record],
    out_dir: str | Path,
) -> Path | None:
    """Write one legacy-shaped ``{\"articles\": [...]}`` journal document."""

    selected = list(records)
    if not selected:
        return None
    if any(_record_value(record, "journal_url") != journal_url for record in selected):
        raise ValueError("Journal records do not all match journal_url")

    slug = _journal_slug(selected)
    destination = Path(out_dir) / slug / f"{slug}.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(
            {"articles": [_content_payload(record) for record in selected]},
            handle,
            ensure_ascii=False,
            indent=2,
        )
        handle.write("\n")
    return destination


def _latest_raw_html_path(raw_html_root: str | Path, url_hash: str) -> Path:
    matches = sorted(Path(raw_html_root).glob(f"**/{url_hash}.html.gz"))
    if not matches:
        raise FileNotFoundError(f"No raw HTML artifact found for url_hash={url_hash}")
    return matches[-1]


def bundle_journal_html(
    records: Iterable[Record],
    raw_html_root: str | Path,
    out_dir: str | Path,
) -> Path | None:
    """Bundle each journal's stored raw HTML into a deterministic ZIP file."""

    selected = list(records)
    if not selected:
        return None

    slug = _journal_slug(selected)
    destination = Path(out_dir) / slug / f"{slug}_html.zip"
    destination.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(
        destination,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=9,
    ) as archive:
        for record in selected:
            identity = _record_value(record, "url_hash")
            if not isinstance(identity, str) or not identity:
                raise ValueError("Bronze record has an invalid url_hash")
            html = read_raw_html(_latest_raw_html_path(raw_html_root, identity))
            entry = zipfile.ZipInfo(f"{identity}.html", date_time=_ZIP_TIMESTAMP)
            entry.compress_type = zipfile.ZIP_DEFLATED
            entry.external_attr = 0o600 << 16
            archive.writestr(entry, html.encode("utf-8"), compress_type=zipfile.ZIP_DEFLATED)
    return destination


def _manifest_status(row: ManifestRecord) -> str | None:
    if isinstance(row, Mapping):
        status = row.get("status")
    else:
        status = getattr(row, "status", None)
    return status if isinstance(status, str) else None


def export_journals(
    publisher: str,
    *,
    bronze_root: str | Path,
    raw_html_root: str | Path,
    out_dir: str | Path,
    manifest: Mapping[str, ManifestRecord],
    journal_url: str | None = None,
) -> dict[str, tuple[Path, Path]]:
    """Export every requested journal whose latest records are manifest-OK.

    ``manifest`` is deliberately supplied by the caller so this module has no
    database write path. A bronze record absent from the manifest, or with any
    status other than ``ok``, is excluded from the export.
    """

    latest = load_latest_bronze_records(publisher, bronze_root)
    ok_records = {
        identity: record
        for identity, record in latest.items()
        if _manifest_status(manifest.get(identity)) == "ok"
    }
    grouped = group_by_journal(ok_records)
    exported: dict[str, tuple[Path, Path]] = {}

    for grouped_url, records in grouped.items():
        if journal_url is not None and grouped_url != journal_url:
            continue
        if not records:
            continue
        json_path = write_journal_json(grouped_url, records, out_dir)
        zip_path = bundle_journal_html(records, raw_html_root, out_dir)
        if json_path is not None and zip_path is not None:
            exported[grouped_url] = (json_path, zip_path)

    return exported
