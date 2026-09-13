"""Persist and recover deterministic raw HTML artifacts."""

from __future__ import annotations

import gzip
from datetime import date
from pathlib import Path


def store_raw_html(
    html: str,
    *,
    raw_html_dir: str | Path,
    publisher: str,
    run_date: date,
    url_hash: str,
) -> Path:
    """Write a page as reproducible gzip-compressed UTF-8 and return its path."""

    path = (
        Path(raw_html_dir)
        / publisher
        / f"{run_date:%Y-%m-%d}"
        / f"{url_hash}.html.gz"
    )
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("wb") as raw_file:
        with gzip.GzipFile(
            filename="", fileobj=raw_file, mode="wb", mtime=0
        ) as gzip_file:
            gzip_file.write(html.encode("utf-8"))

    return path


def read_raw_html(path: str | Path) -> str:
    """Read a gzip-compressed raw HTML artifact as UTF-8 text."""

    with gzip.open(path, mode="rt", encoding="utf-8", newline="") as gzip_file:
        return gzip_file.read()
