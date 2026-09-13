from __future__ import annotations

from datetime import date
from pathlib import Path

from publisher_lakehouse.ingestion.writers.raw_store import (
    read_raw_html,
    store_raw_html,
)


def test_raw_html_round_trip_uses_the_partitioned_path(tmp_path: Path) -> None:
    html = "<html>\r\n<title>Café 中文</title>\r\n</html>"

    path = store_raw_html(
        html,
        raw_html_dir=tmp_path / "raw",
        publisher="sciencedirect",
        run_date=date(2026, 9, 13),
        url_hash="abc123",
    )

    assert path == (
        tmp_path
        / "raw"
        / "sciencedirect"
        / "2026-09-13"
        / "abc123.html.gz"
    )
    assert read_raw_html(path) == html


def test_same_html_produces_byte_identical_gzip_output(tmp_path: Path) -> None:
    arguments = {
        "raw_html_dir": tmp_path / "raw",
        "publisher": "sciencedirect",
        "run_date": date(2026, 9, 13),
        "url_hash": "abc123",
    }

    path = store_raw_html("naïve 📖", **arguments)
    first = path.read_bytes()
    store_raw_html("naïve 📖", **arguments)
    second = path.read_bytes()

    assert first == second
    assert first[4:8] == b"\x00\x00\x00\x00"
