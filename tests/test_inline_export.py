"""Offline coverage for the ingestion-time journal export."""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

from publisher_lakehouse.common.urls import url_hash
from publisher_lakehouse.export.journal_export import slugify

# Importing the fixture registers its deterministic browser/retry patches for
# this module, just as test_pipeline_gates.py does.
from test_pipeline_manifest import (  # noqa: F401
    ARTICLE_ONE,
    ARTICLE_TWO,
    BASED_URL,
    JOURNAL_TITLE,
    JOURNAL_URL,
    PUBLISHER,
    FakeScraper,
    RecordingRepository,
    _fresh_manifest_row,
    _run,
    deterministic_pipeline,
)


ARTICLE_THREE = f"{BASED_URL}/science/article/pii/S0000000000000003"


def test_inline_export_writes_per_journal_json(tmp_path: Path) -> None:
    articles = [ARTICLE_ONE, ARTICLE_TWO, ARTICLE_THREE]

    counters = _run(
        tmp_path,
        FakeScraper(articles),
        RecordingRepository(),
    )

    slug = slugify(JOURNAL_TITLE)
    journal_dir = tmp_path / "exports" / PUBLISHER / slug
    json_path = journal_dir / f"{slug}.json"
    zip_path = journal_dir / f"{slug}_html.zip"

    assert counters.bronze_written == len(articles)
    assert json_path.is_file()
    exported = json.loads(json_path.read_text(encoding="utf-8"))
    assert len(exported["articles"]) == len(articles)
    assert [article["article_url"] for article in exported["articles"]] == articles

    assert zip_path.is_file()
    with zipfile.ZipFile(zip_path) as archive:
        assert archive.namelist() == [
            f"{url_hash(article_url)}.html" for article_url in articles
        ]
        assert all(
            archive.read(member).decode("utf-8").startswith("<html>")
            for member in archive.namelist()
        )


def test_skip_print_is_human_readable_and_keeps_structlog(
    tmp_path: Path,
    capsys,
) -> None:
    repository = RecordingRepository(
        {url_hash(ARTICLE_ONE): _fresh_manifest_row(ARTICLE_ONE)}
    )

    _run(tmp_path, FakeScraper([ARTICLE_ONE]), repository)

    captured = capsys.readouterr()
    assert "[skip]" in captured.out
    assert ARTICLE_ONE.rsplit("/", 1)[-1] in captured.out
    assert any(
        event["event"] == "article_skipped_manifest"
        for event in (
            json.loads(line) for line in captured.err.splitlines() if line
        )
    )
