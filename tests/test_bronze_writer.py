from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from publisher_lakehouse.ingestion.writers.bronze_writer import BronzeWriter
from publisher_lakehouse.schemas.article import BronzeArticle


def article(title: str = "Café 中文") -> BronzeArticle:
    return BronzeArticle(
        english_title=title,
        run_id="20260913120000",
        scraped_at=datetime(2026, 9, 13, 12, tzinfo=timezone.utc),
        publisher="sciencedirect",
        url_hash="abc123",
        source_url="https://example.test/article",
    ).with_payload_hash()


def expected_path(tmp_path: Path) -> Path:
    return (
        tmp_path
        / "bronze"
        / "sciencedirect"
        / "ingest_date=2026-09-13"
        / "part-20260913120000.jsonl"
    )


def test_records_are_written_as_complete_json_lines(tmp_path: Path) -> None:
    records = [article(f"Article {index}") for index in range(3)]

    with BronzeWriter(
        tmp_path / "bronze",
        "sciencedirect",
        "20260913120000",
        date(2026, 9, 13),
        flush_every=10,
    ) as writer:
        for record in records:
            writer.write(record)

    lines = expected_path(tmp_path).read_text(encoding="utf-8").splitlines()
    decoded = [json.loads(line) for line in lines]

    assert writer.written_count == len(records)
    assert len(lines) == len(records)
    assert decoded == [record.model_dump(mode="json") for record in records]
    assert [
        {key: value[key] for key in record.content_payload()}
        for value, record in zip(decoded, records, strict=True)
    ] == [record.content_payload() for record in records]


def test_flush_boundary_smaller_than_record_count_keeps_every_record(
    tmp_path: Path,
) -> None:
    records = [article(f"Article {index}") for index in range(5)]

    with BronzeWriter(
        tmp_path / "bronze",
        "sciencedirect",
        "20260913120000",
        date(2026, 9, 13),
        flush_every=2,
    ) as writer:
        for record in records[:2]:
            writer.write(record)
        flushed_lines = expected_path(tmp_path).read_text(
            encoding="utf-8"
        ).splitlines()
        assert len(flushed_lines) == 2
        for record in records[2:]:
            writer.write(record)

    all_lines = expected_path(tmp_path).read_text(encoding="utf-8").splitlines()
    assert len(all_lines) == 5


def test_flush_persists_the_buffer_before_the_batch_boundary(
    tmp_path: Path,
) -> None:
    records = [article(f"Article {index}") for index in range(2)]

    with BronzeWriter(
        tmp_path / "bronze",
        "sciencedirect",
        "20260913120000",
        date(2026, 9, 13),
        flush_every=50,
    ) as writer:
        for record in records:
            writer.write(record)
        assert not expected_path(tmp_path).read_text(encoding="utf-8")

        writer.flush()

        # Durable mid-run, with the file still open for the rest of the batch.
        mid_run = expected_path(tmp_path).read_text(encoding="utf-8").splitlines()
        assert [json.loads(line) for line in mid_run] == [
            record.model_dump(mode="json") for record in records
        ]

        writer.flush()  # a second flush of an empty buffer writes nothing
        assert len(
            expected_path(tmp_path).read_text(encoding="utf-8").splitlines()
        ) == len(records)

        writer.write(article("Article after flush"))

    all_lines = expected_path(tmp_path).read_text(encoding="utf-8").splitlines()
    assert len(all_lines) == len(records) + 1


def test_exception_inside_context_still_flushes_buffer(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="interrupted"):
        with BronzeWriter(
            tmp_path / "bronze",
            "sciencedirect",
            "20260913120000",
            date(2026, 9, 13),
            flush_every=10,
        ) as writer:
            writer.write(article())
            raise RuntimeError("interrupted")

    flushed_lines = expected_path(tmp_path).read_text(
        encoding="utf-8"
    ).splitlines()
    assert len(flushed_lines) == 1


def test_output_preserves_unicode_and_never_contains_carriage_returns(
    tmp_path: Path,
) -> None:
    record = article()

    with BronzeWriter(
        tmp_path / "bronze",
        "sciencedirect",
        "20260913120000",
        date(2026, 9, 13),
        flush_every=1,
    ) as writer:
        writer.write(record)

    output = expected_path(tmp_path).read_bytes()
    assert "Café 中文".encode() in output
    assert b"\\u00e9" not in output
    assert b"\r" not in output
