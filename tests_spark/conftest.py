"""Local-path fixtures for Spark tests inside the container."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from publisher_lakehouse.transform.schemas import BRONZE_SOURCE_COLUMNS
from publisher_lakehouse.transform.session import build_spark


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("S3_ACCESS_KEY", "test-access")
    os.environ.setdefault("S3_SECRET_KEY", "test-secret")
    os.environ.setdefault("S3_ENDPOINT_URL", "http://localhost:9000")
    session = build_spark("bronze-local-tests")
    yield session
    session.stop()


@pytest.fixture
def article_factory():
    def make(url_hash: str, run_id: str = "run-1", publisher: str = "sciencedirect"):
        record = {
            name: ([] if kind.startswith("ARRAY<") else "")
            for name, kind in BRONZE_SOURCE_COLUMNS
        }
        record.update(
            run_id=run_id,
            scraped_at="2026-09-26T05:35:06.839069Z",
            publisher=publisher,
            url_hash=url_hash,
            source_url=f"https://example.test/{url_hash}",
        )
        return record

    return make


@pytest.fixture
def local_lake(tmp_path: Path):
    publisher = "sciencedirect"
    landing = tmp_path / "landing" / "bronze_jsonl" / publisher
    raw = tmp_path / "landing" / "raw_html" / publisher
    table = tmp_path / "bronze" / "articles"

    def write(records: list[dict], part: str = "run-1", *, html: bool = True) -> Path:
        jsonl = landing / "ingest_date=2026-09-26" / f"part-{part}.jsonl"
        jsonl.parent.mkdir(parents=True, exist_ok=True)
        with jsonl.open("w", encoding="utf-8") as stream:
            for record in records:
                stream.write(json.dumps(record) + "\n")
        if html:
            html_dir = raw / "2026-09-26"
            html_dir.mkdir(parents=True, exist_ok=True)
            for record in records:
                (html_dir / f"{record['url_hash']}.html.gz").write_bytes(b"raw")
        return jsonl

    return {
        "write": write,
        "landing_uri": landing.as_uri(),
        "raw_html_uri": raw.as_uri(),
        "table_uri": table.as_uri(),
        "publisher": publisher,
    }
