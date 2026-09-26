from __future__ import annotations

from pathlib import Path

import pytest
from moto import mock_aws
from pydantic import ValidationError

from publisher_lakehouse.landing.sync import (
    ensure_bucket,
    make_s3_client,
    plan_uploads,
    sync_publisher,
)
from publisher_lakehouse.settings import LakeSettings


def _tree(tmp_path: Path) -> tuple[Path, Path, Path]:
    bronze = tmp_path / "bronze"
    raw = tmp_path / "raw_html"
    part = bronze / "sciencedirect" / "ingest_date=2026-09-26" / "part-run.jsonl"
    part.parent.mkdir(parents=True)
    part.write_text('{"id": 1}\n', encoding="utf-8")
    html = raw / "sciencedirect" / "2026-09-26" / "abc123.html.gz"
    html.parent.mkdir(parents=True)
    html.write_bytes(b"compressed fixture")
    return bronze, raw, part


@mock_aws
def test_sync_uploads_exact_keys_then_is_idempotent_then_replaces_one(
    tmp_path: Path,
) -> None:
    bronze, raw, part = _tree(tmp_path)
    client = make_s3_client(
        LakeSettings(
            s3_endpoint_url="https://s3.amazonaws.com",
            s3_access_key="testing",
            s3_secret_key="testing-secret",
        )
    )
    assert ensure_bucket(client, "lakehouse") is True
    assert ensure_bucket(client, "lakehouse") is False

    first = sync_publisher(client, "lakehouse", "sciencedirect", bronze, raw)
    assert first.files_seen == first.uploaded == 2
    assert first.unchanged == first.replaced == 0
    first.assert_balanced()
    keys = sorted(
        item["Key"]
        for item in client.list_objects_v2(Bucket="lakehouse")["Contents"]
    )
    assert keys == [
        "landing/bronze_jsonl/sciencedirect/ingest_date=2026-09-26/part-run.jsonl",
        "landing/raw_html/sciencedirect/2026-09-26/abc123.html.gz",
    ]
    assert [key for _, key in plan_uploads("sciencedirect", bronze, raw)] == keys

    second = sync_publisher(client, "lakehouse", "sciencedirect", bronze, raw)
    assert second.files_seen == second.unchanged == 2
    assert second.uploaded == second.replaced == second.bytes_uploaded == 0
    second.assert_balanced()

    with part.open("a", encoding="utf-8") as handle:
        handle.write('{"id": 2}\n')
    third = sync_publisher(client, "lakehouse", "sciencedirect", bronze, raw)
    assert third.files_seen == 2
    assert (third.uploaded, third.unchanged, third.replaced) == (0, 1, 1)
    assert third.bytes_uploaded == part.stat().st_size
    third.assert_balanced()


def test_empty_publisher_names_both_searched_directories(tmp_path: Path) -> None:
    bronze, raw = tmp_path / "bronze", tmp_path / "raw_html"
    with pytest.raises(FileNotFoundError) as exc:
        plan_uploads("sciencedirect", bronze, raw)
    assert str(bronze / "sciencedirect") in str(exc.value)
    assert str(raw / "sciencedirect") in str(exc.value)


def test_lake_settings_requires_access_key_and_hides_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("S3_ACCESS_KEY", raising=False)
    monkeypatch.delenv("S3_SECRET_KEY", raising=False)
    with pytest.raises(ValidationError, match="s3_access_key"):
        LakeSettings(_env_file=None, s3_secret_key="unique-secret-value")

    settings = LakeSettings(
        _env_file=None,
        s3_access_key="testing",
        s3_secret_key="unique-secret-value",
    )
    assert "unique-secret-value" not in repr(settings)
