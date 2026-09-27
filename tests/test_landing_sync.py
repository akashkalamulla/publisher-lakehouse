from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pytest
from moto import mock_aws
from pydantic import ValidationError

from publisher_lakehouse.landing.sync import (
    ensure_bucket,
    land_publisher,
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


def _moto_client():
    client = make_s3_client(
        LakeSettings(
            s3_endpoint_url="https://s3.amazonaws.com",
            s3_access_key="testing",
            s3_secret_key="testing-secret",
        )
    )
    ensure_bucket(client, "lakehouse")
    return client


@mock_aws
def test_land_uploads_one_marker_with_changed_keys_and_counts(tmp_path: Path) -> None:
    bronze, raw, _ = _tree(tmp_path)
    client = _moto_client()
    counters, marker_key = land_publisher(
        client, "lakehouse", "sciencedirect", bronze, raw,
        now=datetime(2026, 9, 27, 8, 9, 10, tzinfo=timezone.utc),
    )
    assert (counters.uploaded, counters.replaced) == (2, 0)
    assert marker_key == "landing/_markers/sciencedirect/20260927T080910Z.json"
    keys = [obj["Key"] for obj in client.list_objects_v2(Bucket="lakehouse")["Contents"]]
    assert keys.count(marker_key) == 1
    marker = json.loads(client.get_object(Bucket="lakehouse", Key=marker_key)["Body"].read())
    assert marker == {
        "publisher": "sciencedirect",
        "landed_at": "2026-09-27T08:09:10+00:00",
        "uploaded": 2,
        "replaced": 0,
        "jsonl_keys": [
            "landing/bronze_jsonl/sciencedirect/ingest_date=2026-09-26/part-run.jsonl"
        ],
        "html_count": 1,
    }


@mock_aws
def test_land_without_changes_writes_no_new_marker(tmp_path: Path) -> None:
    bronze, raw, _ = _tree(tmp_path)
    client = _moto_client()
    land_publisher(client, "lakehouse", "sciencedirect", bronze, raw)
    counters, marker_key = land_publisher(client, "lakehouse", "sciencedirect", bronze, raw)
    assert counters.uploaded == counters.replaced == 0
    assert marker_key is None
    keys = [obj["Key"] for obj in client.list_objects_v2(Bucket="lakehouse")["Contents"]]
    assert sum(key.startswith("landing/_markers/") for key in keys) == 1


@mock_aws
def test_upload_failure_does_not_write_marker(tmp_path: Path) -> None:
    bronze, raw, _ = _tree(tmp_path)
    client = _moto_client()

    class FailingClient:
        calls = 0

        def __getattr__(self, name):
            return getattr(client, name)

        def put_object(self, **kwargs):
            self.calls += 1
            if self.calls == 2:
                raise RuntimeError("simulated upload failure")
            return client.put_object(**kwargs)

    with pytest.raises(RuntimeError, match="simulated upload failure"):
        land_publisher(FailingClient(), "lakehouse", "sciencedirect", bronze, raw)
    keys = [obj["Key"] for obj in client.list_objects_v2(Bucket="lakehouse")["Contents"]]
    assert not any(key.startswith("landing/_markers/") for key in keys)


@mock_aws
def test_marker_name_and_schema_for_a_replacement(tmp_path: Path) -> None:
    bronze, raw, part = _tree(tmp_path)
    client = _moto_client()
    land_publisher(client, "lakehouse", "sciencedirect", bronze, raw)
    part.write_text('{"id": 2}\n', encoding="utf-8")
    _, key = land_publisher(
        client, "lakehouse", "sciencedirect", bronze, raw,
        now=datetime(2026, 9, 27, 11, 12, 13, tzinfo=timezone.utc),
    )
    assert key is not None
    assert re.fullmatch(r"landing/_markers/sciencedirect/\d{8}T\d{6}Z\.json", key)
    marker = json.loads(client.get_object(Bucket="lakehouse", Key=key)["Body"].read())
    assert set(marker) == {
        "publisher", "landed_at", "uploaded", "replaced", "jsonl_keys", "html_count",
    }
    assert (marker["uploaded"], marker["replaced"], marker["html_count"]) == (0, 1, 0)
    assert marker["jsonl_keys"] == [
        "landing/bronze_jsonl/sciencedirect/ingest_date=2026-09-26/part-run.jsonl"
    ]
