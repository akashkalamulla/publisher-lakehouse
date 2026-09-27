"""Mirror immutable-looking Phase 1 files to the S3 landing zone."""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from publisher_lakehouse.settings import LakeSettings


MAX_SINGLE_PUT_BYTES = 100_000_000
_NOT_FOUND = {"404", "NoSuchBucket", "NoSuchKey", "NotFound"}


@dataclass(frozen=True)
class LandingCounters:
    files_seen: int = 0
    uploaded: int = 0
    unchanged: int = 0
    replaced: int = 0
    bytes_uploaded: int = 0
    jsonl_keys: tuple[str, ...] = ()
    html_count: int = 0

    def assert_balanced(self) -> None:
        if self.uploaded + self.unchanged + self.replaced != self.files_seen:
            raise RuntimeError("Landing counters do not balance")


def make_s3_client(settings: LakeSettings):
    """Use only the standard S3 API, with S3-compatible path addressing."""

    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key.get_secret_value(),
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def _is_not_found(exc: ClientError) -> bool:
    return str(exc.response.get("Error", {}).get("Code", "")) in _NOT_FOUND


def ensure_bucket(client, bucket: str) -> bool:
    """Create a missing bucket and return whether this call created it."""

    try:
        client.head_bucket(Bucket=bucket)
        return False
    except ClientError as exc:
        if not _is_not_found(exc):
            raise
    client.create_bucket(Bucket=bucket)
    return True


def plan_uploads(
    publisher: str, bronze_dir: str | Path, raw_html_dir: str | Path
) -> list[tuple[Path, str]]:
    """Find the publisher's Phase 1 files and construct OS-neutral S3 keys."""

    if not publisher or publisher in {".", ".."} or "/" in publisher or "\\" in publisher:
        raise ValueError(f"Invalid publisher name: {publisher!r}")

    bronze = Path(bronze_dir) / publisher
    raw = Path(raw_html_dir) / publisher
    planned: list[tuple[Path, str]] = []
    for root, pattern, prefix in (
        (bronze, "*.jsonl", "bronze_jsonl"),
        (raw, "*.html.gz", "raw_html"),
    ):
        if root.is_dir():
            for path in root.rglob(pattern):
                if path.is_file():
                    key = str(PurePosixPath("landing", prefix, publisher, *path.relative_to(root).parts))
                    planned.append((path, key))

    if not planned:
        raise FileNotFoundError(
            f"No bronze JSONL or raw HTML files found in {bronze} or {raw}"
        )
    return sorted(planned, key=lambda item: item[1])


def sync_publisher(
    client,
    bucket: str,
    publisher: str,
    bronze_dir: str | Path,
    raw_html_dir: str | Path,
) -> LandingCounters:
    """Upload new and changed files, verifying each single PUT with MD5."""

    planned = plan_uploads(publisher, bronze_dir, raw_html_dir)
    uploaded = unchanged = replaced = bytes_uploaded = html_count = 0
    jsonl_keys: list[str] = []
    for path, key in planned:
        size = path.stat().st_size
        if size > MAX_SINGLE_PUT_BYTES:
            raise ValueError(f"File exceeds 100 MB single-upload limit: {path}")

        data = path.read_bytes()
        if len(data) != size:
            raise RuntimeError(f"File changed while reading: {path}; rerun lake land")
        digest = hashlib.md5(data).digest()  # S3 ContentMD5 requires MD5.
        md5_hex = digest.hex()
        try:
            remote = client.head_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            if not _is_not_found(exc):
                raise
            remote = None

        if remote is not None:
            etag = str(remote.get("ETag", "")).strip('"')
            same_size = remote.get("ContentLength") == size
            if same_size and (etag == md5_hex or "-" in etag):
                unchanged += 1
                continue

        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentMD5=base64.b64encode(digest).decode("ascii"),
        )
        bytes_uploaded += size
        if remote is None:
            uploaded += 1
        else:
            replaced += 1
        if key.startswith("landing/bronze_jsonl/"):
            jsonl_keys.append(key)
        elif key.startswith("landing/raw_html/"):
            html_count += 1

    counters = LandingCounters(
        files_seen=len(planned),
        uploaded=uploaded,
        unchanged=unchanged,
        replaced=replaced,
        bytes_uploaded=bytes_uploaded,
        jsonl_keys=tuple(jsonl_keys),
        html_count=html_count,
    )
    counters.assert_balanced()
    return counters


def land_publisher(
    client,
    bucket: str,
    publisher: str,
    bronze_dir: str | Path,
    raw_html_dir: str | Path,
    *,
    now: datetime | None = None,
) -> tuple[LandingCounters, str | None]:
    """Sync files, then signal one complete landing batch if anything changed."""

    counters = sync_publisher(client, bucket, publisher, bronze_dir, raw_html_dir)
    if counters.uploaded + counters.replaced == 0:
        return counters, None

    landed_at = now or datetime.now(timezone.utc)
    if landed_at.tzinfo is None or landed_at.utcoffset() is None:
        raise ValueError("Landing marker timestamp must be timezone-aware")
    landed_at = landed_at.astimezone(timezone.utc)
    marker_key = (
        f"landing/_markers/{publisher}/"
        f"{landed_at.strftime('%Y%m%dT%H%M%SZ')}.json"
    )
    marker = {
        "publisher": publisher,
        "landed_at": landed_at.isoformat(),
        "uploaded": counters.uploaded,
        "replaced": counters.replaced,
        "jsonl_keys": list(counters.jsonl_keys),
        "html_count": counters.html_count,
    }
    client.put_object(
        Bucket=bucket,
        Key=marker_key,
        Body=json.dumps(marker, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )
    return counters, marker_key
