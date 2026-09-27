"""Pure marker contract shared by landing and Airflow."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import boto3
from moto import mock_aws

from dags.pl_markers import marker_key, pending_marker_keys, processed_key


@mock_aws
def test_pending_is_markers_minus_processed_siblings():
    first = marker_key("sciencedirect", datetime(2026, 9, 27, tzinfo=timezone.utc))
    second = marker_key("sciencedirect", datetime(2026, 9, 27, 1, tzinfo=timezone.utc))
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="lakehouse")
    for key in (first, second, processed_key(first)):
        client.put_object(Bucket="lakehouse", Key=key, Body=b"{}")
    keys = [item["Key"] for item in client.list_objects_v2(Bucket="lakehouse")["Contents"]]
    assert pending_marker_keys(keys, "sciencedirect") == [second]


def test_pending_keys_are_sorted_and_publisher_scoped():
    start = datetime(2026, 9, 27, tzinfo=timezone.utc)
    early = marker_key("sciencedirect", start)
    late = marker_key("sciencedirect", start + timedelta(hours=1))
    other = marker_key("other", start)
    assert pending_marker_keys([late, other, early], "sciencedirect") == [early, late]


def test_orphan_processed_object_is_ignored():
    marker = marker_key("sciencedirect", datetime(2026, 9, 27, tzinfo=timezone.utc))
    assert pending_marker_keys([processed_key(marker)], "sciencedirect") == []


def test_marker_name_is_utc_and_acknowledgement_is_a_sibling():
    local = datetime(2026, 9, 27, 13, 30, tzinfo=timezone(timedelta(hours=5, minutes=30)))
    key = marker_key("sciencedirect", local)
    assert key == "landing/_markers/sciencedirect/20260927T080000Z.json"
    assert processed_key(key) == key + ".processed"
