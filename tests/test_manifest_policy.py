from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from publisher_lakehouse.manifest.models import ManifestRow
from publisher_lakehouse.manifest.policy import SKIP_REASON, should_fetch


NOW = datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc)
ARTICLE_DAYS = 90


class Task:
    def __init__(self, url_hash: str = "abc123") -> None:
        self.url_hash = url_hash


def row(
    *,
    status: str = "ok",
    age_days: float = 1.0,
    last_seen_at: datetime | None = None,
) -> ManifestRow:
    seen = last_seen_at if last_seen_at is not None else NOW - timedelta(days=age_days)
    return ManifestRow(
        url_hash="abc123",
        normalised_url="https://www.sciencedirect.com/science/article/pii/S1",
        publisher="sciencedirect",
        url_type="article",
        doi="10.1000/test",
        payload_hash="0" * 64,
        first_seen_at=seen,
        last_seen_at=seen,
        last_changed_at=seen,
        fetch_count=1,
        last_http_status=200,
        last_run_id="20260901120000",
        status=status,
    )


def test_unknown_url_is_fetched() -> None:
    assert should_fetch(Task(), None, ARTICLE_DAYS, False, NOW) == (True, "new_url")


def test_fresh_ok_row_inside_the_window_is_skipped() -> None:
    fetch, reason = should_fetch(Task(), row(age_days=1), ARTICLE_DAYS, False, NOW)
    assert fetch is False
    assert reason == SKIP_REASON


def test_stale_ok_row_is_refetched() -> None:
    assert should_fetch(Task(), row(age_days=91), ARTICLE_DAYS, False, NOW) == (
        True,
        "stale",
    )


def test_row_exactly_at_the_window_boundary_is_refetched() -> None:
    fetch, reason = should_fetch(Task(), row(age_days=90), ARTICLE_DAYS, False, NOW)
    assert (fetch, reason) == (True, "stale")


def test_failed_row_is_retried_inside_the_window() -> None:
    assert should_fetch(Task(), row(status="failed"), ARTICLE_DAYS, False, NOW) == (
        True,
        "retry_failed",
    )


def test_blocked_row_always_refetches_however_fresh() -> None:
    # A Cloudflare block recorded as a collection would suppress the article
    # for the entire refresh window.
    fetch, reason = should_fetch(
        Task(), row(status="blocked", age_days=0.001), ARTICLE_DAYS, False, NOW
    )
    assert (fetch, reason) == (True, "retry_blocked")


@pytest.mark.parametrize(
    "status",
    ["not_found", "parse_error", "skipped_in_progress"],
)
def test_non_ok_statuses_are_retried(status: str) -> None:
    fetch, reason = should_fetch(Task(), row(status=status), ARTICLE_DAYS, False, NOW)
    assert fetch is True
    assert reason == f"retry_{status}"


def test_refresh_days_zero_always_fetches() -> None:
    # Discovery tiers run with a zero window; a journal page that is not
    # re-read never reveals a new issue.
    assert should_fetch(Task(), row(age_days=0.0), 0, False, NOW) == (
        True,
        "always_fetch",
    )


def test_force_refetch_overrides_a_fresh_row() -> None:
    assert should_fetch(Task(), row(age_days=1), ARTICLE_DAYS, True, NOW) == (
        True,
        "force_refetch",
    )


def test_force_refetch_on_an_unknown_url_still_fetches() -> None:
    assert should_fetch(Task(), None, ARTICLE_DAYS, True, NOW)[0] is True


def test_naive_now_raises_rather_than_shifting_the_window() -> None:
    with pytest.raises(ValueError) as excinfo:
        should_fetch(Task(), row(), ARTICLE_DAYS, False, NOW.replace(tzinfo=None))

    assert "timezone-aware" in str(excinfo.value)


def test_naive_stored_timestamp_raises() -> None:
    # SQLite would hand back a naive value silently; comparing it to an aware
    # ``now`` must fail loudly rather than shift the window.
    naive = row(last_seen_at=NOW.replace(tzinfo=None))

    with pytest.raises(ValueError) as excinfo:
        should_fetch(Task(), naive, ARTICLE_DAYS, False, NOW)

    assert "last_seen_at" in str(excinfo.value)


def test_naive_timestamp_is_not_reached_when_the_answer_is_already_known() -> None:
    # force_refetch and non-ok statuses short-circuit before any comparison.
    naive = row(status="blocked", last_seen_at=NOW.replace(tzinfo=None))
    assert should_fetch(Task(), naive, ARTICLE_DAYS, False, NOW)[0] is True


def test_negative_refresh_window_raises() -> None:
    with pytest.raises(ValueError):
        should_fetch(Task(), row(), -1, False, NOW)


def test_repeated_calls_are_pure(capsys) -> None:
    first = should_fetch(Task(), row(age_days=1), ARTICLE_DAYS, False, NOW)
    second = should_fetch(Task(), row(age_days=1), ARTICLE_DAYS, False, NOW)

    assert first == second == (False, SKIP_REASON)
    # No I/O, and nothing logged: the caller logs the reason it is handed.
    assert capsys.readouterr() == ("", "")
