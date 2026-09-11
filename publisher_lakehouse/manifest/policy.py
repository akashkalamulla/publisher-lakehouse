"""Whether a URL needs fetching this run.

Pure function, no I/O.  Everything it needs is the task, the manifest row that
was loaded once at run start, the refresh window for the URL's tier, and the
clock.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Protocol

from publisher_lakehouse.manifest.models import ManifestRow, require_aware


#: The reason returned when a URL is skipped.  It doubles as the run counter
#: name, so the pipeline can count the reason it is handed.
SKIP_REASON = "skipped_manifest"


class _HasUrlHash(Protocol):
    url_hash: str


def should_fetch(
    task: _HasUrlHash,
    manifest_row: ManifestRow | None,
    refresh_days: int,
    force_refetch: bool,
    now,
) -> tuple[bool, str]:
    """Return ``(fetch?, reason)``.  The reason is logged and counted."""

    require_aware(now, "now")

    if refresh_days < 0:
        raise ValueError(f"refresh_days cannot be negative, got {refresh_days}")

    if force_refetch:
        return True, "force_refetch"

    if manifest_row is None:
        return True, "new_url"

    if manifest_row.status == "blocked":
        # A Cloudflare block is not a successful collection.  Recording it as
        # one would suppress this article for the whole refresh window and
        # surface as an unexplained QA gap months later.
        return True, "retry_blocked"

    if manifest_row.status != "ok":
        return True, f"retry_{manifest_row.status}"

    if refresh_days == 0:
        # Discovery tiers are configured this way: a journal page that is not
        # re-read is a journal whose new issue is never noticed.
        return True, "always_fetch"

    require_aware(manifest_row.last_seen_at, "manifest_row.last_seen_at")

    if now - manifest_row.last_seen_at >= timedelta(days=refresh_days):
        return True, "stale"

    return False, SKIP_REASON
