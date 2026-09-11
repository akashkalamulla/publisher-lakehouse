"""Run identifiers."""

from __future__ import annotations

from datetime import datetime


RUN_ID_FORMAT = "%Y%m%d%H%M%S"


def new_run_id(now: datetime | None = None) -> str:
    """Create the legacy-compatible timestamp shared by an ingestion run."""

    return (now or datetime.now()).strftime(RUN_ID_FORMAT)

