"""Shared pytest configuration for offline unit tests."""

from __future__ import annotations

import pytest

from publisher_lakehouse.common.logging import (
    clear_run_context,
    configure_logging,
    reset_error_events,
)


@pytest.fixture(autouse=True)
def isolated_logging():
    """Give every test a clean logging configuration and error buffer.

    Collected error events and the bound run id are process-global, so without
    this a test would see another test's failures.
    """

    configure_logging()
    yield
    clear_run_context()
    reset_error_events()
