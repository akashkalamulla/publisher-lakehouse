"""Structured logging and run-scoped error collection."""

from __future__ import annotations

import logging
import re
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path
from threading import Lock
from typing import Any

import structlog
from structlog.contextvars import bind_contextvars, clear_contextvars, merge_contextvars


_ERROR_METHODS = frozenset({"error", "exception", "critical"})
_ERROR_META_KEYS = frozenset(
    {"event", "error_line", "exception", "level", "run_id", "timestamp"}
)
_error_events: list[dict[str, Any]] = []
_error_lock = Lock()
_output_level = logging.INFO


class _LazyStderr:
    """Resolve ``sys.stderr`` per write instead of freezing it at configure time.

    A run that redirects stderr after configuring logging — or a test that
    captures it — must still receive the events.
    """

    def write(self, data: str) -> int:
        return sys.stderr.write(data)

    def flush(self) -> None:
        sys.stderr.flush()


_LOG_STREAM = _LazyStderr()


def _collect_error_event(
    _logger: Any,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    if method_name in _ERROR_METHODS:
        with _error_lock:
            _error_events.append(dict(event_dict))
    return event_dict


def _filter_output_level(
    _logger: Any,
    _method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    event_level = logging.getLevelNamesMapping().get(
        str(event_dict.get("level", "INFO")).upper(),
        logging.INFO,
    )
    if event_level < _output_level:
        raise structlog.DropEvent
    return event_dict


def configure_logging(log_level: str = "INFO", *, clear_errors: bool = True) -> None:
    """Configure newline-delimited JSON logging to standard error.

    The frozen browser layer prints human-readable progress to stdout, so the
    machine-readable stream has to be stderr.  Anything that reads this run's
    logs programmatically reads stderr.
    """

    global _output_level

    level_name = log_level.upper()
    level = getattr(logging, level_name, None)
    if not isinstance(level, int):
        raise ValueError(f"Unknown log level: {log_level}")
    if clear_errors:
        reset_error_events()
    _output_level = level

    structlog.configure(
        processors=[
            merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            _collect_error_event,
            _filter_output_level,
            structlog.processors.JSONRenderer(),
        ],
        # Keep all events flowing through the collector.  The processor above
        # applies the configured output threshold only after ERROR capture.
        wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
        logger_factory=structlog.PrintLoggerFactory(file=_LOG_STREAM),
        cache_logger_on_first_use=False,
    )


def bind_run_id(run_id: str) -> None:
    """Replace the logging context with the current run identifier."""

    clear_contextvars()
    bind_contextvars(run_id=run_id)


def clear_run_context() -> None:
    clear_contextvars()


def get_logger(*args: Any, **initial_values: Any):
    return structlog.get_logger(*args, **initial_values)


def reset_error_events() -> None:
    with _error_lock:
        _error_events.clear()


def collected_error_events() -> tuple[dict[str, Any], ...]:
    with _error_lock:
        return tuple(dict(event) for event in _error_events)


def _render_error_event(event: Mapping[str, Any]) -> str:
    explicit_line = event.get("error_line")
    if explicit_line is not None:
        line = str(explicit_line)
    else:
        parts = [str(event.get("event", "error"))]
        for key, value in event.items():
            if key not in _ERROR_META_KEYS:
                parts.append(f"{key}={value}")
        if event.get("exception"):
            parts.append(str(event["exception"]))
        line = " | ".join(parts)
    return re.sub(r"\r\n?|\n", lambda _match: "\\n", line)


def render_error_lines(
    events: Iterable[Mapping[str, Any]] | None = None,
) -> list[str]:
    """Render collected error events as one physical line per event."""

    source = collected_error_events() if events is None else events
    return [_render_error_event(event) for event in source]


def write_error_report(
    path: str | Path,
    events: Iterable[Mapping[str, Any]] | None = None,
) -> Path:
    """Write the run's compatibility ``errors.txt`` report when explicitly called."""

    report_path = Path(path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    lines = render_error_lines(events)
    with report_path.open("w", encoding="utf-8", newline="\n") as handle:
        for line in lines:
            handle.write(f"{line}\n")
    return report_path
