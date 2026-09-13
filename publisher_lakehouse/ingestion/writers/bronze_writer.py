"""Buffered JSON Lines writer for validated bronze article records."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Self, TextIO

from publisher_lakehouse.schemas.article import BronzeArticle


class BronzeWriter:
    """Append bronze records to one run-partitioned JSON Lines file."""

    def __init__(
        self,
        bronze_dir: str | Path,
        publisher: str,
        run_id: str,
        ingest_date: date,
        flush_every: int,
    ) -> None:
        if flush_every <= 0:
            raise ValueError("flush_every must be greater than zero")

        self.path = (
            Path(bronze_dir)
            / publisher
            / f"ingest_date={ingest_date:%Y-%m-%d}"
            / f"part-{run_id}.jsonl"
        )
        self.flush_every = flush_every
        self.written_count = 0
        self._buffer: list[str] = []
        self._file: TextIO | None = None

    def __enter__(self) -> Self:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("a", encoding="utf-8", newline="\n")
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        try:
            self._flush()
        finally:
            if self._file is not None:
                self._file.close()
                self._file = None

    def write(self, record: BronzeArticle) -> None:
        """Buffer one validated record and flush at the configured boundary."""

        if self._file is None:
            raise RuntimeError("BronzeWriter must be used as a context manager")

        self._buffer.append(
            json.dumps(record.model_dump(mode="json"), ensure_ascii=False) + "\n"
        )
        self.written_count += 1
        if len(self._buffer) >= self.flush_every:
            self._flush()

    def _flush(self) -> None:
        if not self._buffer:
            return
        if self._file is None:
            raise RuntimeError("BronzeWriter must be used as a context manager")

        self._file.writelines(self._buffer)
        self._file.flush()
        self._buffer.clear()
