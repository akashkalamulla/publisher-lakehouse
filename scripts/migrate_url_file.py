"""Convert the legacy ``urlDetails.txt`` list into ``inputs/url_details.csv``.

The legacy file is one bare URL per line with no publisher, no enable flag and
no room for a note.  Every line is migrated: the count is asserted rather than
trusted, because a silently dropped line is a journal that stops being
collected and nothing downstream would notice.

Usage::

    python scripts/migrate_url_file.py [SOURCE] [DESTINATION]
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = PROJECT_ROOT / "urlDetails.txt"
DEFAULT_DESTINATION = PROJECT_ROOT / "inputs" / "url_details.csv"
PUBLISHER = "sciencedirect"
HEADER = ("publisher", "url", "enabled", "note")


def read_urls(source: Path) -> list[str]:
    """Return every non-blank line of the legacy input file, in order."""

    text = source.read_text(encoding="utf-8-sig")
    return [line.strip() for line in text.splitlines() if line.strip()]


def note_for(url: str) -> str:
    """Tag the one bookseries URL so its distinct handling stays visible."""

    return "bookseries" if "/bookseries/" in url.lower() else ""


def write_csv(rows: list[str], destination: Path) -> int:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(HEADER)
        for url in rows:
            writer.writerow([PUBLISHER, url, "true", note_for(url)])
    return len(rows)


def migrate(source: Path, destination: Path) -> int:
    urls = read_urls(source)
    written = write_csv(urls, destination)

    # Re-read the file we just wrote rather than trusting the write.
    with destination.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        round_tripped = list(reader)

    if len(round_tripped) != len(urls):
        raise AssertionError(
            f"Migration lost rows: {len(urls)} URLs in, {len(round_tripped)} rows out"
        )
    if [row["url"] for row in round_tripped] != urls:
        raise AssertionError("Migration changed URL order or content")

    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", nargs="?", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("destination", nargs="?", type=Path, default=DEFAULT_DESTINATION)
    args = parser.parse_args(argv)

    written = migrate(args.source, args.destination)
    bookseries = sum(1 for url in read_urls(args.source) if note_for(url))
    print(
        f"Wrote {written} rows to {args.destination} "
        f"({bookseries} tagged as bookseries)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
