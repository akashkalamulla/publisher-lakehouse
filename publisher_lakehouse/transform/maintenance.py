"""Compact small Delta files in the tracked lake tables without changing data.

Each table is measured with ``DESCRIBE DETAIL``.  ``OPTIMIZE`` runs when the
table has more than one file and its files average under 32 MB.  A row count
and an order-independent checksum over every row's JSON are taken before and
after, pinned to the versions read, and must match; the new history entry
must be ``OPTIMIZE``.  ``VACUUM`` runs only when ``--vacuum-hours`` is given,
never below Delta's 7-day default retention, and Delta's retention check is
never disabled.  The job never publishes: the next publish sees the new
versions.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from dataclasses import dataclass
from typing import Mapping

from publisher_lakehouse.transform.publish import TRACKED_TABLES, lake_sources
from publisher_lakehouse.transform.session import build_spark


SMALL_FILE_BYTES = 32 * 1024 * 1024
MIN_VACUUM_HOURS = 168
RETENTION_CHECK = "spark.databricks.delta.retentionDurationCheck.enabled"


def check_vacuum_hours(hours: int | None) -> int | None:
    """Return the VACUUM retention to use; ``None`` means no VACUUM."""

    if hours is None:
        return None
    if hours < MIN_VACUUM_HOURS:
        raise ValueError(
            f"--vacuum-hours {hours} is below the {MIN_VACUUM_HOURS}-hour (7-day) minimum; "
            "a shorter retention can delete files that readers or time travel still need"
        )
    return hours


def plan_action(num_files: int, size_bytes: int, *, small_file_bytes: int = SMALL_FILE_BYTES) -> str:
    """Return ``optimize`` for a table of several small files, else ``skip``."""

    if num_files > 1 and size_bytes / num_files < small_file_bytes:
        return "optimize"
    return "skip"


def skip_reason(num_files: int, size_bytes: int, *, small_file_bytes: int = SMALL_FILE_BYTES) -> str:
    if num_files == 0:
        return "no files"
    if num_files == 1:
        return "single file"
    return f"average file {size_bytes / num_files / 1024 / 1024:.1f} MB >= {small_file_bytes / 1024 / 1024:.0f} MB"


@dataclass(frozen=True)
class Snapshot:
    version: int
    num_files: int
    size_bytes: int
    rows: int
    checksum: str


@dataclass(frozen=True)
class TableReport:
    table: str
    action: str
    reason: str
    before: Snapshot
    after: Snapshot
    vacuumed: bool = False

    def line(self) -> str:
        files = (
            f"files {self.before.num_files} -> {self.after.num_files}"
            if self.after.num_files != self.before.num_files or self.action == "optimized"
            else f"files {self.before.num_files}"
        )
        version = (
            f"version {self.before.version} -> {self.after.version}"
            if self.after.version != self.before.version
            else f"version {self.before.version}"
        )
        rows = f"rows {self.before.rows} -> {self.after.rows}"
        vacuum = " | vacuumed" if self.vacuumed else ""
        return (
            f"{self.table:<28} {self.action:<14} {files} | {rows} | {version} | "
            f"checksum {'unchanged' if self.after.checksum == self.before.checksum else 'CHANGED'}"
            f"{vacuum} ({self.reason})"
        )


def _latest_version(spark, uri: str) -> int:
    from delta.tables import DeltaTable

    return int(DeltaTable.forPath(spark, uri).history(1).first()["version"])


def data_fingerprint(frame) -> tuple[int, str]:
    """Row count and a SHA-256 over the sorted SHA-256 of each row's JSON."""

    from pyspark.sql import functions as F

    row_hash = F.sha2(F.to_json(F.struct(*[F.col(name) for name in frame.columns])), 256)
    result = frame.select(row_hash.alias("h")).agg(
        F.count("*").alias("rows"),
        F.sha2(F.concat_ws("", F.sort_array(F.collect_list("h"))), 256).alias("checksum"),
    ).first()
    return int(result["rows"]), str(result["checksum"])


def snapshot(spark, uri: str) -> Snapshot:
    """Measure one table at its current version, reading that version only."""

    version = _latest_version(spark, uri)
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{uri}`").first()
    frame = spark.read.format("delta").option("versionAsOf", version).load(uri)
    rows, checksum = data_fingerprint(frame)
    if _latest_version(spark, uri) != version:
        raise RuntimeError(f"{uri} changed while it was measured; another writer is active")
    return Snapshot(
        version=version, num_files=int(detail["numFiles"]),
        size_bytes=int(detail["sizeInBytes"]), rows=rows, checksum=checksum,
    )


def _optimize(spark, name: str, uri: str, before: Snapshot) -> None:
    from delta.tables import DeltaTable

    DeltaTable.forPath(spark, uri).optimize().executeCompaction()
    entries = (
        DeltaTable.forPath(spark, uri).history()
        .where(f"version > {before.version}").orderBy("version").collect()
    )
    operations = [entry["operation"] for entry in entries]
    if operations and operations != ["OPTIMIZE"]:
        raise RuntimeError(f"{name}: expected one OPTIMIZE commit after version {before.version}, got {operations}")


def _vacuum(spark, uri: str, hours: int) -> None:
    from delta.tables import DeltaTable

    if str(spark.conf.get(RETENTION_CHECK, "true")).lower() != "true":
        raise RuntimeError(f"{RETENTION_CHECK} is not enabled; refusing to VACUUM")
    DeltaTable.forPath(spark, uri).vacuum(float(hours))


def run_maintenance(
    spark, source_uris: Mapping[str, str], *, vacuum_hours: int | None = None,
    dry_run: bool = False, small_file_bytes: int = SMALL_FILE_BYTES,
) -> list[TableReport]:
    """Plan, compact, and verify each table; a dry run writes nothing."""

    vacuum_hours = check_vacuum_hours(vacuum_hours)
    reports: list[TableReport] = []
    for name, uri in source_uris.items():
        before = snapshot(spark, uri)
        action = plan_action(before.num_files, before.size_bytes, small_file_bytes=small_file_bytes)
        reason = (
            f"{before.num_files} files averaging "
            f"{before.size_bytes / max(before.num_files, 1) / 1024:.0f} KB"
            if action == "optimize"
            else skip_reason(before.num_files, before.size_bytes, small_file_bytes=small_file_bytes)
        )
        if dry_run:
            if vacuum_hours is not None:
                reason += f"; would VACUUM RETAIN {vacuum_hours} HOURS"
            label = "would-optimize" if action == "optimize" else "would-skip"
            reports.append(TableReport(name, label, reason, before, before))
            continue

        if action == "optimize":
            _optimize(spark, name, uri, before)
        if vacuum_hours is not None:
            _vacuum(spark, uri, vacuum_hours)
        after = snapshot(spark, uri)
        if (after.rows, after.checksum) != (before.rows, before.checksum):
            raise RuntimeError(
                f"{name}: data changed during maintenance: rows {before.rows} -> {after.rows}, "
                f"checksum {before.checksum[:12]} -> {after.checksum[:12]}"
            )
        if action == "skip":
            label = "skipped"
        elif after.version == before.version:
            label, reason = "no-op", reason + "; OPTIMIZE found nothing to compact"
        else:
            label = "optimized"
        reports.append(TableReport(
            name, label, reason, before, after, vacuumed=vacuum_hours is not None,
        ))
    return reports


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compact small files in the tracked Delta tables")
    parser.add_argument(
        "--vacuum-hours", type=int, default=None, metavar="N",
        help=f"also VACUUM with this retention; at least {MIN_VACUUM_HOURS}. Default: no VACUUM",
    )
    parser.add_argument("--dry-run", action="store_true", help="Report the plan without writing")
    args = parser.parse_args(argv)
    try:
        check_vacuum_hours(args.vacuum_hours)
    except ValueError as exc:
        print(f"Maintenance refused: {exc}", file=sys.stderr)
        return 2

    spark = None
    try:
        spark = build_spark("lakehouse-maintenance")
        reports = run_maintenance(
            spark, source_uris=lake_sources(),
            vacuum_hours=args.vacuum_hours, dry_run=args.dry_run,
        )
        mode = "dry run" if args.dry_run else "applied"
        vacuum = f"VACUUM RETAIN {args.vacuum_hours} HOURS" if args.vacuum_hours else "no VACUUM"
        print(f"Maintenance ({mode}, {vacuum}): {len(reports)} of {len(TRACKED_TABLES)} tracked tables")
        for report in reports:
            print("  " + report.line())
        counts = Counter(report.action for report in reports)
        print("Summary: " + " | ".join(f"{action} {n}" for action, n in sorted(counts.items())))
        return 0
    except Exception as exc:
        print(f"Maintenance failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
