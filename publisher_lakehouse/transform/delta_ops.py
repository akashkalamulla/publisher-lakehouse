"""Shared, planned Delta MERGE execution with no-op and metric checks."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class MergeOutcome:
    skipped: bool
    version: int
    inserted: int
    updated: int
    deleted: int
    files_added: int
    metrics: dict[str, str] = field(default_factory=dict)


def merge_with_plan(
    spark,
    *,
    table_uri: str,
    source_view: str,
    merge_sql: str,
    planned_inserts: int,
    planned_updates: int,
    planned_deletes: int = 0,
) -> MergeOutcome:
    """Skip a true no-op; otherwise require Delta to match the computed plan."""

    from delta.tables import DeltaTable

    if min(planned_inserts, planned_updates, planned_deletes) < 0:
        raise ValueError("Planned MERGE counts must be non-negative")
    if not spark.catalog.tableExists(source_view):
        raise ValueError(f"MERGE source view does not exist: {source_view}")

    version_before = int(DeltaTable.forPath(spark, table_uri).history(1).first()["version"])
    if planned_inserts == planned_updates == planned_deletes == 0:
        return MergeOutcome(
            skipped=True, version=version_before, inserted=0, updated=0,
            deleted=0, files_added=0
        )

    spark.sql(merge_sql)
    latest = DeltaTable.forPath(spark, table_uri).history(1).first()
    version = int(latest["version"])
    if latest["operation"] != "MERGE":
        raise RuntimeError(f"Expected MERGE history entry, got {latest['operation']}")
    if version != version_before + 1:
        raise RuntimeError(f"MERGE version {version} did not follow prior version {version_before}")

    metrics = dict(latest["operationMetrics"])
    required = (
        "numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsDeleted",
        "numTargetFilesAdded",
    )
    for key in required:
        if key not in metrics:
            raise RuntimeError(f"MERGE metric {key} missing; available: {sorted(metrics)}")
    inserted = int(metrics["numTargetRowsInserted"])
    updated = int(metrics["numTargetRowsUpdated"])
    deleted = int(metrics["numTargetRowsDeleted"])
    if (inserted, updated, deleted) != (
        planned_inserts, planned_updates, planned_deletes
    ):
        raise RuntimeError(
            f"MERGE metrics disagree with plan: planned inserts={planned_inserts}, "
            f"updates={planned_updates}, deletes={planned_deletes}; "
            f"actual inserts={inserted}, updates={updated}, deletes={deleted}"
        )
    return MergeOutcome(
        skipped=False,
        version=version,
        inserted=inserted,
        updated=updated,
        deleted=deleted,
        files_added=int(metrics["numTargetFilesAdded"]),
        metrics=metrics,
    )
