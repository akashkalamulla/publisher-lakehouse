"""Command-line entry point for publisher ingestion."""

from __future__ import annotations

import asyncio
from dataclasses import asdict
from pathlib import Path
from typing import Annotated

import typer
from sqlalchemy import create_engine

from publisher_lakehouse.common.logging import configure_logging, get_logger
from publisher_lakehouse.export.journal_export import export_journals
from publisher_lakehouse.ingestion.pipeline import run_ingestion
from publisher_lakehouse.landing.sync import (
    ensure_bucket,
    land_publisher,
    make_s3_client,
    plan_uploads,
)
from publisher_lakehouse.manifest.repository import ManifestRepository
from publisher_lakehouse.ops.scrape_health import push_scrape_health
from publisher_lakehouse.settings import (
    load_environment_settings,
    load_lake_settings,
    load_operator_settings,
    load_settings,
    load_warehouse_settings,
)


logger = get_logger(__name__)

app = typer.Typer(no_args_is_help=True)
ingest_app = typer.Typer(no_args_is_help=True)
export_app = typer.Typer(no_args_is_help=True)
lake_app = typer.Typer(no_args_is_help=True)
ops_app = typer.Typer(no_args_is_help=True)
app.add_typer(ingest_app, name="ingest")
app.add_typer(export_app, name="export")
app.add_typer(lake_app, name="lake")
app.add_typer(ops_app, name="ops")


@lake_app.command("init")
def lake_init() -> None:
    """Ensure the lake bucket exists."""

    settings = load_lake_settings()
    client = make_s3_client(settings)
    created = ensure_bucket(client, settings.lake_bucket)
    state = "created" if created else "already existed"
    typer.echo(
        f"Bucket '{settings.lake_bucket}' ready at {settings.s3_endpoint_url} ({state})"
    )


@lake_app.command("land")
def lake_land(
    publisher: Annotated[
        str,
        typer.Option("--publisher", help="Publisher whose Phase 1 files to land."),
    ] = "sciencedirect",
) -> None:
    """Mirror local bronze JSONL and compressed raw HTML into the lake."""

    lake = load_lake_settings()
    paths = load_operator_settings().paths
    configure_logging()
    client = make_s3_client(lake)
    ensure_bucket(client, lake.lake_bucket)
    planned = plan_uploads(publisher, paths.bronze_dir, paths.raw_html_dir)
    counters, marker_key = land_publisher(
        client, lake.lake_bucket, publisher, paths.bronze_dir, paths.raw_html_dir
    )
    jsonl = sum(key.startswith("landing/bronze_jsonl/") for _, key in planned)
    html = sum(key.startswith("landing/raw_html/") for _, key in planned)
    logger.info("landing_sync_complete", publisher=publisher, **asdict(counters))
    typer.echo(
        f"Landed {publisher}: {jsonl} JSONL + {html} HTML | "
        f"uploaded {counters.uploaded} | unchanged {counters.unchanged} | "
        f"replaced {counters.replaced} | "
        f"{counters.bytes_uploaded / (1024 * 1024):.1f} MB"
    )
    typer.echo(f"marker: {marker_key}" if marker_key else "no marker: nothing new landed")


@ingest_app.command("run")
def ingest_run(
    publisher: Annotated[
        str,
        typer.Option("--publisher", help="Publisher adapter to run."),
    ] = "sciencedirect",
    limit: Annotated[
        int | None,
        typer.Option("--limit", min=1, help="Truncate the enabled input list."),
    ] = None,
    force_refetch: Annotated[
        bool,
        typer.Option(
            "--force-refetch",
            help="Fetch articles even when the manifest says they are fresh.",
        ),
    ] = False,
) -> None:
    """Run one publisher ingestion."""

    settings = load_settings()
    configure_logging(settings.environment.log_level)

    if limit is not None:
        logger.warning(
            "ingestion_coverage_truncated",
            warning="COVERAGE TRUNCATED: --limit excludes enabled input rows",
            publisher=publisher,
            limit=limit,
        )

    counters = asyncio.run(
        run_ingestion(
            publisher,
            limit=limit,
            force_refetch=force_refetch,
            settings=settings,
        )
    )

    try:
        counters.assert_balanced()
    except RuntimeError as exc:
        logger.error("ingestion_counter_sum_invalid", error=str(exc))
        raise typer.Exit(code=1) from exc


@export_app.command("run")
def export_run(
    publisher: Annotated[
        str,
        typer.Option("--publisher", help="Publisher whose bronze records to export."),
    ] = "sciencedirect",
    journal_url: Annotated[
        str | None,
        typer.Option("--journal-url", help="Export only this literal journal URL."),
    ] = None,
) -> None:
    """Export manifest-OK bronze records and their raw HTML by journal."""

    settings = load_settings()
    configure_logging(settings.environment.log_level)
    engine = create_engine(settings.environment.database_url)
    try:
        manifest = ManifestRepository(engine).load_for_publisher(publisher)
        exported = export_journals(
            publisher,
            bronze_root=settings.operator.paths.bronze_dir,
            raw_html_root=settings.operator.paths.raw_html_dir,
            out_dir=settings.operator.details.output_path / "out",
            manifest=manifest,
            journal_url=journal_url,
        )
    finally:
        engine.dispose()

    logger.info("journal_export_complete", publisher=publisher, journals=len(exported))


@ops_app.command("push-scrape-health")
def ops_push_scrape_health(
    publisher: Annotated[
        str,
        typer.Option("--publisher", help="Publisher whose scrape health to push."),
    ] = "sciencedirect",
    run_log: Annotated[
        Path | None,
        typer.Option("--run-log", help="An ingest run's stderr log to record as a run."),
    ] = None,
    ingest_exit_code: Annotated[
        int | None,
        typer.Option("--ingest-exit-code", help="Exit code of the ingest run."),
    ] = None,
) -> None:
    """Copy manifest health and run counters into warehouse ops tables."""

    environment = load_environment_settings()
    warehouse = load_warehouse_settings()
    configure_logging(environment.log_level)
    result = push_scrape_health(
        publisher,
        database_url=environment.database_url,
        warehouse=warehouse,
        run_log=run_log,
        ingest_exit_code=ingest_exit_code,
    )
    logger.info(
        "scrape_health_pushed",
        publisher=publisher,
        run_id=result.run["run_id"] if result.run else None,
        manifest_rows=result.manifest_rows,
        journal_rows=result.journal_rows,
    )
    typer.echo(result.summary())


if __name__ == "__main__":
    app()
