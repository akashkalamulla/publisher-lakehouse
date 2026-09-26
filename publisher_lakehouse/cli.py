"""Command-line entry point for publisher ingestion."""

from __future__ import annotations

import asyncio
from typing import Annotated

import typer
from sqlalchemy import create_engine

from publisher_lakehouse.common.logging import configure_logging, get_logger
from publisher_lakehouse.export.journal_export import export_journals
from publisher_lakehouse.ingestion.pipeline import run_ingestion
from publisher_lakehouse.manifest.repository import ManifestRepository
from publisher_lakehouse.settings import load_settings


logger = get_logger(__name__)

app = typer.Typer(no_args_is_help=True)
ingest_app = typer.Typer(no_args_is_help=True)
export_app = typer.Typer(no_args_is_help=True)
app.add_typer(ingest_app, name="ingest")
app.add_typer(export_app, name="export")


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


if __name__ == "__main__":
    app()
