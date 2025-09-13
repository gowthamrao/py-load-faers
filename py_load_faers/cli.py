# -*- coding: utf-8 -*-
"""
This module provides the command-line interface for the FAERS loader.
"""
import logging
from typing import Optional

import typer

from . import config
from . import downloader
from .engine import FaersLoaderEngine
from .models import FAERS_TABLE_MODELS
from py_load_faers_postgres.loader import PostgresLoader

app = typer.Typer(help="A high-performance ETL tool for FAERS data.")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@app.command()
def download(
    quarter: Optional[str] = typer.Option(
        None,
        "--quarter",
        "-q",
        help=(
            "The specific quarter to download (e.g., '2025q1'). "
            "If not provided, the latest will be downloaded."
        ),
    ),
    profile: str = typer.Option(
        "dev", "--profile", "-p", help="The configuration profile to use."
    ),
):
    """Download FAERS quarterly data files."""
    settings = config.load_config(profile=profile)

    target_quarter = quarter
    if not target_quarter:
        logger.info("No quarter specified, attempting to find the latest.")
        target_quarter = downloader.find_latest_quarter()

    if not target_quarter:
        logger.error(
            "Could not determine a quarter to download. " "Please specify one with --quarter."
        )
        raise typer.Exit(code=1)

    downloader.download_quarter(target_quarter, settings.downloader)
    logger.info("Download process finished.")


@app.command()
def run(
    quarter: Optional[str] = typer.Option(
        None,
        "--quarter",
        "-q",
        help=(
            "The specific quarter to process (e.g., '2025q1'). "
            "If not provided, the mode will determine behavior."
        ),
    ),
    mode: str = typer.Option(
        "delta",
        "--mode",
        "-m",
        help="The load mode: 'delta' (default) or 'partial'.",
    ),
    profile: str = typer.Option(
        "dev", "--profile", "-p", help="The configuration profile to use."
    ),
):
    """
    Run the FAERS ETL process.

    In 'delta' mode (default), it loads all new quarters since the last successful run.
    In 'partial' mode, it loads only the specific --quarter provided.
    """
    settings = config.load_config(profile=profile)

    if settings.db.type != "postgresql":
        typer.secho(
            f"Unsupported database type: {settings.db.type}. "
            "Only 'postgresql' is currently supported.",
            err=True,
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)

    db_loader = PostgresLoader(settings.db)
    try:
        db_loader.connect()
        engine = FaersLoaderEngine(config=settings, db_loader=db_loader)
        _, dq_message = engine.run_load(mode=mode, quarter=quarter)
        typer.secho(f"ETL process completed. {dq_message}", fg=typer.colors.GREEN)
    except Exception as e:
        typer.secho(f"An error occurred during the ETL process: {e}", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1)
    finally:
        if db_loader.conn:
            db_loader.conn.close()


@app.command()
def db_init(
    profile: str = typer.Option(
        "dev", "--profile", "-p", help="The configuration profile to use."
    ),
):
    """Initialize the database schema."""
    settings = config.load_config(profile=profile)

    # For now, we hardcode the PostgresLoader. This will be dynamic in the future.
    if settings.db.type != "postgresql":
        logger.error(
            f"Unsupported database type: {settings.db.type}. "
            "Only 'postgresql' is currently supported."
        )
        raise typer.Exit(code=1)

    loader = PostgresLoader(settings.db)

    try:
        loader.connect()
        loader.begin_transaction()
        logger.info("Initializing database schema...")
        # Pass the schema definition to the method
        loader.initialize_schema(FAERS_TABLE_MODELS)
        loader.commit()
        logger.info("Database schema initialization complete.")
    except Exception as e:
        logger.error(f"An error occurred during database initialization: {e}", exc_info=True)
        if loader.conn:
            loader.rollback()
        raise typer.Exit(code=1)
    finally:
        if loader.conn:
            loader.conn.close()


if __name__ == "__main__":
    app()


@app.command()
def db_verify(
    profile: str = typer.Option(
        "dev", "--profile", "-p", help="The configuration profile to use."
    ),
):
    """
    Run data quality checks on the existing database.
    Verifies the integrity of the loaded data, e.g., checking for duplicates.
    """
    settings = config.load_config(profile=profile)
    typer.echo(f"Running data quality verification on profile '{profile}'.")

    if settings.db.type != "postgresql":
        typer.secho(
            f"Unsupported database type: {settings.db.type}. "
            "Only 'postgresql' is currently supported.",
            err=True,
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)

    loader = PostgresLoader(settings.db)
    try:
        loader.connect()
        # No transaction needed for read-only checks
        passed, message = loader.run_post_load_dq_checks()
        if passed:
            typer.secho(message, fg=typer.colors.GREEN)
        else:
            typer.secho(message, err=True, fg=typer.colors.RED)
            raise typer.Exit(code=1)
    except Exception as e:
        typer.secho(f"Data quality verification failed: {e}", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1)
    finally:
        if loader.conn:
            loader.conn.close()
