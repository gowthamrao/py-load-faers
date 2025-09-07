# -*- coding: utf-8 -*-
"""
This module provides the command-line interface for the FAERS loader.
"""
import logging
from typing import Optional

import typer

from . import config
from . import downloader
from py_load_faers_postgres.loader import PostgresLoader

app = typer.Typer(help="A high-performance ETL tool for FAERS data.")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@app.command()
def download(
    quarter: Optional[str] = typer.Option(
        None, "--quarter", "-q", help="The specific quarter to download (e.g., '2025q1'). If not provided, the latest will be downloaded."
    ),
    profile: str = typer.Option("dev", "--profile", "-p", help="The configuration profile to use."),
):
    """Download FAERS quarterly data files."""
    settings = config.load_config(profile=profile)

    target_quarter = quarter
    if not target_quarter:
        logger.info("No quarter specified, attempting to find the latest.")
        target_quarter = downloader.find_latest_quarter()

    if not target_quarter:
        logger.error("Could not determine a quarter to download. Please specify one with --quarter.")
        raise typer.Exit(code=1)

    downloader.download_quarter(target_quarter, settings.downloader)
    logger.info("Download process finished.")


import tempfile
import zipfile
from pathlib import Path
import io

from . import staging
from . import models
from . import parser

@app.command()
def run(
    quarter: str = typer.Option(
        ..., "--quarter", "-q", help="The specific quarter to process (e.g., '2025q1')."
    ),
    profile: str = typer.Option("dev", "--profile", "-p", help="The configuration profile to use."),
):
    """Download, parse, and load a full FAERS quarter into the database."""
    settings = config.load_config(profile=profile)
    loader = PostgresLoader(settings.db)

    try:
        # 1. Download
        logger.info(f"Starting ETL process for quarter {quarter}")
        zip_path = downloader.download_quarter(quarter, settings.downloader)
        if not zip_path:
            raise Exception("Download failed.")

        # 2. Unzip and process
        with tempfile.TemporaryDirectory() as temp_dir:
            logger.info(f"Unzipping {zip_path} to {temp_dir}")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)

            # 3. Load data in a single transaction
            loader.connect()
            loader.begin_transaction()

            table_map = {
                "demo": models.Demo, "drug": models.Drug, "reac": models.Reac,
                "outc": models.Outc, "rpsr": models.Rpsr, "ther": models.Ther,
                "indi": models.Indi,
            }

            for table_name, model in table_map.items():
                # Find the text file for the table (e.g., DEMO25Q1.txt)
                file_pattern = f"{table_name.upper()}{quarter.replace('q', '')}*.txt"
                data_files = list(Path(temp_dir).glob(f"**/{file_pattern}"))

                if not data_files:
                    logger.warning(f"No data file found for table '{table_name}' with pattern '{file_pattern}'. Skipping.")
                    continue

                data_file = data_files[0]

                # 4. Parse, Stage, and Load
                logger.info(f"Processing {data_file} for table '{table_name}'")
                parsed_records = parser.parse_ascii_file(data_file)
                staged_buffer = staging.stage_data_for_copy(parsed_records, model)

                # Convert StringIO to BytesIO for the loader
                bytes_buffer = io.BytesIO(staged_buffer.getvalue().encode('utf-8'))

                loader.execute_native_bulk_load(table_name, bytes_buffer)

            logger.info("Committing transaction.")
            loader.commit()

    except Exception as e:
        logger.error(f"An error occurred during the ETL process: {e}")
        if loader.conn and not loader.conn.autocommit:
            logger.warning("Rolling back transaction.")
            loader.rollback()
        raise typer.Exit(code=1)
    finally:
        if loader.conn:
            loader.conn.close()

    logger.info(f"Successfully loaded FAERS data for quarter {quarter}.")


@app.command()
def db_init(
    profile: str = typer.Option("dev", "--profile", "-p", help="The configuration profile to use."),
):
    """Initialize the database schema."""
    settings = config.load_config(profile=profile)

    # For now, we hardcode the PostgresLoader. This will be dynamic in the future.
    if settings.db.type != "postgresql":
        logger.error(f"Unsupported database type: {settings.db.type}. Only 'postgresql' is currently supported.")
        raise typer.Exit(code=1)

    loader = PostgresLoader(settings.db)

    try:
        loader.connect()
        logger.info("Initializing database schema...")
        loader.initialize_schema()
        logger.info("Database schema initialization complete.")
    except Exception as e:
        logger.error(f"An error occurred during database initialization: {e}")
        raise typer.Exit(code=1)
    finally:
        if loader.conn:
            loader.conn.close()


if __name__ == "__main__":
    app()
