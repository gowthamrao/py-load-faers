# -*- coding: utf-8 -*-
"""
This module contains the main ETL engine for the FAERS data loader.
"""
import io
import logging
import zipfile
from pathlib import Path
from typing import Dict, Any, List

from .config import AppSettings
from .database import AbstractDatabaseLoader
from .downloader import download_quarter
from .models import FAERS_TABLE_MODELS
from .parser import parse_ascii_file
from .processing import get_caseids_to_delete, deduplicate_records
from .staging import stage_data_for_copy

logger = logging.getLogger(__name__)


class FaersLoaderEngine:
    """Orchestrates the FAERS data loading process."""

    def __init__(self, config: AppSettings, db_loader: AbstractDatabaseLoader):
        self.config = config
        self.db_loader = db_loader

    def run_load(self, quarter: str, mode: str = "full"):
        """
        Runs the full ETL process for a given FAERS quarter.

        :param quarter: The quarter to load (e.g., "2025q1").
        :param mode: The load mode ('full' or 'delta'). Currently, only 'full'
                     style processing is implemented.
        """
        logger.info(f"Starting FAERS load for quarter {quarter} in '{mode}' mode.")

        # 1. Download
        zip_path = download_quarter(quarter, self.config.downloader)
        if not zip_path:
            logger.error(f"Download failed for quarter {quarter}. Aborting.")
            return

        # 2. Process Deletions (Nullifications)
        caseids_to_delete = get_caseids_to_delete(zip_path)

        # 3. Parse all relevant files from the archive
        # This is a simplified approach for the ASCII format.
        # It assumes all files are in a flat structure within the zip.
        parsed_data: Dict[str, List[Dict[str, Any]]] = {}
        with zipfile.ZipFile(zip_path, 'r') as zf:
            for table_name in FAERS_TABLE_MODELS.keys():
                # FAERS files are named like 'DEMO25Q1.txt'.
                # The quarter format is YYYYqQ, e.g., "2025q1".
                year_short = quarter[2:4]  # e.g., "25"
                quarter_num = quarter[-1]   # e.g., "1"
                file_pattern = f"{table_name.upper()}{year_short}Q{quarter_num}.txt"

                try:
                    # Find the exact filename from the zip archive, case-insensitively
                    data_filename = next(
                        f for f in zf.namelist() if f.upper() == file_pattern.upper()
                    )
                    logger.info(f"Found data file: {data_filename} for table {table_name}")

                    # Create a temporary path to the extracted file
                    # This is necessary because our parser expects a Path object
                    zf.extract(data_filename, path=self.config.downloader.download_dir)
                    extracted_file_path = Path(self.config.downloader.download_dir) / data_filename

                    records = list(parse_ascii_file(extracted_file_path))
                    parsed_data[table_name] = records

                    # Clean up extracted file
                    extracted_file_path.unlink()

                except StopIteration:
                    logger.warning(f"Could not find data file for table '{table_name}' in {zip_path}")
                    parsed_data[table_name] = []

        if 'demo' not in parsed_data or not parsed_data['demo']:
            logger.error("No demographic data (DEMO) found or parsed. Cannot proceed with deduplication.")
            return

        # 4. Deduplicate
        primaryids_to_keep = deduplicate_records(parsed_data['demo'])

        # 5. Filter, Stage, and Load each table's data
        self.db_loader.connect()
        self.db_loader.begin_transaction()
        try:
            for table_name, records in parsed_data.items():
                logger.info(f"Processing table: {table_name}")

                # Filter records based on deletions and deduplication
                filtered_records = [
                    r for r in records
                    if r.get('caseid') not in caseids_to_delete and
                       r.get('primaryid') in primaryids_to_keep
                ]

                if not filtered_records:
                    logger.info(f"No records to load for table '{table_name}' after filtering.")
                    continue

                logger.info(f"Filtered {len(records)} records down to {len(filtered_records)} for table '{table_name}'.")

                # Stage data for COPY
                model = FAERS_TABLE_MODELS[table_name]
                staged_data_buffer = stage_data_for_copy(iter(filtered_records), model)

                # Convert StringIO to a binary stream (BytesIO) for the loader
                staged_data_bytes = io.BytesIO(staged_data_buffer.read().encode('utf-8'))
                staged_data_buffer.close()

                # Load data
                self.db_loader.execute_native_bulk_load(table_name, staged_data_bytes)

            self.db_loader.commit()
            logger.info("Successfully loaded all tables for quarter {quarter}.")

        except Exception as e:
            logger.error(f"An error occurred during the loading process: {e}")
            self.db_loader.rollback()
            logger.error("Transaction has been rolled back.")
            raise
        finally:
            # Clean up the downloaded zip file
            zip_path.unlink()
            logger.info(f"Cleaned up downloaded file: {zip_path}")
