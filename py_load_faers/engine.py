# -*- coding: utf-8 -*-
"""
This module contains the main ETL engine for the FAERS data loader.
"""
import csv
import logging
import shutil
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Generator, Set

from .config import AppSettings
from .database import AbstractDatabaseLoader
from .downloader import download_quarter, find_latest_quarter
from .models import FAERS_TABLE_MODELS
from .parser import parse_xml_file
from .processing import deduplicate_polars
from .staging import stage_data_to_csv_files

logger = logging.getLogger(__name__)


def _generate_quarters_to_load(
    start_quarter: Optional[str], end_quarter: str
) -> Generator[str, None, None]:
    """Generates a sequence of quarters from a start to an end point."""
    if not start_quarter:
        start_year, start_q = int(end_quarter[:4]), int(end_quarter[-1])
    else:
        start_year, start_q = int(start_quarter[:4]), int(start_quarter[-1])

    end_year, end_q = int(end_quarter[:4]), int(end_quarter[-1])

    current_year, current_q = start_year, start_q
    while current_year < end_year or (current_year == end_year and current_q <= end_q):
        yield f"{current_year}q{current_q}"
        current_q += 1
        if current_q > 4:
            current_q = 1
            current_year += 1


class FaersLoaderEngine:
    """Orchestrates the FAERS data loading process."""

    def __init__(self, config: AppSettings, db_loader: AbstractDatabaseLoader):
        self.config = config
        self.db_loader = db_loader

    def run_load(self, mode: str = "delta", quarter: Optional[str] = None):
        """Runs the full ETL process."""
        logger.info(f"Starting FAERS load in '{mode}' mode.")
        self.db_loader.begin_transaction()
        try:
            if quarter:
                self._process_quarter(quarter, "PARTIAL")
            elif mode == "delta":
                last_loaded = self.db_loader.get_last_successful_load()
                latest_available = find_latest_quarter()
                if not latest_available:
                    logger.warning(
                        "Could not determine the latest available quarter. Aborting."
                    )
                    self.db_loader.commit()
                    return

                if last_loaded and last_loaded.lower() >= latest_available.lower():
                    logger.info(
                        "Database is already up-to-date. No new quarters to load."
                    )
                    self.db_loader.commit()
                    return

                start_quarter = None
                if last_loaded:
                    year, q = int(last_loaded[:4]), int(last_loaded[-1])
                    q += 1
                    if q > 4:
                        q = 1
                        year += 1
                    start_quarter = f"{year}q{q}"
                else:
                    logger.info(
                        "No previous successful load found. Assuming this is the first run in a delta sequence."
                    )
                    start_quarter = latest_available

                quarters_to_load = _generate_quarters_to_load(
                    start_quarter, latest_available
                )
                for q_to_load in quarters_to_load:
                    self._process_quarter(q_to_load, "DELTA")
            else:
                logger.error(f"Unsupported load mode: {mode}")
                raise NotImplementedError(f"Unsupported load mode: {mode}")

            self.db_loader.commit()
            logger.info(
                "Load process completed successfully and transaction committed."
            )

            # Run post-load DQ checks after a successful commit
            logger.info("Proceeding to post-load data quality checks...")
            self.db_loader.run_post_load_dq_checks()

        except Exception as e:
            logger.error(
                f"An error occurred during the loading process: {e}", exc_info=True
            )
            if self.db_loader.conn:
                self.db_loader.rollback()
                logger.error("Transaction has been rolled back.")
            raise

    def _process_quarter(self, quarter: str, load_type: str):
        """
        Downloads, processes, and loads data for a single FAERS quarter using a unified,
        scalable pipeline.
        """
        logger.info(f"Processing quarter: {quarter}")
        load_id = uuid.uuid4()
        metadata = {
            "load_id": load_id,
            "quarter": quarter,
            "load_type": load_type,
            "start_timestamp": datetime.now(timezone.utc),
            "end_timestamp": None,
            "status": "RUNNING",
            "source_checksum": None,
            "rows_extracted": 0,
            "rows_loaded": 0,
            "rows_updated": 0,
            "rows_deleted": 0,
        }
        self.db_loader.update_load_history(metadata)

        zip_path = None
        staging_dir = Path(tempfile.mkdtemp(prefix=f"faers_{quarter}_"))
        logger.info(f"Created staging directory: {staging_dir}")

        try:
            download_result = download_quarter(quarter, self.config.downloader)
            if not download_result:
                raise RuntimeError(f"Download failed for quarter {quarter}.")

            zip_path, checksum = download_result
            metadata["source_checksum"] = checksum

            # --- Unified Parsing and Staging ---
            with zipfile.ZipFile(zip_path, "r") as zf:
                is_xml = any(f.lower().endswith(".xml") for f in zf.namelist())
                if is_xml:
                    logger.info("Detected XML format.")
                    xml_filename = next(
                        f for f in zf.namelist() if f.lower().endswith(".xml")
                    )
                    xml_stream = zf.open(xml_filename)
                    record_iterator, nullified_ids = parse_xml_file(xml_stream)
                    caseids_for_deletion = get_caseids_to_delete(zip_path)
                    caseids_for_deletion.update(nullified_ids)
                    staged_chunk_files = stage_data_to_csv_files(
                        record_iterator,
                        FAERS_TABLE_MODELS,
                        self.config.processing.chunk_size,
                        staging_dir,
                    )
                else:
                    logger.info("Detected ASCII format.")
                    all_records = self._parse_all_from_zip(zip_path)
                    caseids_for_deletion = get_caseids_to_delete(zip_path)
                    staged_chunk_files = self._stage_ascii_records(
                        all_records, staging_dir
                    )

            # --- Unified Deduplication ---
            demo_chunks = staged_chunk_files.get("demo", [])
            if not demo_chunks:
                logger.warning(
                    f"No DEMO records found for quarter {quarter}. Skipping."
                )
                metadata["status"] = "SUCCESS"
                return

            primaryids_to_keep = deduplicate_polars(demo_chunks)

            # --- Unified Filtering and Loading ---
            final_staged_files = self._filter_staged_files(
                staged_chunk_files, primaryids_to_keep
            )
            data_sources = {
                table: path
                for table, path in final_staged_files.items()
                if path and path.stat().st_size > 0
            }

            final_demo_path = final_staged_files.get("demo")
            caseids_to_upsert = set()
            if final_demo_path and final_demo_path.exists():
                with open(final_demo_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f, delimiter="$")
                    caseids_to_upsert = {
                        row["caseid"] for row in reader if row.get("caseid")
                    }

            if caseids_for_deletion:
                deleted_count = self.db_loader.execute_deletions(
                    list(caseids_for_deletion)
                )
                metadata["rows_deleted"] = deleted_count

            if caseids_to_upsert:
                self.db_loader.handle_delta_merge(list(caseids_to_upsert), data_sources)

            metadata["status"] = "SUCCESS"
        except Exception as e:
            metadata["status"] = "FAILED"
            raise e
        finally:
            metadata["end_timestamp"] = datetime.now(timezone.utc)
            self.db_loader.update_load_history(metadata)
            if zip_path and zip_path.exists():
                zip_path.unlink()
            if staging_dir and staging_dir.exists():
                shutil.rmtree(staging_dir)

    def _filter_staged_files(
        self, staged_files: Dict[str, List[Path]], primaryids_to_keep: Set[str]
    ) -> Dict[str, Path]:
        """
        Filters chunked CSV files based on a set of primaryids to keep.
        Writes the filtered data to a new final CSV file for each table.
        """
        logger.info(
            f"Filtering staged files to keep {len(primaryids_to_keep)} records."
        )
        final_files = {}
        if not staged_files or not primaryids_to_keep:
            return final_files

        temp_dir = next(iter(staged_files.values()))[0].parent

        for table_name, chunks in staged_files.items():
            if not chunks:
                continue

            final_path = temp_dir / f"{table_name}_final.csv"
            final_files[table_name] = final_path

            with open(final_path, "w", newline="", encoding="utf-8") as f_out:
                writer = None
                for chunk_path in chunks:
                    with open(chunk_path, "r", newline="", encoding="utf-8") as f_in:
                        reader = csv.reader(f_in, delimiter="$")
                        try:
                            headers = next(reader)
                        except StopIteration:
                            continue  # Chunk is empty

                        if writer is None:
                            writer = csv.writer(f_out, delimiter="$")
                            writer.writerow(headers)
                            try:
                                primaryid_idx = headers.index("primaryid")
                            except ValueError:
                                logger.error(
                                    f"'primaryid' not found in headers for {table_name}"
                                )
                                break

                        for row in reader:
                            if row[primaryid_idx] in primaryids_to_keep:
                                writer.writerow(row)
        return final_files

