# -*- coding: utf-8 -*-
"""
This module contains the main ETL engine for the FAERS data loader.
"""
import io
import logging
import uuid
import zipfile
import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Generator

from .config import AppSettings
from .database import AbstractDatabaseLoader
from .downloader import download_quarter, find_latest_quarter
from .models import FAERS_TABLE_MODELS
from .processing import get_caseids_to_delete, deduplicate_records
from .staging import stage_data_for_copy

logger = logging.getLogger(__name__)


def _generate_quarters_to_load(start_quarter: Optional[str], end_quarter: str) -> Generator[str, None, None]:
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
                    logger.warning("Could not determine the latest available quarter. Aborting.")
                    self.db_loader.commit()
                    return

                if last_loaded and last_loaded.lower() >= latest_available.lower():
                    logger.info("Database is already up-to-date. No new quarters to load.")
                    self.db_loader.commit()
                    return

                start_quarter = None
                if last_loaded:
                    year, q = int(last_loaded[:4]), int(last_loaded[-1])
                    q += 1
                    if q > 4: q = 1; year += 1
                    start_quarter = f"{year}q{q}"
                else:
                    logger.info("No previous successful load found. Assuming this is the first run in a delta sequence.")
                    start_quarter = latest_available

                quarters_to_load = _generate_quarters_to_load(start_quarter, latest_available)
                for q_to_load in quarters_to_load:
                    self._process_quarter(q_to_load, "DELTA")
            else:
                logger.error(f"Unsupported load mode: {mode}")
                raise NotImplementedError(f"Unsupported load mode: {mode}")

            self.db_loader.commit()
            logger.info("Load process completed successfully and transaction committed.")

        except Exception as e:
            logger.error(f"An error occurred during the loading process: {e}", exc_info=True)
            if self.db_loader.conn:
                self.db_loader.rollback()
                logger.error("Transaction has been rolled back.")
            raise

    def _process_quarter(self, quarter: str, load_type: str):
        """Downloads, processes, and loads data for a single FAERS quarter. Assumes a transaction is already open."""
        logger.info(f"Processing quarter: {quarter}")
        load_id = uuid.uuid4()
        metadata = {
            "load_id": load_id, "quarter": quarter, "load_type": load_type,
            "start_timestamp": datetime.now(timezone.utc), "end_timestamp": None,
            "status": "RUNNING", "source_checksum": None, "rows_extracted": 0,
            "rows_loaded": 0, "rows_updated": 0, "rows_deleted": 0,
        }
        self.db_loader.update_load_history(metadata)

        zip_path = None
        try:
            # 1. Download
            zip_path = download_quarter(quarter, self.config.downloader)
            if not zip_path:
                raise RuntimeError(f"Download failed for quarter {quarter}.")

            # 2. Detect format and delegate processing
            with zipfile.ZipFile(zip_path, 'r') as zf:
                filenames = [f.lower() for f in zf.namelist()]
                is_xml = any(f.endswith('.xml') for f in filenames)

            if is_xml:
                # Delegate to the new XML processing method
                self._process_xml_quarter(zip_path, metadata)
                # The XML processor will update metadata and clean up, so we can return
                return

            # --- Original ASCII Logic (Untouched) ---
            all_records = self._parse_all_from_zip(zip_path)
            metadata["rows_extracted"] = sum(len(records) for records in all_records.values())

            # 3. Handle Deletions and Deduplication
            caseids_for_explicit_deletion = get_caseids_to_delete(zip_path)

            demo_records = all_records.get("demo", [])
            if not demo_records:
                logger.warning(f"No DEMO records found for quarter {quarter}. Skipping.")
                metadata["status"] = "SUCCESS" # Mark as success since there's nothing to load
                return

            primaryids_to_keep = deduplicate_records(demo_records)

            # 4. Filter all tables based on deduplication results
            final_records = {}
            total_rows_loaded = 0
            for table_name, records in all_records.items():
                filtered = [r for r in records if r.get('primaryid') in primaryids_to_keep]
                final_records[table_name] = filtered
                total_rows_loaded += len(filtered)
            metadata["rows_loaded"] = total_rows_loaded

            # 5. Stage filtered data for bulk loading
            data_sources = {}
            for table_name, records in final_records.items():
                if records:
                    model = FAERS_TABLE_MODELS[table_name]
                    string_io = stage_data_for_copy(iter(records), model)
                    data_sources[table_name] = io.BytesIO(string_io.read().encode('utf-8'))

            # 6. Load to DB
            caseids_to_upsert = {r['caseid'] for r in final_records["demo"]}

            if caseids_for_explicit_deletion:
                deleted_count = self.db_loader.execute_deletions(list(caseids_for_explicit_deletion))
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

    def _parse_all_from_zip(self, zip_path: Path) -> Dict[str, List[Dict[str, Any]]]:
        """Parses all FAERS tables from a zip file directly into memory."""
        all_records: Dict[str, List[Dict[str, Any]]] = {name: [] for name in FAERS_TABLE_MODELS.keys()}

        with zipfile.ZipFile(zip_path, 'r') as zf:
            for table_name in all_records.keys():
                year_short = zip_path.stem.split('_')[-1][2:4]
                q_num = zip_path.stem.split('_')[-1][-1]
                file_pattern = f"{table_name.upper()}{year_short}Q{q_num}.txt"

                try:
                    data_filename = next(f for f in zf.namelist() if f.upper() == file_pattern.upper())
                    with zf.open(data_filename) as f:
                        text_stream = io.TextIOWrapper(f, encoding='utf-8', errors='ignore')
                        reader = csv.reader(text_stream, delimiter='$')
                        try:
                            headers = [h.lower() for h in next(reader)]
                            for row in reader:
                                if len(row) == len(headers):
                                    all_records[table_name].append(dict(zip(headers, row)))
                        except StopIteration:
                            logger.warning(f"File {data_filename} is empty.")
                except StopIteration:
                    logger.warning(f"Could not find data file for table '{table_name}' in {zip_path}")
        return all_records

    def _process_xml_quarter(self, zip_path: Path, metadata: Dict[str, Any]):
        """A new, separate method to process a quarter with XML data."""
        from .parser import parse_xml_file

        logger.info(f"Processing XML quarter from {zip_path}")
        all_records = {name: [] for name in FAERS_TABLE_MODELS.keys()}
        caseids_for_deletion = set()

        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                xml_filename = next(f for f in zf.namelist() if f.lower().endswith('.xml'))
                with zf.open(xml_filename) as xml_stream:
                    # The lxml parser can handle a raw byte stream
                    record_iterator, nullified_ids = parse_xml_file(xml_stream)
                    caseids_for_deletion.update(nullified_ids)
                    for report in record_iterator:
                        for table_name, records in report.items():
                            all_records[table_name].extend(records)

            metadata["rows_extracted"] = sum(len(records) for records in all_records.values())

            # This logic now mirrors the original ASCII path carefully
            demo_records = all_records.get("demo", [])
            if not demo_records:
                logger.warning("No DEMO records found in XML. Skipping.")
                metadata["status"] = "SUCCESS"
                self.db_loader.update_load_history(metadata)
                return

            primaryids_to_keep = deduplicate_records(demo_records)

            final_records = {}
            total_rows_loaded = 0
            for table_name, records in all_records.items():
                filtered = [r for r in records if r.get('primaryid') in primaryids_to_keep]
                final_records[table_name] = filtered
                total_rows_loaded += len(filtered)
            metadata["rows_loaded"] = total_rows_loaded

            data_sources = {}
            for table_name, records in final_records.items():
                if records:
                    model = FAERS_TABLE_MODELS[table_name]
                    string_io = stage_data_for_copy(iter(records), model)
                    data_sources[table_name] = io.BytesIO(string_io.read().encode('utf-8'))

            # Correctly derive the caseids to upsert from the *deduplicated* records
            final_demo_records = final_records.get("demo", [])
            caseids_to_upsert = {r['caseid'] for r in final_demo_records}

            # Combine explicit deletions from nullification flags with those from delta logic
            if caseids_for_deletion:
                deleted_count = self.db_loader.execute_deletions(list(caseids_for_deletion))
                metadata["rows_deleted"] = deleted_count

            if caseids_to_upsert:
                self.db_loader.handle_delta_merge(list(caseids_to_upsert), data_sources)

            metadata["status"] = "SUCCESS"
        except Exception as e:
            metadata["status"] = "FAILED"
            # Re-raise the exception so it's caught by the main run_load handler
            raise e
        finally:
            # The main handler will update metadata and clean up the zip
            pass
