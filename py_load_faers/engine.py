# -*- coding: utf-8 -*-
"""
This module contains the main ETL engine for the FAERS data loader.
"""
import csv
import io
import logging
import shutil
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Generator, Set

from .config import AppSettings
from .database import AbstractDatabaseLoader
from .downloader import download_quarter, find_latest_quarter
from .models import FAERS_TABLE_MODELS
from .parser import parse_xml_file
from .processing import get_caseids_to_delete, merge_sorted_files, stream_deduplicate_sorted_file, deduplicate_records_in_memory
from .staging import stage_data_to_csv_files

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

            # Run post-load DQ checks after a successful commit
            logger.info("Proceeding to post-load data quality checks...")
            self.db_loader.run_post_load_dq_checks()

        except Exception as e:
            logger.error(f"An error occurred during the loading process: {e}", exc_info=True)
            if self.db_loader.conn:
                self.db_loader.rollback()
                logger.error("Transaction has been rolled back.")
            raise

    def _process_quarter(self, quarter: str, load_type: str):
        """
        Downloads, processes, and loads data for a single FAERS quarter.
        Uses a streaming approach for XML and an in-memory approach for ASCII.
        """
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
        staging_dir = None
        try:
            zip_path = download_quarter(quarter, self.config.downloader)
            if not zip_path:
                raise RuntimeError(f"Download failed for quarter {quarter}.")

            with zipfile.ZipFile(zip_path, 'r') as zf:
                is_xml = any(f.lower().endswith('.xml') for f in zf.namelist())

            if is_xml:
                # --- XML Streaming Path ---
                logger.info("Detected XML format. Using memory-efficient streaming path.")
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    xml_filename = next(f for f in zf.namelist() if f.lower().endswith('.xml'))
                    xml_stream = zf.open(xml_filename)
                    record_iterator, nullified_ids = parse_xml_file(xml_stream)
                    caseids_for_deletion = get_caseids_to_delete(zip_path)
                    caseids_for_deletion.update(nullified_ids)

                staged_chunk_files = stage_data_to_csv_files(record_iterator, FAERS_TABLE_MODELS, self.config.processing.chunk_size)
                staging_dir = staged_chunk_files["demo"][0].parent if staged_chunk_files.get("demo") else None

                demo_chunks = staged_chunk_files.get("demo", [])
                if not demo_chunks:
                    logger.warning(f"No DEMO records found for quarter {quarter}. Skipping.")
                    metadata["status"] = "SUCCESS"
                    return

                merged_demo_path = staging_dir / "demo_sorted_merged.csv"
                # The new merge_sorted_files handles the complex sort correctly.
                merge_sorted_files(demo_chunks, merged_demo_path)
                primaryids_to_keep = stream_deduplicate_sorted_file(merged_demo_path)

                final_staged_files = self._filter_staged_files(staged_chunk_files, primaryids_to_keep)
                data_sources = {table: path for table, path in final_staged_files.items() if path}

                final_demo_path = final_staged_files.get("demo")
                caseids_to_upsert = set()
                if final_demo_path:
                    with open(final_demo_path, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f, delimiter='$')
                        caseids_to_upsert = {row['caseid'] for row in reader}

            else:
                # --- ASCII In-Memory Path ---
                logger.warning("Detected ASCII format. Using in-memory processing path. This may consume significant memory for large files.")
                all_records = self._parse_all_from_zip(zip_path)
                caseids_for_deletion = get_caseids_to_delete(zip_path)

                demo_records = all_records.get("demo", [])
                if not demo_records:
                    logger.warning(f"No DEMO records found for quarter {quarter}. Skipping.")
                    metadata["status"] = "SUCCESS"
                    return

                primaryids_to_keep = deduplicate_records_in_memory(demo_records)

                final_records = {}
                for table_name, records in all_records.items():
                    final_records[table_name] = [r for r in records if r.get('primaryid') in primaryids_to_keep]

                caseids_to_upsert = {r['caseid'] for r in final_records.get("demo", [])}

                # Staging for ASCII path (write to temp files to conform to loader interface)
                staging_dir = Path(tempfile.mkdtemp(prefix="faers_ascii_staging_"))
                logger.info(f"Using temporary staging directory for ASCII results: {staging_dir}")
                data_sources = {}
                for table_name, records in final_records.items():
                    if records:
                        model = FAERS_TABLE_MODELS[table_name]
                        headers = [field.lower() for field in model.model_fields.keys()]
                        file_path = staging_dir / f"{table_name}_final.csv"
                        with file_path.open("w", newline="", encoding="utf-8") as f:
                            writer = csv.writer(f, delimiter='$')
                            writer.writerow(headers)
                            for record in records:
                                writer.writerow([record.get(h) for h in headers])
                        data_sources[table_name] = file_path

            # --- Common Loading Logic ---
            if caseids_for_deletion:
                deleted_count = self.db_loader.execute_deletions(list(caseids_for_deletion))
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

    def _filter_staged_files(self, staged_files: Dict[str, List[Path]], primaryids_to_keep: Set[str]) -> Dict[str, Path]:
        """
        Filters chunked CSV files based on a set of primaryids to keep.
        Writes the filtered data to a new final CSV file for each table.
        """
        logger.info(f"Filtering staged files to keep {len(primaryids_to_keep)} records.")
        final_files = {}
        if not staged_files or not primaryids_to_keep:
            return final_files

        temp_dir = next(iter(staged_files.values()))[0].parent

        for table_name, chunks in staged_files.items():
            if not chunks:
                continue

            final_path = temp_dir / f"{table_name}_final.csv"
            final_files[table_name] = final_path

            with open(final_path, 'w', newline='', encoding='utf-8') as f_out:
                writer = None
                for chunk_path in chunks:
                    with open(chunk_path, 'r', newline='', encoding='utf-8') as f_in:
                        reader = csv.reader(f_in, delimiter='$')
                        try:
                            headers = next(reader)
                        except StopIteration:
                            continue # Chunk is empty

                        if writer is None:
                            writer = csv.writer(f_out, delimiter='$')
                            writer.writerow(headers)
                            try:
                                primaryid_idx = headers.index('primaryid')
                            except ValueError:
                                logger.error(f"'primaryid' not found in headers for {table_name}")
                                break

                        for row in reader:
                            if row[primaryid_idx] in primaryids_to_keep:
                                writer.writerow(row)
        return final_files

    def _parse_all_from_zip(self, zip_path: Path) -> Dict[str, List[Dict[str, Any]]]:
        """Parses all FAERS ASCII tables from a zip file directly into memory."""
        all_records: Dict[str, List[Dict[str, Any]]] = {name: [] for name in FAERS_TABLE_MODELS.keys()}
        year_short = zip_path.stem.split('_')[-1][2:4]
        q_num = zip_path.stem.split('_')[-1][-1]

        with zipfile.ZipFile(zip_path, 'r') as zf:
            for table_name in all_records.keys():
                file_pattern = f"{table_name.upper()}{year_short}Q{q_num}.txt"
                try:
                    data_filename = next(f for f in zf.namelist() if f.upper().endswith(file_pattern.upper()))
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
