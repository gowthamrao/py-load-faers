# -*- coding: utf-8 -*-
"""
This module contains the main ETL engine for the FAERS data loader.
"""
import logging
import shutil
import tempfile
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, Iterator, List, Optional, Set, Tuple, Type, cast

import polars as pl
from pydantic import BaseModel

from .config import AppSettings
from .database import AbstractDatabaseLoader
from .downloader import download_quarter, find_latest_quarter
from .models import FAERS_TABLE_MODELS
from .parser import parse_ascii_quarter, parse_xml_file
from .processing import deduplicate_polars
from .staging import stage_data, extract_zip_archive

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

    def run_load(
        self, mode: str = "delta", quarter: Optional[str] = None
    ) -> Optional[Tuple[bool, str]]:
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
                    return None

                if last_loaded and last_loaded.lower() >= latest_available.lower():
                    logger.info("Database is already up-to-date. No new quarters to load.")
                    self.db_loader.commit()
                    return None

                start_qtr: Optional[str] = None
                if last_loaded:
                    year, q = int(last_loaded[:4]), int(last_loaded[-1])
                    q += 1
                    if q > 4:
                        q = 1
                        year += 1
                    start_qtr = f"{year}q{q}"
                else:
                    logger.info(
                        "No previous successful load found. Assuming this is "
                        "the first run in a delta sequence."
                    )
                    start_qtr = latest_available

                quarters_to_load = _generate_quarters_to_load(start_qtr, latest_available)
                for q_to_load in quarters_to_load:
                    self._process_quarter(q_to_load, "DELTA")
            else:
                logger.error(f"Unsupported load mode: {mode}")
                raise NotImplementedError(f"Unsupported load mode: {mode}")

            self.db_loader.commit()
            logger.info("Load process completed successfully and transaction committed.")

            # Run post-load DQ checks after a successful commit
            logger.info("Proceeding to post-load data quality checks...")
            dq_passed, dq_message = self.db_loader.run_post_load_dq_checks()
            return dq_passed, dq_message

        except Exception as e:
            logger.error(f"An error occurred during the loading process: {e}", exc_info=True)
            if self.db_loader.conn:
                self.db_loader.rollback()
                logger.error("Transaction has been rolled back.")
            raise
        return None

    def _process_quarter(self, quarter: str, load_type: str) -> None:
        """
        Downloads, processes, and loads data for a single FAERS quarter using a unified,
        scalable pipeline that supports multiple formats.
        """
        logger.info(f"Processing quarter: {quarter}")
        load_id = uuid.uuid4()
        start_time = datetime.now(timezone.utc)

        # Initialize metadata with all required fields to prevent SQL errors
        metadata: Dict[str, Any] = {
            "load_id": load_id,
            "quarter": quarter,
            "load_type": load_type,
            "start_timestamp": start_time,
            "end_timestamp": None,
            "status": "RUNNING",
            "source_checksum": None,
            "rows_extracted": 0,
            "rows_loaded": 0,
            "rows_updated": 0,
            "rows_deleted": 0,
            "error_message": None,
        }
        self.db_loader.update_load_history(metadata)

        zip_path, staging_dir = None, None
        try:
            staging_dir = Path(tempfile.mkdtemp(prefix=f"faers_{quarter}_"))
            download_result = download_quarter(quarter, self.config.downloader)
            if not download_result:
                raise RuntimeError(f"Download failed for quarter {quarter}.")

            zip_path, checksum = download_result
            metadata["source_checksum"] = checksum

            # --- Unified Parsing and Staging ---
            record_iterator, nullified_ids = self._parse_quarter_zip(zip_path, staging_dir)
            staged_chunk_files = stage_data(
                record_iterator,
                cast(Dict[str, Type[BaseModel]], FAERS_TABLE_MODELS),
                self.config.processing,
                staging_dir,
            )

            # --- Unified Deduplication ---
            demo_chunks = staged_chunk_files.get("demo", [])
            if not demo_chunks:
                logger.warning(f"No DEMO records found for {quarter}. Skipping.")
                metadata["status"] = "SUCCESS"
                return

            primaryids_to_keep = deduplicate_polars(
                demo_chunks, self.config.processing.staging_format, nullified_ids
            )

            # --- Unified Filtering and Loading ---
            final_staged_files = self._filter_staged_files_polars(
                staged_chunk_files,
                primaryids_to_keep,
                self.config.processing.staging_format,
            )

            data_sources = {
                table: path
                for table, path in final_staged_files.items()
                if path and path.stat().st_size > 0
            }

            caseids_to_upsert = self._get_caseids_from_final_demo(final_staged_files.get("demo"))

            if nullified_ids:
                deleted_count = self.db_loader.execute_deletions(list(nullified_ids))
                metadata["rows_deleted"] = deleted_count

            if caseids_to_upsert:
                self.db_loader.handle_delta_merge(list(caseids_to_upsert), data_sources)

            metadata["status"] = "SUCCESS"
        except Exception as e:
            metadata["status"] = "FAILED"
            metadata["error_message"] = str(e)
            logger.error(f"Processing failed for quarter {quarter}: {e}", exc_info=True)
            raise
        finally:
            metadata["end_timestamp"] = datetime.now(timezone.utc)
            self.db_loader.update_load_history(metadata)
            if zip_path and zip_path.exists():
                zip_path.unlink()
            if staging_dir and staging_dir.exists():
                shutil.rmtree(staging_dir)

    def _parse_quarter_zip(
        self, zip_path: Path, extract_dir: Path
    ) -> Tuple[Iterator[Dict[str, Any]], Set[str]]:
        """Detects format and parses a FAERS zip archive."""
        with zipfile.ZipFile(zip_path, "r") as zf:
            filenames = zf.namelist()
            if any(f.lower().endswith(".xml") for f in filenames):
                logger.info("Detected XML format.")
                xml_filename = next(f for f in filenames if f.lower().endswith(".xml"))
                with zf.open(xml_filename) as xml_stream:
                    record_iterator, nullified_ids = parse_xml_file(xml_stream)
                    # Consume the generator into a list while the stream is open
                    return iter(list(record_iterator)), nullified_ids
            else:
                logger.info("Detected ASCII format.")
                extract_zip_archive(zip_path, extract_dir)
                return parse_ascii_quarter(extract_dir)

    def _get_caseids_from_final_demo(self, demo_file: Optional[Path]) -> Set[str]:
        """Extracts caseids from the final deduplicated demo file."""
        if not demo_file or not demo_file.exists():
            return set()

        logger.info(f"Extracting caseids to upsert from {demo_file}")
        if self.config.processing.staging_format == "parquet":
            df = pl.read_parquet(demo_file)
        else:
            df = pl.read_csv(demo_file, separator="$")

        return set(df["caseid"].to_list())

    def _filter_staged_files_polars(
        self,
        staged_files: Dict[str, List[Path]],
        primaryids_to_keep: Set[str],
        format: str,
    ) -> Dict[str, Path]:
        """Filters staged files using Polars for efficiency."""
        logger.info(
            f"Filtering staged files to keep {len(primaryids_to_keep)} records using Polars."
        )
        final_files: Dict[str, Path] = {}
        if not staged_files or not primaryids_to_keep:
            return final_files

        temp_dir = next(iter(staged_files.values()))[0].parent
        primaryids_series = pl.Series("primaryid", list(primaryids_to_keep))

        for table_name, chunks in staged_files.items():
            if not chunks:
                continue

            final_path = temp_dir / f"{table_name}_final.{format}"
            final_files[table_name] = final_path

            if format == "parquet":
                lazy_frames = [pl.scan_parquet(chunk) for chunk in chunks]
            else:
                lazy_frames = [
                    pl.scan_csv(chunk, separator="$", has_header=True, ignore_errors=True)
                    for chunk in chunks
                ]

            if not lazy_frames:
                continue

            df_combined = pl.concat(lazy_frames)

            # Ensure all columns are present, casting to Utf8 for consistency
            for col in df_combined.columns:
                df_combined = df_combined.with_columns(pl.col(col).cast(pl.Utf8))

            # --- Data Cleaning Step ---
            # Apply specific cleaning logic for the 'drug' table
            if table_name == "drug" and "drugname" in df_combined.columns:
                df_combined = df_combined.with_columns(
                    pl.col("drugname")
                    .str.strip_chars()
                    .str.replace(r"(?i)^NULL$", "")
                    .str.replace_all(r"[^a-zA-Z0-9\s]+", "")
                    .str.to_uppercase()
                    .alias("drugname")
                )

            filtered_df = df_combined.join(
                primaryids_series.to_frame().lazy(), on="primaryid", how="inner"
            )

            collected_df = filtered_df.collect()
            if collected_df.height > 0:
                logger.debug(f"Writing {collected_df.height} rows to {final_path}")
                if format == "parquet":
                    collected_df.write_parquet(final_path, compression="zstd")
                else:
                    collected_df.write_csv(final_path, separator="$")
            else:
                logger.warning(f"No records left for table {table_name} after filtering.")
                # Create an empty file with headers so downstream steps don't fail
                if format == "csv":
                    model_type = cast(
                        Optional[Type[BaseModel]], FAERS_TABLE_MODELS.get(table_name)
                    )
                    if model_type:
                        model_headers = [
                            f.lower() for f in model_type.model_fields.keys()
                        ]
                        with open(final_path, "w") as f:
                            f.write("$".join(model_headers))

        return final_files
