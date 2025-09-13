# -*- coding: utf-8 -*-
"""
This module provides functions for staging parsed data for bulk loading.
"""
import csv
import logging
import tempfile
import zipfile
from pathlib import Path
from typing import Dict, Any, Type, List, Optional, Iterable
from pydantic import BaseModel
import polars as pl

from .config import ProcessingSettings

logger = logging.getLogger(__name__)


def stage_data(
    record_iterator: Iterable[Dict[str, Any]],
    table_models: Dict[str, Type[BaseModel]],
    settings: ProcessingSettings,
    staging_dir: Optional[Path] = None,
) -> Dict[str, List[Path]]:
    """
    Dispatcher function to stage data to the configured format (CSV or Parquet).
    """
    logger.info(f"Staging data to format: {settings.staging_format}")
    if settings.staging_format == "parquet":
        return stage_data_to_parquet_files(
            record_iterator,
            table_models,
            settings.chunk_size,
            staging_dir,
        )
    elif settings.staging_format == "csv":
        return stage_data_to_csv_files(
            record_iterator,
            table_models,
            settings.chunk_size,
            staging_dir,
        )
    else:
        raise ValueError(f"Unsupported staging format: {settings.staging_format}")


def stage_data_to_parquet_files(
    record_iterator: Iterable[Dict[str, Any]],
    table_models: Dict[str, Type[BaseModel]],
    chunk_size: int = 1_000_000,
    staging_dir: Optional[Path] = None,
) -> Dict[str, List[Path]]:
    """
    Parses a stream of FAERS reports and writes them to chunked Parquet files.
    """
    logger.info(f"Staging records to chunked Parquet files with chunk size {chunk_size}.")
    if staging_dir:
        temp_dir = staging_dir
    else:
        temp_dir = Path(tempfile.mkdtemp(prefix="faers_staging_parquet_"))
    logger.info(f"Using staging directory: {temp_dir}")

    staged_files: Dict[str, List[Path]] = {table_name: [] for table_name in table_models.keys()}
    record_buffers: Dict[str, List[Dict[str, Any]]] = {
        table_name: [] for table_name in table_models.keys()
    }
    file_counters: Dict[str, int] = {table_name: 0 for table_name in table_models.keys()}

    for report in record_iterator:
        for table_name, records in report.items():
            if table_name in record_buffers:
                record_buffers[table_name].extend(records)

        for table_name, buffer in record_buffers.items():
            if len(buffer) >= chunk_size:
                _flush_buffer_to_parquet(
                    temp_dir,
                    table_name,
                    buffer,
                    file_counters,
                    staged_files,
                    table_models[table_name],
                )
                buffer.clear()

    for table_name, buffer in record_buffers.items():
        if buffer:
            _flush_buffer_to_parquet(
                temp_dir,
                table_name,
                buffer,
                file_counters,
                staged_files,
                table_models[table_name],
            )

    logger.info("Staging to Parquet files complete.")
    return staged_files


def _flush_buffer_to_parquet(
    temp_dir: Path,
    table_name: str,
    buffer: List[Dict[str, Any]],
    file_counters: Dict[str, int],
    staged_files: Dict[str, List[Path]],
    model: Type[BaseModel],
) -> None:
    """Helper to write a buffer of records to a new Parquet file."""
    if not buffer:
        return

    chunk_num = file_counters[table_name]
    file_path = temp_dir / f"{table_name}_chunk_{chunk_num}.parquet"
    logger.debug(f"Flushing {len(buffer)} records for table '{table_name}' to " f"{file_path}")

    headers = [field.lower() for field in model.model_fields.keys()]

    # Ensure all records have the same keys, filling missing with None
    records = [{h: record.get(h) for h in headers} for record in buffer]

    df = pl.DataFrame(records, schema=headers)
    df.write_parquet(file_path, compression="zstd")

    staged_files[table_name].append(file_path)
    file_counters[table_name] += 1


def stage_data_to_csv_files(
    record_iterator: Iterable[Dict[str, Any]],
    table_models: Dict[str, Type[BaseModel]],
    chunk_size: int = 1_000_000,
    staging_dir: Optional[Path] = None,
) -> Dict[str, List[Path]]:
    """
    Parses a stream of FAERS reports and writes them to chunked, temporary
    CSV files.

    :param record_iterator: An iterator that yields parsed FAERS reports.
    :param table_models: A dictionary mapping table names to Pydantic models.
    :param chunk_size: The number of records to hold in memory per table
        before flushing to a file.
    :param staging_dir: An optional path to a directory for staging files. If
        not provided, a new one is created.
    :return: A dictionary mapping table names to a list of paths to the
        created CSV files.
    """
    logger.info(f"Staging records to chunked CSV files with chunk size {chunk_size}.")
    if staging_dir:
        temp_dir = staging_dir
    else:
        temp_dir = Path(tempfile.mkdtemp(prefix="faers_staging_"))
    logger.info(f"Using staging directory: {temp_dir}")

    staged_files: Dict[str, List[Path]] = {table_name: [] for table_name in table_models.keys()}
    record_buffers: Dict[str, List[Dict[str, Any]]] = {
        table_name: [] for table_name in table_models.keys()
    }
    file_counters: Dict[str, int] = {table_name: 0 for table_name in table_models.keys()}

    for report in record_iterator:
        for table_name, records in report.items():
            if table_name in record_buffers:
                record_buffers[table_name].extend(records)

        # Check if any buffer has reached the chunk size
        for table_name, buffer in record_buffers.items():
            if len(buffer) >= chunk_size:
                _flush_buffer_to_disk(
                    temp_dir,
                    table_name,
                    buffer,
                    file_counters,
                    staged_files,
                    table_models[table_name],
                )
                buffer.clear()

    # Flush any remaining records
    for table_name, buffer in record_buffers.items():
        if buffer:
            _flush_buffer_to_disk(
                temp_dir,
                table_name,
                buffer,
                file_counters,
                staged_files,
                table_models[table_name],
            )

    logger.info("Staging to CSV files complete.")
    return staged_files


def _flush_buffer_to_disk(
    temp_dir: Path,
    table_name: str,
    buffer: List[Dict[str, Any]],
    file_counters: Dict[str, int],
    staged_files: Dict[str, List[Path]],
    model: Type[BaseModel],
) -> None:
    """Helper to write a buffer of records to a new CSV file."""
    chunk_num = file_counters[table_name]
    file_path = temp_dir / f"{table_name}_chunk_{chunk_num}.csv"
    logger.debug(f"Flushing {len(buffer)} records for table '{table_name}' to {file_path}")

    headers = [field.lower() for field in model.model_fields.keys()]

    with file_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter="$", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(headers)
        for record in buffer:
            writer.writerow([record.get(h) for h in headers])

    staged_files[table_name].append(file_path)
    file_counters[table_name] += 1


def extract_zip_archive(zip_path: Path, extract_to_dir: Path) -> List[Path]:
    """
    Extracts all contents of a zip archive to a specified directory.

    :param zip_path: The path to the zip file.
    :param extract_to_dir: The directory where contents will be extracted.
    :return: A list of paths to the extracted files.
    """
    logger.info(f"Extracting archive {zip_path} to {extract_to_dir}...")
    extract_to_dir.mkdir(parents=True, exist_ok=True)

    extracted_files = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_to_dir)
        for member in zf.infolist():
            if not member.is_dir():
                extracted_files.append(extract_to_dir / member.filename)

    logger.info(f"Successfully extracted {len(extracted_files)} files.")
    return extracted_files
