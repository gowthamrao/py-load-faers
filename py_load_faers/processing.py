# -*- coding: utf-8 -*-
"""
This module handles the core data processing and transformation logic,
such as deduplication and nullification.
"""
import logging
import re
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Set

import polars as pl

logger = logging.getLogger(__name__)


def get_caseids_to_delete(zip_path: Path) -> Set[str]:
    """
    Scans a FAERS quarterly data ZIP file for a deletion list and extracts
    the CASEIDs to be nullified.

    The deletion file is expected to be a text file with names like
    'delete_cases_yyyyqq.txt' or 'del_yyyyqq.txt'.

    :param zip_path: The path to the downloaded .zip file.
    :return: A set of CASEID strings to be deleted. Returns an empty set
             if no deletion file is found or the file is empty.
    """
    # Regex to find potential deletion files within the zip archive.
    # This is case-insensitive and looks for files starting with 'del'
    # and ending in '.txt'.
    delete_file_pattern = re.compile(r"del.*\.txt", re.IGNORECASE)
    case_ids_to_delete: Set[str] = set()

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            delete_filename = None
            for filename in zf.namelist():
                if delete_file_pattern.match(filename):
                    delete_filename = filename
                    logger.info(f"Found deletion file in archive: {delete_filename}")
                    break

            if not delete_filename:
                logger.info(f"No deletion file found in {zip_path}.")
                return case_ids_to_delete

            # If a deletion file is found, read the CASEIDs from it.
            with zf.open(delete_filename) as f:
                # The file is expected to contain one CASEID per line.
                # We read as text and decode, then strip whitespace.
                for line in f:
                    case_id = line.decode("utf-8").strip()
                    if case_id.isdigit():
                        case_ids_to_delete.add(case_id)

            logger.info(f"Extracted {len(case_ids_to_delete)} CASEIDs to delete.")

    except zipfile.BadZipFile:
        logger.error(f"The file {zip_path} is not a valid zip file.")
        raise
    except Exception as e:
        logger.error(f"An error occurred while processing deletion file in {zip_path}: {e}")
        raise

    return case_ids_to_delete


def deduplicate_polars(
    demo_files: List[Path], format: str, case_ids_to_ignore: Set[str] | None = None
) -> Set[str]:
    """
    Applies the FDA-recommended deduplication logic using Polars for scalability.
    This function can process multiple files (CSV or Parquet) in a memory-efficient way.

    :param demo_files: A list of Path objects pointing to the DEMO files.
    :param format: The format of the files ('csv' or 'parquet').
    :param case_ids_to_ignore: An optional set of case IDs to exclude from
        deduplication.
    :return: A set of PRIMARYID strings that should be kept.
    """
    if not demo_files:
        return set()

    valid_files = [f for f in demo_files if f.exists() and f.stat().st_size > 0]
    if not valid_files:
        logger.warning("No valid, non-empty DEMO files found for deduplication.")
        return set()

    logger.info(f"Starting Polars-based deduplication for {len(valid_files)} {format} file(s).")

    try:
        if format == "csv":
            lazy_query = pl.scan_csv(
                valid_files,
                separator="$",
                has_header=True,
                ignore_errors=True,
                schema_overrides={
                    "primaryid": pl.Utf8,
                    "caseid": pl.Utf8,
                    "fda_dt": pl.Utf8,
                },
            )
        elif format == "parquet":
            lazy_query = pl.scan_parquet(valid_files)
        else:
            raise ValueError(f"Unsupported format for deduplication: {format}")

        required_cols = {"primaryid", "caseid", "fda_dt"}
        if not required_cols.issubset(lazy_query.columns):
            missing = required_cols - set(lazy_query.columns)
            raise ValueError(f"Deduplication failed due to missing columns: {missing}")

        if case_ids_to_ignore:
            logger.info(f"Excluding {len(case_ids_to_ignore)} nullified cases from deduplication.")
            lazy_query = lazy_query.filter(~pl.col("caseid").is_in(list(case_ids_to_ignore)))

        deduplicated_query = lazy_query.select(["primaryid", "caseid", "fda_dt"]).with_columns(
            pl.col("fda_dt").str.to_date("%Y%m%d", strict=False).alias("fda_dt_parsed")
        )
        deduplicated_query = deduplicated_query.drop_nulls(
            subset=["primaryid", "caseid", "fda_dt_parsed"]
        ).sort(
            by=["caseid", "fda_dt_parsed", "primaryid"],
            descending=[False, True, True],
        )
        deduplicated_query = deduplicated_query.unique(
            subset="caseid", keep="first", maintain_order=True
        ).select("primaryid")

        try:
            result_df = deduplicated_query.collect(streaming=True)
        except BaseException as e:
            # Catching BaseException is broad, but Polars can panic on
            # header-only files in a way that isn't caught by a standard
            # Exception.
            logger.warning(
                "Could not collect deduplication results, likely due to "
                f"empty/invalid file. Error: {e}"
            )
            return set()

        if result_df.is_empty():
            logger.info("Deduplication resulted in zero records to keep.")
            return set()

        primary_ids_to_keep = set(result_df["primaryid"].to_list())

        logger.info(
            f"Polars deduplication complete. {len(primary_ids_to_keep)} unique cases to keep."
        )
        return primary_ids_to_keep

    except (pl.exceptions.ColumnNotFoundError, pl.exceptions.SchemaError) as e:
        logger.error(f"Deduplication failed due to missing or mismatched columns: {e}")
        raise ValueError("Deduplication failed due to missing or mismatched columns.") from e
    except Exception as e:
        logger.error(f"An unexpected error occurred during Polars deduplication: {e}")
        raise


def clean_drug_names(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Cleans drug names in a list of records.

    :param records: A list of drug records (dictionaries).
    :return: The list of records with cleaned drug names.
    """
    for record in records:
        if "drugname" in record and isinstance(record["drugname"], str):
            drug_name = record["drugname"].strip()
            if drug_name.upper() == "NULL":
                drug_name = ""
            # Remove special characters, keeping only letters, numbers, and spaces
            drug_name = re.sub(r"[^a-zA-Z0-9\s]+", "", drug_name)
            record["drugname"] = drug_name.upper()
    return records
