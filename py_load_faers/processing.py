# -*- coding: utf-8 -*-
"""
This module handles the core data processing and transformation logic,
such as deduplication and nullification.
"""
import logging
import re
import zipfile
from pathlib import Path
from typing import Set, List, Dict, Any

import pandas as pd

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
        with zipfile.ZipFile(zip_path, 'r') as zf:
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
                    case_id = line.decode('utf-8').strip()
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


def deduplicate_records(demo_records: List[Dict[str, Any]]) -> Set[str]:
    """
    Applies the FDA-recommended deduplication logic to a list of demographic records.

    The logic is: for each CASEID, select the record with the maximum FDA_DT.
    If there's a tie, select the record with the maximum PRIMARYID.

    :param demo_records: A list of dictionaries, where each dictionary represents
                         a record from a FAERS 'DEMO' file.
    :return: A set of PRIMARYID strings that should be kept after deduplication.
    """
    if not demo_records:
        return set()

    logger.info(f"Starting deduplication for {len(demo_records)} records.")

    # Create a DataFrame from the records
    df = pd.DataFrame(demo_records)

    # Ensure required columns are present
    required_cols = {'caseid', 'primaryid', 'fda_dt'}
    if not required_cols.issubset(df.columns):
        missing_cols = required_cols - set(df.columns)
        raise ValueError(f"Input data is missing required columns: {missing_cols}")

    # Convert columns to appropriate types for sorting
    # Using errors='coerce' will turn unparseable values into NaT/NaN
    df['caseid'] = pd.to_numeric(df['caseid'], errors='coerce')
    df['primaryid'] = pd.to_numeric(df['primaryid'], errors='coerce')
    df['fda_dt'] = pd.to_datetime(df['fda_dt'], format='%Y%m%d', errors='coerce')

    # Drop rows where key fields could not be parsed
    df.dropna(subset=['caseid', 'primaryid', 'fda_dt'], inplace=True)

    # Sort by the deduplication criteria
    # Ascending=False gives us descending order, so the "max" values are first
    df.sort_values(
        by=['caseid', 'fda_dt', 'primaryid'],
        ascending=[True, False, False],
        inplace=True
    )

    # Drop duplicates based on caseid, keeping the first entry, which is the
    # one with the max fda_dt and primaryid due to the sort.
    df_deduplicated = df.drop_duplicates(subset='caseid', keep='first')

    # The 'primaryid' column now contains the IDs of the records to keep.
    # Convert to integer first to remove decimals, then to string.
    primary_ids_to_keep = set(df_deduplicated['primaryid'].astype(int).astype(str))

    logger.info(f"Deduplication complete. {len(primary_ids_to_keep)} unique cases to keep.")

    return primary_ids_to_keep
