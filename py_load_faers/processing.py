# -*- coding: utf-8 -*-
"""
This module handles the core data processing and transformation logic,
such as deduplication and nullification.
"""
import csv
import heapq
import logging
import re
import zipfile
from pathlib import Path
from typing import Set, List, Dict, Any, Iterator, Iterable

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


def merge_sorted_files(file_paths: List[Path], output_path: Path) -> None:
    """
    Merges multiple sorted files into a single sorted file using a k-way merge.
    The sort order is specific to the FAERS deduplication logic:
    caseid (asc), fda_dt (desc), primaryid (desc).
    """
    logger.info(f"Merging {len(file_paths)} sorted files into {output_path}.")

    class SortableRow:
        def __init__(self, row: List[str], headers: List[str]):
            self.row = row
            self._headers = headers
            # Cache the values used for comparison
            self.caseid = self._get('caseid')
            self.fda_dt = self._get('fda_dt')
            self.primaryid = self._get('primaryid')

        def _get(self, key: str) -> str:
            try:
                return self.row[self._headers.index(key)]
            except (ValueError, IndexError):
                return ""

        def __lt__(self, other: "SortableRow") -> bool:
            if self.caseid != other.caseid:
                return self.caseid < other.caseid
            if self.fda_dt != other.fda_dt:
                return self.fda_dt > other.fda_dt  # Note: Descending order
            if self.primaryid != other.primaryid:
                return self.primaryid > other.primaryid  # Note: Descending order
            return False

    _readers = [open(f, 'r', newline='', encoding='utf-8') for f in file_paths]
    try:
        iterators = [csv.reader(r, delimiter='$') for r in _readers]
        headers_list = [next(it) for it in iterators]

        if not headers_list:
            logger.warning("No files to merge.")
            return

        # Wrap each row in the sortable class
        wrapped_iterators = [
            (SortableRow(row, headers) for row in it)
            for it, headers in zip(iterators, headers_list)
        ]

        merged_iterator = heapq.merge(*wrapped_iterators)

        with output_path.open('w', newline='', encoding='utf-8') as f_out:
            writer = csv.writer(f_out, delimiter='$')
            writer.writerow(headers_list[0])  # Write header
            for wrapped_row in merged_iterator:
                writer.writerow(wrapped_row.row)

    finally:
        for r in _readers:
            r.close()

def stream_deduplicate_sorted_file(sorted_demo_path: Path) -> Set[str]:
    """
    Reads a sorted DEMO file and yields the primaryid of records to keep.
    The file must be sorted by caseid (asc), fda_dt (desc), and primaryid (desc).
    """
    logger.info(f"Performing streaming deduplication on {sorted_demo_path}.")
    primaryids_to_keep = set()
    current_caseid = None

    with sorted_demo_path.open('r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='$')
        try:
            headers = next(reader)
            caseid_idx = headers.index('caseid')
            primaryid_idx = headers.index('primaryid')
        except (StopIteration, ValueError):
            logger.warning(f"File {sorted_demo_path} is empty or has no header.")
            return set()

        for row in reader:
            caseid = row[caseid_idx]
            if caseid != current_caseid:
                # This is the first time we see this caseid.
                # Because the file is sorted, this row is the best one for this case.
                primaryids_to_keep.add(row[primaryid_idx])
                current_caseid = caseid

    logger.info(f"Streaming deduplication complete. {len(primaryids_to_keep)} unique cases to keep.")
    return primaryids_to_keep


def deduplicate_records_in_memory(demo_records: List[Dict[str, Any]]) -> Set[str]:
    """
    Applies the FDA-recommended deduplication logic to an in-memory list of records.
    This function uses pandas and is intended for smaller datasets (e.g., ASCII files).

    :param demo_records: A list of dictionaries, where each dictionary represents a record.
    :return: A set of PRIMARYID strings that should be kept.
    """
    if not demo_records:
        return set()

    logger.info(f"Starting in-memory deduplication for {len(demo_records)} records.")
    df = pd.DataFrame(demo_records)

    required_cols = {'caseid', 'primaryid', 'fda_dt'}
    if not required_cols.issubset(df.columns):
        missing_cols = required_cols - set(df.columns)
        raise ValueError(f"Input data is missing required columns for deduplication: {missing_cols}")

    # Convert types, coercing errors to nulls
    df['caseid'] = pd.to_numeric(df['caseid'], errors='coerce')
    df['primaryid'] = pd.to_numeric(df['primaryid'], errors='coerce')
    df['fda_dt'] = pd.to_datetime(df['fda_dt'], format='%Y%m%d', errors='coerce')
    df.dropna(subset=['caseid', 'primaryid', 'fda_dt'], inplace=True)

    # Sort by the deduplication criteria: caseid asc, fda_dt desc, primaryid desc
    df.sort_values(
        by=['caseid', 'fda_dt', 'primaryid'],
        ascending=[True, False, False],
        inplace=True
    )

    df_deduplicated = df.drop_duplicates(subset='caseid', keep='first')
    primary_ids_to_keep = set(df_deduplicated['primaryid'].astype(int).astype(str))

    logger.info(f"In-memory deduplication complete. {len(primary_ids_to_keep)} unique cases to keep.")
    return primary_ids_to_keep
