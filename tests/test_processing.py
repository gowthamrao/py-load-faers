# -*- coding: utf-8 -*-
"""
Tests for the data processing module.
"""
import zipfile
from pathlib import Path

import csv
import pytest
from py_load_faers.processing import get_caseids_to_delete, stream_deduplicate_sorted_file


def test_get_caseids_to_delete_found(tmp_path: Path):
    """
    Test that get_caseids_to_delete correctly finds and parses a deletion file.
    """
    zip_path = tmp_path / "faers_ascii_2025q1.zip"
    delete_filename = "del25q1.txt"
    case_ids_to_delete = ["1001", "1002", "1003"]

    # Create a dummy zip file with a deletion list
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr(delete_filename, "\n".join(case_ids_to_delete))
        zf.writestr("other_file.txt", "some data")

    # Run the function
    result = get_caseids_to_delete(zip_path)

    # Assert the result is correct
    assert result == set(case_ids_to_delete)


def test_get_caseids_to_delete_not_found(tmp_path: Path):
    """
    Test that get_caseids_to_delete returns an empty set when no deletion file exists.
    """
    zip_path = tmp_path / "faers_ascii_2025q1.zip"

    # Create a dummy zip file without a deletion list
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("other_file.txt", "some data")

    # Run the function
    result = get_caseids_to_delete(zip_path)

    # Assert the result is an empty set
    assert result == set()


def test_stream_deduplicate_sorted_file(tmp_path: Path):
    """
    Test the streaming deduplication logic on a pre-sorted CSV file.
    """
    # 1. Define test data and expected results
    records = [
        # Case 1: Three versions, middle one is latest date. Will be sorted to the top.
        {'caseid': '1', 'primaryid': '101', 'fda_dt': '20230115'},
        {'caseid': '1', 'primaryid': '103', 'fda_dt': '20230210'},
        {'caseid': '1', 'primaryid': '102', 'fda_dt': '20230320'},  # Keep
        # Case 2: Unique case
        {'caseid': '2', 'primaryid': '201', 'fda_dt': '20230101'},  # Keep
        # Case 3: Date tie, higher primaryid wins. Will be sorted to the top.
        {'caseid': '3', 'primaryid': '301', 'fda_dt': '20230505'},
        {'caseid': '3', 'primaryid': '302', 'fda_dt': '20230505'},  # Keep
        # Case 5: Another simple case
        {'caseid': '5', 'primaryid': '501', 'fda_dt': '20240101'},  # Keep
    ]
    expected_ids = {'102', '201', '302', '501'}

    # 2. Sort records as they would be by the staging process
    # Sort by primaryid (desc), then fda_dt (desc), then caseid (asc)
    records.sort(key=lambda r: r.get("primaryid", ""), reverse=True)
    records.sort(key=lambda r: r.get("fda_dt", "0"), reverse=True)
    records.sort(key=lambda r: r.get("caseid", ""))

    # 3. Create a temporary sorted CSV file
    sorted_csv_path = tmp_path / "sorted_demo.csv"
    headers = ['caseid', 'primaryid', 'fda_dt']
    with open(sorted_csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter='$')
        writer.writeheader()
        writer.writerows(records)

    # 4. Run the function
    result = stream_deduplicate_sorted_file(sorted_csv_path)

    # 5. Assert the result
    assert result == expected_ids


def test_stream_deduplicate_empty_file(tmp_path: Path):
    """Test that the streaming deduplication handles an empty file."""
    empty_csv_path = tmp_path / "empty.csv"
    empty_csv_path.touch()

    result = stream_deduplicate_sorted_file(empty_csv_path)
    assert result == set()


# --- Tests for the In-Memory (ASCII Path) Deduplication ---
from py_load_faers.processing import deduplicate_records_in_memory

def test_deduplicate_records_in_memory_basic():
    """Test the core logic: latest fda_dt wins."""
    records = [
        {'caseid': '1', 'primaryid': '101', 'fda_dt': '20240101'},
        {'caseid': '1', 'primaryid': '102', 'fda_dt': '20240201'},  # Keep
        {'caseid': '2', 'primaryid': '201', 'fda_dt': '20240301'},  # Keep
    ]
    expected_ids = {'102', '201'}
    result = deduplicate_records_in_memory(records)
    assert result == expected_ids

def test_deduplicate_records_in_memory_tiebreaker():
    """Test the tie-breaking logic: when fda_dt is the same, latest primaryid wins."""
    records = [
        {'caseid': '3', 'primaryid': '301', 'fda_dt': '20240401'},
        {'caseid': '3', 'primaryid': '302', 'fda_dt': '20240401'},  # Keep
    ]
    expected_ids = {'302'}
    result = deduplicate_records_in_memory(records)
    assert result == expected_ids

def test_deduplicate_records_in_memory_complex_mix():
    """Test a complex mix of scenarios."""
    records = [
        {'caseid': '1', 'primaryid': '102', 'fda_dt': '20230320'},  # Keep
        {'caseid': '1', 'primaryid': '101', 'fda_dt': '20230115'},
        {'caseid': '2', 'primaryid': '201', 'fda_dt': '20230101'},  # Keep
        {'caseid': '3', 'primaryid': '302', 'fda_dt': '20230505'},  # Keep
        {'caseid': '3', 'primaryid': '301', 'fda_dt': '20230505'},
    ]
    expected_ids = {'102', '201', '302'}
    result = deduplicate_records_in_memory(records)
    assert result == expected_ids

def test_deduplicate_in_memory_empty_input():
    """Test that the in-memory function handles empty input gracefully."""
    assert deduplicate_records_in_memory([]) == set()

def test_deduplicate_in_memory_missing_columns():
    """Test that the in-memory function raises an error if a required column is missing."""
    records = [{'caseid': '1', 'primaryid': '101'}]  # Missing 'fda_dt'
    with pytest.raises(ValueError, match="missing required columns"):
        deduplicate_records_in_memory(records)
