# -*- coding: utf-8 -*-
"""
Tests for the data processing module.
"""
import zipfile
from pathlib import Path

import pytest
from py_load_faers.processing import get_caseids_to_delete, deduplicate_records


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


def test_deduplicate_records_basic():
    """
    Test the core deduplication logic: latest fda_dt wins.
    """
    records = [
        # Case 1: Version 2 is later, should be kept
        {'caseid': '1', 'primaryid': '101', 'fda_dt': '20240101'},
        {'caseid': '1', 'primaryid': '102', 'fda_dt': '20240201'}, # Keep
        # Case 2: This one is unique, should be kept
        {'caseid': '2', 'primaryid': '201', 'fda_dt': '20240301'}, # Keep
    ]
    expected_ids = {'102', '201'}
    result = deduplicate_records(records)
    assert result == expected_ids


def test_deduplicate_records_tiebreaker():
    """
    Test the tie-breaking logic: when fda_dt is the same, latest primaryid wins.
    """
    records = [
        # Case 3: Same fda_dt, higher primaryid should win
        {'caseid': '3', 'primaryid': '301', 'fda_dt': '20240401'},
        {'caseid': '3', 'primaryid': '302', 'fda_dt': '20240401'}, # Keep
    ]
    expected_ids = {'302'}
    result = deduplicate_records(records)
    assert result == expected_ids


def test_deduplicate_records_complex_mix():
    """
    Test a complex mix of scenarios including multiple versions and ties.
    """
    records = [
        # Case 1: Three versions, middle one is latest date
        {'caseid': '1', 'primaryid': '101', 'fda_dt': '20230115'},
        {'caseid': '1', 'primaryid': '102', 'fda_dt': '20230320'}, # Keep
        {'caseid': '1', 'primaryid': '103', 'fda_dt': '20230210'},
        # Case 2: Unique case
        {'caseid': '2', 'primaryid': '201', 'fda_dt': '20230101'}, # Keep
        # Case 3: Date tie, higher primaryid wins
        {'caseid': '3', 'primaryid': '301', 'fda_dt': '20230505'},
        {'caseid': '3', 'primaryid': '302', 'fda_dt': '20230505'}, # Keep
        # Case 4: Malformed/missing data, should be ignored
        {'caseid': '4', 'primaryid': '401', 'fda_dt': None},
        {'caseid': '4', 'primaryid': None, 'fda_dt': '20230601'},
        {'caseid': None, 'primaryid': '403', 'fda_dt': '20230601'},
        # Case 5: Another simple case
        {'caseid': '5', 'primaryid': '501', 'fda_dt': '20240101'}, # Keep
    ]
    expected_ids = {'102', '201', '302', '501'}
    result = deduplicate_records(records)
    assert result == expected_ids


def test_deduplicate_empty_input():
    """Test that the function handles empty input gracefully."""
    assert deduplicate_records([]) == set()


def test_deduplicate_missing_columns():
    """Test that the function raises an error if a required column is missing."""
    records = [
        {'caseid': '1', 'primaryid': '101'}, # Missing 'fda_dt'
    ]
    with pytest.raises(ValueError, match="missing required columns"):
        deduplicate_records(records)
