# -*- coding: utf-8 -*-
"""
Tests for the data processing module.
"""
import csv
import zipfile
from pathlib import Path
from typing import Any, Callable, Dict, List

import pytest

from py_load_faers.processing import deduplicate_polars, get_caseids_to_delete


def test_get_caseids_to_delete_found(tmp_path: Path) -> None:
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

    result = get_caseids_to_delete(zip_path)
    assert result == set(case_ids_to_delete)


def test_get_caseids_to_delete_not_found(tmp_path: Path) -> None:
    """
    Test that get_caseids_to_delete returns an empty set when no deletion file exists.
    """
    zip_path = tmp_path / "faers_ascii_2025q1.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("other_file.txt", "some data")
    result = get_caseids_to_delete(zip_path)
    assert result == set()


@pytest.fixture
def create_demo_csv(
    tmp_path: Path,
) -> Callable[[List[Dict[str, Any]], str], Path]:
    """A pytest fixture to create a sample DEMO csv file for testing."""

    def _create_csv(records: List[Dict[str, Any]], filename: str = "test_demo.csv") -> Path:
        csv_path = tmp_path / filename
        # Use a minimal set of headers for simplicity
        min_headers = ["primaryid", "caseid", "fda_dt"]

        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f, delimiter="$")
            writer.writerow(min_headers)
            for record in records:
                writer.writerow([record.get(h, "") for h in min_headers])
        return csv_path

    return _create_csv


def test_deduplicate_polars_basic(
    create_demo_csv: Callable[[List[Dict[str, Any]], str], Path]
) -> None:
    """Test the core logic: latest fda_dt wins."""
    records = [
        {"caseid": "1", "primaryid": "101", "fda_dt": "20240101"},
        {"caseid": "1", "primaryid": "102", "fda_dt": "20240201"},  # Keep
        {"caseid": "2", "primaryid": "201", "fda_dt": "20240301"},  # Keep
    ]
    expected_ids = {"102", "201"}

    demo_file = create_demo_csv(records, "test_deduplicate_polars_basic.csv")
    result = deduplicate_polars([demo_file], "csv")
    assert result == expected_ids


def test_deduplicate_polars_tiebreaker(
    create_demo_csv: Callable[[List[Dict[str, Any]], str], Path]
) -> None:
    """Test the tie-breaking logic: when fda_dt is the same, latest primaryid wins."""
    records = [
        {"caseid": "3", "primaryid": "301", "fda_dt": "20240401"},
        {"caseid": "3", "primaryid": "302", "fda_dt": "20240401"},  # Keep
    ]
    expected_ids = {"302"}

    demo_file = create_demo_csv(records, "test_deduplicate_polars_tiebreaker.csv")
    result = deduplicate_polars([demo_file], "csv")
    assert result == expected_ids


def test_deduplicate_polars_complex_mix(
    create_demo_csv: Callable[[List[Dict[str, Any]], str], Path]
) -> None:
    """Test a complex mix of scenarios."""
    records = [
        # This order is intentionally mixed up to ensure sorting works correctly
        {"caseid": "1", "primaryid": "101", "fda_dt": "20230115"},
        {"caseid": "3", "primaryid": "301", "fda_dt": "20230505"},
        {"caseid": "2", "primaryid": "201", "fda_dt": "20230101"},  # Keep
        {"caseid": "1", "primaryid": "102", "fda_dt": "20230320"},  # Keep
        {"caseid": "3", "primaryid": "302", "fda_dt": "20230505"},  # Keep
    ]
    expected_ids = {"102", "201", "302"}

    demo_file = create_demo_csv(records, "test_deduplicate_polars_complex_mix.csv")
    result = deduplicate_polars([demo_file], "csv")
    assert result == expected_ids


def test_deduplicate_polars_multiple_files(
    create_demo_csv: Callable[[List[Dict[str, Any]], str], Path]
) -> None:
    """Test that deduplication works correctly across multiple source files."""
    records1 = [
        {"caseid": "1", "primaryid": "101", "fda_dt": "20240101"},
        {"caseid": "2", "primaryid": "201", "fda_dt": "20240301"},  # Keep
    ]
    records2 = [
        {"caseid": "1", "primaryid": "102", "fda_dt": "20240201"},  # Keep
        {"caseid": "3", "primaryid": "301", "fda_dt": "20240401"},  # Keep
    ]
    expected_ids = {"102", "201", "301"}

    demo_file1 = create_demo_csv(records1, "demo1.csv")
    demo_file2 = create_demo_csv(records2, "demo2.csv")

    result = deduplicate_polars([demo_file1, demo_file2], "csv")
    assert result == expected_ids


def test_deduplicate_polars_empty_input() -> None:
    """Test that the function handles an empty list of files."""
    assert deduplicate_polars([], "csv") == set()


def test_deduplicate_polars_empty_file(
    create_demo_csv: Callable[[List[Dict[str, Any]], str], Path]
) -> None:
    """Test that the function handles a file that is empty or has only a header."""
    demo_file = create_demo_csv([], "test_deduplicate_polars_empty_file.csv")
    assert deduplicate_polars([demo_file], "csv") == set()


def test_deduplicate_polars_missing_column(tmp_path: Path) -> None:
    """Test that the function raises an error if a required column is missing."""
    csv_path = tmp_path / "bad_data.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f, delimiter="$")
        writer.writerow(["caseid", "some_other_column"])
        writer.writerow(["1", "abc"])

    with pytest.raises(ValueError, match="Deduplication failed due to missing columns"):
        deduplicate_polars([csv_path], "csv")
