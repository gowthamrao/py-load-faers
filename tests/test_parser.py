# -*- coding: utf-8 -*-
"""
Tests for the ASCII file parser.
"""
from pathlib import Path
from py_load_faers.parser import parse_ascii_file, parse_ascii_quarter

SAMPLE_DEMO_DATA = """\
PRIMARYID$CASEID$CASEVERSION$I_F_CODE$EVENT_DT$MFR_DT
12345$67890$1$I$20250101$20250102
54321$98765$2$F$20250201$20250202
"""


def test_parse_ascii_file(tmp_path: Path) -> None:
    """Test that a standard FAERS ASCII file is parsed correctly."""
    data_file = tmp_path / "DEMO25Q1.txt"
    data_file.write_text(SAMPLE_DEMO_DATA)

    records = list(parse_ascii_file(data_file))

    assert len(records) == 2

    # Check the first record
    assert records[0] == {
        "primaryid": "12345",
        "caseid": "67890",
        "caseversion": "1",
        "i_f_code": "I",
        "event_dt": "20250101",
        "mfr_dt": "20250102",
    }

    # Check the second record's keys
    assert records[1]["primaryid"] == "54321"
    assert records[1]["caseid"] == "98765"


def test_parse_empty_file(tmp_path: Path) -> None:
    """Test that parsing an empty file yields no records."""
    data_file = tmp_path / "EMPTY.txt"
    data_file.write_text("")

    records = list(parse_ascii_file(data_file))
    assert len(records) == 0


def test_parse_ascii_quarter() -> None:
    """
    Tests the main ASCII parsing logic which reads a directory of ASCII files,
    handles deletions, and structures the data correctly.
    """
    # Point to the directory containing the unzipped ASCII files
    test_data_dir = Path("tests/integration/test_data/ascii_quarter")

    # This is the function we will build in the next step
    record_generator, nullified_case_ids = parse_ascii_quarter(test_data_dir)

    # Convert generator to list to inspect the results
    records = list(record_generator)

    # 1. Check that the deleted case was identified
    assert nullified_case_ids == {"102"}

    # 2. Check that the correct number of records are returned (3 total cases - 1 deleted)
    assert len(records) == 2

    # 3. Deeply inspect the structure of a returned record to ensure it matches the XML
    # parser's output
    case_101 = next((r for r in records if r["demo"][0]["caseid"] == "101"), None)
    assert case_101 is not None

    # Check the demo table data for case 101
    assert case_101["demo"] == [
        {
            "primaryid": "10101",
            "caseid": "101",
            "fda_dt": "20240101",
            "sex": "F",
            "age": "55",
            "reporter_country": "US",
        }
    ]

    # Check the drug table data for case 101
    assert len(case_101["drug"]) == 2
    assert case_101["drug"][0] == {
        "primaryid": "10101",
        "caseid": "101",
        "drug_seq": "1",
        "drugname": "ASPIRIN",
        "role_cod": "PS",
    }
    assert case_101["drug"][1]["drugname"] == "LISINOPRIL"

    # Check the reaction table data for case 101
    assert case_101["reac"] == [{"primaryid": "10101", "caseid": "101", "pt": "RASH"}]

    # Check case 103 as well
    case_103 = next((r for r in records if r["demo"][0]["caseid"] == "103"), None)
    assert case_103 is not None
    assert case_103["drug"][0]["drugname"] == "IBUPROFEN"
    assert len(case_103["reac"]) == 2
    assert {"primaryid": "10301", "caseid": "103", "pt": "NAUSEA"} in case_103["reac"]
    assert {"primaryid": "10301", "caseid": "103", "pt": "DIZZINESS"} in case_103["reac"]


def test_parse_header_only_file(tmp_path: Path) -> None:
    """Test that a file with only a header yields no records."""
    data_file = tmp_path / "HEADER.txt"
    data_file.write_text("PRIMARYID$CASEID")

    records = list(parse_ascii_file(data_file))
    assert len(records) == 0
