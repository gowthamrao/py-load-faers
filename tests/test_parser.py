# -*- coding: utf-8 -*-
"""
Tests for the ASCII file parser.
"""
from pathlib import Path
from py_load_faers.parser import parse_ascii_file

SAMPLE_DEMO_DATA = """\
PRIMARYID$CASEID$CASEVERSION$I_F_CODE$EVENT_DT$MFR_DT
12345$67890$1$I$20250101$20250102
54321$98765$2$F$20250201$20250202
"""

def test_parse_ascii_file(tmp_path: Path):
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

def test_parse_empty_file(tmp_path: Path):
    """Test that parsing an empty file yields no records."""
    data_file = tmp_path / "EMPTY.txt"
    data_file.write_text("")

    records = list(parse_ascii_file(data_file))
    assert len(records) == 0

def test_parse_header_only_file(tmp_path: Path):
    """Test that a file with only a header yields no records."""
    data_file = tmp_path / "HEADER.txt"
    data_file.write_text("PRIMARYID$CASEID")

    records = list(parse_ascii_file(data_file))
    assert len(records) == 0
