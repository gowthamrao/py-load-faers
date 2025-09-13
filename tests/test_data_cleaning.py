# -*- coding: utf-8 -*-
"""
Tests for the data cleaning functions.
"""
from py_load_faers.processing import clean_drug_names


def test_trim_whitespace_from_drug_name() -> None:
    """Test that leading/trailing whitespace is trimmed from drug names."""
    sample_records = [
        {"drugname": "  aspirin  "},
        {"drugname": "ibuprofen"},
        {"drugname": "  paracetamol"},
    ]
    cleaned_records = clean_drug_names(sample_records)
    assert cleaned_records[0]["drugname"] == "ASPIRIN"
    assert cleaned_records[1]["drugname"] == "IBUPROFEN"
    assert cleaned_records[2]["drugname"] == "PARACETAMOL"


def test_convert_drug_name_to_uppercase() -> None:
    """Test that drug names are converted to uppercase."""
    sample_records = [
        {"drugname": "Aspirin"},
        {"drugname": "ibuprofen"},
    ]
    cleaned_records = clean_drug_names(sample_records)
    assert cleaned_records[0]["drugname"] == "ASPIRIN"
    assert cleaned_records[1]["drugname"] == "IBUPROFEN"


def test_handle_null_string_in_drug_name() -> None:
    """Test that 'NULL' strings are handled correctly."""
    sample_records = [
        {"drugname": "NULL"},
        {"drugname": "  null  "},
        {"drugname": "aspirin"},
    ]
    cleaned_records = clean_drug_names(sample_records)
    assert cleaned_records[0]["drugname"] == ""
    assert cleaned_records[1]["drugname"] == ""
    assert cleaned_records[2]["drugname"] == "ASPIRIN"


def test_remove_special_characters_from_drug_name() -> None:
    """Test that special characters are removed from drug names."""
    sample_records = [
        {"drugname": "@Tylenol"},
        {"drugname": "aspirin-81mg"},
        {"drugname": "drug_name with (parentheses)"},
        {"drugname": "multi   space"},
    ]
    cleaned_records = clean_drug_names(sample_records)
    assert cleaned_records[0]["drugname"] == "TYLENOL"
    assert cleaned_records[1]["drugname"] == "ASPIRIN81MG"
    assert cleaned_records[2]["drugname"] == "DRUGNAME WITH PARENTHESES"
    assert cleaned_records[3]["drugname"] == "MULTI   SPACE"
