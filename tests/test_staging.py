# -*- coding: utf-8 -*-
"""
Tests for the staging module.
"""
import zipfile
from pathlib import Path
from py_load_faers.staging import extract_zip_archive


def test_extract_zip_archive(tmp_path: Path) -> None:
    """
    Test that a zip archive is correctly extracted.
    """
    # 1. Create a dummy zip file
    zip_path = tmp_path / "test_archive.zip"
    file1_content = "hello"
    file2_content = "world"

    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("file1.txt", file1_content)
        zf.writestr("subdir/file2.txt", file2_content)

    # 2. Call the extraction function
    extract_dir = tmp_path / "extracted"
    extracted_files = extract_zip_archive(zip_path, extract_dir)

    # 3. Assertions
    assert extract_dir.exists()
    assert (extract_dir / "file1.txt").is_file()
    assert (extract_dir / "subdir" / "file2.txt").is_file()

    # Check the list of returned files
    assert len(extracted_files) == 2
    assert extract_dir / "file1.txt" in extracted_files
    assert extract_dir / "subdir" / "file2.txt" in extracted_files

    # Check file contents
    assert (extract_dir / "file1.txt").read_text() == file1_content
    assert (extract_dir / "subdir" / "file2.txt").read_text() == file2_content
