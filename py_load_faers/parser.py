# -*- coding: utf-8 -*-
"""
This module provides functions for parsing FAERS data files.
"""
import csv
import logging
from pathlib import Path
from typing import IO, Iterator, Dict, Any

logger = logging.getLogger(__name__)

def parse_ascii_file(file_path: Path, encoding: str = 'utf-8') -> Iterator[Dict[str, Any]]:
    """
    Parses a dollar-delimited FAERS ASCII data file.

    This function reads the file, determines headers from the first line,
    and yields each subsequent row as a dictionary. It can handle
    different file encodings.

    :param file_path: The path to the ASCII data file.
    :param encoding: The encoding of the file.
    :return: An iterator that yields each row as a dictionary.
    """
    logger.info(f"Parsing ASCII file: {file_path} with encoding {encoding}")

    try:
        with file_path.open('r', encoding=encoding, errors='ignore') as f:
            # The FAERS files are dollar-delimited, which can be handled by the csv module.
            reader = csv.reader(f, delimiter='$')

            # Read the header row and normalize column names to lowercase
            try:
                headers = [h.lower() for h in next(reader)]
            except StopIteration:
                logger.warning(f"File {file_path} is empty or has no header.")
                return

            # Yield each row as a dictionary
            for row in reader:
                # Ensure the row has the same number of columns as the header
                if len(row) == len(headers):
                    yield dict(zip(headers, row))
                else:
                    logger.warning(f"Skipping malformed row in {file_path}: {row}")

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except Exception as e:
        logger.error(f"An error occurred while parsing {file_path}: {e}")
        raise
