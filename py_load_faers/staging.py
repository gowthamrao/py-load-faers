# -*- coding: utf-8 -*-
"""
This module provides functions for staging parsed data for bulk loading.
"""
import csv
import io
import logging
from typing import Iterator, Dict, Any, Type
from pydantic import BaseModel

logger = logging.getLogger(__name__)


def stage_data_for_copy(records: Iterator[Dict[str, Any]], model: Type[BaseModel]) -> io.StringIO:
    """
    Converts an iterator of records into an in-memory, dollar-delimited CSV
    file suitable for PostgreSQL's COPY command.

    The order of columns in the CSV is determined by the field order in the
    provided Pydantic model, ensuring alignment with the database table.

    :param records: An iterator of dictionaries, where each dictionary is a record.
    :param model: The Pydantic model corresponding to the data, used to define columns.
    :return: An in-memory io.StringIO buffer containing the CSV data.
    """
    logger.info(f"Staging data for model {model.__name__} for bulk copy.")

    # Use StringIO to build the CSV in memory
    buffer = io.StringIO()

    # Get column headers from the Pydantic model to ensure correct order
    headers = [field.lower() for field in model.model_fields.keys()]

    writer = csv.writer(buffer, delimiter='$', quoting=csv.QUOTE_MINIMAL)

    # Write header
    writer.writerow(headers)

    # Write data rows
    for record in records:
        # Build the row in the correct order, substituting None for missing keys
        row = [record.get(h) for h in headers]
        writer.writerow(row)

    # Rewind the buffer to the beginning so it can be read
    buffer.seek(0)

    logger.info(f"Staging complete for model {model.__name__}.")
    return buffer
