# -*- coding: utf-8 -*-
"""
This module defines the abstract base class for all database loaders.
"""
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


class AbstractDatabaseLoader(ABC):
    """
    An abstract base class that defines the interface for database-specific loaders.
    """

    conn: Optional[Any] = None

    @abstractmethod
    def connect(self) -> None:
        """Establish a connection to the database."""
        raise NotImplementedError

    @abstractmethod
    def begin_transaction(self) -> None:
        """Begin a new database transaction."""
        raise NotImplementedError

    @abstractmethod
    def commit(self) -> None:
        """Commit the current transaction."""
        raise NotImplementedError

    @abstractmethod
    def rollback(self) -> None:
        """Roll back the current transaction."""
        raise NotImplementedError

    @abstractmethod
    def initialize_schema(self, schema_definition: Dict[str, Any]) -> None:
        """
        Create the necessary tables and metadata structures in the database.

        :param schema_definition: A dictionary defining the tables and columns.
        """
        raise NotImplementedError

    @abstractmethod
    def execute_native_bulk_load(self, table_name: str, file_path: Path) -> None:
        """
        Execute a native bulk load operation from a file.

        :param table_name: The name of the target table.
        :param file_path: The path to the source data file (e.g., CSV).
        """
        raise NotImplementedError

    @abstractmethod
    def execute_deletions(self, case_ids: List[str]) -> int:
        """
        Delete records from the database based on a list of CASEIDs.

        :param case_ids: A list of case IDs to be deleted.
        :return: The number of records deleted.
        """
        raise NotImplementedError

    @abstractmethod
    def handle_delta_merge(
        self,
        caseids_to_upsert: List[str],
        data_sources: Dict[str, Path],
    ) -> None:
        """
        Merge new data from staged files into the final tables for a delta load.

        This involves deleting old versions of cases and bulk-loading new versions.
        :param caseids_to_upsert: A list of case IDs that will be updated or inserted.
        :param data_sources: A dictionary mapping table names to their source
            file paths.
        """
        raise NotImplementedError

    @abstractmethod
    def update_load_history(self, metadata: Dict[str, Any]) -> None:
        """
        Update the process metadata table with the status of a load operation.

        :param metadata: A dictionary containing the metadata to record.
        """
        raise NotImplementedError

    @abstractmethod
    def get_last_successful_load(self) -> Optional[str]:
        """
        Retrieve the identifier of the last successfully loaded quarter.

        :return: The quarter string (e.g., "2025Q3") or None if no successful loads.
        """
        raise NotImplementedError

    @abstractmethod
    def run_post_load_dq_checks(self) -> Tuple[bool, str]:
        """
        Runs post-load data quality (DQ) checks against the database.

        :return: A tuple containing a boolean success status and a message.
        """
        raise NotImplementedError
