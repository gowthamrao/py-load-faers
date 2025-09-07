# -*- coding: utf-8 -*-
"""
This module defines the abstract base class for all database loaders.
"""
from abc import ABC, abstractmethod
from typing import IO, List, Any, Dict, Optional


class AbstractDatabaseLoader(ABC):
    """
    An abstract base class that defines the interface for database-specific loaders.
    """

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
    def execute_native_bulk_load(
        self, table_name: str, data_source: IO[bytes], options: Dict[str, Any]
    ) -> None:
        """
        Execute a native bulk load operation.

        :param table_name: The name of the target table.
        :param data_source: A file-like object (stream) containing the data.
        :param options: A dictionary of database-specific options for the load.
        """
        raise NotImplementedError

    @abstractmethod
    def execute_deletions(self, case_ids: List[int]) -> None:
        """
        Delete records from the database based on a list of CASEIDs.

        :param case_ids: A list of case IDs to be deleted.
        """
        raise NotImplementedError

    @abstractmethod
    def handle_delta_merge(self, staging_details: Dict[str, Any]) -> None:
        """
        Merge new data from a staging area into the final tables for a delta load.

        :param staging_details: Details about the staged data to be merged.
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
