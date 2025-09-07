# -*- coding: utf-8 -*-
"""
This module provides the PostgreSQL implementation of the AbstractDatabaseLoader.
"""
import logging
from typing import IO, Any, Dict, List, Optional, Type
from pydantic import BaseModel

import psycopg
from psycopg.rows import dict_row

from py_load_faers.config import DatabaseSettings
from py_load_faers.database import AbstractDatabaseLoader
from py_load_faers import models

logger = logging.getLogger(__name__)


class PostgresLoader(AbstractDatabaseLoader):
    """PostgreSQL database loader implementation."""

    def __init__(self, settings: DatabaseSettings):
        self.settings = settings
        self.conn: Optional[psycopg.Connection] = None

    def connect(self) -> None:
        """Establish a connection to the PostgreSQL database."""
        try:
            logger.info(f"Connecting to PostgreSQL database '{self.settings.dbname}' on host '{self.settings.host}'...")
            self.conn = psycopg.connect(
                conninfo=f"host={self.settings.host} port={self.settings.port} "
                         f"dbname={self.settings.dbname} user={self.settings.user} "
                         f"password={self.settings.password}",
                row_factory=dict_row
            )
            logger.info("Database connection successful.")
        except psycopg.Error as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def begin_transaction(self) -> None:
        if self.conn:
            self.conn.autocommit = False

    def commit(self) -> None:
        if self.conn:
            self.conn.commit()

    def rollback(self) -> None:
        if self.conn:
            self.conn.rollback()

    def initialize_schema(self) -> None:
        """Create FAERS tables based on Pydantic models if they don't exist."""
        if not self.conn:
            raise ConnectionError("No database connection available.")

        table_map = {
            "demo": models.Demo,
            "drug": models.Drug,
            "reac": models.Reac,
            "outc": models.Outc,
            "rpsr": models.Rpsr,
            "ther": models.Ther,
            "indi": models.Indi,
        }

        with self.conn.cursor() as cur:
            for table_name, model in table_map.items():
                ddl = self._generate_create_table_ddl(table_name, model)
                logger.info(f"Executing DDL for table '{table_name}':\n{ddl}")
                cur.execute(ddl)

            # Create metadata table
            meta_ddl = self._generate_metadata_table_ddl()
            logger.info(f"Executing DDL for metadata table:\n{meta_ddl}")
            cur.execute(meta_ddl)

        self.commit()
        logger.info("Schema initialization complete.")

    def _generate_create_table_ddl(self, table_name: str, model: Type[BaseModel]) -> str:
        """Generate a CREATE TABLE statement from a Pydantic model."""

        def pydantic_to_sql_type(field: Any) -> str:
            """Convert Pydantic field type to PostgreSQL type."""
            type_map = {
                str: "TEXT",
                int: "BIGINT",
                float: "DOUBLE PRECISION",
            }
            # Check if the type is Optional
            if type(None) in getattr(field.annotation, '__args__', []):
                # It's Optional, get the inner type
                inner_type = [arg for arg in field.annotation.__args__ if arg is not type(None)][0]
                return type_map.get(inner_type, "TEXT")
            return type_map.get(field.annotation, "TEXT")

        columns = []
        for field_name, field in model.model_fields.items():
            sql_type = pydantic_to_sql_type(field)
            columns.append(f'"{field_name.lower()}" {sql_type}')

        # Define primary keys based on the FAERS data structure
        primary_keys = {
            "demo": ["primaryid"],
            "drug": ["primaryid", "drug_seq"],
            "reac": ["primaryid", "pt"],
            "outc": ["primaryid", "outc_cod"],
            "rpsr": ["primaryid", "rpsr_cod"],
            "ther": ["primaryid", "dsg_drug_seq"],
            "indi": ["primaryid", "indi_drug_seq"],
        }
        pk_str = ", ".join(f'"{k}"' for k in primary_keys.get(table_name, []))
        if pk_str:
            columns.append(f"PRIMARY KEY ({pk_str})")

        columns_str = ",\n    ".join(columns)
        return f"CREATE TABLE IF NOT EXISTS {table_name} (\n    {columns_str}\n);"

    def _generate_metadata_table_ddl(self) -> str:
        return """
        CREATE TABLE IF NOT EXISTS _faers_load_history (
            load_id UUID PRIMARY KEY,
            quarter VARCHAR(10) NOT NULL,
            load_type VARCHAR(20) NOT NULL,
            start_timestamp TIMESTAMPTZ NOT NULL,
            end_timestamp TIMESTAMPTZ,
            status VARCHAR(20) NOT NULL,
            source_checksum VARCHAR(64),
            rows_extracted BIGINT,
            rows_loaded BIGINT,
            rows_updated BIGINT,
            rows_deleted BIGINT
        );
        """

    def execute_native_bulk_load(self, table_name: str, data_source: IO[bytes]) -> None:
        """
        Load data into PostgreSQL using the COPY command.

        :param table_name: The name of the target table.
        :param data_source: A file-like object (stream) containing the CSV data.
        """
        if not self.conn:
            raise ConnectionError("No database connection available.")

        logger.info(f"Starting native bulk load into table '{table_name}'...")
        copy_sql = f"""
            COPY {table_name} FROM STDIN (
                FORMAT CSV,
                HEADER TRUE,
                DELIMITER '$',
                NULL ''
            )
        """
        with self.conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                while data := data_source.read(8192):
                    copy.write(data)

        logger.info(f"Bulk load into table '{table_name}' complete.")

    def execute_deletions(self, case_ids: List[int]) -> None:
        raise NotImplementedError("Delta load logic is not yet implemented.")

    def handle_delta_merge(self, staging_details: Dict[str, Any]) -> None:
        raise NotImplementedError("Delta load logic is not yet implemented.")

    def update_load_history(self, metadata: Dict[str, Any]) -> None:
        raise NotImplementedError("Metadata update logic is not yet implemented.")

    def get_last_successful_load(self) -> Optional[str]:
        raise NotImplementedError("Delta load logic is not yet implemented.")
