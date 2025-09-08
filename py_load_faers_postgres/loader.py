# -*- coding: utf-8 -*-
"""
This module provides the PostgreSQL implementation of the AbstractDatabaseLoader.
"""
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Type
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

    def initialize_schema(self, schema_definition: Dict[str, Any]) -> None:
        """Create FAERS tables based on Pydantic models if they don't exist."""
        if not self.conn:
            raise ConnectionError("No database connection available.")

        # The schema_definition provides the mapping from table name to model
        table_map = schema_definition

        with self.conn.cursor() as cur:
            for table_name, model in table_map.items():
                if model:  # Ensure there is a model to generate DDL from
                    ddl = self._generate_create_table_ddl(table_name, model)
                    logger.info(f"Executing DDL for table '{table_name}':\n{ddl}")
                    cur.execute(ddl)

            # Create metadata table
            meta_ddl = self._generate_metadata_table_ddl()
            logger.info(f"Executing DDL for metadata table:\n{meta_ddl}")
            cur.execute(meta_ddl)

        # The caller is responsible for committing the transaction.
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

    def execute_native_bulk_load(self, table_name: str, file_path: Path) -> None:
        """
        Load data into PostgreSQL using the COPY command from a file.

        :param table_name: The name of the target table.
        :param file_path: The path to the source CSV file.
        """
        if not self.conn:
            raise ConnectionError("No database connection available.")
        if not file_path.exists() or file_path.stat().st_size == 0:
            logger.info(f"Skipping bulk load for table '{table_name}' as source file is empty or missing: {file_path}")
            return

        logger.info(f"Starting native bulk load into table '{table_name}' from {file_path}...")
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
                with open(file_path, 'rb') as f:
                    while data := f.read(8192):
                        copy.write(data)

        logger.info(f"Bulk load into table '{table_name}' complete.")

    def execute_deletions(self, case_ids: List[str]) -> int:
        """
        Delete all records associated with a list of case_ids from all FAERS tables.

        :param case_ids: A list of caseid strings to delete.
        :return: The total number of rows deleted.
        """
        if not self.conn:
            raise ConnectionError("No database connection available.")
        if not case_ids:
            logger.info("No case_ids provided for deletion.")
            return 0

        logger.info(f"Starting deletion for {len(case_ids)} case_ids.")

        # We need to get the corresponding primaryid values from the demo table first,
        # as other tables are linked via primaryid.
        with self.conn.cursor() as cur:
            cur.execute("SELECT primaryid FROM demo WHERE caseid = ANY(%s)", (list(case_ids),))
            primary_ids = [row['primaryid'] for row in cur.fetchall()]

        if not primary_ids:
            logger.info("No matching primary_ids found for the given case_ids. Nothing to delete.")
            return 0

        logger.info(f"Found {len(primary_ids)} primary_ids to delete across all tables.")

        total_rows_deleted = 0
        faers_tables = ["ther", "rpsr", "reac", "outc", "indi", "drug", "demo"]

        with self.conn.cursor() as cur:
            for table in faers_tables:
                # All tables are linked by primaryid.
                delete_sql = f"DELETE FROM {table} WHERE primaryid = ANY(%s)"
                cur.execute(delete_sql, (primary_ids,))
                rows_deleted = cur.rowcount
                total_rows_deleted += rows_deleted
                logger.info(f"Deleted {rows_deleted} rows from table '{table}'.")

        logger.info(f"Total rows deleted across all tables: {total_rows_deleted}")
        return total_rows_deleted

    def handle_delta_merge(self, case_ids_to_upsert: List[str], data_sources: Dict[str, Path]) -> None:
        """
        Handles a delta load by deleting existing case versions and bulk inserting new ones.

        :param case_ids_to_upsert: A list of caseid strings that are new or updated.
        :param data_sources: A dictionary mapping table names to their source file paths.
        """
        if not self.conn:
            raise ConnectionError("No database connection available.")

        # First, delete all existing versions of the cases being loaded.
        # This handles both updates (removing the old version) and ensuring
        # idempotency if the load is re-run.
        if case_ids_to_upsert:
            self.execute_deletions(case_ids_to_upsert)

        # Now, bulk load the new data for each table.
        faers_tables = ["demo", "drug", "reac", "outc", "rpsr", "ther", "indi"]
        for table in faers_tables:
            file_path = data_sources.get(table)
            if file_path:
                self.execute_native_bulk_load(table, file_path)

    def update_load_history(self, metadata: Dict[str, Any]) -> None:
        """
        Insert or update a record in the _faers_load_history table.

        :param metadata: A dictionary containing the metadata to record.
        """
        if not self.conn:
            raise ConnectionError("No database connection available.")

        logger.debug(f"Updating load history with metadata: {metadata}")

        # SQL to insert or update the load history record
        sql = """
            INSERT INTO _faers_load_history (
                load_id, quarter, load_type, start_timestamp, end_timestamp,
                status, source_checksum, rows_extracted, rows_loaded,
                rows_updated, rows_deleted
            ) VALUES (
                %(load_id)s, %(quarter)s, %(load_type)s, %(start_timestamp)s, %(end_timestamp)s,
                %(status)s, %(source_checksum)s, %(rows_extracted)s, %(rows_loaded)s,
                %(rows_updated)s, %(rows_deleted)s
            )
            ON CONFLICT (load_id) DO UPDATE SET
                end_timestamp = EXCLUDED.end_timestamp,
                status = EXCLUDED.status,
                source_checksum = EXCLUDED.source_checksum,
                rows_extracted = EXCLUDED.rows_extracted,
                rows_loaded = EXCLUDED.rows_loaded,
                rows_updated = EXCLUDED.rows_updated,
                rows_deleted = EXCLUDED.rows_deleted;
        """

        with self.conn.cursor() as cur:
            cur.execute(sql, metadata)
        logger.info(f"Load history updated for load_id {metadata.get('load_id')}.")


    def get_last_successful_load(self) -> Optional[str]:
        """
        Retrieve the identifier of the last successfully loaded quarter.

        :return: The quarter string (e.g., "2025Q3") or None if no successful loads.
        """
        if not self.conn:
            raise ConnectionError("No database connection available.")

        logger.info("Querying for the last successful load...")

        sql = """
            SELECT quarter
            FROM _faers_load_history
            WHERE status = 'SUCCESS'
            ORDER BY quarter DESC
            LIMIT 1;
        """
        with self.conn.cursor() as cur:
            cur.execute(sql)
            result = cur.fetchone()

        if result:
            last_quarter = result["quarter"]
            logger.info(f"Last successful load was for quarter: {last_quarter}")
            return last_quarter
        else:
            logger.info("No successful loads found in history.")
            return None
