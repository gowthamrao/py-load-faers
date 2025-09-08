# -*- coding: utf-8 -*-
"""
Integration test to verify the correct handling of ASCII file deletions.
"""
import zipfile
import pytest
from pathlib import Path
from sqlalchemy import create_engine, text

from py_load_faers.config import AppSettings, DatabaseSettings, DownloaderSettings, ProcessingSettings
from py_load_faers.engine import FaersLoaderEngine
from py_load_faers_postgres.loader import PostgresLoader
from testcontainers.postgres import PostgresContainer

@pytest.mark.integration
def test_ascii_deletion_before_deduplication():
    """
    This test verifies the critical bug fix where records marked for deletion
    are removed *before* the deduplication logic is applied.
    """
    with PostgresContainer("postgres:13") as postgres:
        # 1. Setup: Database and Configuration
        db_settings = DatabaseSettings(
            host=postgres.get_container_host_ip(),
            port=postgres.get_exposed_port(5432),
                user="test",
            password="test",
                dbname="test",
        )
        config = AppSettings(
            db=db_settings,
            downloader=DownloaderSettings(download_dir="/tmp/faers_test"),
            processing=ProcessingSettings(),
        )

        # 2. Create a dummy FAERS ASCII zip file with a specific scenario
        zip_path = Path(config.downloader.download_dir) / "faers_ascii_2025q1.zip"
        zip_path.parent.mkdir(exist_ok=True)

        # Test Data:
        # - Case '456' has two versions. Version 2 is "better" (later date).
        # - Case '123' is a normal case that should be loaded.
        # - The deletion file explicitly lists case '456' for deletion.
        demo_data = (
            "primaryid$caseid$fda_dt\n"
            "1001$123$20250101\n"
            "2001$456$20250101\n"
            "2002$456$20250201\n" # This version would be chosen by deduplication
        )
        drug_data = (
            "primaryid$caseid$drug_seq$drugname\n"
            "1001$123$1$Aspirin\n"
            "2001$456$1$Tylenol\n"
            "2002$456$1$Ibuprofen\n"
        )
        delete_data = "456\n"

        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("DEMO25Q1.TXT", demo_data)
            zf.writestr("DRUG25Q1.TXT", drug_data)
            zf.writestr("DEL25Q1.TXT", delete_data)

        # 3. Run the Loader Engine
        db_loader = PostgresLoader(config.db)
        engine = FaersLoaderEngine(config, db_loader)

        # Connect to the database and initialize the schema
        db_loader.connect()
        from py_load_faers.models import FAERS_TABLE_MODELS
        db_loader.initialize_schema(FAERS_TABLE_MODELS)
        db_loader.commit() # Commit schema creation

        # We override the download function to use our local dummy file
        # This is a bit of a hack for testing, but effective.
        from unittest.mock import patch
        with patch('py_load_faers.engine.download_quarter', return_value=zip_path):
            engine.run_load(mode="delta", quarter="2025q1")

        # 4. Assertions: Verify the database state
        # The loader uses a dict_row factory, so we access results by column name.
        with db_loader.conn.cursor() as cur:
            # Assert that case '456' is NOT in the demo table
            cur.execute("SELECT COUNT(*) as count FROM demo WHERE caseid = '456'")
            assert cur.fetchone()['count'] == 0, "Case '456' should have been deleted but was found."

            # Assert that case '456' is also NOT in the drug table
            cur.execute("SELECT COUNT(*) as count FROM drug WHERE caseid = '456'")
            assert cur.fetchone()['count'] == 0, "Drug data for case '456' should have been deleted."

            # Assert that case '123' IS in the demo table
            cur.execute("SELECT COUNT(*) as count FROM demo WHERE caseid = '123'")
            assert cur.fetchone()['count'] == 1, "Case '123' should have been loaded but was not found."

            # Assert that the correct primaryid for case '123' is present
            cur.execute("SELECT primaryid FROM demo WHERE caseid = '123'")
            assert cur.fetchone()['primaryid'] == "1001"
