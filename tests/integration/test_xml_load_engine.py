# -*- coding: utf-8 -*-
import pytest
import zipfile
from pathlib import Path
import psycopg
from testcontainers.postgres import PostgresContainer
from py_load_faers.config import AppSettings, DatabaseSettings, DownloaderSettings
from py_load_faers.engine import FaersLoaderEngine
from py_load_faers_postgres.loader import PostgresLoader

pytestmark = pytest.mark.integration

@pytest.fixture(scope="module")
def postgres_container():
    with PostgresContainer("postgres:13") as container:
        yield container

@pytest.fixture(scope="module")
def db_settings(postgres_container: PostgresContainer) -> DatabaseSettings:
    return DatabaseSettings(
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(5432),
        user="test",
        password="test",
        dbname="test",
    )

@pytest.fixture
def realistic_xml_zip(tmp_path: Path) -> Path:
    """Creates a zip file containing the realistic FAERS XML test data."""
    xml_file_path = Path(__file__).parent / "test_data/realistic_faers.xml"
    xml_content = xml_file_path.read_text()

    zip_path = tmp_path / "faers_xml_2025q1.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("faers_2025q1.xml", xml_content)

    return zip_path


def test_full_xml_load_with_deduplication_and_nullification(db_settings: DatabaseSettings, realistic_xml_zip: Path, mocker):
    """
    This integration test verifies the end-to-end XML loading process,
    specifically checking that the deduplication and nullification logic
    is correctly applied according to the FRD.
    """
    mocker.patch("py_load_faers.engine.download_quarter", return_value=realistic_xml_zip)

    # Need to get the schema definition from the models
    from py_load_faers.models import Demo, Drug, Reac, Outc, Rpsr, Ther, Indi
    schema_definition = {
        "demo": Demo, "drug": Drug, "reac": Reac, "outc": Outc,
        "rpsr": Rpsr, "ther": Ther, "indi": Indi
    }

    config = AppSettings(
        db=db_settings,
        downloader=DownloaderSettings(download_dir=str(realistic_xml_zip.parent)),
    )

    db_loader = PostgresLoader(config.db)
    db_loader.connect()
    # Pass the schema definition as required by the updated signature
    db_loader.initialize_schema(schema_definition)
    db_loader.commit()

    engine = FaersLoaderEngine(config, db_loader)
    engine.run_load(quarter="2025q1")

    with psycopg.connect(db_loader.conn.info.conninfo) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            # 1. Verify the final state of the data
            # After processing, only case 102 should exist in the database,
            # as case 101 was nullified by version V3.
            cur.execute("SELECT * FROM demo")
            final_demo_records = cur.fetchall()
            assert len(final_demo_records) == 1, "Should only be one record in the demo table"

            loaded_case = final_demo_records[0]
            assert loaded_case['caseid'] == '102'
            assert loaded_case['primaryid'] == 'V4'

            # 2. Verify that case 101 is completely gone
            cur.execute("SELECT COUNT(*) FROM demo WHERE caseid = '101'")
            assert cur.fetchone()['count'] == 0, "Case 101 should have been deleted due to nullification"
            cur.execute("SELECT COUNT(*) FROM drug WHERE primaryid IN ('V1', 'V2', 'V3')")
            assert cur.fetchone()['count'] == 0, "No data from any version of Case 101 should be loaded"

            # 3. Verify the load history metadata
            cur.execute("SELECT * FROM _faers_load_history")
            meta_res = cur.fetchone()
            assert meta_res is not None, "Load history metadata should exist"
            assert meta_res['status'] == 'SUCCESS'

            # The engine should report the logical deletion of the nullified case.
            assert meta_res['rows_deleted'] == 1, "Should report 1 case as deleted/nullified"

            # The number of loaded rows should correspond to the tables populated for case 102.
            # The test data for V4 populates: demo, drug, reac, rpsr.
            assert meta_res['rows_loaded'] == 4, "Should report 4 rows loaded for Case 102"
