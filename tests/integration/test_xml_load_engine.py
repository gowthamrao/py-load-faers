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
        user=postgres_container.username,
        password=postgres_container.password,
        dbname=postgres_container.dbname,
    )

@pytest.fixture
def sample_xml_zip(tmp_path: Path) -> Path:
    # Use the sample file created in the same directory
    xml_file_path = Path(__file__).parent / "sample_faers.xml"
    xml_content = xml_file_path.read_text()

    zip_path = tmp_path / "faers_xml_2025q1.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("faers_2025q1.xml", xml_content)

    return zip_path

def test_full_xml_load_with_all_tables(db_settings: DatabaseSettings, sample_xml_zip: Path, mocker):
    mocker.patch("py_load_faers.engine.download_quarter", return_value=sample_xml_zip)

    config = AppSettings(
        database=db_settings,
        downloader=DownloaderSettings(download_dir=str(sample_xml_zip.parent)),
    )

    db_loader = PostgresLoader(config.database)
    db_loader.connect()
    db_loader.initialize_schema()
    db_loader.commit()

    # This test will fail until the engine is modified to handle XML
    # But we add it now to be ready.
    engine = FaersLoaderEngine(config, db_loader)
    engine.run_load(quarter="2025q1")

    with psycopg.connect(db_loader.conn.info.conninfo) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            # Expected counts from sample_faers.xml:
            # CASE-001: 1 DEMO, 1 DRUG, 1 REAC, 1 INDI, 1 THER, 1 RPSR, 1 OUTC = 7 records
            # CASE-002: 1 DEMO, 2 DRUG, 2 REAC, 2 INDI, 0 THER, 1 RPSR, 1 OUTC = 9 records
            # Total loaded: 16 records

            cur.execute("SELECT count(*) FROM demo")
            assert cur.fetchone()['count'] == 2
            cur.execute("SELECT count(*) FROM drug")
            assert cur.fetchone()['count'] == 3
            cur.execute("SELECT count(*) FROM reac")
            assert cur.fetchone()['count'] == 3
            cur.execute("SELECT count(*) FROM indi")
            assert cur.fetchone()['count'] == 3
            cur.execute("SELECT count(*) FROM ther")
            assert cur.fetchone()['count'] == 1
            cur.execute("SELECT count(*) FROM rpsr")
            assert cur.fetchone()['count'] == 2
            cur.execute("SELECT count(*) FROM outc")
            assert cur.fetchone()['count'] == 2

            # Verify specific data points from CASE-002
            cur.execute("SELECT * FROM drug WHERE caseid = 'CASE-002' ORDER BY drug_seq")
            case2_drugs = cur.fetchall()
            assert len(case2_drugs) == 2
            assert case2_drugs[1]['role_cod'] == '2' # Concomitant

            # Verify metadata
            cur.execute("SELECT status, rows_deleted, rows_loaded FROM _faers_load_history")
            meta_res = cur.fetchone()
            assert meta_res['status'] == 'SUCCESS'
            assert meta_res['rows_deleted'] == 1 # CASE-003 was nullified
            assert meta_res['rows_loaded'] == 16

            # Verify CASE-003 was not loaded
            cur.execute("SELECT count(*) FROM demo WHERE caseid = 'CASE-003'")
            assert cur.fetchone()['count'] == 0
