# -*- coding: utf-8 -*-
import pytest
import zipfile
from pathlib import Path

from testcontainers.postgres import PostgresContainer
from py_load_faers.config import AppSettings, DatabaseSettings, DownloaderSettings
from py_load_faers.engine import FaersLoaderEngine
from py_load_faers_postgres.loader import PostgresLoader
from typer.testing import CliRunner
from py_load_faers.cli import app

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def postgres_container():
    """Starts a PostgreSQL container for the test session."""
    with PostgresContainer("postgres:13") as pg:
        yield pg


@pytest.fixture
def db_settings(postgres_container):
    """Provides database settings from the running container."""
    return DatabaseSettings(
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(5432),
        user=postgres_container.POSTGRES_USER,
        password=postgres_container.POSTGRES_PASSWORD,
        dbname=postgres_container.POSTGRES_DB,
    )


@pytest.fixture
def app_settings(tmp_path, db_settings):
    """Provides application settings for the test."""
    return AppSettings(
        downloader=DownloaderSettings(download_dir=str(tmp_path)),
        db=db_settings,
    )


def create_mock_zip(zip_path: Path, quarter: str, data: dict, deletions: list = []):
    """Creates a mock FAERS zip file with the given data."""
    year_short = quarter[2:4]
    q_num = quarter[-1]
    with zipfile.ZipFile(zip_path, "w") as zf:
        for table, content in data.items():
            filename = f"{table.upper()}{year_short}Q{q_num}.txt"
            zf.writestr(filename, content)
        if deletions:
            # The deletion file requires a header
            content = "caseid\n" + "\n".join(deletions)
            zf.writestr(f"del_{quarter}.txt", content)


@pytest.fixture
def mock_faers_data(tmp_path):
    """Creates mock FAERS data files for testing."""
    # --- Quarter 1 Data (2024Q1) ---
    # Case 101: Initial version
    # Case 102: Will be updated in Q2
    # Case 103: Will be deleted in Q2
    q1_data = {
        "demo": (
            "primaryid$caseid$fda_dt\n"
            "1001$101$20240101\n"
            "1002$102$20240101\n"
            "1003$103$20240101"
        ),
        "drug": (
            "primaryid$drug_seq$drugname\n"
            "1001$1$Aspirin\n"
            "1002$1$Ibuprofen\n"
            "1003$1$Tylenol"
        ),
    }
    create_mock_zip(tmp_path / "faers_ascii_2024q1.zip", "2024q1", q1_data)

    # --- Quarter 2 Data (2024Q2) ---
    # Case 102: Updated version (new fda_dt and primaryid)
    # Case 104: New case
    q2_data = {
        "demo": (
            "primaryid$caseid$fda_dt\n"
            "2002$102$20240401\n"  # Updated case with new, higher primaryid
            "1004$104$20240401"  # New case
        ),
        "drug": ("primaryid$drug_seq$drugname\n" "2002$1$Ibuprofen PM\n" "1004$1$Advil"),
    }
    # Case 103 will be deleted via this deletion file
    create_mock_zip(tmp_path / "faers_ascii_2024q2.zip", "2024q2", q2_data, deletions=["103"])

    return tmp_path


def test_delta_load_end_to_end(app_settings, db_settings, mock_faers_data, mocker):
    """
    Tests the end-to-end delta loading process using the CLI runner.
    1. Initialize the DB.
    2. Load Q1 data.
    3. Run a delta load for Q2.
    4. Verify the database state is correct.
    """
    # Mock the downloader functions to use local mock files
    mocker.patch(
        "py_load_faers.engine.download_quarter",
        side_effect=lambda q, s: (mock_faers_data / f"faers_ascii_{q}.zip", "dummy"),
    )
    mocker.patch("py_load_faers.engine.find_latest_quarter", return_value="2024q2")

    runner = CliRunner()
    env = {
        "PY_LOAD_FAERS_DB__HOST": db_settings.host,
        "PY_LOAD_FAERS_DB__PORT": str(db_settings.port),
        "PY_LOAD_FAERS_DB__USER": db_settings.user,
        "PY_LOAD_FAERS_DB__PASSWORD": db_settings.password,
        "PY_LOAD_FAERS_DB__DBNAME": db_settings.dbname,
        "PY_LOAD_FAERS_DOWNLOADER__DOWNLOAD_DIR": app_settings.downloader.download_dir,
    }

    # --- 1. Initialize the schema ---
    result_init = runner.invoke(app, ["db-init"], env=env)
    assert result_init.exit_code == 0

    # --- 2. Run initial load for Q1 ---
    # The 'run' command in the CLI is not updated for delta logic yet.
    # We will call the engine directly for this test.
    pg_loader_q1 = PostgresLoader(db_settings)
    pg_loader_q1.connect()
    engine_q1 = FaersLoaderEngine(app_settings, pg_loader_q1)
    engine_q1.run_load(quarter="2024q1")
    pg_loader_q1.conn.close()

    # --- Verify Q1 Load ---
    verify_loader = PostgresLoader(db_settings)
    verify_loader.connect()
    with verify_loader.conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM demo")
        assert cur.fetchone()["count"] == 3
        cur.execute("SELECT caseid FROM demo ORDER BY caseid")
        results = [r["caseid"] for r in cur.fetchall()]
        assert results == ["101", "102", "103"]
    verify_loader.conn.close()

    # --- 3. Run Delta Load (which should pick up Q2) ---
    # Re-initialize the engine to simulate a new run
    pg_loader_delta = PostgresLoader(db_settings)
    pg_loader_delta.connect()
    engine_delta = FaersLoaderEngine(app_settings, pg_loader_delta)
    engine_delta.run_load(mode="delta")

    # --- 4. Verify Final Database State ---
    with pg_loader_delta.conn.cursor() as cur:
        # Check total counts
        cur.execute("SELECT COUNT(*) FROM demo")
        assert cur.fetchone()["count"] == 3  # 101, 102 (new), 104

        # Check case content
        cur.execute("SELECT primaryid, caseid FROM demo ORDER BY caseid")
        results = cur.fetchall()
        assert [r["caseid"] for r in results] == ["101", "102", "104"]

        # Verify Case 102 was updated (has the new primaryid)
        assert results[1]["primaryid"] == "2002"

        # Verify Case 103 is deleted
        cur.execute("SELECT COUNT(*) FROM demo WHERE caseid = '103'")
        assert cur.fetchone()["count"] == 0

        # Verify drug name for updated case 102
        cur.execute("SELECT drugname FROM drug WHERE primaryid = '2002'")
        assert cur.fetchone()["drugname"] == "IBUPROFEN PM"

        # Verify load history
        cur.execute("SELECT quarter, status FROM _faers_load_history ORDER BY quarter")
        history = cur.fetchall()
        assert len(history) == 2
        assert history[0]["quarter"] == "2024q1"
        assert history[0]["status"] == "SUCCESS"
        assert history[1]["quarter"] == "2024q2"
        assert history[1]["status"] == "SUCCESS"
