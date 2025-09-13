# -*- coding: utf-8 -*-
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterator, List

import psycopg
import pytest
from psycopg.rows import dict_row
from pytest_mock import MockerFixture
from testcontainers.postgres import PostgresContainer

from py_load_faers.config import AppSettings, DatabaseSettings, DownloaderSettings
from py_load_faers.engine import FaersLoaderEngine
from py_load_faers_postgres.loader import PostgresLoader
from py_load_faers.models import FAERS_TABLE_MODELS

# Mark all tests in this file as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def postgres_container() -> Iterator[PostgresContainer]:
    """Starts a PostgreSQL container for the test session."""
    with PostgresContainer("postgres:13") as pg:
        yield pg


@pytest.fixture
def db_settings(postgres_container: PostgresContainer) -> DatabaseSettings:
    """Provides database settings from the running container."""
    return DatabaseSettings(
        type="postgresql",
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(5432),
        user=postgres_container.POSTGRES_USER,
        password=postgres_container.POSTGRES_PASSWORD,
        dbname=postgres_container.POSTGRES_DB,
    )


@pytest.fixture
def app_settings(tmp_path: Path, db_settings: DatabaseSettings) -> AppSettings:
    """Provides application settings for the test."""
    return AppSettings(
        db=db_settings,
        downloader=DownloaderSettings(
            download_dir=str(tmp_path), retries=3, timeout=60
        ),
    )


def create_mock_zip(
    zip_path: Path, quarter: str, data: Dict[str, str], deletions: List[str] = []
) -> None:
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
def mock_faers_data(tmp_path: Path) -> Path:
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
    create_mock_zip(
        tmp_path / "faers_ascii_2024q2.zip", "2024q2", q2_data, deletions=["103"]
    )

    return tmp_path


def test_delta_load_end_to_end(
    app_settings: AppSettings,
    db_settings: DatabaseSettings,
    mock_faers_data: Path,
    mocker: MockerFixture,
) -> None:
    """
    Tests the end-to-end delta loading process.
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

    # --- 1. Initialize the schema ---
    db_loader_init = PostgresLoader(db_settings)
    db_loader_init.connect()
    assert db_loader_init.conn is not None
    db_loader_init.initialize_schema(FAERS_TABLE_MODELS)
    db_loader_init.commit()
    db_loader_init.conn.close()

    # --- 2. Run initial load for Q1 ---
    pg_loader_q1 = PostgresLoader(db_settings)
    pg_loader_q1.connect()
    assert pg_loader_q1.conn is not None
    engine_q1 = FaersLoaderEngine(app_settings, pg_loader_q1)
    engine_q1.run_load(quarter="2024q1")
    pg_loader_q1.conn.close()

    # --- Verify Q1 Load ---
    verify_loader = PostgresLoader(db_settings)
    verify_loader.connect()
    assert verify_loader.conn is not None
    with verify_loader.conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT COUNT(*) FROM demo")
        count_res = cur.fetchone()
        assert count_res is not None
        assert count_res["count"] == 3
        cur.execute("SELECT caseid FROM demo ORDER BY caseid")
        results = [r["caseid"] for r in cur.fetchall()]
        assert results == ["101", "102", "103"]
    verify_loader.conn.close()

    # --- 3. Run Delta Load (which should pick up Q2) ---
    pg_loader_delta = PostgresLoader(db_settings)
    pg_loader_delta.connect()
    assert pg_loader_delta.conn is not None
    engine_delta = FaersLoaderEngine(app_settings, pg_loader_delta)
    engine_delta.run_load(mode="delta")

    # --- 4. Verify Final Database State ---
    with pg_loader_delta.conn.cursor(row_factory=dict_row) as cur:
        # Check total counts
        cur.execute("SELECT COUNT(*) FROM demo")
        count_res = cur.fetchone()
        assert count_res is not None
        assert count_res["count"] == 3  # 101, 102 (new), 104

        # Check case content
        cur.execute("SELECT primaryid, caseid FROM demo ORDER BY caseid")
        results = cur.fetchall()
        assert [r["caseid"] for r in results] == ["101", "102", "104"]

        # Verify Case 102 was updated (has the new primaryid)
        assert results[1]["primaryid"] == "2002"

        # Verify Case 103 is deleted
        cur.execute("SELECT COUNT(*) FROM demo WHERE caseid = '103'")
        count_res = cur.fetchone()
        assert count_res is not None
        assert count_res["count"] == 0

        # Verify drug name for updated case 102
        cur.execute("SELECT drugname FROM drug WHERE primaryid = '2002'")
        drug_res = cur.fetchone()
        assert drug_res is not None
        assert drug_res["drugname"] == "IBUPROFEN PM"

        # Verify load history
        cur.execute("SELECT quarter, status FROM _faers_load_history ORDER BY quarter")
        history = cur.fetchall()
        assert len(history) == 2
        assert history[0]["quarter"] == "2024q1"
        assert history[0]["status"] == "SUCCESS"
        assert history[1]["quarter"] == "2024q2"
        assert history[1]["status"] == "SUCCESS"
    pg_loader_delta.conn.close()
