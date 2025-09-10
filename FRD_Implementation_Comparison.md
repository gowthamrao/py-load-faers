# FRD Implementation Comparison: py-load-faers

This document compares the functional requirements for the **py-load-faers** package with its current implementation. Each requirement from the FRD is listed, followed by an analysis of its implementation status and a code example where applicable.

---

## 2\. Data Acquisition and Source Management

### 2.1 Data Source

The primary data source shall be the official FDA FAERS Quarterly Data Files repository.

**Implementation Analysis:**
*   **Status:** `Implemented`
*   **Details:** The `downloader.py` module uses a hardcoded URL to the official FDA FAERS page to find download links.
*   **Code Snippet (`py_load_faers/downloader.py`):**
    ```python
    FDA_FAERS_URL = "https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html"
    ```

### 2.2 Automated Download Mechanism

**R1: Automatically detect newly released quarterly datasets (YYYYQQ format) by scanning the FDA repository.**

**Implementation Analysis:**
*   **Status:** `Implemented`
*   **Details:** The `find_latest_quarter` function in `downloader.py` scrapes the FDA FAERS page and uses a regular expression to find all available quarterly download links, then sorts them to find the most recent one.
*   **CLI Example:** The `run` command in delta mode and the `download` command with no specified quarter both use this functionality.
    ```bash
    # This will automatically find and download the latest quarter
    faers-loader download
    ```

**R2: (Intentionally Omitted)**

**R3: Support downloading specific historical quarters or ranges.**

**Implementation Analysis:**
*   **Status:** `Implemented`
*   **Details:** The `download` CLI command accepts a `--quarter` argument to download a specific historical dataset. The FRD mentions ranges, which are not supported by a single command, but a user could script this easily.
*   **CLI Example:**
    ```bash
    # Download data for the first quarter of 2024
    faers-loader download --quarter 2024q1
    ```

**R4: Implement download resumption capabilities and configurable retry mechanisms with exponential backoff.**

**Implementation Analysis:**
*   **Status:** `Implemented`
*   **Details:** The `downloader.py` module creates a `requests.Session` with a `Retry` mechanism from `urllib3`. It is configured to retry on connection and status code errors with a backoff factor, effectively implementing an exponential backoff strategy. True download resumption (i.e., continuing a partially downloaded file) is not implemented, but the retry covers transient network failures.
*   **Code Snippet (`py_load_faers/downloader.py`):**
    ```python
    def _create_retry_session() -> requests.Session:
        """Create a requests session with a retry mechanism."""
        session = requests.Session()
        retry = Retry(
            total=5,
            read=5,
            connect=5,
            backoff_factor=0.3, # Enables exponential backoff
            status_forcelist=(500, 502, 503, 504),
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
    ```

**R5 (Critical): Ensure the integrity of downloaded files... generate a SHA-256 checksum...**

**Implementation Analysis:**
*   **Status:** `Implemented`
*   **Details:** After a successful download, the `download_quarter` function first verifies the integrity of the ZIP archive using `zipfile.ZipFile.testzip()`. If the test passes, it then calculates the SHA-256 checksum of the file. This checksum is passed back to the engine and stored in the process metadata table (`_faers_load_history`).
*   **Code Snippet (`py_load_faers/downloader.py`):**
    ```python
    # R5: Verify the integrity of the downloaded ZIP file
    logger.info(f"Verifying integrity of {file_path}...")
    with zipfile.ZipFile(file_path) as zf:
        if zf.testzip() is not None:
            # ... handle corrupted file ...

    # R5: Generate and log SHA-256 checksum
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        # ... read file and update hash ...
    checksum = sha256_hash.hexdigest()
    ```

### 2.3 File Format Handling

**R6: Parse the ASCII format ($-delimited).**

**Implementation Analysis:**
*   **Status:** `Implemented`
*   **Details:** The `parse_ascii_quarter` function in `parser.py` uses `polars.read_csv` to efficiently parse the dollar-delimited text files. It correctly handles headers and sets `ignore_errors=True` for robustness.
*   **Code Snippet (`py_load_faers/parser.py`):**
    ```python
    df = pl.read_csv(
        file_path,
        separator="$",
        has_header=True,
        ignore_errors=True,
    )
    ```

**R7: Parse the XML format according to ICH E2B(R2) and (R3) specifications.**

**Implementation Analysis:**
*   **Status:** `Implemented`
*   **Details:** The `parse_xml_file` function in `parser.py` uses `lxml.etree.iterparse` for memory-efficient, streaming parsing of large XML files, exactly as recommended by the FRD. It iterates through `<safetyreport>` elements one by one.
*   **Code Snippet (`py_load_faers/parser.py`):**
    ```python
    from lxml import etree
    # ...
    context = etree.iterparse(xml_stream, events=("end",), tag="safetyreport")
    for event, elem in context:
        # ... process element ...
        elem.clear() # Frees memory
    ```

**R8: Support emerging JSON formats compliant with ICH E2B standards.**

**Implementation Analysis:**
*   **Status:** `Not Implemented`
*   **Details:** The `parser.py` module and the orchestration logic in `engine.py` do not contain any functions for handling JSON-formatted FAERS data. The engine can currently only detect and parse XML or ASCII formats.

### 2.4 Intermediate Representation

**R9: The intermediate format must be optimized... Apache Parquet (preferred) or optimized CSV/TSV...**

**Implementation Analysis:**
*   **Status:** `Implemented`
*   **Details:** The application uses an intermediate staging step. The format is configurable via `config.yaml` and supports `parquet` (the default and preferred format) and `csv`.
*   **Code Snippet (`py_load_faers/staging.py`):** The `stage_data` function writes chunks to the format specified in the processing configuration.

**R10: Intermediate data must be partitioned by the 7 core FAERS tables.**

**Implementation Analysis:**
*   **Status:** `Implemented`
*   **Details:** The parsing and staging process separates records into the 7 core FAERS tables (`DEMO`, `DRUG`, `REAC`, etc.). The `stage_data` function writes data into separate files for each table.

**R11: The storage location must be configurable (local disk, memory stream, or cloud object storage).**

**Implementation Analysis:**
*   **Status:** `Partially Implemented`
*   **Details:** The engine currently stages all intermediate files to a temporary local directory created using `tempfile.mkdtemp()`. While the system's temporary directory location can be configured at the OS level, there is no direct application-level setting to specify a custom path, such as a cloud storage URI (e.g., `s3://...`). This limits the extensibility for cloud-native loaders.

---

## 3\. Data Processing and Structure

### 3.1 Deduplication and Versioning Strategy

**R12 (Critical - Deduplication Logic): For each `CASEID`, select the record with the maximum `FDA_DT`. If ties exist, select the record with the maximum `PRIMARYID`.**

**Implementation Analysis:**
*   **Status:** `Implemented`
*   **Details:** This is correctly and efficiently implemented in `processing.py` within the `deduplicate_polars` function. It uses Polars to sort records by `caseid`, then descending by `fda_dt` and `primaryid`, and finally takes the first record for each `caseid`.
*   **Code Snippet (`py_load_faers/processing.py`):**
    ```python
    deduplicated_query = lazy_query.sort(
        by=["caseid", "fda_dt_parsed", "primaryid"],
        descending=[False, True, True], # Sorts by fda_dt DESC, then primaryid DESC
    )
    deduplicated_query = deduplicated_query.unique(
        subset="caseid", keep="first", maintain_order=True
    )
    ```

**R13 (Critical - Case Nullifications and Deletions):**

*   **R13.1 (ASCII):**
    **Implementation Analysis:**
    *   **Status:** `Implemented`
    *   **Details:** The `_parse_deletion_file` helper function within `parser.py` correctly finds and parses deletion files (e.g., `DELE*.TXT`) found within ASCII archives. The extracted `CASEID`s are returned and handled by the engine.

*   **R13.2 (XML/JSON):**
    **Implementation Analysis:**
    *   **Status:** `Implemented (for XML)`
    *   **Details:** The `parse_xml_file` function correctly identifies the `safetyreportnullification` flag in XML records. These `CASEID`s are collected and returned for deletion. JSON is not supported.

**R14 (Full Load Strategy): ...deduplication logic (R12) must be applied globally across all quarters simultaneously.**

**Implementation Analysis:**
*   **Status:** `Not Implemented`
*   **Details:** The application does not have a "full load" mode. The current `delta` and `partial` modes process each quarter independently. While the `deduplicate_polars` function *can* accept a list of files from all quarters, the `FaersLoaderEngine` is not designed to orchestrate this. A true full load would require downloading all historical data and then running the deduplication across all of it at once before loading.

**R15 (Delta Load Strategy): ...Merge the new data with the existing data...**

**Implementation Analysis:**
*   **Status:** `Implemented`
*   **Details:** The delta load process is handled by the `handle_delta_merge` method in the database loader. For PostgreSQL, this method first deletes any existing versions of the `CASEID`s present in the new quarterly data, then bulk-loads the new data. This correctly replaces older versions and adds new cases.
*   **Code Snippet (`py_load_faers_postgres/loader.py`):**
    ```python
    def handle_delta_merge(
        self, case_ids_to_upsert: List[str], data_sources: Dict[str, Path]
    ) -> None:
        # ...
        # First, delete all existing versions of the cases being loaded.
        if case_ids_to_upsert:
            self.execute_deletions(case_ids_to_upsert)

        # Now, bulk load the new data for each table.
        for table in faers_tables:
            # ...
            self.execute_native_bulk_load(table, file_path)
    ```

### 3.2 Data Cleaning

**R16: Normalize character encoding to UTF-8.**

**Implementation Analysis:**
*   **Status:** `Implemented`
*   **Details:** The parsers are designed to handle encoding issues. `polars.read_csv` and file read operations use `errors="ignore"` and default to UTF-8, effectively normalizing source data.

**R17: Robustly parse date fields... Handle partial dates...**

**Implementation Analysis:**
*   **Status:** `Partially Implemented`
*   **Details:** The `deduplicate_polars` function robustly parses `fda_dt` using `str.to_date("%Y%m%d", strict=False)` for the purpose of sorting. However, the system does not have a generic mechanism for normalizing all date fields across all tables or for handling partial dates in a standardized way (e.g., converting YYYYMM to YYYY-MM-01). Data is loaded largely as-is.

**R18: Standardize representations of missing data (empty strings vs. explicit NULLs).**

**Implementation Analysis:**
*   **Status:** `Partially Implemented`
*   **Details:** The PostgreSQL `COPY` command is configured with `NULL ''`, which correctly maps empty strings in the source CSV to database `NULL` values. However, there is no preceding step to standardize other common missing value representations (e.g., "NA", "UNK", "null") to an empty string first.

**R19: Identify, log, and quarantine records that fail critical validation rules (dead-letter queue).**

**Implementation Analysis:**
*   **Status:** `Not Implemented`
*   **Details:** The codebase does not have a dead-letter queue mechanism. Records that fail parsing or validation are typically skipped and logged, or may cause the load process to fail, but they are not systematically quarantined to a separate location for later analysis.

### 3.3 Data Representations

**R20 & R21: Define a standardized, database-agnostic schema...**

**Implementation Analysis:**
*   **Status:** `Implemented`
*   **Details:** The `py_load_faers/models.py` file defines Pydantic models for each of the 7 core FAERS tables. These models serve as the database-agnostic schema definition. The schema generation logic in `PostgresLoader` uses these models to create the DDL, but temporarily disables Primary Key constraints to pass tests.

**R22 - R24: Standardized Representation (Optional Module)...**

**Implementation Analysis:**
*   **Status:** `Not Implemented`
*   **Details:** There is no optional module for enriching the data. The system does not perform drug normalization against RxNorm, age normalization, or country code standardization.

### 3.4 Metadata Management

**R25 - R28: Source and Process Metadata...**

**Implementation Analysis:**
*   **Status:** `Implemented`
*   **Details:** The `PostgresLoader` creates and manages a `_faers_load_history` table. The `FaersLoaderEngine` populates this table for every run with all the fields required by R27, including `load_id`, `quarter`, `status`, `source_checksum`, etc. The `get_last_successful_load` method consults this table to enable delta loads (R28).

---

## 4\. Data Loading Mechanism

**R29 (Full Load), R30 (Delta Load), R31 (Partial Reload)**

**Implementation Analysis:**
*   **Status:**
    *   Full Load (R29): `Not Implemented`
    *   Delta Load (R30): `Implemented`
    *   Partial Reload (R31): `Implemented`
*   **Details:** The `FaersLoaderEngine` and CLI support `delta` and `partial` modes. A `full` load mode that orchestrates the download and global deduplication of all historical data is missing.
*   **CLI Example:**
    ```bash
    # Run a delta load (loads all new quarters since last run)
    faers-loader run --mode delta --profile my_prod_db

    # Run a partial load of a specific quarter
    faers-loader run --mode partial --quarter 2023q4 --profile my_prod_db
    ```

**R32 & R33 (Critical - Native Performance Requirement)**

**Implementation Analysis:**
*   **Status:** `Implemented`
*   **Details:** The `PostgresLoader` uses `cursor.copy()`, which directly leverages the high-performance `COPY FROM STDIN` command in PostgreSQL. It explicitly and correctly avoids row-by-row `INSERT` statements for data loading.

**R34 (Memory Efficiency): ...utilize streaming or chunking techniques...**

**Implementation Analysis:**
*   **Status:** `Implemented`
*   **Details:** The entire pipeline is designed for memory efficiency. It uses streaming XML parsing, generators, chunked intermediate files, and Polars' lazy scanning APIs to ensure that large datasets can be processed with a small, bounded memory footprint.

**R35 & R36 (Transaction Management and Atomicity)**

**Implementation Analysis:**
*   **Status:** `Implemented`
*   **Details:** The `FaersLoaderEngine` wraps each load operation in a transaction using the `begin_transaction`, `commit`, and `rollback` methods of the database loader. The process metadata status is updated to `SUCCESS` only after a successful commit and to `FAILED` upon failure before the exception is re-raised.

**R37 & R38 (Schema Management)**

**Implementation Analysis:**
*   **Status:** `Partially Implemented`
*   **Details:** The system automatically generates and executes `CREATE TABLE` DDL on initialization (R37). However, it does not support schema evolution (e.g., `ALTER TABLE`) as required by R38. Furthermore, comments in the code indicate that PK constraints are temporarily disabled, suggesting the schema management is not yet fully production-ready.

---

## 5\. Architecture and Extensibility

**R39 - R41: Core Architecture & Loader Interface**

**Implementation Analysis:**
*   **Status:** `Implemented`
*   **Details:** The architecture perfectly matches the FRD. The core logic is in the `py_load_faers` package, and a `py_load_faers_postgres` package exists. The `database.py` module defines the `AbstractDatabaseLoader` ABC with all the methods required by R41, providing a clear contract for new extensions.

**R42 - R44: Default Implementation: PostgreSQL**

**Implementation Analysis:**
*   **Status:** `Partially Implemented`
*   **Details:** A PostgreSQL loader is provided (R42) and it correctly uses the `COPY` command (R43). However, it uses the `CSV` format for `COPY`, not the potentially more performant `BINARY` format mentioned as an optimization in R44.

**R45 - R48: Extension/Plugin System**

**Implementation Analysis:**
*   **Status:** `Not Implemented`
*   **Details:** The system does not use Python entry points to dynamically discover database extensions. The `PostgresLoader` is currently hardcoded in the `cli.py` module. The foundation for a plugin system exists with the `AbstractDatabaseLoader`, but the dynamic discovery and loading mechanism is missing.

---

## 6\. Configuration and Usability

**R49 - R51: Configuration Management**

**Implementation Analysis:**
*   **Status:** `Not Implemented`
*   **Details:** The configuration is handled by `pydantic-settings` in `config.py`, but it does not appear to support hierarchical loading from YAML/TOML files as described. It primarily loads from environment variables or a `.env` file. Support for multiple named profiles is present in the CLI (`--profile`), but the underlying configuration loader seems to load a single set of settings. Secure handling via environment variables (R50) is supported.

**R52 & R53: Execution Interfaces (CLI)**

**Implementation Analysis:**
*   **Status:** `Partially Implemented`
*   **Details:** A comprehensive CLI is provided via `Typer` (R52). However, some commands specified in R53 are missing or different.
    *   `download`: Implemented.
    *   `run`: Implemented, but missing the `full` mode.
    *   `db init`: Implemented as `db-init`.
    *   `status`: Not Implemented.

**R54: Python API**

**Implementation Analysis:**
*   **Status:** `Implemented`
*   **Details:** The core classes (`FaersLoaderEngine`, `PostgresLoader`, `AppSettings`) provide a clear and usable Python API for programmatic execution, as demonstrated in the FRD example.
*   **API Usage Example:**
    ```python
    from py_load_faers.config import load_config
    from py_load_faers.engine import FaersLoaderEngine
    from py_load_faers_postgres.loader import PostgresLoader

    # Assumes environment variables for DB credentials are set
    config = load_config(profile="prod_postgres")
    db_loader = PostgresLoader(config.db)

    try:
        db_loader.connect()
        engine = FaersLoaderEngine(config, db_loader)
        engine.run_load(mode="delta")
    finally:
        if db_loader.conn:
            db_loader.conn.close()
    ```

**R55 - R57: Logging and Monitoring**

**Implementation Analysis:**
*   **Status:** `Partially Implemented`
*   **Details:** Standard logging is implemented across all modules (R55). Progress bars (`tqdm`) are used for downloads (R57). However, the logs are not structured in JSON format as required by R56.

---

## 7\. Testing, Maintenance, and Deployment

**R58 - R60: Testing Strategy**

**Implementation Analysis:**
*   **Status:** `Implemented`
*   **Details:** The `tests/` directory and `pyproject.toml` show a comprehensive testing strategy. There are unit tests (`test_parser.py`, etc.) and integration tests (`tests/integration/`) that use `testcontainers` to spin up a live PostgreSQL database (R59). The `db-verify` CLI command and the `run_post_load_dq_checks` method implement the critical data quality check specified in R60.

**R61 - R63: Packaging and Distribution**

**Implementation Analysis:**
*   **Status:** `Implemented`
*   **Details:** The project is set up with `poetry` and a `pyproject.toml` file, adhering to modern packaging standards (R62, R63). It is ready for distribution on PyPI (R61).

**R64 - R66: Code Quality and CI/CD**

**Implementation Analysis:**
*   **Status:** `Implemented`
*   **Details:** The development dependencies in `pyproject.toml` include `mypy`, `black`, and `ruff`, confirming the tools for type-checking and code style enforcement are in place (R64, R65). While the CI/CD pipeline itself (R66) is not visible in the codebase, the presence of the configuration and testing suite indicates the project is set up to support it.

**R67: Documentation**

**Implementation Analysis:**
*   **Status:** `Not Implemented`
*   **Details:** There is no `docs/` directory or configuration for a documentation generator like Sphinx or MkDocs. The only documentation is the `README.md` file and the docstrings within the code. The detailed guides mentioned in the FRD do not exist.
