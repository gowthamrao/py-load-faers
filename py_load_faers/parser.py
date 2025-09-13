# -*- coding: utf-8 -*-
"""
This module provides functions for parsing FAERS data files.
"""
import csv
import logging
from pathlib import Path
from typing import IO, Any, Dict, Iterator, List, Optional, Set, Tuple

import polars as pl
from lxml import etree

logger = logging.getLogger(__name__)


def parse_ascii_quarter(
    quarter_dir: Path,
) -> Tuple[Iterator[Dict[str, Any]], Set[str]]:
    """
    Parses a directory of unzipped FAERS ASCII files for a single quarter
    using a more memory-efficient, separated-table approach.
    """
    nullified_case_ids = _parse_deletion_file(quarter_dir)
    faers_data = _load_ascii_tables_to_polars(quarter_dir)

    if "demo" not in faers_data or faers_data["demo"].is_empty():
        logger.warning(f"No DEMO data in {quarter_dir}.")
        return iter([]), nullified_case_ids

    # Filter out nullified cases from the main demo table first
    df_demo = faers_data["demo"].filter(~pl.col("caseid").is_in(list(nullified_case_ids)))

    # Get the set of primaryids that we need to keep for all other tables
    primaryids_to_keep = df_demo["primaryid"].to_list()

    # Filter all other tables based on the primaryids to keep
    for name, df in faers_data.items():
        if name != "demo" and df is not None and not df.is_empty():
            faers_data[name] = df.filter(pl.col("primaryid").is_in(primaryids_to_keep))

    def record_generator() -> Iterator[Dict[str, Any]]:
        # Iterate over each valid demo record (one per case version)
        for demo_row in df_demo.to_dicts():
            primary_id = demo_row["primaryid"]
            case_id = demo_row["caseid"]

            report: Dict[str, Any] = {"demo": [demo_row]}

            # For each sub-table, filter its dataframe for the current primaryid
            for table_name, df_table in faers_data.items():
                if table_name == "demo":
                    continue

                table_records: List[Dict[str, Any]] = []
                if df_table is not None and not df_table.is_empty():
                    # Filter the already-reduced table for the specific primary_id
                    records = df_table.filter(pl.col("primaryid") == primary_id).to_dicts()

                    # Add caseid for consistency
                    for r in records:
                        r["caseid"] = case_id
                    table_records.extend(records)

                report[table_name] = table_records

            yield report

    return record_generator(), nullified_case_ids


def _parse_deletion_file(quarter_dir: Path) -> Set[str]:
    """Finds and parses a FAERS deletion file, returning a set of CASEIDs."""
    deletion_patterns = [
        "DELE*.[tT][xX][tT]",
        "DELETED_CASES_*.[tT][xX][tT]",
        "del_*.[tT][xX][tT]",
    ]
    nullified_case_ids: Set[str] = set()
    for pattern in deletion_patterns:
        try:
            deletion_file = next(quarter_dir.glob(pattern))
            logger.info(f"Found deletion file: {deletion_file}")
            with deletion_file.open("r", encoding="utf-8", errors="ignore") as f:
                reader = csv.reader(f, delimiter="$")
                try:
                    header = [h.lower() for h in next(reader)]
                    caseid_idx = header.index("caseid")
                    for row in reader:
                        if row:
                            nullified_case_ids.add(row[caseid_idx])
                except (StopIteration, ValueError):
                    logger.warning(
                        f"Could not read header or find 'caseid' column in " f"{deletion_file}"
                    )
            logger.info(f"Found {len(nullified_case_ids)} case IDs for deletion.")
            return nullified_case_ids
        except StopIteration:
            continue
    logger.info("No deletion file found for this quarter.")
    return nullified_case_ids


def _load_ascii_tables_to_polars(
    quarter_dir: Path,
) -> Dict[str, pl.DataFrame]:
    """
    Loads all recognized FAERS table files from a directory into Polars
    DataFrames.
    """
    table_names = ["demo", "drug", "reac", "outc", "rpsr", "ther", "indi"]
    dataframes: Dict[str, pl.DataFrame] = {}

    for table in table_names:
        try:
            # Find file ignoring case, e.g., DEMO24Q1.TXT or demo24q1.txt
            file_path = next(quarter_dir.glob(f"{table.upper()}*.[tT][xX][tT]"))
            df = pl.read_csv(
                file_path,
                separator="$",
                has_header=True,
                ignore_errors=True,
            )
            # Normalize column names to lowercase and cast all to string
            df.columns = [c.lower() for c in df.columns]
            df = df.with_columns(pl.all().cast(pl.Utf8))
            dataframes[table] = df
            logger.debug(f"Loaded {file_path} into DataFrame with {len(df)} rows.")
        except StopIteration:
            logger.warning(f"No data file found for table '{table}' in {quarter_dir}")
        except Exception as e:
            logger.error(f"Failed to load table {table} from {quarter_dir}: {e}")

    return dataframes


def parse_xml_file(xml_stream: IO[Any]) -> Tuple[Iterator[Dict[str, Any]], Set[str]]:
    """
    Parses a FAERS XML data file from a stream using a memory-efficient
    approach.

    :param xml_stream: A file-like object (stream) containing the XML data.
    :return: A tuple containing an iterator for safety reports and a set of
        nullified case IDs.
    """
    logger.info("Parsing XML stream with full table extraction.")
    nullified_case_ids: Set[str] = set()

    def element_text(
        elem: etree._Element, path: str, default: Optional[str] = None
    ) -> Optional[str]:
        node = elem.find(path)
        return node.text if node is not None and node.text is not None else default

    def record_generator() -> Iterator[Dict[str, Any]]:
        try:
            context = etree.iterparse(xml_stream, events=("end",), tag="safetyreport")
            for event, elem in context:
                primary_id = element_text(elem, "safetyreportid")
                # Per ICH E2B, the caseid is nested. Using the path from our
                # test data.
                case_id = element_text(elem, "case/caseid")

                if not primary_id or not case_id:
                    # If we don't have the core identifiers, skip the record.
                    elem.clear()
                    continue

                # If a report is nullified, the entire case is considered nullified.
                if element_text(elem, "safetyreportnullification") == "1":
                    nullified_case_ids.add(case_id)
                    elem.clear()
                    continue

                report_records: Dict[str, List[Dict[str, Any]]] = {
                    "demo": [],
                    "drug": [],
                    "reac": [],
                    "outc": [],
                    "rpsr": [],
                    "ther": [],
                    "indi": [],
                }
                patient = elem.find("patient")
                summary = elem.find("summary")

                if patient is not None:
                    report_records["demo"].append(
                        {
                            "primaryid": primary_id,
                            "caseid": case_id,
                            "fda_dt": element_text(elem, "receiptdate"),
                            "sex": element_text(patient, "patientsex"),
                            "age": element_text(patient, "patientonsetage"),
                            "age_cod": element_text(patient, "patientonsetageunit"),
                            "reporter_country": element_text(
                                elem, "primarysource/reportercountry"
                            ),
                            "occr_country": element_text(elem, "occurcountry"),
                        }
                    )

                primary_source = elem.find("primarysource")
                if primary_source is not None:
                    report_records["rpsr"].append(
                        {
                            "primaryid": primary_id,
                            "caseid": case_id,
                            "rpsr_cod": element_text(primary_source, "qualification"),
                        }
                    )

                if patient is not None:
                    for drug in patient.findall("drug"):
                        drug_seq = element_text(drug, "drugsequencenumber")
                        report_records["drug"].append(
                            {
                                "primaryid": primary_id,
                                "caseid": case_id,
                                "drug_seq": drug_seq,
                                "role_cod": element_text(drug, "drugcharacterization"),
                                "drugname": element_text(drug, "medicinalproduct"),
                            }
                        )
                        indication = drug.find("drugindication/indicationmeddrapt")
                        if indication is not None:
                            report_records["indi"].append(
                                {
                                    "primaryid": primary_id,
                                    "caseid": case_id,
                                    "indi_drug_seq": drug_seq,
                                    "indi_pt": indication.text,
                                }
                            )
                        report_records["ther"].append(
                            {
                                "primaryid": primary_id,
                                "caseid": case_id,
                                "dsg_drug_seq": drug_seq,
                                "start_dt": element_text(drug, "drugstartdate"),
                            }
                        )
                    for reaction in patient.findall("reaction"):
                        report_records["reac"].append(
                            {
                                "primaryid": primary_id,
                                "caseid": case_id,
                                "pt": element_text(reaction, "reactionmeddrapt"),
                            }
                        )
                if summary is not None:
                    report_records["outc"].append(
                        {
                            "primaryid": primary_id,
                            "caseid": case_id,
                            "outc_cod": element_text(summary, "result"),
                        }
                    )
                yield report_records
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
            del context
        except Exception as e:
            logger.error(f"An unexpected error occurred during XML parsing: {e}", exc_info=True)
            raise

    return record_generator(), nullified_case_ids


def parse_ascii_file(file_path: Path, encoding: str = "utf-8") -> Iterator[Dict[str, Any]]:
    """
    Parses a dollar-delimited FAERS ASCII data file.

    This function reads the file, determines headers from the first line,
    and yields each subsequent row as a dictionary. It can handle
    different file encodings.

    :param file_path: The path to the ASCII data file.
    :param encoding: The encoding of the file.
    :return: An iterator that yields each row as a dictionary.
    """
    logger.info(f"Parsing ASCII file: {file_path} with encoding {encoding}")

    try:
        with file_path.open("r", encoding=encoding, errors="ignore") as f:
            # The FAERS files are dollar-delimited, which can be handled by the csv module.
            reader = csv.reader(f, delimiter="$")

            # Read the header row and normalize column names to lowercase
            try:
                headers = [h.lower() for h in next(reader)]
            except StopIteration:
                logger.warning(f"File {file_path} is empty or has no header.")
                return

            # Yield each row as a dictionary
            for row in reader:
                # Ensure the row has the same number of columns as the header
                if len(row) == len(headers):
                    yield dict(zip(headers, row))
                else:
                    logger.warning(f"Skipping malformed row in {file_path}: {row}")

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except Exception as e:
        logger.error(f"An error occurred while parsing {file_path}: {e}")
        raise
