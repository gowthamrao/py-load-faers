# -*- coding: utf-8 -*-
"""
This module provides functions for parsing FAERS data files.
"""
import csv
import logging
from pathlib import Path
from typing import IO, Iterator, Dict, Any, Tuple, Set

logger = logging.getLogger(__name__)


def parse_xml_file(xml_stream: IO) -> Tuple[Iterator[Dict[str, Any]], Set[str]]:
    """
    Parses a FAERS XML data file from a stream using a memory-efficient approach.

    :param xml_stream: A file-like object (stream) containing the XML data.
    :return: A tuple containing an iterator for safety reports and a set of nullified case IDs.
    """
    from lxml import etree
    from typing import Tuple, Set

    logger.info("Parsing XML stream with full table extraction.")
    nullified_case_ids: Set[str] = set()

    def element_text(elem, path, default=None):
        node = elem.find(path)
        return node.text if node is not None and node.text is not None else default

    def record_generator():
        try:
            context = etree.iterparse(xml_stream, events=('end',), tag='safetyreport')
            for event, elem in context:
                primary_id = element_text(elem, 'safetyreportid')
                # Per ICH E2B, the caseid is nested. Using the path from our test data.
                case_id = element_text(elem, 'case/caseid')

                if not primary_id or not case_id:
                    # If we don't have the core identifiers, skip the record.
                    elem.clear()
                    continue

                # If a report is nullified, the entire case is considered nullified.
                if element_text(elem, 'safetyreportnullification') == '1':
                    nullified_case_ids.add(case_id)
                    elem.clear()
                    continue

                report_records = {
                    "demo": [], "drug": [], "reac": [], "outc": [],
                    "rpsr": [], "ther": [], "indi": []
                }
                patient = elem.find('patient')
                summary = elem.find('summary')

                report_records['demo'].append({
                    'primaryid': primary_id, 'caseid': case_id,
                    'fda_dt': element_text(elem, 'receiptdate'),
                    'sex': element_text(patient, 'patientsex'),
                    'age': element_text(patient, 'patientonsetage'),
                    'age_cod': element_text(patient, 'patientonsetageunit'),
                    'reporter_country': element_text(elem, 'primarysource/reportercountry'),
                    'occr_country': element_text(elem, 'occurcountry'),
                })

                primary_source = elem.find('primarysource')
                if primary_source is not None:
                    report_records['rpsr'].append({
                        'primaryid': primary_id, 'caseid': case_id,
                        'rpsr_cod': element_text(primary_source, 'qualification'),
                    })

                if patient is not None:
                    for drug in patient.findall('drug'):
                        drug_seq = element_text(drug, 'drugsequencenumber')
                        report_records['drug'].append({
                            'primaryid': primary_id, 'caseid': case_id, 'drug_seq': drug_seq,
                            'role_cod': element_text(drug, 'drugcharacterization'),
                            'drugname': element_text(drug, 'medicinalproduct'),
                        })
                        indication = drug.find('drugindication/indicationmeddrapt')
                        if indication is not None:
                            report_records['indi'].append({
                                'primaryid': primary_id, 'caseid': case_id, 'indi_drug_seq': drug_seq,
                                'indi_pt': indication.text,
                            })
                        report_records['ther'].append({
                            'primaryid': primary_id, 'caseid': case_id, 'dsg_drug_seq': drug_seq,
                            'start_dt': element_text(drug, 'drugstartdate'),
                        })
                    for reaction in patient.findall('reaction'):
                        report_records['reac'].append({
                            'primaryid': primary_id, 'caseid': case_id,
                            'pt': element_text(reaction, 'reactionmeddrapt'),
                        })
                if summary is not None:
                    report_records['outc'].append({
                        'primaryid': primary_id, 'caseid': case_id,
                        'outc_cod': element_text(summary, 'result'),
                    })
                yield report_records
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
            del context
        except Exception as e:
            logger.error(f"An unexpected error occurred during XML parsing: {e}", exc_info=True)
            raise
    return record_generator(), nullified_case_ids

def parse_ascii_file(file_path: Path, encoding: str = 'utf-8') -> Iterator[Dict[str, Any]]:
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
        with file_path.open('r', encoding=encoding, errors='ignore') as f:
            # The FAERS files are dollar-delimited, which can be handled by the csv module.
            reader = csv.reader(f, delimiter='$')

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
