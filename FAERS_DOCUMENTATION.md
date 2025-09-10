# FAERS Data Source Documentation

This document provides an overview of the FDA Adverse Event Reporting System (FAERS) data source, which is relevant for users of this ETL (Extract, Transform, Load) package.

## What is FAERS?

The FDA Adverse Event Reporting System (FAERS) is a database that contains adverse event reports, medication error reports, and product quality complaints resulting in adverse events that were submitted to the FDA. The database is designed to support the FDA's post-marketing safety surveillance program for drug and therapeutic biologic products.

The informatic structure of the FAERS database adheres to the international safety reporting guidance issued by the International Conference on Harmonisation (ICH E2B).

## Data Source

FAERS data is made available to the public as quarterly data extract files. These files can be downloaded from the [FDA's website](https://fis.fda.gov/extensions/FPD-QDE-FAERS/FPD-QDE-FAERS.html).

### Data Format

The quarterly data files are available in two formats:
*   **ASCII**: Delimited text files.
*   **XML**: Extensible Markup Language files (previously SGML).

This ETL tool is designed to work with these files.

## Data Structure

The FAERS data is provided in a relational format, meaning the data is split across multiple files that can be linked together. A user of this data should be familiar with the concept of relational databases.

Each quarterly data release consists of the following files:

*   `DEMO`: Contains patient demographic and administrative information. This is the primary file, and it contains one record for each report.
*   `DRUG`: Contains drug information. There can be multiple records in this file for each report in the `DEMO` file, as a patient may be taking multiple drugs.
*   `REAC`: Contains all adverse reactions for the report. There can be multiple records in this file for each report in the `DEMO` file.
*   `OUTC`: Contains patient outcomes for the report. There can be multiple records in this file for each report in the `DEMO` file.
*   `RPSR`: Contains the source of the report (e.g., healthcare professional, consumer). There can be multiple records in this file for each report in the `DEMO` file.
*   `THER`: Contains drug therapy start and end dates. There can be multiple therapy date records for each drug in the `DRUG` file.
*   `INDI`: Contains the indications for use (the reason the drug was taken). There can be multiple indications for each drug in the `DRUG` file.

Each of these files has a set of fields that are detailed in the README files provided with the data downloads.

## Key Considerations for Users

When working with FAERS data, it is important to be aware of the following:

*   **Data is not cumulative**: The quarterly data files are not cumulative. Each file contains only the reports processed during that quarter. To get a complete picture, you would need to process files from all quarters of interest.
*   **Relational Structure**: To properly analyze the data, you need to join the different files using their key fields (e.g., `ISR` or `CASE` number).
*   **MedDRA Coding**: Adverse events and medication errors in FAERS are coded using the [Medical Dictionary for Regulatory Activities (MedDRA)](https://www.meddra.org/) terminology. Understanding MedDRA is crucial for interpreting the reaction data.
*   **Data Quality**: Like many large, public databases, FAERS data may contain errors or inconsistencies. A significant issue is the presence of duplicate reports for the same adverse event. Users should consider implementing a de-duplication strategy as part of their analysis.
*   **Updates**: The structure of the FAERS data can change over time. Always refer to the latest FDA documentation and the README files provided with the data for the most current information on file structures and fields.
