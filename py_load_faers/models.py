# -*- coding: utf-8 -*-
"""
This module defines the Pydantic models for the 7 core FAERS tables.

NOTE: The schema defined here is inferred from the FAERS documentation and
common data elements. It may require refinement if a more precise, official
schema definition is found.
"""
from typing import Optional
from pydantic import BaseModel, Field


class Demo(BaseModel):
    """Demographic and administrative information."""

    primaryid: str = Field(
        ...,
        description="The unique identifier for a specific version of a case report.",
    )
    caseid: str = Field(..., description="A unique identifier for an adverse event case.")
    caseversion: Optional[str] = None
    i_f_code: Optional[str] = None
    event_dt: Optional[str] = None
    mfr_dt: Optional[str] = None
    init_fda_dt: Optional[str] = None
    fda_dt: Optional[str] = None
    rept_cod: Optional[str] = None
    auth_num: Optional[str] = None
    mfr_num: Optional[str] = None
    mfr_sndr: Optional[str] = None
    lit_ref: Optional[str] = None
    age: Optional[float] = None
    age_cod: Optional[str] = None
    age_grp: Optional[str] = None
    sex: Optional[str] = None
    e_sub: Optional[str] = None
    wt: Optional[float] = None
    wt_cod: Optional[str] = None
    rept_dt: Optional[str] = None
    to_mfr: Optional[str] = None
    occp_cod: Optional[str] = None
    reporter_country: Optional[str] = None
    occr_country: Optional[str] = None


class Drug(BaseModel):
    """Drug information from the case reports."""

    primaryid: str
    caseid: str
    drug_seq: str
    role_cod: str
    drugname: str
    prod_ai: Optional[str] = None
    val_vbm: Optional[str] = None
    route: Optional[str] = None
    dose_vbm: Optional[str] = None
    cum_dose_chr: Optional[str] = None
    cum_dose_unit: Optional[str] = None
    dechal: Optional[str] = None
    rechal: Optional[str] = None
    lot_num: Optional[str] = None
    exp_dt: Optional[str] = None
    nda_num: Optional[str] = None
    dose_amt: Optional[str] = None
    dose_unit: Optional[str] = None
    dose_form: Optional[str] = None
    dose_freq: Optional[str] = None


class Reac(BaseModel):
    """Reaction information from the reports."""

    primaryid: str
    caseid: str
    pt: str
    drug_rec_act: Optional[str] = None


class Outc(BaseModel):
    """Patient outcome information from the reports."""

    primaryid: str
    caseid: str
    outc_cod: str


class Rpsr(BaseModel):
    """Information on the source of the reports."""

    primaryid: str
    caseid: str
    rpsr_cod: str


class Ther(BaseModel):
    """Drug therapy information."""

    primaryid: str
    caseid: str
    dsg_drug_seq: str
    start_dt: Optional[str] = None
    end_dt: Optional[str] = None
    dur: Optional[str] = None
    dur_cod: Optional[str] = None


class Indi(BaseModel):
    """Indication for use for each drug."""

    primaryid: str
    caseid: str
    indi_drug_seq: str
    indi_pt: str


# Mapping of FAERS table names to their Pydantic models
FAERS_TABLE_MODELS = {
    "demo": Demo,
    "drug": Drug,
    "reac": Reac,
    "outc": Outc,
    "rpsr": Rpsr,
    "ther": Ther,
    "indi": Indi,
}
