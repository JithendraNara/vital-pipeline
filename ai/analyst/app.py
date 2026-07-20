"""Synthetic-only structured cohort API for the local OMOP-shaped demo.

The former natural-language ``/ask`` and ``/plan`` flows are deliberately
disabled. They generated and executed model-provided SQL and sent result rows
to an external service. No partial SQL parser is used here: the only executable
SQL is a deterministic cohort template whose caller-controlled values are
passed to DuckDB as parameters.

Endpoints:
  POST /ask     fixed 503 disabled response; performs no model or database work
  POST /plan    fixed 503 disabled response; performs no model or database work
  POST /cohort  bounded structured filters over synthetic local data
  GET  /schema
  GET  /health
"""
from __future__ import annotations

import json
import logging
import os
import re
from enum import Enum
from typing import Annotated, Any

import duckdb
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, Field, StringConstraints

log = logging.getLogger("vital-analyst")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

SYNTHETIC_DATA_BOUNDARY = "synthetic_only"
DISABLED_DETAIL = "AI-generated SQL routes are disabled for this synthetic-data demo."
MAX_FILTER_VALUES = 20
# OMOP concept identifiers are integer ids. This synthetic API uses a
# non-negative signed 32-bit domain, including OMOP's conventional 0 sentinel.
MAX_OMOP_CONCEPT_ID = 2_147_483_647


def _data_mode(value: str) -> str:
    if value != "synthetic":
        raise ValueError("DATA_MODE must remain 'synthetic'; real patient data is not supported")
    return value


def _schema_name(value: str) -> str:
    """Validate the operator-controlled identifier before placing it in SQL."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("DB_SCHEMA must be a simple SQL identifier")
    return value


DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "dbt_project/dbt.duckdb")
DB_SCHEMA = _schema_name(os.environ.get("DB_SCHEMA", "main"))
DATA_MODE = _data_mode(os.environ.get("DATA_MODE", "synthetic"))

app = FastAPI(
    title="Vital Pipeline Synthetic Cohort Demo",
    description=(
        "Synthetic-only local demonstration. AI-generated SQL is disabled; "
        "only bounded, parameterized structured cohort filters are available."
    ),
    version="0.3.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def run_query(sql: str, parameters: list[object] | None = None) -> list[dict[str, Any]]:
    """Execute an application-owned SQL template with bound values."""
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        df = con.execute(sql, parameters or []).fetch_df()
        return json.loads(df.to_json(orient="records"))
    except Exception as exc:
        log.error("Cohort query failed: %s", exc)
        raise
    finally:
        con.close()


class CcsCategory(str, Enum):
    infectious_disease = "infectious_disease"
    neoplasms = "neoplasms"
    blood_disease = "blood_disease"
    endocrine = "endocrine"
    mental_health = "mental_health"
    nervous_system = "nervous_system"
    eye_disorder = "eye_disorder"
    ear_disorder = "ear_disorder"
    circulatory = "circulatory"
    respiratory = "respiratory"
    digestive = "digestive"
    skin = "skin"
    musculoskeletal = "musculoskeletal"
    genitourinary = "genitourinary"
    pregnancy = "pregnancy"
    perinatal = "perinatal"
    congenital = "congenital"
    symptoms_signs = "symptoms_signs"
    injury = "injury"
    external_cause = "external_cause"
    health_services = "health_services"
    unmapped = "unmapped"


class MeasurementCategory(str, Enum):
    vitals_bp = "vitals_bp"
    vitals_hr = "vitals_hr"
    vitals_temp = "vitals_temp"
    vitals_body = "vitals_body"
    lab_metabolic = "lab_metabolic"
    lab_renal = "lab_renal"
    lab_hematology = "lab_hematology"
    lab_endocrine = "lab_endocrine"
    other = "other"


class AdherenceCategory(str, Enum):
    adherent = "adherent"
    partially_adherent = "partially_adherent"
    non_adherent = "non_adherent"


BoundedFilterValue = Annotated[
    str,
    StringConstraints(min_length=1, max_length=64),
]


class DisabledFeatureResponse(BaseModel):
    status: str
    detail: str


class CohortFilters(BaseModel):
    """Bounded filters supported by the existing synthetic demo schema."""

    model_config = ConfigDict(extra="forbid")

    min_age: int | None = Field(default=None, ge=0, le=130)
    max_age: int | None = Field(default=None, ge=0, le=130)
    gender_concept_id: int | None = Field(
        default=None, ge=0, le=MAX_OMOP_CONCEPT_ID
    )
    ccs_categories: list[CcsCategory] = Field(default_factory=list, max_length=MAX_FILTER_VALUES)
    drug_classes: list[BoundedFilterValue] = Field(
        default_factory=list, max_length=MAX_FILTER_VALUES
    )
    measurement_categories: list[MeasurementCategory] = Field(
        default_factory=list, max_length=MAX_FILTER_VALUES
    )
    adherence_categories: list[AdherenceCategory] = Field(
        default_factory=list, max_length=MAX_FILTER_VALUES
    )
    min_pdc: float | None = Field(default=None, ge=0, le=1)
    min_visits: int | None = Field(default=None, ge=0, le=1_000_000)
    min_drugs: int | None = Field(default=None, ge=0, le=1_000_000)
    min_measurements: int | None = Field(default=None, ge=0, le=1_000_000)
    state: list[BoundedFilterValue] = Field(default_factory=list, max_length=MAX_FILTER_VALUES)


class CohortRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    filters: CohortFilters = Field(default_factory=CohortFilters)


class CohortResponse(BaseModel):
    cohort_size: int
    sql: str
    sample: list[dict[str, Any]]


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "data_boundary": SYNTHETIC_DATA_BOUNDARY}


@app.get("/schema")
def schema() -> dict[str, Any]:
    return {
        "data_boundary": SYNTHETIC_DATA_BOUNDARY,
        "schema": DB_SCHEMA,
        "tables": [
            "omcdm_person",
            "omcdm_condition_occurrence",
            "omcdm_visit_occurrence",
            "omcdm_drug_exposure",
            "omcdm_measurement",
            "mart_member_roster",
            "mart_medication_adherence",
            "mart_condition_drug_pairs",
            "int_member_months",
        ],
    }


def _disabled_response() -> DisabledFeatureResponse:
    return DisabledFeatureResponse(status="disabled", detail=DISABLED_DETAIL)


@app.post("/ask", response_model=DisabledFeatureResponse, status_code=503)
def ask() -> DisabledFeatureResponse:
    """Fail closed: natural-language SQL generation is not exposed."""
    return _disabled_response()


@app.post("/plan", response_model=DisabledFeatureResponse, status_code=503)
def plan() -> DisabledFeatureResponse:
    """Fail closed: multi-step model planning and SQL execution are not exposed."""
    return _disabled_response()


def _placeholders(values: list[object]) -> str:
    return ", ".join("?" for _ in values)


def _enum_values(values: list[Enum]) -> list[str]:
    return [str(value.value) for value in values]


@app.post("/cohort", response_model=CohortResponse)
def cohort(req: CohortRequest) -> CohortResponse:
    """Query only local synthetic data using fixed SQL and bound parameters."""
    f = req.filters
    s_person = f"{DB_SCHEMA}_omop.omcdm_person"
    s_cond = f"{DB_SCHEMA}_omop.omcdm_condition_occurrence"
    s_visit = f"{DB_SCHEMA}_omop.omcdm_visit_occurrence"
    s_drug = f"{DB_SCHEMA}_omop.omcdm_drug_exposure"
    s_meas = f"{DB_SCHEMA}_omop.omcdm_measurement"
    s_roster = f"{DB_SCHEMA}_marts.mart_member_roster"
    s_adherence = f"{DB_SCHEMA}_marts.mart_medication_adherence"

    where_clauses = ["1=1"]
    parameters: list[object] = []

    if f.min_age is not None:
        where_clauses.append(
            f"({s_person}.year_of_birth <= EXTRACT(YEAR FROM CURRENT_DATE) - ?)"
        )
        parameters.append(f.min_age)
    if f.max_age is not None:
        where_clauses.append(
            f"({s_person}.year_of_birth >= EXTRACT(YEAR FROM CURRENT_DATE) - ?)"
        )
        parameters.append(f.max_age)
    if f.gender_concept_id is not None:
        where_clauses.append(f"{s_person}.gender_concept_id = ?")
        parameters.append(f.gender_concept_id)
    if f.ccs_categories:
        values = _enum_values(f.ccs_categories)
        where_clauses.append(
            f"EXISTS (SELECT 1 FROM {s_cond} co "
            f"WHERE co.person_id = {s_person}.person_id "
            f"AND co.ccs_category IN ({_placeholders(values)}))"
        )
        parameters.extend(values)
    if f.drug_classes:
        values = list(f.drug_classes)
        where_clauses.append(
            f"EXISTS (SELECT 1 FROM {s_drug} d "
            f"WHERE d.person_id = {s_person}.person_id "
            f"AND d.drug_code_type IN ({_placeholders(values)}))"
        )
        parameters.extend(values)
    if f.measurement_categories:
        values = _enum_values(f.measurement_categories)
        where_clauses.append(
            f"EXISTS (SELECT 1 FROM {s_meas} m "
            f"WHERE m.person_id = {s_person}.person_id "
            f"AND m.measurement_category IN ({_placeholders(values)}))"
        )
        parameters.extend(values)
    if f.adherence_categories:
        values = _enum_values(f.adherence_categories)
        where_clauses.append(
            f"EXISTS (SELECT 1 FROM {s_adherence} ad "
            f"WHERE ad.person_id = {s_person}.person_id "
            f"AND ad.adherence_category IN ({_placeholders(values)}))"
        )
        parameters.extend(values)
    if f.min_pdc is not None:
        where_clauses.append(
            f"EXISTS (SELECT 1 FROM {s_adherence} ad "
            f"WHERE ad.person_id = {s_person}.person_id AND ad.pdc_score >= ?)"
        )
        parameters.append(f.min_pdc)
    if f.min_visits is not None:
        where_clauses.append(
            f"(SELECT COUNT(*) FROM {s_visit} v "
            f"WHERE v.person_id = {s_person}.person_id) >= ?"
        )
        parameters.append(f.min_visits)
    if f.min_drugs is not None:
        where_clauses.append(
            f"(SELECT COUNT(*) FROM {s_drug} d "
            f"WHERE d.person_id = {s_person}.person_id) >= ?"
        )
        parameters.append(f.min_drugs)
    if f.min_measurements is not None:
        where_clauses.append(
            f"(SELECT COUNT(*) FROM {s_meas} m "
            f"WHERE m.person_id = {s_person}.person_id) >= ?"
        )
        parameters.append(f.min_measurements)
    if f.state:
        values = list(f.state)
        where_clauses.append(
            f"EXISTS (SELECT 1 FROM {s_roster} m "
            f"WHERE m.member_id = {s_person}.person_source_value "
            f"AND m.state IN ({_placeholders(values)}))"
        )
        parameters.extend(values)

    where_sql = " AND ".join(where_clauses)
    sample_sql = f"""
    SELECT person_id, year_of_birth, gender_concept_id
    FROM {s_person}
    WHERE {where_sql}
    LIMIT 1000
    """
    count_sql = f"""
    SELECT COUNT(DISTINCT person_id) AS cohort_size
    FROM {s_person}
    WHERE {where_sql}
    """

    rows = run_query(sample_sql, parameters)
    size_rows = run_query(count_sql, parameters)
    size = size_rows[0].get("cohort_size", 0) if size_rows else 0
    return CohortResponse(cohort_size=int(size), sql=sample_sql, sample=rows[:50])


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
