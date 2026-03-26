# Synthea → OMOP Pipeline

## Overview

This pipeline demonstrates the industry-standard approach for generating synthetic healthcare data and transforming it into the **OMOP Common Data Model** (Observational Health Data Sciences and Informatics) using **dbt**.

**Why this matters:**
- **Synthea** (MIT License) generates realistic synthetic patient records — demographics, encounters, conditions, medications, observations, procedures. Used by CMS, VA, and hundreds of health systems for testing.
- **OMOP CDM** is the global standard for observational research analytics. OHDSI (Observational Health Data Sciences and Informatics) maintains it. Over 2 billion patient records mapped to OMOP worldwide.
- **dbt-synthea** (OHDSI) is the official open-source ETL for this conversion.
- This pattern appears in every serious healthcare data engineering portfolio.

## Architecture

```
Synthea (FHIR R4 / CSV)
    ↓
staging_synthea_*     ← raw Synthea tables (conditions, medications, observations...)
    ↓
omop_*                ← OMOP-standard concepts (person, visit_occurrence, drug_exposure...)
    ↓
marts/                ← analytics-ready OMOP marts (condition_popularity, drug_utilization...)
```

## Synthea Data Model (Source)

Synthea outputs CSV files that map to OMOP CDM tables:

| Synthea CSV | OMOP CDM Table | Description |
|-------------|----------------|-------------|
| patients.csv | person | Demographics, birthdate, gender, race |
| encounters.csv | visit_occurrence | Office visits, emergency, inpatient, outpatient |
| conditions.csv | condition_occurrence | Diagnoses (ICD-10-CM) |
| medications.csv | drug_exposure | Prescriptions (RxNorm) |
| procedures.csv | procedure_occurrence | Procedures (CPT/ICD-10-PCS) |
| observations.csv | measurement | Vital signs, lab results |
| allergies.csv | condition_occurrence | Allergy records |
| careplans.csv | care_plan | Treatment plans |

## dbt Models

### Staging Layer (`staging/`)
Raw Synthea CSVs loaded into `staging_synthea_*` tables via dbt external source.

### OMOP Standardization Layer (`omop/`)
- `stg_synthea_conditions.sql` — maps ICD-10-CM → SNOMED-CT via concept_map
- `stg_synthea_medications.sql` — maps RxNorm → RxNorm via concept_map
- `stg_synthea_visits.sql` — maps visit types (emergency, inpatient...) to OMOP visit_concept_id
- `stg_synthea_procedures.sql` — CPT/HCPCS → SNOMED-CT procedure concepts
- `stg_synthea_observations.sql` — lab values and vital signs with unit standardization

### Mart Layer (`marts/`)
- `mart_condition_prevalence.sql` — condition frequency by age group and gender
- `mart_drug_utilization.sql` — prescription volume by drug class, prescribers
- `mart_visit_summary.sql` — visit type distribution, average length of stay
- `mart_patient_characteristics.sql` — demographic summaries for cohort analysis

## Usage

```bash
# 1. Generate Synthea data (requires Java 17+)
cd synthea_pipeline
./run_synthea.sh --exporter.fhir.export=false --exporter.csv.export=true 1000

# 2. Load CSVs into database
python3 load_synthea_csv.py --path ./output/csv/ --target-schema staging_synthea

# 3. Run dbt pipeline
cd dbt_synthea_project
dbt deps
dbt seed
dbt run --target prod
dbt test

# 4. Query OMOP marts
dbt run --select mart_condition_prevalence
```

## OMOP Concept Mapping Notes

OMOP CDM requires standardized vocabularies. Key mappings used:
- ICD-10-CM → SNOMED-CT (conditions)
- RxNorm → RxNorm (drugs — already standardized)
- CPT → SNOMED-CT (procedures)
- LOINC → LOINC (labs/observations)
- Visit types → OMOP visit_concept_ids (38004168=inpatient, 9201=emergency...)

## Reference

- Synthea: https://github.com/synthetichealth/synthea
- OHDSI/dbt-synthea: https://github.com/OHDSI/dbt-synthea
- OMOP CDM: https://ohdsi.github.io/CommonDataModel/
- OHDSI Vocabulary: https://github.com/OHDSI/Vocabulary-v5.0
