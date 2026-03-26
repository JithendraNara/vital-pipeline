# vital-pipeline

> Healthcare data operations — eligibility QA, claims analytics, and ETL pipelines.

A portfolio of production-grade healthcare data engineering work: eligibility file validation, claims analytics, and end-to-end ETL pipelines built with the modern data stack.

---

## 🗂️ Project Structure

```
vital-pipeline/
├── notebooks/
│   └── eligibility-qa/           # Eligibility file QA (Sword Health case study)
├── dbt_project/
│   ├── models/                   # dbt data models
│   │   ├── staging/             # Raw → staging
│   │   ├── intermediate/        # Business logic
│   │   └── marts/              # Analytics-ready
│   ├── tests/                   # dbt data quality tests
│   ├── seeds/                   # Reference data (ICD-10, CPT codes)
│   └── macros/                  # Reusable SQL macros
├── sql/
│   └── healthcare-analytics/    # Window functions, CTEs, healthcare queries
├── pipelines/
│   └── eligibility-etl/         # Python + Airflow DAG
├── data_quality/               # Great Expectations suite
└── docs/
    └── data-dictionary.md      # Column-level documentation
```

---

## 🏗️ Architecture

```
[Eligibility CSV] ──▶ [Python QA] ──▶ [Staging Tables]
                                         │
[Claims Data]   ──▶ [dbt Pipeline] ───▶ [Intermediate Models]
   (CMS SynPUF)                            │
                                         ▼
                              [Mart Tables ──▶ Analytics / BI]
```

---

## 🛠️ Tech Stack

| Layer | Tool |
|-------|------|
| Language | Python 3.11+ |
| Notebooks | Jupyter (pandas, matplotlib) |
| Transformation | dbt Core |
| Data Warehouse | PostgreSQL (local) / Snowflake-ready |
| Orchestration | Apache Airflow |
| Data Quality | Great Expectations + dbt tests |
| SQL Style | Window functions, CTEs, recursive queries |

---

## 📋 Eligibility QA

Validates member eligibility files against business rules:

- [x] Zip code format validation (leading zeros preserved)
- [x] Duplicate `mem_id` detection (hard blocker)
- [x] Age consistency (`covered_relation` vs DOB cross-check)
- [x] Missing required fields (DOB, email, phone)
- [x] Active-only filtering (terminated members excluded)
- [x] ICD-10 / CPT code validation
- [x] Downstream impact documentation

See: [notebooks/eligibility-qa/](notebooks/eligibility-qa/)

---

## 🗃️ dbt Models

Three-layer medallion architecture:

**Staging** — raw data cleaned, typed, renamed
- `stg_eligibility_members`
- `stg_claims_medical`
- `stg_claims_pharmacy`

**Intermediate** — business logic applied
- `int_member_months`
- `int_claim_cost_analysis`
- `int_icd10_chronic_conditions`

**Marts** — analytics-ready aggregates
- `mart_member_roster`
- `mart_claims_summary`
- `mart_quality_measures`

---

## 🧪 Data Quality

Every model includes dbt tests:

```yaml
models:
  - name: stg_eligibility_members
    columns:
      - name: mem_id
        tests:
          - unique
          - not_null
      - name: dob
        tests:
          - not_null
          - dbt_utils.recency:
              datepart: year
              interval: 120
              group: null
```

Plus Great Expectations suites for healthcare-specific validation.

---

## 📊 SQL Playbook

Healthcare analytics queries demonstrating:

- **Window functions** — member enrollment spans, claim sequencing, cost running totals
- **Recursive CTEs** — hierarchical diagnosis code traversal
- **Complex joins** — claims ↔ eligibility ↔ provider linking
- **Optimization** — indexing strategy, query plans

See: [sql/healthcare-analytics/](sql/healthcare-analytics/)

---

## 🚀 Setup

```bash
# Clone
git clone https://github.com/JithendraNara/vital-pipeline.git
cd vital-pipeline

# Python environment
python3 -m venv venv
source venv/bin/activate
pip install pandas jupyter great-expectations python-telegram-bot aiohttp

# dbt setup
pip install dbt-postgres
dbt deps

# Airflow
export AIRFLOW_HOME=./airflow
airflow db init
python pipelines/eligibility-etl/dag.py  # register DAG

# Run eligibility QA
cd notebooks/eligibility-qa
jupyter notebook
```

---

## 📖 Data Dictionary

See [docs/data-dictionary.md](docs/data-dictionary.md) for column-level documentation covering:
- Member demographics
- Medical and pharmacy claims
- ICD-10, CPT, HCPCS codes
- Quality measure specifications

---

## 🎯 Key Skills Demonstrated

| Skill | Where |
|-------|-------|
| Python + Pandas | `notebooks/eligibility-qa/` |
| SQL (window fns, CTEs) | `sql/healthcare-analytics/` |
| dbt modeling | `dbt_project/models/` |
| Data quality testing | `dbt_project/tests/` + `data_quality/` |
| ETL pipeline design | `pipelines/eligibility-etl/` |
| Healthcare domain | ICD-10, CPT, PMPM, VBC, HCC |

---

## 📄 License

MIT
