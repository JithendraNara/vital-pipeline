# vital-pipeline

> Healthcare data operations — eligibility QA, claims analytics, and ETL pipelines.

A portfolio of production-grade healthcare data engineering work: eligibility file validation, claims analytics, and end-to-end ETL pipelines built with the modern data stack.

---

## 🗂️ Project Structure

```
vital-pipeline/
├── notebooks/
│   └── eligibility-qa/         # Eligibility file QA notebook + synthetic data
├── ai/
│   ├── anomaly_detection/      # Claims ML anomaly detection (sklearn)
│   └── qa_assistant/           # LLM natural language SQL interface (MiniMax)
├── synthea_pipeline/           # Synthea → OMOP CDM via dbt
├── docs_parsing/               # PDF parsing pipeline (LiteParse + Tesseract OCR)
├── dbt_project/
│   ├── models/                 # dbt data models
│   │   ├── staging/            # Raw → staging
│   │   ├── intermediate/       # Business logic
│   │   └── marts/              # Analytics-ready
│   ├── tests/                  # dbt data quality tests
│   ├── seeds/                  # Reference data (ICD-10, CPT codes)
│   └── macros/                 # Reusable SQL macros
├── sql/
│   └── healthcare-analytics/   # Window functions, CTEs, healthcare queries
├── pipelines/
│   └── eligibility-etl/        # Python + Airflow DAG
├── data_quality/              # Great Expectations suite
├── prefect_flows/             # Prefect 3.x pipeline (modern orchestration)
├── infrastructure/            # Terraform IaC (AWS VPC, RDS, S3, ECS)
├── data_contracts/            # Open Data Contract Standard YAML SLA
├── docs/
│   ├── data-dictionary.md     # Column-level documentation
│   └── architecture_diagram.md # Full Mermaid architecture diagrams
└── .github/workflows/
    └── ci.yaml               # 6-job CI pipeline
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

## 🤖 AI / ML Layer

### Anomaly Detection (`ai/anomaly_detection/`)
Detects unusual claims patterns using statistical and ML methods:
- **Z-score** — univariate cost outliers (>3σ)
- **IQR** — non-parametric outlier bounds
- **Isolation Forest** — multivariate ML anomaly detection
- **Utilization analysis** — flags members with abnormally high claim frequency
- Risk tiers: Normal → Monitor → Elevated → Critical

See: [ai/anomaly_detection/](ai/anomaly_detection/)

### LLM QA Assistant (`ai/qa_assistant/`)
Natural language interface to the eligibility dataset:
- Ask questions in plain English → SQL is generated and executed
- Built with MiniMax + SQLite
- Safe: read-only queries, no data modification
- Demo mode with 8 pre-set healthcare data quality questions

See: [ai/qa_assistant/](ai/qa_assistant/)

---

## 📄 PDF Document Parsing (`docs_parsing/`)

LiteParse (LlamaIndex, open-source) — fast local PDF parsing with bounding boxes + OCR:
- **No cloud dependency** — 100% local, HIPAA-compliant for PHI documents
- Built-in Tesseract.js OCR — scanned documents supported
- Bounding box extraction — field-level parsing (extract member ID without touching the rest)
- Batch directory processing — parse dozens of PDFs in one run
- Screenshot generation — visual page images for LLM verification
- Feeds directly into the LLM QA assistant for natural language querying

See: [docs_parsing/](docs_parsing/)

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
| **AI/ML anomaly detection** | `ai/anomaly_detection/` (Isolation Forest, Z-score, IQR) |
| **LLM + natural language SQL** | `ai/qa_assistant/` (MiniMax, RAG-style Q&A) |
| **PDF parsing + OCR** | `docs_parsing/` (LiteParse, Tesseract.js) |
| SQL (window fns, CTEs) | `sql/healthcare-analytics/` |
| dbt modeling | `dbt_project/models/` |
| OMOP CDM mapping | `synthea_pipeline/` (ICD-10 → SNOMED, RxNorm) |
| Prefect orchestration | `prefect_flows/` (modern Airflow alternative) |
| Terraform IaC | `infrastructure/` (AWS VPC, RDS, S3, ECS) |
| Data contracts | `data_contracts/` (ODCS v3 YAML SLA) |
| Healthcare domain | ICD-10, CPT, PMPM, VBC, HCC, OMOP |

---

## 📄 License

MIT
