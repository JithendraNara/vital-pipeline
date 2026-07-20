# real-time-healthcare-pipeline

> Synthetic healthcare streaming prototype for local development.
>
> Generated EHR/IoT-style events → Redpanda → Pydantic validation → DuckDB
> silver and quarantine tables, with a local ML demo and Streamlit dashboard.

[![OMOP CDM](https://img.shields.io/badge/OMOP-shaped_v5.4-blue)](https://ohdsi.github.io/CommonDataModel/)
[![Kafka](https://img.shields.io/badge/Kafka-Redpanda-black)](https://redpanda.com/)

This repository demonstrates a local, synthetic data path. Event producers publish
generated records to Redpanda, the local consumer validates them with Pydantic,
optionally enriches them from an OMOP-shaped DuckDB database, and writes DuckDB
silver tables. Invalid records are stored in a local `dlq_silver` quarantine table.
The repository also contains a synthetic readmission-model demo, a dashboard, and
standalone encryption, access-policy, audit, and masking utilities.

## Limitations

- This project uses synthetic data only. No real patient records are included or
  approved for use.
- It is a portfolio prototype, not a service deployment. There is no demonstrated
  cloud streaming sink, catalog integration, deployment run, performance benchmark,
  recovery run, or schema-compatibility run.
- Duplicate event behavior, consumer restart behavior, message loss, quarantine
  routing, and end-to-end latency have not been demonstrated with a broker-backed
  test artifact.
- The consumer writes invalid records to a DuckDB quarantine table; it does not
  publish them to the configured `healthcare.dlq` topic.
- The OMOP-shaped models provide partial OMOP coverage and partial terminology
  mappings. They are not a complete or validated OMOP implementation.
- The readmission model is a local software demonstration. It is not clinically validated,
  calibrated for a target population, fairness-tested, or prospectively evaluated.
  It is not intended for diagnosis, treatment, or triage.
- SHAP output is a model explanation, not clinical causality.
- No throughput, latency, accuracy, patient count, or quality-check count in this
  README should be inferred beyond the output of a run performed by the reader.

## Security / Data Boundaries

- The repository is not a HIPAA compliance determination, certification, or
  independent security assessment. It includes no BAA, organizational policy set,
  identity provider, consent workflow, or incident-response program.
- Local Redpanda uses plaintext transport and development-only service bindings.
  Local credentials and generated keys are disposable development values, not
  deployment controls.
- The governance package contains illustrative utilities. They are not integrated
  across the producer, local consumer, model API, and dashboard data path.
- The dashboard has no authentication and can display patient-level synthetic data.
  Do not expose it to an untrusted network or connect it to real data.
- In the coordinated SQL-boundary patch, `/ask` and `/plan` are disabled and the
  remaining bounded, parameterized `/cohort` route is synthetic-only. No row-level result is sent to an external model.
  The API remains unauthenticated; do not expose it or connect it to real data.
- Invalid payloads are copied into the local quarantine table without redaction.
  Use only generated data and treat the quarantine database as sensitive.

## Demonstrated local architecture

```text
generated EHR/IoT-style events
             |
             v
   Redpanda Kafka topics
             |
             v
 Pydantic validation + optional OMOP-shaped DuckDB enrichment
             |
             +------ valid ------> DuckDB *_silver tables
             |
             +------ invalid ----> DuckDB dlq_silver quarantine table

synthetic training data -> LightGBM/MLflow demo -> prediction topic/API
                                                    |
                                                    v
                                        unauthenticated local dashboard

Illustrative governance utilities are separate from this data path.
```

See [the streaming module](streaming/README.md),
[the ML module](ml/README.md), [the governance module](governance/README.md),
[the dashboard module](app/dashboard/README.md), and
[the architecture notes](docs/architecture_diagram.md) for the same boundaries.

## Components

| Area | What is present |
|---|---|
| Streaming | Redpanda topic configuration, generated producers, Pydantic event models, local Kafka consumer |
| Local storage | DuckDB silver tables and `dlq_silver` quarantine table |
| Batch data | dbt models over generated eligibility and Synthea-like inputs, shaped around a subset of OMOP |
| ML demo | Synthetic training CLI, LightGBM predictor, MLflow wrapper, FastAPI scorer, Kafka scorer |
| Dashboard | Read-only Streamlit views over local DuckDB data and prediction messages |
| Governance utilities | AES-GCM helpers, a DuckDB audit logger, Python access-policy evaluator, masking helpers, consumer wrapper |
| Cohort API | Coordinated SQL patch disables `/ask` and `/plan`; `/cohort` uses application-owned parameterized SQL over synthetic data |

## Implemented local data models

The local dbt project is source-visible and runnable against DuckDB. It is
OMOP-shaped rather than a complete or certified CDM implementation.

- Staging: [`stg_eligibility_members.sql`](dbt_project/models/staging/stg_eligibility_members.sql)
- Intermediate: [`int_member_months.sql`](dbt_project/models/intermediate/int_member_months.sql)
- OMOP-shaped models: [`person`](dbt_project/models/omop/omcdm_person.sql),
  [`condition_occurrence`](dbt_project/models/omop/omcdm_condition_occurrence.sql),
  [`visit_occurrence`](dbt_project/models/omop/omcdm_visit_occurrence.sql),
  [`drug_exposure`](dbt_project/models/omop/omcdm_drug_exposure.sql), and
  [`measurement`](dbt_project/models/omop/omcdm_measurement.sql)
- Marts: [`mart_member_roster.sql`](dbt_project/models/marts/mart_member_roster.sql),
  [`mart_condition_drug_pairs.sql`](dbt_project/models/marts/mart_condition_drug_pairs.sql),
  and [`mart_medication_adherence.sql`](dbt_project/models/marts/mart_medication_adherence.sql)

See the [data dictionary](docs/data-dictionary.md) for field descriptions. Coverage
and terminology mappings remain partial, as stated above.

## Contracts and quality utilities

- [`streaming/schemas/events.py`](streaming/schemas/events.py) defines the Pydantic
  event contracts used by the generated producers and local consumer.
- [`data_quality/run_gx_suite.py`](data_quality/run_gx_suite.py) contains direct DuckDB/SQL column
  checks, OMOP-shaped row-level checks, and a freshness check. Run
  output, not a hand-maintained count, is the evidence for a particular execution.
- [`eligibility_data_contract.yml`](data_contracts/eligibility_data_contract.yml) is
  an example data contract for the eligibility input.

## Illustrative orchestration and infrastructure source

These files are useful design artifacts but are not deployment or readiness evidence:

- [`pipelines/eligibility-etl/dag.py`](pipelines/eligibility-etl/dag.py) — Airflow
  source for the eligibility flow; not demonstrated by a tracked scheduler run.
- [`prefect_flows/real_time_healthcare_flow.py`](prefect_flows/real_time_healthcare_flow.py)
  — incomplete local orchestration source; it is not the canonical quickstart and
  has no bounded end-to-end proof.
- [`infrastructure/main.tf`](infrastructure/main.tf) — incomplete, illustrative
  Terraform source; it is not a validated deployment configuration.

## Local quickstart

Run commands from the repository root. These steps intentionally use generated
data and local services.

```bash
# Install the local batch and streaming dependencies.
pip install -r streaming/producers/requirements.txt
pip install dbt-core dbt-duckdb duckdb

# Seed an OMOP-shaped DuckDB database and build the local dbt models.
python scripts/seed_omop.py --patients 500
cd dbt_project
mkdir -p ~/.dbt
cp profiles.yml.example ~/.dbt/profiles.yml
DBT_PROFILES_DIR=~/.dbt dbt seed --profile vital_pipeline --target local
DBT_PROFILES_DIR=~/.dbt dbt build --profile vital_pipeline --target local
cd ..

# Start the local broker and create the configured topics.
docker compose -f docker-compose.yml \
  -f streaming/docker-compose.streaming.yml \
  up -d redpanda redpanda-console
python streaming/scripts/create_topics.py
```

In separate terminals:

```bash
# Local consumer: Pydantic validation and DuckDB output.
python streaming/consumers/glue_etl_job.py --mode local

# Generated EHR-style events. Stop with Ctrl-C.
python streaming/producers/healthcare_producer.py --patients 50 --rate 20

# Generated device telemetry. Stop with Ctrl-C.
python streaming/seeders/iot_device_simulator.py --patients 50
```

Inspect local output with DuckDB:

```bash
duckdb streaming/warehouse/silver.db \
  -c "SELECT COUNT(*) FROM vitals_silver; SELECT COUNT(*) FROM dlq_silver;"
```

The counts are run-dependent; this repository does not provide a recorded
performance or recovery result.

## Optional local ML demo

```bash
pip install -r ml/requirements.txt
python ml/scripts/train.py --synthetic 1000 --tracking-uri sqlite:///mlflow.db
```

See `ml/README.md` before starting the scorer or dashboard. Their outputs are for
software demonstration only.

## Governance utility checks

```bash
pip install -r governance/requirements.txt
python -m pip install pytest
pytest governance/tests/ -v
```

Passing these unit tests demonstrates the utility behavior under test. It does not
establish integration across the pipeline or compliance with any regulation.

## Documentation claim check

The claim boundary uses only the Python standard library:

```bash
python -m unittest tests.test_documentation_claims -v
```

For combined CI integration, the required contract job should use this exact command
so the workflow contract and documentation boundary share one JUnit artifact:

```bash
pytest tests/test_workflow_contract.py tests/test_documentation_claims.py -v --junitxml=ci-contract.xml
```

Other test suites have separate dependencies and scopes. Run the commands in their
module documentation; do not treat a local subset as evidence for the full system.

## Project structure

```text
real-time-healthcare-pipeline/
├── streaming/             # Redpanda producers, event models, local consumer
├── dbt_project/           # Local dbt models and DuckDB profile example
├── scripts/               # Generated OMOP-shaped seed data
├── ml/                    # Synthetic model demo
├── governance/            # Standalone illustrative controls
├── app/dashboard/         # Local unauthenticated Streamlit dashboard
├── ai/analyst/            # Synthetic cohort API; `/ask` and `/plan` disabled
├── docs/                  # Architecture and data dictionary
└── tests/                 # Documentation claim boundary
```

## License

MIT
