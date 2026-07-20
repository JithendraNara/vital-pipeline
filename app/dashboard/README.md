# Dashboard module — local synthetic-data viewer

This Streamlit application displays generated prediction messages and local
OMOP-shaped/DuckDB context. It is a read-only software demo with three views: a
recent-prediction board, patient detail, and local pipeline table counts.

## Limitations

- The dashboard is not a monitoring service and has no availability, freshness,
  completeness, latency, or recovery guarantee.
- Its in-memory prediction buffer starts at the latest Kafka offset and is bounded;
  it is not a durable history.
- Table counts indicate only what is present in the configured local databases.
- Model scores and SHAP feature contributions come from the synthetic ML demo. They
  have no established medical validity or causal interpretation.
- The displayed age logic and OMOP-shaped source models are simplified for the demo.

## Security / Data Boundaries

- The application has no authentication, authorization, session policy, consent
  check, audit integration, or field-level access enforcement.
- It can display patient-level identifiers, demographics, conditions, visits,
  measurements, and model outputs from its configured databases.
- Run it only on an isolated development machine with synthetic data. Do not expose
  it to an untrusted network or configure it with real patient records.
- Read-only DuckDB connections prevent dashboard writes to those databases; they do
  not provide user authentication or a complete data-access boundary.
- This module is not a HIPAA compliance determination and is not intended for
  diagnosis, treatment, or triage.

## Views

- **Recent prediction board** — keeps a bounded in-memory window of messages from
  `healthcare.predictions` and renders score/risk-band summaries and feature
  contributions from the synthetic model demo.
- **Patient detail** — reads configured OMOP-shaped and silver DuckDB files to show
  synthetic demographics, conditions, visits, measurements, and a recent score.
- **Pipeline table counts** — displays local row counts and connection/model state.
  These are convenience diagnostics, not health or freshness guarantees.

## Runtime dependencies

```bash
pip install -r app/requirements.txt
pip install -r streaming/producers/requirements.txt  # confluent-kafka consumer
streamlit run app/dashboard/clinical_dashboard.py --server.port 8501
```

Then open <http://localhost:8501> on the same development machine.

The optional prediction view expects the local Redpanda and ML scorer described in
the top-level and ML READMEs. The other views expect the configured local DuckDB
files. Missing services or tables may produce empty or unavailable views.

## Implementation notes

- A background Kafka consumer is cached with `st.cache_resource`.
- Recent prediction messages are held in a bounded `collections.deque`.
- DuckDB connections used for context queries are opened read-only.
- No dashboard action updates a patient record or retrains a model.

## Test dependencies

```bash
python -m pip install pytest
pytest app/dashboard/tests/ -v
python -m unittest tests.test_documentation_claims -v
```

The tests cover local application behavior. They do not demonstrate authentication,
privacy controls, medical validation, or end-to-end pipeline health.
