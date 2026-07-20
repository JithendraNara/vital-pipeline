# Local synthetic prototype architecture

This diagram describes only the locally implemented portfolio path. Dashed edges
represent optional or illustrative integrations that are not enforced end to end.

## Local batch/dbt lineage

```mermaid
flowchart LR
    Seed["Generated eligibility and Synthea-like tables"]
    Staging["stg_eligibility_members"]
    Intermediate["int_member_months"]
    Person["omcdm_person"]
    Condition["omcdm_condition_occurrence"]
    Visit["omcdm_visit_occurrence"]
    Drug["omcdm_drug_exposure"]
    Measurement["omcdm_measurement"]
    Roster["mart_member_roster"]
    Pairs["mart_condition_drug_pairs"]
    Adherence["mart_medication_adherence"]

    Seed --> Staging --> Intermediate --> Roster
    Seed --> Person
    Seed --> Condition --> Pairs
    Seed --> Visit
    Seed --> Drug --> Pairs
    Seed --> Measurement
    Drug --> Adherence
```

Source links:

- [`dbt_project/models/staging/stg_eligibility_members.sql`](../dbt_project/models/staging/stg_eligibility_members.sql)
- [`dbt_project/models/intermediate/int_member_months.sql`](../dbt_project/models/intermediate/int_member_months.sql)
- [`dbt_project/models/omop/omcdm_person.sql`](../dbt_project/models/omop/omcdm_person.sql)
- [`dbt_project/models/omop/omcdm_condition_occurrence.sql`](../dbt_project/models/omop/omcdm_condition_occurrence.sql)
- [`dbt_project/models/omop/omcdm_visit_occurrence.sql`](../dbt_project/models/omop/omcdm_visit_occurrence.sql)
- [`dbt_project/models/omop/omcdm_drug_exposure.sql`](../dbt_project/models/omop/omcdm_drug_exposure.sql)
- [`dbt_project/models/omop/omcdm_measurement.sql`](../dbt_project/models/omop/omcdm_measurement.sql)
- [`dbt_project/models/marts/mart_member_roster.sql`](../dbt_project/models/marts/mart_member_roster.sql)
- [`dbt_project/models/marts/mart_condition_drug_pairs.sql`](../dbt_project/models/marts/mart_condition_drug_pairs.sql)
- [`dbt_project/models/marts/mart_medication_adherence.sql`](../dbt_project/models/marts/mart_medication_adherence.sql)

This is source lineage, not a claim that every edge has a tracked end-to-end run.

```mermaid
flowchart LR
    subgraph Generated["Generated inputs only"]
        EHR["EHR-style event generator"]
        IoT["Device telemetry simulator"]
        Seed["Synthea-like / eligibility seed data"]
    end

    subgraph LocalData["Local data path"]
        RP["Redpanda\nplaintext local broker"]
        Consumer["Python consumer\nJSON decode + Pydantic validation"]
        OMOP["OMOP-shaped DuckDB\npartial models/mappings"]
        Silver["DuckDB silver tables"]
        Quarantine["DuckDB dlq_silver\noriginal invalid payload"]
    end

    subgraph DemoConsumers["Local demo consumers"]
        Train["Synthetic LightGBM / MLflow demo"]
        Scorer["Kafka scorer / FastAPI"]
        Predictions["healthcare.predictions"]
        Dashboard["Streamlit dashboard\nno authentication"]
    end

    subgraph Separate["Separate illustrative utilities"]
        Crypto["AES-GCM helpers"]
        Policy["Python access-policy evaluator"]
        Audit["DuckDB audit logger"]
        Mask["Masking helpers"]
    end

    EHR --> RP
    IoT --> RP
    RP --> Consumer
    Seed --> OMOP
    OMOP --> Consumer
    Consumer -->|valid| Silver
    Consumer -->|invalid| Quarantine
    Seed --> Train
    Train --> Scorer
    RP --> Scorer
    Scorer --> Predictions
    Predictions --> Dashboard
    OMOP --> Dashboard
    Silver --> Dashboard

    Crypto -. "not integrated across flagship path" .-> Consumer
    Policy -. "not integrated across flagship path" .-> Dashboard
    Audit -. "not integrated across flagship path" .-> Scorer
    Mask -. "standalone transforms" .-> OMOP
```

## Trust boundaries

```mermaid
flowchart TB
    User["Local developer"] --> Services["Host-published local services"]
    Services --> Broker["Plaintext Redpanda"]
    Services --> DB["Local DuckDB files"]
    Services --> UI["Unauthenticated APIs and dashboard"]
    Analyst["Synthetic cohort API\n/ask and /plan disabled"]
    DB --> Analyst
```

- All input is synthetic. Real patient records and PHI are outside the supported
  boundary.
- Local ports, plaintext broker traffic, development credentials, and generated keys
  are suitable only for an isolated development environment.
- In the coordinated SQL-boundary patch, `/ask` and `/plan` are disabled. `/cohort`
  uses bounded, application-owned parameterized SQL over synthetic data, and no
  row-level cohort result is sent to an external model. The API remains
  unauthenticated and local-only.
- Quarantine records contain original invalid payloads and require the same local
  protection as the source stream.
- Governance utilities are shown separately because the primary producer, consumer,
  scorer, and dashboard do not consistently invoke them.

## Evidence boundaries

- Pydantic event validation and DuckDB writes are implemented in
  `streaming/consumers/glue_etl_job.py` local mode.
- Invalid records are written to `dlq_silver`; the `healthcare.dlq` Kafka topic is
  configured but not used by that consumer.
- No broker-backed artifact demonstrates duplicate policy, checkpoint restart,
  compatibility changes, loss, latency, or quarantine rate.
- The ML path is a generated-data software demo and is not clinically validated or
  intended for diagnosis, treatment, or triage.
- The security utilities and local tests do not constitute a HIPAA compliance
  determination, deployment assessment, or control integration proof.
