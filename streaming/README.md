# Streaming module — local synthetic EHR and IoT events

This module demonstrates generated events flowing through Redpanda to a Python
consumer. Pydantic models validate event payloads, an optional local DuckDB lookup
adds limited person attributes, and valid records are written to DuckDB silver
tables. Invalid records are copied into the DuckDB `dlq_silver` quarantine table.

```text
generated producers -> Redpanda -> Pydantic validation -> DuckDB *_silver
                                         |
                                         +-----------> DuckDB dlq_silver
```

## Limitations

- Only the local `--mode local` consumer path is implemented. AWS Glue mode is not implemented:
  `consumers/glue_etl_job.py` raises `NotImplementedError` for it.
- JSON schema files are tracked for reference, but the producer and consumer do not
  demonstrate compatibility enforcement or registration with a schema service.
- The configured `healthcare.dlq` topic is not used by the consumer. Validation
  failures go to the local DuckDB quarantine table with the original payload.
- The `(topic, partition_id, offset_id)` primary key and replacement write behavior
  have no broker-backed duplicate or restart proof. They are not evidence of
  event-level deduplication or exactly-once processing.
- No tracked run artifact establishes throughput, end-to-end latency, loss, restart
  behavior, or quarantine rate.
- This module uses generated data and is not a real EHR or device integration.

## Security / Data Boundaries

- The compose overlay uses plaintext Kafka transport, unauthenticated local services,
  and host-published ports. Keep it on an isolated development machine.
- Patient, encounter, provider, facility, and device identifiers are generated but
  are still copied into Kafka keys/payloads and local tables. Do not substitute real
  identifiers.
- Invalid payloads are stored without redaction in `dlq_silver`.
- The main producer and consumer do not call the encryption, audit, or access-policy
  utilities in `governance/`. The separate governed-consumer example does not make
  this path governed end to end.
- This module is not a HIPAA compliance determination and is not approved for PHI.

## Topics

`streaming/topics.yaml` configures these local topics:

| Topic | Local producer or consumer |
|---|---|
| `healthcare.vitals` | generated EHR producer; local consumer |
| `healthcare.admissions` | generated EHR producer; local consumer and ML scorer |
| `healthcare.lab_results` | generated EHR producer; local consumer |
| `iot.telemetry` | generated device simulator; local consumer |
| `healthcare.predictions` | local ML scorer output and dashboard input |
| `healthcare.dlq` | configured only; the local consumer writes DuckDB quarantine rows instead |

Payloads are JSON. The Python boundary uses Pydantic event models in
`streaming/schemas/events.py`.

## Event contracts

[`streaming/schemas/events.py`](schemas/events.py) defines the implemented payload
types: `VitalsEvent`, `AdmissionEvent`, `LabResultEvent`, `IoTTelemetryEvent`,
`PredictionEvent`, and `DeadLetterEvent`. The producers validate their generated
objects before publishing, and the local consumer selects a Pydantic type by topic.
The tracked JSON schema files are reference artifacts; they are not evidence of
registry-backed compatibility enforcement.

## Local quickstart

Run from the repository root:

```bash
pip install -r streaming/producers/requirements.txt

docker compose -f docker-compose.yml \
  -f streaming/docker-compose.streaming.yml \
  up -d redpanda redpanda-console

python streaming/scripts/create_topics.py
```

Start the consumer:

```bash
python streaming/consumers/glue_etl_job.py \
  --mode local \
  --omop-duckdb dbt_project/dbt.duckdb
```

Start one or both generated producers in other terminals:

```bash
python streaming/producers/healthcare_producer.py --patients 50 --rate 20
python streaming/seeders/iot_device_simulator.py --patients 50
```

For a bounded local consumer run, add `--max-runtime SECONDS`. The producer also
accepts `--max-runtime SECONDS`; the device simulator runs until interrupted.

Inspect the DuckDB tables:

```bash
duckdb streaming/warehouse/silver.db \
  -c "SHOW TABLES; SELECT COUNT(*) FROM vitals_silver; SELECT COUNT(*) FROM dlq_silver;"
```

Do not expect fixed counts; they depend on the commands and runtime used.

## What the local consumer does

1. Polls `healthcare.vitals`, `healthcare.admissions`,
   `healthcare.lab_results`, and `iot.telemetry` by default.
2. Decodes JSON and validates it against the matching Pydantic model.
3. Optionally looks up `gender_concept_id` and `year_of_birth` in the local
   `omcdm_person` table.
4. Writes valid batches to topic-specific DuckDB silver tables.
5. Writes validation failures to `dlq_silver`.
6. Commits consumer offsets after selected flush paths.

That description is implementation behavior, not a recovery guarantee.

## Runtime dependencies

```bash
pip install -r streaming/producers/requirements.txt
```

This installs the Kafka client, Pydantic, YAML, and DuckDB packages used by the local
producer and consumer.

## Test dependencies

```bash
python -m pip install pytest
pytest streaming/tests/ -v
python -m unittest tests.test_documentation_claims -v
```

The tracked streaming suite is unit-level and requires no broker. It does not prove
the missing duplicate, restart, compatibility, or quarantine-routing behaviors.
