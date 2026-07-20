# ML module — synthetic readmission-model demo

This module demonstrates feature construction, training, local MLflow logging, a
FastAPI scoring surface, and a Kafka scoring loop using generated data. The shipped
model name is `readmission_30d`; that name describes the software target, not a
validated medical use.

## Limitations

- The model is trained on generated examples for the documented path. Repository
  tests do not establish real-world discrimination, calibration, subgroup behavior,
  transportability, prospective performance, or patient benefit.
- No model card, fixed evaluation dataset, acceptance threshold, external validation,
  drift policy, or monitored deployment artifact is included.
- A registry stage named `Production` is an MLflow label used by the demo. It is not
  evidence of a deployed or approved model.
- The Kafka scorer and API have no broker-backed end-to-end evidence in the tracked
  unit suite.
- Planned model ideas are not implemented and are intentionally omitted here.

## Security / Data Boundaries

- This is a software demonstration, not clinically validated decision support and
  not intended for diagnosis, treatment, or triage.
- Scores, risk bands, and feature contributions must not be used to make care or
  coverage decisions.
- SHAP output is a model explanation, not clinical causality.
- The API and dashboard have no authentication or patient authorization.
  Keep them on an isolated machine and use generated identifiers only.
- The main scoring path does not integrate the encryption, audit, or access-policy
  utilities from `governance/`.
- This module is not a HIPAA compliance determination and is not approved for PHI.

## Included components

| Component | Path | Role in the demo |
|---|---|---|
| Feature engineering | `ml/outcomes/feature_engineering.py` | Builds model features from generated or local DuckDB inputs |
| Predictor | `ml/outcomes/readmission_predictor.py` | Trains and scores a LightGBM classifier |
| Registry wrapper | `ml/outcomes/model_registry.py` | Logs models and metadata to MLflow |
| Training CLI | `ml/scripts/train.py` | Trains from generated rows or a supplied local DuckDB file |
| Scoring API | `ml/api/app.py` | Returns demo scores for a supplied generated patient ID |
| Kafka scorer | `ml/realtime/scorer.py` | Consumes admission events and publishes prediction events |

## Feature and API shape

[`ml/outcomes/readmission_predictor.py`](outcomes/readmission_predictor.py) declares
the input feature names used by the demo. They cover generated demographics, visit
history, conditions, drugs, recent vitals, and a simplified comorbidity proxy.
[`ml/outcomes/feature_engineering.py`](outcomes/feature_engineering.py) builds the
feature frame from synthetic rows or configured local DuckDB inputs.

`POST /predict` in [`ml/api/app.py`](api/app.py) accepts a `patient_id` and returns
the software contract fields `patient_id`, `score`, `risk_band`,
`top_feature_contributions`, `features_used`, `model_version`, and `scored_at`.
This documents response shape only; it does not supply an expected score or assert
medical meaning.

## Runtime dependencies

```bash
pip install -r ml/requirements.txt
pip install -r streaming/producers/requirements.txt  # Kafka scorer/event contracts
```

## Local synthetic training

```bash
python ml/scripts/train.py --synthetic 1000 --tracking-uri sqlite:///mlflow.db
```

The number above selects a generated training-set size; it is not a reported model
metric or a claim about a run tracked in this repository.

To exercise the local MLflow service defined by this repository:

```bash
docker compose -f ml/docker-compose.ml.yml up -d
python ml/scripts/train.py \
  --synthetic 1000 \
  --tracking-uri http://localhost:5000 \
  --promote
```

## Optional local scorer

The scorer expects a model assigned to MLflow's `Production` stage, a running local
Redpanda broker, and an identifier that exists in the configured OMOP-shaped DuckDB
file. Its `--once PATIENT_ID` option performs one score and publishes one prediction
instead of entering the continuous consumer loop. Exact output depends on the
generated training run and local data; no expected score is asserted here.

## Test dependencies

```bash
python -m pip install pytest
pytest ml/tests/ -v
python -m unittest tests.test_documentation_claims -v
```

These are local unit tests. Their scope does not include medical validation,
authorization, broker recovery, or a controlled performance run.
