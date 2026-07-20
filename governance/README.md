# Governance module — illustrative local utilities

This package contains independently testable examples for AES-GCM encryption,
field classification, DuckDB-backed audit events, Python access-policy evaluation,
masking transformations, and a Kafka consumer wrapper. It is a collection of
building blocks for generated local data, not an integrated governance system.

## Limitations

- The main producer, streaming consumer, model API, analyst API, and dashboard do
  not use these utilities end to end; the utilities are not integrated across the
  flagship path.
- The Python policy engine is local application logic. It is not an identity system,
  authorization service, organizational policy, or deployment policy.
- The audit backend is append-only only by application convention. It has no
  tamper-evident chain, signature, retention enforcement, or independent monitor.
- Masking helpers apply selected transformations. They do not by themselves prove
  de-identification, re-identification risk, or suitability for a data release.
- Local key handling does not demonstrate managed key storage, rotation, recovery,
  separation of duties, or deployment access controls.
- Unit tests cover utility behavior only. There is no pipeline integration test that
  proves all sensitive reads and writes pass through these utilities.

## Security / Data Boundaries

- This module is not a HIPAA compliance determination, certification, or security
  assessment and is not approved for real PHI.
- Use only generated data. The local key in `HEALTHCARE_KMS_KEY` is a disposable
  development secret; do not commit it or treat it as a deployment key.
- Authentication, transport encryption, broker authorization, service identity,
  consent, incident response, and external-service policy are outside this module.
- The governed-consumer wrapper emits read audit records, but wrapping a consumer
  does not encrypt producer payloads, protect Kafka keys, redact quarantine records,
  or authorize dashboard access.
- Deterministic encryption leaks equality patterns by design and needs a threat model
  before any use beyond this local example.
- When `HEALTHCARE_KMS_DETERMINISTIC_PEPPER` is configured, deterministic envelopes
  use a derived `-det` key id that the current decrypt path cannot resolve. A
  deterministic round trip can therefore raise `InvalidTag`. Do not showcase or
  rely on pepper-enabled deterministic encryption until that key-resolution gap has
  an implementation fix and regression test.

## Included components

| Component | Path | Demonstrated local behavior |
|---|---|---|
| Crypto service | `governance/encryption/crypto.py` | AES-GCM envelope encrypt/decrypt with local key-manager abstractions |
| PHI field registry | `governance/encryption/phi_fields.py` | Field-name classification metadata used by the encryptor |
| Record encryptor | `governance/encryption/encryptor.py` | Applies configured field transforms to dictionary-like records |
| Audit logger | `governance/audit/audit_logger.py` | Writes application audit events to a DuckDB backend |
| Masking helpers | `governance/masking/deidentify.py` | Hashing, suppression, generalization, and date-reduction examples |
| Policy evaluator | `governance/rbac/policies.py` | Pure-Python allow/deny decisions for defined sample roles and fields |
| Consumer wrapper | `governance/middleware/governed_consumer.py` | Wraps consumer polling with audit emission |

## Local encryption example

```python
from governance.encryption.crypto import CryptoService
from governance.encryption.encryptor import PHIEncryptor

crypto = CryptoService()
encryptor = PHIEncryptor(crypto)

envelope = encryptor.encrypt_value(
    "person.email", "synthetic@example.invalid", context_id="synthetic-12345"
)
plaintext = encryptor.decrypt_value("person.email", envelope)
```

`person.email` uses random-IV mode, so this example does not exercise the deterministic
pepper gap. It demonstrates the library call only; it does not mean records produced
by `streaming/producers/healthcare_producer.py` are encrypted.

## Local access-policy example

```python
from governance.rbac.policies import Actor, AccessRequest, PolicyEngine, Resource

decision = PolicyEngine().evaluate(
    AccessRequest(
        actor=Actor(id="synthetic-user", role="data_scientist"),
        action="read",
        resource=Resource(
            type="table", id="omcdm_person", fields=["person.mrn"]
        ),
        purpose="model_training",
    )
)
```

The caller supplies the actor and purpose. This example does not authenticate them.
The source also includes an illustrative `clinician` role. Its name and sample field
matrix document policy-engine behavior only; they do not establish identity,
authorization integration, appropriate medical access, or compliance.

Masking transformations such as `deidentify_omop_person`, `deidentify_visit`, and
`deidentify_iot_event` are implemented in
[`governance/masking/deidentify.py`](masking/deidentify.py). They are examples of
hashing, suppression, and generalization—not a release-safety determination.

## Runtime dependencies

```bash
pip install -r governance/requirements.txt

# Create a disposable local key for this shell.
export HEALTHCARE_KMS_KEY=$(python -c \
  "import os,base64; print(base64.b64encode(os.urandom(32)).decode())")
```

## Test dependencies

```bash
python -m pip install pytest
pytest governance/tests/ -v
python -m unittest tests.test_documentation_claims -v
```

The optional `governance/scripts/e2e_governance_test.py` exercises a separate local
example with Redpanda. It does not retrofit the flagship producer/consumer/dashboard
path and must not be used as evidence that all reads and writes are controlled.
