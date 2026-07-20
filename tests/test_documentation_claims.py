"""Guard flagship copy without rejecting truthful limitation language."""

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]
FLAGSHIP_DOCS = (
    Path("README.md"),
    Path("streaming/README.md"),
    Path("governance/README.md"),
    Path("ml/README.md"),
    Path("app/dashboard/README.md"),
    Path("docs/architecture_diagram.md"),
)

# Positive assertions that are unsupported at the pinned revision. The patterns
# deliberately allow nouns in truthful negative or illustrative statements.
UNSUPPORTED_ASSERTIONS = (
    re.compile(r"production[-\s]+(?:ready|quality|grade)", re.IGNORECASE),
    re.compile(r"(?<!not )HIPAA[-\s]+(?:compliant|governed|grade)", re.IGNORECASE),
    re.compile(
        r"\bAWS\s+Glue(?:\s+mode)?\s+(?:is\s+)?(?:implemented|supported|runs|deployable)\b",
        re.IGNORECASE,
    ),
    re.compile(r"\bsame\s+code\s+path\s+(?:runs|works)\s+in\s+AWS\s+Glue\b", re.IGNORECASE),
    re.compile(
        r"\b(?:consumer|pipeline|job)\s+(?:writes?|lands?)(?:\s+\w+){0,5}\s+(?:to|into|in)\s+Iceberg(?:\s+v3)?\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bAll\s+(?:records\s+)?(?:are\s+)?written\s+(?:to|as)\s+Iceberg(?:\s+v3)?\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:all\s+topics\s+use|enforced\s+by|registered\s+with)\s+(?:the\s+)?Schema\s+Registry\b",
        re.IGNORECASE,
    ),
    re.compile(r"sub-second\s+latency", re.IGNORECASE),
    re.compile(r"\bclinician\s+can\b", re.IGNORECASE),
    re.compile(r"(?<!not )\bclinically\s+validated\b", re.IGNORECASE),
)

ACCEPTED_NEGATIVE_FIXTURES = (
    "AWS Glue mode is not implemented.",
    "AWS Glue mode is not implemented and is not supported.",
    "The consumer does not write to Iceberg v3.",
    "No records land in Iceberg.",
    "The /ask and /plan analyst routes are disabled.",
    "Clinical dashboard title; not intended for medical use.",
    "The clinician role is an illustrative sample policy.",
)

REJECTED_POSITIVE_FIXTURES = (
    "AWS Glue mode is implemented and supported.",
    "The consumer writes validated records to Iceberg v3.",
    "All topics use Schema Registry.",
    "HIPAA-governed data platform.",
    "Production-ready deployment.",
    "Sub-second latency.",
    "A clinician can use the score to make a decision.",
)

CI_DOCUMENTATION_COMMAND = (
    "pytest tests/test_workflow_contract.py tests/test_documentation_claims.py "
    "-v --junitxml=ci-contract.xml"
)


def _read(path: Path) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def _unsupported_matches(path: Path, text: str) -> list[str]:
    violations: list[str] = []
    for pattern in UNSUPPORTED_ASSERTIONS:
        for match in pattern.finditer(text):
            line = text.count("\n", 0, match.start()) + 1
            matched = " ".join(match.group(0).split())
            violations.append(f"{path}:{line}: {matched!r} [{pattern.pattern}]")
    return violations


class DocumentationClaimsTest(unittest.TestCase):
    def assertContainsAll(self, path: Path, statements: tuple[str, ...]) -> None:
        text = _read(path).casefold()
        missing = [statement for statement in statements if statement.casefold() not in text]
        self.assertFalse(missing, f"{path}: missing material boundaries/links: {missing}")

    def test_flagship_copy_omits_unsupported_positive_assertions(self) -> None:
        violations: list[str] = []
        for relative_path in FLAGSHIP_DOCS:
            violations.extend(_unsupported_matches(relative_path, _read(relative_path)))
        self.assertFalse(
            violations, "unsupported documentation assertions:\n" + "\n".join(violations)
        )

    def test_truthful_negative_and_illustrative_language_is_accepted(self) -> None:
        for fixture in ACCEPTED_NEGATIVE_FIXTURES:
            self.assertEqual([], _unsupported_matches(Path("<fixture>"), fixture), fixture)

    def test_unsupported_positive_fixture_language_is_rejected(self) -> None:
        for fixture in REJECTED_POSITIVE_FIXTURES:
            self.assertTrue(_unsupported_matches(Path("<fixture>"), fixture), fixture)

    def test_violation_output_includes_path_line_and_matched_text(self) -> None:
        violations = _unsupported_matches(
            Path("fixture.md"), "truthful boundary\nProduction-ready deployment."
        )
        self.assertEqual(1, len(violations))
        self.assertIn("fixture.md:2:", violations[0])
        self.assertIn("'Production-ready'", violations[0])

    def test_top_level_readme_states_required_boundaries_and_substance(self) -> None:
        self.assertContainsAll(
            Path("README.md"),
            (
                "## Limitations",
                "## Security / Data Boundaries",
                "synthetic data only",
                "not a HIPAA compliance determination",
                "not clinically validated",
                "not intended for diagnosis, treatment, or triage",
                "SHAP output is a model explanation, not clinical causality",
                "partial OMOP coverage",
                "`/ask` and `/plan` are disabled",
                "bounded, parameterized `/cohort`",
                "no row-level result is sent to an external model",
                "dbt_project/models/omop/omcdm_person.sql",
                "dbt_project/models/intermediate/int_member_months.sql",
                "dbt_project/models/marts/mart_member_roster.sql",
                "data_quality/run_gx_suite.py",
                "direct DuckDB/SQL column",
                "data_contracts/eligibility_data_contract.yml",
                "pipelines/eligibility-etl/dag.py",
                "prefect_flows/real_time_healthcare_flow.py",
                "infrastructure/main.tf",
                "docs/data-dictionary.md",
                "synthetic cohort API; `/ask` and `/plan` disabled",
            ),
        )
        readme = _read(Path("README.md"))
        self.assertNotIn("Great Expectations", readme)
        governance_block = re.search(
            r"## Governance utility checks(?P<body>.*?)(?:\n## |\Z)",
            readme,
            re.DOTALL,
        )
        self.assertIsNotNone(governance_block)
        self.assertIn("python -m pip install pytest", governance_block.group("body"))

    def test_streaming_doc_has_material_contract_and_boundaries(self) -> None:
        self.assertContainsAll(
            Path("streaming/README.md"),
            (
                "## Limitations",
                "## Security / Data Boundaries",
                "AWS Glue mode is not implemented",
                "synthetic",
                "plaintext Kafka transport",
                "not a HIPAA compliance determination",
                "VitalsEvent",
                "AdmissionEvent",
                "LabResultEvent",
                "IoTTelemetryEvent",
                "DeadLetterEvent",
                "streaming/schemas/events.py",
                "dlq_silver",
            ),
        )

    def test_governance_doc_has_material_examples_and_boundaries(self) -> None:
        self.assertContainsAll(
            Path("governance/README.md"),
            (
                "## Limitations",
                "## Security / Data Boundaries",
                "illustrative",
                "not integrated",
                "not a HIPAA compliance determination",
                "authentication",
                "illustrative `clinician` role",
                "governance/masking/deidentify.py",
                "HEALTHCARE_KMS_DETERMINISTIC_PEPPER",
                "InvalidTag",
            ),
        )

    def test_ml_doc_has_feature_api_and_medical_boundaries(self) -> None:
        self.assertContainsAll(
            Path("ml/README.md"),
            (
                "## Limitations",
                "## Security / Data Boundaries",
                "synthetic",
                "not clinically validated",
                "not intended for diagnosis, treatment, or triage",
                "no authentication",
                "ml/outcomes/feature_engineering.py",
                "POST /predict",
                "score",
                "risk_band",
                "top_feature_contributions",
                "runtime dependencies",
                "test dependencies",
            ),
        )

    def test_dashboard_doc_has_views_dependencies_and_boundaries(self) -> None:
        self.assertContainsAll(
            Path("app/dashboard/README.md"),
            (
                "## Limitations",
                "## Security / Data Boundaries",
                "synthetic data",
                "no authentication",
                "runtime dependencies",
                "streaming/producers/requirements.txt",
                "test dependencies",
                "recent prediction board",
                "patient detail",
                "pipeline table counts",
            ),
        )

    def test_architecture_restores_local_batch_lineage(self) -> None:
        self.assertContainsAll(
            Path("docs/architecture_diagram.md"),
            (
                "dbt_project/models/staging/stg_eligibility_members.sql",
                "dbt_project/models/intermediate/int_member_months.sql",
                "dbt_project/models/omop/omcdm_person.sql",
                "dbt_project/models/omop/omcdm_condition_occurrence.sql",
                "dbt_project/models/omop/omcdm_visit_occurrence.sql",
                "dbt_project/models/omop/omcdm_drug_exposure.sql",
                "dbt_project/models/omop/omcdm_measurement.sql",
                "dbt_project/models/marts/mart_member_roster.sql",
            ),
        )

    def test_combined_ci_command_is_explicit(self) -> None:
        self.assertIn(CI_DOCUMENTATION_COMMAND, _read(Path("README.md")))


if __name__ == "__main__":
    unittest.main()
