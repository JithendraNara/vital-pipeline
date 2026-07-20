"""Contract tests for the required default-branch CI gate."""
from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yaml"
DEFAULT_BRANCH = "master"
REQUIRED_SUITES = {
    "pipeline-test",
    "streaming-test",
    "ml-test",
    "governance-test",
    "app-test",
}
CONTRACT_COMMAND = (
    "pytest tests/test_workflow_contract.py tests/test_documentation_claims.py "
    "-v --junitxml=ci-contract.xml"
)
CONTRACT_ARTIFACT_NAME = "ci-workflow-contract-results"
TRIVY_ACTION_SHA = "57a97c7e7821a5776cebc9bb87c984fa69cba8f1"


def load_workflow() -> dict:
    """Parse Actions YAML without YAML 1.1 treating `on` as a boolean."""
    data = yaml.load(WORKFLOW.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    assert isinstance(data, dict)
    return data


def branch_names(trigger: object) -> set[str]:
    if isinstance(trigger, dict):
        branches = trigger.get("branches", [])
        if isinstance(branches, list):
            return set(branches)
    return set()


def test_ci_targets_the_actual_default_branch_and_is_dispatchable():
    workflow = load_workflow()
    triggers = workflow.get("on", {})

    assert isinstance(triggers, dict)
    assert DEFAULT_BRANCH in branch_names(triggers.get("push"))
    assert DEFAULT_BRANCH in branch_names(triggers.get("pull_request"))
    assert "workflow_dispatch" in triggers


def test_ci_uses_a_concurrency_policy():
    concurrency = load_workflow().get("concurrency")

    assert isinstance(concurrency, dict)
    assert concurrency.get("group")
    assert concurrency.get("cancel-in-progress") == "true"


def test_advertised_suites_are_required_jobs():
    jobs = load_workflow().get("jobs")

    assert isinstance(jobs, dict)
    assert REQUIRED_SUITES.issubset(jobs)
    for name in REQUIRED_SUITES:
        job = jobs[name]
        assert isinstance(job, dict)
        assert job.get("continue-on-error") != "true", name
        assert job.get("if") in (None, "${{ always() }}"), name
        assert job.get("runs-on"), name
        assert job.get("steps"), name


def test_pipeline_runs_and_uploads_the_ci_contract():
    jobs = load_workflow().get("jobs")
    assert isinstance(jobs, dict)
    pipeline = jobs.get("pipeline-test")
    assert isinstance(pipeline, dict)
    steps = pipeline.get("steps")
    assert isinstance(steps, list)

    run_commands = [step.get("run", "") for step in steps if isinstance(step, dict)]
    assert any(CONTRACT_COMMAND in command for command in run_commands)
    assert any("PyYAML==6.0.2" in command for command in run_commands)

    uploads = [
        step
        for step in steps
        if isinstance(step, dict) and step.get("uses") == "actions/upload-artifact@v4"
    ]
    assert len(uploads) == 1
    upload = uploads[0]
    assert upload.get("if") == "always()"
    assert upload.get("with") == {
        "name": CONTRACT_ARTIFACT_NAME,
        "path": "ci-contract.xml",
        "if-no-files-found": "error",
    }


def test_governance_job_does_not_claim_compliance() -> None:
    governance = load_workflow()["jobs"]["governance-test"]
    assert governance["name"] == "Governance controls (not compliance validation)"


def test_python_jobs_use_scoped_critical_static_gates() -> None:
    jobs = load_workflow()["jobs"]
    for job_name, compile_targets, lint_targets in (
        ("pipeline-test", "ai data_quality scripts", "ai/ data_quality/ scripts/"),
        ("streaming-test", "streaming", "streaming/"),
        ("ml-test", "ml", "ml/"),
        ("governance-test", "governance", "governance/"),
        ("app-test", "app", "app/"),
    ):
        commands = "\n".join(
            step.get("run", "")
            for step in jobs[job_name]["steps"]
            if isinstance(step, dict)
        )
        assert f"python -m compileall -q {compile_targets}" in commands
        assert f"flake8 {lint_targets} --select=E9,F63,F7,F82" in commands
        assert "black --check" not in commands
        assert "isort --check-only" not in commands
        assert "--max-line-length" not in commands


def test_dashboard_tests_import_from_repository_root() -> None:
    app_job = load_workflow()["jobs"]["app-test"]
    commands = "\n".join(
        step.get("run", "")
        for step in app_job["steps"]
        if isinstance(step, dict)
    )
    assert "pip install -r streaming/producers/requirements.txt" in commands
    assert "PYTHONPATH=. pytest app/dashboard/tests/ -v --tb=short" in commands


def test_docker_job_builds_without_credentials_or_publishing() -> None:
    docker_job = load_workflow()["jobs"]["docker"]
    assert "if" not in docker_job
    steps = docker_job["steps"]
    assert not any(
        "docker/login-action" in step.get("uses", "")
        for step in steps
        if isinstance(step, dict)
    )
    build = next(
        step for step in steps
        if isinstance(step, dict)
        and step.get("uses", "").startswith("docker/build-push-action@")
    )
    options = build["with"]
    assert options.get("push") != "true"
    assert options.get("load") == "true"
    assert "tags" not in options
    assert "secrets.GITHUB_TOKEN" not in WORKFLOW.read_text(encoding="utf-8")
    assert "refs/heads/main" not in WORKFLOW.read_text(encoding="utf-8")


def test_trivy_action_is_pinned_to_reviewed_full_commit() -> None:
    workflow_text = WORKFLOW.read_text(encoding="utf-8")
    assert f"aquasecurity/trivy-action@{TRIVY_ACTION_SHA}" in workflow_text
    assert "aquasecurity/trivy-action@master" not in workflow_text
    assert "aquasecurity/trivy-action@v" not in workflow_text
