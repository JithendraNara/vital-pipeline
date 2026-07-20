from pathlib import Path

import yaml


COMPOSE = Path(__file__).resolve().parents[3] / "docker-compose.yml"
STREAMING_OVERLAY = (
    Path(__file__).resolve().parents[3]
    / "streaming"
    / "docker-compose.streaming.yml"
)


def test_compose_is_valid_synthetic_local_configuration() -> None:
    config = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = config["services"]
    analyst = services["ai-analyst"]

    assert analyst["environment"]["DATA_MODE"] == "synthetic"
    assert analyst["ports"] == ["127.0.0.1:8000:8000"]
    assert "MINIMAX" not in repr(config).upper()


def test_compose_local_credentials_are_consistent_environment_defaults() -> None:
    config = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = config["services"]
    minio = services["minio"]["environment"]
    mc = services["mc"]
    iceberg = services["iceberg-rest"]["environment"]
    postgres = services["postgres"]["environment"]

    assert minio["MINIO_ROOT_USER"] == iceberg["AWS_ACCESS_KEY_ID"]
    assert minio["MINIO_ROOT_PASSWORD"] == iceberg["AWS_SECRET_ACCESS_KEY"]
    assert mc["environment"]["MINIO_ROOT_USER"] == minio["MINIO_ROOT_USER"]
    assert mc["environment"]["MINIO_ROOT_PASSWORD"] == minio["MINIO_ROOT_PASSWORD"]
    assert "$${MINIO_ROOT_USER}" in mc["entrypoint"]
    assert "$${MINIO_ROOT_PASSWORD}" in mc["entrypoint"]
    assert postgres["POSTGRES_PASSWORD"] == iceberg["JDBC_PASSWORD"]

    credential_values = [
        minio["MINIO_ROOT_USER"],
        minio["MINIO_ROOT_PASSWORD"],
        postgres["POSTGRES_PASSWORD"],
    ]
    assert all(value.startswith("${") and ":-" in value for value in credential_values)
    assert "***" not in COMPOSE.read_text(encoding="utf-8")


def test_merged_streaming_compose_is_loopback_only_and_immutable() -> None:
    """Validate the documented base + streaming-overlay configuration."""
    base = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    overlay = yaml.safe_load(STREAMING_OVERLAY.read_text(encoding="utf-8"))
    merged_services = {**base["services"], **overlay["services"]}

    assert {"redpanda", "redpanda-console"}.issubset(merged_services)
    for service in merged_services.values():
        for port in service.get("ports", []):
            assert str(port).startswith("127.0.0.1:"), port

    console_image = merged_services["redpanda-console"]["image"]
    assert "@sha256:" in console_image
    assert not console_image.endswith(":latest")
    assert len(console_image.rsplit("@sha256:", 1)[1]) == 64
