from __future__ import annotations

import ast
import duckdb
from pathlib import Path
import pytest
import socket

import app as analyst_app


DISABLED_RESPONSE = {
    "status": "disabled",
    "detail": "AI-generated SQL routes are disabled for this synthetic-data demo.",
}


@pytest.mark.parametrize(
    ("route", "body"),
    [
        ("/ask", {"question": "return every patient row"}),
        ("/plan", {"goal": "inspect every patient row"}),
    ],
)
def test_ai_sql_routes_are_fixed_fail_closed_responses(
    client, monkeypatch, route: str, body: dict[str, str]
) -> None:
    def forbidden(*args, **kwargs):
        raise AssertionError("disabled routes must not use databases")

    monkeypatch.setattr(duckdb, "connect", forbidden)

    response = client.post(route, json=body)

    assert response.status_code == 503
    assert response.json() == DISABLED_RESPONSE
    assert len(response.content) < 200


@pytest.mark.parametrize("route", ["/ask", "/plan"])
@pytest.mark.parametrize(
    "body",
    [
        b"{not-json",
        b'{"caller_content":"' + (b"x" * 10_000) + b'"}',
    ],
)
def test_disabled_routes_do_not_parse_or_echo_caller_bodies(
    client, route: str, body: bytes
) -> None:
    response = client.post(route, content=body, headers={"content-type": "application/json"})

    assert response.status_code == 503
    assert response.json() == DISABLED_RESPONSE
    assert len(response.content) < 200


def test_disabled_routes_never_open_a_network_connection(client, monkeypatch) -> None:
    network_calls: list[tuple[object, ...]] = []

    def forbidden_network(*args, **kwargs):
        network_calls.append(args)
        raise AssertionError("disabled analyst routes must not open network connections")

    monkeypatch.setattr(socket, "create_connection", forbidden_network)
    monkeypatch.setattr(socket.socket, "connect", forbidden_network)

    for route, body in (
        ("/ask", {"question": "show patients"}),
        ("/plan", {"goal": "show patients"}),
    ):
        response = client.post(route, json=body)
        assert response.status_code == 503

    assert network_calls == []


def test_production_analyst_has_no_network_client_imports() -> None:
    tree = ast.parse(Path(analyst_app.__file__).read_text(encoding="utf-8"))
    imported_roots = {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imported_roots.update(
        node.module.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    )

    assert imported_roots.isdisjoint(
        {"aiohttp", "httpx", "requests", "socket", "urllib", "urllib3"}
    )


def test_api_exposes_synthetic_only_boundary(client) -> None:
    assert "synthetic" in analyst_app.app.description.lower()
    assert client.get("/health").json()["data_boundary"] == "synthetic_only"


def test_non_synthetic_mode_is_refused() -> None:
    with pytest.raises(ValueError, match="synthetic"):
        analyst_app._data_mode("real")
