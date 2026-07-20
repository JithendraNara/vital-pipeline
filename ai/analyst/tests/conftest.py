from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


ANALYST_DIR = Path(__file__).resolve().parents[1]
if str(ANALYST_DIR) not in sys.path:
    sys.path.insert(0, str(ANALYST_DIR))

import app as analyst_app  # noqa: E402


@pytest.fixture
def client() -> TestClient:
    return TestClient(analyst_app.app, raise_server_exceptions=False)
