"""
Smoke tests to ensure the ingest app starts and serves HTTP.

These catch regressions like import errors or uvicorn misconfiguration
that silently prevent the server from listening.
"""
from __future__ import annotations

from starlette.testclient import TestClient

from kodiak.entrypoints.ingest import app


def test_ingest_app_responds_to_root() -> None:
    """The app must start and respond 200 on GET /."""
    client = TestClient(app, raise_server_exceptions=True)
    response = client.get("/")
    assert response.status_code == 200
    assert response.text == "OK"


def test_ingest_app_webhook_endpoint_rejects_missing_headers() -> None:
    """POST /api/github/hook without required headers must return 400, not crash."""
    client = TestClient(app, raise_server_exceptions=False)
    response = client.post("/api/github/hook", json={})
    assert response.status_code == 400
