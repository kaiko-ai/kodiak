from __future__ import annotations

import os
from unittest.mock import AsyncMock, patch

import pytest
from starlette import status
from starlette.testclient import TestClient

from kodiak.entrypoints.debug import initialize_debug_token
from kodiak.entrypoints.ingest import app

# All debug routes that must be protected.
DEBUG_ROUTES = ["/debug", "/debug/queues", "/debug/queues.json"]


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture(autouse=True)
def _init_token() -> None:
    """Re-initialize token for every test so state doesn't leak."""
    initialize_debug_token()


def _get_token() -> str:
    from kodiak.entrypoints.debug import _debug_token

    return _debug_token


# ---------------------------------------------------------------------------
# Auth enforcement — every debug route must 403 without a valid token
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("route", DEBUG_ROUTES)
def test_no_token_returns_403(client: TestClient, route: str) -> None:
    """Every debug route must reject requests with no token at all."""
    res = client.get(route)
    assert res.status_code == status.HTTP_403_FORBIDDEN
    assert res.json() == {"error": "forbidden"}


@pytest.mark.parametrize("route", DEBUG_ROUTES)
def test_wrong_token_returns_403(client: TestClient, route: str) -> None:
    """Every debug route must reject requests with an incorrect token."""
    res = client.get(route, params={"token": "definitely-wrong"})
    assert res.status_code == status.HTTP_403_FORBIDDEN
    assert res.json() == {"error": "forbidden"}


@pytest.mark.parametrize("route", DEBUG_ROUTES)
def test_empty_token_returns_403(client: TestClient, route: str) -> None:
    """An empty string token must not pass auth."""
    res = client.get(route, params={"token": ""})
    assert res.status_code == status.HTTP_403_FORBIDDEN


@pytest.mark.parametrize("route", DEBUG_ROUTES)
def test_forged_cookie_returns_403(client: TestClient, route: str) -> None:
    """A hand-crafted cookie with the wrong value must be rejected."""
    client.cookies.set("kodiak_debug_token", "forged-value")
    res = client.get(route)
    assert res.status_code == status.HTTP_403_FORBIDDEN


# ---------------------------------------------------------------------------
# Successful auth — query param
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("route", DEBUG_ROUTES)
@patch("kodiak.entrypoints.debug.redis_bot")
def test_valid_token_returns_200(
    mock_redis: AsyncMock, client: TestClient, route: str
) -> None:
    """Every debug route must accept a correct ?token= param."""
    mock_redis.smembers = AsyncMock(return_value=set())
    token = _get_token()
    res = client.get(route, params={"token": token})
    assert res.status_code == status.HTTP_200_OK


# ---------------------------------------------------------------------------
# Cookie auth flow
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("route", DEBUG_ROUTES)
@patch("kodiak.entrypoints.debug.redis_bot")
def test_token_sets_cookie(
    mock_redis: AsyncMock, client: TestClient, route: str
) -> None:
    """Authenticating via ?token= must set an httponly cookie."""
    mock_redis.smembers = AsyncMock(return_value=set())
    token = _get_token()
    res = client.get(route, params={"token": token})
    assert res.status_code == status.HTTP_200_OK
    assert "kodiak_debug_token" in res.cookies


@pytest.mark.parametrize("route", DEBUG_ROUTES)
@patch("kodiak.entrypoints.debug.redis_bot")
def test_cookie_auth_works_without_query_param(
    mock_redis: AsyncMock, client: TestClient, route: str
) -> None:
    """After initial ?token= auth, the cookie alone grants access."""
    mock_redis.smembers = AsyncMock(return_value=set())
    token = _get_token()

    # First request — authenticate via query param
    res1 = client.get(route, params={"token": token})
    assert res1.status_code == status.HTTP_200_OK

    # Second request — no token in URL, cookie should suffice
    res2 = client.get(route)
    assert res2.status_code == status.HTTP_200_OK


# ---------------------------------------------------------------------------
# Endpoint content checks
# ---------------------------------------------------------------------------


@patch("kodiak.entrypoints.debug.redis_bot")
def test_debug_index_content(mock_redis: AsyncMock, client: TestClient) -> None:
    """/debug should return an HTML page with links to sub-routes."""
    mock_redis.smembers = AsyncMock(return_value=set())
    token = _get_token()
    res = client.get("/debug", params={"token": token})
    assert res.status_code == status.HTTP_200_OK
    assert "text/html" in res.headers["content-type"]
    assert "/debug/queues" in res.text


@patch("kodiak.entrypoints.debug.redis_bot")
def test_debug_queues_html_content(mock_redis: AsyncMock, client: TestClient) -> None:
    """/debug/queues should return HTML with queue status tables."""
    mock_redis.smembers = AsyncMock(return_value=set())
    token = _get_token()
    res = client.get("/debug/queues", params={"token": token})
    assert res.status_code == status.HTTP_200_OK
    assert "text/html" in res.headers["content-type"]
    assert "Kodiak Queue Status" in res.text
    assert "Merge Queues" in res.text


@patch("kodiak.entrypoints.debug.redis_bot")
def test_debug_queues_json_content(mock_redis: AsyncMock, client: TestClient) -> None:
    """/debug/queues.json should return structured JSON."""
    mock_redis.smembers = AsyncMock(return_value=set())
    token = _get_token()
    res = client.get("/debug/queues.json", params={"token": token})
    assert res.status_code == status.HTTP_200_OK
    data = res.json()
    assert "merge_queues" in data
    assert "webhook_queues" in data
    assert "ingest_queues" in data
    assert "collected_at" in data


# ---------------------------------------------------------------------------
# DEBUG_TOKEN env var
# ---------------------------------------------------------------------------


@patch("kodiak.entrypoints.debug.redis_bot")
def test_env_token_override(mock_redis: AsyncMock) -> None:
    """DEBUG_TOKEN env var should be respected as the auth token."""
    mock_redis.smembers = AsyncMock(return_value=set())

    with patch.dict(os.environ, {"DEBUG_TOKEN": "my-stable-token"}):
        initialize_debug_token()

    assert _get_token() == "my-stable-token"

    # Fresh client — no cookies from previous requests
    fresh = TestClient(app)
    res = fresh.get("/debug", params={"token": "my-stable-token"})
    assert res.status_code == status.HTTP_200_OK

    # A wrong token must be rejected (fresh client again to avoid cookie)
    fresh2 = TestClient(app)
    res_bad = fresh2.get("/debug", params={"token": "definitely-not-it"})
    assert res_bad.status_code == status.HTTP_403_FORBIDDEN
