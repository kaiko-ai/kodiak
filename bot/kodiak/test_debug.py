from __future__ import annotations

import os
from unittest.mock import AsyncMock, Mock, patch

import pytest
from starlette import status
from starlette.testclient import TestClient

from kodiak.debug_history import DEBUG_EVENT_HISTORY_KEY
from kodiak.entrypoints.debug import initialize_debug_token
from kodiak.entrypoints.ingest import app
from kodiak.queue import INGEST_QUEUE_NAMES, WEBHOOK_QUEUE_NAMES, WebhookEvent
from kodiak.schemas import RawWebhookEvent

# All debug routes that must be protected.
DEBUG_ROUTES = ["/debug", "/debug/queues", "/debug/queues.json"]


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture(autouse=True)
def _init_token() -> None:
    """Re-initialize token for every test so state doesn't leak."""
    initialize_debug_token(_force=True)


def _get_token() -> str:
    from kodiak.entrypoints.debug import _debug_token

    return _debug_token


async def _empty_scan_iter() -> bytes:
    if False:
        yield b""


def _configure_empty_debug_redis(mock_redis: AsyncMock) -> None:
    mock_redis.smembers = AsyncMock(return_value=set())
    mock_redis.zcard = AsyncMock(return_value=0)
    mock_redis.get = AsyncMock(return_value=None)
    mock_redis.zrange = AsyncMock(return_value=[])
    mock_redis.llen = AsyncMock(return_value=0)
    mock_redis.lrange = AsyncMock(return_value=[])
    mock_redis.scan_iter = Mock(return_value=_empty_scan_iter())


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
    client.cookies["kodiak_debug_token"] = "forged-value"
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
    _configure_empty_debug_redis(mock_redis)
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
    _configure_empty_debug_redis(mock_redis)
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
    _configure_empty_debug_redis(mock_redis)
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
    _configure_empty_debug_redis(mock_redis)
    token = _get_token()
    res = client.get("/debug", params={"token": token})
    assert res.status_code == status.HTTP_200_OK
    assert "text/html" in res.headers["content-type"]
    assert "/debug/queues" in res.text


@patch("kodiak.entrypoints.debug.redis_bot")
def test_debug_queues_html_content(mock_redis: AsyncMock, client: TestClient) -> None:
    """/debug/queues should return HTML with queue status tables."""
    _configure_empty_debug_redis(mock_redis)
    token = _get_token()
    res = client.get("/debug/queues", params={"token": token})
    assert res.status_code == status.HTTP_200_OK
    assert "text/html" in res.headers["content-type"]
    assert "Kodiak Debug Console" in res.text
    assert "Merge Queues" in res.text
    assert "Recent PR Timelines" in res.text


@patch("kodiak.entrypoints.debug.redis_bot")
def test_debug_queues_json_content(mock_redis: AsyncMock, client: TestClient) -> None:
    """/debug/queues.json should return structured JSON."""
    _configure_empty_debug_redis(mock_redis)
    token = _get_token()
    res = client.get("/debug/queues.json", params={"token": token})
    assert res.status_code == status.HTTP_200_OK
    data = res.json()
    assert "summary" in data
    assert "queue_definitions" in data
    assert "merge_queues" in data
    assert "webhook_queues" in data
    assert "ingest_queues" in data
    assert "recent_events" in data
    assert "pr_timelines" in data
    assert "webhook_timelines" in data
    assert "collected_at" in data


@patch("kodiak.entrypoints.debug.redis_bot")
def test_debug_queues_json_shows_queue_previews_and_timelines(
    mock_redis: AsyncMock, client: TestClient
) -> None:
    merge_queue_name = "merge_queue:117046149.acme/widgets/main"
    webhook_queue_name = "webhook:117046149"
    ingest_queue_name = "kodiak:ingest:117046149"
    merge_member = WebhookEvent(
        repo_owner="acme",
        repo_name="widgets",
        pull_request_number=42,
        installation_id="117046149",
        target_name="main",
    ).json()
    raw_webhook = RawWebhookEvent(
        event_name="pull_request",
        delivery_id="delivery-123",
        payload={
            "action": "labeled",
            "installation": {"id": 117046149},
            "repository": {
                "name": "widgets",
                "owner": {"login": "acme"},
            },
            "pull_request": {
                "number": 42,
                "base": {"ref": "main"},
                "head": {"ref": "feature/example", "sha": "abc123"},
            },
            "sender": {"login": "octocat"},
        },
    ).json()
    debug_event = (
        '{"ts": 100.0, "stage": "decision", "event_type": "merge_decision", '
        '"message": "Queued the PR for merge", "installation_id": "117046149", '
        '"owner": "acme", "repo": "widgets", "pr_number": 42, '
        '"pr_key": "acme/widgets#42", "delivery_id": "delivery-123", '
        '"event_name": "pull_request", "action": "labeled", '
        '"details": {"reason": "queued_for_merge"}}'
    )

    async def scan_iter(*_: object, **__: object) -> bytes:
        yield b"merge_queue_by_install:117046149"

    async def smembers_side_effect(key: str) -> set[bytes]:
        if key == WEBHOOK_QUEUE_NAMES:
            return {webhook_queue_name.encode()}
        if key == INGEST_QUEUE_NAMES:
            return {ingest_queue_name.encode()}
        if key == "merge_queue_by_install:117046149":
            return {merge_queue_name.encode()}
        return set()

    async def zcard_side_effect(key: str) -> int:
        if key == merge_queue_name:
            return 1
        if key == webhook_queue_name:
            return 1
        return 0

    async def get_side_effect(key: str) -> bytes | None:
        if key == merge_queue_name + ":target":
            return merge_member.encode()
        if key == merge_queue_name + ":target:time":
            return b"50.0"
        return None

    async def zrange_side_effect(
        key: str, start: int, end: int, withscores: bool = False
    ) -> list[tuple[bytes, float]]:
        if key in {merge_queue_name, webhook_queue_name}:
            return [(merge_member.encode(), 25.0)]
        return []

    async def llen_side_effect(key: str) -> int:
        return 1 if key == ingest_queue_name else 0

    async def lrange_side_effect(key: str, start: int, end: int) -> list[bytes]:
        if key == ingest_queue_name:
            return [raw_webhook.encode()]
        if key == DEBUG_EVENT_HISTORY_KEY:
            return [debug_event.encode()]
        return []

    mock_redis.scan_iter = Mock(return_value=scan_iter())
    mock_redis.smembers = AsyncMock(side_effect=smembers_side_effect)
    mock_redis.zcard = AsyncMock(side_effect=zcard_side_effect)
    mock_redis.get = AsyncMock(side_effect=get_side_effect)
    mock_redis.zrange = AsyncMock(side_effect=zrange_side_effect)
    mock_redis.llen = AsyncMock(side_effect=llen_side_effect)
    mock_redis.lrange = AsyncMock(side_effect=lrange_side_effect)

    token = _get_token()
    res = client.get("/debug/queues.json", params={"token": token})
    assert res.status_code == status.HTTP_200_OK

    data = res.json()
    assert data["summary"]["merge_queue_count"] == 1
    assert data["merge_queues"][0]["current_target"]["pr_number"] == 42
    assert data["webhook_queues"][0]["pending_events"][0]["pr_number"] == 42
    assert (
        data["ingest_queues"][0]["pending_events"][0]["delivery_id"] == "delivery-123"
    )
    assert data["pr_timelines"][0]["pr_key"] == "acme/widgets#42"
    assert data["webhook_timelines"][0]["delivery_id"] == "delivery-123"


# ---------------------------------------------------------------------------
# DEBUG_TOKEN env var
# ---------------------------------------------------------------------------


@patch("kodiak.entrypoints.debug.redis_bot")
def test_env_token_override(mock_redis: AsyncMock) -> None:
    """DEBUG_TOKEN env var should be respected as the auth token."""
    _configure_empty_debug_redis(mock_redis)

    with patch.dict(os.environ, {"DEBUG_TOKEN": "my-stable-token"}):
        initialize_debug_token(_force=True)

    assert _get_token() == "my-stable-token"

    # Fresh client — no cookies from previous requests
    fresh = TestClient(app)
    res = fresh.get("/debug", params={"token": "my-stable-token"})
    assert res.status_code == status.HTTP_200_OK

    # A wrong token must be rejected (fresh client again to avoid cookie)
    fresh2 = TestClient(app)
    res_bad = fresh2.get("/debug", params={"token": "definitely-not-it"})
    assert res_bad.status_code == status.HTTP_403_FORBIDDEN
