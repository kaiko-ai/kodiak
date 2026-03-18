"""
Accept webhooks from GitHub and add them to the Redis queues.
"""

from __future__ import annotations

import hashlib
import hmac
from typing import Any

import structlog
import uvicorn
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware
from starlette import status
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, Response

from kodiak import app_config as conf
from kodiak.debug_history import record_debug_event, summarize_webhook_payload
from kodiak.entrypoints.debug import (
    debug_index,
    debug_queues_html,
    debug_queues_json,
    initialize_debug_token,
)
from kodiak.entrypoints.worker import PubsubIngestQueueSchema
from kodiak.logging import configure_logging
from kodiak.queue import INGEST_QUEUE_NAMES, QUEUE_PUBSUB_INGEST, get_ingest_queue
from kodiak.redis_client import redis_bot
from kodiak.schemas import RawWebhookEvent

configure_logging()
initialize_debug_token()

logger = structlog.get_logger()

# Only these event types are processed by handle_webhook_event() in queue.py.
# All others are discarded by the worker, so we filter them at ingest to avoid
# wasting Redis writes and worker cycles.
HANDLED_EVENT_TYPES: frozenset[str] = frozenset(
    {
        "check_run",
        "pull_request",
        "pull_request_review",
        "pull_request_review_thread",
        "push",
        "status",
    }
)

app = Starlette()
app.add_middleware(SentryAsgiMiddleware)
app.add_route("/debug", debug_index, methods=["GET"])
app.add_route("/debug/queues", debug_queues_html, methods=["GET"])
app.add_route("/debug/queues.json", debug_queues_json, methods=["GET"])


@app.route("/", methods=["GET"])
async def root(_: Request) -> Response:
    return PlainTextResponse("OK")


@app.route("/api/github/hook", methods=["POST"])
async def github_webhook_event(request: Request) -> Response:
    expected_sha = hmac.new(
        key=conf.SECRET_KEY.encode(), msg=(await request.body()), digestmod=hashlib.sha1
    ).hexdigest()

    try:
        github_event = request.headers["X-Github-Event"]
    except KeyError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="missing required X-Github-Event header",
        ) from e
    try:
        hub_signature = request.headers["X-Hub-Signature"]
    except KeyError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="missing required X-Hub-Signature header",
        ) from e

    sha = hub_signature.replace("sha1=", "")
    if not hmac.compare_digest(sha, expected_sha):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid signature: X-Hub-Signature",
        )
    event: dict[str, Any] = await request.json()
    delivery_id = request.headers.get("X-GitHub-Delivery")

    log = logger.bind(event_name=github_event, event=event)
    installation_id: int | None = event.get("installation", {}).get("id")
    webhook_summary = summarize_webhook_payload(event_name=github_event, payload=event)
    owner = webhook_summary.get("owner")
    repo = webhook_summary.get("repo")
    pr_number = webhook_summary.get("pull_request_number")
    action = webhook_summary.get("action")

    await record_debug_event(
        stage="ingest",
        event_type="webhook_received",
        message="Received GitHub webhook",
        installation_id=str(installation_id) if installation_id is not None else None,
        owner=owner,
        repo=repo,
        pr_number=pr_number,
        delivery_id=delivery_id,
        event_name=github_event,
        action=action,
        details=webhook_summary,
    )

    if github_event in {
        "github_app_authorization",
        "installation",
        "installation_repositories",
    }:
        log.info("administrative_event_received")
        await record_debug_event(
            stage="ingest",
            event_type="webhook_ignored",
            message="Ignored administrative webhook",
            delivery_id=delivery_id,
            event_name=github_event,
            action=action,
        )
        return JSONResponse({"ok": True})

    if installation_id is None:
        log.warning("unexpected_event_skipped")
        await record_debug_event(
            stage="ingest",
            event_type="webhook_skipped_missing_installation",
            message="Skipped webhook because installation id is missing",
            delivery_id=delivery_id,
            event_name=github_event,
            action=action,
            owner=owner,
            repo=repo,
            pr_number=pr_number,
        )
        return JSONResponse({"ok": True})

    if github_event not in HANDLED_EVENT_TYPES:
        log.info("unhandled_event_filtered", event_name=github_event)
        await record_debug_event(
            stage="ingest",
            event_type="webhook_filtered",
            message="Filtered webhook because Kodiak does not handle this event type",
            installation_id=str(installation_id),
            owner=owner,
            repo=repo,
            pr_number=pr_number,
            delivery_id=delivery_id,
            event_name=github_event,
            action=action,
        )
        return JSONResponse({"ok": True})

    ingest_queue = get_ingest_queue(installation_id)

    ingest_queue_length = await redis_bot.rpush(
        ingest_queue,
        RawWebhookEvent(
            event_name=github_event,
            payload=event,
            delivery_id=delivery_id,
        ).json(),
    )

    await redis_bot.ltrim(ingest_queue, 0, conf.INGEST_QUEUE_LENGTH)
    await redis_bot.sadd(INGEST_QUEUE_NAMES, ingest_queue)
    await redis_bot.publish(
        QUEUE_PUBSUB_INGEST,
        PubsubIngestQueueSchema(installation_id=installation_id).json(),
    )
    await record_debug_event(
        stage="ingest",
        event_type="webhook_enqueued_for_ingest",
        message="Queued raw webhook for ingest workers",
        installation_id=str(installation_id),
        owner=owner,
        repo=repo,
        pr_number=pr_number,
        queue_name=ingest_queue,
        delivery_id=delivery_id,
        event_name=github_event,
        action=action,
        details={"ingest_queue_length": ingest_queue_length},
    )
    return JSONResponse({"ok": True})


if __name__ == "__main__":
    uvicorn.run("kodiak.entrypoints.ingest:app", host="0.0.0.0", port=conf.PORT)
