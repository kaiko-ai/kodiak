"""
Debug UI for Kodiak queue inspection.

Provides /debug/queues (HTML) and /debug/queues.json (JSON) endpoints
with token-based authentication.
"""

from __future__ import annotations

import asyncio
import hmac
import html
import json
import os
import secrets
import time
from typing import Any, Optional

import pydantic
import structlog
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, Response

from kodiak.debug_history import DEBUG_EVENT_HISTORY_KEY, summarize_webhook_payload
from kodiak.queue import (
    INGEST_QUEUE_NAMES,
    MERGE_QUEUE_BY_INSTALL_PREFIX,
    MERGE_QUEUE_NAMES,
    WEBHOOK_QUEUE_NAMES,
    WebhookEvent,
    installation_id_from_queue,
)
from kodiak.redis_client import redis_bot
from kodiak.schemas import RawWebhookEvent

logger = structlog.get_logger()

DEFAULT_QUEUE_PREVIEW_LIMIT = 25
DEFAULT_HISTORY_LIMIT = 200
MAX_QUEUE_PREVIEW_LIMIT = 100
MAX_HISTORY_LIMIT = 1_000

_debug_token: str = ""

QUEUE_DEFINITIONS = {
    "ingest_queues": (
        "Raw GitHub webhooks waiting to be picked up by ingest workers. "
        "These are the closest thing to the original webhook stream."
    ),
    "webhook_queues": (
        "Despite the name, these are PR evaluation queues, not raw GitHub "
        "webhook payloads. Each item is a deduplicated PR that Kodiak needs to "
        "re-evaluate."
    ),
    "merge_queues": (
        "Per-repository, per-target-branch merge queues. The current target, if "
        "present, is the PR actively occupying the merge slot."
    ),
    "registry_behavior": (
        "Queue registries keep queue names around even when the queue is empty, "
        "so empty queues can still show up here as idle registrations."
    ),
}


def initialize_debug_token(*, _force: bool = False) -> None:
    """
    Initialize the debug token. Call once at module load in ingest.py.
    Uses DEBUG_TOKEN env var if set, otherwise generates a random token.
    Idempotent — subsequent calls are no-ops so uvicorn's double-import
    of the ingest module doesn't overwrite the token.
    """
    global _debug_token
    if _debug_token and not _force:
        return
    _debug_token = os.environ.get("DEBUG_TOKEN") or secrets.token_urlsafe(32)
    logger.info("debug_token_initialized", token=_debug_token)


def require_debug_token(request: Request) -> bool:
    """
    Validate debug token from query param or cookie.
    Returns True if valid, False otherwise.
    """
    token = request.query_params.get("token")
    if token and hmac.compare_digest(token, _debug_token):
        return True
    cookie_token = request.cookies.get("kodiak_debug_token")
    return bool(cookie_token and hmac.compare_digest(cookie_token, _debug_token))


def _decode_redis_value(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _escape(value: Any) -> str:
    return html.escape(str(value), quote=True)


def _parse_int_param(
    raw_value: Optional[str], *, default: int, minimum: int, maximum: int
) -> int:
    if raw_value is None:
        return default
    try:
        value = int(raw_value)
    except ValueError:
        return default
    return max(minimum, min(maximum, value))


def _format_ts(value: Optional[float]) -> Optional[str]:
    if value is None:
        return None
    return time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(value))


def _format_age_seconds(value: Optional[float], *, now: float) -> Optional[int]:
    if value is None:
        return None
    return max(0, int(now - value))


def _queue_member_from_webhook_event(
    event: WebhookEvent, *, score: float, now: float
) -> dict[str, Any]:
    return {
        "owner": event.repo_owner,
        "repo": event.repo_name,
        "pr_number": event.pull_request_number,
        "target_branch": event.target_name,
        "installation_id": event.installation_id,
        "head_sha": getattr(event, "head_sha", None),
        "enqueued_at": score,
        "enqueued_at_iso": _format_ts(score),
        "age_sec": _format_age_seconds(score, now=now),
    }


def _queue_member_from_raw_webhook(
    event: RawWebhookEvent,
) -> dict[str, Any]:
    summary = summarize_webhook_payload(
        event_name=event.event_name,
        payload=event.payload,
    )
    return {
        "delivery_id": event.delivery_id,
        "event_name": event.event_name,
        "action": summary.get("action"),
        "installation_id": summary.get("installation_id"),
        "owner": summary.get("owner"),
        "repo": summary.get("repo"),
        "pr_number": summary.get("pull_request_number"),
        "target_branch": summary.get("target_branch"),
        "head_branch": summary.get("head_branch"),
        "sender": summary.get("sender"),
        "ref": summary.get("ref"),
        "sha": summary.get("sha"),
        "status_context": summary.get("status_context"),
        "status_state": summary.get("status_state"),
        "check_run_name": summary.get("check_run_name"),
        "check_run_status": summary.get("check_run_status"),
        "check_run_conclusion": summary.get("check_run_conclusion"),
    }


def _parse_debug_event(raw: bytes | str) -> dict[str, Any] | None:
    try:
        result: dict[str, Any] = json.loads(_decode_redis_value(raw))
        return result
    except json.JSONDecodeError:
        return None


def _event_matches_installation(
    data: dict[str, Any], installation_id: Optional[str]
) -> bool:
    if installation_id is None:
        return True
    return data.get("installation_id") == installation_id


def _parse_merge_queue_name(name: str) -> tuple[str | None, str | None, str | None]:
    try:
        _, rest = name.split(":", 1)
        _, repo_part = rest.split(".", 1)
        owner, repo, branch = repo_part.split("/", 2)
    except ValueError:
        return None, None, None
    return owner, repo, branch


async def _collect_registered_merge_queue_names() -> list[str]:
    queue_names_raw = await redis_bot.smembers(MERGE_QUEUE_NAMES)
    queue_names = {_decode_redis_value(name) for name in queue_names_raw}

    async for registry_key_raw in redis_bot.scan_iter(
        match=f"{MERGE_QUEUE_BY_INSTALL_PREFIX}*"
    ):
        registry_key = _decode_redis_value(registry_key_raw)
        members_raw = await redis_bot.smembers(registry_key)
        queue_names.update(_decode_redis_value(member) for member in members_raw)

    return sorted(queue_names)


async def _collect_merge_queues(
    *, preview_limit: int, installation_id: Optional[str]
) -> list[dict[str, Any]]:
    queue_names = await _collect_registered_merge_queue_names()
    if installation_id is not None:
        queue_names = [
            name
            for name in queue_names
            if installation_id_from_queue(name) == installation_id
        ]

    now = time.time()
    queues = []
    for name in queue_names:
        owner, repo, branch = _parse_merge_queue_name(name)
        size = await redis_bot.zcard(name)
        target_key = name + ":target"
        target_time_key = name + ":target:time"
        target_raw = await redis_bot.get(target_key)
        target_time_raw = await redis_bot.get(target_time_key)

        current_target = None
        merge_duration_sec = None
        if target_raw:
            try:
                target_event = WebhookEvent.parse_raw(target_raw)
                current_target = {
                    "owner": target_event.repo_owner,
                    "repo": target_event.repo_name,
                    "pr_number": target_event.pull_request_number,
                    "target_branch": target_event.target_name,
                    "installation_id": target_event.installation_id,
                }
            except (
                json.JSONDecodeError,
                pydantic.ValidationError,
                TypeError,
                ValueError,
            ):
                current_target = {"raw": _decode_redis_value(target_raw)}
        if target_time_raw:
            try:
                target_ts = float(target_time_raw)
                merge_duration_sec = _format_age_seconds(target_ts, now=now)
                if current_target is not None:
                    current_target["started_at"] = target_ts
                    current_target["started_at_iso"] = _format_ts(target_ts)
                    current_target["duration_sec"] = merge_duration_sec
            except (TypeError, ValueError):
                pass

        members = await redis_bot.zrange(
            name,
            0,
            max(preview_limit - 1, 0),
            withscores=True,
        )
        pr_list = []
        for member, score in members:
            try:
                event = WebhookEvent.parse_raw(member)
                pr_list.append(
                    _queue_member_from_webhook_event(event, score=score, now=now)
                )
            except (
                json.JSONDecodeError,
                pydantic.ValidationError,
                TypeError,
                ValueError,
            ):
                pr_list.append(
                    {
                        "raw": _decode_redis_value(member),
                        "enqueued_at": score,
                        "enqueued_at_iso": _format_ts(score),
                        "age_sec": _format_age_seconds(score, now=now),
                    }
                )

        queues.append(
            {
                "name": name,
                "installation_id": installation_id_from_queue(name),
                "owner": owner,
                "repo": repo,
                "branch": branch,
                "size": size,
                "registered": True,
                "active": bool(current_target or size > 0),
                "current_target": current_target,
                "merging_pr": current_target.get("pr_number")
                if isinstance(current_target, dict)
                else None,
                "merge_duration_sec": merge_duration_sec,
                "queued_prs": pr_list,
                "preview_truncated": size > len(pr_list),
            }
        )
    return queues


async def _collect_webhook_queues(
    *, preview_limit: int, installation_id: Optional[str]
) -> list[dict[str, Any]]:
    queue_names_raw = await redis_bot.smembers(WEBHOOK_QUEUE_NAMES)
    now = time.time()
    queues = []
    for raw in sorted(_decode_redis_value(value) for value in queue_names_raw):
        if (
            installation_id is not None
            and installation_id_from_queue(raw) != installation_id
        ):
            continue

        size = await redis_bot.zcard(raw)
        oldest_ts = None
        newest_ts = None
        if size > 0:
            oldest = await redis_bot.zrange(raw, 0, 0, withscores=True)
            newest = await redis_bot.zrange(raw, -1, -1, withscores=True)
            if oldest:
                oldest_ts = oldest[0][1]
            if newest:
                newest_ts = newest[0][1]

        members = await redis_bot.zrange(
            raw,
            0,
            max(preview_limit - 1, 0),
            withscores=True,
        )
        pending_events = []
        for member, score in members:
            try:
                event = WebhookEvent.parse_raw(member)
                pending_events.append(
                    _queue_member_from_webhook_event(event, score=score, now=now)
                )
            except (
                json.JSONDecodeError,
                pydantic.ValidationError,
                TypeError,
                ValueError,
            ):
                pending_events.append(
                    {
                        "raw": _decode_redis_value(member),
                        "enqueued_at": score,
                        "enqueued_at_iso": _format_ts(score),
                        "age_sec": _format_age_seconds(score, now=now),
                    }
                )

        queues.append(
            {
                "name": raw,
                "installation_id": installation_id_from_queue(raw),
                "kind": "pr_evaluation",
                "description": QUEUE_DEFINITIONS["webhook_queues"],
                "size": size,
                "oldest_event_ts": oldest_ts,
                "oldest_event_iso": _format_ts(oldest_ts),
                "newest_event_ts": newest_ts,
                "newest_event_iso": _format_ts(newest_ts),
                "pending_events": pending_events,
                "preview_truncated": size > len(pending_events),
            }
        )
    return queues


async def _collect_ingest_queues(
    *, preview_limit: int, installation_id: Optional[str]
) -> list[dict[str, Any]]:
    queue_names_raw = await redis_bot.smembers(INGEST_QUEUE_NAMES)
    queues = []
    for raw in sorted(_decode_redis_value(value) for value in queue_names_raw):
        if (
            installation_id is not None
            and installation_id_from_queue(raw) != installation_id
        ):
            continue

        length = await redis_bot.llen(raw)
        raw_events = await redis_bot.lrange(raw, 0, max(preview_limit - 1, 0))
        pending_events = []
        for event_raw in raw_events:
            try:
                event = RawWebhookEvent.parse_raw(event_raw)
                pending_events.append(_queue_member_from_raw_webhook(event))
            except (
                json.JSONDecodeError,
                pydantic.ValidationError,
                TypeError,
                ValueError,
            ):
                pending_events.append({"raw": _decode_redis_value(event_raw)})

        queues.append(
            {
                "name": raw,
                "installation_id": installation_id_from_queue(raw),
                "kind": "raw_github_webhook",
                "description": QUEUE_DEFINITIONS["ingest_queues"],
                "length": length,
                "pending_events": pending_events,
                "preview_truncated": length > len(pending_events),
            }
        )
    return queues


async def _collect_recent_debug_events(
    *, history_limit: int, installation_id: Optional[str]
) -> list[dict[str, Any]]:
    raw_events = await redis_bot.lrange(
        DEBUG_EVENT_HISTORY_KEY,
        0,
        max(history_limit - 1, 0),
    )
    events = []
    for raw_event in raw_events:
        event = _parse_debug_event(raw_event)
        if event is None or not _event_matches_installation(event, installation_id):
            continue
        event["ts_iso"] = _format_ts(event.get("ts"))
        events.append(event)
    return events


def _group_pr_timelines(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for event in events:
        pr_key = event.get("pr_key")
        if not pr_key:
            continue
        group = grouped.setdefault(
            pr_key,
            {
                "pr_key": pr_key,
                "installation_id": event.get("installation_id"),
                "owner": event.get("owner"),
                "repo": event.get("repo"),
                "pr_number": event.get("pr_number"),
                "events": [],
            },
        )
        group["events"].append(event)

    timelines = []
    for group in grouped.values():
        group_events = sorted(
            group["events"], key=lambda item: float(item.get("ts", 0))
        )
        latest_event = group_events[-1]
        latest_status = next(
            (
                item.get("details", {}).get("status")
                for item in reversed(group_events)
                if item.get("event_type") == "status_set"
            ),
            None,
        )
        timelines.append(
            {
                **group,
                "events": group_events,
                "latest_ts": latest_event.get("ts"),
                "latest_ts_iso": latest_event.get("ts_iso"),
                "decision_count": sum(
                    1
                    for item in group_events
                    if item.get("stage") == "decision"
                    or item.get("event_type") in {"merge_blocked", "config_error"}
                ),
                "latest_status": latest_status,
            }
        )

    return sorted(timelines, key=lambda item: item.get("latest_ts") or 0, reverse=True)


def _group_webhook_timelines(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for event in events:
        if event.get("event_name") is None:
            continue
        delivery_id = event.get("delivery_id") or (
            f"{event.get('event_name')}:{event.get('installation_id')}:"
            f"{event.get('owner')}:{event.get('repo')}:{event.get('pr_number')}:"
            f"{int(event.get('ts', 0))}"
        )
        group = grouped.setdefault(
            delivery_id,
            {
                "delivery_id": event.get("delivery_id"),
                "event_name": event.get("event_name"),
                "action": event.get("action"),
                "installation_id": event.get("installation_id"),
                "owner": event.get("owner"),
                "repo": event.get("repo"),
                "pr_number": event.get("pr_number"),
                "events": [],
            },
        )
        group["events"].append(event)

    timelines = []
    for group in grouped.values():
        group_events = sorted(
            group["events"], key=lambda item: float(item.get("ts", 0))
        )
        latest_event = group_events[-1]
        fanout_count = next(
            (
                item.get("details", {}).get("fanout_count")
                for item in reversed(group_events)
                if item.get("event_type") == "webhook_processed"
            ),
            None,
        )
        timelines.append(
            {
                **group,
                "events": group_events,
                "latest_ts": latest_event.get("ts"),
                "latest_ts_iso": latest_event.get("ts_iso"),
                "fanout_count": fanout_count,
            }
        )

    return sorted(timelines, key=lambda item: item.get("latest_ts") or 0, reverse=True)


async def _collect_all_data(
    *,
    preview_limit: int,
    history_limit: int,
    installation_id: Optional[str],
) -> dict[str, Any]:
    merge_queues, webhook_queues, ingest_queues, recent_events = await asyncio.gather(
        _collect_merge_queues(
            preview_limit=preview_limit, installation_id=installation_id
        ),
        _collect_webhook_queues(
            preview_limit=preview_limit, installation_id=installation_id
        ),
        _collect_ingest_queues(
            preview_limit=preview_limit, installation_id=installation_id
        ),
        _collect_recent_debug_events(
            history_limit=history_limit, installation_id=installation_id
        ),
    )

    pr_timelines = _group_pr_timelines(recent_events)
    webhook_timelines = _group_webhook_timelines(recent_events)

    return {
        "summary": {
            "merge_queue_count": len(merge_queues),
            "merge_queue_active_count": sum(
                1 for queue in merge_queues if queue["active"]
            ),
            "merge_queue_pending_prs": sum(queue["size"] for queue in merge_queues),
            "webhook_queue_count": len(webhook_queues),
            "webhook_queue_pending_events": sum(
                queue["size"] for queue in webhook_queues
            ),
            "ingest_queue_count": len(ingest_queues),
            "ingest_queue_pending_events": sum(
                queue["length"] for queue in ingest_queues
            ),
            "recent_event_count": len(recent_events),
            "recent_pr_count": len(pr_timelines),
            "recent_webhook_count": len(webhook_timelines),
        },
        "queue_definitions": QUEUE_DEFINITIONS,
        "filters": {
            "installation_id": installation_id,
            "queue_preview_limit": preview_limit,
            "history_limit": history_limit,
        },
        "merge_queues": merge_queues,
        "webhook_queues": webhook_queues,
        "ingest_queues": ingest_queues,
        "recent_events": recent_events,
        "pr_timelines": pr_timelines,
        "webhook_timelines": webhook_timelines,
        "collected_at": time.time(),
    }


def _render_json_block(data: Any) -> str:
    return f"<pre>{_escape(json.dumps(data, indent=2, sort_keys=True))}</pre>"


def _render_summary_cards(summary: dict[str, Any]) -> str:
    cards = [
        ("Merge Queues", summary["merge_queue_count"]),
        ("Active Merge Queues", summary["merge_queue_active_count"]),
        ("Queued PRs", summary["merge_queue_pending_prs"]),
        ("Eval Queue Events", summary["webhook_queue_pending_events"]),
        ("Ingest Webhooks", summary["ingest_queue_pending_events"]),
        ("Recent Debug Events", summary["recent_event_count"]),
        ("PR Timelines", summary["recent_pr_count"]),
        ("Webhook Timelines", summary["recent_webhook_count"]),
    ]
    return "".join(
        f"""
        <article class="summary-card">
            <div class="summary-label">{_escape(label)}</div>
            <div class="summary-value">{_escape(value)}</div>
        </article>
        """
        for label, value in cards
    )


def _render_queue_item_list(items: list[dict[str, Any]], *, title_key: str) -> str:
    if not items:
        return '<div class="empty-state">No pending items in preview.</div>'

    rendered = []
    for item in items:
        title = item.get(title_key) or item.get("raw") or "item"
        meta_parts = []
        for label, key in (
            ("PR", "pr_number"),
            ("Repo", "repo"),
            ("Owner", "owner"),
            ("Target", "target_branch"),
            ("Age", "age_sec"),
            ("Event", "event_name"),
            ("Action", "action"),
            ("Delivery", "delivery_id"),
        ):
            value = item.get(key)
            if value is None:
                continue
            if key == "age_sec":
                value = f"{value}s"
            meta_parts.append(
                f"<span><strong>{_escape(label)}:</strong> {_escape(value)}</span>"
            )
        rendered.append(
            f"""
            <li class="queue-item">
                <div class="queue-item-title">{_escape(title)}</div>
                <div class="meta-line">{"".join(meta_parts)}</div>
                <details>
                    <summary>Details</summary>
                    {_render_json_block(item)}
                </details>
            </li>
            """
        )
    return f'<ul class="queue-item-list">{"".join(rendered)}</ul>'


def _render_queue_cards(
    queues: list[dict[str, Any]], *, kind: str, empty_message: str
) -> str:
    if not queues:
        return f'<div class="empty-state">{_escape(empty_message)}</div>'

    cards = []
    for queue in queues:
        if kind == "merge":
            preview = _render_queue_item_list(
                queue["queued_prs"],
                title_key="pr_number",
            )
            current_target_html = (
                _render_json_block(queue["current_target"])
                if queue["current_target"] is not None
                else '<div class="empty-state">No PR is currently occupying the merge slot.</div>'
            )
            cards.append(
                f"""
                <article class="queue-card">
                    <div class="queue-card-head">
                        <div>
                            <h3>{_escape(queue.get("owner") or "?")}/{_escape(queue.get("repo") or "?")} / {_escape(queue.get("branch") or "?")}</h3>
                            <div class="muted">{_escape(queue["name"])}</div>
                        </div>
                        <span class="pill {"pill-active" if queue["active"] else "pill-idle"}">{"active" if queue["active"] else "idle"}</span>
                    </div>
                    <div class="meta-grid">
                        <div><strong>Installation:</strong> {_escape(queue["installation_id"])}</div>
                        <div><strong>Queued PRs:</strong> {_escape(queue["size"])}</div>
                        <div><strong>Merging PR:</strong> {_escape(queue.get("merging_pr") or "-")}</div>
                        <div><strong>Merge Duration:</strong> {_escape(f"{queue['merge_duration_sec']}s" if queue["merge_duration_sec"] is not None else "-")}</div>
                    </div>
                    <details open>
                        <summary>Current Merge Slot</summary>
                        {current_target_html}
                    </details>
                    <details open>
                        <summary>Queued PR Preview</summary>
                        {preview}
                    </details>
                </article>
                """
            )
        elif kind == "webhook":
            preview = _render_queue_item_list(
                queue["pending_events"],
                title_key="pr_number",
            )
            cards.append(
                f"""
                <article class="queue-card">
                    <div class="queue-card-head">
                        <div>
                            <h3>{_escape(queue["name"])}</h3>
                            <div class="muted">{_escape(queue["description"])}</div>
                        </div>
                        <span class="pill {"pill-active" if queue["size"] else "pill-idle"}">{"busy" if queue["size"] else "idle"}</span>
                    </div>
                    <div class="meta-grid">
                        <div><strong>Installation:</strong> {_escape(queue["installation_id"])}</div>
                        <div><strong>Queued PR evaluations:</strong> {_escape(queue["size"])}</div>
                        <div><strong>Oldest:</strong> {_escape(queue.get("oldest_event_iso") or "-")}</div>
                        <div><strong>Newest:</strong> {_escape(queue.get("newest_event_iso") or "-")}</div>
                    </div>
                    <details open>
                        <summary>Pending PR Evaluation Preview</summary>
                        {preview}
                    </details>
                </article>
                """
            )
        else:
            preview = _render_queue_item_list(
                queue["pending_events"],
                title_key="event_name",
            )
            cards.append(
                f"""
                <article class="queue-card">
                    <div class="queue-card-head">
                        <div>
                            <h3>{_escape(queue["name"])}</h3>
                            <div class="muted">{_escape(queue["description"])}</div>
                        </div>
                        <span class="pill {"pill-active" if queue["length"] else "pill-idle"}">{"busy" if queue["length"] else "idle"}</span>
                    </div>
                    <div class="meta-grid">
                        <div><strong>Installation:</strong> {_escape(queue["installation_id"])}</div>
                        <div><strong>Queued raw webhooks:</strong> {_escape(queue["length"])}</div>
                    </div>
                    <details open>
                        <summary>Pending Webhook Preview</summary>
                        {preview}
                    </details>
                </article>
                """
            )

    return "".join(cards)


def _render_timeline_event(event: dict[str, Any]) -> str:
    details = event.get("details")
    details_html = ""
    if details:
        details_html = f"""
        <details>
            <summary>Details</summary>
            {_render_json_block(details)}
        </details>
        """
    return f"""
    <li class="timeline-entry">
        <div class="timeline-entry-head">
            <span class="pill pill-stage">{_escape(event.get("stage") or "?")}</span>
            <code>{_escape(event.get("event_type") or "?")}</code>
            <span class="muted">{_escape(event.get("ts_iso") or "-")}</span>
        </div>
        <div class="timeline-message">{_escape(event.get("message") or "")}</div>
        {details_html}
    </li>
    """


def _render_timeline_groups(
    groups: list[dict[str, Any]],
    *,
    title_builder: str,
    empty_message: str,
) -> str:
    if not groups:
        return f'<div class="empty-state">{_escape(empty_message)}</div>'

    rendered = []
    for group in groups:
        if title_builder == "pr":
            title = (
                f"{group.get('owner')}/{group.get('repo')} #{group.get('pr_number')}"
            )
            meta = [
                f"<span><strong>Latest:</strong> {_escape(group.get('latest_ts_iso') or '-')}</span>",
                f"<span><strong>Decisions:</strong> {_escape(group.get('decision_count') or 0)}</span>",
            ]
            if group.get("latest_status"):
                meta.append(
                    f"<span><strong>Latest Status:</strong> {_escape(group['latest_status'])}</span>"
                )
        else:
            title = (
                f"{group.get('event_name') or 'webhook'}"
                f" / {_escape(group.get('action') or '-')}"
            )
            meta = [
                f"<span><strong>Latest:</strong> {_escape(group.get('latest_ts_iso') or '-')}</span>",
                f"<span><strong>Delivery:</strong> {_escape(group.get('delivery_id') or 'n/a')}</span>",
            ]
            if group.get("owner") and group.get("repo"):
                meta.append(
                    f"<span><strong>Repo:</strong> {_escape(group['owner'])}/{_escape(group['repo'])}</span>"
                )
            if group.get("pr_number") is not None:
                meta.append(
                    f"<span><strong>PR:</strong> {_escape(group['pr_number'])}</span>"
                )
            if group.get("fanout_count") is not None:
                meta.append(
                    f"<span><strong>Fan-out:</strong> {_escape(group['fanout_count'])}</span>"
                )

        rendered.append(
            f"""
            <article class="timeline-card">
                <div class="queue-card-head">
                    <div>
                        <h3>{title}</h3>
                    </div>
                </div>
                <div class="meta-line">{"".join(meta)}</div>
                <ol class="timeline-list">
                    {"".join(_render_timeline_event(event) for event in group["events"])}
                </ol>
            </article>
            """
        )

    return "".join(rendered)


def _render_recent_events(events: list[dict[str, Any]]) -> str:
    if not events:
        return '<div class="empty-state">No recent debug events recorded yet.</div>'

    rows = []
    for event in events:
        target = "-"
        if (
            event.get("owner")
            and event.get("repo")
            and event.get("pr_number") is not None
        ):
            target = f"{event['owner']}/{event['repo']}#{event['pr_number']}"
        elif event.get("owner") and event.get("repo"):
            target = f"{event['owner']}/{event['repo']}"
        rows.append(
            f"""
            <tr>
                <td>{_escape(event.get("ts_iso") or "-")}</td>
                <td><code>{_escape(event.get("stage") or "-")}</code></td>
                <td><code>{_escape(event.get("event_type") or "-")}</code></td>
                <td>{_escape(target)}</td>
                <td>{_escape(event.get("queue_name") or "-")}</td>
                <td>{_escape(event.get("message") or "-")}</td>
            </tr>
            """
        )
    return f"""
    <table>
        <thead>
            <tr>
                <th>Timestamp</th>
                <th>Stage</th>
                <th>Type</th>
                <th>Target</th>
                <th>Queue</th>
                <th>Message</th>
            </tr>
        </thead>
        <tbody>
            {"".join(rows)}
        </tbody>
    </table>
    """


def _render_html(data: dict[str, Any]) -> str:
    collected_at = _format_ts(data["collected_at"]) or "-"
    install_filter = data["filters"]["installation_id"] or "all installations"
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>Kodiak Debug Console</title>
    <meta http-equiv="refresh" content="30">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        :root {{
            --bg: #f5efe7;
            --surface: #fffaf4;
            --surface-strong: #fff;
            --ink: #1f2a30;
            --muted: #6d7a80;
            --border: #d7ccc0;
            --accent: #127475;
            --accent-2: #e67e22;
            --accent-3: #bc4b51;
            --idle: #90a4ae;
            --shadow: 0 14px 40px rgba(31, 42, 48, 0.08);
        }}
        * {{ box-sizing: border-box; }}
        body {{
            margin: 0;
            font-family: "SFMono-Regular", Menlo, Monaco, Consolas, "Liberation Mono", monospace;
            background:
                radial-gradient(circle at top right, rgba(230, 126, 34, 0.18), transparent 28%),
                linear-gradient(180deg, #f8f2ea 0%, var(--bg) 100%);
            color: var(--ink);
        }}
        main {{
            width: min(1500px, calc(100vw - 32px));
            margin: 0 auto;
            padding: 24px 0 64px;
        }}
        h1, h2, h3 {{ margin: 0; }}
        h1 {{
            font-size: clamp(2rem, 4vw, 3.4rem);
            letter-spacing: -0.04em;
        }}
        h2 {{
            font-size: 1.2rem;
            margin-bottom: 14px;
        }}
        h3 {{
            font-size: 1rem;
            margin-bottom: 6px;
        }}
        p {{ margin: 0; }}
        code {{
            background: rgba(18, 116, 117, 0.08);
            padding: 0.12rem 0.35rem;
            border-radius: 6px;
        }}
        pre {{
            white-space: pre-wrap;
            word-break: break-word;
            margin: 10px 0 0;
            padding: 12px;
            border-radius: 12px;
            background: #f3ece4;
            border: 1px solid var(--border);
        }}
        section {{
            margin-top: 22px;
            padding: 20px;
            border-radius: 22px;
            background: rgba(255, 250, 244, 0.88);
            border: 1px solid rgba(215, 204, 192, 0.8);
            box-shadow: var(--shadow);
        }}
        .hero {{
            display: grid;
            gap: 14px;
            padding: 28px;
            background: linear-gradient(135deg, rgba(18, 116, 117, 0.08), rgba(230, 126, 34, 0.14));
        }}
        .hero-copy {{
            display: grid;
            gap: 10px;
        }}
        .muted {{ color: var(--muted); }}
        .summary-grid, .queue-grid, .timeline-grid {{
            display: grid;
            gap: 14px;
        }}
        .summary-grid {{
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
        }}
        .queue-grid, .timeline-grid {{
            grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
        }}
        .summary-card, .queue-card, .timeline-card {{
            background: var(--surface-strong);
            border: 1px solid var(--border);
            border-radius: 18px;
            padding: 16px;
            box-shadow: var(--shadow);
        }}
        .summary-label {{
            color: var(--muted);
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }}
        .summary-value {{
            font-size: 2rem;
            margin-top: 8px;
            color: var(--accent);
            font-weight: 700;
        }}
        .queue-card-head {{
            display: flex;
            justify-content: space-between;
            gap: 12px;
            align-items: flex-start;
            margin-bottom: 12px;
        }}
        .meta-grid {{
            display: grid;
            gap: 8px 14px;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            margin-bottom: 10px;
        }}
        .meta-line {{
            display: flex;
            flex-wrap: wrap;
            gap: 12px;
            margin-bottom: 10px;
            color: var(--muted);
        }}
        .pill {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            padding: 4px 10px;
            border-radius: 999px;
            font-size: 0.78rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.08em;
        }}
        .pill-active {{
            background: rgba(18, 116, 117, 0.14);
            color: var(--accent);
        }}
        .pill-idle {{
            background: rgba(144, 164, 174, 0.18);
            color: #455a64;
        }}
        .pill-stage {{
            background: rgba(230, 126, 34, 0.15);
            color: var(--accent-2);
        }}
        .empty-state {{
            padding: 14px;
            border: 1px dashed var(--border);
            border-radius: 14px;
            color: var(--muted);
            background: rgba(255, 255, 255, 0.7);
        }}
        details {{
            margin-top: 10px;
            border-top: 1px solid rgba(215, 204, 192, 0.8);
            padding-top: 10px;
        }}
        summary {{
            cursor: pointer;
            color: var(--accent);
        }}
        .queue-item-list, .timeline-list {{
            margin: 10px 0 0;
            padding-left: 18px;
        }}
        .queue-item, .timeline-entry {{
            margin-top: 10px;
        }}
        .queue-item-title {{
            font-weight: 700;
        }}
        .timeline-entry-head {{
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            align-items: center;
            margin-bottom: 6px;
        }}
        .timeline-message {{
            font-size: 0.96rem;
            line-height: 1.45;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: var(--surface-strong);
            border-radius: 18px;
            overflow: hidden;
            box-shadow: var(--shadow);
        }}
        th, td {{
            text-align: left;
            padding: 12px;
            border-bottom: 1px solid rgba(215, 204, 192, 0.7);
            vertical-align: top;
        }}
        th {{
            background: rgba(18, 116, 117, 0.08);
        }}
        @media (max-width: 720px) {{
            main {{ width: min(100vw - 20px, 100%); }}
            section, .hero {{ padding: 16px; }}
            .queue-grid, .timeline-grid {{ grid-template-columns: 1fr; }}
        }}
    </style>
</head>
<body>
    <main>
        <section class="hero">
            <div class="hero-copy">
                <p class="muted">Collected at {collected_at}</p>
                <h1>Kodiak Debug Console</h1>
                <p>This view shows live queue state plus a bounded debug history of webhook processing, PR evaluations, status updates, queue moves, and merge decisions.</p>
                <p class="muted">Scope: {_escape(install_filter)}. Queue preview limit: {_escape(data["filters"]["queue_preview_limit"])}. History limit: {_escape(data["filters"]["history_limit"])}.</p>
            </div>
            <div class="summary-grid">
                {_render_summary_cards(data["summary"])}
            </div>
        </section>

        <section>
            <h2>How To Read This</h2>
            <p>{_escape(QUEUE_DEFINITIONS["ingest_queues"])}</p>
            <p>{_escape(QUEUE_DEFINITIONS["webhook_queues"])}</p>
            <p>{_escape(QUEUE_DEFINITIONS["merge_queues"])}</p>
            <p class="muted">{_escape(QUEUE_DEFINITIONS["registry_behavior"])}</p>
        </section>

        <section>
            <h2>Merge Queues ({_escape(len(data["merge_queues"]))})</h2>
            <div class="queue-grid">
                {_render_queue_cards(data["merge_queues"], kind="merge", empty_message="No merge queues are currently registered.")}
            </div>
        </section>

        <section>
            <h2>PR Evaluation Queues ({_escape(len(data["webhook_queues"]))})</h2>
            <div class="queue-grid">
                {_render_queue_cards(data["webhook_queues"], kind="webhook", empty_message="No PR evaluation queues are currently registered.")}
            </div>
        </section>

        <section>
            <h2>Raw Webhook Ingest Queues ({_escape(len(data["ingest_queues"]))})</h2>
            <div class="queue-grid">
                {_render_queue_cards(data["ingest_queues"], kind="ingest", empty_message="No ingest queues are currently registered.")}
            </div>
        </section>

        <section>
            <h2>Recent PR Timelines ({_escape(len(data["pr_timelines"]))})</h2>
            <div class="timeline-grid">
                {_render_timeline_groups(data["pr_timelines"], title_builder="pr", empty_message="No recent PR debug history recorded yet.")}
            </div>
        </section>

        <section>
            <h2>Recent Webhook Timelines ({_escape(len(data["webhook_timelines"]))})</h2>
            <div class="timeline-grid">
                {_render_timeline_groups(data["webhook_timelines"], title_builder="webhook", empty_message="No recent webhook debug history recorded yet.")}
            </div>
        </section>

        <section>
            <h2>Recent Debug Events ({_escape(len(data["recent_events"]))})</h2>
            {_render_recent_events(data["recent_events"])}
        </section>
    </main>
</body>
</html>"""


def _debug_collection_options(request: Request) -> tuple[int, int, Optional[str]]:
    preview_limit = _parse_int_param(
        request.query_params.get("queue_preview_limit"),
        default=DEFAULT_QUEUE_PREVIEW_LIMIT,
        minimum=1,
        maximum=MAX_QUEUE_PREVIEW_LIMIT,
    )
    history_limit = _parse_int_param(
        request.query_params.get("history_limit"),
        default=DEFAULT_HISTORY_LIMIT,
        minimum=1,
        maximum=MAX_HISTORY_LIMIT,
    )
    installation_id = request.query_params.get("installation_id") or None
    return preview_limit, history_limit, installation_id


async def debug_index(request: Request) -> Response:
    if not require_debug_token(request):
        return JSONResponse({"error": "forbidden"}, status_code=403)

    html_content = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>Kodiak Debug</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body {
            font-family: "SFMono-Regular", Menlo, Monaco, Consolas, monospace;
            margin: 0;
            background: #f5efe7;
            color: #1f2a30;
        }
        main {
            max-width: 900px;
            margin: 0 auto;
            padding: 32px 16px 48px;
        }
        section {
            background: rgba(255, 250, 244, 0.95);
            border: 1px solid #d7ccc0;
            border-radius: 22px;
            padding: 24px;
            box-shadow: 0 14px 40px rgba(31, 42, 48, 0.08);
        }
        h1 { margin-top: 0; }
        a { color: #127475; }
        code {
            background: rgba(18, 116, 117, 0.08);
            padding: 0.12rem 0.35rem;
            border-radius: 6px;
        }
        ul { line-height: 1.8; }
        p { line-height: 1.6; }
    </style>
</head>
<body>
    <main>
        <section>
            <h1>Kodiak Debug</h1>
            <p>The queue console now shows live queue contents plus recent webhook and PR timelines. Helpful query params:</p>
            <ul>
                <li><a href="/debug/queues">Queue Console (HTML)</a></li>
                <li><a href="/debug/queues.json">Queue Console (JSON)</a></li>
                <li><code>installation_id=117046149</code> to focus on one installation</li>
                <li><code>queue_preview_limit=50</code> to show more pending queue items</li>
                <li><code>history_limit=400</code> to show more recent debug history</li>
            </ul>
        </section>
    </main>
</body>
</html>"""
    response = HTMLResponse(html_content)
    if request.query_params.get("token"):
        response.set_cookie(
            "kodiak_debug_token",
            _debug_token,
            httponly=True,
            samesite="strict",
            max_age=86400,
        )
    return response


async def debug_queues_html(request: Request) -> Response:
    if not require_debug_token(request):
        return JSONResponse({"error": "forbidden"}, status_code=403)

    preview_limit, history_limit, installation_id = _debug_collection_options(request)
    data = await _collect_all_data(
        preview_limit=preview_limit,
        history_limit=history_limit,
        installation_id=installation_id,
    )
    response = HTMLResponse(_render_html(data))

    if request.query_params.get("token"):
        response.set_cookie(
            "kodiak_debug_token",
            _debug_token,
            httponly=True,
            samesite="strict",
            max_age=86400,
        )
    return response


async def debug_queues_json(request: Request) -> Response:
    if not require_debug_token(request):
        return JSONResponse({"error": "forbidden"}, status_code=403)

    preview_limit, history_limit, installation_id = _debug_collection_options(request)
    data = await _collect_all_data(
        preview_limit=preview_limit,
        history_limit=history_limit,
        installation_id=installation_id,
    )
    response = JSONResponse(data)

    if request.query_params.get("token"):
        response.set_cookie(
            "kodiak_debug_token",
            _debug_token,
            httponly=True,
            samesite="strict",
            max_age=86400,
        )
    return response
