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


def _gh_pr_url(owner: Any, repo: Any, pr_number: Any) -> str | None:
    if owner and repo and pr_number is not None:
        return f"https://github.com/{_escape(owner)}/{_escape(repo)}/pull/{_escape(pr_number)}"
    return None


def _gh_repo_url(owner: Any, repo: Any) -> str | None:
    if owner and repo:
        return f"https://github.com/{_escape(owner)}/{_escape(repo)}"
    return None


def _gh_link(label: str, owner: Any, repo: Any, pr_number: Any = None) -> str:
    url = _gh_pr_url(owner, repo, pr_number) or _gh_repo_url(owner, repo)
    if url:
        return f'<a href="{url}" target="_blank" rel="noopener">{_escape(label)}</a>'
    return _escape(label)


def _format_age(seconds: int | float | None) -> str:
    if seconds is None:
        return "-"
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60}s"
    return f"{s // 3600}h {(s % 3600) // 60}m"


STAT_COLORS: dict[str, str] = {
    "accent": "#4da6ff",
    "green": "#2dd4a8",
    "yellow": "#f5c542",
    "purple": "#a78bfa",
    "cyan": "#22d3ee",
    "muted": "#8899aa",
}


def _render_summary_cards(summary: dict[str, Any]) -> str:
    cards = [
        ("Merge Queues", summary["merge_queue_count"], "accent", f"{summary['merge_queue_active_count']} active"),
        ("Queued PRs", summary["merge_queue_pending_prs"], "green", "waiting to merge"),
        ("Eval Events", summary["webhook_queue_pending_events"], "yellow", f"{summary['webhook_queue_count']} queues"),
        ("Ingest Queue", summary["ingest_queue_pending_events"], "purple", f"{summary['ingest_queue_count']} queues"),
        ("PR Timelines", summary["recent_pr_count"], "cyan", "tracked recently"),
        ("Debug Events", summary["recent_event_count"], "muted", "in buffer"),
    ]
    return "".join(
        f"""
        <article class="stat-card {color}">
            <div class="stat-label">{_escape(label)}</div>
            <div class="stat-value c-{color}">{_escape(value)}</div>
            <div class="stat-sub">{_escape(sub)}</div>
        </article>
        """
        for label, value, color, sub in cards
    )


def _render_merge_pipeline(queue: dict[str, Any]) -> str:
    owner = queue.get("owner") or ""
    repo = queue.get("repo") or ""
    target = queue.get("current_target")
    nodes: list[str] = []
    if target:
        pr_link = _gh_link(
            f"#{target['pr_number']}", owner, repo, target["pr_number"]
        )
        nodes.append(
            f'<div class="pipeline-node merging">'
            f'<span class="pipeline-dot"></span>'
            f'<span class="pipeline-pr">{pr_link}</span>'
            f'<span class="pipeline-timer">{_format_age(queue.get("merge_duration_sec"))}</span>'
            f"</div>"
        )
    else:
        nodes.append('<div class="pipeline-node idle-slot">Empty slot</div>')
    for pr in queue.get("queued_prs", []):
        pr_link = _gh_link(f"#{pr['pr_number']}", owner, repo, pr["pr_number"])
        nodes.append('<span class="pipeline-arrow">&rarr;</span>')
        nodes.append(
            f'<div class="pipeline-node queued">'
            f'<span class="pipeline-pr">{pr_link}</span>'
            f'<span class="pipeline-timer">{_format_age(pr.get("age_sec"))}</span>'
            f"</div>"
        )
    return f'<div class="merge-pipeline">{"".join(nodes)}</div>'


def _render_pr_row(
    item: dict[str, Any], *, position: str, pill_class: str, pill_label: str
) -> str:
    owner = item.get("owner") or ""
    repo = item.get("repo") or ""
    pr_num = item.get("pr_number")
    label = f"{owner}/{repo} #{pr_num}" if pr_num else f"{owner}/{repo}"
    link = _gh_link(label, owner, repo, pr_num)
    sha = item.get("head_sha") or ""
    short_sha = sha[:7] if sha else ""
    branch = item.get("target_branch") or ""
    age = _format_age(item.get("age_sec"))
    return f"""
    <li class="pr-row">
        <span class="pr-position">{_escape(position)}</span>
        <div class="pr-info">
            <div class="pr-title">{link}</div>
            <span class="pr-sha">{_escape(short_sha)}</span>
        </div>
        <span class="pr-branch">{_escape(branch)}</span>
        <span class="pill {pill_class}">{_escape(pill_label)}</span>
        <span class="pr-age">{_escape(age)}</span>
    </li>"""


def _render_ingest_row(item: dict[str, Any]) -> str:
    event_name = item.get("event_name") or "?"
    action = item.get("action") or ""
    owner = item.get("owner") or ""
    repo = item.get("repo") or ""
    pr_num = item.get("pr_number")
    target = f"{owner}/{repo}"
    if pr_num:
        target += f" #{pr_num}"
    sender = item.get("sender") or (item.get("delivery_id") or "")[:8]
    return f"""
    <li class="pr-row" style="grid-template-columns: 36px 1fr auto auto;">
        <span class="pr-position">&bull;</span>
        <div class="pr-info">
            <div class="pr-title">{_escape(event_name)} <span class="text-secondary">{_escape(action)}</span></div>
            <div class="pr-sha">{_escape(target)}</div>
        </div>
        <span class="pill pill-ingest">{_escape(event_name)}</span>
        <span class="pr-sha">{_escape(sender)}</span>
    </li>"""


def _render_queue_cards(
    queues: list[dict[str, Any]], *, kind: str, empty_message: str
) -> str:
    if not queues:
        return f'<div class="empty-state">{_escape(empty_message)}</div>'

    cards = []
    for queue in queues:
        if kind == "merge":
            pipeline = _render_merge_pipeline(queue)
            pr_rows = "".join(
                _render_pr_row(
                    pr,
                    position=f"#{i + 1}",
                    pill_class="pill-queued",
                    pill_label="Queued",
                )
                for i, pr in enumerate(queue.get("queued_prs", []))
            )
            pr_list = f'<ul class="pr-list">{pr_rows}</ul>' if pr_rows else ""
            active_cls = "pill-active" if queue["active"] else "pill-idle"
            active_label = "Active" if queue["active"] else "Idle"
            cards.append(
                f"""
                <article class="queue-card">
                    <div class="queue-card-header">
                        <div class="queue-card-title">
                            <h3>{_escape(queue.get("owner") or "?")}/{_escape(queue.get("repo") or "?")} <span class="text-secondary">/ {_escape(queue.get("branch") or "?")}</span></h3>
                        </div>
                        <div class="queue-card-meta">
                            <span class="meta-item"><strong>{_escape(queue["size"])}</strong> queued</span>
                            <span class="pill {active_cls}"><span class="pill-dot"></span> {active_label}</span>
                        </div>
                    </div>
                    {pipeline}
                    {pr_list}
                </article>
                """
            )
        elif kind == "webhook":
            pr_rows = "".join(
                _render_pr_row(
                    ev,
                    position="&bull;",
                    pill_class="pill-evaluation",
                    pill_label="Pending",
                )
                for ev in queue.get("pending_events", [])
            )
            body = f'<ul class="pr-list">{pr_rows}</ul>' if pr_rows else '<div class="empty-state">No pending evaluations</div>'
            busy_cls = "pill-active" if queue["size"] else "pill-idle"
            busy_label = "Busy" if queue["size"] else "Idle"
            cards.append(
                f"""
                <article class="queue-card">
                    <div class="queue-card-header">
                        <div class="queue-card-title">
                            <h3>Installation {_escape(queue["installation_id"])}</h3>
                        </div>
                        <div class="queue-card-meta">
                            <span class="meta-item"><strong>{_escape(queue["size"])}</strong> pending</span>
                            <span class="meta-item text-tertiary">Oldest: {_escape(queue.get("oldest_event_iso") or "-")}</span>
                            <span class="pill {busy_cls}"><span class="pill-dot"></span> {busy_label}</span>
                        </div>
                    </div>
                    {body}
                </article>
                """
            )
        else:
            ingest_rows = "".join(
                _render_ingest_row(ev)
                for ev in queue.get("pending_events", [])
            )
            body = f'<ul class="pr-list">{ingest_rows}</ul>' if ingest_rows else '<div class="empty-state">No pending webhooks</div>'
            busy_cls = "pill-active" if queue["length"] else "pill-idle"
            busy_label = "Busy" if queue["length"] else "Idle"
            cards.append(
                f"""
                <article class="queue-card">
                    <div class="queue-card-header">
                        <div class="queue-card-title">
                            <h3>Installation {_escape(queue["installation_id"])}</h3>
                        </div>
                        <div class="queue-card-meta">
                            <span class="meta-item"><strong>{_escape(queue["length"])}</strong> pending</span>
                            <span class="pill {busy_cls}"><span class="pill-dot"></span> {busy_label}</span>
                        </div>
                    </div>
                    {body}
                </article>
                """
            )

    return "".join(cards)


def _stage_pill_class(stage: str | None) -> str:
    return f"pill-{stage}" if stage in ("merge", "decision", "evaluation", "ingest") else "pill-evaluation"


def _status_pill_class(status: str | None) -> str:
    mapping = {
        "merging": "pill-merging",
        "queued": "pill-queued",
        "waiting": "pill-waiting",
        "waiting_for_ci": "pill-waiting",
        "blocked": "pill-blocked",
    }
    return mapping.get(status or "", "pill-idle")


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
    stage = event.get("stage") or "evaluation"
    return f"""
    <li class="timeline-step stage-{_escape(stage)}">
        <div class="timeline-step-time">{_escape((event.get("ts_iso") or "-").split(" ")[1] if event.get("ts_iso") else "-")}</div>
        <div class="timeline-step-dot"></div>
        <div class="timeline-step-content">
            <div class="timeline-step-type">
                <span class="pill {_stage_pill_class(stage)}" style="font-size:0.62rem;padding:2px 7px;">{_escape(stage)}</span>
                {_escape(event.get("event_type") or "?")}
            </div>
            <div class="timeline-step-msg">{_escape(event.get("message") or "")}</div>
            {details_html}
        </div>
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
            owner = group.get("owner") or ""
            repo = group.get("repo") or ""
            pr_num = group.get("pr_number")
            title_label = f"{owner}/{repo} #{pr_num}"
            title = _gh_link(title_label, owner, repo, pr_num)
            status = group.get("latest_status")
            status_html = (
                f'<span class="pill {_status_pill_class(status)}">{_escape(status)}</span>'
                if status
                else ""
            )
            event_count = len(group.get("events", []))
            meta_html = f"""
                {status_html}
                <span>{event_count} event{"s" if event_count != 1 else ""}</span>
                <span>{_escape((group.get("latest_ts_iso") or "-").split(" ")[1] if group.get("latest_ts_iso") else "-")}</span>
            """
        else:
            title = (
                f"{_escape(group.get('event_name') or 'webhook')}"
                f" / {_escape(group.get('action') or '-')}"
            )
            owner = group.get("owner")
            repo = group.get("repo")
            pr_num = group.get("pr_number")
            if owner and repo and pr_num is not None:
                title += f" &mdash; {_gh_link(f'{owner}/{repo}#{pr_num}', owner, repo, pr_num)}"
            elif owner and repo:
                title += f" &mdash; {_gh_link(f'{owner}/{repo}', owner, repo)}"
            event_count = len(group.get("events", []))
            fanout = group.get("fanout_count")
            fanout_html = f"<span>Fan-out: {_escape(fanout)}</span>" if fanout is not None else ""
            meta_html = f"""
                <span>{event_count} event{"s" if event_count != 1 else ""}</span>
                <span>Delivery: {_escape(group.get("delivery_id") or "n/a")}</span>
                {fanout_html}
            """

        rendered.append(
            f"""
            <article class="timeline-card">
                <div class="timeline-card-header">
                    <h4>{title}</h4>
                    <div class="timeline-card-meta">{meta_html}</div>
                </div>
                <div class="timeline-body">
                    <ul class="timeline-steps">
                        {"".join(_render_timeline_event(event) for event in group["events"])}
                    </ul>
                </div>
            </article>
            """
        )

    return "".join(rendered)


def _render_recent_events(events: list[dict[str, Any]]) -> str:
    if not events:
        return '<div class="empty-state">No recent debug events recorded yet.</div>'

    rows = []
    for event in events:
        owner = event.get("owner")
        repo = event.get("repo")
        pr_num = event.get("pr_number")
        if owner and repo and pr_num is not None:
            target_html = _gh_link(f"{owner}/{repo}#{pr_num}", owner, repo, pr_num)
        elif owner and repo:
            target_html = _gh_link(f"{owner}/{repo}", owner, repo)
        else:
            target_html = "-"
        stage = event.get("stage") or "-"
        time_str = (event.get("ts_iso") or "-").split(" ")[1] if event.get("ts_iso") else "-"
        rows.append(
            f"""
            <tr>
                <td class="col-time">{_escape(time_str)}</td>
                <td><span class="pill {_stage_pill_class(event.get("stage"))}" style="font-size:0.62rem;padding:2px 7px;">{_escape(stage)}</span></td>
                <td><code>{_escape(event.get("event_type") or "-")}</code></td>
                <td class="col-target">{target_html}</td>
                <td class="col-message" title="{_escape(event.get("message") or "")}">{_escape(event.get("message") or "-")}</td>
            </tr>
            """
        )
    return f"""
    <table class="event-table">
        <thead>
            <tr>
                <th>Time</th>
                <th>Stage</th>
                <th>Type</th>
                <th>Target</th>
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
        *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
        :root {{
            --bg: #0a0e14;
            --surface: #131920;
            --surface-raised: #1a2230;
            --surface-overlay: #1e2a38;
            --border: #243044;
            --border-subtle: #1a2535;
            --ink: #e2eaf3;
            --ink-secondary: #8899aa;
            --ink-tertiary: #576878;
            --accent: #4da6ff;
            --accent-subtle: rgba(77, 166, 255, 0.12);
            --green: #2dd4a8;
            --green-subtle: rgba(45, 212, 168, 0.12);
            --yellow: #f5c542;
            --yellow-subtle: rgba(245, 197, 66, 0.12);
            --red: #ff6b6b;
            --red-subtle: rgba(255, 107, 107, 0.12);
            --purple: #a78bfa;
            --purple-subtle: rgba(167, 139, 250, 0.12);
            --orange: #fb923c;
            --orange-subtle: rgba(251, 146, 60, 0.12);
            --cyan: #22d3ee;
            --cyan-subtle: rgba(34, 211, 238, 0.12);
            --radius-sm: 6px; --radius-md: 10px; --radius-lg: 14px;
            --shadow: 0 2px 8px rgba(0,0,0,0.2), 0 12px 40px rgba(0,0,0,0.15);
            --font-mono: 'SF Mono', 'JetBrains Mono', Menlo, Monaco, Consolas, monospace;
            --font-sans: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, sans-serif;
            --transition: 180ms cubic-bezier(0.4, 0, 0.2, 1);
        }}
        html {{ scroll-behavior: smooth; }}
        body {{
            font-family: var(--font-sans);
            background: var(--bg);
            color: var(--ink);
            line-height: 1.5;
            -webkit-font-smoothing: antialiased;
        }}
        main {{
            width: min(1400px, calc(100vw - 40px));
            margin: 0 auto;
            padding: 28px 0 64px;
        }}
        a {{ color: var(--accent); text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        h1, h2, h3, h4 {{ margin: 0; }}
        h1 {{ font-size: clamp(1.6rem, 3vw, 2.2rem); font-weight: 700; letter-spacing: -0.03em; }}
        h2 {{ font-size: 1.05rem; font-weight: 600; margin-bottom: 14px; display: flex; align-items: center; gap: 8px; }}
        h2 .count {{ font-size: 0.72rem; background: var(--surface-overlay); color: var(--ink-secondary); padding: 2px 8px; border-radius: 999px; font-weight: 500; }}
        h3 {{ font-size: 0.9rem; font-weight: 600; }}
        h4 {{ font-size: 0.88rem; font-weight: 600; }}
        p {{ margin: 0; }}
        code {{ background: var(--surface-overlay); padding: 0.15rem 0.4rem; border-radius: 4px; font-family: var(--font-mono); font-size: 0.82rem; }}
        pre {{ white-space: pre-wrap; word-break: break-word; margin: 10px 0 0; padding: 12px; border-radius: var(--radius-md); background: var(--surface-raised); border: 1px solid var(--border); font-family: var(--font-mono); font-size: 0.78rem; color: var(--ink-secondary); }}
        section {{ margin-top: 24px; padding: 20px; border-radius: var(--radius-lg); background: var(--surface); border: 1px solid var(--border); box-shadow: var(--shadow); }}
        .text-secondary {{ color: var(--ink-secondary); }}
        .text-tertiary {{ color: var(--ink-tertiary); }}

        /* Hero */
        .hero {{ padding: 28px; background: linear-gradient(135deg, rgba(77,166,255,0.06), rgba(167,139,250,0.06)); }}
        .hero-copy {{ display: grid; gap: 8px; }}

        /* Stats */
        .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 10px; margin-top: 16px; }}
        .stat-card {{ background: var(--surface-raised); border: 1px solid var(--border); border-radius: var(--radius-md); padding: 14px 16px; position: relative; overflow: hidden; }}
        .stat-card::after {{ content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px; border-radius: 2px 2px 0 0; }}
        .stat-card.accent::after {{ background: var(--accent); }}
        .stat-card.green::after {{ background: var(--green); }}
        .stat-card.yellow::after {{ background: var(--yellow); }}
        .stat-card.purple::after {{ background: var(--purple); }}
        .stat-card.cyan::after {{ background: var(--cyan); }}
        .stat-card.muted::after {{ background: var(--ink-tertiary); }}
        .stat-label {{ font-size: 0.7rem; color: var(--ink-tertiary); text-transform: uppercase; letter-spacing: 0.07em; font-weight: 500; }}
        .stat-value {{ font-size: 1.7rem; font-weight: 750; font-variant-numeric: tabular-nums; letter-spacing: -0.04em; line-height: 1.2; }}
        .stat-value.c-accent {{ color: var(--accent); }}
        .stat-value.c-green {{ color: var(--green); }}
        .stat-value.c-yellow {{ color: var(--yellow); }}
        .stat-value.c-purple {{ color: var(--purple); }}
        .stat-value.c-cyan {{ color: var(--cyan); }}
        .stat-value.c-muted {{ color: var(--ink-secondary); }}
        .stat-sub {{ font-size: 0.74rem; color: var(--ink-tertiary); }}

        /* Queue cards */
        .queue-grid {{ display: grid; gap: 12px; }}
        .queue-card {{ background: var(--surface-raised); border: 1px solid var(--border); border-radius: var(--radius-lg); overflow: hidden; transition: border-color var(--transition); }}
        .queue-card:hover {{ border-color: var(--ink-tertiary); }}
        .queue-card-header {{ padding: 14px 18px; display: flex; align-items: center; justify-content: space-between; gap: 12px; border-bottom: 1px solid var(--border-subtle); }}
        .queue-card-title {{ display: flex; align-items: center; gap: 10px; min-width: 0; }}
        .queue-card-meta {{ display: flex; align-items: center; gap: 12px; flex-shrink: 0; }}
        .meta-item {{ font-size: 0.78rem; color: var(--ink-secondary); display: flex; align-items: center; gap: 4px; }}
        .meta-item strong {{ color: var(--ink); font-weight: 600; }}

        /* Pipeline */
        .merge-pipeline {{ padding: 14px 18px; background: linear-gradient(90deg, var(--green-subtle), var(--accent-subtle) 35%, transparent 70%); border-bottom: 1px solid var(--border-subtle); display: flex; align-items: center; gap: 0; overflow-x: auto; }}
        .pipeline-node {{ display: flex; align-items: center; gap: 8px; padding: 6px 12px; border-radius: 8px; font-size: 0.82rem; font-weight: 500; white-space: nowrap; flex-shrink: 0; }}
        .pipeline-node.merging {{ background: var(--green-subtle); border: 1px solid rgba(45,212,168,0.25); color: var(--green); }}
        .pipeline-node.merging .pipeline-dot {{ width: 8px; height: 8px; border-radius: 50%; background: var(--green); box-shadow: 0 0 10px rgba(45,212,168,0.5); animation: gp 1.5s ease-in-out infinite; }}
        @keyframes gp {{ 0%,100% {{ box-shadow: 0 0 8px rgba(45,212,168,0.4); }} 50% {{ box-shadow: 0 0 16px rgba(45,212,168,0.7); }} }}
        .pipeline-node.queued {{ background: var(--accent-subtle); border: 1px solid rgba(77,166,255,0.15); color: var(--accent); }}
        .pipeline-node.idle-slot {{ background: var(--surface-raised); border: 1px dashed var(--border); color: var(--ink-tertiary); font-style: italic; }}
        .pipeline-arrow {{ color: var(--ink-tertiary); flex-shrink: 0; margin: 0 4px; font-size: 0.9rem; opacity: 0.5; }}
        .pipeline-timer {{ font-family: var(--font-mono); font-size: 0.75rem; opacity: 0.8; }}
        .pipeline-pr {{ font-weight: 700; }}
        .pipeline-pr a {{ color: inherit; text-decoration: none; }}
        .pipeline-pr a:hover {{ text-decoration: underline; }}

        /* Pills */
        .pill {{ display: inline-flex; align-items: center; gap: 5px; padding: 3px 10px; border-radius: 999px; font-size: 0.7rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em; white-space: nowrap; }}
        .pill-dot {{ width: 6px; height: 6px; border-radius: 50%; background: currentColor; }}
        .pill-merging {{ background: var(--green-subtle); color: var(--green); }}
        .pill-queued {{ background: var(--accent-subtle); color: var(--accent); }}
        .pill-waiting {{ background: var(--yellow-subtle); color: var(--yellow); }}
        .pill-blocked {{ background: var(--red-subtle); color: var(--red); }}
        .pill-idle {{ background: var(--surface-overlay); color: var(--ink-tertiary); }}
        .pill-active {{ background: var(--green-subtle); color: var(--green); }}
        .pill-ingest {{ background: var(--purple-subtle); color: var(--purple); }}
        .pill-evaluation {{ background: var(--accent-subtle); color: var(--accent); }}
        .pill-decision {{ background: var(--yellow-subtle); color: var(--yellow); }}
        .pill-merge {{ background: var(--green-subtle); color: var(--green); }}

        /* PR list rows */
        .pr-list {{ list-style: none; }}
        .pr-row {{ display: grid; grid-template-columns: 36px 1fr auto auto auto; align-items: center; gap: 12px; padding: 10px 18px; border-bottom: 1px solid var(--border-subtle); font-size: 0.84rem; transition: background var(--transition); }}
        .pr-row:last-child {{ border-bottom: none; }}
        .pr-row:hover {{ background: var(--surface-overlay); }}
        .pr-position {{ font-family: var(--font-mono); font-weight: 700; color: var(--ink-tertiary); font-size: 0.78rem; text-align: center; }}
        .pr-info {{ min-width: 0; }}
        .pr-title {{ font-weight: 600; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
        .pr-sha {{ font-family: var(--font-mono); font-size: 0.72rem; color: var(--ink-tertiary); }}
        .pr-branch {{ font-size: 0.76rem; color: var(--ink-secondary); font-family: var(--font-mono); background: var(--surface-overlay); padding: 2px 8px; border-radius: var(--radius-sm); }}
        .pr-age {{ font-family: var(--font-mono); font-size: 0.8rem; color: var(--ink-secondary); text-align: right; white-space: nowrap; }}

        /* Event table */
        .event-table {{ width: 100%; border-collapse: separate; border-spacing: 0; background: var(--surface-raised); border: 1px solid var(--border); border-radius: var(--radius-lg); overflow: hidden; }}
        .event-table th {{ background: var(--surface-overlay); font-size: 0.68rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--ink-tertiary); padding: 10px 14px; text-align: left; font-weight: 600; border-bottom: 1px solid var(--border); white-space: nowrap; }}
        .event-table td {{ padding: 8px 14px; border-bottom: 1px solid var(--border-subtle); font-size: 0.82rem; vertical-align: middle; }}
        .event-table tr:last-child td {{ border-bottom: none; }}
        .event-table tbody tr:hover td {{ background: var(--surface-overlay); }}
        .col-time {{ font-family: var(--font-mono); font-size: 0.76rem; color: var(--ink-tertiary); white-space: nowrap; }}
        .col-target {{ font-weight: 500; }}
        .col-message {{ color: var(--ink-secondary); max-width: 420px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}

        /* Timeline */
        .timeline-card {{ background: var(--surface-raised); border: 1px solid var(--border); border-radius: var(--radius-lg); overflow: hidden; margin-bottom: 10px; }}
        .timeline-card-header {{ padding: 12px 16px; display: flex; align-items: center; justify-content: space-between; gap: 12px; border-bottom: 1px solid var(--border-subtle); }}
        .timeline-card-meta {{ display: flex; align-items: center; gap: 10px; font-size: 0.76rem; color: var(--ink-tertiary); }}
        .timeline-body {{ padding: 0; }}
        .timeline-steps {{ list-style: none; padding: 0; }}
        .timeline-step {{ display: grid; grid-template-columns: 78px 10px 1fr; gap: 0 10px; padding: 0 16px; min-height: 46px; }}
        .timeline-step-time {{ font-family: var(--font-mono); font-size: 0.7rem; color: var(--ink-tertiary); text-align: right; padding-top: 13px; }}
        .timeline-step-dot {{ position: relative; display: flex; flex-direction: column; align-items: center; }}
        .timeline-step-dot::before {{ content: ''; width: 9px; height: 9px; border-radius: 50%; background: var(--accent); margin-top: 15px; flex-shrink: 0; z-index: 1; }}
        .timeline-step-dot::after {{ content: ''; width: 2px; flex: 1; background: var(--border); }}
        .timeline-step:last-child .timeline-step-dot::after {{ display: none; }}
        .timeline-step.stage-merge .timeline-step-dot::before {{ background: var(--green); }}
        .timeline-step.stage-decision .timeline-step-dot::before {{ background: var(--yellow); }}
        .timeline-step.stage-evaluation .timeline-step-dot::before {{ background: var(--accent); }}
        .timeline-step.stage-ingest .timeline-step-dot::before {{ background: var(--purple); }}
        .timeline-step-content {{ padding: 10px 0 12px; }}
        .timeline-step-type {{ font-size: 0.76rem; font-weight: 600; font-family: var(--font-mono); margin-bottom: 2px; display: flex; align-items: center; gap: 6px; }}
        .timeline-step-msg {{ font-size: 0.8rem; color: var(--ink-secondary); }}

        /* Empty state */
        .empty-state {{ text-align: center; padding: 32px; color: var(--ink-tertiary); font-size: 0.86rem; border: 1px dashed var(--border); border-radius: var(--radius-md); }}

        /* Details */
        details {{ margin-top: 8px; }}
        summary {{ cursor: pointer; color: var(--accent); font-size: 0.82rem; }}

        @media (max-width: 720px) {{
            main {{ width: min(100vw - 20px, 100%); }}
            section {{ padding: 14px; }}
            .queue-grid {{ grid-template-columns: 1fr; }}
            .pr-row {{ grid-template-columns: 28px 1fr auto; }}
            .pr-branch, .pr-sha {{ display: none; }}
        }}
    </style>
</head>
<body>
    <main>
        <section class="hero">
            <div class="hero-copy">
                <p class="text-tertiary" style="font-size:0.78rem;">Collected at {collected_at}</p>
                <h1>Kodiak Debug Console</h1>
                <p class="text-secondary" style="font-size:0.88rem;">Live queue state + bounded debug history. Scope: {_escape(install_filter)}. Preview limit: {_escape(data["filters"]["queue_preview_limit"])}. History limit: {_escape(data["filters"]["history_limit"])}.</p>
            </div>
            <div class="stats-grid">
                {_render_summary_cards(data["summary"])}
            </div>
        </section>

        <section>
            <h2>Merge Queues <span class="count">{_escape(len(data["merge_queues"]))}</span></h2>
            <div class="queue-grid">
                {_render_queue_cards(data["merge_queues"], kind="merge", empty_message="No merge queues are currently registered.")}
            </div>
        </section>

        <section>
            <h2>PR Evaluation Queues <span class="count">{_escape(len(data["webhook_queues"]))}</span></h2>
            <div class="queue-grid">
                {_render_queue_cards(data["webhook_queues"], kind="webhook", empty_message="No PR evaluation queues are currently registered.")}
            </div>
        </section>

        <section>
            <h2>Ingest Queues <span class="count">{_escape(len(data["ingest_queues"]))}</span></h2>
            <div class="queue-grid">
                {_render_queue_cards(data["ingest_queues"], kind="ingest", empty_message="No ingest queues are currently registered.")}
            </div>
        </section>

        <section>
            <h2>PR Timelines <span class="count">{_escape(len(data["pr_timelines"]))}</span></h2>
            {_render_timeline_groups(data["pr_timelines"], title_builder="pr", empty_message="No recent PR debug history recorded yet.")}
        </section>

        <section>
            <h2>Webhook Timelines <span class="count">{_escape(len(data["webhook_timelines"]))}</span></h2>
            {_render_timeline_groups(data["webhook_timelines"], title_builder="webhook", empty_message="No recent webhook debug history recorded yet.")}
        </section>

        <section>
            <h2>Recent Events <span class="count">{_escape(len(data["recent_events"]))}</span></h2>
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
