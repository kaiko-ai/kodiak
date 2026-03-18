from __future__ import annotations

import json
import time
from typing import Any, Mapping, Optional

import structlog
from redis.exceptions import RedisError

import kodiak.app_config as conf
from kodiak.redis_client import redis_bot

logger = structlog.get_logger()

DEBUG_EVENT_HISTORY_KEY = "kodiak:debug:events"


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    if isinstance(value, Mapping):
        return {str(key): _json_safe(val) for key, val in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return str(value)


def summarize_webhook_payload(
    *, event_name: str, payload: Mapping[str, Any]
) -> dict[str, Any]:
    installation = payload.get("installation")
    installation_id = (
        str(installation.get("id"))
        if isinstance(installation, Mapping) and installation.get("id") is not None
        else None
    )

    repository = payload.get("repository")
    owner = None
    repo = None
    repo_private = None
    if isinstance(repository, Mapping):
        owner_data = repository.get("owner")
        if isinstance(owner_data, Mapping):
            owner = owner_data.get("login")
        repo = repository.get("name")
        repo_private = repository.get("private")

    pull_request = payload.get("pull_request")
    pull_request_number = None
    target_branch = None
    head_branch = None
    head_sha = None
    if isinstance(pull_request, Mapping):
        pull_request_number = pull_request.get("number")
        base = pull_request.get("base")
        if isinstance(base, Mapping):
            target_branch = base.get("ref")
        head = pull_request.get("head")
        if isinstance(head, Mapping):
            head_branch = head.get("ref")
            head_sha = head.get("sha")

    check_run = payload.get("check_run")
    check_run_name = None
    check_run_status = None
    check_run_conclusion = None
    if isinstance(check_run, Mapping):
        check_run_name = check_run.get("name")
        check_run_status = check_run.get("status")
        check_run_conclusion = check_run.get("conclusion")
        head_sha = head_sha or check_run.get("head_sha")

    sender = payload.get("sender")
    sender_login = sender.get("login") if isinstance(sender, Mapping) else None

    return {
        "event_name": event_name,
        "action": payload.get("action"),
        "installation_id": installation_id,
        "owner": owner,
        "repo": repo,
        "repo_private": repo_private,
        "pull_request_number": pull_request_number or payload.get("number"),
        "target_branch": target_branch,
        "head_branch": head_branch,
        "sender": sender_login,
        "ref": payload.get("ref"),
        "sha": payload.get("sha") or payload.get("after") or head_sha,
        "status_context": payload.get("context"),
        "status_state": payload.get("state"),
        "check_run_name": check_run_name,
        "check_run_status": check_run_status,
        "check_run_conclusion": check_run_conclusion,
    }


async def record_debug_event(
    *,
    stage: str,
    event_type: str,
    message: str,
    installation_id: Optional[str] = None,
    owner: Optional[str] = None,
    repo: Optional[str] = None,
    pr_number: Optional[int] = None,
    queue_name: Optional[str] = None,
    delivery_id: Optional[str] = None,
    event_name: Optional[str] = None,
    action: Optional[str] = None,
    details: Optional[Mapping[str, Any]] = None,
) -> None:
    entry = {
        "ts": time.time(),
        "stage": stage,
        "event_type": event_type,
        "message": message,
    }
    if installation_id is not None:
        entry["installation_id"] = installation_id
    if owner is not None:
        entry["owner"] = owner
    if repo is not None:
        entry["repo"] = repo
    if pr_number is not None:
        entry["pr_number"] = pr_number
    if queue_name is not None:
        entry["queue_name"] = queue_name
    if delivery_id is not None:
        entry["delivery_id"] = delivery_id
    if event_name is not None:
        entry["event_name"] = event_name
    if action is not None:
        entry["action"] = action
    if owner is not None and repo is not None:
        entry["repo_key"] = f"{owner}/{repo}"
    if owner is not None and repo is not None and pr_number is not None:
        entry["pr_key"] = f"{owner}/{repo}#{pr_number}"
    if details:
        entry["details"] = _json_safe(details)

    try:
        await redis_bot.lpush(
            DEBUG_EVENT_HISTORY_KEY,
            json.dumps(entry, sort_keys=True),
        )
        await redis_bot.ltrim(
            DEBUG_EVENT_HISTORY_KEY,
            0,
            conf.DEBUG_EVENT_HISTORY_LENGTH - 1,
        )
    except (RedisError, TypeError, ValueError):
        logger.warning(
            "debug_event_record_failed",
            stage=stage,
            event_type=event_type,
            exc_info=True,
        )
