"""
Debug UI for Kodiak queue inspection.

Provides /debug/queues (HTML) and /debug/queues.json (JSON) endpoints
with token-based authentication.
"""

from __future__ import annotations

import asyncio
import hmac
import json
import os
import secrets
import time
from typing import Any

import structlog
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, Response

from kodiak.queue import INGEST_QUEUE_NAMES, MERGE_QUEUE_NAMES, WEBHOOK_QUEUE_NAMES
from kodiak.redis_client import redis_bot

logger = structlog.get_logger()

_debug_token: str = ""


def initialize_debug_token() -> None:
    """
    Initialize the debug token. Call once at module load in ingest.py.
    Uses DEBUG_TOKEN env var if set, otherwise generates a random token.
    """
    global _debug_token
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


async def _collect_merge_queues() -> list[dict[str, Any]]:
    """Collect merge queue data from Redis."""
    queue_names_raw = await redis_bot.smembers(MERGE_QUEUE_NAMES)
    queues = []
    for raw in queue_names_raw:
        name = raw.decode() if isinstance(raw, bytes) else raw
        # name format: merge_queue:{install}.{owner}/{repo}/{branch}
        try:
            _, rest = name.split(":", 1)
            install_part, repo_part = rest.split(".", 1)
        except ValueError:
            install_part = ""
            repo_part = name

        size = await redis_bot.zcard(name)
        target_key = name + ":target"
        target_time_key = name + ":target:time"
        target_raw = await redis_bot.get(target_key)
        target_time_raw = await redis_bot.get(target_time_key)

        merging_pr = None
        merge_duration_sec = None
        if target_raw:
            try:
                merging_pr = json.loads(target_raw).get("pull_request_number")
            except (json.JSONDecodeError, AttributeError, TypeError):
                merging_pr = target_raw.decode() if isinstance(target_raw, bytes) else str(target_raw)
        if target_time_raw:
            try:
                enqueue_time = float(target_time_raw)
                merge_duration_sec = int(time.time() - enqueue_time)
            except (ValueError, TypeError):
                pass

        members = await redis_bot.zrange(name, 0, 100, withscores=True)
        pr_list = []
        for member, score in members:
            try:
                data = json.loads(member)
                pr_list.append(
                    {
                        "number": data.get("pull_request_number"),
                        "enqueued_at": score,
                    }
                )
            except (json.JSONDecodeError, AttributeError, TypeError, KeyError):
                pr_list.append({"raw": member.decode() if isinstance(member, bytes) else str(member), "enqueued_at": score})

        queues.append(
            {
                "name": name,
                "repo": repo_part,
                "installation_id": install_part,
                "size": size,
                "merging_pr": merging_pr,
                "merge_duration_sec": merge_duration_sec,
                "queued_prs": pr_list,
            }
        )
    return queues


async def _collect_webhook_queues() -> list[dict[str, Any]]:
    """Collect webhook queue data from Redis."""
    queue_names_raw = await redis_bot.smembers(WEBHOOK_QUEUE_NAMES)
    queues = []
    for raw in queue_names_raw:
        name = raw.decode() if isinstance(raw, bytes) else raw
        size = await redis_bot.zcard(name)

        oldest_ts = None
        newest_ts = None
        if size > 0:
            oldest = await redis_bot.zrange(name, 0, 0, withscores=True)
            newest = await redis_bot.zrange(name, -1, -1, withscores=True)
            if oldest:
                oldest_ts = oldest[0][1]
            if newest:
                newest_ts = newest[0][1]

        queues.append(
            {
                "name": name,
                "size": size,
                "oldest_event_ts": oldest_ts,
                "newest_event_ts": newest_ts,
            }
        )
    return queues


async def _collect_ingest_queues() -> list[dict[str, Any]]:
    """Collect ingest queue data from Redis."""
    queue_names_raw = await redis_bot.smembers(INGEST_QUEUE_NAMES)
    queues = []
    for raw in queue_names_raw:
        name = raw.decode() if isinstance(raw, bytes) else raw
        length = await redis_bot.llen(name)
        queues.append({"name": name, "length": length})
    return queues


async def _collect_all_data() -> dict[str, Any]:
    merge_queues, webhook_queues, ingest_queues = await asyncio.gather(
        _collect_merge_queues(),
        _collect_webhook_queues(),
        _collect_ingest_queues(),
    )
    return {
        "merge_queues": merge_queues,
        "webhook_queues": webhook_queues,
        "ingest_queues": ingest_queues,
        "collected_at": time.time(),
    }


def _render_html(data: dict[str, Any]) -> str:
    merge_rows = ""
    for q in data["merge_queues"]:
        pr_nums = ", ".join(str(p.get("number", "?")) for p in q["queued_prs"])
        duration = f"{q['merge_duration_sec']}s" if q["merge_duration_sec"] is not None else "-"
        merge_rows += f"""<tr>
            <td>{q['repo']}</td>
            <td>{q['size']}</td>
            <td>{q['merging_pr'] or '-'}</td>
            <td>{duration}</td>
            <td>{pr_nums or '-'}</td>
        </tr>"""

    webhook_rows = ""
    for q in data["webhook_queues"]:
        oldest = f"{q['oldest_event_ts']:.1f}" if q["oldest_event_ts"] else "-"
        newest = f"{q['newest_event_ts']:.1f}" if q["newest_event_ts"] else "-"
        webhook_rows += f"""<tr>
            <td>{q['name']}</td>
            <td>{q['size']}</td>
            <td>{oldest}</td>
            <td>{newest}</td>
        </tr>"""

    ingest_rows = ""
    for q in data["ingest_queues"]:
        ingest_rows += f"""<tr>
            <td>{q['name']}</td>
            <td>{q['length']}</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html>
<head>
    <title>Kodiak Debug - Queue Status</title>
    <meta http-equiv="refresh" content="30">
    <style>
        body {{ font-family: monospace; margin: 2em; background: #1a1a2e; color: #eee; }}
        h1 {{ color: #e94560; }}
        h2 {{ color: #0f3460; background: #16213e; padding: 0.5em; border-radius: 4px; color: #e94560; }}
        table {{ border-collapse: collapse; width: 100%; margin-bottom: 2em; }}
        th, td {{ border: 1px solid #333; padding: 8px 12px; text-align: left; }}
        th {{ background: #16213e; color: #e94560; }}
        tr:nth-child(even) {{ background: #16213e; }}
        tr:hover {{ background: #0f3460; }}
        .timestamp {{ color: #888; font-size: 0.9em; }}
    </style>
</head>
<body>
    <h1>Kodiak Queue Status</h1>
    <p class="timestamp">Collected at: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(data['collected_at']))}</p>

    <h2>Merge Queues ({len(data['merge_queues'])})</h2>
    <table>
        <tr><th>Repo/Branch</th><th>Size</th><th>Merging PR</th><th>Duration</th><th>Queued PRs</th></tr>
        {merge_rows or '<tr><td colspan="5">No merge queues</td></tr>'}
    </table>

    <h2>Webhook Queues ({len(data['webhook_queues'])})</h2>
    <table>
        <tr><th>Queue</th><th>Size</th><th>Oldest Event</th><th>Newest Event</th></tr>
        {webhook_rows or '<tr><td colspan="4">No webhook queues</td></tr>'}
    </table>

    <h2>Ingest Queues ({len(data['ingest_queues'])})</h2>
    <table>
        <tr><th>Queue</th><th>Length</th></tr>
        {ingest_rows or '<tr><td colspan="2">No ingest queues</td></tr>'}
    </table>
</body>
</html>"""


async def debug_index(request: Request) -> Response:
    if not require_debug_token(request):
        return JSONResponse({"error": "forbidden"}, status_code=403)

    html = """<!DOCTYPE html>
<html>
<head>
    <title>Kodiak Debug</title>
    <style>
        body { font-family: monospace; margin: 2em; background: #1a1a2e; color: #eee; }
        h1 { color: #e94560; }
        a { color: #4ea8de; }
        ul { line-height: 2; }
    </style>
</head>
<body>
    <h1>Kodiak Debug</h1>
    <ul>
        <li><a href="/debug/queues">Queue Status (HTML)</a></li>
        <li><a href="/debug/queues.json">Queue Status (JSON)</a></li>
    </ul>
</body>
</html>"""
    response = HTMLResponse(html)
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

    data = await _collect_all_data()
    html = _render_html(data)
    response = HTMLResponse(html)

    # Set cookie if authenticated via query param so subsequent requests
    # don't need the token in the URL.
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

    data = await _collect_all_data()
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
