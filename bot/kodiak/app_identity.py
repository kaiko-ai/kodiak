"""
Resolve the GitHub App's identity (slug, bot login) at startup.

Instead of relying on a manually configured GITHUB_APP_NAME env var, we call
GET /app with a short-lived JWT to learn our own slug.  This avoids
mis-configurations that can cause infinite-loop bugs (e.g. the bot
not recognising its own reviews).
"""

from __future__ import annotations

import json
import logging
import urllib.request
from datetime import datetime, timedelta
from typing import Optional

import jwt as pyjwt

from kodiak import app_config as conf

logger = logging.getLogger(__name__)

_app_slug: Optional[str] = None


def _generate_jwt() -> str:
    """Create a short-lived App JWT for the GET /app call."""
    now = datetime.now()  # noqa: DTZ005
    payload = dict(
        iat=int(now.timestamp()),
        exp=int((now + timedelta(minutes=5)).timestamp()),
        iss=conf.GITHUB_APP_ID,
    )
    token: bytes = pyjwt.encode(payload=payload, key=conf.PRIVATE_KEY, algorithm="RS256")
    return token.decode("utf-8")


def init() -> None:
    """Fetch app identity from GitHub API.  Call once at process startup."""
    global _app_slug
    token = _generate_jwt()
    req = urllib.request.Request(
        conf.GITHUB_V3_API_ROOT + "/app",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
        },
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())
    _app_slug = data["slug"]
    logger.info(
        "resolved app identity: slug=%s app_id=%s", _app_slug, conf.GITHUB_APP_ID
    )


def app_slug() -> str:
    """The GitHub App slug (e.g. ``kaiko-kodiak``)."""
    if _app_slug is not None:
        return _app_slug
    # Fallback for tests or code that runs before init().
    if conf.GITHUB_APP_NAME is not None:
        return conf.GITHUB_APP_NAME
    raise RuntimeError(
        "app_identity.init() has not been called and GITHUB_APP_NAME is not set"
    )


def bot_login() -> str:
    """Bot login in REST API / webhook payloads (e.g. ``kaiko-kodiak[bot]``)."""
    return app_slug() + "[bot]"


def graphql_login() -> str:
    """Bot login in GraphQL responses (e.g. ``kaiko-kodiak``)."""
    return app_slug()
