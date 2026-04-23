"""
Startup reconciliation scan.

On worker startup, scan all GitHub App installations and enqueue their open PRs
for evaluation. This catches PRs that became mergeable during downtime or whose
webhooks were dropped by GitHub.

The scan runs as a background asyncio task so webhook processing starts immediately.
"""

from __future__ import annotations

import asyncio
import time

import sentry_sdk
import structlog

from kodiak import app_config as conf
from kodiak._pr_scan_utils import (
    is_pr_potentially_actionable,
)
from kodiak.http import HttpClient
from kodiak.queries import generate_jwt, get_token_for_install
from kodiak.queue import RedisWebhookQueue, WebhookEvent

logger = structlog.get_logger()

# Same structure as refresh_pull_requests.py but without `privacy: PRIVATE`
# so both public and private repos are scanned.  The app only has access to
# repos it is installed on, so this is safe.
SCAN_QUERY = """
query ($login: String!) {
  userdata: user(login: $login) {
    repositories(first: 100, orderBy: {field: UPDATED_AT, direction: DESC}) {
      nodes {
        name
        pullRequests(first: 100, states: OPEN, orderBy: {field: UPDATED_AT, direction: DESC}) {
          nodes {
            number
            baseRef {
              name
            }
            isDraft
            autoMergeRequest {
              enabledAt
            }
            labels(first: 10) {
              nodes {
                name
              }
            }
          }
        }
      }
    }
  }
  organizationdata: organization(login: $login) {
    repositories(first: 100, orderBy: {field: UPDATED_AT, direction: DESC}) {
      nodes {
        name
        pullRequests(first: 100, states: OPEN, orderBy: {field: UPDATED_AT, direction: DESC}) {
          nodes {
            number
            baseRef {
              name
            }
            isDraft
            autoMergeRequest {
              enabledAt
            }
            labels(first: 10) {
              nodes {
                name
              }
            }
          }
        }
      }
    }
  }
}
"""


async def list_app_installations(*, http: HttpClient) -> list[dict[str, object]]:
    """List all installations of the GitHub App, with pagination."""
    app_token = generate_jwt(
        private_key=conf.PRIVATE_KEY, app_identifier=conf.GITHUB_APP_ID
    )
    headers = {
        "Accept": "application/vnd.github.machine-man-preview+json",
        "Authorization": f"Bearer {app_token}",
    }
    results: list[dict[str, object]] = []
    url: str | None = conf.v3_url("/app/installations")
    while url:
        res = await http.get(url, headers=headers)
        res.raise_for_status()
        results.extend(res.json())
        url = res.links.get("next", {}).get("url")
    return results


async def scan_installation(
    *,
    installation_id: str,
    login: str,
    queue: RedisWebhookQueue,
    http: HttpClient,
) -> int:
    """
    Scan a single installation's open PRs and enqueue actionable ones.

    Returns the number of events enqueued.
    """
    token = await get_token_for_install(session=http, installation_id=installation_id)
    res = await http.post(
        conf.GITHUB_V4_API_URL,
        json={"query": SCAN_QUERY, "variables": {"login": login}},
        headers={"Authorization": f"Bearer {token}"},
    )
    res.raise_for_status()

    data = res.json()["data"]
    org_data = data["organizationdata"]
    user_data = data["userdata"]

    if org_data is not None:
        account_data = org_data
    elif user_data is not None:
        account_data = user_data
    else:
        logger.warning(
            "startup_scan_no_data",
            installation_id=installation_id,
            login=login,
        )
        return 0

    enqueued = 0
    for repository in account_data["repositories"]["nodes"]:
        repo_name: str = repository["name"]
        for pull_request in repository["pullRequests"]["nodes"]:
            if not is_pr_potentially_actionable(pull_request):
                continue
            event = WebhookEvent(
                repo_owner=login,
                repo_name=repo_name,
                target_name=pull_request["baseRef"]["name"],
                pull_request_number=pull_request["number"],
                installation_id=installation_id,
            )
            await queue.enqueue(event=event)
            enqueued += 1

    return enqueued


async def _scan_with_semaphore(
    sem: asyncio.Semaphore,
    installation: dict[str, object],
    queue: RedisWebhookQueue,
    http: HttpClient,
) -> int:
    """Scan a single installation under a concurrency semaphore."""
    installation_id = str(installation["id"])
    account = installation.get("account")
    if not isinstance(account, dict) or "login" not in account:
        logger.warning(
            "startup_scan_skip_bad_install",
            installation_id=installation_id,
        )
        return 0

    login = str(account["login"])
    async with sem:
        try:
            return await scan_installation(
                installation_id=installation_id,
                login=login,
                queue=queue,
                http=http,
            )
        except Exception:
            logger.exception(
                "startup_scan_installation_failed",
                installation_id=installation_id,
                login=login,
            )
            sentry_sdk.capture_exception()
            return 0


async def run_startup_scan(queue: RedisWebhookQueue) -> None:
    """
    Scan all installations and enqueue open PRs for evaluation.

    Designed to run as a fire-and-forget background task at worker startup.
    """
    if not conf.STARTUP_RECONCILIATION_ENABLED:
        logger.info("startup_reconciliation_disabled")
        return

    start = time.monotonic()
    log = logger.bind(task="startup_reconciliation")
    log.info("startup_reconciliation_started")

    try:
        async with HttpClient() as http:
            installations = await list_app_installations(http=http)
            log.info(
                "startup_reconciliation_installations_found",
                count=len(installations),
            )

            sem = asyncio.Semaphore(conf.STARTUP_RECONCILIATION_CONCURRENCY)
            results = await asyncio.gather(
                *[
                    _scan_with_semaphore(sem, install, queue, http)
                    for install in installations
                ],
                return_exceptions=True,
            )

        total_enqueued = 0
        failed = 0
        for result in results:
            if isinstance(result, BaseException):
                failed += 1
                sentry_sdk.capture_exception(result)
            else:
                total_enqueued += result

        log.info(
            "startup_reconciliation_completed",
            total_installations=len(installations),
            total_enqueued=total_enqueued,
            failed=failed,
            duration=time.monotonic() - start,
        )
    except Exception:
        log.exception("startup_reconciliation_failed")
        sentry_sdk.capture_exception()
