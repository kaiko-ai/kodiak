"""
Shared helpers for scanning GitHub installations and filtering PRs.

This module is intentionally free of module-level side effects (no sentry_sdk.init(),
no structlog.configure()) so it can be safely imported from any process.
"""

from __future__ import annotations

from typing import Any, cast

from kodiak import app_config as conf
from kodiak.http import HttpClient
from kodiak.queries import generate_jwt

# Default automerge labels that Kodiak looks for.
# PRs without any of these labels are skipped during scans since Kodiak
# will immediately ignore them anyway (saving ~1.5s of API calls per PR).
DEFAULT_AUTOMERGE_LABELS = frozenset({"automerge", "dependencies", "version-bump"})


def is_pr_potentially_actionable(
    pull_request: dict[str, Any],
    automerge_labels: frozenset[str] = DEFAULT_AUTOMERGE_LABELS,
) -> bool:
    """
    Quick pre-filter to skip PRs that Kodiak will certainly ignore.

    Returns True if the PR *might* be actionable (has a matching label and
    is not a draft). Returns True on missing data to err on the side of
    caution.
    """
    if pull_request.get("isDraft", False):
        return False

    if pull_request.get("autoMergeRequest") is not None:
        return True

    labels_node = pull_request.get("labels")
    if labels_node is None:
        # If labels data is missing, enqueue to be safe.
        return True

    label_names = {
        node["name"] for node in labels_node.get("nodes", []) if "name" in node
    }
    return bool(label_names & automerge_labels)


async def get_login_for_install(*, http: HttpClient, installation_id: str) -> str:
    """Resolve the account login (org or user name) for a GitHub App installation."""
    app_token = generate_jwt(
        private_key=conf.PRIVATE_KEY, app_identifier=conf.GITHUB_APP_ID
    )
    res = await http.get(
        conf.v3_url(f"/app/installations/{installation_id}"),
        headers={
            "Accept": "application/vnd.github.machine-man-preview+json",
            "Authorization": f"Bearer {app_token}",
        },
    )
    res.raise_for_status()
    return cast(str, res.json()["account"]["login"])
