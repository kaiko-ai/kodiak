from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kodiak.startup_reconciliation import (
    list_app_installations,
    run_startup_scan,
    scan_installation,
)


def _make_graphql_response(
    *,
    repos: list[dict[str, object]],
    kind: str = "organization",
) -> dict[str, object]:
    """Build a fake GraphQL response payload."""
    org_key = "organizationdata" if kind == "organization" else "userdata"
    other_key = "userdata" if kind == "organization" else "organizationdata"
    return {
        "data": {
            org_key: {"repositories": {"nodes": repos}},
            other_key: None,
        }
    }


def _make_repo(name: str, pull_requests: list[dict[str, object]]) -> dict[str, object]:
    return {"name": name, "pullRequests": {"nodes": pull_requests}}


def _make_pr(
    number: int,
    base: str = "main",
    is_draft: bool = False,
    labels: list[str] | None = None,
) -> dict[str, object]:
    pr: dict[str, object] = {
        "number": number,
        "baseRef": {"name": base},
        "isDraft": is_draft,
    }
    if labels is not None:
        pr["labels"] = {"nodes": [{"name": n} for n in labels]}
    return pr


# ---------------------------------------------------------------------------
# list_app_installations
# ---------------------------------------------------------------------------


class TestListAppInstallations:
    async def test_single_page(self) -> None:
        http = AsyncMock()
        response = MagicMock()
        response.json.return_value = [{"id": 1, "account": {"login": "org1"}}]
        response.links = {}
        http.get.return_value = response

        with patch("kodiak.startup_reconciliation.generate_jwt", return_value="jwt"):
            result = await list_app_installations(http=http)

        assert result == [{"id": 1, "account": {"login": "org1"}}]
        assert http.get.call_count == 1

    async def test_pagination(self) -> None:
        http = AsyncMock()

        page1 = MagicMock()
        page1.json.return_value = [{"id": 1, "account": {"login": "org1"}}]
        page1.links = {
            "next": {"url": "https://api.github.com/app/installations?page=2"}
        }

        page2 = MagicMock()
        page2.json.return_value = [{"id": 2, "account": {"login": "org2"}}]
        page2.links = {}

        http.get.side_effect = [page1, page2]

        with patch("kodiak.startup_reconciliation.generate_jwt", return_value="jwt"):
            result = await list_app_installations(http=http)

        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[1]["id"] == 2
        assert http.get.call_count == 2


# ---------------------------------------------------------------------------
# scan_installation
# ---------------------------------------------------------------------------


class TestScanInstallation:
    async def test_enqueues_actionable_prs(self) -> None:
        queue = AsyncMock()
        http = AsyncMock()

        graphql_data = _make_graphql_response(
            repos=[
                _make_repo(
                    "repo-a",
                    [
                        _make_pr(1, labels=["automerge"]),
                        _make_pr(2, labels=["automerge"]),
                    ],
                ),
                _make_repo(
                    "repo-b",
                    [
                        _make_pr(10, labels=["dependencies"]),
                    ],
                ),
            ]
        )
        response = MagicMock()
        response.json.return_value = graphql_data
        http.post.return_value = response

        with patch(
            "kodiak.startup_reconciliation.get_token_for_install",
            return_value="token",
        ):
            count = await scan_installation(
                installation_id="123",
                login="my-org",
                queue=queue,
                http=http,
            )

        assert count == 3
        assert queue.enqueue.call_count == 3

    async def test_skips_drafts_and_unlabeled(self) -> None:
        queue = AsyncMock()
        http = AsyncMock()

        graphql_data = _make_graphql_response(
            repos=[
                _make_repo(
                    "repo-a",
                    [
                        _make_pr(1, is_draft=True, labels=["automerge"]),
                        _make_pr(2, labels=["bug"]),
                        _make_pr(3, labels=["automerge"]),
                    ],
                ),
            ]
        )
        response = MagicMock()
        response.json.return_value = graphql_data
        http.post.return_value = response

        with patch(
            "kodiak.startup_reconciliation.get_token_for_install",
            return_value="token",
        ):
            count = await scan_installation(
                installation_id="123",
                login="my-org",
                queue=queue,
                http=http,
            )

        assert count == 1
        assert queue.enqueue.call_count == 1
        event = queue.enqueue.call_args_list[0].kwargs["event"]
        assert event.pull_request_number == 3

    async def test_handles_user_account(self) -> None:
        queue = AsyncMock()
        http = AsyncMock()

        graphql_data = _make_graphql_response(
            repos=[
                _make_repo(
                    "my-repo",
                    [
                        _make_pr(5, labels=["automerge"]),
                    ],
                ),
            ],
            kind="user",
        )
        response = MagicMock()
        response.json.return_value = graphql_data
        http.post.return_value = response

        with patch(
            "kodiak.startup_reconciliation.get_token_for_install",
            return_value="token",
        ):
            count = await scan_installation(
                installation_id="456",
                login="some-user",
                queue=queue,
                http=http,
            )

        assert count == 1

    async def test_returns_zero_when_no_data(self) -> None:
        queue = AsyncMock()
        http = AsyncMock()

        response = MagicMock()
        response.json.return_value = {
            "data": {"organizationdata": None, "userdata": None}
        }
        http.post.return_value = response

        with patch(
            "kodiak.startup_reconciliation.get_token_for_install",
            return_value="token",
        ):
            count = await scan_installation(
                installation_id="789",
                login="ghost",
                queue=queue,
                http=http,
            )

        assert count == 0
        assert queue.enqueue.call_count == 0


# ---------------------------------------------------------------------------
# run_startup_scan
# ---------------------------------------------------------------------------


def _setup_run_scan_mocks(
    mock_conf: MagicMock,
    mock_client_cls: MagicMock,
) -> AsyncMock:
    """Common setup for run_startup_scan tests."""
    mock_conf.STARTUP_RECONCILIATION_ENABLED = True
    mock_conf.STARTUP_RECONCILIATION_CONCURRENCY = 5
    mock_http = AsyncMock()
    mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_http)
    mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)
    return mock_http


class TestRunStartupScan:
    async def test_disabled(self) -> None:
        queue = AsyncMock()
        with patch("kodiak.startup_reconciliation.conf") as mock_conf:
            mock_conf.STARTUP_RECONCILIATION_ENABLED = False
            await run_startup_scan(queue)

        assert queue.enqueue.call_count == 0

    @pytest.mark.parametrize("failing_index", [0, 1])
    async def test_partial_failure(self, failing_index: int) -> None:
        """One installation failing should not prevent others from being scanned."""
        queue = AsyncMock()

        installations = [
            {"id": 100, "account": {"login": "good-org"}},
            {"id": 200, "account": {"login": "bad-org"}},
        ]

        async def fake_scan(
            *,
            installation_id: str,
            login: str,
            queue: object,
            http: object,
        ) -> int:
            if installation_id == str(installations[failing_index]["id"]):
                raise RuntimeError("GitHub API error")
            return 5

        with patch("kodiak.startup_reconciliation.conf") as mock_conf, patch(
            "kodiak.startup_reconciliation.list_app_installations",
            return_value=installations,
        ), patch(
            "kodiak.startup_reconciliation.scan_installation", side_effect=fake_scan
        ), patch("kodiak.startup_reconciliation.HttpClient") as mock_client_cls, patch(
            "kodiak.startup_reconciliation.sentry_sdk"
        ):
            _setup_run_scan_mocks(mock_conf, mock_client_cls)
            await run_startup_scan(queue)

        # Should not raise — the scan completes with partial results.

    async def test_enqueues_across_installations(self) -> None:
        queue = AsyncMock()

        installations = [
            {"id": 10, "account": {"login": "org-a"}},
            {"id": 20, "account": {"login": "org-b"}},
        ]

        call_count = 0

        async def fake_scan(
            *,
            installation_id: str,
            login: str,
            queue: object,
            http: object,
        ) -> int:
            nonlocal call_count
            call_count += 1
            return 3

        with patch("kodiak.startup_reconciliation.conf") as mock_conf, patch(
            "kodiak.startup_reconciliation.list_app_installations",
            return_value=installations,
        ), patch(
            "kodiak.startup_reconciliation.scan_installation", side_effect=fake_scan
        ), patch("kodiak.startup_reconciliation.HttpClient") as mock_client_cls, patch(
            "kodiak.startup_reconciliation.sentry_sdk"
        ):
            _setup_run_scan_mocks(mock_conf, mock_client_cls)
            await run_startup_scan(queue)

        assert call_count == 2
