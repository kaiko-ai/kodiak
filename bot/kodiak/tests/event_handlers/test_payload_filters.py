import pytest

from kodiak.events.base import Installation
from kodiak.events.check_run import (
    CheckRun,
    CheckRunEvent,
    Owner as CheckRunOwner,
    PullRequest as CheckRunPullRequest,
    PullRequestRepository,
    Ref as CheckRunRef,
    Repository as CheckRunRepository,
)
from kodiak.events.pull_request import (
    Owner as PullRequestOwner,
    PullRequest as PullRequestPayload,
    PullRequestEvent,
    Ref as PullRequestRef,
    Repository as PullRequestRepositoryPayload,
)
from kodiak.events.pull_request_review import (
    Owner as ReviewOwner,
    PullRequest as ReviewPullRequest,
    PullRequestReviewEvent,
    Ref as ReviewRef,
    Repository as ReviewRepository,
    Review,
    ReviewUser,
)
from kodiak.queue import WebhookEvent, check_run, pr_event, pr_review


@pytest.mark.asyncio
async def test_pr_event_ignores_non_actionable_pull_request_actions() -> None:
    event = PullRequestEvent(
        action="assigned",
        installation=Installation(id=42),
        number=123,
        pull_request=PullRequestPayload(base=PullRequestRef(ref="main")),
        repository=PullRequestRepositoryPayload(
            name="widgets",
            owner=PullRequestOwner(login="acme"),
        ),
    )

    assert await pr_event(event) == []


@pytest.mark.asyncio
async def test_pr_review_ignores_edited_review_body_updates() -> None:
    event = PullRequestReviewEvent(
        action="edited",
        installation=Installation(id=42),
        pull_request=ReviewPullRequest(number=123, base=ReviewRef(ref="main")),
        repository=ReviewRepository(name="widgets", owner=ReviewOwner(login="acme")),
        review=Review(user=ReviewUser(login="octocat")),
    )

    assert await pr_review(event) == []


def test_check_run_ignores_requested_actions() -> None:
    internal_repository_id = 554453
    event = CheckRunEvent(
        action="requested_action",
        installation=Installation(id=69039045),
        check_run=CheckRun(
            name="circleci: build",
            pull_requests=[
                CheckRunPullRequest(
                    number=123,
                    base=CheckRunRef(
                        ref="main",
                        repo=PullRequestRepository(id=internal_repository_id),
                    ),
                )
            ],
        ),
        repository=CheckRunRepository(
            id=internal_repository_id,
            name="cake-api",
            owner=CheckRunOwner(login="acme-corp"),
        ),
    )

    assert check_run(event) == []


@pytest.mark.asyncio
async def test_pr_event_keeps_actionable_pull_request_actions() -> None:
    event = PullRequestEvent(
        action="synchronize",
        installation=Installation(id=42),
        number=123,
        pull_request=PullRequestPayload(base=PullRequestRef(ref="main")),
        repository=PullRequestRepositoryPayload(
            name="widgets",
            owner=PullRequestOwner(login="acme"),
        ),
    )

    assert await pr_event(event) == [
        WebhookEvent(
            repo_owner="acme",
            repo_name="widgets",
            pull_request_number=123,
            installation_id="42",
            target_name="main",
        )
    ]
