from __future__ import annotations

import asyncio
import json
import time
import typing
import urllib
import uuid
from asyncio.tasks import Task
from dataclasses import dataclass
from datetime import timedelta
from typing import Iterator, MutableMapping, NoReturn, Optional, Tuple

import sentry_sdk
import structlog
import zstandard as zstd
from pydantic import BaseModel
from typing_extensions import Literal, Protocol

from kodiak import (
    app_config as conf,
    app_identity,
    queries,
)
from kodiak.debug_history import record_debug_event, summarize_webhook_payload
from kodiak.events import (
    CheckRunEvent,
    PullRequestEvent,
    PullRequestReviewEvent,
    PullRequestReviewThreadEvent,
    PushEvent,
    StatusEvent,
)
from kodiak.events.status import Branch
from kodiak.pull_request import evaluate_pr
from kodiak.queries import Client
from kodiak.redis_client import redis_bot, redis_web_api

logger = structlog.get_logger()


INGEST_QUEUE_NAMES = "kodiak_ingest_queue_names"
MERGE_QUEUE_NAMES = "kodiak_merge_queue_names:v2"
MERGE_QUEUE_BY_INSTALL_PREFIX = "merge_queue_by_install:"
WEBHOOK_QUEUE_NAMES = "kodiak_webhook_queue_names"
QUEUE_PUBSUB_INGEST = "kodiak:pubsub:ingest"
WEBHOOK_LATEST_HEAD_SHA_PREFIX = "kodiak:webhook_latest_head_sha:"
MERGE_COOLDOWN_PREFIX = "kodiak:merge_cooldown:"
MERGE_COOLDOWN_TTL = 300  # 5-minute cooldown after merge queue poll timeout

REQUEUE_ATTEMPTS_PREFIX = "kodiak:requeue_attempts:"
REQUEUE_ATTEMPTS_TTL = 300  # auto-expire so PRs get a fresh chance after 5 minutes

STATUS_DEDUP_PREFIX = "kodiak:last_status:"
STATUS_DEDUP_TTL = 300  # Only re-post identical status after 5 minutes

NOINTENT_CACHE_PREFIX = "kodiak:nointent:"
NOINTENT_CACHE_TTL = int(timedelta(hours=1).total_seconds())
# Short-lived sentinel value written by clear_nolabel_cache to prevent
# concurrent stale evaluations from re-poisoning the cache.
NOINTENT_CLEARED_TTL = 120

# Per-PR evaluation lock.  Webhook consumers run with
# WEBHOOK_CONSUMER_CONCURRENCY > 1 and a burst of events for the same PR
# (opened + labeled + synchronize + check_run, etc.) can otherwise produce
# parallel evaluations that each read stale state and each post a duplicate
# approval / update_branch / comment.  The lock serialises evaluations for
# one PR while leaving different PRs free to run in parallel.
PR_EVAL_LOCK_PREFIX = "kodiak:pr_eval_lock:"
# Safe upper bound: covers a normal evaluation plus a few PollForever
# cycles.  If the holder crashes, the key expires and another worker can
# proceed.
PR_EVAL_LOCK_TTL_SEC = 180
# Delay before a contention-skipped event is retried, so the lock holder
# has a chance to finish first.
PR_EVAL_LOCK_CONTENTION_REQUEUE_DELAY_SEC = 2

# Compare-and-delete so a worker never releases a lock it no longer owns
# (e.g. its TTL expired and another worker re-acquired it).
_RELEASE_PR_EVAL_LOCK_LUA = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""


def get_ingest_queue(installation_id: int) -> str:
    return f"kodiak:ingest:{installation_id}"


def _nolabel_cache_key(install: str, owner: str, repo: str, number: int) -> str:
    return f"{NOINTENT_CACHE_PREFIX}{install}:{owner}/{repo}#{number}"


async def set_nolabel_cache(install: str, owner: str, repo: str, number: int) -> None:
    """Cache that a PR was skipped because it lacks merge/update intent.

    Skips the write if a "cleared" sentinel is present — this means a
    label-change webhook was recently processed and a concurrent stale
    evaluation should not re-poison the cache.
    """
    key = _nolabel_cache_key(install, owner, repo, number)
    current = await redis_bot.get(key)
    if current == b"cleared":
        return
    await redis_bot.set(key, b"1", ex=NOINTENT_CACHE_TTL)


async def check_nolabel_cache(install: str, owner: str, repo: str, number: int) -> bool:
    """Return True if we've cached that this PR lacks merge/update intent."""
    result = await redis_bot.get(_nolabel_cache_key(install, owner, repo, number))
    return result == b"1"


async def clear_nolabel_cache(install: str, owner: str, repo: str, number: int) -> None:
    """Invalidate the no-automerge-label cache for a PR.

    Sets a short-lived "cleared" sentinel instead of deleting the key.
    This prevents concurrent stale evaluations (which started before the
    label change) from re-setting the cache after we clear it.
    """
    await redis_bot.set(
        _nolabel_cache_key(install, owner, repo, number),
        b"cleared",
        ex=NOINTENT_CLEARED_TTL,
    )


def _pr_eval_lock_key(install: str, owner: str, repo: str, number: int) -> str:
    return f"{PR_EVAL_LOCK_PREFIX}{install}:{owner}/{repo}#{number}"


async def acquire_pr_eval_lock(
    install: str, owner: str, repo: str, number: int
) -> Optional[str]:
    """Try to acquire the per-PR evaluation lock.

    Returns a token on success, or None if another worker holds it.
    The token must be passed to release_pr_eval_lock to release the lock.
    """
    token = uuid.uuid4().hex
    acquired = await redis_bot.set(
        _pr_eval_lock_key(install, owner, repo, number),
        token,
        nx=True,
        ex=PR_EVAL_LOCK_TTL_SEC,
    )
    return token if acquired else None


async def release_pr_eval_lock(
    install: str, owner: str, repo: str, number: int, token: str
) -> None:
    """Release the per-PR evaluation lock iff this worker still owns it."""
    await redis_bot.eval(  # type: ignore[no-untyped-call]
        _RELEASE_PR_EVAL_LOCK_LUA,
        1,
        _pr_eval_lock_key(install, owner, repo, number),
        token,
    )


def _merge_cooldown_key(install: str, owner: str, repo: str, number: int) -> str:
    return f"{MERGE_COOLDOWN_PREFIX}{install}:{owner}/{repo}#{number}"


async def set_merge_cooldown(
    install: str, owner: str, repo: str, number: int, head_sha: str
) -> None:
    """Prevent a PR from immediately re-entering the merge queue after a timeout.

    The cooldown is scoped to ``head_sha``: a new push invalidates it so the
    user can always force a retry by pushing a fix.
    """
    await redis_bot.set(
        _merge_cooldown_key(install, owner, repo, number),
        head_sha.encode(),
        ex=MERGE_COOLDOWN_TTL,
    )


async def check_merge_cooldown(
    install: str, owner: str, repo: str, number: int, head_sha: str
) -> bool:
    """Return True if this PR was recently ejected from the merge queue on ``head_sha``.

    A cooldown set on a different SHA is considered stale and ignored.
    """
    stored = await redis_bot.get(_merge_cooldown_key(install, owner, repo, number))
    return stored is not None and stored == head_sha.encode()


async def clear_merge_cooldown(
    install: str, owner: str, repo: str, number: int
) -> None:
    """Remove any active merge cooldown for this PR."""
    await redis_bot.delete(_merge_cooldown_key(install, owner, repo, number))


def _requeue_attempts_key(
    install: str, owner: str, repo: str, number: int, reason: str
) -> str:
    return f"{REQUEUE_ATTEMPTS_PREFIX}{install}:{owner}/{repo}#{number}:{reason}"


async def increment_requeue_attempts(
    install: str, owner: str, repo: str, number: int, reason: str
) -> int:
    """Increment and return the requeue attempt counter for a PR.

    The counter auto-expires after REQUEUE_ATTEMPTS_TTL seconds so that
    the PR gets a fresh set of attempts if the GitHub state eventually resolves.
    """
    key = _requeue_attempts_key(install, owner, repo, number, reason)
    count = await redis_bot.incr(key)
    await redis_bot.expire(key, REQUEUE_ATTEMPTS_TTL)
    return int(count)


def _status_dedup_key(install: str, owner: str, repo: str, number: int) -> str:
    return f"{STATUS_DEDUP_PREFIX}{install}:{owner}/{repo}#{number}"


async def check_status_dedup(
    install: str, owner: str, repo: str, number: int, msg: str
) -> bool:
    """Return True if this exact status was already posted recently."""
    cached = await redis_bot.get(_status_dedup_key(install, owner, repo, number))
    return cached == msg.encode()


async def set_status_dedup(
    install: str, owner: str, repo: str, number: int, msg: str
) -> None:
    """Record that this status was posted for dedup purposes."""
    await redis_bot.set(
        _status_dedup_key(install, owner, repo, number),
        msg.encode(),
        ex=STATUS_DEDUP_TTL,
    )


RETRY_RATE_SECONDS = 2

# Maximum number of PRs to enqueue in a single status_event when the event
# comes from a fork (len(refs)==0). Prevents 100+ PR bulk enqueue floods.
MAX_BULK_ENQUEUE = 50

# These webhook actions do not affect mergeability or queue position, so
# re-fetching the full PR state would only burn API quota.
NON_ACTIONABLE_PULL_REQUEST_ACTIONS: frozenset[str] = frozenset(
    {
        "assigned",
        "unassigned",
        "locked",
        "unlocked",
        "milestoned",
        "demilestoned",
    }
)
NON_ACTIONABLE_PULL_REQUEST_REVIEW_ACTIONS: frozenset[str] = frozenset({"edited"})
NON_ACTIONABLE_CHECK_RUN_ACTIONS: frozenset[str] = frozenset({"requested_action"})


def installation_id_from_queue(queue_name: str) -> str:
    """
    Extract the installation id from the queue names

    On restart we only have queue names, so we need to extract installation ids from the names.

    webhook:848733 -> 848733
    merge_queue:848733.chdsbd/kodiak/master -> 848733
    """
    return queue_name.partition(":")[2].partition(".")[0]


def _decode_redis_text(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value


async def get_registered_merge_queue_names() -> set[str]:
    queue_names_raw = await redis_bot.smembers(MERGE_QUEUE_NAMES)
    queue_names = {_decode_redis_text(name) for name in queue_names_raw}

    async for registry_key_raw in redis_bot.scan_iter(
        match=f"{MERGE_QUEUE_BY_INSTALL_PREFIX}*"
    ):
        registry_key = _decode_redis_text(registry_key_raw)
        members_raw = await redis_bot.smembers(registry_key)
        queue_names.update(_decode_redis_text(name) for name in members_raw)

    return queue_names


class WebhookQueueProtocol(Protocol):
    async def enqueue(self, *, event: WebhookEvent) -> None: ...

    async def enqueue_for_repo(
        self, *, event: WebhookEvent, first: bool
    ) -> int | None: ...


async def pr_event(pr: PullRequestEvent) -> list[WebhookEvent]:
    """
    Trigger evaluation of modified PR.
    """
    if pr.action in NON_ACTIONABLE_PULL_REQUEST_ACTIONS:
        return []
    # Invalidate the no-intent cache when labels or native auto-merge state
    # changes so that the next evaluation performs the full GitHub API check.
    if pr.action in (
        "labeled",
        "unlabeled",
        "auto_merge_enabled",
        "auto_merge_disabled",
    ):
        await clear_nolabel_cache(
            install=str(pr.installation.id),
            owner=pr.repository.owner.login,
            repo=pr.repository.name,
            number=pr.number,
        )
    event = WebhookEvent(
        repo_owner=pr.repository.owner.login,
        repo_name=pr.repository.name,
        pull_request_number=pr.number,
        target_name=pr.pull_request.base.ref,
        installation_id=str(pr.installation.id),
        head_sha=pr.pull_request.head.sha if pr.pull_request.head is not None else None,
        action=pr.action,
    )
    # Proactively clean up queues when a PR is closed (merged or not).
    # The evaluation will still run (for branch deletion, final status, etc.)
    # but the merge queue slot is freed immediately.
    if pr.action == "closed":
        await redis_bot.zrem(get_merge_queue_name(event), event.merge_queue_member())
        target_key = event.get_merge_target_queue_name()
        current_target = await redis_bot.get(target_key)
        if current_target == event.merge_queue_member().encode():
            await redis_bot.delete(target_key)
            await redis_bot.delete(target_key + ":time")
    return [event]


def check_run(check_run_event: CheckRunEvent) -> list[WebhookEvent]:
    """
    Trigger evaluation of all PRs included in check run.
    """
    if check_run_event.action in NON_ACTIONABLE_CHECK_RUN_ACTIONS:
        return []
    # Prevent an infinite loop when we update our own check run.
    # Prefer matching by app_id (robust) with a name fallback.
    if (
        check_run_event.check_run.app is not None
        and str(check_run_event.check_run.app.id) == conf.GITHUB_APP_ID
    ) or check_run_event.check_run.name == queries.CHECK_RUN_NAME:
        return []
    events = []
    for pr in check_run_event.check_run.pull_requests:
        # filter out pull requests for other repositories
        if pr.base.repo.id != check_run_event.repository.id:
            continue
        if pr.draft:
            continue
        events.append(
            WebhookEvent(
                repo_owner=check_run_event.repository.owner.login,
                repo_name=check_run_event.repository.name,
                pull_request_number=pr.number,
                target_name=pr.base.ref,
                installation_id=str(check_run_event.installation.id),
                head_sha=check_run_event.check_run.head_sha,
            )
        )
    return events


def find_branch_names_latest(sha: str, branches: list[Branch]) -> list[str]:
    """
    from the docs:
        The "branches" key is "an array of branch objects containing the status'
        SHA. Each branch contains the given SHA, but the SHA may or may not be
        the head of the branch. The array includes a maximum of 10 branches.""
    https://developer.github.com/v3/activity/events/types/#statusevent

    NOTE(chdsbd): only take branches with commit at branch head to reduce
    potential number of api requests we need to make.
    """
    return [branch.name for branch in branches if branch.commit.sha == sha]


async def status_event(status_event: StatusEvent) -> list[WebhookEvent]:
    """
    Trigger evaluation of all PRs associated with the status event commit SHA.
    """
    owner = status_event.repository.owner.login
    repo = status_event.repository.name
    installation_id = str(status_event.installation.id)
    log = logger.bind(owner=owner, repo=repo, install=installation_id)

    refs = find_branch_names_latest(
        sha=status_event.sha, branches=status_event.branches
    )

    async with Client(
        owner=owner, repo=repo, installation_id=installation_id
    ) as api_client:
        fork_path = len(refs) == 0
        if fork_path:
            # when a pull request is from a fork the status event will not have
            # any `branches`, so to be able to trigger evaluation of the PR, we
            # fetch all pull requests.
            #
            # I think we could optimize this by selecting only the fork PRs, but
            # I worry that we might miss some events where `branches` is empty,
            # but not because of a fork.
            pr_results = [await api_client.get_open_pull_requests()]
            log.info("could not find refs for status_event")
        else:
            pr_requests = [
                api_client.get_open_pull_requests(head=f"{owner}:{ref}") for ref in refs
            ]
            pr_results = await asyncio.gather(*pr_requests)

        all_events: set[WebhookEvent] = set()
        for prs in pr_results:
            if prs is None:
                continue
            for pr in prs:
                all_events.add(
                    WebhookEvent(
                        repo_owner=owner,
                        repo_name=repo,
                        pull_request_number=pr.number,
                        target_name=pr.base.ref,
                        installation_id=str(installation_id),
                        head_sha=status_event.sha,
                    )
                )

        pr_count = len(all_events)
        log.info(
            "status_event_enqueue",
            pr_count=pr_count,
            fork_path=fork_path,
        )
        if fork_path and pr_count > MAX_BULK_ENQUEUE:
            log.warning(
                "status_event_bulk_enqueue_capped",
                total=pr_count,
                cap=MAX_BULK_ENQUEUE,
            )

        queued_events = []
        for enqueued, event in enumerate(all_events):
            if fork_path and enqueued >= MAX_BULK_ENQUEUE:
                break
            queued_events.append(event)
        return queued_events


async def pr_review(
    review: PullRequestReviewEvent | PullRequestReviewThreadEvent,
) -> list[WebhookEvent]:
    """
    Trigger evaluation of the modified PR.
    """
    # Prevent an infinite loop when Kodiak's own approval triggers a
    # pull_request_review webhook, which would re-evaluate the PR and
    # potentially approve it again.
    if (
        isinstance(review, PullRequestReviewEvent)
        and review.action in NON_ACTIONABLE_PULL_REQUEST_REVIEW_ACTIONS
    ):
        return []
    if (
        isinstance(review, PullRequestReviewEvent)
        and review.review is not None
        and review.review.user is not None
        and review.review.user.login == app_identity.bot_login()
    ):
        return []
    return [
        WebhookEvent(
            repo_owner=review.repository.owner.login,
            repo_name=review.repository.name,
            pull_request_number=review.pull_request.number,
            target_name=review.pull_request.base.ref,
            installation_id=str(review.installation.id),
            head_sha=review.pull_request.head.sha
            if review.pull_request.head is not None
            else None,
        )
    ]


def get_branch_name(raw_ref: str) -> str | None:
    """
    Extract the branch name from the ref
    """
    if raw_ref.startswith("refs/heads/"):
        return raw_ref.split("refs/heads/", 1)[1]
    return None


async def push(push_event: PushEvent) -> list[WebhookEvent]:
    """
    Trigger evaluation of PRs that depend on the pushed branch.
    """
    owner = push_event.repository.owner.login
    repo = push_event.repository.name
    installation_id = str(push_event.installation.id)
    branch_name = get_branch_name(push_event.ref)
    log = logger.bind(ref=push_event.ref, branch_name=branch_name)
    if branch_name is None:
        log.info("could not extract branch name from ref")
        return []
    async with Client(
        owner=owner, repo=repo, installation_id=installation_id
    ) as api_client:
        # find all the PRs that depend on the branch affected by this push and
        # queue them for evaluation.
        # Any PR that has a base ref matching our event ref is dependent.
        prs = await api_client.get_open_pull_requests(base=branch_name)
        if prs is None:
            log.info("api call to find pull requests failed")
            return []
        events = []
        for pr in prs:
            events.append(
                WebhookEvent(
                    repo_owner=owner,
                    repo_name=repo,
                    pull_request_number=pr.number,
                    target_name=pr.base.ref,
                    installation_id=installation_id,
                )
            )
        return events


def compress_payload(data: dict[str, object]) -> bytes:
    cctx = zstd.ZstdCompressor()
    return cctx.compress(json.dumps(data).encode())


async def handle_webhook_event(
    queue: WebhookQueueProtocol,
    event_name: str,
    payload: dict[str, object],
    delivery_id: str | None = None,
) -> None:
    webhook_summary = summarize_webhook_payload(event_name=event_name, payload=payload)
    installation_id = webhook_summary.get("installation_id")
    owner = webhook_summary.get("owner")
    repo = webhook_summary.get("repo")
    pr_number = webhook_summary.get("pull_request_number")
    action = webhook_summary.get("action")

    log = logger.bind(
        event_name=event_name,
        delivery_id=delivery_id,
        install=installation_id,
        owner=owner,
        repo=repo,
        number=pr_number,
    )

    await record_debug_event(
        stage="fanout",
        event_type="webhook_processing_started",
        message="Started processing webhook",
        installation_id=installation_id,
        owner=owner,
        repo=repo,
        pr_number=pr_number,
        delivery_id=delivery_id,
        event_name=event_name,
        action=action,
        details=webhook_summary,
    )

    usage_reported = False
    if conf.USAGE_REPORTING and event_name in conf.USAGE_REPORTING_EVENTS:
        # store events in Redis for dequeue by web api job.
        #
        # We limit the queue length to ensure that if the dequeue job fails, we
        # won't overload Redis.
        await redis_web_api.rpush(
            b"kodiak:webhook_event",
            compress_payload(dict(event_name=event_name, payload=payload)),
        )
        await redis_web_api.ltrim(
            b"kodiak:webhook_event", 0, conf.USAGE_REPORTING_QUEUE_LENGTH
        )
        log = log.bind(usage_reported=True)
        usage_reported = True
        await record_debug_event(
            stage="fanout",
            event_type="usage_reporting_enqueued",
            message="Stored webhook for usage reporting",
            installation_id=installation_id,
            owner=owner,
            repo=repo,
            pr_number=pr_number,
            delivery_id=delivery_id,
            event_name=event_name,
            action=action,
        )

    generated_events: list[WebhookEvent] = []
    event_parsed = True
    if event_name == "check_run":
        generated_events = check_run(CheckRunEvent.parse_obj(payload))
    elif event_name == "pull_request":
        generated_events = await pr_event(PullRequestEvent.parse_obj(payload))
    elif event_name == "pull_request_review":
        generated_events = await pr_review(PullRequestReviewEvent.parse_obj(payload))
    elif event_name == "pull_request_review_thread":
        generated_events = await pr_review(
            PullRequestReviewThreadEvent.parse_obj(payload)
        )
    elif event_name == "push":
        generated_events = await push(PushEvent.parse_obj(payload))
    elif event_name == "status":
        generated_events = await status_event(StatusEvent.parse_obj(payload))
    else:
        log = log.bind(event_parsed=False)
        event_parsed = False

    for event in generated_events:
        if action in ("labeled", "unlabeled"):
            # Remove any stale entry so the labeled/unlabeled event isn't
            # silently dropped by the NX-dedup in enqueue's ZADD.
            await redis_bot.zrem(
                get_webhook_queue_name(event), event.webhook_queue_member()
            )
        await queue.enqueue(event=event)

    await record_debug_event(
        stage="fanout",
        event_type="webhook_processed",
        message="Finished processing webhook",
        installation_id=installation_id,
        owner=owner,
        repo=repo,
        pr_number=pr_number,
        delivery_id=delivery_id,
        event_name=event_name,
        action=action,
        details={
            "event_parsed": event_parsed,
            "fanout_count": len(generated_events),
            "generated_pr_events": [
                {
                    "owner": event.repo_owner,
                    "repo": event.repo_name,
                    "pr_number": event.pull_request_number,
                    "target_branch": event.target_name,
                }
                for event in generated_events
            ],
            "usage_reported": usage_reported,
        },
    )

    log.info("webhook_event_handled", fanout_count=len(generated_events))


class WebhookEvent(BaseModel):
    repo_owner: str
    repo_name: str
    pull_request_number: int
    installation_id: str
    target_name: str
    head_sha: Optional[str] = None
    # Webhook action (e.g. "closed", "opened").  Used to fast-track close
    # events past the merge-queue and nolabel-cache skip optimizations.
    action: Optional[str] = None

    def webhook_queue_member(self) -> str:
        return super().json(exclude_none=True)

    def get_merge_queue_name(self) -> str:
        return get_merge_queue_name(self)

    def get_merge_target_queue_name(self) -> str:
        return self.get_merge_queue_name() + ":target"

    def get_webhook_queue_name(self) -> str:
        return get_webhook_queue_name(self)

    def get_latest_webhook_head_sha_key(self) -> str:
        return (
            f"{WEBHOOK_LATEST_HEAD_SHA_PREFIX}{self.installation_id}:"
            f"{self.repo_owner}/{self.repo_name}#{self.pull_request_number}"
        )

    def merge_queue_member(self) -> str:
        return super().json(exclude={"head_sha", "action"}, exclude_none=True)

    def __hash__(self) -> int:
        return (
            hash(self.repo_owner)
            + hash(self.repo_name)
            + hash(self.pull_request_number)
            + hash(self.installation_id)
        )


async def bzpopmin_with_timeout(queue_name: str) -> Tuple[bytes, bytes, float] | None:
    return await redis_bot.bzpopmin(
        [queue_name], timeout=conf.REDIS_BLOCKING_POP_TIMEOUT_SEC
    )


async def process_webhook_event(
    webhook_queue: RedisWebhookQueue,
    queue_name: str,
    log: structlog.BoundLogger,
) -> None:
    log.info("block for new webhook event")
    webhook_event_json = await bzpopmin_with_timeout(queue_name)
    if webhook_event_json is None:
        return
    log.info("parsing webhook event")
    webhook_event = WebhookEvent.parse_raw(webhook_event_json[1])
    is_active_merging = (
        await redis_bot.get(webhook_event.get_merge_target_queue_name())
        == webhook_event.merge_queue_member().encode()
    )
    await record_debug_event(
        stage="evaluation_queue",
        event_type="pr_evaluation_dequeued",
        message="Dequeued PR evaluation event",
        installation_id=webhook_event.installation_id,
        owner=webhook_event.repo_owner,
        repo=webhook_event.repo_name,
        pr_number=webhook_event.pull_request_number,
        queue_name=queue_name,
        details={
            "target_branch": webhook_event.target_name,
            "score": webhook_event_json[2],
            "is_active_merging": is_active_merging,
            "head_sha": webhook_event.head_sha,
        },
    )

    if webhook_event.head_sha is not None:
        latest_head_sha = await redis_bot.get(
            webhook_event.get_latest_webhook_head_sha_key()
        )
        latest_head_sha_str = (
            _decode_redis_text(latest_head_sha) if latest_head_sha else None
        )
        if (
            latest_head_sha_str is not None
            and latest_head_sha_str != webhook_event.head_sha
        ):
            log.info(
                "skip evaluation for stale webhook event",
                number=webhook_event.pull_request_number,
                queued_head_sha=webhook_event.head_sha,
                latest_head_sha=latest_head_sha_str,
            )
            await record_debug_event(
                stage="evaluation",
                event_type="pr_evaluation_skipped_stale_sha",
                message="Skipped evaluation because a newer head SHA was already queued",
                installation_id=webhook_event.installation_id,
                owner=webhook_event.repo_owner,
                repo=webhook_event.repo_name,
                pr_number=webhook_event.pull_request_number,
                queue_name=queue_name,
                details={
                    "target_branch": webhook_event.target_name,
                    "queued_head_sha": webhook_event.head_sha,
                    "latest_head_sha": latest_head_sha_str,
                },
            )
            return

    # Skip the full evaluation cycle (GitHub API call + mergeable check +
    # redundant approve/queue) if this PR is already sitting in the merge
    # queue waiting its turn.  The repo queue consumer will handle it when
    # it reaches the head of the queue.  We still allow evaluations for
    # the PR that is *actively* being merged so that status updates and
    # check-run changes are processed.
    # We also allow "closed" events through so that a closed PR is
    # promptly removed from the merge queue instead of waiting until it
    # reaches the head.
    is_close_event = webhook_event.action == "closed"
    if not is_active_merging and not is_close_event:
        merge_queue_score = await redis_bot.zscore(
            webhook_event.get_merge_queue_name(), webhook_event.merge_queue_member()
        )
        if merge_queue_score is not None:
            log.info(
                "skip evaluation for already-enqueued PR",
                number=webhook_event.pull_request_number,
            )
            await record_debug_event(
                stage="evaluation",
                event_type="pr_evaluation_skipped",
                message="Skipped evaluation because PR is already in the merge queue",
                installation_id=webhook_event.installation_id,
                owner=webhook_event.repo_owner,
                repo=webhook_event.repo_name,
                pr_number=webhook_event.pull_request_number,
                queue_name=queue_name,
                details={
                    "target_branch": webhook_event.target_name,
                    "merge_queue_name": webhook_event.get_merge_queue_name(),
                    "merge_queue_score": merge_queue_score,
                },
            )
            return

    async def dequeue() -> None:
        await redis_bot.zrem(
            webhook_event.get_merge_queue_name(), webhook_event.merge_queue_member()
        )

    async def requeue(*, delay_sec: float = 0) -> None:
        await redis_bot.zadd(
            webhook_event.get_webhook_queue_name(),
            {webhook_event.webhook_queue_member(): time.time() + delay_sec},
            nx=True,
        )

    async def queue_for_merge(*, first: bool) -> Optional[int]:
        return await webhook_queue.enqueue_for_repo(event=webhook_event, first=first)

    # Skip the full evaluation cycle if we've previously cached that this PR
    # lacks merge/update intent. The cache is invalidated by label changes and
    # pull_request auto-merge enable/disable actions, and synthetic refresh /
    # startup events bypass it because they have no webhook action.
    # Close events always bypass this cache so that closed PRs are promptly
    # dequeued.
    if (
        not is_active_merging
        and not is_close_event
        and webhook_event.action is not None
        and await check_nolabel_cache(
            install=webhook_event.installation_id,
            owner=webhook_event.repo_owner,
            repo=webhook_event.repo_name,
            number=webhook_event.pull_request_number,
        )
    ):
        log.info(
            "skip evaluation for cached no-intent",
            number=webhook_event.pull_request_number,
        )
        await record_debug_event(
            stage="evaluation",
            event_type="pr_evaluation_skipped_nolabel_cache",
            message="Skipped evaluation because PR was cached as missing merge/update intent",
            installation_id=webhook_event.installation_id,
            owner=webhook_event.repo_owner,
            repo=webhook_event.repo_name,
            pr_number=webhook_event.pull_request_number,
            queue_name=queue_name,
            details={"target_branch": webhook_event.target_name},
        )
        return

    # Serialise concurrent webhook evaluations of the same PR.  Without this
    # guard, a burst of events lands on parallel consumers and each reads a
    # pre-approval / pre-update snapshot, producing duplicate approvals or
    # branch updates.
    lock_token = await acquire_pr_eval_lock(
        webhook_event.installation_id,
        webhook_event.repo_owner,
        webhook_event.repo_name,
        webhook_event.pull_request_number,
    )
    if lock_token is None:
        log.info(
            "skip evaluation: another worker is evaluating this PR; requeuing",
            number=webhook_event.pull_request_number,
        )
        await record_debug_event(
            stage="evaluation",
            event_type="pr_evaluation_skipped_locked",
            message=(
                "Skipped evaluation because another worker holds the PR "
                "evaluation lock; requeued for retry"
            ),
            installation_id=webhook_event.installation_id,
            owner=webhook_event.repo_owner,
            repo=webhook_event.repo_name,
            pr_number=webhook_event.pull_request_number,
            queue_name=queue_name,
            details={"target_branch": webhook_event.target_name},
        )
        await redis_bot.zadd(
            webhook_event.get_webhook_queue_name(),
            {
                webhook_event.webhook_queue_member(): (
                    time.time() + PR_EVAL_LOCK_CONTENTION_REQUEUE_DELAY_SEC
                )
            },
            nx=True,
        )
        return

    log.info("evaluate pr for webhook event")
    await record_debug_event(
        stage="evaluation",
        event_type="pr_evaluation_started",
        message="Started PR evaluation from webhook queue",
        installation_id=webhook_event.installation_id,
        owner=webhook_event.repo_owner,
        repo=webhook_event.repo_name,
        pr_number=webhook_event.pull_request_number,
        queue_name=queue_name,
        details={
            "target_branch": webhook_event.target_name,
            "is_active_merging": is_active_merging,
        },
    )
    try:
        await evaluate_pr(
            install=webhook_event.installation_id,
            owner=webhook_event.repo_owner,
            repo=webhook_event.repo_name,
            number=webhook_event.pull_request_number,
            merging=False,
            dequeue_callback=dequeue,
            requeue_callback=requeue,
            queue_for_merge_callback=queue_for_merge,
            is_active_merging=is_active_merging,
            log=log,
        )
    finally:
        await release_pr_eval_lock(
            webhook_event.installation_id,
            webhook_event.repo_owner,
            webhook_event.repo_name,
            webhook_event.pull_request_number,
            lock_token,
        )
    await record_debug_event(
        stage="evaluation",
        event_type="pr_evaluation_finished",
        message="Finished PR evaluation from webhook queue",
        installation_id=webhook_event.installation_id,
        owner=webhook_event.repo_owner,
        repo=webhook_event.repo_name,
        pr_number=webhook_event.pull_request_number,
        queue_name=queue_name,
        details={"target_branch": webhook_event.target_name},
    )


async def webhook_event_consumer(
    *, webhook_queue: RedisWebhookQueue, queue_name: str
) -> typing.NoReturn:
    """
    Worker to process incoming webhook events from redis

    1. process mergeability information and update github check status for pr
    2. enqueue pr into repo queue for merging, if mergeability passed
    """

    # We need to define a custom Hub so that we can set the scope correctly.
    # Without creating a new hub we end up overwriting the scopes of other
    # consumers.
    #
    # https://github.com/getsentry/sentry-python/issues/147#issuecomment-432959196
    # https://github.com/getsentry/sentry-python/blob/0da369f839ee2c383659c91ea8858abcac04b869/sentry_sdk/integrations/aiohttp.py#L80-L83
    # https://github.com/getsentry/sentry-python/blob/464ca8dda09155fcc43dfbb6fa09cf00313bf5b8/sentry_sdk/integrations/asgi.py#L90-L113
    with sentry_sdk.Hub(sentry_sdk.Hub.current) as hub:
        with hub.configure_scope() as scope:
            scope.set_tag("queue", queue_name)
            scope.set_tag("installation", installation_id_from_queue(queue_name))
        log = logger.bind(
            queue=queue_name, install=installation_id_from_queue(queue_name)
        )
        log.info("start webhook event consumer")
        while True:
            await process_webhook_event(webhook_queue, queue_name, log)


async def process_repo_queue(log: structlog.BoundLogger, queue_name: str) -> None:
    log.info("block for new repo event")
    result = await bzpopmin_with_timeout(queue_name)
    if result is None:
        return
    _key, value, score = result
    webhook_event = WebhookEvent.parse_raw(value)
    target_name = webhook_event.get_merge_target_queue_name()
    # TTL bounds how long an orphaned marker can block the repo queue if the
    # worker crashes or is restarted between setting the marker and the
    # try/finally cleanup below.
    target_marker_ttl = max(conf.MERGE_QUEUE_POLL_TIMEOUT_SEC * 2, 600)
    # mark this PR as being merged currently. we check this elsewhere to set proper status codes
    await redis_bot.set(
        target_name, webhook_event.merge_queue_member(), ex=target_marker_ttl
    )
    await redis_bot.set(target_name + ":time", str(score), ex=target_marker_ttl)
    await record_debug_event(
        stage="merge_queue",
        event_type="merge_started",
        message="Started processing PR at the head of the merge queue",
        installation_id=webhook_event.installation_id,
        owner=webhook_event.repo_owner,
        repo=webhook_event.repo_name,
        pr_number=webhook_event.pull_request_number,
        queue_name=queue_name,
        details={
            "target_branch": webhook_event.target_name,
            "target_key": target_name,
            "enqueued_at": score,
        },
    )

    async def dequeue() -> None:
        await redis_bot.zrem(
            webhook_event.get_merge_queue_name(), webhook_event.merge_queue_member()
        )

    async def requeue(*, delay_sec: float = 0) -> None:
        await redis_bot.zadd(
            webhook_event.get_webhook_queue_name(),
            {webhook_event.webhook_queue_member(): time.time() + delay_sec},
            nx=True,
        )

    async def queue_for_merge(*, first: bool) -> Optional[int]:
        raise NotImplementedError

    log.info("evaluate PR for merging")
    try:
        await evaluate_pr(
            install=webhook_event.installation_id,
            owner=webhook_event.repo_owner,
            repo=webhook_event.repo_name,
            number=webhook_event.pull_request_number,
            dequeue_callback=dequeue,
            requeue_callback=requeue,
            merging=True,
            is_active_merging=False,
            queue_for_merge_callback=queue_for_merge,
            log=log,
        )
    finally:
        # Always clear the target marker so an unhandled exception cannot
        # leave the repo queue permanently blocked behind a dead PR.
        log.info("remove target marker", target_name=target_name)
        await redis_bot.delete(target_name)
        await redis_bot.delete(target_name + ":time")
    await record_debug_event(
        stage="merge_queue",
        event_type="merge_finished",
        message="Finished processing PR at the head of the merge queue",
        installation_id=webhook_event.installation_id,
        owner=webhook_event.repo_owner,
        repo=webhook_event.repo_name,
        pr_number=webhook_event.pull_request_number,
        queue_name=queue_name,
        details={
            "target_branch": webhook_event.target_name,
            "target_key": target_name,
        },
    )


async def repo_queue_consumer(*, queue_name: str) -> typing.NoReturn:
    """
    Worker for a repo given by :queue_name:

    Pull webhook events off redis queue and process for mergeability.

    We only run one of these per repo as we can only merge one PR at a time
    to be efficient. This also alleviates the need of locks.
    """
    installation = installation_id_from_queue(queue_name)
    with sentry_sdk.Hub(sentry_sdk.Hub.current) as hub:
        with hub.configure_scope() as scope:
            scope.set_tag("queue", queue_name)
            scope.set_tag("installation", installation)
        log = logger.bind(queue=queue_name, install=installation)
        log.info("start repo_consumer")
        while True:
            await process_repo_queue(log, queue_name)


T = typing.TypeVar("T")


def find_position(x: typing.Iterable[T], v: T) -> typing.Optional[int]:
    for index, item in enumerate(x):
        if item == v:
            return index
    return None


ONE_DAY = int(timedelta(days=1).total_seconds())


@dataclass(frozen=True)
class TaskMeta:
    kind: Literal["repo", "webhook"]
    queue_name: str


class RedisWebhookQueue:
    def __init__(self) -> None:
        self.worker_tasks: MutableMapping[
            str, tuple[Task[NoReturn], Literal["repo", "webhook"]]
        ] = {}  # type: ignore [assignment]

    async def create(self) -> None:
        # restart repo workers
        merge_queues, webhook_queues = await asyncio.gather(
            get_registered_merge_queue_names(),
            redis_bot.smembers(WEBHOOK_QUEUE_NAMES),
        )
        for merge_result in merge_queues:
            self.start_repo_worker(queue_name=merge_result)

        for webhook_result in webhook_queues:
            queue_name = webhook_result.decode()
            self.start_webhook_worker(queue_name=queue_name)

    def start_webhook_worker(self, *, queue_name: str) -> None:
        logger.info(
            "start_webhook_worker",
            queue_name=queue_name,
            concurrency=conf.WEBHOOK_CONSUMER_CONCURRENCY,
        )
        for i in range(conf.WEBHOOK_CONSUMER_CONCURRENCY):
            worker_key = f"{queue_name}:worker:{i}"
            self._start_worker(
                worker_key,
                "webhook",
                webhook_event_consumer(webhook_queue=self, queue_name=queue_name),
            )

    def start_repo_worker(self, *, queue_name: str) -> None:
        self._start_worker(
            queue_name,
            "repo",
            repo_queue_consumer(
                queue_name=queue_name,
            ),
        )

    def _start_worker(
        self,
        key: str,
        kind: Literal["repo", "webhook"],
        fut: typing.Coroutine[None, None, NoReturn],
    ) -> None:
        log = logger.bind(queue_name=key, kind=kind)
        worker_task_result = self.worker_tasks.get(key)
        if worker_task_result is not None:
            worker_task, _task_kind = worker_task_result
            if not worker_task.done():
                fut.close()
                return
            log.warning("worker_task_failed")
            # task failed. record result and restart
            exception = worker_task.exception()
            log.warning("worker_task_exception", excep=exception)
            sentry_sdk.capture_exception(exception)
        log.info("creating task for queue")
        # create new task for queue
        self.worker_tasks[key] = (asyncio.create_task(fut), kind)

    async def enqueue(self, *, event: WebhookEvent) -> None:
        """
        add :event: to webhook queue
        """
        queue_name = get_webhook_queue_name(event)
        if event.head_sha is not None:
            await redis_bot.set(
                event.get_latest_webhook_head_sha_key(),
                event.head_sha,
                ex=ONE_DAY,
            )
        async with redis_bot.pipeline(transaction=True) as pipe:
            pipe.sadd(WEBHOOK_QUEUE_NAMES, queue_name)
            pipe.zadd(queue_name, {event.webhook_queue_member(): time.time()}, nx=True)
            pipe.zcard(queue_name)
            results = await pipe.execute()
        inserted = bool(results[1]) if len(results) > 1 else None
        queue_depth = results[-1] if results else None
        log = logger.bind(
            owner=event.repo_owner,
            repo=event.repo_name,
            number=event.pull_request_number,
            install=event.installation_id,
        )
        log.info("enqueue webhook event", queue_depth=queue_depth, inserted=inserted)
        await record_debug_event(
            stage="evaluation_queue",
            event_type="pr_evaluation_enqueued",
            message="Queued PR for evaluation",
            installation_id=event.installation_id,
            owner=event.repo_owner,
            repo=event.repo_name,
            pr_number=event.pull_request_number,
            queue_name=queue_name,
            details={
                "target_branch": event.target_name,
                "head_sha": event.head_sha,
                "queue_depth": queue_depth,
                "inserted": inserted,
            },
        )
        self.start_webhook_worker(queue_name=queue_name)

    async def enqueue_for_repo(
        self, *, event: WebhookEvent, first: bool
    ) -> Optional[int]:
        """
        1. get the corresponding repo queue for event
        2. add key to MERGE_QUEUE_NAMES so on restart we can recreate the
        worker for the queue.
        3. add event
        4. start worker (will create new worker if one does not exist)

        returns position of event in queue
        """
        queue_name = get_merge_queue_name(event)
        async with redis_bot.pipeline(transaction=True) as pipe:
            merge_queues_by_install = (
                f"{MERGE_QUEUE_BY_INSTALL_PREFIX}{event.installation_id}"
            )
            pipe.sadd(MERGE_QUEUE_NAMES, queue_name)
            pipe.sadd(merge_queues_by_install, queue_name)
            pipe.expire(merge_queues_by_install, time=ONE_DAY)
            if first:
                # place at front of queue. To allow us to always place this PR at
                # the front, we should not pass only_if_not_exists.
                pipe.zadd(queue_name, {event.merge_queue_member(): 1.0})
            else:
                # use only_if_not_exists to prevent changing queue positions on new
                # webhook events.
                pipe.zadd(
                    queue_name, {event.merge_queue_member(): time.time()}, nx=True
                )
            pipe.zrange(queue_name, 0, 1000, withscores=True)
            results = await pipe.execute()
        log = logger.bind(
            owner=event.repo_owner,
            repo=event.repo_name,
            number=event.pull_request_number,
            install=event.installation_id,
        )

        zrange_results = results[-1]  # type: list[tuple[bytes, float]]
        log.info("enqueue repo event", queue_depth=len(zrange_results))
        self.start_repo_worker(queue_name=queue_name)
        kvs = sorted(
            ((key, value) for key, value in zrange_results), key=lambda x: x[1]
        )
        position = find_position(
            (key for key, value in kvs), event.merge_queue_member().encode()
        )
        inserted = bool(results[3]) if len(results) > 3 else None
        await record_debug_event(
            stage="merge_queue",
            event_type="pr_added_to_merge_queue",
            message="Queued PR for merge",
            installation_id=event.installation_id,
            owner=event.repo_owner,
            repo=event.repo_name,
            pr_number=event.pull_request_number,
            queue_name=queue_name,
            details={
                "target_branch": event.target_name,
                "head_sha": event.head_sha,
                "queue_depth": len(zrange_results),
                "inserted": inserted,
                "position": position,
                "priority": first,
            },
        )
        return position

    def all_tasks(self) -> Iterator[tuple[TaskMeta, Task[NoReturn]]]:
        for worker_key, (task, task_kind) in self.worker_tasks.items():
            # For webhook workers the key is "queue_name:worker:N",
            # for repo workers it's just the queue name.
            if task_kind == "webhook":
                # Strip ":worker:N" suffix to recover the original queue name.
                queue_name = worker_key.rsplit(":worker:", 1)[0]
            else:
                queue_name = worker_key
            yield (TaskMeta(kind=task_kind, queue_name=queue_name), task)


def get_merge_queue_name(event: WebhookEvent) -> str:
    escaped_target = urllib.parse.quote(event.target_name)
    return f"merge_queue:{event.installation_id}.{event.repo_owner}/{event.repo_name}/{escaped_target}"


def get_webhook_queue_name(event: WebhookEvent) -> str:
    return f"webhook:{event.installation_id}"
