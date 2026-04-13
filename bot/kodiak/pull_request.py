from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping, Optional, Type

import structlog
from typing_extensions import Protocol

import kodiak.app_config as conf
from kodiak.debug_history import record_debug_event as record_debug_history_event
from kodiak.errors import (
    ApiCallException,
    GitHubApiInternalServerError,
    PollForever,
    RetryForSkippableChecks,
)
from kodiak.evaluation import POLL_REASON_UPDATING_BRANCH, mergeable
from kodiak.http import HTTPStatusError as HTTPError
from kodiak.queries import Client, EventInfoResponse

logger = structlog.get_logger()


RETRY_RATE_SECONDS = 2
POLL_RATE_SECONDS = 10


class RequeueCallback(Protocol):
    async def __call__(self, *, delay_sec: float = 0) -> None: ...


async def get_pr(
    install: str,
    owner: str,
    repo: str,
    number: int,
    dequeue_callback: Callable[[], Awaitable[None]],
    requeue_callback: RequeueCallback,
    queue_for_merge_callback: QueueForMergeCallback,
) -> Optional[PRV2]:
    log = logger.bind(install=install, owner=owner, repo=repo, number=number)
    async with Client(installation_id=install, owner=owner, repo=repo) as api_client:
        event = await api_client.get_event_info(pr_number=number)
        if event is None:
            log.info("failed to find event")
            return None
        return PRV2(
            event,
            install=install,
            owner=owner,
            repo=repo,
            number=number,
            dequeue_callback=dequeue_callback,
            requeue_callback=requeue_callback,
            queue_for_merge_callback=queue_for_merge_callback,
        )


@dataclass(frozen=True)
class APICallError:
    api_name: str
    http_status: str
    response_body: str


async def evaluate_pr(
    install: str,
    owner: str,
    repo: str,
    number: int,
    merging: bool,
    dequeue_callback: Callable[[], Awaitable[None]],
    requeue_callback: RequeueCallback,
    queue_for_merge_callback: QueueForMergeCallback,
    is_active_merging: bool,
    log: structlog.BoundLogger,
) -> None:
    skippable_check_timeout = 4
    api_call_retries_remaining = 5
    api_call_errors = []  # type: list[APICallError]
    poll_start_time: float | None = None
    last_poll_reason: str = ""
    cycle_count = 0
    timeout_count = 0
    # Track the head SHA at the time we last called update_branch() so we can
    # skip duplicate calls while GitHub is still processing the branch update.
    pending_update_sha: str | None = None
    start_time = time.monotonic()
    log = log.bind(owner=owner, repo=repo, number=number, merging=merging)
    await record_debug_history_event(
        stage="evaluation",
        event_type="evaluation_loop_started",
        message="Started PR evaluation loop",
        installation_id=install,
        owner=owner,
        repo=repo,
        pr_number=number,
        details={"merging": merging, "is_active_merging": is_active_merging},
    )
    while True:
        log.info("get_pr")
        try:
            pr = await asyncio.wait_for(
                get_pr(
                    install=install,
                    owner=owner,
                    repo=repo,
                    number=number,
                    dequeue_callback=dequeue_callback,
                    requeue_callback=requeue_callback,
                    queue_for_merge_callback=queue_for_merge_callback,
                ),
                timeout=conf.PR_EVALUATION_TIMEOUT_SEC,
            )
            try:
                if pr is None:
                    log.info("failed to get_pr")
                    if merging:
                        raise ApiCallException(
                            method="kodiak/get_pr",
                            http_status_code=0,
                            response=b"",
                        )
                    return
                await asyncio.wait_for(
                    mergeable(
                        api=pr,
                        subscription=pr.event.subscription,
                        config=pr.event.config,
                        config_str=pr.event.config_str,
                        config_path=pr.event.config_file_expression,
                        app_id=conf.GITHUB_APP_ID,
                        repository=pr.event.repository,
                        pull_request=pr.event.pull_request,
                        branch_protection=pr.event.branch_protection,
                        ruleset_rules=pr.event.ruleset_rules,
                        review_requests=pr.event.review_requests,
                        bot_reviews=pr.event.bot_reviews,
                        contexts=pr.event.status_contexts,
                        check_runs=pr.event.check_runs,
                        commits=pr.event.commits,
                        valid_merge_methods=pr.event.valid_merge_methods,
                        merging=merging,
                        is_active_merge=is_active_merging,
                        skippable_check_timeout=skippable_check_timeout,
                        api_call_errors=api_call_errors,
                        api_call_retries_remaining=api_call_retries_remaining,
                        head_exists=pr.event.head_exists,
                        pending_update_sha=pending_update_sha,
                    ),
                    timeout=conf.PR_EVALUATION_TIMEOUT_SEC,
                )
                duration_ms = int((time.monotonic() - start_time) * 1000)
                log.info(
                    "evaluate_pr_complete",
                    duration_ms=duration_ms,
                    cycles=cycle_count,
                )
                await record_debug_history_event(
                    stage="evaluation",
                    event_type="evaluation_loop_completed",
                    message="Completed PR evaluation loop",
                    installation_id=install,
                    owner=owner,
                    repo=repo,
                    pr_number=number,
                    details={
                        "duration_ms": duration_ms,
                        "cycles": cycle_count,
                        "merging": merging,
                    },
                )
            except RetryForSkippableChecks:
                if skippable_check_timeout > 0:
                    skippable_check_timeout -= 1
                    log.info("waiting for skippable checks to pass")
                    await record_debug_history_event(
                        stage="evaluation",
                        event_type="evaluation_retry_skippable_checks",
                        message="Retrying while waiting for skippable checks",
                        installation_id=install,
                        owner=owner,
                        repo=repo,
                        pr_number=number,
                        details={
                            "remaining_retries": skippable_check_timeout,
                            "retry_rate_seconds": RETRY_RATE_SECONDS,
                        },
                    )
                    await asyncio.sleep(RETRY_RATE_SECONDS)
                    continue
            except PollForever as e:
                cycle_count += 1
                if e.reason:
                    last_poll_reason = e.reason
                # If we just called update_branch(), record the current head
                # SHA so we can skip duplicate API calls on subsequent cycles
                # while GitHub is still processing the branch update.
                # Reset the guard once the reason changes (update propagated).
                if e.reason and e.reason.startswith(POLL_REASON_UPDATING_BRANCH):
                    if pr is not None:
                        pending_update_sha = pr.event.pull_request.latest_sha
                else:
                    pending_update_sha = None
                if poll_start_time is None:
                    poll_start_time = time.monotonic()
                elapsed_ms = int((time.monotonic() - poll_start_time) * 1000)
                log.info(
                    "polling",
                    cycle=cycle_count,
                    elapsed_ms=elapsed_ms,
                    reason=last_poll_reason,
                )
                await record_debug_history_event(
                    stage="evaluation",
                    event_type="evaluation_polling",
                    message="Polling PR for a mergeability change",
                    installation_id=install,
                    owner=owner,
                    repo=repo,
                    pr_number=number,
                    details={
                        "cycle": cycle_count,
                        "elapsed_ms": elapsed_ms,
                        "merging": merging,
                        "reason": last_poll_reason,
                    },
                )
                if (
                    merging
                    and (time.monotonic() - poll_start_time)
                    > conf.MERGE_QUEUE_POLL_TIMEOUT_SEC
                ):
                    timeout_msg = (
                        f"⚠️ Timed out waiting in merge queue ({last_poll_reason}). "
                        "Will retry on next webhook event."
                        if last_poll_reason
                        else "⚠️ Timed out waiting in merge queue. Will retry on next webhook event."
                    )
                    log.warning(
                        "merge_queue_poll_timeout",
                        elapsed_sec=int(time.monotonic() - poll_start_time),
                        cycles=cycle_count,
                        reason=last_poll_reason,
                    )
                    await record_debug_history_event(
                        stage="evaluation",
                        event_type="merge_queue_poll_timeout",
                        message="Polling timed out; dequeuing PR from merge queue",
                        installation_id=install,
                        owner=owner,
                        repo=repo,
                        pr_number=number,
                        details={
                            "elapsed_sec": int(time.monotonic() - poll_start_time),
                            "cycles": cycle_count,
                            "reason": last_poll_reason,
                        },
                    )
                    # Post a visible status so the user knows what's stuck.
                    assert pr is not None
                    await pr.set_status(timeout_msg)
                    # Dequeue from merge queue but do NOT re-queue to webhook
                    # queue. This prevents the infinite timeout/re-queue cycle
                    # where a perpetually-missing required check (e.g. a commit
                    # status that was never posted on the HEAD commit) blocks
                    # the entire merge queue.  The PR will be re-evaluated when
                    # the next natural webhook event fires (check completion,
                    # push, review, label change, etc.).
                    await dequeue_callback()
                    # Set a cooldown so the PR doesn't immediately re-enter
                    # the merge queue on the next webhook event.
                    from kodiak.queue import set_merge_cooldown

                    await set_merge_cooldown(install, owner, repo, number)
                    return
                await asyncio.sleep(POLL_RATE_SECONDS)
                continue
            except ApiCallException as e:
                # if we have some api exception, it's likely a temporary error that
                # can be resolved by calling GitHub again.
                if api_call_retries_remaining:
                    api_call_errors.append(
                        APICallError(
                            api_name=e.method,
                            http_status=str(e.status_code),
                            response_body=str(e.response),
                        )
                    )
                    api_call_retries_remaining -= 1
                    log.info("problem contacting remote api. retrying")
                    await record_debug_history_event(
                        stage="evaluation",
                        event_type="evaluation_api_retry",
                        message="Retrying after a GitHub API problem",
                        installation_id=install,
                        owner=owner,
                        repo=repo,
                        pr_number=number,
                        details={
                            "method": e.method,
                            "status_code": e.status_code,
                            "retries_remaining": api_call_retries_remaining,
                        },
                    )
                    continue
                log.warning("api_call_retries_remaining", exc_info=True)
                await record_debug_history_event(
                    stage="evaluation",
                    event_type="evaluation_api_retry_exhausted",
                    message="Exhausted PR evaluation API retries",
                    installation_id=install,
                    owner=owner,
                    repo=repo,
                    pr_number=number,
                    details={"method": e.method, "status_code": e.status_code},
                )
            return
        except asyncio.TimeoutError:
            timeout_count += 1
            backoff_sec = min(10 * (2 ** (timeout_count - 1)), 300)
            log.warning(
                "mergeable_timeout",
                timeout_count=timeout_count,
                max_timeouts=conf.PR_EVALUATION_MAX_TIMEOUT_RETRIES,
                backoff_sec=backoff_sec,
                exc_info=True,
            )
            await record_debug_history_event(
                stage="evaluation",
                event_type="evaluation_timeout",
                message="PR evaluation timed out",
                installation_id=install,
                owner=owner,
                repo=repo,
                pr_number=number,
                details={
                    "retry": timeout_count,
                    "max_retries": conf.PR_EVALUATION_MAX_TIMEOUT_RETRIES,
                    "backoff_sec": backoff_sec,
                },
            )
            if not merging and timeout_count >= conf.PR_EVALUATION_MAX_TIMEOUT_RETRIES:
                log.warning(
                    "max_evaluation_timeouts_reached",
                    timeouts=timeout_count,
                )
                return
            await requeue_callback(delay_sec=backoff_sec)


class QueueForMergeCallback(Protocol):
    async def __call__(self, *, first: bool) -> Optional[int]: ...


class PRV2:
    """
    Representation of a PR for Kodiak.

    This class implements the PRAPI protocol found in evaluation.py
    """

    event: EventInfoResponse

    def __init__(
        self,
        event: EventInfoResponse,
        install: str,
        owner: str,
        repo: str,
        number: int,
        dequeue_callback: Callable[[], Awaitable[None]],
        requeue_callback: RequeueCallback,
        queue_for_merge_callback: QueueForMergeCallback,
        client: Optional[Type[Client]] = None,
    ):
        self.install = install
        self.owner = owner
        self.repo = repo
        self.number = number
        self.event = event
        self.dequeue_callback = dequeue_callback
        self.requeue_callback = requeue_callback
        self.queue_for_merge_callback = queue_for_merge_callback
        self.log = logger.bind(install=install, owner=owner, repo=repo, number=number)
        self.client = client or Client
        self._last_status_message: str | None = None

    async def record_debug_event(
        self,
        *,
        stage: str,
        event_type: str,
        message: str,
        details: Optional[Mapping[str, Any]] = None,
    ) -> None:
        await record_debug_history_event(
            stage=stage,
            event_type=event_type,
            message=message,
            installation_id=self.install,
            owner=self.owner,
            repo=self.repo,
            pr_number=self.number,
            details=details,
        )

    async def cache_no_automerge_label(self) -> None:
        from kodiak.queue import set_nolabel_cache

        await set_nolabel_cache(self.install, self.owner, self.repo, self.number)

    async def check_merge_cooldown(self) -> bool:
        from kodiak.queue import check_merge_cooldown

        return await check_merge_cooldown(
            self.install, self.owner, self.repo, self.number
        )

    async def increment_requeue_attempts(self, reason: str) -> int:
        from kodiak.queue import increment_requeue_attempts

        return await increment_requeue_attempts(
            self.install, self.owner, self.repo, self.number, reason
        )

    async def dequeue(self) -> None:
        self.log.info("dequeue")
        await self.record_debug_event(
            stage="pr_action",
            event_type="pr_dequeued",
            message="Removed PR from the merge queue",
        )
        await self.dequeue_callback()

    async def requeue(self) -> None:
        self.log.info("requeue")
        await self.record_debug_event(
            stage="pr_action",
            event_type="pr_requeued",
            message="Requeued PR for another evaluation pass",
        )
        await self.requeue_callback()

    async def set_status(
        self, msg: str, *, markdown_content: Optional[str] = None
    ) -> None:
        """
        Display a message to a user through a github check

        `markdown_content` is the message displayed on the detail view for a
        status check. This detail view is accessible via the "Details" link
        alongside the summary/detail content.
        """
        from redis.exceptions import RedisError

        from kodiak.queue import check_status_dedup, set_status_dedup

        redis_dedup = False
        try:
            redis_dedup = await check_status_dedup(
                self.install, self.owner, self.repo, self.number, msg
            )
        except (OSError, RedisError):
            self.log.debug("status_dedup_check_failed", exc_info=True)
        if msg == self._last_status_message or redis_dedup:
            self.log.info("set_status_skipped_duplicate", message=msg)
            await self.record_debug_event(
                stage="status",
                event_type="status_skipped_duplicate",
                message="Skipped duplicate status update",
                details={"status": msg},
            )
            return
        self.log.info("set_status", message=msg, markdown_content=markdown_content)
        async with self.client(
            installation_id=self.install, owner=self.owner, repo=self.repo
        ) as api_client:
            res = await api_client.create_notification(
                head_sha=self.event.pull_request.latest_sha,
                message=msg,
                summary=markdown_content,
            )
            try:
                res.raise_for_status()
            except HTTPError:
                self.log.warning(
                    "failed to create notification", res=res, exc_info=True
                )
                await self.record_debug_event(
                    stage="status",
                    event_type="status_failed",
                    message="Failed to publish a status update",
                    details={"status": msg},
                )
            else:
                self._last_status_message = msg
                try:
                    await set_status_dedup(
                        self.install, self.owner, self.repo, self.number, msg
                    )
                except (OSError, RedisError):
                    self.log.debug("status_dedup_set_failed", exc_info=True)
                await self.record_debug_event(
                    stage="status",
                    event_type="status_set",
                    message="Published a status update",
                    details={
                        "status": msg,
                        "markdown_content": markdown_content,
                    },
                )

    async def pull_requests_for_ref(self, ref: str) -> Optional[int]:
        log = self.log.bind(ref=ref)
        log.info("pull_requests_for_ref", ref=ref)
        async with self.client(
            installation_id=self.install, owner=self.owner, repo=self.repo
        ) as api_client:
            prs = await api_client.get_open_pull_requests(base=ref)
            if prs is None:
                # our api request failed.
                log.info("failed to get pull request info for ref")
                return None
            return len(prs)

    async def delete_branch(self, branch_name: str) -> None:
        self.log.info("delete_branch", branch_name=branch_name)
        await self.record_debug_event(
            stage="github_action",
            event_type="delete_branch_started",
            message="Attempting to delete the branch after merge",
            details={"branch_name": branch_name},
        )
        async with self.client(
            installation_id=self.install, owner=self.owner, repo=self.repo
        ) as api_client:
            res = await api_client.delete_branch(branch=branch_name)
            try:
                res.raise_for_status()
            except HTTPError as e:
                if e.response is not None and e.response.status_code == 422:
                    self.log.info("branch already deleted, nothing to do", res=res)
                    await self.record_debug_event(
                        stage="github_action",
                        event_type="delete_branch_already_deleted",
                        message="Branch was already deleted",
                        details={"branch_name": branch_name},
                    )
                else:
                    self.log.warning("failed to delete branch", res=res, exc_info=True)
                    await self.record_debug_event(
                        stage="github_action",
                        event_type="delete_branch_failed",
                        message="Failed to delete the branch",
                        details={
                            "branch_name": branch_name,
                            "status_code": res.status_code,
                        },
                    )
            else:
                await self.record_debug_event(
                    stage="github_action",
                    event_type="delete_branch_succeeded",
                    message="Deleted the branch",
                    details={"branch_name": branch_name},
                )

    async def update_branch(self) -> None:
        self.log.info("update_branch")
        await self.record_debug_event(
            stage="github_action",
            event_type="update_branch_started",
            message="Attempting to update the PR branch",
        )
        async with self.client(
            installation_id=self.install, owner=self.owner, repo=self.repo
        ) as api_client:
            res = await api_client.update_branch(pull_number=self.number)
            try:
                res.raise_for_status()
            except HTTPError as e:
                # GitHub returns 422 when the branch is already up-to-date.
                # This is not an error from Kodiak's perspective — the branch
                # doesn't need updating, so treat it like a success.
                if res.status_code == 422:
                    self.log.info(
                        "update_branch: branch already up-to-date (422), skipping",
                        res=res,
                    )
                    await self.record_debug_event(
                        stage="github_action",
                        event_type="update_branch_already_up_to_date",
                        message="Branch is already up-to-date, no update needed",
                        details={"status_code": res.status_code, "response": res.text},
                    )
                    return
                self.log.warning("failed to update branch", res=res, exc_info=True)
                await self.record_debug_event(
                    stage="github_action",
                    event_type="update_branch_failed",
                    message="Failed to update the PR branch",
                    details={"status_code": res.status_code},
                )
                # we raise an exception to retry this request.
                raise ApiCallException(
                    method="pull_request/update_branch",
                    http_status_code=res.status_code,
                    response=res.content,
                ) from e
            else:
                await self.record_debug_event(
                    stage="github_action",
                    event_type="update_branch_succeeded",
                    message="Updated the PR branch",
                )

    async def approve_pull_request(self) -> None:
        self.log.info("approve_pull_request")
        await self.record_debug_event(
            stage="github_action",
            event_type="approve_pull_request_started",
            message="Attempting to approve the PR",
        )
        async with self.client(
            installation_id=self.install, owner=self.owner, repo=self.repo
        ) as api_client:
            res = await api_client.approve_pull_request(pull_number=self.number)
            try:
                res.raise_for_status()
            except HTTPError:
                self.log.warning(
                    "failed to approve pull request", res=res, exc_info=True
                )
                await self.record_debug_event(
                    stage="github_action",
                    event_type="approve_pull_request_failed",
                    message="Failed to approve the PR",
                    details={"status_code": res.status_code},
                )
            else:
                await self.record_debug_event(
                    stage="github_action",
                    event_type="approve_pull_request_succeeded",
                    message="Approved the PR",
                )

    async def trigger_test_commit(self) -> None:
        self.log.info("trigger_test_commit")
        await self.record_debug_event(
            stage="github_action",
            event_type="trigger_test_commit_started",
            message="Attempting to trigger a mergeability recalculation",
        )
        async with self.client(
            installation_id=self.install, owner=self.owner, repo=self.repo
        ) as api_client:
            res = await api_client.get_pull_request(number=self.number)
            try:
                res.raise_for_status()
            except HTTPError:
                self.log.warning(
                    "failed to get pull request for test commit trigger",
                    res=res,
                    exc_info=True,
                )
                await self.record_debug_event(
                    stage="github_action",
                    event_type="trigger_test_commit_failed",
                    message="Failed to trigger a mergeability recalculation",
                    details={"status_code": res.status_code},
                )
            else:
                await self.record_debug_event(
                    stage="github_action",
                    event_type="trigger_test_commit_succeeded",
                    message="Triggered a mergeability recalculation",
                )

    async def merge(
        self,
        merge_method: str,
        commit_title: Optional[str],
        commit_message: Optional[str],
    ) -> None:
        self.log.info("merge", method=merge_method)
        await self.record_debug_event(
            stage="github_action",
            event_type="merge_started",
            message="Attempting to merge the PR",
            details={
                "merge_method": merge_method,
                "commit_title": commit_title,
            },
        )
        async with self.client(
            installation_id=self.install, owner=self.owner, repo=self.repo
        ) as api_client:
            res = await api_client.merge_pull_request(
                number=self.number,
                merge_method=merge_method,
                commit_title=commit_title,
                commit_message=commit_message,
            )
            try:
                res.raise_for_status()
            except HTTPError as e:
                if e.response is not None and e.response.status_code == 405:
                    self.log.warning(
                        "merge_rejected_405",
                        res=res,
                    )
                    await self.record_debug_event(
                        stage="github_action",
                        event_type="merge_rejected_405",
                        message="GitHub rejected merge (405). PR may have been merged externally or is not in a mergeable state.",
                        details={
                            "merge_method": merge_method,
                            "status_code": 405,
                        },
                    )
                    await self.set_status(
                        "⚠️ GitHub rejected the merge (405). The PR may have been merged externally or is not in a mergeable state."
                    )
                    await self.dequeue()
                    return
                self.log.warning("failed to merge pull request", res=res, exc_info=True)
                await self.record_debug_event(
                    stage="github_action",
                    event_type="merge_failed",
                    message="Failed to merge the PR",
                    details={
                        "merge_method": merge_method,
                        "status_code": res.status_code,
                    },
                )
                if e.response is not None and e.response.status_code == 500:
                    raise GitHubApiInternalServerError from e
                # we raise an exception to retry this request.
                raise ApiCallException(
                    method="pull_request/merge",
                    http_status_code=res.status_code,
                    response=res.content,
                ) from e
            else:
                await self.record_debug_event(
                    stage="github_action",
                    event_type="merge_succeeded",
                    message="Merged the PR",
                    details={"merge_method": merge_method},
                )

    async def update_ref(self, ref: str, sha: str) -> None:
        self.log.info("update_ref", ref=ref, sha=sha)
        await self.record_debug_event(
            stage="github_action",
            event_type="update_ref_started",
            message="Attempting a fast-forward ref update",
            details={"ref": ref, "sha": sha},
        )
        async with self.client(
            installation_id=self.install, owner=self.owner, repo=self.repo
        ) as api_client:
            res = await api_client.update_ref(ref=ref, sha=sha)
            try:
                res.raise_for_status()
            except HTTPError as e:
                if e.response is not None and e.response.status_code == 422:
                    self.log.info("fast forward update not possible.", res=res)
                else:
                    self.log.warning("failed to update ref", res=res, exc_info=True)
                await self.record_debug_event(
                    stage="github_action",
                    event_type="update_ref_failed",
                    message="Failed to fast-forward the base ref",
                    details={
                        "ref": ref,
                        "sha": sha,
                        "status_code": res.status_code,
                    },
                )
                # we raise an exception to retry this request.
                raise ApiCallException(
                    method="pull_request/update_ref",
                    http_status_code=res.status_code,
                    response=res.content,
                ) from e
            else:
                await self.record_debug_event(
                    stage="github_action",
                    event_type="update_ref_succeeded",
                    message="Fast-forwarded the base ref",
                    details={"ref": ref, "sha": sha},
                )

    async def queue_for_merge(self, *, first: bool) -> Optional[int]:
        self.log.info("queue_for_merge")
        position = await self.queue_for_merge_callback(first=first)
        await self.record_debug_event(
            stage="pr_action",
            event_type="queue_for_merge_requested",
            message="Requested that the PR be added to the merge queue",
            details={"first": first, "position": position},
        )
        return position

    async def add_label(self, label: str) -> None:
        """
        add label to pull request
        """
        self.log.info("add_label", label=label)
        await self.record_debug_event(
            stage="github_action",
            event_type="add_label_started",
            message="Attempting to add a label",
            details={"label": label},
        )
        async with self.client(
            installation_id=self.install, owner=self.owner, repo=self.repo
        ) as api_client:
            res = await api_client.add_label(label, pull_number=self.number)
            try:
                res.raise_for_status()
            except HTTPError as exc:
                self.log.warning(
                    "failed to add label", label=label, res=res, exc_info=True
                )
                await self.record_debug_event(
                    stage="github_action",
                    event_type="add_label_failed",
                    message="Failed to add a label",
                    details={"label": label, "status_code": res.status_code},
                )
                raise ApiCallException(
                    method="pull_request/add_label",
                    http_status_code=res.status_code,
                    response=res.content,
                ) from exc
            else:
                await self.record_debug_event(
                    stage="github_action",
                    event_type="add_label_succeeded",
                    message="Added a label",
                    details={"label": label},
                )

    async def remove_label(self, label: str) -> None:
        """
        remove the PR label specified by `label_id` for a given `pr_number`
        """
        self.log.info("remove_label", label=label)
        await self.record_debug_event(
            stage="github_action",
            event_type="remove_label_started",
            message="Attempting to remove a label",
            details={"label": label},
        )
        async with self.client(
            installation_id=self.install, owner=self.owner, repo=self.repo
        ) as api_client:
            res = await api_client.delete_label(label, pull_number=self.number)
            try:
                res.raise_for_status()
            except HTTPError as exc:
                self.log.warning(
                    "failed to delete label", label=label, res=res, exc_info=True
                )
                await self.record_debug_event(
                    stage="github_action",
                    event_type="remove_label_failed",
                    message="Failed to remove a label",
                    details={"label": label, "status_code": res.status_code},
                )
                # we raise an exception to retry this request.
                raise ApiCallException(
                    method="pull_request/delete_label",
                    http_status_code=res.status_code,
                    response=res.content,
                ) from exc
            else:
                await self.record_debug_event(
                    stage="github_action",
                    event_type="remove_label_succeeded",
                    message="Removed a label",
                    details={"label": label},
                )

    async def create_comment(self, body: str) -> None:
        """
        create a comment on the specified `pr_number` with the given `body` as text.
        """
        self.log.info("create_comment", body=body)
        await self.record_debug_event(
            stage="github_action",
            event_type="create_comment_started",
            message="Attempting to create a PR comment",
            details={"body": body},
        )
        async with self.client(
            installation_id=self.install, owner=self.owner, repo=self.repo
        ) as api_client:
            res = await api_client.create_comment(body=body, pull_number=self.number)
            try:
                res.raise_for_status()
            except HTTPError:
                self.log.warning("failed to create comment", res=res, exc_info=True)
                await self.record_debug_event(
                    stage="github_action",
                    event_type="create_comment_failed",
                    message="Failed to create a PR comment",
                    details={"status_code": res.status_code},
                )
            else:
                await self.record_debug_event(
                    stage="github_action",
                    event_type="create_comment_succeeded",
                    message="Created a PR comment",
                    details={"body": body},
                )
