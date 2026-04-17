from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, patch

import pytest
import structlog

from kodiak.queue import (
    RedisWebhookQueue,
    TaskMeta,
    WebhookEvent,
    installation_id_from_queue,
    process_webhook_event,
)


@pytest.mark.parametrize(
    "queue_name, expected_installation_id",
    (
        ("merge_queue:11256551.sbdchd/squawk/main.test.foo", "11256551"),
        ("merge_queue:11256551.sbdchd/squawk", "11256551"),
        ("merge_queue:11256551.sbdchd/squawk:repo/main:test.branch", "11256551"),
        ("webhook:11256551", "11256551"),
        ("", ""),
    ),
)
def test_installation_id_from_queue(
    queue_name: str, expected_installation_id: str
) -> None:
    """
    We should gracefully parse an installation id from the queue name
    """
    assert installation_id_from_queue(queue_name) == expected_installation_id


def _fake_create_task(coro):  # type: ignore[no-untyped-def]
    """
    Stand-in for asyncio.create_task that closes the coroutine (to avoid
    'coroutine was never awaited' warnings) and returns a _FakeTask.
    """
    coro.close()
    return _FakeTask()


class TestWebhookConsumerConcurrency:
    def test_start_webhook_worker_creates_multiple_tasks(self) -> None:
        """
        start_webhook_worker should create WEBHOOK_CONSUMER_CONCURRENCY tasks
        for a single queue name, each with a unique worker key.
        """
        queue = RedisWebhookQueue()
        concurrency = 4
        with patch(
            "kodiak.queue.conf.WEBHOOK_CONSUMER_CONCURRENCY", concurrency
        ), patch(
            "kodiak.queue.asyncio.create_task", side_effect=_fake_create_task
        ) as mock_create_task:
            queue.start_webhook_worker(queue_name="webhook:12345")

        assert mock_create_task.call_count == concurrency
        # All tasks should be stored under unique keys
        assert len(queue.worker_tasks) == concurrency
        for i in range(concurrency):
            key = f"webhook:12345:worker:{i}"
            assert key in queue.worker_tasks

    def test_start_webhook_worker_is_idempotent(self) -> None:
        """
        Calling start_webhook_worker twice should not create duplicate tasks
        if the existing ones are still running.
        """
        queue = RedisWebhookQueue()
        concurrency = 2
        with patch(
            "kodiak.queue.conf.WEBHOOK_CONSUMER_CONCURRENCY", concurrency
        ), patch(
            "kodiak.queue.asyncio.create_task", side_effect=_fake_create_task
        ) as mock_create_task:
            queue.start_webhook_worker(queue_name="webhook:12345")
            first_call_count = mock_create_task.call_count

            # Second call should not create new tasks
            queue.start_webhook_worker(queue_name="webhook:12345")
            assert mock_create_task.call_count == first_call_count

    def test_all_tasks_returns_original_queue_name_for_webhook(self) -> None:
        """
        all_tasks should strip the ':worker:N' suffix from webhook worker keys
        so that the caller gets the original queue name for restart logic.
        """
        queue = RedisWebhookQueue()
        concurrency = 3
        with patch(
            "kodiak.queue.conf.WEBHOOK_CONSUMER_CONCURRENCY", concurrency
        ), patch("kodiak.queue.asyncio.create_task", side_effect=_fake_create_task):
            queue.start_webhook_worker(queue_name="webhook:99999")

        tasks = list(queue.all_tasks())
        assert len(tasks) == concurrency
        for meta, _task in tasks:
            assert meta.kind == "webhook"
            assert meta.queue_name == "webhook:99999"

    def test_repo_worker_key_unchanged(self) -> None:
        """
        Repo workers should still use the raw queue name as key (no :worker:N).
        """
        queue = RedisWebhookQueue()
        with patch("kodiak.queue.asyncio.create_task", side_effect=_fake_create_task):
            queue.start_repo_worker(queue_name="merge_queue:12345.owner/repo/main")

        assert "merge_queue:12345.owner/repo/main" in queue.worker_tasks
        tasks = list(queue.all_tasks())
        assert len(tasks) == 1
        assert tasks[0][0] == TaskMeta(
            kind="repo", queue_name="merge_queue:12345.owner/repo/main"
        )


class _FakeTask:
    """Minimal fake asyncio.Task that reports as not-done."""

    def done(self) -> bool:
        return False

    def exception(self) -> Optional[BaseException]:
        return None


@pytest.mark.asyncio
async def test_process_webhook_event_skips_stale_head_sha(mocker) -> None:  # type: ignore[no-untyped-def]
    event = WebhookEvent(
        repo_owner="acme",
        repo_name="widgets",
        pull_request_number=42,
        installation_id="117046149",
        target_name="main",
        head_sha="oldsha",
    )
    queue_name = event.get_webhook_queue_name()

    redis_mock = mocker.patch("kodiak.queue.redis_bot")
    redis_mock.get = mocker.AsyncMock(
        side_effect=lambda key: {
            event.get_merge_target_queue_name(): None,
            event.get_latest_webhook_head_sha_key(): b"newsha",
        }.get(key)
    )
    redis_mock.zscore = mocker.AsyncMock(return_value=None)

    mocker.patch(
        "kodiak.queue.bzpopmin_with_timeout",
        mocker.AsyncMock(
            return_value=(
                queue_name.encode(),
                event.webhook_queue_member().encode(),
                123.0,
            )
        ),
    )
    mock_evaluate_pr = mocker.patch("kodiak.queue.evaluate_pr", mocker.AsyncMock())
    mocker.patch("kodiak.queue.record_debug_event", mocker.AsyncMock())

    await process_webhook_event(
        RedisWebhookQueue(),
        queue_name,
        structlog.get_logger(),
    )

    mock_evaluate_pr.assert_not_called()
    redis_mock.zscore.assert_not_called()


class _EvalLockRedisMock:
    """Minimal async stand-in for redis_bot covering lock + queue APIs."""

    def __init__(self, *, acquire_lock: bool) -> None:
        self.acquire_lock = acquire_lock
        self.zadd_calls: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = []
        self.eval_calls: List[Tuple[Any, ...]] = []
        self.set_calls: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = []

    async def get(self, key: str) -> Optional[bytes]:
        # Used for merge-target key and latest-head-sha key checks earlier
        # in process_webhook_event.  Return None so those paths pass.
        return None

    async def zscore(self, *args: Any, **kwargs: Any) -> Optional[float]:
        return None

    async def set(self, *args: Any, **kwargs: Any) -> bool:
        self.set_calls.append((args, kwargs))
        # Only NX=True calls are lock acquisitions; others (if any) succeed.
        if kwargs.get("nx"):
            return bool(self.acquire_lock)
        return True

    async def zadd(self, *args: Any, **kwargs: Any) -> int:
        self.zadd_calls.append((args, kwargs))
        return 1

    async def eval(self, *args: Any, **kwargs: Any) -> int:
        self.eval_calls.append(args)
        return 1


async def _run_process_webhook_event_with_lock(
    mocker: Any,
    *,
    acquire_lock: bool,
    evaluate_raises: Optional[BaseException] = None,
) -> Tuple[_EvalLockRedisMock, AsyncMock]:
    event = WebhookEvent(
        repo_owner="acme",
        repo_name="widgets",
        pull_request_number=42,
        installation_id="117046149",
        target_name="main",
        head_sha=None,
    )
    queue_name = event.get_webhook_queue_name()

    redis_mock = _EvalLockRedisMock(acquire_lock=acquire_lock)
    mocker.patch("kodiak.queue.redis_bot", redis_mock)
    mocker.patch(
        "kodiak.queue.bzpopmin_with_timeout",
        mocker.AsyncMock(
            return_value=(
                queue_name.encode(),
                event.webhook_queue_member().encode(),
                123.0,
            )
        ),
    )
    mocker.patch(
        "kodiak.queue.check_nolabel_cache", mocker.AsyncMock(return_value=False)
    )
    mocker.patch("kodiak.queue.record_debug_event", mocker.AsyncMock())
    if evaluate_raises is not None:
        mock_evaluate_pr = mocker.patch(
            "kodiak.queue.evaluate_pr",
            mocker.AsyncMock(side_effect=evaluate_raises),
        )
    else:
        mock_evaluate_pr = mocker.patch("kodiak.queue.evaluate_pr", mocker.AsyncMock())

    if evaluate_raises is not None:
        with pytest.raises(type(evaluate_raises)):
            await process_webhook_event(
                RedisWebhookQueue(), queue_name, structlog.get_logger()
            )
    else:
        await process_webhook_event(
            RedisWebhookQueue(), queue_name, structlog.get_logger()
        )
    return redis_mock, mock_evaluate_pr


@pytest.mark.asyncio
async def test_process_webhook_event_acquires_and_releases_pr_eval_lock(
    mocker: Any,
) -> None:
    """On the happy path we acquire the lock, run evaluate_pr, then release."""
    redis_mock, mock_evaluate_pr = await _run_process_webhook_event_with_lock(
        mocker, acquire_lock=True
    )

    mock_evaluate_pr.assert_awaited_once()
    # One SET call for the lock (nx=True, ex=TTL)
    lock_sets = [c for c in redis_mock.set_calls if c[1].get("nx")]
    assert len(lock_sets) == 1
    # Lock released via Lua eval (compare-and-delete)
    assert len(redis_mock.eval_calls) == 1
    # No contention requeue
    assert redis_mock.zadd_calls == []


@pytest.mark.asyncio
async def test_process_webhook_event_skips_and_requeues_when_locked(
    mocker: Any,
) -> None:
    """When another worker holds the lock, we skip evaluate_pr and requeue."""
    redis_mock, mock_evaluate_pr = await _run_process_webhook_event_with_lock(
        mocker, acquire_lock=False
    )

    mock_evaluate_pr.assert_not_called()
    # Exactly one requeue via zadd with nx=True
    assert len(redis_mock.zadd_calls) == 1
    _args, kwargs = redis_mock.zadd_calls[0]
    assert kwargs.get("nx") is True
    # Nothing to release since we never acquired
    assert redis_mock.eval_calls == []


@pytest.mark.asyncio
async def test_process_webhook_event_releases_lock_on_evaluate_pr_error(
    mocker: Any,
) -> None:
    """If evaluate_pr raises, the lock must still be released."""
    redis_mock, _mock_evaluate_pr = await _run_process_webhook_event_with_lock(
        mocker, acquire_lock=True, evaluate_raises=RuntimeError("boom")
    )

    # Lock was released despite the exception
    assert len(redis_mock.eval_calls) == 1


def test_webhook_event_queue_serialization_is_sha_aware() -> None:
    event = WebhookEvent(
        repo_owner="acme",
        repo_name="widgets",
        pull_request_number=42,
        installation_id="117046149",
        target_name="main",
        head_sha="abc123",
    )

    assert '"head_sha": "abc123"' in event.webhook_queue_member()
    assert "head_sha" not in event.merge_queue_member()
