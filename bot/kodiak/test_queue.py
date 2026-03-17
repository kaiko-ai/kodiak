from typing import Optional
from unittest.mock import patch

import pytest

from kodiak.queue import RedisWebhookQueue, TaskMeta, installation_id_from_queue


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
