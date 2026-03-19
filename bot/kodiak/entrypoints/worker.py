"""
Process webhook events from the Redis queues.
"""

from __future__ import annotations

import asyncio
from asyncio.tasks import Task
from typing import NoReturn

import pydantic
import sentry_sdk
import structlog

from kodiak import (
    app_config as conf,
    app_identity,
)
from kodiak.assertions import assert_never
from kodiak.debug_history import record_debug_event, summarize_webhook_payload
from kodiak.logging import configure_logging
from kodiak.queue import (
    INGEST_QUEUE_NAMES,
    QUEUE_PUBSUB_INGEST,
    RedisWebhookQueue,
    WebhookQueueProtocol,
    get_ingest_queue,
    handle_webhook_event,
)
from kodiak.redis_client import redis_bot
from kodiak.schemas import RawWebhookEvent

configure_logging()

logger = structlog.get_logger()


async def work_ingest_queue(queue: WebhookQueueProtocol, queue_name: str) -> NoReturn:
    log = logger.bind(queue_name=queue_name, task="work_ingest_queue")

    log.info("start working ingest_queue")
    while True:
        res = await redis_bot.blpop(
            [queue_name], timeout=conf.REDIS_BLOCKING_POP_TIMEOUT_SEC
        )
        if res is None:
            continue
        _, value = res
        parsed_event = RawWebhookEvent.parse_raw(value)
        webhook_summary = summarize_webhook_payload(
            event_name=parsed_event.event_name,
            payload=parsed_event.payload,
        )
        installation_id = webhook_summary.get("installation_id")
        owner = webhook_summary.get("owner")
        repo = webhook_summary.get("repo")
        pr_number = webhook_summary.get("pull_request_number")
        action = webhook_summary.get("action")
        await record_debug_event(
            stage="ingest",
            event_type="ingest_dequeued",
            message="Dequeued raw webhook from ingest queue",
            installation_id=installation_id,
            owner=owner,
            repo=repo,
            pr_number=pr_number,
            queue_name=queue_name,
            delivery_id=parsed_event.delivery_id,
            event_name=parsed_event.event_name,
            action=action,
        )
        try:
            await asyncio.wait_for(
                handle_webhook_event(
                    queue=queue,
                    event_name=parsed_event.event_name,
                    payload=parsed_event.payload,
                    delivery_id=parsed_event.delivery_id,
                ),
                timeout=60,
            )
        except asyncio.TimeoutError:
            log.warning("handle_webhook_event timed out")
            await record_debug_event(
                stage="ingest",
                event_type="ingest_handle_timeout",
                message="Timed out while processing a raw webhook",
                installation_id=installation_id,
                owner=owner,
                repo=repo,
                pr_number=pr_number,
                queue_name=queue_name,
                delivery_id=parsed_event.delivery_id,
                event_name=parsed_event.event_name,
                action=action,
            )
        else:
            await record_debug_event(
                stage="ingest",
                event_type="ingest_handled",
                message="Finished handling a raw webhook",
                installation_id=installation_id,
                owner=owner,
                repo=repo,
                pr_number=pr_number,
                queue_name=queue_name,
                delivery_id=parsed_event.delivery_id,
                event_name=parsed_event.event_name,
                action=action,
            )
        log.info("ingest_event_handled")


class PubsubIngestQueueSchema(pydantic.BaseModel):
    installation_id: int


async def ingest_queue_starter(
    ingest_workers: dict[str, Task[NoReturn]], queue: RedisWebhookQueue
) -> None:
    """
    Listen on Redis Pubsub and start queue worker if we don't have one already.
    """
    pubsub = redis_bot.pubsub()
    await pubsub.subscribe(QUEUE_PUBSUB_INGEST)
    log = logger.bind(task="ingest_queue_starter")
    log.info("start watch for ingest_queues")
    while True:
        reply = await pubsub.get_message(ignore_subscribe_messages=True, timeout=10)
        if reply is None:
            continue
        installation_id = PubsubIngestQueueSchema.parse_raw(
            reply["data"]
        ).installation_id
        queue_name = get_ingest_queue(installation_id)
        if queue_name not in ingest_workers:
            ingest_workers[queue_name] = asyncio.create_task(
                work_ingest_queue(queue, queue_name=queue_name)
            )
            log.info("started new task")


async def main() -> NoReturn:
    # Resolve the app's own identity (slug, bot login) from the GitHub API
    # before processing any events.  This prevents mis-configurations of
    # GITHUB_APP_NAME from causing infinite approval/check-run loops.
    app_identity.init()

    queue = RedisWebhookQueue()
    await queue.create()

    ingest_workers = dict()

    ingest_queue_names = await redis_bot.smembers(INGEST_QUEUE_NAMES)
    log = logger.bind(task="main_worker")

    for queue_name_bytes in ingest_queue_names:
        queue_name = queue_name_bytes.decode()
        if queue_name not in ingest_workers:
            log.info("start ingest_queue_worker", queue_name=queue_name)
            ingest_workers[queue_name] = asyncio.create_task(
                work_ingest_queue(queue, queue_name=queue_name)
            )

    log.info(
        "worker_startup_summary",
        ingest_queue_count=len(ingest_queue_names),
        webhook_consumer_concurrency=conf.WEBHOOK_CONSUMER_CONCURRENCY,
        merge_queue_poll_timeout_sec=conf.MERGE_QUEUE_POLL_TIMEOUT_SEC,
    )
    log.info("start ingest_queue_watcher")
    ingest_queue_watcher = asyncio.create_task(
        ingest_queue_starter(ingest_workers, queue)
    )

    while True:
        # Health check the various tasks and recreate them if necessary.
        # There's probably a cleaner way to do this.
        await asyncio.sleep(0.25)
        for queue_name, worker_task in ingest_workers.items():
            if worker_task is None or not worker_task.done():
                continue
            logger.warning("worker_task_restart", kind="ingest")
            # task failed. record result and restart
            exception = worker_task.exception()
            logger.info("exception", excep=exception)
            sentry_sdk.capture_exception(exception)
            ingest_workers[queue_name] = asyncio.create_task(
                work_ingest_queue(queue, queue_name=queue_name)
            )
        for task_meta, cur_task in queue.all_tasks():
            if not cur_task.done():
                continue
            logger.warning("worker_task_restart", kind=task_meta.kind)
            # task failed. record result and restart
            exception = cur_task.exception()
            logger.info("exception", excep=exception)
            sentry_sdk.capture_exception(exception)
            if task_meta.kind == "repo":
                queue.start_repo_worker(queue_name=task_meta.queue_name)
            elif task_meta.kind == "webhook":
                queue.start_webhook_worker(queue_name=task_meta.queue_name)
            else:
                assert_never(task_meta.kind)
        if ingest_queue_watcher.done():
            logger.warning("worker_task_restart", kind="ingest_queue_watcher")
            exception = ingest_queue_watcher.exception()
            logger.info("exception", excep=exception)
            sentry_sdk.capture_exception(exception)
            ingest_queue_watcher = asyncio.create_task(
                ingest_queue_starter(ingest_workers, queue)
            )


if __name__ == "__main__":
    asyncio.run(main())
