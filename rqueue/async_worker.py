import asyncio
import inspect
import logging
import signal
import time
from collections.abc import Sequence

import redis

from rqueue.async_job_queue import AsyncJobQueue
from rqueue.job import JobStatus

logger = logging.getLogger(__name__)


class AsyncWorker:
    def __init__(
        self,
        queue: AsyncJobQueue,
        consumer_name: str,
        concurrency: int = 5,
        queues: Sequence[str] | None = None,
    ):
        if concurrency < 1:
            raise ValueError("concurrency must be a positive integer")
        self.queue = queue
        self.consumer = consumer_name
        self.queues = list(queues or [queue.DEFAULT_QUEUE])
        self.concurrency = concurrency
        self.is_running = False
        self._tasks: set[asyncio.Task] = set()

    def stop(self) -> None:
        self.is_running = False

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.stop)

        self.is_running = True
        last_scheduler_check = 0.0
        last_reclaim_check = 0.0
        logger.info(
            "Async worker %r listening on queues: %s",
            self.consumer,
            ", ".join(self.queues),
        )
        try:
            while self.is_running:
                now = time.monotonic()
                try:
                    if now - last_scheduler_check > 5:
                        await self.queue.enqueue_scheduled_jobs()
                        last_scheduler_check = now

                    available = self.concurrency - len(self._tasks)
                    if now - last_reclaim_check > 30 and available:
                        stale_jobs = await self.queue.reclaim_stale(
                            self.consumer,
                            min_idle_time=30000,
                            queues=self.queues,
                            count=available,
                        )
                        for message_id, job_id, queue_name in stale_jobs:
                            logger.info("Reclaimed stale job %s", job_id)
                            self._schedule_job(message_id, job_id, queue_name)
                        last_reclaim_check = now

                    available = self.concurrency - len(self._tasks)
                    if available:
                        jobs = await self.queue.fetch_jobs(
                            self.consumer,
                            count=available,
                            block_ms=1000,
                            queues=self.queues,
                        )
                        for message_id, job_id, queue_name in jobs:
                            self._schedule_job(message_id, job_id, queue_name)
                    else:
                        await asyncio.sleep(0.1)
                except redis.ConnectionError:
                    logger.exception("Lost connection to Redis; retrying in 5 seconds")
                    await asyncio.sleep(5)
        finally:
            self.is_running = False
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)
            if self._tasks:
                await asyncio.gather(*self._tasks, return_exceptions=True)

    def _schedule_job(
        self, message_id: str, job_id: str, queue_name: str
    ) -> None:
        task = asyncio.create_task(
            self._process_message(message_id, job_id, queue_name)
        )
        self._tasks.add(task)
        task.add_done_callback(self._task_finished)

    def _task_finished(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        error = task.exception() if not task.cancelled() else None
        if error is not None:
            logger.error(
                "Async job processing failed unexpectedly",
                exc_info=(type(error), error, error.__traceback__),
            )

    async def _process_message(
        self, message_id: str, job_id: str, queue_name: str
    ) -> None:
        job = await self.queue.get_job(job_id)
        if job is None:
            logger.error("Job %s not found; acknowledging message", job_id)
            await self.queue.ack(message_id, queue_name)
            return

        await self.queue.update_status(job, JobStatus.PROCESSING)
        try:
            func, args, kwargs = job.unpack_payload()
            if inspect.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = await asyncio.to_thread(func, *args, **kwargs)
                if inspect.isawaitable(result):
                    result = await result
            logger.info("Completed job %s: %r", job_id, result)
        except Exception as error:
            logger.exception("Job %s failed", job_id)
            await self.queue.retry_job(job, message_id, error=str(error))
            return

        await self.queue.update_status(job, JobStatus.COMPLETED)
        await self.queue.ack(message_id, queue_name)
