import time

import redis
from redis.asyncio import Redis

from rqueue.job import Job, JobStatus
from rqueue.job_queue import JobQueue


class AsyncJobQueue:
    """Async Redis operations used by AsyncWorker."""

    STREAM = JobQueue.STREAM
    GROUP = JobQueue.GROUP
    DEFAULT_QUEUE = JobQueue.DEFAULT_QUEUE
    MAX_ATTEMPTS = JobQueue.MAX_ATTEMPS
    STATUS_PREFIX = JobQueue.STATUS_PREFIX
    JOB_PREFIX = JobQueue.JOB_PREFIX
    DELAYED_KEY = "job-queue:delayed"
    DEAD_LETTER_STREAM = "jobs:dead"

    def __init__(self, redis_client: Redis):
        self.redis = redis_client

    async def get_job(self, job_id: str) -> Job | None:
        data = await self.redis.get(self._job_key(job_id))
        if not data:
            return None
        return Job.deserialize(data)

    async def fetch_jobs(
        self, consumer_name: str, count: int = 1, block_ms: int = 1000, queues=None
    ) -> list[tuple[str, str, str]]:
        queues = queues or [self.DEFAULT_QUEUE]
        streams = {}
        for queue_name in queues:
            await self._create_consumer_group(queue_name)
            streams[self._stream_for_queue(queue_name)] = ">"

        messages = await self.redis.xreadgroup(
            self.GROUP, consumer_name, streams, count=count, block=block_ms
        )
        return [
            (message_id, data["job_id"], self._queue_for_stream(stream))
            for stream, entries in messages
            for message_id, data in entries
        ]

    async def update_status(self, job: Job, new_status: str) -> None:
        old_status = job.status
        await self.redis.srem(self._status_key(old_status), job.id)
        await self.redis.sadd(self._status_key(new_status), job.id)
        job.status = new_status
        await self._save_job(job)

    async def ack(self, message_id: str, queue_name: str = DEFAULT_QUEUE) -> None:
        await self.redis.xack(
            self._stream_for_queue(queue_name), self.GROUP, message_id
        )

    async def retry_job(
        self, job: Job, message_id: str, error: str | None = None, base_delay: int = 10
    ) -> None:
        job.attempts += 1
        job.error = error or "max retries exceeded"

        if job.attempts >= self.MAX_ATTEMPTS:
            await self.update_status(job, JobStatus.DEAD_LETTER)
            await self.redis.xadd(
                self.DEAD_LETTER_STREAM,
                {
                    "job_id": job.id,
                    "error": job.error,
                    "attempts": str(job.attempts),
                },
            )
            await self.ack(message_id, job.queue_name)
            return

        delay_seconds = base_delay * (2 ** (job.attempts - 1))
        await self.update_status(job, JobStatus.RETRYING)
        await self.redis.zadd(
            self.DELAYED_KEY, {job.id: time.time() + delay_seconds}
        )
        await self.ack(message_id, job.queue_name)

    async def enqueue_scheduled_jobs(self) -> int:
        ready_jobs = await self.redis.zrangebyscore(
            self.DELAYED_KEY, 0, time.time()
        )
        if not ready_jobs:
            return 0

        async with self.redis.pipeline() as pipeline:
            for job_id in ready_jobs:
                pipeline.zrem(self.DELAYED_KEY, job_id)
            removed = await pipeline.execute()

        promoted = 0
        for job_id, was_removed in zip(ready_jobs, removed):
            if not was_removed:
                continue
            job = await self.get_job(job_id)
            if job is None:
                continue
            await self.update_status(job, JobStatus.QUEUED)
            await self.redis.xadd(
                self._stream_for_queue(job.queue_name), {"job_id": job_id}
            )
            promoted += 1
        return promoted

    async def reclaim_stale(
        self,
        consumer_name: str,
        min_idle_time: int = 30000,
        queues=None,
        count: int = 10,
    ) -> list[tuple[str, str, str]]:
        queues = queues or [self.DEFAULT_QUEUE]
        reclaimed = []
        for queue_name in queues:
            if len(reclaimed) >= count:
                break
            await self._create_consumer_group(queue_name)
            result = await self.redis.xautoclaim(
                self._stream_for_queue(queue_name),
                self.GROUP,
                consumer_name,
                min_idle_time,
                "0-0",
                count=count - len(reclaimed),
            )
            reclaimed.extend(
                (message_id, data["job_id"], queue_name)
                for message_id, data in result[1]
            )
        return reclaimed

    async def _create_consumer_group(self, queue_name: str) -> None:
        try:
            await self.redis.xgroup_create(
                self._stream_for_queue(queue_name),
                self.GROUP,
                id="0",
                mkstream=True,
            )
        except redis.exceptions.ResponseError as error:
            if "BUSYGROUP" not in str(error):
                raise

    async def _save_job(self, job: Job) -> None:
        await self.redis.set(self._job_key(job.id), job.serialize())

    def _stream_for_queue(self, queue_name: str) -> str:
        if not queue_name or "," in queue_name:
            raise ValueError("queue_name must be a non-empty queue name")
        if queue_name == self.DEFAULT_QUEUE:
            return self.STREAM
        return f"{self.STREAM}:{queue_name}"

    def _queue_for_stream(self, stream: str | bytes) -> str:
        if isinstance(stream, bytes):
            stream = stream.decode()
        if stream == self.STREAM:
            return self.DEFAULT_QUEUE
        return stream[len(f"{self.STREAM}:") :]

    def _job_key(self, job_id: str) -> str:
        return f"{self.JOB_PREFIX}:{job_id}"

    def _status_key(self, status: str) -> str:
        return f"{self.STATUS_PREFIX}:{status}"
