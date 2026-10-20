import argparse
import asyncio
from datetime import datetime

import redis
import redis.asyncio

from rqueue.async_job_queue import AsyncJobQueue
from rqueue.async_worker import AsyncWorker
from rqueue.dead_letter_queue import DeadLetterQueue
from rqueue.delayed_queue import DelayedQueue
from rqueue.job import JobStatus
from rqueue.job_queue import JobQueue
from rqueue.worker import Worker


def _positive_int(value):
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _create_redis_client():
    return redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


def _job_line(job):
    return (
        f"{job.id}  queue={job.queue_name}  status={job.status}  "
        f"attempts={job.attempts}"
    )


def _list_status(queue, status):
    print(status)
    jobs = queue.get_jobs_by_status(status)
    print(f"{status} ({len(jobs)})")
    for job in jobs:
        print(f"  {_job_line(job)}")


def _status_choices():
    return (
        JobStatus.QUEUED,
        JobStatus.PROCESSING,
        JobStatus.SCHEDULED,
        JobStatus.RETRYING,
        JobStatus.COMPLETED,
        JobStatus.DEAD_LETTER,
    )


def _inspect_job(queue, job_id):
    job = queue.get_job(job_id)
    if job is None:
        print(f"Job not found: {job_id}")
        return 1

    func = job.payload.get("func")
    if func is not None:
        func = f"{getattr(func, '__module__', '')}.{getattr(func, '__qualname__', repr(func))}"

    print(f"id: {job.id}")
    print(f"queue: {job.queue_name}")
    print(f"status: {job.status}")
    print(f"attempts: {job.attempts}")
    print(f"error: {job.error or '-'}")
    print(f"function: {func or '-'}")
    print(f"args: {job.payload.get('args', ())!r}")
    print(f"kwargs: {job.payload.get('kwargs', {})!r}")
    return 0


def _list_failed(dead_letter_queue, limit):
    entries = dead_letter_queue.fetch_dead_jobs(count=limit)
    if not entries:
        print("No failed jobs.")
        return

    for message_id, data in entries:
        print(
            f"{message_id}  job={data.get('job_id', '-')}  "
            f"attempts={data.get('attempts', '-')}  error={data.get('error', '-')}"
        )


def _list_delayed(queue, delayed_queue, limit):
    entries = delayed_queue.get_all_members()[:limit]
    if not entries:
        print("No delayed jobs.")
        return

    for job_id, score in entries:
        if isinstance(job_id, bytes):
            job_id = job_id.decode()
        job = queue.get_job(job_id)
        run_at = datetime.fromtimestamp(float(score)).astimezone().isoformat(
            timespec="seconds"
        )
        if job is None:
            print(f"{job_id}  run_at={run_at}  (job record missing)")
        else:
            print(
                f"{job_id}  queue={job.queue_name}  status={job.status}  "
                f"run_at={run_at}"
            )


def build_parser():
    parser = argparse.ArgumentParser(prog="rqueue", description="Redis job queue tools")
    commands = parser.add_subparsers(dest="command", required=True)

    worker_parser = commands.add_parser("worker", help="Start a worker")
    worker_parser.add_argument("--queues", default="default")
    worker_parser.add_argument("--name", default="worker")
    worker_parser.add_argument("--threads", type=_positive_int, default=5)

    async_worker_parser = commands.add_parser(
        "async-worker", help="Start an asyncio worker"
    )
    async_worker_parser.add_argument("--queues", default="default")
    async_worker_parser.add_argument("--name", default="async-worker")
    async_worker_parser.add_argument("--concurrency", type=_positive_int, default=5)

    list_parser = commands.add_parser("list", help="List jobs by status")
    list_parser.add_argument("--status", choices=_status_choices(), required=True)

    job_parser = commands.add_parser("job", help="Inspect a job")
    job_commands = job_parser.add_subparsers(dest="job_command", required=True)
    inspect_parser = job_commands.add_parser("inspect", help="Show a job's details")
    inspect_parser.add_argument("job_id")

    failed_parser = commands.add_parser("failed", help="Inspect failed jobs")
    failed_commands = failed_parser.add_subparsers(dest="failed_command", required=True)
    failed_list = failed_commands.add_parser("list", help="List dead-letter jobs")
    failed_list.add_argument("--limit", type=_positive_int, default=20)

    delayed_parser = commands.add_parser("delayed", help="Inspect delayed jobs")
    delayed_commands = delayed_parser.add_subparsers(dest="delayed_command", required=True)
    delayed_list = delayed_commands.add_parser("list", help="List scheduled and retrying jobs")
    delayed_list.add_argument("--limit", type=_positive_int, default=20)

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    redis_client = _create_redis_client()

    if args.command == "worker":
        queues = [name.strip() for name in args.queues.split(",") if name.strip()]
        if not queues:
            parser.error("--queues must contain at least one queue name")
        worker = Worker(
            queue=JobQueue(redis_client),
            consumer_name=args.name,
            queues=queues,
            max_threads=args.threads,
        )
        worker.start()
        return 0

    if args.command == "async-worker":
        queues = [name.strip() for name in args.queues.split(",") if name.strip()]
        if not queues:
            parser.error("--queues must contain at least one queue name")
        return asyncio.run(_run_async_worker(args, queues))

    if args.command == "list":
        _list_status(JobQueue(redis_client), args.status)
        return 0

    if args.command == "job":
        return _inspect_job(JobQueue(redis_client), args.job_id)

    if args.command == "failed":
        _list_failed(DeadLetterQueue(redis_client), args.limit)
        return 0

    if args.command == "delayed":
        _list_delayed(JobQueue(redis_client), DelayedQueue(redis_client), args.limit)
        return 0

    parser.error(f"unsupported command: {args.command}")


async def _run_async_worker(args, queues):
    redis_client = redis.asyncio.Redis(
        host="localhost", port=6379, db=0, decode_responses=True
    )
    try:
        worker = AsyncWorker(
            queue=AsyncJobQueue(redis_client),
            consumer_name=args.name,
            queues=queues,
            concurrency=args.concurrency,
        )
        await worker.start()
    finally:
        await redis_client.aclose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
