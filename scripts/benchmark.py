"""Load harness: measures throughput and scheduling lateness as instances are added.

createdb scheduler_bench && DATABASE_URL=...scheduler_bench alembic upgrade head
python scripts/benchmark.py --executions 2000 --scale 1,2,4,8
"""

from __future__ import annotations

import argparse
import asyncio
import os
import statistics
import sys
import time
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import select, text

from scheduler.db import Database, create_database, db_now, queries
from scheduler.db.models import Execution, Job
from scheduler.domain import ExecutionState, OverlapPolicy, ScheduleKind
from scheduler.testing import Receiver


@dataclass(frozen=True, slots=True)
class Result:
    instances: int
    executions: int
    seconds: float
    p50_ms: float
    p95_ms: float
    p99_ms: float

    @property
    def throughput(self) -> float:
        return self.executions / self.seconds


async def reset(db: Database) -> None:
    async with db.transaction() as session:
        await session.execute(
            text("TRUNCATE attempts, executions, jobs, workers RESTART IDENTITY CASCADE")
        )


async def seed(db: Database, endpoint: str, count: int) -> None:
    async with db.transaction() as session:
        job = Job(
            name="benchmark",
            schedule_kind=ScheduleKind.CRON,
            cron_expression="* * * * *",
            endpoint=endpoint,
            method="POST",
            headers={},
            body="{}",
            overlap_policy=OverlapPolicy.ALLOW,
            max_attempts=1,
        )
        await queries.create_job(session, job)
        now = await db_now(session)
        await queries.insert_occurrences(
            session,
            job.id,
            [
                (now - timedelta(milliseconds=count - index), None, ExecutionState.PENDING)
                for index in range(count)
            ],
        )


async def lateness_percentiles(db: Database) -> tuple[float, float, float]:
    async with db.session() as session:
        rows = (
            (
                await session.execute(
                    select(Execution.lateness_ms).where(Execution.lateness_ms.is_not(None))
                )
            )
            .scalars()
            .all()
        )
    ordered = sorted(float(value) for value in rows)
    if len(ordered) < 100:
        return (0.0, 0.0, 0.0)
    quantiles = statistics.quantiles(ordered, n=100)
    return (quantiles[49], quantiles[94], quantiles[98])


async def spawn_worker(url: str, worker_id: str) -> asyncio.subprocess.Process:
    """Workers are separate processes on purpose: N asyncio loops inside one interpreter
    share one core, and the scaling curve would measure the GIL rather than the database."""
    env = {
        **os.environ,
        "DATABASE_URL": url,
        "SCHEDULER_WORKER_ID": worker_id,
        "SCHEDULER_POLL_INTERVAL_SECONDS": "0.05",
        "SCHEDULER_CLAIM_BATCH_SIZE": "50",
        "SCHEDULER_MAX_INFLIGHT_DISPATCHES": "64",
        "SCHEDULER_LEASE_SECONDS": "30",
        "SCHEDULER_ALLOW_PRIVATE_ENDPOINTS": "true",
        "SCHEDULER_LOG_LEVEL": "WARNING",
    }
    return await asyncio.create_subprocess_exec(
        sys.executable, "-m", "scheduler.cli", "worker", "--no-materialiser", env=env
    )


async def run_once(url: str, instances: int, executions: int) -> Result:
    control = create_database(url)
    await reset(control)

    async with Receiver() as receiver:
        await seed(control, receiver.url, executions)

        started = time.monotonic()
        workers = [await spawn_worker(url, f"bench-{index}") for index in range(instances)]
        try:
            await receiver.wait_for_count(executions)
            elapsed = time.monotonic() - started
        finally:
            for worker in workers:
                worker.terminate()
            await asyncio.gather(*(worker.wait() for worker in workers))

    p50, p95, p99 = await lateness_percentiles(control)
    await control.dispose()
    return Result(instances, executions, elapsed, p50, p95, p99)


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url",
        default="postgresql://scheduler:scheduler@localhost:5432/scheduler_bench",
    )
    parser.add_argument("--executions", type=int, default=2_000)
    parser.add_argument("--scale", default="1,2,4,8", help="comma-separated instance counts")
    args = parser.parse_args()

    print(f"{'instances':>9}  {'deliveries/s':>12}  {'p50 ms':>8}  {'p95 ms':>8}  {'p99 ms':>8}")
    for instances in [int(value) for value in args.scale.split(",")]:
        result = await run_once(args.database_url, instances, args.executions)
        print(
            f"{result.instances:>9}  {result.throughput:>12.1f}  "
            f"{result.p50_ms:>8.0f}  {result.p95_ms:>8.0f}  {result.p99_ms:>8.0f}"
        )


if __name__ == "__main__":
    asyncio.run(main())
