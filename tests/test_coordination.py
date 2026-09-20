"""N instances, one database, every due execution delivered exactly once.

The project's headline claim and the failure modes around it: concurrent claims, a lease held
by a slow worker, a worker killed mid-dispatch, and a worker asked to stop politely.
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys
import uuid
from datetime import timedelta

import pytest

from scheduler.config import Settings
from scheduler.db import Database, create_database, db_now, queries
from scheduler.delivery import Dispatcher, HttpDeliverer
from scheduler.domain import TERMINAL_STATES, ExecutionState
from scheduler.engine import Sweeper, WorkerRuntime
from scheduler.testing import Receiver
from tests.conftest import cron_job, due_now, due_now_fleet, wait_for

pytestmark = pytest.mark.db


async def test_concurrent_claimers_take_every_row_exactly_once(db: Database) -> None:
    total = 1_000
    await due_now(db, cron_job(), count=total)

    async def worker(worker_id: str) -> list[uuid.UUID]:
        taken: list[uuid.UUID] = []
        while True:
            async with db.transaction() as session:
                batch = await queries.claim_batch(
                    session, worker_id=worker_id, batch_size=50, lease_seconds=60
                )
            if not batch:
                return taken
            taken.extend(execution.id for execution in batch)
            await asyncio.sleep(0)

    results = await asyncio.gather(*(worker(f"w{i}") for i in range(10)))

    claimed = [execution_id for result in results for execution_id in result]
    assert len(claimed) == total, "a row was claimed more than once"
    assert len(set(claimed)) == total, "duplicate claims"
    assert sum(1 for result in results if result) > 1, "SKIP LOCKED should spread the work"


async def test_the_heartbeat_keeps_a_long_dispatch_safe(db: Database, settings: Settings) -> None:
    """The fix: an execution taking several lease durations still completes exactly once."""
    settings.lease_seconds = 1
    settings.heartbeat_interval_seconds = 0.2

    async with Receiver(delay_seconds=3.0) as receiver:
        await due_now(db, cron_job(endpoint=receiver.url))
        dispatcher = Dispatcher(db, settings, HttpDeliverer())
        sweeper = Sweeper(db, settings)

        async with db.transaction() as session:
            claimed = await queries.claim_batch(
                session, worker_id=settings.worker_id, batch_size=1, lease_seconds=1
            )
        dispatch = asyncio.create_task(dispatcher.run(claimed[0].id))

        swept = 0
        while not dispatch.done():
            swept += await sweeper.run_once()
            await asyncio.sleep(0.2)
        await dispatch

        async with db.session() as session:
            execution = await queries.get_execution(session, claimed[0].id)

    assert swept == 0
    assert receiver.count == 1
    assert execution is not None and execution.state is ExecutionState.SUCCEEDED


REPEATS = int(os.environ.get("SCHEDULER_TEST_REPEATS", "1"))
JOBS = int(os.environ.get("SCHEDULER_TEST_JOBS", "200"))


@pytest.mark.parametrize("instances", [1, 2, 10])
@pytest.mark.parametrize("run", range(REPEATS))
async def test_every_due_execution_is_delivered_exactly_once(
    db: Database, database_url: str, settings: Settings, instances: int, run: int
) -> None:
    async with Receiver() as receiver:
        await due_now_fleet(db, endpoint=receiver.url, count=JOBS)

        databases = [create_database(database_url, pool_max_size=5) for _ in range(instances)]
        runtimes = [
            WorkerRuntime(
                database,
                settings.model_copy(update={"worker_id": f"worker-{index}"}),
                enable_materialiser=False,
            )
            for index, database in enumerate(databases)
        ]
        try:
            await asyncio.gather(*(runtime.start() for runtime in runtimes))

            async def all_delivered() -> bool:
                return receiver.count >= JOBS

            await wait_for(all_delivered, timeout=60)
            await asyncio.sleep(1.0)
        finally:
            await asyncio.gather(*(runtime.stop() for runtime in runtimes))
            await asyncio.gather(*(database.dispose() for database in databases))

        keys = receiver.keys()

    async with db.session() as session:
        rows, total = await queries.list_executions(session, limit=JOBS)

    assert receiver.count == JOBS, "a duplicate delivery escaped the claim query"
    assert len(set(keys)) == JOBS, "two workers ran the same execution"
    assert total == JOBS
    assert all(row.state is ExecutionState.SUCCEEDED for row in rows)
    assert all(row.attempt == 1 for row in rows)
    assert len({row.claimed_by for row in rows}) == instances, "work should spread across workers"


EXECUTIONS = 20


async def spawn_worker(
    database_url: str, worker_id: str, *, lease_seconds: int
) -> asyncio.subprocess.Process:
    env = {
        **os.environ,
        "DATABASE_URL": database_url,
        "SCHEDULER_WORKER_ID": worker_id,
        "SCHEDULER_LEASE_SECONDS": str(lease_seconds),
        "SCHEDULER_POLL_INTERVAL_SECONDS": "0.1",
        "SCHEDULER_HEARTBEAT_INTERVAL_SECONDS": "0.5",
        "SCHEDULER_SWEEP_INTERVAL_SECONDS": "0.5",
        "SCHEDULER_ALLOW_PRIVATE_ENDPOINTS": "true",
        "SCHEDULER_LOG_LEVEL": "WARNING",
    }
    return await asyncio.create_subprocess_exec(
        sys.executable, "-m", "scheduler.cli", "worker", "--no-materialiser", env=env
    )


async def terminal_states(db: Database) -> list[ExecutionState]:
    async with db.session() as session:
        rows, _ = await queries.list_executions(session, limit=100)
    return [row.state for row in rows]


@pytest.mark.chaos
async def test_sigkill_mid_execution_loses_no_work(
    db: Database, database_url: str, settings: Settings
) -> None:
    """Criterion 2: a killed worker's claims come back and are delivered."""
    async with Receiver(delay_seconds=1.0) as receiver:
        async with db.transaction() as session:
            job = await queries.create_job(
                session,
                cron_job(endpoint=receiver.url, overlap_policy="allow"),  # type: ignore[arg-type]
            )
            now = await db_now(session)
            await queries.insert_occurrences(
                session,
                job.id,
                [
                    (now - timedelta(seconds=EXECUTIONS - i), None, ExecutionState.PENDING)
                    for i in range(EXECUTIONS)
                ],
            )

        doomed = await spawn_worker(database_url, "doomed", lease_seconds=2)
        try:

            async def in_flight() -> bool:
                return receiver.count >= 5

            await wait_for(in_flight, timeout=30)
            doomed.kill()
            await doomed.wait()
        finally:
            if doomed.returncode is None:
                doomed.kill()

        assert doomed.returncode == -signal.SIGKILL

        rescuer = await spawn_worker(database_url, "rescuer", lease_seconds=5)
        try:

            async def all_settled() -> bool:
                states = await terminal_states(db)
                return len(states) == EXECUTIONS and all(s in TERMINAL_STATES for s in states)

            await wait_for(all_settled, timeout=60)
        finally:
            rescuer.terminate()
            await rescuer.wait()

        states = await terminal_states(db)
        keys = receiver.keys()

    assert all(state is ExecutionState.SUCCEEDED for state in states), "work was lost"
    assert len(set(keys)) == EXECUTIONS, "an execution was never delivered"
    assert receiver.count >= EXECUTIONS


@pytest.mark.chaos
async def test_sigterm_releases_claims_immediately(
    db: Database, database_url: str, settings: Settings
) -> None:
    """The deploy path: a graceful stop should not park work for a lease duration."""
    async with Receiver(delay_seconds=0.2) as receiver:
        await due_now(db, cron_job(endpoint=receiver.url), count=EXECUTIONS)

        worker = await spawn_worker(database_url, "deploying", lease_seconds=300)
        try:

            async def started() -> bool:
                return receiver.count >= 1

            await wait_for(started, timeout=30)
            worker.terminate()
            await asyncio.wait_for(worker.wait(), timeout=30)
        finally:
            if worker.returncode is None:
                worker.kill()

        async with db.session() as session:
            rows, _ = await queries.list_executions(session, limit=100)

        assert all(row.state in TERMINAL_STATES or row.claimed_by is None for row in rows), (
            "claims were left leased after a graceful shutdown"
        )
