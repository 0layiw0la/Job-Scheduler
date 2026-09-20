"""Fixtures, object mothers and waiting helpers for the suite."""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from scheduler.config import Settings
from scheduler.db import Database, create_database, db_now, queries
from scheduler.db.models import Base, Execution, Job
from scheduler.domain import (
    TERMINAL_STATES,
    BackoffStrategy,
    ExecutionState,
    OverlapPolicy,
    ScheduleKind,
)


class FrozenClock:
    """A manually advanced clock, so schedule tests can jump across days without waiting.

    Only tests need one: the engine reads time from the database, never from the process.
    """

    def __init__(self, start: datetime) -> None:
        self._now = start.astimezone(UTC)

    def now(self) -> datetime:
        return self._now

    def set(self, moment: datetime) -> datetime:
        self._now = moment.astimezone(UTC)
        return self._now


DEFAULT_TEST_URL = "postgresql://scheduler:scheduler@localhost:5432/scheduler_test"

TABLES = ("attempts", "executions", "jobs", "workers")


def test_database_url() -> str:
    return os.environ.get("TEST_DATABASE_URL", DEFAULT_TEST_URL)


@pytest.fixture(scope="session")
def database_url() -> str:
    return test_database_url()


@pytest.fixture(scope="session")
def schema(database_url: str) -> None:
    """Build the schema once per session from the models.

    Synchronous on purpose: a session-scoped async fixture would pin every test to one event
    loop. Migrations are checked separately, in tests/test_migrations.py.
    """

    async def build() -> None:
        db = create_database(database_url)
        async with db.engine.begin() as connection:
            await connection.run_sync(Base.metadata.drop_all)
            await connection.run_sync(Base.metadata.create_all)
        await db.dispose()

    asyncio.run(build())


@pytest.fixture
async def db(database_url: str, schema: None) -> AsyncIterator[Database]:
    database = create_database(database_url, pool_max_size=25)
    async with database.transaction() as session:
        await session.execute(text(f"TRUNCATE {', '.join(TABLES)} RESTART IDENTITY CASCADE"))
    yield database
    await database.dispose()


@pytest.fixture
def settings(database_url: str) -> Settings:
    return Settings(
        database_url=database_url,
        worker_id="test-worker",
        poll_interval_seconds=0.05,
        heartbeat_interval_seconds=0.2,
        lease_seconds=2,
        sweep_interval_seconds=0.1,
        materialise_interval_seconds=0.1,
        claim_batch_size=50,
        max_inflight_dispatches=16,
        allow_private_endpoints=True,
        shutdown_grace_seconds=5,
    )


@pytest.fixture
async def worker(db: Database, settings: Settings) -> AsyncIterator[object]:
    """A running worker with the materialiser off, so tests control what is due."""
    from scheduler.engine import WorkerRuntime

    runtime = WorkerRuntime(db, settings, enable_materialiser=False)
    await runtime.start()
    try:
        yield runtime
    finally:
        await runtime.stop()


def cron_job(
    *,
    name: str = "job",
    cron: str = "* * * * *",
    timezone: str = "UTC",
    endpoint: str = "http://127.0.0.1:1/hook",
    overlap_policy: OverlapPolicy = OverlapPolicy.SKIP,
    max_attempts: int = 3,
    backoff_strategy: BackoffStrategy = BackoffStrategy.CONSTANT,
    backoff_base_ms: int = 10,
    backoff_max_delay_ms: int = 100,
    catchup: bool = False,
    catchup_limit: int = 10,
    queue_depth_limit: int = 1,
    read_timeout_ms: int = 5_000,
    materialised_through: datetime | None = None,
) -> Job:
    return Job(
        name=name,
        schedule_kind=ScheduleKind.CRON,
        cron_expression=cron,
        timezone=timezone,
        endpoint=endpoint,
        method="POST",
        headers={},
        body='{"ping": true}',
        overlap_policy=overlap_policy,
        max_attempts=max_attempts,
        backoff_strategy=backoff_strategy,
        backoff_base_ms=backoff_base_ms,
        backoff_max_delay_ms=backoff_max_delay_ms,
        catchup=catchup,
        catchup_limit=catchup_limit,
        queue_depth_limit=queue_depth_limit,
        read_timeout_ms=read_timeout_ms,
        materialised_through=materialised_through,
    )


async def due_now(db: Database, job, *, count: int = 1) -> list[uuid.UUID]:
    """Persist `job` with `count` executions already due."""
    async with db.transaction() as session:
        await queries.create_job(session, job)
        now = await db_now(session)
        await queries.insert_occurrences(
            session,
            job.id,
            [
                (now - timedelta(seconds=count - i), None, ExecutionState.PENDING)
                for i in range(count)
            ],
        )
        rows = await queries.recent_for_job(session, job.id, limit=count)
        return [row.id for row in rows]


async def due_now_fleet(db: Database, *, endpoint: str, count: int) -> None:
    """Persist `count` separate jobs, each with one execution due now — the shape the
    exactly-once test needs, where every delivery belongs to a different job."""
    async with db.transaction() as session:
        now = await db_now(session)
        for index in range(count):
            job = cron_job(name=f"job-{index}", endpoint=endpoint)
            await queries.create_job(session, job)
            await queries.insert_occurrences(session, job.id, [(now, None, ExecutionState.PENDING)])


async def wait_for[T](
    condition: Callable[[], Awaitable[T | None]],
    *,
    timeout: float = 10.0,  # noqa: ASYNC109 - a polling helper; the timeout is the assertion
    interval: float = 0.05,
) -> T:
    """Poll until `condition` returns something truthy, or fail the test."""
    try:
        async with asyncio.timeout(timeout):
            while True:
                result = await condition()
                if result:
                    return result
                await asyncio.sleep(interval)
    except TimeoutError:
        raise AssertionError(f"condition not met within {timeout}s") from None


async def wait_for_terminal(db: Database, execution_id: uuid.UUID, **kwargs: float) -> Execution:
    async def check() -> Execution | None:
        async with db.session() as session:
            execution = await queries.get_execution(session, execution_id, with_attempts=True)
        if execution is not None and execution.state in TERMINAL_STATES:
            return execution
        return None

    return await wait_for(check, **kwargs)
