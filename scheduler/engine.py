"""The loops every instance runs: materialise, claim, sweep.

No leader and no coordinator: every instance runs all three, and the claim query decides
who does what.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from . import __version__
from .config import Settings
from .db import Database, db_now, queries
from .db.models import Job
from .delivery import Dispatcher, HttpDeliverer
from .domain import ExecutionState, JobState

logger = logging.getLogger(__name__)


MAX_ERROR_BACKOFF_SECONDS = 30.0


async def run_periodically(
    name: str,
    interval: float,
    body: Callable[[], Awaitable[object]],
    stop: asyncio.Event,
) -> None:
    """Failures are logged and backed off, never raised: a blip must not disarm the loop."""
    failures = 0
    while not stop.is_set():
        try:
            await body()
            failures = 0
            delay = interval
        except asyncio.CancelledError:
            raise
        except Exception:
            failures += 1
            delay = min(interval * (2**failures), MAX_ERROR_BACKOFF_SECONDS)
            logger.exception("loop_iteration_failed", extra={"loop": name, "backoff": delay})
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except TimeoutError:
            continue


class Materialiser:
    def __init__(self, db: Database, settings: Settings) -> None:
        self._db = db
        self._settings = settings

    async def run_once(self, *, job_limit: int = 100) -> int:
        horizon = timedelta(seconds=self._settings.materialise_horizon_seconds)
        created = 0
        async with self._db.transaction() as session:
            now = await db_now(session)
            jobs = await queries.claim_for_materialisation(
                session, horizon=horizon, limit=job_limit
            )
            for job in jobs:
                created += await self.materialise_job(session, job, now=now)
        return created

    async def materialise_job(self, session: AsyncSession, job: Job, *, now: datetime) -> int:
        horizon_end = now + timedelta(seconds=self._settings.materialise_horizon_seconds)
        schedule = queries.schedule_for(job)
        cursor = job.materialised_through or job.created_at
        replayed = 0
        rows: list[tuple[datetime, datetime | None, ExecutionState]] = []

        for _ in range(self._settings.materialise_max_occurrences):
            occurrence = schedule.next_after(cursor)
            if occurrence is None:
                await queries.set_state(session, job, JobState.COMPLETED)
                break
            if occurrence.scheduled_for > horizon_end:
                break
            cursor = occurrence.scheduled_for

            state = ExecutionState.PENDING
            if occurrence.scheduled_for < now:
                if job.catchup and replayed < job.catchup_limit:
                    replayed += 1
                else:
                    state = ExecutionState.MISSED
            rows.append((occurrence.scheduled_for, occurrence.shifted_from, state))

        inserted = await queries.insert_occurrences(session, job.id, rows)
        await queries.advance_materialised_through(session, job.id, max(cursor, now))
        if inserted:
            logger.info(
                "materialised",
                extra={
                    "job_id": str(job.id),
                    "executions_created": inserted,
                    "materialised_through": cursor.isoformat(),
                },
            )
        return inserted


WORKER_STALE_MULTIPLIER = 6


class Sweeper:
    def __init__(self, db: Database, settings: Settings) -> None:
        self._db = db
        self._settings = settings

    async def run_once(self) -> int:
        stale_after = timedelta(
            seconds=self._settings.heartbeat_interval_seconds * WORKER_STALE_MULTIPLIER
        )
        async with self._db.transaction() as session:
            reclaimed = await queries.sweep_expired_leases(session)
            await queries.prune_stale(session, stale_after=stale_after)

        if reclaimed:
            logger.warning(
                "leases_expired",
                extra={"count": len(reclaimed), "execution_ids": [str(i) for i in reclaimed]},
            )
        return len(reclaimed)


class WorkerRuntime:
    def __init__(
        self,
        db: Database,
        settings: Settings,
        *,
        deliverer: HttpDeliverer | None = None,
        enable_materialiser: bool = True,
        enable_sweeper: bool = True,
    ) -> None:
        self._db = db
        self._settings = settings
        self._deliverer = deliverer or HttpDeliverer(
            response_excerpt_bytes=settings.response_excerpt_bytes
        )
        self._dispatcher = Dispatcher(db, settings, self._deliverer)
        self._materialiser = Materialiser(db, settings)
        self._sweeper = Sweeper(db, settings)
        self._enable_materialiser = enable_materialiser
        self._enable_sweeper = enable_sweeper

        self._stop = asyncio.Event()
        self._stopping = False
        self._loops: list[asyncio.Task[None]] = []
        self._dispatches: set[asyncio.Task[None]] = set()

    @property
    def worker_id(self) -> str:
        return self._settings.worker_id

    @property
    def inflight(self) -> int:
        return len(self._dispatches)

    async def start(self) -> None:
        await self._heartbeat_once()
        self._loops = [
            asyncio.create_task(
                run_periodically(
                    "claimer", self._settings.poll_interval_seconds, self.claim_once, self._stop
                )
            ),
            asyncio.create_task(
                run_periodically(
                    "heartbeat",
                    self._settings.heartbeat_interval_seconds,
                    self._heartbeat_once,
                    self._stop,
                )
            ),
        ]
        if self._enable_materialiser:
            self._loops.append(
                asyncio.create_task(
                    run_periodically(
                        "materialiser",
                        self._settings.materialise_interval_seconds,
                        self._materialiser.run_once,
                        self._stop,
                    )
                )
            )
        if self._enable_sweeper:
            self._loops.append(
                asyncio.create_task(
                    run_periodically(
                        "sweeper",
                        self._settings.sweep_interval_seconds,
                        self._sweeper.run_once,
                        self._stop,
                    )
                )
            )
        logger.info("worker_started", extra={"worker_id": self.worker_id})

    async def claim_once(self) -> int:
        capacity = self._settings.max_inflight_dispatches - len(self._dispatches)
        batch_size = min(self._settings.claim_batch_size, capacity)
        if batch_size <= 0 or self._stop.is_set():
            return 0

        async with self._db.transaction() as session:
            claimed = await queries.claim_batch(
                session,
                worker_id=self.worker_id,
                batch_size=batch_size,
                lease_seconds=self._settings.lease_seconds,
            )

        for execution in claimed:
            self._spawn_dispatch(execution.id)
        return len(claimed)

    def _spawn_dispatch(self, execution_id: uuid.UUID) -> None:
        task = asyncio.create_task(self._dispatch(execution_id))
        self._dispatches.add(task)
        task.add_done_callback(self._dispatches.discard)

    async def _dispatch(self, execution_id: uuid.UUID) -> None:
        try:
            await self._dispatcher.run(execution_id)
        except Exception:
            logger.exception("dispatch_failed", extra={"execution_id": str(execution_id)})

    async def _heartbeat_once(self) -> None:
        async with self._db.transaction() as session:
            await queries.heartbeat(
                session,
                worker_id=self.worker_id,
                version=__version__,
                local_now=datetime.now(UTC),
                claimed_count=len(self._dispatches),
                shutting_down=self._stop.is_set(),
            )

    async def stop(self) -> None:
        """SIGTERM: stop claiming, release unstarted claims, drain what is in flight."""
        if self._stopping:
            return
        self._stopping = True
        self._stop.set()
        for loop in self._loops:
            loop.cancel()
        for loop in self._loops:
            with contextlib.suppress(asyncio.CancelledError):
                await loop

        async with self._db.transaction() as session:
            released = await queries.release_claims(session, self.worker_id)

        if self._dispatches:
            done, pending = await asyncio.wait(
                self._dispatches, timeout=self._settings.shutdown_grace_seconds
            )
            for task in pending:
                task.cancel()
            logger.info(
                "drained_dispatches", extra={"finished": len(done), "abandoned": len(pending)}
            )

        async with self._db.transaction() as session:
            await queries.deregister(session, self.worker_id)
        await self._deliverer.aclose()
        logger.info(
            "worker_stopped", extra={"worker_id": self.worker_id, "released": len(released)}
        )

    async def run_forever(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._stop.set)
        await self.start()
        await self._stop.wait()
        await self.stop()
