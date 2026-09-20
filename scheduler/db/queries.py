"""Every statement the scheduler runs.

The claim, the lease transitions and the materialising insert are concurrency primitives,
not CRUD helpers: the guarantee holds only while there is exactly one of each.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any, cast

from sqlalchemy import (
    ColumnElement,
    DateTime,
    Select,
    String,
    Uuid,
    delete,
    func,
    literal,
    or_,
    select,
    tuple_,
    update,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql.expression import Executable

from ..domain import (
    LEASED_STATES,
    AttemptOutcome,
    ExecutionState,
    ExecutionTrigger,
    JobState,
    ScheduleKind,
)
from ..schedules import CronSchedule, OneOffSchedule, Schedule
from .models import Attempt, Execution, Job, Worker

SCHEDULE_TRIGGER_PREDICATE = Execution.trigger == literal(
    ExecutionTrigger.SCHEDULE.value, String, literal_execute=True
)


async def _affected_rows(session: AsyncSession, statement: Executable) -> int:
    """Session.execute is typed as Result; only CursorResult carries rowcount."""
    result = await session.execute(statement)
    return cast("CursorResult[Any]", result).rowcount


def _lease_until(lease_seconds: int) -> ColumnElement[datetime]:
    return func.now() + timedelta(seconds=lease_seconds)


async def insert_occurrences(
    session: AsyncSession,
    job_id: uuid.UUID,
    occurrences: Sequence[tuple[datetime, datetime | None, ExecutionState]],
) -> int:
    """ON CONFLICT DO NOTHING, so concurrent materialisers cannot double-insert."""
    if not occurrences:
        return 0
    rows = [
        {
            "id": uuid.uuid4(),
            "job_id": job_id,
            "scheduled_for": scheduled_for,
            "shifted_from": shifted_from,
            "state": state,
            "trigger": ExecutionTrigger.SCHEDULE,
        }
        for scheduled_for, shifted_from, state in occurrences
    ]
    stmt = (
        insert(Execution)
        .values(rows)
        .on_conflict_do_nothing(
            index_elements=[Execution.job_id, Execution.scheduled_for],
            index_where=SCHEDULE_TRIGGER_PREDICATE,
        )
        .returning(Execution.id)
    )
    result = await session.execute(stmt)
    return len(result.scalars().all())


async def create_out_of_band(
    session: AsyncSession,
    job_id: uuid.UUID,
    scheduled_for: datetime,
    trigger: ExecutionTrigger,
) -> Execution:
    execution = Execution(
        job_id=job_id,
        scheduled_for=scheduled_for,
        trigger=trigger,
        state=ExecutionState.PENDING,
    )
    session.add(execution)
    await session.flush()
    return execution


async def claim_batch(
    session: AsyncSession,
    *,
    worker_id: str,
    batch_size: int,
    lease_seconds: int,
) -> list[Execution]:
    """SKIP LOCKED: instances take different rows instead of queueing behind each other."""
    due = (
        select(Execution.id)
        .where(
            Execution.state == ExecutionState.PENDING,
            Execution.scheduled_for <= func.now(),
        )
        .order_by(Execution.scheduled_for)
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    stmt = (
        update(Execution)
        .where(Execution.id.in_(due.scalar_subquery()))
        .values(
            state=ExecutionState.CLAIMED,
            claimed_by=worker_id,
            lease_expires_at=_lease_until(lease_seconds),
        )
        .returning(Execution)
        .execution_options(synchronize_session=False, populate_existing=True)
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def start_attempt(
    session: AsyncSession, execution_id: uuid.UUID, worker_id: str
) -> Execution | None:
    """Returns None if the lease was lost between the claim and now."""
    stmt = (
        update(Execution)
        .where(
            Execution.id == execution_id,
            Execution.claimed_by == worker_id,
            Execution.state == ExecutionState.CLAIMED,
        )
        .values(
            state=ExecutionState.RUNNING,
            attempt=Execution.attempt + 1,
            started_at=func.now(),
            lateness_ms=func.cast(
                func.extract("epoch", func.now() - Execution.scheduled_for) * 1000,
                Execution.lateness_ms.type,
            ),
        )
        .returning(Execution)
        .execution_options(synchronize_session=False, populate_existing=True)
    )
    return (await session.execute(stmt)).scalars().one_or_none()


async def extend_lease(
    session: AsyncSession, execution_id: uuid.UUID, worker_id: str, lease_seconds: int
) -> bool:
    """Heartbeat a lease held by this worker. False means the sweeper already took the row."""
    stmt = (
        update(Execution)
        .where(
            Execution.id == execution_id,
            Execution.claimed_by == worker_id,
            Execution.state.in_(LEASED_STATES),
        )
        .values(lease_expires_at=_lease_until(lease_seconds))
        .execution_options(synchronize_session=False)
    )
    return await _affected_rows(session, stmt) == 1


async def sweep_expired_leases(session: AsyncSession, limit: int = 500) -> list[uuid.UUID]:
    expired = (
        select(Execution.id)
        .where(
            Execution.state.in_(LEASED_STATES),
            Execution.lease_expires_at < func.now(),
        )
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    stmt = (
        update(Execution)
        .where(Execution.id.in_(expired.scalar_subquery()))
        .values(
            state=ExecutionState.PENDING,
            claimed_by=None,
            lease_expires_at=None,
            abandoned_count=Execution.abandoned_count + 1,
        )
        .returning(Execution.id)
        .execution_options(synchronize_session=False)
    )
    return list((await session.execute(stmt)).scalars().all())


async def release_claims(session: AsyncSession, worker_id: str) -> list[uuid.UUID]:
    """Hand back claims this worker never started, so a deploy leaves no recovery gap."""
    stmt = (
        update(Execution)
        .where(
            Execution.claimed_by == worker_id,
            Execution.state == ExecutionState.CLAIMED,
        )
        .values(state=ExecutionState.PENDING, claimed_by=None, lease_expires_at=None)
        .returning(Execution.id)
        .execution_options(synchronize_session=False)
    )
    return list((await session.execute(stmt)).scalars().all())


async def finish(
    session: AsyncSession,
    execution_id: uuid.UUID,
    worker_id: str,
    state: ExecutionState,
    error: str | None = None,
) -> bool:
    stmt = (
        update(Execution)
        .where(
            Execution.id == execution_id,
            Execution.claimed_by == worker_id,
            Execution.state.in_(LEASED_STATES),
        )
        .values(
            state=state,
            completed_at=func.now(),
            last_error=error,
            lease_expires_at=None,
        )
        .execution_options(synchronize_session=False)
    )
    return await _affected_rows(session, stmt) == 1


async def reschedule(
    session: AsyncSession,
    execution_id: uuid.UUID,
    worker_id: str,
    delay: timedelta,
    error: str | None = None,
) -> bool:
    """A retry and a queue deferral are both this: the same row, scheduled later."""
    stmt = (
        update(Execution)
        .where(
            Execution.id == execution_id,
            Execution.claimed_by == worker_id,
            Execution.state.in_(LEASED_STATES),
        )
        .values(
            state=ExecutionState.PENDING,
            scheduled_for=func.now() + delay,
            claimed_by=None,
            lease_expires_at=None,
            last_error=error,
        )
        .execution_options(synchronize_session=False)
    )
    return await _affected_rows(session, stmt) == 1


async def record_attempt(
    session: AsyncSession,
    *,
    execution_id: uuid.UUID,
    attempt: int,
    worker_id: str,
    outcome: AttemptOutcome,
    started_at: datetime,
    finished_at: datetime,
    duration_ms: int,
    status_code: int | None,
    error: str | None,
    response_excerpt: str | None,
) -> Attempt:
    row = Attempt(
        execution_id=execution_id,
        attempt=attempt,
        worker_id=worker_id,
        outcome=outcome,
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=duration_ms,
        status_code=status_code,
        error=error,
        response_excerpt=response_excerpt,
    )
    session.add(row)
    await session.flush()
    return row


async def count_earlier_inflight(session: AsyncSession, execution: Execution) -> int:
    """Running executions of this job older than this one.

    Ordered rather than symmetric: two firings claimed together would otherwise skip
    each other and deliver nothing.
    """
    stmt = select(func.count()).where(
        Execution.job_id == execution.job_id,
        Execution.state.in_(LEASED_STATES),
        tuple_(Execution.scheduled_for, Execution.id)
        < tuple_(
            literal(execution.scheduled_for, DateTime(timezone=True)),
            literal(execution.id, Uuid),
        ),
    )
    return (await session.execute(stmt)).scalar_one()


async def count_queued(session: AsyncSession, job_id: uuid.UUID, exclude_id: uuid.UUID) -> int:
    stmt = select(func.count()).where(
        Execution.job_id == job_id,
        Execution.id != exclude_id,
        Execution.state == ExecutionState.PENDING,
        Execution.scheduled_for <= func.now(),
    )
    return (await session.execute(stmt)).scalar_one()


async def get_execution(
    session: AsyncSession, execution_id: uuid.UUID, *, with_attempts: bool = False
) -> Execution | None:
    stmt: Select = select(Execution).where(Execution.id == execution_id)
    if with_attempts:
        stmt = stmt.options(selectinload(Execution.attempts))
    return (await session.execute(stmt)).scalars().one_or_none()


async def delete_pending_for_job(session: AsyncSession, job_id: uuid.UUID) -> int:
    stmt = delete(Execution).where(
        Execution.job_id == job_id,
        Execution.state == ExecutionState.PENDING,
    )
    return await _affected_rows(session, stmt)


async def pending_backlog(session: AsyncSession) -> int:
    stmt = select(func.count()).where(
        Execution.state == ExecutionState.PENDING,
        Execution.scheduled_for <= func.now(),
    )
    return (await session.execute(stmt)).scalar_one()


async def list_executions(
    session: AsyncSession,
    *,
    job_id: uuid.UUID | None = None,
    state: ExecutionState | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[Sequence[Execution], int]:
    filters = []
    if job_id is not None:
        filters.append(Execution.job_id == job_id)
    if state is not None:
        filters.append(Execution.state == state)
    if since is not None:
        filters.append(Execution.scheduled_for >= since)
    if until is not None:
        filters.append(Execution.scheduled_for <= until)

    base = select(Execution).where(*filters)
    total = (
        await session.execute(select(func.count()).select_from(Execution).where(*filters))
    ).scalar_one()
    rows = (
        (
            await session.execute(
                base.order_by(Execution.scheduled_for.desc()).limit(limit).offset(offset)
            )
        )
        .scalars()
        .all()
    )
    return rows, total


async def recent_for_job(
    session: AsyncSession, job_id: uuid.UUID, limit: int = 50
) -> Sequence[Execution]:
    stmt = (
        select(Execution)
        .where(Execution.job_id == job_id)
        .order_by(Execution.scheduled_for.desc())
        .limit(limit)
    )
    return (await session.execute(stmt)).scalars().all()


def schedule_for(job: Job) -> Schedule:
    if job.schedule_kind is ScheduleKind.CRON:
        return CronSchedule(job.cron_expression or "", job.timezone)
    return OneOffSchedule(job.run_at, job.timezone)  # type: ignore[arg-type]


async def create_job(session: AsyncSession, job: Job) -> Job:
    session.add(job)
    await session.flush()
    return job


async def get_job(session: AsyncSession, job_id: uuid.UUID) -> Job | None:
    stmt = select(Job).where(Job.id == job_id)
    return (await session.execute(stmt)).scalars().one_or_none()


async def get_by_idempotency_key(session: AsyncSession, key: str) -> Job | None:
    stmt = select(Job).where(Job.idempotency_key == key)
    return (await session.execute(stmt)).scalars().one_or_none()


async def list_jobs(
    session: AsyncSession,
    *,
    state: JobState | None = None,
    limit: int = 50,
    offset: int = 0,
) -> Sequence[Job]:
    stmt = select(Job).order_by(Job.created_at.desc())
    if state is not None:
        stmt = stmt.where(Job.state == state)
    return (await session.execute(stmt.limit(limit).offset(offset))).scalars().all()


async def count_jobs(session: AsyncSession, *, state: JobState | None = None) -> int:
    stmt = select(func.count()).select_from(Job)
    if state is not None:
        stmt = stmt.where(Job.state == state)
    return (await session.execute(stmt)).scalar_one()


async def set_state(session: AsyncSession, job: Job, state: JobState) -> None:
    job.state = state
    await session.flush()


async def claim_for_materialisation(
    session: AsyncSession, *, horizon: timedelta, limit: int
) -> Sequence[Job]:
    """SKIP LOCKED here is throughput only; correctness comes from the unique index."""
    stmt = (
        select(Job)
        .where(
            Job.state == JobState.ACTIVE,
            or_(
                Job.materialised_through.is_(None),
                Job.materialised_through < func.now() + horizon,
            ),
        )
        .order_by(Job.materialised_through.asc().nullsfirst())
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    return (await session.execute(stmt)).scalars().all()


async def advance_materialised_through(
    session: AsyncSession, job_id: uuid.UUID, through: datetime
) -> None:
    stmt = (
        update(Job)
        .where(Job.id == job_id)
        .values(
            materialised_through=func.greatest(
                func.coalesce(Job.materialised_through, through), through
            )
        )
        .execution_options(synchronize_session=False)
    )
    await session.execute(stmt)


async def heartbeat(
    session: AsyncSession,
    *,
    worker_id: str,
    version: str,
    local_now: datetime,
    claimed_count: int,
    shutting_down: bool = False,
) -> int:
    """Drift is measured, not corrected: the claim query already uses the database clock."""
    local = literal(local_now, DateTime(timezone=True))
    drift = func.cast(func.extract("epoch", local - func.now()) * 1000, Worker.clock_drift_ms.type)
    values = {
        "id": worker_id,
        "version": version,
        "started_at": func.now(),
        "last_heartbeat": func.now(),
        "clock_drift_ms": drift,
        "claimed_count": claimed_count,
        "shutting_down": shutting_down,
    }
    stmt = (
        insert(Worker)
        .values(values)
        .on_conflict_do_update(
            index_elements=[Worker.id],
            set_={
                "last_heartbeat": func.now(),
                "clock_drift_ms": drift,
                "claimed_count": claimed_count,
                "shutting_down": shutting_down,
                "version": version,
            },
        )
        .returning(Worker.clock_drift_ms)
    )
    return (await session.execute(stmt)).scalar_one()


async def deregister(session: AsyncSession, worker_id: str) -> None:
    await session.execute(delete(Worker).where(Worker.id == worker_id))


async def list_live(session: AsyncSession, *, stale_after: timedelta) -> Sequence[Worker]:
    stmt = (
        select(Worker).where(Worker.last_heartbeat > func.now() - stale_after).order_by(Worker.id)
    )
    return (await session.execute(stmt)).scalars().all()


async def prune_stale(session: AsyncSession, *, stale_after: timedelta) -> int:
    stmt = delete(Worker).where(Worker.last_heartbeat < func.now() - stale_after)
    return await _affected_rows(session, stmt)
