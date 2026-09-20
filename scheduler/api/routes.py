"""HTTP handlers: jobs, executions, and the operational endpoints."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, Header, Query, Request, Response, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import db_now, queries
from ..db.models import Execution, Job
from ..domain import ConflictError, ExecutionState, ExecutionTrigger, JobState, NotFoundError
from ..engine import WORKER_STALE_MULTIPLIER
from ..jobs import apply_update, build_job, next_execution_at, upcoming_occurrences
from ..schemas import (
    AttemptRead,
    ClusterView,
    ExecutionRead,
    JobCreate,
    JobDetail,
    JobRead,
    JobUpdate,
    Page,
    WorkerRead,
)
from .utils import SessionDep, SettingsDep, enforce_create_rate_limit, require_api_key

jobs_router = APIRouter(prefix="/v1/jobs", tags=["jobs"])


async def _load_job(session: AsyncSession, job_id: uuid.UUID) -> Job:
    job = await queries.get_job(session, job_id)
    if job is None:
        raise NotFoundError(f"job {job_id} not found")
    return job


def _read(job: Job, *, next_at: datetime | None = None) -> JobRead:
    model = JobRead.model_validate(job)
    model.next_execution_at = next_at
    return model


@jobs_router.post("", status_code=status.HTTP_201_CREATED, response_model=JobRead)
async def create_job(
    request: Request,
    payload: JobCreate,
    session: SessionDep,
    settings: SettingsDep,
    response: Response,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> JobRead:
    enforce_create_rate_limit(request)

    if idempotency_key:
        existing = await queries.get_by_idempotency_key(session, idempotency_key)
        if existing is not None:
            response.status_code = status.HTTP_200_OK
            return _read(existing, next_at=next_execution_at(existing, after=await db_now(session)))

    job = await build_job(payload, settings)
    job.idempotency_key = idempotency_key
    await queries.create_job(session, job)
    await session.commit()
    return _read(job, next_at=next_execution_at(job, after=await db_now(session)))


@jobs_router.get("", response_model=Page[JobRead])
async def list_jobs(
    session: SessionDep,
    state: JobState | None = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> Page[JobRead]:
    jobs = await queries.list_jobs(session, state=state, limit=limit, offset=offset)
    total = await queries.count_jobs(session, state=state)
    now = await db_now(session)
    return Page[JobRead](
        items=[_read(job, next_at=next_execution_at(job, after=now)) for job in jobs],
        total=total,
        limit=limit,
        offset=offset,
    )


@jobs_router.get("/{job_id}", response_model=JobDetail)
async def get_job(job_id: uuid.UUID, session: SessionDep) -> JobDetail:
    job = await _load_job(session, job_id)
    now = await db_now(session)
    recent = await queries.recent_for_job(session, job.id, limit=50)
    detail = JobDetail.model_validate(job)
    detail.upcoming = upcoming_occurrences(job, after=now, count=20)
    detail.next_execution_at = detail.upcoming[0] if detail.upcoming else None
    detail.recent_executions = [ExecutionRead.model_validate(row) for row in recent]
    return detail


@jobs_router.patch("/{job_id}", response_model=JobRead)
async def update_job(
    job_id: uuid.UUID,
    payload: JobUpdate,
    session: SessionDep,
    settings: SettingsDep,
) -> JobRead:
    job = await _load_job(session, job_id)
    await apply_update(job, payload, settings)
    await session.commit()
    return _read(job, next_at=next_execution_at(job, after=await db_now(session)))


@jobs_router.delete("/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
async def cancel_job(job_id: uuid.UUID, session: SessionDep) -> Response:
    job = await _load_job(session, job_id)
    job.state = JobState.CANCELLED
    await queries.delete_pending_for_job(session, job.id)
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@jobs_router.post("/{job_id}/pause", response_model=JobRead)
async def pause_job(job_id: uuid.UUID, session: SessionDep) -> JobRead:
    job = await _load_job(session, job_id)
    if job.state is JobState.CANCELLED:
        raise ConflictError("a cancelled job cannot be paused")
    job.state = JobState.PAUSED
    await queries.delete_pending_for_job(session, job.id)
    await session.commit()
    return _read(job)


@jobs_router.post("/{job_id}/resume", response_model=JobRead)
async def resume_job(job_id: uuid.UUID, session: SessionDep) -> JobRead:
    job = await _load_job(session, job_id)
    if job.state is JobState.CANCELLED:
        raise ConflictError("a cancelled job cannot be resumed")
    job.state = JobState.ACTIVE
    job.materialised_through = await db_now(session)
    await session.commit()
    return _read(job, next_at=next_execution_at(job, after=job.materialised_through))


@jobs_router.post(
    "/{job_id}/trigger",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ExecutionRead,
)
async def trigger_job(job_id: uuid.UUID, session: SessionDep) -> ExecutionRead:
    job = await _load_job(session, job_id)
    if job.state is JobState.CANCELLED:
        raise ConflictError("a cancelled job cannot be triggered")
    execution = await queries.create_out_of_band(
        session, job.id, await db_now(session), ExecutionTrigger.MANUAL
    )
    await session.commit()
    return ExecutionRead.model_validate(execution)


executions_router = APIRouter(prefix="/v1/executions", tags=["executions"])


REPLAYABLE_STATES = (ExecutionState.DEAD, ExecutionState.SKIPPED, ExecutionState.MISSED)


async def _load_execution(session: AsyncSession, execution_id: uuid.UUID) -> tuple[Execution, Job]:
    execution = await queries.get_execution(session, execution_id)
    if execution is None:
        raise NotFoundError(f"execution {execution_id} not found")
    job = await queries.get_job(session, execution.job_id)
    if job is None:
        raise NotFoundError(f"execution {execution_id} not found")
    return execution, job


@executions_router.get("", response_model=Page[ExecutionRead])
async def list_executions(
    session: SessionDep,
    job_id: uuid.UUID | None = None,
    state: ExecutionState | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> Page[ExecutionRead]:
    rows, total = await queries.list_executions(
        session,
        job_id=job_id,
        state=state,
        since=since,
        until=until,
        limit=limit,
        offset=offset,
    )
    return Page[ExecutionRead](
        items=[ExecutionRead.model_validate(row) for row in rows],
        total=total,
        limit=limit,
        offset=offset,
    )


@executions_router.get("/{execution_id}", response_model=ExecutionRead)
async def get_execution(execution_id: uuid.UUID, session: SessionDep) -> ExecutionRead:
    execution, _ = await _load_execution(session, execution_id)
    return ExecutionRead.model_validate(execution)


@executions_router.get("/{execution_id}/attempts", response_model=list[AttemptRead])
async def get_attempts(execution_id: uuid.UUID, session: SessionDep) -> list[AttemptRead]:
    await _load_execution(session, execution_id)
    execution = await queries.get_execution(session, execution_id, with_attempts=True)
    assert execution is not None
    return [AttemptRead.model_validate(attempt) for attempt in execution.attempts]


@executions_router.post(
    "/{execution_id}/replay",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ExecutionRead,
)
async def replay_execution(execution_id: uuid.UUID, session: SessionDep) -> ExecutionRead:
    execution, job = await _load_execution(session, execution_id)
    if execution.state not in REPLAYABLE_STATES:
        raise ConflictError(f"execution in state {execution.state.value} cannot be replayed")
    replay = await queries.create_out_of_band(
        session, job.id, await db_now(session), ExecutionTrigger.REPLAY
    )
    await session.commit()
    return ExecutionRead.model_validate(replay)


ops_router = APIRouter(tags=["ops"])


@ops_router.get("/healthz")
async def healthz() -> dict[str, str]:
    """Liveness only — deliberately does not touch the database."""
    return {"status": "ok"}


@ops_router.get("/readyz")
async def readyz(session: SessionDep, response: Response) -> dict[str, str]:
    try:
        await session.execute(text("SELECT 1"))
    except Exception as exc:
        response.status_code = 503
        return {"status": "unavailable", "detail": str(exc)}
    return {"status": "ready"}


@ops_router.get("/v1/cluster", response_model=ClusterView, dependencies=[Depends(require_api_key)])
async def cluster(session: SessionDep, settings: SettingsDep) -> ClusterView:
    stale_after = timedelta(seconds=settings.heartbeat_interval_seconds * WORKER_STALE_MULTIPLIER)
    workers = await queries.list_live(session, stale_after=stale_after)
    return ClusterView(
        workers=[WorkerRead.model_validate(worker) for worker in workers],
        pending_backlog=await queries.pending_backlog(session),
    )


__all__ = ["executions_router", "jobs_router", "ops_router"]
