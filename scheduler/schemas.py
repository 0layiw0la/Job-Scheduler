from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .domain import (
    AttemptOutcome,
    BackoffStrategy,
    ExecutionState,
    ExecutionTrigger,
    JobState,
    OverlapPolicy,
    ScheduleKind,
)

MAX_ATTEMPTS_CAP = 20
MAX_BACKOFF_DELAY_MS = 24 * 60 * 60 * 1000
MAX_BODY_BYTES = 256 * 1024
MAX_CATCHUP_LIMIT = 1_000


class JobBase(BaseModel):
    name: Annotated[str, Field(min_length=1, max_length=200)]
    endpoint: Annotated[str, Field(max_length=2048)]
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"] = "POST"
    headers: dict[str, str] = Field(default_factory=dict)
    body: Annotated[str | None, Field(max_length=MAX_BODY_BYTES)] = None
    timezone: str = "UTC"

    connect_timeout_ms: Annotated[int, Field(ge=100, le=60_000)] = 5_000
    read_timeout_ms: Annotated[int, Field(ge=100, le=300_000)] = 30_000

    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    backoff_base_ms: Annotated[int, Field(ge=100, le=MAX_BACKOFF_DELAY_MS)] = 1_000
    backoff_multiplier: Annotated[float, Field(ge=1.0, le=10.0)] = 2.0
    backoff_max_delay_ms: Annotated[int, Field(ge=100, le=MAX_BACKOFF_DELAY_MS)] = 300_000
    max_attempts: Annotated[int, Field(ge=1, le=MAX_ATTEMPTS_CAP)] = 5

    overlap_policy: OverlapPolicy = OverlapPolicy.SKIP
    queue_depth_limit: Annotated[int, Field(ge=0, le=1_000)] = 1
    catchup: bool = False
    catchup_limit: Annotated[int, Field(ge=0, le=MAX_CATCHUP_LIMIT)] = 10


class JobCreate(JobBase):
    cron: str | None = None
    run_at: datetime | None = None

    @model_validator(mode="after")
    def exactly_one_schedule(self) -> JobCreate:
        if (self.cron is None) == (self.run_at is None):
            raise ValueError("provide exactly one of 'cron' or 'run_at'")
        if self.run_at is not None and self.run_at.tzinfo is None:
            raise ValueError("'run_at' must include a UTC offset")
        return self


class JobUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=200)
    endpoint: str | None = Field(default=None, max_length=2048)
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"] | None = None
    headers: dict[str, str] | None = None
    body: str | None = Field(default=None, max_length=MAX_BODY_BYTES)
    cron: str | None = None
    run_at: datetime | None = None
    timezone: str | None = None
    backoff_strategy: BackoffStrategy | None = None
    backoff_base_ms: int | None = Field(default=None, ge=100, le=MAX_BACKOFF_DELAY_MS)
    backoff_multiplier: float | None = Field(default=None, ge=1.0, le=10.0)
    backoff_max_delay_ms: int | None = Field(default=None, ge=100, le=MAX_BACKOFF_DELAY_MS)
    max_attempts: int | None = Field(default=None, ge=1, le=MAX_ATTEMPTS_CAP)
    overlap_policy: OverlapPolicy | None = None
    queue_depth_limit: int | None = Field(default=None, ge=0, le=1_000)
    catchup: bool | None = None
    catchup_limit: int | None = Field(default=None, ge=0, le=MAX_CATCHUP_LIMIT)


class JobRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    state: JobState
    schedule_kind: ScheduleKind
    cron_expression: str | None
    run_at: datetime | None
    timezone: str
    endpoint: str
    method: str
    headers: dict[str, str]
    body: str | None
    backoff_strategy: BackoffStrategy
    backoff_base_ms: int
    backoff_multiplier: float
    backoff_max_delay_ms: int
    max_attempts: int
    overlap_policy: OverlapPolicy
    queue_depth_limit: int
    catchup: bool
    catchup_limit: int
    materialised_through: datetime | None
    created_at: datetime
    updated_at: datetime
    next_execution_at: datetime | None = None


class AttemptRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    attempt: int
    worker_id: str
    outcome: AttemptOutcome
    started_at: datetime
    finished_at: datetime
    duration_ms: int
    status_code: int | None
    error: str | None
    response_excerpt: str | None


class ExecutionRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    job_id: uuid.UUID
    scheduled_for: datetime
    state: ExecutionState
    trigger: ExecutionTrigger
    attempt: int
    abandoned_count: int
    claimed_by: str | None
    lease_expires_at: datetime | None
    shifted_from: datetime | None
    started_at: datetime | None
    completed_at: datetime | None
    lateness_ms: int | None
    last_error: str | None


class JobDetail(JobRead):
    upcoming: list[datetime] = Field(default_factory=list)
    recent_executions: list[ExecutionRead] = Field(default_factory=list)


class WorkerRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    version: str
    started_at: datetime
    last_heartbeat: datetime
    clock_drift_ms: int
    claimed_count: int
    shutting_down: bool


class Page[T](BaseModel):
    items: list[T]
    total: int
    limit: int
    offset: int


class ClusterView(BaseModel):
    workers: list[WorkerRead]
    pending_backlog: int
