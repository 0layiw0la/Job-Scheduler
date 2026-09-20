from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from ..domain import (
    AttemptOutcome,
    BackoffStrategy,
    ExecutionState,
    ExecutionTrigger,
    JobState,
    OverlapPolicy,
    ScheduleKind,
)

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s",
    "pk": "pk_%(table_name)s",
}


def _enum(python_enum: type, name: str) -> Enum:
    return Enum(
        python_enum,
        name=name,
        native_enum=False,
        length=32,
        values_callable=lambda e: [member.value for member in e],
        validate_strings=True,
    )


def utcnow() -> datetime:
    from datetime import UTC

    return datetime.now(UTC)


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)

    type_annotation_map = {  # noqa: RUF012
        datetime: DateTime(timezone=True),
        uuid.UUID: UUID(as_uuid=True),
        dict[str, str]: JSONB,
    }


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(200))

    schedule_kind: Mapped[ScheduleKind] = mapped_column(_enum(ScheduleKind, "schedule_kind"))
    cron_expression: Mapped[str | None] = mapped_column(String(200))
    run_at: Mapped[datetime | None]
    timezone: Mapped[str] = mapped_column(String(64), default="UTC")

    endpoint: Mapped[str] = mapped_column(Text)
    method: Mapped[str] = mapped_column(String(10), default="POST")
    headers: Mapped[dict[str, str]] = mapped_column(default=dict)
    body: Mapped[str | None] = mapped_column(Text)
    connect_timeout_ms: Mapped[int] = mapped_column(default=5_000)
    read_timeout_ms: Mapped[int] = mapped_column(default=30_000)

    backoff_strategy: Mapped[BackoffStrategy] = mapped_column(
        _enum(BackoffStrategy, "backoff_strategy"), default=BackoffStrategy.EXPONENTIAL
    )
    backoff_base_ms: Mapped[int] = mapped_column(default=1_000)
    backoff_multiplier: Mapped[float] = mapped_column(Float, default=2.0)
    backoff_max_delay_ms: Mapped[int] = mapped_column(default=300_000)
    max_attempts: Mapped[int] = mapped_column(default=5)

    overlap_policy: Mapped[OverlapPolicy] = mapped_column(
        _enum(OverlapPolicy, "overlap_policy"), default=OverlapPolicy.SKIP
    )
    queue_depth_limit: Mapped[int] = mapped_column(default=1)
    catchup: Mapped[bool] = mapped_column(default=False)
    catchup_limit: Mapped[int] = mapped_column(default=10)

    state: Mapped[JobState] = mapped_column(_enum(JobState, "job_state"), default=JobState.ACTIVE)
    materialised_through: Mapped[datetime | None]
    idempotency_key: Mapped[str | None] = mapped_column(String(200))

    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=utcnow)

    executions: Mapped[list[Execution]] = relationship(
        back_populates="job", cascade="all, delete-orphan", passive_deletes=True
    )

    __table_args__ = (
        UniqueConstraint("idempotency_key"),
        CheckConstraint(
            "(schedule_kind = 'cron' AND cron_expression IS NOT NULL AND run_at IS NULL)"
            " OR (schedule_kind = 'once' AND run_at IS NOT NULL AND cron_expression IS NULL)",
            name="schedule_shape",
        ),
        CheckConstraint("max_attempts >= 1", name="max_attempts_positive"),
        CheckConstraint("queue_depth_limit >= 0", name="queue_depth_non_negative"),
        Index(
            "ix_jobs_materialise_cursor",
            "materialised_through",
            postgresql_where=state == JobState.ACTIVE,
        ),
    )


class Execution(Base):
    __tablename__ = "executions"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"))
    scheduled_for: Mapped[datetime]
    state: Mapped[ExecutionState] = mapped_column(
        _enum(ExecutionState, "execution_state"), default=ExecutionState.PENDING
    )
    trigger: Mapped[ExecutionTrigger] = mapped_column(
        _enum(ExecutionTrigger, "execution_trigger"), default=ExecutionTrigger.SCHEDULE
    )

    attempt: Mapped[int] = mapped_column(default=0)
    abandoned_count: Mapped[int] = mapped_column(default=0)
    claimed_by: Mapped[str | None] = mapped_column(String(128))
    lease_expires_at: Mapped[datetime | None]

    shifted_from: Mapped[datetime | None]
    started_at: Mapped[datetime | None]
    completed_at: Mapped[datetime | None]
    lateness_ms: Mapped[int | None]
    last_error: Mapped[str | None] = mapped_column(Text)

    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=utcnow)

    job: Mapped[Job] = relationship(back_populates="executions")
    attempts: Mapped[list[Attempt]] = relationship(
        back_populates="execution",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="Attempt.attempt",
    )

    __table_args__ = (
        Index(
            "uq_executions_job_id_scheduled_for",
            "job_id",
            "scheduled_for",
            unique=True,
            postgresql_where=trigger == ExecutionTrigger.SCHEDULE,
        ),
        Index(
            "ix_executions_due",
            "scheduled_for",
            postgresql_where=state == ExecutionState.PENDING,
        ),
        Index(
            "ix_executions_lease",
            "lease_expires_at",
            postgresql_where=state.in_([ExecutionState.CLAIMED, ExecutionState.RUNNING]),
        ),
        Index("ix_executions_job_recent", "job_id", "scheduled_for"),
    )


class Attempt(Base):
    __tablename__ = "attempts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    execution_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("executions.id", ondelete="CASCADE"), index=True
    )
    attempt: Mapped[int]
    worker_id: Mapped[str] = mapped_column(String(128))
    outcome: Mapped[AttemptOutcome] = mapped_column(_enum(AttemptOutcome, "attempt_outcome"))

    started_at: Mapped[datetime]
    finished_at: Mapped[datetime]
    duration_ms: Mapped[int]
    status_code: Mapped[int | None]
    error: Mapped[str | None] = mapped_column(Text)
    response_excerpt: Mapped[str | None] = mapped_column(Text)

    execution: Mapped[Execution] = relationship(back_populates="attempts")


class Worker(Base):
    __tablename__ = "workers"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    version: Mapped[str] = mapped_column(String(32))
    started_at: Mapped[datetime]
    last_heartbeat: Mapped[datetime]
    clock_drift_ms: Mapped[int] = mapped_column(default=0)
    claimed_count: Mapped[int] = mapped_column(default=0)
    shutting_down: Mapped[bool] = mapped_column(default=False)
