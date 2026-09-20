"""initial schema

Revision ID: 0b9d4dc5654f
Revises:
Create Date: 2026-09-19 14:04:57.557432

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "0b9d4dc5654f"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "jobs",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column(
            "schedule_kind",
            sa.Enum("cron", "once", name="schedule_kind", native_enum=False, length=32),
            nullable=False,
        ),
        sa.Column("cron_expression", sa.String(length=200), nullable=True),
        sa.Column("run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("timezone", sa.String(length=64), nullable=False),
        sa.Column("endpoint", sa.Text(), nullable=False),
        sa.Column("method", sa.String(length=10), nullable=False),
        sa.Column("headers", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("body", sa.Text(), nullable=True),
        sa.Column("connect_timeout_ms", sa.Integer(), nullable=False),
        sa.Column("read_timeout_ms", sa.Integer(), nullable=False),
        sa.Column(
            "backoff_strategy",
            sa.Enum(
                "constant", "exponential", name="backoff_strategy", native_enum=False, length=32
            ),
            nullable=False,
        ),
        sa.Column("backoff_base_ms", sa.Integer(), nullable=False),
        sa.Column("backoff_multiplier", sa.Float(), nullable=False),
        sa.Column("backoff_max_delay_ms", sa.Integer(), nullable=False),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column(
            "overlap_policy",
            sa.Enum("skip", "queue", "allow", name="overlap_policy", native_enum=False, length=32),
            nullable=False,
        ),
        sa.Column("queue_depth_limit", sa.Integer(), nullable=False),
        sa.Column("catchup", sa.Boolean(), nullable=False),
        sa.Column("catchup_limit", sa.Integer(), nullable=False),
        sa.Column(
            "state",
            sa.Enum(
                "active",
                "paused",
                "completed",
                "cancelled",
                name="job_state",
                native_enum=False,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column("materialised_through", sa.DateTime(timezone=True), nullable=True),
        sa.Column("idempotency_key", sa.String(length=200), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "(schedule_kind = 'cron' AND cron_expression IS NOT NULL AND run_at IS NULL) OR (schedule_kind = 'once' AND run_at IS NOT NULL AND cron_expression IS NULL)",
            name=op.f("ck_jobs_schedule_shape"),
        ),
        sa.CheckConstraint("max_attempts >= 1", name=op.f("ck_jobs_max_attempts_positive")),
        sa.CheckConstraint("queue_depth_limit >= 0", name=op.f("ck_jobs_queue_depth_non_negative")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_jobs")),
        sa.UniqueConstraint("idempotency_key", name=op.f("uq_jobs_idempotency_key")),
    )
    op.create_index(
        "ix_jobs_materialise_cursor",
        "jobs",
        ["materialised_through"],
        unique=False,
        postgresql_where=sa.text("state = 'active'"),
    )
    op.create_table(
        "workers",
        sa.Column("id", sa.String(length=128), nullable=False),
        sa.Column("version", sa.String(length=32), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_heartbeat", sa.DateTime(timezone=True), nullable=False),
        sa.Column("clock_drift_ms", sa.Integer(), nullable=False),
        sa.Column("claimed_count", sa.Integer(), nullable=False),
        sa.Column("shutting_down", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_workers")),
    )
    op.create_table(
        "executions",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("job_id", sa.UUID(), nullable=False),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "state",
            sa.Enum(
                "pending",
                "claimed",
                "running",
                "succeeded",
                "dead",
                "skipped",
                "missed",
                "cancelled",
                name="execution_state",
                native_enum=False,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column(
            "trigger",
            sa.Enum(
                "schedule",
                "manual",
                "replay",
                name="execution_trigger",
                native_enum=False,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("abandoned_count", sa.Integer(), nullable=False),
        sa.Column("claimed_by", sa.String(length=128), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("shifted_from", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lateness_ms", sa.Integer(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["job_id"], ["jobs.id"], name=op.f("fk_executions_job_id"), ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_executions")),
    )
    op.create_index(
        "ix_executions_due",
        "executions",
        ["scheduled_for"],
        unique=False,
        postgresql_where=sa.text("state = 'pending'"),
    )
    op.create_index(
        "ix_executions_job_recent", "executions", ["job_id", "scheduled_for"], unique=False
    )
    op.create_index(
        "ix_executions_lease",
        "executions",
        ["lease_expires_at"],
        unique=False,
        postgresql_where=sa.text("state IN ('claimed', 'running')"),
    )
    op.create_index(
        "uq_executions_job_id_scheduled_for",
        "executions",
        ["job_id", "scheduled_for"],
        unique=True,
        postgresql_where=sa.text("trigger = 'schedule'"),
    )
    op.create_table(
        "attempts",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("execution_id", sa.UUID(), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("worker_id", sa.String(length=128), nullable=False),
        sa.Column(
            "outcome",
            sa.Enum(
                "succeeded",
                "failed",
                "timeout",
                "error",
                name="attempt_outcome",
                native_enum=False,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("duration_ms", sa.Integer(), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("response_excerpt", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["execution_id"],
            ["executions.id"],
            name=op.f("fk_attempts_execution_id"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_attempts")),
    )
    op.create_index(op.f("ix_attempts_execution_id"), "attempts", ["execution_id"], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f("ix_attempts_execution_id"), table_name="attempts")
    op.drop_table("attempts")
    op.drop_index(
        "uq_executions_job_id_scheduled_for",
        table_name="executions",
        postgresql_where=sa.text("trigger = 'schedule'"),
    )
    op.drop_index(
        "ix_executions_lease",
        table_name="executions",
        postgresql_where=sa.text("state IN ('claimed', 'running')"),
    )
    op.drop_index("ix_executions_job_recent", table_name="executions")
    op.drop_index(
        "ix_executions_due", table_name="executions", postgresql_where=sa.text("state = 'pending'")
    )
    op.drop_table("executions")
    op.drop_table("workers")
    op.drop_index(
        "ix_jobs_materialise_cursor",
        table_name="jobs",
        postgresql_where=sa.text("state = 'active'"),
    )
    op.drop_table("jobs")
