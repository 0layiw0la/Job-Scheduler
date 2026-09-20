"""The vocabulary: states a job or execution can be in, and failures the API reports."""

from __future__ import annotations

from enum import StrEnum


class JobState(StrEnum):
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class ScheduleKind(StrEnum):
    CRON = "cron"
    ONCE = "once"


class ExecutionState(StrEnum):
    PENDING = "pending"
    CLAIMED = "claimed"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    DEAD = "dead"
    SKIPPED = "skipped"
    MISSED = "missed"
    CANCELLED = "cancelled"


LEASED_STATES = (ExecutionState.CLAIMED, ExecutionState.RUNNING)

TERMINAL_STATES = (
    ExecutionState.SUCCEEDED,
    ExecutionState.DEAD,
    ExecutionState.SKIPPED,
    ExecutionState.MISSED,
    ExecutionState.CANCELLED,
)


class ExecutionTrigger(StrEnum):
    SCHEDULE = "schedule"
    MANUAL = "manual"
    REPLAY = "replay"


class OverlapPolicy(StrEnum):
    SKIP = "skip"
    QUEUE = "queue"
    ALLOW = "allow"


class BackoffStrategy(StrEnum):
    CONSTANT = "constant"
    EXPONENTIAL = "exponential"


class AttemptOutcome(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMEOUT = "timeout"
    ERROR = "error"


class SchedulerError(Exception):
    """Base class for errors that map to a client-visible failure."""


class ValidationError(SchedulerError):
    """A job definition or request is invalid. Maps to HTTP 422."""


class NotFoundError(SchedulerError):
    """The addressed resource does not exist. Maps to HTTP 404."""


class ConflictError(SchedulerError):
    """The request conflicts with current state. Maps to HTTP 409."""
