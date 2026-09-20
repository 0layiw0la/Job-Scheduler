"""The delivery path: make the call, decide what the outcome means, settle the row."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from enum import StrEnum

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from .config import Settings
from .db import Database, queries
from .db.models import Execution, Job
from .domain import AttemptOutcome, BackoffStrategy, ExecutionState, OverlapPolicy

logger = logging.getLogger(__name__)


USER_AGENT = "job-scheduler/0.1"


def delivery_headers(
    *,
    execution_id: uuid.UUID,
    job_id: uuid.UUID,
    attempt: int,
    scheduled_for: datetime,
    custom: dict[str, str] | None = None,
) -> dict[str, str]:
    """Idempotency-Key is the execution id, so it is stable across retries of one firing."""
    headers = dict(custom or {})
    headers.update(
        {
            "Idempotency-Key": str(execution_id),
            "X-Job-Id": str(job_id),
            "X-Execution-Id": str(execution_id),
            "X-Attempt": str(attempt),
            "X-Scheduled-For": scheduled_for.astimezone(UTC).isoformat(),
            "User-Agent": USER_AGENT,
        }
    )
    return headers


@dataclass(slots=True)
class DeliveryRequest:
    url: str
    method: str
    headers: dict[str, str] = field(default_factory=dict)
    body: str | None = None
    connect_timeout_ms: int = 5_000
    read_timeout_ms: int = 30_000


@dataclass(slots=True)
class DeliveryResult:
    outcome: AttemptOutcome
    started_at: datetime
    finished_at: datetime
    duration_ms: int
    status_code: int | None = None
    error: str | None = None
    response_excerpt: str | None = None
    retry_after: str | None = None


class HttpDeliverer:
    """Owns one connection pool for the whole process."""

    def __init__(
        self, *, response_excerpt_bytes: int = 2048, client: httpx.AsyncClient | None = None
    ) -> None:
        self._excerpt_bytes = response_excerpt_bytes
        self._client = client or httpx.AsyncClient(
            follow_redirects=False,
            limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
        )

    async def deliver(self, request: DeliveryRequest) -> DeliveryResult:
        timeout = httpx.Timeout(
            connect=request.connect_timeout_ms / 1000,
            read=request.read_timeout_ms / 1000,
            write=request.read_timeout_ms / 1000,
            pool=request.connect_timeout_ms / 1000,
        )
        started_at = datetime.now(UTC)
        try:
            response = await self._client.request(
                request.method,
                request.url,
                headers=request.headers,
                content=request.body,
                timeout=timeout,
            )
        except httpx.TimeoutException as exc:
            return self._failure(started_at, AttemptOutcome.TIMEOUT, f"timeout: {exc!r}")
        except httpx.HTTPError as exc:
            return self._failure(started_at, AttemptOutcome.ERROR, f"transport error: {exc!r}")

        finished_at = datetime.now(UTC)
        excerpt = response.text[: self._excerpt_bytes] if response.content else None
        outcome = (
            AttemptOutcome.SUCCEEDED if 200 <= response.status_code < 300 else AttemptOutcome.FAILED
        )
        return DeliveryResult(
            outcome=outcome,
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=_millis(started_at, finished_at),
            status_code=response.status_code,
            response_excerpt=excerpt,
            retry_after=response.headers.get("retry-after"),
        )

    @staticmethod
    def _failure(started_at: datetime, outcome: AttemptOutcome, error: str) -> DeliveryResult:
        finished_at = datetime.now(UTC)
        return DeliveryResult(
            outcome=outcome,
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=_millis(started_at, finished_at),
            error=error,
        )

    async def aclose(self) -> None:
        await self._client.aclose()


def _millis(start: datetime, end: datetime) -> int:
    return int((end - start).total_seconds() * 1000)


RETRYABLE_CLIENT_ERRORS = frozenset({408, 425, 429})

RETRY_AFTER_STATUSES = frozenset({429, 503})

MAX_RETRY_AFTER = timedelta(hours=1)


class Verdict(StrEnum):
    SUCCESS = "success"
    RETRY = "retry"
    FATAL = "fatal"


def classify_status(status_code: int) -> Verdict:
    if 200 <= status_code < 300:
        return Verdict.SUCCESS
    if status_code in RETRYABLE_CLIENT_ERRORS or status_code >= 500:
        return Verdict.RETRY
    return Verdict.FATAL


def parse_retry_after(value: str | None, *, now: datetime) -> timedelta | None:
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        delay = timedelta(seconds=int(value))
    else:
        try:
            parsed = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        delay = parsed - now
    if delay < timedelta(0):
        return timedelta(0)
    return min(delay, MAX_RETRY_AFTER)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    strategy: BackoffStrategy
    base_ms: int
    multiplier: float
    max_delay_ms: int
    max_attempts: int

    @classmethod
    def from_job(cls, job: Job) -> RetryPolicy:
        return cls(
            strategy=job.backoff_strategy,
            base_ms=job.backoff_base_ms,
            multiplier=job.backoff_multiplier,
            max_delay_ms=job.backoff_max_delay_ms,
            max_attempts=job.max_attempts,
        )

    def ceiling_for(self, attempt: int) -> timedelta:
        """The un-jittered upper bound for the delay after `attempt` failed attempts."""
        if attempt < 1:
            attempt = 1
        if self.strategy is BackoffStrategy.CONSTANT:
            delay_ms = float(self.base_ms)
        else:
            delay_ms = self.base_ms * (self.multiplier ** (attempt - 1))
        return timedelta(milliseconds=min(delay_ms, float(self.max_delay_ms)))

    def delay_for(self, attempt: int, *, rng: random.Random | None = None) -> timedelta:
        """Full jitter: uniform(0, ceiling), so failed jobs do not retry in lockstep."""
        ceiling = self.ceiling_for(attempt)
        generator = rng or random
        return timedelta(seconds=generator.uniform(0.0, ceiling.total_seconds()))

    def exhausted(self, attempt: int) -> bool:
        return attempt >= self.max_attempts


class Dispatcher:
    def __init__(
        self,
        db: Database,
        settings: Settings,
        deliverer: HttpDeliverer,
    ) -> None:
        self._db = db
        self._settings = settings
        self._deliverer = deliverer

    async def run(self, execution_id: uuid.UUID) -> None:
        started = await self._begin(execution_id)
        if started is None:
            return
        execution, job = started

        heartbeat = asyncio.create_task(self._heartbeat(execution.id))
        try:
            result = await self._deliverer.deliver(self._build_request(execution, job))
        finally:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat

        await self._settle(execution, job, result)

    async def _begin(self, execution_id: uuid.UUID) -> tuple[Execution, Job] | None:
        """Apply the overlap policy, then move the row to running. None means we are done."""
        async with self._db.transaction() as session:
            execution = await queries.get_execution(session, execution_id)
            if execution is None or execution.state is not ExecutionState.CLAIMED:
                return None
            job = await queries.get_job(session, execution.job_id)
            if job is None:
                return None

            if job.overlap_policy is not OverlapPolicy.ALLOW:
                if await queries.count_earlier_inflight(session, execution):
                    await self._apply_overlap_policy(session, execution, job)
                    return None

            running = await queries.start_attempt(session, execution.id, self._settings.worker_id)
            if running is None:
                logger.warning("lease_lost_before_start", extra={"execution_id": str(execution.id)})
                return None
            return running, job

    async def _apply_overlap_policy(
        self, session: AsyncSession, execution: Execution, job: Job
    ) -> None:
        if job.overlap_policy is OverlapPolicy.SKIP:
            await queries.finish(
                session,
                execution.id,
                self._settings.worker_id,
                ExecutionState.SKIPPED,
                error="skipped: previous execution still running",
            )
            return

        queued = await queries.count_queued(session, job.id, execution.id)
        if queued >= job.queue_depth_limit:
            await queries.finish(
                session,
                execution.id,
                self._settings.worker_id,
                ExecutionState.SKIPPED,
                error=f"skipped: queue depth limit {job.queue_depth_limit} reached",
            )
            return

        await queries.reschedule(
            session,
            execution.id,
            self._settings.worker_id,
            timedelta(seconds=self._settings.poll_interval_seconds),
        )

    def _build_request(self, execution: Execution, job: Job) -> DeliveryRequest:
        return DeliveryRequest(
            url=job.endpoint,
            method=job.method,
            headers=delivery_headers(
                execution_id=execution.id,
                job_id=job.id,
                attempt=execution.attempt,
                scheduled_for=execution.scheduled_for,
                custom=job.headers,
            ),
            body=job.body,
            connect_timeout_ms=job.connect_timeout_ms,
            read_timeout_ms=job.read_timeout_ms,
        )

    async def _settle(self, execution: Execution, job: Job, result: DeliveryResult) -> None:
        policy = RetryPolicy.from_job(job)
        verdict = (
            classify_status(result.status_code) if result.status_code is not None else Verdict.RETRY
        )

        async with self._db.transaction() as session:
            await queries.record_attempt(
                session,
                execution_id=execution.id,
                attempt=execution.attempt,
                worker_id=self._settings.worker_id,
                outcome=result.outcome,
                started_at=result.started_at,
                finished_at=result.finished_at,
                duration_ms=result.duration_ms,
                status_code=result.status_code,
                error=result.error,
                response_excerpt=result.response_excerpt,
            )

            if verdict is Verdict.SUCCESS:
                state = ExecutionState.SUCCEEDED
                await queries.finish(session, execution.id, self._settings.worker_id, state)
            elif verdict is Verdict.RETRY and not policy.exhausted(execution.attempt):
                delay = self._retry_delay(policy, execution.attempt, result)
                await queries.reschedule(
                    session,
                    execution.id,
                    self._settings.worker_id,
                    delay,
                    error=self._describe(result),
                )
                logger.info(
                    "retry_scheduled",
                    extra={
                        "execution_id": str(execution.id),
                        "job_id": str(job.id),
                        "attempt": execution.attempt,
                        "delay_seconds": round(delay.total_seconds(), 3),
                    },
                )
                return
            else:
                state = ExecutionState.DEAD
                await queries.finish(
                    session,
                    execution.id,
                    self._settings.worker_id,
                    state,
                    error=self._describe(result),
                )

        logger.info(
            "execution_settled",
            extra={
                "execution_id": str(execution.id),
                "job_id": str(job.id),
                "attempt": execution.attempt,
                "state": state.value,
                "status_code": result.status_code,
                "duration_ms": result.duration_ms,
                "worker_id": self._settings.worker_id,
            },
        )

    def _retry_delay(self, policy: RetryPolicy, attempt: int, result: DeliveryResult) -> timedelta:
        if result.status_code in RETRY_AFTER_STATUSES:
            retry_after = parse_retry_after(result.retry_after, now=datetime.now(UTC))
            if retry_after is not None:
                return retry_after
        return policy.delay_for(attempt)

    @staticmethod
    def _describe(result: DeliveryResult) -> str:
        if result.error:
            return result.error
        excerpt = (result.response_excerpt or "")[:200]
        return f"HTTP {result.status_code}: {excerpt}".strip()

    async def _heartbeat(self, execution_id: uuid.UUID) -> None:
        """Extend the lease mid-call, so a dispatch slower than the lease is not stolen."""
        interval = self._settings.heartbeat_interval_seconds
        while True:
            await asyncio.sleep(interval)
            async with self._db.transaction() as session:
                held = await queries.extend_lease(
                    session, execution_id, self._settings.worker_id, self._settings.lease_seconds
                )
            if not held:
                logger.warning("lease_lost_midflight", extra={"execution_id": str(execution_id)})
                return
