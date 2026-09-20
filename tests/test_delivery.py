"""Surviving a hostile endpoint: retry until it works, give up honestly when it does not."""

from __future__ import annotations

import asyncio

import pytest

from scheduler.config import Settings
from scheduler.db import Database
from scheduler.domain import AttemptOutcome, BackoffStrategy, ExecutionState
from scheduler.testing import Receiver
from tests.conftest import cron_job, due_now, wait_for_terminal

pytestmark = pytest.mark.db


async def test_transient_failures_are_retried_until_one_succeeds(
    db: Database, settings: Settings, worker: object
) -> None:
    async with Receiver(statuses=[500, 500, 500], default_status=200) as receiver:
        job = cron_job(
            endpoint=receiver.url,
            max_attempts=5,
            backoff_strategy=BackoffStrategy.EXPONENTIAL,
            backoff_base_ms=100,
            backoff_max_delay_ms=400,
        )
        (execution_id,) = await due_now(db, job)

        execution = await wait_for_terminal(db, execution_id, timeout=20)

    assert execution.state is ExecutionState.SUCCEEDED
    assert execution.attempt == 4
    assert receiver.count == 4
    assert [a.status_code for a in execution.attempts] == [500, 500, 500, 200]
    assert [a.attempt for a in execution.attempts] == [1, 2, 3, 4]

    assert set(receiver.keys()) == {str(execution_id)}

    gaps = [
        (b.received_at - a.received_at).total_seconds()
        for a, b in zip(receiver.deliveries, receiver.deliveries[1:], strict=False)
    ]
    ceilings = [0.1, 0.2, 0.4]
    assert all(gap <= ceiling + 1.0 for gap, ceiling in zip(gaps, ceilings, strict=True))


async def test_permanent_failure_dead_letters_with_full_history(
    db: Database, settings: Settings, worker: object
) -> None:
    async with Receiver(default_status=500) as receiver:
        job = cron_job(endpoint=receiver.url, max_attempts=3)
        (execution_id,) = await due_now(db, job)

        execution = await wait_for_terminal(db, execution_id, timeout=20)
        await asyncio.sleep(0.5)

    assert execution.state is ExecutionState.DEAD
    assert execution.attempt == 3
    assert receiver.count == 3
    assert len(execution.attempts) == 3
    assert all(a.outcome is AttemptOutcome.FAILED for a in execution.attempts)
    assert execution.last_error is not None and "500" in execution.last_error
