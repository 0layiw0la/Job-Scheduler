"""A definition becomes correctly-dated rows.

Cron arithmetic in a timezone that shifts under it, and materialisation that stays correct
when several instances do it at once.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from sqlalchemy import select

from scheduler.config import Settings
from scheduler.db import Database, db_now, queries
from scheduler.db.models import Execution
from scheduler.engine import Materialiser
from scheduler.schedules import CronSchedule, iter_occurrences
from tests.conftest import FrozenClock, cron_job

LONDON = ZoneInfo("Europe/London")


REFERENCE = datetime(2026, 3, 2, 12, 0, tzinfo=UTC)


def utc(*args: int) -> datetime:
    return datetime(*args, tzinfo=UTC)


LONDON = ZoneInfo("Europe/London")
NEW_YORK = ZoneInfo("America/New_York")
SYDNEY = ZoneInfo("Australia/Sydney")


def test_london_spring_forward_shifts_a_job_in_the_gap() -> None:
    clock = FrozenClock(utc(2026, 3, 28, 12, 0))
    occurrence = CronSchedule("30 1 * * *", "Europe/London").next_after(clock.now())

    assert occurrence is not None
    assert occurrence.scheduled_for == utc(2026, 3, 29, 1, 0)
    assert occurrence.local_time == datetime(2026, 3, 29, 2, 0, tzinfo=LONDON)
    assert occurrence.shifted_from == datetime(2026, 3, 29, 1, 30)


def test_london_autumn_back_fires_once_on_the_first_occurrence() -> None:
    occurrences = iter_occurrences(
        CronSchedule("30 1 * * *", "Europe/London"), utc(2026, 10, 24, 12, 0), 2
    )

    assert [o.scheduled_for for o in occurrences] == [
        utc(2026, 10, 25, 0, 30),
        utc(2026, 10, 26, 1, 30),
    ]
    assert all(o.shifted_from is None for o in occurrences)


def test_daily_job_keeps_its_local_hour_across_a_transition() -> None:
    occurrences = iter_occurrences(
        CronSchedule("0 9 * * *", "Europe/London"), utc(2026, 3, 27, 0, 0), 4
    )

    assert [o.local_time.hour for o in occurrences] == [9, 9, 9, 9]
    assert [o.scheduled_for.hour for o in occurrences] == [9, 9, 8, 8]


async def executions_of(db: Database, job_id) -> list[Execution]:
    async with db.session() as session:
        rows = await session.execute(
            select(Execution).where(Execution.job_id == job_id).order_by(Execution.scheduled_for)
        )
        return list(rows.scalars().all())


async def test_ten_concurrent_materialisers_produce_exactly_one_row_per_occurrence(
    db: Database, settings: Settings
) -> None:
    settings.materialise_horizon_seconds = 3_600
    async with db.transaction() as session:
        now = await db_now(session)
        job = await queries.create_job(session, cron_job(materialised_through=now))

    results = await asyncio.gather(*(Materialiser(db, settings).run_once() for _ in range(10)))
    rows = await executions_of(db, job.id)

    assert len(rows) == 60
    assert sum(results) == 60, "ON CONFLICT DO NOTHING should absorb every duplicate"
    assert len({row.scheduled_for for row in rows}) == 60
