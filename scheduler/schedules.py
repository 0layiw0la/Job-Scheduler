"""Cron and one-off schedules, both reduced to "next UTC instant after X".

Local wall-clock time exists here and nowhere else, which is what keeps DST bugs to one file.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter

from .domain import ValidationError

CRON_ALIASES = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}

_MAX_RESOLVE_STEPS = 128


@dataclass(frozen=True, slots=True)
class Occurrence:
    scheduled_for: datetime
    """The firing instant, in UTC."""

    local_time: datetime
    """The same instant rendered in the job's timezone, for display and debugging."""

    shifted_from: datetime | None = None
    """Set when DST moved this firing: the wall-clock time the job asked for but that
    never existed on that date."""


class Schedule(Protocol):
    def next_after(self, after: datetime) -> Occurrence | None:
        """The first occurrence strictly after `after` (UTC), or None if exhausted."""


def parse_timezone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValidationError(f"unknown IANA timezone: {name!r}") from exc


def normalise_cron(expression: str) -> str:
    expr = expression.strip()
    resolved = CRON_ALIASES.get(expr.lower(), expr)
    if len(resolved.split()) != 5:
        raise ValidationError(
            "cron expression must have five fields (minute hour day month weekday) "
            "or be a supported @alias; second-granularity cron is not supported"
        )
    if not croniter.is_valid(resolved):
        raise ValidationError(f"unparseable cron expression: {expression!r}")
    return resolved


def resolve_local(naive_local: datetime, tz: ZoneInfo) -> tuple[datetime, datetime | None]:
    """Wall-clock time in `tz` to a UTC instant.

    Gap (spring forward): fire at the instant the clocks jump, and report the shift.
    Ambiguity (autumn back): fire on the first occurrence only.
    """
    first = naive_local.replace(tzinfo=tz, fold=0)
    second = naive_local.replace(tzinfo=tz, fold=1)
    first_utc = first.astimezone(UTC)
    second_utc = second.astimezone(UTC)

    if first.utcoffset() == second.utcoffset():
        return first_utc, None
    if first_utc > second_utc:
        return _transition_instant(second_utc, first_utc, tz), naive_local
    return first_utc, None


def _transition_instant(before: datetime, after: datetime, tz: ZoneInfo) -> datetime:
    """Bisect for the exact second at which `tz` changes offset between two instants."""
    baseline = before.astimezone(tz).utcoffset()
    lo, hi = before, after
    while hi - lo > timedelta(seconds=1):
        mid = lo + (hi - lo) / 2
        if mid.astimezone(tz).utcoffset() == baseline:
            lo = mid
        else:
            hi = mid
    return hi.replace(microsecond=0)


class CronSchedule:
    def __init__(self, expression: str, timezone: str = "UTC") -> None:
        self.expression = normalise_cron(expression)
        self.timezone = timezone
        self._tz = parse_timezone(timezone)

    def next_after(self, after: datetime) -> Occurrence | None:
        after = after.astimezone(UTC)
        cursor = after.astimezone(self._tz).replace(tzinfo=None, microsecond=0)
        it = croniter(self.expression, cursor)
        for _ in range(_MAX_RESOLVE_STEPS):
            candidate: datetime = it.get_next(datetime)
            scheduled_for, shifted_from = resolve_local(candidate, self._tz)
            if scheduled_for > after:
                return Occurrence(
                    scheduled_for=scheduled_for,
                    local_time=scheduled_for.astimezone(self._tz),
                    shifted_from=shifted_from,
                )
        return None

    def __repr__(self) -> str:
        return f"CronSchedule({self.expression!r}, {self.timezone!r})"


class OneOffSchedule:
    def __init__(self, run_at: datetime, timezone: str = "UTC") -> None:
        if run_at.tzinfo is None:
            raise ValidationError("one-off run_at must include a timezone offset")
        self.run_at = run_at.astimezone(UTC).replace(microsecond=0)
        self.timezone = timezone
        self._tz = parse_timezone(timezone)

    def next_after(self, after: datetime) -> Occurrence | None:
        if self.run_at <= after.astimezone(UTC):
            return None
        return Occurrence(
            scheduled_for=self.run_at,
            local_time=self.run_at.astimezone(self._tz),
        )

    def __repr__(self) -> str:
        return f"OneOffSchedule({self.run_at.isoformat()!r})"


def iter_occurrences(schedule: Schedule, after: datetime, limit: int) -> list[Occurrence]:
    out: list[Occurrence] = []
    cursor = after
    for _ in range(limit):
        occurrence = schedule.next_after(cursor)
        if occurrence is None:
            break
        out.append(occurrence)
        cursor = occurrence.scheduled_for
    return out
