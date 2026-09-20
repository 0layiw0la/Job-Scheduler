"""Turning a request into a job definition: validation, endpoint safety, firing times."""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from datetime import datetime
from urllib.parse import urlparse

from .config import Settings
from .db import queries
from .db.models import Job
from .domain import ScheduleKind, ValidationError
from .schedules import Schedule, iter_occurrences, normalise_cron, parse_timezone
from .schemas import JobCreate, JobUpdate

ALLOWED_SCHEMES = frozenset({"http", "https"})
ALLOWED_METHODS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"})


def _is_blocked(address: str) -> bool:
    ip = ipaddress.ip_address(address)
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


def resolve_addresses(host: str) -> list[str]:
    try:
        infos = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    except socket.gaierror as exc:
        raise ValidationError(f"endpoint host does not resolve: {host}") from exc
    return [str(info[4][0]) for info in infos]


def validate_endpoint(url: str, *, allow_private: bool = False) -> str:
    """Reject non-public targets: a scheduler that will POST anywhere is an SSRF engine."""
    parsed = urlparse(url)
    if parsed.scheme not in ALLOWED_SCHEMES:
        raise ValidationError("endpoint must be an absolute http:// or https:// URL")
    if not parsed.hostname:
        raise ValidationError("endpoint must include a host")
    if allow_private:
        return url

    try:
        literal_blocked = _is_blocked(parsed.hostname)
    except ValueError:
        literal_blocked = False
    if literal_blocked:
        raise ValidationError("endpoint resolves to a private or loopback address")

    for address in resolve_addresses(parsed.hostname):
        if _is_blocked(address):
            raise ValidationError("endpoint resolves to a private or loopback address")
    return url


def validate_method(method: str) -> str:
    upper = method.upper()
    if upper not in ALLOWED_METHODS:
        raise ValidationError(f"unsupported HTTP method: {method}")
    return upper


async def _validated_endpoint(endpoint: str, settings: Settings) -> str:
    return await asyncio.to_thread(
        validate_endpoint, endpoint, allow_private=settings.allow_private_endpoints
    )


async def build_job(payload: JobCreate, settings: Settings) -> Job:
    parse_timezone(payload.timezone)
    endpoint = await _validated_endpoint(payload.endpoint, settings)

    job = Job(
        name=payload.name,
        endpoint=endpoint,
        method=validate_method(payload.method),
        headers=payload.headers,
        body=payload.body,
        timezone=payload.timezone,
        connect_timeout_ms=payload.connect_timeout_ms,
        read_timeout_ms=payload.read_timeout_ms,
        backoff_strategy=payload.backoff_strategy,
        backoff_base_ms=payload.backoff_base_ms,
        backoff_multiplier=payload.backoff_multiplier,
        backoff_max_delay_ms=payload.backoff_max_delay_ms,
        max_attempts=payload.max_attempts,
        overlap_policy=payload.overlap_policy,
        queue_depth_limit=payload.queue_depth_limit,
        catchup=payload.catchup,
        catchup_limit=payload.catchup_limit,
    )
    if payload.cron is not None:
        job.schedule_kind = ScheduleKind.CRON
        job.cron_expression = normalise_cron(payload.cron)
    else:
        job.schedule_kind = ScheduleKind.ONCE
        job.run_at = payload.run_at
    return job


async def apply_update(job: Job, payload: JobUpdate, settings: Settings) -> Job:
    changes = payload.model_dump(exclude_unset=True)

    if "endpoint" in changes:
        job.endpoint = await _validated_endpoint(changes.pop("endpoint"), settings)
    if "method" in changes:
        job.method = validate_method(changes.pop("method"))
    if "timezone" in changes:
        timezone = changes.pop("timezone")
        parse_timezone(timezone)
        job.timezone = timezone

    cron = changes.pop("cron", None)
    run_at = changes.pop("run_at", None)
    if cron is not None and run_at is not None:
        raise ValidationError("provide at most one of 'cron' or 'run_at'")
    if cron is not None:
        job.schedule_kind = ScheduleKind.CRON
        job.cron_expression = normalise_cron(cron)
        job.run_at = None
    elif run_at is not None:
        if run_at.tzinfo is None:
            raise ValidationError("'run_at' must include a UTC offset")
        job.schedule_kind = ScheduleKind.ONCE
        job.run_at = run_at
        job.cron_expression = None

    for field, value in changes.items():
        setattr(job, field, value)

    if cron is not None or run_at is not None:
        job.materialised_through = None
    return job


def upcoming_occurrences(job: Job, *, after: datetime, count: int = 20) -> list[datetime]:
    schedule: Schedule = queries.schedule_for(job)
    return [occurrence.scheduled_for for occurrence in iter_occurrences(schedule, after, count)]


def next_execution_at(job: Job, *, after: datetime) -> datetime | None:
    occurrence = queries.schedule_for(job).next_after(after)
    return occurrence.scheduled_for if occurrence else None
