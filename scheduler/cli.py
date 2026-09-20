"""Command line entry points: schedule inspection, the worker, and the API."""

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime

from .config import Settings, configure_logging, load_settings
from .db import create_database
from .schedules import CronSchedule, OneOffSchedule, Schedule, iter_occurrences, parse_timezone


def _build_schedule(args: argparse.Namespace) -> Schedule:
    if args.at:
        return OneOffSchedule(datetime.fromisoformat(args.at), args.timezone)
    return CronSchedule(args.cron, args.timezone)


def cmd_next(args: argparse.Namespace) -> int:
    schedule = _build_schedule(args)
    tz = parse_timezone(args.timezone)
    reference = (
        datetime.fromisoformat(args.after).astimezone(UTC) if args.after else datetime.now(UTC)
    )

    print(f"{'utc':<26}  {'local (' + args.timezone + ')':<34}  note")
    for occurrence in iter_occurrences(schedule, reference, args.count):
        note = ""
        if occurrence.shifted_from:
            note = f"shifted from {occurrence.shifted_from.isoformat(sep=' ')} (DST gap)"
        print(
            f"{occurrence.scheduled_for.isoformat():<26}  "
            f"{occurrence.scheduled_for.astimezone(tz).isoformat():<34}  {note}"
        )
    return 0


def cmd_worker(args: argparse.Namespace) -> int:
    from .engine import WorkerRuntime

    settings = _settings(args)
    configure_logging(settings.log_level)

    async def run() -> None:
        db = create_database(
            settings.database_url,
            pool_max_size=settings.db_pool_max_size,
        )
        runtime = WorkerRuntime(
            db,
            settings,
            enable_materialiser=not args.no_materialiser,
            enable_sweeper=not args.no_sweeper,
        )
        try:
            await runtime.run_forever()
        finally:
            await db.dispose()

    asyncio.run(run())
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    import uvicorn

    from .api import create_app

    settings = _settings(args)
    app = create_app(settings, run_worker=args.with_worker)
    uvicorn.run(app, host=args.host, port=args.port, log_config=None)
    return 0


def _settings(args: argparse.Namespace) -> Settings:
    overrides = {"database_url": args.database_url} if args.database_url else {}
    return load_settings(**overrides)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="scheduler", description=__doc__)
    parser.add_argument("--database-url", default=None, help="overrides DATABASE_URL")
    sub = parser.add_subparsers(dest="command", required=True)

    nxt = sub.add_parser("next", help="print the next N firings of a schedule")
    group = nxt.add_mutually_exclusive_group(required=True)
    group.add_argument("--cron", help="five-field cron expression or @alias")
    group.add_argument("--at", help="ISO-8601 instant for a one-off job")
    nxt.add_argument("--timezone", default="UTC", help="IANA timezone, e.g. Europe/London")
    nxt.add_argument("--count", type=int, default=20)
    nxt.add_argument("--after", default=None, help="reference instant (default: now)")
    nxt.set_defaults(func=cmd_next)

    worker = sub.add_parser("worker", help="run the scheduling loops")
    worker.add_argument("--no-materialiser", action="store_true")
    worker.add_argument("--no-sweeper", action="store_true")
    worker.set_defaults(func=cmd_worker)

    serve = sub.add_parser("serve", help="run the control-plane API")
    serve.add_argument("--host", default="0.0.0.0")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--with-worker", action="store_true", help="also run the loops in-process")
    serve.set_defaults(func=cmd_serve)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
