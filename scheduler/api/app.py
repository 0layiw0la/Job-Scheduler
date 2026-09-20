from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse

from ..config import Settings, configure_logging, load_settings
from ..db import Database, create_database
from ..domain import ConflictError, NotFoundError, ValidationError
from ..engine import WorkerRuntime
from .routes import executions_router, jobs_router, ops_router
from .utils import RateLimiter, require_api_key

logger = logging.getLogger(__name__)

DESCRIPTION = """
A durable webhook scheduler. Submit a job, it fires at the right time, survives process
death, and runs correctly with N instances competing for the same work.

Delivery is **at least once**: every call carries a stable `Idempotency-Key` header
(the execution id) so receivers can deduplicate and get exactly-once effects.
"""


def create_app(
    settings: Settings | None = None,
    *,
    database: Database | None = None,
    run_worker: bool = False,
) -> FastAPI:
    settings = settings or load_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging(settings.log_level)
        app.state.settings = settings
        app.state.db = database or create_database(
            settings.database_url,
            pool_max_size=settings.db_pool_max_size,
        )
        if settings.api_key is None:
            logger.warning("api_key_not_set", extra={"detail": "the API accepts any caller"})
        app.state.rate_limiter = RateLimiter(
            limit=settings.api_create_rate_limit,
            window_seconds=settings.api_create_rate_window_seconds,
        )
        app.state.worker = WorkerRuntime(app.state.db, settings) if run_worker else None
        if app.state.worker is not None:
            await app.state.worker.start()
        try:
            yield
        finally:
            if app.state.worker is not None:
                await app.state.worker.stop()
            if database is None:
                await app.state.db.dispose()

    app = FastAPI(
        title="Job Scheduler",
        version="0.1.0",
        description=DESCRIPTION,
        lifespan=lifespan,
    )

    guarded = [Depends(require_api_key)]
    app.include_router(jobs_router, dependencies=guarded)
    app.include_router(executions_router, dependencies=guarded)
    app.include_router(ops_router)

    @app.exception_handler(ValidationError)
    async def _validation(_: Request, exc: ValidationError) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    @app.exception_handler(NotFoundError)
    async def _not_found(_: Request, exc: NotFoundError) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(ConflictError)
    async def _conflict(_: Request, exc: ConflictError) -> JSONResponse:
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    return app
