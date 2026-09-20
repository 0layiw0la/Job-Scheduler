"""Configuration and logging setup: everything the process needs to know before it starts."""

from __future__ import annotations

import json
import logging
import os
import socket
import sys
import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _default_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SCHEDULER_", env_file=".env", extra="ignore")

    database_url: str = Field(
        default="postgresql://scheduler:scheduler@localhost:5432/scheduler",
        validation_alias="DATABASE_URL",
    )
    db_pool_max_size: int = 10

    worker_id: str = Field(default_factory=_default_worker_id)

    poll_interval_seconds: float = 1.0
    claim_batch_size: int = 50
    lease_seconds: int = 60
    heartbeat_interval_seconds: float = 10.0
    max_inflight_dispatches: int = 32

    materialise_horizon_seconds: int = 3600
    materialise_max_occurrences: int = 500
    materialise_interval_seconds: float = 5.0
    sweep_interval_seconds: float = 5.0

    shutdown_grace_seconds: float = 30.0

    response_excerpt_bytes: int = 2048
    allow_private_endpoints: bool = False

    api_key: str | None = None
    api_create_rate_limit: int = 60
    api_create_rate_window_seconds: int = 60

    log_level: str = "INFO"


def load_settings(**overrides: object) -> Settings:
    return Settings(**overrides)  # type: ignore[arg-type]


_RESERVED = frozenset(logging.LogRecord("", 0, "", 0, "", None, None).__dict__) | {
    "asctime",
    "message",
    "taskName",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED:
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())
    logging.getLogger("uvicorn.access").handlers.clear()
