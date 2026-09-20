"""Engine, session factory, and the database clock the whole cluster reads time from."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def async_url(url: str) -> str:
    """Force the asyncpg driver, so callers can configure a plain postgresql:// URL."""
    parsed = make_url(url)
    if parsed.drivername in ("postgresql", "postgres"):
        parsed = parsed.set(drivername="postgresql+asyncpg")
    return parsed.render_as_string(hide_password=False)


async def db_now(session: AsyncSession) -> datetime:
    """The database's clock, which is the only clock the scheduler trusts."""
    return (await session.execute(select(func.now()))).scalar_one()


@dataclass(slots=True)
class Database:
    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        async with self.session_factory() as session:
            yield session

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        async with self.session_factory() as session, session.begin():
            yield session

    async def dispose(self) -> None:
        await self.engine.dispose()


def create_database(url: str, *, pool_max_size: int = 10, echo: bool = False) -> Database:
    engine = create_async_engine(
        async_url(url),
        echo=echo,
        pool_size=pool_max_size,
        max_overflow=0,
        pool_pre_ping=True,
        pool_timeout=10,
    )
    factory = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
    return Database(engine=engine, session_factory=factory)
