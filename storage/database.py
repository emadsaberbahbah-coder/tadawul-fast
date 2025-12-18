from __future__ import annotations

from datetime import datetime
from typing import Any, AsyncGenerator, Optional

from fastapi import HTTPException
from sqlalchemy import BigInteger, DateTime, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import JSON

from config import get_settings


class Base(DeclarativeBase):
    pass


def _is_postgres(url: str) -> bool:
    u = (url or "").lower()
    return u.startswith("postgres://") or u.startswith("postgresql://")


class DataHistory(Base):
    """
    Append-only self-learning history table.
    """
    __tablename__ = "data_history"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    page_id: Mapped[str] = mapped_column(String(128), index=True, nullable=False)
    region: Mapped[str] = mapped_column(String(32), index=True, nullable=False)

    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    schema_hash: Mapped[str] = mapped_column(String(64), index=True, nullable=False)

    payload: Mapped[Any] = mapped_column(
        JSONB if _is_postgres(get_settings().database_url) else JSON,
        nullable=False,
    )

    metadata: Mapped[Any] = mapped_column(
        JSONB if _is_postgres(get_settings().database_url) else JSON,
        nullable=True,
    )


_settings = get_settings()
_engine: Optional[AsyncEngine] = None
_sessionmaker: Optional[async_sessionmaker[AsyncSession]] = None

if _settings.database_url:
    _engine = create_async_engine(_settings.database_url, pool_pre_ping=True)
    _sessionmaker = async_sessionmaker(_engine, expire_on_commit=False)


async def init_db() -> None:
    """
    Creates tables on startup if DATABASE_URL is configured.
    """
    if not _engine:
        return
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency: yields an AsyncSession.
    """
    if not _sessionmaker:
        raise HTTPException(status_code=503, detail="DATABASE_URL is not configured on the server.")
    async with _sessionmaker() as session:
        yield session
