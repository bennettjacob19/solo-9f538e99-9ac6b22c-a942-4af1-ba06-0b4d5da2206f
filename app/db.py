"""Database connection pool and schema bootstrap.

The app uses a small psycopg connection pool for normal transactional work.
Index DDL uses separate, short-lived autocommit connections because
``CREATE INDEX CONCURRENTLY`` cannot run inside a transaction block.
"""
from __future__ import annotations

import logging

from psycopg_pool import AsyncConnectionPool

from .config import settings

logger = logging.getLogger("audit_indexer.db")

pool: AsyncConnectionPool | None = None

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS audit_events (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id   TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS index_tasks (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    operation_key TEXT NOT NULL,
    mode         TEXT NOT NULL CHECK (mode IN ('tenant_occurred', 'event_type_occurred')),
    status       TEXT NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED')),
    error        TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (operation_key, mode)
);

-- Fast lookup of the most advanced task for an index mode.
CREATE INDEX IF NOT EXISTS idx_index_tasks_mode_status
    ON index_tasks (mode, status);
"""


async def open_pool() -> AsyncConnectionPool:
    global pool
    pool = AsyncConnectionPool(
        conninfo=settings.database_dsn,
        min_size=settings.pool_min_size,
        max_size=settings.pool_max_size,
        kwargs={"autocommit": False},
        open=False,
    )
    await pool.open()
    async with pool.connection() as conn:
        await conn.execute(SCHEMA_SQL)
    logger.info("connection pool open and schema ensured")
    return pool


async def close_pool() -> None:
    if pool is not None:
        await pool.close()
        logger.info("connection pool closed")


async def get_conn():
    """FastAPI dependency: borrow a pooled connection (native async-gen)."""
    assert pool is not None, "pool not initialised"
    async with pool.connection() as conn:
        yield conn
