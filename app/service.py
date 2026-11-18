"""Index task lifecycle: application, reconciliation, and the build worker.

Concurrency / idempotency rules
-------------------------------
* One physical index exists per *mode*; its name is deterministic, so requests
  with different ``operation_key`` values but the same mode never trigger a
  second physical build.
* ``(operation_key, mode)`` is UNIQUE, so identical concurrent retries share
  one task row. A retry of a FAILED task re-queues the same row.
* The same ``operation_key`` with a different mode is rejected with 409.
* All DDL uses ``CREATE/DROP INDEX CONCURRENTLY`` on autocommit connections,
  so event writes and queries keep working while a build runs.
* Truth comes from the PostgreSQL catalog (``pg_class``/``pg_index``):
  invalid residue is never reported as SUCCEEDED, retries drop residue first,
  and valid indexes are never dropped.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import psycopg

from . import db
from .config import settings
from .schemas import IndexMode, TaskStatus

logger = logging.getLogger("audit_indexer.service")

# Deterministic, mode-level index names -> "same mode, different key" reuses
# the single physical index instead of building another one.
INDEX_SPECS: dict[IndexMode, tuple[str, str]] = {
    IndexMode.tenant_occurred: (
        "idx_audit_events_tenant_occurred",
        "CREATE INDEX CONCURRENTLY idx_audit_events_tenant_occurred "
        "ON audit_events (tenant_id, occurred_at DESC)",
    ),
    IndexMode.event_type_occurred: (
        "idx_audit_events_event_type_occurred",
        "CREATE INDEX CONCURRENTLY idx_audit_events_event_type_occurred "
        "ON audit_events (event_type, occurred_at DESC)",
    ),
}

CATALOG_SQL = """
SELECT i.indisvalid, i.indisready, c.oid
FROM pg_class c
JOIN pg_index i ON i.indexrelid = c.oid
JOIN pg_class t ON t.oid = i.indrelid
WHERE c.relname = %s
  AND t.relname = 'audit_events'
  AND c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
"""


@dataclass(frozen=True)
class IndexCatalogState:
    exists: bool
    valid: bool
    ready: bool
    oid: int | None


async def inspect_index(conn, index_name: str) -> IndexCatalogState:
    """Read the authoritative state of an index from the catalog."""
    cur = await conn.execute(CATALOG_SQL, [index_name])
    row = await cur.fetchone()
    if row is None:
        return IndexCatalogState(False, False, False, None)
    indisvalid, indisready, oid = row
    return IndexCatalogState(True, bool(indisvalid), bool(indisready), oid)


# --------------------------------------------------------------------------- #
# Task application
# --------------------------------------------------------------------------- #
class ModeConflictError(Exception):
    """Same operation_key already exists with a different index mode."""


TASK_COLUMNS = (
    "id",
    "operation_key",
    "mode",
    "status",
    "error",
    "created_at",
    "updated_at",
)


def _task_dict(row: tuple) -> dict:
    return dict(zip(TASK_COLUMNS, row))


async def apply_for_index(operation_key: str, mode: IndexMode) -> tuple[dict, bool]:
    """Create (or share) an index task.

    Returns ``(task_row, created)``. Raises :class:`ModeConflictError` when
    the operation key is already bound to another mode.
    """
    assert db.pool is not None
    async with db.pool.connection() as conn:
        async with conn.transaction():
            cur = await conn.execute(
                f"SELECT {', '.join(TASK_COLUMNS)} FROM index_tasks "
                "WHERE operation_key = %s ORDER BY id",
                [operation_key],
            )
            existing = await cur.fetchall()

            for row in existing:
                task = _task_dict(row)
                if task["mode"] != mode.value:
                    raise ModeConflictError(
                        f"operation_key {operation_key!r} already uses mode "
                        f"{task['mode']!r}; cannot switch to {mode.value!r}"
                    )

            if existing:
                task = _task_dict(existing[0])
                # A retry of a failed attempt re-queues the shared task.
                if task["status"] == TaskStatus.FAILED.value:
                    await conn.execute(
                        "UPDATE index_tasks SET status = 'PENDING', error = NULL, "
                        "updated_at = now() WHERE id = %s AND status = 'FAILED'",
                        [task["id"]],
                    )
                    task["status"] = TaskStatus.PENDING.value
                    task["error"] = None
                return task, False

            # ON CONFLICT DO NOTHING covers the race where two identical
            # requests insert at the same instant: both end up sharing one row.
            await conn.execute(
                "INSERT INTO index_tasks (operation_key, mode, status) "
                "VALUES (%s, %s, 'PENDING') ON CONFLICT DO NOTHING",
                [operation_key, mode.value],
            )
            row = await (
                await conn.execute(
                    f"SELECT {', '.join(TASK_COLUMNS)} FROM index_tasks "
                    "WHERE operation_key = %s AND mode = %s",
                    [operation_key, mode.value],
                )
            ).fetchone()
            return _task_dict(row), True


async def get_task(task_id: int) -> dict | None:
    assert db.pool is not None
    async with db.pool.connection() as conn:
        cur = await conn.execute(
            f"SELECT {', '.join(TASK_COLUMNS)} FROM index_tasks WHERE id = %s",
            [task_id],
        )
        row = await cur.fetchone()
        return _task_dict(row) if row else None


async def list_tasks() -> list[dict]:
    assert db.pool is not None
    async with db.pool.connection() as conn:
        cur = await conn.execute(
            f"SELECT {', '.join(TASK_COLUMNS)} FROM index_tasks ORDER BY id"
        )
        return [_task_dict(row) for row in await cur.fetchall()]


# --------------------------------------------------------------------------- #
# Startup reconciliation: make task state match the PostgreSQL catalog
# --------------------------------------------------------------------------- #
async def reconcile_with_catalog() -> None:
    """Fix task state after a restart or an externally failed build.

    * Valid catalog index  -> task must be SUCCEEDED.
    * Missing/invalid index -> RUNNING/SUCCEEDED tasks become FAILED
      (invalid residue is left for the next retry to clean up; it is never
      reported as success and a valid index is never dropped).
    * PENDING/FAILED rows are left for the worker (retry cleans residue).
    """
    assert db.pool is not None
    logger.info("reconciling index tasks with PostgreSQL catalog")
    async with db.pool.connection() as conn:
        cur = await conn.execute("SELECT id, mode, status FROM index_tasks ORDER BY id")
        tasks = await cur.fetchall()
        for task_id, mode_value, status in tasks:
            mode = IndexMode(mode_value)
            index_name = INDEX_SPECS[mode][0]
            state = await inspect_index(conn, index_name)

            if state.exists and state.valid:
                if status != TaskStatus.SUCCEEDED.value:
                    await conn.execute(
                        "UPDATE index_tasks SET status = 'SUCCEEDED', error = NULL, "
                        "updated_at = now() WHERE id = %s",
                        [task_id],
                    )
                    logger.info("task %s: catalog index %s is valid -> SUCCEEDED", task_id, index_name)
            else:
                reason = "index missing from catalog" if not state.exists else "index is INVALID in catalog"
                if status in (TaskStatus.RUNNING.value, TaskStatus.SUCCEEDED.value):
                    await conn.execute(
                        "UPDATE index_tasks SET status = 'FAILED', error = %s, "
                        "updated_at = now() WHERE id = %s",
                        [f"reconciled: {reason}", task_id],
                    )
                    logger.warning("task %s: %s -> FAILED", task_id, reason)


# --------------------------------------------------------------------------- #
# Build worker
# --------------------------------------------------------------------------- #
async def _claim_pending_task(conn) -> dict | None:
    """Atomically claim one PENDING task (SKIP LOCKED safe for multi-worker)."""
    cur = await conn.execute(
        """
        UPDATE index_tasks t SET status = 'RUNNING', updated_at = now()
        WHERE t.id = (
            SELECT id FROM index_tasks
            WHERE status = 'PENDING'
            ORDER BY id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING id, operation_key, mode, status, error, created_at, updated_at
        """
    )
    row = await cur.fetchone()
    return _task_dict(row) if row else None


async def _set_terminal(task_id: int, status: TaskStatus, error: str | None) -> None:
    assert db.pool is not None
    async with db.pool.connection() as conn:
        await conn.execute(
            "UPDATE index_tasks SET status = %s, error = %s, updated_at = now() WHERE id = %s",
            [status.value, error, task_id],
        )


async def _build_one_index(task: dict) -> None:
    """Serial, catalog-driven build for one claimed task.

    DDL runs on a dedicated autocommit connection because
    ``CREATE INDEX CONCURRENTLY`` cannot run inside a transaction.
    """
    task_id = task["id"]
    mode = IndexMode(task["mode"])
    index_name, create_sql = INDEX_SPECS[mode]
    logger.info("task %s: building %s", task_id, index_name)

    try:
        async with await psycopg.AsyncConnection.connect(
            settings.database_dsn, autocommit=True
        ) as ddl_conn:
            # Advisory lock serialises builds of the same mode even if several
            # API processes ever run workers; it auto-releases on disconnect.
            await ddl_conn.execute("SELECT pg_advisory_lock(hashtext(%s))", [index_name])
            try:
                state = await inspect_index(ddl_conn, index_name)

                if state.exists and state.valid:
                    # Another task for this mode already built it: reuse it,
                    # never rebuild, never drop a valid index.
                    logger.info("task %s: %s already valid (oid=%s), reusing", task_id, index_name, state.oid)
                else:
                    if state.exists:
                        # Invalid residue from a previous failed build:
                        # retries clean it up before rebuilding.
                        logger.warning("task %s: dropping INVALID residue %s", task_id, index_name)
                        await ddl_conn.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {index_name}")

                    logger.info("task %s: %s", task_id, create_sql)
                    await ddl_conn.execute(create_sql)

                state = await inspect_index(ddl_conn, index_name)
                if not (state.exists and state.valid):
                    raise RuntimeError(
                        f"{index_name} is not valid after build "
                        f"(exists={state.exists}, valid={state.valid})"
                    )
            finally:
                await ddl_conn.execute("SELECT pg_advisory_unlock(hashtext(%s))", [index_name])

        await _set_terminal(task_id, TaskStatus.SUCCEEDED, None)
        logger.info("task %s: SUCCEEDED", task_id)
    except Exception as exc:  # noqa: BLE001 - record every failure on the task
        message = str(exc).strip() or exc.__class__.__name__
        logger.exception("task %s: build failed: %s", task_id, message)
        await _set_terminal(task_id, TaskStatus.FAILED, message[:2000])


async def worker_loop(stop_event: asyncio.Event) -> None:
    """Single worker loop; tasks are claimed and built one at a time."""
    assert db.pool is not None
    logger.info("index build worker started")
    try:
        while not stop_event.is_set():
            try:
                async with db.pool.connection() as conn:
                    task = await _claim_pending_task(conn)
                if task is None:
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=settings.worker_poll_seconds)
                    except asyncio.TimeoutError:
                        pass
                    continue
                await _build_one_index(task)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - worker must survive transient errors
                logger.exception("worker iteration failed; will retry")
                await asyncio.sleep(settings.worker_poll_seconds)
    finally:
        logger.info("index build worker stopped")
