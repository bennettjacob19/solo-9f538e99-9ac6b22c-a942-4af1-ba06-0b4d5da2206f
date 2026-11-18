"""FastAPI application: index task management plus event writes/queries."""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import Depends, FastAPI, HTTPException, Query, status
from psycopg_pool import AsyncConnectionPool

from .db import get_conn, open_pool, close_pool
from .schemas import EventIn, EventOut, IndexMode, IndexRequest, TaskOut, TaskStatus
from .service import (
    INDEX_SPECS,
    ModeConflictError,
    apply_for_index,
    get_task,
    list_tasks,
    reconcile_with_catalog,
    worker_loop,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("audit_indexer.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await open_pool()
    # After a restart, align every task row with the real catalog state
    # before accepting or building anything.
    await reconcile_with_catalog()
    stop_event = asyncio.Event()
    worker = asyncio.create_task(worker_loop(stop_event))
    try:
        yield
    finally:
        stop_event.set()
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass
        await close_pool()


app = FastAPI(
    title="Audit Event Online Index Service",
    version="1.0.0",
    description=(
        "Apply for online (CONCURRENTLY) indexes on the continuously written "
        "audit_events table, track task status, and write/query events."
    ),
    lifespan=lifespan,
)


def _to_task_out(task: dict) -> TaskOut:
    mode = IndexMode(task["mode"])
    return TaskOut(
        id=task["id"],
        operation_key=task["operation_key"],
        mode=mode,
        status=TaskStatus(task["status"]),
        error=task["error"],
        index_name=INDEX_SPECS[mode][0],
        created_at=task["created_at"],
        updated_at=task["updated_at"],
    )


# --------------------------------------------------------------------------- #
# Index task endpoints
# --------------------------------------------------------------------------- #
@app.post(
    "/index-tasks",
    response_model=TaskOut,
    status_code=status.HTTP_201_CREATED,
    tags=["index-tasks"],
    summary="Apply for an index (concurrent identical retries share the task)",
)
async def create_index_task(req: IndexRequest):
    try:
        task, _created = await apply_for_index(req.operation_key, req.mode)
    except ModeConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
    return _to_task_out(task)


@app.get("/index-tasks", response_model=list[TaskOut], tags=["index-tasks"])
async def get_all_tasks():
    return [_to_task_out(t) for t in await list_tasks()]


@app.get("/index-tasks/{task_id}", response_model=TaskOut, tags=["index-tasks"])
async def get_single_task(task_id: int):
    task = await get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    return _to_task_out(task)


# --------------------------------------------------------------------------- #
# Event endpoints
# --------------------------------------------------------------------------- #
@app.post(
    "/events",
    response_model=EventOut,
    status_code=status.HTTP_201_CREATED,
    tags=["events"],
    summary="Write one audit event (always available, including during builds)",
)
async def create_event(event: EventIn, conn: AsyncConnectionPool = Depends(get_conn)):
    cur = await conn.execute(
        "INSERT INTO audit_events (tenant_id, event_type, occurred_at) "
        "VALUES (%s, %s, COALESCE(%s, now())) RETURNING id, tenant_id, event_type, occurred_at",
        [event.tenant_id, event.event_type, event.occurred_at],
    )
    row = await cur.fetchone()
    return EventOut(id=row[0], tenant_id=row[1], event_type=row[2], occurred_at=row[3])


@app.get("/events", response_model=list[EventOut], tags=["events"], summary="Query events")
async def query_events(
    tenant_id: str | None = Query(None, description="filter by tenant_id"),
    event_type: str | None = Query(None, description="filter by event_type"),
    occurred_after: datetime | None = Query(None, description="inclusive lower bound"),
    occurred_before: datetime | None = Query(None, description="exclusive upper bound"),
    limit: int = Query(100, ge=1, le=1000),
    conn: AsyncConnectionPool = Depends(get_conn),
):
    if not tenant_id and not event_type:
        raise HTTPException(
            status_code=400,
            detail="at least one of tenant_id or event_type is required",
        )

    clauses: list[str] = []
    params: list = []
    if tenant_id:
        params.append(tenant_id)
        clauses.append(f"tenant_id = %s")
    if event_type:
        params.append(event_type)
        clauses.append(f"event_type = %s")
    if occurred_after:
        params.append(occurred_after)
        clauses.append(f"occurred_at >= %s")
    if occurred_before:
        params.append(occurred_before)
        clauses.append(f"occurred_at < %s")
    params.append(limit)

    sql = (
        "SELECT id, tenant_id, event_type, occurred_at FROM audit_events "
        + " WHERE " + " AND ".join(clauses)
        + " ORDER BY occurred_at DESC, id DESC LIMIT %s"
    )
    cur = await conn.execute(sql, params)
    rows = await cur.fetchall()
    return [
        EventOut(id=r[0], tenant_id=r[1], event_type=r[2], occurred_at=r[3])
        for r in rows
    ]


@app.get("/health", tags=["system"])
async def health():
    return {"status": "ok"}
