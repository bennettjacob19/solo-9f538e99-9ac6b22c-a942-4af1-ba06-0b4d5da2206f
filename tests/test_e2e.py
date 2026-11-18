"""End-to-end tests covering the required key scenarios."""
from __future__ import annotations

import asyncio
import time

import httpx
import psycopg
import pytest

from asgi_lifespan import LifespanManager

from app.config import settings
from app.main import app
from app.schemas import IndexMode
from app.service import INDEX_SPECS

TENANT_MODE = IndexMode.tenant_occurred.value
EVENT_MODE = IndexMode.event_type_occurred.value


class AppClient:
    """Start the FastAPI app (lifespan incl. worker) with an ASGI client."""

    def __init__(self):
        self.manager = LifespanManager(app, startup_timeout=30, shutdown_timeout=30)
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self):
        await self.manager.__aenter__()
        self._client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        )
        await self._client.__aenter__()
        return self._client

    async def __aexit__(self, *exc):
        await self._client.__aexit__(*exc)
        await self.manager.__aexit__(*exc)


async def wait_status(client, task_id, expected, timeout=60.0):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        r = await client.get(f"/index-tasks/{task_id}")
        assert r.status_code == 200
        last = r.json()
        if last["status"] == expected:
            return last
        if expected != "FAILED" and last["status"] == "FAILED":
            pytest.fail(f"task {task_id} FAILED unexpectedly: {last['error']}")
        await asyncio.sleep(0.1)
    pytest.fail(f"task {task_id} did not reach {expected}; last={last}")


def catalog_state(dsn: str, index_name: str):
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            """
            SELECT i.indisvalid, i.indisready
            FROM pg_class c
            JOIN pg_index i ON i.indexrelid = c.oid
            JOIN pg_class t ON t.oid = i.indrelid
            WHERE c.relname = %s AND t.relname = 'audit_events'
            """,
            [index_name],
        ).fetchone()
    return row  # None | (valid, ready)


@pytest.mark.asyncio
async def test_health_write_and_query(clean_db):
    async with AppClient() as client:
        assert (await client.get("/health")).json() == {"status": "ok"}

        r = await client.post(
            "/events", json={"tenant_id": "t1", "event_type": "login"}
        )
        assert r.status_code == 201
        event = r.json()
        assert event["id"] > 0 and event["occurred_at"]

        r = await client.post(
            "/events",
            json={
                "tenant_id": "t1",
                "event_type": "logout",
                "occurred_at": "2026-01-01T00:00:00Z",
            },
        )
        assert r.status_code == 201

        # Filtered queries required by the task.
        r = await client.get("/events", params={"tenant_id": "t1"})
        assert r.status_code == 200 and len(r.json()) == 2

        r = await client.get("/events", params={"event_type": "logout"})
        assert len(r.json()) == 1 and r.json()[0]["event_type"] == "logout"

        r = await client.get(
            "/events",
            params={
                "tenant_id": "t1",
                "occurred_after": "2025-01-01T00:00:00Z",
                "occurred_before": "2027-01-01T00:00:00Z",
            },
        )
        assert len(r.json()) == 2

        r = await client.get("/events")
        assert r.status_code == 400


@pytest.mark.asyncio
async def test_full_index_lifecycle(clean_db):
    dsn = clean_db
    index_name = INDEX_SPECS[IndexMode.tenant_occurred][0]
    assert catalog_state(dsn, index_name) is None

    async with AppClient() as client:
        r = await client.post(
            "/index-tasks", json={"operation_key": "k1", "mode": TENANT_MODE}
        )
        assert r.status_code == 201
        task = r.json()
        assert task["status"] in ("PENDING", "RUNNING")
        assert task["index_name"] == index_name

        done = await wait_status(client, task["id"], "SUCCEEDED")
        assert done["error"] is None

    state = catalog_state(dsn, index_name)
    assert state == (True, True)


@pytest.mark.asyncio
async def test_concurrent_identical_requests_share_one_task(clean_db):
    async with AppClient() as client:
        payload = {"operation_key": "dup-key", "mode": TENANT_MODE}
        responses = await asyncio.gather(
            *[client.post("/index-tasks", json=payload) for _ in range(8)]
        )
        codes = {r.status_code for r in responses}
        assert codes <= {201, 200}
        ids = {r.json()["id"] for r in responses}
        assert ids == {responses[0].json()["id"]}  # single shared task

        task_id = responses[0].json()["id"]
        await wait_status(client, task_id, "SUCCEEDED")

        listing = (await client.get("/index-tasks")).json()
        same = [t for t in listing if t["operation_key"] == "dup-key"]
        assert len(same) == 1


@pytest.mark.asyncio
async def test_mode_switch_conflicts_409(clean_db):
    async with AppClient() as client:
        r1 = await client.post(
            "/index-tasks", json={"operation_key": "shared-key", "mode": TENANT_MODE}
        )
        assert r1.status_code == 201
        await wait_status(client, r1.json()["id"], "SUCCEEDED")

        r2 = await client.post(
            "/index-tasks",
            json={"operation_key": "shared-key", "mode": EVENT_MODE},
        )
        assert r2.status_code == 409
        assert "mode" in r2.json()["detail"]

        # Same key + same mode remains idempotent.
        r3 = await client.post(
            "/index-tasks", json={"operation_key": "shared-key", "mode": TENANT_MODE}
        )
        assert r3.status_code == 201 and r3.json()["id"] == r1.json()["id"]


@pytest.mark.asyncio
async def test_same_mode_different_key_builds_once(clean_db):
    dsn = clean_db
    index_name = INDEX_SPECS[IndexMode.event_type_occurred][0]

    async with AppClient() as client:
        r1 = await client.post(
            "/index-tasks", json={"operation_key": "key-A", "mode": EVENT_MODE}
        )
        await wait_status(client, r1.json()["id"], "SUCCEEDED")

        r2 = await client.post(
            "/index-tasks", json={"operation_key": "key-B", "mode": EVENT_MODE}
        )
        assert r2.status_code == 201
        # The second task must also succeed quickly by reusing the physical index.
        await wait_status(client, r2.json()["id"], "SUCCEEDED", timeout=30)

    # Exactly one physical index for the mode.
    with psycopg.connect(dsn) as conn:
        count = conn.execute(
            """
            SELECT count(*) FROM pg_class c
            JOIN pg_index i ON i.indexrelid = c.oid
            JOIN pg_class t ON t.oid = i.indrelid
            WHERE t.relname = 'audit_events' AND c.relname = %s
            """,
            [index_name],
        ).fetchone()[0]
    assert count == 1


@pytest.mark.asyncio
async def test_invalid_residue_retry_cleans_and_succeeds(clean_db):
    """Simulate a failed CONCURRENTLY build leaving an INVALID index."""
    dsn = clean_db
    index_name = INDEX_SPECS[IndexMode.tenant_occurred][0]

    # Create a real table + an invalid index residue directly in the catalog.
    with psycopg.connect(dsn) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS audit_events (id bigint primary key, "
            "tenant_id text not null, event_type text not null, "
            "occurred_at timestamptz not null default now())"
        )
        conn.execute(f"CREATE INDEX {index_name} ON audit_events (tenant_id)")
        conn.execute("SET allow_system_table_mods = on")
        conn.execute(
            "UPDATE pg_index SET indisvalid = false, indisready = false "
            "WHERE indexrelid = %s::regclass",
            [index_name],
        )
        conn.execute("SET allow_system_table_mods = off")
    assert catalog_state(dsn, index_name) == (False, False)

    async with AppClient() as client:
        r = await client.post(
            "/index-tasks", json={"operation_key": "residue-key", "mode": TENANT_MODE}
        )
        task = await wait_status(client, r.json()["id"], "SUCCEEDED")
        assert task["error"] is None

    assert catalog_state(dsn, index_name) == (True, True)


@pytest.mark.asyncio
async def test_failed_build_retried_then_succeeds(clean_db):
    """Force a build failure, observe FAILED, then retry successfully."""
    dsn = clean_db
    index_name = INDEX_SPECS[IndexMode.tenant_occurred][0]

    # A view occupying the index name makes both DROP IF EXISTS and CREATE fail.
    with psycopg.connect(dsn) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS audit_events ("
            "id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY)"
        )
        conn.execute(f"CREATE VIEW {index_name} AS SELECT 1 AS x")

    async with AppClient() as client:
        r = await client.post(
            "/index-tasks", json={"operation_key": "fail-key", "mode": TENANT_MODE}
        )
        failed = await wait_status(client, r.json()["id"], "FAILED")
        assert failed["error"]  # failure reason recorded

        # Remove the obstruction, then retry the SAME (key, mode) pair.
        with psycopg.connect(dsn) as conn:
            conn.execute(f"DROP VIEW {index_name}")
            conn.execute(
                "ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS tenant_id text "
                "NOT NULL DEFAULT 'x', ADD COLUMN IF NOT EXISTS event_type text "
                "NOT NULL DEFAULT 'y', ADD COLUMN IF NOT EXISTS occurred_at "
                "timestamptz NOT NULL DEFAULT now()"
            )

        r2 = await client.post(
            "/index-tasks", json={"operation_key": "fail-key", "mode": TENANT_MODE}
        )
        # Same shared task row, re-queued.
        assert r2.json()["id"] == failed["id"]
        await wait_status(client, failed["id"], "SUCCEEDED")

    assert catalog_state(dsn, index_name) == (True, True)


@pytest.mark.asyncio
async def test_restart_reconciles_running_task_and_valid_index(clean_db):
    dsn = clean_db
    index_name = INDEX_SPECS[IndexMode.tenant_occurred][0]

    async with AppClient() as client:
        r = await client.post(
            "/index-tasks", json={"operation_key": "restart-key", "mode": TENANT_MODE}
        )
        task_id = r.json()["id"]
        await wait_status(client, task_id, "SUCCEEDED")

    # Simulate a crash mid-build: task row says RUNNING but no index exists.
    with psycopg.connect(dsn) as conn:
        conn.execute("UPDATE index_tasks SET status = 'RUNNING' WHERE id = %s", [task_id])
        conn.execute(f"DROP INDEX IF EXISTS {index_name}")

    # Restart: reconciliation must not claim success for missing residue.
    async with AppClient() as client:
        r = await client.get(f"/index-tasks/{task_id}")
        assert r.json()["status"] == "FAILED"
        assert "reconciled" in r.json()["error"]

        # Retry after restart: residue check + rebuild works.
        r2 = await client.post(
            "/index-tasks", json={"operation_key": "restart-key", "mode": TENANT_MODE}
        )
        assert r2.json()["id"] == task_id
        await wait_status(client, task_id, "SUCCEEDED")

    assert catalog_state(dsn, index_name) == (True, True)


@pytest.mark.asyncio
async def test_valid_index_never_dropped_on_restart(clean_db):
    dsn = clean_db
    index_name = INDEX_SPECS[IndexMode.tenant_occurred][0]

    async with AppClient() as client:
        r = await client.post(
            "/index-tasks", json={"operation_key": "keep-key", "mode": TENANT_MODE}
        )
        task_id = r.json()["id"]
        await wait_status(client, task_id, "SUCCEEDED")

    oid_before = catalog_state(dsn, index_name)
    assert oid_before == (True, True)
    with psycopg.connect(dsn) as conn:
        oid = conn.execute("SELECT %s::regclass::oid", [index_name]).fetchone()[0]

    # Restart twice; the valid index must survive untouched.
    for _ in range(2):
        async with AppClient():
            pass
    with psycopg.connect(dsn) as conn:
        oid_after = conn.execute("SELECT %s::regclass::oid", [index_name]).fetchone()[0]
        row = conn.execute(
            "SELECT status FROM index_tasks WHERE id = %s", [task_id]
        ).fetchone()
    assert oid_after == oid
    assert row[0] == "SUCCEEDED"


@pytest.mark.asyncio
async def test_writes_queries_while_build_is_blocked(clean_db):
    """Hold the build's advisory lock so CREATE INDEX is in flight, then prove
    normal writes and filtered queries are still served."""
    dsn = clean_db
    index_name = INDEX_SPECS[IndexMode.tenant_occurred][0]

    with psycopg.connect(dsn) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS audit_events ("
            "id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
            "tenant_id text NOT NULL, event_type text NOT NULL, "
            "occurred_at timestamptz NOT NULL DEFAULT now())"
        )
        conn.execute(
            "INSERT INTO audit_events (tenant_id, event_type) "
            "SELECT 't-' || (g % 10), 'e-' || (g % 5) "
            "FROM generate_series(1, 50000) g"
        )

    locker = psycopg.connect(dsn, autocommit=True)
    try:
        locker.execute("SELECT pg_advisory_lock(hashtext(%s))", [index_name])

        async with AppClient() as client:
            r = await client.post(
                "/index-tasks", json={"operation_key": "blocked-key", "mode": TENANT_MODE}
            )
            task_id = r.json()["id"]

            # Wait until the worker is genuinely blocked inside the build.
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                status = (await client.get(f"/index-tasks/{task_id}")).json()["status"]
                if status == "RUNNING":
                    break
                await asyncio.sleep(0.05)
            else:
                pytest.fail("task never reached RUNNING")

            # The build is now waiting on the advisory lock: traffic must flow.
            for i in range(10):
                w = await client.post(
                    "/events", json={"tenant_id": "live", "event_type": "live-ev"}
                )
                assert w.status_code == 201
                q = await client.get("/events", params={"tenant_id": "live"})
                assert q.status_code == 200 and len(q.json()) == i + 1
                await asyncio.sleep(0.05)

            locker.execute("SELECT pg_advisory_unlock(hashtext(%s))", [index_name])
            await wait_status(client, task_id, "SUCCEEDED", timeout=120)
    finally:
        locker.close()

    assert catalog_state(dsn, index_name) == (True, True)


@pytest.mark.asyncio
async def test_writes_and_queries_available_during_build(clean_db):
    """Saturate the table so CONCURRENTLY takes a while; meanwhile write/query."""
    dsn = clean_db
    index_name = INDEX_SPECS[IndexMode.event_type_occurred][0]

    # Pre-seed a sizeable table before starting the app.
    with psycopg.connect(dsn) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS audit_events ("
            "id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
            "tenant_id text NOT NULL, event_type text NOT NULL, "
            "occurred_at timestamptz NOT NULL DEFAULT now())"
        )
        conn.execute(
            "INSERT INTO audit_events (tenant_id, event_type) "
            "SELECT 'tenant-' || (g % 50), 'type-' || (g % 20) "
            "FROM generate_series(1, 200000) g"
        )

    async with AppClient() as client:
        build = await client.post(
            "/index-tasks", json={"operation_key": "heavy-key", "mode": EVENT_MODE}
        )
        task_id = build.json()["id"]

        # While the build runs (or finishes), writes and filtered queries work.
        for i in range(20):
            w = await client.post(
                "/events",
                json={"tenant_id": f"live-{i}", "event_type": "live-type"},
            )
            assert w.status_code == 201
            q = await client.get(
                "/events", params={"event_type": "live-type", "limit": 1000}
            )
            assert q.status_code == 200 and len(q.json()) == i + 1
            await asyncio.sleep(0.05)

        await wait_status(client, task_id, "SUCCEEDED", timeout=120)

    assert catalog_state(dsn, index_name) == (True, True)

    # The finished index is actually used by the filtered query.
    with psycopg.connect(dsn) as conn:
        plan = conn.execute(
            "EXPLAIN SELECT * FROM audit_events WHERE event_type = 'type-1' "
            "ORDER BY occurred_at DESC LIMIT 10"
        ).fetchall()
    assert any("idx_audit_events_event_type_occurred" in line[0] for line in plan), plan
