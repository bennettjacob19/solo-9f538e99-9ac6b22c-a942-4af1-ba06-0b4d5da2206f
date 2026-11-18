"""Test fixtures: an embedded PostgreSQL (pgserver) + reset application state.

Run with:  .venv/bin/pytest -v
The embedded server data lives under ./.pgdata-test and is reused across runs.
"""
from __future__ import annotations

import os
import pathlib

import pgserver
import pytest

PGDATA = pathlib.Path(__file__).resolve().parent.parent / ".pgdata-test"
TEST_DB = "auditdb_test"


@pytest.fixture(scope="session")
def pg_server():
    import psycopg

    PGDATA.mkdir(parents=True, exist_ok=True)
    server = pgserver.get_server(str(PGDATA))
    admin_dsn = server.get_uri()  # default maintenance database
    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        exists = conn.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", [TEST_DB]
        ).fetchone()
        if exists is None:
            conn.execute(f"CREATE DATABASE {TEST_DB}")
    dsn = server.get_uri(TEST_DB)
    os.environ["DATABASE_DSN"] = dsn
    os.environ["POOL_MIN_SIZE"] = "1"
    os.environ["POOL_MAX_SIZE"] = "8"
    os.environ["WORKER_POLL_SECONDS"] = "0.05"
    yield server, dsn


@pytest.fixture()
def clean_db(pg_server):
    """Drop app tables between tests so each test starts fresh."""
    import psycopg

    _server, dsn = pg_server
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS index_tasks CASCADE")
        conn.execute("DROP TABLE IF EXISTS audit_events CASCADE")
    yield dsn
