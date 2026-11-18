"""Application configuration, driven entirely by environment variables.

Values are read lazily so tests can point ``DATABASE_DSN`` at an embedded
PostgreSQL instance before the application opens its pool.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


@dataclass
class Settings:
    @property
    def database_dsn(self) -> str:
        return _env(
            "DATABASE_DSN",
            "postgresql://postgres:postgres@localhost:5432/auditdb",
        )

    @property
    def pool_min_size(self) -> int:
        return int(_env("POOL_MIN_SIZE", "1"))

    @property
    def pool_max_size(self) -> int:
        return int(_env("POOL_MAX_SIZE", "8"))

    @property
    def worker_poll_seconds(self) -> float:
        return float(_env("WORKER_POLL_SECONDS", "0.5"))


settings = Settings()
