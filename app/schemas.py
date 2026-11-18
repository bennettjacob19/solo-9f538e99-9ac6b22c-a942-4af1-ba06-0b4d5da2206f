"""Pydantic schemas for the HTTP API."""
from __future__ import annotations

import enum
from datetime import datetime

from pydantic import BaseModel, Field


class IndexMode(str, enum.Enum):
    # tenant_id + occurred_at composite index
    tenant_occurred = "tenant_occurred"
    # event_type + occurred_at composite index
    event_type_occurred = "event_type_occurred"


class TaskStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class IndexRequest(BaseModel):
    operation_key: str = Field(..., min_length=1, max_length=200)
    mode: IndexMode


class TaskOut(BaseModel):
    id: int
    operation_key: str
    mode: IndexMode
    status: TaskStatus
    error: str | None
    index_name: str | None
    created_at: datetime
    updated_at: datetime


class EventIn(BaseModel):
    tenant_id: str = Field(..., min_length=1, max_length=200)
    event_type: str = Field(..., min_length=1, max_length=200)
    occurred_at: datetime | None = None


class EventOut(EventIn):
    id: int
    occurred_at: datetime
