"""Internal types and constants for the DuckGresQL Python SDK."""

from __future__ import annotations

import enum


class JobStatus(enum.Enum):
    """Status of an async query job."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
