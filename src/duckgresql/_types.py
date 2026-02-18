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


# SQL prefixes that indicate a read query (returns rows).
_READ_PREFIXES = frozenset({
    "SELECT",
    "WITH",
    "EXPLAIN",
    "SHOW",
    "DESCRIBE",
    "PRAGMA",
    "TABLE",
    "FROM",
    "VALUES",
})


def _is_read_query(sql: str) -> bool:
    """Return True if *sql* looks like a read query (SELECT, etc.)."""
    first_word = sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ""
    return first_word in _READ_PREFIXES
