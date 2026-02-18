"""Exception hierarchy for the DuckGresQL Python SDK."""

from __future__ import annotations


class DuckgresqlError(Exception):
    """Base exception for all DuckGresQL errors."""


class ConnectionError(DuckgresqlError):
    """Raised when a connection to the server cannot be established."""


class AuthenticationError(DuckgresqlError):
    """Raised when authentication fails (invalid token, expired, revoked)."""


class QueryError(DuckgresqlError):
    """Raised when a query execution fails on the server."""


class JobError(DuckgresqlError):
    """Raised when an async job operation fails."""


class TimeoutError(DuckgresqlError):
    """Raised when an operation exceeds the configured timeout."""
