"""DuckGresQL Python SDK — DuckDB-compatible client for remote databases.

Quick start::

    import duckgresql

    conn = duckgresql.connect(token="dkgql_...", database="mydb")
    result = conn.execute("SELECT * FROM users LIMIT 10")
    print(result.fetchall())
    conn.close()
"""

from __future__ import annotations

from duckgresql._config import (
    DEFAULT_FLIGHT_PORT,
    DEFAULT_HOST,
    DEFAULT_REST_PORT,
    DEFAULT_REST_SCHEME,
    DEFAULT_USE_TLS,
)
from duckgresql._flight import Parameters
from duckgresql._types import JobStatus
from duckgresql._version import __version__
from duckgresql.async_connection import DuckgresqlAsync
from duckgresql.async_job import AsyncJob, AsyncJobAsync
from duckgresql.connection import Duckgresql
from duckgresql.exceptions import (
    AuthenticationError,
    ConnectionError,
    DuckgresqlError,
    JobError,
    QueryError,
    TimeoutError,
)
from duckgresql.result import DuckgresqlResult


def connect(
    host: str = DEFAULT_HOST,
    *,
    token: str,
    database: str,
    port: int = DEFAULT_FLIGHT_PORT,
    use_tls: bool = DEFAULT_USE_TLS,
    rest_port: int = DEFAULT_REST_PORT,
    rest_scheme: str = DEFAULT_REST_SCHEME,
) -> Duckgresql:
    """Create a synchronous connection to a DuckGresQL server.

    Parameters
    ----------
    host : str
        Server hostname or IP.  Defaults to ``DUCKGRESQL_HOST`` env var, or
        the value baked in at release time.
    token : str
        API token (``dkgql_…`` prefix).
    database : str
        Database name or UUID.
    port : int
        Arrow Flight SQL (gRPC) port.  Defaults to ``DUCKGRESQL_FLIGHT_PORT``
        env var, or the value baked in at release time.
    use_tls : bool
        Use TLS for Flight SQL.  Defaults to ``DUCKGRESQL_USE_TLS`` env var.
    rest_port : int
        REST API port.  Defaults to ``DUCKGRESQL_REST_PORT`` env var, or the
        value baked in at release time.
    rest_scheme : str
        ``"http"`` or ``"https"``.  Defaults to ``DUCKGRESQL_REST_SCHEME``
        env var.
    """
    return Duckgresql(
        host,
        token=token,
        database=database,
        port=port,
        use_tls=use_tls,
        rest_port=rest_port,
        rest_scheme=rest_scheme,
    )


async def connect_async(
    host: str = DEFAULT_HOST,
    *,
    token: str,
    database: str,
    port: int = DEFAULT_FLIGHT_PORT,
    use_tls: bool = DEFAULT_USE_TLS,
    rest_port: int = DEFAULT_REST_PORT,
    rest_scheme: str = DEFAULT_REST_SCHEME,
) -> DuckgresqlAsync:
    """Create an asynchronous connection to a DuckGresQL server.

    Same parameters as :func:`connect`.  Must be ``await``-ed.
    """
    return await DuckgresqlAsync.create(
        host,
        token=token,
        database=database,
        port=port,
        use_tls=use_tls,
        rest_port=rest_port,
        rest_scheme=rest_scheme,
    )


__all__ = [
    "__version__",
    "connect",
    "connect_async",
    "Duckgresql",
    "DuckgresqlAsync",
    "DuckgresqlResult",
    "AsyncJob",
    "AsyncJobAsync",
    "JobStatus",
    "Parameters",
    "DuckgresqlError",
    "ConnectionError",
    "AuthenticationError",
    "QueryError",
    "JobError",
    "TimeoutError",
]
