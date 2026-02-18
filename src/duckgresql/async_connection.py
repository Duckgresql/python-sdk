"""Asynchronous connection class for DuckGresQL."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

from duckgresql._config import (
    DEFAULT_FLIGHT_PORT,
    DEFAULT_HOST,
    DEFAULT_REST_PORT,
    DEFAULT_REST_SCHEME,
    DEFAULT_USE_TLS,
)
from duckgresql._flight import FlightSQLClient
from duckgresql._rest_async import AsyncRestClient
from duckgresql._types import _is_read_query
from duckgresql.async_job import AsyncJobAsync
from duckgresql.exceptions import ConnectionError
from duckgresql.result import DuckgresqlResult


class DuckgresqlAsync:
    """Asynchronous connection to a DuckGresQL server.

    Flight SQL calls (which are synchronous in pyarrow) are dispatched via
    :func:`asyncio.to_thread`.  REST calls use the native async httpx client.

    Use :func:`duckgresql.connect_async` to create instances.
    """

    def __init__(
        self,
        flight: FlightSQLClient,
        rest: AsyncRestClient,
        conn_token: str,
    ) -> None:
        self._flight = flight
        self._rest = rest
        self._conn_token = conn_token
        self._closed = False

    @classmethod
    async def create(
        cls,
        host: str = DEFAULT_HOST,
        *,
        token: str,
        database: str,
        port: int = DEFAULT_FLIGHT_PORT,
        use_tls: bool = DEFAULT_USE_TLS,
        rest_port: int = DEFAULT_REST_PORT,
        rest_scheme: str = DEFAULT_REST_SCHEME,
    ) -> DuckgresqlAsync:
        """Factory that performs the async handshake and returns a ready connection."""
        # Flight SQL client (sync, will be used via to_thread)
        flight = await asyncio.to_thread(
            FlightSQLClient, host, port, token, database, use_tls=use_tls,
        )

        # REST client (async)
        base_url = f"{rest_scheme}://{host}:{rest_port}"
        rest = AsyncRestClient(base_url)
        try:
            conn_token = await rest.connect(token, database)
        except Exception as exc:
            flight.close()
            await rest.close()
            raise ConnectionError(f"REST /connect failed: {exc}") from exc

        return cls(flight, rest, conn_token)

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    async def execute(
        self,
        query: str,
        parameters: Sequence[Any] | None = None,
    ) -> DuckgresqlResult:
        """Execute *query* via Flight SQL (in a thread) and return a result."""
        self._ensure_open()
        sql = _interpolate(query, parameters)

        if _is_read_query(sql):
            table = await asyncio.to_thread(self._flight.execute_query, sql)
            return DuckgresqlResult(table)
        else:
            affected = await asyncio.to_thread(self._flight.execute_update, sql)
            return DuckgresqlResult(affected_rows=affected)

    async def sql(
        self,
        query: str,
        parameters: Sequence[Any] | None = None,
    ) -> DuckgresqlResult:
        """Alias for :meth:`execute`."""
        return await self.execute(query, parameters)

    async def executemany(
        self,
        query: str,
        parameters_list: Sequence[Sequence[Any]],
    ) -> DuckgresqlResult:
        """Execute *query* once per parameter set."""
        self._ensure_open()
        total_affected = 0
        for params in parameters_list:
            sql = _interpolate(query, params)
            affected = await asyncio.to_thread(self._flight.execute_update, sql)
            total_affected += affected
        return DuckgresqlResult(affected_rows=total_affected)

    async def execute_async(
        self,
        query: str,
        parameters: Sequence[Any] | None = None,
        bindings: Any | None = None,
    ) -> AsyncJobAsync:
        """Submit *query* for async execution. Returns an :class:`AsyncJobAsync`."""
        self._ensure_open()
        sql = _interpolate(query, parameters)
        job_id = await self._rest.submit_async(self._conn_token, sql, bindings)
        return AsyncJobAsync(self._rest, self._conn_token, job_id)

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """Close the connection and release resources."""
        if not self._closed:
            self._flight.close()
            await self._rest.close()
            self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    async def __aenter__(self) -> DuckgresqlAsync:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ensure_open(self) -> None:
        if self._closed:
            raise ConnectionError("Connection is closed")


def _interpolate(query: str, parameters: Sequence[Any] | None) -> str:
    """Replace ``$1``, ``$2``, … placeholders with literal values."""
    if not parameters:
        return query
    sql = query
    for i, val in enumerate(parameters, start=1):
        placeholder = f"${i}"
        if isinstance(val, str):
            escaped = val.replace("'", "''")
            literal = f"'{escaped}'"
        elif val is None:
            literal = "NULL"
        else:
            literal = str(val)
        sql = sql.replace(placeholder, literal)
    return sql
