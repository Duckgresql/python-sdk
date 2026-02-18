"""Synchronous connection class for DuckGresQL."""

from __future__ import annotations

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
from duckgresql._rest import RestClient
from duckgresql._types import _is_read_query
from duckgresql.async_job import AsyncJob
from duckgresql.exceptions import ConnectionError
from duckgresql.result import DuckgresqlResult


class Duckgresql:
    """Synchronous connection to a DuckGresQL server.

    Mirrors the DuckDB Python API (``execute``, ``fetchall``, ``fetchdf``, …).

    Use :func:`duckgresql.connect` to create instances.
    """

    def __init__(
        self,
        host: str = DEFAULT_HOST,
        *,
        token: str,
        database: str,
        port: int = DEFAULT_FLIGHT_PORT,
        use_tls: bool = DEFAULT_USE_TLS,
        rest_port: int = DEFAULT_REST_PORT,
        rest_scheme: str = DEFAULT_REST_SCHEME,
    ) -> None:
        self._host = host
        self._token = token
        self._database = database
        self._closed = False

        # Flight SQL (primary transport for execute)
        self._flight = FlightSQLClient(
            host, port, token, database, use_tls=use_tls,
        )

        # REST client (for /connect + async jobs)
        base_url = f"{rest_scheme}://{host}:{rest_port}"
        self._rest = RestClient(base_url)
        try:
            self._conn_token: str = self._rest.connect(token, database)
        except Exception as exc:
            self._flight.close()
            self._rest.close()
            raise ConnectionError(f"REST /connect failed: {exc}") from exc

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    def execute(
        self,
        query: str,
        parameters: Sequence[Any] | None = None,
    ) -> DuckgresqlResult:
        """Execute *query* via Flight SQL and return a :class:`DuckgresqlResult`.

        Read queries (SELECT, WITH, …) use ``execute_query``.
        DML queries (INSERT, UPDATE, DELETE, …) use ``execute_update``.
        """
        self._ensure_open()
        sql = self._interpolate(query, parameters)

        if _is_read_query(sql):
            table = self._flight.execute_query(sql)
            return DuckgresqlResult(table)
        else:
            affected = self._flight.execute_update(sql)
            return DuckgresqlResult(affected_rows=affected)

    def sql(
        self,
        query: str,
        parameters: Sequence[Any] | None = None,
    ) -> DuckgresqlResult:
        """Alias for :meth:`execute`."""
        return self.execute(query, parameters)

    def executemany(
        self,
        query: str,
        parameters_list: Sequence[Sequence[Any]],
    ) -> DuckgresqlResult:
        """Execute *query* once per parameter set. Returns the last result."""
        self._ensure_open()
        total_affected = 0
        for params in parameters_list:
            sql = self._interpolate(query, params)
            total_affected += self._flight.execute_update(sql)
        return DuckgresqlResult(affected_rows=total_affected)

    def execute_async(
        self,
        query: str,
        parameters: Sequence[Any] | None = None,
        bindings: Any | None = None,
    ) -> AsyncJob:
        """Submit *query* for asynchronous execution via REST.

        Returns an :class:`AsyncJob` that can be polled for results.
        """
        self._ensure_open()
        sql = self._interpolate(query, parameters)
        job_id = self._rest.submit_async(self._conn_token, sql, bindings)
        return AsyncJob(self._rest, self._conn_token, job_id)

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the connection and release resources."""
        if not self._closed:
            self._flight.close()
            self._rest.close()
            self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    def __enter__(self) -> Duckgresql:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ensure_open(self) -> None:
        if self._closed:
            raise ConnectionError("Connection is closed")

    @staticmethod
    def _interpolate(query: str, parameters: Sequence[Any] | None) -> str:
        """Replace positional ``$1``, ``$2``, … placeholders with literal values.

        This is a convenience for simple parameter substitution client-side.
        For production use, rely on server-side parameter binding via
        ``execute_async(bindings=...)``.
        """
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
