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
from duckgresql._flight import FlightSQLClient, Parameters
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
        parameters: Parameters = None,
    ) -> DuckgresqlResult:
        """Execute *query* via Flight SQL and return a :class:`DuckgresqlResult`.

        Parameters are bound server-side.  Pass a list for positional
        placeholders (``$1``, ``$2``, …) or a dict for named placeholders
        (``$name``).
        """
        self._ensure_open()
        if _is_read_query(query):
            table = self._flight.execute_query(query, parameters)
            return DuckgresqlResult(table)
        else:
            affected = self._flight.execute_update(query, parameters)
            return DuckgresqlResult(affected_rows=affected)

    def sql(
        self,
        query: str,
        parameters: Parameters = None,
    ) -> DuckgresqlResult:
        """Alias for :meth:`execute`."""
        return self.execute(query, parameters)

    def executemany(
        self,
        query: str,
        parameters_list: Sequence[Sequence[Any] | dict[str, Any]],
    ) -> DuckgresqlResult:
        """Execute *query* once per parameter set. Returns the last result."""
        self._ensure_open()
        total_affected = 0
        for params in parameters_list:
            total_affected += self._flight.execute_update(query, params)
        return DuckgresqlResult(affected_rows=total_affected)

    def execute_async(
        self,
        query: str,
        parameters: Parameters = None,
    ) -> AsyncJob:
        """Submit *query* for asynchronous execution via REST.

        Returns an :class:`AsyncJob` that can be polled for results.
        """
        self._ensure_open()
        job_id = self._rest.submit_async(self._conn_token, query, parameters)
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
