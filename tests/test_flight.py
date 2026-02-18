"""Tests for FlightSQLClient."""

from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from duckgresql._flight import FlightSQLClient
from duckgresql.exceptions import AuthenticationError, QueryError


class TestFlightSQLClient:
    def test_connect_and_query(
        self, mock_flight_client: MagicMock, sample_table: pa.Table
    ) -> None:
        client = FlightSQLClient("localhost", 47470, "tok", "db")
        result = client.execute_query("SELECT 1")
        assert result.num_rows == sample_table.num_rows
        client.close()

    def test_execute_update(self, mock_flight_client: MagicMock) -> None:
        # Make do_get return a table with affected_rows column
        affected_table = pa.table({"affected_rows": [5]})
        mock_reader = MagicMock()
        mock_reader.read_all.return_value = affected_table
        mock_flight_client.do_get.return_value = mock_reader

        client = FlightSQLClient("localhost", 47470, "tok", "db")
        count = client.execute_update("INSERT INTO t VALUES (1)")
        assert count == 5
        client.close()

    def test_auth_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import pyarrow.flight as flight

        mock_client = MagicMock()
        mock_client.authenticate_basic_token.side_effect = (
            flight.FlightUnauthenticatedError("bad token")
        )
        monkeypatch.setattr("pyarrow.flight.FlightClient", lambda *a, **kw: mock_client)

        with pytest.raises(AuthenticationError, match="bad token"):
            FlightSQLClient("localhost", 47470, "bad", "db")

    def test_query_error(self, mock_flight_client: MagicMock) -> None:
        mock_flight_client.get_flight_info.side_effect = RuntimeError("query failed")

        client = FlightSQLClient("localhost", 47470, "tok", "db")
        with pytest.raises(QueryError, match="query failed"):
            client.execute_query("BAD SQL")

    def test_close_idempotent(self, mock_flight_client: MagicMock) -> None:
        client = FlightSQLClient("localhost", 47470, "tok", "db")
        client.close()
        client.close()  # should not raise
        assert client.closed

    def test_empty_endpoints(self, mock_flight_client: MagicMock) -> None:
        mock_info = MagicMock()
        mock_info.endpoints = []
        mock_flight_client.get_flight_info.return_value = mock_info

        client = FlightSQLClient("localhost", 47470, "tok", "db")
        result = client.execute_query("SELECT 1")
        assert result.num_rows == 0
