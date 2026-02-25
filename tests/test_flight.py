"""Tests for FlightSQLClient."""

from __future__ import annotations

from unittest.mock import MagicMock, call

import pyarrow as pa
import pyarrow.flight as pflight
import pytest

from duckgresql._flight import (
    FlightSQLClient,
    _build_params_batch,
    _flight_sql_command,
    _parse_prepared_statement_result,
    _parse_varint,
)
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
        mock_client = MagicMock()
        mock_client.authenticate_basic_token.side_effect = (
            pflight.FlightUnauthenticatedError("bad token")
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


class TestFlightSqlCommand:
    def test_command_without_parameters(self) -> None:
        cmd = _flight_sql_command("SELECT 1")
        assert b"SELECT 1" in cmd

    def test_command_with_none_uses_direct_path(
        self, mock_flight_client: MagicMock
    ) -> None:
        """When parameters is None, use direct CommandStatementQuery (no do_action)."""
        client = FlightSQLClient("localhost", 47470, "tok", "db")
        client.execute_query("SELECT 1")
        mock_flight_client.do_action.assert_not_called()


class TestPreparedStatementFlow:
    def test_execute_query_with_positional_parameters(
        self, mock_flight_client: MagicMock, sample_table: pa.Table
    ) -> None:
        """Positional params trigger the prepared statement flow."""
        client = FlightSQLClient("localhost", 47470, "tok", "db")
        result = client.execute_query("SELECT * FROM t WHERE id = $1", [42])

        assert result.num_rows == sample_table.num_rows

        # Verify the full sequence: create → bind (do_put) → execute → close
        action_calls = mock_flight_client.do_action.call_args_list
        assert len(action_calls) == 2
        assert action_calls[0][0][0].type == "CreatePreparedStatement"
        assert action_calls[1][0][0].type == "ClosePreparedStatement"

        mock_flight_client.do_put.assert_called_once()
        mock_flight_client.get_flight_info.assert_called_once()
        mock_flight_client.do_get.assert_called_once()

    def test_execute_query_with_named_parameters(
        self, mock_flight_client: MagicMock, sample_table: pa.Table
    ) -> None:
        """Named dict params trigger the prepared statement flow."""
        client = FlightSQLClient("localhost", 47470, "tok", "db")
        result = client.execute_query("SELECT * FROM t WHERE id = $id", {"id": 42})

        assert result.num_rows == sample_table.num_rows

        action_calls = mock_flight_client.do_action.call_args_list
        assert len(action_calls) == 2
        assert action_calls[0][0][0].type == "CreatePreparedStatement"
        assert action_calls[1][0][0].type == "ClosePreparedStatement"

    def test_execute_update_with_parameters(
        self, mock_flight_client: MagicMock
    ) -> None:
        """execute_update with params uses prepared statement flow."""
        affected_table = pa.table({"affected_rows": [1]})
        mock_reader = MagicMock()
        mock_reader.read_all.return_value = affected_table
        mock_flight_client.do_get.return_value = mock_reader

        client = FlightSQLClient("localhost", 47470, "tok", "db")
        count = client.execute_update(
            "INSERT INTO t (id) VALUES ($id)", {"id": 99},
        )

        assert count == 1
        action_calls = mock_flight_client.do_action.call_args_list
        assert len(action_calls) == 2
        assert action_calls[0][0][0].type == "CreatePreparedStatement"
        assert action_calls[1][0][0].type == "ClosePreparedStatement"

    def test_close_called_on_error(
        self, mock_flight_client: MagicMock
    ) -> None:
        """ClosePreparedStatement is called even when GetFlightInfo fails."""
        mock_flight_client.get_flight_info.side_effect = RuntimeError("boom")

        client = FlightSQLClient("localhost", 47470, "tok", "db")
        with pytest.raises(QueryError, match="boom"):
            client.execute_query("SELECT $1", [1])

        # Close should still be called
        action_calls = mock_flight_client.do_action.call_args_list
        assert len(action_calls) == 2
        assert action_calls[1][0][0].type == "ClosePreparedStatement"


class TestBuildParamsBatch:
    def test_positional_params(self) -> None:
        batch = _build_params_batch([42, "hello"])
        assert batch.num_rows == 1
        assert batch.num_columns == 2
        assert batch.schema.names == ["0", "1"]
        assert batch.column(0)[0].as_py() == 42
        assert batch.column(1)[0].as_py() == "hello"

    def test_named_params(self) -> None:
        batch = _build_params_batch({"id": 42, "name": "alice"})
        assert batch.num_rows == 1
        assert batch.num_columns == 2
        assert batch.schema.names == ["id", "name"]
        assert batch.column(0)[0].as_py() == 42
        assert batch.column(1)[0].as_py() == "alice"


class TestProtobufHelpers:
    def test_parse_varint(self) -> None:
        # Single byte varint
        val, off = _parse_varint(b"\x05", 0)
        assert val == 5
        assert off == 1

        # Multi-byte varint: 300 = 0xAC 0x02
        val, off = _parse_varint(b"\xac\x02", 0)
        assert val == 300
        assert off == 2

    def test_parse_prepared_statement_result(self) -> None:
        from duckgresql._flight import _pb_string, _pb_bytes_field

        handle = b"my-handle-123"
        inner = _pb_bytes_field(1, handle)
        type_url = "type.googleapis.com/arrow.flight.protocol.sql.ActionCreatePreparedStatementResult"
        body = _pb_string(1, type_url) + _pb_bytes_field(2, inner)

        parsed = _parse_prepared_statement_result(body)
        assert parsed == handle
