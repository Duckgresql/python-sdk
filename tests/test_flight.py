"""Tests for FlightSQLClient."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

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


class TestDirectParamsPath:
    """Tests for the direct parameter path (x-params-json header, 2 RPCs)."""

    def test_direct_path_used_when_server_supports_it(
        self, mock_flight_client: MagicMock, sample_table: pa.Table
    ) -> None:
        """When get_flight_info succeeds with params header, no prepared statement RPCs."""
        client = FlightSQLClient("localhost", 47470, "tok", "db")
        result = client.execute_query("SELECT * FROM t WHERE id = $1", [42])

        assert result.num_rows == sample_table.num_rows
        # No prepared statement RPCs
        mock_flight_client.do_action.assert_not_called()
        mock_flight_client.do_put.assert_not_called()
        # Direct path: get_flight_info + do_get
        mock_flight_client.get_flight_info.assert_called_once()
        mock_flight_client.do_get.assert_called_once()

    def test_direct_path_sends_x_params_json_header(
        self, mock_flight_client: MagicMock, sample_table: pa.Table
    ) -> None:
        """Positional params are encoded as a plain JSON array."""
        import base64
        import json

        client = FlightSQLClient("localhost", 47470, "tok", "db")
        client.execute_query("SELECT $1", [42])

        call_opts = mock_flight_client.get_flight_info.call_args[1].get("options") or \
            mock_flight_client.get_flight_info.call_args[0][1]
        headers = {k: v for k, v in call_opts.headers}
        assert b"x-params-json" in headers
        decoded = json.loads(base64.b64decode(headers[b"x-params-json"]))
        assert decoded == [{"v": 42}]

    def test_direct_path_with_named_params(
        self, mock_flight_client: MagicMock, sample_table: pa.Table
    ) -> None:
        """Named params are encoded as an array of single-key objects."""
        import base64
        import json

        client = FlightSQLClient("localhost", 47470, "tok", "db")
        client.execute_query("SELECT $name", {"name": "alice"})

        call_opts = mock_flight_client.get_flight_info.call_args[0][1]
        headers = {k: v for k, v in call_opts.headers}
        decoded = json.loads(base64.b64decode(headers[b"x-params-json"]))
        assert decoded == [{"n": True, "k": "name", "v": "alice"}]

    def test_fallback_to_prepared_on_unimplemented(
        self, mock_flight_client: MagicMock, sample_table: pa.Table
    ) -> None:
        """Server returning Unimplemented triggers fallback to prepared statements."""
        original_return = mock_flight_client.get_flight_info.return_value
        call_count = 0

        def unimplemented_then_ok(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            # First call (direct probe) fails with Unimplemented
            if call_count == 1:
                raise RuntimeError("Unimplemented: direct params not supported")
            return original_return

        mock_flight_client.get_flight_info.side_effect = unimplemented_then_ok

        client = FlightSQLClient("localhost", 47470, "tok", "db")
        result = client.execute_query("SELECT $1", [42])

        assert result.num_rows == sample_table.num_rows
        assert client._direct_params_supported is False
        # Fell back to prepared statement path
        action_calls = mock_flight_client.do_action.call_args_list
        assert action_calls[0][0][0].type == "CreatePreparedStatement"

    def test_direct_support_remembered(
        self, mock_flight_client: MagicMock, sample_table: pa.Table
    ) -> None:
        """After successful probe, direct path is used without re-probing."""
        client = FlightSQLClient("localhost", 47470, "tok", "db")
        client.execute_query("SELECT $1", [1])
        client.execute_query("SELECT $1", [2])

        assert client._direct_params_supported is True
        mock_flight_client.do_action.assert_not_called()
        assert mock_flight_client.get_flight_info.call_count == 2

    def test_fallback_remembered(
        self, mock_flight_client: MagicMock, sample_table: pa.Table
    ) -> None:
        """After Unimplemented fallback, prepared path is used without re-probing."""
        original_return = mock_flight_client.get_flight_info.return_value

        def unimplemented_once(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("Unimplemented")
            return original_return

        call_count = 0
        mock_flight_client.get_flight_info.side_effect = unimplemented_once

        client = FlightSQLClient("localhost", 47470, "tok", "db")
        client.execute_query("SELECT $1", [1])  # probe fails, falls back
        assert client._direct_params_supported is False

        # Second call goes straight to prepared path (no re-probe)
        mock_flight_client.get_flight_info.side_effect = None
        mock_flight_client.get_flight_info.return_value = original_return
        client.execute_query("SELECT $1", [2])

        # Two Creates total (one per call, since different params don't matter —
        # same query uses cached handle for second call)
        create_calls = [c for c in mock_flight_client.do_action.call_args_list
                        if c[0][0].type == "CreatePreparedStatement"]
        assert len(create_calls) == 1  # handle reused


class TestPreparedStatementFlow:
    """Tests for the prepared statement path (used when direct params not supported)."""

    @pytest.fixture(autouse=True)
    def _force_prepared_path(self, mock_flight_client: MagicMock) -> None:
        """Force all clients in this class to use the prepared statement path."""
        self._mock_flight_client = mock_flight_client

    def _make_client(self) -> FlightSQLClient:
        client = FlightSQLClient("localhost", 47470, "tok", "db")
        client._direct_params_supported = False
        return client

    def test_execute_query_with_positional_parameters(
        self, sample_table: pa.Table
    ) -> None:
        client = self._make_client()
        result = client.execute_query("SELECT * FROM t WHERE id = $1", [42])

        assert result.num_rows == sample_table.num_rows

        action_calls = self._mock_flight_client.do_action.call_args_list
        assert len(action_calls) == 1
        assert action_calls[0][0][0].type == "CreatePreparedStatement"

        self._mock_flight_client.do_put.assert_called_once()
        self._mock_flight_client.get_flight_info.assert_called_once()
        self._mock_flight_client.do_get.assert_called_once()

    def test_execute_query_with_named_parameters(
        self, sample_table: pa.Table
    ) -> None:
        client = self._make_client()
        result = client.execute_query("SELECT * FROM t WHERE id = $id", {"id": 42})

        assert result.num_rows == sample_table.num_rows

        action_calls = self._mock_flight_client.do_action.call_args_list
        assert len(action_calls) == 1
        assert action_calls[0][0][0].type == "CreatePreparedStatement"

    def test_cached_handle_reused(
        self, sample_table: pa.Table
    ) -> None:
        client = self._make_client()
        client.execute_query("SELECT * FROM t WHERE id = $1", [42])
        client.execute_query("SELECT * FROM t WHERE id = $1", [99])

        action_calls = self._mock_flight_client.do_action.call_args_list
        assert len(action_calls) == 1
        assert self._mock_flight_client.do_put.call_count == 2

    def test_different_queries_get_separate_handles(
        self, sample_table: pa.Table
    ) -> None:
        client = self._make_client()
        client.execute_query("SELECT * FROM t WHERE id = $1", [1])
        client.execute_query("SELECT * FROM t WHERE name = $1", ["alice"])

        action_calls = self._mock_flight_client.do_action.call_args_list
        assert len(action_calls) == 2

    def test_handle_evicted_and_retried_on_error(
        self, sample_table: pa.Table
    ) -> None:
        client = self._make_client()

        client.execute_query("SELECT $1", [1])
        assert self._mock_flight_client.do_action.call_count == 1

        original_return = self._mock_flight_client.get_flight_info.return_value
        call_count = 0

        def fail_once(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("handle expired")
            return original_return

        self._mock_flight_client.get_flight_info.side_effect = fail_once

        result = client.execute_query("SELECT $1", [2])
        assert result.num_rows == sample_table.num_rows
        assert self._mock_flight_client.do_action.call_count == 2

    def test_close_cleans_up_cached_handles(
        self, sample_table: pa.Table
    ) -> None:
        client = self._make_client()
        client.execute_query("SELECT $1", [1])
        client.execute_query("SELECT $name", {"name": "x"})

        assert self._mock_flight_client.do_action.call_count == 2
        client.close()

        time.sleep(0.05)
        action_calls = self._mock_flight_client.do_action.call_args_list
        close_calls = [c for c in action_calls if c[0][0].type == "ClosePreparedStatement"]
        assert len(close_calls) == 2


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
        from duckgresql._flight import _pb_bytes_field, _pb_string

        handle = b"my-handle-123"
        inner = _pb_bytes_field(1, handle)
        type_url = (
            "type.googleapis.com/arrow.flight.protocol.sql"
            ".ActionCreatePreparedStatementResult"
        )
        body = _pb_string(1, type_url) + _pb_bytes_field(2, inner)

        parsed = _parse_prepared_statement_result(body)
        assert parsed == handle
