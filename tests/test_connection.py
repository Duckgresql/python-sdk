"""Tests for Duckgresql (sync connection)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from duckgresql.connection import Duckgresql
from duckgresql.exceptions import ConnectionError
from duckgresql.result import DuckgresqlResult


@pytest.fixture()
def conn(mock_flight_client: MagicMock, monkeypatch: pytest.MonkeyPatch) -> Duckgresql:
    """Create a Duckgresql with mocked Flight + REST."""
    mock_http = MagicMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"connection_token": "conn_test"}
    mock_http.post.return_value = mock_resp
    monkeypatch.setattr("httpx.Client", lambda **kw: mock_http)

    connection = Duckgresql(
        "localhost",
        token="dkgql_test",
        database="testdb",
    )
    # Stash mock for assertions
    connection._mock_http = mock_http  # type: ignore[attr-defined]
    return connection


class TestDuckgresql:
    def test_execute_select(self, conn: Duckgresql) -> None:
        result = conn.execute("SELECT * FROM users")
        assert isinstance(result, DuckgresqlResult)
        assert result.rowcount == 3  # from sample_table fixture

    def test_execute_insert(
        self, conn: Duckgresql, mock_flight_client: MagicMock
    ) -> None:
        # Override do_get to return affected_rows
        affected_table = pa.table({"affected_rows": [2]})
        mock_reader = MagicMock()
        mock_reader.read_all.return_value = affected_table
        mock_flight_client.do_get.return_value = mock_reader

        result = conn.execute("INSERT INTO t VALUES (1, 'a')")
        assert result.rowcount == 2

    def test_sql_alias(self, conn: Duckgresql) -> None:
        result = conn.sql("SELECT 1")
        assert isinstance(result, DuckgresqlResult)

    def test_execute_with_positional_parameters(
        self, conn: Duckgresql, mock_flight_client: MagicMock
    ) -> None:
        """Positional params trigger the prepared statement flow."""
        conn.execute("SELECT * FROM t WHERE id = $1", [42])

        # Verify prepared statement flow was used (do_action called)
        action_calls = mock_flight_client.do_action.call_args_list
        assert len(action_calls) == 2
        assert action_calls[0][0][0].type == "CreatePreparedStatement"
        assert action_calls[1][0][0].type == "ClosePreparedStatement"
        mock_flight_client.do_put.assert_called_once()

    def test_execute_with_named_parameters(
        self, conn: Duckgresql, mock_flight_client: MagicMock
    ) -> None:
        """Named dict params trigger the prepared statement flow."""
        conn.execute("SELECT * FROM t WHERE id = $id", {"id": 42})

        action_calls = mock_flight_client.do_action.call_args_list
        assert len(action_calls) == 2
        assert action_calls[0][0][0].type == "CreatePreparedStatement"
        assert action_calls[1][0][0].type == "ClosePreparedStatement"
        mock_flight_client.do_put.assert_called_once()

    def test_executemany(
        self, conn: Duckgresql, mock_flight_client: MagicMock
    ) -> None:
        affected_table = pa.table({"affected_rows": [1]})
        mock_reader = MagicMock()
        mock_reader.read_all.return_value = affected_table
        mock_flight_client.do_get.return_value = mock_reader

        result = conn.executemany(
            "INSERT INTO t (id) VALUES ($1)",
            [[1], [2], [3]],
        )
        assert result.rowcount == 3
        # Each call uses the prepared statement flow (2 do_action calls each)
        assert mock_flight_client.do_action.call_count == 6  # 3 * (create + close)
        assert mock_flight_client.do_put.call_count == 3

    def test_executemany_with_named_parameters(
        self, conn: Duckgresql, mock_flight_client: MagicMock
    ) -> None:
        affected_table = pa.table({"affected_rows": [1]})
        mock_reader = MagicMock()
        mock_reader.read_all.return_value = affected_table
        mock_flight_client.do_get.return_value = mock_reader

        result = conn.executemany(
            "INSERT INTO t (id) VALUES ($id)",
            [{"id": 1}, {"id": 2}],
        )
        assert result.rowcount == 2
        assert mock_flight_client.do_action.call_count == 4  # 2 * (create + close)

    def test_execute_async_returns_job(self, conn: Duckgresql) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"job_id": "job_xyz", "status": "pending"}
        conn._mock_http.post.return_value = mock_resp  # type: ignore[attr-defined]

        job = conn.execute_async("SELECT * FROM large_table")
        assert job.job_id == "job_xyz"

    def test_execute_async_with_parameters(self, conn: Duckgresql) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"job_id": "job_abc", "status": "pending"}
        conn._mock_http.post.return_value = mock_resp  # type: ignore[attr-defined]

        job = conn.execute_async(
            "SELECT * FROM t WHERE id = $id", {"id": 99}
        )
        assert job.job_id == "job_abc"

        # Verify params were sent as bindings in the REST payload
        post_call = conn._mock_http.post.call_args  # type: ignore[attr-defined]
        payload = post_call.kwargs.get("json") or post_call[1].get("json")
        assert payload["query"] == "SELECT * FROM t WHERE id = $id"
        assert payload["bindings"] == {"id": 99}

    def test_close(self, conn: Duckgresql) -> None:
        assert not conn.closed
        conn.close()
        assert conn.closed

    def test_execute_after_close(self, conn: Duckgresql) -> None:
        conn.close()
        with pytest.raises(ConnectionError, match="closed"):
            conn.execute("SELECT 1")

    def test_context_manager(
        self, mock_flight_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"connection_token": "conn_test"}
        mock_http.post.return_value = mock_resp
        monkeypatch.setattr("httpx.Client", lambda **kw: mock_http)

        with Duckgresql("localhost", token="tok", database="db") as c:
            assert not c.closed
        assert c.closed
