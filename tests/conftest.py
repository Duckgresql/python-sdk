"""Shared test fixtures for the DuckGresQL Python SDK."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa
import pytest


@pytest.fixture()
def sample_table() -> pa.Table:
    """A small Arrow table for testing result methods."""
    return pa.table({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "score": [95.5, 87.0, 72.3],
    })


def _make_prepared_handle_result(handle: bytes = b"test-handle") -> bytes:
    """Build a fake ActionCreatePreparedStatementResult wrapped in Any."""
    from duckgresql._flight import _varint, _pb_string, _pb_bytes_field

    # ActionCreatePreparedStatementResult { bytes prepared_statement_handle = 1; }
    inner = _pb_bytes_field(1, handle)
    type_url = "type.googleapis.com/arrow.flight.protocol.sql.ActionCreatePreparedStatementResult"
    return _pb_string(1, type_url) + _pb_bytes_field(2, inner)


@pytest.fixture()
def mock_flight_client(monkeypatch: pytest.MonkeyPatch, sample_table: pa.Table) -> MagicMock:
    """Monkeypatch pyarrow.flight.FlightClient to avoid real gRPC connections."""
    mock_client = MagicMock()
    mock_client.authenticate_basic_token.return_value = (b"authorization", b"Bearer conn_abc123")

    # get_flight_info returns an info object with one endpoint
    mock_endpoint = MagicMock()
    mock_endpoint.ticket = MagicMock()
    mock_info = MagicMock()
    mock_info.endpoints = [mock_endpoint]
    mock_client.get_flight_info.return_value = mock_info

    # do_get returns a reader that yields the sample table
    mock_reader = MagicMock()
    mock_reader.read_all.return_value = sample_table
    mock_client.do_get.return_value = mock_reader

    # do_action returns a prepared statement handle result
    mock_action_result = MagicMock()
    mock_action_result.body.to_pybytes.return_value = _make_prepared_handle_result()
    mock_client.do_action.return_value = [mock_action_result]

    # do_put returns (writer, metadata_reader)
    mock_writer = MagicMock()
    mock_client.do_put.return_value = (mock_writer, MagicMock())

    monkeypatch.setattr("pyarrow.flight.FlightClient", lambda *a, **kw: mock_client)
    return mock_client


@pytest.fixture()
def mock_rest_responses(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Monkeypatch httpx.Client to return canned REST responses."""
    responses: dict[str, Any] = {
        "connect": {"connection_token": "conn_test123"},
        "submit_async": {"job_id": "job_abc", "status": "pending"},
        "get_job_pending": {"job_id": "job_abc", "status": "pending", "query": "SELECT 1"},
        "get_job_completed": {
            "job_id": "job_abc",
            "status": "completed",
            "query": "SELECT 1",
            "row_count": 1,
        },
        "get_job_failed": {
            "job_id": "job_abc",
            "status": "failed",
            "error": "syntax error",
        },
        "get_job_result": {
            "columns": ["id", "value"],
            "results": [{"id": 1, "value": "hello"}],
        },
    }
    return responses
