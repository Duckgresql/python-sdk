"""Tests for RestClient and AsyncRestClient."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from duckgresql._rest import RestClient
from duckgresql.exceptions import AuthenticationError, JobError


class _FakeResponse:
    """Minimal httpx.Response stand-in."""

    def __init__(self, status_code: int, data: dict[str, Any]) -> None:
        self.status_code = status_code
        self._data = data
        self.text = str(data)

    def json(self) -> dict[str, Any]:
        return self._data


class TestRestClient:
    def test_connect_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_http = MagicMock()
        mock_http.post.return_value = _FakeResponse(200, {"connection_token": "conn_xyz"})
        monkeypatch.setattr("httpx.Client", lambda **kw: mock_http)

        client = RestClient("http://localhost:3100")
        token = client.connect("dkgql_tok", "mydb")
        assert token == "conn_xyz"

    def test_connect_auth_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_http = MagicMock()
        mock_http.post.return_value = _FakeResponse(
            401, {"error": {"code": "TOKEN_INVALID", "message": "bad token"}}
        )
        monkeypatch.setattr("httpx.Client", lambda **kw: mock_http)

        client = RestClient("http://localhost:3100")
        with pytest.raises(AuthenticationError, match="bad token"):
            client.connect("bad_tok", "mydb")

    def test_submit_async(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_http = MagicMock()
        mock_http.post.return_value = _FakeResponse(
            200, {"job_id": "job_123", "status": "pending"}
        )
        monkeypatch.setattr("httpx.Client", lambda **kw: mock_http)

        client = RestClient("http://localhost:3100")
        job_id = client.submit_async("conn_tok", "SELECT 1")
        assert job_id == "job_123"

    def test_get_job(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_http = MagicMock()
        mock_http.get.return_value = _FakeResponse(
            200, {"job_id": "job_123", "status": "completed"}
        )
        monkeypatch.setattr("httpx.Client", lambda **kw: mock_http)

        client = RestClient("http://localhost:3100")
        data = client.get_job("conn_tok", "job_123")
        assert data["status"] == "completed"

    def test_get_job_result(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_http = MagicMock()
        mock_http.get.return_value = _FakeResponse(
            200,
            {"columns": ["x"], "results": [{"x": 1}]},
        )
        monkeypatch.setattr("httpx.Client", lambda **kw: mock_http)

        client = RestClient("http://localhost:3100")
        data = client.get_job_result("conn_tok", "job_123")
        assert data["columns"] == ["x"]

    def test_cancel_job(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_http = MagicMock()
        mock_http.post.return_value = _FakeResponse(200, {"success": True})
        monkeypatch.setattr("httpx.Client", lambda **kw: mock_http)

        client = RestClient("http://localhost:3100")
        client.cancel_job("conn_tok", "job_123")  # should not raise

    def test_job_error_response(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_http = MagicMock()
        mock_http.get.return_value = _FakeResponse(
            400, {"error": {"code": "JOB_NOT_READY", "message": "not completed"}}
        )
        monkeypatch.setattr("httpx.Client", lambda **kw: mock_http)

        client = RestClient("http://localhost:3100")
        with pytest.raises(JobError, match="not completed"):
            client.get_job_result("conn_tok", "job_123")

    def test_auth_error_on_job(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_http = MagicMock()
        mock_http.get.return_value = _FakeResponse(401, {})
        monkeypatch.setattr("httpx.Client", lambda **kw: mock_http)

        client = RestClient("http://localhost:3100")
        with pytest.raises(AuthenticationError):
            client.get_job("conn_tok", "job_123")

    def test_close(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_http = MagicMock()
        monkeypatch.setattr("httpx.Client", lambda **kw: mock_http)

        client = RestClient("http://localhost:3100")
        assert not client.closed
        client.close()
        assert client.closed
        client.close()  # idempotent
