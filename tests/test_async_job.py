"""Tests for AsyncJob and AsyncJobAsync."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from duckgresql._types import JobStatus
from duckgresql.async_job import AsyncJob, AsyncJobAsync, _rows_to_table
from duckgresql.exceptions import JobError, TimeoutError
from duckgresql.result import DuckgresqlResult


class TestRowsToTable:
    def test_basic_conversion(self) -> None:
        data = {
            "columns": ["id", "name"],
            "results": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
        }
        table = _rows_to_table(data)
        assert table.num_rows == 2
        assert table.column_names == ["id", "name"]

    def test_empty_results(self) -> None:
        data = {"columns": ["x"], "results": []}
        table = _rows_to_table(data)
        assert table.num_rows == 0

    def test_no_columns(self) -> None:
        data = {"columns": [], "results": []}
        table = _rows_to_table(data)
        assert table.num_rows == 0


class TestAsyncJob:
    def _make_job(self) -> tuple[AsyncJob, MagicMock]:
        mock_rest = MagicMock()
        job = AsyncJob(mock_rest, "conn_tok", "job_123")
        return job, mock_rest

    def test_job_id(self) -> None:
        job, _ = self._make_job()
        assert job.job_id == "job_123"

    def test_status(self) -> None:
        job, mock_rest = self._make_job()
        mock_rest.get_job.return_value = {"status": "running"}
        assert job.status() == JobStatus.RUNNING

    def test_is_done_completed(self) -> None:
        job, mock_rest = self._make_job()
        mock_rest.get_job.return_value = {"status": "completed"}
        assert job.is_done()

    def test_is_done_pending(self) -> None:
        job, mock_rest = self._make_job()
        mock_rest.get_job.return_value = {"status": "pending"}
        assert not job.is_done()

    def test_result_completed(self) -> None:
        job, mock_rest = self._make_job()
        mock_rest.get_job.return_value = {"status": "completed"}
        mock_rest.get_job_result.return_value = {
            "columns": ["x"],
            "results": [{"x": 42}],
        }
        result = job.result(timeout=5)
        assert isinstance(result, DuckgresqlResult)
        assert result.fetchone() == (42,)

    def test_result_failed(self) -> None:
        job, mock_rest = self._make_job()
        mock_rest.get_job.return_value = {"status": "failed", "error": "syntax error"}
        with pytest.raises(JobError, match="syntax error"):
            job.result(timeout=5)

    def test_result_cancelled(self) -> None:
        job, mock_rest = self._make_job()
        mock_rest.get_job.return_value = {"status": "cancelled"}
        with pytest.raises(JobError, match="cancelled"):
            job.result(timeout=5)

    @patch("duckgresql.async_job.time")
    def test_result_timeout(self, mock_time: MagicMock) -> None:
        job, mock_rest = self._make_job()
        mock_rest.get_job.return_value = {"status": "running"}
        # Simulate time passing beyond deadline
        mock_time.monotonic.side_effect = [0.0, 100.0]
        mock_time.sleep = MagicMock()

        with pytest.raises(TimeoutError, match="did not complete"):
            job.result(timeout=1)

    def test_cancel(self) -> None:
        job, mock_rest = self._make_job()
        job.cancel()
        mock_rest.cancel_job.assert_called_once_with("conn_tok", "job_123")


class TestAsyncJobAsync:
    @pytest.mark.asyncio
    async def test_status(self) -> None:
        mock_rest = MagicMock()
        mock_rest.get_job = AsyncMock(return_value={"status": "pending"})
        job = AsyncJobAsync(mock_rest, "conn_tok", "job_abc")
        assert await job.status() == JobStatus.PENDING

    @pytest.mark.asyncio
    async def test_is_done(self) -> None:
        mock_rest = MagicMock()
        mock_rest.get_job = AsyncMock(return_value={"status": "completed"})
        job = AsyncJobAsync(mock_rest, "conn_tok", "job_abc")
        assert await job.is_done()

    @pytest.mark.asyncio
    async def test_result(self) -> None:
        mock_rest = MagicMock()
        mock_rest.get_job = AsyncMock(return_value={"status": "completed"})
        mock_rest.get_job_result = AsyncMock(
            return_value={"columns": ["val"], "results": [{"val": 1}]}
        )
        job = AsyncJobAsync(mock_rest, "conn_tok", "job_abc")
        result = await job.result(timeout=5)
        assert result.fetchone() == (1,)

    @pytest.mark.asyncio
    async def test_cancel(self) -> None:
        mock_rest = MagicMock()
        mock_rest.cancel_job = AsyncMock()
        job = AsyncJobAsync(mock_rest, "conn_tok", "job_abc")
        await job.cancel()
        mock_rest.cancel_job.assert_called_once_with("conn_tok", "job_abc")
