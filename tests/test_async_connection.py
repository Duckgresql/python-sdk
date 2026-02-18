"""Tests for DuckgresqlAsync."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pyarrow as pa
import pytest

from duckgresql.async_connection import DuckgresqlAsync
from duckgresql.exceptions import ConnectionError
from duckgresql.result import DuckgresqlResult


@pytest.fixture()
def async_conn(sample_table: pa.Table) -> DuckgresqlAsync:
    """Create a DuckgresqlAsync with mocked internals."""
    mock_flight = MagicMock()
    mock_flight.execute_query.return_value = sample_table
    mock_flight.execute_update.return_value = 1
    mock_flight.closed = False

    mock_rest = MagicMock()
    mock_rest.submit_async = AsyncMock(return_value="job_abc")
    mock_rest.close = AsyncMock()
    mock_rest.closed = False

    return DuckgresqlAsync(mock_flight, mock_rest, "conn_test")


class TestDuckgresqlAsync:
    @pytest.mark.asyncio
    async def test_execute_select(self, async_conn: DuckgresqlAsync) -> None:
        result = await async_conn.execute("SELECT 1")
        assert isinstance(result, DuckgresqlResult)
        assert result.rowcount == 3  # from sample_table

    @pytest.mark.asyncio
    async def test_execute_insert(self, async_conn: DuckgresqlAsync) -> None:
        result = await async_conn.execute("INSERT INTO t VALUES (1)")
        assert result.rowcount == 1

    @pytest.mark.asyncio
    async def test_sql_alias(self, async_conn: DuckgresqlAsync) -> None:
        result = await async_conn.sql("SELECT 1")
        assert isinstance(result, DuckgresqlResult)

    @pytest.mark.asyncio
    async def test_executemany(self, async_conn: DuckgresqlAsync) -> None:
        result = await async_conn.executemany(
            "INSERT INTO t (id) VALUES ($1)",
            [[1], [2]],
        )
        assert result.rowcount == 2

    @pytest.mark.asyncio
    async def test_execute_async_returns_job(self, async_conn: DuckgresqlAsync) -> None:
        job = await async_conn.execute_async("SELECT * FROM big")
        assert job.job_id == "job_abc"

    @pytest.mark.asyncio
    async def test_close(self, async_conn: DuckgresqlAsync) -> None:
        assert not async_conn.closed
        await async_conn.close()
        assert async_conn.closed

    @pytest.mark.asyncio
    async def test_execute_after_close(self, async_conn: DuckgresqlAsync) -> None:
        await async_conn.close()
        with pytest.raises(ConnectionError, match="closed"):
            await async_conn.execute("SELECT 1")

    @pytest.mark.asyncio
    async def test_context_manager(self, async_conn: DuckgresqlAsync) -> None:
        async with async_conn as c:
            assert not c.closed
        assert async_conn.closed
