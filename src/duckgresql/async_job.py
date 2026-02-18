"""Async job wrappers for DuckGresQL REST-based query jobs."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pyarrow as pa

from duckgresql._rest import RestClient
from duckgresql._rest_async import AsyncRestClient
from duckgresql._types import JobStatus
from duckgresql.exceptions import JobError, TimeoutError
from duckgresql.result import DuckgresqlResult


def _rows_to_table(data: dict[str, Any]) -> pa.Table:
    """Convert the REST ``{"columns": [...], "results": [...]}`` response to
    a :class:`pyarrow.Table`.
    """
    columns: list[str] = data.get("columns", [])
    results: list[dict[str, Any]] = data.get("results", [])

    if not columns or not results:
        return pa.table({})

    arrays: dict[str, list[Any]] = {col: [] for col in columns}
    for row in results:
        for col in columns:
            arrays[col].append(row.get(col))

    return pa.table(arrays)


class AsyncJob:
    """Synchronous handle for an async query job submitted via REST."""

    def __init__(self, rest: RestClient, conn_token: str, job_id: str) -> None:
        self._rest = rest
        self._conn_token = conn_token
        self._job_id = job_id

    @property
    def job_id(self) -> str:
        return self._job_id

    def status(self) -> JobStatus:
        """Poll the server for the current job status."""
        data = self._rest.get_job(self._conn_token, self._job_id)
        return JobStatus(data["status"])

    def is_done(self) -> bool:
        """Return ``True`` if the job has reached a terminal state."""
        st = self.status()
        return st in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)

    def result(
        self,
        timeout: float = 300.0,
        poll_interval: float = 0.5,
    ) -> DuckgresqlResult:
        """Block until the job completes and return the result.

        Uses exponential back-off starting from *poll_interval* up to 5 s.
        """
        deadline = time.monotonic() + timeout
        interval = poll_interval

        while True:
            data = self._rest.get_job(self._conn_token, self._job_id)
            st = JobStatus(data["status"])

            if st == JobStatus.COMPLETED:
                result_data = self._rest.get_job_result(self._conn_token, self._job_id)
                table = _rows_to_table(result_data)
                return DuckgresqlResult(table)

            if st == JobStatus.FAILED:
                raise JobError(f"Job {self._job_id} failed: {data.get('error', 'unknown')}")

            if st == JobStatus.CANCELLED:
                raise JobError(f"Job {self._job_id} was cancelled")

            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Job {self._job_id} did not complete within {timeout}s"
                )

            time.sleep(interval)
            interval = min(interval * 1.5, 5.0)

    def cancel(self) -> None:
        """Request cancellation of the job."""
        self._rest.cancel_job(self._conn_token, self._job_id)


class AsyncJobAsync:
    """Async handle for an async query job submitted via REST."""

    def __init__(self, rest: AsyncRestClient, conn_token: str, job_id: str) -> None:
        self._rest = rest
        self._conn_token = conn_token
        self._job_id = job_id

    @property
    def job_id(self) -> str:
        return self._job_id

    async def status(self) -> JobStatus:
        """Poll the server for the current job status."""
        data = await self._rest.get_job(self._conn_token, self._job_id)
        return JobStatus(data["status"])

    async def is_done(self) -> bool:
        """Return ``True`` if the job has reached a terminal state."""
        st = await self.status()
        return st in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)

    async def result(
        self,
        timeout: float = 300.0,
        poll_interval: float = 0.5,
    ) -> DuckgresqlResult:
        """Await until the job completes and return the result."""
        deadline = asyncio.get_event_loop().time() + timeout
        interval = poll_interval

        while True:
            data = await self._rest.get_job(self._conn_token, self._job_id)
            st = JobStatus(data["status"])

            if st == JobStatus.COMPLETED:
                result_data = await self._rest.get_job_result(self._conn_token, self._job_id)
                table = _rows_to_table(result_data)
                return DuckgresqlResult(table)

            if st == JobStatus.FAILED:
                raise JobError(f"Job {self._job_id} failed: {data.get('error', 'unknown')}")

            if st == JobStatus.CANCELLED:
                raise JobError(f"Job {self._job_id} was cancelled")

            if asyncio.get_event_loop().time() >= deadline:
                raise TimeoutError(
                    f"Job {self._job_id} did not complete within {timeout}s"
                )

            await asyncio.sleep(interval)
            interval = min(interval * 1.5, 5.0)

    async def cancel(self) -> None:
        """Request cancellation of the job."""
        await self._rest.cancel_job(self._conn_token, self._job_id)
