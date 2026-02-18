"""Asynchronous REST client for DuckGresQL async query endpoints."""

from __future__ import annotations

from typing import Any, cast

import httpx

from duckgresql.exceptions import AuthenticationError, ConnectionError, JobError


class AsyncRestClient:
    """Thin asynchronous wrapper around :mod:`httpx` for DuckGresQL REST API."""

    def __init__(self, base_url: str, *, timeout: float = 30.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._http = httpx.AsyncClient(base_url=self._base_url, timeout=timeout)
        self._closed = False

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    async def connect(self, token: str, database: str) -> str:
        """Exchange an API token + database for a connection token."""
        try:
            resp = await self._http.post(
                "/connect",
                json={"database": database},
                headers={"Authorization": f"Bearer {token}"},
            )
        except httpx.HTTPError as exc:
            raise ConnectionError(f"REST connect failed: {exc}") from exc

        if resp.status_code == 401:
            body = resp.json()
            msg = body.get("error", {}).get("message", "Authentication failed")
            raise AuthenticationError(msg)
        if resp.status_code >= 400:
            raise ConnectionError(f"REST connect returned {resp.status_code}: {resp.text}")

        return cast(str, resp.json()["connection_token"])

    # ------------------------------------------------------------------
    # Async job endpoints
    # ------------------------------------------------------------------

    def _headers(self, conn_token: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {conn_token}"}

    async def submit_async(
        self,
        conn_token: str,
        query: str,
        bindings: Any | None = None,
    ) -> str:
        """Submit an async query and return the ``job_id``."""
        payload: dict[str, Any] = {"query": query}
        if bindings is not None:
            payload["bindings"] = bindings

        resp = await self._http.post(
            "/query/async",
            json=payload,
            headers=self._headers(conn_token),
        )
        self._check_response(resp)
        return cast(str, resp.json()["job_id"])

    async def get_job(self, conn_token: str, job_id: str) -> dict[str, Any]:
        """Get status/metadata for a specific job."""
        resp = await self._http.get(
            f"/query/jobs/{job_id}",
            headers=self._headers(conn_token),
        )
        self._check_response(resp)
        return cast(dict[str, Any], resp.json())

    async def get_job_result(self, conn_token: str, job_id: str) -> dict[str, Any]:
        """Get the result rows for a completed job."""
        resp = await self._http.get(
            f"/query/jobs/{job_id}/result",
            headers=self._headers(conn_token),
        )
        self._check_response(resp)
        return cast(dict[str, Any], resp.json())

    async def cancel_job(self, conn_token: str, job_id: str) -> None:
        """Request cancellation of a pending/running job."""
        resp = await self._http.post(
            f"/query/jobs/{job_id}/cancel",
            headers=self._headers(conn_token),
        )
        self._check_response(resp)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _check_response(resp: httpx.Response) -> None:
        if resp.status_code == 401:
            raise AuthenticationError("Connection token invalid or expired")
        if resp.status_code >= 400:
            try:
                body = resp.json()
                msg = body.get("error", {}).get("message", resp.text)
            except Exception:
                msg = resp.text
            raise JobError(f"REST request failed ({resp.status_code}): {msg}")

    async def close(self) -> None:
        if not self._closed:
            await self._http.aclose()
            self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed
