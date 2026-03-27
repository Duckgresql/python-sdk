"""Low-level Arrow Flight SQL client for DuckGresQL."""

from __future__ import annotations

import base64
import json
import threading
import time
from collections.abc import Mapping, Sequence
from typing import Any

import pyarrow as pa
import pyarrow.flight as flight

from duckgresql.exceptions import AuthenticationError, ConnectionError, QueryError

#: Accepted parameter types: positional sequence or named dict.
Parameters = Sequence[Any] | dict[str, Any] | None

# ---------------------------------------------------------------------------
# Minimal protobuf encoder for Flight SQL command descriptors
# ---------------------------------------------------------------------------
# The Arrow Flight SQL protocol requires GetFlightInfo/DoGet to receive a
# FlightDescriptor whose `cmd` bytes are a serialised google.protobuf.Any
# wrapping the appropriate command message (e.g. CommandStatementQuery).
# Passing raw SQL bytes causes "proto: cannot parse invalid wire-format data".

_COMMAND_TYPE_URL = (
    "type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"
)
_CREATE_PREPARED_TYPE_URL = (
    "type.googleapis.com/arrow.flight.protocol.sql.ActionCreatePreparedStatementRequest"
)
_PREPARED_QUERY_TYPE_URL = (
    "type.googleapis.com/arrow.flight.protocol.sql.CommandPreparedStatementQuery"
)
_CLOSE_PREPARED_TYPE_URL = (
    "type.googleapis.com/arrow.flight.protocol.sql.ActionClosePreparedStatementRequest"
)


def _varint(n: int) -> bytes:
    result = bytearray()
    while n > 0x7F:
        result.append((n & 0x7F) | 0x80)
        n >>= 7
    result.append(n)
    return bytes(result)


def _pb_string(field: int, value: str) -> bytes:
    data = value.encode("utf-8")
    return _varint((field << 3) | 2) + _varint(len(data)) + data


def _pb_bytes_field(field: int, value: bytes) -> bytes:
    return _varint((field << 3) | 2) + _varint(len(value)) + value


def _flight_sql_command(query: str) -> bytes:
    """Return bytes for google.protobuf.Any(CommandStatementQuery{query})."""
    cmd = _pb_string(1, query)
    return _pb_string(1, _COMMAND_TYPE_URL) + _pb_bytes_field(2, cmd)


# ---------------------------------------------------------------------------
# Prepared statement protobuf helpers
# ---------------------------------------------------------------------------

def _create_prepared_statement_request(query: str) -> bytes:
    """Encode ActionCreatePreparedStatementRequest wrapped in google.protobuf.Any."""
    inner = _pb_string(1, query)
    return _pb_string(1, _CREATE_PREPARED_TYPE_URL) + _pb_bytes_field(2, inner)


def _prepared_statement_query(handle: bytes) -> bytes:
    """Encode CommandPreparedStatementQuery wrapped in google.protobuf.Any."""
    inner = _pb_bytes_field(1, handle)
    return _pb_string(1, _PREPARED_QUERY_TYPE_URL) + _pb_bytes_field(2, inner)


def _close_prepared_statement_request(handle: bytes) -> bytes:
    """Encode ActionClosePreparedStatementRequest wrapped in google.protobuf.Any."""
    inner = _pb_bytes_field(1, handle)
    return _pb_string(1, _CLOSE_PREPARED_TYPE_URL) + _pb_bytes_field(2, inner)


def _parse_varint(data: bytes, offset: int) -> tuple[int, int]:
    """Read a varint from *data* starting at *offset*. Returns (value, new_offset)."""
    result = 0
    shift = 0
    while offset < len(data):
        b = data[offset]
        offset += 1
        result |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            return result, offset
        shift += 7
    raise ValueError("truncated varint")


def _parse_prepared_statement_result(body: bytes) -> bytes:
    """Parse ActionCreatePreparedStatementResult and extract prepared_statement_handle.

    The result is a google.protobuf.Any wrapping the result message.
    We look for field 1 (bytes) = prepared_statement_handle inside the inner value.
    """
    # First, unwrap the google.protobuf.Any envelope to get the inner value.
    # Any { string type_url = 1; bytes value = 2; }
    inner_value = body
    offset = 0
    while offset < len(body):
        tag, offset = _parse_varint(body, offset)
        field_number = tag >> 3
        wire_type = tag & 0x07
        if wire_type == 2:  # length-delimited
            length, offset = _parse_varint(body, offset)
            field_data = body[offset:offset + length]
            offset += length
            if field_number == 2:
                inner_value = field_data
                break
        elif wire_type == 0:  # varint
            _, offset = _parse_varint(body, offset)
        elif wire_type == 5:  # 32-bit
            offset += 4
        elif wire_type == 1:  # 64-bit
            offset += 8
        else:
            raise ValueError(f"unsupported wire type {wire_type}")

    # Now parse the inner message: ActionCreatePreparedStatementResult
    # { bytes prepared_statement_handle = 1; ... }
    offset = 0
    while offset < len(inner_value):
        tag, offset = _parse_varint(inner_value, offset)
        field_number = tag >> 3
        wire_type = tag & 0x07
        if wire_type == 2:
            length, offset = _parse_varint(inner_value, offset)
            field_data = inner_value[offset:offset + length]
            offset += length
            if field_number == 1:
                return field_data
        elif wire_type == 0:
            _, offset = _parse_varint(inner_value, offset)
        elif wire_type == 5:
            offset += 4
        elif wire_type == 1:
            offset += 8
        else:
            raise ValueError(f"unsupported wire type {wire_type}")

    raise ValueError("prepared_statement_handle not found in result")


# ---------------------------------------------------------------------------
# Parameter batch builder
# ---------------------------------------------------------------------------

def _build_params_batch(parameters: Sequence[Any] | dict[str, Any]) -> pa.RecordBatch:
    """Build a single-row RecordBatch from positional or named parameters."""
    if isinstance(parameters, Mapping):
        arrays = [pa.array([v]) for v in parameters.values()]
        names = list(parameters.keys())
    else:
        arrays = [pa.array([v]) for v in parameters]
        names = [str(i) for i in range(len(parameters))]
    return pa.record_batch(arrays, names=names)


class FlightSQLClient:
    """Thin wrapper around :class:`pyarrow.flight.FlightClient` for Flight SQL.

    Authentication uses BasicAuth where *username* is the API token and
    *password* is the database name.  The server returns a ``conn_`` bearer
    token that is sent on every subsequent RPC.
    """

    def __init__(
        self,
        host: str,
        port: int,
        token: str,
        database: str,
        *,
        use_tls: bool = False,
    ) -> None:
        scheme = "grpc+tls" if use_tls else "grpc"
        location = f"{scheme}://{host}:{port}"
        try:
            self._client = flight.FlightClient(location)
        except Exception as exc:
            raise ConnectionError(f"Failed to connect to Flight SQL at {location}: {exc}") from exc

        # BasicAuth handshake — username=token, password=database
        try:
            header_pair = self._client.authenticate_basic_token(token, database)
            self._auth_header: tuple[bytes, bytes] = header_pair
        except flight.FlightUnauthenticatedError as exc:
            raise AuthenticationError(f"Flight SQL authentication failed: {exc}") from exc
        except Exception as exc:
            raise ConnectionError(f"Flight SQL handshake failed: {exc}") from exc

        self._closed = False

        # Whether the server supports direct params via x-params-json header.
        # None = not yet probed, True/False = probed result.
        self._direct_params_supported: bool | None = None

        # Cache: query → (handle_bytes, created_at)
        # Server TTL is 5 min; we use 4 min to stay safe.
        self._handle_cache: dict[str, tuple[bytes, float]] = {}
        self._cache_ttl = 240.0  # 4 minutes
        self._cache_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _close_prepared_async(
        self, handle: bytes, opts: flight.FlightCallOptions,
    ) -> None:
        """Close a prepared statement in a background thread (fire-and-forget)."""
        def _close() -> None:
            try:
                close_body = _close_prepared_statement_request(handle)
                close_action = flight.Action("ClosePreparedStatement", close_body)
                list(self._client.do_action(close_action, opts))
            except Exception:
                pass  # best-effort cleanup

        threading.Thread(target=_close, daemon=True).start()

    def _get_or_create_handle(
        self, query: str, opts: flight.FlightCallOptions,
    ) -> tuple[bytes, bool]:
        """Return (handle, from_cache). Creates a new handle if not cached or expired."""
        now = time.monotonic()
        with self._cache_lock:
            cached = self._handle_cache.get(query)
            if cached is not None:
                handle, created_at = cached
                if now - created_at < self._cache_ttl:
                    return handle, True
                # Expired — remove and fall through
                del self._handle_cache[query]

        # Create new prepared statement (outside lock to avoid blocking)
        action_body = _create_prepared_statement_request(query)
        action = flight.Action("CreatePreparedStatement", action_body)
        results = list(self._client.do_action(action, opts))
        if not results:
            raise QueryError("CreatePreparedStatement returned no result")
        handle = _parse_prepared_statement_result(results[0].body.to_pybytes())

        with self._cache_lock:
            self._handle_cache[query] = (handle, time.monotonic())
        return handle, False

    def _evict_handle(self, query: str) -> None:
        """Remove a handle from the cache (e.g. after a server error)."""
        with self._cache_lock:
            self._handle_cache.pop(query, None)

    def _call_options(
        self, extra_headers: list[tuple[bytes, bytes]] | None = None,
    ) -> flight.FlightCallOptions:
        """Build call options with the bearer token from the handshake."""
        headers: list[tuple[bytes, bytes]] = [self._auth_header]
        if extra_headers:
            headers.extend(extra_headers)
        return flight.FlightCallOptions(headers=headers)

    @staticmethod
    def _encode_params_header(
        parameters: Sequence[Any] | dict[str, Any],
    ) -> tuple[bytes, bytes]:
        """Encode parameters as a base64 JSON gRPC metadata header.

        Each parameter is encoded as a ``ParamMsg`` object:
        - Positional: ``[{"v": 1234}, {"v": "hello"}]``
        - Named:      ``[{"n": true, "k": "id", "v": 1234}]``
        """
        if isinstance(parameters, Mapping):
            params_array = [{"n": True, "k": k, "v": v} for k, v in parameters.items()]
        else:
            params_array = [{"v": v} for v in parameters]
        encoded = base64.b64encode(json.dumps(params_array).encode()).decode()
        return (b"x-params-json", encoded.encode())

    def _execute_direct(
        self, query: str, parameters: Sequence[Any] | dict[str, Any],
    ) -> pa.Table:
        """Execute a parameterized query via the direct 2-RPC path.

        Sends parameters as a base64-encoded JSON header (x-params-json)
        alongside a standard CommandStatementQuery. 2 RPCs: GetFlightInfo → DoGet.
        """
        params_header = self._encode_params_header(parameters)
        opts = self._call_options(extra_headers=[params_header])
        descriptor = flight.FlightDescriptor.for_command(
            _flight_sql_command(query),
        )
        info = self._client.get_flight_info(descriptor, opts)
        if not info.endpoints:
            return pa.table({})

        ticket = info.endpoints[0].ticket
        reader = self._client.do_get(ticket, opts)
        return reader.read_all()

    def _execute_prepared(
        self, query: str, parameters: Sequence[Any] | dict[str, Any],
        *, _retry: bool = True,
    ) -> pa.Table:
        """Execute a query using a cached or new prepared statement handle.

        On cache hit: DoPut → GetFlightInfo → DoGet (3 RPCs)
        On cache miss: CreatePreparedStatement → DoPut → GetFlightInfo → DoGet (4 RPCs)
        """
        opts = self._call_options()
        handle, from_cache = self._get_or_create_handle(query, opts)

        try:
            # Bind parameters via DoPut
            batch = _build_params_batch(parameters)
            cmd = _prepared_statement_query(handle)
            descriptor = flight.FlightDescriptor.for_command(cmd)
            writer, _ = self._client.do_put(descriptor, batch.schema, opts)
            writer.write_batch(batch)
            writer.done_writing()
            writer.close()

            # GetFlightInfo to get endpoints
            info = self._client.get_flight_info(descriptor, opts)
            if not info.endpoints:
                return pa.table({})

            # DoGet to fetch results
            ticket = info.endpoints[0].ticket
            reader = self._client.do_get(ticket, opts)
            return reader.read_all()
        except Exception:
            # Handle may have expired server-side; evict and retry once if cached
            self._evict_handle(query)
            if from_cache and _retry:
                return self._execute_prepared(query, parameters, _retry=False)
            raise

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def execute_query(self, query: str, parameters: Parameters = None) -> pa.Table:
        """Execute a query and return the full result as a :class:`pyarrow.Table`.

        When *parameters* are provided, the client first attempts the direct
        path (params sent via ``x-params-json`` gRPC header — 2 RPCs).  If the
        server returns ``Unimplemented``, it transparently falls back to the
        prepared-statement path and remembers the result for future calls.
        """
        try:
            if parameters is not None:
                return self._execute_with_params(query, parameters)

            descriptor = flight.FlightDescriptor.for_command(
                _flight_sql_command(query),
            )
            opts = self._call_options()
            info = self._client.get_flight_info(descriptor, opts)
            if not info.endpoints:
                return pa.table({})

            ticket = info.endpoints[0].ticket
            reader = self._client.do_get(ticket, opts)
            return reader.read_all()
        except flight.FlightUnauthenticatedError as exc:
            raise AuthenticationError(str(exc)) from exc
        except QueryError:
            raise
        except Exception as exc:
            raise QueryError(f"Query execution failed: {exc}") from exc

    def _execute_with_params(
        self, query: str, parameters: Sequence[Any] | dict[str, Any],
    ) -> pa.Table:
        """Route parameterized queries to the direct or prepared-statement path."""
        # Already know the server doesn't support direct params
        if self._direct_params_supported is False:
            return self._execute_prepared(query, parameters)

        # Already confirmed direct params work
        if self._direct_params_supported is True:
            return self._execute_direct(query, parameters)

        # First parameterized call — probe the server
        try:
            result = self._execute_direct(query, parameters)
            self._direct_params_supported = True
            return result
        except flight.FlightUnavailableError:
            raise
        except flight.FlightUnauthenticatedError:
            raise
        except Exception as exc:
            if "Unimplemented" in str(exc):
                self._direct_params_supported = False
                return self._execute_prepared(query, parameters)
            raise

    def close(self) -> None:
        """Close cached prepared statements and the underlying Flight client."""
        if not self._closed:
            opts = self._call_options()
            with self._cache_lock:
                for handle, _ in self._handle_cache.values():
                    self._close_prepared_async(handle, opts)
                self._handle_cache.clear()
            self._client.close()
            self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed
