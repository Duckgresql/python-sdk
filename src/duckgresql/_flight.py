"""Low-level Arrow Flight SQL client for DuckGresQL."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

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

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _call_options(self) -> flight.FlightCallOptions:
        """Build call options with the bearer token from the handshake."""
        return flight.FlightCallOptions(headers=[self._auth_header])

    def _execute_prepared(
        self, query: str, parameters: Sequence[Any] | dict[str, Any],
    ) -> pa.Table:
        """Execute a query using the prepared statement RPC sequence.

        1. CreatePreparedStatement → handle
        2. DoPut with parameter RecordBatch → bind
        3. GetFlightInfo with handle → endpoints
        4. DoGet → results
        5. ClosePreparedStatement (in finally)
        """
        opts = self._call_options()
        handle: bytes | None = None
        try:
            # 1. Create prepared statement
            action_body = _create_prepared_statement_request(query)
            action = flight.Action("CreatePreparedStatement", action_body)
            results = list(self._client.do_action(action, opts))
            if not results:
                raise QueryError("CreatePreparedStatement returned no result")
            handle = _parse_prepared_statement_result(results[0].body.to_pybytes())

            # 2. Bind parameters via DoPut
            batch = _build_params_batch(parameters)
            cmd = _prepared_statement_query(handle)
            descriptor = flight.FlightDescriptor.for_command(cmd)
            writer, _ = self._client.do_put(descriptor, batch.schema, opts)
            writer.write_batch(batch)
            writer.done_writing()
            writer.close()

            # 3. GetFlightInfo to get endpoints
            info = self._client.get_flight_info(descriptor, opts)
            if not info.endpoints:
                return pa.table({})

            # 4. DoGet to fetch results
            ticket = info.endpoints[0].ticket
            reader = self._client.do_get(ticket, opts)
            return reader.read_all()
        finally:
            # 5. Close prepared statement
            if handle is not None:
                try:
                    close_body = _close_prepared_statement_request(handle)
                    close_action = flight.Action("ClosePreparedStatement", close_body)
                    list(self._client.do_action(close_action, opts))
                except Exception:
                    pass  # best-effort cleanup

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def execute_query(self, query: str, parameters: Parameters = None) -> pa.Table:
        """Execute a read query and return the full result as a :class:`pyarrow.Table`."""
        try:
            if parameters is not None:
                return self._execute_prepared(query, parameters)

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

    def execute_update(self, query: str, parameters: Parameters = None) -> int:
        """Execute a DML statement and return the number of affected rows."""
        try:
            if parameters is not None:
                table = self._execute_prepared(query, parameters)
            else:
                opts = self._call_options()
                descriptor = flight.FlightDescriptor.for_command(
                    _flight_sql_command(query),
                )
                info = self._client.get_flight_info(descriptor, opts)

                if not info.endpoints:
                    return 0

                ticket = info.endpoints[0].ticket
                reader = self._client.do_get(ticket, opts)
                table = reader.read_all()

            # The server returns a single-row table with the affected count
            # when the statement is DML. If it returns regular rows, return
            # the row count instead.
            if table.num_columns == 1 and table.column_names[0] == "affected_rows":
                return int(table.column(0)[0].as_py())
            return cast(int, table.num_rows)
        except flight.FlightUnauthenticatedError as exc:
            raise AuthenticationError(str(exc)) from exc
        except QueryError:
            raise
        except Exception as exc:
            raise QueryError(f"Update execution failed: {exc}") from exc

    def close(self) -> None:
        """Close the underlying Flight client."""
        if not self._closed:
            self._client.close()
            self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed
