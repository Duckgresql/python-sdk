"""Low-level Arrow Flight SQL client for DuckGresQL."""

from __future__ import annotations

from typing import cast

import pyarrow as pa
import pyarrow.flight as flight

from duckgresql.exceptions import AuthenticationError, ConnectionError, QueryError

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
    # CommandStatementQuery { string query = 1; }
    cmd = _pb_string(1, query)
    # google.protobuf.Any { string type_url = 1; bytes value = 2; }
    return _pb_string(1, _COMMAND_TYPE_URL) + _pb_bytes_field(2, cmd)


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

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def execute_query(self, query: str) -> pa.Table:
        """Execute a read query and return the full result as a :class:`pyarrow.Table`."""
        try:
            descriptor = flight.FlightDescriptor.for_command(_flight_sql_command(query))
            opts = self._call_options()
            info = self._client.get_flight_info(descriptor, opts)

            # Flight SQL returns one or more endpoints; read the first.
            if not info.endpoints:
                return pa.table({})

            ticket = info.endpoints[0].ticket
            reader = self._client.do_get(ticket, opts)
            return reader.read_all()
        except flight.FlightUnauthenticatedError as exc:
            raise AuthenticationError(str(exc)) from exc
        except Exception as exc:
            raise QueryError(f"Query execution failed: {exc}") from exc

    def execute_update(self, query: str) -> int:
        """Execute a DML statement and return the number of affected rows."""
        try:
            opts = self._call_options()
            # For DML we use the same descriptor path; the server decides
            # based on the SQL whether to return rows or an update count.
            descriptor = flight.FlightDescriptor.for_command(_flight_sql_command(query))
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
