"""Query result wrapper for the DuckGresQL Python SDK."""

from __future__ import annotations

from typing import Any, cast

import pyarrow as pa


class DuckgresqlResult:
    """Wraps query results, providing DuckDB-style fetch methods.

    Internally stores a :class:`pyarrow.Table`.  For DML statements that
    only return an affected-row count, set *affected_rows* and leave *table*
    as ``None``.
    """

    def __init__(
        self,
        table: pa.Table | None = None,
        *,
        affected_rows: int | None = None,
    ) -> None:
        self._table: pa.Table = table if table is not None else pa.table({})
        self._affected_rows = affected_rows
        self._cursor: int = 0

    # ------------------------------------------------------------------
    # DB-API 2.0-style properties
    # ------------------------------------------------------------------

    @property
    def description(self) -> list[tuple[str, str, None, None, None, None, None]] | None:
        """Column metadata in DB-API 2.0 ``description`` format.

        Each entry is ``(name, type_code, None, None, None, None, None)``.
        Returns ``None`` for DML results with no columns.
        """
        if self._table.num_columns == 0:
            return None
        return [
            (field.name, str(field.type), None, None, None, None, None)
            for field in self._table.schema
        ]

    @property
    def rowcount(self) -> int:
        """Number of rows in the result, or affected-row count for DML."""
        if self._affected_rows is not None:
            return self._affected_rows
        return cast(int, self._table.num_rows)

    @property
    def columns(self) -> list[str]:
        """Column names."""
        return cast(list[str], self._table.column_names)

    # ------------------------------------------------------------------
    # Fetch methods
    # ------------------------------------------------------------------

    def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row, or ``None`` if exhausted."""
        if self._cursor >= self._table.num_rows:
            return None
        row = tuple(
            self._table.column(i)[self._cursor].as_py()
            for i in range(self._table.num_columns)
        )
        self._cursor += 1
        return row

    def fetchmany(self, size: int = 1) -> list[tuple[Any, ...]]:
        """Fetch up to *size* rows."""
        rows: list[tuple[Any, ...]] = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows."""
        rows: list[tuple[Any, ...]] = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    def fetchdf(self) -> Any:
        """Return a :class:`pandas.DataFrame`.

        Raises :class:`ImportError` if *pandas* is not installed.
        """
        try:
            import pandas  # noqa: F401
        except ImportError:
            raise ImportError(
                "pandas is required for fetchdf(). "
                "Install it with: pip install duckgresql[pandas]"
            ) from None
        return cast(Any, self._table.to_pandas())

    def fetchnumpy(self) -> dict[str, Any]:
        """Return a dict mapping column names to NumPy arrays.

        Raises :class:`ImportError` if *numpy* is not installed.
        """
        try:
            import numpy  # noqa: F401
        except ImportError:
            raise ImportError(
                "numpy is required for fetchnumpy(). "
                "Install it with: pip install duckgresql[numpy]"
            ) from None
        return {name: self._table.column(name).to_numpy() for name in self._table.column_names}

    def fetch_arrow_table(self) -> pa.Table:
        """Return the underlying :class:`pyarrow.Table` (zero-copy)."""
        return self._table

    def __repr__(self) -> str:
        if self._affected_rows is not None:
            return f"<DuckgresqlResult affected_rows={self._affected_rows}>"
        return f"<DuckgresqlResult columns={self.columns} rows={self._table.num_rows}>"
