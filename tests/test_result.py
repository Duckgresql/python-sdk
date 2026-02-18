"""Tests for DuckgresqlResult."""

from __future__ import annotations

import pyarrow as pa
import pytest

from duckgresql.result import DuckgresqlResult


class TestDuckgresqlResult:
    def test_fetchone(self, sample_table: pa.Table) -> None:
        result = DuckgresqlResult(sample_table)
        row = result.fetchone()
        assert row == (1, "Alice", 95.5)

    def test_fetchone_exhausted(self, sample_table: pa.Table) -> None:
        result = DuckgresqlResult(sample_table)
        result.fetchall()
        assert result.fetchone() is None

    def test_fetchall(self, sample_table: pa.Table) -> None:
        result = DuckgresqlResult(sample_table)
        rows = result.fetchall()
        assert len(rows) == 3
        assert rows[0] == (1, "Alice", 95.5)
        assert rows[2] == (3, "Charlie", 72.3)

    def test_fetchmany(self, sample_table: pa.Table) -> None:
        result = DuckgresqlResult(sample_table)
        rows = result.fetchmany(2)
        assert len(rows) == 2
        assert rows[0] == (1, "Alice", 95.5)
        assert rows[1] == (2, "Bob", 87.0)

    def test_fetchmany_past_end(self, sample_table: pa.Table) -> None:
        result = DuckgresqlResult(sample_table)
        rows = result.fetchmany(10)
        assert len(rows) == 3

    def test_fetch_arrow_table(self, sample_table: pa.Table) -> None:
        result = DuckgresqlResult(sample_table)
        t = result.fetch_arrow_table()
        assert t is sample_table

    def test_description(self, sample_table: pa.Table) -> None:
        result = DuckgresqlResult(sample_table)
        desc = result.description
        assert desc is not None
        assert len(desc) == 3
        assert desc[0][0] == "id"
        assert desc[1][0] == "name"

    def test_description_empty(self) -> None:
        result = DuckgresqlResult(pa.table({}))
        assert result.description is None

    def test_columns(self, sample_table: pa.Table) -> None:
        result = DuckgresqlResult(sample_table)
        assert result.columns == ["id", "name", "score"]

    def test_rowcount_table(self, sample_table: pa.Table) -> None:
        result = DuckgresqlResult(sample_table)
        assert result.rowcount == 3

    def test_rowcount_affected(self) -> None:
        result = DuckgresqlResult(affected_rows=42)
        assert result.rowcount == 42

    def test_repr_table(self, sample_table: pa.Table) -> None:
        result = DuckgresqlResult(sample_table)
        r = repr(result)
        assert "columns=" in r
        assert "rows=3" in r

    def test_repr_affected(self) -> None:
        result = DuckgresqlResult(affected_rows=5)
        assert "affected_rows=5" in repr(result)

    def test_fetchdf_missing_pandas(
        self, sample_table: pa.Table, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import builtins
        original_import = builtins.__import__

        def mock_import(name: str, *args, **kwargs):  # type: ignore[no-untyped-def]
            if name == "pandas":
                raise ImportError("no pandas")
            return original_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", mock_import)
        result = DuckgresqlResult(sample_table)
        with pytest.raises(ImportError, match="pandas"):
            result.fetchdf()

    def test_fetchnumpy_missing_numpy(
        self, sample_table: pa.Table, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import builtins
        original_import = builtins.__import__

        def mock_import(name: str, *args, **kwargs):  # type: ignore[no-untyped-def]
            if name == "numpy":
                raise ImportError("no numpy")
            return original_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", mock_import)
        result = DuckgresqlResult(sample_table)
        with pytest.raises(ImportError, match="numpy"):
            result.fetchnumpy()

    def test_cursor_advances(self, sample_table: pa.Table) -> None:
        result = DuckgresqlResult(sample_table)
        result.fetchone()
        result.fetchone()
        remaining = result.fetchall()
        assert len(remaining) == 1
        assert remaining[0] == (3, "Charlie", 72.3)
