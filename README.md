# DuckGresQL Python SDK

Python client for [DuckGresQL](https://github.com/jjballano/duckgresql) — a DuckDB-compatible API for remote databases. Uses **Arrow Flight SQL** for fast query execution and a REST API for async job management.

## Installation

Requires Python 3.11+

```bash
pip install duckgresql
```

With optional extras:

```bash
pip install duckgresql[pandas]   # adds fetchdf() support
pip install duckgresql[all]      # pandas + numpy
```

## Quick Start

### Synchronous

```python
import duckgresql

conn = duckgresql.connect(
    token="dkgql_your_api_token",
    database="my_database",
)

# Execute a query
result = conn.execute("SELECT * FROM users LIMIT 10")
print(result.fetchall())

# Get a pandas DataFrame
df = conn.execute("SELECT * FROM sales").fetchdf()

# DML
result = conn.execute("INSERT INTO users (name) VALUES ('Alice')")
print(result.rowcount)  # affected rows

conn.close()
```

### Async

```python
import asyncio
import duckgresql

async def main():
    conn = await duckgresql.connect_async(
        token="dkgql_your_api_token",
        database="my_database",
    )

    result = await conn.execute("SELECT * FROM users")
    print(result.fetchall())

    await conn.close()

asyncio.run(main())
```

### Context Manager

```python
with duckgresql.connect(token="...", database="...") as conn:
    result = conn.execute("SELECT 1")
    print(result.fetchone())
```

### Async Jobs (Long-Running Queries)

```python
conn = duckgresql.connect(token="...", database="...")

job = conn.execute_async("SELECT * FROM very_large_table")
print(f"Job submitted: {job.job_id}")

# Poll until done (with exponential backoff)
result = job.result(timeout=600)
print(result.fetchall())
```

## Connection Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `token` | *required* | API token (`dkgql_…`) |
| `database` | *required* | Database name or UUID |

## Result Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `fetchone()` | `tuple \| None` | Next row |
| `fetchmany(size)` | `list[tuple]` | Up to *size* rows |
| `fetchall()` | `list[tuple]` | All remaining rows |
| `fetchdf()` | `DataFrame` | Pandas DataFrame (requires `pandas`) |
| `fetchnumpy()` | `dict[str, ndarray]` | NumPy arrays (requires `numpy`) |
| `fetch_arrow_table()` | `pyarrow.Table` | Zero-copy Arrow table |

## Result Properties

| Property | Type | Description |
|----------|------|-------------|
| `description` | `list \| None` | DB-API 2.0 column metadata |
| `rowcount` | `int` | Row count or affected rows |
| `columns` | `list[str]` | Column names |

## Architecture

- **Arrow Flight SQL (gRPC)** — Primary transport for `execute()`. Fast, columnar, zero-copy.
- **REST API** — Used only for `execute_async()` (job submission/polling), since Flight SQL has no native job queue pattern.

Both transports authenticate via the same API token. Flight SQL uses BasicAuth handshake; REST uses the `/connect` endpoint to exchange the token for a session token.

---

**Developing the SDK?** See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, testing, building, and publishing.
