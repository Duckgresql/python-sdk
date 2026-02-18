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
    "localhost",
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
        "localhost",
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
with duckgresql.connect("localhost", token="...", database="...") as conn:
    result = conn.execute("SELECT 1")
    print(result.fetchone())
```

### Async Jobs (Long-Running Queries)

```python
conn = duckgresql.connect("localhost", token="...", database="...")

job = conn.execute_async("SELECT * FROM very_large_table")
print(f"Job submitted: {job.job_id}")

# Poll until done (with exponential backoff)
result = job.result(timeout=600)
print(result.fetchall())
```

## Connection Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `host` | `"localhost"` | Server hostname |
| `token` | *required* | API token (`dkgql_…`) |
| `database` | *required* | Database name or UUID |
| `port` | `47470` | Flight SQL (gRPC) port |
| `use_tls` | `False` | Enable TLS for Flight SQL |
| `rest_port` | `3100` | REST API port |
| `rest_scheme` | `"http"` | `"http"` or `"https"` |

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

## Development

**Development requires Python 3.13+** (users can install with Python 3.11+). This repo uses [uv](https://docs.astral.sh/uv/) for installs and scripts; `.python-version` pins Python 3.13.

```bash
make install          # uv sync (editable install + dev + all extras)
make test             # pytest
make lint             # ruff check
make typecheck        # mypy
make build            # build wheel + sdist
```

### Testing the package as if installed from PyPI

To verify the library works the same way end users get it (from a built wheel, not editable):

1. **Install tools and build the package**
   ```bash
   make install        # installs uv deps + build
   make build          # creates dist/duckgresql-*.whl
   ```

2. **Create a clean environment and install only the wheel** (no repo code)
   ```bash
   uv venv .venv-pypi-test --python 3.13
   uv pip install --python .venv-pypi-test/bin/python dist/duckgresql-*.whl
   ```

3. **Use it like a normal install**
   ```bash
   .venv-pypi-test/bin/python -c "
   import duckgresql
   print('Version:', duckgresql.__version__)
   # Optional: connect if you have a running DuckGresQL server
   # conn = duckgresql.connect('localhost', token='dkgql_...', database='mydb')
   # print(conn.execute('SELECT 1').fetchone())
   "
   ```

   Or run your own script:
   ```bash
   .venv-pypi-test/bin/python your_script.py
   ```

4. **Optional: match production defaults before building**  
   If you want the same defaults as the published package, set release env vars and inject before building:
   ```bash
   export DUCKGRESQL_RELEASE_HOST=api.duckgresql.com
   export DUCKGRESQL_RELEASE_FLIGHT_PORT=47470
   export DUCKGRESQL_RELEASE_REST_PORT=3100
   make inject-defaults
   make build
   ```
   Then repeat steps 2–3 with the new `dist/` wheel.

You can also run `make test-pypi-install` to build, create `.venv-pypi-test`, install the wheel, and run a quick import/version check.

## Version

The SDK version (`1.4.4.0`) tracks DuckDB version `1.4.4`, with the last digit as the SDK patch version.

## Publishing

See [RELEASING.md](./RELEASING.md) for instructions on publishing new versions to PyPI.
