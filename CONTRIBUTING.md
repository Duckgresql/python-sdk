# Contributing to the DuckGresQL Python SDK

This document is for **developers of the SDK** (contributors and maintainers). For using the library, see [README.md](README.md).

## Prerequisites

- **Python 3.13+** (SDK development; end users can install with Python 3.11+)
- [uv](https://docs.astral.sh/uv/) for installs and scripts
- `.python-version` in the repo pins Python 3.13

## Development Setup

```bash
make install    # uv sync (editable install + dev + all extras)
make test       # pytest
make lint       # ruff check
make typecheck  # mypy
make build      # build wheel + sdist
```

Other targets: `make format`, `make inject-defaults`, `make publish`, `make help`.

## Testing the Package as if Installed from PyPI

To verify the library works the same way end users get it (from a built wheel, not editable):

1. **Install tools and build the package**
   ```bash
   make install
   make build
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
   # conn = duckgresql.connect(token='dkgql_...', database='mydb')
   # print(conn.execute('SELECT 1').fetchone())
   "
   ```
   Or run your own script: `.venv-pypi-test/bin/python your_script.py`

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

You can also run **`make test-pypi-install`** to build, create `.venv-pypi-test`, install the wheel, and run a quick import/version check.

## Version Scheme

The SDK version (`1.4.4.0`) tracks DuckDB version `1.4.4`, with the last digit as the SDK patch version.

## Publishing

See [RELEASING.md](RELEASING.md) for instructions on publishing new versions to PyPI.
