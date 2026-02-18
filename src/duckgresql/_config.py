"""Connection defaults.

In development, these values are read from environment variables so you can
point the SDK at a local server without changing code. A ``.env`` file in the
current working directory is loaded automatically when this module is imported.

In release builds, this file is regenerated with hardcoded production values
by ``scripts/inject_release_defaults.py`` — users who install from PyPI never
need to set any environment variables.

Development environment variables
----------------------------------
DUCKGRESQL_HOST          Server hostname (default: localhost)
DUCKGRESQL_FLIGHT_PORT   Arrow Flight SQL (gRPC) port (default: 47470)
DUCKGRESQL_REST_PORT     REST API port (default: 3100)
DUCKGRESQL_USE_TLS       Enable TLS for Flight SQL, 'true'/'1'/'yes' (default: false)
DUCKGRESQL_REST_SCHEME   REST transport scheme, 'http' or 'https' (default: http)
"""

from __future__ import annotations

import os as _os

from dotenv import load_dotenv

load_dotenv()

DEFAULT_HOST: str = _os.environ.get("DUCKGRESQL_HOST", "localhost")
DEFAULT_FLIGHT_PORT: int = int(_os.environ.get("DUCKGRESQL_FLIGHT_PORT", "47470"))
DEFAULT_REST_PORT: int = int(_os.environ.get("DUCKGRESQL_REST_PORT", "3100"))
_tls_env: str = _os.environ.get("DUCKGRESQL_USE_TLS", "").lower()
DEFAULT_USE_TLS: bool = _tls_env in ("true", "1", "yes")
DEFAULT_REST_SCHEME: str = _os.environ.get("DUCKGRESQL_REST_SCHEME", "http")
