from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv


def _load_env(env_path: str | None = None) -> None:
    """Load environment variables so that duckgresql uses those defaults.

    If *env_path* is given, only that file is loaded.  Otherwise, the
    ``.env`` at the repo root and the one in the current directory are
    loaded (current directory wins).
    """

    if env_path is not None:
        load_dotenv(Path(env_path).resolve(), override=True)
        return

    here = Path(__file__).resolve()
    repo_root = here.parent.parent

    # .env at the repo root (e.g. /path/to/python-sdk/.env)
    load_dotenv(repo_root / ".env")
    # .env in the current directory (overrides if present)
    load_dotenv()


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Run a SQL query against DuckGresQL")
    parser.add_argument(
        "query",
        help="SQL query to execute. Use $1, $2 for parameters (e.g. \"SELECT * FROM t WHERE id = $1\") and --params \"[3]\" for bindings.",
    )
    parser.add_argument(
        "--params",
        help="Bindings for the query (Python literal, e.g. \"[3]\" or \"['x']\"). Via make: use single-quoted Q so the shell does not expand $$ (e.g. Q='SELECT * FROM t WHERE id = $$1' P=\"[3]\").",
        default=None,
    )
    parser.add_argument("--env", default=None, help="Path to .env file (default: .env in repo root / cwd)")
    args = parser.parse_args()

    _load_env(args.env)

    # Import after env is loaded so the SDK picks up the variables
    import duckgresql

    query = args.query
    params = args.params
    token = os.environ.get("DUCKGRESQL_TOKEN")
    database = os.environ.get("DUCKGRESQL_DATABASE")
    print(token, database)
    if not token or not database:
        raise SystemExit(
            "Faltan variables de entorno: DUCKGRESQL_TOKEN y/o "
            "DUCKGRESQL_DATABASE en el fichero .env"
        )

    print("duckgresql", duckgresql.__version__)

    conn = duckgresql.connect(token=token, database=database)
    try:
        import ast

        parsed_params = None
        if params:
            try:
                # Try to interpret params as Python literal (list/dict/None/str/etc.)
                parsed_params = ast.literal_eval(params)
            except Exception:
                # Fallback: use as simple string if not parseable
                parsed_params = params
        print("parsed_params", parsed_params)
        print("query", query)
        result = conn.execute(query, parsed_params)
        rows = result.fetchall()
        print("Resultado de la query:", rows)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
