from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv


def _load_env() -> None:
    """Load `.env` so that duckgresql uses those defaults.

    Primero intenta cargar el `.env` en la raíz del repo (el padre de
    esta carpeta `example/`), y luego uno en el directorio actual
    por si quieres tener un `.env` específico.
    """

    here = Path(__file__).resolve()
    repo_root = here.parent.parent

    # .env en la raíz del repo (p. ej. /path/to/python-sdk/.env)
    load_dotenv(repo_root / ".env")
    # .env en el directorio actual (sobrescribe si hace falta)
    load_dotenv()


def main() -> None:
    _load_env()

    # Importamos aquí para que ya estén cargadas las variables de entorno
    import duckgresql

    if len(sys.argv) < 2:
        print("Uso: python run_query.py '<consulta_sql>'")
        raise SystemExit(1)

    query = sys.argv[1]

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
        result = conn.execute(query)
        rows = result.fetchall()
        print("Resultado de la query:", rows)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
