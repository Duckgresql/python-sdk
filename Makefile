.PHONY: help install test lint format typecheck inject-defaults build publish test-pypi-install test-pypi-install-pypi-prod

# Default target
.DEFAULT_GOAL := help

# UV uses .python-version (3.13) and installs that Python if missing. No need to choose a version.
UV ?= uv

##@ Python SDK

help: ## Display this help message
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

install: ## Sync deps with UV (editable install + dev + all extras; uses .python-version; UV installs Python 3.13 if needed)
	$(UV) sync --extra dev --extra all

test: ## Run unit tests
	$(UV) run pytest tests/ -v

lint: ## Lint code
	$(UV) run ruff check src/ tests/

format: ## Format code
	$(UV) run ruff format src/ tests/

typecheck: ## Type-check with mypy
	$(UV) run mypy src/duckgresql/

inject-defaults: ## Inject release defaults into _config.py (requires DUCKGRESQL_RELEASE_* env vars)
	$(UV) run python scripts/inject_release_defaults.py

build: ## Build package
	$(UV) run python -m build

publish: ## Publish to PyPI (requires TWINE_PASSWORD or trusted publishing)
	$(UV) run python -m twine upload dist/*

test-pypi-install: ## Build, install wheel into .venv-pypi-test, and verify (simulate PyPI install)
	$(UV) run python -m build
	$(UV) venv .venv-pypi-test --python 3.13
	$(UV) pip install --python .venv-pypi-test/bin/python dist/duckgresql-*.whl
	@echo "--- Done. Use: .venv-pypi-test/bin/python your_script.py <sql_query> ---"

# For parameterized queries use single quotes for Q so the shell doesn't expand $$ (e.g. Q='SELECT * FROM t WHERE id = $$1' P="[3]").
test-pypi-install-prod: ## Like test-pypi-install but with production defaults. Usage: make test-pypi-install-prod Q="SELECT 1" or Q='SELECT * FROM t WHERE id = $$1' P="[3]"
	@test -f .env.prod || { echo "Error: .env.prod not found. Copy .env.prod.example and fill in values."; exit 1; }
	@test -n "$(Q)" || { echo "Error: query required. Use single quotes for parameterized Q: make test-pypi-install-prod Q='SELECT * FROM t WHERE id = \$$1' P=\"[3]\""; exit 1; }
	@# Load .env.prod, inject production config, build, restore dev config, then install
	set -a && . ./.env.prod && set +a && $(UV) run python scripts/inject_release_defaults.py
	$(UV) run python -m build
	git checkout src/duckgresql/_config.py
	rm -rf .venv-pypi-test
	$(UV) venv .venv-pypi-test --python 3.13
	$(UV) pip install --python .venv-pypi-test/bin/python dist/duckgresql-*.whl
	$(UV) pip install --python .venv-pypi-test/bin/python python-dotenv
	.venv-pypi-test/bin/python example/run_query.py --env .env.prod '$(Q)' --params '$(P)'

test-pypi-install-pypi-prod: ## Install real duckgresql from PyPI and run query. Usage: make test-pypi-install-pypi-prod Q="SELECT 1" DUCKGRESQL_TOKEN=... DUCKGRESQL_DATABASE=...
	@test -n "$(Q)" || { echo "Error: query required. Usage: make test-pypi-install-pypi-prod Q=\"SELECT 1\" DUCKGRESQL_TOKEN=... DUCKGRESQL_DATABASE=..."; exit 1; }
	@test -n "$(DUCKGRESQL_TOKEN)" || { echo "Error: DUCKGRESQL_TOKEN required."; exit 1; }
	@test -n "$(DUCKGRESQL_DATABASE)" || { echo "Error: DUCKGRESQL_DATABASE required."; exit 1; }
	rm -rf .venv-pypi-test
	$(UV) venv .venv-pypi-test --python 3.13
	$(UV) pip install --python .venv-pypi-test/bin/python duckgresql python-dotenv
	DUCKGRESQL_TOKEN="$(DUCKGRESQL_TOKEN)" DUCKGRESQL_DATABASE="$(DUCKGRESQL_DATABASE)" .venv-pypi-test/bin/python example/run_query.py "$(Q)"
