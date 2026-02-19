.PHONY: help install test lint format typecheck inject-defaults build publish test-pypi-install

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
