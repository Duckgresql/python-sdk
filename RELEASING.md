# Releasing the Python SDK

This document describes how to publish a new version of the DuckGresQL Python SDK to PyPI.

## Prerequisites

1. **PyPI Account with Trusted Publishing configured**
   - Go to [PyPI](https://pypi.org) and create an account
   - Create the `duckgresql` project (first release only)
   - Configure Trusted Publishing for this GitHub repository

2. **GitHub Repository Secrets**
   - No manual API tokens needed — uses OIDC Trusted Publishing
   - The `pypi` environment is configured in the workflow

3. **GitHub Repository Variables** (Settings → Secrets and variables → Actions → Variables)

   The release workflow injects these as hardcoded values into the published package so
   users who install from PyPI need zero configuration:

   | Variable | Required | Example | Description |
   |---|---|---|---|
   | `DUCKGRESQL_RELEASE_HOST` | Yes | `api.duckgresql.com` | Production server hostname |
   | `DUCKGRESQL_RELEASE_FLIGHT_PORT` | Yes | `47470` | Arrow Flight SQL (gRPC) port |
   | `DUCKGRESQL_RELEASE_REST_PORT` | Yes | `3100` | REST API port |
   | `DUCKGRESQL_RELEASE_USE_TLS` | No | `true` | TLS for Flight SQL (default: `true`) |
   | `DUCKGRESQL_RELEASE_REST_SCHEME` | No | `https` | REST transport scheme (default: `https`) |

   > **Note:** These are repository **Variables** (not Secrets) since they are not sensitive.

## Release Process

### 1. Update Version

Edit `src/duckgresql/_version.py`:

```python
__version__ = "1.4.4.1"  # Increment as needed
```

**Version scheme:** `MAJOR.MINOR.PATCH.SDK_PATCH`
- `MAJOR.MINOR.PATCH` tracks DuckDB version (currently `1.4.4`)
- `SDK_PATCH` increments for SDK-only changes

### 2. Update Changelog

Add release notes to `logs/dev_changelog/sdk/YYYY-MM-DD_release-X.X.X.X.md`:

```markdown
# Python SDK v1.4.4.1 Release

**Date**: YYYY-MM-DD
**Type**: Release

## Changes
- Bug fix: Fixed XYZ
- Feature: Added ABC

## Breaking Changes
None
```

### 3. Commit and Push

```bash
git add src/duckgresql/_version.py logs/dev_changelog/sdk/
git commit -m "chore(sdk): bump version to 1.4.4.1"
git push origin main
```

### 4. Create and Push Tag

```bash
# Create annotated tag
git tag -a v1.4.4.1 -m "Python SDK v1.4.4.1"

# Push tag to trigger release workflow
git push origin v1.4.4.1
```

### 5. Monitor Release

1. Go to **Actions** tab in GitHub
2. Watch the `Python SDK Release` workflow
3. Workflow will:
   - Verify version matches tag
   - Run tests, lint, type-check
   - **Inject production defaults** into `_config.py` from GitHub repository variables
   - Build wheel and sdist (with hardcoded server settings)
   - Publish to PyPI (via Trusted Publishing)
   - Create GitHub Release with artifacts

### 6. Verify Publication

```bash
# Check PyPI
open https://pypi.org/project/duckgresql/

# Test installation
pip install duckgresql==1.4.4.1
python -c "import duckgresql; print(duckgresql.__version__)"
```

## Configuring PyPI Trusted Publishing

### First-Time Setup

1. **Create PyPI project** (first release only):
   ```bash
   # Manually create the project on PyPI via the web UI
   # OR do a manual upload for v1.0.0 using API token
   ```

2. **Add Trusted Publisher** (on PyPI project settings):
   - Go to https://pypi.org/manage/project/duckgresql/settings/publishing/
   - Add a new publisher:
     - **Owner**: `duckgresql` (your GitHub org/user)
     - **Repository**: `duckgresql`
     - **Workflow**: `release.yml`
     - **Environment**: `pypi`

3. **Done** — no API tokens needed!

## Troubleshooting

### Tag already exists
```bash
# Delete local tag
git tag -d v1.4.4.1

# Delete remote tag
git push origin :refs/tags/v1.4.4.1

# Recreate and push
git tag -a v1.4.4.1 -m "Python SDK v1.4.4.1"
git push origin v1.4.4.1
```

### Version mismatch error
The workflow checks that the tag version matches `_version.py`. Ensure they match exactly:
- Tag: `v1.4.4.1`
- Code: `__version__ = "1.4.4.1"`

### PyPI upload fails
- Check that Trusted Publishing is configured correctly
- Verify the `pypi` environment exists in GitHub Settings > Environments
- Check workflow logs for detailed error messages

## CI/CD Workflows

### `check.yml` (Continuous Integration)
- Runs on every push/PR to `main`
- Tests on Python 3.11, 3.12, 3.13 (ensures compatibility)
- Runs lint, type-check, tests, build

### `release.yml` (Release)
- Triggered by tags matching `v*`
- Verifies version, runs full test suite
- Publishes to PyPI via Trusted Publishing
- Creates GitHub Release with artifacts

## Pre-Release Checklist

- [ ] Version bumped in `_version.py`
- [ ] Changelog updated
- [ ] All tests passing locally (`make test`)
- [ ] Lint clean (`make lint`)
- [ ] Type-check passing (`make typecheck`)
- [ ] README.md updated (if needed)
- [ ] GitHub repository variables set (`DUCKGRESQL_RELEASE_HOST`, `DUCKGRESQL_RELEASE_FLIGHT_PORT`, `DUCKGRESQL_RELEASE_REST_PORT`)
- [ ] Committed and pushed to main
- [ ] Tag created and pushed

### Test release defaults locally

```bash
# Fill in .env.prod with production values (see .env.prod.example)
make test-pypi-install-prod Q="SELECT 1"
```

## Post-Release Checklist

- [ ] GitHub Release created successfully
- [ ] PyPI shows new version
- [ ] `pip install duckgresql==X.X.X.X` works
- [ ] Announcement posted (if major release)
