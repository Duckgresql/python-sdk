"""Tests for _config module and inject_release_defaults script."""

from __future__ import annotations

import importlib
import pathlib
import sys
import types

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _reload_config() -> types.ModuleType:
    """Force a fresh import of _config so env var changes take effect."""
    sys.modules.pop("duckgresql._config", None)
    return importlib.import_module("duckgresql._config")


@pytest.fixture(autouse=True)
def _clean_config_module() -> pytest.FixtureRequest:  # type: ignore[return]
    """Pop _config from sys.modules before and after each test."""
    sys.modules.pop("duckgresql._config", None)
    yield
    sys.modules.pop("duckgresql._config", None)


# ---------------------------------------------------------------------------
# _config defaults
# ---------------------------------------------------------------------------


class TestConfigDefaults:
    def test_defaults_without_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for var in (
            "DUCKGRESQL_HOST",
            "DUCKGRESQL_FLIGHT_PORT",
            "DUCKGRESQL_REST_PORT",
            "DUCKGRESQL_USE_TLS",
            "DUCKGRESQL_REST_SCHEME",
        ):
            monkeypatch.delenv(var, raising=False)

        cfg = _reload_config()

        assert cfg.DEFAULT_HOST == "localhost"
        assert cfg.DEFAULT_FLIGHT_PORT == 47470
        assert cfg.DEFAULT_REST_PORT == 3100
        assert cfg.DEFAULT_USE_TLS is False
        assert cfg.DEFAULT_REST_SCHEME == "http"

    def test_env_host_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DUCKGRESQL_HOST", "myserver.example.com")
        cfg = _reload_config()
        assert cfg.DEFAULT_HOST == "myserver.example.com"

    def test_env_flight_port_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DUCKGRESQL_FLIGHT_PORT", "50000")
        cfg = _reload_config()
        assert cfg.DEFAULT_FLIGHT_PORT == 50000

    def test_env_rest_port_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DUCKGRESQL_REST_PORT", "9000")
        cfg = _reload_config()
        assert cfg.DEFAULT_REST_PORT == 9000

    @pytest.mark.parametrize("value", ["true", "True", "TRUE", "1", "yes"])
    def test_env_use_tls_truthy(
        self, monkeypatch: pytest.MonkeyPatch, value: str
    ) -> None:
        monkeypatch.setenv("DUCKGRESQL_USE_TLS", value)
        cfg = _reload_config()
        assert cfg.DEFAULT_USE_TLS is True

    @pytest.mark.parametrize("value", ["false", "False", "0", "no", ""])
    def test_env_use_tls_falsy(
        self, monkeypatch: pytest.MonkeyPatch, value: str
    ) -> None:
        monkeypatch.setenv("DUCKGRESQL_USE_TLS", value)
        cfg = _reload_config()
        assert cfg.DEFAULT_USE_TLS is False

    def test_env_rest_scheme_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DUCKGRESQL_REST_SCHEME", "https")
        cfg = _reload_config()
        assert cfg.DEFAULT_REST_SCHEME == "https"

    def test_flight_port_is_int(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DUCKGRESQL_FLIGHT_PORT", "12345")
        cfg = _reload_config()
        assert isinstance(cfg.DEFAULT_FLIGHT_PORT, int)

    def test_rest_port_is_int(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DUCKGRESQL_REST_PORT", "9999")
        cfg = _reload_config()
        assert isinstance(cfg.DEFAULT_REST_PORT, int)


# ---------------------------------------------------------------------------
# inject_release_defaults script
# ---------------------------------------------------------------------------


def _import_inject_script() -> types.ModuleType:
    """Import inject_release_defaults from scripts/ without installing it."""
    scripts_dir = pathlib.Path(__file__).parent.parent / "scripts"
    spec = importlib.util.spec_from_file_location(  # type: ignore[attr-defined]
        "inject_release_defaults",
        scripts_dir / "inject_release_defaults.py",
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)  # type: ignore[attr-defined]
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


class TestInjectReleaseDefaults:
    def test_writes_hardcoded_values(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
    ) -> None:
        monkeypatch.setenv("DUCKGRESQL_RELEASE_HOST", "api.example.com")
        monkeypatch.setenv("DUCKGRESQL_RELEASE_FLIGHT_PORT", "47470")
        monkeypatch.setenv("DUCKGRESQL_RELEASE_REST_PORT", "3100")
        monkeypatch.setenv("DUCKGRESQL_RELEASE_USE_TLS", "true")
        monkeypatch.setenv("DUCKGRESQL_RELEASE_REST_SCHEME", "https")

        out = tmp_path / "_config.py"
        mod = _import_inject_script()
        mod.main(output_path=out)

        content = out.read_text()
        assert "api.example.com" in content
        assert "47470" in content
        assert "3100" in content
        assert "True" in content
        assert "'https'" in content
        # Must NOT contain env-var reading code
        assert "os.environ" not in content

    def test_default_tls_is_true(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
    ) -> None:
        monkeypatch.setenv("DUCKGRESQL_RELEASE_HOST", "api.example.com")
        monkeypatch.setenv("DUCKGRESQL_RELEASE_FLIGHT_PORT", "47470")
        monkeypatch.setenv("DUCKGRESQL_RELEASE_REST_PORT", "3100")
        monkeypatch.delenv("DUCKGRESQL_RELEASE_USE_TLS", raising=False)
        monkeypatch.delenv("DUCKGRESQL_RELEASE_REST_SCHEME", raising=False)

        out = tmp_path / "_config.py"
        mod = _import_inject_script()
        mod.main(output_path=out)

        content = out.read_text()
        assert "True" in content   # TLS on by default in release
        assert "'https'" in content  # scheme defaults to https when TLS on

    def test_missing_required_env_exits(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
    ) -> None:
        monkeypatch.delenv("DUCKGRESQL_RELEASE_HOST", raising=False)
        monkeypatch.delenv("DUCKGRESQL_RELEASE_FLIGHT_PORT", raising=False)
        monkeypatch.delenv("DUCKGRESQL_RELEASE_REST_PORT", raising=False)

        out = tmp_path / "_config.py"
        mod = _import_inject_script()

        with pytest.raises(SystemExit) as exc_info:
            mod.main(output_path=out)

        assert exc_info.value.code != 0
        assert not out.exists()
