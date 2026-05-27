"""Tests for the platform abstraction factory and base contract (ADR-002).

Concrete platform classes (fabric, local_lite, ...) are implemented from Session 2
onward. Until then the factory's contract is what we can pin down: it routes by the
LAKEHOUSE_PLATFORM env var, rejects unknown names, and surfaces a clear ImportError
for registered-but-not-yet-built platforms.
"""

import pytest

from local.platform import base, factory


def test_unknown_platform_raises_value_error():
    with pytest.raises(ValueError, match="Unknown platform"):
        factory.get_platform("does-not-exist")


def test_env_var_overridden_by_explicit_arg(monkeypatch):
    monkeypatch.setenv(factory.ENV_VAR, "fabric")
    # Explicit arg wins over env var; both are unimplemented, so ImportError, not
    # AttributeError — proving the module path was resolved from the arg.
    with pytest.raises(ImportError):
        factory.get_platform("local_lite")


def test_registered_platforms_present():
    assert set(factory.PLATFORMS) == {
        "fabric",
        "databricks",
        "aws",
        "gcp",
        "local_spark",
        "local_lite",
    }


def test_default_platform_is_local_lite(monkeypatch):
    monkeypatch.delenv(factory.ENV_VAR, raising=False)
    # Default resolves to local_lite, which is not built yet -> ImportError on import.
    with pytest.raises(ImportError):
        factory.get_platform()


def test_base_validate_layer():
    class _Stub(base.LakehousePlatform):  # minimal concrete subclass for the guard
        def storage_path(self, layer, table): ...
        def read_bronze_fhir(self, cohort=None): ...
        def write_silver(self, table, data, mode="merge"): ...
        def read_silver(self, table): ...
        def write_gold(self, table, data): ...
        def log_metric(self, table, metric, value): ...
        def send_alert(self, severity, message): ...
        def get_spark_session(self): ...

    stub = _Stub()
    stub._validate_layer("silver")  # no raise
    with pytest.raises(ValueError, match="Unknown layer"):
        stub._validate_layer("platinum")
