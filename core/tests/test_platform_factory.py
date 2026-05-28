"""Tests for the platform abstraction factory and base contract (ADR-002).

``local_lite`` is implemented (Session 2); ``fabric`` is a stub that imports cleanly
but raises ``NotImplementedError`` on every method (Session 4.5, ADR-017). The other
platforms (databricks, aws, gcp, local_spark) are still un-built — their import paths
point at modules that don't exist, so requesting them raises ImportError. The factory's
job is to route by the LAKEHOUSE_PLATFORM env var and reject unknown names.
"""

import pytest

from core.platform import base, factory
from core.platform.local_lite import LocalLitePlatform


def test_unknown_platform_raises_value_error():
    with pytest.raises(ValueError, match="Unknown platform"):
        factory.get_platform("does-not-exist")


def test_explicit_arg_overrides_env_var(monkeypatch):
    # Env says local_lite, but the explicit arg (databricks, no module yet) wins —
    # the ImportError proves the module path was resolved from the arg, not the env.
    monkeypatch.setenv(factory.ENV_VAR, "local_lite")
    with pytest.raises(ImportError):
        factory.get_platform("databricks")


def test_unbuilt_platform_raises_import_error():
    with pytest.raises(ImportError):
        factory.get_platform("aws")


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
    assert isinstance(factory.get_platform(), LocalLitePlatform)


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
