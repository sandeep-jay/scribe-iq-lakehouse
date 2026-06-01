"""Tests for the platform abstraction factory and base contract.

This factory only dispatches local execution surfaces that share the
``LakehousePlatform`` ABC — ``local_lite`` (built) and ``local_spark``
(stub, no module yet). Per ADR-022, cloud-native platforms (Fabric,
Databricks, AWS, GCP) are independent end-to-end implementations and are
instantiated directly inside their own notebooks / entry points; they do
not go through this factory.
"""

import pytest

from core.platform import base, factory
from core.platform.local_lite import LocalLitePlatform


def test_unknown_platform_raises_value_error():
    with pytest.raises(ValueError, match="Unknown platform"):
        factory.get_platform("does-not-exist")


def test_fabric_not_in_factory():
    """Fabric is an independent impl (ADR-022) — not dispatched through this factory."""
    assert "fabric" not in factory.PLATFORMS
    with pytest.raises(ValueError, match="Unknown platform"):
        factory.get_platform("fabric")


def test_explicit_arg_overrides_env_var(monkeypatch):
    # Env says local_lite, but the explicit arg (local_spark, no module yet) wins —
    # the ImportError proves the module path was resolved from the arg, not the env.
    monkeypatch.setenv(factory.ENV_VAR, "local_lite")
    with pytest.raises(ImportError):
        factory.get_platform("local_spark")


def test_unbuilt_platform_raises_import_error():
    with pytest.raises(ImportError):
        factory.get_platform("local_spark")


def test_registered_platforms_present():
    assert set(factory.PLATFORMS) == {"local_spark", "local_lite"}


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
