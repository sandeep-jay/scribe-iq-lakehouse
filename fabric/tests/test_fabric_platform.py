"""Contract tests for FabricPlatform — verifies the stub satisfies the interface.

When Session 5 implements the methods, these tests stay green and become the
backbone of platform-contract coverage; they enforce that every method declared
on :class:`core.platform.base.LakehousePlatform` exists on FabricPlatform and
that the factory dispatches to this class for ``LAKEHOUSE_PLATFORM=fabric``.
"""

from __future__ import annotations

import inspect

import pytest

from core.platform.base import LakehousePlatform
from core.platform.factory import PLATFORMS
from fabric.platform import FabricPlatform


def test_subclass_of_interface():
    assert issubclass(FabricPlatform, LakehousePlatform)


def test_factory_dispatch_string_matches():
    assert PLATFORMS["fabric"] == "fabric.platform.FabricPlatform"


def test_implements_all_abstract_methods():
    """Every abstract method on LakehousePlatform must be present on FabricPlatform."""
    abstract = {
        name
        for name, member in inspect.getmembers(LakehousePlatform, inspect.isfunction)
        if getattr(member, "__isabstractmethod__", False)
    }
    implemented = {
        name for name, _ in inspect.getmembers(FabricPlatform, inspect.isfunction)
    }
    missing = abstract - implemented
    assert not missing, f"FabricPlatform missing abstract methods: {missing}"


def test_methods_raise_not_implemented():
    """Stub state — every method should fail loudly, not silently return None."""
    fp = FabricPlatform()
    with pytest.raises(NotImplementedError):
        fp.storage_path("silver", "patient")
    with pytest.raises(NotImplementedError):
        fp.read_bronze_fhir(cohort="A")
