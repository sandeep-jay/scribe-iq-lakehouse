"""Contract + behaviour tests for FabricPlatform.

The four offline contract tests run in every PR — they verify the class still
satisfies the abstract interface and can be imported / constructed without a
Fabric runtime. The ``pytest.mark.fabric`` behaviour tests run only when
``FABRIC_TENANT_ID`` is set in the environment (i.e. against a real workspace).
"""

from __future__ import annotations

import inspect
import os
from datetime import UTC, datetime

import pyarrow as pa
import pytest

from core.platform.base import LakehousePlatform
from core.platform.factory import PLATFORMS
from fabric.platform import FabricPlatform

# --------------------------------------------------------------------- offline


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
    implemented = {name for name, _ in inspect.getmembers(FabricPlatform, inspect.isfunction)}
    missing = abstract - implemented
    assert not missing, f"FabricPlatform missing abstract methods: {missing}"


def test_storage_path_builds_onelake_uri():
    """storage_path is pure (no Fabric runtime needed) — verify the URI shape."""
    fp = FabricPlatform(workspace_id="ws-guid", lakehouse_name="scribe_iq")
    assert fp.storage_path("silver", "patient") == (
        "abfss://ws-guid@onelake.dfs.fabric.microsoft.com/scribe_iq.Lakehouse/Tables/silver/patient"
    )
    assert fp.storage_path("gold", "encounter_summary") == (
        "abfss://ws-guid@onelake.dfs.fabric.microsoft.com/"
        "scribe_iq.Lakehouse/Tables/gold/encounter_summary"
    )
    assert fp.storage_path("bronze", "fhir") == (
        "abfss://ws-guid@onelake.dfs.fabric.microsoft.com/scribe_iq.Lakehouse/Files/bronze/fhir"
    )


def test_storage_path_rejects_bad_layer():
    fp = FabricPlatform(workspace_id="ws", lakehouse_name="lh")
    with pytest.raises(ValueError, match="Unknown layer"):
        fp.storage_path("platinum", "patient")


# --------------------------------------------- behaviour (real Fabric workspace)

_fabric_only = pytest.mark.skipif(
    "FABRIC_TENANT_ID" not in os.environ,
    reason="Requires a real Fabric workspace; set FABRIC_TENANT_ID + workspace/lakehouse to enable",
)


@_fabric_only
@pytest.mark.fabric
def test_round_trip_write_read_silver_against_real_workspace():
    """Write a throwaway Arrow table to Silver and read it back via Spark."""
    from core.transforms.silver_patient import build_silver_patient

    fp = FabricPlatform()  # resolves workspace + lakehouse from mssparkutils
    payload = build_silver_patient(
        [{"patient_id": "test-fp", "gender": "other", "source_file": "fabric-test"}],
        datetime.now(tz=UTC),
    )
    fp.write_silver("__fabric_test", payload, mode="overwrite")
    out: pa.Table = fp.read_silver("__fabric_test")
    rows = out.to_pylist()
    assert any(r.get("patient_id") == "test-fp" for r in rows)
