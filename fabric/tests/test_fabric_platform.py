"""Offline + behaviour tests for FabricPlatform (ADR-022).

The offline tests run in every PR — they verify the class imports cleanly
without a Fabric runtime and that ``storage_path`` builds the expected
OneLake URIs. The ``pytest.mark.fabric`` behaviour test runs only when
``FABRIC_TENANT_ID`` is set in the environment (i.e. against a real
workspace).
"""

from __future__ import annotations

import os

import pytest

from fabric.platform import FabricPlatform

# --------------------------------------------------------------------- offline


def test_storage_path_builds_onelake_uri():
    """storage_path is pure (no Fabric runtime needed) — verify GUID-based URI shape."""
    fp = FabricPlatform(workspace_id="ws-guid", lakehouse_id="lh-guid")
    assert fp.storage_path("silver", "patient") == (
        "abfss://ws-guid@onelake.dfs.fabric.microsoft.com/lh-guid/Tables/silver/patient"
    )
    assert fp.storage_path("gold", "encounter_summary") == (
        "abfss://ws-guid@onelake.dfs.fabric.microsoft.com/lh-guid/Tables/gold/encounter_summary"
    )
    assert fp.storage_path("bronze", "fhir") == (
        "abfss://ws-guid@onelake.dfs.fabric.microsoft.com/lh-guid/Files/bronze/fhir"
    )


def test_files_path_builds_onelake_uri():
    """files_path returns Files/-rooted URIs (manifests, bronze JSON, etc.)."""
    fp = FabricPlatform(workspace_id="ws-guid", lakehouse_id="lh-guid")
    assert fp.files_path() == (
        "abfss://ws-guid@onelake.dfs.fabric.microsoft.com/lh-guid/Files"
    )
    assert fp.files_path("bronze/_metadata/ingest_manifest.json") == (
        "abfss://ws-guid@onelake.dfs.fabric.microsoft.com/"
        "lh-guid/Files/bronze/_metadata/ingest_manifest.json"
    )


def test_storage_path_rejects_bad_layer():
    fp = FabricPlatform(workspace_id="ws", lakehouse_id="lh")
    with pytest.raises(ValueError, match="Invalid layer"):
        fp.storage_path("platinum", "patient")


def test_name_attribute():
    """The class-level ``name`` attribute lets manifest/logging code identify the platform."""
    assert FabricPlatform.name == "fabric"


# --------------------------------------------- behaviour (real Fabric workspace)

_fabric_only = pytest.mark.skipif(
    "FABRIC_TENANT_ID" not in os.environ,
    reason="Requires a real Fabric workspace; set FABRIC_TENANT_ID + workspace/lakehouse to enable",
)


@_fabric_only
@pytest.mark.fabric
def test_round_trip_write_read_silver_against_real_workspace():
    """Write a throwaway Spark DataFrame to Silver and read it back."""
    from fabric.transforms.registry import REGISTRY

    fp = FabricPlatform()  # resolves workspace + lakehouse from mssparkutils
    spark = fp.get_spark_session()
    entry = REGISTRY["patient"]
    row = (
        "test-fp", None, "other", None, None, None, None, None, False, None,
        "fabric-test", None,
    )
    df = spark.createDataFrame([row], schema=entry.schema)
    fp.write_silver_spark("__fabric_test", df, mode="overwrite")
    out = fp.read_silver_spark("__fabric_test")
    assert out.filter("patient_id = 'test-fp'").count() == 1
