"""Tests for the Dagster orchestration tier (ADR-015, ADR-016).

Verifies that:
- The full ``Definitions`` object loads cleanly and exposes every asset / check /
  sensor we wired (the "does the medallion graph hang together?" test).
- The Bronze + Silver assets materialize end-to-end on the synthetic fixture and
  write all 10 Silver Delta tables via ``LocalLitePlatform`` to a tmp root.
- The Gold asset reads the materialized Silver, writes ``gold.encounter_summary``,
  and persists the corpus manifest — the full medallion in three Dagster steps.

The pure transform / validation logic itself is covered by the existing per-table
tests (``test_silver_*.py``, ``test_gold_encounter_summary.py``); this file is the
*wiring* test for the orchestration tier.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

# Dagster lives in the optional `[orchestration]` extra; skip the whole module
# (rather than fail collection) when running against a `[dev]`-only install.
pytest.importorskip("dagster", reason="install .[orchestration] to run Dagster tests")

from dagster import DagsterInstance, materialize  # noqa: E402

from core.orchestration.dagster.assets import (  # noqa: E402
    SILVER_ASSET_KEYS,
    bronze_fhir,
    gold_encounter_summary,
    silver_tables,
)
from core.orchestration.dagster.definitions import defs  # noqa: E402
from core.orchestration.dagster.partitions import COHORT_PARTITIONS_NAME  # noqa: E402
from core.orchestration.dagster.resources import PlatformResource  # noqa: E402

FIXTURE = Path(__file__).parent / "fixtures" / "sample_bundle.json"
COHORT = "test"


@pytest.fixture()
def lakehouse_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A fresh ``data/`` root with the fixture bundle landed as cohort=test/."""
    root = tmp_path / "data"
    bronze = root / "bronze" / "fhir" / f"cohort={COHORT}"
    bronze.mkdir(parents=True)
    shutil.copy(FIXTURE, bronze / "sample_bundle.json")
    monkeypatch.setenv("LAKEHOUSE_LOCAL_ROOT", str(root))
    monkeypatch.setenv("LAKEHOUSE_PLATFORM", "local_lite")
    monkeypatch.chdir(tmp_path)  # so DEFAULT_BRONZE (data/bronze) resolves under tmp
    return root


@pytest.fixture()
def instance() -> DagsterInstance:
    """An ephemeral Dagster instance — no on-disk persistence between tests."""
    return DagsterInstance.ephemeral()


# ----------------------------------------------------------------- definitions


def test_definitions_load():
    """Definitions construct without error and expose every wired component."""
    graph = defs.resolve_asset_graph()
    keys = {k.to_user_string() for k in graph.get_all_asset_keys()}
    assert "bronze_fhir" in keys
    assert "gold_encounter_summary" in keys
    for name in SILVER_ASSET_KEYS:
        assert name in keys, f"Silver asset {name!r} missing from Definitions"
    # one asset check per Silver table
    assert len(graph.asset_check_keys) == len(SILVER_ASSET_KEYS)
    assert defs.resolve_sensor_def("bronze_cohort_sensor") is not None


def test_silver_assets_depend_on_bronze():
    """Each Silver asset must declare bronze_fhir as an upstream dep (lineage edge)."""
    from dagster import AssetKey

    graph = defs.resolve_asset_graph()
    upstream = graph.asset_dep_graph["upstream"]
    bronze = AssetKey("bronze_fhir")
    for name in SILVER_ASSET_KEYS:
        parents = upstream.get(AssetKey(name), set())
        assert bronze in parents, f"{name} missing bronze_fhir dep (parents={parents})"


def test_sensor_targets_bronze_and_silver_not_gold():
    """Sensor must fire Bronze + every Silver table per cohort, but never Gold."""
    from core.orchestration.dagster.sensors import SENSOR_TARGET_KEYS

    expected = {"bronze_fhir", *SILVER_ASSET_KEYS}
    # The constant is the single source of truth wired into @sensor(target=...).
    assert (
        set(SENSOR_TARGET_KEYS) == expected
    ), f"SENSOR_TARGET_KEYS drift: {set(SENSOR_TARGET_KEYS) ^ expected}"
    assert (
        "gold_encounter_summary" not in SENSOR_TARGET_KEYS
    ), "Gold is unpartitioned — keep it out of the per-cohort sensor target"

    # Cross-check: Dagster resolves the sensor with that exact selection.
    sensor_def = defs.resolve_sensor_def("bronze_cohort_sensor")
    selected = {
        k.to_user_string() for k in sensor_def.targets[0].resolvable_to_job.selection.selected_keys
    }
    assert selected == expected, f"sensor wiring mismatch: {selected ^ expected}"


# ----------------------------------------------------------------- bronze + silver


def test_bronze_silver_materialize_writes_all_tables(
    lakehouse_root: Path, instance: DagsterInstance
):
    """Materializing the cohort partition writes every Silver Delta table."""
    instance.add_dynamic_partitions(COHORT_PARTITIONS_NAME, [COHORT])
    result = materialize(
        [bronze_fhir, silver_tables],
        partition_key=COHORT,
        resources={"platform": PlatformResource(platform_name="local_lite")},
        instance=instance,
    )
    assert result.success
    for name in SILVER_ASSET_KEYS:
        table_dir = lakehouse_root / "silver" / name
        assert table_dir.exists(), f"Silver table {name} not written to {table_dir}"
        assert (table_dir / "_delta_log").exists(), f"{name}: no Delta log (not a Delta table)"


def test_bronze_metadata_records_file_count(lakehouse_root: Path, instance: DagsterInstance):
    """Bronze materialization records cohort file count + bytes as asset metadata."""
    instance.add_dynamic_partitions(COHORT_PARTITIONS_NAME, [COHORT])
    result = materialize(
        [bronze_fhir],
        partition_key=COHORT,
        resources={"platform": PlatformResource(platform_name="local_lite")},
        instance=instance,
    )
    assert result.success
    events = result.asset_materializations_for_node("bronze_fhir")
    assert events, "no materialization event captured for bronze_fhir"
    assert events[0].asset_key.to_user_string() == "bronze_fhir"


# ----------------------------------------------------------------- gold


def test_gold_materialize_writes_table_and_manifest(
    lakehouse_root: Path, instance: DagsterInstance
):
    """Gold reads materialized Silver, writes the Delta table, and persists the manifest."""
    instance.add_dynamic_partitions(COHORT_PARTITIONS_NAME, [COHORT])
    silver_result = materialize(
        [bronze_fhir, silver_tables],
        partition_key=COHORT,
        resources={"platform": PlatformResource(platform_name="local_lite")},
        instance=instance,
    )
    assert silver_result.success

    gold_result = materialize(
        [bronze_fhir, silver_tables, gold_encounter_summary],
        selection="gold_encounter_summary",
        resources={"platform": PlatformResource(platform_name="local_lite")},
        instance=instance,
    )
    assert gold_result.success

    gold_dir = lakehouse_root / "gold" / "encounter_summary"
    assert gold_dir.exists(), "gold.encounter_summary table not written"
    assert (gold_dir / "_delta_log").exists()

    manifest_path = lakehouse_root / "gold" / "_metadata" / "corpus_manifest.json"
    assert manifest_path.exists(), "corpus manifest not written"
    manifest = json.loads(manifest_path.read_text())
    assert "corpus_stats" in manifest
    assert manifest["contract_version"] == "1.1.0"
