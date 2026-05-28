"""Tests for LocalLitePlatform — storage paths, Delta write/merge/read, CDC, Bronze read."""

import json
from datetime import UTC, datetime

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from core.platform.local_lite import LocalLitePlatform

INGEST_TS = datetime(2026, 5, 27, tzinfo=UTC)


def _patient_table(rows):
    from core.transforms.silver_patient import build_silver_patient

    return build_silver_patient(rows, INGEST_TS)


def test_storage_path(tmp_path):
    pf = LocalLitePlatform(root=tmp_path)
    assert pf.storage_path("silver", "patient") == str(tmp_path / "silver" / "patient")


def test_storage_path_rejects_bad_layer(tmp_path):
    pf = LocalLitePlatform(root=tmp_path)
    with pytest.raises(ValueError, match="Unknown layer"):
        pf.storage_path("platinum", "patient")


def test_write_creates_table_with_cdc_enabled(tmp_path):
    pf = LocalLitePlatform(root=tmp_path)
    pf.write_silver("patient", _patient_table([{"patient_id": "p1", "source_file": "f"}]))
    dt = DeltaTable(pf.storage_path("silver", "patient"))
    assert dt.metadata().configuration.get("delta.enableChangeDataFeed") == "true"


def test_merge_upserts_on_primary_key(tmp_path):
    pf = LocalLitePlatform(root=tmp_path)
    pf.write_silver(
        "patient", _patient_table([{"patient_id": "p1", "gender": "female", "source_file": "f"}])
    )
    # Re-merge same key with a changed field + a new key.
    pf.write_silver(
        "patient",
        _patient_table(
            [
                {"patient_id": "p1", "gender": "male", "source_file": "f"},
                {"patient_id": "p2", "gender": "female", "source_file": "f"},
            ]
        ),
        mode="merge",
    )
    rows = {r["patient_id"]: r["gender"] for r in pf.read_silver("patient").to_pylist()}
    assert rows == {"p1": "male", "p2": "female"}  # upsert, not duplicate


def test_read_bronze_fhir_by_cohort(tmp_path):
    fhir = tmp_path / "bronze" / "fhir" / "cohort=A"
    fhir.mkdir(parents=True)
    (fhir / "p1.json").write_text(json.dumps({"resourceType": "Bundle", "id": "b1"}))
    (fhir / "bad.json").write_text("{not json")
    pf = LocalLitePlatform(root=tmp_path)
    bundles = pf.read_bronze_fhir(cohort="A")
    assert len(bundles) == 1  # bad file skipped
    assert bundles[0]["id"] == "b1"


def test_get_spark_session_is_none(tmp_path):
    assert LocalLitePlatform(root=tmp_path).get_spark_session() is None


def test_merge_dedupes_target_with_legacy_duplicates(tmp_path):
    """ADR-019: MERGE on a table with pre-existing target-side dups must dedupe + succeed.

    Simulates the failure mode from Session 4: a legacy Silver table written via
    OVERWRITE before dedup_by_key() landed in every build_silver_* contains
    duplicate primary keys. Calling write_silver(..., mode="merge") used to fail
    with delta-rs's "matched a target row with multiple source rows" — now the
    pre-merge guard rewrites the deduped target first.
    """
    pf = LocalLitePlatform(root=tmp_path)
    path = pf.storage_path("silver", "patient")

    # Legacy state: simulate target dups by concatenating two single-row builds
    # (each row clean coming out of build_silver_patient, but the union has two
    # rows with patient_id="p1" — last one is the "male" version).
    p1_female = _patient_table([{"patient_id": "p1", "gender": "female", "source_file": "f1"}])
    p1_male = _patient_table([{"patient_id": "p1", "gender": "male", "source_file": "f2"}])
    p2 = _patient_table([{"patient_id": "p2", "gender": "female", "source_file": "f3"}])
    dup_table = pa.concat_tables([p1_female, p1_male, p2])
    write_deltalake(path, dup_table, mode="overwrite")
    assert DeltaTable(path).to_pyarrow_table().num_rows == 3  # confirm dup present

    # Cohort re-fires: source includes p1 with its current canonical value plus a
    # new p3. The target gets deduped (some p1 survivor wins — doesn't matter
    # which because the MERGE then overwrites it with the source's p1). p3 is
    # inserted. p2 is untouched by both dedup and MERGE.
    pf.write_silver(
        "patient",
        _patient_table(
            [
                {"patient_id": "p1", "gender": "nonbinary", "source_file": "f5"},
                {"patient_id": "p3", "gender": "male", "source_file": "f4"},
            ]
        ),
        mode="merge",
    )

    rows = {r["patient_id"]: r["gender"] for r in pf.read_silver("patient").to_pylist()}
    assert rows == {"p1": "nonbinary", "p2": "female", "p3": "male"}
