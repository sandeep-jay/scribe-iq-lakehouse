"""Tests for LocalLitePlatform — storage paths, Delta write/merge/read, CDC, Bronze read."""

import json
from datetime import UTC, datetime

import pytest
from deltalake import DeltaTable

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
