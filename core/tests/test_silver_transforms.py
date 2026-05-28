"""Tests for the Silver transforms — schema conformance and value mapping.

Builds every Silver table from the parsed synthetic fixture and checks schema
equality, key values, type coercion, and the genomic data_limitation invariant.
"""

from datetime import UTC, date, datetime

import pyarrow as pa
import pytest

from core.transforms import silver_genomics
from core.transforms.registry import SILVER_TABLES

INGEST_TS = datetime(2026, 5, 27, tzinfo=UTC)


@pytest.fixture()
def stamped(parsed):
    """Parsed fixture records with source_file stamped, as the pipeline does."""
    for name in SILVER_TABLES:
        for record in parsed.get(name, []):
            record["source_file"] = "sample_bundle.json"
    return parsed


@pytest.mark.parametrize("table_name", list(SILVER_TABLES))
def test_build_matches_declared_schema(table_name, stamped):
    spec = SILVER_TABLES[table_name]
    table = spec.build(stamped[table_name], INGEST_TS)
    assert table.schema == spec.schema
    # ingest_timestamp populated on every row.
    if table.num_rows:
        assert all(v == INGEST_TS for v in table["ingest_timestamp"].to_pylist())


def test_patient_values_and_types(stamped):
    table = SILVER_TABLES["patient"].build(stamped["patient"], INGEST_TS)
    row = table.to_pylist()[0]
    assert row["patient_id"] == "patient-synthetic-001"
    assert row["birth_date"] == date(1980, 7, 15)
    assert row["deceased"] is False
    assert row["source_file"] == "sample_bundle.json"
    assert pa.types.is_date(table.schema.field("birth_date").type)


def test_observation_components_serialized(stamped):
    table = SILVER_TABLES["observation"].build(stamped["observation"], INGEST_TS)
    bp = next(r for r in table.to_pylist() if r["observation_id"] == "obs-bp")
    assert bp["components_json"] is not None
    assert "8480-6" in bp["components_json"]
    # scalar observation has no components_json
    hr = next(r for r in table.to_pylist() if r["observation_id"] == "obs-vital-hr")
    assert hr["components_json"] is None
    assert hr["value"] == 72.0


def test_soap_note_flags_preserved(stamped):
    table = SILVER_TABLES["soap_note"].build(stamped["soap_note"], INGEST_TS)
    rows = {r["note_id"]: r for r in table.to_pylist()}
    assert rows["docref-002"]["has_objective"] is False  # Coherent-style note
    assert rows["docref-001"]["has_objective"] is True  # classic markers note


def test_genomic_data_limitation_invariant(stamped):
    table = SILVER_TABLES["genomic_report"].build(stamped["genomic_report"], INGEST_TS)
    assert table["data_limitation"].null_count == 0


def test_genomic_build_backfills_missing_limitation():
    """If a record somehow lacks data_limitation, the transform backfills it."""
    record = {"report_id": "g9", "patient_id": "p1", "data_limitation": None}
    table = silver_genomics.build_silver_genomic([record], INGEST_TS)
    assert table.to_pylist()[0]["data_limitation"] == silver_genomics.GENOMIC_DATA_LIMITATION
