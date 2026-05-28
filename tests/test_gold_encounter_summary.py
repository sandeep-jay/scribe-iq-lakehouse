"""Tests for the Gold encounter_summary transform, corpus manifest, and contract.

Builds the Silver tables from the synthetic fixture, denormalizes to Gold, and checks:
schema conformance, encounter grain, age-at-encounter, vitals (incl. BP parsed from
components_json), lab/condition/medication aggregation, null-safe optional context,
deterministic summary_id, the manifest stats, and that the committed JSON Schema +
contract field lists stay in sync with GOLD_SCHEMA (generated-first, ADR-011).
"""

from datetime import UTC, date, datetime

import pyarrow as pa
import pytest

from local.gold.corpus_manifest import build_corpus_manifest
from local.gold.encounter_summary import (
    CONTRACT_VERSION,
    GOLD_SCHEMA,
    OPTIONAL_FIELDS,
    REQUIRED_FIELDS,
    SILVER_SOURCES,
    build_encounter_summary,
)
from local.transforms.registry import SILVER_TABLES
from scripts.gen_corpus_schema import OUTPUT_PATH, _serialize, render_schema

INGEST_TS = datetime(2026, 5, 27, tzinfo=UTC)
CREATED_TS = datetime(2026, 5, 27, 12, 0, tzinfo=UTC)


@pytest.fixture()
def silver(parsed) -> dict[str, pa.Table]:
    """All Silver tables built from the parsed fixture, source_file stamped."""
    tables = {}
    for name, spec in SILVER_TABLES.items():
        for record in parsed.get(name, []):
            record["source_file"] = "sample_bundle.json"
        tables[name] = spec.build(parsed.get(name, []), INGEST_TS)
    return tables


@pytest.fixture()
def gold(silver) -> pa.Table:
    """The Gold encounter_summary built from the fixture's Silver tables."""
    return build_encounter_summary(silver, created_ts=CREATED_TS)


def _row(gold: pa.Table, encounter_id: str) -> dict:
    return next(r for r in gold.to_pylist() if r["encounter_id"] == encounter_id)


# --------------------------------------------------------------- schema & grain


def test_schema_matches_declared(gold):
    assert gold.schema == GOLD_SCHEMA


def test_one_row_per_encounter(silver, gold):
    assert gold.num_rows == silver["encounter"].num_rows == 2


def test_summary_id_deterministic_and_unique(silver):
    first = build_encounter_summary(silver, created_ts=CREATED_TS)
    second = build_encounter_summary(silver, created_ts=datetime(2030, 1, 1, tzinfo=UTC))
    ids_a = first.column("summary_id").to_pylist()
    ids_b = second.column("summary_id").to_pylist()
    assert ids_a == ids_b, "summary_id must be stable across rebuilds"
    assert len(set(ids_a)) == len(ids_a), "summary_id must be unique per encounter"


# --------------------------------------------------------------- populated row


def test_full_context_encounter(gold):
    row = _row(gold, "encounter-001")
    assert row["patient_id"] == "patient-synthetic-001"
    assert row["patient_gender"] == "female"
    assert row["encounter_date"] == date(2021, 3, 15)
    assert row["encounter_type"] == "Encounter for check up (procedure)"
    assert row["active_conditions"] == ["Essential hypertension (disorder)"]
    assert row["active_medications"] == ["Aspirin 81 MG Oral Tablet"]
    assert row["procedures"] == ["Blood pressure taking (procedure)"]


def test_age_at_encounter(gold):
    # born 1980-07-15; encounter 2021-03-15 -> birthday not yet reached -> 40
    assert _row(gold, "encounter-001")["patient_age"] == 40


def test_vitals_struct_with_bp_from_components(gold):
    vitals = _row(gold, "encounter-001")["recent_vitals"]
    assert vitals["heart_rate"] == 72.0
    assert vitals["bp_systolic"] == 154.0
    assert vitals["bp_diastolic"] == 85.0
    assert vitals["o2_saturation"] is None  # not present in fixture


def test_labs_list(gold):
    labs = _row(gold, "encounter-001")["recent_labs"]
    assert {"name": "Glucose", "value": 95.0, "unit": "mg/dL"} in labs


def test_optional_context_flags(gold):
    row = _row(gold, "encounter-001")
    assert row["has_ecg"] is True
    assert row["ecg_rhythm"] == "Normal sinus rhythm"
    assert row["imaging"]["has_imaging"] is True
    assert row["imaging"]["modality"] == "MR"
    assert row["has_genomics"] is True
    assert row["soap_note_text"] is not None
    assert row["soap_note_id"] == "docref-001"


def test_silver_versions_struct_fields(gold):
    versions = _row(gold, "encounter-001")["silver_versions"]
    assert set(versions) == set(SILVER_SOURCES)


# ----------------------------------------------- as-of-date problem list (ADR-014)


def test_problem_list_carries_forward(gold):
    # Hypertension (onset 2019, no abatement) was recorded at encounter-001 but is active
    # as of encounter-002 too — so it carries forward (ADR-014), unlike the old
    # encounter-only join which left encounter-002 empty.
    row = _row(gold, "encounter-002")
    assert row["active_conditions"] == ["Essential hypertension (disorder)"]
    assert row["active_medications"] == ["Aspirin 81 MG Oral Tablet"]


def test_sparse_optional_context_still_null_safe(gold):
    # encounter-002 carries the problem list but has no labs/procedures/ecg/imaging/genomics.
    row = _row(gold, "encounter-002")
    assert row["recent_labs"] == []
    assert row["procedures"] == []
    assert row["soap_note_id"] == "docref-002"
    assert row["has_ecg"] is False
    assert row["imaging"]["has_imaging"] is False
    assert row["has_genomics"] is False
    # recent_vitals struct is always present; members are null when absent
    assert row["recent_vitals"]["heart_rate"] is None


def test_as_of_date_onset_and_abatement_gates():
    # Synthetic Silver: one patient, two encounters; a chronic condition, one that resolves
    # between them, and one not yet onset at the first encounter; one active + one stopped med.
    enc_rows = [
        {"encounter_id": "e1", "patient_id": "p", "start_date": "2021-01-01", "type_display": "x"},
        {"encounter_id": "e2", "patient_id": "p", "start_date": "2021-12-31", "type_display": "x"},
    ]
    cond_rows = [
        {
            "condition_id": "c1",
            "patient_id": "p",
            "display": "Chronic dz",
            "onset_date": "2019-01-01",
            "abatement_date": None,
        },
        {
            "condition_id": "c2",
            "patient_id": "p",
            "display": "Acute dz",
            "onset_date": "2020-06-01",
            "abatement_date": "2021-06-01",
        },
        {
            "condition_id": "c3",
            "patient_id": "p",
            "display": "Later dz",
            "onset_date": "2021-09-01",
            "abatement_date": None,
        },
    ]
    med_rows = [
        {
            "medication_request_id": "m1",
            "patient_id": "p",
            "display": "Drug A",
            "status": "active",
            "authored_on": "2021-03-01",
        },
        {
            "medication_request_id": "m2",
            "patient_id": "p",
            "display": "Drug B",
            "status": "stopped",
            "authored_on": "2020-01-01",
        },
    ]
    silver = {name: spec.build([], INGEST_TS) for name, spec in SILVER_TABLES.items()}
    silver["encounter"] = SILVER_TABLES["encounter"].build(enc_rows, INGEST_TS)
    silver["patient"] = SILVER_TABLES["patient"].build(
        [{"patient_id": "p", "gender": "female"}], INGEST_TS
    )
    silver["condition"] = SILVER_TABLES["condition"].build(cond_rows, INGEST_TS)
    silver["medication_request"] = SILVER_TABLES["medication_request"].build(med_rows, INGEST_TS)

    g = {
        r["encounter_id"]: r
        for r in build_encounter_summary(silver, created_ts=CREATED_TS).to_pylist()
    }
    # e1 (2021-01-01): chronic active; acute active (abates 2021-06, later); future not yet onset.
    assert g["e1"]["active_conditions"] == ["Acute dz", "Chronic dz"]
    # e2 (2021-12-31): chronic active; acute resolved (abated 2021-06-01); future now onset.
    assert g["e2"]["active_conditions"] == ["Chronic dz", "Later dz"]
    # Drug A authored 2021-03-01: not yet started at e1, active by e2. Drug B stopped -> excluded.
    assert g["e1"]["active_medications"] == []
    assert g["e2"]["active_medications"] == ["Drug A"]


def test_required_fields_present_and_non_null(gold):
    # The non-list required value fields are guaranteed non-null for every row.
    guaranteed = ("summary_id", "patient_id", "encounter_id", "encounter_type")
    for row in gold.to_pylist():
        for field in guaranteed:
            assert row[field] is not None, f"{field} unexpectedly null"


def test_empty_silver_yields_empty_gold_with_schema(silver):
    empty = {name: table.slice(0, 0) for name, table in silver.items()}
    table = build_encounter_summary(empty, created_ts=CREATED_TS)
    assert table.num_rows == 0
    assert table.schema == GOLD_SCHEMA


# --------------------------------------------------------------- corpus manifest


def test_corpus_manifest_stats(silver, gold):
    counts = {name: table.num_rows for name, table in silver.items()}
    manifest = build_corpus_manifest(
        gold,
        silver_counts=counts,
        created_ts=CREATED_TS,
        platform_name="local_lite",
        silver_versions={"patient": 7},
    )
    assert manifest["contract_version"] == CONTRACT_VERSION
    assert manifest["row_count"] == 2
    stats = manifest["corpus_stats"]
    assert stats["encounters"] == 2
    assert stats["distinct_patients"] == 1
    assert stats["with_soap_note"] == 2
    assert stats["with_ecg"] == 1
    assert stats["with_imaging"] == 1
    assert stats["with_genomics"] == 1
    sources = {s["table"]: s for s in manifest["silver_sources"]}
    assert sources["silver.patient"]["delta_version"] == 7
    assert sources["silver.observation"]["row_count"] == counts["observation"]


# --------------------------------------------------------------- contract guards


def test_contract_field_lists_cover_schema():
    assert set(REQUIRED_FIELDS).isdisjoint(OPTIONAL_FIELDS)
    assert set(REQUIRED_FIELDS) | set(OPTIONAL_FIELDS) == set(GOLD_SCHEMA.names)


def test_json_schema_is_current():
    assert OUTPUT_PATH.exists(), "run scripts/gen_corpus_schema.py"
    assert OUTPUT_PATH.read_text() == _serialize(
        render_schema()
    ), "schemas/gold_encounter_summary.json is stale — run: python scripts/gen_corpus_schema.py"


def test_json_schema_marks_required_and_versioned():
    schema = render_schema()
    assert schema["required"] == list(REQUIRED_FIELDS)
    assert schema["x-contract-version"] == CONTRACT_VERSION
    assert set(schema["properties"]) == set(GOLD_SCHEMA.names)


def test_gold_built_corpus_validates_against_json_schema(gold):
    """Every built row conforms to the published JSON Schema contract."""
    jsonschema = pytest.importorskip("jsonschema")
    schema = render_schema()
    validator = jsonschema.Draft202012Validator(schema)
    for row in gold.to_pylist():
        instance = _json_safe(row)
        errors = sorted(validator.iter_errors(instance), key=str)
        assert not errors, f"{row['encounter_id']}: {[e.message for e in errors]}"


def _json_safe(value):
    """Coerce datetimes/dates to ISO strings so a row can be JSON-Schema validated."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value
