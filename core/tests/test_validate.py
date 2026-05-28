"""Tests for the validation layer — checks fire correctly and ingest_log builds."""

from datetime import UTC, datetime

import pyarrow as pa

from core.validation.validate import (
    INGEST_LOG_SCHEMA,
    results_to_arrow,
    validate_table,
)

INGEST_TS = datetime(2026, 5, 27, tzinfo=UTC)


def _table(cols):
    return pa.table(cols)


def test_passes_when_rules_met():
    data = _table({"patient_id": ["a", "b", "c"]})
    rules = {"min_rows": 2, "required_non_null": ["patient_id"], "unique_keys": ["patient_id"]}
    result = validate_table("patient", data, rules)
    assert result.passed
    assert result.failed_checks == []


def test_min_rows_failure():
    result = validate_table("patient", _table({"patient_id": ["a"]}), {"min_rows": 5})
    assert not result.passed
    assert any("min_rows" in c for c in result.failed_checks)


def test_null_and_uniqueness_failures():
    data = _table({"patient_id": ["a", "a", None]})
    rules = {"required_non_null": ["patient_id"], "unique_keys": ["patient_id"]}
    result = validate_table("patient", data, rules)
    assert not result.passed
    assert any("nulls_in:patient_id" in c for c in result.failed_checks)
    assert any("non_unique:patient_id" in c for c in result.failed_checks)


def test_soap_short_notes_and_sections():
    data = _table(
        {
            "note_id": ["1", "2"],
            "char_count": [10, 20],  # both below 100 -> 100% short
            "has_subjective": [True, False],
            "has_assessment": [True, False],
            "has_plan": [True, False],  # only 1/2 complete -> 0.5 < 0.8
        }
    )
    rules = {
        "min_char_count": 100,
        "max_short_pct": 0.2,
        "required_section_flags": ["has_subjective", "has_assessment", "has_plan"],
        "required_sections_pct": 0.8,
    }
    result = validate_table("soap_note", data, rules)
    assert not result.passed
    assert any("short_notes_pct" in c for c in result.failed_checks)
    assert any("sections_pct" in c for c in result.failed_checks)


def test_numeric_range_failure():
    data = _table({"ecg_id": ["1", "2"], "heart_rate_bpm": [70, 400]})
    rules = {"numeric_ranges": {"heart_rate_bpm": [30, 250]}}
    result = validate_table("ecg_metadata", data, rules)
    assert not result.passed
    assert any("out_of_range:heart_rate_bpm" in c for c in result.failed_checks)


def test_numeric_range_ignores_nulls():
    data = _table({"ecg_id": ["1", "2"], "heart_rate_bpm": [70, None]})
    rules = {"numeric_ranges": {"heart_rate_bpm": [30, 250]}}
    assert validate_table("ecg_metadata", data, rules).passed


def test_results_to_arrow_schema():
    results = [validate_table("patient", _table({"patient_id": ["a"]}), {"min_rows": 1})]
    log = results_to_arrow(results, INGEST_TS)
    assert log.schema == INGEST_LOG_SCHEMA
    assert log.to_pylist()[0]["table"] == "patient"
    assert log.to_pylist()[0]["passed"] is True
