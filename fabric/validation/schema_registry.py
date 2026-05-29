"""Silver validation rules (Fabric copy of the core registry).

Independent copy under ADR-022 — same rule grammar so a future "one source of
truth" extraction is mechanical, but each platform owns its own validation
config to avoid cross-platform import coupling. When changing a rule, change
both this file and ``core/validation/schema_registry.py`` (or extract to a
shared YAML, future work).

Two Coherent-grounded deviations from the literal spec, identical to core:

1. SOAP completeness checks S/A/P, not S/O/A/P — Coherent's Markdown clinical
   headers have no Objective section, so ``has_objective`` would fail every note.
2. ECG / imaging / genomic / medication / procedure tables can legitimately be
   empty in Coherent (``min_rows = 0``); uniqueness + non-null still apply.

Rule keys:
    min_rows                int    — minimum acceptable row count
    required_non_null       [str]  — columns that must have zero nulls
    unique_keys             [str]  — columns whose values must be unique
    min_char_count          int    — per-row "short text" threshold (soap_note)
    max_short_pct           float  — max fraction allowed below min_char_count
    required_section_flags  [str]  — boolean flag columns expected together
    required_sections_pct   float  — min fraction of rows having all required flags
    numeric_ranges          {col: [lo, hi]} — physiological bounds (non-null rows)
"""

from __future__ import annotations

VALIDATION_RULES: dict[str, dict] = {
    "patient": {
        "min_rows": 100,
        "required_non_null": ["patient_id", "gender"],
        "unique_keys": ["patient_id"],
    },
    "encounter": {
        "min_rows": 100,
        "required_non_null": ["encounter_id", "patient_id"],
        "unique_keys": ["encounter_id"],
    },
    "condition": {
        "min_rows": 50,
        "required_non_null": ["condition_id", "patient_id", "code"],
        "unique_keys": ["condition_id"],
    },
    "observation": {
        "min_rows": 100,
        "required_non_null": ["observation_id", "patient_id", "code"],
        "unique_keys": ["observation_id"],
    },
    "medication_request": {
        "min_rows": 0,
        "required_non_null": ["medication_request_id", "patient_id"],
        "unique_keys": ["medication_request_id"],
    },
    "procedure": {
        "min_rows": 0,
        "required_non_null": ["procedure_id", "patient_id"],
        "unique_keys": ["procedure_id"],
    },
    "soap_note": {
        "min_rows": 50,
        "required_non_null": ["note_id", "patient_id", "note_text"],
        "unique_keys": ["note_id"],
        "min_char_count": 100,
        "max_short_pct": 0.2,
        "required_section_flags": ["has_subjective", "has_assessment", "has_plan"],
        "required_sections_pct": 0.8,
    },
    "ecg_metadata": {
        "min_rows": 0,
        "required_non_null": ["ecg_id", "patient_id"],
        "unique_keys": ["ecg_id"],
        "numeric_ranges": {"heart_rate_bpm": [30, 250]},
    },
    "imaging_study": {
        "min_rows": 0,
        "required_non_null": ["study_id", "patient_id"],
        "unique_keys": ["study_id"],
    },
    "genomic_report": {
        "min_rows": 0,
        "required_non_null": ["report_id", "patient_id", "data_limitation"],
        "unique_keys": ["report_id"],
    },
}
