"""Validation rules per Silver table (spec §5.6, adjusted to Coherent reality).

Two deviations from the spec's literal rules, both grounded in what the real Synthea
Coherent FHIR actually contains (see ADR-005 / HANDOFF discoveries):

1. SOAP completeness checks the S/A/P flags, not all four. Coherent notes use Markdown
   clinical headers and have no Objective section, so requiring ``has_objective`` would
   fail every note. ``required_section_flags`` therefore omits objective.
2. ECG and genomic reports live outside the FHIR bundles (ECG waveforms as Binary;
   genomics in the S3 ``dna/`` prefix), so their tables are frequently empty. Their
   ``min_rows`` is 0 — presence is optional, but uniqueness/non-null still apply when rows exist.

Each rule dict may contain:
    min_rows                int    — minimum acceptable row count
    required_non_null       [str]  — columns that must have zero nulls
    unique_keys             [str]  — columns whose values must be unique
    min_char_count          int    — per-row threshold for "short" text (soap_note)
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
        # Coherent notes lack an Objective section — see module docstring / ADR-005.
        "required_section_flags": ["has_subjective", "has_assessment", "has_plan"],
        "required_sections_pct": 0.8,
    },
    "ecg_metadata": {
        "min_rows": 0,  # ECG reports are sparse/absent in Coherent FHIR
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
        "min_rows": 0,  # genomics live in the S3 dna/ prefix, not FHIR
        "required_non_null": ["report_id", "patient_id", "data_limitation"],
        "unique_keys": ["report_id"],
    },
}
