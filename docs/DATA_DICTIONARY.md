# Data Dictionary — Silver layer

> **Generated file — do not edit by hand.** Regenerate with
> `python scripts/gen_data_dictionary.py` whenever a Silver schema or validation rule
> changes. Source of truth: `local/transforms/registry.py` (schemas) and
> `local/validation/schema_registry.py` (rules). See ADR-009 / ADR-011.

All Silver tables are Delta tables with Change Data Feed enabled
(`delta.enableChangeDataFeed = true`). `source_file` and `ingest_timestamp` are
pipeline-added provenance columns present on every table.

## silver.patient

**Primary key:** `patient_id` · **Columns:** 12

**Validation:** min rows 100; unique `patient_id`

| Column | Type | Required | Notes |
|--------|------|----------|-------|
| `patient_id` | string | ✓ | Primary key |
| `birth_date` | date |  |  |
| `gender` | string | ✓ |  |
| `race` | string |  |  |
| `ethnicity` | string |  |  |
| `state` | string |  |  |
| `city` | string |  |  |
| `zip` | string |  |  |
| `deceased` | boolean |  |  |
| `deceased_date` | timestamp[us, UTC] |  |  |
| `source_file` | string |  | Pipeline provenance — Bronze bundle the row came from |
| `ingest_timestamp` | timestamp[us, UTC] |  | Pipeline provenance — UTC run timestamp |

## silver.encounter

**Primary key:** `encounter_id` · **Columns:** 13

**Validation:** min rows 100; unique `encounter_id`

| Column | Type | Required | Notes |
|--------|------|----------|-------|
| `encounter_id` | string | ✓ | Primary key |
| `patient_id` | string | ✓ |  |
| `type_code` | string |  |  |
| `type_display` | string |  |  |
| `class_code` | string |  |  |
| `start_date` | timestamp[us, UTC] |  |  |
| `end_date` | timestamp[us, UTC] |  |  |
| `status` | string |  |  |
| `provider_id` | string |  |  |
| `reason_code` | string |  |  |
| `reason_display` | string |  |  |
| `source_file` | string |  | Pipeline provenance — Bronze bundle the row came from |
| `ingest_timestamp` | timestamp[us, UTC] |  | Pipeline provenance — UTC run timestamp |

## silver.condition

**Primary key:** `condition_id` · **Columns:** 11

**Validation:** min rows 50; unique `condition_id`

| Column | Type | Required | Notes |
|--------|------|----------|-------|
| `condition_id` | string | ✓ | Primary key |
| `patient_id` | string | ✓ |  |
| `encounter_id` | string |  |  |
| `code` | string | ✓ |  |
| `display` | string |  |  |
| `clinical_status` | string |  |  |
| `onset_date` | timestamp[us, UTC] |  |  |
| `abatement_date` | timestamp[us, UTC] |  |  |
| `recorded_date` | timestamp[us, UTC] |  |  |
| `source_file` | string |  | Pipeline provenance — Bronze bundle the row came from |
| `ingest_timestamp` | timestamp[us, UTC] |  | Pipeline provenance — UTC run timestamp |

## silver.observation

**Primary key:** `observation_id` · **Columns:** 13

**Validation:** min rows 100; unique `observation_id`

| Column | Type | Required | Notes |
|--------|------|----------|-------|
| `observation_id` | string | ✓ | Primary key |
| `patient_id` | string | ✓ |  |
| `encounter_id` | string |  |  |
| `code` | string | ✓ |  |
| `display` | string |  |  |
| `category` | string |  |  |
| `value` | double |  |  |
| `unit` | string |  |  |
| `value_string` | string |  |  |
| `components_json` | string |  | Observation components as JSON, e.g. BP systolic/diastolic (ADR-009) |
| `effective_date` | timestamp[us, UTC] |  |  |
| `source_file` | string |  | Pipeline provenance — Bronze bundle the row came from |
| `ingest_timestamp` | timestamp[us, UTC] |  | Pipeline provenance — UTC run timestamp |

## silver.medication_request

**Primary key:** `medication_request_id` · **Columns:** 11

**Validation:** min rows 0; unique `medication_request_id`

| Column | Type | Required | Notes |
|--------|------|----------|-------|
| `medication_request_id` | string | ✓ | Primary key |
| `patient_id` | string | ✓ |  |
| `encounter_id` | string |  |  |
| `code` | string |  |  |
| `display` | string |  |  |
| `status` | string |  |  |
| `intent` | string |  |  |
| `authored_on` | timestamp[us, UTC] |  |  |
| `dosage_text` | string |  |  |
| `source_file` | string |  | Pipeline provenance — Bronze bundle the row came from |
| `ingest_timestamp` | timestamp[us, UTC] |  | Pipeline provenance — UTC run timestamp |

## silver.procedure

**Primary key:** `procedure_id` · **Columns:** 10

**Validation:** min rows 0; unique `procedure_id`

| Column | Type | Required | Notes |
|--------|------|----------|-------|
| `procedure_id` | string | ✓ | Primary key |
| `patient_id` | string | ✓ |  |
| `encounter_id` | string |  |  |
| `code` | string |  |  |
| `display` | string |  |  |
| `status` | string |  |  |
| `performed_start` | timestamp[us, UTC] |  |  |
| `performed_end` | timestamp[us, UTC] |  |  |
| `source_file` | string |  | Pipeline provenance — Bronze bundle the row came from |
| `ingest_timestamp` | timestamp[us, UTC] |  | Pipeline provenance — UTC run timestamp |

## silver.soap_note

**Primary key:** `note_id` · **Columns:** 14

**Validation:** min rows 50; unique `note_id`; ≥80% rows with [has_subjective, has_assessment, has_plan]; ≤20% notes under 100 chars

| Column | Type | Required | Notes |
|--------|------|----------|-------|
| `note_id` | string | ✓ | Primary key |
| `patient_id` | string | ✓ |  |
| `encounter_id` | string |  |  |
| `note_date` | timestamp[us, UTC] |  |  |
| `note_text` | string | ✓ |  |
| `has_subjective` | boolean |  |  |
| `has_objective` | boolean |  |  |
| `has_assessment` | boolean |  |  |
| `has_plan` | boolean |  |  |
| `char_count` | int32 |  |  |
| `word_count` | int32 |  |  |
| `binary_id` | string |  |  |
| `source_file` | string |  | Pipeline provenance — Bronze bundle the row came from |
| `ingest_timestamp` | timestamp[us, UTC] |  | Pipeline provenance — UTC run timestamp |

## silver.ecg_metadata

**Primary key:** `ecg_id` · **Columns:** 14

**Validation:** min rows 0; unique `ecg_id`; `heart_rate_bpm` in [30, 250]

| Column | Type | Required | Notes |
|--------|------|----------|-------|
| `ecg_id` | string | ✓ | Primary key |
| `patient_id` | string | ✓ |  |
| `encounter_id` | string |  |  |
| `report_date` | timestamp[us, UTC] |  |  |
| `status` | string |  |  |
| `conclusion` | string |  |  |
| `rhythm` | string |  |  |
| `heart_rate_bpm` | int32 |  |  |
| `pr_interval_ms` | int32 |  |  |
| `qrs_duration_ms` | int32 |  |  |
| `has_waveform` | boolean |  |  |
| `waveform_binary_id` | string |  |  |
| `source_file` | string |  | Pipeline provenance — Bronze bundle the row came from |
| `ingest_timestamp` | timestamp[us, UTC] |  | Pipeline provenance — UTC run timestamp |

## silver.imaging_study

**Primary key:** `study_id` · **Columns:** 22

**Validation:** min rows 0; unique `study_id`

| Column | Type | Required | Notes |
|--------|------|----------|-------|
| `study_id` | string | ✓ | Primary key |
| `patient_id` | string | ✓ |  |
| `encounter_id` | string |  |  |
| `started_date` | timestamp[us, UTC] |  |  |
| `status` | string |  |  |
| `modality` | string |  |  |
| `body_site` | string |  |  |
| `body_site_display` | string |  |  |
| `series_count` | int32 |  |  |
| `instance_count` | int32 |  |  |
| `study_description` | string |  |  |
| `series_description` | string |  |  |
| `study_date` | date |  |  |
| `manufacturer` | string |  |  |
| `magnetic_field_strength` | double |  |  |
| `slice_thickness_mm` | double |  |  |
| `rows` | int32 |  |  |
| `columns` | int32 |  |  |
| `dicom_binary_id` | string |  |  |
| `dicom_extracted` | boolean |  |  |
| `source_file` | string |  | Pipeline provenance — Bronze bundle the row came from |
| `ingest_timestamp` | timestamp[us, UTC] |  | Pipeline provenance — UTC run timestamp |

## silver.genomic_report

**Primary key:** `report_id` · **Columns:** 13

**Validation:** min rows 0; unique `report_id`

| Column | Type | Required | Notes |
|--------|------|----------|-------|
| `report_id` | string | ✓ | Primary key |
| `patient_id` | string | ✓ |  |
| `encounter_id` | string |  |  |
| `report_date` | timestamp[us, UTC] |  |  |
| `status` | string |  |  |
| `gene_panel_name` | string |  |  |
| `result_summary` | string |  |  |
| `has_pathogenic_variant` | boolean |  |  |
| `family_history_flag` | boolean |  |  |
| `binary_id` | string |  |  |
| `data_limitation` | string | ✓ | Always populated — Synthea inheritance limitation note (ADR-007) |
| `source_file` | string |  | Pipeline provenance — Bronze bundle the row came from |
| `ingest_timestamp` | timestamp[us, UTC] |  | Pipeline provenance — UTC run timestamp |

## silver.ingest_log (validation audit)

**Columns:** 5

| Column | Type | Required | Notes |
|--------|------|----------|-------|
| `table` | string |  |  |
| `row_count` | int64 |  |  |
| `passed` | boolean |  |  |
| `failed_checks` | string |  |  |
| `ingest_timestamp` | timestamp[us, UTC] |  | Pipeline provenance — UTC run timestamp |
