# Corpus Contract — `gold.encounter_summary`

**Contract version:** `1.1.0` &nbsp;·&nbsp; **Status:** Active &nbsp;·&nbsp; **Spec:** §5.4 / §5.7

This is the handoff interface between the lakehouse and its downstream AI consumers:

- **Ollama generation pipeline** — grounds synthetic clinical dialogue on each summary.
- **scribe-iq** (RAG) — replaces the 19-patient dev corpus with this 1,278-patient corpus.
- **clinical-bert-pipeline** (NLP) — consumes `soap_note_text` + structured labels.

One row = one clinical encounter. The machine-readable schema is
[`schemas/gold_encounter_summary.json`](../schemas/gold_encounter_summary.json) (JSON
Schema, Draft 2020-12), **generated** from the Arrow `GOLD_SCHEMA` in
[`local/gold/encounter_summary.py`](../local/gold/encounter_summary.py) by
`scripts/gen_corpus_schema.py` (ADR-011, ADR-012). This document is the human-readable
companion; on any disagreement, the generated JSON Schema and the code win.

---

## Guarantee

### Required fields — always present for every encounter

| Field | Type | Notes |
|-------|------|-------|
| `summary_id` | string | Deterministic UUIDv5 of `encounter_id` — stable across rebuilds |
| `patient_id` | string | FK to `silver.patient` |
| `encounter_id` | string | FK to `silver.encounter`; the grain key |
| `patient_age` | integer | Anniversary-based age at the encounter date (may be null if birth date missing) |
| `patient_gender` | string | |
| `encounter_type` | string | `silver.encounter.type_display` (may be empty string) |
| `encounter_date` | date | Encounter start date (may be null if the source timestamp is absent) |
| `active_conditions` | array[string] | Patient problem list **active as of the encounter date** — onset ≤ date and not yet abated (ADR-014). Distinct display names; may be empty |
| `active_medications` | array[string] | Active medications **as of the encounter date** — `status=active`, authored ≤ date (ADR-014). Distinct display names; may be empty |

"Present" means the key always exists. List fields are never null — they are `[]` when
empty. `patient_age` / `encounter_date` are present but may be null when the underlying
source value is missing.

### Optional fields — present only when the data exists

| Field | Type | Absent value |
|-------|------|--------------|
| `recent_vitals` | struct{heart_rate, bp_systolic, bp_diastolic, temperature, o2_saturation} | struct present; members null |
| `recent_labs` | array[struct{name, value, unit}] | `[]` |
| `procedures` | array[string] | `[]` |
| `soap_note_text` | string | `null` |
| `soap_note_id` | string | `null` |
| `ecg_finding` / `ecg_rhythm` | string | `null` |
| `has_ecg` | boolean | `false` |
| `imaging` | struct{has_imaging, modality, body_site_display, study_description, study_date, series_count, dicom_binary_id} | `has_imaging=false`, members null |
| `has_genomics` | boolean | `false` |
| `genomic_summary` | string | `null` |
| `created_timestamp` | timestamp | always present (build time, UTC) |
| `silver_versions` | struct[10×int] | Delta version of each Silver source at build time (lineage); members null if unavailable |

**Consumer rule:** handle null/empty optional fields gracefully.
- A summary **with** `soap_note_text` uses it as the **primary** generation anchor.
- A summary **without** `soap_note_text` uses the structured fields only for grounding.

`recent_vitals` and `imaging` are always-present structs (never null at the top level) so
consumers can read members without a null guard on the struct itself; check `imaging.has_imaging`
and individual vital members for presence.

---

## What the corpus looks like (full run, 2026-05-27)

Built from the entire Synthea Coherent dataset (1,278 patients). See
[BENCHMARKS.md](BENCHMARKS.md) for timings.

| Metric | Value |
|--------|-------|
| Encounters (rows) | 143,946 |
| Distinct patients | 1,278 |
| With SOAP note | 143,946 (100%) |
| With labs | 26,059 |
| With vitals | 19,830 |
| With imaging | 3,752 (298 with DICOM headers) |
| With genomics | 419 |
| With ECG | 0 |
| Avg conditions / encounter | 9.57 (as-of-date problem list, ADR-014) |
| Avg medications / encounter | 1.66 (as-of-date, ADR-014) |
| Encounters with empty problem list | 0.9% |

### Known limitations (honest, not hidden)

1. **Medications are a forward `status=active` approximation, not a point-in-time timeline.**
   As of v1.1.0 (ADR-014) `active_medications` carries forward any `status=active` med from
   its authoring date — good for chronic/ongoing meds. But FHIR has no medication *stop* date,
   so a med that was active at a *past* encounter and later stopped cannot be reconstructed:
   it won't appear on those past encounters. Conditions, by contrast, are temporally precise
   (onset + abatement gated). A precise med timeline would require the CSV `STOP` column,
   deliberately out of scope (ADR-013).
2. **`has_ecg` is always false.** Coherent has no ECG `DiagnosticReport`s in the FHIR
   bundles (ECG is Binary waveform data, roadmap Phase 3). The fields exist for forward
   compatibility.
3. **Genomics is synthetic.** `genomic_summary` derives from `silver.genomic_report`,
   which carries the mandatory `data_limitation` note (ADR-007): Synthea simulated
   inheritance, not clinical variants. No pathogenic variants are real.
4. **Vitals coverage is partial** (~14% of encounters) — only encounters with vital-sign
   observations linked to them populate `recent_vitals`. Blood pressure is parsed from the
   Silver `components_json` (systolic LOINC 8480-6 / diastolic 8462-4).
5. **`imaging.study_description` is null even when `has_imaging` is true.** Imaging metadata
   comes from FHIR (modality, body site, series count) for all 3,752 studies; the 298 with a
   downloaded DICOM file additionally carry `study_date` (and Silver-level dimensions /
   slice thickness). But Coherent's DICOM descriptive tags are placeholder `"UNKNOWN"`, which
   we normalize to null (ADR-013) — so there is no human-readable study description to ground
   on. Use `modality` + `body_site_display` for imaging grounding.

---

## Versioning policy

`contract_version` follows semver and is embedded in the JSON Schema (`x-contract-version`)
and every corpus manifest.

- **PATCH** — additive notes / docs only, no schema change.
- **MINOR** — new optional field, or a new value in an existing field. Backward-compatible.
- **MAJOR** — remove/rename a field, change a type, or change the meaning of a required
  field. **Breaking** — `scribe-iq` and `clinical-bert-pipeline` pin against the major
  version and must be updated in lockstep.

A `tests/test_gold_encounter_summary.py` contract test fails if the generated JSON Schema,
the `REQUIRED_FIELDS`/`OPTIONAL_FIELDS` lists, and `GOLD_SCHEMA` ever fall out of sync — so
a breaking change cannot land silently without a version bump.

## Lineage

Each build writes `gold/_metadata/corpus_manifest.json` (see
[`local/gold/corpus_manifest.py`](../local/gold/corpus_manifest.py)) recording the
contract version, row count, per-Silver-table row counts and Delta versions, the platform,
and the coverage statistics above — the provenance record handed to consumers alongside
this contract.
