# ADR-014: Problem-list-as-of-date for active_conditions / active_medications

**Date:** 2026-05-27
**Status:** Accepted
**Amends:** ADR-012 (the conditions/medications *scoping* sub-decision; the encounter grain is unchanged)
**Contract impact:** `gold.encounter_summary` v1.0.0 → **v1.1.0** (MINOR)
**Deciders:** Sandeep Jayaprakash

## Context

ADR-012 v1.0.0 scoped `active_conditions` / `active_medications` to the encounter where the
condition/medication was *recorded* (join on `encounter_id`). Synthea records a condition
once, so this left the corpus extremely sparse: **avg 0.08 conditions and 0.05 medications
per encounter** — most encounters showed an empty problem list even for patients with active
chronic disease. ADR-012 explicitly deferred a "problem-list-as-of-date" enrichment as a
future MINOR. This ADR makes that change, now that it's the chosen next priority for corpus
quality (the corpus feeds Ollama generation, scribe-iq RAG, and clinical-bert).

Inspecting real Coherent FHIR confirmed the temporal fields available (see ADR-013-style
data inspection): `Condition.onsetDateTime` (100%) and `Condition.abatementDateTime` (~32%,
the resolved ones); `MedicationRequest.authoredOn` + `status`, but **no medication stop date**
in FHIR (the STOP is only in the CSV, which is out of scope per ADR-013).

## Decision

Compute both fields as the patient's clinical state **as of each encounter's date**, via a
patient-level join (no longer `encounter_id`-scoped):

- **`active_conditions`** — a condition is active at an encounter when
  `onset_date <= encounter_date` **and** (`abatement_date` is null **or**
  `abatement_date > encounter_date`). A chronic condition recorded once therefore carries
  forward to every later encounter; a resolved condition drops off after its abatement date.
  This required adding **`abatement_date`** to `silver.condition` (from
  `Condition.abatementDateTime`) — an additive Silver column.
- **`active_medications`** — a `status == "active"` medication is active at an encounter when
  it was authored on/before the encounter date. Medications are pre-aggregated to one row per
  `(patient, display)` at their earliest start to keep the patient-level join small.

Both remain `array[string]` of distinct display names — **same schema, changed semantics** →
MINOR contract bump to 1.1.0 (`CONTRACT_VERSION`, regenerated JSON Schema, CORPUS_CONTRACT).

## Consequences

- **Positive:** corpus grounding jumps to **avg 9.57 conditions and 1.66 medications per
  encounter** (from 0.08 / 0.05); only 0.9% of encounters now have an empty problem list
  (was the large majority). The lists are clinically coherent (e.g. Diabetes → diabetic renal
  disease → CKD co-occur). Conditions are temporally correct (onset + abatement gated).
- **Accepted limitation (medications):** FHIR carries no medication stop date, so
  `active_medications` is a forward approximation — a med marked `active` shows from its
  authoring date onward, and a med that was active at a *past* encounter but later stopped is
  not reconstructable from FHIR alone. Documented in CORPUS_CONTRACT. (A precise med timeline
  would need the CSV `STOP` column — deliberately out of scope, ADR-013.)
- **Cost:** the two aggregations become patient-level many-to-many joins (~1.7M condition
  pairs, meds pre-deduped); Gold build stays well under the Silver step. Adding
  `abatement_date` requires a Silver rebuild.
- Many Synthea "conditions" are SDOH/social factors (e.g. "Full-time employment", "Stress")
  modeled as non-abating `Condition` resources, which inflates the average — expected and
  faithful to the source.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|--------|------|------|-------------|
| Problem-list-as-of-date (chosen) | Realistic grounding; temporally correct conditions | Med stop date unavailable in FHIR; needs a Silver column | Best fit; the planned ADR-012 MINOR |
| Keep encounter-recorded scoping (v1.0) | Simplest; no Silver change | Corpus near-empty (0.08/enc); weak grounding | The problem we set out to fix |
| Use CSV `START`/`STOP` for precise med windows | Exact med timeline | Mixes CSV into a FHIR-first lakehouse (contradicts ADR-013) | Out of scope; FHIR-first |
| Current-status snapshot (ignore dates) | Trivial | Not point-in-time; future conditions leak into past encounters | Temporally wrong |
