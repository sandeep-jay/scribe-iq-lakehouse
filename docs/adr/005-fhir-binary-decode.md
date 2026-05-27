# ADR-005: FHIR Binary Base64 decode for SOAP notes

**Date:** 2026-05-27
**Status:** Accepted
**Deciders:** Sandeep Jayaprakash

## Context

Synthea Coherent embeds clinical SOAP notes inside FHIR Binary resources, encoded
in Base64. The link from patient/encounter to note goes through DocumentReference.
An alternative interpretation was that notes might be in separate text files or
accessible via a different FHIR resource type. Investigation of the Synthea Coherent
dataset confirmed the Binary/DocumentReference pattern.

## Decision

Decode SOAP notes from FHIR Binary resources using Base64 decode in fhir_parser.py.
Link notes to encounters via DocumentReference (subject → patient_id, context.encounter
→ encounter_id). Apply heuristic section detection (S/O/A/P markers) to populate
has_subjective, has_objective, has_assessment, has_plan boolean flags. Document the
heuristic nature of section detection in code and data dictionary.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|--------|------|------|-------------|
| Separate text file extraction | Familiar file I/O | Notes are not in separate files | Not how Synthea Coherent works |
| CCDA extraction | Standards-based | Synthea uses Binary, not CCDA here | Wrong resource type |
| Current approach (Binary decode) | Correct for this dataset | Heuristic section detection | Only correct approach |

## Consequences

**Positive:**
- Captures all ~800-1,000 SOAP notes available in Synthea Coherent
- Decoded text is the primary grounding anchor for Ollama generation
- Section detection provides structure for downstream NLP tasks

**Negative:**
- Section detection is heuristic — false positive/negative rate not measured
- Synthea SOAP notes are template-driven — less linguistic variety than real notes

**Neutral:**
- binary_id column preserved in silver.soap_note for full traceability to source

## Implementation notes
- `local/transforms/fhir_parser.py` — extract_soap_note() method
- `local/transforms/silver_soap_notes.py` — Silver table writer
- `fabric/notebooks/05_silver_soap_notes.ipynb` — streaming extraction with Auto Loader
- SOAP section markers: "SUBJECTIVE:", "OBJECTIVE:", "ASSESSMENT:", "PLAN:" (case-insensitive)
- `tests/test_silver_soap_notes.py` — test Base64 decode, section detection, null handling
