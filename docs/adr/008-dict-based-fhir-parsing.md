# ADR-008: Dict-based FHIR parsing (not fhir.resources models)

**Date:** 2026-05-27
**Status:** Accepted
**Deciders:** Sandeep Jayaprakash

## Context

The spec (section 5.3) sketches `FHIRBundleParser` using the `fhir.resources` library
(`from fhir.resources.bundle import Bundle`) with attribute access on typed Pydantic
models (e.g. `binary_resource.data`). While implementing Session 1, two project rules
constrained that approach:

1. `.claude/rules/transforms.md` and the healthcare-data skill mandate **`.get()` with a
   default for every FHIR field — never direct key/attribute access** — because Synthea
   bundles have optional fields everywhere.
2. Transforms in `local/transforms/` must stay **pure and dependency-light** (ADR-002):
   they are imported into Fabric Spark UDFs, where a heavy Pydantic dependency and strict
   model validation are friction.

Inspecting a real Coherent bundle (`Al123_Medhurst46_*.json`, 815 KB, 333 entries)
surfaced concrete mismatches with strict typed models:

- References use the `urn:uuid:<id>` form, and practitioners are referenced by
  identifier query (`Practitioner?identifier=...|9999999799`), not by resource id.
- SOAP notes are Base64 **inline** in `DocumentReference.content[].attachment.data`
  (no separate `Binary` resource in most bundles), contradicting the spec's
  `extract_soap_note(binary_resource, doc_ref)` signature.
- `ImagingStudy.series.modality` / `.bodySite` are bare `Coding` dicts, not
  `CodeableConcept` wrappers.

`fhir.resources` strict validation routinely rejects real-world Synthea quirks, which
would force per-field error handling anyway — eroding the benefit of typed models.

## Decision

Parse FHIR bundles as **plain Python dicts** using `.get()` access throughout, with no
`fhir.resources` dependency. `FHIRBundleParser.parse_bundle()` indexes bundle entries by
`resourceType` and by id (covering both `resource.id` and `entry.fullUrl`), then
dispatches to `extract_*` methods that return flat record dicts aligned with the Silver
schemas. A `strip_reference()` helper normalizes `urn:uuid:`, `Type/id`, and bare-id
references. `extract_soap_note(doc_ref, binary_index=None)` resolves both inline
attachments and `Binary`-by-`url` references.

`pydicom` (ADR-006) is imported lazily inside `_extract_dicom_headers` so the module
loads and tests run without it installed.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|--------|------|------|-------------|
| `fhir.resources` typed models (spec sketch) | Standards-validated, IDE autocomplete | Heavy dep in Spark UDFs; strict validation fails on Synthea quirks; attribute access conflicts with the `.get()` rule | Friction outweighs typing benefit on synthetic data |
| `fhirclient` / other FHIR SDK | Similar typing | Same dependency-weight and validation issues | Same as above |
| Dict-based `.get()` parsing (chosen) | Tolerant of optional/odd fields, zero heavy deps, matches transform rules, trivially testable | No compile-time schema guarantees | Schema is enforced downstream in Silver validation (Session 2) instead |

## Consequences

**Positive:**
- Transforms stay pure and import-light — drop straight into Fabric Spark UDFs.
- Tolerant of real Synthea quirks (`urn:uuid:`, inline attachments, bare Codings).
- Easy to unit test with small inline dicts and one compact fixture.

**Negative:**
- No compile-time FHIR schema validation; field-name typos surface only at runtime/test.
  Mitigated by full test coverage and downstream Silver schema validation (Session 2).

**Neutral:**
- Diverges from the spec's literal `fhir.resources` sketch; the spec's intent (extract all
  resource types, decode SOAP, metadata-only DICOM/genomics) is fully preserved.

## Implementation notes
- `local/transforms/fhir_parser.py` — `FHIRBundleParser`, `strip_reference`, helpers.
- Section detection maps both Markdown clinical headers ("# Chief Complaint", "# Assessment
  and Plan") and classic "SUBJECTIVE:/OBJECTIVE:" markers to S/O/A/P flags (ADR-005).
  Coherent notes lack an Objective section, so `has_objective` is frequently `False` —
  honest, documented behavior, not a bug.
- `has_pathogenic_variant` uses negation-aware text detection so "No pathogenic variants
  detected" → `False` (ADR-007).
- `tests/test_fhir_parser.py`, `tests/test_silver_soap_notes.py` — full coverage on the
  synthetic `tests/fixtures/sample_bundle.json`.
