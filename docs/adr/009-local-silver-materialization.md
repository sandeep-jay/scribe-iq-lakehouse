# ADR-009: Local Silver materialization — delta-rs, type coercion, component JSON

**Date:** 2026-05-27
**Status:** Accepted
**Deciders:** Sandeep Jayaprakash

## Context

Session 2 materializes the parsed FHIR records as Silver Delta tables on the local
(non-Fabric) tier. Several concrete choices were needed that the spec left open or that
real Coherent data forced:

1. **Engine.** The local tier must write CDC-enabled Delta without Spark or cloud creds
   (ADR-003), and the pipeline must stay portable to Fabric unchanged (ADR-002).
2. **Typing.** Transforms must return explicitly-typed Arrow tables (ADR-004), but FHIR
   delivers everything as strings (dates, codes, numbers) with optional fields everywhere.
3. **Nested data.** `Observation` blood-pressure rows carry systolic/diastolic as a
   `component` array — a nested structure that complicates a flat, portable Delta schema.
4. **Validation realism.** Spec §5.6 assumed SOAP notes have all four S/O/A/P sections
   and that ECG rows exist; neither holds for Coherent (ADR-005 discoveries).

## Decision

- **`LocalLitePlatform` writes via `deltalake` (delta-rs).** `write_silver` creates the
  table on first write with `delta.enableChangeDataFeed=true` (non-negotiable #5) and
  upserts on the registered primary key via `DeltaTable.merge(...)` on subsequent writes.
  Each cohort is processed as a micro-batch MERGE — the local analogue of an Auto Loader
  streaming trigger.
- **Type coercion is field-type-driven.** `schema_utils.build_arrow_table` coerces each
  value to its declared Arrow type (timestamps → UTC-aware `timestamp[us]`, dates →
  `date32`, codes → `string`, etc.), so adding a column never needs a bespoke rule.
  Clinical codes are never cast to numeric.
- **Observation components are serialized to a `components_json` string column.** This
  keeps the Delta schema flat and engine-portable while preserving structured values for
  Gold to parse. The scalar `value`/`unit` columns remain for simple observations.
- **Validation rules track Coherent reality.** SOAP completeness checks S/A/P (not
  Objective, which Coherent notes lack); ECG/genomic `min_rows` is 0 (those reports live
  outside the FHIR bundles). Rules live in `validation/schema_registry.py` with comments.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|--------|------|------|-------------|
| Spark (local[*]) for Silver | Identical to Fabric API | JVM dependency, slow startup, heavy on M1 | ADR-003 picks Polars/delta-rs for the lite tier |
| Nested `list<struct>` for components | Fully structured | More fragile across engines; harder Gold schema; delta-rs nested edge cases | JSON string is simpler and lossless enough |
| Parse-time typed FHIR models | Compile-time types | Rejected in ADR-008 (dict parsing) | Inconsistent with the parser layer |
| Keep spec's literal validation rules | Matches the written spec | Would fail every run on real Coherent data | Honest rules beat aspirational ones |

## Consequences

**Positive:**
- Full local pipeline runs with `pip install` only; same transforms target Fabric later.
- CDC enabled from creation — downstream change-feed consumers work on day one.
- Flat, portable Silver schemas; Gold can still recover BP via `components_json`.

**Negative:**
- `components_json` requires a parse step in Gold for component-based vitals.
- delta-rs MERGE performance is untested at the full 1,281-bundle scale until the run
  (recorded in HANDOFF).

**Neutral:**
- Validation thresholds are intentionally lenient for sparse tables; tightening them is a
  one-line change in `schema_registry.py` if Fabric data differs.

## Implementation notes
- `local/platform/local_lite.py` — `write_silver` create/merge logic, CDC config.
- `local/transforms/schema_utils.py` — `build_arrow_table`, `parse_date`, `parse_timestamp`.
- `local/transforms/registry.py` — table → (schema, primary_key, build_fn) mapping.
- `local/validation/{schema_registry,validate}.py` — rules + checks, `silver.ingest_log`.
- `local/pipeline.py` — per-cohort micro-batch orchestration.
