# ADR-012: Gold encounter_summary — denormalization engine, grain, and lineage

**Date:** 2026-05-27
**Status:** Accepted
**Deciders:** Sandeep Jayaprakash

## Context

The Gold layer must denormalize the 10 Silver tables into `gold.encounter_summary`, the
corpus contract (spec §5.4/§5.7) that feeds the Ollama generation pipeline, scribe-iq, and
clinical-bert-pipeline. The build joins ~144k encounters against ~670k observations, ~209k
medication requests, and seven other tables, and emits nested types (struct vitals, array
labs, struct imaging). Four decisions had to be made: (1) the join/aggregation engine,
(2) the grain and how "active" conditions/medications are scoped, (3) how the spec's
`silver_versions` lineage is sourced given pure transforms can't touch the platform, and
(4) how the corpus contract is kept honest over time.

## Decision

**Engine — Polars in a pure transform.** `local/gold/encounter_summary.py` receives the
Silver tables as `pa.Table` inputs and returns a `pa.Table` (ADR-002/004): no platform,
Spark, or Delta imports, no file paths. Polars is used purely as an in-process
join/aggregation engine over the Arrow inputs — it is engine-agnostic, not a storage/cloud
dependency, and is already a `[local]` extra (ADR-003). The output is assembled
column-by-column against an explicit `GOLD_SCHEMA` (never inferred), including the nested
struct/list columns — mirroring the Silver `build_arrow_table` philosophy so
`table.schema == GOLD_SCHEMA` holds exactly. Full-dataset build: **143,946 rows in ~5s** on a
single laptop; delta-rs writes and reads the nested types cleanly with CDC enabled.

**Grain — one row per encounter; conditions/meds scoped to the encounter.** Conditions,
medications, procedures, labs and vitals are aggregated by `encounter_id` exactly as the
spec join specifies. `active_conditions` filters out `resolved`/`inactive`/`remission`
clinical status; `active_medications` filters to `status == "active"`; vitals/SOAP/ECG/
imaging/genomics take the latest record (by date) linked to the encounter. `summary_id` is
a deterministic UUIDv5 of `encounter_id` so rebuilds are idempotent (overwrite-safe, stable
lineage downstream).

**Lineage — `silver_versions` injected by the caller.** A pure transform cannot read Delta
versions, so the platform gained two honest capabilities: `table_version(layer, table)`
(concrete `None` default on the base; delta-rs `version()` on `LocalLitePlatform`) and
`write_gold_manifest(manifest)`. The pipeline captures `{table: version}` before the build
and passes it into both the `silver_versions` struct (stamped on every row — a
snapshot-level provenance marker) and the corpus manifest written to
`gold/_metadata/corpus_manifest.json`.

**Contract integrity — generated-first + conformance test.** The machine-readable
`schemas/gold_encounter_summary.json` is generated from `GOLD_SCHEMA` by
`scripts/gen_corpus_schema.py` (ADR-011). `tests/test_gold_encounter_summary.py` asserts
the committed schema is current, that `REQUIRED_FIELDS ∪ OPTIONAL_FIELDS == GOLD_SCHEMA`,
and that every built row validates against the published JSON Schema — so a breaking change
cannot land without a `CONTRACT_VERSION` bump.

## Consequences

- **Positive:** Gold stays portable (a Fabric/Spark notebook can mirror the same pure
  transform); the corpus contract cannot silently drift; nested types are exact; the build
  is fast enough to re-run freely.
- **Negative / accepted limitation:** encounter-grain scoping makes `active_conditions`
  and `active_medications` sparse (avg ~0.08 / ~0.05 per encounter) because Synthea records
  a condition/med once rather than as a running list. This is documented as a v1.0
  limitation in [CORPUS_CONTRACT.md](../CORPUS_CONTRACT.md); the SOAP note (present on 100%
  of encounters) is the primary generation anchor. **Production path:** a
  problem-list-as-of-date join (carry active conditions forward to every subsequent
  encounter for the patient) — deferred, would be a MINOR contract bump.
- `has_ecg` is always false (no ECG in Coherent FHIR); fields kept for forward compat.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|--------|------|------|-------------|
| Polars pure transform (chosen) | Fast joins, engine-agnostic, exact Arrow schema | Polars dependency in build path | Best fit; already a local extra; stays platform-free |
| Pure-Python dict aggregation | Zero extra deps | Slow/awkward over 670k obs; nested grouping by hand | Poor fit for the join scale |
| Spark/DuckDB SQL join | Familiar SQL | Spark violates transform isolation; DuckDB adds a second engine in the hot path | Unnecessary; Polars suffices locally |
| Patient-level problem-list grain | Clinically richer structured fields | Deviates from spec §5.4; larger scope | Deferred to a future MINOR version |
| `silver_versions` from inside the transform | Self-contained | Forces a platform/Delta import into a pure transform (breaks ADR-002) | Inject from the caller instead |
