# ADR-020: Fabric distributed parsing via `applyInPandas`

**Date:** 2026-05-29
**Status:** Superseded by [ADR-022](../../adr/022-platform-independent-implementations.md) (same day, 2026-05-29)
**Deciders:** Sandeep Jayaprakash

> **Superseded same-day.** The `applyInPandas` bridge was a workaround for the
> shared-builder model imposed by ADR-002 / ADR-004. ADR-022 removed that
> constraint by giving Fabric its own Spark-native transforms; the bridge is
> gone and `fabric/spark_helpers.py` was deleted. Preserved here for
> historical context — the trade-off discussion still illustrates *why*
> shared builders + cloud-native engines don't compose cleanly.

## Context

The medallion's transforms (`core/transforms/silver_*.py`) are pure Python:
they take `list[dict]` records and return `pa.Table` with an explicit schema
(ADR-002, ADR-004). This portability is load-bearing — the same builders run
under three orchestrators (CLI, Dagster, Fabric notebooks) on two engines
(delta-rs on LocalLite, Spark on Fabric).

The first Fabric notebook pass exploited none of Spark's parallelism: the
notebook driver walked `Files/bronze/fhir/cohort=*/*.json` one bundle at a
time via `mssparkutils.fs.head`, parsed them serially with
`FHIRBundleParser`, built a `pa.Table` on the driver, then handed it to
Spark via `spark.createDataFrame(pa_table.to_pandas())`. Only the final
Delta write distributed. At 1,278 patients × 10 Silver tables that was a
single-threaded 5-10 minute driver burn; at any realistic production scale
it would not finish.

The portfolio claim "this runs on Fabric" requires notebooks that
demonstrably *use* Spark — executor parallelism visible in Fabric Monitor,
multiple tasks per stage, partitions doing work in parallel. Not "Python
that happens to run on a Spark cluster."

## Decision

**Distribute via `applyInPandas` — keep the pure-Python transforms unchanged.**

For every Silver notebook (02–07):

1. Read FHIR bundles as a **partitioned Spark DataFrame** via
   `spark.read.text(wholetext=True)` → one row per `.json` file with columns
   `path` + `value`. Helper: `fabric.spark_helpers.read_fhir_bundles_distributed`.

2. **Distribute parsing** with `bundles_df.groupBy(F.spark_partition_id()).applyInPandas(udf, schema)`:
   the UDF receives one Spark partition as a pandas DataFrame, runs the
   pure-Python `FHIRBundleParser` + the canonical `build_silver_<table>`
   builder on it, returns `pa_table.to_pandas()`. Spark executes the UDF in
   parallel across executors. Helper:
   `fabric.spark_helpers.make_partition_parser(table_name, builder, ingest_ts)`.

3. **Output schema** is derived from the canonical `spec.schema` via
   `fabric.spark_helpers.pa_to_spark_schema(pa_schema)` — one Spark
   `StructType` per Silver table, generated programmatically from the same
   `pa.Schema` LocalLite uses. No drift possible.

4. **Spark-native Delta write** — new `FabricPlatform.write_silver_spark(table, df, mode)`
   takes a Spark DataFrame directly, runs `DeltaTable.merge()` server-side
   (with the ADR-019 target-dedup guard). No pa.Table round-trip.

Existing `write_silver(pa.Table)` / `read_silver() → pa.Table` wrappers stay
for LocalLite-style callers (CLI pipeline, Dagster).

### Gold and validation — driver-side compute by design

Two notebooks intentionally **don't** distribute compute:

- **`09_gold_encounter_summary`** uses Spark for parallel Silver reads + the
  Gold Delta write, but `build_encounter_summary` runs on the driver (Polars).
  Gold is a global denormalization — every encounter joins back to active
  conditions / medications / vitals as-of-date across the whole corpus. At
  Coherent scale (~143k encounters × 10 source tables) Polars on the driver
  finishes in ~5s; the equivalent Spark DataFrame DAG would take longer with
  more code. ADR-002 portability — the same builder runs locally.

- **`08_silver_validation`** uses Spark for Silver reads but
  `validate_table()` runs rule checks on a `pa.Table` (matches CLI + Dagster
  call sites). Validation rules are metadata-bound aggregations — cheap on
  the driver after Spark hands the data over.

Both notebooks would be rewritable in Spark SQL/DataFrame API, but the lift
is large and the perf is already fine. Documented as future-work in ADR-022
if scale ever demands it.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| **Pure Spark SQL/DataFrame transforms in `fabric/`** | Most idiomatic for Fabric; full Spark optimizer | Duplicate of `core/transforms/`; ADR-002 portability dies; large refactor; tests would also need duplication | Worst-of-both-worlds: lose code reuse without gaining much over `applyInPandas` |
| **Driver-only Python (the original notebooks)** | Simplest code; matches ADR-002 trivially | No distributed processing; slow at any scale; Spark cluster idle while driver works; weak Fabric story | Performance + portfolio narrative both fail |
| **Pandas UDFs / `mapInPandas`** | Similar perf profile to applyInPandas | Less suited to per-table extraction from heterogeneous bundles; row-at-a-time iteration less natural | applyInPandas is the better fit for partition-batch parse |
| **`applyInPandas` (chosen)** | Pure transforms unchanged; ADR-002 holds; actual executor parallelism; matches Fabric idioms; visible in Monitor | Output schema must be derived (`pa_to_spark_schema` helper); multi-table notebooks (04, 07) re-parse bundles N times | The right trade-off |

## Consequences

**Positive:**
- **Actual parallelism** — Fabric Monitor shows N executor tasks per stage,
  partitions processing in parallel. Portfolio screenshot demonstrates Spark
  use, not "Python on Spark."
- **Same transforms everywhere** — `core/transforms/silver_*.py` unchanged;
  LocalLite, Dagster, and Fabric all call the same builders. ADR-002 holds.
- **Scales with capacity** — adding executors (F8 → F32) gives linear-ish
  speedup on Silver builds. Limited by partition count from the read.
- **Spark-native MERGE** — `write_silver_spark` runs the Delta MERGE
  server-side with full optimizer benefits. The ADR-019 dedup guard
  translates cleanly to Spark (`dropDuplicates([pk])`).
- **Honest perf story** — every notebook's distributed read + parse step is
  measurable; total time scales with data volume + executor count.

**Negative:**
- **Multi-table notebooks (04 clinical, 07 ecg+genomics) re-parse bundles
  N times** — one applyInPandas pass per output table; bundle parsed N times
  for N tables. At Coherent scale and parser speed (~few ms/bundle) the
  overhead is small (~10-20s); negligible vs the alternative complexity of
  emitting all N tables from one UDF call with a polymorphic schema.
  Documented; deferred-optimization target.
- **Output schema must be flat** (or serialize complex types as JSON
  strings) for `pa_to_spark_schema` to produce a Spark `StructType` cleanly.
  All current Silver schemas already do this via `core.transforms.schema_utils.build_arrow_table`,
  but future schemas with raw `struct`/`list` types would need either a
  schema-converter extension or per-column JSON serialization.
- **Driver memory still bounds Gold + validation** — both notebooks
  materialize full Silver tables to `pa.Table` before processing. Currently
  fits comfortably; if Coherent grows 10x, Gold or validation would need a
  Spark rewrite.

**Neutral:**
- `applyInPandas` UDFs serialize the partition to pandas via Arrow → fast,
  but the Python interpreter on the executor is single-threaded for the UDF
  call itself. Parallelism comes from running N UDFs in parallel across N
  executors, not from intra-UDF threading. This matches the way LocalLite
  + Polars also process records.
- The pa.Table-flavoured public methods (`write_silver`, `read_silver`,
  `write_gold`) become convenience wrappers that round-trip via Spark
  internally. Slightly slower than the Spark-native methods but kept for
  ADR-002 portability with LocalLite-style callers.

## Implementation notes

- **`fabric/spark_helpers.py`** — three exported functions:
  - `pa_to_spark_schema(pa_schema) → StructType`
  - `read_fhir_bundles_distributed(spark, fhir_root, *, num_partitions=None) → DataFrame`
  - `make_partition_parser(table_name, builder, ingest_ts) → callable`
- **`fabric/platform.py`** — added `write_silver_spark`, `read_silver_spark`,
  `write_gold_spark`, `read_gold_spark`. Refactored internal `_write_delta_spark`
  as the single Spark-native write path; pa.Table wrappers
  (`write_silver`, `write_gold`) delegate.
- **All Spark imports are lazy** inside method bodies — `fabric.spark_helpers`
  and `fabric.platform` import cleanly outside Fabric (contract tests, local
  dev).
- **Silver notebooks 02–07** rewritten to the distributed pattern; cells
  documented step-by-step. Validation notebook 08 reads Spark → pa.Table →
  validate_table → Spark write of ingest_log. Gold notebook 09 reads Spark →
  build_encounter_summary on driver → Spark write. Validation gates notebook
  10 reads Spark, uses `ensure_env()` public method.

## Related

- ADR-002 (platform abstraction) — `applyInPandas` keeps `core/transforms/`
  platform-agnostic
- ADR-004 (Arrow interchange) — Spark schema derived from `pa.Schema`; same
  shape end-to-end
- ADR-009 (local Silver materialization) — Spark-native MERGE mirrors the
  delta-rs MERGE locally
- ADR-019 (Silver MERGE idempotency) — target-side dedup guard applied
  inside `_write_delta_spark`
- ADR-021 (Fabric notebook source-of-truth) — sibling decision adopted in
  the same refactor
- `fabric/spark_helpers.py`, `fabric/notebooks/*.Notebook/notebook-content.py`
