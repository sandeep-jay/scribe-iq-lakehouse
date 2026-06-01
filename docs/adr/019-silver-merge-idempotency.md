# ADR-019: Silver MERGE idempotency — pre-merge target dedup guard

**Date:** 2026-05-28
**Status:** Accepted
**Deciders:** Sandeep Jayaprakash

## Context

The Silver write path on `LocalLitePlatform` (and, by ADR-002, every future
platform tier) is "OVERWRITE on first write, MERGE on subsequent writes" — see
[core/platform/local_lite.py:133](https://github.com/sandeep-jay/scribe-iq-lakehouse/blob/main/core/platform/local_lite.py#L133)
(`_write_delta`). delta-rs's MERGE requires that for each row in the *target*
table, at most one row in the *source* matches the predicate `target.pk =
source.pk` — otherwise it errors with *"matched a target row with multiple
source rows"*.

Source-side duplicate prevention is in place: every `build_silver_*` calls
`dedup_by_key()` ([core/transforms/schema_utils.py:119](https://github.com/sandeep-jay/scribe-iq-lakehouse/blob/main/core/transforms/schema_utils.py#L119))
before emitting its `pa.Table`, so any single MERGE invocation feeds delta-rs a
key-unique source. This was added in Session 3 once the duplicate-FHIR-extract
pattern was observed.

The remaining gap, discovered in Session 4 (Dagster sensor re-firing on a
populated cohort), is **target-side dups left over from legacy OVERWRITE
writes** — Silver tables built *before* `dedup_by_key()` landed in every
builder. Those tables have rows with the same PK; the first subsequent MERGE
fails. Workaround was `rm -rf data/silver data/gold` before any re-run, which
makes the pipeline non-idempotent and breaks Dagster's "materialize this
partition" affordance.

This is a one-time data-shape defect, but it surfaces every time someone with
an older `data/` directory pulls the latest code and re-runs the pipeline — and
it will resurface for the Fabric tier the moment ADR-019 is not extended there.

## Decision

**Detect-and-rewrite the deduped target before MERGE, when (and only when)
target-side duplicates exist on the merge key.**

In `LocalLitePlatform._write_delta`, when `mode == "merge"` and the table
already exists:

1. Read the existing Delta as a `pa.Table` (`dt.to_pyarrow_table()`).
2. Count duplicates on the primary key via `pc.count_distinct` — O(rows), no
   shuffle.
3. **If duplicates exist**, rewrite the target deduped via
   `write_deltalake(..., mode="overwrite", configuration=_CDC_CONFIG)`, then
   refresh the `DeltaTable` handle. Log a warning recording the affected table
   + duplicate-row count.
4. Proceed with the original MERGE.
5. If no duplicates, fall through to MERGE unchanged — happy path stays fast.

Dedup helper (`_dedup_target`) uses an in-memory row-index approach
(`group_by(pk).aggregate(("_idx", "min"))` then `is_in` mask) — pyarrow-only,
no polars/duckdb dependency, no schema introspection needed.

**Survivor semantics — important caveat.** `dedup_by_key()` on the *source* side
keeps the last occurrence because the in-memory record list preserves
insertion order. The *target* side cannot offer the same guarantee: Delta does
not preserve write order on read, so "last write wins" is undefined when
reading a dirty Delta table back as `pa.Table`. The helper therefore keeps a
deterministic but arbitrary survivor (smallest read-order index per key). This
is correct for the failure mode being fixed: the MERGE that follows will write
the *current* source on top of the deduped target, so any PK present in both
sides ends up with the source value — which is the canonical truth. PKs that
exist *only* in the dirty target (orphans not in the new source) keep an
undefined survivor; this is acceptable because legacy dup state has no
canonical row anyway.

FabricPlatform (Phase 2 of the Session 5 plan,
[docs/roadmap/fabric-execution-plan.md](https://github.com/sandeep-jay/scribe-iq-lakehouse/blob/main/docs/roadmap/fabric-execution-plan.md))
will implement the Spark equivalent: `df.dropDuplicates([pk])` rewrite on
detection, same triggering condition.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| Force a clean-slate rebuild before any MERGE (`rm -rf data/silver`) | Trivial to write | Pipeline is non-idempotent; Dagster "rematerialize partition" requires manual filesystem surgery; loses CDC history every run | Status quo we're moving away from |
| Dedupe target on every MERGE, regardless of whether dups exist | Simpler control flow (no count step) | Pays the rewrite cost (~143k rows × ~10 tables × full Delta overwrite) on every Dagster run forever; CDC log becomes noisy | Wasteful — dup state is rare |
| Move dedup to `core/transforms/build_*` (the Session 4 Open Decision recommendation) | Single bottleneck per HANDOFF | Source side is already deduped — the defect is target-side. A change in `build_*` cannot fix data that is already on disk | Misdiagnoses the defect |
| One-off migration script (`scripts/dedupe_silver.py`) that rewrites all Silver tables once | No runtime cost | Manual remediation step that every developer / CI run must remember; doesn't survive new cohorts being merged into a table that hasn't been remediated | Brittle for a multi-developer / CI scenario |
| **Detect-and-rewrite target on demand in `_write_delta`** | Idempotent; zero cost on clean tables; localized to the persistence layer; ports identically to Spark | Adds an `O(rows)` count + a Delta-table read every merge call (~ms on 143k rows); first run on a dirty table writes an extra Delta version | Right tradeoff |

## Consequences

**Positive:**
- `python -m core.surfaces.cli.pipeline --with-gold` is now idempotent — no
  `rm -rf` required between runs.
- Dagster's "materialize partition" works correctly even when the underlying
  Silver tables predate ADR-019's fix.
- The fix lives in one place (the persistence layer). Transforms, the Dagster
  asset graph, and the CLI orchestrator are unchanged.
- FabricPlatform inherits the same contract: implement the Spark equivalent in
  `_write_delta` and the same idempotency guarantee holds.
- Regression test (`test_merge_dedupes_target_with_legacy_duplicates` in
  [core/tests/test_local_lite.py](https://github.com/sandeep-jay/scribe-iq-lakehouse/blob/main/core/tests/test_local_lite.py)) pins the
  behaviour: a target with intentional dups must MERGE-then-dedupe in one call.

**Negative:**
- Every MERGE pays an `O(rows)` `count_distinct` + a `to_pyarrow_table()` read
  to check for dups. Measured cost at 143k-row scale: sub-millisecond for the
  count, ~50 ms for the read. Acceptable.
- The first MERGE on a dirty table writes an extra Delta version (the dedup
  rewrite). CDC consumers will see a large "remove + insert" batch for that
  version. Acceptable as a one-time event.
- Warning log line is emitted whenever a dirty table is encountered. Useful
  signal — leave verbose, don't suppress.

## Related

- ADR-002 (platform abstraction — the guard must port to every platform tier)
- ADR-009 (local Silver materialization — defines the OVERWRITE-then-MERGE
  contract that this ADR closes the gap on)
- ADR-015 / ADR-016 (Dagster — the materialize-partition affordance that
  surfaced this defect)
- ADR-017 / ADR-018 (multi-platform layout — context for Fabric's matching impl)
- [docs/roadmap/fabric-execution-plan.md](https://github.com/sandeep-jay/scribe-iq-lakehouse/blob/main/docs/roadmap/fabric-execution-plan.md)
  Phase 2 (Spark equivalent in FabricPlatform)
