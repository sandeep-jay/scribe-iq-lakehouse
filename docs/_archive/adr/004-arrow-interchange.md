# ADR-004: Arrow as transform interchange format

**Date:** 2026-05-27
**Status:** Superseded by [ADR-022](../../adr/022-platform-independent-implementations.md) (2026-05-29)
**Deciders:** Sandeep Jayaprakash

> **Superseded.** `pa.Table` is no longer the cross-platform interchange type.
> It remains the interchange type *within* `core/` — LocalLite transforms still
> return `pa.Table` — but cloud-native tiers use their engine's native frame
> (Spark DataFrame on Fabric). Preserved here for historical context.

## Context

Transforms in local/transforms/ need to work with both Spark (Fabric) and Polars
(local lite). Spark operates on DataFrames, Polars operates on Polars frames — if
transforms return one type, they can't be used by the other engine without conversion
code in every transform. A common interchange format is needed that both engines
accept natively.

## Decision

All transform functions return `pyarrow.Table`. Apache Arrow is natively supported
by both Spark (`spark.createDataFrame(arrow_table)`) and Polars (`pl.from_arrow(table)`)
with zero serialization overhead. The platform layer is responsible for converting
the Arrow table to the appropriate format for writing. Transforms never return
Spark DataFrames or Polars frames directly.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|--------|------|------|-------------|
| Return Spark DataFrame | Natural for Fabric | Breaks local Polars tier | Platform lock-in |
| Return Polars frame | Fast locally | Polars → Spark requires conversion | Platform lock-in |
| Return dict/list | Universal | Loses schema, slow for large data | No type safety or performance |
| Return pa.Table | Both engines accept natively | Requires pyarrow dependency | Best trade-off |

## Consequences

**Positive:**
- Single transform codebase works on all platforms
- Type-safe schema via PyArrow schema definitions
- Zero-copy conversion to Polars and near-zero to Spark

**Negative:**
- pyarrow is a required dependency even for the local lite tier
- Developers must think in Arrow schema, not Polars or Spark schema

**Neutral:**
- delta-rs (local tier) also accepts Arrow natively — consistent throughout

## Implementation notes
- Every function in local/transforms/ has return type `pa.Table`
- Schema definitions live alongside transforms (or in schemas/)
- Platform write_silver() and write_gold() handle pa.Table → Delta write
- Tests validate Arrow schema compliance
