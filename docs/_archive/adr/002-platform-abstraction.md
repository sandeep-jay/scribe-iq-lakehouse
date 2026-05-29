# ADR-002: Platform abstraction layer design

**Date:** 2026-05-27
**Status:** Superseded by [ADR-022](../adr/022-platform-independent-implementations.md) (2026-05-29)
**Deciders:** Sandeep Jayaprakash

> **Superseded.** The `LakehousePlatform` ABC no longer serves as the universal
> contract across cloud-native platforms; under ADR-022 each platform tier is
> an independent end-to-end implementation. The ABC + factory remain in
> `core/platform/` to dispatch local execution surfaces (LocalLite, future
> LocalSpark). Preserved here for historical context.

## Context

The lakehouse needs to run on Microsoft Fabric during the trial and fall back to local
Polars/DuckDB after trial expiry. Long-term, the architecture should support Databricks,
AWS Glue, and GCP Dataproc without modifying transform logic. Without abstraction,
every platform migration requires touching every transform file.

## Decision

Implement a `LakehousePlatform` abstract base class in `local/platform/base.py` with
methods for storage_path(), read_bronze_fhir(), write_silver(), read_silver(),
write_gold(), log_metric(), send_alert(), and get_spark_session(). A factory function
in `local/platform/factory.py` reads the LAKEHOUSE_PLATFORM environment variable and
returns the correct platform instance. All transforms receive the platform instance as
a parameter — they never import platform-specific code directly.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|--------|------|------|-------------|
| Direct Fabric imports in notebooks | Simpler, less code | Platform lock-in, untestable locally | Breaks local dev and portability |
| Config file per platform | Flexible config | Logic still scattered | Doesn't prevent platform imports in transforms |
| Current approach (ABC + factory) | Full isolation, testable | More initial code | Best long-term trade-off |

## Consequences

**Positive:**
- Transforms in local/transforms/ are 100% platform-independent
- Can test all transform logic locally without Fabric/Spark
- Platform migration = change one env var + implement new class

**Negative:**
- More initial boilerplate (base.py, factory.py, per-platform implementations)
- Platform-specific features (e.g., mssparkutils) must be wrapped

**Neutral:**
- Estimated migration effort to Databricks: 2-3 days (stub in databricks.py documents this)

## Implementation notes
- `local/platform/base.py` — abstract interface (build first)
- `local/platform/factory.py` — env var router
- `local/platform/fabric.py` — Fabric implementation (primary)
- `local/platform/local_lite.py` — Polars + delta-rs (fallback)
- Stubs: `databricks.py`, `aws.py`, `gcp.py` with migration notes
- Arrow (pa.Table) is the interchange format between transforms and platform (see ADR-004)
