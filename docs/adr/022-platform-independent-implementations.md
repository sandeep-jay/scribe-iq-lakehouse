# ADR-022: Independent per-platform implementations

**Date:** 2026-05-29
**Status:** Accepted
**Deciders:** Sandeep Jayaprakash

## Context

ADR-002 (Platform abstraction) and ADR-004 (Arrow as transform interchange)
together prescribed one shared transform layer in `core/transforms/`: every
platform consumed the same pure-Python builders that returned `pa.Table`.
The intent was code-reuse — one Silver implementation reused across LocalLite,
Fabric, and future Databricks/AWS.

In practice the model produced two failure modes on Fabric:

1. **Lowest-common-denominator transforms.** The shared builders had to take
   `list[dict]` records and return `pa.Table`. That foreclosed every native
   capability of the target engine — no distributed parsing, no Spark schema
   inference, no Delta-side aggregation pushdown. The Fabric path was strictly
   slower than a Spark-native equivalent.
2. **The bridge tax.** ADR-020 patched the parallelism problem with
   `applyInPandas` — each Spark executor unpickled the pure-Python
   `FHIRBundleParser`, called the same `build_silver_*` builder, and handed
   pandas back to Spark. The bridge cost (PyArrow ↔ pandas ↔ Spark) erased
   most of the parallelism it was meant to unlock, the codepath was
   harder to debug, and the "shared" builder still needed Spark-specific
   wrappers in `fabric/spark_helpers.py`. The user pushback was direct:
   *"i asked for pure spark implementation. you are cutting corners.
   Implement in pure spark and delta lake formats."*

The repo-layout decision (ADR-017) had already created top-level
`fabric/` / future `databricks/` / `aws/` domains. The remaining question
was whether those domains share their transform logic with `core/` or are
independent end-to-end implementations. User direction: *"Y was always
the ask. These are independent implementations. Core runs locally. Fabric
runs Fabric native. In future when we add Databricks and AWS, they run
independent native code end to end."*

## Decision

**Each platform tier is an independent end-to-end implementation.**

- `core/` runs only locally (LocalLite — Polars + delta-rs + DuckDB). It
  keeps its own transforms (`core/transforms/silver_*.py`), Gold logic
  (`core/gold/encounter_summary.py`), validation (`core/validation/`), and
  the `LakehousePlatform` ABC + factory — but the factory now dispatches
  only local surfaces, not cloud platforms.
- `fabric/` has its own Spark-native transforms (`fabric/transforms/silver_*.py`),
  Gold (`fabric/gold/encounter_summary.py`), validation
  (`fabric/validation/validate.py`), and platform class
  (`fabric/platform.FabricPlatform`). `FabricPlatform` is **not** a subclass
  of `LakehousePlatform`. There is no PyArrow round-trip on the Fabric path —
  every interchange is a Spark DataFrame.
- Future `databricks/` and `aws/` tiers follow the same shape: each owns
  its complete Silver / Gold / validation / platform stack, written in the
  engine native to that cloud.

**Cross-platform compatibility is enforced at the contract layer, not the
code layer.**

- Silver schemas are column-name + type identical across platforms so the
  tables would union-compat if a future workload needed it.
- Gold schemas are identical field-for-field (same struct shapes, same
  field names). `CONTRACT_VERSION` is duplicated as a constant in both
  `core.gold.encounter_summary` and `fabric.gold.encounter_summary`; both
  must be bumped in lockstep on any breaking change.
- Corpus manifest field names match across platforms; downstream consumers
  (`scribe-iq` RAG, `clinical-bert-pipeline`, Ollama generation) cannot
  tell which platform produced the corpus they're reading.

**Validation rules are duplicated.** `fabric/validation/schema_registry.py`
is a copy of `core/validation/schema_registry.py` with the same rule grammar.
A future extraction to a shared YAML config is possible but is mechanical;
the cost of duplication is lower than the cost of cross-tier import coupling.

## Consequences

**Positive:**
- Each tier exploits its engine's native capabilities — Fabric uses
  `from_json + explode + window` with no Python bridge; LocalLite uses
  Polars expressions directly. Neither pays a portability tax.
- Each tier's notebooks / orchestrator only need that tier's modules. The
  Fabric Environment doesn't have `core.*` on its path; the LocalLite CLI
  doesn't have `fabric.*` installed. A stray cross-tier import fails loudly.
- A platform-specific simplification stays inside that platform's code —
  e.g., the Fabric Silver `imaging_study` is FHIR-only (pydicom enrichment
  is a local-tier responsibility) without affecting LocalLite.
- The user's stated portfolio model ("show I can build each platform native,
  not lowest-common-denominator") is the architecture.

**Negative:**
- Same transform logic is implemented twice (Polars expressions on local,
  Spark expressions on Fabric). Every behavior change has to be made twice.
- Validation rules are duplicated. Drift between the two registries is
  possible and would only surface on a side-by-side audit.
- Schema parity is enforced by convention, not by a single source of truth.
  If `silver.observation` adds a column on one platform, the other has to
  follow or the Gold schema diverges silently.

**Mitigations:**
- Both tiers' Silver/Gold schemas are exported as code constants
  (`SCHEMA = StructType(...)` / `pa.schema(...)`). A future
  cross-tier schema-parity test can diff the two and fail CI on divergence.
- `CONTRACT_VERSION` is reviewed on every Gold change as the explicit
  in-code reminder that both tiers must move together.

## Supersedes

- **[ADR-002](../_archive/adr/002-platform-abstraction.md)** — the
  `LakehousePlatform` ABC is no longer the universal contract; it now
  governs only the local execution surfaces (LocalLite, future LocalSpark).
- **[ADR-004](../_archive/adr/004-arrow-interchange.md)** —
  `pa.Table` is no longer the cross-platform interchange type. It remains
  the interchange type *within* `core/` (LocalLite's transforms still return
  `pa.Table`). Fabric uses Spark DataFrames end-to-end.
- **[ADR-020](../_archive/adr/020-fabric-distributed-parsing.md)** — the
  `applyInPandas` bridge is removed. Spark-native `from_json` against the
  `BUNDLE_SCHEMA` is the parser on Fabric.

## Related

- ADR-017 (Multi-platform repo layout) — already aligned with this model;
  amended to call out the dependency direction explicitly.
- ADR-018 (CI/CD monorepo) — wheel still ships `core/` + `fabric/`; the
  Fabric Environment installs the wheel and imports only `fabric.*`.
- ADR-019 (Silver MERGE idempotency) — the target-dedup guard is
  implemented twice (LocalLite's `_write_delta`, Fabric's
  `_write_delta_spark`). Same behavior; each tier owns its impl.
