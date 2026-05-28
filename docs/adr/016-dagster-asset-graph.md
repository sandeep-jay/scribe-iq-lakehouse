# ADR-016: Medallion as a Dagster software-defined asset graph

**Date:** 2026-05-27
**Status:** Accepted
**Extends:** [ADR-015](015-dagster-local-orchestration.md) (the modelling decision under the
adopted orchestrator); honours [ADR-002](002-platform-abstraction.md) (platform isolation) and
[ADR-004](004-arrow-interchange.md) (Arrow interchange)
**Contract impact:** none
**Deciders:** Sandeep Jayaprakash

## Context

ADR-015 adopts Dagster. Dagster offers two modelling styles — imperative *ops/jobs* or
declarative *software-defined assets* — and a default *IO-manager* persistence model that
conflicts with this repo's single persistence authority (the `LakehousePlatform` over delta-rs,
ADR-002/009). The model must (a) preserve the pure-transform + platform-abstraction invariants,
(b) not double-parse Bronze (the current pipeline parses each cohort once and builds all 10
Silver tables from that parse), and (c) yield the lineage graph that is the *reason* we chose
Dagster over Prefect.

## Decision

Model the medallion as **software-defined assets, partitioned by cohort, where assets call the
platform for persistence and return only materialization metadata** — no Dagster IO manager owns
the Delta bytes:

- **Bronze** landed data as source/observable assets, cohort-partitioned.
- A single **`@multi_asset`** parses each cohort once and emits all 10 Silver tables as distinct
  asset outputs (parse-once preserved; Dagster still renders 10 nodes in the graph). Each Silver
  table carries an **`@asset_check`** wrapping `validate_table()`.
- **`gold_encounter_summary`** depends on the 10 Silver assets; **`corpus_manifest`** depends on
  Gold (both unpartitioned — Gold aggregates across all cohorts).
- The **platform is a Dagster resource** reading `LAKEHOUSE_PLATFORM`; assets call
  `platform.write_silver` / `write_gold` exactly as the notebooks and CLI do. A cohort-watching
  **`@sensor`** is the Dagster analogue of the Auto Loader streaming-sim (spec §5.2).

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|--------|------|------|-------------|
| **Assets + platform-persisted (chosen)** | Best lineage; preserves single persistence authority; parse-once | Assets return metadata not data; slightly non-idiomatic (no IO manager) | — |
| Ops / jobs (imperative) | Closest to current `pipeline.py` | No asset lineage — forfeits the reason we picked Dagster | Defeats the purpose |
| Custom IOManager wrapping the platform | Most "Dagster-native" | Couples Dagster to the platform; two persistence paths to reason about | Violates single-authority simplicity (ADR-002/009) |
| Per-table Silver assets, each re-parsing the cohort | Cleanest graph | Re-parses bundles ~10× — wasteful | `multi_asset` gives the same graph, parse-once |
| Materialize the parsed-records dict as an intermediate asset | Clean lineage, parse-once | Large dict serialized through an IO manager every cohort | Unnecessary memory/IO vs `multi_asset` |

## Consequences

**Positive:**
- Cohort-partitioned assets give incremental per-cohort materialization + **backfill** (the
  working MERGE path) — ends the `rm -rf` full rebuild from ADR-015's context.
- Validation is **visible as asset checks**, not just rows in `ingest_log`.
- Transforms stay **100% Dagster-unaware** (ADR-002 intact) — the asset functions are thin
  adapters over `_parse_cohort`, `SILVER_TABLES[*].build`, and `build_gold` internals.
- The asset graph is a literal **medallion diagram** for reviewers.

**Negative:**
- The "assets return metadata, the platform persists" pattern is slightly non-idiomatic Dagster
  and needs a one-line explanation for reviewers.
- `multi_asset` couples the 10 Silver outputs into one parse step — acceptable, and it matches
  what `pipeline.py` already does.

**Neutral:**
- Gold stays unpartitioned and full-rebuild (~6.5 s).
- `silver.ingest_log` is still written (for Fabric parity) even though checks also surface in the UI.

## Implementation notes

- `orchestration/partitions.py` — cohort partitions (`Static` or `Dynamic` from `cohort_labels()`).
- `orchestration/assets.py` — Silver `@multi_asset` (reuse `_parse_cohort` + `SILVER_TABLES[*].build`
  + `platform.write_silver`); `gold` + `corpus_manifest` assets (reuse `build_gold` internals).
- `orchestration/checks.py` — `@asset_check` per Silver table wrapping `validate_table()`.
- `orchestration/resources.py` — platform resource from `get_platform()`;
  `orchestration/sensors.py` — cohort sensor whose target is `bronze_fhir` **plus the 10
  Silver asset keys** (sourced from `SILVER_TABLES` so the selection cannot drift): each
  new cohort triggers one Dagster run that materializes Bronze + Silver for that
  partition. Gold is intentionally **not** in the sensor target — it is unpartitioned and
  rebuilt manually (or by a downstream schedule) once the cohorts of interest are present.
- `orchestration/definitions.py` — `Definitions(assets, asset_checks, resources, sensors, schedules)`.
- `tests/test_dagster_defs.py` — `Definitions` loads; `materialize()` on the fixture writes all
  Silver + Gold; asset checks pass.
