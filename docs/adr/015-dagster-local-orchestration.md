# ADR-015: Dagster for local pipeline orchestration

**Date:** 2026-05-27
**Status:** Accepted
**Contract impact:** none — orchestration layer only; `gold.encounter_summary` stays v1.1.0
**Deciders:** Sandeep Jayaprakash

## Context

The local Bronze → Silver → Gold pipeline is orchestrated *imperatively* by
[`local/pipeline.py`](https://github.com/sandeep-jay/scribe-iq-lakehouse/blob/main/core/surfaces/cli/pipeline.py): a hand-rolled sequential driver
(`run_pipeline` + `build_gold`) wired with `argparse`. It is correct, but flat — no DAG
view, no per-table or per-cohort retry/backfill, no run history, and every full re-run is a
destructive `rm -rf data/silver data/gold` rebuild (delta-rs MERGE cannot whole-table
re-update — see HANDOFF "Discoveries"). Two pressures make this worth fixing now: (1) for a
data-engineering portfolio, *orchestration* is a named, expected competency that an imperative
script does not signal; (2) Fabric will orchestrate via Data Factory, but that path is
trial-bound and expires (~12 days), so the **local** path needs a durable orchestrator that
outlives the trial.

## Decision

Adopt **Dagster** as the local pipeline orchestrator, implemented as a new top-level
`orchestration/` package that imports the existing pure transforms and platform — a **third
execution surface** alongside the `local.pipeline` CLI and the Fabric notebooks, with zero
duplicated transform logic. Keep `local/pipeline.py` as the dependency-free CLI for CI and
simple runs; Dagster is the richer, observable, interactive local runner, **not** a
replacement. Sequence the work as **Dagster (Session 4) → Fabric (Session 5)** so the asset
graph documents the DAG the Fabric notebooks then mirror, while keeping Dagster tight enough
(~1–2 sessions) that the Fabric trial retains runway. Asset modelling, partitions, checks and
the persistence pattern are specified separately in [ADR-016](016-dagster-asset-graph.md).

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|--------|------|------|-------------|
| **Dagster (chosen)** | Asset/lineage model maps 1:1 onto the medallion; partitions = cohorts; asset checks = validation; strongest data-engineering signal | Heavier; defines its own project layout; steeper learning curve | — |
| Prefect | Lowest friction — wraps existing functions as `@flow`/`@task`; local UI, retries, scheduling | Flow-centric, not asset-centric → weaker lineage story; less "lakehouse" fit | Good, but Dagster's asset graph is the better portfolio artifact for a medallion |
| Airflow | Ubiquitous name recognition | Scheduler + webserver + metadata DB is heavy for a single-laptop dev loop; DAG-of-tasks is not data-aware | Operationally heavy for the local tier |
| Makefile / `just` | Trivial; targets encode DAG order | No observability, retry, state, or lineage | Cosmetic, not real orchestration |
| Status quo (imperative CLI) | Simplest; zero deps | No DAG/retry/backfill/history; weak signal | The gap we set out to close — but kept as the CLI path |

## Consequences

**Positive:**
- The medallion is rendered as an **asset graph** — the lakehouse made visual for reviewers.
- Per-cohort **partitioned materialization + backfill** replaces the `rm -rf` full rebuild:
  each cohort is the incremental MERGE path that actually works (ADR-016).
- Validation surfaces as first-class **asset checks**; run history/metadata live in the UI.
- The same pure transforms now demonstrably run under **three orchestrators** (CLI, Dagster,
  Fabric Data Factory) — the concrete payoff of ADR-002 (platform isolation) and ADR-004 (Arrow).

**Negative:**
- New dev dependency (`dagster` + `dagster-webserver`) and a UI process to run.
- Learning curve / some ceremony; another surface to keep in sync as transforms evolve
  (mitigated: assets are thin wrappers over existing functions).

**Neutral:**
- Orchestration choice is **local-only** — Fabric still orchestrates via Data Factory; Dagster
  is not deployed to Fabric. Corpus contract and JSON schemas are unchanged.

## Implementation notes

- New `orchestration/` package (`definitions.py`, `assets.py`, `checks.py`, `resources.py`,
  `partitions.py`, `sensors.py`) imports `local.transforms` / `local.gold` / `local.platform`;
  it is **never imported by** them (ADR-002 isolation — mirrors the Spark/notebook rule, and
  should be added to `.claude/rules/transforms.md`: no `orchestration`/`dagster` import in
  `local/transforms/`).
- `dagster` + `dagster-webserver` added to `pyproject.toml` `[dev]` (pinned); runs fine on
  a typical laptop. `tests/test_dagster_defs.py` materializes assets on the fixture via the
  LocalLitePlatform (tests alongside, per non-negotiables).
- Modelling/partitions/checks/persistence: see [ADR-016](016-dagster-asset-graph.md).
