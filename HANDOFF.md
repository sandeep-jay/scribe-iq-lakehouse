# HANDOFF — Fabric tier + docs site merged to main; site live
**Date:** 2026-06-01 · **Branch:** `main`

> State only. For what happened in this (or any prior) session see [CHANGELOG.md](CHANGELOG.md).
> Narrative belongs there, not here.

## Current state

The **Fabric Spark-native tier (ADR-022)** and the **MkDocs documentation site** are both
**merged to `main`** (PR #1 Fabric, PR #2 docs, + CI hotfix PR #3). The docs site is **live**
at https://sandeep-jay.github.io/scribe-iq-lakehouse/ (Pages deploy green). CI is green —
`core-pr-tests` (ruff + pytest **128 passed / 1 skipped**) and the docs `mkdocs build --strict`
(zero warnings). The LocalLite tier runs the full medallion end-to-end (143,946 Gold rows); the
Fabric tier ran green on F4 against a 100-patient sample (notebooks 00–10).

Merged branches are deleted. Pushed to **GitHub only** — the Azure DevOps remote (Fabric Git
Integration source) is behind `main`; `git push origin main` syncs it when desired.

## Next task

**Resume the Fabric demo deliverables** (carried over from Session 5):
1. Fabric **Data Pipeline** — `fabric/data_factory/medallion_pipeline.DataPipeline/` chaining
   notebooks 00–10 on-success (one-click run; headline demo artifact).
2. **Power BI Direct Lake** report on `gold.encounter_summary` (count cards, top-conditions
   bar, SOAP-length distribution).
3. Confirm screenshots in `fabric/docs/screenshots/`.

## Open decisions

| Decision | Options | Owner |
|---|---|---|
| Full-corpus Fabric re-run | Yes (~1,278 bundles, stronger numbers) / No (stay on 100-sample) | User |
| Sync Azure DevOps (`git push origin main`) | Now / next time touching Fabric | User |
| Power BI report scope | Minimal (cards + chart) / Richer (drillthrough) | User |

## Blockers / waiting-on

- None blocking. Pages live; CI green; branches merged and pruned.

## First task for next session

Build the Fabric Data Pipeline (`medallion_pipeline.DataPipeline/.platform` +
`pipeline-content.json`), then the Power BI Direct Lake report on `gold.encounter_summary`.
