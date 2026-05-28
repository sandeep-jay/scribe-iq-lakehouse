# HANDOFF — Session 5 · Fabric end-to-end + dedup fix + Power BI
**Date:** 2026-05-28 · **Branch:** `main` · **Plan:** [docs/roadmap/fabric-execution-plan.md](docs/roadmap/fabric-execution-plan.md)

> State only. For what happened in this (or any prior) session see [CHANGELOG.md](CHANGELOG.md).
> Narrative belongs there, not here.

## Current state

Phases 1–3 of the Fabric execution plan are committed (`8e11bb6` dedup fix +
ADR-019, `bfec591` FabricPlatform real impl + REST upload, `7d1d6bf` DEPLOYMENT
runbook + .env machinery, `5e70947` notebook 00_setup, `5c29bb7` HANDOFF/
CHANGELOG sync). The Fabric workspace `scribe_iq_lakehouse_fabric` is
provisioned (lakehouse + Environment `scribe-iq-lakehouse-env` with the core
wheel + 4 PyPI deps published); 128 tests + 1 skipped (`@pytest.mark.fabric`).
Phase 4 is paused on a two-step user unblock: no `origin` git remote
configured, and Fabric Git Integration not wired — both are needed before
`fabric/notebooks/00_setup.ipynb` can run in the workspace.

## Next task

**Wire `origin` + Fabric Git Integration so notebook 00 can run.**

```bash
# 1. Add the GitHub remote (create the repo on GitHub first; private is fine).
git remote add origin git@github.com:<your-user>/scribe-iq-lakehouse.git
git push -u origin main

# 2. In Fabric: Workspace settings → Git integration → Connect
#    repo: <your-user>/scribe-iq-lakehouse
#    branch: main
#    folder: /fabric/notebooks
#    direction: Bidirectional

# 3. Open 00_setup in the workspace → attach lakehouse + env via top bar →
#    Run all cells → screenshot the display() cell as
#    fabric/docs/screenshots/00_workspace_overview.png
```

Detailed walkthrough + gotchas: [fabric/docs/DEPLOYMENT.md](fabric/docs/DEPLOYMENT.md) Step 6.

## Open decisions

| Decision | Options | Owner | Due |
|---|---|---|---|
| Service Principal registration | Skip (manual UI uploads forever) / Register (enables REST + CI) | User | Before 3rd wheel re-upload becomes annoying |
| Coherent ingest scope in 01_bronze_ingest | Full 1,278 patients (~10 min, ~14 GB OneLake) / Stratified sample (~200 patients, ~1 min) | User + Claude | Before authoring 01 |
| Portfolio video timing | Record after notebook 05 / after Phase 4 complete / after Phase 7 (Power BI) | User | Open since Session 4 — Fabric trial sets the floor (~14 days) |

## Blockers / waiting-on

- **`git remote add origin`** — no GitHub remote configured; Fabric Git Integration cannot connect until this is done. User action.
- **First green run of `00_setup.ipynb` in Fabric** — pending Git Integration; will reveal any `FabricPlatform` adjustments before notebook 01 is authored.

## First task for next session

Run `00_setup.ipynb` in the Fabric workspace (after git remote + Git Integration are wired); if all 4 gates pass, ping me to author `01_bronze_ingest.ipynb`. If any gate fails, paste the failing cell's stack trace and which gate.
