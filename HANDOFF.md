# HANDOFF — Documentation site built; Fabric Session 5 awaiting review
**Date:** 2026-05-31 · **Branch:** `feat/docs-site` (off `feat/fabric-spark-native`)

> State only. For what happened in this (or any prior) session see [CHANGELOG.md](CHANGELOG.md).
> Narrative belongs there, not here.

## Current state

The **MkDocs Material documentation site is built and `mkdocs build --strict` is green**
(zero warnings) on `feat/docs-site`. 8 new narrative pages (Home, Reviewer Guide, Case
Study, Design Notes, Responsible Data, Engine Parity, Portfolio, About) + 6 mermaid
diagrams (both engine-native tiers D2/D3 + the ADR-022 parity D4); all existing docs
(ARCHITECTURE, 19 ADRs, CORPUS_CONTRACT, BENCHMARKS, RUNBOOK, DATA_DICTIONARY, PLAYBOOK)
wired into nav after a **doc-consistency pass** that reconciled the reviewer-facing docs
to ADR-017/022 (architecture story, `local/`→`core/` paths, broken ADR links, Fabric
status, counts). `.github/workflows/docs.yml` deploys to GitHub Pages (Pages-artifact,
gated on the generated-doc `--check`s + strict build). Tests green (128 passed, 1 skipped);
generated-doc gates pass.

`feat/docs-site` is **based on `feat/fabric-spark-native`** (12 Fabric commits, still
**awaiting user review before any PR** — standing instruction), so the docs branch
currently also carries the Fabric Session 5 work.

## Next task

1. **Maintainer one-time:** GitHub → Settings → Pages → Source = **GitHub Actions**
   (required before the first deploy; the workflow is push-to-`main` + `workflow_dispatch`).
2. Choose the docs-site merge path (see open decisions).
3. Resume Fabric demo deliverables (Data Factory pipeline, Power BI Direct Lake) — carried
   over from Session 5.

## Open decisions

| Decision | Options | Owner |
|---|---|---|
| Docs-site merge path | (a) fold into the Fabric PR → `main` · (b) rebase docs-only onto `main` as a standalone PR | User |
| Pre-existing ruff debt (20 findings: `fabric/*`, `core/orchestration/*`) | fix as part of the Fabric PR — out of scope for docs | User |
| Full-corpus Fabric re-run before PR | Yes (~1,278 bundles, more impressive numbers) / No (stay on 100-sample) | User |
| Power BI report scope | Minimal (3 cards + chart) / Richer (drillthrough) | User |

## Blockers / waiting-on

- **User review of `feat/fabric-spark-native`** before opening any PR (standing "don't push
  a PR without my say so").
- GitHub Pages source setting (manual, one-time) before the first site deploy.

## First task for next session

Set Pages source = GitHub Actions; then decide the docs-site merge path. If standalone,
rebase the docs changes onto `main` and open `feat/docs-site → main`; otherwise fold them
into the Fabric PR.
