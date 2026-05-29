# HANDOFF — Session 5 · Fabric Spark-native rewrite (ADR-022)
**Date:** 2026-05-29 · **Branch:** `feat/fabric-spark-native` · **Plan:** [docs/roadmap/fabric-execution-plan.md](docs/roadmap/fabric-execution-plan.md)

> State only. For what happened in this (or any prior) session see [CHANGELOG.md](CHANGELOG.md).
> Narrative belongs there, not here.

## Current state

Branch `feat/fabric-spark-native` is 7 commits ahead of `main` and contains
the full pure-Spark Fabric rewrite per ADR-022: `fabric/transforms/`
(bundle_schema + 10 Spark-native Silver builders + registry),
`fabric/gold/` (Spark-native encounter_summary + corpus_manifest),
`fabric/validation/` (single-`.agg()`-per-table), slimmed
`fabric/platform.py` (Spark-only, no PyArrow wrappers, no ABC inheritance),
deleted `fabric/spark_helpers.py`, rewritten notebooks 00 + 02–10, ADRs
002/004/020 archived under `docs/_archive/adr/` with `ADR-022` adopted, and
CLAUDE.md / `.claude/rules/` updated. Tests pass: 128 passed + 1 skipped
(workspace-only). Branch is unpushed; user explicitly held PR opening for
review.

## Next task

**User review of branch `feat/fabric-spark-native`, then push + open PR.**

```bash
git log --oneline main..feat/fabric-spark-native   # 7 commits to review
git diff main..feat/fabric-spark-native -- fabric/ docs/adr/ CLAUDE.md
# After approval:
git push -u origin feat/fabric-spark-native
gh pr create --title "Fabric Spark-native rewrite (ADR-022)" --body "..."
```

The first cloud-side smoke after merge is `00_setup` in the Fabric
workspace, then 02 (smallest table — fast feedback on the Spark
`from_json` path before running 04/05 which fan out to more tables).

## Open decisions

| Decision | Options | Owner | Due |
|---|---|---|---|
| Coherent ingest scope for `01_bronze_ingest` | Full ~1,278 patients (~10 min, ~14 GB OneLake) / Stratified ~200 patients (~1 min) | User + Claude | Before authoring 01 |
| Service Principal registration | Skip (manual UI uploads forever) / Register (enables REST + CI) | User | Before 3rd wheel re-upload becomes annoying |

## Blockers / waiting-on

- **User review of `feat/fabric-spark-native`** before PR is opened
  (explicit instruction: *"don't push a PR without my say so"*).

## First task for next session

After PR merges, run `00_setup` in the Fabric workspace; if all 4 gates
pass, smoke-run notebook 02 against a small cohort to verify the Spark
`from_json + BUNDLE_SCHEMA` path produces non-empty `silver.patient`.
