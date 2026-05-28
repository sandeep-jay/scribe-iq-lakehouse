# scribe-iq-lakehouse — Claude Code Configuration

## Project
Production-pattern healthcare data lakehouse on Synthea Coherent (~1,500 synthetic
patients). Fabric-first medallion: Bronze → Silver → Gold. Feeds scribe-iq (RAG)
and clinical-bert-pipeline (NLP). Ollama generation pipeline produces patient-linked
clinical dialogues from Gold encounter_summary.

## Developer context
- M1 Max 32GB (M5 Max 128GB arriving June 2026)
- Fabric trial: ~15 days remaining — Fabric notebooks are priority
- LAKEHOUSE_PLATFORM env var controls execution environment
- Active job hunt — capture Fabric screenshots before trial expires

## System context
Consumes: s3://synthea-open-data/coherent/ (open data, no credentials needed)
Produces: gold.encounter_summary → scribe-iq (RAG) + clinical-bert-pipeline (NLP)
          gold.synthetic_dialogue → Ollama generation pipeline
Upstream of: scribe-iq (replaces 19-patient dev corpus with 1,500-patient corpus)

## Architecture principles
- Two top-level domains: `core/` (platform-agnostic + local execution surface)
  and `fabric/` (Fabric-specific impl + notebooks + deploy). Future Databricks
  and AWS land as siblings to `fabric/`. See ADR-017, docs/roadmap/multi-platform-reorg.md
- One-way dependency: `fabric/` → `core/`. `core/` NEVER imports from `fabric/`,
  `databricks/`, or `aws/`. Enforced by transform/notebook rules below.
- `core/` is a versioned wheel — every platform tier consumes it as a library,
  not source files (ADR-018).
- Platform abstraction: ALL cloud I/O via core/platform/ — never directly in transforms/
- Arrow interchange: transforms return pa.Table, never Spark DataFrames or Polars frames
- Pure transforms: no file paths, no platform imports in core/transforms/
- Notebooks import from core.transforms — zero duplicate logic
- Every notebook follows the 8-cell documentation template
- Full spec: docs/roadmap/scribe-iq-lakehouse-spec.md

## Non-negotiables
1. Never hardcode OneLake paths — always platform.storage_path()
2. Never import Fabric/Spark in core/transforms/
3. `core/` never imports from `fabric/`, `databricks/`, or `aws/` (one-way dependency)
4. Every new transform gets a test in core/tests/ alongside implementation
5. Every architectural decision gets an ADR in docs/adr/
6. CDC enabled on all Silver tables: delta.enableChangeDataFeed = true
7. data_limitation column always populated in silver.genomic_report
8. pydicom stop_before_pixels=True everywhere — never load pixel data
9. No credentials in notebooks — Fabric Environment Variables or Key Vault only
10. Session ends with updated HANDOFF.md
11. Logs never contain raw patient/encounter identifiers or bundle filenames —
    redact identifier-bearing values via core.redaction.redact() (ADR-010)

## Session protocol
START: Read HANDOFF.md → state current status in 3 sentences → begin first task
END:   HANDOFF.md (state-only, ~150 line ceiling, 5 sections — narrative goes in CHANGELOG) →
       CHANGELOG.md → sync docs (regen DATA_DICTIONARY; update ARCHITECTURE/BENCHMARKS/
       CORPUS_CONTRACT if changed) → pytest → pending ADRs → commit
       Generated docs: only core/scripts/gen_data_dictionary.py writes (one file); pre-commit
       `--check` is read-only. Never hand-bulldoze a doc — surface conflicts (ADR-011).

HANDOFF/CHANGELOG boundary: HANDOFF owns "where we are + what's next" (current state · next
task · open decisions · blockers · first task for next session — that's it). CHANGELOG owns
"what happened" (Added / Changed / Tests, by session). Never re-narrate session work in
HANDOFF; never keep prior-session sub-sections there. If you're writing narrative in HANDOFF,
it belongs in CHANGELOG.

## Key files
  docs/roadmap/scribe-iq-lakehouse-spec.md   Full implementation spec
  docs/roadmap/MASTER_PLAN.md                Cross-repo weekend execution plan
  docs/roadmap/multi-platform-reorg.md       Repo layout + CI/CD model (ADR-017/018)
  docs/roadmap/fabric-execution-plan.md      Session 5 plan — Fabric end-to-end + dedup fix + Power BI
  docs/adr/                                  ADRs — read before touching architecture
  HANDOFF.md                                 Current session state (updated every session)
  CHANGELOG.md                               All meaningful changes
  core/platform/base.py                      Platform abstraction interface (ADR-002)
  core/platform/local_lite.py                LocalLitePlatform (Polars + delta-rs)
  core/transforms/                           Engine-agnostic transform logic (pure Python)
  core/transforms/registry.py                Silver table → schema/key/build mapping
  core/validation/                           Schema registry + quality checks → ingest_log
  core/orchestration/dagster/                Dagster asset graph (local-only, ADR-015/016)
  core/surfaces/cli/pipeline.py              Local Bronze → Silver → Gold CLI orchestration
  core/redaction.py                          PHI-safe log references (ADR-010)
  fabric/platform.py                         FabricPlatform (consumes core via wheel)
  fabric/notebooks/                          Fabric execution notebooks (00–10)
  fabric/deploy/                             fabric-cicd config + wheel upload helpers
  .github/workflows/                         core-build · core-pr-tests · fabric-deploy

## Claude Code config
  .claude/settings.json        Tracked: curated allow globs + deny + hooks (portable paths)
  .claude/settings.local.json  Gitignored: personal/auto-approved permissions (machine-specific)

## Screenshot capture priority (Fabric trial ~15 days remaining)
If time is tight: capture screenshots BEFORE polishing notebooks.
Evidence of a running system matters more than polished code not captured.
Checklist: docs/roadmap/scribe-iq-lakehouse-spec.md section 15.2

## Skills available (lazy-loaded from .claude/skills/)
/healthcare-data  — FHIR handling, Base64 decode, DICOM, Synthea limitations
/delta-patterns   — Delta Lake, CDC, medallion rules, MERGE patterns, streaming
/mlops            — MLflow, DVC, eval_report.json, M1 Max training config
/session-end      — Full end-of-session protocol
/new-transform    — Scaffold transform + test + Fabric notebook
/new-adr          — Write ADR with template, update index
