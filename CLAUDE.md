# scribe-iq-lakehouse — Claude Code Configuration

## Project
Production-pattern healthcare data lakehouse on Synthea Coherent (~1,500 synthetic
patients). Fabric-first medallion: Bronze → Silver → Gold. Feeds scribe-iq (RAG)
and clinical-bert-pipeline (NLP). Ollama generation pipeline produces patient-linked
clinical dialogues from Gold encounter_summary.

## Execution context
- Paid Fabric capacity active — Fabric notebooks are the priority surface
- Fabric notebooks instantiate `FabricPlatform()` directly (no factory, no env var) — ADR-022
- Local CLI / Dagster path uses LAKEHOUSE_PLATFORM (default `local_lite`) — factory dispatches local surfaces only
- Capture Fabric screenshots as you go (trial-window evidence)

## System context
Consumes: s3://synthea-open-data/coherent/ (open data, no credentials needed)
Produces: gold.encounter_summary → scribe-iq (RAG) + clinical-bert-pipeline (NLP)
          gold.synthetic_dialogue → Ollama generation pipeline
Upstream of: scribe-iq (replaces 19-patient dev corpus with 1,500-patient corpus)

## Architecture principles
- Two top-level domains today: `core/` (LocalLite execution + Dagster + CLI)
  and `fabric/` (Spark-native end-to-end on Microsoft Fabric). Future
  Databricks and AWS land as siblings to `fabric/`. See ADR-017, ADR-022,
  docs/roadmap/multi-platform-reorg.md
- **Independent per-platform implementations (ADR-022).** Each tier owns
  its complete Silver + Gold + validation stack written for its engine
  native:
  - `core/transforms/` returns `pa.Table` (Polars + delta-rs path)
  - `fabric/transforms/` returns Spark DataFrames (Spark + Delta path)
  - Cross-tier compatibility is by schema parity + lockstep
    `CONTRACT_VERSION` bumps, not by code sharing.
- One-way dependency on shared utilities only: cloud tiers may import
  narrow utilities (e.g. `core.redaction`) from the wheel; cloud tiers
  do NOT import transform / Gold / validation logic from `core/`. `core/`
  never imports from `fabric/`, `databricks/`, or `aws/`.
- `core/` ships as a versioned wheel that includes `fabric/` (ADR-018);
  Fabric Environment installs the wheel and imports only `fabric.*`.
- Pure transforms: no file paths, no platform imports anywhere under
  `core/transforms/` or `fabric/transforms/`.
- Every notebook follows the cell-by-cell template in
  `.claude/rules/notebooks.md`.
- Full spec: docs/roadmap/scribe-iq-lakehouse-spec.md

## Non-negotiables
1. Never hardcode OneLake paths — always platform.storage_path()
2. Never import Spark / notebookutils / delta in `core/transforms/` (LocalLite stays pure-Python)
3. Never import `core.transforms.*` or `core.gold.*` from `fabric/` — independent impl (ADR-022)
4. `core/` never imports from `fabric/`, `databricks/`, or `aws/` (one-way)
5. Every new transform gets a test alongside implementation (`core/tests/` for core, `fabric/tests/` for fabric)
6. Every architectural decision gets an ADR in docs/adr/
7. CDC enabled on all Silver tables: delta.enableChangeDataFeed = true
8. data_limitation column always populated in silver.genomic_report (ADR-007)
9. pydicom stop_before_pixels=True everywhere it appears — never load pixel data (ADR-006)
10. No credentials in notebooks — Fabric Environment Variables or Key Vault only
11. Session ends with updated HANDOFF.md
12. Logs never contain raw patient/encounter identifiers or bundle filenames —
    redact identifier-bearing values via core.redaction.redact() (ADR-010)
13. Cross-platform schema parity — `core.gold.encounter_summary.CONTRACT_VERSION`
    and `fabric.gold.encounter_summary.CONTRACT_VERSION` bump in lockstep on any
    Gold change (ADR-022)

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
  docs/roadmap/multi-platform-reorg.md       Repo layout + CI/CD model (ADR-017/018)
  docs/roadmap/fabric-execution-plan.md      Session 5 plan — Fabric end-to-end + dedup fix + Power BI
  docs/adr/                                  ADRs — read before touching architecture
  HANDOFF.md                                 Current session state (updated every session)
  CHANGELOG.md                               All meaningful changes
  core/platform/base.py                      LakehousePlatform ABC (LocalLite only post-ADR-022)
  core/platform/local_lite.py                LocalLitePlatform (Polars + delta-rs)
  core/transforms/                           LocalLite Silver builders (pure Python, return pa.Table)
  core/transforms/registry.py                LocalLite Silver registry
  core/gold/encounter_summary.py             LocalLite Gold builder + CONTRACT_VERSION
  core/validation/                           LocalLite validation rules + checks → ingest_log
  core/orchestration/dagster/                Dagster asset graph (local-only, ADR-015/016)
  core/surfaces/cli/pipeline.py              Local Bronze → Silver → Gold CLI orchestration
  core/redaction.py                          PHI-safe log references (ADR-010)
  fabric/platform.py                         FabricPlatform (Spark-native, independent — ADR-022)
  fabric/transforms/                         Fabric Silver builders (return Spark DataFrame, from_json + BUNDLE_SCHEMA)
  fabric/transforms/registry.py              Fabric Silver registry
  fabric/gold/encounter_summary.py           Fabric Gold builder + CONTRACT_VERSION (match core's)
  fabric/validation/                         Fabric Silver validation (single .agg() per table)
  fabric/notebooks/                          Fabric execution notebooks (00 + 02–10, .Notebook/ format — ADR-021)
  fabric/deploy/                             fabric-cicd config + wheel upload helpers
  docs/adr/                                  Active ADRs (read before touching architecture)
  docs/_archive/adr/                         Superseded ADRs (002/004/020 → ADR-022)
  HANDOFF.md                                 Current session state
  CHANGELOG.md                               All meaningful changes
  .github/workflows/                         core-build · core-pr-tests · fabric-deploy (manual)

## Claude Code config
  .claude/settings.json        Tracked: curated allow globs + deny + hooks (portable paths)
  .claude/settings.local.json  Gitignored: personal/auto-approved permissions (machine-specific)

## Screenshot capture priority (Fabric trial window)
If time is tight: capture screenshots BEFORE polishing notebooks.
Evidence of a running system matters more than polished code not captured.
Checklist: docs/roadmap/scribe-iq-lakehouse-spec.md section 15.2

## Skills available (lazy-loaded from .claude/skills/)
/healthcare-data  — FHIR handling, Base64 decode, DICOM, Synthea limitations
/delta-patterns   — Delta Lake, CDC, medallion rules, MERGE patterns, streaming
/session-end      — Full end-of-session protocol
/new-transform    — Scaffold transform + test + Fabric notebook
/new-adr          — Write ADR with template, update index
