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
- Platform abstraction: ALL cloud I/O via local/platform/ — never directly in transforms/
- Arrow interchange: transforms return pa.Table, never Spark DataFrames or Polars frames
- Pure transforms: no file paths, no platform imports in local/transforms/
- Notebooks import from local/transforms/ — zero duplicate logic
- Every notebook follows the 8-cell documentation template
- Full spec: docs/roadmap/scribe-iq-lakehouse-spec.md

## Non-negotiables
1. Never hardcode OneLake paths — always platform.storage_path()
2. Never import Fabric/Spark in local/transforms/
3. Every new transform gets a test in tests/ alongside implementation
4. Every architectural decision gets an ADR in docs/adr/
5. CDC enabled on all Silver tables: delta.enableChangeDataFeed = true
6. data_limitation column always populated in silver.genomic_report
7. pydicom stop_before_pixels=True everywhere — never load pixel data
8. No credentials in notebooks — Fabric Environment Variables or Key Vault only
9. Session ends with updated HANDOFF.md

## Session protocol
START: Read HANDOFF.md → state current status in 3 sentences → begin first task
END:   HANDOFF.md → CHANGELOG.md → pytest → pending ADRs → commit

## Key files
  docs/roadmap/scribe-iq-lakehouse-spec.md   Full implementation spec
  docs/roadmap/MASTER_PLAN.md                Cross-repo weekend execution plan
  docs/adr/                                  ADRs — read before touching architecture
  HANDOFF.md                                 Current session state (updated every session)
  CHANGELOG.md                               All meaningful changes
  local/platform/base.py                     Platform abstraction interface
  local/transforms/                          Engine-agnostic transform logic (pure Python)
  fabric/notebooks/                          Fabric execution notebooks (00-10)

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
