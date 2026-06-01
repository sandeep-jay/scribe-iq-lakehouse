# ADR Index — scribe-iq-lakehouse

!!! note "Path note"
    ADRs are immutable, dated records. Those written before **2026-05-28** cite the original
    `local/` package; the multi-platform reorg ([ADR-017](017-multi-platform-repo-layout.md))
    renamed that tree to `core/` (and `local/pipeline.py` → `core/surfaces/cli/pipeline.py`).
    Older ADRs are not retro-edited — see ADR-017 for the mapping.

| # | Title | Status | Date |
|---|-------|--------|------|
| 001 | Fabric-first development approach | Accepted | 2026-05-27 |
| 002 | Platform abstraction layer design | Superseded by 022 → [_archive](../_archive/adr/002-platform-abstraction.md) | 2026-05-27 |
| 003 | Polars + DuckDB for local lite tier | Accepted | 2026-05-27 |
| 004 | Arrow as transform interchange format | Superseded by 022 → [_archive](../_archive/adr/004-arrow-interchange.md) | 2026-05-27 |
| 005 | FHIR Binary Base64 decode for SOAP notes | Accepted | 2026-05-27 |
| 006 | DICOM stop_before_pixels metadata extraction | Accepted | 2026-05-27 |
| 007 | Genomic data_limitation as first-class column | Accepted | 2026-05-27 |
| 008 | Dict-based FHIR parsing (not fhir.resources models) | Accepted | 2026-05-27 |
| 009 | Local Silver materialization — delta-rs, type coercion, component JSON | Accepted | 2026-05-27 |
| 010 | PHI-safe logging via redaction | Accepted | 2026-05-27 |
| 011 | Generated-first documentation | Accepted | 2026-05-27 |
| 012 | Gold encounter_summary — engine, grain, lineage | Accepted | 2026-05-27 |
| 013 | DICOM ingest, FHIR↔DICOM linkage, header extraction | Accepted | 2026-05-27 |
| 014 | Problem-list-as-of-date for active_conditions/medications | Accepted | 2026-05-27 |
| 015 | Dagster for local pipeline orchestration | Accepted | 2026-05-27 |
| 016 | Medallion as a Dagster software-defined asset graph | Accepted | 2026-05-27 |
| 017 | Multi-platform repo layout — `core/` + per-platform domains | Accepted (amended 2026-05-29) | 2026-05-28 |
| 018 | Monorepo CI/CD — `core/` as a wheel, per-platform deploy workflows | Accepted | 2026-05-28 |
| 019 | Silver MERGE idempotency — pre-merge target dedup guard | Accepted | 2026-05-28 |
| 020 | Fabric distributed parsing via `applyInPandas` | Superseded by 022 → [_archive](../_archive/adr/020-fabric-distributed-parsing.md) | 2026-05-29 |
| 021 | Fabric `.Notebook/` as notebook source of truth (drop `.ipynb`) | Accepted | 2026-05-29 |
| 022 | Independent per-platform implementations | Accepted | 2026-05-29 |
