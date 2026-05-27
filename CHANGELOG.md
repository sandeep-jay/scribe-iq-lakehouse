# Changelog

All notable changes to scribe-iq-lakehouse.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)

## [Unreleased]

### Post-Session-2 hardening
#### Security
- `local/redaction.py`: `redact()` → non-reversible `ref:<hash>` for identifier-bearing
  values. Applied to "skipping unreadable bundle" warnings in `pipeline.py` and
  `local_lite.py`, which previously logged Synthea filenames embedding patient name + UUID
  (ADR-010). 4 redaction tests added (83 total).
- `fhir_parser.py`: per-bundle DEBUG summary logs counts only (no identifiers) + explicit
  logging-policy note in the module docstring.
#### Changed
- Split Claude Code settings: tracked `.claude/settings.json` trimmed to curated allow
  globs + deny + hooks (hook command now uses `$CLAUDE_PROJECT_DIR`, portable); personal/
  auto-approved permissions moved to gitignored `.claude/settings.local.json`.

### Session 2 — Local Bronze + Silver pipeline (full dataset)
#### Added
- `local/ingest/download.py`: parallel `aws s3 sync` (no-sign-request) + round-robin
  cohort partitioning (A/B/C) + ingest manifest
- `local/platform/local_lite.py`: `LocalLitePlatform` (Polars + delta-rs) — Delta
  write/read, CDC enabled on create, MERGE-upsert on primary key
- `local/transforms/schema_utils.py`: field-type-driven Arrow coercion (UTC timestamps,
  date32, string codes) + dedup
- `local/transforms/silver_{patient,encounter,clinical,soap_notes,ecg,imaging,genomics}.py`
  and `registry.py` — all 10 Silver tables with explicit Arrow schemas (ADR-004)
- `local/validation/{schema_registry,validate}.py`: per-table quality checks →
  `silver.ingest_log`
- `local/ingest/{bronze_landing,streaming_sim}.py`: cohort inventory + Auto Loader replay sim
- `local/pipeline.py`: per-cohort micro-batch Bronze→Silver orchestration
- 36 new tests (schema_utils, silver transforms, local_lite Delta round-trip, validation) —
  79 total, all passing
- ADR-009: Local Silver materialization (delta-rs, type coercion, component JSON)
- venv + full `[local,dev]` extras (polars, deltalake, duckdb, watchdog, pydicom)
#### Results
- Full run: 1,280 files (1,278 patients + `organizations.json` + `practitioners.json`)
  → all 10 Silver Delta tables in **2m30s** on M1 Max, all validations passed.
  Row counts: encounter 143,946 · observation 669,898 · medication_request 209,401 ·
  procedure 56,092 · soap_note 143,946 · condition 15,956 · imaging_study 3,752 ·
  genomic_report 419 · patient 1,278 · ecg_metadata 0 (ECG is Binary waveform, not FHIR).
- CDC (`delta.enableChangeDataFeed`) enabled on every Silver table.

### Session 1 — Repo scaffold + FHIR parser
#### Added
- Repo scaffold per spec §4: `pyproject.toml`, `requirements.txt`, `local/` package
  tree (`platform`, `transforms`, `ingest`, `gold`, `validation`), `tests/`, README stub
- `local/platform/base.py`: `LakehousePlatform` abstract interface (ADR-002)
- `local/platform/factory.py`: `LAKEHOUSE_PLATFORM` env-var router (default `local_lite`)
- `local/transforms/fhir_parser.py`: `FHIRBundleParser` — extract_patient, encounter,
  condition, observation (scalar + component), medication_request, procedure, soap_note
  (Base64 decode + S/O/A/P section detection), ecg_metadata, imaging_study (FHIR + DICOM
  passes), genomic_report; `strip_reference` handles `urn:uuid:`/`Type/id` forms
- `tests/fixtures/sample_bundle.json`: synthetic 17-resource bundle covering every type
- `tests/test_fhir_parser.py`, `tests/test_silver_soap_notes.py`,
  `tests/test_platform_factory.py`, `tests/conftest.py` — 43 tests, all passing
- ADR-008: Dict-based FHIR parsing (not fhir.resources models)
### Changed
- end-of-file-fixer normalized trailing newlines across .claude/ files
### Notes
- Parser validated against a real Coherent bundle (in gitignored `data/`); SOAP notes use
  Markdown clinical headers, not literal SOAP markers, and lack an Objective section —
  `has_objective` is honestly `False` for most Coherent notes (see ADR-008).
- pre-commit auto-install conflicts with Claude Code global core.hooksPath;
  use `pre-commit run --all-files` manually or rely on CI. See HANDOFF.md caveat.

## [0.1.0] — 2026-05-27
### Added
- Claude Code configuration: CLAUDE.md, HANDOFF.md, session protocol
- Global ~/.claude/CLAUDE.md with developer context and security rules
- ~/claude-os/ skill library: 7 skills, templates, init.sh
- .claude/skills/: healthcare-data, delta-patterns, mlops
- .claude/commands/: session-end, new-transform, new-adr, session-start
- .claude/rules/: transforms.md, notebooks.md (path-scoped context)
- .claude/hooks/scan-secrets.sh: pre-write secret detection
- docs/adr/README.md: ADR index
- ADR-001: Fabric-first development approach
- ADR-002: Platform abstraction layer design
- ADR-003: Polars + DuckDB for local lite tier
- ADR-004: Arrow as transform interchange format
- ADR-005: FHIR Binary Base64 decode for SOAP notes
- ADR-006: DICOM stop_before_pixels metadata extraction
- ADR-007: Genomic data_limitation as first-class column
- .pre-commit-config.yaml: detect-secrets, gitleaks, bandit, semgrep
- .semgrep/healthcare.yml: OneLake path and PHI logging rules
