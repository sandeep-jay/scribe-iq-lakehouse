# Changelog

All notable changes to scribe-iq-lakehouse.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)

## [Unreleased]
### Added
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
