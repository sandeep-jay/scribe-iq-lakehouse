# Changelog

All notable changes to scribe-iq-lakehouse.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)

## [Unreleased]

### Session 3 — DICOM ingest + imaging header extraction (ADR-013)
#### Added
- `local/ingest/dicom_index.py`: `DicomIndex` maps DICOM `StudyInstanceUID` → local `.dcm`
  path (the FHIR↔DICOM join key) and serves bytes; `study_uid_from_filename()` parses the
  Coherent file-name convention. File names embed patient names → never logged raw (ADR-010).
- `local/ingest/download.py`: `download_assets()` + CLI `--with-dicom` / `--with-csv` /
  `--assets-only` sync the DICOM (~9.3 GiB, 298 files) and CSV (~466 MB) prefixes into Bronze,
  writing `_metadata/assets_manifest.json`. CSV is landed for reference; not otherwise processed.
- `tests/test_dicom_extraction.py`: 11 tests (synthetic in-memory DICOM, no committed binary)
  — UID linkage, placeholder→null, DA-date formatting, FHIR-authoritative modality, DicomIndex,
  the parse_bundle resolver path, bad-bytes resilience. 114 tests total.
- ADR-013: DICOM ingest, FHIR↔DICOM linkage by StudyInstanceUID, header extraction semantics.
#### Changed
- `fhir_parser.py`: `parse_bundle(bundle, dicom_resolver=...)` injects DICOM bytes via a
  callback (parser stays pure — I/O lives in the ingest layer); `imaging_study_uid()` helper;
  `_extract_dicom_headers` normalizes Coherent placeholder tokens (`UNKNOWN`…) → null and DICOM
  `DA` dates → ISO; FHIR stays authoritative for `modality`; `dicom_binary_id` = StudyInstanceUID
  (never the patient-named file); a malformed file is caught per-study (`dicom_extracted=False`).
- `local/pipeline.py`: builds a `DicomIndex` once and threads the resolver through `_parse_cohort`.
- `tests/fixtures/sample_bundle.json`: ImagingStudy now carries a real `urn:oid:` identifier.
#### Full-run result
- 298 of 3,752 `silver.imaging_study` rows enriched with DICOM `rows`/`columns`/
  `slice_thickness_mm`/`study_date`; Gold `imaging` struct surfaces `study_date` +
  `dicom_binary_id` for those encounters. Descriptive tags are placeholder `UNKNOWN` → null
  (honest limitation, documented in CORPUS_CONTRACT). Clean full rebuild: Silver 2m19s + Gold ~5s.
#### Note
- delta-rs MERGE errors on a whole-table re-update (every source row matches); full re-runs
  build from a clean slate (`rm -rf data/silver data/gold`). MERGE upsert remains for
  incremental per-cohort landing. Recorded as a pipeline operational note.

### Session 3 — Gold layer + corpus contract (ADR-012)
#### Added
- `local/gold/encounter_summary.py`: pure transform denormalizing all 10 Silver tables →
  `gold.encounter_summary` (one row per encounter). Polars join/aggregation engine; output
  assembled against an explicit `GOLD_SCHEMA` (nested struct vitals/imaging/versions + array
  conditions/meds/labs). Deterministic `summary_id` (UUIDv5 of encounter_id); BP parsed from
  Silver `components_json`; anniversary-based age-at-encounter. Defines the corpus contract
  (`CONTRACT_VERSION`, `REQUIRED_FIELDS`, `OPTIONAL_FIELDS`).
- `local/gold/corpus_manifest.py`: lineage manifest — contract version, per-Silver row
  counts + Delta versions, platform, and corpus coverage stats.
- `scripts/gen_corpus_schema.py` + `schemas/gold_encounter_summary.json`: machine-readable
  JSON Schema (Draft 2020-12) generated from `GOLD_SCHEMA` (`--check` for CI); never hand-edited.
- `docs/CORPUS_CONTRACT.md`: human contract — required/optional guarantees, real corpus
  coverage, honest limitations (encounter-grain sparsity, ECG=0, synthetic genomics), semver
  versioning policy.
- `tests/test_gold_encounter_summary.py`: 17 tests — schema/grain, age, vitals (BP from
  components), labs, null-safe optional context, idempotent summary_id, manifest stats,
  contract field-list coverage, JSON Schema currency, and per-row validation against the
  published JSON Schema (`jsonschema`). 103 tests total.
- ADR-012: Gold engine (Polars pure transform), grain, `silver_versions` lineage, contract integrity.
#### Changed
- `local/pipeline.py`: added `build_gold()` + CLI flags `--with-gold` / `--gold-only`.
- Platform interface: `table_version(layer, table)` (delta-rs `version()` on `local_lite`,
  `None` default on base) and `write_gold_manifest()`; `local_lite` also gained `read_gold()`.
- `pyproject.toml`: `jsonschema>=4.0` added to `[dev]` for corpus-contract validation.
#### Enforcement
- `.pre-commit-config.yaml`: read-only `corpus-schema-current` hook
  (`gen_corpus_schema.py --check`); `/session-end` doc-sync now regenerates the corpus schema.
#### Full-run result
- 1,278 patients → **143,946** `gold.encounter_summary` rows in **~5s** (M1 Max), nested
  Delta types + CDC verified; manifest written to `gold/_metadata/corpus_manifest.json`.

### Documentation — generated-first (ADR-011)
#### Added
- `scripts/gen_data_dictionary.py`: renders `docs/DATA_DICTIONARY.md` from the registry
  schemas + validation rules (`--check` mode for CI); never hand-edited.
- `docs/DATA_DICTIONARY.md`: generated — all 10 Silver tables + `ingest_log`.
- `docs/ARCHITECTURE.md`: as-built view (Mermaid diagram + done-vs-planned status table),
  distinct from the spec's intent.
- `docs/BENCHMARKS.md`: real Session 2 run metrics (1,280 bundles → Silver in 2m30s,
  per-table row counts) + engine comparison matrix.
- `tests/test_docs_generated.py`: doc-as-test — fails if DATA_DICTIONARY is stale (86 total).
- ADR-011: Generated-first documentation.
#### Enforcement
- `/session-end` command + CLAUDE.md protocol: added a "Sync the docs" step (regenerate
  DATA_DICTIONARY; update ARCHITECTURE/BENCHMARKS/CORPUS_CONTRACT by judgment; never
  bulldoze hand-written docs).
- `.pre-commit-config.yaml`: local `data-dictionary-current` hook runs
  `gen_data_dictionary.py --check` — read-only, fails the commit on drift, never writes.
#### Deferred
- `docs/CORPUS_CONTRACT.md` + its schema-conformance test → built with the Gold layer
  (a contract test is only meaningful once `gold.encounter_summary` exists).

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
