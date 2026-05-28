# Changelog

All notable changes to scribe-iq-lakehouse.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)

## [Unreleased]

### Session 5 (in progress) — Fabric end-to-end + dedup fix + Power BI
Plan: [docs/roadmap/fabric-execution-plan.md](docs/roadmap/fabric-execution-plan.md). 7 phases; 1–3 complete, 4 in progress.

#### Added
- **ADR-019** (Silver MERGE idempotency) — pre-merge target-side dedup guard
  in `LocalLitePlatform._write_delta`. Fixes the *"matched a target row with
  multiple source rows"* failure that occurred re-merging into Silver tables
  written before `dedup_by_key()` was added to every `build_silver_*`. Helpers
  `_duplicate_row_count` + `_dedup_target` are pyarrow-only; only triggers a
  rewrite when total ≠ distinct on the PK. Survivor semantics are
  "some-survivor-wins" (Delta doesn't preserve write order on read) — the
  following MERGE writes the source's canonical value on top.
- Regression test `test_merge_dedupes_target_with_legacy_duplicates` in
  `core/tests/test_local_lite.py` — writes intentionally-duplicate target via
  raw `write_deltalake`, asserts subsequent `write_silver` MERGE succeeds with
  canonical source value winning.
- Real `FabricPlatform` implementation in `fabric/platform.py` (10 methods
  replacing the Session 4.5 NotImplementedError stubs): schema-enabled OneLake
  abfss URIs, pa.Table↔Spark round-trip via pandas, `DeltaTable.merge()` with
  matching ADR-019 dedup guard (Spark equivalent: `dropDuplicates([pk])`),
  CDC enabled on all writes, manifest via `mssparkutils.fs.put`. All
  Fabric-runtime imports (`pyspark`, `notebookutils`, `delta.tables`) are
  lazy inside method bodies — module imports cleanly outside Fabric so the
  offline contract tests run without Fabric.
- Real `fabric/deploy/upload_wheel.py` — MSAL Service Principal → Fabric REST
  v1 client. PUT `/workspaces/{ws}/environments/{env}/staging/libraries`,
  POST `/publish`, then poll until publish state is `Success` (600 s deadline).
- `[fabric]` install extra (`msal>=1.28`, `requests>=2.31`) in `pyproject.toml`.
- `pytest.mark.fabric` marker for behaviour tests that require a real workspace
  (registered in `[tool.pytest.ini_options]`); 1 such test gated on
  `FABRIC_TENANT_ID` env var.
- 2 new offline contract tests in `fabric/tests/test_fabric_platform.py`:
  `test_storage_path_builds_onelake_uri` + `test_storage_path_rejects_bad_layer`.
  4 → 5 offline tests; obsolete `test_methods_raise_not_implemented` removed.
- `docs/roadmap/fabric-execution-plan.md` — 7-phase Session 5 execution plan,
  linked from `docs/roadmap/MASTER_PLAN.md` and `CLAUDE.md` key files.
- `.env.example` at repo root — canonical FABRIC_* env-var inventory with
  capture instructions and consumer list. `.gitignore` updated with
  `!.env.example` exception so the template tracks while `.env` stays out.
- `boto3>=1.34` to `fabric/environments/lakehouse_env.yml` — anonymous-mode
  S3 client for the public Synthea Coherent bucket, supersedes the original
  S3-shortcut design (Fabric shortcuts require AWS credentials).
- `fabric/notebooks/00_setup.ipynb` — first Phase 4 notebook (4 verification
  gates: wheel imports, Spark + workspace ID, FabricPlatform URI, boto3
  anonymous S3). Follows the 8-cell template with cells 4–8 adapted for
  setup verification (no Delta write, no `silver.ingest_log` row).

#### Changed
- `fabric/docs/DEPLOYMENT.md` — rewritten as a step-by-step operator runbook
  based on the live Phase 3 walkthrough. 6 numbered setup steps with verify
  lines, Path A (manual UI) and Path B (REST automation) wheel-upload paths,
  comprehensive Gotchas section ("Manage access" not in Settings, External
  repositories ≠ Built-in libraries, "+ Add library" stays clickable, SP
  secret shown only once, lakehouse must be schema-enabled, environment
  publish takes 2–5 min).
- `.github/workflows/fabric-deploy.yml` — installs `[fabric]` extra +
  `fabric-cicd`, runs the real `upload_wheel.py`, and invokes
  `fabric-cicd smoke-run` against notebook 05 (replacing the placeholder
  echo from Session 4.5).
- `HANDOFF.md` Open Decisions row "Silver parse-output deduplication" flipped
  to DONE — ADR-019.

#### Provisioned (Session 5 Phase 3)
- Fabric workspace `scribe_iq_lakehouse_fabric` (Central US)
- Schema-enabled lakehouse `scribe_iq_lakehouse_fabric`
- Environment `scribe-iq-lakehouse-env` (Runtime 1.3, Spark 3.5, Delta 3.2)
  with 4 PyPI deps + the `scribe_iq_lakehouse-0.1.0` core wheel published.
- IDs live in local `.env` (gitignored) / GitHub `fabric-prod` secrets — never
  in committed files.

#### Tests
128 passed, 1 skipped (`@pytest.mark.fabric` without `FABRIC_TENANT_ID`). 122
core + 5 fabric offline + 1 fabric behaviour (skipped). All Phase 1–3 code
files ruff + black clean.

### Session 4.5 — Multi-platform repo reorg (`core/` + `fabric/`)
#### Added
- **ADR-017** (multi-platform repo layout) and **ADR-018** (CI/CD monorepo, core as wheel).
- `docs/roadmap/multi-platform-reorg.md` — full planning doc behind the reorg.
- Top-level `fabric/` domain: `platform.py` stub, `notebooks/`, `environments/lakehouse_env.yml`,
  `deploy/{upload_wheel.py,fabric_cicd_config.yml}`, `tests/test_fabric_platform.py`,
  `docs/{DEPLOYMENT.md,SCREENSHOTS.md}`, `scripts/capture_lineage.py`. The stub raises
  `NotImplementedError` on every method so accidental Fabric dispatch fails loudly.
- `.github/workflows/`: `core-pr-tests.yml`, `core-build.yml`, `fabric-deploy.yml` (skeleton);
  `databricks-deploy.yml.disabled` and `aws-deploy.yml.disabled` as visible templates.
- One-way dependency rule (`core/` never imports from any platform tier) enforced by CI grep.

#### Changed
- `local/` → `core/` (via `git mv`, history preserved). `core/` now bundles the
  platform-agnostic kernel + `core/platform/local_lite.py` (LocalLite impl) +
  `core/orchestration/dagster/` + `core/surfaces/cli/pipeline.py` + `core/tests/` +
  `core/scripts/` + `core/docs/`.
- Imports rewritten: `from local.X` → `from core.X` across all Python, docstrings, and
  top-level docs. Factory strings for `fabric`/`databricks`/`aws`/`gcp` now point outside
  `core/` (e.g., `"fabric.platform.FabricPlatform"`).
- `pyproject.toml`: package discovery `["core*", "fabric*"]`; testpaths
  `["core/tests", "fabric/tests"]`; `[tool.dagster] module_name = "core.orchestration.dagster.definitions"`.
- `CLAUDE.md`, `.claude/rules/transforms.md`, `.claude/rules/notebooks.md`: paths and
  cross-domain-import rule updated.
- README: new "Repository layout" section with two-domain tree + "See also" link to the
  separate `fabric-lakehouse-hls-readmission` repo.
- `core/scripts/gen_*.py`: `_REPO_ROOT` climbs one extra level (`parent.parent.parent`)
  now that scripts live one directory deeper.

#### Tests
- 122 core tests still pass; 4 new `fabric/tests/test_fabric_platform.py` contract tests
  verify FabricPlatform subclasses `LakehousePlatform`, implements every abstract method,
  and that every method currently raises `NotImplementedError`. Total: 126 passing.

### Session 4 (cont.) — Demoability polish: data shape visible, not just lineage
#### Added
- `local/preview.py` — new shared Markdown renderer module: `schema_md`, `sample_md`,
  `bundle_resource_counts`, `bundle_summary_md`, `gold_encounter_card`. Pure-Python,
  framework-agnostic; used by both the Dagster asset metadata and the CLI walkthrough.
- `core/scripts/demo_walkthrough.py` (~280 LOC) — one-patient end-to-end medallion tour using
  the `rich` library. Auto-picks an anchor patient with ≥3 conditions, ≥3 meds, ≥1 SOAP
  note (or `--patient-id <uuid>`), then renders Bronze (FHIR resource counts + sample
  Patient JSON) → Parse (records dict) → Silver (patient row + 3-5 encounters /
  observations / conditions / meds + reference schema) → Gold (full SOAP note card with
  active conditions/medications/vitals/imaging). `--pause N` for screencast pacing.
- `docs/demo/notebooks/demo_notebook.sql` — 20-cell DuckDB UI source over the Delta tables:
  corpus headlines · schema · top conditions/medications · demographics · encounter mix ·
  longitudinal span · co-morbidity buckets · vitals percentiles · imaging modalities ·
  one-patient timeline · as-of-date condition evolution · full SOAP note · keyword cohort
  search · coverage stats · lineage (silver_versions struct) · cross-layer condition join ·
  final corpus shape.
- `docs/demo/notebooks/README.md` + `docs/demo/notebooks/demo.duckdb` (gitignored binary,
  pre-built locally) — how to open / regenerate.
- `docs/demo/PLAYBOOK.md` — portfolio-video recording playbook: 5-beat structure (hook /
  raw mess / medallion / transformation / payoff), preflight + window setup, 6-take
  shot list (incl. optional live sensor demo), edit guidance, publishing checklist,
  contingencies.
- `pyproject.toml` `[dev]` extra: added `rich>=13.0` for the walkthrough.
#### Changed
- `core/validation/validate.py` — added `@dataclass CheckOutcome(name, passed, detail)`
  and `ValidationResult.checks: list[CheckOutcome]` + an `ok()` method. Every rule now
  records its outcome (passing + failing alike), with the actual numbers in the detail
  string (e.g. `unique:patient_id` → "1,278/1,278 distinct"). `failed_checks` is kept
  unchanged for `silver.ingest_log` and CLI pipeline compatibility.
- `core/orchestration/dagster/checks.py` — `AssetCheckResult.metadata` now includes `rules_total`,
  `rules_passed`, `rules_failed`, and a `rules` Markdown table (`MetadataValue.md`) so
  clicking a check in the UI shows the full rule-by-rule breakdown, not just a green dot.
- `core/orchestration/dagster/assets.py` — each `MaterializeResult.metadata` now carries rendered data
  shape via `local.preview`:
  - `bronze_fhir` → first bundle's FHIR resource-type breakdown table
  - 10 Silver outputs → schema table + first-5-row Markdown table per partition
  - `gold_encounter_summary` → full schema + a sample-encounter card with the SOAP note
    rendered as readable text
- `core/platform/local_lite.py` — `LocalLitePlatform.__init__` now anchors relative
  storage roots to the repo (via `Path(__file__).resolve().parents[2]`), not `Path.cwd()`.
  Fixes a bug where Dagster sensor-triggered runs (spawned from the daemon's CWD) could
  not find `data/bronze`. Absolute env-var values pass through unchanged.
- `core/orchestration/dagster/assets.py` + `core/orchestration/dagster/sensors.py` — both now derive `bronze_root`
  from `platform.create().root` (not the module-level relative `DEFAULT_BRONZE`). Sensor
  now takes the platform resource for consistency.
- `core/orchestration/dagster/sensors.py` — target widened from `bronze_fhir` only to `bronze_fhir` +
  all 10 Silver asset keys, so each cohort drop materializes Bronze and Silver end-to-end
  in one Dagster run. Gold stays manual (unpartitioned aggregate). 6th wiring test in
  `tests/test_dagster_defs.py` pins the selection (`SENSOR_TARGET_KEYS`).
- `.gitignore` — added `*.duckdb` family (DuckDB UI notebooks are binary, machine-specific).
- `README.md` — Demo section, Operations row, layout entries pointing at the demo
  walkthrough and DuckDB notebook.
- `docs/ARCHITECTURE.md` — module map extended (`local/preview.py`, demo paragraph noting
  the three demo surfaces share `local/preview.py` for one set of renderers).
- `docs/RUNBOOK.md` — §5 adds the DuckDB UI command; §6 explains "what clicking an asset
  shows" + rule-by-rule check detail; links to PLAYBOOK.
- `docs/BENCHMARKS.md` — Execution-surfaces row for Dagster extended (metadata richness);
  new "Demo / read-only query surface" subsection for walkthrough + notebook + playbook.
#### Contract impact
- None — orchestration layer + presentation layer only. `gold.encounter_summary` stays
  **v1.1.0**; `silver.*` schemas unchanged.

### Session 4 — Dagster local orchestration (ADR-015, ADR-016)
#### Added
- `core/orchestration/dagster/` — new top-level package modelling the medallion as a software-defined
  Dagster asset graph. Third execution surface alongside the `core.surfaces.cli.pipeline` CLI and the
  (upcoming) Fabric notebooks; reuses the pure transforms verbatim — zero duplicate logic.
  - `assets.py`: `bronze_fhir` (cohort-partitioned inventory) → `silver_tables`
    `@multi_asset` (parse-once → 10 distinct Silver asset nodes, MERGE-upsert via
    `platform.write_silver`) → `gold_encounter_summary` (unpartitioned aggregate that also
    co-writes the corpus manifest via `platform.write_gold_manifest`).
  - `checks.py`: factory-built `@asset_check` per Silver table wrapping
    `validate_table()` — stays in lockstep with `SILVER_TABLES`.
  - `partitions.py`: cohort `DynamicPartitionsDefinition` — per-cohort materialization +
    backfill replaces the `rm -rf` full rebuild.
  - `resources.py`: `PlatformResource(ConfigurableResource)` delegates to
    `get_platform(LAKEHOUSE_PLATFORM)` — Dagster runs honour the platform env var the way
    the CLI does. Single persistence authority (ADR-002/009/016) — no IOManager.
  - `sensors.py`: `bronze_cohort_sensor` (default STOPPED) — Dagster analogue of the
    Auto Loader streaming-sim (spec §5.2); diffs `cohort_labels()` against the dynamic
    partition set and emits `RunRequest`s + `build_add_request` in one tick. **Target
    is `bronze_fhir` + the 10 Silver asset keys** (sourced from `SILVER_TABLES` via
    `SENSOR_TARGET_KEYS`), so each new cohort materializes Bronze and all 10 Silver
    tables in a single Dagster run — demoable end-to-end cohort flow. Gold stays out of
    the sensor target (unpartitioned aggregate; rebuilt manually).
  - `definitions.py`: thin top-level `Definitions(...)`.
- `tests/test_dagster_defs.py` (6 tests): Definitions load + every asset/check/sensor
  present, Silver assets carry the `bronze_fhir` lineage edge, **sensor target =
  Bronze + 10 Silver and excludes Gold** (asserted both against `SENSOR_TARGET_KEYS`
  and Dagster's resolved selection), Bronze+Silver materialize writes all 10 Silver
  Delta tables on the fixture, Bronze metadata records file count, Gold materialize
  writes the table + the corpus manifest. Guarded by `pytest.importorskip("dagster")`
  so `[dev]`-only installs collect cleanly. **122 tests**.
- ADR-015: adopt Dagster for **local** orchestration; sequence Dagster → Fabric so the
  asset graph documents the DAG the Fabric notebooks mirror. Local-only — Fabric still
  orchestrates via Data Factory.
- ADR-016: medallion = software-defined asset graph; `@multi_asset` for Silver (parse-once
  + 10-node render); platform-persisted (not IOManager) preserves single authority;
  Bronze + Silver cohort-partitioned, Gold unpartitioned.
- `[tool.dagster] module_name=orchestration.definitions` in `pyproject.toml` — `dagster
  dev` loads the medallion graph; UI on `http://localhost:3000`.
#### Changed
- `pyproject.toml`: new optional `[orchestration]` extra (`dagster>=1.8,<2.0`,
  `dagster-webserver>=1.8,<2.0`); `orchestration*` added to `setuptools.packages.find`.
  Kept out of `[dev]` to keep CI minimal — install with `pip install -e ".[local,dev,orchestration]"`.
- `.claude/rules/transforms.md`: banned `dagster` / `orchestration` imports from
  `core/transforms/` (orchestration imports transforms, never the reverse — mirrors the
  Spark/notebook isolation rule).
- `.gitignore`: `dagster_home/`, `.tmp_dagster_home*/`, `.dagster/` so `dagster dev` local
  state never lands in git.
- `docs/roadmap/scribe-iq-lakehouse-spec.md` §9: renumbered — **Session 4 = Dagster**,
  Session 5 = Fabric, Session 6 = CI/docs.
- `docs/roadmap/MASTER_PLAN.md`: post-weekend update note explaining the Dagster→Fabric
  sequence and the rationale (permanent artifact independent of the expiring Fabric trial).
- `docs/adr/README.md`: ADR 015/016 added to index.
#### Contract impact
- None — orchestration layer only; `gold.encounter_summary` stays **v1.1.0**.

### Session 3 — Documentation refresh (README + Runbook)
#### Added
- `docs/RUNBOOK.md`: operational runbook — prerequisites/config, first full run, ingest
  (FHIR + DICOM/CSV), build procedures (full / gold-only / single-cohort / clean rebuild),
  build verification (delta-rs + DuckDB snippets), doc regeneration, and a troubleshooting
  table (the clean-slate MERGE gotcha, missing cohorts, DICOM placeholders, etc.).
#### Changed
- `README.md`: rewrote the Session-1 stub into a full overview — accurate counts (1,278
  patients, 116 tests), correct install (`pip install -e ".[local,dev]"`), an Operations
  command table, data-products/contract section, current layout, and a documentation map.
- `docs/ARCHITECTURE.md`, `docs/BENCHMARKS.md`: corrected stale figures (corpus contract
  v1.0.0 → v1.1.0; Gold build ~5s → ~6.5s) and linked the runbook.

### Session 3 — Problem-list-as-of-date corpus enrichment (ADR-014, contract v1.1.0)
#### Changed
- `gold.encounter_summary` `active_conditions` / `active_medications` now reflect the
  patient's clinical state **as of each encounter date**, not just what was recorded at that
  encounter (ADR-014). Conditions: `onset ≤ date AND (abatement null OR abatement > date)` —
  chronic conditions carry forward, resolved ones drop off. Medications: `status=active` and
  authored ≤ date. Same `array[string]` schema, changed semantics → **contract v1.1.0** (MINOR).
- `silver.condition`: added `abatement_date` (from `Condition.abatementDateTime`) — additive
  column; `fhir_parser.extract_condition` now emits it.
- `core/gold/encounter_summary.py`: `_conditions`/`_medications` → `_active_conditions`/
  `_active_medications` patient-level as-of-date joins (meds pre-aggregated to earliest start).
- Regenerated `docs/DATA_DICTIONARY.md` (condition column) + `schemas/gold_encounter_summary.json`
  (x-contract-version 1.1.0).
#### Impact (full run)
- **avg conditions/encounter 0.08 → 9.57; avg medications/encounter 0.05 → 1.66**; encounters
  with an empty problem list dropped to 0.9%. Problem lists are clinically coherent and
  temporally gated; no duplicates. DICOM enrichment intact (298 studies). Gold build ~6.5s.
#### Limitation
- FHIR has no medication stop date, so `active_medications` is a forward `status=active`
  approximation (a med stopped after a past encounter still won't appear on it). Conditions
  are temporally precise. Documented in CORPUS_CONTRACT (ADR-014).
#### Tests
- New as-of-date unit test (onset gate, abatement exclusion, med start gate) + fixture
  carry-forward test; condition schema test covers `abatement_date`. 116 tests pass.

### Session 3 — DICOM ingest + imaging header extraction (ADR-013)
#### Added
- `core/ingest/dicom_index.py`: `DicomIndex` maps DICOM `StudyInstanceUID` → local `.dcm`
  path (the FHIR↔DICOM join key) and serves bytes; `study_uid_from_filename()` parses the
  Coherent file-name convention. File names embed patient names → never logged raw (ADR-010).
- `core/ingest/download.py`: `download_assets()` + CLI `--with-dicom` / `--with-csv` /
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
- `core/surfaces/cli/pipeline.py`: builds a `DicomIndex` once and threads the resolver through `_parse_cohort`.
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
- `core/gold/encounter_summary.py`: pure transform denormalizing all 10 Silver tables →
  `gold.encounter_summary` (one row per encounter). Polars join/aggregation engine; output
  assembled against an explicit `GOLD_SCHEMA` (nested struct vitals/imaging/versions + array
  conditions/meds/labs). Deterministic `summary_id` (UUIDv5 of encounter_id); BP parsed from
  Silver `components_json`; anniversary-based age-at-encounter. Defines the corpus contract
  (`CONTRACT_VERSION`, `REQUIRED_FIELDS`, `OPTIONAL_FIELDS`).
- `core/gold/corpus_manifest.py`: lineage manifest — contract version, per-Silver row
  counts + Delta versions, platform, and corpus coverage stats.
- `core/scripts/gen_corpus_schema.py` + `schemas/gold_encounter_summary.json`: machine-readable
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
- `core/surfaces/cli/pipeline.py`: added `build_gold()` + CLI flags `--with-gold` / `--gold-only`.
- Platform interface: `table_version(layer, table)` (delta-rs `version()` on `local_lite`,
  `None` default on base) and `write_gold_manifest()`; `local_lite` also gained `read_gold()`.
- `pyproject.toml`: `jsonschema>=4.0` added to `[dev]` for corpus-contract validation.
#### Enforcement
- `.pre-commit-config.yaml`: read-only `corpus-schema-current` hook
  (`gen_corpus_schema.py --check`); `/session-end` doc-sync now regenerates the corpus schema.
#### Full-run result
- 1,278 patients → **143,946** `gold.encounter_summary` rows in **~5s** on a single
  laptop, nested Delta types + CDC verified; manifest written to
  `gold/_metadata/corpus_manifest.json`.

### Documentation — generated-first (ADR-011)
#### Added
- `core/scripts/gen_data_dictionary.py`: renders `docs/DATA_DICTIONARY.md` from the registry
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
- `core/redaction.py`: `redact()` → non-reversible `ref:<hash>` for identifier-bearing
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
- `core/ingest/download.py`: parallel `aws s3 sync` (no-sign-request) + round-robin
  cohort partitioning (A/B/C) + ingest manifest
- `core/platform/local_lite.py`: `LocalLitePlatform` (Polars + delta-rs) — Delta
  write/read, CDC enabled on create, MERGE-upsert on primary key
- `core/transforms/schema_utils.py`: field-type-driven Arrow coercion (UTC timestamps,
  date32, string codes) + dedup
- `core/transforms/silver_{patient,encounter,clinical,soap_notes,ecg,imaging,genomics}.py`
  and `registry.py` — all 10 Silver tables with explicit Arrow schemas (ADR-004)
- `core/validation/{schema_registry,validate}.py`: per-table quality checks →
  `silver.ingest_log`
- `core/ingest/{bronze_landing,streaming_sim}.py`: cohort inventory + Auto Loader replay sim
- `core/surfaces/cli/pipeline.py`: per-cohort micro-batch Bronze→Silver orchestration
- 36 new tests (schema_utils, silver transforms, local_lite Delta round-trip, validation) —
  79 total, all passing
- ADR-009: Local Silver materialization (delta-rs, type coercion, component JSON)
- venv + full `[local,dev]` extras (polars, deltalake, duckdb, watchdog, pydicom)
#### Results
- Full run: 1,280 files (1,278 patients + `organizations.json` + `practitioners.json`)
  → all 10 Silver Delta tables in **2m30s** on a single laptop, all validations passed.
  Row counts: encounter 143,946 · observation 669,898 · medication_request 209,401 ·
  procedure 56,092 · soap_note 143,946 · condition 15,956 · imaging_study 3,752 ·
  genomic_report 419 · patient 1,278 · ecg_metadata 0 (ECG is Binary waveform, not FHIR).
- CDC (`delta.enableChangeDataFeed`) enabled on every Silver table.

### Session 1 — Repo scaffold + FHIR parser
#### Added
- Repo scaffold per spec §4: `pyproject.toml`, `requirements.txt`, `local/` package
  tree (`platform`, `transforms`, `ingest`, `gold`, `validation`), `tests/`, README stub
- `core/platform/base.py`: `LakehousePlatform` abstract interface (ADR-002)
- `core/platform/factory.py`: `LAKEHOUSE_PLATFORM` env-var router (default `local_lite`)
- `core/transforms/fhir_parser.py`: `FHIRBundleParser` — extract_patient, encounter,
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
