# HANDOFF — Session 4.5 (Multi-platform reorg before Fabric)
**Date:** 2026-05-28
**Repo:** scribe-iq-lakehouse
**Branch:** main

---

## Session 4.5 summary (this update)

Restructured the repo into two top-level domains before starting Session 5 (Fabric) —
`core/` (platform-agnostic kernel + LocalLite + Dagster + CLI + tests + scripts) and
`fabric/` (Fabric platform stub + notebooks placeholder + deploy config + tests + docs).
`local/` and `orchestration/` and `tests/` and `scripts/` were `git mv`'d into `core/`
(history preserved); all imports rewritten `from local.X` → `from core.X`; factory
strings rewritten so `LAKEHOUSE_PLATFORM=fabric` dispatches to `fabric.platform.FabricPlatform`
(outside `core/`); `pyproject.toml` packages now `["core*", "fabric*"]`; testpaths
`["core/tests", "fabric/tests"]`. ADR-017 (layout) + ADR-018 (CI/CD monorepo) document
the decisions; `core/` will be built as a versioned wheel and consumed by each platform
tier via its native install mechanism (Fabric Environment, Databricks Asset Bundle, AWS
Lambda layer). GitHub workflows scaffolded: `core-pr-tests.yml`, `core-build.yml`,
`fabric-deploy.yml` (skeleton — completed in Session 5); `databricks-deploy.yml.disabled`
and `aws-deploy.yml.disabled` as visible templates. Fabric Git Integration will target
`/fabric/notebooks/` (subfolder), `core` arrives as a wheel uploaded to a Fabric Environment.
The companion repo `fabric-lakehouse-hls-readmission` stays separate (different narrative,
CSV-first); cross-linked from README "See also". 126 tests passing (122 core + 4 fabric
contract). Planning doc: [docs/roadmap/multi-platform-reorg.md](docs/roadmap/multi-platform-reorg.md).

**Next session (Session 5 — Fabric)**: Implement `fabric/platform.py` against the
`LakehousePlatform` interface (OneLake Delta + Fabric Spark). Build notebooks 00–10
in `fabric/notebooks/` importing `from core.transforms…`. Wire `fabric/deploy/upload_wheel.py`
against the Fabric REST API. Configure workspace Git Integration → `/fabric/notebooks/`.
Capture screenshots per `fabric/docs/SCREENSHOTS.md` before trial expires (~11 days).

---

## Session 4 summary (prior — preserved below for context)
Inserted a local **Dagster** orchestration tier between Session 3's Gold corpus and the
upcoming Fabric work — a permanent portfolio artifact independent of the expiring Fabric
trial — then layered demoability work on top so the asset graph isn't just lineage but
**shows what flowed**. New top-level `core/orchestration/dagster/` package models the medallion as a
software-defined asset graph: `bronze_fhir` (per-cohort inventory) → `silver_tables`
`@multi_asset` (parse-once → 10 distinct Silver asset nodes, MERGE-upserted) →
`gold_encounter_summary` (unpartitioned aggregate that also co-writes the corpus manifest).
Cohorts map 1:1 to `DynamicPartitionsDefinition` partitions — per-cohort materialization +
backfill replaces the `rm -rf` full rebuild documented in Session 3's "Discoveries". Each
Silver asset carries an `@asset_check` wrapping `validate_table()` (factory keeps it in
lockstep with `SILVER_TABLES`), and a Bronze cohort `@sensor` is the Dagster analogue of
the Auto Loader streaming-sim (spec §5.2). Sensor target is `bronze_fhir` + the 10 Silver
asset keys, so one cohort drop → one Dagster run materializes Bronze + Silver end-to-end;
Gold stays manual (unpartitioned).

**Demoability polish layered on top** (the actual UI/CLI experience): `local/preview.py`
emits Markdown renderings (schema tables, sample-row tables, bundle resource-type
breakdowns, an encounter-card with the SOAP note rendered) that the Dagster
`MaterializeResult.metadata` panels now carry — clicking any asset shows what it
produced. `validate_table` was refactored to record every rule's outcome (not just
failures) as `CheckOutcome(name, passed, detail)`, so the `@asset_check` UI shows a
rule-by-rule pass/fail table with the actual numbers ("unique:encounter_id → 143,946/143,946
distinct"), not just a green dot. New [`core/scripts/demo_walkthrough.py`](core/scripts/demo_walkthrough.py)
follows one anchor patient Bronze → Parse → Silver → Gold in a rich-formatted CLI;
new [`docs/demo/notebooks/demo_notebook.sql`](docs/demo/notebooks/demo_notebook.sql) is a
20-cell DuckDB UI source (`duckdb -ui`) over the same Delta tables for a SQL audience.
Recording guide: [`docs/demo/PLAYBOOK.md`](docs/demo/PLAYBOOK.md). The platform is a
`ConfigurableResource` delegating to `get_platform(LAKEHOUSE_PLATFORM)` — assets persist
via `platform.write_*`, **not** an IOManager (ADR-016). `LocalLitePlatform` now anchors
relative storage roots to the repo (via `__file__`), not `Path.cwd()`, so Dagster
sensor-triggered runs spawned from the daemon's CWD resolve `data/bronze` correctly. 
Transforms remain **100% Dagster-unaware** — `.claude/rules/transforms.md` bans
`dagster`/`orchestration` imports (mirrors the Spark rule). **122 tests passing**
(+6 wiring tests). ruff/black clean. Spec §9: Session 4=Dagster, Session 5=Fabric,
Session 6=CI/docs.

---

## Current state

**Working (new this session):**
- `core/orchestration/dagster/partitions.py` — `cohort_partitions` (`DynamicPartitionsDefinition`),
  `COHORT_PARTITIONS_NAME = "cohort"`. Sensor mutates the set; assets read it.
- `core/orchestration/dagster/resources.py` — `PlatformResource(ConfigurableResource)` with
  `platform_name: str | None`; `create()` returns a fresh `LakehousePlatform` via the
  factory. Honours `LAKEHOUSE_PLATFORM` (ADR-002).
- `core/orchestration/dagster/assets.py` — `bronze_fhir` (cohort-partitioned inventory asset),
  `silver_tables` `@multi_asset` (10 `AssetSpec`s, all deps=`bronze_fhir`,
  partitions=cohort; calls `_parse_cohort` + `SILVER_TABLES[*].build` +
  `platform.write_silver(mode="merge")`), `gold_encounter_summary` (deps on the 10
  Silver assets; reuses `build_encounter_summary` + `build_corpus_manifest`,
  `p.write_gold` + `p.write_gold_manifest`, `MetadataValue.json(corpus_stats)`).
- `core/orchestration/dagster/checks.py` — `_make_check(table_name)` factory builds one
  `@asset_check` per Silver table, all wrapping `validate_table()`; surfaces in UI.
- `core/orchestration/dagster/sensors.py` — `bronze_cohort_sensor`, default STOPPED, 30 s interval.
  **Target = `bronze_fhir` + 10 Silver asset keys** (sourced from `SILVER_TABLES` via
  `SENSOR_TARGET_KEYS`) so each new cohort fires one Dagster run that materializes
  Bronze + Silver in a single step. Gold stays manual (unpartitioned). Diffs
  `cohort_labels(DEFAULT_BRONZE)` against `get_dynamic_partitions(...)` and emits
  `RunRequest`s + a single `build_add_request` in one `SensorResult`.
- `core/orchestration/dagster/definitions.py` — thin `Definitions(assets, asset_checks, sensors,
  resources={"platform": PlatformResource()})`.
- `tests/test_dagster_defs.py` (6 tests) — Definitions load + all asset keys present +
  10 checks wired; Silver assets declare `bronze_fhir` lineage edge; **sensor target
  covers Bronze + 10 Silver, excludes Gold** (via `SENSOR_TARGET_KEYS` + cross-checked
  against Dagster's resolved selection); Bronze+Silver materialize on the fixture writes
  10 Delta tables; Bronze metadata records file count; Gold materialize writes the Delta
  table + the manifest. Guarded by `pytest.importorskip("dagster")` so `[dev]`-only
  installs still collect cleanly.
- `pyproject.toml` — new **`[orchestration]`** extra (`dagster>=1.8,<2.0`,
  `dagster-webserver>=1.8,<2.0`); `[tool.dagster] module_name=orchestration.definitions`;
  `orchestration*` added to setuptools packages.
- `.claude/rules/transforms.md` — banned `dagster` / `orchestration` imports from
  `core/transforms/` (orchestration imports transforms, never the reverse).
- `.gitignore` — `dagster_home/`, `.tmp_dagster_home*/`, `.dagster/`, `*.duckdb` family.
- ADR-015 (adopt Dagster, sequence Dagster → Fabric) + ADR-016 (assets +
  platform-persisted; multi_asset for parse-once; cohort partitions). `docs/adr/README.md`
  updated. `docs/roadmap/scribe-iq-lakehouse-spec.md` §9 renumbered;
  `docs/roadmap/MASTER_PLAN.md` post-weekend update note.

**Demoability polish (new this session, on top of the orchestration tier):**
- `core/validation/validate.py` — added `CheckOutcome(name, passed, detail)` + `ok()` method
  on `ValidationResult`; every rule now records its outcome (pass + fail), not just failures.
  `failed_checks` kept for CLI/`ingest_log` compatibility.
- `core/orchestration/dagster/checks.py` — `AssetCheckResult.metadata` now includes a Markdown table of
  every rule's pass/fail + numbers (`rules_total`, `rules_passed`, `rules_failed`, `rules`).
- `core/orchestration/dagster/assets.py` — each `MaterializeResult.metadata` carries:
  - `bronze_fhir` → first bundle's FHIR resource-type breakdown (`MetadataValue.md`)
  - 10 Silver outputs → schema table + first-5-row table per partition
  - `gold_encounter_summary` → full schema + sample-encounter card with SOAP note rendered
- `local/preview.py` — new shared module: `schema_md`, `sample_md`, `bundle_resource_counts`,
  `bundle_summary_md`, `gold_encounter_card`. Pure-Python, framework-agnostic; same
  renderings power the Dagster UI metadata AND the CLI walkthrough.
- `core/platform/local_lite.py` — `LocalLitePlatform.__init__` now anchors relative
  storage roots to the repo (via `Path(__file__).resolve().parents[2]`), not `Path.cwd()`,
  so Dagster sensor-triggered runs (which spawn from the daemon's CWD) find `data/bronze`.
  Absolute env-var values pass through unchanged.
- `core/orchestration/dagster/assets.py` + `core/orchestration/dagster/sensors.py` — both derive `bronze_root` from
  `platform.create().root` (not the module-level relative `DEFAULT_BRONZE`). Sensor now
  takes the platform resource for consistency.
- `core/scripts/demo_walkthrough.py` — new (~280 LOC) — one-patient Bronze → Parse → Silver → Gold
  tour using the `rich` library. Auto-picks an anchor patient (≥3 conditions, ≥3 meds, ≥1
  SOAP note) or accepts `--patient-id`; `--pause N` for screencast pacing. Reuses
  `local/preview.py` so it shows the same data shape as the Dagster UI.
- `docs/demo/notebooks/demo_notebook.sql` — new 20-cell DuckDB UI source (corpus headlines,
  schema, top conditions, demographics, encounter mix, vitals percentiles, imaging
  modalities, one patient's encounter timeline, as-of-date condition evolution, full SOAP
  note, keyword cohort search, coverage, lineage, cross-layer join, final shape).
- `docs/demo/notebooks/demo.duckdb` — pre-built DuckDB notebook for local convenience
  (gitignored; binary with absolute paths baked in). README has a one-line regenerator.
- `docs/demo/notebooks/README.md` — how to open / regenerate / per-cell guide.
- `docs/demo/PLAYBOOK.md` — full portfolio-video recording playbook: 5-beat structure,
  preflight checklist, window setup, take-by-take sequence (with optional sensor demo
  contingency), editing notes, publishing checklist.
- `pyproject.toml` — `rich>=13.0` added to `[dev]` (used by `core/scripts/demo_walkthrough.py`).
- `tests/test_dagster_defs.py` — 6th test (`test_sensor_targets_bronze_and_silver_not_gold`)
  pins the sensor selection.

**Carried from Session 3 (all still working):**
- Gold layer (`gold.encounter_summary` 143,946 rows · contract v1.1.0 · as-of-date
  problem list, ADR-014), DICOM enrichment (298 studies, ADR-013), full doc set
  (README, RUNBOOK, ARCHITECTURE, DATA_DICTIONARY(gen), BENCHMARKS, CORPUS_CONTRACT).
- 7 Silver transforms + registry + validation; Bronze→Silver pipeline; LocalLitePlatform;
  PHI-safe redaction (ADR-010); generated-first docs (ADR-011).

**Asset graph (live in `dagster dev`):**
```
bronze_fhir [cohort-partitioned]   ← metadata: file count + FHIR resource breakdown (md)
   └─→ silver_tables (multi_asset, cohort-partitioned)   ← per-output: schema + sample rows (md)
         ├─→ patient · encounter · observation · condition · procedure
         ├─→ medication_request · soap_note · imaging_study · genomic_report · ecg_metadata
         │     (each carries @asset_check → rule-by-rule pass/fail table in UI)
         └─→ gold_encounter_summary [unpartitioned aggregate]
               metadata: corpus_stats + full schema + sample-encounter card with SOAP note
               (also writes gold/_metadata/corpus_manifest.json)
```

**Three demo surfaces (same renderers via [`local/preview.py`](local/preview.py)):**
```
Dagster UI      → click an asset, panel shows schema + sample rows + bundle/SOAP card
CLI walkthrough → python -m scripts.demo_walkthrough (--pause 2.5 for screencast)
DuckDB notebook → duckdb docs/demo/notebooks/demo.duckdb -ui (20 SQL cells)
```

**In progress:**
- Nothing — Session 4 committed as `8abdde2` (29 files, +2,668/−167). Ready for next session.

**Blocked:**
- Fabric workspace + S3 shortcut (manual; needed for Session 5, ~11 days of trial left).

**Discoveries / caveats (carry forward):**
- **Assets return `MaterializeResult`, the platform persists** (ADR-016) — slightly
  non-idiomatic Dagster (no IOManager), but it's the only way to keep
  `LakehousePlatform` as the single persistence authority (ADR-002). One-line callout
  for reviewers: "Dagster owns the DAG and observability; the platform owns the bytes."
- **`@multi_asset` was the right choice** — per-table assets would have re-parsed each
  cohort 10×. `multi_asset` with 10 `AssetSpec`s gives the same 10-node graph rendering
  while parsing once. The trade is that materializing one Silver table from the UI
  re-materializes all 10 — fine because parse is the cost, write is cheap.
- **Sensor is default-STOPPED** — otherwise `dagster dev` immediately fires the existing
  cohorts as "new". Start it manually from the UI when demoing cohort drops.
- **Sensor materializes Bronze + Silver per cohort** (not just Bronze). One `RunRequest`
  per new cohort, target = `bronze_fhir` + 10 Silver keys → Bronze and the 10 Silver
  tables for that partition land in one Dagster run. Gold rebuild stays manual because
  Gold is unpartitioned — including it in a partitioned `RunRequest` would error.
- **Dagster goes in `[orchestration]` extra, not `[dev]`** — keeps Dagster off the CI hot
  path and off any minimal install. The test file uses `pytest.importorskip("dagster")`.
- **No Gold partitioning** — Gold aggregates across cohorts, so it's intentionally
  unpartitioned and rebuilt full (~6.5 s; ADR-016 "Neutral"). The MERGE whole-table
  failure mode from Session 3 doesn't apply because Gold uses overwrite, not merge.
- **Manifest is co-materialized with Gold**, not a downstream asset — `LakehousePlatform`
  doesn't expose `read_gold` on the abstract base; co-locating also matches what
  `build_gold` already does in the CLI.
- **CWD fix in LocalLitePlatform** — Dagster sensor-triggered runs spawn from the daemon's
  working directory (not the repo root), so the old `Path("data")` resolved nowhere. Now
  anchored to repo via `Path(__file__).resolve().parents[2]`. Symptom this fixed: "Bronze
  cohort 'X' has no bundles under data/bronze" on sensor-fired runs even when the bundles
  were clearly there.
- **MERGE re-run on already-populated Silver tables fails** — source-side duplicate primary
  keys in parse output (some FHIR extracts emit duplicates) collide with target-side
  duplicates from the original OVERWRITE write, producing delta-rs "matched a target row
  with multiple source rows". The CLI build works because first-cohort = OVERWRITE accepts
  source dups, and subsequent cohorts MERGE with *disjoint* patient_ids. Re-running a
  cohort through Dagster on populated tables triggers the bug. **For demo:** clean-slate
  Silver/Gold before letting Dagster build (`rm -rf data/silver data/gold`). Long-term
  fix: dedupe in `extract_*` or in `build_*` before write (deferred — Open Decision).
- **Demo notebook `.duckdb` paths** — DuckDB UI's working directory is `$HOME` by default,
  so `delta_scan(...)` needs absolute paths. The pre-built `docs/demo/notebooks/demo.duckdb`
  bakes them in (gitignored, machine-specific). `demo_notebook.sql` uses
  `getvariable('repo')` with a clear `/ABSOLUTE/PATH/TO/...` placeholder cloners must edit.

---

## Test status
```
122 passed (venv: .venv/bin/python -m pytest)
  + test_dagster_defs (6): Definitions load + every asset/check/sensor present + sensor
    target = Bronze + 10 Silver (excludes Gold); Silver assets carry bronze_fhir lineage
    edge; Bronze+Silver materialize writes all 10 Silver Delta tables on the fixture;
    Bronze metadata records file count; Gold materialize writes encounter_summary +
    corpus_manifest.json (v1.1.0)
  + test_validate (7): now exercising the richer CheckOutcome list as well as the legacy
    failed_checks strings (existing assertions unchanged)
ruff: All checks passed   |   black: 55 files clean
```

To run each surface locally:
```bash
pip install -e ".[local,dev,orchestration]"

# A. CLI (default, dependency-light)
python -m core.surfaces.cli.pipeline --with-gold

# B. Dagster (asset graph + sensor + checks; install needs DAGSTER_HOME)
export DAGSTER_HOME="$PWD/dagster_home"; mkdir -p "$DAGSTER_HOME"
dagster dev                                      # http://localhost:3000

# C. CLI walkthrough (one patient, rich-formatted Bronze → Gold)
python -m scripts.demo_walkthrough

# D. DuckDB UI notebook (20 SQL cells over Silver/Gold)
brew install duckdb                              # needs ≥1.2
duckdb docs/demo/notebooks/demo.duckdb -ui       # http://localhost:4213
```

---

## Next session — start here

**Recommended first task (Session 5 — Fabric execution):** create the Fabric workspace +
lakehouse and the S3 shortcut to `s3://synthea-open-data/coherent/`, then build the
notebook sequence following the 8-cell template (`.claude/rules/notebooks.md`):
`00_setup`, `01_bronze_ingest`, `05_silver_soap_notes` (demo centerpiece — MUST display
a decoded SOAP note), `09_gold_encounter_summary`. Notebooks import the **same** pure
transforms from `core/transforms/` and `core/gold/` — zero duplicate logic — now
demonstrably the **third** execution surface (after the CLI and Dagster). **Capture
screenshots as you go** (trial ~11 days; spec §15.2). **Read first:** spec §6 (notebook
sequence), `.claude/rules/notebooks.md`, ADR-001 (Fabric-first).
**Watch out:** `FabricPlatform` is registered in the factory but NOT implemented —
implement it (Spark read/write + CDC + `table_version`/`write_gold_manifest`) before the
notebooks run.

**Time-pressure note:** Fabric is the only deadline-driven artifact (trial ~11 days). The
Dagster + DuckDB demo artifacts are already permanent. If recording the portfolio video
takes priority, that's also a valid next-session path — use `docs/demo/PLAYBOOK.md`.

**Alternative paths if Fabric is blocked:**
1. **Silver dedup fix** (Open Decision below) — eliminates the MERGE re-run failure mode
   so Dagster backfill is idempotent. Real data-quality work; new ADR.
2. **Record the portfolio video** following `docs/demo/PLAYBOOK.md` — 90 sec for LinkedIn,
   3 min for portfolio site. The recording stage (DuckDB UI + Dagster + CLI walkthrough)
   is all ready to go.
3. **Wire `scribe-iq` to consume the corpus** — swap its 19-patient dev corpus for the
   1,278-patient `gold.encounter_summary` (the consumer payoff).

**Stretch:** point Dagster's `LAKEHOUSE_PLATFORM` env var at the new `FabricPlatform` and
demonstrate the same asset graph materializing into Fabric — the "three execution
surfaces, one transform tier" payoff.

---

## Open decisions

| Decision | Options | Recommendation | Status |
|----------|---------|----------------|--------|
| Sensor default status | RUNNING vs STOPPED | STOPPED (avoid replay-on-startup) | DONE — ADR-016 |
| Persistence pattern | IOManager vs platform-persisted | platform-persisted (single authority) | DONE — ADR-016 |
| Silver asset modelling | per-table vs multi_asset | multi_asset (parse-once, 10-node render) | DONE — ADR-016 |
| Dagster install extra | `[dev]` vs `[orchestration]` | `[orchestration]` (keep CI minimal) | DONE |
| Run Dagster on Fabric? | yes vs local-only | Local-only — Fabric uses Data Factory | DONE — ADR-015 "Neutral" |
| Fabric platform impl | Spark in FabricPlatform vs reuse local | Implement FabricPlatform I/O | Session 5 |
| Silver parse-output deduplication | dedupe in `extract_*` vs `build_*` vs leave (clean-slate workaround) | Dedupe in `build_*` (single bottleneck; preserves extract simplicity) | OPEN — discovered Session 4 (MERGE re-run on populated tables fails: source dups × target dups → "matched a target row with multiple source rows"). Needs ADR-017 + the fix. |
| Portfolio video timing | Record now vs after Fabric vs after dedup fix | Record now — demo artifacts are stable, won't get better by waiting | OPEN — `docs/demo/PLAYBOOK.md` ready when you are |

---

## Key state
```
LAKEHOUSE_PLATFORM=local_lite (default) — LocalLitePlatform implemented; Dagster uses it via PlatformResource
Storage root: data/ (gitignored) — bronze/{fhir,dicom,csv} + silver/<10> + gold/{encounter_summary,_metadata}
Bronze: 1,280 FHIR bundles (4.6 GB) + 298 DICOM (.dcm, 9.3 GB) + 16 CSV (466 MB)
Silver: 10 Delta tables (CDC, validation passing via @asset_check); ingest_log only on CLI builds
Gold: encounter_summary (143,946 rows, as-of-date problem list) + corpus_manifest.json
Orchestration: orchestration/ — `dagster dev` (asset graph) ✓ live in this session, sensor proven
Demo: core/scripts/demo_walkthrough.py (rich CLI) + docs/demo/notebooks/demo_notebook.sql (DuckDB)
      + docs/demo/PLAYBOOK.md (recording guide). Shared renderers via local/preview.py.
Docs: README, RUNBOOK, ARCHITECTURE, DATA_DICTIONARY(gen), BENCHMARKS, CORPUS_CONTRACT (v1.1.0)
Contract: v1.1.0 — unchanged this session (ADR-015/016 "Contract impact: none")
Tests: 122 passing (+6 Dagster wiring, importorskip-guarded so [dev]-only installs stay clean)
Full re-run (CLI path): rm -rf data/silver data/gold; python -m core.surfaces.cli.pipeline --with-gold
Full re-run (Dagster path): wipe data/silver+gold first, then backfill every cohort partition
                            of bronze_fhir + silver_tables, then materialize gold_encounter_summary
Session 4 commit: 8abdde2 — feat(orchestration): Dagster medallion asset graph + demo surfaces
Fabric workspace: NOT YET CREATED  |  Fabric trial: ~11 days remaining
M5 Max: arriving ~June 2, 2026
```

---

## Files changed this session

**Orchestration tier (initial Dagster work):**
- `orchestration/__init__.py` — new (re-exports `defs`)
- `core/orchestration/dagster/partitions.py` — new (`cohort_partitions`, `COHORT_PARTITIONS_NAME`)
- `core/orchestration/dagster/resources.py` — new (`PlatformResource`)
- `core/orchestration/dagster/assets.py` — new; later enriched with `MetadataValue.md` schema/sample/card
- `core/orchestration/dagster/checks.py` — new (10 `@asset_check`s via factory); later enriched with
  rule-by-rule pass/fail Markdown table in metadata
- `core/orchestration/dagster/sensors.py` — new (`bronze_cohort_sensor`); target widened to
  `bronze_fhir` + 10 Silver asset keys; platform resource injected
- `core/orchestration/dagster/definitions.py` — new (`defs`)
- `tests/test_dagster_defs.py` — new (6 wiring tests, `importorskip` guarded)

**Demoability polish (this session, on top of orchestration tier):**
- `local/preview.py` — new shared Markdown renderer module
- `core/validation/validate.py` — `CheckOutcome` + `ValidationResult.checks` + `ok()`
- `core/platform/local_lite.py` — repo-anchored root (CWD-independent for Dagster daemon)
- `core/scripts/demo_walkthrough.py` — new (~280 LOC, `rich` CLI walkthrough)
- `docs/demo/PLAYBOOK.md` — new (recording playbook)
- `docs/demo/notebooks/demo_notebook.sql` — new (20-cell DuckDB UI source)
- `docs/demo/notebooks/README.md` — new (notebook usage / regeneration)
- `docs/demo/notebooks/demo.duckdb` — generated locally (gitignored)
- `pyproject.toml` — `[orchestration]` extra; `rich` added to `[dev]`; `[tool.dagster]`;
  `orchestration*` package
- `.claude/rules/transforms.md` — banned dagster/orchestration imports
- `.gitignore` — `dagster_home/`, `.tmp_dagster_home*/`, `.dagster/`, `*.duckdb*`

**ADRs / docs:**
- `docs/adr/015-dagster-local-orchestration.md` — new
- `docs/adr/016-dagster-asset-graph.md` — new
- `docs/adr/README.md` — ADR 015/016 added to index
- `docs/ARCHITECTURE.md` — module map + demo paragraph extended
- `docs/RUNBOOK.md` — §5 DuckDB UI command, §6 asset metadata + check-detail explainer
- `docs/BENCHMARKS.md` — execution-surfaces table extended, demo surfaces note
- `docs/roadmap/scribe-iq-lakehouse-spec.md` — §9 renumbered; Session 4 = Dagster
- `docs/roadmap/MASTER_PLAN.md` — post-weekend update note (Dagster → Fabric sequence)
- `README.md` — Demo + notebook section, Operations row, layout entries
- `CHANGELOG.md` — Session 4 section (orchestration + polish)
- `HANDOFF.md` — this file (Session 3 → Session 4)

## ADRs (running list)
- ADR-008 dict parsing · ADR-009 local Silver · ADR-010 PHI-safe logging ·
  ADR-011 generated-first docs · ADR-012 Gold encounter_summary (engine/grain/lineage) ·
  ADR-013 DICOM ingest + FHIR↔DICOM linkage + header extraction ·
  ADR-014 problem-list-as-of-date (conditions/meds; contract v1.1.0) ·
  **ADR-015 Dagster for local pipeline orchestration · ADR-016 medallion as a Dagster asset graph**

## Note on settings.json churn
The harness may append auto-approved Bash permissions to the **tracked**
`.claude/settings.json`; relocate them into gitignored `.claude/settings.local.json` and
`git restore` the tracked file. Recurs each session.
