# Session 5 — Fabric end-to-end + Silver dedup fix + Power BI

**Owner:** Sandeep Jayaprakash
**Created:** 2026-05-28
**Status:** APPROVED — IN EXECUTION
**Predecessor:** [multi-platform-reorg.md](multi-platform-reorg.md) (Session 4.5)
**Spec reference:** [scribe-iq-lakehouse-spec.md](scribe-iq-lakehouse-spec.md) §6 (notebook sequence), §15.2 (screenshot checklist)

---

## Context

Session 4.5 closed with the repo reorganized into `core/` (platform-agnostic
kernel shipping as a wheel) and `fabric/` (platform stub + empty notebooks +
deploy scaffold + 4 contract tests). 126 tests pass, the wheel builds and
installs into a fresh venv, but **nothing in `fabric/` actually runs** —
`FabricPlatform` is a pure `NotImplementedError` stub across all 10 abstract
methods, the Fabric workspace doesn't exist yet, and
[fabric/deploy/upload_wheel.py](../../fabric/deploy/upload_wheel.py) is a
placeholder.

Session 5 delivers the **full Fabric tier end-to-end** — FHIR Bronze ingest →
10 Silver tables → Gold encounter_summary — driven by Fabric Notebooks plus a
Fabric Data Pipeline orchestrator, with exploratory analysis and a Power BI
dashboard layered on top, and the lingering Silver-MERGE-dedup defect fixed
(ADR + code) so the platform behaves identically to LocalLite on re-runs.

Hard constraint: **Fabric trial ≈ 11 days remaining**. Sequencing prioritizes
getting *something running and screenshot-able* in Fabric early, then layering
polish. Local tier (LocalLite + Dagster + DuckDB + CLI walkthrough) is the
permanent fallback if the trial expires mid-session.

---

## Sequencing (risk-ordered, each phase commits cleanly)

| Phase | Deliverable | Trial needed? | Effort |
|---|---|---|---|
| 1 | Silver dedup fix + ADR-019 | No | ~1 evening |
| 2 | FabricPlatform real impl + REST wheel upload + CI step | No | ~1 evening |
| 3 | Workspace + lakehouse + S3 shortcut + Environment (UI) | **Yes** | ~½ evening |
| 4 | Notebooks 00–10 (medallion sequence, 8-cell template) | **Yes** | ~2–3 evenings |
| 5 | Fabric Data Pipeline → `fabric/pipelines/medallion.json` | **Yes** | ~½ evening |
| 6 | `fabric/notebooks/11_exploratory_analysis.ipynb` | **Yes** | ~½ evening |
| 7 | Power BI dashboard (Direct Lake) + `.pbip` committed | **Yes** | ~1–2 evenings |

If the trial expires between phases, every prior phase's deliverables are
permanent. Phases 1–2 don't require the trial at all.

---

## Phase 1 — Silver dedup fix

**Defect** (HANDOFF Discoveries lines 311–318): MERGE on a Silver table that
was originally written via OVERWRITE *before* `dedup_by_key()` was added to
every `build_silver_*` will fail with delta-rs's *"matched a target row with
multiple source rows"*. Source side is now clean (all 10 builders call
`dedup_by_key()` in
[core/transforms/schema_utils.py:119](../../core/transforms/schema_utils.py#L119)),
but target-side dups from legacy overwrites remain.

**Mechanism** (ratified in ADR-019): make
[`LocalLitePlatform._write_delta`](../../core/platform/local_lite.py#L133-L157)
detect target-side dups on the merge key and rewrite the target deduped (last-
write-wins on PK, matching `dedup_by_key`'s semantics) **before** invoking the
delta-rs MERGE. Detection uses a cheap `pa.Table.group_by(key).count()` —
O(rows), no shuffle needed for the LocalLite scale (~143k rows). FabricPlatform
gets the same guard in Phase 2 (Spark-native equivalent: `dropDuplicates([pk])`
on a transient read before MERGE).

**Files to modify:**
- [core/platform/local_lite.py](../../core/platform/local_lite.py) — extend `_write_delta` with the pre-merge target-dedup guard.
- [core/transforms/registry.py](../../core/transforms/registry.py) — no change; reuse `SILVER_PRIMARY_KEYS`.
- `core/tests/test_local_lite_platform.py` (or analogous file — verify name during impl) — add a regression test:
  1. Write a Silver table OVERWRITE with intentional target-side dups.
  2. Call `write_silver(..., mode="merge")` again.
  3. Assert it succeeds and the table is deduped on PK.
- `docs/adr/019-silver-merge-idempotency.md` (new) — documents the defect, the rewrite-on-detect mechanism, the alternative (force a clean-slate rebuild), and the perf trade-off.
- [docs/adr/README.md](../adr/README.md) — add ADR-019 to index.
- [HANDOFF.md](../../HANDOFF.md) Open Decisions table — flip "Silver parse-output deduplication" from OPEN to DONE — ADR-019.

**Verify end-to-end:**
```bash
# 1. Regression test passes
.venv/bin/python -m pytest core/tests/test_local_lite_platform.py -k dedup -v

# 2. Full suite still green (126 → 127+)
.venv/bin/python -m pytest

# 3. Re-run on a populated Silver table no longer requires rm -rf:
.venv/bin/python -m core.surfaces.cli.pipeline --with-gold   # baseline
.venv/bin/python -m core.surfaces.cli.pipeline --with-gold   # re-run — must succeed without wiping data/silver

# 4. Dagster sensor re-fire on the same cohort succeeds (was the original failure mode):
DAGSTER_HOME="$PWD/dagster_home" dagster asset materialize \
  --select 'bronze_fhir,silver_tables' \
  --partition '<existing-cohort-label>' \
  -m core.orchestration.dagster.definitions
```

---

## Phase 2 — FabricPlatform implementation

Reference impl: [core/platform/local_lite.py](../../core/platform/local_lite.py)
(Polars + delta-rs). Fabric mirror uses Spark + the bundled delta-spark.

**Files to modify:**
- [fabric/platform.py](../../fabric/platform.py) — replace every `NotImplementedError`:

  | Method | Implementation |
  |---|---|
  | `storage_path(layer, table)` | Return OneLake abfss URI built from env-injected workspace + lakehouse + the same `<root>/<layer>/<table>` shape LocalLite uses. Lakehouse name + workspace ID come from notebookutils (`mssparkutils.env.getWorkspaceName()`, etc.) cached in `__init__`. |
  | `read_bronze_fhir(cohort)` | List JSON files under `Files/bronze/fhir/cohort=<cohort>/` via `mssparkutils.fs.ls`, read each as text, `json.loads`, return `list[dict]`. Identical contract to LocalLite. |
  | `write_silver(table, data, mode)` | Convert `pa.Table` → Spark DataFrame (`spark.createDataFrame(data.to_pandas())`). For mode="merge", use `DeltaTable.forPath(spark, path).alias("t").merge(...).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()`. For first write or mode="overwrite": `df.write.format("delta").mode("overwrite").option("delta.enableChangeDataFeed", "true").save(path)`. **Apply the Phase 1 target-side dedup guard** (Spark equivalent: `existing.dropDuplicates([pk])` rewrite when dup count > 0). |
  | `read_silver(table)` | `spark.read.format("delta").load(path).toPandas()` → `pa.Table.from_pandas`. |
  | `write_gold(table, data)` | Same as `write_silver` but mode="overwrite", no merge key. |
  | `log_metric(table, metric, value)` | Emit structured log via Fabric's `print()` (captured in notebook run logs) plus optional row in `silver.ingest_log` table. |
  | `send_alert(severity, message)` | Print + raise on `severity == "critical"` (Data Activator integration deferred). |
  | `get_spark_session()` | Return the global `spark` from notebookutils — Fabric injects it. In unit tests, return `None`. |
  | `table_version(layer, table)` | `DeltaTable.forPath(spark, storage_path(layer, table)).history(1).first()["version"]`. |
  | `write_gold_manifest(manifest)` | Write JSON to `Files/gold/_metadata/corpus_manifest.json` via `mssparkutils.fs.put`. |

- [fabric/tests/test_fabric_platform.py](../../fabric/tests/test_fabric_platform.py) — keep the 4 contract tests as the offline gate; add a new `pytest.mark.fabric` slow suite that only runs when `FABRIC_TENANT_ID` is set, exercising a round-trip write→read on a throwaway table.
- [fabric/deploy/upload_wheel.py](../../fabric/deploy/upload_wheel.py) — implement the REST PUT to `https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/environments/{environment_id}/staging/libraries` using Service Principal auth (MSAL `ConfidentialClientApplication` for the token, `requests.put` for the upload, then POST `/publish` to commit). Honour the 5 required env vars already documented in the docstring.
- [.github/workflows/fabric-deploy.yml](../../.github/workflows/fabric-deploy.yml) — replace the TODO step with the now-real `python fabric/deploy/upload_wheel.py --wheel core/dist/scribe_iq_lakehouse_core-*.whl --environment-id $FABRIC_ENVIRONMENT_ID` invocation; add the smoke-run step via `fabric-cicd` CLI targeting notebook 05.

**Verify end-to-end:**
```bash
# 1. Contract tests still pass (offline)
.venv/bin/python -m pytest fabric/tests/

# 2. Behaviour suite passes against real workspace (requires SP credentials)
FABRIC_TENANT_ID=... .venv/bin/python -m pytest fabric/tests/ -m fabric

# 3. Wheel upload works (one-shot manual run before relying on CI)
python -m build --wheel
python fabric/deploy/upload_wheel.py \
  --wheel core/dist/scribe_iq_lakehouse_core-0.1.0-py3-none-any.whl \
  --environment-id $FABRIC_ENVIRONMENT_ID
```

---

## Phase 3 — Workspace setup (manual UI)

Steps documented in [fabric/docs/DEPLOYMENT.md](../../fabric/docs/DEPLOYMENT.md);
execute, then update the doc with the actual workspace ID + environment ID (no
secrets — just identifiers). Screenshots **00_workspace_overview**,
**01_lakehouse_tables**, **02_bronze_landing** per
[fabric/docs/SCREENSHOTS.md](../../fabric/docs/SCREENSHOTS.md).

---

## Phase 4 — Notebooks (medallion sequence)

10 notebooks, all in [fabric/notebooks/](../../fabric/notebooks/), all
following the 8-cell template from
[.claude/rules/notebooks.md](../../.claude/rules/notebooks.md). Every notebook
imports `from core.*` — zero duplicate transform logic.

| # | Notebook | Purpose | Imports from core |
|---|---|---|---|
| 00 | `00_setup.ipynb` | Verify Environment, wheel installed, Spark session live, S3 shortcut readable. No data writes. | `core.platform.factory.get_platform` |
| 01 | `01_bronze_ingest.ipynb` | List cohorts under shortcut, copy FHIR JSON into `Files/bronze/fhir/cohort=<c>/`, write `_metadata/manifest.json`. | `core.ingest.bronze_landing.cohort_labels` |
| 02 | `02_silver_patient.ipynb` | Build silver.patient. | `core.transforms.silver_patient.build_silver_patient`, `SILVER_TABLES["patient"]` |
| 03 | `03_silver_encounter.ipynb` | Build silver.encounter. | `core.transforms.silver_encounter.build_silver_encounter` |
| 04 | `04_silver_clinical.ipynb` | Build condition + observation + medication_request + procedure (shared parser pass). | `core.transforms.silver_clinical.*` |
| 05 | `05_silver_soap_notes.ipynb` | **Demo centerpiece.** Build silver.soap_note. **MUST `display()` a decoded SOAP note** in Cell 7 — readable clinical text, not a Base64 blob. | `core.transforms.silver_soap_notes.build_silver_soap_note` |
| 06 | `06_silver_imaging_dicom.ipynb` | Build silver.imaging_study with DICOM header enrichment (ADR-006, `stop_before_pixels=True`). | `core.transforms.silver_imaging.build_silver_imaging`, `core.ingest.dicom_index.DicomIndex` |
| 07 | `07_silver_ecg_genomics.ipynb` | Build silver.ecg_metadata + silver.genomic_report. Cell 6 explicitly calls out `data_limitation` (ADR-007). | `core.transforms.silver_ecg.*`, `core.transforms.silver_genomics.*` |
| 08 | `08_silver_validation.ipynb` | Run `validate_table` over all 10 Silver tables; write `silver.ingest_log` rows. | `core.validation.validate.validate_table` |
| 09 | `09_gold_encounter_summary.ipynb` | Build gold.encounter_summary + co-write corpus_manifest.json. | `core.gold.encounter_summary.build_encounter_summary`, `core.gold.corpus_manifest.build_corpus_manifest` |
| 10 | `10_gold_validation.ipynb` | Final contract check: row count = 143,946, contract version = 1.1.0, sample-encounter card display. | `core.gold.contract` (or wherever `CONTRACT_VERSION` lives) |

Each notebook ends with the validation cell asserting `MIN_ROWS`
([.claude/rules/notebooks.md](../../.claude/rules/notebooks.md)) and writes
to `silver.ingest_log`. **Screenshot
[`05_silver_soap_demo`, `09_gold_encounter_summary`, `11_environment_wheel`] the
moment each notebook runs green** — do not batch.

---

## Phase 5 — Fabric Data Pipeline

Build the pipeline in the Fabric UI (Data Factory experience), then export the
JSON (UI button → Download) into `fabric/pipelines/medallion.json` (Git-
tracked, version-controlled diff target).

```
Notebook 01 (Bronze ingest)
  └─ on success → Notebooks 02-07 (Silver builds, in parallel where DAG allows)
       └─ on success → Notebook 08 (Silver validation)
            └─ on success → Notebook 09 (Gold build)
                 └─ on success → Notebook 10 (Gold validation)
```

Schedule: leave unscheduled (manual trigger only — same as Dagster sensor
default STOPPED, ADR-016). Screenshot **`10_dagster_local_compare`**: Dagster
asset graph on the left, Fabric Data Pipeline graph on the right.

---

## Phase 6 — Exploratory analysis notebook

`fabric/notebooks/11_exploratory_analysis.ipynb` — same 8-cell template, but
the "transform" cell is a series of Spark SQL queries lifted from
[docs/demo/notebooks/demo_notebook.sql](../demo/notebooks/demo_notebook.sql).
Strongest cells to port: 4 (encounter types), 6 (top conditions), 7 (top meds),
9 (problem-list complexity), 10 (vitals percentiles), 14 (as-of-date condition
evolution), 15 (decoded SOAP note), 17 (corpus coverage %).

Independent of the medallion DAG — reads Gold, doesn't write. Cell 1 callout:
*"Read-only — safe to re-run during a recording."*

---

## Phase 7 — Power BI dashboard

Build directly on the Fabric Lakehouse SQL endpoint using **Direct Lake mode**
(no data copy — queries hit OneLake Delta files via Vertipaq when possible,
falls back to DirectQuery on miss).

**8 visuals:**
1. Encounter Volume by Year × Gender (stacked bar)
2. Top 15 Active Conditions by encounter count (bar)
3. Condition Co-occurrence Heatmap (top 10 × top 10)
4. SOAP Note Coverage by Year (line, 100% reference)
5. Vitals Availability KPIs (4 cards: HR / BP / temp / O2)
6. Medication–Condition Pairs Top 20 (matrix)
7. Patient Encounter Frequency — Top Utilizers (bar, color = max_conditions)
8. Imaging Modality × Body Site (matrix)

Power BI needs a **flattened fact table** for `active_conditions` and
`active_medications` (both `array<string>`). Build as views in the SQL endpoint
— not materialized tables — to avoid duplicating Gold:

```sql
CREATE VIEW gold.encounter_condition_fact AS
SELECT summary_id, patient_id, encounter_id, encounter_date,
       patient_age, patient_gender, encounter_type,
       explode(active_conditions) AS condition_display
FROM gold.encounter_summary;
-- analogous view for medications
```

Export as a `.pbip` project (Power BI Desktop → File → Save As → Power BI
Project) into `fabric/powerbi/scribe_iq.pbip` so model + report definitions are
Git-tracked. `.gitignore` rule: ignore `*.pbix` binary, track the `.pbip` JSON
definition. Screenshot the full dashboard + one drill-through.

---

## Files to modify (consolidated)

**Phase 1 — dedup fix:**
- [core/platform/local_lite.py](../../core/platform/local_lite.py)
- `core/tests/test_local_lite_platform.py` (or analogous test file)
- `docs/adr/019-silver-merge-idempotency.md` (new)
- [docs/adr/README.md](../adr/README.md)
- [HANDOFF.md](../../HANDOFF.md)

**Phase 2 — FabricPlatform:**
- [fabric/platform.py](../../fabric/platform.py)
- [fabric/tests/test_fabric_platform.py](../../fabric/tests/test_fabric_platform.py)
- [fabric/deploy/upload_wheel.py](../../fabric/deploy/upload_wheel.py)
- [.github/workflows/fabric-deploy.yml](../../.github/workflows/fabric-deploy.yml)

**Phase 3 — workspace setup:**
- [fabric/docs/DEPLOYMENT.md](../../fabric/docs/DEPLOYMENT.md)

**Phase 4 — notebooks:**
- `fabric/notebooks/00_setup.ipynb` … `10_gold_validation.ipynb` (10 new files)

**Phase 5 — pipeline:**
- `fabric/pipelines/medallion.json` (new)

**Phase 6 — exploratory notebook:**
- `fabric/notebooks/11_exploratory_analysis.ipynb` (new)

**Phase 7 — Power BI:**
- `fabric/powerbi/scribe_iq.pbip` (new project directory + `.gitignore` rules)

**Session-end (every phase boundary):**
- [HANDOFF.md](../../HANDOFF.md) — current state + next task
- [CHANGELOG.md](../../CHANGELOG.md) — per-phase entries
- Conventional commit per phase (`feat(silver):`, `feat(fabric):`, `docs(fabric):`, etc.)

---

## Critical files to read before each phase

- **Phase 1:** [core/platform/local_lite.py:133-157](../../core/platform/local_lite.py#L133-L157) (`_write_delta`), [core/transforms/schema_utils.py:119-129](../../core/transforms/schema_utils.py#L119-L129) (`dedup_by_key`), [HANDOFF.md](../../HANDOFF.md) lines 311-318 (defect description), [docs/adr/template.md](../adr/template.md).
- **Phase 2:** [core/platform/base.py](../../core/platform/base.py) (interface), [core/platform/local_lite.py](../../core/platform/local_lite.py) (reference impl), [fabric/docs/DEPLOYMENT.md](../../fabric/docs/DEPLOYMENT.md), ADR-001/002/017/018.
- **Phase 3:** [fabric/docs/DEPLOYMENT.md](../../fabric/docs/DEPLOYMENT.md), [fabric/docs/SCREENSHOTS.md](../../fabric/docs/SCREENSHOTS.md), [docs/roadmap/scribe-iq-lakehouse-spec.md](scribe-iq-lakehouse-spec.md) §15.2.
- **Phase 4:** [.claude/rules/notebooks.md](../../.claude/rules/notebooks.md), [core/transforms/registry.py](../../core/transforms/registry.py), [docs/DATA_DICTIONARY.md](../DATA_DICTIONARY.md).
- **Phase 6:** [docs/demo/notebooks/demo_notebook.sql](../demo/notebooks/demo_notebook.sql), [docs/demo/PLAYBOOK.md](../demo/PLAYBOOK.md).
- **Phase 7:** [docs/DATA_DICTIONARY.md](../DATA_DICTIONARY.md), [docs/CORPUS_CONTRACT.md](../CORPUS_CONTRACT.md) (especially as-of-date ADR-014).

---

## Stopping points

- **After Phase 1:** ADR-019 + dedup fix landed; 127+ tests; Dagster + CLI re-runs no longer require `rm -rf data/silver`. Reviewable PR.
- **After Phase 2:** FabricPlatform real impl; contract tests still pass offline; CI workflow can do an end-to-end deploy when SP secrets present.
- **After Phase 3:** Workspace live; environment provisioned; wheel uploaded; first screenshots captured.
- **After Phase 4:** Full medallion runs in Fabric; screenshots in hand; SOAP note demo centerpiece works.
- **After Phase 5:** Fabric Data Pipeline JSON committed; two orchestrators (Dagster local, Data Factory cloud) running the same medallion.
- **After Phase 6:** Exploratory notebook proves the corpus visually.
- **After Phase 7:** Portfolio-ready dashboard + `.pbip` committed.
