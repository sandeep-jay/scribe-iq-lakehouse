# Runbook — scribe-iq-lakehouse (local tier)

Operational procedures for ingesting, building, verifying, and troubleshooting the lakehouse
on the `local_lite` platform (Polars + delta-rs, zero cloud). The local stack ships with two
execution surfaces — the `local.pipeline` CLI (default, dependency-light, the CI path) and
a **Dagster** asset graph (`orchestration/`, ADR-015/016, optional `[orchestration]` extra);
both reuse the same pure transforms. Fabric procedures land with the notebooks in Session 5.
For *why* the system is shaped this way, see [ARCHITECTURE.md](ARCHITECTURE.md) and the
[ADRs](adr/README.md); for reference numbers, see [BENCHMARKS.md](BENCHMARKS.md).

> All commands assume the repo root and an activated venv (`source .venv/bin/activate`). If you
> haven't activated it, prefix with `.venv/bin/` (e.g. `.venv/bin/python -m local.pipeline`).

---

## 1. Prerequisites

| Need | Why | Check |
|------|-----|-------|
| Python 3.11+ | runtime | `python --version` |
| venv with `[local,dev]` extras | polars, delta-rs, duckdb, pytest, jsonschema | `pip install -e ".[local,dev]"` |
| Optional `[orchestration]` extra | Dagster + dagster-webserver (for §6) | `pip install -e ".[local,dev,orchestration]"` |
| AWS CLI | S3 ingest only (not needed to run tests or rebuild from existing Bronze) | `aws --version` |
| Disk | FHIR 4.6 GB; +9.3 GB if pulling DICOM; Silver/Gold a few hundred MB | `df -h .` |

The S3 source is **AWS Open Data — no credentials required**
(`s3://synthea-open-data/coherent/unzipped/`). All ingest uses `--no-sign-request`.

### Configuration

| Variable | Default | Purpose |
|----------|---------|---------|
| `LAKEHOUSE_PLATFORM` | `local_lite` | Selects the platform implementation (factory). |
| `LAKEHOUSE_LOCAL_ROOT` | `data` | Local storage root for bronze/silver/gold. |
| `DAGSTER_HOME` | (unset) | Where `dagster dev` keeps run history / schedules / storage. Set to `$PWD/dagster_home` so it lives next to the repo and is gitignored. |

Storage layout (all gitignored under `data/`):

```
data/
├── bronze/
│   ├── fhir/cohort=A|B|C/*.json        # 1,280 FHIR bundles
│   ├── dicom/*.dcm                     # 298 DICOM files (optional)
│   ├── csv/*.csv                       # 16 CSV exports (optional, reference only)
│   └── _metadata/{manifest,assets_manifest}.json
├── silver/<10 tables>/ + ingest_log/   # Delta, CDC enabled
└── gold/encounter_summary/ + _metadata/corpus_manifest.json
```

---

## 2. First full run (end to end)

```bash
# 1. Install
python -m venv .venv && source .venv/bin/activate
pip install -e ".[local,dev]"

# 2. Land FHIR into Bronze (~18 min, network-bound; partitions into cohorts A/B/C)
python -m local.ingest.download --bronze-root data/bronze

# 3. (Optional) Land DICOM + CSV assets (~10 GB; enables imaging header extraction)
python -m local.ingest.download --assets-only --with-dicom --with-csv

# 4. Build Bronze → Silver → Gold (~2.5 min Silver + ~6.5 s Gold)
python -m local.pipeline --with-gold

# 5. Verify (see §5)
```

Step 4 prints a JSON summary of per-table row counts and the Gold corpus stats.

---

## 3. Ingest

### FHIR bundles

```bash
python -m local.ingest.download --bronze-root data/bronze        # full dataset (1,280 files)
python -m local.ingest.download --max-files 30                   # dev subset (fast)
```

Bundles download flat, then partition **round-robin** into `cohort=A|B|C` (balanced, order-
independent samples — the local analogue of streaming micro-batches). A manifest is written to
`data/bronze/_metadata/manifest.json`. Re-runs are idempotent (`aws s3 sync` skips existing).

### DICOM + CSV assets (optional)

```bash
python -m local.ingest.download --assets-only --with-dicom --with-csv
```

- **DICOM** (~9.3 GB, 298 files) → `data/bronze/dicom/`. Enables `silver.imaging_study` header
  extraction (study_date, dimensions, slice thickness) for the 298 studies that have a file.
  See [ADR-013](adr/013-dicom-ingest-and-linkage.md). Writes `_metadata/assets_manifest.json`.
- **CSV** (~466 MB, 16 files) → `data/bronze/csv/`. Landed for reference; **not** processed by
  any transform (the lakehouse is FHIR-first).

If you skip DICOM, the pipeline still runs — imaging rows simply keep FHIR-only metadata
(DICOM header columns stay null, `dicom_extracted = false`).

---

## 4. Build the medallion

### Full Bronze → Silver → Gold

```bash
python -m local.pipeline --with-gold
```

Per cohort: parse bundles → build all 10 Silver tables → MERGE-upsert into Delta (DICOM headers
merged in if `data/bronze/dicom/` is present). After all cohorts land, every table is read back,
validated (spec §5.6) and logged to `silver.ingest_log`. Then Gold is denormalized and written
with its corpus manifest.

### Gold only (Silver already built)

```bash
python -m local.pipeline --gold-only
```

Reads the existing Silver tables and rebuilds `gold.encounter_summary` + manifest (~6.5 s). Use
after changing only Gold logic.

### Single cohort / incremental

```bash
python -m local.pipeline --cohort A           # process one cohort (MERGE-upsert)
python -m local.pipeline --cohort A --cohort B # process several
```

MERGE upsert means re-landing a cohort updates its rows in place — the intended path for
incremental ingest.

### Full clean rebuild

```bash
rm -rf data/silver data/gold
python -m local.pipeline --with-gold
```

**Required for a whole-dataset re-run** (see [Troubleshooting](#troubleshooting)). Silver and
Gold are derived data and always reproducible from Bronze, so deleting them is safe.

### Streaming simulation

`local/ingest/streaming_sim.py` replays cohort partitions one at a time with a filesystem
watchdog — the local stand-in for Fabric Auto Loader (spec §5.2). It is a library used by the
pipeline/tests, not a standalone CLI.

---

## 5. Verify a build

Quick row-count + coverage check (matches the pipeline's printed summary and
[BENCHMARKS.md](BENCHMARKS.md)):

```python
import json
from deltalake import DeltaTable

for t in ["patient", "encounter", "condition", "observation", "imaging_study"]:
    dt = DeltaTable(f"data/silver/{t}")
    print(f"silver.{t:14s} rows={dt.to_pyarrow_dataset().count_rows():>7}  v{dt.version()}")

g = DeltaTable("data/gold/encounter_summary")
print("gold.encounter_summary rows:", g.to_pyarrow_dataset().count_rows())
print(json.load(open("data/gold/_metadata/corpus_manifest.json"))["corpus_stats"])
```

Expected at full scale: `encounter` = 143,946; `gold.encounter_summary` = 143,946;
`distinct_patients` = 1,278; avg conditions/encounter ≈ 9.6, medications ≈ 1.7; imaging studies
with DICOM = 298. Validation failures show up as `_failed > 0` in the run summary and as rows in
`silver.ingest_log` with `passed = false`.

Ad-hoc SQL over the Delta tables with DuckDB (no Spark):

```python
import duckdb
duckdb.sql("""
  SELECT encounter_type, count(*) n
  FROM delta_scan('data/gold/encounter_summary')
  GROUP BY 1 ORDER BY n DESC LIMIT 10
""").show()
```

Read a SOAP-note sample (the demo centerpiece):

```python
duckdb.sql("SELECT soap_note_text FROM delta_scan('data/gold/encounter_summary') "
           "WHERE soap_note_text IS NOT NULL LIMIT 1").show(max_width=120)
```

Or run the full one-patient walkthrough for a rendered Bronze → Silver → Gold demo
(also what's reused by the Dagster asset metadata, see §6):

```bash
python -m scripts.demo_walkthrough              # auto-picks a "good demo" patient
python -m scripts.demo_walkthrough --pause 1.5  # 1.5s between sections (screencast pacing)
python -m scripts.demo_walkthrough --patient-id <uuid>   # reproducible
```

For **interactive SQL exploration** — corpus headlines, top conditions, full SOAP notes,
keyword search, as-of-date condition evolution — open the DuckDB UI notebook:

```bash
brew install duckdb                                  # needs DuckDB ≥1.2 (-ui flag)
duckdb docs/demo/notebooks/demo.duckdb -ui           # opens http://localhost:4213
```

20 SQL cells over the Delta tables; see [`docs/demo/notebooks/README.md`](demo/notebooks/README.md)
for the per-cell guide and how to regenerate the `.duckdb` (gitignored) if missing.
For recording a portfolio demo video around it, see [`docs/demo/PLAYBOOK.md`](demo/PLAYBOOK.md).

---

## 6. Run via Dagster (optional)

The medallion is also exposed as a Dagster software-defined asset graph in `orchestration/`
([ADR-015](adr/015-dagster-local-orchestration.md), [ADR-016](adr/016-dagster-asset-graph.md))
— a richer, observable execution surface alongside the CLI. The same pure transforms run
under both; only the orchestration layer differs.

### Install + launch the UI

```bash
pip install -e ".[local,dev,orchestration]"
export DAGSTER_HOME="$PWD/dagster_home" && mkdir -p "$DAGSTER_HOME"
dagster dev                    # opens http://localhost:3000 (asset graph)
```

`dagster dev` reads `[tool.dagster] module_name = "orchestration.definitions"` from
`pyproject.toml`. The asset graph nodes are:

```
bronze_fhir [cohort-partitioned]
  └─→ silver_tables (multi_asset, cohort-partitioned, 10 outputs)
       ├─→ patient · encounter · observation · condition · procedure
       ├─→ medication_request · soap_note · imaging_study · genomic_report · ecg_metadata
       │     (each with a @asset_check wrapping validate_table)
       └─→ gold_encounter_summary [unpartitioned aggregate]
             (also writes gold/_metadata/corpus_manifest.json)
```

### Per-cohort materialization & backfill

Each cohort under `data/bronze/fhir/cohort=*/` is a Dagster partition that flows through
**both** `bronze_fhir` and the `silver_tables` multi-asset (Gold is unpartitioned). From
the UI:

1. **Assets → bronze_fhir → Materialize → pick a partition (e.g. `A`)** — runs Bronze and
   all 10 Silver tables for cohort `A` in one Dagster run (MERGE-upsert); row counts
   surface as asset metadata.
2. **Backfill** the missing partitions (`B`, `C`, …) from the same page — or right-click
   `silver_tables` → Materialize all to fan out across every registered partition.
3. After all cohorts are present, materialize **`gold_encounter_summary`** (unpartitioned)
   to rebuild the corpus and the manifest.

This is the **incremental path that actually works** — full-table whole rebuilds still need
the CLI clean-slate dance (see Troubleshooting), because delta-rs MERGE is incremental by
design.

### What clicking an asset shows

Each asset surfaces inline metadata so the graph isn't just lineage — it's the actual data:

- **`bronze_fhir[<cohort>]`** — file count + total bytes + a Markdown breakdown of the
  first bundle (FHIR resource-type counts: Patient, Encounter, Observation, ...).
- **Silver outputs (per partition)** — row count, primary key, the full Arrow schema as a
  table, and a sample of the first 5 rows. Click any one of the 10 Silver assets to see
  what that table actually looks like for that cohort.
- **`gold_encounter_summary`** — corpus stats JSON + Silver source versions + the **full
  schema** + one sample encounter rendered as a Markdown card (patient/date/age + SOAP
  note text + active conditions / medications / vitals / imaging).

The same renderings are reused by `python -m scripts.demo_walkthrough` for a CLI audience
and by the DuckDB UI notebook for an SQL audience (see §5 above and
[`docs/demo/PLAYBOOK.md`](demo/PLAYBOOK.md) for the portfolio-video recording guide).

### Asset checks — rule-by-rule detail

Every Silver table carries an `@asset_check` that re-reads via the platform and runs the
same `validate_table()` the CLI logs to `silver.ingest_log`. Click the check in the UI to
see a Markdown table of **every rule** that ran — name, pass/fail, and the actual numbers
(e.g. `non_null:patient_id` → "0 nulls / 1,278 rows", `unique:encounter_id` → "143,946/143,946
distinct"). Failures surface as red badges; pre-fix this was the imperative pipeline's only
visibility into validation.

### Bronze cohort sensor (the demo)

`bronze_cohort_sensor` is the Dagster analogue of the Auto Loader streaming-sim
([spec §5.2](roadmap/scribe-iq-lakehouse-spec.md#52-streaming-simulation)). Every tick it
diffs cohort directories under `data/bronze/fhir/` against the registered dynamic
partitions and, for each **new** cohort, fires a single `RunRequest` that materializes
`bronze_fhir` **and all 10 Silver tables** for that partition — Bronze → Silver in one
run. Gold is intentionally **not** in the sensor target (unpartitioned aggregate; rebuild
manually once the cohorts of interest are present).

- **Default status: STOPPED.** Otherwise it would replay every existing cohort on
  `dagster dev` startup. Start it from **Overview → Sensors → bronze_cohort_sensor**.
- **Polling interval: 30 s.**
- **Drop a new cohort to demo it:** create `data/bronze/fhir/cohort=D/` with one or more
  FHIR bundles and wait up to 30 s — the run appears in **Runs** and Bronze + Silver
  partitions for `D` fill in. (To re-demo across A/B/C, you can clear the dynamic partition
  set first: `dagster asset wipe --partitions A B C` or, simpler for a fresh repo, just
  point at a fresh `LAKEHOUSE_LOCAL_ROOT` so the sensor sees A/B/C as new.)
- The sensor adds new partition keys in the same evaluation as it issues runs
  (`dynamic_partitions_requests` + `run_requests` in one `SensorResult`) so the partitions
  exist before the runs start.

### Persistence model (ADR-016)

Assets return `MaterializeResult` (metadata only) — **the `LakehousePlatform` writes the
Delta bytes**, not a Dagster IOManager. Single persistence authority; Dagster owns the DAG
and observability. The `[orchestration]` extra is *optional* by design — CI and the CLI
path don't need it; `tests/test_dagster_defs.py` uses `pytest.importorskip` so the suite
collects cleanly without it.

---

## 7. Regenerate generated docs

The data dictionary and corpus JSON Schema are **generated from code** and guarded by tests +
pre-commit ([ADR-011](adr/011-generated-first-docs.md)). Regenerate after changing a Silver
schema, a validation rule, or the Gold schema/contract:

```bash
python scripts/gen_data_dictionary.py      # docs/DATA_DICTIONARY.md
python scripts/gen_corpus_schema.py        # schemas/gold_encounter_summary.json
# CI / pre-commit run the read-only --check variants and fail on drift.
```

Never hand-edit those two files; durable per-column prose goes in the generator's `_COLUMN_NOTES`.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `DeltaError: MERGE matched a target row with multiple source rows` | Full re-run on top of existing tables — MERGE upsert is for *incremental* landing, not whole-table re-update (every source row matches) | `rm -rf data/silver data/gold` then `python -m local.pipeline --with-gold` (clean slate; rebuilds from Bronze) |
| `RuntimeError: No cohorts found under .../fhir/` | Bronze not populated | Run `python -m local.ingest.download` first |
| `RuntimeError: AWS CLI not found on PATH` | AWS CLI missing | Install AWS CLI (only ingest needs it; tests/rebuilds don't) |
| Imaging DICOM columns all null / `dicom_extracted = false` | DICOM prefix not pulled | `python -m local.ingest.download --assets-only --with-dicom`, then rebuild Silver |
| `Skipping unreadable DICOM header for one imaging study` (warning) | A malformed `.dcm` | Non-fatal by design — FHIR metadata stands for that study ([ADR-013](adr/013-dicom-ingest-and-linkage.md)) |
| `imaging.study_description` is null even with DICOM | Coherent ships placeholder `UNKNOWN` description tags, normalized to null ([ADR-013](adr/013-dicom-ingest-and-linkage.md)) | Expected — ground imaging on `modality` + `body_site_display` |
| `active_conditions` huge (avg ~9.6) | As-of-date problem list incl. Synthea SDOH/social "conditions" ([ADR-014](adr/014-problem-list-as-of-date.md)) | Expected — faithful to source |
| `DATA_DICTIONARY.md is out of date` (test/commit fails) | Silver schema/rule changed without regen | `python scripts/gen_data_dictionary.py` (and `gen_corpus_schema.py`) |
| Out of disk during DICOM pull | 9.3 GB of `.dcm` | DICOM is optional and re-pullable from S3; safe to delete `data/bronze/dicom` |
| `.claude/settings.json` keeps showing modified | Harness appends auto-approved permissions to the tracked file | Move them to gitignored `.claude/settings.local.json`, `git restore` the tracked file |
| `ModuleNotFoundError: No module named 'dagster'` when running `dagster dev` or `tests/test_dagster_defs.py` | `[orchestration]` extra not installed (deliberately optional) | `pip install -e ".[local,dev,orchestration]"`; the test file uses `importorskip` so the rest of the suite still runs |
| `dagster dev` creates a stray `.tmp_dagster_home_*/` next to the repo | `DAGSTER_HOME` is unset | `export DAGSTER_HOME="$PWD/dagster_home"` before `dagster dev` (gitignored) |
| Dagster `bronze_cohort_sensor` not firing | Sensor is **default-STOPPED** (avoids replaying existing cohorts on startup) | Overview → Sensors → toggle on |
| Dagster Silver materialize errors with `MERGE matched a target row with multiple source rows` | Re-materializing an already-landed cohort against a stale full-table state (same root cause as the CLI gotcha) | Materialize per-partition (cohorts are the working incremental unit); for a full reset, use the CLI clean-slate path |

### Logging & PHI safety

Logs never contain patient/encounter identifiers, note text, or bundle filenames — identifier-
bearing values are redacted to a non-reversible `ref:<hash>` ([ADR-010](adr/010-phi-safe-logging.md)).
Set log level via standard `logging` config; the pipeline emits INFO progress + per-table metrics.
