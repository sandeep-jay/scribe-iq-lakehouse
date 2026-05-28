# Benchmarks & data baseline

Reference numbers for the lakehouse — captured so changes in scale, runtime, or table
sizes are visible over time, and so reviewers can see proof-of-scale. Update when the
pipeline or dataset materially changes.

## Environment

| | |
|--|--|
| Machine | Apple Silicon laptop |
| Python | 3.11.9 (`.venv`) |
| Platform | `local_lite` — Polars 1.41 · deltalake (delta-rs) 1.6 · DuckDB 1.5 · pyarrow 24 |
| Source | `s3://synthea-open-data/coherent/unzipped/fhir/` (AWS Open Data, no credentials) |

## Dataset (Bronze)

| Metric | Value |
|--------|-------|
| FHIR files landed | 1,280 (2 are reference files: `organizations.json`, `practitioners.json`) |
| Distinct patients | 1,278 |
| Total size | 4.6 GiB (4,619,518,472 bytes) |
| Cohort partition | A = 427 · B = 427 · C = 426 (round-robin) |

## Ingest (S3 → Bronze)

| Metric | Value |
|--------|-------|
| FHIR `aws s3 sync` + cohort partition | ~18m36s (network-bound, parallel sync) |
| DICOM `aws s3 sync` (`--with-dicom`) | 298 files / 9.3 GiB (network-bound; one-time) |
| CSV `aws s3 sync` (`--with-csv`) | 16 files / 466 MB (reference only, not processed) |

## Pipeline (Bronze → Silver) — full dataset

| Metric | Value |
|--------|-------|
| Wall clock | **2m19s** (128.0s user · 7.5s sys · 97% CPU) — includes DICOM header reads for 298 studies |
| Per cohort (parse + build + MERGE) | A ~50s · B ~46s · C ~54s |
| Validation + read-back | <1s total |
| Cohorts | 3 (processed as sequential micro-batches) |
| Validations failed | 0 |

### Silver row counts

| Table | Rows | Notes |
|-------|-----:|-------|
| observation | 669,898 | vitals + labs (BP components in `components_json`) |
| medication_request | 209,401 | |
| encounter | 143,946 | |
| soap_note | 143,946 | ~1 note per encounter; S/A/P, no Objective (ADR-005) |
| procedure | 56,092 | |
| condition | 15,956 | |
| imaging_study | 3,752 | FHIR metadata; 298 enriched with DICOM headers (ADR-013) |
| genomic_report | 419 | `data_limitation` 100% populated; 0 pathogenic (synthetic) |
| patient | 1,278 | |
| ecg_metadata | 0 | ECG is Binary waveform, not in FHIR — roadmap Phase 3 |
| ingest_log | 11 | one validation row per Silver table per run |

## Gold (Silver → `gold.encounter_summary`) — full dataset

Polars in-process denormalization of all 10 Silver tables → one row per encounter
(ADR-012). Reads Silver via the platform, builds with the pure Gold transform, writes one
Delta table (overwrite + CDC) plus the corpus manifest.

| Metric | Value |
|--------|-------|
| Wall clock | **~6.5s** (incl. patient-level as-of-date joins for conditions/meds, ADR-014) |
| Output rows | 143,946 (one per encounter) |
| Output columns | 22 (incl. nested struct vitals/imaging/versions + array conditions/meds/labs) |
| Nested types in Delta | round-trip verified; CDC enabled |

### Corpus coverage

| Metric | Value | Note |
|--------|------:|------|
| Encounters | 143,946 | |
| Distinct patients | 1,278 | |
| With SOAP note | 143,946 (100%) | primary generation anchor |
| With labs | 26,059 | |
| With vitals | 19,830 | BP parsed from `components_json` |
| With imaging | 3,752 | 298 carry DICOM `study_date` (ADR-013) |
| With genomics | 419 | synthetic (ADR-007) |
| With ECG | 0 | no ECG in Coherent FHIR |
| Avg conditions / encounter | 9.57 | as-of-date problem list, onset+abatement gated (ADR-014) |
| Avg medications / encounter | 1.66 | as-of-date, status=active (ADR-014) |
| Encounters w/ empty problem list | 0.9% | down from the majority under the old encounter-grain join |

## Engine comparison (target matrix)

Same transforms, different platforms (one env var). Only `local_lite` is measured today.

| Capability | local_lite | local_spark | Fabric | Databricks | AWS | GCP |
|------------|-----------|-------------|--------|------------|-----|-----|
| Bronze→Silver (full) | ✅ 2m30s | — | 🔜 S5 | roadmap | roadmap | roadmap |
| Silver→Gold (full) | ✅ ~6.5s | — | 🔜 S5 | roadmap | roadmap | roadmap |
| CDC | ✅ | — | 🔜 | roadmap | roadmap | roadmap |
| Streaming | sim only | — | 🔜 Auto Loader | roadmap | roadmap | roadmap |
| Cost (1.3k pts) | $0 | $0 | trial | — | — | — |

## Execution surfaces

Same transforms, three orchestrators (the concrete payoff of ADR-002 / ADR-004):

| Surface | Where | Use it for |
|---------|-------|-----------|
| `core.surfaces.cli.pipeline` CLI | `core/surfaces/cli/pipeline.py` | Default, dependency-light, CI gate; full-rebuild + per-cohort flags |
| **Dagster asset graph** | `core/orchestration/dagster/` (ADR-015/016) | Per-cohort backfill via UI, `validate_table` as asset checks (rule-by-rule pass/fail in metadata), sensor on `data/bronze/fhir/`, run history. Each asset's `MaterializeResult` carries schema + sample rows + sample-bundle/SOAP-card markdown so clicking a node shows what materialized |
| Fabric notebooks | `fabric/notebooks/` (Session 5) | Same transforms over Spark + OneLake; Auto Loader streaming |

Dagster timings track the CLI numbers above (the work is in the transforms; orchestration
overhead is ~ms per asset on the fixture). No standalone benchmark — the value is the
graph, the checks, and partition-level backfill, not throughput.

### Demo / read-only query surface

For exploration alongside the three execution surfaces (not a fourth tier — purely
read-only over the existing Delta tables):

| Tool | Where | Demo use |
|------|-------|---------|
| `core/scripts/demo_walkthrough.py` | rich CLI, one patient end-to-end | Bronze → Parse → Silver → Gold for one anchor patient, with full SOAP note rendered |
| `docs/demo/notebooks/demo_notebook.sql` | DuckDB UI, 20 SQL cells | Corpus headlines, top conditions, as-of-date condition growth, full SOAP notes, keyword cohort search |
| `docs/demo/PLAYBOOK.md` | recording guide | 5-beat demo video script + take-by-take recording sequence |

All three render via `local/preview.py`, so the Dagster asset metadata, the CLI walkthrough,
and the SQL notebook present the same data shape.

## Reproduce

```bash
pip install -e ".[local,dev]"                 # or: .venv
python -m core.ingest.download --bronze-root data/bronze   # FHIR, ~4.6 GiB, network-bound
python -m core.ingest.download --assets-only --with-dicom --with-csv  # +9.3 GiB DICOM, 466 MB CSV
python -m core.surfaces.cli.pipeline --bronze-root data/bronze --with-gold  # → silver/* (~2m19s) + gold/* (~6.5s)
python -m core.surfaces.cli.pipeline --gold-only                        # rebuild Gold from existing Silver
```

> Full re-runs build from a clean slate (delta-rs MERGE is for incremental cohort landing,
> not whole-table re-update); remove `data/silver` + `data/gold` before a full
> `--with-gold` rebuild. Both are reproducible from Bronze.

## Methodology & caveats

- Single run, warm OS file cache; numbers are indicative, not a controlled benchmark.
- Ingest time is network-bound and will vary; the pipeline time is the stable figure.
- `local_lite` holds one cohort's records in memory at a time (~1/3 of the data); peak
  RSS stayed well under what a typical dev laptop offers. Full-dataset-in-memory was
  deliberately avoided.
- Fabric/Spark figures will be filled in when those platforms are implemented (Session 5).
