# scribe-iq-lakehouse

Production-pattern healthcare data lakehouse on [Synthea Coherent](https://registry.opendata.aws/synthea-coherent-data/)
(1,278 synthetic patients, FHIR R4). Fabric-first **medallion** architecture — Bronze → Silver
→ Gold — that runs identically on a laptop (Polars + delta-rs) or Microsoft Fabric via a
single platform-abstraction layer. The Gold corpus (`gold.encounter_summary`) feeds
[`scribe-iq`](docs/roadmap/scribe-iq-lakehouse-spec.md) (clinical RAG),
`clinical-bert-pipeline` (NLP), and an Ollama dialogue-generation pipeline.

**Status:** Bronze → Silver → **Gold** fully built and run end-to-end on the complete
1,278-patient dataset locally (143,946 encounter summaries). DICOM imaging headers ingested.
**Dagster** local orchestration tier renders the medallion as a software-defined asset graph
(third execution surface alongside the CLI and the upcoming Fabric notebooks). Fabric
execution (notebooks 00–10) is the next milestone. Synthetic data only — **no PHI**.

```
 AWS Open Data S3            Bronze (raw)              Silver (10 Delta tables)        Gold
 coherent/unzipped/   ──►   fhir/ · dicom/ · csv/  ──► patient · encounter · …    ──►  encounter_summary
 (no credentials)           + manifests               condition · observation …       (1 row / encounter)
                                                       CDC enabled, validated          + corpus_manifest.json
                                                                                       └─► scribe-iq · BERT · Ollama

  execution surfaces (same pure transforms): local.pipeline CLI · Dagster (orchestration/) · Fabric notebooks
```

---

## Quick start

Requires Python 3.11+ and (for ingest only) the AWS CLI.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[local,dev]"     # core + local-lite (polars/delta-rs/duckdb) + dev tooling
# Optional: add ",orchestration" for the Dagster asset-graph UI (dagster + dagster-webserver)
pytest                            # 122 tests, no cloud / Fabric / network needed
```

Parse a single FHIR bundle (pure, no I/O):

```python
import json
from local.transforms.fhir_parser import FHIRBundleParser

bundle = json.load(open("tests/fixtures/sample_bundle.json"))
records = FHIRBundleParser().parse_bundle(bundle)
#  -> {"patient": [...], "encounter": [...], "soap_note": [...], "condition": [...], ...}
```

Run the whole lakehouse locally (downloads ~4.6 GB FHIR, then builds Silver + Gold):

```bash
python -m local.ingest.download --bronze-root data/bronze   # FHIR → Bronze (~18 min, network-bound)
python -m local.pipeline --with-gold                        # Bronze → Silver → Gold (~2.5 min)
```

Full operational procedures — ingest, rebuilds, DICOM, verification, troubleshooting — are in
the **[Runbook](docs/RUNBOOK.md)**.

### See one patient flow through the medallion

```bash
python -m scripts.demo_walkthrough           # auto-picks a good demo patient
python -m scripts.demo_walkthrough --patient-id <uuid>
```

Renders one synthetic patient's journey Bronze → Parse → Silver → Gold in the terminal:
FHIR resource-type counts, the raw Patient JSON, parsed records, that patient's typed Silver
rows, and finally one `gold.encounter_summary` row with the SOAP note rendered as readable
clinical text. The same data shape is rendered inline in the Dagster asset graph — click any
asset and the metadata panel shows schema + sample rows for that materialization.

### Query the lakehouse interactively (DuckDB UI)

```bash
brew install duckdb                                     # needs ≥1.2 for the -ui flag
duckdb docs/demo/notebooks/demo.duckdb -ui              # opens browser at http://localhost:4213
```

20-cell notebook over the Delta tables — corpus headlines, top conditions (anemia,
hypertension, diabetes), as-of-date condition evolution for one patient, full SOAP notes,
keyword cohort search, coverage stats. Pure SQL, no Spark. See
[**docs/demo/notebooks/README.md**](docs/demo/notebooks/README.md) for setup and the per-cell
guide; recording guide is in [**docs/demo/PLAYBOOK.md**](docs/demo/PLAYBOOK.md).

---

## Operations (common tasks)

| Task | Command |
|------|---------|
| Install (local + dev) | `pip install -e ".[local,dev]"` |
| Install + Dagster orchestration tier | `pip install -e ".[local,dev,orchestration]"` |
| Download FHIR → Bronze | `python -m local.ingest.download --bronze-root data/bronze` |
| Download DICOM + CSV (optional, ~10 GB) | `python -m local.ingest.download --assets-only --with-dicom --with-csv` |
| Build Bronze → Silver → Gold | `python -m local.pipeline --with-gold` |
| Rebuild **Gold only** (Silver exists) | `python -m local.pipeline --gold-only` |
| Process a single cohort | `python -m local.pipeline --cohort A` |
| **Full clean rebuild** | `rm -rf data/silver data/gold && python -m local.pipeline --with-gold` |
| Launch Dagster UI (asset graph) | `DAGSTER_HOME="$PWD/dagster_home" dagster dev` |
| One-patient demo walkthrough | `python -m scripts.demo_walkthrough` |
| Interactive SQL notebook (DuckDB) | `duckdb docs/demo/notebooks/demo.duckdb -ui` |
| Run tests / lint / format | `pytest` · `ruff check local tests scripts` · `black local tests scripts` |
| Regenerate generated docs | `python scripts/gen_data_dictionary.py` · `python scripts/gen_corpus_schema.py` |

> **Gotcha — full re-runs need a clean slate.** delta-rs MERGE upsert is for *incremental*
> per-cohort landing, not whole-table re-update; a full re-run on top of existing tables errors.
> Remove `data/silver` + `data/gold` first (both rebuild from Bronze). See the
> [Runbook → Troubleshooting](docs/RUNBOOK.md#troubleshooting).

The execution engine is chosen by the `LAKEHOUSE_PLATFORM` env var (default `local_lite`);
the local storage root is `data/` (override with `LAKEHOUSE_LOCAL_ROOT`). Nothing under
`data/` is committed.

---

## Architecture at a glance

See **[ARCHITECTURE.md](docs/ARCHITECTURE.md)** for the as-built diagram and module map, and the
[ADRs](docs/adr/README.md) for *why*.

- **Platform abstraction** ([ADR-002](docs/adr/002-platform-abstraction.md)) — all cloud/engine
  I/O goes through `local/platform/`. One env var selects Fabric, local-lite, Databricks, AWS,
  or GCP; transform code never changes.
- **Pure transforms + Arrow interchange** ([ADR-004](docs/adr/004-arrow-interchange.md)) —
  `local/transforms/` and `local/gold/` are engine-agnostic Python that return explicitly-typed
  `pyarrow.Table`s. No Spark/Delta/platform imports, no file paths. Polars is used only as an
  in-process join engine.
- **Three execution surfaces, one transform tier** — the same pure transforms run under the
  `local.pipeline` CLI (default, dependency-light), a **Dagster** asset graph
  ([ADR-015](docs/adr/015-dagster-local-orchestration.md),
  [ADR-016](docs/adr/016-dagster-asset-graph.md)) with cohort-partitioned backfill and
  `validate_table` surfaced as asset checks, and the Fabric notebooks (next).
- **CDC everywhere** — Change Data Feed is enabled on every Silver/Gold Delta table on creation
  ([ADR-009](docs/adr/009-local-silver-materialization.md)).
- **Honest data modeling** — genomic `data_limitation` is a first-class column
  ([ADR-007](docs/adr/007-genomic-data-limitation.md)); DICOM headers extracted without pixel
  data ([ADR-006](docs/adr/006-dicom-stop-before-pixels.md), [ADR-013](docs/adr/013-dicom-ingest-and-linkage.md));
  the Gold problem list is point-in-time, as of each encounter date
  ([ADR-014](docs/adr/014-problem-list-as-of-date.md)).
- **PHI-safe by construction** — logs never contain patient/encounter identifiers or bundle
  filenames; identifier-bearing values are redacted to a non-reversible `ref:<hash>`
  ([ADR-010](docs/adr/010-phi-safe-logging.md)).

---

## Data products

| Product | What | Contract |
|---------|------|----------|
| `silver.*` (10 tables) | Typed, deduped, CDC-enabled FHIR entities | [DATA_DICTIONARY.md](docs/DATA_DICTIONARY.md) (generated) |
| `gold.encounter_summary` | Denormalized corpus, one row per encounter | [CORPUS_CONTRACT.md](docs/CORPUS_CONTRACT.md) **v1.1.0** + [`schemas/gold_encounter_summary.json`](schemas/gold_encounter_summary.json) |
| `gold/_metadata/corpus_manifest.json` | Lineage + coverage stats per build | — |

The corpus contract is the versioned handoff to downstream consumers; a test fails if the code,
the JSON Schema, and the contract ever drift apart ([ADR-011](docs/adr/011-generated-first-docs.md)).
Real run metrics (timings, row counts, coverage) live in [BENCHMARKS.md](docs/BENCHMARKS.md).

---

## Repository layout

| Path | Purpose |
|------|---------|
| `local/platform/` | Cloud/engine abstraction — the only place platform code lives ([ADR-002](docs/adr/002-platform-abstraction.md)) |
| `local/transforms/` | Pure FHIR → record transforms + Silver schemas/registry (return Arrow) |
| `local/gold/` | Gold denormalization (`encounter_summary`) + corpus manifest |
| `local/validation/` | Schema/quality rules → `silver.ingest_log` |
| `local/ingest/` | S3 download, cohort partitioning, DICOM index, streaming simulation |
| `local/pipeline.py` | Bronze → Silver → Gold orchestration (`run_pipeline`, `build_gold`) |
| `local/redaction.py` | PHI-safe log redaction ([ADR-010](docs/adr/010-phi-safe-logging.md)) |
| `orchestration/` | Dagster asset graph — assets, checks, sensor, partitions, resources ([ADR-015](docs/adr/015-dagster-local-orchestration.md), [ADR-016](docs/adr/016-dagster-asset-graph.md)) |
| `docs/demo/` | Demo playbook + DuckDB notebook ([PLAYBOOK.md](docs/demo/PLAYBOOK.md), [notebooks/README.md](docs/demo/notebooks/README.md)) |
| `scripts/` | Generators for the code-mirroring docs (`--check` in CI) · `demo_walkthrough.py` |
| `schemas/` | Machine-readable corpus JSON Schema |
| `fabric/notebooks/` | Fabric execution notebooks 00–10 (Session 5) |
| `tests/` | 122 unit tests + synthetic `sample_bundle.json` fixture |
| `docs/` | ARCHITECTURE · RUNBOOK · DATA_DICTIONARY · CORPUS_CONTRACT · BENCHMARKS · ADRs · roadmap |

---

## Documentation map

- **[RUNBOOK.md](docs/RUNBOOK.md)** — operational procedures, verification, troubleshooting
- **[ARCHITECTURE.md](docs/ARCHITECTURE.md)** — as-built design, diagram, module map
- **[DATA_DICTIONARY.md](docs/DATA_DICTIONARY.md)** — every Silver column + validation rule (generated)
- **[CORPUS_CONTRACT.md](docs/CORPUS_CONTRACT.md)** — the Gold handoff contract (v1.1.0)
- **[BENCHMARKS.md](docs/BENCHMARKS.md)** — real run metrics + engine matrix
- **[demo/PLAYBOOK.md](docs/demo/PLAYBOOK.md)** — recording guide for the portfolio video demo
- **[docs/adr/](docs/adr/README.md)** — 16 Architecture Decision Records
- **[docs/roadmap/](docs/roadmap/scribe-iq-lakehouse-spec.md)** — full spec + cross-repo plan

## Testing & quality

```bash
pytest                              # 122 tests; fixture-only, no cloud/network
ruff check local tests scripts      # lint
black --check local tests scripts   # format check
python scripts/gen_data_dictionary.py --check   # docs-as-test (CI gate)
python scripts/gen_corpus_schema.py --check
```

Tests run against the synthetic `tests/fixtures/sample_bundle.json` — never real patient data.
`pre-commit` adds secret scanning (detect-secrets, gitleaks), security linting (bandit, semgrep),
and read-only doc-currency gates.

## License

MIT. Built on Synthea Coherent **synthetic** data — contains no real patient information.
