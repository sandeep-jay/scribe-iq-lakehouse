# Notebooks — interactive demos for the lakehouse

This directory holds DuckDB UI notebooks that query the Silver/Gold Delta tables
directly — the third "self-serve" surface alongside the
[`local.pipeline`](../../../local/pipeline.py) CLI and the
[Dagster asset graph](../../../orchestration/). No Python required; pure SQL.

## Files

| File | Purpose | Tracked? |
|------|---------|----------|
| `demo_notebook.sql` | Canonical 20-cell demo notebook source — copy/paste into DuckDB UI | yes |
| `demo.duckdb` | Pre-built DuckDB database with all Delta views wired (so opening it gives a ready-to-query session) | **no** — local state, paths are machine-specific, `*.duckdb` is gitignored |

## Prerequisites

- DuckDB **≥ 1.2** (needs the `-ui` flag): `brew install duckdb`
- Silver + Gold tables built locally — run `python -m local.pipeline --with-gold`
  or materialize through Dagster first (see [docs/RUNBOOK.md §6](../../RUNBOOK.md))

## Quick start

Run from the repo root:

```bash
# Opens browser at http://localhost:4213 with the views already in the schema sidebar.
duckdb docs/demo/notebooks/demo.duckdb -ui
```

If `demo.duckdb` doesn't exist (e.g. fresh clone, gitignored), regenerate it in
one line — the views point at your local `data/` directory:

```bash
.venv/bin/python -c "
import duckdb, os
con = duckdb.connect('docs/demo/notebooks/demo.duckdb')
con.execute('INSTALL delta; LOAD delta;')
repo = os.path.abspath('.')
for v in ['patient','encounter','observation','condition','medication_request','procedure','soap_note','imaging_study','genomic_report','ecg_metadata']:
    con.execute(f\"CREATE OR REPLACE VIEW silver_{v} AS SELECT * FROM delta_scan('{repo}/data/silver/{v}')\")
con.execute(f\"CREATE OR REPLACE VIEW gold AS SELECT * FROM delta_scan('{repo}/data/gold/encounter_summary')\")
"
```

Or just open a fresh DuckDB UI session and paste **Cell 0** from `demo_notebook.sql`
(edit the `SET VARIABLE repo = ...` line to your repo's absolute path).

## Demo notebook — what's inside

20 cells, grouped:

| Cells | Theme |
|-------|-------|
| **0** | Setup — load Delta extension, create one view per Silver/Gold table |
| **1–4** | Corpus shape — headlines, schema, encounter mix, demographics |
| **5–7** | Clinical signal — top conditions, top medications, longitudinal span |
| **8–10** | Distributions — co-morbidity buckets, vitals percentiles, imaging modalities |
| **11–14** | One patient's journey — anchor picker, timeline, as-of-date evolution, full SOAP note |
| **15–16** | Cohort queries — keyword search, coverage stats |
| **17–20** | Lineage + sanity — Silver versions, cross-layer join, final shape |

Full cell-by-cell rationale + the screencast highlight reel are in the sibling
[**PLAYBOOK.md**](../PLAYBOOK.md).

## Tips

- **Save your changes** in DuckDB UI — the notebook view persists into the
  `.duckdb` file. Reopen later to pick up exactly where you left off.
- **Absolute paths** — DuckDB UI's working directory is `$HOME` by default, so
  every `delta_scan(...)` needs an absolute path. Cell 0 handles this via the
  `repo` variable; edit that one constant if your repo lives elsewhere.
- **`silver.ingest_log`** is only written by the CLI path (`local.pipeline`),
  not by the Dagster path. If you built Silver via Dagster, the validation log
  lives in the Dagster UI's asset-check panel instead — see
  [ADR-016](../../adr/016-dagster-asset-graph.md).
