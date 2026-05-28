# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 09 — Gold: `encounter_summary` + corpus manifest
#
# **Purpose.** Denormalize the 10 Silver tables into `gold.encounter_summary` — one row per encounter, with patient demographics, the encounter's SOAP note, active conditions/medications/vitals as-of-encounter-date, latest imaging/ECG/genomics. This is the **corpus contract handoff** to downstream consumers (`scribe-iq` RAG, `clinical-bert-pipeline` NLP).
#
# **Inputs.** All 10 Silver Delta tables populated by 02–07.
#
# **Outputs.**
# - Delta table `Tables/gold/encounter_summary` (overwrite — Gold is fully derived).
# - JSON file `Files/gold/_metadata/corpus_manifest.json` — lineage (Silver versions, row counts) + corpus stats (counts, coverage %, distribution).
#
# **Expected scale.** ~143,946 encounter summary rows at full Coherent scale (matches `silver.encounter`).
#
# **Contract version.** 1.1.0 — see [docs/CORPUS_CONTRACT.md](../../docs/CORPUS_CONTRACT.md). Any schema change is a contract version bump.
#
# Screenshot the corpus-stats + sample-encounter card as `09_gold_encounter_summary.png`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture context
#
# - **[ADR-012](../../docs/adr/012-gold-encounter-summary.md)** — Gold grain = encounter; engine = pure-Python pyarrow; explicit `GOLD_SCHEMA` matched exactly.
# - **[ADR-014](../../docs/adr/014-problem-list-as-of-date.md)** — active_conditions / active_medications are computed *as of the encounter date*, not current-as-of-now. The killer query — chronic disease accumulates visibly over a patient's encounter timeline.
# - **Lineage:** Silver table versions are read via `platform.table_version("silver", name)` and captured in the manifest so downstream consumers can pin a corpus build to specific Silver versions.
# - **Idempotent rebuild:** Gold is overwrite-mode; any Silver change → re-run 09 to regenerate. No Gold MERGE.
#
# Pure logic lives in `core/gold/encounter_summary.py` + `core/gold/corpus_manifest.py`. This notebook is the Fabric execution surface; the same builders run under the CLI (`python -m core.surfaces.cli.pipeline --gold-only`) and the Dagster `gold_encounter_summary` asset.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
from datetime import UTC, datetime

os.environ["LAKEHOUSE_PLATFORM"] = "fabric"

from core.gold.corpus_manifest import build_corpus_manifest
from core.gold.encounter_summary import (
    CONTRACT_VERSION,
    SILVER_SOURCES,
    TABLE_NAME,
    build_encounter_summary,
)
from core.platform.factory import get_platform

platform = get_platform()
created_ts = datetime.now(UTC)
print(f"Platform: {platform.name} · Building gold.{TABLE_NAME} · Contract v{CONTRACT_VERSION}")
print(f"Reading {len(SILVER_SOURCES)} Silver sources: {', '.join(SILVER_SOURCES)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform approach
#
# 1. Read every Silver source table via the platform (`platform.read_silver(name) → pa.Table`).
# 2. Capture each Silver table's Delta version via `platform.table_version("silver", name)` — these go into the lineage manifest.
# 3. `build_encounter_summary(silver_tables, created_ts, silver_versions)` does the join/denormalization in pure Python + pyarrow.
# 4. `platform.write_gold("encounter_summary", gold)` — overwrite mode + CDC on.
# 5. `build_corpus_manifest(...)` produces the lineage JSON (Silver versions + corpus stats); `platform.write_gold_manifest(...)` writes it to `Files/gold/_metadata/corpus_manifest.json`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver = {name: platform.read_silver(name) for name in SILVER_SOURCES}
counts = {name: t.num_rows for name, t in silver.items()}
versions = {name: platform.table_version("silver", name) for name in SILVER_SOURCES}

print("Silver source row counts (and Delta versions):")
for name in SILVER_SOURCES:
    print(f"  silver.{name:<20s} rows={counts[name]:>8,}  v{versions[name]}")

gold = build_encounter_summary(silver, created_ts=created_ts, silver_versions=versions)
print(f"\nBuilt gold.{TABLE_NAME}: {gold.num_rows:,} rows, {len(gold.schema)} columns")

platform.write_gold(TABLE_NAME, gold)
print(f"Wrote gold.{TABLE_NAME} to OneLake (overwrite mode, CDC on)")

manifest = build_corpus_manifest(
    gold,
    silver_counts=counts,
    created_ts=created_ts,
    platform_name=platform.name,
    silver_versions=versions,
)
platform.write_gold_manifest(manifest)
print(f"Wrote corpus_manifest.json (contract v{manifest['contract_version']})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation — corpus shape + sample encounter card
#
# Two cells of evidence:
# 1. **Corpus stats** from the manifest — counts, coverage %, distribution per the contract.
# 2. **Sample encounter card** — one fully-rendered row showing the denormalization actually works (patient demographics + SOAP note + active conditions / medications). This is the screenshot reviewers care about most after the SOAP-note demo in 05.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
gold_df = spark.read.format("delta").load(platform.storage_path("gold", TABLE_NAME))
count = gold_df.count()
print(f"gold.{TABLE_NAME} row count: {count:,}")

print("\n=== corpus_stats from manifest ===")
print(json.dumps(manifest["corpus_stats"], indent=2))

print("\n=== schema (truncated to 20 cols) ===")
for f in list(gold_df.schema)[:20]:
    print(f"  {f.name:<32s} {f.dataType.simpleString()}")

print("\n=== sample rows (3) ===")
display(
    gold_df.select(
        "summary_id",
        "patient_id",
        "encounter_id",
        "encounter_date",
        "patient_age",
        "patient_gender",
        "encounter_type",
        F.size("active_conditions").alias("n_conditions"),
        F.size("active_medications").alias("n_medications"),
        F.length("latest_soap_note").alias("soap_chars"),
    ).limit(3)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Sample encounter card — find one with a substantial SOAP note + ≥3 active conditions
card = (
    gold_df.filter(F.length("latest_soap_note") >= 200)
    .filter(F.size("active_conditions") >= 3)
    .orderBy(F.col("encounter_date").desc())
    .limit(1)
    .collect()
)
if not card:
    card = gold_df.limit(1).collect()
r = card[0]

conditions_html = "".join(f"<li>{c}</li>" for c in (r["active_conditions"] or [])[:10])
meds_html = "".join(f"<li>{m}</li>" for m in (r["active_medications"] or [])[:10])
soap = (r["latest_soap_note"] or "(no SOAP note)").replace("<", "&lt;").replace(">", "&gt;")

displayHTML(
    f"<div style='font-family:-apple-system,sans-serif;max-width:900px;padding:16px;line-height:1.55;'>"
    f"<h3>Sample encounter — gold.{TABLE_NAME}</h3>"
    f"<p><b>patient_id:</b> <code>{r['patient_id']}</code> &nbsp;·&nbsp; "
    f"<b>encounter_id:</b> <code>{r['encounter_id']}</code></p>"
    f"<p><b>date:</b> {r['encounter_date']} &nbsp;·&nbsp; "
    f"<b>age:</b> {r['patient_age']} &nbsp;·&nbsp; <b>gender:</b> {r['patient_gender']} &nbsp;·&nbsp; "
    f"<b>type:</b> {r['encounter_type']}</p><hr>"
    f"<h4>Active conditions (as of encounter date)</h4><ul>{conditions_html or '<li>(none)</li>'}</ul>"
    f"<h4>Active medications</h4><ul>{meds_html or '<li>(none)</li>'}</ul>"
    f"<h4>SOAP note</h4>"
    f"<pre style='white-space:pre-wrap;font-size:13px;background:#f6f8fa;padding:14px;border-radius:6px;'>{soap}</pre>"
    f"</div>"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

platform.log_metric(TABLE_NAME, "row_count", count)
platform.log_metric(TABLE_NAME, "contract_version", CONTRACT_VERSION)
print(f"09_gold_encounter_summary complete — {count:,} rows, contract v{CONTRACT_VERSION}.")
print("Next: 10_gold_validation (final contract check)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
