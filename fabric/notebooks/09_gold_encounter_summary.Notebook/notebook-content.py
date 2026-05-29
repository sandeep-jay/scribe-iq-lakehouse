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
# **Purpose.** Denormalize the 10 Silver tables into `gold.encounter_summary`
# — one row per encounter, with patient demographics, the encounter's SOAP
# note, active conditions / medications / vitals as-of-encounter-date, latest
# imaging / ECG / genomics. The corpus contract handoff to downstream
# consumers (`scribe-iq` RAG, `clinical-bert-pipeline` NLP).
#
# **Inputs.** All 10 Silver Delta tables populated by 02–07.
#
# **Outputs.**
# - Delta `Tables/gold/encounter_summary` (overwrite — Gold is fully derived).
# - JSON `Files/gold/_metadata/corpus_manifest.json` — Silver lineage + corpus
#   stats.
#
# **Expected scale.** ~143,946 rows at full Coherent (matches `silver.encounter`).
#
# **Contract version.** 1.1.0 — see [docs/CORPUS_CONTRACT.md](../../docs/CORPUS_CONTRACT.md).
#
# Screenshot the sample-encounter card (Cell 13) as `09_gold_encounter_summary.png`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture context — Spark I/O, pure-Python global denorm
#
# Gold is a **global** denormalization: every encounter joins back to active
# conditions, medications, vitals as-of-date (ADR-014), latest SOAP / imaging
# / ECG / genomics. Polars handles this on the driver in ~5s at full
# Coherent scale — much faster than the equivalent Spark DataFrame DAG would
# be at this row count, with simpler code.
#
# Notebook flow:
# 1. **Spark reads** each Silver Delta in parallel.
# 2. **Driver-side build** — `build_encounter_summary(silver_dict, ts,
#    versions)` runs the canonical Polars denorm logic (same code LocalLite
#    uses; ADR-002).
# 3. **Spark-native write** — Gold Delta via `write_gold_spark`; CDC on.
# 4. **Manifest** — corpus stats + Silver lineage as JSON in
#    `Files/gold/_metadata/`.
#
# Pure logic lives in `core/gold/encounter_summary.py` +
# `core/gold/corpus_manifest.py`. ADR-020 explains the Spark-I/O +
# pure-Python-compute split.

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
spark = platform.get_spark_session()
created_ts = datetime.now(UTC)
print(f"Platform: {platform.name} · Building gold.{TABLE_NAME} · Contract v{CONTRACT_VERSION}")
print(f"Reading {len(SILVER_SOURCES)} Silver sources: {', '.join(SILVER_SOURCES)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1 — Spark-read every Silver source (parallel, distributed I/O)
#
# `platform.read_silver` for each table — internally uses
# `read_silver_spark` then materializes to pa.Table for the global build.
# Captures Delta versions for the manifest's lineage block.

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
    print(f"  silver.{name:<22s} rows={counts[name]:>8,}  v{versions[name]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Build Gold via canonical builder (driver-side, Polars)
#
# `build_encounter_summary` is the single source of truth for Gold across
# LocalLite, Dagster, and Fabric. Returns a `pa.Table` matching `GOLD_SCHEMA`
# exactly.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold = build_encounter_summary(silver, created_ts=created_ts, silver_versions=versions)
print(f"Built gold.{TABLE_NAME}: {gold.num_rows:,} rows, {len(gold.schema)} columns")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3 — Spark-native Delta write + manifest

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_df = spark.createDataFrame(gold.to_pandas())
platform.write_gold_spark(TABLE_NAME, gold_df)
print(f"Wrote gold.{TABLE_NAME} to OneLake (Spark overwrite, CDC on)")

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

# ## Validation — corpus shape
#
# Two cells: corpus stats + denorm sample row table. Sample encounter card
# (Cell 13) is the screenshot.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

from pyspark.sql import functions as F  # noqa: N812

gold_df = platform.read_gold_spark(TABLE_NAME)
count = gold_df.count()
print(f"gold.{TABLE_NAME} row count: {count:,}")

print("\n=== corpus_stats from manifest ===")
print(json.dumps(manifest["corpus_stats"], indent=2))

print("\n=== schema (first 20 cols) ===")
for f in list(gold_df.schema)[:20]:
    print(f"  {f.name:<32s} {f.dataType.simpleString()}")

print("\n=== sample rows ===")
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

# Sample encounter card — substantial SOAP + ≥3 conditions if available
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
    f"<div style='font-family:-apple-system,sans-serif;max-width:900px;"
    f"padding:16px;line-height:1.55;'>"
    f"<h3>Sample encounter — gold.{TABLE_NAME}</h3>"
    f"<p><b>patient_id:</b> <code>{r['patient_id']}</code>  ·  "
    f"<b>encounter_id:</b> <code>{r['encounter_id']}</code></p>"
    f"<p><b>date:</b> {r['encounter_date']}  ·  "
    f"<b>age:</b> {r['patient_age']}  ·  <b>gender:</b> {r['patient_gender']}  ·  "
    f"<b>type:</b> {r['encounter_type']}</p><hr>"
    f"<h4>Active conditions (as of encounter date)</h4><ul>{conditions_html or '<li>(none)</li>'}</ul>"
    f"<h4>Active medications</h4><ul>{meds_html or '<li>(none)</li>'}</ul>"
    f"<h4>SOAP note</h4>"
    f"<pre style='white-space:pre-wrap;font-size:13px;background:#f6f8fa;"
    f"padding:14px;border-radius:6px;'>{soap}</pre></div>"
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
print("Next: 10_gold_validation (final contract gate)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
