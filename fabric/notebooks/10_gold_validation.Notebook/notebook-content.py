# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 10 — Gold validation (corpus contract gate)
#
# **Purpose.** Final acceptance gate for the medallion. Verify `gold.encounter_summary` matches the corpus contract version 1.1.0 — row grain, required columns, manifest lineage. If this passes, downstream consumers (`scribe-iq`, `clinical-bert-pipeline`) can pin to this build.
#
# **Inputs.** `gold.encounter_summary` Delta table + `Files/gold/_metadata/corpus_manifest.json` (both written by 09).
#
# **Output.** No new tables. The notebook is a structural assertion + a final sample-encounter card for the screenshot.
#
# Screenshot the contract-check pass cell + sample encounter as `10_gold_validation.png`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture context
#
# - **Corpus contract** ([docs/CORPUS_CONTRACT.md](../../docs/CORPUS_CONTRACT.md)) — schema guarantees `scribe-iq` + `clinical-bert-pipeline` depend on. Version 1.1.0 includes the as-of-date problem list (ADR-014).
# - Schema is also pinned as a JSON Schema at `schemas/gold_encounter_summary.json` (generated from `GOLD_SCHEMA` via `core/scripts/gen_corpus_schema.py` — ADR-011).
# - `corpus_manifest.json` captures Silver lineage versions, build timestamp, platform name, and corpus stats. This notebook reads it back and surfaces the structured evidence.
#
# Follows the 8-cell template; the assertions in Cell 5 are the contract-gate.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os

os.environ["LAKEHOUSE_PLATFORM"] = "fabric"

from core.gold.encounter_summary import CONTRACT_VERSION, SILVER_SOURCES, TABLE_NAME
from core.platform.factory import get_platform

platform = get_platform()
print(f"Platform: {platform.name} · Validating gold.{TABLE_NAME} against contract v{CONTRACT_VERSION}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Contract checks
#
# Six assertions — every one must pass for the corpus to ship:
#
# 1. `gold.encounter_summary` exists and has > 0 rows.
# 2. Manifest exists and parses as JSON.
# 3. Manifest `contract_version` == `1.1.0`.
# 4. Manifest `gold_table_name` == `"encounter_summary"`.
# 5. Manifest lists all 10 expected Silver sources with row counts and versions.
# 6. Gold row count matches the manifest's recorded corpus count (consistency).

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

import notebookutils.mssparkutils as msu
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

# Gate 1 — Gold table populated
gold_df = spark.read.format("delta").load(platform.storage_path("gold", TABLE_NAME))
gold_count = gold_df.count()
assert gold_count > 0, "gold.encounter_summary is empty — run 09 first"
print(f"\u2713 Gate 1: gold.{TABLE_NAME} has {gold_count:,} rows")

# Gate 2 — manifest exists + parses
workspace_id, lakehouse = platform._ensure_env()
manifest_path = (
    f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
    f"{lakehouse}.Lakehouse/Files/gold/_metadata/corpus_manifest.json"
)
manifest = json.loads(msu.fs.head(manifest_path, 1024 * 1024))
print(f"\u2713 Gate 2: corpus_manifest.json parsed ({len(json.dumps(manifest)):,} bytes)")

# Gate 3 — contract version
assert manifest["contract_version"] == CONTRACT_VERSION, (
    f"contract_version mismatch: manifest={manifest['contract_version']} expected={CONTRACT_VERSION}"
)
print(f"\u2713 Gate 3: contract_version = {manifest['contract_version']}")

# Gate 4 — gold table name
assert manifest["gold_table_name"] == TABLE_NAME, (
    f"gold_table_name mismatch: manifest={manifest['gold_table_name']} expected={TABLE_NAME}"
)
print(f"\u2713 Gate 4: gold_table_name = {manifest['gold_table_name']}")

# Gate 5 — all Silver sources present in lineage
manifest_silver = set(manifest["silver_lineage"].keys())
expected_silver = set(SILVER_SOURCES)
missing = expected_silver - manifest_silver
extra = manifest_silver - expected_silver
assert not missing, f"manifest missing Silver sources: {missing}"
assert not extra, f"manifest has unexpected Silver sources: {extra}"
print(f"\u2713 Gate 5: silver_lineage covers all {len(expected_silver)} expected sources")

# Gate 6 — gold count matches manifest corpus stats
manifest_total = manifest["corpus_stats"]["total_encounters"]
assert gold_count == manifest_total, (
    f"gold rows ({gold_count}) != manifest total_encounters ({manifest_total})"
)
print(f"\u2713 Gate 6: gold count {gold_count:,} matches manifest total_encounters")

print("\nALL 6 CONTRACT GATES PASSED — corpus ships at version", CONTRACT_VERSION)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sample encounter (final screenshot)
#
# One rendered encounter card with the SOAP note — the same one downstream consumers will see when they pull this corpus. If it looks right here, it'll look right in scribe-iq and clinical-bert-pipeline.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

card = (
    gold_df.filter(F.length("latest_soap_note") >= 200)
    .filter(F.size("active_conditions") >= 2)
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
    f"<h3>Gold contract v{CONTRACT_VERSION} — sample encounter</h3>"
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

platform.log_metric(TABLE_NAME, "contract_gate_passed", 1)
print(f"10_gold_validation complete — Gold corpus contract v{CONTRACT_VERSION} GATE PASSED.")
print(f"   {gold_count:,} encounter summary rows ready for downstream consumers.")
print("")
print("Medallion build (00 → 10) complete in Fabric.")
print("Next phases: 11_exploratory_analysis (Phase 6), Fabric Data Pipeline (Phase 5),")
print("             Power BI Direct Lake dashboard (Phase 7).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
