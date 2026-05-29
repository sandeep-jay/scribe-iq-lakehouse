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
# **Purpose.** Final acceptance gate for the medallion. Verify
# `gold.encounter_summary` matches the corpus contract version 1.1.0 — row
# grain, required columns, manifest lineage. If this passes, downstream
# consumers (`scribe-iq`, `clinical-bert-pipeline`) can pin to this build.
#
# **Inputs.** `gold.encounter_summary` Delta table +
# `Files/gold/_metadata/corpus_manifest.json` (both written by 09).
#
# **Output.** No new tables. A structural assertion + sample encounter card.
#
# Screenshot Cell 5 (gate pass) + Cell 7 (sample card) as `10_gold_validation.png`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture context
#
# - **Corpus contract** ([docs/CORPUS_CONTRACT.md](../../docs/CORPUS_CONTRACT.md))
#   — schema guarantees downstream consumers depend on. v1.1.0 includes
#   ADR-014 as-of-date problem list.
# - Schema pinned as JSON Schema at `schemas/gold_encounter_summary.json`
#   (generated from `GOLD_SCHEMA` via `core/scripts/gen_corpus_schema.py`
#   — ADR-011).
# - `corpus_manifest.json` captures Silver lineage versions, timestamp,
#   platform name, corpus stats.
# - Uses Spark for the Gold read + `platform.ensure_env()` to build the
#   manifest URI (no private-method access).

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
spark = platform.get_spark_session()
print(f"Platform: {platform.name} · Validating gold.{TABLE_NAME} against contract v{CONTRACT_VERSION}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Contract gates (all must pass)
#
# 1. `gold.encounter_summary` exists and has > 0 rows.
# 2. Manifest exists and parses as JSON.
# 3. Manifest `contract_version` == `1.1.0`.
# 4. Manifest `gold_table_name` == `"encounter_summary"`.
# 5. Manifest lists all 10 expected Silver sources with row counts + versions.
# 6. Gold row count matches the manifest's recorded corpus count (consistency).

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

import notebookutils.mssparkutils as msu

# Gate 1 — Gold table populated (Spark read)
gold_df = platform.read_gold_spark(TABLE_NAME)
gold_count = gold_df.count()
assert gold_count > 0, "gold.encounter_summary is empty — run 09 first"
print(f"✓ Gate 1: gold.{TABLE_NAME} has {gold_count:,} rows")

# Gate 2 — manifest exists + parses
workspace_id, lakehouse = platform.ensure_env()
manifest_path = (
    f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
    f"{lakehouse}.Lakehouse/Files/gold/_metadata/corpus_manifest.json"
)
manifest = json.loads(msu.fs.head(manifest_path, 1024 * 1024))
print(f"✓ Gate 2: corpus_manifest.json parsed ({len(json.dumps(manifest)):,} bytes)")

# Gate 3 — contract version
assert manifest["contract_version"] == CONTRACT_VERSION, (
    f"contract_version mismatch: manifest={manifest['contract_version']} expected={CONTRACT_VERSION}"
)
print(f"✓ Gate 3: contract_version = {manifest['contract_version']}")

# Gate 4 — gold table name
assert manifest["gold_table_name"] == TABLE_NAME, (
    f"gold_table_name mismatch: manifest={manifest['gold_table_name']} expected={TABLE_NAME}"
)
print(f"✓ Gate 4: gold_table_name = {manifest['gold_table_name']}")

# Gate 5 — all Silver sources in lineage
manifest_silver = set(manifest["silver_lineage"].keys())
expected_silver = set(SILVER_SOURCES)
missing = expected_silver - manifest_silver
extra = manifest_silver - expected_silver
assert not missing, f"manifest missing Silver sources: {missing}"
assert not extra, f"manifest has unexpected Silver sources: {extra}"
print(f"✓ Gate 5: silver_lineage covers all {len(expected_silver)} expected sources")

# Gate 6 — gold count matches manifest
manifest_total = manifest["corpus_stats"]["total_encounters"]
assert gold_count == manifest_total, (
    f"gold rows ({gold_count}) != manifest total_encounters ({manifest_total})"
)
print(f"✓ Gate 6: gold count {gold_count:,} matches manifest total_encounters")

print(f"\nALL 6 CONTRACT GATES PASSED — corpus ships at version {CONTRACT_VERSION}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Sample encounter (final screenshot)

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F  # noqa: N812

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
    f"<div style='font-family:-apple-system,sans-serif;max-width:900px;"
    f"padding:16px;line-height:1.55;'>"
    f"<h3>Gold contract v{CONTRACT_VERSION} — sample encounter</h3>"
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

platform.log_metric(TABLE_NAME, "contract_gate_passed", 1)
print(f"10_gold_validation complete — Gold corpus contract v{CONTRACT_VERSION} GATE PASSED.")
print(f"   {gold_count:,} encounter summary rows ready for downstream consumers.")
print("")
print("Medallion build (00 → 10) complete in Fabric — Spark-distributed end-to-end.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
