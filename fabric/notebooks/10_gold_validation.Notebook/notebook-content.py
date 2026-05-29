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
# `gold.encounter_summary` matches the corpus contract — row grain, required
# columns, manifest lineage. If this passes, downstream consumers
# (`scribe-iq`, `clinical-bert-pipeline`, Ollama dialogue generation) can pin
# to this build.
#
# **Inputs.** `gold.encounter_summary` Delta table +
# `Files/gold/_metadata/corpus_manifest.json` (both written by 09).
#
# **Output.** No new tables — a structural assertion + sample encounter card.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture
#
# - **Corpus contract** ([docs/CORPUS_CONTRACT.md](../../docs/CORPUS_CONTRACT.md))
#   — schema guarantees downstream consumers depend on.
# - **[ADR-022](../../docs/adr/022-platform-independent-implementations.md)** —
#   Fabric Gold has its own `CONTRACT_VERSION` pinned in
#   `fabric.gold.encounter_summary`, bumped in lockstep with core's on any
#   breaking change.
# - `corpus_manifest.json` captures Silver lineage versions, build timestamp,
#   platform name, and corpus coverage stats. Same shape as the manifest the
#   local-tier pipeline produces.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

import notebookutils.mssparkutils as msu
from pyspark.sql import functions as F  # noqa: N812

from fabric.gold.encounter_summary import CONTRACT_VERSION, SILVER_SOURCES, TABLE_NAME
from fabric.platform import FabricPlatform

platform = FabricPlatform()
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
# 3. Manifest `contract_version` matches the pinned value.
# 4. Manifest `gold_table` == `"gold.encounter_summary"`.
# 5. Manifest `silver_sources` lists all 10 expected Silver tables.
# 6. Gold row count matches the manifest's `row_count` (consistency).

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gate 1 — Gold table populated
gold_df = platform.read_gold_spark(TABLE_NAME)
gold_count = gold_df.count()
assert gold_count > 0, "gold.encounter_summary is empty — run 09 first"
print(f"✓ Gate 1: gold.{TABLE_NAME} has {gold_count:,} rows")

# Gate 2 — manifest exists + parses
manifest_path = platform.files_path("gold/_metadata/corpus_manifest.json")
manifest = json.loads(msu.fs.head(manifest_path, 1024 * 1024))
print(f"✓ Gate 2: corpus_manifest.json parsed ({len(json.dumps(manifest)):,} bytes)")

# Gate 3 — contract version
assert manifest["contract_version"] == CONTRACT_VERSION, (
    f"contract_version mismatch: manifest={manifest['contract_version']} expected={CONTRACT_VERSION}"
)
print(f"✓ Gate 3: contract_version = {manifest['contract_version']}")

# Gate 4 — gold table name
expected_gold = f"gold.{TABLE_NAME}"
assert manifest["gold_table"] == expected_gold, (
    f"gold_table mismatch: manifest={manifest['gold_table']} expected={expected_gold}"
)
print(f"✓ Gate 4: gold_table = {manifest['gold_table']}")

# Gate 5 — all Silver sources in lineage
manifest_silver = {entry["table"].removeprefix("silver.") for entry in manifest["silver_sources"]}
expected_silver = set(SILVER_SOURCES)
missing = expected_silver - manifest_silver
extra = manifest_silver - expected_silver
assert not missing, f"manifest missing Silver sources: {missing}"
assert not extra, f"manifest has unexpected Silver sources: {extra}"
print(f"✓ Gate 5: silver_sources covers all {len(expected_silver)} expected tables")

# Gate 6 — gold count matches manifest
assert gold_count == manifest["row_count"], (
    f"gold rows ({gold_count}) != manifest row_count ({manifest['row_count']})"
)
print(f"✓ Gate 6: gold count {gold_count:,} matches manifest row_count")

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

card = (
    gold_df.filter(F.length("soap_note_text") >= 200)
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
soap = (r["soap_note_text"] or "(no SOAP note)").replace("<", "&lt;").replace(">", "&gt;")

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
print("Medallion build (00 → 10) complete in Fabric — Spark-distributed end-to-end (ADR-022).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
