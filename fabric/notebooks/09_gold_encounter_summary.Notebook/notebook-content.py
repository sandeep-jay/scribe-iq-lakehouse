# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 09 — Gold: `encounter_summary` (pure Spark)
#
# **Purpose.** Denormalize the 10 Silver tables into one row per encounter —
# the corpus handed to scribe-iq (RAG), clinical-bert-pipeline (NLP), and the
# Ollama dialogue generation pipeline. Active conditions / medications are
# the patient's problem list **as of the encounter date** (ADR-014); vitals,
# labs, SOAP, ECG, imaging, genomics are joined per encounter.
#
# **Output.** `Tables/gold/encounter_summary` (overwrite, CDC enabled) plus
# `Files/gold/_metadata/corpus_manifest.json` (lineage + coverage stats).

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture
#
# - **[ADR-022](../../docs/adr/022-platform-independent-implementations.md)** —
#   Fabric Gold is Spark-native; output schema matches `core.gold.encounter_summary`
#   so the corpus contract is platform-agnostic.
# - **[ADR-014](../../docs/adr/014-active-problem-list-semantics.md)** — Active
#   conditions/medications are problem-list-as-of-encounter-date.
# - **CONTRACT_VERSION** is pinned in `fabric.gold.encounter_summary`; bump in
#   lockstep with `core.gold.encounter_summary` on any breaking change.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import UTC, datetime

from fabric.gold.corpus_manifest import build_corpus_manifest
from fabric.gold.encounter_summary import (
    CONTRACT_VERSION,
    SILVER_SOURCES,
    TABLE_NAME,
    build_encounter_summary,
)
from fabric.platform import FabricPlatform

platform = FabricPlatform()
spark = platform.get_spark_session()
created_ts = datetime.now(UTC)
print(f"Contract: {CONTRACT_VERSION} · Gold table: gold.{TABLE_NAME} · created_ts: {created_ts.isoformat()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1 — Read every Silver source + capture lineage
#
# We capture both row counts (for the manifest's `silver_sources` block) and
# Delta versions (recorded verbatim in the `silver_versions` struct on every
# Gold row) so a downstream consumer can reproduce the corpus from Time Travel.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver: dict = {}
silver_counts: dict = {}
silver_versions: dict = {}
for name in SILVER_SOURCES:
    df = platform.read_silver_spark(name)
    silver[name] = df
    silver_counts[name] = df.count()
    silver_versions[name] = platform.table_version("silver", name)
    print(f"silver.{name}: {silver_counts[name]:,} rows  (delta v{silver_versions[name]})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Build Gold (joins + windows) + overwrite write

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_df = build_encounter_summary(silver, created_ts=created_ts, silver_versions=silver_versions)
platform.write_gold_spark(TABLE_NAME, gold_df)
print(f"Wrote gold.{TABLE_NAME}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3 — Build + write the corpus manifest
#
# JSON manifest at `Files/gold/_metadata/corpus_manifest.json`. Same shape as
# the manifest the local-tier pipeline produces — downstream consumers see one
# contract regardless of which platform built the corpus.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

written = platform.read_gold_spark(TABLE_NAME)
manifest = build_corpus_manifest(
    written,
    silver_counts=silver_counts,
    created_ts=created_ts,
    silver_versions=silver_versions,
)
platform.write_gold_manifest(manifest)
print(f"Wrote corpus manifest · row_count={manifest['row_count']:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation — coverage stats + sample row

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

stats = manifest["corpus_stats"]
print("Corpus coverage:")
for k, v in stats.items():
    print(f"  {k:>32}: {v}")

assert manifest["row_count"] > 0, "Gold corpus is empty"
display(written.limit(3))
platform.log_metric(TABLE_NAME, "row_count", manifest["row_count"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
