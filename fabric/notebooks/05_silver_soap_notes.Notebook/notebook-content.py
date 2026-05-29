# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 05 — Silver: `soap_note` (demo centerpiece, distributed Spark)
#
# **Purpose.** Distributed extraction + Base64 decode of Synthea SOAP notes
# from FHIR `DocumentReference` resources. **Demo centerpiece** — the
# validation cell must show a *human-readable* decoded SOAP note (not a
# Base64 blob).
#
# **Inputs.** `Files/bronze/fhir/cohort=*/*.json`. Synthea embeds note text
# Base64-encoded inside `DocumentReference.content[].attachment.data`
# (inline; ADR-005).
#
# **Output.** Delta table `Tables/silver/soap_note` (MERGE on `note_id`,
# CDC enabled). Decoded `note_text` + S/O/A/P section flags + length metrics.
#
# **Expected scale.** ~143,946 rows at full Coherent.
#
# **Screenshot Cell 11 (decoded HTML) as `05_silver_soap_demo.png` — highest
# priority capture in the project.**

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture context
#
# - **[ADR-005](../../docs/adr/005-fhir-binary-decode.md)** — Base64 decode
#   from inline attachments.
# - **[ADR-020](../../docs/adr/020-fabric-distributed-parsing.md)** —
#   `applyInPandas` distributes parse + decode across executors.
# - **Section detection is heuristic** — Coherent uses Markdown headers
#   (`# Chief Complaint`, `# Assessment and Plan`); rarely an Objective
#   section, so `has_objective` is frequently `False`. Honest, not a bug.
# - **PHI policy ([ADR-010](../../docs/adr/010-phi-safe-logging.md))** — note
#   text shown because Synthea is synthetic. Real PHI requires
#   `core.redaction.redact()`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
from datetime import UTC, datetime

os.environ["LAKEHOUSE_PLATFORM"] = "fabric"

from core.platform.factory import get_platform
from core.transforms.registry import SILVER_TABLES
from fabric.spark_helpers import (
    make_partition_parser,
    pa_to_spark_schema,
    read_fhir_bundles_distributed,
)

TABLE = "soap_note"
MIN_ROWS = 1
MIN_NOTE_CHARS = 100

platform = get_platform()
spark = platform.get_spark_session()
ingest_ts = datetime.now(UTC)
spec = SILVER_TABLES[TABLE]
print(f"Platform: {platform.name} · Table: {TABLE} · PK: {spec.primary_key}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1 — Read FHIR bundles distributed

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fhir_root = platform.storage_path("bronze", "fhir")
bundles_df = read_fhir_bundles_distributed(spark, fhir_root)
print(f"Bundles read: {bundles_df.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Distributed parse + Base64 decode + section detection

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F  # noqa: N812

parse_udf = make_partition_parser(TABLE, spec.build, ingest_ts)
spark_schema = pa_to_spark_schema(spec.schema)
silver_df = bundles_df.groupBy(F.spark_partition_id()).applyInPandas(
    parse_udf, schema=spark_schema
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

platform.write_silver_spark(TABLE, silver_df, mode="merge")
print(f"Wrote silver.{TABLE} (Spark-native MERGE, CDC on)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation — corpus shape
#
# Row count + section-coverage stats + length distribution + sample rows.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

written = platform.read_silver_spark(TABLE)
count = written.count()
assert count >= MIN_ROWS, f"Row count {count} below minimum {MIN_ROWS}"

coverage = written.agg(
    F.count("*").alias("rows"),
    F.sum(F.col("has_subjective").cast("int")).alias("subjective"),
    F.sum(F.col("has_objective").cast("int")).alias("objective"),
    F.sum(F.col("has_assessment").cast("int")).alias("assessment"),
    F.sum(F.col("has_plan").cast("int")).alias("plan"),
    F.round(F.avg("char_count"), 0).alias("avg_chars"),
    F.round(F.avg("word_count"), 0).alias("avg_words"),
)
print(f"silver.{TABLE} row count: {count:,}")
display(coverage)
display(
    written.select(
        "note_id", "patient_id", "encounter_id", "char_count",
        "has_subjective", "has_assessment", "has_plan",
    ).limit(5)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# DEMO CENTERPIECE — decoded SOAP note rendered as readable HTML.
# Capture THIS cell's output as fabric/docs/screenshots/05_silver_soap_demo.png
# ============================================================================
sample = (
    written.filter(F.col("char_count") >= MIN_NOTE_CHARS)
    .filter(F.col("has_assessment") & F.col("has_plan"))
    .orderBy(F.desc("char_count"))
    .select("note_id", "patient_id", "encounter_id", "char_count", "note_text")
    .limit(1)
    .collect()
)
if not sample:
    sample = (
        written.select("note_id", "patient_id", "encounter_id", "char_count", "note_text")
        .limit(1)
        .collect()
    )

row = sample[0]
soap = (row["note_text"] or "").replace("<", "&lt;").replace(">", "&gt;")
displayHTML(
    f"<div style='font-family:-apple-system,sans-serif;max-width:900px;"
    f"padding:16px;line-height:1.55;'>"
    f"<h3>Sample SOAP note</h3>"
    f"<p><b>note_id:</b> <code>{row['note_id']}</code><br>"
    f"<b>patient_id:</b> <code>{row['patient_id']}</code><br>"
    f"<b>encounter_id:</b> <code>{row['encounter_id']}</code>"
    f"  ·  <b>chars:</b> {row['char_count']:,}</p><hr>"
    f"<pre style='white-space:pre-wrap;font-size:13px;background:#f6f8fa;"
    f"padding:14px;border-radius:6px;'>{soap}</pre></div>"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

platform.log_metric(TABLE, "row_count", count)
print("05_silver_soap_notes complete — decoded-note screenshot is the priority capture.")
print("Next: 06_silver_imaging_dicom")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
