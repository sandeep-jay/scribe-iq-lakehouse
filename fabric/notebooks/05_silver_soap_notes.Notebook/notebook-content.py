# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 05 — Silver: `soap_note` (pure Spark, demo centerpiece)
#
# **Purpose.** Build `silver.soap_note` from `DocumentReference` resources.
# Coherent embeds the clinical note as Base64 in `content[0].attachment.data`;
# the Spark transform decodes it with `unbase64 → decode("UTF-8")` and runs
# case-insensitive regex to flag the S / O / A / P sections.
#
# **Output.** `Tables/silver/soap_note` — MERGE on `note_id`, CDC enabled.
#
# **Demo rule (.claude/rules/notebooks.md).** This notebook MUST show a
# decoded SOAP note in the readback cell — that's the "reviewer-readable
# clinical text" that proves the medallion is doing what it claims.
# Screenshot the decoded `note_text` column as `05_silver_soap_notes.png`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture
#
# - **[ADR-022](../../docs/adr/022-platform-independent-implementations.md)** —
#   Independent Spark-native impl.
# - **[ADR-005](../../docs/adr/005-soap-note-base64-decoding.md)** — Base64
#   decoding contract for Coherent DocumentReference attachments.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import UTC, datetime

from fabric.platform import FabricPlatform
from fabric.transforms.registry import REGISTRY

TABLE = "soap_note"
MIN_ROWS = 50

platform = FabricPlatform()
spark = platform.get_spark_session()
ingest_ts = datetime.now(UTC)
spec = REGISTRY[TABLE]
print(f"Platform: {platform.name} · Table: {TABLE} · PK: {spec.primary_key}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1 — Read Bronze bundles

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bundles_df = platform.read_bronze_bundles_spark()
print(f"Bundles read: {bundles_df.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Build Silver (decode + section flags) + MERGE

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df = spec.build(bundles_df, ingest_ts)
platform.write_silver_spark(TABLE, silver_df, mode="merge")
print(f"Wrote silver.{TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation — show a fully decoded SOAP note
#
# Required by `.claude/rules/notebooks.md`: the reviewer must see readable
# clinical text in the notebook output.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

written = platform.read_silver_spark(TABLE)
count = written.count()
print(f"silver.{TABLE} row count: {count:,}")
assert count >= MIN_ROWS, f"Row count {count} below minimum {MIN_ROWS}"

# Sample row metadata + a fully decoded note (truncated to keep the cell readable).
display(
    written.select(
        "note_id",
        "patient_id",
        "encounter_id",
        "note_date",
        "char_count",
        "has_subjective",
        "has_assessment",
        "has_plan",
    ).limit(5)
)

example = written.select("note_id", "note_text").limit(1).collect()
if example:
    text = example[0]["note_text"] or ""
    print(f"\n--- DECODED SOAP NOTE (note_id={example[0]['note_id']}) ---")
    print(text[:2000])
    if len(text) > 2000:
        print(f"... [truncated, total {len(text):,} chars]")

platform.log_metric(TABLE, "row_count", count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
