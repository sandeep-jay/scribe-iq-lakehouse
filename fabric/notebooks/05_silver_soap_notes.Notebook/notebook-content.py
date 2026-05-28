# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 05 — Silver: soap_note (demo centerpiece)
#
# **Purpose.** Extract and decode SOAP clinical notes from Synthea Coherent FHIR bundles. **This is the demo centerpiece** — the validation cell must show a *human-readable* decoded SOAP note (Subjective / Objective / Assessment / Plan), not a Base64 blob. A reviewer should be able to read clinical text in the notebook output and immediately understand what the corpus actually contains.
#
# **Inputs.** `Files/bronze/fhir/cohort=*/*.json` — Synthea embeds the note text Base64-encoded inside `DocumentReference.content[].attachment.data` (not in a separate `Binary` resource — ADR-005).
#
# **Output.** Delta table `Tables/silver/soap_note` (CDC enabled, MERGE-upserted on `note_id`). Columns: decoded `note_text` + boolean `has_subjective` / `has_objective` / `has_assessment` / `has_plan` flags + `char_count` / `word_count`.
#
# **Expected scale.** ~143,946 rows at full Coherent scale (one note per encounter).
#
# Screenshot the **Cell 7** decoded-note output as `05_silver_soap_demo.png` per `fabric/docs/SCREENSHOTS.md`. **This is the highest-priority screenshot.**

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture context
#
# - **[ADR-005](../../docs/adr/005-fhir-binary-decode.md)** — FHIR Binary Base64 decode pattern. Notes live inline in `DocumentReference.content[].attachment.data`; decode with explicit `utf-8`.
# - **Section detection is heuristic** — Coherent SOAP notes use Markdown clinical headers (`# Chief Complaint`, `# Assessment and Plan`, ...), not literal `SUBJECTIVE:/OBJECTIVE:` markers. The parser maps both vocabularies to S/O/A/P flags. Coherent notes rarely contain an Objective section, so `has_objective` is frequently `False` — this is **honest, not a bug** (documented in `fhir_parser.py` and `core/transforms/silver_soap_notes.py`).
# - **PHI policy** ([ADR-010](../../docs/adr/010-phi-safe-logging.md)) — the note *text* is intentionally displayed in the validation cell because this is synthetic Synthea data with no PHI. Real PHI would require redaction; the architecture supports it via `core.redaction.redact()`.
#
# Follows the 8-cell template; Cell 7 has the **mandatory decoded-note display** that the screenshot captures.

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
from core.transforms.fhir_parser import FHIRBundleParser
from core.transforms.registry import SILVER_TABLES

TABLE = "soap_note"
MIN_ROWS = 1  # any non-empty cohort with DocumentReferences yields notes.
MIN_NOTE_CHARS = 100  # SOAP notes below 100 chars are likely stubs, not real clinical text.

platform = get_platform()
ingest_ts = datetime.now(UTC)
spec = SILVER_TABLES[TABLE]
print(f"Platform: {platform.name} · Table: {TABLE} · Primary key: {spec.primary_key}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform approach
#
# 1. Read every FHIR bundle once.
# 2. `FHIRBundleParser.parse_bundle` finds each `DocumentReference`, locates the inline Base64 attachment, decodes to UTF-8 text, and runs the heuristic S/O/A/P section detector.
# 3. `build_silver_soap_note` produces the typed `pa.Table` with `note_text` + four section-presence flags + length metrics. Source-side dedup runs on `note_id`.
# 4. MERGE-upsert into Delta with CDC on.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bundles = platform.read_bronze_fhir()
print(f"Bundles read: {len(bundles)}")

parser = FHIRBundleParser()
records: list[dict] = []
for bundle in bundles:
    parsed = parser.parse_bundle(bundle)
    records.extend(parsed.get(TABLE, []))
print(f"{TABLE} records parsed (decoded): {len(records):,}")

table = spec.build(records, ingest_ts)
print(f"Built {TABLE} table: {table.num_rows:,} rows, {len(table.schema)} columns")

platform.write_silver(TABLE, table, mode="merge")
print(f"Wrote silver.{TABLE} to OneLake (mode=merge, CDC on)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation — the demo cell
#
# **The two outputs below are what the reviewer sees.** The first is corpus-shape evidence (row counts, section coverage %, length distribution). The second is the actual clinical text: a full decoded SOAP note rendered as readable Markdown. Both are mandatory; the decoded-note output is the highest-priority Fabric screenshot.
#
# If the decoded note looks like Base64 garbage, the parser regressed — fix in `core/transforms/fhir_parser.py` (Base64 decode + section detection), rebuild the wheel, re-upload, re-run.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
df = spark.read.format("delta").load(platform.storage_path("silver", TABLE))
count = df.count()

# Corpus-shape evidence — section coverage + length distribution.
coverage = df.agg(
    F.count("*").alias("rows"),
    F.sum(F.col("has_subjective").cast("int")).alias("subjective"),
    F.sum(F.col("has_objective").cast("int")).alias("objective"),
    F.sum(F.col("has_assessment").cast("int")).alias("assessment"),
    F.sum(F.col("has_plan").cast("int")).alias("plan"),
    F.round(F.avg("char_count"), 0).alias("avg_chars"),
    F.round(F.avg("word_count"), 0).alias("avg_words"),
)
print(f"silver.{TABLE} row count: {count:,}")
assert count >= MIN_ROWS, f"Row count {count} below minimum {MIN_ROWS}"
display(coverage)

# Sample row (small, sortable) — schema visible.
display(df.select("note_id", "patient_id", "encounter_id", "char_count", "has_subjective", "has_assessment", "has_plan").limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# DEMO CENTERPIECE — decoded SOAP note rendered as readable Markdown.
# Capture THIS cell's output as fabric/docs/screenshots/05_silver_soap_demo.png
# ============================================================================
sample = (
    df.filter(F.col("char_count") >= MIN_NOTE_CHARS)
    .filter(F.col("has_assessment") & F.col("has_plan"))  # prefer notes with A+P sections
    .orderBy(F.desc("char_count"))
    .select("note_id", "patient_id", "encounter_id", "char_count", "note_text")
    .limit(1)
    .collect()
)
if not sample:
    sample = df.select("note_id", "patient_id", "encounter_id", "char_count", "note_text").limit(1).collect()

row = sample[0]
header = (
    f"### Sample SOAP note\n\n"
    f"**note_id:** `{row['note_id']}`  ·  **patient_id:** `{row['patient_id']}`  "
    f"·  **encounter_id:** `{row['encounter_id']}`  ·  **chars:** {row['char_count']:,}\n\n"
    f"---\n\n"
)
displayHTML(
    f"<div style='font-family: -apple-system, BlinkMacSystemFont, sans-serif; "
    f"max-width: 900px; padding: 16px; line-height: 1.55;'>"
    f"<h3>Sample SOAP note</h3>"
    f"<p><b>note_id:</b> <code>{row['note_id']}</code><br>"
    f"<b>patient_id:</b> <code>{row['patient_id']}</code><br>"
    f"<b>encounter_id:</b> <code>{row['encounter_id']}</code> &nbsp;·&nbsp; "
    f"<b>chars:</b> {row['char_count']:,}</p><hr>"
    f"<pre style='white-space: pre-wrap; font-size: 13px; background: #f6f8fa; "
    f"padding: 14px; border-radius: 6px;'>{row['note_text']}</pre></div>"
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
