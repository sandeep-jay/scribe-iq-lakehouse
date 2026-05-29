# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 04 — Silver: clinical (condition + observation + medication_request + procedure)
#
# **Purpose.** Build the four core *clinical* Silver tables in one notebook so the FHIR parser runs once per bundle instead of four times.
#
# **Inputs.** `Files/bronze/fhir/cohort=*/*.json`.
#
# **Outputs.** Four Delta tables under `Tables/silver/` — `condition`, `observation`, `medication_request`, `procedure`. All CDC enabled, MERGE-upserted on their respective primary keys.
#
# **Expected scale (full Coherent):**
# - `condition`: ~16,000 rows
# - `observation`: ~670,000 rows *(largest table — vitals, labs, social factors)*
# - `medication_request`: ~209,000 rows
# - `procedure`: ~56,000 rows
#
# **Dependencies.** Same as 02/03.
#
# Screenshot the 4-table validation summary cell as `04_silver_clinical.png`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture context
#
# - **Parse-once optimization:** all four extractors live in `core.transforms.silver_clinical`; the parser walks each bundle once and emits four record streams in a single pass. The local CLI uses the same shape (see `core.surfaces.cli.pipeline._parse_cohort`).
# - **[ADR-008](../../docs/adr/008-dict-based-fhir-parsing.md)** — dict-based parsing, `.get()` with defaults everywhere; Synthea Coherent has optional fields throughout.
# - **Clinical-code rule** (healthcare-data skill) — SNOMED / LOINC / ICD codes stored as `str`, never cast to int (preserves leading zeros + special chars like `E11.9`).
# - **[ADR-019](../../docs/adr/019-silver-merge-idempotency.md)** — each MERGE has the target-dedup guard; re-runs are idempotent.
#
# Follows the 8-cell template (cells 5 and 7 each iterate across the 4 tables).

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

CLINICAL_TABLES = ("condition", "observation", "medication_request", "procedure")
MIN_ROWS = {"condition": 1, "observation": 10, "medication_request": 1, "procedure": 1}

platform = get_platform()
ingest_ts = datetime.now(UTC)
for t in CLINICAL_TABLES:
    print(f"  {t:<20s} PK={SILVER_TABLES[t].primary_key}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform approach
#
# 1. Read every bundle once via `platform.read_bronze_fhir()`.
# 2. Parse each bundle once; accumulate records into a `{table_name → list[dict]}` map (parse-once pattern from the CLI).
# 3. For each of the four clinical tables: build the typed `pa.Table` via the registry's `spec.build`, then MERGE-upsert via `platform.write_silver(name, table, mode="merge")`.
#
# Cell 5 prints per-table record counts so a parse-only failure (zero records for a table) is visible before the MERGE.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

parser = FHIRBundleParser()
accumulated: dict[str, list[dict]] = {t: [] for t in CLINICAL_TABLES}
n_bundles = 0
for path, bundle in platform.iter_bronze_files():
    n_bundles += 1
    parsed = parser.parse_bundle(bundle)
    src = path.rsplit("/", 1)[-1]
    for t in CLINICAL_TABLES:
        for r in parsed.get(t, []):
            r["source_file"] = src
            accumulated[t].append(r)
print(f"Bundles read: {n_bundles}")

for t in CLINICAL_TABLES:
    print(f"  {t:<20s} records parsed: {len(accumulated[t]):>8,}")

for t in CLINICAL_TABLES:
    spec = SILVER_TABLES[t]
    table = spec.build(accumulated[t], ingest_ts)
    platform.write_silver(t, table, mode="merge")
    print(f"  silver.{t:<20s} rows={table.num_rows:>8,}  (mode=merge, CDC on)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation
#
# Read each of the 4 tables back via Spark, assert minimum row counts, display a summary table + one sample row per table. Full validation in `08_silver_validation`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

summary_rows = []
for t in CLINICAL_TABLES:
    df = spark.read.format("delta").load(platform.storage_path("silver", t))
    n = df.count()
    assert n >= MIN_ROWS[t], f"silver.{t} rows {n} below minimum {MIN_ROWS[t]}"
    summary_rows.append((t, n, SILVER_TABLES[t].primary_key))
    print(f"\n--- silver.{t} (rows={n:,}) ---")
    display(df.limit(3))

summary_df = spark.createDataFrame(summary_rows, schema="table STRING, row_count BIGINT, primary_key STRING")
display(summary_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for t, n, _ in summary_rows:
    platform.log_metric(t, "row_count", n)
print("04_silver_clinical complete — next: 05_silver_soap_notes (demo centerpiece)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
