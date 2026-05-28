# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 03 — Silver: encounter
#
# **Purpose.** Build `silver.encounter` from Bronze FHIR bundles. One row per FHIR `Encounter` resource — the grain of the medallion's Gold layer.
#
# **Inputs.** `Files/bronze/fhir/cohort=*/*.json`.
#
# **Output.** Delta table `Tables/silver/encounter` (CDC enabled, MERGE-upserted on `encounter_id`).
#
# **Expected scale.** ~143,946 encounters at full Coherent scale (≈113 encounters/patient over a synthetic lifetime).
#
# **Dependencies.** Same as `02_silver_patient`.
#
# Screenshot the validation cell output as `03_silver_encounter.png`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture context
#
# - **[ADR-002](../../docs/adr/002-platform-abstraction.md)** + **[ADR-004](../../docs/adr/004-arrow-interchange.md)** + **[ADR-009](../../docs/adr/009-local-silver-materialization.md)** — same contract as 02.
# - **[ADR-014](../../docs/adr/014-problem-list-as-of-date.md)** — encounter rows carry `period_start`, which Gold uses to compute the as-of-date active problem list per encounter.
#
# `encounter_id` is the PK and the join key linking every downstream Silver row (condition, observation, medication_request, procedure, soap_note) to its visit. Without `silver.encounter` populated, Gold cannot reconstruct the encounter timeline.
#
# Follows the 8-cell template.

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

TABLE = "encounter"
MIN_ROWS = 10  # any non-trivial cohort produces dozens; full corpus = 143,946.

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
# Same pattern as 02 — read bundles, parse with `FHIRBundleParser`, collect `encounter` records, build the typed `pa.Table`, MERGE-upsert into Delta on `encounter_id`. Source-side dedup runs inside `build_silver_encounter` (last-write-wins on `encounter_id`). Period start/end + class/type LOINC codes are preserved as strings (never cast to int — clinical-code rule).

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
print(f"{TABLE} records parsed: {len(records):,}")

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

# ## Validation
#
# Row count + encounters-per-patient sanity check + sample rows. Full validation lives in `08_silver_validation`.

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
distinct_patients = df.select("patient_id").distinct().count()
print(f"silver.{TABLE} row count: {count:,}")
print(f"distinct patients with encounters: {distinct_patients:,}")
print(f"avg encounters / patient: {count / max(distinct_patients, 1):.1f}")
assert count >= MIN_ROWS, f"Row count {count} below minimum {MIN_ROWS}"
display(df.orderBy(F.col("period_start").desc()).limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

platform.log_metric(TABLE, "row_count", count)
platform.log_metric(TABLE, "distinct_patients", distinct_patients)
print(f"03_silver_encounter complete — next: 04_silver_clinical")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
