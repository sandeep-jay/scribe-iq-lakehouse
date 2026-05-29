# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 03 — Silver: `encounter` (distributed Spark)
#
# **Purpose.** Build `silver.encounter` — one row per FHIR `Encounter`. This is
# the grain of the Gold layer's `encounter_summary`; every downstream Silver
# row joins back to `encounter_id`.
#
# **Inputs.** `Files/bronze/fhir/cohort=*/*.json`.
#
# **Output.** Delta table `Tables/silver/encounter` (MERGE on `encounter_id`,
# CDC enabled).
#
# **Expected scale.** ~143,946 rows at full Coherent (~113 encounters/patient).
#
# Screenshot the Cell 11 output as `03_silver_encounter.png`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture context
#
# Same distributed pattern as `02_silver_patient`: Spark.read.text →
# applyInPandas(parser UDF) → write_silver_spark with MERGE. Encounter is the
# temporal anchor for ADR-014's as-of-date problem list — `period_start` must
# parse correctly downstream.

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

TABLE = "encounter"
MIN_ROWS = 10

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

# ## Step 1 — Read FHIR bundles as a Spark DataFrame

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fhir_root = platform.storage_path("bronze", "fhir")
bundles_df = read_fhir_bundles_distributed(spark, fhir_root)
print(f"Bundles read: {bundles_df.count():,}  ·  partitions: {bundles_df.rdd.getNumPartitions()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Distributed parse → Silver DataFrame

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

# MARKDOWN ********************

# ## Step 3 — Spark-native MERGE

# METADATA ********************

# META {
# META   "language": "markdown",
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

# ## Validation
#
# Row count + encounters-per-patient ratio + sample.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

written = platform.read_silver_spark(TABLE)
count = written.count()
distinct_patients = written.select("patient_id").distinct().count()
print(f"silver.{TABLE} row count: {count:,}")
print(f"distinct patients with encounters: {distinct_patients:,}")
print(f"avg encounters / patient: {count / max(distinct_patients, 1):.1f}")
assert count >= MIN_ROWS, f"Row count {count} below minimum {MIN_ROWS}"
display(written.orderBy(F.col("period_start").desc()).limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

platform.log_metric(TABLE, "row_count", count)
platform.log_metric(TABLE, "distinct_patients", distinct_patients)
print("03_silver_encounter complete — next: 04_silver_clinical")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
