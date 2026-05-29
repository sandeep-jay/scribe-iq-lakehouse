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
# **Purpose.** Build the four core clinical Silver tables. Bundles read once
# into a cached distributed Spark DataFrame; 4 `applyInPandas` pipelines run
# parse + build for each table in parallel across executors.
#
# **Outputs.** Four Delta tables under `Tables/silver/`. MERGE on PK, CDC on.
#
# **Expected scale (full Coherent):**
# - `condition` ~16,000 rows
# - `observation` ~670,000 rows (largest table)
# - `medication_request` ~209,000 rows
# - `procedure` ~56,000 rows
#
# Screenshot the per-table summary in Cell 15 as `04_silver_clinical.png`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture context
#
# - **Read-once, parse-4-times trade:** `bundles_df.cache()` keeps bundles
#   in executor memory after the first action so the 4 subsequent
#   `applyInPandas` passes don't re-read OneLake. The parser is cheap
#   (~few ms/bundle); 4× parse cost is small vs the cost of avoiding it.
#   Documented in ADR-020.
# - **Clinical-code rule:** SNOMED / LOINC / ICD as `str`, never `int`.
# - **[ADR-019](../../docs/adr/019-silver-merge-idempotency.md)** dedup guard
#   per-table inside `write_silver_spark`.

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

CLINICAL_TABLES = ("condition", "observation", "medication_request", "procedure")
MIN_ROWS = {"condition": 1, "observation": 10, "medication_request": 1, "procedure": 1}

platform = get_platform()
spark = platform.get_spark_session()
ingest_ts = datetime.now(UTC)
for t in CLINICAL_TABLES:
    print(f"  {t:<22s} PK={SILVER_TABLES[t].primary_key}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1 — Read FHIR bundles once + cache
#
# Bundles cached in executor memory so the 4 subsequent applyInPandas passes
# don't re-read OneLake. For full scale-out (much larger cohort), swap
# `cache()` for `persist(StorageLevel.MEMORY_AND_DISK)`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F  # noqa: N812

fhir_root = platform.storage_path("bronze", "fhir")
bundles_df = read_fhir_bundles_distributed(spark, fhir_root).cache()
n_bundles = bundles_df.count()
print(f"Bundles cached: {n_bundles:,}  ·  partitions: {bundles_df.rdd.getNumPartitions()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Per-table distributed parse + Spark-native MERGE
#
# For each clinical table: build the `applyInPandas` UDF + Spark schema,
# distribute the parse, write via Spark MERGE.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

per_table_counts = []
for t in CLINICAL_TABLES:
    spec = SILVER_TABLES[t]
    parse_udf = make_partition_parser(t, spec.build, ingest_ts)
    spark_schema = pa_to_spark_schema(spec.schema)
    silver_df = bundles_df.groupBy(F.spark_partition_id()).applyInPandas(
        parse_udf, schema=spark_schema
    )
    platform.write_silver_spark(t, silver_df, mode="merge")
    n = platform.read_silver_spark(t).count()
    per_table_counts.append((t, n))
    print(f"  silver.{t:<22s} rows={n:>8,}  (MERGE, CDC on)")

bundles_df.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation
#
# Per-table assertions + summary table + 3 sample rows per table.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary_rows = []
for t, n in per_table_counts:
    assert n >= MIN_ROWS[t], f"silver.{t} rows {n} below minimum {MIN_ROWS[t]}"
    summary_rows.append((t, n, SILVER_TABLES[t].primary_key))

summary_df = spark.createDataFrame(
    summary_rows, schema="table STRING, row_count BIGINT, primary_key STRING"
)
display(summary_df)

for t, _ in per_table_counts:
    print(f"\n--- silver.{t} (sample) ---")
    display(platform.read_silver_spark(t).limit(3))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for t, n in per_table_counts:
    platform.log_metric(t, "row_count", n)
print("04_silver_clinical complete — next: 05_silver_soap_notes (demo centerpiece)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
