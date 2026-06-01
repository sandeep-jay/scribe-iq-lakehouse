# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 04 — Silver: clinical tables (pure Spark)
#
# **Purpose.** Build the four clinical Silver tables in one pass, sharing the
# Bronze read:
#
# - `silver.condition`
# - `silver.observation`
# - `silver.medication_request`
# - `silver.procedure`
#
# Each table has its own Spark-native builder in `fabric.transforms.silver_clinical`;
# the registry exposes them as separate entries so we loop and MERGE.
#
# **Output.** Four Delta tables under `Tables/silver/`, each CDC-enabled and
# MERGE-upserted on its own PK.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture
#
# - **[ADR-022](../../docs/adr/022-platform-independent-implementations.md)** —
#   Independent Fabric Spark transforms.
# - **[ADR-019](../../docs/adr/019-silver-merge-idempotency.md)** — Pre-merge
#   target dedup guard runs server-side per table.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import UTC, datetime

from fabric.platform import FabricPlatform
from fabric.transforms.registry import REGISTRY

TABLES = ["condition", "observation", "medication_request", "procedure"]
MIN_ROWS = {"condition": 50, "observation": 100, "medication_request": 0, "procedure": 0}

platform = FabricPlatform()
spark = platform.get_spark_session()
ingest_ts = datetime.now(UTC)
print(f"Platform: {platform.name} · Tables: {TABLES}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1 — Read Bronze once, reuse for all four builders
#
# Cache the bundles DataFrame so the four downstream `from_json` parses share
# one read pass instead of re-scanning OneLake four times.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bundles_df = platform.read_bronze_bundles_spark().cache()
n_bundles = bundles_df.count()  # materialize the cache
print(f"Bundles read: {n_bundles:,}  ·  partitions: {bundles_df.rdd.getNumPartitions()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Build + MERGE each clinical table

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

counts: dict[str, int] = {}
for table in TABLES:
    spec = REGISTRY[table]
    silver_df = spec.build(bundles_df, ingest_ts)
    platform.write_silver_spark(table, silver_df, mode="merge")
    written = platform.read_silver_spark(table)
    count = written.count()
    counts[table] = count
    print(f"silver.{table}: {count:,} rows")
    assert count >= MIN_ROWS[table], f"silver.{table} count {count} below min {MIN_ROWS[table]}"
    platform.log_metric(table, "row_count", count)

bundles_df.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation — sample each table

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for table in TABLES:
    print(f"--- silver.{table} (first 3 rows) ---")
    display(platform.read_silver_spark(table).limit(3))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
