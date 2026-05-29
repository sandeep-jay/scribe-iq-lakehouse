# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 03 — Silver: `encounter` (pure Spark)
#
# **Purpose.** Build `silver.encounter` from Bronze bundles using
# `fabric.transforms.silver_encounter`. Each Encounter resource projects to one
# row keyed by `encounter_id`, with patient/provider references stripped of
# their `urn:uuid:` / `Patient/` prefixes.
#
# **Output.** `Tables/silver/encounter` — MERGE on `encounter_id`, CDC enabled.
#
# **Expected scale.** ~50k–80k rows at full Coherent.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture
#
# - **[ADR-022](../../docs/adr/022-platform-independent-implementations.md)** —
#   Fabric-native transform; no `core/` import.
# - **[ADR-019](../../docs/adr/019-silver-merge-idempotency.md)** — Pre-merge
#   target dedup guard.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import UTC, datetime

from fabric.platform import FabricPlatform
from fabric.transforms.registry import REGISTRY

TABLE = "encounter"
MIN_ROWS = 100

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

# ## Step 1 — Read Bronze bundles distributed
# Same pattern as 02 — `path` + `value` Spark DataFrame, one row per bundle file.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bundles_df = platform.read_bronze_bundles_spark()
print(f"Bundles read: {bundles_df.count():,}  ·  partitions: {bundles_df.rdd.getNumPartitions()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Build Silver + MERGE on `encounter_id`

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

# ## Validation
# Readback, row count assertion, sample display.

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
display(written.limit(5))
platform.log_metric(TABLE, "row_count", count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
