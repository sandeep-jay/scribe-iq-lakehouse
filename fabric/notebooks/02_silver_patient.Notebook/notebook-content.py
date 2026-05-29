# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 02 — Silver: `patient` (pure Spark)
#
# **Purpose.** Build `silver.patient` from Bronze FHIR bundles using a Spark-native
# transform from `fabric.transforms.silver_patient`. Each Spark executor parses a
# slice of bundles via `from_json(BUNDLE_SCHEMA)` then projects to the Silver
# schema — no `applyInPandas`, no Python bridge.
#
# **Inputs.** `Files/bronze/fhir/cohort=*/*.json` (Synthea Coherent bundles).
#
# **Output.** Delta table `Tables/silver/patient` (MERGE-upserted on `patient_id`,
# CDC enabled).
#
# **Expected scale.** ~1,278 rows at full Coherent (1 row per patient).
#
# Screenshot the readback `display()` output as `02_silver_patient.png`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture
#
# - **[ADR-022](../../docs/adr/022-platform-independent-implementations.md)** —
#   Fabric runs its own Spark-native transforms (no shared `core/` builders).
# - **[ADR-019](../../docs/adr/019-silver-merge-idempotency.md)** — Pre-merge
#   target dedup guard runs server-side inside `platform.write_silver_spark`.
# - **[ADR-009](../../docs/adr/009-local-silver-materialization.md)** — Delta
#   MERGE on the primary key, CDC enabled.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import UTC, datetime

from fabric.platform import FabricPlatform
from fabric.transforms.registry import REGISTRY

TABLE = "patient"
MIN_ROWS = 100

platform = FabricPlatform()
spark = platform.get_spark_session()
ingest_ts = datetime.now(UTC)
spec = REGISTRY[TABLE]
print(f"Platform: {platform.name} · Table: {TABLE} · PK: {spec.primary_key}")
print(f"Spark version: {spark.version} · ingest_ts: {ingest_ts.isoformat()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1 — Read Bronze bundles as a Spark DataFrame
#
# `read_bronze_bundles_spark` calls `spark.read.text(wholetext=True)` so each
# `.json` file becomes one row in a partitioned DataFrame. Columns: `path`
# (input file URI), `value` (raw JSON text).

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

# ## Step 2 — Build Silver (Spark-native projection + MERGE)
#
# `spec.build(bundles_df, ingest_ts)` parses bundles with `from_json` against
# the union Bundle schema, explodes `entry`, filters resources to Patient, and
# projects into `silver.patient` with deterministic window dedup on the PK
# (ADR-019 semantics). The write is a native Delta MERGE on `patient_id`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df = spec.build(bundles_df, ingest_ts)
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
# Read the materialized table back, assert minimum row count, show a sample.
# Full rule-based validation runs in `08_silver_validation`.

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
