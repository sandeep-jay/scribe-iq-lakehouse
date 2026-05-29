# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 02 — Silver: `patient` (distributed Spark)
#
# **Purpose.** Build `silver.patient` from Bronze FHIR bundles using Spark's
# distributed `applyInPandas` — each executor parses a partition of bundles in
# parallel, calling the same pure-Python `core.transforms.silver_patient.build_silver_patient`
# the LocalLite tier uses. One source of truth for the transform; Fabric brings
# the parallelism.
#
# **Inputs.** `Files/bronze/fhir/cohort=*/*.json` (Synthea Coherent bundles).
#
# **Output.** Delta table `Tables/silver/patient` (MERGE-upserted on `patient_id`,
# CDC enabled).
#
# **Expected scale.** ~1,278 rows at full Coherent (1 row per patient).
#
# **Dependencies.** Env `scribe-iq-lakehouse-env` with the core wheel +
# `pyarrow`, `pydicom`, `python-dateutil`, `boto3`. Run after `01_bronze_ingest`.
#
# Screenshot the Cell 11 `display()` output as `02_silver_patient.png`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture context
#
# - **[ADR-002](../../docs/adr/002-platform-abstraction.md)** — Pure transforms
#   in `core/`; this notebook is the Fabric execution surface.
# - **[ADR-020](../../docs/adr/020-fabric-distributed-parsing.md)** — Distributed
#   parsing via `applyInPandas`: `core.transforms.silver_patient.build_silver_patient`
#   is unchanged; we wrap it in a Spark UDF that runs on every executor.
# - **[ADR-019](../../docs/adr/019-silver-merge-idempotency.md)** — Pre-merge
#   target-side dedup guard runs inside `platform.write_silver_spark`.
# - **[ADR-009](../../docs/adr/009-local-silver-materialization.md)** — Delta
#   MERGE on the primary key, CDC enabled.
#
# Distributed pipeline:
# 1. **Read** — `spark.read.text(wholetext=True)` reads each bundle as one row,
#    distributed across partitions.
# 2. **Parse** — `applyInPandas` runs the partition-parser UDF on each Spark
#    executor in parallel. Each UDF call uses the existing `FHIRBundleParser`
#    + `build_silver_patient`.
# 3. **Write** — Spark-native Delta MERGE via `platform.write_silver_spark`.

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

TABLE = "patient"
MIN_ROWS = 1

platform = get_platform()
spark = platform.get_spark_session()
ingest_ts = datetime.now(UTC)
spec = SILVER_TABLES[TABLE]
print(f"Platform: {platform.name} · Table: {TABLE} · PK: {spec.primary_key}")
print(f"Spark version: {spark.version} · ingest_ts: {ingest_ts.isoformat()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1 — Read FHIR bundles as a Spark DataFrame
#
# `read_fhir_bundles_distributed` calls `spark.read.text(wholetext=True)` which
# reads each `.json` file as one row of a partitioned Spark DataFrame. Columns:
# `path` (input file URI), `value` (raw JSON text).
#
# Spark decides partition count from file sizes by default; override
# `num_partitions=N` if you want to force higher parallelism (e.g. on a larger
# cohort where the default is too coarse).

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fhir_root = platform.storage_path("bronze", "fhir")
bundles_df = read_fhir_bundles_distributed(spark, fhir_root)
print(f"Bundles read: {bundles_df.count():,}  ·  partitions: {bundles_df.rdd.getNumPartitions()}")
bundles_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Parse + build Silver via `applyInPandas` (distributed)
#
# `make_partition_parser` returns a function that:
# 1. Receives one Spark partition as `pd.DataFrame(path, value)`
# 2. JSON-parses each bundle with the pure-Python `FHIRBundleParser`
# 3. Extracts `patient` records, stamps `source_file = path.basename`
# 4. Calls `spec.build(records, ingest_ts)` (= `build_silver_patient`) to
#    produce the typed `pa.Table` — same builder LocalLite uses (ADR-002)
# 5. Returns `to_pandas()` for Spark to consume
#
# Spark runs this on every executor in parallel; the output schema is derived
# from the canonical `spec.schema` via `pa_to_spark_schema`.

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
print(f"silver.{TABLE} planned schema: {len(spark_schema.fields)} columns")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3 — Spark-native Delta MERGE (CDC + ADR-019 guard)
#
# `platform.write_silver_spark` writes the Spark DataFrame directly to OneLake
# Delta using a native `DeltaTable.merge()` — no driver-side pa.Table conversion.
# The ADR-019 target dedup guard runs server-side: if the existing table has
# duplicate primary keys, it's rewritten deduped before the MERGE, then the
# MERGE applies the new data on top.
#
# First run = CREATE + CDC enabled. Subsequent runs = MERGE on `patient_id`.

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
# Read back via Spark, assert minimum row count, show a sample. Full
# rule-based validation lives in `08_silver_validation` which logs every
# rule outcome to `silver.ingest_log`.

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

platform.log_metric(TABLE, "row_count", count)
print("02_silver_patient complete — next: 03_silver_encounter")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
