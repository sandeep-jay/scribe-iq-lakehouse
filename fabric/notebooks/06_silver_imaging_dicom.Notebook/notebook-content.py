# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 06 — Silver: `imaging_study` (pure Spark, FHIR-only)
#
# **Purpose.** Build `silver.imaging_study` from `ImagingStudy` resources.
#
# **Scope on Fabric (ADR-022).** This is a FHIR-only projection — modality,
# body-site, series/instance counts, description. The DICOM-header enrichment
# fields (`study_instance_uid`, `manufacturer`, `dcm_rows/columns`, etc.) are
# left null on this platform; populating them requires reading the on-disk
# `.dcm` files with `pydicom`, which is local-path work
# (see `core.transforms.silver_imaging`). The columns remain in the schema
# so the local and Fabric Silver tables stay union-compatible.
#
# **Output.** `Tables/silver/imaging_study` — MERGE on `study_id`, CDC on.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture
#
# - **[ADR-022](../../docs/adr/022-platform-independent-implementations.md)** —
#   Fabric tier does FHIR-only Imaging; pydicom enrichment is a local-tier
#   responsibility.
# - **[ADR-006](../../docs/adr/006-pydicom-stop-before-pixels.md)** — When the
#   local path populates DICOM fields it does so with `stop_before_pixels=True`
#   (no pixel data ever loaded).

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import UTC, datetime

from fabric.platform import FabricPlatform
from fabric.transforms.registry import REGISTRY

TABLE = "imaging_study"
MIN_ROWS = 0  # Coherent imaging is sparse; presence is optional

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

# ## Step 2 — Build Silver + MERGE on `study_id`

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
