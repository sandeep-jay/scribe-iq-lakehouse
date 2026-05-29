# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 06 — Silver: `imaging_study` (distributed Spark, FHIR-only on Fabric)
#
# **Purpose.** Build `silver.imaging_study` from FHIR `ImagingStudy` via the
# distributed Spark pattern. DICOM header enrichment (ADR-013) is supported
# by the transform but **opt-in** — boto3 anonymous Bronze ingest doesn't
# include `.dcm` files, so Fabric defaults to FHIR-only.
#
# **Outputs.** Delta `Tables/silver/imaging_study` (MERGE on `study_id`,
# CDC on). When DICOM file is absent, `dicom_extracted = false` and header
# columns are null — **expected, not a bug**.
#
# **Expected scale.** ~3,752 studies at full Coherent; ~298 have a `.dcm`
# in the open dataset.
#
# Screenshot modality / body-site breakdown as `06_silver_imaging.png`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture context
#
# - **[ADR-006](../../docs/adr/006-dicom-stop-before-pixels.md)** —
#   `pydicom.dcmread(..., stop_before_pixels=True)` always.
# - **[ADR-013](../../docs/adr/013-dicom-ingest-and-linkage.md)** — FHIR↔DICOM
#   by `StudyInstanceUID`. Coherent placeholder `UNKNOWN` tags normalized to
#   `None`.
# - **[ADR-020](../../docs/adr/020-fabric-distributed-parsing.md)** —
#   `applyInPandas` distribution. DICOM enrichment requires broadcasting
#   `DicomIndex` to executors; deferred.

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

TABLE = "imaging_study"
MIN_ROWS = 1

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

# ## Step 1 — Read FHIR bundles distributed

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fhir_root = platform.storage_path("bronze", "fhir")
bundles_df = read_fhir_bundles_distributed(spark, fhir_root)
print(f"Bundles read: {bundles_df.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Distributed parse (FHIR-only)
#
# `parse_bundle` called with `dicom_resolver=None` (default). `dicom_extracted`
# = false for all rows. To enable DICOM enrichment later: land `.dcm` files,
# broadcast `DicomIndex` to executors, pass resolver into `parse_bundle`.

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
# Row count + DICOM-coverage percent (0% expected on Fabric) + modality +
# body-site breakdowns.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

written = platform.read_silver_spark(TABLE)
count = written.count()
dicom_pct = written.agg(
    F.round(100 * F.avg(F.col("dicom_extracted").cast("int")), 1).alias("dicom_pct")
).first()["dicom_pct"]
print(f"silver.{TABLE} row count: {count:,}")
print(f"DICOM-header coverage: {dicom_pct}% (expected 0% on Fabric — DICOM ingest deferred)")
assert count >= MIN_ROWS, f"Row count {count} below minimum {MIN_ROWS}"

print("\nModality breakdown:")
display(written.groupBy("modality").count().orderBy(F.col("count").desc()).limit(10))

print("\nBody-site breakdown:")
display(written.groupBy("body_site_display").count().orderBy(F.col("count").desc()).limit(10))

display(written.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

platform.log_metric(TABLE, "row_count", count)
platform.log_metric(TABLE, "dicom_coverage_pct", dicom_pct or 0)
print("06_silver_imaging_dicom complete — next: 07_silver_ecg_genomics")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
