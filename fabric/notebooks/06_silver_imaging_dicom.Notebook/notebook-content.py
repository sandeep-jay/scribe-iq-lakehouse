# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 06 — Silver: imaging_study
#
# **Purpose.** Build `silver.imaging_study` from FHIR `ImagingStudy` resources. DICOM header enrichment (ADR-013) is supported by the transform but is **opt-in** — Phase 4 of the medallion pulls FHIR-only by default; DICOM file ingest is deferred.
#
# **Inputs.** `Files/bronze/fhir/cohort=*/*.json` for FHIR metadata. **Optional:** `Files/bronze/dicom/*.dcm` for header enrichment.
#
# **Output.** Delta table `Tables/silver/imaging_study` (CDC enabled, MERGE-upserted on `study_id`). Carries FHIR fields + optional DICOM headers (study_date, dimensions, slice thickness, etc.). When DICOM file is absent, `dicom_extracted = false` and header columns are null — **expected, not a bug**.
#
# **Expected scale.** ~3,752 imaging studies at full Coherent scale; 298 of those have a downloadable DICOM file in the open dataset.
#
# Screenshot the modality / body-site breakdown cell as `06_silver_imaging.png`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture context
#
# - **[ADR-006](../../docs/adr/006-dicom-stop-before-pixels.md)** — `pydicom.dcmread(..., stop_before_pixels=True)` always. Never load pixel data in this transform tier — metadata only, GPU-free.
# - **[ADR-013](../../docs/adr/013-dicom-ingest-and-linkage.md)** — FHIR↔DICOM linkage by `StudyInstanceUID`. Synthea Coherent ships placeholder `UNKNOWN` description tags that this transform normalizes to `None` to avoid faux signal.
# - **DICOM enrichment is optional in Fabric:** the boto3 anonymous ingest in `01_bronze_ingest` pulls FHIR JSON only; the ~9 GB DICOM prefix is intentionally deferred. The transform handles `dicom_resolver=None` cleanly — `dicom_extracted = false` for all rows in that case.
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

TABLE = "imaging_study"
MIN_ROWS = 1

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
# 1. Parse all bundles once; collect `ImagingStudy` records.
# 2. Call `build_silver_imaging(records, ingest_ts)` — FHIR fields only on this notebook's path (no `dicom_resolver` passed). To enable DICOM header enrichment in the future, land `.dcm` files under `Files/bronze/dicom/`, instantiate `DicomIndex`, and pass it as `dicom_resolver` into the parser (mirrors what `core.surfaces.cli.pipeline._parse_cohort` does locally — see ADR-013).
# 3. MERGE-upsert into Delta.

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
    parsed = parser.parse_bundle(bundle)  # dicom_resolver=None → FHIR-only
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
# Row count + modality/body-site breakdown (the most useful imaging shape signal). DICOM-header coverage is reported via `dicom_extracted` — expected to be 0% on Fabric (no DICOM files landed).

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
dicom_pct = df.agg(F.round(100 * F.avg(F.col("dicom_extracted").cast("int")), 1).alias("dicom_pct")).first()["dicom_pct"]
print(f"silver.{TABLE} row count: {count:,}")
print(f"DICOM-header coverage: {dicom_pct}% (expected 0% without DICOM file ingest)")
assert count >= MIN_ROWS, f"Row count {count} below minimum {MIN_ROWS}"

print("\nModality breakdown:")
display(df.groupBy("modality").count().orderBy(F.col("count").desc()).limit(10))

print("\nBody-site breakdown:")
display(df.groupBy("body_site_display").count().orderBy(F.col("count").desc()).limit(10))

print("\nSample rows:")
display(df.limit(5))

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
