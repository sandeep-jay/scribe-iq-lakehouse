# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 07 — Silver: `ecg_metadata` + `genomic_report` (distributed Spark)
#
# **Purpose.** Build two specialty-modality Silver tables with honest
# limitations baked into their schemas. Same read-once / parse-per-table
# distributed pattern as `04_silver_clinical`.
#
# **Outputs.** Two Delta tables:
# - `ecg_metadata` (PK `ecg_id`) — metadata only; Coherent ECG is SBML model
#   output, not real waveforms; `has_waveform = false` always.
# - `genomic_report` (PK `report_id`) — metadata only; `data_limitation`
#   non-nullable per [ADR-007](../../docs/adr/007-genomic-data-limitation.md).
#
# **Expected scale (full Coherent).** `ecg_metadata` typically 0 rows.
# `genomic_report` ≈ 419 rows.
#
# Screenshot the `data_limitation` contract assertion as `07_silver_ecg_genomics.png`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture context
#
# - **[ADR-007](../../docs/adr/007-genomic-data-limitation.md)** —
#   `data_limitation` non-nullable; flags Synthea genomics as
#   inheritance simulation, not clinical variants.
# - **ECG candor:** signal lives in Binary waveform resources (out of scope).
#   `has_waveform = false` reflects metadata-only extraction.
# - **[ADR-020](../../docs/adr/020-fabric-distributed-parsing.md)** —
#   `applyInPandas` distribution.

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

SPECIALTY_TABLES = ("ecg_metadata", "genomic_report")

platform = get_platform()
spark = platform.get_spark_session()
ingest_ts = datetime.now(UTC)
for t in SPECIALTY_TABLES:
    print(f"  {t:<22s} PK={SILVER_TABLES[t].primary_key}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1 — Read FHIR bundles once + cache

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F  # noqa: N812

fhir_root = platform.storage_path("bronze", "fhir")
bundles_df = read_fhir_bundles_distributed(spark, fhir_root).cache()
print(f"Bundles cached: {bundles_df.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Per-table parse + write

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

per_table_counts = []
for t in SPECIALTY_TABLES:
    spec = SILVER_TABLES[t]
    parse_udf = make_partition_parser(t, spec.build, ingest_ts)
    spark_schema = pa_to_spark_schema(spec.schema)
    silver_df = bundles_df.groupBy(F.spark_partition_id()).applyInPandas(
        parse_udf, schema=spark_schema
    )
    platform.write_silver_spark(t, silver_df, mode="merge")
    n = platform.read_silver_spark(t).count()
    per_table_counts.append((t, n))
    print(f"  silver.{t:<22s} rows={n:>6,}  (MERGE, CDC on)")

bundles_df.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation
#
# - `genomic_report.data_limitation` non-null on every row (ADR-007 contract).
# - `ecg_metadata.has_waveform` false on every row (honest-limitation contract).

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# genomic_report — ADR-007 contract
gen_df = platform.read_silver_spark("genomic_report")
gen_count = gen_df.count()
null_limitations = gen_df.filter(F.col("data_limitation").isNull()).count()
print(f"silver.genomic_report rows: {gen_count:,}")
print(f"  null data_limitation rows: {null_limitations}  (must be 0 — ADR-007)")
assert null_limitations == 0, f"ADR-007 violation: {null_limitations} rows missing data_limitation"
if gen_count > 0:
    print("\ndata_limitation distinct values:")
    display(gen_df.groupBy("data_limitation").count())
    print("\nSample rows:")
    display(gen_df.limit(3))

# ecg_metadata — has_waveform contract
ecg_df = platform.read_silver_spark("ecg_metadata")
ecg_count = ecg_df.count()
print(f"\nsilver.ecg_metadata rows: {ecg_count:,}")
if ecg_count > 0:
    waveform_count = ecg_df.filter(F.col("has_waveform")).count()
    print(f"  rows with has_waveform=True: {waveform_count}  (expected 0)")
    display(ecg_df.limit(3))
else:
    print("  ECG from DiagnosticReport is rare in Coherent; 0 is expected and honest.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

platform.log_metric("genomic_report", "row_count", gen_count)
platform.log_metric("ecg_metadata", "row_count", ecg_count)
print("07_silver_ecg_genomics complete — next: 08_silver_validation")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
