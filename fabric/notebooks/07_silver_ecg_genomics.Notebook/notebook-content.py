# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 07 — Silver: `ecg_metadata` + `genomic_report` (pure Spark)
#
# **Purpose.** Build the two DiagnosticReport-derived Silver tables in one
# Bronze pass. Both transforms filter `DiagnosticReport` resources by
# code-text regex (ECG keywords for the first, genomic keywords for the
# second) so we cache the bundle read and reuse it.
#
# **Scope on Fabric (ADR-022).** ECG cross-Observation enrichment (heart rate,
# rhythm, intervals derived from linked Observations) is intentionally **not**
# done on the Fabric tier — the columns are present in the schema for
# union-compatibility with the local tier but stay null here. Genomics has
# `data_limitation` populated (ADR-007 non-nullable contract).
#
# **Outputs.** `Tables/silver/ecg_metadata` and `Tables/silver/genomic_report`,
# both CDC-enabled.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture
#
# - **[ADR-022](../../docs/adr/022-platform-independent-implementations.md)** —
#   Independent Spark-native impl.
# - **[ADR-007](../../docs/adr/007-genomic-data-limitation.md)** —
#   `data_limitation` is non-nullable on `silver.genomic_report`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import UTC, datetime

from fabric.platform import FabricPlatform
from fabric.transforms.registry import REGISTRY

TABLES = ["ecg_metadata", "genomic_report"]
MIN_ROWS = {"ecg_metadata": 0, "genomic_report": 0}  # both sparse in Coherent

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

# ## Step 1 — Read Bronze once, cache, reuse

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bundles_df = platform.read_bronze_bundles_spark().cache()
n_bundles = bundles_df.count()
print(f"Bundles read: {n_bundles:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Build + MERGE each table

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for table in TABLES:
    spec = REGISTRY[table]
    silver_df = spec.build(bundles_df, ingest_ts)
    platform.write_silver_spark(table, silver_df, mode="merge")
    written = platform.read_silver_spark(table)
    count = written.count()
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
