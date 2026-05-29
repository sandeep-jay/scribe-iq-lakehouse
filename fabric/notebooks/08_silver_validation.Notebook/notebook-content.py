# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 08 — Silver validation → `silver.ingest_log`
#
# **Purpose.** Run rule-based validation against every materialized Silver
# table and append a row per table to `Tables/silver/ingest_log`. Each rule
# (min row count, non-null PK / required columns, key uniqueness, SOAP short-
# note fraction, ECG numeric ranges, etc.) runs in a single Spark `.agg()`
# per table; threshold comparison is plain Python on the result.
#
# **Output.** `Tables/silver/ingest_log` (append) — one row per table per
# notebook run. CDC enabled. Consumed by `09_gold_encounter_summary` for
# lineage and by Power BI for the medallion-health dashboard.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture
#
# - **[ADR-022](../../docs/adr/022-platform-independent-implementations.md)** —
#   Fabric runs its own `fabric.validation` (Spark-native, mirrors the
#   core grammar).
# - Rules live in `fabric.validation.schema_registry`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import UTC, datetime

from fabric.platform import FabricPlatform
from fabric.validation.schema_registry import VALIDATION_RULES
from fabric.validation.validate import results_to_dataframe, validate_table

platform = FabricPlatform()
spark = platform.get_spark_session()
ingest_ts = datetime.now(UTC)
TABLES = list(VALIDATION_RULES)
print(f"Validating {len(TABLES)} Silver tables · ingest_ts: {ingest_ts.isoformat()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1 — Run every rule against every Silver table
#
# One round-trip per table: a single `.agg()` over the Silver DataFrame
# computes every metric (counts, null counts, distinct counts, range
# violations, section completeness), then thresholds are checked in Python.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = []
for table in TABLES:
    try:
        df = platform.read_silver_spark(table)
    except Exception as err:  # noqa: BLE001 — surface table-missing as a soft fail
        print(f"!! could not read silver.{table}: {err}")
        continue
    result = validate_table(table, df)
    results.append(result)
    status = "PASS" if result.passed else "FAIL"
    print(f"{status}  silver.{table}: {result.row_count:,} rows · {len(result.checks)} checks")
    for check in result.checks:
        flag = "✓" if check.passed else "✗"
        print(f"    {flag} {check.name}: {check.detail}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Append results to `silver.ingest_log`

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

log_df = results_to_dataframe(spark, results, ingest_ts)
platform.write_silver_spark("ingest_log", log_df, mode="append")
print(f"Appended {len(results)} rows to silver.ingest_log")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation summary

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(platform.read_silver_spark("ingest_log").orderBy("ingest_timestamp", ascending=False).limit(20))

failing = [r.table for r in results if not r.passed]
if failing:
    platform.send_alert("warning", f"Validation failures: {failing}")
else:
    print("All Silver tables passed validation.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
