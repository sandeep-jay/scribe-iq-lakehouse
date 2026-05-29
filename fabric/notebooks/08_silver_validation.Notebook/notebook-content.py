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
# **Purpose.** Run the full validation rule set across all 10 Silver tables
# and append every rule outcome (per table, per rule) to `silver.ingest_log`.
# Audit table the corpus contract relies on; queryable evidence the medallion
# built correctly.
#
# **Inputs.** Every Silver table populated by 02–07.
#
# **Output.** Delta table `Tables/silver/ingest_log` (append-only). Each row:
# `table_name · check_name · passed · detail · ingest_ts`.
#
# Screenshot the per-table summary as `13_ingest_log.png`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture context — Spark for I/O, pa.Table for rule checks
#
# `validate_table` runs rule-by-rule on a `pa.Table` (matches CLI + Dagster
# call sites — ADR-002 portability). Notebook flow:
# 1. **Spark reads** each Silver Delta via `platform.read_silver_spark` →
#    `platform.read_silver` (pa.Table convenience wrapper).
# 2. **Driver-side validation** — `validate_table(name, pa_table)` produces
#    `ValidationResult(checks=[CheckOutcome, ...])`. Validation rules are
#    metadata-bound aggregations (counts, distinct, non-null fractions) —
#    fast on the driver after Spark hands the data over.
# 3. **Spark-native write** — `platform.write_silver_spark("ingest_log",
#    spark_df, mode="append")` lands the audit row.
#
# `ingest_log` itself is append-only (no PK in registry; append never goes
# through MERGE so no PK lookup needed).

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
from core.validation.validate import results_to_arrow, validate_table

platform = get_platform()
spark = platform.get_spark_session()
ingest_ts = datetime.now(UTC)
print(f"Platform: {platform.name} · Validating {len(SILVER_TABLES)} Silver tables")
print(f"ingest_ts: {ingest_ts.isoformat()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1 — Validate each Silver table
#
# For each of the 10 Silver tables: Spark-read → pa.Table → `validate_table`
# → accumulate results. Each ValidationResult carries every rule's outcome,
# not just failures (the Dagster asset graph relies on the same shape).

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary_rows = []
results = []
for name in SILVER_TABLES:
    table = platform.read_silver(name)  # pa.Table via Spark→pandas convert
    result = validate_table(name, table)
    results.append(result)
    n_rules = len(result.checks)
    n_failed = sum(1 for c in result.checks if not c.passed)
    summary_rows.append((name, table.num_rows, n_rules, n_failed, result.passed))
    platform.log_metric(name, "row_count", table.num_rows)
    if not result.passed:
        platform.send_alert(
            "warning", f"Validation failed for silver.{name}: {result.failed_checks}"
        )
    print(
        f"  silver.{name:<22s} rows={table.num_rows:>8,}  "
        f"rules={n_rules:>2}  failed={n_failed:>2}  passed={result.passed}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Write all rule outcomes to silver.ingest_log
#
# `results_to_arrow` flattens every rule outcome into rows. We convert the
# resulting pa.Table to a Spark DataFrame and append via `write_silver_spark`
# — same write path silver tables use.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ingest_log_pa = results_to_arrow(results, ingest_ts)
ingest_log_df = spark.createDataFrame(ingest_log_pa.to_pandas())
platform.write_silver_spark("ingest_log", ingest_log_df, mode="append")
print(f"\nAppended {ingest_log_pa.num_rows} rule outcomes to silver.ingest_log")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation views
#
# Three Spark-native views — all screenshottable:
# 1. Per-table summary.
# 2. This run's `ingest_log` rows (filter by `ingest_ts`).
# 3. Any failing rules.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F  # noqa: N812

summary_df = spark.createDataFrame(
    summary_rows,
    schema="table STRING, row_count BIGINT, n_rules INT, n_failed INT, passed BOOLEAN",
)
print("Per-table validation summary:")
display(summary_df)

log_df = platform.read_silver_spark("ingest_log")
this_run = log_df.filter(F.col("ingest_ts") == F.lit(ingest_ts))
print(f"\nThis run's ingest_log rows: {this_run.count()}")
display(this_run.orderBy("table_name", "check_name").limit(50))

failed = this_run.filter(~F.col("passed"))
failed_count = failed.count()
print(f"\nFailing rules this run: {failed_count}")
if failed_count > 0:
    print("Failure detail (screenshot if non-zero):")
    display(failed.select("table_name", "check_name", "detail"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

total_rules = sum(s[2] for s in summary_rows)
total_failed = sum(s[3] for s in summary_rows)
print(f"08_silver_validation complete — {total_rules} rules across 10 tables, {total_failed} failed.")
print("Next: 09_gold_encounter_summary")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
