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
# **Purpose.** Run the full validation rule set across all 10 Silver tables and append every result (per rule, per table) to `silver.ingest_log`. This is the audit table the corpus contract relies on — `ingest_log` is queryable evidence that the medallion built correctly.
#
# **Inputs.** Every Silver table populated by 02–07.
#
# **Output.** Delta table `Tables/silver/ingest_log` (append-only). Each row: `table_name · check_name · passed · detail · ingest_ts`.
#
# **Run after.** 02 → 07 (every Silver table built).
#
# Screenshot the per-table pass/fail summary cell as `13_ingest_log.png` per `fabric/docs/SCREENSHOTS.md`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture context
#
# - Same `validate_table` and `results_to_arrow` used by the local CLI (`core.surfaces.cli.pipeline.run_pipeline`) and the Dagster `@asset_check`s — single rule set, three execution surfaces.
# - Rules per table come from `core.validation.schema_registry` (row count thresholds, non-null required columns, referential integrity, clinical ranges where applicable).
# - A failing check does **not** crash the notebook — it surfaces in `ingest_log` as `passed=False` and is screenshot-evidence of honest reporting. The cell at the end raises only if a Silver table is missing entirely (a structural failure).
# - `silver.ingest_log` is **append-only**: every run is a new historical entry. To find the latest run, query `MAX(ingest_ts)`.
#
# Follows the 8-cell template (cells 5/7 are loops across all 10 tables).

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
ingest_ts = datetime.now(UTC)
print(f"Platform: {platform.name} · Validating {len(SILVER_TABLES)} Silver tables")
print(f"ingest_ts: {ingest_ts.isoformat()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform approach (validation pass)
#
# For each Silver table in the registry:
# 1. `platform.read_silver(name)` → `pa.Table`
# 2. `validate_table(name, table)` → `ValidationResult` with rule-by-rule outcomes
# 3. Append the row count and pass/fail to in-memory summary; on failure, also send a `warning` alert via the platform
#
# After the loop, `results_to_arrow(results, ingest_ts)` flattens every rule outcome to an Arrow table and we `platform.write_silver("ingest_log", ..., mode="append")` it. Re-running the notebook appends a fresh batch — no overwrite, no loss of history.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary_rows = []
results = []
for name in SILVER_TABLES:
    table = platform.read_silver(name)
    result = validate_table(name, table)
    results.append(result)
    n_rules = len(result.checks)
    n_failed = sum(1 for c in result.checks if not c.passed)
    summary_rows.append((name, table.num_rows, n_rules, n_failed, result.passed))
    platform.log_metric(name, "row_count", table.num_rows)
    if not result.passed:
        platform.send_alert("warning", f"Validation failed for silver.{name}: {result.failed_checks}")
    print(f"  silver.{name:<20s} rows={table.num_rows:>8,}  rules={n_rules:>2}  failed={n_failed:>2}  passed={result.passed}")

ingest_log_table = results_to_arrow(results, ingest_ts)
platform.write_silver("ingest_log", ingest_log_table, mode="append")
print(f"\nAppended {ingest_log_table.num_rows} rule outcomes to silver.ingest_log")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation (of validation) — the audit shape
#
# Three views to screenshot:
# 1. Per-table summary (rows, rule count, failed count, overall pass)
# 2. Recent `ingest_log` entries for *this* run (filtered by `ingest_ts`)
# 3. Any failing rule details (empty if everything passes)

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

summary_df = spark.createDataFrame(
    summary_rows,
    schema="table STRING, row_count BIGINT, n_rules INT, n_failed INT, passed BOOLEAN",
)
print("Per-table validation summary:")
display(summary_df)

log_df = spark.read.format("delta").load(platform.storage_path("silver", "ingest_log"))
this_run = log_df.filter(F.col("ingest_ts") == F.lit(ingest_ts))
print(f"\nThis run's ingest_log rows: {this_run.count()}")
display(this_run.orderBy("table_name", "check_name").limit(50))

failed = this_run.filter(F.col("passed") == False)  # noqa: E712
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
