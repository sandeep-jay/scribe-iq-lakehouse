# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 02 — Silver: patient
#
# **Purpose.** Build `silver.patient` from Bronze FHIR bundles.
#
# **Inputs.** `Files/bronze/fhir/cohort=*/*.json` (Synthea Coherent bundles, one per patient — landed by `01_bronze_ingest`).
#
# **Output.** Delta table `Tables/silver/patient` (CDC enabled, MERGE-upserted on `patient_id`).
#
# **Expected scale.** 1 row per patient → ~1,278 rows at full Coherent scale.
#
# **Dependencies (Environment `scribe-iq-lakehouse-env`).**
# - `scribe_iq_lakehouse-0.1.0` wheel — provides `core.transforms.silver_patient`, `FabricPlatform`
# - `pyarrow`, `python-dateutil` (transform deps)
#
# **Run after.** `01_bronze_ingest` (otherwise `Files/bronze/fhir/` is empty).
#
# Screenshot the validation cell output as `02_silver_patient.png` per `fabric/docs/SCREENSHOTS.md`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture context
#
# - **[ADR-002](../../docs/adr/002-platform-abstraction.md)** — All cloud I/O via `FabricPlatform`; this notebook never touches abfss paths directly.
# - **[ADR-004](../../docs/adr/004-arrow-interchange.md)** — `build_silver_patient` returns `pa.Table` with an explicit schema.
# - **[ADR-008](../../docs/adr/008-dict-based-fhir-parsing.md)** — Bundles parsed as plain dicts; every FHIR field accessed via `.get()` with a default.
# - **[ADR-009](../../docs/adr/009-local-silver-materialization.md)** — Delta MERGE-upsert with CDC enabled; mirrored exactly on Fabric.
# - **[ADR-019](../../docs/adr/019-silver-merge-idempotency.md)** — Target-side dedup guard runs automatically inside `FabricPlatform._write_delta` if pre-existing duplicates are present on the `patient_id` key.
#
# Pure logic lives in `core/transforms/silver_patient.py` — this notebook is just the Fabric execution surface. The same `build_silver_patient` runs under the local CLI (`python -m core.surfaces.cli.pipeline`) and the Dagster asset graph; only the platform parameter changes.
#
# Follows the 8-cell template ([`.claude/rules/notebooks.md`](../../.claude/rules/notebooks.md)).

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

TABLE = "patient"
MIN_ROWS = 1  # Coherent demo cohort: assert at least one patient landed; relax/raise per scope.

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
# 1. Read every FHIR bundle from `Files/bronze/fhir/cohort=*/` via `platform.read_bronze_fhir()` (no cohort filter — all cohorts in one pass).
# 2. Parse each bundle with `FHIRBundleParser` and collect Patient records.
# 3. Call `spec.build(records, ingest_ts)` (which is `build_silver_patient`) to produce a typed `pa.Table` matching `SCHEMA` — `dedup_by_key` runs source-side, last-write-wins on `patient_id`.
# 4. `platform.write_silver("patient", table, mode="merge")` — first run does OVERWRITE+CDC; subsequent runs MERGE on `patient_id`. If target somehow has dups (legacy data), the ADR-019 guard rewrites the deduped target before the MERGE.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

parser = FHIRBundleParser()
records: list[dict] = []
n_bundles = 0
for path, bundle in platform.iter_bronze_files():
    n_bundles += 1
    parsed = parser.parse_bundle(bundle)
    src = path.rsplit("/", 1)[-1]
    for r in parsed.get(TABLE, []):
        r["source_file"] = src
        records.append(r)
print(f"Bundles read: {n_bundles} · {TABLE} records parsed: {len(records):,}")

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
# Read the table back via Spark, assert minimum row count, display a sample. The minimum is intentionally low (1) — this notebook is a unit check; full corpus-wide validation lives in `08_silver_validation` which runs the schema-registry rule set via `core.validation.validate_table` and writes outcomes to `silver.ingest_log`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
df = spark.read.format("delta").load(platform.storage_path("silver", TABLE))
count = df.count()
print(f"silver.{TABLE} row count: {count:,}")
assert count >= MIN_ROWS, f"Row count {count} below minimum {MIN_ROWS}"
display(df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Per-notebook quick metric — full ingest_log is written by 08_silver_validation.
platform.log_metric(TABLE, "row_count", count)
print(f"02_silver_patient complete — next: 03_silver_encounter")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
