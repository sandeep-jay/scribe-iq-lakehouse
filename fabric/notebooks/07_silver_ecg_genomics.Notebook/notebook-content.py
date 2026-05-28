# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 07 — Silver: ecg_metadata + genomic_report
#
# **Purpose.** Build the two specialty-modality Silver tables: `ecg_metadata` and `genomic_report`. Both have honest-limitations baked into the schema — the data isn't clinical-grade and the columns say so.
#
# **Inputs.** `Files/bronze/fhir/cohort=*/*.json` — both transforms read `DiagnosticReport` resources, classified by LOINC code / keyword.
#
# **Outputs.** Two Delta tables under `Tables/silver/`:
# - `ecg_metadata` (PK `ecg_id`) — metadata only; Coherent ECG is SBML-model-generated, not a real waveform; `has_waveform = false` always.
# - `genomic_report` (PK `report_id`) — metadata only; `data_limitation` non-nullable per [ADR-007](../../docs/adr/007-genomic-data-limitation.md).
#
# **Expected scale (full Coherent).** `ecg_metadata` is typically 0 rows (ECG lives in Binary waveform resources, not DiagnosticReport — documented). `genomic_report` ≈ 419 rows.
#
# Screenshot the `data_limitation` coverage cell as `07_silver_ecg_genomics.png`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture context
#
# - **[ADR-007](../../docs/adr/007-genomic-data-limitation.md)** — `data_limitation` is a **first-class non-nullable column** on `genomic_report`. Synthea genomics models inheritance simulation, not real clinical variants; this column flags that fact to every downstream consumer (vs hiding it in a README footnote).
# - **ECG candor:** the transform looks for `DiagnosticReport`s with ECG LOINC codes / keywords; Coherent's ECG signal is delivered via a separate Binary waveform (not in scope here). `has_waveform = false` reflects metadata-only extraction. Future Phase 3 ECG waveform feature extraction is in the roadmap (spec §9).
# - **Clinical-code rule** — SNOMED / LOINC stored as `str`.
#
# Follows the 8-cell template (cells 5/7 iterate across the 2 tables).

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

SPECIALTY_TABLES = ("ecg_metadata", "genomic_report")
MIN_ROWS = {"ecg_metadata": 0, "genomic_report": 0}  # both can legitimately be 0 in dev cohorts.

platform = get_platform()
ingest_ts = datetime.now(UTC)
for t in SPECIALTY_TABLES:
    print(f"  {t:<20s} PK={SILVER_TABLES[t].primary_key}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform approach
#
# Parse-once → build → MERGE-upsert per table, same as 04. The genomic builder always populates `data_limitation` with the canonical Synthea note (`Synthea simulated inheritance — not clinical variants`) — if it ever comes out `None`, the transform raises `ValueError` (`.claude/rules/transforms.md` non-negotiable).

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bundles = platform.read_bronze_fhir()
print(f"Bundles read: {len(bundles)}")

parser = FHIRBundleParser()
accumulated: dict[str, list[dict]] = {t: [] for t in SPECIALTY_TABLES}
for bundle in bundles:
    parsed = parser.parse_bundle(bundle)
    for t in SPECIALTY_TABLES:
        accumulated[t].extend(parsed.get(t, []))

for t in SPECIALTY_TABLES:
    print(f"  {t:<20s} records parsed: {len(accumulated[t]):>6,}")

for t in SPECIALTY_TABLES:
    spec = SILVER_TABLES[t]
    table = spec.build(accumulated[t], ingest_ts)
    platform.write_silver(t, table, mode="merge")
    print(f"  silver.{t:<20s} rows={table.num_rows:>6,}  (mode=merge, CDC on)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation
#
# For `genomic_report`: assert every row has a non-null `data_limitation` (the ADR-007 contract). For `ecg_metadata`: confirm `has_waveform` is `False` everywhere (the honest-limitation contract).

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

# genomic_report — ADR-007 contract check
gen_df = spark.read.format("delta").load(platform.storage_path("silver", "genomic_report"))
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
ecg_df = spark.read.format("delta").load(platform.storage_path("silver", "ecg_metadata"))
ecg_count = ecg_df.count()
print(f"\nsilver.ecg_metadata rows: {ecg_count:,}")
if ecg_count > 0:
    waveform_count = ecg_df.filter(F.col("has_waveform") == True).count()  # noqa: E712
    print(f"  rows with has_waveform=True: {waveform_count}  (expected 0 — metadata-only)")
    display(ecg_df.limit(3))
else:
    print("  ECG metadata-from-DiagnosticReport is rare in Coherent; 0 is expected and honest.")

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
