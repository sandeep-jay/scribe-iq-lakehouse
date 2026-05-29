"""Spark-native ``silver.ecg_metadata`` builder (ADR-022).

Filter DiagnosticReports whose code text or coding display mentions an ECG
keyword. Cross-Observation enrichment (heart rate / rhythm / intervals from
linked Observations) is intentionally **not** done at Silver — those fields are
left null. Rationale: keeps the Silver builder pure-projection (no joins),
matching how core.transforms.silver_ecg also leaves them null when Coherent
doesn't populate them inline. Downstream Gold can join silver.observation if
needed.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from fabric.transforms._common import (
    dedup_keep_last,
    parse_bundles_to_resources,
    strip_reference,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

PRIMARY_KEY = "ecg_id"
TABLE_NAME = "ecg_metadata"

# Substring patterns evaluated case-insensitively against code.text +
# code.coding[*].display to classify a DiagnosticReport as an ECG report.
ECG_PATTERNS = ["ecg", "ekg", "electrocardiogra"]
ECG_REGEX = "(?i)(" + "|".join(ECG_PATTERNS) + ")"

SCHEMA = StructType(
    [
        StructField("ecg_id", StringType(), True),
        StructField("patient_id", StringType(), True),
        StructField("encounter_id", StringType(), True),
        StructField("report_date", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("rhythm", StringType(), True),
        StructField("heart_rate_bpm", DoubleType(), True),
        StructField("pr_interval_ms", DoubleType(), True),
        StructField("qrs_duration_ms", DoubleType(), True),
        StructField("qt_interval_ms", DoubleType(), True),
        StructField("has_waveform", BooleanType(), True),
        StructField("waveform_lead_count", IntegerType(), True),
        StructField("source_file", StringType(), True),
        StructField("ingest_timestamp", TimestampType(), True),
    ]
)


def build(bundles_df: DataFrame, ingest_ts: datetime) -> DataFrame:
    """Build silver.ecg_metadata from a Spark DataFrame of FHIR bundles."""
    resources = parse_bundles_to_resources(bundles_df)
    dr = resources.filter(F.col("r.resourceType") == "DiagnosticReport")

    # Concatenate all candidate text fields and regex-match for ECG-ness.
    coding_displays = F.array_join(
        F.transform(F.col("r.code.coding"), lambda c: F.coalesce(c["display"], F.lit(""))),
        " ",
    )
    code_text = F.coalesce(F.col("r.code.text"), F.lit(""))
    classification_text = F.concat_ws(" ", code_text, coding_displays)
    ecg = dr.filter(classification_text.rlike(ECG_REGEX))

    # ADR-014: pixel data / waveform binary is never loaded at Silver.
    # ``has_waveform`` is always False on this tier; loaders that need waveforms
    # consume DiagnosticReport.presentedForm separately downstream.
    silver = ecg.select(
        F.col("r.id").alias("ecg_id"),
        strip_reference(F.col("r.subject.reference")).alias("patient_id"),
        strip_reference(F.col("r.encounter.reference")).alias("encounter_id"),
        F.coalesce(
            F.to_timestamp(F.col("r.effectiveDateTime")),
            F.to_timestamp(F.col("r.issued")),
        ).alias("report_date"),
        F.col("r.status").alias("status"),
        F.lit(None).cast(StringType()).alias("rhythm"),
        F.lit(None).cast(DoubleType()).alias("heart_rate_bpm"),
        F.lit(None).cast(DoubleType()).alias("pr_interval_ms"),
        F.lit(None).cast(DoubleType()).alias("qrs_duration_ms"),
        F.lit(None).cast(DoubleType()).alias("qt_interval_ms"),
        F.lit(False).alias("has_waveform"),  # noqa: FBT003
        F.lit(None).cast(IntegerType()).alias("waveform_lead_count"),
        F.col("source_file"),
        F.lit(ingest_ts).cast(TimestampType()).alias("ingest_timestamp"),
    )

    return dedup_keep_last(silver, PRIMARY_KEY)
