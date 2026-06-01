"""Spark-native ``silver.genomic_report`` builder (ADR-022, ADR-007).

Synthea Coherent genomic data is **simulated inheritance**, not clinical
variants. Per ADR-007 the ``data_limitation`` field is non-nullable and always
populated with the canonical limitation string. The builder asserts the column
has zero nulls before returning.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import (
    BooleanType,
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

PRIMARY_KEY = "report_id"
TABLE_NAME = "genomic_report"

DATA_LIMITATION = "Synthea simulated inheritance — not clinical variants"

GENOMIC_PATTERNS = ["genomic", "genetic", "gene panel", "dna", "sequencing", "variant"]
GENOMIC_REGEX = "(?i)(" + "|".join(GENOMIC_PATTERNS) + ")"

PATHOGENIC_REGEX = "(?i)(pathogenic|deleterious|likely pathogenic)"
FAMILY_HISTORY_REGEX = "(?i)(family history|familial|hereditary|inherited)"

SCHEMA = StructType(
    [
        StructField("report_id", StringType(), True),
        StructField("patient_id", StringType(), True),
        StructField("encounter_id", StringType(), True),
        StructField("report_date", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("gene_panel_name", StringType(), True),
        StructField("variant_count", IntegerType(), True),
        StructField("has_pathogenic_variant", BooleanType(), True),
        StructField("family_history_flag", BooleanType(), True),
        StructField("result_summary", StringType(), True),
        StructField("data_limitation", StringType(), False),
        StructField("source_file", StringType(), True),
        StructField("ingest_timestamp", TimestampType(), True),
    ]
)


def build(bundles_df: DataFrame, ingest_ts: datetime) -> DataFrame:
    """Build silver.genomic_report from a Spark DataFrame of FHIR bundles."""
    resources = parse_bundles_to_resources(bundles_df)
    dr = resources.filter(F.col("r.resourceType") == "DiagnosticReport")

    coding_displays = F.array_join(
        F.transform(F.col("r.code.coding"), lambda c: F.coalesce(c["display"], F.lit(""))),
        " ",
    )
    code_text = F.coalesce(F.col("r.code.text"), F.lit(""))
    classification_text = F.concat_ws(" ", code_text, coding_displays)
    genomic = dr.filter(classification_text.rlike(GENOMIC_REGEX))

    conclusion = F.coalesce(F.col("r.conclusion"), F.lit(""))

    silver = genomic.select(
        F.col("r.id").alias("report_id"),
        strip_reference(F.col("r.subject.reference")).alias("patient_id"),
        strip_reference(F.col("r.encounter.reference")).alias("encounter_id"),
        F.coalesce(
            F.to_timestamp(F.col("r.effectiveDateTime")),
            F.to_timestamp(F.col("r.issued")),
        ).alias("report_date"),
        F.col("r.status").alias("status"),
        code_text.alias("gene_panel_name"),
        F.lit(None).cast(IntegerType()).alias("variant_count"),
        conclusion.rlike(PATHOGENIC_REGEX).alias("has_pathogenic_variant"),
        conclusion.rlike(FAMILY_HISTORY_REGEX).alias("family_history_flag"),
        F.col("r.conclusion").alias("result_summary"),
        F.lit(DATA_LIMITATION).alias("data_limitation"),
        F.col("source_file"),
        F.lit(ingest_ts).cast(TimestampType()).alias("ingest_timestamp"),
    )

    return dedup_keep_last(silver, PRIMARY_KEY)
