"""Spark-native ``silver.encounter`` builder (ADR-022)."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import (
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

PRIMARY_KEY = "encounter_id"
TABLE_NAME = "encounter"

SCHEMA = StructType(
    [
        StructField("encounter_id", StringType(), True),
        StructField("patient_id", StringType(), True),
        StructField("type_code", StringType(), True),
        StructField("type_display", StringType(), True),
        StructField("class_code", StringType(), True),
        StructField("start_date", TimestampType(), True),
        StructField("end_date", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("provider_id", StringType(), True),
        StructField("reason_code", StringType(), True),
        StructField("reason_display", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("ingest_timestamp", TimestampType(), True),
    ]
)


def build(bundles_df: DataFrame, ingest_ts: datetime) -> DataFrame:
    """Build silver.encounter from a Spark DataFrame of FHIR bundles."""
    resources = parse_bundles_to_resources(bundles_df)
    enc = resources.filter(F.col("r.resourceType") == "Encounter")

    type0_coding0 = F.col("r.type").getItem(0)["coding"].getItem(0)
    reason0_coding0 = F.col("r.reasonCode").getItem(0)["coding"].getItem(0)
    participant0 = F.col("r.participant").getItem(0)["individual"]["reference"]

    silver = enc.select(
        F.col("r.id").alias("encounter_id"),
        strip_reference(F.col("r.subject.reference")).alias("patient_id"),
        type0_coding0["code"].alias("type_code"),
        type0_coding0["display"].alias("type_display"),
        F.col("r.class.code").alias("class_code"),
        F.to_timestamp(F.col("r.period.start")).alias("start_date"),
        F.to_timestamp(F.col("r.period.end")).alias("end_date"),
        F.col("r.status").alias("status"),
        strip_reference(participant0).alias("provider_id"),
        reason0_coding0["code"].alias("reason_code"),
        reason0_coding0["display"].alias("reason_display"),
        F.col("source_file"),
        F.lit(ingest_ts).cast(TimestampType()).alias("ingest_timestamp"),
    )

    return dedup_keep_last(silver, PRIMARY_KEY)
