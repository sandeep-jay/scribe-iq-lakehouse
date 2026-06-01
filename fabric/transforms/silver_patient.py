"""Spark-native ``silver.patient`` builder (ADR-022)."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import (
    BooleanType,
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from fabric.transforms._common import dedup_keep_last, parse_bundles_to_resources

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

PRIMARY_KEY = "patient_id"
TABLE_NAME = "patient"

RACE_URL = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race"
ETHNICITY_URL = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity"

SCHEMA = StructType(
    [
        StructField("patient_id", StringType(), True),
        StructField("birth_date", DateType(), True),
        StructField("gender", StringType(), True),
        StructField("race", StringType(), True),
        StructField("ethnicity", StringType(), True),
        StructField("state", StringType(), True),
        StructField("city", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("deceased", BooleanType(), True),
        StructField("deceased_date", TimestampType(), True),
        StructField("source_file", StringType(), True),
        StructField("ingest_timestamp", TimestampType(), True),
    ]
)


def build(bundles_df: DataFrame, ingest_ts: datetime) -> DataFrame:
    """Build silver.patient from a Spark DataFrame of FHIR bundles."""
    resources = parse_bundles_to_resources(bundles_df)
    patients = resources.filter(F.col("r.resourceType") == "Patient")

    race_outer = F.filter(F.col("r.extension"), lambda e: e["url"] == RACE_URL)
    race_inner = F.filter(
        race_outer.getItem(0)["extension"], lambda e: e["url"] == "ombCategory"
    )

    eth_outer = F.filter(F.col("r.extension"), lambda e: e["url"] == ETHNICITY_URL)
    eth_inner = F.filter(
        eth_outer.getItem(0)["extension"], lambda e: e["url"] == "ombCategory"
    )

    deceased_flag = F.coalesce(
        F.col("r.deceasedBoolean"),
        F.col("r.deceasedDateTime").isNotNull(),
        F.lit(False),  # noqa: FBT003
    )

    silver = patients.select(
        F.col("r.id").alias("patient_id"),
        F.to_date(F.col("r.birthDate")).alias("birth_date"),
        F.col("r.gender").alias("gender"),
        race_inner.getItem(0)["valueCoding"]["display"].alias("race"),
        eth_inner.getItem(0)["valueCoding"]["display"].alias("ethnicity"),
        F.col("r.address").getItem(0)["state"].alias("state"),
        F.col("r.address").getItem(0)["city"].alias("city"),
        F.col("r.address").getItem(0)["postalCode"].alias("zip"),
        deceased_flag.alias("deceased"),
        F.to_timestamp(F.col("r.deceasedDateTime")).alias("deceased_date"),
        F.col("source_file"),
        F.lit(ingest_ts).cast(TimestampType()).alias("ingest_timestamp"),
    )

    return dedup_keep_last(silver, PRIMARY_KEY)
