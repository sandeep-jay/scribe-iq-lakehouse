"""Spark-native ``silver.imaging_study`` builder (ADR-022).

FHIR-only projection of ImagingStudy. DICOM-enrichment fields (UIDs read from
pixel-stripped headers, manufacturer, etc.) are left null at this tier —
populating them requires reading the on-disk DCM files with pydicom, which is
out of scope for the in-Spark Bundle pass. The columns remain in the schema so
local + Fabric tables are union-compatible.
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

PRIMARY_KEY = "study_id"
TABLE_NAME = "imaging_study"

SCHEMA = StructType(
    [
        StructField("study_id", StringType(), True),
        StructField("patient_id", StringType(), True),
        StructField("encounter_id", StringType(), True),
        StructField("started_date", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("modality", StringType(), True),
        StructField("modality_display", StringType(), True),
        StructField("body_site_code", StringType(), True),
        StructField("body_site_display", StringType(), True),
        StructField("series_count", IntegerType(), True),
        StructField("instance_count", IntegerType(), True),
        StructField("description", StringType(), True),
        # DICOM-enrichment fields (null on Fabric, populated by core's pydicom path)
        StructField("study_instance_uid", StringType(), True),
        StructField("series_instance_uid", StringType(), True),
        StructField("sop_class_uid", StringType(), True),
        StructField("dcm_manufacturer", StringType(), True),
        StructField("dcm_modality", StringType(), True),
        StructField("dcm_rows", IntegerType(), True),
        StructField("dcm_columns", IntegerType(), True),
        StructField("dcm_pixel_data_stripped", BooleanType(), True),
        StructField("source_file", StringType(), True),
        StructField("ingest_timestamp", TimestampType(), True),
    ]
)


def build(bundles_df: DataFrame, ingest_ts: datetime) -> DataFrame:
    """Build silver.imaging_study from a Spark DataFrame of FHIR bundles."""
    resources = parse_bundles_to_resources(bundles_df)
    img = resources.filter(F.col("r.resourceType") == "ImagingStudy")

    modality0 = F.col("r.modality").getItem(0)
    series0_bodysite = F.col("r.series").getItem(0)["bodySite"]

    silver = img.select(
        F.col("r.id").alias("study_id"),
        strip_reference(F.col("r.subject.reference")).alias("patient_id"),
        strip_reference(F.col("r.encounter.reference")).alias("encounter_id"),
        F.to_timestamp(F.col("r.started")).alias("started_date"),
        F.col("r.status").alias("status"),
        modality0["code"].alias("modality"),
        modality0["display"].alias("modality_display"),
        series0_bodysite["code"].alias("body_site_code"),
        series0_bodysite["display"].alias("body_site_display"),
        F.col("r.numberOfSeries").alias("series_count"),
        F.col("r.numberOfInstances").alias("instance_count"),
        F.col("r.description").alias("description"),
        F.lit(None).cast(StringType()).alias("study_instance_uid"),
        F.lit(None).cast(StringType()).alias("series_instance_uid"),
        F.lit(None).cast(StringType()).alias("sop_class_uid"),
        F.lit(None).cast(StringType()).alias("dcm_manufacturer"),
        F.lit(None).cast(StringType()).alias("dcm_modality"),
        F.lit(None).cast(IntegerType()).alias("dcm_rows"),
        F.lit(None).cast(IntegerType()).alias("dcm_columns"),
        F.lit(None).cast(BooleanType()).alias("dcm_pixel_data_stripped"),
        F.col("source_file"),
        F.lit(ingest_ts).cast(TimestampType()).alias("ingest_timestamp"),
    )

    return dedup_keep_last(silver, PRIMARY_KEY)
