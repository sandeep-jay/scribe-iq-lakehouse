"""Spark-native ``silver.soap_note`` builder (ADR-022).

DocumentReference.content[0].attachment.data is a Base64-encoded SOAP note in
the Coherent dataset. We decode in-Spark with ``unbase64 → decode`` and run
case-insensitive regex over the result for S / O / A / P section detection —
identical heuristics to ``core.transforms.silver_soap_notes``.
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

PRIMARY_KEY = "note_id"
TABLE_NAME = "soap_note"

SCHEMA = StructType(
    [
        StructField("note_id", StringType(), True),
        StructField("patient_id", StringType(), True),
        StructField("encounter_id", StringType(), True),
        StructField("note_date", TimestampType(), True),
        StructField("note_text", StringType(), True),
        StructField("has_subjective", BooleanType(), True),
        StructField("has_objective", BooleanType(), True),
        StructField("has_assessment", BooleanType(), True),
        StructField("has_plan", BooleanType(), True),
        StructField("char_count", IntegerType(), True),
        StructField("word_count", IntegerType(), True),
        StructField("binary_id", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("ingest_timestamp", TimestampType(), True),
    ]
)


def _section_flag(lower_text, pattern: str):
    """``True`` if any line in ``lower_text`` starts with ``pattern``."""
    return lower_text.rlike(rf"(?m)^\s*{pattern}\b")


def build(bundles_df: DataFrame, ingest_ts: datetime) -> DataFrame:
    """Build silver.soap_note from a Spark DataFrame of FHIR bundles."""
    resources = parse_bundles_to_resources(bundles_df)
    docs = resources.filter(F.col("r.resourceType") == "DocumentReference")

    attachment0 = F.col("r.content").getItem(0)["attachment"]
    encoded = attachment0["data"]
    decoded = F.decode(F.unbase64(encoded), "UTF-8")

    # Only keep rows that actually have a decoded body — empty attachments aren't notes.
    docs_with_text = docs.filter(encoded.isNotNull() & (F.length(encoded) > 0))

    lower = F.lower(decoded)
    enc_ref = F.col("r.context.encounter").getItem(0)["reference"]

    silver = docs_with_text.select(
        F.col("r.id").alias("note_id"),
        strip_reference(F.col("r.subject.reference")).alias("patient_id"),
        strip_reference(enc_ref).alias("encounter_id"),
        F.to_timestamp(F.col("r.date")).alias("note_date"),
        decoded.alias("note_text"),
        _section_flag(lower, r"subjective[:\.]?").alias("has_subjective"),
        _section_flag(lower, r"objective[:\.]?").alias("has_objective"),
        _section_flag(lower, r"assessment[:\.]?").alias("has_assessment"),
        _section_flag(lower, r"plan[:\.]?").alias("has_plan"),
        F.length(decoded).alias("char_count"),
        F.size(F.split(decoded, r"\s+")).alias("word_count"),
        # binary_id: Coherent embeds Base64 inline, so there's no separate Binary
        # resource. We surface contentType as a placeholder so downstream tools can
        # tell embedded-inline (this field non-null) vs separate-Binary (null).
        attachment0["contentType"].alias("binary_id"),
        F.col("source_file"),
        F.lit(ingest_ts).cast(TimestampType()).alias("ingest_timestamp"),
    )

    return dedup_keep_last(silver, PRIMARY_KEY)
