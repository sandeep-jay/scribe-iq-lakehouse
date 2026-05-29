"""Spark-native Silver builders for the four "clinical" tables (ADR-022).

Tables: ``condition``, ``observation``, ``medication_request``, ``procedure``.
Each builder filters resources by ``resourceType`` then projects to its own
schema. SNOMED/LOINC/ICD codes stay ``str`` (never cast to int) per the
healthcare-data rule.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import (
    DoubleType,
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
    from pyspark.sql import Column, DataFrame


def _code_coding0(col: Column) -> Column:
    """Convenience: ``CodeableConcept.coding[0]`` extractor."""
    return col["coding"].getItem(0)


# --------------------------------------------------------------------- condition

CONDITION_PRIMARY_KEY = "condition_id"
CONDITION_TABLE_NAME = "condition"

CONDITION_SCHEMA = StructType(
    [
        StructField("condition_id", StringType(), True),
        StructField("patient_id", StringType(), True),
        StructField("encounter_id", StringType(), True),
        StructField("code", StringType(), True),
        StructField("display", StringType(), True),
        StructField("clinical_status", StringType(), True),
        StructField("onset_date", TimestampType(), True),
        StructField("abatement_date", TimestampType(), True),
        StructField("recorded_date", TimestampType(), True),
        StructField("source_file", StringType(), True),
        StructField("ingest_timestamp", TimestampType(), True),
    ]
)


def build_condition(bundles_df: DataFrame, ingest_ts: datetime) -> DataFrame:
    resources = parse_bundles_to_resources(bundles_df)
    cond = resources.filter(F.col("r.resourceType") == "Condition")
    code0 = _code_coding0(F.col("r.code"))
    silver = cond.select(
        F.col("r.id").alias("condition_id"),
        strip_reference(F.col("r.subject.reference")).alias("patient_id"),
        strip_reference(F.col("r.encounter.reference")).alias("encounter_id"),
        code0["code"].alias("code"),
        code0["display"].alias("display"),
        _code_coding0(F.col("r.clinicalStatus"))["code"].alias("clinical_status"),
        F.to_timestamp(F.col("r.onsetDateTime")).alias("onset_date"),
        F.to_timestamp(F.col("r.abatementDateTime")).alias("abatement_date"),
        F.to_timestamp(F.col("r.recordedDate")).alias("recorded_date"),
        F.col("source_file"),
        F.lit(ingest_ts).cast(TimestampType()).alias("ingest_timestamp"),
    )
    return dedup_keep_last(silver, CONDITION_PRIMARY_KEY)


# ------------------------------------------------------------------- observation

OBSERVATION_PRIMARY_KEY = "observation_id"
OBSERVATION_TABLE_NAME = "observation"

OBSERVATION_SCHEMA = StructType(
    [
        StructField("observation_id", StringType(), True),
        StructField("patient_id", StringType(), True),
        StructField("encounter_id", StringType(), True),
        StructField("code", StringType(), True),
        StructField("display", StringType(), True),
        StructField("category", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
        StructField("value_string", StringType(), True),
        StructField("components_json", StringType(), True),
        StructField("effective_date", TimestampType(), True),
        StructField("source_file", StringType(), True),
        StructField("ingest_timestamp", TimestampType(), True),
    ]
)


def build_observation(bundles_df: DataFrame, ingest_ts: datetime) -> DataFrame:
    resources = parse_bundles_to_resources(bundles_df)
    obs = resources.filter(F.col("r.resourceType") == "Observation")
    code0 = _code_coding0(F.col("r.code"))
    cat0 = _code_coding0(F.col("r.category").getItem(0))
    # components_json — JSON-serialized so Silver schema stays flat (matches core impl).
    components_json = F.when(
        F.col("r.component").isNotNull() & (F.size(F.col("r.component")) > 0),
        F.to_json(F.col("r.component")),
    ).otherwise(F.lit(None).cast(StringType()))

    silver = obs.select(
        F.col("r.id").alias("observation_id"),
        strip_reference(F.col("r.subject.reference")).alias("patient_id"),
        strip_reference(F.col("r.encounter.reference")).alias("encounter_id"),
        code0["code"].alias("code"),
        code0["display"].alias("display"),
        cat0["code"].alias("category"),
        F.col("r.valueQuantity.value").cast(DoubleType()).alias("value"),
        F.col("r.valueQuantity.unit").alias("unit"),
        F.col("r.valueString").alias("value_string"),
        components_json.alias("components_json"),
        F.to_timestamp(F.col("r.effectiveDateTime")).alias("effective_date"),
        F.col("source_file"),
        F.lit(ingest_ts).cast(TimestampType()).alias("ingest_timestamp"),
    )
    return dedup_keep_last(silver, OBSERVATION_PRIMARY_KEY)


# ------------------------------------------------------------- medication_request

MEDICATION_REQUEST_PRIMARY_KEY = "medication_request_id"
MEDICATION_REQUEST_TABLE_NAME = "medication_request"

MEDICATION_REQUEST_SCHEMA = StructType(
    [
        StructField("medication_request_id", StringType(), True),
        StructField("patient_id", StringType(), True),
        StructField("encounter_id", StringType(), True),
        StructField("code", StringType(), True),
        StructField("display", StringType(), True),
        StructField("status", StringType(), True),
        StructField("intent", StringType(), True),
        StructField("authored_on", TimestampType(), True),
        StructField("dosage_text", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("ingest_timestamp", TimestampType(), True),
    ]
)


def build_medication_request(bundles_df: DataFrame, ingest_ts: datetime) -> DataFrame:
    resources = parse_bundles_to_resources(bundles_df)
    meds = resources.filter(F.col("r.resourceType") == "MedicationRequest")
    code0 = _code_coding0(F.col("r.medicationCodeableConcept"))
    dosage0_text = F.col("r.dosageInstruction").getItem(0)["text"]
    silver = meds.select(
        F.col("r.id").alias("medication_request_id"),
        strip_reference(F.col("r.subject.reference")).alias("patient_id"),
        strip_reference(F.col("r.encounter.reference")).alias("encounter_id"),
        code0["code"].alias("code"),
        code0["display"].alias("display"),
        F.col("r.status").alias("status"),
        F.col("r.intent").alias("intent"),
        F.to_timestamp(F.col("r.authoredOn")).alias("authored_on"),
        dosage0_text.alias("dosage_text"),
        F.col("source_file"),
        F.lit(ingest_ts).cast(TimestampType()).alias("ingest_timestamp"),
    )
    return dedup_keep_last(silver, MEDICATION_REQUEST_PRIMARY_KEY)


# --------------------------------------------------------------------- procedure

PROCEDURE_PRIMARY_KEY = "procedure_id"
PROCEDURE_TABLE_NAME = "procedure"

PROCEDURE_SCHEMA = StructType(
    [
        StructField("procedure_id", StringType(), True),
        StructField("patient_id", StringType(), True),
        StructField("encounter_id", StringType(), True),
        StructField("code", StringType(), True),
        StructField("display", StringType(), True),
        StructField("status", StringType(), True),
        StructField("performed_start", TimestampType(), True),
        StructField("performed_end", TimestampType(), True),
        StructField("source_file", StringType(), True),
        StructField("ingest_timestamp", TimestampType(), True),
    ]
)


def build_procedure(bundles_df: DataFrame, ingest_ts: datetime) -> DataFrame:
    resources = parse_bundles_to_resources(bundles_df)
    proc = resources.filter(F.col("r.resourceType") == "Procedure")
    code0 = _code_coding0(F.col("r.code"))
    # Procedure.performed[x] is performedDateTime XOR performedPeriod — coalesce to start.
    performed_start = F.coalesce(
        F.to_timestamp(F.col("r.performedDateTime")),
        F.to_timestamp(F.col("r.performedPeriod.start")),
    )
    silver = proc.select(
        F.col("r.id").alias("procedure_id"),
        strip_reference(F.col("r.subject.reference")).alias("patient_id"),
        strip_reference(F.col("r.encounter.reference")).alias("encounter_id"),
        code0["code"].alias("code"),
        code0["display"].alias("display"),
        F.col("r.status").alias("status"),
        performed_start.alias("performed_start"),
        F.to_timestamp(F.col("r.performedPeriod.end")).alias("performed_end"),
        F.col("source_file"),
        F.lit(ingest_ts).cast(TimestampType()).alias("ingest_timestamp"),
    )
    return dedup_keep_last(silver, PROCEDURE_PRIMARY_KEY)
