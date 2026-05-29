"""Spark-native ``gold.encounter_summary`` (ADR-022, mirrors core/gold contract).

One row per ``silver.encounter``. ``active_conditions`` / ``active_medications``
are the patient's problem list **as of the encounter date** (ADR-014), not the
set recorded at the encounter. Vitals/labs/SOAP/ECG/imaging/genomics are
joined per encounter; absent context becomes null or empty list per the
corpus contract.

Schema matches ``core.gold.encounter_summary.GOLD_SCHEMA`` field-for-field so
``CONTRACT_VERSION`` stays valid regardless of which platform materialized it.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from pyspark.sql import Window
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# Mirror the core contract — bump in lockstep with core/gold/encounter_summary.py.
CONTRACT_VERSION = "1.1.0"

SILVER_SOURCES: tuple[str, ...] = (
    "patient",
    "encounter",
    "condition",
    "observation",
    "medication_request",
    "procedure",
    "soap_note",
    "ecg_metadata",
    "imaging_study",
    "genomic_report",
)

TABLE_NAME = "encounter_summary"
PRIMARY_KEY = "encounter_id"

_SUMMARY_NAMESPACE = uuid.UUID("5c0d9b1e-1f7a-5a3c-9b2e-7d6f4a2c8e10")

# LOINC codes for structured vitals — see core/gold/encounter_summary.py.
_LOINC_HEART_RATE = "8867-4"
_LOINC_TEMPERATURE = "8310-5"
_LOINC_O2_SAT = ("2708-6", "59408-5")
_LOINC_BP = "85354-9"
_LOINC_BP_SYS = "8480-6"
_LOINC_BP_DIA = "8462-4"

# ----------------------------------------------------------------------- schema

VITALS_STRUCT = StructType(
    [
        StructField("heart_rate", DoubleType(), True),
        StructField("bp_systolic", DoubleType(), True),
        StructField("bp_diastolic", DoubleType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("o2_saturation", DoubleType(), True),
    ]
)

LAB_STRUCT = StructType(
    [
        StructField("name", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
    ]
)

IMAGING_STRUCT = StructType(
    [
        StructField("has_imaging", BooleanType(), True),
        StructField("modality", StringType(), True),
        StructField("body_site_display", StringType(), True),
        StructField("study_description", StringType(), True),
        StructField("study_date", DateType(), True),
        StructField("series_count", IntegerType(), True),
        StructField("dicom_binary_id", StringType(), True),
    ]
)

VERSIONS_STRUCT = StructType([StructField(name, LongType(), True) for name in SILVER_SOURCES])

GOLD_SCHEMA = StructType(
    [
        StructField("summary_id", StringType(), True),
        StructField("patient_id", StringType(), True),
        StructField("encounter_id", StringType(), True),
        StructField("patient_age", IntegerType(), True),
        StructField("patient_gender", StringType(), True),
        StructField("encounter_type", StringType(), True),
        StructField("encounter_date", DateType(), True),
        StructField("active_conditions", ArrayType(StringType()), True),
        StructField("active_medications", ArrayType(StringType()), True),
        StructField("recent_vitals", VITALS_STRUCT, True),
        StructField("recent_labs", ArrayType(LAB_STRUCT), True),
        StructField("procedures", ArrayType(StringType()), True),
        StructField("soap_note_text", StringType(), True),
        StructField("soap_note_id", StringType(), True),
        StructField("ecg_finding", StringType(), True),
        StructField("ecg_rhythm", StringType(), True),
        StructField("has_ecg", BooleanType(), True),
        StructField("imaging", IMAGING_STRUCT, True),
        StructField("has_genomics", BooleanType(), True),
        StructField("genomic_summary", StringType(), True),
        StructField("created_timestamp", TimestampType(), True),
        StructField("silver_versions", VERSIONS_STRUCT, True),
    ]
)

# --------------------------------------------------------------- public build


def build_encounter_summary(
    silver: dict[str, DataFrame],
    *,
    created_ts: datetime,
    silver_versions: dict[str, int] | None = None,
) -> DataFrame:
    """Build ``gold.encounter_summary`` from the Silver Spark DataFrames.

    Args:
        silver: ``table_name -> Spark DataFrame`` for every name in
            :data:`SILVER_SOURCES` (read via ``platform.read_silver_spark``).
        created_ts: Build timestamp stamped on every row.
        silver_versions: Optional Delta version per Silver table at read time;
            recorded verbatim in the ``silver_versions`` lineage struct.

    Returns:
        A Spark DataFrame with columns matching :data:`GOLD_SCHEMA`.
    """
    missing = [n for n in SILVER_SOURCES if n not in silver]
    if missing:
        msg = f"Missing Silver tables: {missing}"
        raise KeyError(msg)

    base = _encounter_base(silver["encounter"], silver["patient"])
    joined = (
        base.join(_active_conditions(base, silver["condition"]), on="encounter_id", how="left")
        .join(
            _active_medications(base, silver["medication_request"]),
            on="encounter_id",
            how="left",
        )
        .join(_procedures(silver["procedure"]), on="encounter_id", how="left")
        .join(_labs(silver["observation"]), on="encounter_id", how="left")
        .join(_vitals(silver["observation"]), on="encounter_id", how="left")
        .join(_soap(silver["soap_note"]), on="encounter_id", how="left")
        .join(_ecg(silver["ecg_metadata"]), on="encounter_id", how="left")
        .join(_imaging(silver["imaging_study"]), on="encounter_id", how="left")
        .join(_genomics(silver["genomic_report"]), on="encounter_id", how="left")
    )

    versions = silver_versions or {}
    versions_struct = F.struct(
        *[F.lit(versions.get(name)).cast(LongType()).alias(name) for name in SILVER_SOURCES]
    )

    return joined.select(
        _summary_id_expr(F.col("encounter_id")).alias("summary_id"),
        F.col("patient_id"),
        F.col("encounter_id"),
        F.col("patient_age"),
        F.col("patient_gender"),
        F.col("encounter_type"),
        F.col("encounter_date"),
        F.coalesce(F.col("active_conditions"), F.array().cast(ArrayType(StringType()))).alias(
            "active_conditions"
        ),
        F.coalesce(F.col("active_medications"), F.array().cast(ArrayType(StringType()))).alias(
            "active_medications"
        ),
        F.struct(
            F.col("v_heart_rate").alias("heart_rate"),
            F.col("v_bp_systolic").alias("bp_systolic"),
            F.col("v_bp_diastolic").alias("bp_diastolic"),
            F.col("v_temperature").alias("temperature"),
            F.col("v_o2_saturation").alias("o2_saturation"),
        ).alias("recent_vitals"),
        F.coalesce(F.col("recent_labs"), F.array().cast(ArrayType(LAB_STRUCT))).alias(
            "recent_labs"
        ),
        F.coalesce(F.col("procedures"), F.array().cast(ArrayType(StringType()))).alias("procedures"),
        F.col("soap_note_text"),
        F.col("soap_note_id"),
        F.col("ecg_finding"),
        F.col("ecg_rhythm"),
        F.coalesce(F.col("_has_ecg"), F.lit(False)).alias("has_ecg"),  # noqa: FBT003
        F.struct(
            F.coalesce(F.col("_has_imaging"), F.lit(False)).alias("has_imaging"),  # noqa: FBT003
            F.col("img_modality").alias("modality"),
            F.col("img_body_site_display").alias("body_site_display"),
            F.col("img_study_description").alias("study_description"),
            F.col("img_study_date").alias("study_date"),
            F.col("img_series_count").cast(IntegerType()).alias("series_count"),
            F.col("img_dicom_binary_id").alias("dicom_binary_id"),
        ).alias("imaging"),
        F.coalesce(F.col("_has_genomics"), F.lit(False)).alias("has_genomics"),  # noqa: FBT003
        F.col("genomic_summary"),
        F.lit(created_ts).cast(TimestampType()).alias("created_timestamp"),
        versions_struct.alias("silver_versions"),
    )


# --------------------------------------------------------------- internals


def _summary_id_expr(encounter_id):
    """Deterministic UUIDv5 over a fixed namespace + ``encounter_id`` (Spark expr).

    Spark's built-in ``uuid()`` is v4 random — not deterministic. Synthesize v5
    per RFC 4122: ``SHA1(namespace_bytes || name_bytes)``, then set the version
    nibble (position 13) to ``5`` and force the variant top two bits to ``10``
    (position 17 of the 32-char hex string).
    """
    ns_bin = F.unhex(F.lit(_SUMMARY_NAMESPACE.bytes.hex()))
    name_bin = F.encode(F.coalesce(encounter_id, F.lit("")), "UTF-8")
    hex32 = F.substring(F.lower(F.sha1(F.concat(ns_bin, name_bin))), 1, 32)

    # variant: (int(hex32[16], 16) & 0x3) | 0x8 → one hex char in {8, 9, a, b}.
    char17_int = F.conv(F.substring(hex32, 17, 1), 16, 10).cast("int")
    variant_hex = F.lower(
        F.conv(char17_int.bitwiseAND(F.lit(3)).bitwiseOR(F.lit(8)).cast("string"), 10, 16)
    )

    return F.concat_ws(
        "-",
        F.substring(hex32, 1, 8),
        F.substring(hex32, 9, 4),
        F.concat(F.lit("5"), F.substring(hex32, 14, 3)),
        F.concat(variant_hex, F.substring(hex32, 18, 3)),
        F.substring(hex32, 21, 12),
    )


def _encounter_base(encounter: DataFrame, patient: DataFrame) -> DataFrame:
    """Encounter + patient demographics with age-at-encounter + encounter_date."""
    enc = encounter.select(
        "encounter_id",
        "patient_id",
        F.col("type_display").alias("encounter_type"),
        F.to_date("start_date").alias("encounter_date"),
    )
    pat = patient.select("patient_id", "birth_date", F.col("gender").alias("patient_gender"))
    joined = enc.join(pat, on="patient_id", how="left")
    return joined.withColumn(
        "patient_age", _age_expr(F.col("birth_date"), F.col("encounter_date"))
    ).select(
        "encounter_id",
        "patient_id",
        "patient_age",
        "patient_gender",
        "encounter_type",
        "encounter_date",
    )


def _age_expr(birth, on):
    """Anniversary-based age in whole years (null if either date is null)."""
    not_yet = (F.month(on) < F.month(birth)) | (
        (F.month(on) == F.month(birth)) & (F.dayofmonth(on) < F.dayofmonth(birth))
    )
    return (F.year(on) - F.year(birth) - not_yet.cast(IntegerType())).cast(IntegerType())


def _active_conditions(base: DataFrame, condition: DataFrame) -> DataFrame:
    """Active conditions per encounter (problem list as of encounter_date, ADR-014)."""
    enc = base.select("encounter_id", "patient_id", "encounter_date")
    cond = condition.filter(F.col("display").isNotNull()).select(
        "patient_id",
        "display",
        F.to_date("onset_date").alias("onset"),
        F.to_date("abatement_date").alias("abatement"),
    )
    return (
        enc.join(cond, on="patient_id", how="inner")
        .filter(
            (F.col("onset").isNull() | (F.col("onset") <= F.col("encounter_date")))
            & (F.col("abatement").isNull() | (F.col("abatement") > F.col("encounter_date")))
        )
        .groupBy("encounter_id")
        .agg(F.sort_array(F.collect_set("display")).alias("active_conditions"))
    )


def _active_medications(base: DataFrame, medication: DataFrame) -> DataFrame:
    """Active medications per encounter, as of encounter_date (ADR-014)."""
    enc = base.select("encounter_id", "patient_id", "encounter_date")
    starts = (
        medication.filter(F.col("display").isNotNull() & (F.col("status") == "active"))
        .groupBy("patient_id", "display")
        .agg(F.min(F.to_date("authored_on")).alias("start"))
    )
    return (
        enc.join(starts, on="patient_id", how="inner")
        .filter(F.col("start").isNull() | (F.col("start") <= F.col("encounter_date")))
        .groupBy("encounter_id")
        .agg(F.sort_array(F.collect_set("display")).alias("active_medications"))
    )


def _procedures(procedure: DataFrame) -> DataFrame:
    return (
        procedure.filter(F.col("display").isNotNull())
        .groupBy("encounter_id")
        .agg(F.sort_array(F.collect_set("display")).alias("procedures"))
    )


def _labs(observation: DataFrame) -> DataFrame:
    """Per-encounter list of laboratory observations as {name, value, unit} structs."""
    labs = observation.filter(
        (F.col("category") == "laboratory") & F.col("display").isNotNull()
    ).select(
        "encounter_id",
        F.col("display").alias("name"),
        F.col("value").cast(DoubleType()).alias("value"),
        "unit",
    ).distinct()
    return labs.groupBy("encounter_id").agg(
        F.collect_list(F.struct("name", "value", "unit")).alias("recent_labs"),
    )


def _latest_for_codes(observation: DataFrame, codes: tuple[str, ...], alias: str) -> DataFrame:
    """Latest non-null ``value`` (by effective_date) for the given LOINC codes."""
    w = Window.partitionBy("encounter_id").orderBy(F.col("effective_date").desc_nulls_last())
    filtered = observation.filter(F.col("code").isin(list(codes)) & F.col("value").isNotNull())
    return (
        filtered.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select("encounter_id", F.col("value").cast(DoubleType()).alias(alias))
    )


def _vitals(observation: DataFrame) -> DataFrame:
    """Per-encounter structured vitals (heart rate, BP, temperature, SpO2)."""
    hr = _latest_for_codes(observation, (_LOINC_HEART_RATE,), "v_heart_rate")
    temp = _latest_for_codes(observation, (_LOINC_TEMPERATURE,), "v_temperature")
    o2 = _latest_for_codes(observation, _LOINC_O2_SAT, "v_o2_saturation")
    sys_, dia = _blood_pressure(observation)
    out = hr
    for frame in (temp, o2, sys_, dia):
        out = out.join(frame, on="encounter_id", how="outer")
    return out


def _blood_pressure(observation: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Latest systolic/diastolic values parsed from BP ``components_json``."""
    comp_schema = ArrayType(
        StructType(
            [
                StructField("code", StringType(), True),
                StructField("value", DoubleType(), True),
            ]
        )
    )

    bp = (
        observation.filter(
            (F.col("code") == _LOINC_BP) & F.col("components_json").isNotNull()
        )
        .withColumn("_comp", F.from_json("components_json", comp_schema))
        .withColumn("_c", F.explode("_comp"))
        .select(
            "encounter_id",
            "effective_date",
            F.col("_c.code").alias("_ccode"),
            F.col("_c.value").alias("_cval"),
        )
    )

    def _component(code: str, alias: str) -> DataFrame:
        w = Window.partitionBy("encounter_id").orderBy(F.col("effective_date").desc_nulls_last())
        return (
            bp.filter(F.col("_ccode") == code)
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .select("encounter_id", F.col("_cval").cast(DoubleType()).alias(alias))
        )

    return _component(_LOINC_BP_SYS, "v_bp_systolic"), _component(_LOINC_BP_DIA, "v_bp_diastolic")


def _soap(soap_note: DataFrame) -> DataFrame:
    w = Window.partitionBy("encounter_id").orderBy(F.col("note_date").desc_nulls_last())
    return (
        soap_note.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            "encounter_id",
            F.col("note_text").alias("soap_note_text"),
            F.col("note_id").alias("soap_note_id"),
        )
    )


def _ecg(ecg: DataFrame) -> DataFrame:
    w = Window.partitionBy("encounter_id").orderBy(F.col("report_date").desc_nulls_last())
    return (
        ecg.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            "encounter_id",
            # ``conclusion`` isn't in fabric.silver_ecg.SCHEMA — left null on this tier.
            F.lit(None).cast(StringType()).alias("ecg_finding"),
            F.col("rhythm").alias("ecg_rhythm"),
            F.lit(True).alias("_has_ecg"),  # noqa: FBT003
        )
    )


def _imaging(imaging: DataFrame) -> DataFrame:
    """Latest imaging-study metadata per encounter.

    Maps fabric.silver_imaging columns onto core's Gold imaging field names so
    the Gold schema stays cross-platform identical. ``dicom_binary_id`` is null
    on the Fabric tier (no pydicom enrichment).
    """
    w = Window.partitionBy("encounter_id").orderBy(F.col("started_date").desc_nulls_last())
    return (
        imaging.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            "encounter_id",
            F.col("modality").alias("img_modality"),
            F.col("body_site_display").alias("img_body_site_display"),
            F.col("description").alias("img_study_description"),
            F.to_date("started_date").alias("img_study_date"),
            F.col("series_count").alias("img_series_count"),
            F.lit(None).cast(StringType()).alias("img_dicom_binary_id"),
            F.lit(True).alias("_has_imaging"),  # noqa: FBT003
        )
    )


def _genomics(genomic: DataFrame) -> DataFrame:
    w = Window.partitionBy("encounter_id").orderBy(F.col("report_date").desc_nulls_last())
    return (
        genomic.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            "encounter_id",
            F.col("result_summary").alias("genomic_summary"),
            F.lit(True).alias("_has_genomics"),  # noqa: FBT003
        )
    )


# Silence unused-import linters on FloatType — exposed as a re-export for callers
# that want to construct compatible literals against this module's schema.
_ = FloatType
