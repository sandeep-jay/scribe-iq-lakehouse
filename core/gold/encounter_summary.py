"""Gold transform: ``gold.encounter_summary`` (spec §5.4, §5.7 corpus contract).

Denormalizes the Silver tables into one row per encounter — the handoff corpus that
feeds the Ollama generation pipeline, scribe-iq (RAG) and clinical-bert-pipeline (NLP).

Pure transform (ADR-002/004): it receives the Silver tables as ``pa.Table`` inputs and
returns a ``pa.Table``. No platform, Spark, or Delta imports and no file paths — the
caller reads Silver via the platform and writes the result via the platform. Polars is
used purely as an in-process join/aggregation engine (it is engine-agnostic, not a
storage/cloud dependency); the output schema is defined explicitly, never inferred.

Grain: one row per ``silver.encounter`` row. ``active_conditions``/``active_medications``
are the patient's problem list *as of the encounter date* (onset/abatement and authored
gated — ADR-014), not just what was recorded at that encounter. Observations/procedures
are aggregated to the encounter; SOAP/ECG/imaging/genomic context is the latest record
linked to it. Optional clinical context is null (or an empty list) when absent — the
corpus contract (§5.7) requires consumers to handle that.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from datetime import datetime

import polars as pl
import pyarrow as pa

from core.transforms.schema_utils import TS

# --------------------------------------------------------------------- contract

#: Corpus contract version (semver). Bump on breaking schema/semantics changes — both
#: scribe-iq and clinical-bert-pipeline pin against this (see docs/CORPUS_CONTRACT.md, §5.7).
#: 1.1.0 — active_conditions/active_medications became problem-list-as-of-date (ADR-014).
CONTRACT_VERSION = "1.1.0"

#: Silver tables consumed, in the order recorded in the ``silver_versions`` lineage struct.
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

#: Always present for every encounter (corpus contract §5.7). List fields may be empty.
REQUIRED_FIELDS: tuple[str, ...] = (
    "summary_id",
    "patient_id",
    "encounter_id",
    "patient_age",
    "patient_gender",
    "encounter_type",
    "encounter_date",
    "active_conditions",
    "active_medications",
)

#: Present only when the underlying Silver data exists (else null / empty / false).
OPTIONAL_FIELDS: tuple[str, ...] = (
    "recent_vitals",
    "recent_labs",
    "procedures",
    "soap_note_text",
    "soap_note_id",
    "ecg_finding",
    "ecg_rhythm",
    "has_ecg",
    "imaging",
    "has_genomics",
    "genomic_summary",
    "silver_versions",
    "created_timestamp",
)

#: Stable namespace so ``summary_id`` is deterministic per encounter (idempotent rebuilds).
_SUMMARY_NAMESPACE = uuid.UUID("5c0d9b1e-1f7a-5a3c-9b2e-7d6f4a2c8e10")

# LOINC codes for the structured vitals surfaced in ``recent_vitals``.
_LOINC_HEART_RATE = "8867-4"
_LOINC_TEMPERATURE = "8310-5"
_LOINC_O2_SAT = ("2708-6", "59408-5")  # SpO2 — two LOINC variants seen in Coherent
_LOINC_BLOOD_PRESSURE = "85354-9"  # component-based: systolic 8480-6 / diastolic 8462-4
_LOINC_BP_SYSTOLIC = "8480-6"
_LOINC_BP_DIASTOLIC = "8462-4"

# --------------------------------------------------------------------- schema

_VITALS_STRUCT = pa.struct(
    [
        ("heart_rate", pa.float64()),
        ("bp_systolic", pa.float64()),
        ("bp_diastolic", pa.float64()),
        ("temperature", pa.float64()),
        ("o2_saturation", pa.float64()),
    ]
)

_LAB_STRUCT = pa.struct(
    [
        ("name", pa.string()),
        ("value", pa.float64()),
        ("unit", pa.string()),
    ]
)

_IMAGING_STRUCT = pa.struct(
    [
        ("has_imaging", pa.bool_()),
        ("modality", pa.string()),
        ("body_site_display", pa.string()),
        ("study_description", pa.string()),
        ("study_date", pa.date32()),
        ("series_count", pa.int32()),
        ("dicom_binary_id", pa.string()),
    ]
)

#: ``silver_versions`` lineage: Delta version of each Silver source at build time.
_VERSIONS_STRUCT = pa.struct([(name, pa.int64()) for name in SILVER_SOURCES])

GOLD_SCHEMA = pa.schema(
    [
        ("summary_id", pa.string()),
        ("patient_id", pa.string()),
        ("encounter_id", pa.string()),
        ("patient_age", pa.int32()),
        ("patient_gender", pa.string()),
        ("encounter_type", pa.string()),
        ("encounter_date", pa.date32()),
        ("active_conditions", pa.list_(pa.string())),
        ("active_medications", pa.list_(pa.string())),
        ("recent_vitals", _VITALS_STRUCT),
        ("recent_labs", pa.list_(_LAB_STRUCT)),
        ("procedures", pa.list_(pa.string())),
        ("soap_note_text", pa.string()),
        ("soap_note_id", pa.string()),
        ("ecg_finding", pa.string()),
        ("ecg_rhythm", pa.string()),
        ("has_ecg", pa.bool_()),
        ("imaging", _IMAGING_STRUCT),
        ("has_genomics", pa.bool_()),
        ("genomic_summary", pa.string()),
        ("created_timestamp", TS),
        ("silver_versions", _VERSIONS_STRUCT),
    ]
)

TABLE_NAME = "encounter_summary"
PRIMARY_KEY = "encounter_id"


# --------------------------------------------------------------- build


def build_encounter_summary(
    silver: Mapping[str, pa.Table],
    *,
    created_ts: datetime,
    silver_versions: Mapping[str, int] | None = None,
) -> pa.Table:
    """Denormalize the Silver tables into ``gold.encounter_summary``.

    Args:
        silver: Mapping of Silver table name -> ``pa.Table`` (all of
            :data:`SILVER_SOURCES`). The caller reads these via the platform.
        created_ts: Build timestamp stamped on every row (``created_timestamp``).
        silver_versions: Optional Delta version per Silver table at read time,
            recorded verbatim in the ``silver_versions`` lineage struct. Missing
            entries are stored as null.

    Returns:
        A ``pa.Table`` matching :data:`GOLD_SCHEMA` exactly — one row per encounter.

    Raises:
        KeyError: If a required Silver table is missing from ``silver``.
    """
    frames = {name: pl.from_arrow(silver[name]) for name in SILVER_SOURCES}

    base = _encounter_base(frames["encounter"], frames["patient"])
    joined = (
        base.join(_active_conditions(base, frames["condition"]), on="encounter_id", how="left")
        .join(
            _active_medications(base, frames["medication_request"]), on="encounter_id", how="left"
        )
        .join(_procedures(frames["procedure"]), on="encounter_id", how="left")
        .join(_labs(frames["observation"]), on="encounter_id", how="left")
        .join(_vitals(frames["observation"]), on="encounter_id", how="left")
        .join(_soap(frames["soap_note"]), on="encounter_id", how="left")
        .join(_ecg(frames["ecg_metadata"]), on="encounter_id", how="left")
        .join(_imaging(frames["imaging_study"]), on="encounter_id", how="left")
        .join(_genomics(frames["genomic_report"]), on="encounter_id", how="left")
    )

    versions = {name: (silver_versions or {}).get(name) for name in SILVER_SOURCES}
    rows = [_assemble_row(r, created_ts, versions) for r in joined.iter_rows(named=True)]
    arrays = [pa.array([row[f.name] for row in rows], type=f.type) for f in GOLD_SCHEMA]
    return pa.Table.from_arrays(arrays, schema=GOLD_SCHEMA)


def _summary_id(encounter_id: str) -> str:
    """Deterministic summary UUID derived from the encounter id (idempotent rebuilds)."""
    return str(uuid.uuid5(_SUMMARY_NAMESPACE, encounter_id or ""))


def _encounter_base(encounter: pl.DataFrame, patient: pl.DataFrame) -> pl.DataFrame:
    """Encounter + patient demographics, with age-at-encounter and encounter_date."""
    enc = encounter.select(
        "encounter_id",
        "patient_id",
        pl.col("type_display").alias("encounter_type"),
        "start_date",
    )
    pat = patient.select("patient_id", "birth_date", "gender")
    return (
        enc.join(pat, on="patient_id", how="left")
        .with_columns(encounter_date=pl.col("start_date").dt.date())
        .with_columns(
            patient_age=_age_expr(pl.col("birth_date"), pl.col("encounter_date")),
            patient_gender=pl.col("gender"),
        )
        .select(
            "encounter_id",
            "patient_id",
            "patient_age",
            "patient_gender",
            "encounter_type",
            "encounter_date",
        )
    )


def _age_expr(birth: pl.Expr, on: pl.Expr) -> pl.Expr:
    """Anniversary-based age in whole years (null if either date is null)."""
    not_yet = (on.dt.month() < birth.dt.month()) | (
        (on.dt.month() == birth.dt.month()) & (on.dt.day() < birth.dt.day())
    )
    return (on.dt.year() - birth.dt.year() - not_yet.cast(pl.Int32)).cast(pl.Int32)


def _active_conditions(base: pl.DataFrame, condition: pl.DataFrame) -> pl.DataFrame:
    """Patient problem list active *as of each encounter date* (ADR-014).

    A condition is active at an encounter if it started on/before the encounter date and
    had not resolved (abated) by then — so a chronic condition recorded once carries
    forward to every later encounter, not just the one where it was first recorded.
    """
    enc = base.select("encounter_id", "patient_id", "encounter_date")
    cond = condition.filter(pl.col("display").is_not_null()).select(
        "patient_id",
        "display",
        pl.col("onset_date").dt.date().alias("onset"),
        pl.col("abatement_date").dt.date().alias("abatement"),
    )
    return (
        enc.join(cond, on="patient_id", how="inner")
        .filter(
            (pl.col("onset").is_null() | (pl.col("onset") <= pl.col("encounter_date")))
            & (pl.col("abatement").is_null() | (pl.col("abatement") > pl.col("encounter_date")))
        )
        .group_by("encounter_id")
        .agg(pl.col("display").unique().sort().alias("active_conditions"))
    )


def _active_medications(base: pl.DataFrame, medication: pl.DataFrame) -> pl.DataFrame:
    """Active medications as of each encounter date (ADR-014).

    FHIR has no medication stop date, so this is a ``status == "active"`` approximation: a
    med authored on/before the encounter date and still marked active carries forward to
    that encounter. Distinct meds are pre-aggregated to their earliest start to keep the
    patient-level join small. Historical point-in-time for stopped meds is not recoverable
    from FHIR alone (see CORPUS_CONTRACT).
    """
    enc = base.select("encounter_id", "patient_id", "encounter_date")
    starts = (
        medication.filter(pl.col("display").is_not_null() & (pl.col("status") == "active"))
        .group_by("patient_id", "display")
        .agg(pl.col("authored_on").dt.date().min().alias("start"))
    )
    return (
        enc.join(starts, on="patient_id", how="inner")
        .filter(pl.col("start").is_null() | (pl.col("start") <= pl.col("encounter_date")))
        .group_by("encounter_id")
        .agg(pl.col("display").unique().sort().alias("active_medications"))
    )


def _procedures(procedure: pl.DataFrame) -> pl.DataFrame:
    """Distinct display names of procedures performed at the encounter."""
    return (
        procedure.filter(pl.col("display").is_not_null())
        .group_by("encounter_id")
        .agg(pl.col("display").unique().sort().alias("procedures"))
    )


def _labs(observation: pl.DataFrame) -> pl.DataFrame:
    """Laboratory observations as a per-encounter list of {name, value, unit} structs."""
    return (
        observation.filter((pl.col("category") == "laboratory") & pl.col("display").is_not_null())
        .select("encounter_id", "display", "value", "unit")
        .unique()
        .sort("encounter_id", "display", "value")
        .with_columns(
            lab=pl.struct(
                name=pl.col("display"),
                value=pl.col("value"),
                unit=pl.col("unit"),
            )
        )
        .group_by("encounter_id")
        .agg(pl.col("lab").alias("recent_labs"))
    )


def _latest_value(observation: pl.DataFrame, codes: tuple[str, ...], alias: str) -> pl.DataFrame:
    """Latest non-null ``value`` (by effective_date) for the given LOINC code(s)."""
    return (
        observation.filter(pl.col("code").is_in(codes) & pl.col("value").is_not_null())
        .sort("effective_date")
        .group_by("encounter_id")
        .agg(pl.col("value").last().alias(alias))
    )


def _vitals(observation: pl.DataFrame) -> pl.DataFrame:
    """Per-encounter structured vitals (HR, BP systolic/diastolic, temp, SpO2)."""
    hr = _latest_value(observation, (_LOINC_HEART_RATE,), "v_heart_rate")
    temp = _latest_value(observation, (_LOINC_TEMPERATURE,), "v_temperature")
    o2 = _latest_value(observation, _LOINC_O2_SAT, "v_o2_saturation")
    sys, dia = _blood_pressure(observation)
    out = hr
    for frame in (temp, o2, sys, dia):
        out = out.join(frame, on="encounter_id", how="full", coalesce=True)
    return out


def _blood_pressure(observation: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Latest systolic/diastolic values parsed from BP ``components_json``."""
    comp_dtype = pl.List(pl.Struct({"code": pl.Utf8, "value": pl.Float64}))
    long = (
        observation.filter(
            (pl.col("code") == _LOINC_BLOOD_PRESSURE) & pl.col("components_json").is_not_null()
        )
        .sort("effective_date")
        .with_columns(_comp=pl.col("components_json").str.json_decode(comp_dtype))
        .explode("_comp")
        .with_columns(
            _ccode=pl.col("_comp").struct.field("code"),
            _cval=pl.col("_comp").struct.field("value"),
        )
    )

    def _component(code: str, alias: str) -> pl.DataFrame:
        return (
            long.filter(pl.col("_ccode") == code)
            .group_by("encounter_id")
            .agg(pl.col("_cval").last().alias(alias))
        )

    return _component(_LOINC_BP_SYSTOLIC, "v_bp_systolic"), _component(
        _LOINC_BP_DIASTOLIC, "v_bp_diastolic"
    )


def _soap(soap_note: pl.DataFrame) -> pl.DataFrame:
    """Latest SOAP note text + id per encounter."""
    return (
        soap_note.sort("note_date")
        .group_by("encounter_id")
        .agg(
            pl.col("note_text").last().alias("soap_note_text"),
            pl.col("note_id").last().alias("soap_note_id"),
        )
    )


def _ecg(ecg: pl.DataFrame) -> pl.DataFrame:
    """Latest ECG finding/rhythm per encounter, with a presence flag."""
    return (
        ecg.sort("report_date")
        .group_by("encounter_id")
        .agg(
            pl.col("conclusion").last().alias("ecg_finding"),
            pl.col("rhythm").last().alias("ecg_rhythm"),
            pl.lit(True).alias("_has_ecg"),
        )
    )


def _imaging(imaging: pl.DataFrame) -> pl.DataFrame:
    """Latest imaging study metadata per encounter, with a presence flag."""
    return (
        imaging.sort("started_date")
        .group_by("encounter_id")
        .agg(
            pl.col("modality").last(),
            pl.col("body_site_display").last(),
            pl.col("study_description").last(),
            pl.col("study_date").last(),
            pl.col("series_count").last(),
            pl.col("dicom_binary_id").last(),
            pl.lit(True).alias("_has_imaging"),
        )
    )


def _genomics(genomic: pl.DataFrame) -> pl.DataFrame:
    """Latest genomic report summary per encounter, with a presence flag."""
    return (
        genomic.sort("report_date")
        .group_by("encounter_id")
        .agg(
            pl.col("result_summary").last().alias("genomic_summary"),
            pl.lit(True).alias("_has_genomics"),
        )
    )


def _assemble_row(r: Mapping, created_ts: datetime, versions: Mapping[str, int | None]) -> dict:
    """Build one Gold row dict from a joined Polars record, applying contract defaults."""
    return {
        "summary_id": _summary_id(r["encounter_id"]),
        "patient_id": r["patient_id"],
        "encounter_id": r["encounter_id"],
        "patient_age": r["patient_age"],
        "patient_gender": r["patient_gender"],
        "encounter_type": r["encounter_type"],
        "encounter_date": r["encounter_date"],
        "active_conditions": r.get("active_conditions") or [],
        "active_medications": r.get("active_medications") or [],
        "recent_vitals": {
            "heart_rate": r.get("v_heart_rate"),
            "bp_systolic": r.get("v_bp_systolic"),
            "bp_diastolic": r.get("v_bp_diastolic"),
            "temperature": r.get("v_temperature"),
            "o2_saturation": r.get("v_o2_saturation"),
        },
        "recent_labs": r.get("recent_labs") or [],
        "procedures": r.get("procedures") or [],
        "soap_note_text": r.get("soap_note_text"),
        "soap_note_id": r.get("soap_note_id"),
        "ecg_finding": r.get("ecg_finding"),
        "ecg_rhythm": r.get("ecg_rhythm"),
        "has_ecg": bool(r.get("_has_ecg")),
        "imaging": {
            "has_imaging": bool(r.get("_has_imaging")),
            "modality": r.get("modality"),
            "body_site_display": r.get("body_site_display"),
            "study_description": r.get("study_description"),
            "study_date": r.get("study_date"),
            "series_count": r.get("series_count"),
            "dicom_binary_id": r.get("dicom_binary_id"),
        },
        "has_genomics": bool(r.get("_has_genomics")),
        "genomic_summary": r.get("genomic_summary"),
        "created_timestamp": created_ts,
        "silver_versions": {name: versions.get(name) for name in SILVER_SOURCES},
    }
