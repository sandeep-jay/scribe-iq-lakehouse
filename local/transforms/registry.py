"""Registry binding each Silver table to its schema, primary key, and builder.

Keys match the logical table names emitted by ``FHIRBundleParser.parse_bundle``.
The pipeline iterates this registry to build and write every Silver table, and the
platform uses ``primary_key`` as the MERGE/upsert predicate column. Centralizing the
mapping keeps the pipeline declarative and the platform free of table-specific logic.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

import pyarrow as pa

from local.transforms import (
    silver_clinical,
    silver_ecg,
    silver_encounter,
    silver_genomics,
    silver_imaging,
    silver_patient,
    silver_soap_notes,
)


@dataclass(frozen=True)
class TableSpec:
    """How to build and key one Silver table."""

    name: str
    primary_key: str
    schema: pa.Schema
    build: Callable[[list[dict], datetime], pa.Table]


SILVER_TABLES: dict[str, TableSpec] = {
    "patient": TableSpec(
        "patient",
        silver_patient.PRIMARY_KEY,
        silver_patient.SCHEMA,
        silver_patient.build_silver_patient,
    ),
    "encounter": TableSpec(
        "encounter",
        silver_encounter.PRIMARY_KEY,
        silver_encounter.SCHEMA,
        silver_encounter.build_silver_encounter,
    ),
    "condition": TableSpec(
        "condition",
        silver_clinical.CONDITION_KEY,
        silver_clinical.CONDITION_SCHEMA,
        silver_clinical.build_silver_condition,
    ),
    "observation": TableSpec(
        "observation",
        silver_clinical.OBSERVATION_KEY,
        silver_clinical.OBSERVATION_SCHEMA,
        silver_clinical.build_silver_observation,
    ),
    "medication_request": TableSpec(
        "medication_request",
        silver_clinical.MEDICATION_KEY,
        silver_clinical.MEDICATION_SCHEMA,
        silver_clinical.build_silver_medication_request,
    ),
    "procedure": TableSpec(
        "procedure",
        silver_clinical.PROCEDURE_KEY,
        silver_clinical.PROCEDURE_SCHEMA,
        silver_clinical.build_silver_procedure,
    ),
    "soap_note": TableSpec(
        "soap_note",
        silver_soap_notes.PRIMARY_KEY,
        silver_soap_notes.SCHEMA,
        silver_soap_notes.build_silver_soap_note,
    ),
    "ecg_metadata": TableSpec(
        "ecg_metadata",
        silver_ecg.PRIMARY_KEY,
        silver_ecg.SCHEMA,
        silver_ecg.build_silver_ecg,
    ),
    "imaging_study": TableSpec(
        "imaging_study",
        silver_imaging.PRIMARY_KEY,
        silver_imaging.SCHEMA,
        silver_imaging.build_silver_imaging,
    ),
    "genomic_report": TableSpec(
        "genomic_report",
        silver_genomics.PRIMARY_KEY,
        silver_genomics.SCHEMA,
        silver_genomics.build_silver_genomic,
    ),
}

#: table name -> primary key column, for platform MERGE predicates.
SILVER_PRIMARY_KEYS: dict[str, str] = {
    name: spec.primary_key for name, spec in SILVER_TABLES.items()
}
