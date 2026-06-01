"""Fabric Silver-table registry — ``table_name → (schema, pk, build_fn)``.

The Fabric counterpart to ``core.transforms.registry``. Notebooks and the Gold
layer iterate this dict; nothing else in fabric/ should hardcode table names.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from fabric.transforms import (
    silver_clinical,
    silver_ecg,
    silver_encounter,
    silver_genomics,
    silver_imaging,
    silver_patient,
    silver_soap_notes,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


@dataclass(frozen=True)
class SilverTable:
    """Registry entry for one Silver table."""

    name: str
    primary_key: str
    schema: StructType
    build: Callable[[DataFrame, datetime], DataFrame]


REGISTRY: dict[str, SilverTable] = {
    silver_patient.TABLE_NAME: SilverTable(
        name=silver_patient.TABLE_NAME,
        primary_key=silver_patient.PRIMARY_KEY,
        schema=silver_patient.SCHEMA,
        build=silver_patient.build,
    ),
    silver_encounter.TABLE_NAME: SilverTable(
        name=silver_encounter.TABLE_NAME,
        primary_key=silver_encounter.PRIMARY_KEY,
        schema=silver_encounter.SCHEMA,
        build=silver_encounter.build,
    ),
    silver_clinical.CONDITION_TABLE_NAME: SilverTable(
        name=silver_clinical.CONDITION_TABLE_NAME,
        primary_key=silver_clinical.CONDITION_PRIMARY_KEY,
        schema=silver_clinical.CONDITION_SCHEMA,
        build=silver_clinical.build_condition,
    ),
    silver_clinical.OBSERVATION_TABLE_NAME: SilverTable(
        name=silver_clinical.OBSERVATION_TABLE_NAME,
        primary_key=silver_clinical.OBSERVATION_PRIMARY_KEY,
        schema=silver_clinical.OBSERVATION_SCHEMA,
        build=silver_clinical.build_observation,
    ),
    silver_clinical.MEDICATION_REQUEST_TABLE_NAME: SilverTable(
        name=silver_clinical.MEDICATION_REQUEST_TABLE_NAME,
        primary_key=silver_clinical.MEDICATION_REQUEST_PRIMARY_KEY,
        schema=silver_clinical.MEDICATION_REQUEST_SCHEMA,
        build=silver_clinical.build_medication_request,
    ),
    silver_clinical.PROCEDURE_TABLE_NAME: SilverTable(
        name=silver_clinical.PROCEDURE_TABLE_NAME,
        primary_key=silver_clinical.PROCEDURE_PRIMARY_KEY,
        schema=silver_clinical.PROCEDURE_SCHEMA,
        build=silver_clinical.build_procedure,
    ),
    silver_soap_notes.TABLE_NAME: SilverTable(
        name=silver_soap_notes.TABLE_NAME,
        primary_key=silver_soap_notes.PRIMARY_KEY,
        schema=silver_soap_notes.SCHEMA,
        build=silver_soap_notes.build,
    ),
    silver_ecg.TABLE_NAME: SilverTable(
        name=silver_ecg.TABLE_NAME,
        primary_key=silver_ecg.PRIMARY_KEY,
        schema=silver_ecg.SCHEMA,
        build=silver_ecg.build,
    ),
    silver_genomics.TABLE_NAME: SilverTable(
        name=silver_genomics.TABLE_NAME,
        primary_key=silver_genomics.PRIMARY_KEY,
        schema=silver_genomics.SCHEMA,
        build=silver_genomics.build,
    ),
    silver_imaging.TABLE_NAME: SilverTable(
        name=silver_imaging.TABLE_NAME,
        primary_key=silver_imaging.PRIMARY_KEY,
        schema=silver_imaging.SCHEMA,
        build=silver_imaging.build,
    ),
}
