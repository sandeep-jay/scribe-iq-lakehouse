"""Silver transforms: condition, observation, medication_request, procedure (spec §5.4).

Grouped per the spec's ``silver_clinical`` module. Each entity has its own explicit
schema, primary key, and ``build_*`` function returning a typed Arrow table.

Observation ``component`` arrays (e.g. systolic/diastolic for blood pressure) are
serialized to a ``components_json`` string column. This keeps the Delta schema flat
and portable while preserving the structured values for Gold to parse later.
"""

from __future__ import annotations

import json
from datetime import datetime

import pyarrow as pa

from core.transforms.schema_utils import TS, build_arrow_table, dedup_by_key

# --------------------------------------------------------------------- condition

CONDITION_KEY = "condition_id"
CONDITION_SCHEMA = pa.schema(
    [
        ("condition_id", pa.string()),
        ("patient_id", pa.string()),
        ("encounter_id", pa.string()),
        ("code", pa.string()),
        ("display", pa.string()),
        ("clinical_status", pa.string()),
        ("onset_date", TS),
        ("abatement_date", TS),
        ("recorded_date", TS),
        ("source_file", pa.string()),
        ("ingest_timestamp", TS),
    ]
)


def build_silver_condition(records: list[dict], ingest_ts: datetime) -> pa.Table:
    """Build the ``silver.condition`` table."""
    return build_arrow_table(dedup_by_key(records, CONDITION_KEY), CONDITION_SCHEMA, ingest_ts)


# ------------------------------------------------------------------- observation

OBSERVATION_KEY = "observation_id"
OBSERVATION_SCHEMA = pa.schema(
    [
        ("observation_id", pa.string()),
        ("patient_id", pa.string()),
        ("encounter_id", pa.string()),
        ("code", pa.string()),
        ("display", pa.string()),
        ("category", pa.string()),
        ("value", pa.float64()),
        ("unit", pa.string()),
        ("value_string", pa.string()),
        ("components_json", pa.string()),
        ("effective_date", TS),
        ("source_file", pa.string()),
        ("ingest_timestamp", TS),
    ]
)


def build_silver_observation(records: list[dict], ingest_ts: datetime) -> pa.Table:
    """Build the ``silver.observation`` table, serializing components to JSON."""
    prepared = []
    for record in dedup_by_key(records, OBSERVATION_KEY):
        row = dict(record)
        components = row.get("components") or []
        row["components_json"] = json.dumps(components) if components else None
        prepared.append(row)
    return build_arrow_table(prepared, OBSERVATION_SCHEMA, ingest_ts)


# ------------------------------------------------------------ medication_request

MEDICATION_KEY = "medication_request_id"
MEDICATION_SCHEMA = pa.schema(
    [
        ("medication_request_id", pa.string()),
        ("patient_id", pa.string()),
        ("encounter_id", pa.string()),
        ("code", pa.string()),
        ("display", pa.string()),
        ("status", pa.string()),
        ("intent", pa.string()),
        ("authored_on", TS),
        ("dosage_text", pa.string()),
        ("source_file", pa.string()),
        ("ingest_timestamp", TS),
    ]
)


def build_silver_medication_request(records: list[dict], ingest_ts: datetime) -> pa.Table:
    """Build the ``silver.medication_request`` table."""
    return build_arrow_table(dedup_by_key(records, MEDICATION_KEY), MEDICATION_SCHEMA, ingest_ts)


# --------------------------------------------------------------------- procedure

PROCEDURE_KEY = "procedure_id"
PROCEDURE_SCHEMA = pa.schema(
    [
        ("procedure_id", pa.string()),
        ("patient_id", pa.string()),
        ("encounter_id", pa.string()),
        ("code", pa.string()),
        ("display", pa.string()),
        ("status", pa.string()),
        ("performed_start", TS),
        ("performed_end", TS),
        ("source_file", pa.string()),
        ("ingest_timestamp", TS),
    ]
)


def build_silver_procedure(records: list[dict], ingest_ts: datetime) -> pa.Table:
    """Build the ``silver.procedure`` table."""
    return build_arrow_table(dedup_by_key(records, PROCEDURE_KEY), PROCEDURE_SCHEMA, ingest_ts)
