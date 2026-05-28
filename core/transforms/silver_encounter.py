"""Silver transform: ``silver.encounter`` (spec §5.4)."""

from __future__ import annotations

from datetime import datetime

import pyarrow as pa

from core.transforms.schema_utils import TS, build_arrow_table, dedup_by_key

PRIMARY_KEY = "encounter_id"

SCHEMA = pa.schema(
    [
        ("encounter_id", pa.string()),
        ("patient_id", pa.string()),
        ("type_code", pa.string()),
        ("type_display", pa.string()),
        ("class_code", pa.string()),
        ("start_date", TS),
        ("end_date", TS),
        ("status", pa.string()),
        ("provider_id", pa.string()),
        ("reason_code", pa.string()),
        ("reason_display", pa.string()),
        ("source_file", pa.string()),
        ("ingest_timestamp", TS),
    ]
)


def build_silver_encounter(records: list[dict], ingest_ts: datetime) -> pa.Table:
    """Build the ``silver.encounter`` table from parsed encounter records."""
    return build_arrow_table(dedup_by_key(records, PRIMARY_KEY), SCHEMA, ingest_ts)
