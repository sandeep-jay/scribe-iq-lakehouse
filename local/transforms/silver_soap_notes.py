"""Silver transform: ``silver.soap_note`` (spec §5.4, ADR-005).

The heuristic ``section_headers`` list from the parser is dropped here — Silver keeps
the four S/O/A/P boolean flags plus counts; raw headers stay debug-only.
"""

from __future__ import annotations

from datetime import datetime

import pyarrow as pa

from local.transforms.schema_utils import TS, build_arrow_table, dedup_by_key

PRIMARY_KEY = "note_id"

SCHEMA = pa.schema(
    [
        ("note_id", pa.string()),
        ("patient_id", pa.string()),
        ("encounter_id", pa.string()),
        ("note_date", TS),
        ("note_text", pa.string()),
        ("has_subjective", pa.bool_()),
        ("has_objective", pa.bool_()),
        ("has_assessment", pa.bool_()),
        ("has_plan", pa.bool_()),
        ("char_count", pa.int32()),
        ("word_count", pa.int32()),
        ("binary_id", pa.string()),
        ("source_file", pa.string()),
        ("ingest_timestamp", TS),
    ]
)


def build_silver_soap_note(records: list[dict], ingest_ts: datetime) -> pa.Table:
    """Build the ``silver.soap_note`` table from decoded SOAP-note records."""
    return build_arrow_table(dedup_by_key(records, PRIMARY_KEY), SCHEMA, ingest_ts)
