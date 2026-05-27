"""Silver transform: ``silver.patient`` (spec §5.4).

Pure transform — takes parsed patient record dicts and returns a typed Arrow table.
"""

from __future__ import annotations

from datetime import datetime

import pyarrow as pa

from local.transforms.schema_utils import DATE, TS, build_arrow_table, dedup_by_key

PRIMARY_KEY = "patient_id"

SCHEMA = pa.schema(
    [
        ("patient_id", pa.string()),
        ("birth_date", DATE),
        ("gender", pa.string()),
        ("race", pa.string()),
        ("ethnicity", pa.string()),
        ("state", pa.string()),
        ("city", pa.string()),
        ("zip", pa.string()),
        ("deceased", pa.bool_()),
        ("deceased_date", TS),
        ("source_file", pa.string()),
        ("ingest_timestamp", TS),
    ]
)


def build_silver_patient(records: list[dict], ingest_ts: datetime) -> pa.Table:
    """Build the ``silver.patient`` table from parsed patient records."""
    return build_arrow_table(dedup_by_key(records, PRIMARY_KEY), SCHEMA, ingest_ts)
