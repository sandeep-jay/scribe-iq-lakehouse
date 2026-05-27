"""Silver transform: ``silver.ecg_metadata`` (spec §5.4).

Metadata only — the Binary waveform signal is never decoded here (roadmap Phase 3).
"""

from __future__ import annotations

from datetime import datetime

import pyarrow as pa

from local.transforms.schema_utils import TS, build_arrow_table, dedup_by_key

PRIMARY_KEY = "ecg_id"

SCHEMA = pa.schema(
    [
        ("ecg_id", pa.string()),
        ("patient_id", pa.string()),
        ("encounter_id", pa.string()),
        ("report_date", TS),
        ("status", pa.string()),
        ("conclusion", pa.string()),
        ("rhythm", pa.string()),
        ("heart_rate_bpm", pa.int32()),
        ("pr_interval_ms", pa.int32()),
        ("qrs_duration_ms", pa.int32()),
        ("has_waveform", pa.bool_()),
        ("waveform_binary_id", pa.string()),
        ("source_file", pa.string()),
        ("ingest_timestamp", TS),
    ]
)


def build_silver_ecg(records: list[dict], ingest_ts: datetime) -> pa.Table:
    """Build the ``silver.ecg_metadata`` table from ECG report records."""
    return build_arrow_table(dedup_by_key(records, PRIMARY_KEY), SCHEMA, ingest_ts)
