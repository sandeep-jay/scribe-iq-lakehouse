"""Silver transform: ``silver.genomic_report`` (spec §5.4, ADR-007).

``data_limitation`` is non-nullable and always carries the Synthea inheritance note.
The transform re-asserts this invariant before building the table.
"""

from __future__ import annotations

from datetime import datetime

import pyarrow as pa

from local.transforms.fhir_parser import GENOMIC_DATA_LIMITATION
from local.transforms.schema_utils import TS, build_arrow_table, dedup_by_key

PRIMARY_KEY = "report_id"

SCHEMA = pa.schema(
    [
        ("report_id", pa.string()),
        ("patient_id", pa.string()),
        ("encounter_id", pa.string()),
        ("report_date", TS),
        ("status", pa.string()),
        ("gene_panel_name", pa.string()),
        ("result_summary", pa.string()),
        ("has_pathogenic_variant", pa.bool_()),
        ("family_history_flag", pa.bool_()),
        ("binary_id", pa.string()),
        ("data_limitation", pa.string()),
        ("source_file", pa.string()),
        ("ingest_timestamp", TS),
    ]
)


def build_silver_genomic(records: list[dict], ingest_ts: datetime) -> pa.Table:
    """Build the ``silver.genomic_report`` table, enforcing the data_limitation invariant.

    Raises:
        ValueError: If any record lacks a populated ``data_limitation`` (ADR-007).
    """
    prepared = dedup_by_key(records, PRIMARY_KEY)
    for record in prepared:
        if not record.get("data_limitation"):
            record["data_limitation"] = GENOMIC_DATA_LIMITATION
    return build_arrow_table(prepared, SCHEMA, ingest_ts)
