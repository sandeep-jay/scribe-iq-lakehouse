"""Silver transform: ``silver.imaging_study`` (spec §5.4, ADR-006).

FHIR ImagingStudy metadata plus optional DICOM header fields. Pixel data is never
loaded (``stop_before_pixels`` lives in the parser). ``dicom_extracted`` records
whether the pydicom header pass ran for each row.
"""

from __future__ import annotations

from datetime import datetime

import pyarrow as pa

from core.transforms.schema_utils import DATE, TS, build_arrow_table, dedup_by_key

PRIMARY_KEY = "study_id"

SCHEMA = pa.schema(
    [
        ("study_id", pa.string()),
        ("patient_id", pa.string()),
        ("encounter_id", pa.string()),
        ("started_date", TS),
        ("status", pa.string()),
        ("modality", pa.string()),
        ("body_site", pa.string()),
        ("body_site_display", pa.string()),
        ("series_count", pa.int32()),
        ("instance_count", pa.int32()),
        ("study_description", pa.string()),
        ("series_description", pa.string()),
        ("study_date", DATE),
        ("manufacturer", pa.string()),
        ("magnetic_field_strength", pa.float64()),
        ("slice_thickness_mm", pa.float64()),
        ("rows", pa.int32()),
        ("columns", pa.int32()),
        ("dicom_binary_id", pa.string()),
        ("dicom_extracted", pa.bool_()),
        ("source_file", pa.string()),
        ("ingest_timestamp", TS),
    ]
)


def build_silver_imaging(records: list[dict], ingest_ts: datetime) -> pa.Table:
    """Build the ``silver.imaging_study`` table from imaging metadata records."""
    return build_arrow_table(dedup_by_key(records, PRIMARY_KEY), SCHEMA, ingest_ts)
