"""Arrow schema + type-coercion helpers shared by the Silver transforms (ADR-004).

Transforms define an explicit ``pa.schema`` and hand a list of record dicts to
:func:`build_arrow_table`, which coerces each value to its declared Arrow type.
Coercion is driven by the field type itself, so adding a column never needs a
per-column rule. This module imports only ``pyarrow`` + ``dateutil`` — no platform,
Spark, or Delta imports (ADR-002).
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pyarrow as pa
from dateutil import parser as _dtp

# Canonical Arrow types reused across Silver schemas.
TS = pa.timestamp("us", tz="UTC")
DATE = pa.date32()

INGEST_TS_FIELD = "ingest_timestamp"
SOURCE_FILE_FIELD = "source_file"


def parse_date(value: Any) -> date | None:
    """Parse a FHIR date string to a ``date``; return ``None`` on failure."""
    if value in (None, ""):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    try:
        return _dtp.parse(str(value)).date()
    except (ValueError, OverflowError, TypeError):
        return None


def parse_timestamp(value: Any) -> datetime | None:
    """Parse a FHIR datetime string to a UTC-aware ``datetime``; ``None`` on failure.

    Naive timestamps are assumed UTC; offset-aware timestamps are converted to UTC
    so the column has a single, comparable timezone.
    """
    if value in (None, ""):
        return None
    try:
        dt = _dtp.parse(str(value))
    except (ValueError, OverflowError, TypeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _to_int(value: Any) -> int | None:
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _coerce(value: Any, dtype: pa.DataType) -> Any:
    """Coerce a Python value to match an Arrow field type. ``None`` stays ``None``."""
    if value is None:
        return None
    if pa.types.is_timestamp(dtype):
        return parse_timestamp(value)
    if pa.types.is_date(dtype):
        return parse_date(value)
    if pa.types.is_floating(dtype):
        return _to_float(value)
    if pa.types.is_integer(dtype):
        return _to_int(value)
    if pa.types.is_boolean(dtype):
        return bool(value)
    if pa.types.is_string(dtype):
        return str(value)
    return value


def build_arrow_table(
    records: list[dict],
    schema: pa.Schema,
    ingest_ts: datetime,
) -> pa.Table:
    """Build a typed Arrow table from record dicts against an explicit schema.

    Each value is coerced to its field's Arrow type. The ``ingest_timestamp``
    column (if present in the schema) is filled with ``ingest_ts`` for every row;
    all other columns are read from each record by field name (missing -> null).

    Args:
        records: Flat record dicts (already stamped with ``source_file`` if used).
        schema: The target Arrow schema (explicit, never inferred — ADR-004).
        ingest_ts: Pipeline ingest timestamp applied to all rows.

    Returns:
        A ``pa.Table`` matching ``schema`` exactly, with zero rows handled cleanly.
    """
    columns: dict[str, list] = {field.name: [] for field in schema}
    for record in records:
        for field in schema:
            if field.name == INGEST_TS_FIELD:
                value: Any = ingest_ts
            else:
                value = _coerce(record.get(field.name), field.type)
            columns[field.name].append(value)
    arrays = [pa.array(columns[f.name], type=f.type) for f in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


def dedup_by_key(records: list[dict], key: str) -> list[dict]:
    """Drop duplicate records by primary key, keeping the last occurrence.

    Synthea is one bundle per patient so cross-bundle key collisions are rare, but
    re-running a cohort can re-emit the same row; last-write-wins matches the MERGE
    upsert semantics used downstream.
    """
    seen: dict[str, dict] = {}
    for record in records:
        seen[record.get(key, "")] = record
    return list(seen.values())
