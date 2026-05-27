"""Tests for local.transforms.schema_utils — date/type coercion and table building."""

from datetime import UTC, date, datetime

import pyarrow as pa

from local.transforms.schema_utils import (
    DATE,
    TS,
    build_arrow_table,
    dedup_by_key,
    parse_date,
    parse_timestamp,
)


def test_parse_date_variants():
    assert parse_date("1980-07-15") == date(1980, 7, 15)
    assert parse_date("2021-03-15T09:00:00-04:00") == date(2021, 3, 15)
    assert parse_date(None) is None
    assert parse_date("") is None
    assert parse_date("not-a-date") is None


def test_parse_timestamp_naive_assumed_utc():
    ts = parse_timestamp("2021-03-15T09:00:00")
    assert ts == datetime(2021, 3, 15, 9, 0, tzinfo=UTC)


def test_parse_timestamp_offset_converted_to_utc():
    ts = parse_timestamp("2021-03-15T09:00:00-04:00")
    assert ts == datetime(2021, 3, 15, 13, 0, tzinfo=UTC)


def test_parse_timestamp_bad_value():
    assert parse_timestamp("garbage") is None
    assert parse_timestamp(None) is None


def test_build_arrow_table_coerces_and_fills_ingest_ts():
    schema = pa.schema(
        [
            ("id", pa.string()),
            ("when", TS),
            ("dob", DATE),
            ("score", pa.float64()),
            ("count", pa.int32()),
            ("flag", pa.bool_()),
            ("ingest_timestamp", TS),
        ]
    )
    ts = datetime(2026, 1, 1, tzinfo=UTC)
    records = [
        {
            "id": 123,
            "when": "2021-03-15T09:00:00-04:00",
            "dob": "1980-07-15",
            "score": "3.5",
            "count": "7",
            "flag": 1,
        },
        {"id": "x"},  # missing fields -> nulls
    ]
    table = build_arrow_table(records, schema, ts)
    assert table.schema == schema
    rows = table.to_pylist()
    assert rows[0]["id"] == "123"  # coerced to string
    assert rows[0]["dob"] == date(1980, 7, 15)
    assert rows[0]["score"] == 3.5
    assert rows[0]["count"] == 7
    assert rows[0]["flag"] is True
    assert rows[0]["ingest_timestamp"] == ts
    assert rows[1]["when"] is None
    assert rows[1]["ingest_timestamp"] == ts  # filled even when record omits it


def test_build_arrow_table_empty_records():
    schema = pa.schema([("id", pa.string()), ("ingest_timestamp", TS)])
    table = build_arrow_table([], schema, datetime.now(UTC))
    assert table.num_rows == 0
    assert table.schema == schema


def test_dedup_by_key_keeps_last():
    records = [{"k": "a", "v": 1}, {"k": "a", "v": 2}, {"k": "b", "v": 3}]
    out = {r["k"]: r["v"] for r in dedup_by_key(records, "k")}
    assert out == {"a": 2, "b": 3}
