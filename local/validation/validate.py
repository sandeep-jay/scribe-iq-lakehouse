"""Silver validation — run quality checks and emit ``silver.ingest_log`` rows (spec §5.6).

``validate_table`` evaluates a PyArrow table against the rules in
:data:`~local.validation.schema_registry.VALIDATION_RULES` and returns a
:class:`ValidationResult`. ``results_to_arrow`` turns a batch of results into the
``silver.ingest_log`` table the platform writes for lineage/audit.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc

from local.transforms.schema_utils import TS
from local.validation.schema_registry import VALIDATION_RULES

INGEST_LOG_SCHEMA = pa.schema(
    [
        ("table", pa.string()),
        ("row_count", pa.int64()),
        ("passed", pa.bool_()),
        ("failed_checks", pa.string()),
        ("ingest_timestamp", TS),
    ]
)


@dataclass
class ValidationResult:
    """Outcome of validating one Silver table."""

    table: str
    row_count: int
    passed: bool = True
    failed_checks: list[str] = field(default_factory=list)

    def fail(self, check: str) -> None:
        """Record a failed check."""
        self.passed = False
        self.failed_checks.append(check)


def validate_table(table_name: str, data: pa.Table, rules: dict | None = None) -> ValidationResult:
    """Validate a Silver table against its rules.

    Args:
        table_name: Logical table name (key into ``VALIDATION_RULES``).
        data: The built Silver table.
        rules: Override rules; defaults to the registered rules for ``table_name``.

    Returns:
        A :class:`ValidationResult` with pass/fail status and any failed check names.
    """
    rules = rules if rules is not None else VALIDATION_RULES.get(table_name, {})
    result = ValidationResult(table=table_name, row_count=data.num_rows)

    if data.num_rows < rules.get("min_rows", 0):
        result.fail(f"min_rows<{rules['min_rows']}(={data.num_rows})")

    for column in rules.get("required_non_null", []):
        if column in data.column_names and data[column].null_count > 0:
            result.fail(f"nulls_in:{column}({data[column].null_count})")

    for column in rules.get("unique_keys", []):
        if column in data.column_names and data.num_rows:
            distinct = pc.count_distinct(data[column]).as_py()
            if distinct != data.num_rows:
                result.fail(f"non_unique:{column}({distinct}/{data.num_rows})")

    _check_soap_quality(data, rules, result)
    _check_numeric_ranges(data, rules, result)
    return result


def _check_soap_quality(data: pa.Table, rules: dict, result: ValidationResult) -> None:
    """Apply SOAP-note-specific checks (short-text rate, section completeness)."""
    if data.num_rows == 0:
        return

    if "min_char_count" in rules and "char_count" in data.column_names:
        short = pc.sum(pc.less(data["char_count"], rules["min_char_count"])).as_py() or 0
        short_pct = short / data.num_rows
        if short_pct > rules.get("max_short_pct", 1.0):
            result.fail(f"short_notes_pct>{rules['max_short_pct']}(={short_pct:.2f})")

    flags = rules.get("required_section_flags")
    if flags and all(f in data.column_names for f in flags):
        complete = data[flags[0]]
        for flag in flags[1:]:
            complete = pc.and_(complete, data[flag])
        pct = pc.sum(complete).as_py() / data.num_rows
        if pct < rules.get("required_sections_pct", 0.0):
            result.fail(f"sections_pct<{rules['required_sections_pct']}(={pct:.2f})")


def _check_numeric_ranges(data: pa.Table, rules: dict, result: ValidationResult) -> None:
    """Flag non-null values that fall outside physiological bounds."""
    for column, (lo, hi) in rules.get("numeric_ranges", {}).items():
        if column not in data.column_names:
            continue
        col = data[column]
        non_null = pc.drop_null(col)
        if len(non_null) == 0:
            continue
        out_of_range = pc.sum(pc.or_(pc.less(non_null, lo), pc.greater(non_null, hi))).as_py() or 0
        if out_of_range > 0:
            result.fail(f"out_of_range:{column}({out_of_range} not in [{lo},{hi}])")


def results_to_arrow(results: list[ValidationResult], ingest_ts: datetime) -> pa.Table:
    """Convert validation results into the ``silver.ingest_log`` Arrow table."""
    rows = {
        "table": [r.table for r in results],
        "row_count": [r.row_count for r in results],
        "passed": [r.passed for r in results],
        "failed_checks": [",".join(r.failed_checks) for r in results],
        "ingest_timestamp": [ingest_ts] * len(results),
    }
    arrays = [pa.array(rows[f.name], type=f.type) for f in INGEST_LOG_SCHEMA]
    return pa.Table.from_arrays(arrays, schema=INGEST_LOG_SCHEMA)
