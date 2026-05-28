"""Silver validation — run quality checks and emit ``silver.ingest_log`` rows (spec §5.6).

``validate_table`` evaluates a PyArrow table against the rules in
:data:`~core.validation.schema_registry.VALIDATION_RULES` and returns a
:class:`ValidationResult`. ``results_to_arrow`` turns a batch of results into the
``silver.ingest_log`` table the platform writes for lineage/audit.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc

from core.transforms.schema_utils import TS
from core.validation.schema_registry import VALIDATION_RULES

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
class CheckOutcome:
    """One rule's outcome — name, pass/fail, and a human-readable detail string.

    Recorded for *every* rule the validator runs (passing and failing alike) so
    the Dagster asset-check metadata can render a full rule-by-rule table, not
    just a count of failures.
    """

    name: str
    passed: bool
    detail: str


@dataclass
class ValidationResult:
    """Outcome of validating one Silver table.

    Two views over the same data:
    - ``checks`` — every rule that ran, with its outcome and detail. Used by the
      Dagster ``@asset_check`` to render a full breakdown in the UI.
    - ``failed_checks`` — legacy summary of failure strings only. Still written
      to ``silver.ingest_log`` and consumed by the CLI pipeline alert path.
    """

    table: str
    row_count: int
    passed: bool = True
    failed_checks: list[str] = field(default_factory=list)
    checks: list[CheckOutcome] = field(default_factory=list)

    def fail(self, check: str, name: str | None = None) -> None:
        """Record a failed check.

        ``check`` is the legacy detail string (e.g. ``"nulls_in:patient_id(5)"``);
        ``name`` is an optional short rule name for the UI. Defaults to the
        detail string itself when not provided, preserving prior behavior.
        """
        self.passed = False
        self.failed_checks.append(check)
        self.checks.append(CheckOutcome(name=name or check, passed=False, detail=check))

    def ok(self, name: str, detail: str) -> None:
        """Record a passing check — name + detail line for the UI."""
        self.checks.append(CheckOutcome(name=name, passed=True, detail=detail))


def validate_table(table_name: str, data: pa.Table, rules: dict | None = None) -> ValidationResult:
    """Validate a Silver table against its rules.

    Args:
        table_name: Logical table name (key into ``VALIDATION_RULES``).
        data: The built Silver table.
        rules: Override rules; defaults to the registered rules for ``table_name``.

    Returns:
        A :class:`ValidationResult` with pass/fail status, every rule's outcome
        (``checks``), and the legacy failure strings (``failed_checks``).
    """
    rules = rules if rules is not None else VALIDATION_RULES.get(table_name, {})
    result = ValidationResult(table=table_name, row_count=data.num_rows)

    min_rows = rules.get("min_rows", 0)
    if data.num_rows < min_rows:
        result.fail(f"min_rows<{min_rows}(={data.num_rows})", name=f"min_rows>={min_rows}")
    elif "min_rows" in rules:
        result.ok(f"min_rows>={min_rows}", f"{data.num_rows} rows (>= {min_rows})")

    for column in rules.get("required_non_null", []):
        if column not in data.column_names:
            continue
        nulls = data[column].null_count
        if nulls > 0:
            result.fail(f"nulls_in:{column}({nulls})", name=f"non_null:{column}")
        else:
            result.ok(f"non_null:{column}", f"0 nulls / {data.num_rows} rows")

    for column in rules.get("unique_keys", []):
        if column not in data.column_names or not data.num_rows:
            continue
        distinct = pc.count_distinct(data[column]).as_py()
        if distinct != data.num_rows:
            result.fail(f"non_unique:{column}({distinct}/{data.num_rows})", name=f"unique:{column}")
        else:
            result.ok(f"unique:{column}", f"{distinct}/{data.num_rows} distinct")

    _check_soap_quality(data, rules, result)
    _check_numeric_ranges(data, rules, result)
    return result


def _check_soap_quality(data: pa.Table, rules: dict, result: ValidationResult) -> None:
    """Apply SOAP-note-specific checks (short-text rate, section completeness)."""
    if data.num_rows == 0:
        return

    if "min_char_count" in rules and "char_count" in data.column_names:
        min_chars = rules["min_char_count"]
        max_short = rules.get("max_short_pct", 1.0)
        short = pc.sum(pc.less(data["char_count"], min_chars)).as_py() or 0
        short_pct = short / data.num_rows
        name = f"short_notes_pct<={max_short:.0%}"
        if short_pct > max_short:
            result.fail(f"short_notes_pct>{max_short}(={short_pct:.2f})", name=name)
        else:
            result.ok(name, f"{short_pct:.1%} short (< {min_chars} chars), {short}/{data.num_rows}")

    flags = rules.get("required_section_flags")
    if flags and all(f in data.column_names for f in flags):
        min_pct = rules.get("required_sections_pct", 0.0)
        complete = data[flags[0]]
        for flag in flags[1:]:
            complete = pc.and_(complete, data[flag])
        pct = pc.sum(complete).as_py() / data.num_rows
        name = f"sections_pct>={min_pct:.0%}"
        if pct < min_pct:
            result.fail(f"sections_pct<{min_pct}(={pct:.2f})", name=name)
        else:
            result.ok(name, f"{pct:.1%} of rows have all {len(flags)} sections")


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
        name = f"range:{column}∈[{lo},{hi}]"
        if out_of_range > 0:
            result.fail(f"out_of_range:{column}({out_of_range} not in [{lo},{hi}])", name=name)
        else:
            result.ok(name, f"all {len(non_null)} non-null values within [{lo}, {hi}]")


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
