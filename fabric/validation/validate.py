"""Spark-native Silver validation → ``silver.ingest_log`` row (ADR-022).

One round-trip per table: build a single ``.agg()`` over the Spark DataFrame
that computes every metric the rules need (row count, null counts, distinct
counts, short-note fraction, section completeness, out-of-range counts) in
one pass, then materialize once with ``.first()`` and compare to thresholds
in plain Python. Same rule grammar + same ingest_log schema as core/validation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from fabric.validation.schema_registry import VALIDATION_RULES

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row, SparkSession

INGEST_LOG_SCHEMA = StructType(
    [
        StructField("table", StringType(), True),
        StructField("row_count", LongType(), True),
        StructField("passed", BooleanType(), True),
        StructField("failed_checks", StringType(), True),
        StructField("ingest_timestamp", TimestampType(), True),
    ]
)


@dataclass
class CheckOutcome:
    """One rule's outcome — name, pass/fail, and a human-readable detail string."""

    name: str
    passed: bool
    detail: str


@dataclass
class ValidationResult:
    """Outcome of validating one Silver Spark DataFrame."""

    table: str
    row_count: int
    passed: bool = True
    failed_checks: list[str] = field(default_factory=list)
    checks: list[CheckOutcome] = field(default_factory=list)

    def fail(self, check: str, name: str | None = None) -> None:
        self.passed = False
        self.failed_checks.append(check)
        self.checks.append(CheckOutcome(name=name or check, passed=False, detail=check))

    def ok(self, name: str, detail: str) -> None:
        self.checks.append(CheckOutcome(name=name, passed=True, detail=detail))


def validate_table(
    table_name: str,
    df: DataFrame,
    rules: dict | None = None,
) -> ValidationResult:
    """Validate a Silver Spark DataFrame against its rules.

    Args:
        table_name: Logical table name (key into :data:`VALIDATION_RULES`).
        df: The built Silver DataFrame.
        rules: Override rules; defaults to ``VALIDATION_RULES[table_name]``.

    Returns:
        :class:`ValidationResult` with pass/fail status and every rule's outcome.
    """
    rules = rules if rules is not None else VALIDATION_RULES.get(table_name, {})
    metrics = _collect_metrics(df, rules, table_name)
    row_count = int(metrics["row_count"] or 0)
    result = ValidationResult(table=table_name, row_count=row_count)

    _check_min_rows(rules, row_count, result)
    _check_non_null(rules, metrics, row_count, result)
    _check_unique(rules, metrics, row_count, result)
    _check_soap_quality(rules, metrics, row_count, result)
    _check_numeric_ranges(rules, metrics, result)
    return result


def _collect_metrics(df: DataFrame, rules: dict, table_name: str) -> Row:
    """Run a single .agg() that computes everything the rules need.

    Returns a Spark Row with named fields like:
        row_count, null_<col>, distinct_<col>, short_count, sections_complete,
        oor_<col>, nn_<col>  (nn_ = non-null count for range checks).

    Falls back to per-rule aggregations if the column isn't present in the
    DataFrame so callers can pass partial schemas safely (e.g. tests).
    """
    columns = set(df.columns)
    exprs = [F.count(F.lit(1)).alias("row_count")]

    for col in rules.get("required_non_null", []):
        if col in columns:
            exprs.append(F.sum(F.col(col).isNull().cast("long")).alias(f"null_{col}"))

    for col in rules.get("unique_keys", []):
        if col in columns:
            exprs.append(F.countDistinct(F.col(col)).alias(f"distinct_{col}"))

    if "min_char_count" in rules and "char_count" in columns:
        exprs.append(
            F.sum((F.col("char_count") < rules["min_char_count"]).cast("long")).alias(
                "short_count"
            )
        )

    flags = rules.get("required_section_flags") or []
    if flags and all(f in columns for f in flags):
        combined = F.col(flags[0])
        for f in flags[1:]:
            combined = combined & F.col(f)
        exprs.append(F.sum(combined.cast("long")).alias("sections_complete"))

    for col, (lo, hi) in rules.get("numeric_ranges", {}).items():
        if col in columns:
            non_null = F.col(col).isNotNull()
            exprs.append(F.sum(non_null.cast("long")).alias(f"nn_{col}"))
            oor = non_null & ((F.col(col) < F.lit(lo)) | (F.col(col) > F.lit(hi)))
            exprs.append(F.sum(oor.cast("long")).alias(f"oor_{col}"))

    return df.agg(*exprs).first()


# ----------------------------------------------------------------------- checks


def _check_min_rows(rules: dict, row_count: int, result: ValidationResult) -> None:
    if "min_rows" not in rules:
        return
    min_rows = rules["min_rows"]
    if row_count < min_rows:
        result.fail(f"min_rows<{min_rows}(={row_count})", name=f"min_rows>={min_rows}")
    else:
        result.ok(f"min_rows>={min_rows}", f"{row_count} rows (>= {min_rows})")


def _check_non_null(rules: dict, m, row_count: int, result: ValidationResult) -> None:  # noqa: ARG001
    for col in rules.get("required_non_null", []):
        key = f"null_{col}"
        if key not in m.asDict():
            continue
        nulls = int(m[key] or 0)
        if nulls > 0:
            result.fail(f"nulls_in:{col}({nulls})", name=f"non_null:{col}")
        else:
            result.ok(f"non_null:{col}", f"0 nulls / {row_count} rows")


def _check_unique(rules: dict, m, row_count: int, result: ValidationResult) -> None:
    if row_count == 0:
        return
    for col in rules.get("unique_keys", []):
        key = f"distinct_{col}"
        if key not in m.asDict():
            continue
        distinct = int(m[key] or 0)
        if distinct != row_count:
            result.fail(
                f"non_unique:{col}({distinct}/{row_count})", name=f"unique:{col}"
            )
        else:
            result.ok(f"unique:{col}", f"{distinct}/{row_count} distinct")


def _check_soap_quality(rules: dict, m, row_count: int, result: ValidationResult) -> None:
    if row_count == 0:
        return
    fields = m.asDict()

    if "min_char_count" in rules and "short_count" in fields:
        min_chars = rules["min_char_count"]
        max_short = rules.get("max_short_pct", 1.0)
        short = int(fields["short_count"] or 0)
        short_pct = short / row_count
        name = f"short_notes_pct<={max_short:.0%}"
        if short_pct > max_short:
            result.fail(f"short_notes_pct>{max_short}(={short_pct:.2f})", name=name)
        else:
            result.ok(
                name,
                f"{short_pct:.1%} short (< {min_chars} chars), {short}/{row_count}",
            )

    flags = rules.get("required_section_flags") or []
    if flags and "sections_complete" in fields:
        min_pct = rules.get("required_sections_pct", 0.0)
        complete = int(fields["sections_complete"] or 0)
        pct = complete / row_count
        name = f"sections_pct>={min_pct:.0%}"
        if pct < min_pct:
            result.fail(f"sections_pct<{min_pct}(={pct:.2f})", name=name)
        else:
            result.ok(name, f"{pct:.1%} of rows have all {len(flags)} sections")


def _check_numeric_ranges(rules: dict, m, result: ValidationResult) -> None:
    fields = m.asDict()
    for col, (lo, hi) in rules.get("numeric_ranges", {}).items():
        nn_key = f"nn_{col}"
        oor_key = f"oor_{col}"
        if nn_key not in fields or oor_key not in fields:
            continue
        non_null = int(fields[nn_key] or 0)
        if non_null == 0:
            continue
        out_of_range = int(fields[oor_key] or 0)
        name = f"range:{col}∈[{lo},{hi}]"
        if out_of_range > 0:
            result.fail(
                f"out_of_range:{col}({out_of_range} not in [{lo},{hi}])", name=name
            )
        else:
            result.ok(name, f"all {non_null} non-null values within [{lo}, {hi}]")


# ---------------------------------------------------------------- ingest log


def results_to_dataframe(
    spark: SparkSession,
    results: list[ValidationResult],
    ingest_ts: datetime,
) -> DataFrame:
    """Convert validation results into the ``silver.ingest_log`` Spark DataFrame."""
    rows = [
        (
            r.table,
            r.row_count,
            r.passed,
            ",".join(r.failed_checks),
            ingest_ts,
        )
        for r in results
    ]
    return spark.createDataFrame(rows, schema=INGEST_LOG_SCHEMA)
