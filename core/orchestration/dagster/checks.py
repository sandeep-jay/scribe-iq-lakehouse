"""Asset checks wrapping :func:`core.validation.validate.validate_table` (ADR-016).

Each Silver asset carries one ``@asset_check`` that re-reads the materialized
table via the platform and runs the *same* validation rules
(:data:`core.validation.schema_registry.VALIDATION_RULES`) the CLI runs at the
end of :func:`core.surfaces.cli.pipeline.run_pipeline`. Checks surface as pass/fail badges on
each Silver asset in the Dagster UI — first-class observability that the
imperative pipeline only exposed by tailing ``silver.ingest_log``.

The checks complement (do not replace) ``silver.ingest_log``: the log is still
appended on every CLI run for Fabric parity. Both views agree by construction
because both call the same ``validate_table``.
"""

from dagster import AssetCheckExecutionContext, AssetCheckResult, MetadataValue, asset_check

from core.orchestration.dagster.resources import PlatformResource
from core.transforms.registry import SILVER_TABLES
from core.validation.validate import ValidationResult, validate_table


def _render_rules_table(result: ValidationResult) -> str:
    """Render every rule's outcome as a Markdown table for the Dagster UI.

    One row per :class:`core.validation.validate.CheckOutcome` — passing rules
    included, not just failures. This is the difference between "1/1 Passed"
    (the bare badge) and a real audit trail you can read from the UI.
    """
    if not result.checks:
        return "_No rules registered for this table._"
    rows = ["| Rule | Result | Detail |", "|---|---|---|"]
    for c in result.checks:
        status = "pass" if c.passed else "**FAIL**"
        rows.append(f"| `{c.name}` | {status} | {c.detail} |")
    return "\n".join(rows)


def _make_check(table_name: str):
    """Build one ``@asset_check`` for a Silver table.

    A factory is used (rather than 10 hand-written decorators) so the asset-check
    set stays in lockstep with :data:`core.transforms.registry.SILVER_TABLES`.

    Args:
        table_name: Silver table / asset key.

    Returns:
        The decorated check function, ready to be passed to ``Definitions(asset_checks=...)``.
    """

    @asset_check(asset=table_name, name=f"{table_name}_validate")
    def _check(context: AssetCheckExecutionContext, platform: PlatformResource) -> AssetCheckResult:
        p = platform.create()
        table = p.read_silver(table_name)
        result = validate_table(table_name, table)
        passed = sum(1 for c in result.checks if c.passed)
        total = len(result.checks)
        context.log.info(
            "Check %s: %d/%d rules passed, rows=%d, failed=%s",
            table_name,
            passed,
            total,
            result.row_count,
            result.failed_checks or "—",
        )
        return AssetCheckResult(
            passed=result.passed,
            metadata={
                "row_count": result.row_count,
                "rules_total": total,
                "rules_passed": passed,
                "rules_failed": total - passed,
                "rules": MetadataValue.md(_render_rules_table(result)),
                "failed_checks": ",".join(result.failed_checks) or "none",
            },
        )

    return _check


#: One asset check per Silver table — keeps the validation surface in sync with the registry.
silver_asset_checks = [_make_check(name) for name in SILVER_TABLES]
