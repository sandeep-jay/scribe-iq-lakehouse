"""Generate docs/DATA_DICTIONARY.md from the Silver registry + validation rules.

Generated-first documentation (ADR-011): the data dictionary is derived from the
single source of truth — the Arrow schemas in ``local/transforms/registry.py`` and the
rules in ``local/validation/schema_registry.py`` — so it can never drift from the code.

Usage:
    python scripts/gen_data_dictionary.py            # write docs/DATA_DICTIONARY.md
    python scripts/gen_data_dictionary.py --check     # exit 1 if the file is stale (CI)

``render()`` is pure (returns the markdown string) so tests can assert the committed
file is current — see tests/test_docs_generated.py.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pyarrow as pa

# Make the repo root importable whether run as a file or a module.
# Script lives at core/scripts/gen_data_dictionary.py — climb 3 levels for repo root.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.transforms.registry import SILVER_TABLES  # noqa: E402
from core.validation.schema_registry import VALIDATION_RULES  # noqa: E402
from core.validation.validate import INGEST_LOG_SCHEMA  # noqa: E402

OUTPUT_PATH = _REPO_ROOT / "docs" / "DATA_DICTIONARY.md"

_HEADER = """# Data Dictionary — Silver layer

> **Generated file — do not edit by hand.** Regenerate with
> `python scripts/gen_data_dictionary.py` whenever a Silver schema or validation rule
> changes. Source of truth: `local/transforms/registry.py` (schemas) and
> `local/validation/schema_registry.py` (rules). See ADR-009 / ADR-011.

All Silver tables are Delta tables with Change Data Feed enabled
(`delta.enableChangeDataFeed = true`). `source_file` and `ingest_timestamp` are
pipeline-added provenance columns present on every table.
"""

# Curated, stable notes for cross-cutting columns (safe to hand-maintain — rarely change).
_COLUMN_NOTES: dict[str, str] = {
    "source_file": "Pipeline provenance — Bronze bundle the row came from",
    "ingest_timestamp": "Pipeline provenance — UTC run timestamp",
    "components_json": "Observation components as JSON, e.g. BP systolic/diastolic (ADR-009)",
    "data_limitation": "Always populated — Synthea inheritance limitation note (ADR-007)",
}


def _type_str(dtype: pa.DataType) -> str:
    """Render an Arrow data type as a human-readable string."""
    if pa.types.is_timestamp(dtype):
        tz = f", {dtype.tz}" if dtype.tz else ""
        return f"timestamp[{dtype.unit}{tz}]"
    if pa.types.is_date(dtype):
        return "date"
    if pa.types.is_floating(dtype):
        return "double"
    if pa.types.is_integer(dtype):
        return str(dtype)
    if pa.types.is_boolean(dtype):
        return "boolean"
    if pa.types.is_string(dtype):
        return "string"
    return str(dtype)


def _table_section(
    title: str, schema: pa.Schema, primary_key: str | None, rules: dict
) -> list[str]:
    """Render one table's heading, validation summary, and column table."""
    required = set(rules.get("required_non_null", []))
    lines = [f"## {title}", ""]

    meta = []
    if primary_key:
        meta.append(f"**Primary key:** `{primary_key}`")
    meta.append(f"**Columns:** {len(schema)}")
    lines.append(" · ".join(meta))
    lines.append("")

    rule_bits = _rule_summary(rules)
    if rule_bits:
        lines.append("**Validation:** " + "; ".join(rule_bits))
        lines.append("")

    lines.append("| Column | Type | Required | Notes |")
    lines.append("|--------|------|----------|-------|")
    for field in schema:
        note = _COLUMN_NOTES.get(field.name, "")
        if field.name == primary_key:
            note = "Primary key" + (f"; {note}" if note else "")
        req = "✓" if field.name in required else ""
        lines.append(f"| `{field.name}` | {_type_str(field.type)} | {req} | {note} |")
    lines.append("")
    return lines


def _rule_summary(rules: dict) -> list[str]:
    """Compact human summary of a table's validation rules."""
    bits: list[str] = []
    if "min_rows" in rules:
        bits.append(f"min rows {rules['min_rows']}")
    if rules.get("unique_keys"):
        bits.append("unique " + ", ".join(f"`{k}`" for k in rules["unique_keys"]))
    if rules.get("required_section_flags"):
        pct = rules.get("required_sections_pct", 0)
        flags = ", ".join(rules["required_section_flags"])
        bits.append(f"≥{pct:.0%} rows with [{flags}]")
    if "min_char_count" in rules:
        bits.append(
            f"≤{rules.get('max_short_pct', 0):.0%} notes under {rules['min_char_count']} chars"
        )
    for col, (lo, hi) in rules.get("numeric_ranges", {}).items():
        bits.append(f"`{col}` in [{lo}, {hi}]")
    return bits


def render() -> str:
    """Render the full DATA_DICTIONARY.md content as a deterministic string."""
    lines = [_HEADER]
    for name, spec in SILVER_TABLES.items():
        lines += _table_section(
            f"silver.{name}", spec.schema, spec.primary_key, VALIDATION_RULES.get(name, {})
        )
    # ingest_log is a real Silver table but lives outside the build registry (audit only).
    lines += _table_section("silver.ingest_log (validation audit)", INGEST_LOG_SCHEMA, None, {})
    return "\n".join(lines).rstrip() + "\n"


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. ``--check`` verifies the file is current without writing."""
    import argparse

    ap = argparse.ArgumentParser(description="Generate the Silver data dictionary")
    ap.add_argument("--check", action="store_true", help="Exit 1 if the file is out of date")
    args = ap.parse_args(argv)

    content = render()
    if args.check:
        current = OUTPUT_PATH.read_text() if OUTPUT_PATH.exists() else ""
        if current != content:
            print(f"{OUTPUT_PATH} is out of date — run: python scripts/gen_data_dictionary.py")
            return 1
        print(f"{OUTPUT_PATH} is up to date.")
        return 0

    OUTPUT_PATH.write_text(content)
    print(f"Wrote {OUTPUT_PATH} ({len(SILVER_TABLES) + 1} tables).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
