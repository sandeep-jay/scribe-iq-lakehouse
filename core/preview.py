"""Formatting helpers for rendering data shape as Markdown / plain text.

Used by both the Dagster asset-graph metadata (so clicking an asset shows what
it produced, not just a green badge) and by ``scripts/demo_walkthrough.py``
(so the CLI walkthrough renders the same data shape).

Pure Python; no Dagster, no ``rich``, no platform imports. Callers choose how
to display the strings returned (Dagster ``MetadataValue.md``, ``rich.Panel``,
plain ``print``, etc.). Designed to fit in the asset metadata panel without
truncation — sample sizes are kept small.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

import pyarrow as pa

#: Default sample size — chosen to fit Dagster's metadata panel comfortably.
DEFAULT_SAMPLE_ROWS = 5
#: Maximum cell width for sample tables; longer strings are truncated with `…`.
MAX_CELL_CHARS = 60


def _truncate(value: Any, max_chars: int = MAX_CELL_CHARS) -> str:
    """Render any value as a Markdown-safe one-line string, truncated if long."""
    if value is None:
        return "_null_"
    s = str(value).replace("|", "\\|").replace("\n", " ")
    return s if len(s) <= max_chars else s[: max_chars - 1] + "…"


def schema_md(table: pa.Table) -> str:
    """Render a PyArrow schema as a Markdown table — ``| column | type |``.

    Args:
        table: Any Arrow table.

    Returns:
        Three-row-minimum Markdown table; nested types are stringified.
    """
    rows = ["| Column | Type |", "|---|---|"]
    for field in table.schema:
        rows.append(f"| `{field.name}` | `{field.type}` |")
    return "\n".join(rows)


def sample_md(table: pa.Table, n: int = DEFAULT_SAMPLE_ROWS) -> str:
    """Render the first ``n`` rows of an Arrow table as a Markdown table.

    Long strings, nested struct/list fields, and ``None`` values are stringified
    and truncated to :data:`MAX_CELL_CHARS`. Designed to be readable, not a
    faithful round-trip — for full data shape, use :func:`schema_md`.

    Args:
        table: Any Arrow table.
        n: Maximum number of rows to render (default 5).

    Returns:
        Markdown table; falls back to ``"_(empty)_"`` if the table has zero rows.
    """
    if table.num_rows == 0:
        return "_(empty table)_"
    head = table.slice(0, min(n, table.num_rows)).to_pylist()
    cols = list(head[0].keys())
    rows = ["| " + " | ".join(f"`{c}`" for c in cols) + " |", "|" + "---|" * len(cols)]
    for row in head:
        rows.append("| " + " | ".join(_truncate(row[c]) for c in cols) + " |")
    return "\n".join(rows)


def bundle_resource_counts(bundle: dict) -> dict[str, int]:
    """Count FHIR resources in a bundle by ``resourceType``.

    Args:
        bundle: Parsed JSON of a FHIR R4 bundle (``{"resourceType": "Bundle",
            "entry": [{"resource": {...}}, ...]}``).

    Returns:
        Mapping from resource type (e.g. ``"Patient"``, ``"Encounter"``,
        ``"Observation"``) to count, sorted by count descending.
    """
    counts: Counter[str] = Counter()
    for entry in bundle.get("entry", []) or []:
        res = entry.get("resource") or {}
        rtype = res.get("resourceType")
        if rtype:
            counts[rtype] += 1
    return dict(counts.most_common())


def bundle_summary_md(bundle_path: Path) -> str:
    """Render a one-bundle summary: file size + resource-type counts.

    Designed for the Bronze asset metadata so a reviewer can see the FHIR
    "shape" landed for a cohort without leaving the asset graph.

    Args:
        bundle_path: Path to a single FHIR bundle JSON file.

    Returns:
        Markdown with the file name, size, total resource count, and a sorted
        resource-type table; or an inline ``_(not found)_`` placeholder.
    """
    if not bundle_path.exists():
        return f"_(bundle not found: `{bundle_path.name}`)_"
    bundle = json.loads(bundle_path.read_text())
    counts = bundle_resource_counts(bundle)
    total = sum(counts.values())
    lines = [
        f"**File:** `{bundle_path.name}`  ",
        f"**Size:** {bundle_path.stat().st_size:,} bytes  ",
        f"**Total FHIR resources:** {total}",
        "",
        "| Resource type | Count |",
        "|---|---:|",
    ]
    for rtype, count in counts.items():
        lines.append(f"| `{rtype}` | {count} |")
    return "\n".join(lines)


def gold_encounter_card(row: dict) -> str:
    """Render one ``gold.encounter_summary`` row as a multi-section Markdown card.

    The card foregrounds the SOAP note text (the demo centerpiece) and shows
    the structural fields (conditions, medications, vitals, imaging) as
    secondary sections. Long lists are truncated.

    Args:
        row: One row from the Gold Delta table as a Python dict (e.g. from
            ``table.slice(i, 1).to_pylist()[0]``).

    Returns:
        Markdown with one ``###`` heading per major section.
    """

    def _list_or_dash(values: list | None, limit: int = 6) -> str:
        if not values:
            return "_(none)_"
        shown = values[:limit]
        suffix = f" _(+{len(values) - limit} more)_" if len(values) > limit else ""
        return ", ".join(f"`{v}`" for v in shown) + suffix

    note = row.get("soap_note_text") or "_(no SOAP note)_"
    lines = [
        f"### Encounter `{row.get('encounter_id')}`",
        "",
        f"**Patient:** `{row.get('patient_id')}` · **Date:** {row.get('encounter_date')}"
        f" · **Type:** {row.get('encounter_type')} · **Age:** {row.get('patient_age')}",
        "",
        "#### SOAP note",
        "",
        "```",
        note if len(note) < 1500 else note[:1500] + " …",
        "```",
        "",
        "#### Problem list (as-of-date, ADR-014)",
        f"- **Conditions:** {_list_or_dash(row.get('active_conditions'))}",
        f"- **Medications:** {_list_or_dash(row.get('active_medications'))}",
        "",
        "#### Recent vitals",
        f"`{row.get('recent_vitals')}`",
        "",
        "#### Imaging",
        f"`{row.get('imaging')}`",
    ]
    return "\n".join(lines)
