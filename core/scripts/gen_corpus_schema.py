"""Generate schemas/gold_encounter_summary.json from the Gold Arrow schema.

Generated-first documentation (ADR-011): the machine-readable corpus contract is
derived from the single source of truth — ``GOLD_SCHEMA`` in
``local/gold/encounter_summary.py`` — so it can never drift from the code. Downstream
consumers (scribe-iq, clinical-bert-pipeline) validate against this JSON Schema.

Usage:
    python scripts/gen_corpus_schema.py            # write schemas/gold_encounter_summary.json
    python scripts/gen_corpus_schema.py --check     # exit 1 if the file is stale (CI)

``render_schema()`` is pure (returns the dict) so tests can assert the committed file is
current — see tests/test_gold_encounter_summary.py.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pyarrow as pa

# Script lives at core/scripts/gen_corpus_schema.py — climb 3 levels for repo root.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.gold.encounter_summary import (  # noqa: E402
    CONTRACT_VERSION,
    GOLD_SCHEMA,
    REQUIRED_FIELDS,
)

OUTPUT_PATH = _REPO_ROOT / "schemas" / "gold_encounter_summary.json"

# Top-level fields whose value may be null (the key is still always present).
# List/struct fields are always present (lists empty, struct members carry the nulls).
_NULLABLE_FIELDS = {
    "patient_age",
    "encounter_date",
    "soap_note_text",
    "soap_note_id",
    "ecg_finding",
    "ecg_rhythm",
    "genomic_summary",
}


def _json_type(dtype: pa.DataType) -> dict:
    """Map an Arrow type to a JSON Schema node. Struct members are all nullable."""
    if pa.types.is_string(dtype):
        return {"type": "string"}
    if pa.types.is_integer(dtype):
        return {"type": "integer"}
    if pa.types.is_floating(dtype):
        return {"type": "number"}
    if pa.types.is_boolean(dtype):
        return {"type": "boolean"}
    if pa.types.is_date(dtype):
        return {"type": "string", "format": "date"}
    if pa.types.is_timestamp(dtype):
        return {"type": "string", "format": "date-time"}
    if pa.types.is_list(dtype):
        return {"type": "array", "items": _json_type(dtype.value_type)}
    if pa.types.is_struct(dtype):
        return {
            "type": "object",
            "additionalProperties": False,
            "properties": {f.name: _nullable(_json_type(f.type)) for f in dtype},
        }
    raise TypeError(f"Unmapped Arrow type: {dtype}")


def _nullable(node: dict) -> dict:
    """Return a copy of a JSON Schema node whose ``type`` also permits null."""
    node = dict(node)
    t = node["type"]
    node["type"] = [t, "null"] if isinstance(t, str) else [*t, "null"]
    return node


def render_schema() -> dict:
    """Render the corpus JSON Schema (Draft 2020-12) as a deterministic dict."""
    properties: dict[str, dict] = {}
    for field in GOLD_SCHEMA:
        node = _json_type(field.type)
        properties[field.name] = _nullable(node) if field.name in _NULLABLE_FIELDS else node
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://scribe-iq.dev/schemas/gold_encounter_summary.json",
        "title": "gold.encounter_summary",
        "description": (
            "Corpus contract for the denormalized encounter summary that feeds the "
            "Ollama generation pipeline, scribe-iq (RAG) and clinical-bert-pipeline (NLP). "
            "One object per clinical encounter. See docs/CORPUS_CONTRACT.md."
        ),
        "x-contract-version": CONTRACT_VERSION,
        "type": "object",
        "additionalProperties": False,
        "required": list(REQUIRED_FIELDS),
        "properties": properties,
    }


def _serialize(schema: dict) -> str:
    """Stable JSON serialization (sorted keys, trailing newline) for diff-friendly files."""
    return json.dumps(schema, indent=2, sort_keys=True) + "\n"


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. ``--check`` verifies the file is current without writing."""
    import argparse

    ap = argparse.ArgumentParser(description="Generate the Gold corpus JSON Schema")
    ap.add_argument("--check", action="store_true", help="Exit 1 if the file is out of date")
    args = ap.parse_args(argv)

    content = _serialize(render_schema())
    if args.check:
        current = OUTPUT_PATH.read_text() if OUTPUT_PATH.exists() else ""
        if current != content:
            print(f"{OUTPUT_PATH} is out of date — run: python scripts/gen_corpus_schema.py")
            return 1
        print(f"{OUTPUT_PATH} is up to date.")
        return 0

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(content)
    print(f"Wrote {OUTPUT_PATH}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
