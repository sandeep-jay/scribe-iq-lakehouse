"""Convert Jupyter ``.ipynb`` notebooks to Microsoft Fabric source format.

Fabric Git Integration syncs items in Fabric's own folder format — plain ``.ipynb``
files at the path are not recognized. Each Fabric notebook must live in a
``<name>.Notebook/`` directory containing:

  - ``notebook-content.py`` — Python source with ``# CELL ********************``
    and ``# MARKDOWN ********************`` magic comments separating cells, plus
    a leading notebook-level ``# METADATA`` block (kernel + lakehouse binding).
  - ``.platform`` — JSON metadata: type, displayName, logicalId (stable GUID).

This script reads every ``.ipynb`` under ``fabric/notebooks/`` and regenerates
its sibling ``.Notebook/`` folder. The logical ID is derived deterministically
from the notebook stem (``uuid.uuid5``) so re-runs produce byte-identical output
unless the source changes — friendly to the pre-commit gate.

The lakehouse binding is intentionally omitted from the generated metadata so
workspace/lakehouse GUIDs stay out of committed files (see ``.env.example``
contract). Users attach a lakehouse manually in the Fabric UI on first run
of each notebook; Fabric persists the binding server-side after that.

Run manually:

    python core/scripts/convert_ipynb_to_fabric.py
    python core/scripts/convert_ipynb_to_fabric.py --notebooks fabric/notebooks/00_setup.ipynb
    python core/scripts/convert_ipynb_to_fabric.py --check     # CI / pre-commit: fail on drift
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path

# Deterministic namespace for notebook logical IDs — never change this UUID,
# changing it would re-issue every logicalId and Fabric would treat existing
# notebooks as brand-new items on the next sync.
_NAMESPACE = uuid.UUID("3a1c2f7e-9b8d-5a4c-b2e1-7f6a4d3c8e91")

_NOTEBOOKS_DIR = Path("fabric/notebooks")
_PLATFORM_SCHEMA = (
    "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/"
    "platformProperties/2.0.0/schema.json"
)


def _logical_id(stem: str) -> str:
    """Stable UUIDv5 for a notebook, derived from its filename stem."""
    return str(uuid.uuid5(_NAMESPACE, stem))


def _meta_block(meta: dict) -> str:
    """Render a Fabric ``# META {...}`` JSON block, one ``# META`` prefix per line."""
    body = json.dumps(meta, indent=2, sort_keys=True)
    return "\n".join(f"# META {line}" if line else "# META" for line in body.split("\n"))


def _notebook_header() -> str:
    """Notebook-level METADATA block — kernel only, no lakehouse binding."""
    meta = {"kernel_info": {"name": "synapse_pyspark"}}
    return (
        "# Fabric notebook source\n\n"
        "# METADATA ********************\n\n"
        f"{_meta_block(meta)}\n"
    )


def _code_cell(source: str) -> str:
    """Render a code cell with its per-cell language metadata."""
    cell_meta = {"language": "python", "language_group": "synapse_pyspark"}
    return (
        "\n# CELL ********************\n\n"
        f"{source.rstrip()}\n\n"
        "# METADATA ********************\n\n"
        f"{_meta_block(cell_meta)}\n"
    )


def _markdown_cell(source: str) -> str:
    """Render a markdown cell — each source line prefixed with ``# ``."""
    prefixed = "\n".join(f"# {line}" if line else "#" for line in source.rstrip().split("\n"))
    cell_meta = {"language": "markdown", "language_group": "synapse_pyspark"}
    return (
        "\n# MARKDOWN ********************\n\n"
        f"{prefixed}\n\n"
        "# METADATA ********************\n\n"
        f"{_meta_block(cell_meta)}\n"
    )


def _ipynb_to_fabric_py(nb: dict) -> str:
    """Convert a parsed ``.ipynb`` dict to Fabric's ``notebook-content.py`` text."""
    out = [_notebook_header()]
    for cell in nb.get("cells", []):
        source = "".join(cell.get("source", []))
        if cell.get("cell_type") == "code":
            out.append(_code_cell(source))
        elif cell.get("cell_type") == "markdown":
            out.append(_markdown_cell(source))
        # raw / other cell types: skip silently — Fabric notebooks support only code + markdown
    return "".join(out)


def _platform_file(display_name: str) -> str:
    """Render the ``.platform`` JSON for a notebook."""
    spec = {
        "$schema": _PLATFORM_SCHEMA,
        "metadata": {"type": "Notebook", "displayName": display_name},
        "config": {"version": "2.0", "logicalId": _logical_id(display_name)},
    }
    return json.dumps(spec, indent=2, sort_keys=True) + "\n"


def convert(ipynb_path: Path, *, check: bool = False) -> bool:
    """Convert one ``.ipynb`` to its sibling ``.Notebook/`` folder.

    Returns ``True`` if any file would change (or did change when not ``check``).
    """
    stem = ipynb_path.stem
    out_dir = ipynb_path.parent / f"{stem}.Notebook"
    nb = json.loads(ipynb_path.read_text())
    py_text = _ipynb_to_fabric_py(nb)
    platform_text = _platform_file(stem)

    py_path = out_dir / "notebook-content.py"
    platform_path = out_dir / ".platform"

    changed = False
    for path, new_text in [(py_path, py_text), (platform_path, platform_text)]:
        current = path.read_text() if path.exists() else None
        if current != new_text:
            changed = True
            if not check:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(new_text)
    return changed


def main() -> int:
    """CLI entry — convert all ``.ipynb`` under ``fabric/notebooks/`` (or a subset)."""
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--notebooks",
        nargs="*",
        type=Path,
        default=None,
        help="Specific .ipynb paths; defaults to every .ipynb under fabric/notebooks/",
    )
    ap.add_argument(
        "--check",
        action="store_true",
        help="Don't write; exit 1 if any .Notebook/ output would change (pre-commit gate).",
    )
    args = ap.parse_args()

    if args.notebooks:
        paths = [p for p in args.notebooks if p.suffix == ".ipynb"]
    else:
        paths = sorted(_NOTEBOOKS_DIR.glob("*.ipynb"))

    if not paths:
        print(f"No .ipynb files found under {_NOTEBOOKS_DIR}/", file=sys.stderr)
        return 0

    any_changed = False
    for ipynb_path in paths:
        changed = convert(ipynb_path, check=args.check)
        status = "CHANGED" if changed else "ok"
        print(f"  [{status}] {ipynb_path} -> {ipynb_path.parent / (ipynb_path.stem + '.Notebook')}/")
        any_changed = any_changed or changed

    if args.check and any_changed:
        print(
            "\nFabric .Notebook/ outputs are stale. Run:\n"
            "  python core/scripts/convert_ipynb_to_fabric.py\n"
            "and re-stage the changes.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
