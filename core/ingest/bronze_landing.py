"""Bronze landing helpers — inventory of raw FHIR bundles, by cohort.

Bronze is raw and append-only: bundles stay as the JSON files that ``download.py``
landed (no parsing, no mutation). This module reads the ingest manifest and builds a
lightweight file inventory used for lineage and for driving the streaming simulation.
"""

from __future__ import annotations

import json
from pathlib import Path

DEFAULT_BRONZE = Path("data/bronze")


def read_manifest(bronze_root: Path = DEFAULT_BRONZE) -> dict | None:
    """Return the ingest manifest dict, or ``None`` if it has not been written yet."""
    path = bronze_root / "_metadata" / "manifest.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


def cohort_labels(bronze_root: Path = DEFAULT_BRONZE) -> list[str]:
    """List cohort labels present under ``<root>/fhir/`` (e.g. ``["A", "B", "C"]``)."""
    fhir_dir = bronze_root / "fhir"
    return sorted(p.name.split("=", 1)[1] for p in fhir_dir.glob("cohort=*") if p.is_dir())


def cohort_files(cohort: str, bronze_root: Path = DEFAULT_BRONZE) -> list[Path]:
    """Return the bundle file paths for one cohort, sorted."""
    return sorted((bronze_root / "fhir" / f"cohort={cohort}").glob("*.json"))


def bronze_inventory(bronze_root: Path = DEFAULT_BRONZE) -> list[dict]:
    """Build a per-file inventory: ``{file, cohort, bytes}`` for every landed bundle."""
    inventory: list[dict] = []
    for label in cohort_labels(bronze_root):
        for path in cohort_files(label, bronze_root):
            inventory.append({"file": path.name, "cohort": label, "bytes": path.stat().st_size})
    return inventory
