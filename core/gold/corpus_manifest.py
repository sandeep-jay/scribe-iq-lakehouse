"""Gold lineage: the ``gold.encounter_summary`` corpus manifest.

:func:`build_corpus_manifest` derives a JSON-serializable manifest from the built Gold
table plus the Silver row counts/versions it was assembled from. It is the machine-
readable provenance record handed to downstream consumers (scribe-iq, clinical-bert-
pipeline, the Ollama generation pipeline) alongside ``docs/CORPUS_CONTRACT.md``.

Pure function (ADR-002/004): no platform, Spark, or Delta imports and no I/O — the
caller writes the returned dict to ``gold/_metadata/corpus_manifest.json`` via the
platform, mirroring the Bronze ingest manifest pattern.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc

from core.gold.encounter_summary import CONTRACT_VERSION, SILVER_SOURCES

MANIFEST_VERSION = "1"


def build_corpus_manifest(
    encounter_summary: pa.Table,
    *,
    silver_counts: Mapping[str, int],
    created_ts: datetime,
    platform_name: str,
    silver_versions: Mapping[str, int] | None = None,
) -> dict:
    """Build the corpus manifest for a materialized ``gold.encounter_summary``.

    Args:
        encounter_summary: The built Gold table.
        silver_counts: Silver table name -> row count used as input.
        created_ts: Build timestamp (UTC).
        platform_name: Platform that materialized the corpus (e.g. ``"local_lite"``).
        silver_versions: Optional Silver Delta versions at read time.

    Returns:
        A JSON-serializable manifest dict (lineage + corpus coverage statistics).
    """
    versions = silver_versions or {}
    return {
        "manifest_version": MANIFEST_VERSION,
        "contract_version": CONTRACT_VERSION,
        "gold_table": "gold.encounter_summary",
        "created_timestamp": created_ts.isoformat(),
        "platform": platform_name,
        "row_count": encounter_summary.num_rows,
        "corpus_stats": _corpus_stats(encounter_summary),
        "silver_sources": [
            {
                "table": f"silver.{name}",
                "row_count": int(silver_counts.get(name, 0)),
                "delta_version": versions.get(name),
            }
            for name in SILVER_SOURCES
        ],
    }


def _corpus_stats(table: pa.Table) -> dict:
    """Coverage statistics over the Gold corpus (counts + per-encounter averages)."""
    n = table.num_rows
    if n == 0:
        return {
            "encounters": 0,
            "distinct_patients": 0,
            "with_soap_note": 0,
            "with_ecg": 0,
            "with_imaging": 0,
            "with_genomics": 0,
            "with_vitals": 0,
            "with_labs": 0,
            "avg_conditions_per_encounter": 0.0,
            "avg_medications_per_encounter": 0.0,
        }

    return {
        "encounters": n,
        "distinct_patients": pc.count_distinct(table["patient_id"]).as_py(),
        "with_soap_note": _non_null(table["soap_note_text"]),
        "with_ecg": _true_count(table["has_ecg"]),
        "with_imaging": _true_count(pc.struct_field(table["imaging"], "has_imaging")),
        "with_genomics": _true_count(table["has_genomics"]),
        "with_vitals": _non_null(pc.struct_field(table["recent_vitals"], "heart_rate")),
        "with_labs": _non_empty_list(table["recent_labs"]),
        "avg_conditions_per_encounter": _avg_list_len(table["active_conditions"]),
        "avg_medications_per_encounter": _avg_list_len(table["active_medications"]),
    }


def _non_null(column: pa.ChunkedArray | pa.Array) -> int:
    """Count non-null values in a column."""
    return pc.sum(pc.cast(pc.is_valid(column), pa.int64())).as_py() or 0


def _true_count(column: pa.ChunkedArray | pa.Array) -> int:
    """Count ``True`` values in a boolean column (nulls count as False)."""
    return pc.sum(pc.cast(pc.fill_null(column, False), pa.int64())).as_py() or 0


def _non_empty_list(column: pa.ChunkedArray | pa.Array) -> int:
    """Count rows whose list value has at least one element."""
    lengths = pc.list_value_length(column)
    return pc.sum(pc.cast(pc.greater(pc.fill_null(lengths, 0), 0), pa.int64())).as_py() or 0


def _avg_list_len(column: pa.ChunkedArray | pa.Array) -> float:
    """Mean number of elements per row for a list column."""
    lengths = pc.fill_null(pc.list_value_length(column), 0)
    mean = pc.mean(pc.cast(lengths, pa.float64())).as_py()
    return round(mean, 3) if mean is not None else 0.0
