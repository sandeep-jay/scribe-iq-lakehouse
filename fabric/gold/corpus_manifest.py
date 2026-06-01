"""Spark-native ``gold.encounter_summary`` corpus manifest (ADR-022).

Mirrors :mod:`core.gold.corpus_manifest` so the manifest JSON produced from a
Fabric build matches the one produced from a LocalLite build field-for-field
(same shape consumed by scribe-iq, clinical-bert-pipeline, the Ollama
generation pipeline).
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from pyspark.sql import functions as F  # noqa: N812

from fabric.gold.encounter_summary import CONTRACT_VERSION, SILVER_SOURCES

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

MANIFEST_VERSION = "1"
PLATFORM_NAME = "fabric"


def build_corpus_manifest(
    encounter_summary: DataFrame,
    *,
    silver_counts: dict[str, int],
    created_ts: datetime,
    silver_versions: dict[str, int] | None = None,
) -> dict:
    """Build the corpus manifest dict for a materialized ``gold.encounter_summary``.

    Args:
        encounter_summary: The built Gold Spark DataFrame.
        silver_counts: Silver table name → row count used as input.
        created_ts: Build timestamp (UTC).
        silver_versions: Optional Silver Delta versions at read time.

    Returns:
        A JSON-serializable manifest dict — lineage + corpus coverage stats.
    """
    versions = silver_versions or {}
    return {
        "manifest_version": MANIFEST_VERSION,
        "contract_version": CONTRACT_VERSION,
        "gold_table": "gold.encounter_summary",
        "created_timestamp": created_ts.isoformat(),
        "platform": PLATFORM_NAME,
        "row_count": encounter_summary.count(),
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


def _corpus_stats(df: DataFrame) -> dict:
    """Coverage statistics over the Gold corpus (single Spark aggregation)."""
    n = df.count()
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

    row = df.agg(
        F.countDistinct("patient_id").alias("distinct_patients"),
        F.sum(F.col("soap_note_text").isNotNull().cast("long")).alias("with_soap_note"),
        F.sum(F.col("has_ecg").cast("long")).alias("with_ecg"),
        F.sum(F.col("imaging.has_imaging").cast("long")).alias("with_imaging"),
        F.sum(F.col("has_genomics").cast("long")).alias("with_genomics"),
        F.sum(F.col("recent_vitals.heart_rate").isNotNull().cast("long")).alias("with_vitals"),
        F.sum((F.size("recent_labs") > 0).cast("long")).alias("with_labs"),
        F.avg(F.size("active_conditions").cast("double")).alias("avg_conditions"),
        F.avg(F.size("active_medications").cast("double")).alias("avg_medications"),
    ).first()

    return {
        "encounters": n,
        "distinct_patients": int(row["distinct_patients"] or 0),
        "with_soap_note": int(row["with_soap_note"] or 0),
        "with_ecg": int(row["with_ecg"] or 0),
        "with_imaging": int(row["with_imaging"] or 0),
        "with_genomics": int(row["with_genomics"] or 0),
        "with_vitals": int(row["with_vitals"] or 0),
        "with_labs": int(row["with_labs"] or 0),
        "avg_conditions_per_encounter": round(float(row["avg_conditions"] or 0.0), 3),
        "avg_medications_per_encounter": round(float(row["avg_medications"] or 0.0), 3),
    }
