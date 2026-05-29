"""Shared Spark helpers for Fabric Silver transforms."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import Window
from pyspark.sql import functions as F  # noqa: N812

from fabric.transforms.bundle_schema import BUNDLE_SCHEMA

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


def parse_bundles_to_resources(bundles_df: DataFrame) -> DataFrame:
    """Parse the bundles DataFrame into one row per FHIR resource.

    Args:
        bundles_df: Spark DataFrame with columns ``path`` (input file URI) and
            ``value`` (raw JSON text of one FHIR Bundle).

    Returns:
        Spark DataFrame with columns ``path``, ``source_file`` (basename of
        path), ``r`` (parsed FHIR resource struct — see RESOURCE_SCHEMA in
        bundle_schema).
    """
    return (
        bundles_df.withColumn("bundle", F.from_json("value", BUNDLE_SCHEMA))
        .select(
            F.col("path"),
            F.element_at(F.split("path", "/"), -1).alias("source_file"),
            F.explode("bundle.entry").alias("entry"),
        )
        .select("path", "source_file", F.col("entry.resource").alias("r"))
    )


def strip_reference(col: Column) -> Column:
    """FHIR reference (``urn:uuid:abc-123`` or ``Patient/abc-123``) → bare id."""
    return F.regexp_replace(col, r"^(urn:uuid:|[A-Za-z]+/)", "")


def dedup_keep_last(df: DataFrame, key: str) -> DataFrame:
    """Drop duplicate rows on ``key``, keeping one deterministic survivor per key.

    Mirrors :func:`core.transforms.schema_utils.dedup_by_key` semantics for the
    Fabric path. Spark doesn't preserve insertion order across files, so the
    survivor is "the row with the largest monotonically_increasing_id" — a
    deterministic but arbitrary choice. The following MERGE writes the source's
    canonical value on top, so cross-platform divergence on dup-survival is
    invisible to downstream consumers (same logic as ADR-019 for the
    LocalLite path).
    """
    w = Window.partitionBy(key).orderBy(F.col("_rid").desc())
    return (
        df.withColumn("_rid", F.monotonically_increasing_id())
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_rid")
    )
