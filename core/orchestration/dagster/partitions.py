"""Cohort partitioning for the medallion asset graph (ADR-016).

Cohorts (the ``cohort=*`` directories landed under ``data/bronze/fhir/``) map 1:1
onto Dagster partitions: each Bronze cohort drop becomes a partition key, and the
Silver multi-asset materializes per partition. This unlocks per-cohort
materialization + backfill — the working incremental MERGE path — and ends the
``rm -rf data/silver data/gold`` full-rebuild dance documented in HANDOFF.

Dynamic (not static) partitions: cohorts are discovered at runtime by
:func:`core.ingest.bronze_landing.cohort_labels`, and the Bronze cohort sensor
(:mod:`orchestration.sensors`) adds new ones to the Dagster instance as they land.
Gold assets are intentionally unpartitioned — they aggregate across all cohorts.
"""

from __future__ import annotations

from dagster import DynamicPartitionsDefinition

#: Name used both as the Dagster partition set identifier and as the parameter
#: passed to ``instance.get_dynamic_partitions(name)`` / ``add_dynamic_partitions``.
COHORT_PARTITIONS_NAME = "cohort"

#: The cohort partition set. Imported by every partitioned asset and by the sensor.
cohort_partitions = DynamicPartitionsDefinition(name=COHORT_PARTITIONS_NAME)
