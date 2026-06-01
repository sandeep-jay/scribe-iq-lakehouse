"""Bronze cohort sensor — the Dagster analogue of the Auto Loader streaming-sim.

Spec §5.2 describes a watchdog-based local streaming simulation that triggers
Silver transforms when a new cohort partition lands under ``data/bronze/fhir/``.
This sensor performs the same role inside Dagster: every tick it lists current
cohort labels, diffs them against the registered dynamic partition set, and
issues ``RunRequest``s for the new ones — with the dynamic-partition add request
in the same evaluation so the partition keys exist before the runs start.

Each ``RunRequest`` materializes the **full cohort chain** — ``bronze_fhir`` plus
the 10 Silver tables produced by the ``silver_tables`` multi-asset — in a single
Dagster run. ``gold_encounter_summary`` is intentionally **not** in the target:
it is unpartitioned and aggregates across all cohorts, so it is rebuilt manually
(or by a downstream schedule) once the cohorts of interest are all present.
"""

from dagster import (
    AssetSelection,
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)

from core.ingest.bronze_landing import cohort_labels
from core.orchestration.dagster.partitions import COHORT_PARTITIONS_NAME, cohort_partitions
from core.orchestration.dagster.resources import PlatformResource
from core.transforms.registry import SILVER_TABLES

#: Assets the sensor materializes per cohort: Bronze + every Silver table.
#: Sourced from :data:`core.transforms.registry.SILVER_TABLES` so the selection
#: stays in lockstep with the multi-asset outputs (no second source of truth).
SENSOR_TARGET_KEYS: tuple[str, ...] = ("bronze_fhir", *SILVER_TABLES.keys())


@sensor(
    target=AssetSelection.assets(*SENSOR_TARGET_KEYS),
    default_status=DefaultSensorStatus.STOPPED,
    minimum_interval_seconds=30,
    description=(
        "Watch <storage>/bronze/fhir/cohort=*/ and materialize bronze_fhir + all 10 "
        "Silver tables for each new cohort. Gold rebuild is manual (unpartitioned aggregate)."
    ),
)
def bronze_cohort_sensor(
    context: SensorEvaluationContext, platform: PlatformResource
) -> SensorResult | SkipReason:
    """Detect new cohort directories and request a materialization for each.

    The sensor is the only place that mutates the dynamic partition set, so the
    rest of the asset graph stays declarative. The platform resource is injected
    so the Bronze root is resolved through the same CWD-independent anchor the
    assets use — sensor-triggered runs are spawned by the Dagster daemon, whose
    working directory is not guaranteed to be the repo root.

    Returns:
        ``SensorResult`` with one ``RunRequest`` per new cohort and a single
        ``add_request`` to register the new partition keys, or ``SkipReason`` if
        no new cohorts are present.
    """
    bronze_root = platform.create().root / "bronze"
    current = set(cohort_labels(bronze_root))
    existing = set(context.instance.get_dynamic_partitions(COHORT_PARTITIONS_NAME))
    new_cohorts = sorted(current - existing)
    if not new_cohorts:
        return SkipReason(
            f"No new cohorts in {bronze_root}/fhir/ (have: {sorted(existing) or '∅'})"
        )
    context.log.info("Discovered %d new cohort(s): %s", len(new_cohorts), new_cohorts)
    return SensorResult(
        run_requests=[RunRequest(partition_key=c) for c in new_cohorts],
        dynamic_partitions_requests=[cohort_partitions.build_add_request(new_cohorts)],
    )
