"""Software-defined assets for the medallion (ADR-016).

``bronze_fhir`` (per-cohort inventory) → ``silver_tables`` (multi-asset:
parse-once → 10 distinct Silver asset nodes) → ``gold_encounter_summary``
(unpartitioned aggregate, also writes the corpus manifest as a side-effect).

Every asset reuses the pure transforms verbatim — no logic is duplicated.
Persistence is delegated to the :class:`PlatformResource`; assets return
:class:`MaterializeResult` with row-count / lineage metadata for the UI.
"""

from collections.abc import Iterable
from datetime import UTC, datetime

from dagster import (
    AssetExecutionContext,
    AssetSpec,
    MaterializeResult,
    MetadataValue,
    asset,
    multi_asset,
)

from local.gold.corpus_manifest import build_corpus_manifest
from local.gold.encounter_summary import (
    CONTRACT_VERSION,
    SILVER_SOURCES,
    build_encounter_summary,
)
from local.gold.encounter_summary import (
    TABLE_NAME as GOLD_TABLE_NAME,
)
from local.ingest.bronze_landing import cohort_files
from local.ingest.dicom_index import DicomIndex
from local.pipeline import _parse_cohort
from local.platform.base import LakehousePlatform
from local.preview import bundle_summary_md, gold_encounter_card, sample_md, schema_md
from local.transforms.fhir_parser import FHIRBundleParser
from local.transforms.registry import SILVER_TABLES
from orchestration.partitions import cohort_partitions
from orchestration.resources import PlatformResource


def _bronze_root(platform: LakehousePlatform):
    """Absolute path to ``bronze/`` from the platform's resolved storage root.

    The platform anchors relative roots to the repo (LocalLitePlatform), so this
    works from any CWD — including the Dagster daemon's subprocess that fires
    sensor-triggered runs (the source of the "Bronze cohort 'X' has no bundles"
    failure mode before the platform-root anchoring was added).
    """
    return platform.root / "bronze"


#: Asset key list for the 10 Silver tables (also the Delta table names).
SILVER_ASSET_KEYS: list[str] = list(SILVER_TABLES.keys())


# --------------------------------------------------------------------- bronze


@asset(
    partitions_def=cohort_partitions,
    group_name="bronze",
    description="Inventory of FHIR bundles landed under <storage>/bronze/fhir/cohort=<key>/.",
)
def bronze_fhir(context: AssetExecutionContext, platform: PlatformResource) -> MaterializeResult:
    """Observability asset: count and total bytes of bundles for the cohort.

    Bronze is raw and append-only — no parsing, no mutation. This asset
    materializes the cohort's *presence* so it shows up as a node in the lineage
    graph and the Silver multi-asset can be triggered per-partition.
    """
    cohort = context.partition_key
    bronze_root = _bronze_root(platform.create())
    files = cohort_files(cohort, bronze_root)
    total_bytes = sum(f.stat().st_size for f in files)
    context.log.info("Bronze cohort %s: %d bundle(s), %d bytes", cohort, len(files), total_bytes)
    # Render the first bundle's resource-type breakdown so reviewers can see
    # the FHIR "shape" landed for this cohort without leaving the asset graph.
    sample_md_text = bundle_summary_md(files[0]) if files else "_(no bundles)_"
    return MaterializeResult(
        metadata={
            "cohort": cohort,
            "files": len(files),
            "total_bytes": MetadataValue.int(total_bytes),
            "path": str(bronze_root / "fhir" / f"cohort={cohort}"),
            "sample_bundle": MetadataValue.md(sample_md_text),
        }
    )


# --------------------------------------------------------------------- silver

#: One ``AssetSpec`` per Silver table; each depends on ``bronze_fhir`` for the
#: same cohort partition, so Dagster renders the medallion fan-out in the graph
#: even though all 10 are produced by a single parse-once function.
_SILVER_SPECS: list[AssetSpec] = [
    AssetSpec(key=name, deps=["bronze_fhir"], group_name="silver") for name in SILVER_ASSET_KEYS
]


@multi_asset(specs=_SILVER_SPECS, partitions_def=cohort_partitions)
def silver_tables(
    context: AssetExecutionContext, platform: PlatformResource
) -> Iterable[MaterializeResult]:
    """Parse one cohort's bundles once, MERGE-upsert into every Silver table.

    Mirrors :func:`local.pipeline.run_pipeline` for a single cohort: the same
    :func:`local.pipeline._parse_cohort` parses bundles (with optional DICOM
    enrichment, ADR-013), then each ``SILVER_TABLES[*].build`` runs and the
    platform persists. One :class:`MaterializeResult` is yielded per Silver asset
    so each is a distinct node in the lineage graph (ADR-016).

    Yields:
        ``MaterializeResult`` for each Silver table with row count + cohort.
    """
    cohort = context.partition_key
    p = platform.create()
    bronze_root = _bronze_root(p)
    files = cohort_files(cohort, bronze_root)
    if not files:
        raise RuntimeError(f"Bronze cohort {cohort!r} has no bundles under {bronze_root}")
    ingest_ts = datetime.now(UTC)
    dicom_index = DicomIndex(bronze_root / "dicom")
    parsed = _parse_cohort(FHIRBundleParser(), files, dicom_index=dicom_index)

    for name, spec in SILVER_TABLES.items():
        table = spec.build(parsed[name], ingest_ts)
        p.write_silver(name, table, mode="merge")
        context.log.info("Silver %s (cohort %s): %d rows", name, cohort, table.num_rows)
        yield MaterializeResult(
            asset_key=name,
            metadata={
                "cohort": cohort,
                "rows": table.num_rows,
                "primary_key": spec.primary_key,
                "schema": MetadataValue.md(schema_md(table)),
                "sample_rows": MetadataValue.md(sample_md(table)),
            },
        )


# ----------------------------------------------------------------------- gold


@asset(
    deps=SILVER_ASSET_KEYS,
    group_name="gold",
    description="Denormalized encounter corpus from all 10 Silver tables (contract v1.1.0).",
)
def gold_encounter_summary(
    context: AssetExecutionContext, platform: PlatformResource
) -> MaterializeResult:
    """Full-cohort Gold build + corpus manifest in one step.

    Reads every Silver source via the platform, denormalizes to the encounter
    corpus, writes the Delta table, and writes the lineage manifest alongside.
    Manifest is co-materialized (rather than a downstream asset) because the
    abstract :class:`LakehousePlatform` does not currently expose ``read_gold``;
    keeping it co-located also matches what :func:`local.pipeline.build_gold` does.
    """
    p = platform.create()
    created_ts = datetime.now(UTC)
    silver = {name: p.read_silver(name) for name in SILVER_SOURCES}
    silver_counts = {name: silver[name].num_rows for name in SILVER_SOURCES}
    versions = {name: p.table_version("silver", name) for name in SILVER_SOURCES}

    gold = build_encounter_summary(silver, created_ts=created_ts, silver_versions=versions)
    p.write_gold(GOLD_TABLE_NAME, gold)
    manifest = build_corpus_manifest(
        gold,
        silver_counts=silver_counts,
        created_ts=created_ts,
        platform_name=p.name,
        silver_versions=versions,
    )
    p.write_gold_manifest(manifest)
    p.log_metric(GOLD_TABLE_NAME, "row_count", gold.num_rows)
    context.log.info(
        "Gold encounter_summary: %d rows (contract v%s)", gold.num_rows, CONTRACT_VERSION
    )
    # Pick a sample encounter with a SOAP note for the asset card — the demo
    # centerpiece. Falls back to the first row if no SOAP-bearing row is found.
    sample_card = "_(no rows)_"
    if gold.num_rows:
        soap_col = gold.column("soap_note_text")
        sample_idx = next((i for i in range(min(gold.num_rows, 200)) if soap_col[i].as_py()), 0)
        sample_row = gold.slice(sample_idx, 1).to_pylist()[0]
        sample_card = gold_encounter_card(sample_row)
    return MaterializeResult(
        metadata={
            "rows": gold.num_rows,
            "contract_version": CONTRACT_VERSION,
            "corpus_stats": MetadataValue.json(manifest["corpus_stats"]),
            "silver_versions": MetadataValue.json(versions),
            "sample_encounter": MetadataValue.md(sample_card),
            "schema": MetadataValue.md(schema_md(gold)),
        }
    )
