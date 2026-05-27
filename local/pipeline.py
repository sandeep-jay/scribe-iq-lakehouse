"""Local end-to-end pipeline: Bronze FHIR -> Silver Delta tables (+ ingest_log).

Each cohort partition is processed as a micro-batch (the local analogue of an Auto
Loader trigger, spec §5.2): bundles are parsed, every Silver table is built from that
cohort's records, and the rows are MERGE-upserted into Delta. After all cohorts land,
each table is read back, validated (spec §5.6), and the results are appended to
``silver.ingest_log``.

Platform-agnostic: it talks only to a ``LakehousePlatform`` and the pure transforms.
Run with ``python -m local.pipeline`` (defaults to LocalLitePlatform).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

from local.gold.corpus_manifest import build_corpus_manifest
from local.gold.encounter_summary import SILVER_SOURCES, TABLE_NAME, build_encounter_summary
from local.ingest.bronze_landing import DEFAULT_BRONZE, cohort_files, cohort_labels
from local.platform.base import LakehousePlatform
from local.platform.factory import get_platform
from local.redaction import redact
from local.transforms.fhir_parser import FHIRBundleParser
from local.transforms.registry import SILVER_TABLES
from local.validation.validate import results_to_arrow, validate_table

logger = logging.getLogger(__name__)


def _parse_cohort(parser: FHIRBundleParser, files: list[Path]) -> dict[str, list[dict]]:
    """Parse every bundle in a cohort, stamping ``source_file`` on each record."""
    accumulated: dict[str, list[dict]] = {name: [] for name in SILVER_TABLES}
    for path in files:
        try:
            bundle = _read_json(path)
        except (OSError, ValueError):
            # Redact the filename — it embeds the patient name/UUID (see local.redaction).
            logger.warning("Skipping unreadable bundle %s", redact(path.name))
            continue
        parsed = parser.parse_bundle(bundle)
        for table_name in SILVER_TABLES:
            for record in parsed.get(table_name, []):
                record["source_file"] = path.name
                accumulated[table_name].append(record)
    return accumulated


def _read_json(path: Path) -> dict:
    import json

    return json.loads(path.read_text())


def run_pipeline(
    platform: LakehousePlatform | None = None,
    bronze_root: Path = DEFAULT_BRONZE,
    cohorts: list[str] | None = None,
    ingest_ts: datetime | None = None,
) -> dict[str, int]:
    """Run the full Bronze -> Silver pipeline.

    Args:
        platform: Target platform; defaults to the factory's configured platform.
        bronze_root: Bronze layer root holding ``fhir/cohort=*/``.
        cohorts: Cohort labels to process; defaults to all present.
        ingest_ts: Ingest timestamp stamped on all rows; defaults to now (UTC).

    Returns:
        ``{table_name: final_row_count}`` plus ``{"_cohorts": n, "_failed": n}``.
    """
    platform = platform or get_platform()
    ingest_ts = ingest_ts or datetime.now(UTC)
    parser = FHIRBundleParser()
    labels = cohorts if cohorts is not None else cohort_labels(bronze_root)
    if not labels:
        raise RuntimeError(f"No cohorts found under {bronze_root}/fhir/ — run download first")

    logger.info("Pipeline start: %d cohort(s) via %s", len(labels), platform.name)
    for label in labels:
        files = cohort_files(label, bronze_root)
        logger.info("Cohort %s: parsing %d bundles", label, len(files))
        records = _parse_cohort(parser, files)
        for name, spec in SILVER_TABLES.items():
            table = spec.build(records[name], ingest_ts)
            platform.write_silver(name, table, mode="merge")
        logger.info("Cohort %s: merged into %d Silver tables", label, len(SILVER_TABLES))

    summary, results = {}, []
    for name in SILVER_TABLES:
        table = platform.read_silver(name)
        result = validate_table(name, table)
        results.append(result)
        summary[name] = table.num_rows
        platform.log_metric(name, "row_count", table.num_rows)
        if not result.passed:
            platform.send_alert("warning", f"Validation failed for {name}: {result.failed_checks}")

    platform.write_silver("ingest_log", results_to_arrow(results, ingest_ts), mode="append")
    summary["_cohorts"] = len(labels)
    summary["_failed"] = sum(1 for r in results if not r.passed)
    logger.info("Pipeline complete: %s", summary)
    return summary


def build_gold(
    platform: LakehousePlatform | None = None,
    created_ts: datetime | None = None,
) -> dict[str, int]:
    """Build ``gold.encounter_summary`` and its corpus manifest from Silver.

    Reads every Silver source table via the platform, denormalizes to the encounter
    corpus (spec §5.4), writes the Gold Delta table, and persists the lineage manifest
    (spec §5.7). Platform-agnostic: talks only to the ``LakehousePlatform`` and the pure
    Gold transforms.

    Args:
        platform: Target platform; defaults to the factory's configured platform.
        created_ts: Build timestamp stamped on all rows; defaults to now (UTC).

    Returns:
        ``{"encounter_summary": row_count}`` merged with the manifest corpus stats.
    """
    platform = platform or get_platform()
    created_ts = created_ts or datetime.now(UTC)

    silver = {name: platform.read_silver(name) for name in SILVER_SOURCES}
    counts = {name: table.num_rows for name, table in silver.items()}
    versions = {name: platform.table_version("silver", name) for name in SILVER_SOURCES}

    gold = build_encounter_summary(silver, created_ts=created_ts, silver_versions=versions)
    platform.write_gold(TABLE_NAME, gold)

    manifest = build_corpus_manifest(
        gold,
        silver_counts=counts,
        created_ts=created_ts,
        platform_name=platform.name,
        silver_versions=versions,
    )
    platform.write_gold_manifest(manifest)
    platform.log_metric(TABLE_NAME, "row_count", gold.num_rows)
    logger.info("Gold build complete: %d encounter summaries", gold.num_rows)
    return {"encounter_summary": gold.num_rows, **manifest["corpus_stats"]}


if __name__ == "__main__":
    import argparse
    import json

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Run the local Bronze -> Silver -> Gold pipeline")
    ap.add_argument("--bronze-root", type=Path, default=DEFAULT_BRONZE)
    ap.add_argument("--cohort", action="append", dest="cohorts", help="Limit to cohort(s)")
    ap.add_argument("--with-gold", action="store_true", help="Build Gold after Silver")
    ap.add_argument(
        "--gold-only",
        action="store_true",
        help="Build Gold from existing Silver (skip Bronze -> Silver)",
    )
    args = ap.parse_args()

    if args.gold_only:
        result = build_gold()
    else:
        result = run_pipeline(bronze_root=args.bronze_root, cohorts=args.cohorts)
        if args.with_gold:
            result["gold"] = build_gold()
    print(json.dumps(result, indent=2))
