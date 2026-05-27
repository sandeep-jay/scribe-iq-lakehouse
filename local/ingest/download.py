"""Bronze ingest — sync Synthea Coherent FHIR bundles from S3 to local Bronze.

The source is AWS Open Data (no credentials): ``s3://synthea-open-data/coherent/``.
Bundles land under ``data/bronze/fhir/`` partitioned into cohorts (``cohort=A`` ...),
which the streaming simulation later replays one partition at a time (spec §5.2).

This module is the only place that talks to S3 directly; it uses the AWS CLI's
parallel ``s3 sync`` for throughput. Everything downstream reads from local Bronze.
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

S3_FHIR_PREFIX = "s3://synthea-open-data/coherent/unzipped/fhir/"
DEFAULT_BRONZE = Path("data/bronze")
DEFAULT_COHORTS = ("A", "B", "C")


@dataclass
class IngestManifest:
    """Provenance record written to ``data/bronze/_metadata/manifest.json``."""

    source: str
    ingested_at: str
    file_count: int
    total_bytes: int
    cohorts: dict[str, int]  # cohort label -> file count

    def write(self, bronze_root: Path) -> Path:
        """Write the manifest JSON under ``<bronze_root>/_metadata/``."""
        meta_dir = bronze_root / "_metadata"
        meta_dir.mkdir(parents=True, exist_ok=True)
        path = meta_dir / "manifest.json"
        path.write_text(json.dumps(asdict(self), indent=2))
        return path


def _aws_sync(prefix: str, dest: Path, max_files: int | None = None) -> None:
    """Run ``aws s3 sync`` (parallel, no-sign-request) from ``prefix`` to ``dest``.

    Args:
        prefix: Source S3 prefix.
        dest: Local destination directory (created if missing).
        max_files: If set, only the first N bundles are fetched (dev/test runs).

    Raises:
        RuntimeError: If the AWS CLI is unavailable or the sync exits non-zero.
    """
    dest.mkdir(parents=True, exist_ok=True)
    if shutil.which("aws") is None:
        raise RuntimeError("AWS CLI not found on PATH; required for Bronze ingest")

    cmd = ["aws", "s3", "sync", prefix, str(dest), "--no-sign-request"]
    if max_files is not None:
        # sync has no count limit; exclude everything then include the first N names.
        names = _list_first_n(prefix, max_files)
        cmd += ["--exclude", "*"]
        for name in names:
            cmd += ["--include", name]

    logger.info("Starting S3 sync to %s", dest)
    result = subprocess.run(cmd, capture_output=True, text=True)  # noqa: S603
    if result.returncode != 0:
        raise RuntimeError(f"aws s3 sync failed: {result.stderr.strip()[:500]}")


def _list_first_n(prefix: str, n: int) -> list[str]:
    """Return the first ``n`` object basenames under an S3 prefix, sorted."""
    out = subprocess.run(  # noqa: S603
        ["aws", "s3", "ls", prefix, "--no-sign-request"],
        capture_output=True,
        text=True,
        check=True,
    )
    names = [
        line.split()[-1]
        for line in out.stdout.splitlines()
        if line.strip() and line.split()[-1].endswith(".json")
    ]
    return sorted(names)[:n]


def assign_cohort(index: int, n_cohorts: int = len(DEFAULT_COHORTS)) -> str:
    """Map a sorted-file index to a cohort label (round-robin, deterministic).

    Round-robin keeps cohorts balanced and order-independent, so each cohort is a
    representative sample of the population rather than an alphabetic slice.
    """
    return DEFAULT_COHORTS[index % n_cohorts]


def partition_into_cohorts(fhir_dir: Path, n_cohorts: int = len(DEFAULT_COHORTS)) -> dict[str, int]:
    """Move flat-downloaded bundles into ``cohort=<label>/`` subdirectories.

    Idempotent: files already inside a ``cohort=*`` dir are left in place.

    Args:
        fhir_dir: ``data/bronze/fhir`` directory holding the downloaded ``*.json``.
        n_cohorts: Number of cohort partitions.

    Returns:
        ``{cohort_label: file_count}``.
    """
    flat = sorted(p for p in fhir_dir.glob("*.json") if p.is_file())
    counts: dict[str, int] = {label: 0 for label in DEFAULT_COHORTS[:n_cohorts]}

    for i, src in enumerate(flat):
        label = assign_cohort(i, n_cohorts)
        cohort_dir = fhir_dir / f"cohort={label}"
        cohort_dir.mkdir(parents=True, exist_ok=True)
        src.rename(cohort_dir / src.name)
        counts[label] += 1

    # Count any pre-existing cohort files (idempotent re-runs).
    for label in counts:
        counts[label] = len(list((fhir_dir / f"cohort={label}").glob("*.json")))
    return counts


def download_fhir(
    bronze_root: Path = DEFAULT_BRONZE,
    max_files: int | None = None,
    n_cohorts: int = len(DEFAULT_COHORTS),
) -> IngestManifest:
    """Sync FHIR bundles from S3, partition into cohorts, and write a manifest.

    Args:
        bronze_root: Bronze layer root (default ``data/bronze``).
        max_files: Limit the number of bundles (dev runs). ``None`` = full dataset.
        n_cohorts: Number of cohort partitions for streaming simulation.

    Returns:
        The :class:`IngestManifest` describing the landed files.
    """
    fhir_dir = bronze_root / "fhir"
    _aws_sync(S3_FHIR_PREFIX, fhir_dir, max_files=max_files)
    cohorts = partition_into_cohorts(fhir_dir, n_cohorts=n_cohorts)

    files = list(fhir_dir.glob("cohort=*/*.json"))
    manifest = IngestManifest(
        source=S3_FHIR_PREFIX,
        ingested_at=datetime.now(UTC).isoformat(),
        file_count=len(files),
        total_bytes=sum(f.stat().st_size for f in files),
        cohorts=cohorts,
    )
    manifest.write(bronze_root)
    logger.info("Landed %d bundles across %d cohorts", manifest.file_count, n_cohorts)
    return manifest


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Download Synthea Coherent FHIR bundles to Bronze")
    ap.add_argument("--bronze-root", type=Path, default=DEFAULT_BRONZE)
    ap.add_argument("--max-files", type=int, default=None, help="Limit bundle count (dev runs)")
    ap.add_argument("--cohorts", type=int, default=len(DEFAULT_COHORTS))
    args = ap.parse_args()

    m = download_fhir(args.bronze_root, max_files=args.max_files, n_cohorts=args.cohorts)
    print(json.dumps(asdict(m), indent=2))
