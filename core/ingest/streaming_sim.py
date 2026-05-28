"""Streaming simulation — replay Bronze cohort partitions as a faux Auto Loader stream.

Production uses Fabric Auto Loader (Structured Streaming) to pick up new files as
they land. Locally we simulate the same trigger pattern by replaying cohort
partitions one at a time and invoking a per-cohort callback — the local analogue of
"a new patient batch arrived" (see STREAMING_DESIGN.md and spec §5.2).

Two modes:
    replay_cohorts()  — deterministic sequential replay (used by the pipeline/tests).
    watch()           — optional watchdog-based watcher that fires when a new cohort
                        directory appears (true file-arrival simulation).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path

from core.ingest.bronze_landing import DEFAULT_BRONZE, cohort_files, cohort_labels

logger = logging.getLogger(__name__)

CohortCallback = Callable[[str, list[Path]], None]


def replay_cohorts(
    on_cohort: CohortCallback,
    bronze_root: Path = DEFAULT_BRONZE,
    cohorts: list[str] | None = None,
) -> list[str]:
    """Replay cohort partitions sequentially, invoking ``on_cohort`` for each.

    Args:
        on_cohort: Called as ``on_cohort(label, files)`` per cohort, simulating a
            micro-batch trigger.
        bronze_root: Bronze layer root.
        cohorts: Specific cohort labels to replay; defaults to all present.

    Returns:
        The cohort labels processed, in order.
    """
    labels = cohorts if cohorts is not None else cohort_labels(bronze_root)
    for label in labels:
        files = cohort_files(label, bronze_root)
        logger.info("Stream trigger: cohort=%s (%d files)", label, len(files))
        on_cohort(label, files)
    return labels


def watch(
    on_cohort: CohortCallback,
    bronze_root: Path = DEFAULT_BRONZE,
    timeout: float | None = None,
) -> None:
    """Watch ``<root>/fhir/`` for new ``cohort=*`` directories and fire on arrival.

    Uses watchdog to mimic Auto Loader's file-arrival trigger. Intended for demos;
    the pipeline itself uses :func:`replay_cohorts` for deterministic runs.

    Args:
        on_cohort: Callback invoked when a new cohort directory is created.
        bronze_root: Bronze layer root to watch.
        timeout: Seconds to watch before returning; ``None`` blocks until interrupted.
    """
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer

    fhir_dir = bronze_root / "fhir"
    fhir_dir.mkdir(parents=True, exist_ok=True)

    class _CohortHandler(FileSystemEventHandler):
        def on_created(self, event) -> None:  # noqa: ANN001 - watchdog signature
            name = Path(event.src_path).name
            if event.is_directory and name.startswith("cohort="):
                label = name.split("=", 1)[1]
                on_cohort(label, cohort_files(label, bronze_root))

    observer = Observer()
    observer.schedule(_CohortHandler(), str(fhir_dir), recursive=False)
    observer.start()
    try:
        observer.join(timeout=timeout)
    finally:
        observer.stop()
        observer.join()
