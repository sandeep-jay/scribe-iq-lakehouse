"""LocalLitePlatform — Polars + delta-rs implementation of the lakehouse (ADR-002/003).

The zero-cloud tier: reads Bronze FHIR JSON from the local filesystem and writes
Silver/Gold as Delta tables via ``deltalake`` (delta-rs), with Change Data Feed
enabled on creation to mirror the Fabric CDC contract (non-negotiable #5). No Spark
and no cloud credentials — runs anywhere ``pip install`` works.

Storage root defaults to ``data/`` and is overridable via ``LAKEHOUSE_LOCAL_ROOT``.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable, write_deltalake

from core.platform.base import LakehousePlatform
from core.redaction import redact
from core.transforms.registry import SILVER_PRIMARY_KEYS

logger = logging.getLogger(__name__)

# Table property that turns on CDC for downstream change-feed consumers.
_CDC_CONFIG = {"delta.enableChangeDataFeed": "true"}

# Repo root (the directory containing ``local/``, ``data/``, ``orchestration/``)
# — used to anchor relative storage roots so the platform is CWD-independent.
# Dagster sensor-triggered runs spawn from the daemon's working directory, which
# is not necessarily the repo root; without this anchor, ``Path("data")`` would
# resolve to a non-existent path inside that CWD.
_REPO_ROOT = Path(__file__).resolve().parents[2]


def _duplicate_row_count(table: pa.Table, key: str) -> int:
    """Return total duplicate rows on ``key`` (rows minus distinct key values)."""
    if table.num_rows == 0:
        return 0
    distinct = pc.count_distinct(table.column(key)).as_py()
    return table.num_rows - distinct


def _dedup_target(table: pa.Table, key: str) -> pa.Table:
    """Drop duplicates on ``key``, keeping one deterministic survivor per key.

    Used only on the target side before MERGE (ADR-019). Delta does not preserve
    write order on read, so "last write wins" is undefined here — we keep the
    row at the smallest read-order index per key (deterministic, but arbitrary).
    Any caller that needs the *canonical* value for a key should provide it in
    the MERGE source; the source-side row will overwrite the survivor.
    """
    indexed = table.append_column("_idx", pa.array(range(table.num_rows), type=pa.int64()))
    grouped = indexed.group_by([key]).aggregate([("_idx", "min")])
    mask = pc.is_in(indexed.column("_idx"), value_set=grouped.column("_idx_min"))
    return indexed.filter(mask).drop_columns(["_idx"])


class LocalLitePlatform(LakehousePlatform):
    """Filesystem + delta-rs lakehouse for local development and CI."""

    name = "local_lite"

    def __init__(self, root: str | os.PathLike[str] | None = None) -> None:
        """Initialize with a storage root (env ``LAKEHOUSE_LOCAL_ROOT`` or ``data``).

        Relative paths are anchored to the repo root (the directory containing
        this package) — never to ``Path.cwd()`` — so the platform behaves
        identically under the CLI (run from repo root) and under Dagster
        sensor-triggered runs (spawned from the daemon's CWD).
        """
        raw = Path(root or os.getenv("LAKEHOUSE_LOCAL_ROOT", "data"))
        self.root = raw if raw.is_absolute() else (_REPO_ROOT / raw).resolve()

    # ----------------------------------------------------------------- paths

    def storage_path(self, layer: str, table: str) -> str:
        """Return ``<root>/<layer>/<table>`` (e.g. ``data/silver/patient``)."""
        self._validate_layer(layer)
        return str(self.root / layer / table)

    # ----------------------------------------------------------------- bronze

    def read_bronze_fhir(self, cohort: str | None = None) -> list[dict]:
        """Load FHIR bundle dicts from ``<root>/bronze/fhir/cohort=*/``.

        Args:
            cohort: Specific cohort label (e.g. ``"A"``); ``None`` reads all cohorts.

        Returns:
            Parsed FHIR bundle dicts. Files that fail to parse are skipped with a warning.
        """
        fhir_dir = self.root / "bronze" / "fhir"
        pattern = f"cohort={cohort}/*.json" if cohort else "cohort=*/*.json"
        bundles: list[dict] = []
        for path in sorted(fhir_dir.glob(pattern)):
            try:
                bundles.append(json.loads(path.read_text()))
            except (json.JSONDecodeError, OSError):
                logger.warning("Skipping unreadable bundle %s", redact(path.name))
        return bundles

    def iter_bronze_files(self, cohort: str | None = None):
        """Yield ``(path, bundle_dict)`` pairs without loading all bundles at once.

        Memory-friendly for full-dataset runs (4.3 GiB / 1,281 bundles): the pipeline
        can stream one bundle at a time instead of holding them all in RAM.
        """
        fhir_dir = self.root / "bronze" / "fhir"
        pattern = f"cohort={cohort}/*.json" if cohort else "cohort=*/*.json"
        for path in sorted(fhir_dir.glob(pattern)):
            try:
                yield path, json.loads(path.read_text())
            except (json.JSONDecodeError, OSError):
                logger.warning("Skipping unreadable bundle %s", redact(path.name))

    # ------------------------------------------------------------ silver/gold

    def write_silver(self, table: str, data: pa.Table, mode: str = "merge") -> None:
        """Write a PyArrow table to a Silver Delta table.

        ``merge`` upserts on the table's primary key (CDC-friendly); ``append`` and
        ``overwrite`` map to the matching delta-rs write modes. CDF is enabled when
        the table is first created.
        """
        self._write_delta(self.storage_path("silver", table), data, mode, table)

    def read_silver(self, table: str) -> pa.Table:
        """Read a Silver Delta table as a PyArrow table."""
        return DeltaTable(self.storage_path("silver", table)).to_pyarrow_table()

    def write_gold(self, table: str, data: pa.Table) -> None:
        """Write a PyArrow table to a Gold Delta table (overwrite semantics)."""
        self._write_delta(self.storage_path("gold", table), data, "overwrite", table)

    def read_gold(self, table: str) -> pa.Table:
        """Read a Gold Delta table as a PyArrow table."""
        return DeltaTable(self.storage_path("gold", table)).to_pyarrow_table()

    def table_version(self, layer: str, table: str) -> int | None:
        """Return the Delta version of a table, or ``None`` if it does not exist yet."""
        path = self.storage_path(layer, table)
        if not DeltaTable.is_deltatable(path):
            return None
        return DeltaTable(path).version()

    def write_gold_manifest(self, manifest: dict) -> None:
        """Write the corpus manifest to ``<root>/gold/_metadata/corpus_manifest.json``."""
        meta_dir = Path(self.storage_path("gold", "_metadata"))
        meta_dir.mkdir(parents=True, exist_ok=True)
        (meta_dir / "corpus_manifest.json").write_text(json.dumps(manifest, indent=2))

    def _write_delta(self, path: str, data: pa.Table, mode: str, table: str) -> None:
        """Create-or-upsert a Delta table at ``path`` honoring the requested mode."""
        exists = DeltaTable.is_deltatable(path)

        if mode == "merge" and exists:
            key = SILVER_PRIMARY_KEYS.get(table)
            if key is None:
                raise ValueError(f"No primary key registered for table {table!r}; cannot merge")
            dt = DeltaTable(path)

            # ADR-019: legacy OVERWRITE writes (made before dedup_by_key landed
            # in every build_silver_*) can leave duplicate primary keys in the
            # target table. delta-rs MERGE then fails with "matched a target row
            # with multiple source rows". Detect-and-rewrite the deduped target
            # only when dups exist — happy path stays fast.
            existing = dt.to_pyarrow_table()
            dup_rows = _duplicate_row_count(existing, key)
            if dup_rows > 0:
                logger.warning(
                    "Target table %r has %d duplicate %s rows; rewriting deduped before MERGE",
                    table,
                    dup_rows,
                    key,
                )
                deduped = _dedup_target(existing, key)
                write_deltalake(path, deduped, mode="overwrite", configuration=_CDC_CONFIG)
                dt = DeltaTable(path)

            (
                dt.merge(
                    source=data,
                    predicate=f"target.{key} = source.{key}",
                    source_alias="source",
                    target_alias="target",
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
            return

        # First write (or explicit overwrite/append): create with CDC enabled.
        write_mode = "append" if (mode == "append" and exists) else "overwrite"
        write_deltalake(path, data, mode=write_mode, configuration=_CDC_CONFIG)

    # --------------------------------------------------------- observability

    def log_metric(self, table: str, metric: str, value: Any) -> None:
        """Log a metric (local tier emits a structured log line)."""
        logger.info("metric table=%s %s=%s", table, metric, value)

    def send_alert(self, severity: str, message: str) -> None:
        """Emit an alert to the log (local tier has no external alerting)."""
        logger.log(
            logging.CRITICAL if severity == "critical" else logging.WARNING,
            "ALERT [%s] %s",
            severity,
            message,
        )

    def get_spark_session(self) -> None:
        """No Spark in the lite tier."""
        return None
