"""FabricPlatform — Microsoft Fabric implementation of LakehousePlatform.

Bronze/Silver/Gold live in a OneLake-backed Lakehouse; Spark via the attached
session; ingest_log and metrics emitted through the same interface used by
LocalLitePlatform so transforms and the Dagster asset graph remain unchanged.

Spark, ``delta-spark``, and ``notebookutils.mssparkutils`` are imported lazily
inside the methods that need them. This keeps the module importable in any
environment (CI, local dev without Fabric, offline contract tests) — only
calling a method that actually needs Spark forces the import.

Storage layout assumes a **schema-enabled** Lakehouse (Fabric default for new
Lakehouses since 2024):

    Tables/<layer>/<table>      Delta tables (silver, gold)
    Files/bronze/fhir/...       Raw FHIR JSON
    Files/gold/_metadata/...    Corpus manifest

See:
    - ADR-001 (Fabric-first)
    - ADR-002 (platform abstraction)
    - ADR-009 (Delta + CDC contract)
    - ADR-017 (multi-platform repo layout)
    - ADR-018 (CI/CD monorepo)
    - ADR-019 (Silver MERGE idempotency — target-dedup guard)
    - docs/roadmap/fabric-execution-plan.md (Phase 2 — this module)
    - fabric/docs/DEPLOYMENT.md
"""

from __future__ import annotations

import json
import logging
from typing import Any

import pyarrow as pa

from core.platform.base import LakehousePlatform
from core.transforms.registry import SILVER_PRIMARY_KEYS

logger = logging.getLogger(__name__)

_CDC_PROPERTY = "delta.enableChangeDataFeed"
_ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com"


class FabricPlatform(LakehousePlatform):
    """Microsoft Fabric platform — OneLake Delta + Fabric Spark session.

    All Spark/notebookutils interactions are deferred to method call time so this
    class can be imported (and its contract verified) outside a Fabric runtime.
    """

    name = "fabric"

    def __init__(
        self,
        workspace_id: str | None = None,
        lakehouse_name: str | None = None,
        spark: Any | None = None,
    ) -> None:
        """Initialize with optional overrides for testability.

        Args:
            workspace_id: Fabric workspace GUID. Falls back to
                ``mssparkutils.env.getWorkspaceId()`` on first method call.
            lakehouse_name: Lakehouse name (e.g. ``"scribe_iq"``). Falls back to
                the attached Lakehouse via ``mssparkutils.lakehouse.get()``.
            spark: Pre-existing ``SparkSession``. Falls back to the Fabric-injected
                global ``spark`` on first method call.
        """
        self._workspace_id = workspace_id
        self._lakehouse_name = lakehouse_name
        self._spark = spark

    # ------------------------------------------------------- lazy environment

    def ensure_env(self) -> tuple[str, str]:
        """Resolve workspace + lakehouse names from mssparkutils on first call.

        Public — notebooks need this to construct ad-hoc paths (e.g. the corpus
        manifest path in ``10_gold_validation``) without re-implementing the
        env resolution.
        """
        if self._workspace_id is None or self._lakehouse_name is None:
            try:
                import notebookutils.mssparkutils as msu
            except ImportError as exc:
                raise RuntimeError(
                    "FabricPlatform needs workspace_id + lakehouse_name; pass them "
                    "explicitly or run inside a Fabric notebook where "
                    "notebookutils.mssparkutils is available."
                ) from exc
            if self._workspace_id is None:
                self._workspace_id = msu.env.getWorkspaceId()
            if self._lakehouse_name is None:
                self._lakehouse_name = msu.lakehouse.get()["displayName"]
        return self._workspace_id, self._lakehouse_name

    # Backward-compat alias — kept so existing internal callers don't break.
    _ensure_env = ensure_env

    def get_spark_session(self) -> Any | None:
        """Return the attached Fabric SparkSession (injected as global ``spark``)."""
        if self._spark is not None:
            return self._spark
        try:
            from pyspark.sql import SparkSession
        except ImportError:
            return None
        self._spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        return self._spark

    # ----------------------------------------------------------------- paths

    def storage_path(self, layer: str, table: str) -> str:
        """Return the OneLake abfss URI for a Delta table or raw-file root."""
        self._validate_layer(layer)
        workspace_id, lakehouse_name = self._ensure_env()
        base = f"abfss://{workspace_id}@{_ONELAKE_HOST}/{lakehouse_name}.Lakehouse"
        if layer == "bronze":
            return f"{base}/Files/bronze/{table}"
        return f"{base}/Tables/{layer}/{table}"

    # ----------------------------------------------------------------- bronze

    def read_bronze_fhir(self, cohort: str | None = None) -> list[dict]:
        """Read FHIR bundles from ``Files/bronze/fhir/cohort=*/*.json``."""
        return [bundle for _path, bundle in self.iter_bronze_files(cohort=cohort)]

    def iter_bronze_files(self, cohort: str | None = None):
        """Yield ``(path, bundle_dict)`` pairs from ``Files/bronze/fhir/cohort=*/``.

        Mirrors :meth:`core.platform.local_lite.LocalLitePlatform.iter_bronze_files`.
        Notebooks use this to stamp ``source_file`` on each parsed record for
        provenance (the CLI pipeline does the equivalent in ``_parse_cohort``).
        """
        try:
            import notebookutils.mssparkutils as msu
        except ImportError as exc:
            raise RuntimeError("iter_bronze_files requires Fabric runtime") from exc

        fhir_root = self.storage_path("bronze", "fhir")
        pattern_dir = f"{fhir_root}/cohort={cohort}" if cohort else fhir_root
        for entry in self._walk_json(msu, pattern_dir, recurse=cohort is None):
            try:
                text = msu.fs.head(entry, 1024 * 1024 * 64)  # 64 MB cap per bundle
                yield entry, json.loads(text)
            except (json.JSONDecodeError, OSError) as err:
                logger.warning("Skipping unreadable bundle: %s", err)

    @staticmethod
    def _walk_json(msu: Any, root: str, *, recurse: bool) -> list[str]:
        """Yield .json paths under ``root``; one cohort deep when ``recurse=True``."""
        out: list[str] = []
        for entry in msu.fs.ls(root):
            if entry.isDir and recurse:
                out.extend(e.path for e in msu.fs.ls(entry.path) if e.name.endswith(".json"))
            elif entry.name.endswith(".json"):
                out.append(entry.path)
        return out

    # ----------------------------------------------------------- silver/gold

    def write_silver(self, table: str, data: pa.Table, mode: str = "merge") -> None:
        """Upsert/append/overwrite a Silver Delta table from a ``pa.Table``.

        Convenience wrapper: converts to a Spark DataFrame via pandas, then
        defers to :meth:`write_silver_spark`. Used by LocalLite-style callers
        (CLI pipeline) that already have a ``pa.Table``. For native Fabric
        notebooks that build Spark DataFrames directly, prefer
        :meth:`write_silver_spark` — skips the pa↔pandas round-trip and
        scales beyond driver memory.
        """
        spark = self.get_spark_session()
        if spark is None:
            raise RuntimeError("write_silver requires an active SparkSession")
        df = spark.createDataFrame(data.to_pandas())
        self.write_silver_spark(table, df, mode=mode)

    def write_silver_spark(self, table: str, df: Any, mode: str = "merge") -> None:
        """Native-Spark write of a Silver Delta table from a Spark DataFrame.

        This is the Fabric-idiomatic write path used by the distributed
        notebooks (ADR-020). Applies the same ADR-019 target-side dedup guard
        before MERGE that the pa.Table path does.

        Args:
            table: Silver table name (registry key).
            df: Spark DataFrame matching ``SILVER_TABLES[table].schema``.
            mode: ``"merge"`` (upsert on primary key), ``"append"``, or
                ``"overwrite"``.
        """
        self._write_delta_spark(self.storage_path("silver", table), df, mode, table)

    def read_silver(self, table: str) -> pa.Table:
        """Read a Silver Delta table as ``pa.Table`` (driver-side; small tables only).

        For large tables or any distributed downstream work, prefer
        :meth:`read_silver_spark` — keeps data in Spark, avoids the
        driver-memory bottleneck of ``.toPandas()``.
        """
        return pa.Table.from_pandas(self.read_silver_spark(table).toPandas())

    def read_silver_spark(self, table: str) -> Any:
        """Read a Silver Delta table as a Spark DataFrame (distributed)."""
        spark = self.get_spark_session()
        if spark is None:
            raise RuntimeError("read_silver_spark requires an active SparkSession")
        return spark.read.format("delta").load(self.storage_path("silver", table))

    def write_gold(self, table: str, data: pa.Table) -> None:
        """Overwrite a Gold Delta table from a ``pa.Table``.

        Convenience wrapper for the pa.Table callers; see
        :meth:`write_gold_spark` for the Spark-native path.
        """
        spark = self.get_spark_session()
        if spark is None:
            raise RuntimeError("write_gold requires an active SparkSession")
        df = spark.createDataFrame(data.to_pandas())
        self.write_gold_spark(table, df)

    def write_gold_spark(self, table: str, df: Any) -> None:
        """Native-Spark overwrite of a Gold Delta table from a Spark DataFrame."""
        self._write_delta_spark(self.storage_path("gold", table), df, "overwrite", table)

    def read_gold_spark(self, table: str) -> Any:
        """Read a Gold Delta table as a Spark DataFrame (distributed)."""
        spark = self.get_spark_session()
        if spark is None:
            raise RuntimeError("read_gold_spark requires an active SparkSession")
        return spark.read.format("delta").load(self.storage_path("gold", table))

    def table_version(self, layer: str, table: str) -> int | None:
        """Return the latest Delta version for ``<layer>.<table>``, or ``None``."""
        spark = self.get_spark_session()
        if spark is None:
            return None
        try:
            from delta.tables import DeltaTable as SparkDeltaTable
        except ImportError:
            return None
        try:
            dt = SparkDeltaTable.forPath(spark, self.storage_path(layer, table))
            row = dt.history(1).first()
            return int(row["version"]) if row is not None else None
        except Exception as err:  # noqa: BLE001 - any Delta error means "no version"
            logger.debug("table_version lookup failed for %s.%s: %s", layer, table, err)
            return None

    def write_gold_manifest(self, manifest: dict) -> None:
        """Write corpus manifest JSON to ``Files/gold/_metadata/corpus_manifest.json``."""
        try:
            import notebookutils.mssparkutils as msu
        except ImportError as exc:
            raise RuntimeError("write_gold_manifest requires Fabric runtime") from exc
        workspace_id, lakehouse_name = self._ensure_env()
        base = f"abfss://{workspace_id}@{_ONELAKE_HOST}/{lakehouse_name}.Lakehouse"
        path = f"{base}/Files/gold/_metadata/corpus_manifest.json"
        msu.fs.put(path, json.dumps(manifest, indent=2), True)  # overwrite=True

    # ----------------------------------------------------------- write helper

    def _write_delta_spark(self, path: str, df: Any, mode: str, table: str) -> None:
        """Create-or-upsert a Delta table at ``path`` from a Spark DataFrame.

        Single internal write path. Both the pa.Table-flavoured wrappers
        (:meth:`write_silver`, :meth:`write_gold`) and the Spark-native
        wrappers (:meth:`write_silver_spark`, :meth:`write_gold_spark`)
        ultimately call here. Mirrors :meth:`LocalLitePlatform._write_delta`,
        including the ADR-019 target-side dedup guard before MERGE.
        """
        spark = self.get_spark_session()
        if spark is None:
            raise RuntimeError(f"_write_delta_spark on {path!r} requires an active SparkSession")

        from delta.tables import DeltaTable as SparkDeltaTable

        exists = SparkDeltaTable.isDeltaTable(spark, path)

        if mode == "merge" and exists:
            key = SILVER_PRIMARY_KEYS.get(table)
            if key is None:
                raise ValueError(f"No primary key registered for table {table!r}; cannot merge")

            # ADR-019: target-side dedup guard. Spark equivalent of the LocalLite
            # pyarrow path — count distinct PKs vs total rows, rewrite deduped
            # only if dups exist.
            target_df = spark.read.format("delta").load(path)
            total = target_df.count()
            distinct = target_df.select(key).distinct().count()
            if total != distinct:
                logger.warning(
                    "Target table %r has %d duplicate %s rows; rewriting deduped before MERGE",
                    table,
                    total - distinct,
                    key,
                )
                deduped = target_df.dropDuplicates([key])
                (
                    deduped.write.format("delta")
                    .mode("overwrite")
                    .option(_CDC_PROPERTY, "true")
                    .save(path)
                )

            target = SparkDeltaTable.forPath(spark, path)
            (
                target.alias("target")
                .merge(df.alias("source"), f"target.{key} = source.{key}")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            return

        # First write or explicit overwrite/append: create with CDC enabled.
        write_mode = "append" if (mode == "append" and exists) else "overwrite"
        df.write.format("delta").mode(write_mode).option(_CDC_PROPERTY, "true").save(path)

    # Backward-compat — old internal callers expected _write_delta(pa.Table).
    def _write_delta(self, path: str, data: pa.Table, mode: str, table: str) -> None:
        """Legacy pa.Table internal write path; converts then delegates to Spark."""
        spark = self.get_spark_session()
        if spark is None:
            raise RuntimeError(f"_write_delta on {path!r} requires an active SparkSession")
        df = spark.createDataFrame(data.to_pandas())
        self._write_delta_spark(path, df, mode, table)

    # --------------------------------------------------------- observability

    def log_metric(self, table: str, metric: str, value: Any) -> None:
        """Emit a metric line (captured in the Fabric notebook run logs)."""
        logger.info("metric table=%s %s=%s", table, metric, value)

    def send_alert(self, severity: str, message: str) -> None:
        """Emit an alert; ``critical`` also raises so the notebook fails loudly."""
        logger.log(
            logging.CRITICAL if severity == "critical" else logging.WARNING,
            "ALERT [%s] %s",
            severity,
            message,
        )
        if severity == "critical":
            raise RuntimeError(f"Critical alert: {message}")
