"""FabricPlatform — Microsoft Fabric OneLake + Spark storage adapter.

Pure-Spark, pure-Fabric. No PyArrow round-trips, no ``core.platform`` ABC,
no shared registry — fabric/ is an independent end-to-end implementation
(ADR-022). Bronze lives under ``Files/bronze/...``; Silver and Gold are
Delta tables under ``Tables/silver/...`` and ``Tables/gold/...`` of a
schema-enabled Lakehouse.

All Spark / ``delta-spark`` / ``notebookutils.mssparkutils`` imports are
deferred to method-call time so this module imports cleanly outside a Fabric
runtime (CI, local contract tests).

See:
    - ADR-001  (Fabric-first)
    - ADR-009  (Delta + CDC contract)
    - ADR-017  (multi-platform repo layout)
    - ADR-018  (CI/CD monorepo)
    - ADR-019  (Silver MERGE idempotency — target-dedup guard)
    - ADR-022  (independent per-platform implementations)
    - fabric/docs/DEPLOYMENT.md
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

_CDC_PROPERTY = "delta.enableChangeDataFeed"
_ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com"
_VALID_LAYERS = {"bronze", "silver", "gold"}


class FabricPlatform:
    """Microsoft Fabric platform — OneLake Delta + Fabric Spark session.

    All methods accept / return Spark DataFrames. There is no ``pa.Table``
    interchange on this platform; the Fabric tier builds and writes Delta
    natively end-to-end.
    """

    name = "fabric"

    def __init__(
        self,
        workspace_id: str | None = None,
        lakehouse_name: str | None = None,
        spark: Any | None = None,
    ) -> None:
        self._workspace_id = workspace_id
        self._lakehouse_name = lakehouse_name
        self._spark = spark

    # ------------------------------------------------------- lazy environment

    def ensure_env(self) -> tuple[str, str]:
        """Resolve workspace + lakehouse from Spark conf on first call.

        Fabric injects the attached lakehouse identity into the Spark session
        as ``trident.*`` config keys. These are the documented, stable Fabric
        API for runtime identity — unlike the Synapse-era
        ``mssparkutils.env.getWorkspaceId()`` which doesn't exist on Fabric.
        """
        if self._workspace_id is None or self._lakehouse_name is None:
            spark = self.get_spark_session()
            if spark is None:
                msg = (
                    "FabricPlatform needs workspace_id + lakehouse_name; pass them "
                    "explicitly or run inside a Fabric notebook with an attached "
                    "Spark session."
                )
                raise RuntimeError(msg)
            try:
                if self._workspace_id is None:
                    self._workspace_id = spark.conf.get("trident.workspace.id")
                if self._lakehouse_name is None:
                    self._lakehouse_name = spark.conf.get("trident.lakehouse.name")
            except Exception as exc:  # noqa: BLE001 — Spark raises NoSuchElementException
                msg = (
                    "Couldn't read Fabric identity from Spark conf "
                    "(trident.workspace.id / trident.lakehouse.name). Either no "
                    "lakehouse is attached to this notebook (top bar → Add lakehouse), "
                    "or you're not running inside a Fabric notebook."
                )
                raise RuntimeError(msg) from exc
        return self._workspace_id, self._lakehouse_name

    def get_spark_session(self) -> SparkSession | None:
        """Return the attached Fabric ``SparkSession`` (injected as global ``spark``)."""
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
        """Return the OneLake ``abfss://`` URI for a Delta table or raw-file root."""
        if layer not in _VALID_LAYERS:
            msg = f"Invalid layer {layer!r}; expected one of {sorted(_VALID_LAYERS)}"
            raise ValueError(msg)
        workspace_id, lakehouse_name = self.ensure_env()
        base = f"abfss://{workspace_id}@{_ONELAKE_HOST}/{lakehouse_name}.Lakehouse"
        if layer == "bronze":
            return f"{base}/Files/bronze/{table}"
        return f"{base}/Tables/{layer}/{table}"

    # ----------------------------------------------------------------- bronze

    def read_bronze_bundles_spark(
        self,
        cohort: str | None = None,
        *,
        num_partitions: int | None = None,
    ) -> DataFrame:
        """Read FHIR bundles under ``Files/bronze/fhir/cohort=*`` as a Spark DataFrame.

        Returns a DataFrame with two columns:
            ``path``   — input file URI (one row per bundle)
            ``value``  — raw JSON text of the bundle

        Partitioned across executors so the downstream Spark transforms parse
        and project distributed — no ``applyInPandas`` bridge, no driver-side
        Python loop. This is the canonical Bronze entry point for every
        Spark-native Silver build on Fabric.

        Args:
            cohort: Optional cohort name. If provided, only that cohort's
                bundles are read (``cohort=<name>/*.json``). If omitted, every
                cohort under ``Files/bronze/fhir/`` is read.
            num_partitions: Override Spark's default partition count for the
                read — useful when executor count is high or bundles are
                imbalanced.
        """
        from pyspark.sql import functions as F  # noqa: N812

        spark = self.get_spark_session()
        if spark is None:
            msg = "read_bronze_bundles_spark requires an active SparkSession"
            raise RuntimeError(msg)

        fhir_root = self.storage_path("bronze", "fhir")
        pattern = f"{fhir_root}/cohort={cohort}/*.json" if cohort else f"{fhir_root}/cohort=*/*.json"
        df = (
            spark.read.text(pattern, wholetext=True)
            .withColumn("path", F.input_file_name())
            .select("path", "value")
        )
        if num_partitions is not None:
            df = df.repartition(num_partitions)
        return df

    def iter_bronze_files(self, cohort: str | None = None):
        """Yield ``(path, bundle_dict)`` pairs for small-scale notebook use.

        Driver-side iteration via ``mssparkutils.fs.head`` — fine for
        validation / smoke notebooks (e.g. ``00_setup``, ``10_gold_validation``)
        but not for medallion builds. The distributed Silver builds consume
        :meth:`read_bronze_bundles_spark` instead.
        """
        try:
            import notebookutils.mssparkutils as msu
        except ImportError as exc:
            msg = "iter_bronze_files requires Fabric runtime"
            raise RuntimeError(msg) from exc

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
        out: list[str] = []
        for entry in msu.fs.ls(root):
            if entry.isDir and recurse:
                out.extend(e.path for e in msu.fs.ls(entry.path) if e.name.endswith(".json"))
            elif entry.name.endswith(".json"):
                out.append(entry.path)
        return out

    # ----------------------------------------------------------- silver/gold

    def write_silver_spark(self, table: str, df: DataFrame, mode: str = "merge") -> None:
        """Upsert / append / overwrite a Silver Delta table from a Spark DataFrame.

        Args:
            table: Silver table name (registry key — see
                :mod:`fabric.transforms.registry`).
            df: Spark DataFrame matching ``REGISTRY[table].schema``.
            mode: ``"merge"`` (upsert on primary key), ``"append"``, or
                ``"overwrite"``.
        """
        self._write_delta_spark(self.storage_path("silver", table), df, mode, table)

    def read_silver_spark(self, table: str) -> DataFrame:
        """Read a Silver Delta table as a Spark DataFrame (distributed)."""
        spark = self.get_spark_session()
        if spark is None:
            msg = "read_silver_spark requires an active SparkSession"
            raise RuntimeError(msg)
        return spark.read.format("delta").load(self.storage_path("silver", table))

    def write_gold_spark(self, table: str, df: DataFrame) -> None:
        """Overwrite a Gold Delta table from a Spark DataFrame."""
        self._write_delta_spark(self.storage_path("gold", table), df, "overwrite", table)

    def read_gold_spark(self, table: str) -> DataFrame:
        """Read a Gold Delta table as a Spark DataFrame (distributed)."""
        spark = self.get_spark_session()
        if spark is None:
            msg = "read_gold_spark requires an active SparkSession"
            raise RuntimeError(msg)
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
        except Exception as err:  # noqa: BLE001 - any Delta error means "no version"
            logger.debug("table_version lookup failed for %s.%s: %s", layer, table, err)
            return None
        return int(row["version"]) if row is not None else None

    def write_gold_manifest(self, manifest: dict) -> None:
        """Write the corpus manifest JSON to ``Files/gold/_metadata/``."""
        try:
            import notebookutils.mssparkutils as msu
        except ImportError as exc:
            msg = "write_gold_manifest requires Fabric runtime"
            raise RuntimeError(msg) from exc
        workspace_id, lakehouse_name = self.ensure_env()
        base = f"abfss://{workspace_id}@{_ONELAKE_HOST}/{lakehouse_name}.Lakehouse"
        path = f"{base}/Files/gold/_metadata/corpus_manifest.json"
        msu.fs.put(path, json.dumps(manifest, indent=2), True)  # overwrite=True

    # ----------------------------------------------------------- write helper

    def _write_delta_spark(self, path: str, df: DataFrame, mode: str, table: str) -> None:
        """Create-or-upsert a Delta table at ``path`` from a Spark DataFrame.

        Implements the ADR-019 target-side dedup guard before MERGE: a target
        table with duplicate primary keys (left over from an earlier bug or
        a manual write) is rewritten deduped before the MERGE so the merge
        condition stays single-row-per-key.
        """
        spark = self.get_spark_session()
        if spark is None:
            msg = f"_write_delta_spark on {path!r} requires an active SparkSession"
            raise RuntimeError(msg)

        from delta.tables import DeltaTable as SparkDeltaTable

        exists = SparkDeltaTable.isDeltaTable(spark, path)

        if mode == "merge" and exists:
            # Deferred: importing the registry pulls in fabric.transforms.silver_*,
            # which require pyspark — fine here (we're already inside a write
            # path that needs Spark) but would break offline importability if
            # placed at module top-level.
            from fabric.transforms.registry import REGISTRY as SILVER_REGISTRY

            entry = SILVER_REGISTRY.get(table)
            if entry is None:
                msg = f"No primary key registered for table {table!r}; cannot merge"
                raise ValueError(msg)
            key = entry.primary_key

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

        write_mode = "append" if (mode == "append" and exists) else "overwrite"
        df.write.format("delta").mode(write_mode).option(_CDC_PROPERTY, "true").save(path)

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
            msg = f"Critical alert: {message}"
            raise RuntimeError(msg)
