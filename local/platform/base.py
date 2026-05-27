"""Abstract lakehouse platform interface (ADR-002).

Every concrete platform (Fabric, local-lite, Databricks, AWS, GCP) implements this
interface. Transforms in ``local/transforms/`` receive a ``LakehousePlatform`` instance
as a parameter and never import platform-specific code directly. Apache Arrow
(``pyarrow.Table``) is the interchange format for all reads/writes (ADR-004).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pyarrow as pa

# Medallion layers, used to validate ``storage_path`` calls across implementations.
LAYERS: tuple[str, ...] = ("bronze", "silver", "gold")


class LakehousePlatform(ABC):
    """Cloud/engine-agnostic I/O surface for the medallion lakehouse.

    Implementations translate these calls to platform-native operations
    (OneLake + Spark on Fabric, delta-rs + Polars locally, etc.). The abstract
    contract is deliberately small: storage paths, layer read/write, and
    observability hooks.
    """

    #: Human-readable platform name, e.g. ``"fabric"`` or ``"local_lite"``.
    name: str = "abstract"

    @abstractmethod
    def storage_path(self, layer: str, table: str) -> str:
        """Return the fully-qualified storage URI for a layer/table.

        Args:
            layer: One of ``"bronze"``, ``"silver"``, ``"gold"``.
            table: Logical table name (without layer prefix), e.g. ``"patient"``.

        Returns:
            A platform-native URI, for example::

                Fabric:     abfss://lakehouse@onelake.dfs.fabric.microsoft.com/silver/patient
                Databricks: dbfs:/mnt/lakehouse/silver/patient
                AWS:        s3://bucket/lakehouse/silver/patient
                GCP:        gs://bucket/lakehouse/silver/patient
                Local:      data/silver/patient

        Raises:
            ValueError: If ``layer`` is not a recognized medallion layer.
        """

    @abstractmethod
    def read_bronze_fhir(self, cohort: str | None = None) -> list[dict]:
        """Read raw FHIR bundles from the Bronze layer.

        Args:
            cohort: Optional cohort partition (e.g. ``"A"``). ``None`` reads all cohorts.

        Returns:
            A list of parsed FHIR bundle dicts (one per patient).
        """

    @abstractmethod
    def write_silver(self, table: str, data: pa.Table, mode: str = "merge") -> None:
        """Write a PyArrow table to a Silver Delta table.

        Args:
            table: Silver table name (without ``silver.`` prefix).
            data: Records to write, as a PyArrow table.
            mode: ``"merge"`` (upsert / CDC), ``"append"``, or ``"overwrite"``.
        """

    @abstractmethod
    def read_silver(self, table: str) -> pa.Table:
        """Read a Silver Delta table as a PyArrow table.

        Args:
            table: Silver table name (without ``silver.`` prefix).

        Returns:
            The table contents as a PyArrow table.
        """

    @abstractmethod
    def write_gold(self, table: str, data: pa.Table) -> None:
        """Write a PyArrow table to a Gold Delta table.

        Args:
            table: Gold table name (without ``gold.`` prefix).
            data: Records to write, as a PyArrow table.
        """

    @abstractmethod
    def log_metric(self, table: str, metric: str, value: Any) -> None:
        """Emit a platform-appropriate metric.

        Implementations route to Fabric monitoring, Databricks metrics, CloudWatch,
        etc. The local tier logs via the standard ``logging`` module.

        Args:
            table: Table the metric pertains to.
            metric: Metric name, e.g. ``"row_count"``.
            value: Metric value.
        """

    @abstractmethod
    def send_alert(self, severity: str, message: str) -> None:
        """Send an alert through the platform's alerting channel.

        Args:
            severity: ``"info"``, ``"warning"``, or ``"critical"``.
            message: Alert body. Must never contain patient_id, encounter_id, or PHI.

        Notes:
            Fabric -> Data Activator, Databricks -> SQL Alerts, AWS -> SNS,
            GCP -> Cloud Monitoring, Local -> log only.
        """

    @abstractmethod
    def get_spark_session(self) -> Any | None:
        """Return the active Spark session, or ``None`` for Spark-free platforms.

        Returns:
            A ``SparkSession`` on Fabric / local-Spark tiers; ``None`` on the
            Polars-based ``LocalLitePlatform``.
        """

    def table_version(self, layer: str, table: str) -> int | None:
        """Return the current Delta version of a table, or ``None`` if unavailable.

        Delta-backed platforms (Fabric, local-lite) override this to expose the
        version that Gold records in its ``silver_versions`` lineage struct. Engines
        without table versioning, or callers reading a non-existent table, get ``None``.

        Args:
            layer: One of ``"bronze"``, ``"silver"``, ``"gold"``.
            table: Logical table name (without layer prefix).
        """
        self._validate_layer(layer)
        return None

    def write_gold_manifest(self, manifest: dict) -> None:
        """Persist the Gold corpus manifest (lineage JSON) alongside the Gold tables.

        Args:
            manifest: JSON-serializable manifest produced by
                :func:`local.gold.corpus_manifest.build_corpus_manifest`.

        Raises:
            NotImplementedError: If the platform has no manifest sink configured.
        """
        raise NotImplementedError(f"{type(self).__name__} does not implement write_gold_manifest")

    def _validate_layer(self, layer: str) -> None:
        """Guard helper for implementations: raise on an unknown medallion layer.

        Args:
            layer: Layer name to validate.

        Raises:
            ValueError: If ``layer`` is not in :data:`LAYERS`.
        """
        if layer not in LAYERS:
            raise ValueError(f"Unknown layer {layer!r}; expected one of {LAYERS}")

    def __repr__(self) -> str:  # noqa: D105 - trivial
        return f"<{type(self).__name__} name={self.name!r}>"
