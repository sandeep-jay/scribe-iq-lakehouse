"""FabricPlatform — Microsoft Fabric implementation of LakehousePlatform.

Bronze/Silver/Gold land in a OneLake-backed Lakehouse; Spark via the attached
session; ingest_log and metrics emitted through the same interface used by
LocalLitePlatform so transforms and the Dagster asset graph remain unchanged.

This module is **not implemented yet** — Session 5 work. The class is wired
into the factory at `fabric.platform.FabricPlatform` (see
`core/platform/factory.py`) and every method currently raises
``NotImplementedError`` so accidental dispatch fails loudly rather than silently
returning empty results.

See:
    - ADR-001 (Fabric-first)
    - ADR-002 (platform abstraction)
    - ADR-017 (multi-platform repo layout)
    - ADR-018 (CI/CD monorepo)
    - docs/roadmap/multi-platform-reorg.md
    - fabric/docs/DEPLOYMENT.md
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from core.platform.base import LakehousePlatform


class FabricPlatform(LakehousePlatform):
    """Microsoft Fabric platform — OneLake Delta + Fabric Spark session."""

    def storage_path(self, layer: str, table: str) -> str:
        raise NotImplementedError("FabricPlatform implemented in Session 5")

    def read_bronze_fhir(self, cohort: str | None = None) -> list[dict]:
        raise NotImplementedError("FabricPlatform implemented in Session 5")

    def write_silver(self, table: str, data: pa.Table, mode: str = "merge") -> None:
        raise NotImplementedError("FabricPlatform implemented in Session 5")

    def read_silver(self, table: str) -> pa.Table:
        raise NotImplementedError("FabricPlatform implemented in Session 5")

    def write_gold(self, table: str, data: pa.Table) -> None:
        raise NotImplementedError("FabricPlatform implemented in Session 5")

    def log_metric(self, table: str, metric: str, value: Any) -> None:
        raise NotImplementedError("FabricPlatform implemented in Session 5")

    def send_alert(self, severity: str, message: str) -> None:
        raise NotImplementedError("FabricPlatform implemented in Session 5")

    def get_spark_session(self) -> Any | None:
        raise NotImplementedError("FabricPlatform implemented in Session 5")

    def table_version(self, layer: str, table: str) -> int | None:
        raise NotImplementedError("FabricPlatform implemented in Session 5")

    def write_gold_manifest(self, manifest: dict) -> None:
        raise NotImplementedError("FabricPlatform implemented in Session 5")
