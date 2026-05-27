"""Platform factory — one env var selects the lakehouse implementation (ADR-002).

Set ``LAKEHOUSE_PLATFORM`` to one of the keys in :data:`PLATFORMS` and call
:func:`get_platform`. The default is ``local_lite`` so transforms and tests run
with zero cloud configuration.

Migrating to a new engine is one env var change plus one new class in this package —
no transform code changes.
"""

from __future__ import annotations

import importlib
import os

from local.platform.base import LakehousePlatform

#: Env var key -> ``"module.path.ClassName"`` for each supported platform.
PLATFORMS: dict[str, str] = {
    "fabric": "local.platform.fabric.FabricPlatform",
    "databricks": "local.platform.databricks.DatabricksPlatform",
    "aws": "local.platform.aws.AWSPlatform",
    "gcp": "local.platform.gcp.GCPPlatform",
    "local_spark": "local.platform.local_spark.LocalSparkPlatform",
    "local_lite": "local.platform.local_lite.LocalLitePlatform",
}

DEFAULT_PLATFORM = "local_lite"
ENV_VAR = "LAKEHOUSE_PLATFORM"


def get_platform(name: str | None = None) -> LakehousePlatform:
    """Instantiate the configured lakehouse platform.

    Args:
        name: Explicit platform key. If ``None``, reads the ``LAKEHOUSE_PLATFORM``
            env var, falling back to ``"local_lite"``.

    Returns:
        A concrete :class:`~local.platform.base.LakehousePlatform` instance.

    Raises:
        ValueError: If the resolved platform name is not registered.
        ImportError: If the platform module/class is registered but not yet implemented.
    """
    key = (name or os.getenv(ENV_VAR, DEFAULT_PLATFORM)).strip().lower()
    if key not in PLATFORMS:
        raise ValueError(f"Unknown platform {key!r}. Set {ENV_VAR} to one of: {sorted(PLATFORMS)}")
    module_path, class_name = PLATFORMS[key].rsplit(".", 1)
    module = importlib.import_module(module_path)
    platform_cls = getattr(module, class_name)
    return platform_cls()
