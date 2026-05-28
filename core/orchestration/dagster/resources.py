"""Dagster resource that vends the configured :class:`LakehousePlatform` (ADR-015).

The orchestration tier deliberately does not subclass or re-implement the platform
abstraction — it *composes* one via :func:`core.platform.factory.get_platform`,
so a Dagster run honours ``LAKEHOUSE_PLATFORM`` exactly the way the CLI does
(ADR-002). The resource is intentionally thin: configuration is the platform name
(or ``None`` to read the env var), and ``create()`` returns a fresh platform
instance per call — platforms are stateless wrappers over the filesystem / cloud
SDK, so re-instantiation is cheap.
"""

from __future__ import annotations

from dagster import ConfigurableResource

from core.platform.base import LakehousePlatform
from core.platform.factory import get_platform


class PlatformResource(ConfigurableResource):
    """Pythonic Dagster resource that constructs a :class:`LakehousePlatform`.

    Attributes:
        platform_name: Explicit platform key (e.g. ``"local_lite"``, ``"fabric"``).
            ``None`` (the default) delegates to ``LAKEHOUSE_PLATFORM`` via the
            factory — matches CLI behaviour.
    """

    platform_name: str | None = None

    def create(self) -> LakehousePlatform:
        """Return a fresh platform instance for the current Dagster step.

        Returns:
            A concrete :class:`LakehousePlatform` chosen by the factory.
        """
        return get_platform(self.platform_name)
