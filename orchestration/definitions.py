"""Top-level Dagster :class:`Definitions` for the lakehouse (ADR-015, ADR-016).

``dagster dev`` loads this module via ``[tool.dagster] module_name`` in
``pyproject.toml`` and renders the medallion asset graph in the UI.

The wiring is intentionally tiny — all the substance lives in the per-concern
modules (:mod:`assets`, :mod:`checks`, :mod:`sensors`, :mod:`resources`,
:mod:`partitions`). Keeping ``Definitions`` thin makes it trivial to add a new
asset or resource without touching this file.
"""

from __future__ import annotations

from dagster import Definitions

from orchestration.assets import bronze_fhir, gold_encounter_summary, silver_tables
from orchestration.checks import silver_asset_checks
from orchestration.resources import PlatformResource
from orchestration.sensors import bronze_cohort_sensor

defs = Definitions(
    assets=[bronze_fhir, silver_tables, gold_encounter_summary],
    asset_checks=silver_asset_checks,
    sensors=[bronze_cohort_sensor],
    # ``platform`` is the resource key — Pythonic resource injection matches it to
    # the ``platform: PlatformResource`` parameter on every asset / check.
    resources={"platform": PlatformResource()},
)
