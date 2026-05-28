"""Dagster orchestration tier for the lakehouse (ADR-015, ADR-016).

A third execution surface alongside the ``local.pipeline`` CLI and the Fabric
notebooks. The same pure transforms in :mod:`local.transforms` and :mod:`local.gold`
are materialized here as software-defined assets, partitioned by cohort, with the
existing :func:`local.validation.validate.validate_table` checks surfaced as
first-class asset checks. Orchestration imports the lakehouse internals; the
lakehouse never imports orchestration (mirrors the Spark/notebook isolation rule
in ``.claude/rules/transforms.md``).
"""

from orchestration.definitions import defs

__all__ = ["defs"]
