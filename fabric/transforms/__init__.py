"""Fabric-native Silver transforms (ADR-022).

Independent end-to-end Spark DataFrame implementation. No imports from
``core.transforms``; no pa.Table round-trips. Each builder consumes
``bundles_df: DataFrame(path, value)`` and emits a Spark DataFrame
matching the registry schema for that table.
"""
