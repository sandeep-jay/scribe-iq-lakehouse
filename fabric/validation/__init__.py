"""Fabric-native validation layer (ADR-022).

Spark-native re-implementation of ``core.validation``. Same rule grammar and
same ``silver.ingest_log`` schema so audit rows from either platform are
union-compatible; the implementation is pure Spark — no PyArrow round-trip.
"""
