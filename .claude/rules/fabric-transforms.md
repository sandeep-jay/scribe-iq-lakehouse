# Rules: fabric/transforms/  (Fabric tier — Spark-native, ADR-022)

These rules apply when editing any file in `fabric/transforms/`. For the
LocalLite tier rules see [transforms.md](transforms.md).

## Platform isolation (ADR-022)
- NO imports from `core.transforms.*`, `core.gold.*`, `core.validation.*` —
  Fabric is an independent end-to-end implementation, not a thin wrapper.
  Narrow utilities (`core.redaction`) are allowed if needed.
- NO imports from `databricks.`, `aws.`, or any other platform tier.
- NO file paths in transforms — paths come from `FabricPlatform.storage_path()`.
- NO `spark.read.format("delta")` inside a transform — transforms receive a
  DataFrame and return one; the platform owns Delta I/O.

## Spark-native, no Python bridge
- ALL parsing is `from_json(value, BUNDLE_SCHEMA)` against the shared union
  schema in `fabric/transforms/bundle_schema.py`. No `applyInPandas`, no
  `udf(...)`, no driver-side Python loops.
- ALL transform functions take a Spark DataFrame (`bundles_df`) + `ingest_ts`
  and return a Spark DataFrame.
- Imports use the `from pyspark.sql import functions as F` alias and the
  explicit `from pyspark.sql.types import StructType, StructField, ...`
  pattern — Spark schemas are declared, never inferred.

## Schema parity with core/ (ADR-022)
- Silver column names + types must match `core/transforms/silver_<table>.py`.
- Gold field names + struct shapes must match `core/gold/encounter_summary.py`.
- Any change here requires the symmetric change in core/ in the same PR.

## FHIR safety (healthcare-data skill)
- Reference fields (`subject.reference`, `encounter.reference`) are stripped
  to bare ids via `_common.strip_reference()` — never written raw with the
  `urn:uuid:` / `Patient/` prefix.
- Clinical codes (SNOMED, LOINC, ICD) stay as `StringType` — never cast to
  numeric.
- patient_id / encounter_id never appear in logging statements; use
  `core.redaction.redact()` if a reference is needed.

## Data limitation (ADR-007)
- `fabric/transforms/silver_genomics.py` SCHEMA marks `data_limitation` as
  `nullable=False` and the builder writes the canonical string literal.

## Test requirement
- Every builder gets at least one test in `fabric/tests/` (a small
  hand-rolled Spark fixture is fine; we don't need full Coherent for unit
  tests).
- The contract test for `FabricPlatform.storage_path` lives in
  `fabric/tests/test_fabric_platform.py`.