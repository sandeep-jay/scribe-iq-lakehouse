# Rules: core/transforms/  (LocalLite tier — Polars + delta-rs)

These rules apply when editing any file in `core/transforms/`. For
`fabric/transforms/` rules, see [fabric-transforms.md](fabric-transforms.md).

## Platform isolation (ADR-015, ADR-017, ADR-022)
- NO imports from core.platform, pyspark, notebookutils, mssparkutils, or delta
- NO imports from dagster or orchestration — orchestration imports transforms,
  never the reverse
- NO imports from `fabric.`, `databricks.`, `aws.`, or any other platform-specific
  package — `core/` never depends on platform tiers
- NO file paths — all paths come from the platform parameter
- NO `spark.read` or `spark.write` — transforms receive data, they don't fetch it

## Arrow return type
- ALL transform functions return `pa.Table`
- Import: `import pyarrow as pa`
- Schema must be explicitly defined, not inferred
- (ADR-004 archived; pa.Table is the LocalLite tier's interchange type — not
  cross-platform; the Fabric tier returns Spark DataFrames per ADR-022)

## FHIR safety (healthcare-data skill)
- ALL FHIR field access uses `.get()` with a default — never direct key access
- Clinical codes (SNOMED, LOINC, ICD) always stored as `str`, never cast to int
- patient_id and encounter_id never appear in logging statements

## Data limitation (ADR-007)
- `extract_genomic_report()` must always set `data_limitation`
- `data_limitation = "Synthea simulated inheritance — not clinical variants"`
- Non-nullable — raise `ValueError` if somehow None

## DICOM (ADR-006)
- `pydicom.dcmread()` always called with `stop_before_pixels=True`
- Never load pixel data in any transform in this directory

## Schema parity with fabric/ (ADR-022)
- Silver column names + types must match `fabric/transforms/silver_<table>.py`
- Gold field names + struct shapes must match `fabric/gold/encounter_summary.py`
- Any change here requires the symmetric change in fabric/ in the same PR

## Test requirement
- Every function in this directory has a corresponding test in `core/tests/`
- Tests use `core/tests/fixtures/sample_bundle.json` — never real patient data
