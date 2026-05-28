# Rules: core/transforms/

These rules apply when editing any file in core/transforms/.

## Platform isolation (ADR-002, ADR-015, ADR-017)
- NO imports from core.platform, pyspark, notebookutils, mssparkutils, or delta
- NO imports from dagster or orchestration — the orchestration tier imports
  transforms, never the reverse (same rule as Spark/notebooks)
- NO imports from `fabric.`, `databricks.`, `aws.`, or any other platform-specific
  package — core never depends on platform tiers (ADR-017 one-way dependency rule)
- NO file paths — all paths come from the platform parameter
- NO spark.read or spark.write — transforms receive data, they don't fetch it

## Arrow return type (ADR-004)
- ALL transform functions return pa.Table
- Import: import pyarrow as pa
- Schema must be explicitly defined, not inferred

## FHIR safety (healthcare-data skill)
- ALL FHIR field access uses .get() with a default — never direct key access
- Clinical codes (SNOMED, LOINC, ICD) always stored as str, never cast to int
- patient_id and encounter_id never appear in logging statements

## Data limitation (ADR-007)
- extract_genomic_report() must always set data_limitation field
- data_limitation = "Synthea simulated inheritance — not clinical variants"
- This field is non-nullable — raise ValueError if somehow None

## DICOM (ADR-006)
- pydicom.dcmread() always called with stop_before_pixels=True
- Never load pixel data in any transform in this directory

## Test requirement
- Every function in this directory has a corresponding test in core/tests/
- Tests use core/tests/fixtures/sample_bundle.json — never real patient data
