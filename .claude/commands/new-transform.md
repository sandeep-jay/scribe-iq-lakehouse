Implement a new transform for: $ARGUMENTS

Follow this sequence:

1. **Read the spec** — check docs/roadmap/scribe-iq-lakehouse-spec.md section 5 for the schema of the target Silver or Gold table.

2. **Create the transform** in `local/transforms/{name}.py`:
   - Function signature: `def transform_{name}(bundle_data: dict) -> pa.Table`
   - Return type: always `pa.Table` (ADR-004: Arrow interchange)
   - No platform imports — pure Python + pyarrow only (ADR-002)
   - Handle all FHIR optional fields with .get() and defaults (healthcare-data skill)
   - Use named constants, not magic numbers

3. **Create the test** in `tests/test_{name}.py`:
   - Use the 5-patient fixture: `tests/fixtures/sample_bundle.json`
   - Test: happy path, missing optional fields, null handling, schema compliance
   - Run pytest tests/test_{name}.py before continuing

4. **Create the Fabric notebook** in `fabric/notebooks/{NN}_{name}.ipynb`:
   - Follow the 8-cell documentation template from CLAUDE.md
   - Import the transform from local.transforms.{name}
   - Include display() output showing sample rows
   - Include validation cell (row count >= minimum)
   - Include ingest_log write cell

5. **If a non-obvious decision was made**, write an ADR: /new-adr

6. **Update CHANGELOG.md** with the new transform.

Do not proceed to the Fabric notebook until tests pass.
