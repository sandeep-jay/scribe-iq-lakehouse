Implement a new transform for: $ARGUMENTS

Follow this sequence:

1. **Read the spec** — check docs/roadmap/scribe-iq-lakehouse-spec.md section 5 for the schema of the target Silver or Gold table.

2. **Create the transform** in `core/transforms/{name}.py`:
   - Function signature: `def build_silver_{name}(records: list[dict], ingest_ts: datetime) -> pa.Table`
   - Return type: always `pa.Table` (ADR-004: Arrow interchange)
   - No platform imports — pure Python + pyarrow only (ADR-002)
   - No `fabric.`/`databricks.`/`aws.` imports (ADR-017 one-way dependency)
   - Handle all FHIR optional fields with .get() and defaults (healthcare-data skill)
   - Use named constants, not magic numbers
   - Call `dedup_by_key()` on records before building the Arrow table (ADR-019)

3. **Register in SILVER_TABLES** at `core/transforms/registry.py` — name, primary_key, schema, build function.

4. **Create the test** in `core/tests/test_{name}.py`:
   - Use the 5-patient fixture: `core/tests/fixtures/sample_bundle.json`
   - Test: happy path, missing optional fields, null handling, schema compliance
   - Run `pytest core/tests/test_{name}.py` before continuing

5. **Create the Fabric notebook** in `fabric/notebooks/{NN}_{name}.ipynb`:
   - Follow the 8-cell documentation template from `.claude/rules/notebooks.md`
   - Import the transform from `core.transforms.{name}`
   - Include display() output showing sample rows
   - Include validation cell (row count >= minimum)
   - Include ingest_log write cell

6. **If a non-obvious decision was made**, write an ADR: /new-adr

7. **Update CHANGELOG.md** with the new transform.

Do not proceed to the Fabric notebook until tests pass.
