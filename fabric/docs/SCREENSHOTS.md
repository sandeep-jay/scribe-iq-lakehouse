# Fabric Screenshot Capture Checklist

Trial countdown means evidence-of-running > polished-code. Capture as you build, not at the end.

Each screenshot lives in `fabric/docs/screenshots/{ordinal}_{slug}.png` and is referenced from the README "Evidence" section.

## Must-have (before trial expires)

- [ ] **00_workspace_overview** — Fabric workspace with `scribe-iq-lakehouse` lakehouse + attached environment.
- [ ] **01_lakehouse_tables** — Silver tables present (patient, encounter, observation, condition, etc.).
- [ ] **02_bronze_landing** — Bronze FHIR bundles in OneLake browser.
- [ ] **05_silver_soap_demo** — Decoded SOAP note text visible in notebook `display()` output. **This is the demo centerpiece** — readability matters more than every other capture combined.
- [ ] **09_gold_encounter_summary** — Gold encounter_summary table preview with denormalized fields.
- [ ] **10_dagster_local_compare** — Dagster asset graph (local) and Fabric pipeline side-by-side, demonstrating the same transforms running on both surfaces.
- [ ] **11_environment_wheel** — Fabric Environment page showing `scribe-iq-lakehouse-core-X.Y.Z.whl` installed.

## Nice-to-have

- [ ] **12_git_integration** — Workspace Git status showing sync with `/fabric/notebooks/`.
- [ ] **13_ingest_log** — `silver.ingest_log` table with successful rows (per-cohort validation results).
- [ ] **14_data_factory_pipeline** — Fabric Data Factory pipeline graph (if implemented).

## Capture pattern

1. Run notebook to completion.
2. Screenshot output cell (don't crop the row count).
3. Save as `fabric/docs/screenshots/{ordinal}_{slug}.png`.
4. Add a one-line caption in README under "Evidence".

Don't batch — every notebook produces one screenshot the moment it runs successfully. If the trial expires mid-capture, what's saved is what survives.
