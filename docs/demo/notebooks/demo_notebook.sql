-- =============================================================================
--   scribe-iq-lakehouse — DuckDB demo notebook
--   Paste cells one-by-one into DuckDB UI (`duckdb -ui`).
--   Each `-- # ...` header is a logical cell boundary.
--
--   IMPORTANT — set the absolute path to YOUR repo's data/ directory before
--   running cell 0. DuckDB UI runs from $HOME by default, so absolute paths
--   are required for `delta_scan(...)`.
-- =============================================================================


-- # 0. Setup — load Delta extension and create one view per Silver/Gold table
--   (cell title: "Setup — Delta views")

INSTALL delta;
LOAD delta;

-- ⚠ EDIT THIS LINE — set to your repo's absolute path on this machine
--   (grab it with `pwd` from the repo root).
-- Examples:
--   SET VARIABLE repo = '/Users/you/Workbench/scribe-iq-lakehouse';
--   SET VARIABLE repo = '/home/you/projects/scribe-iq-lakehouse';
SET VARIABLE repo = '/ABSOLUTE/PATH/TO/scribe-iq-lakehouse';

CREATE OR REPLACE VIEW silver_patient            AS SELECT * FROM delta_scan(getvariable('repo') || '/data/silver/patient');
CREATE OR REPLACE VIEW silver_encounter          AS SELECT * FROM delta_scan(getvariable('repo') || '/data/silver/encounter');
CREATE OR REPLACE VIEW silver_observation        AS SELECT * FROM delta_scan(getvariable('repo') || '/data/silver/observation');
CREATE OR REPLACE VIEW silver_condition          AS SELECT * FROM delta_scan(getvariable('repo') || '/data/silver/condition');
CREATE OR REPLACE VIEW silver_medication_request AS SELECT * FROM delta_scan(getvariable('repo') || '/data/silver/medication_request');
CREATE OR REPLACE VIEW silver_procedure          AS SELECT * FROM delta_scan(getvariable('repo') || '/data/silver/procedure');
CREATE OR REPLACE VIEW silver_soap_note          AS SELECT * FROM delta_scan(getvariable('repo') || '/data/silver/soap_note');
CREATE OR REPLACE VIEW silver_imaging_study      AS SELECT * FROM delta_scan(getvariable('repo') || '/data/silver/imaging_study');
CREATE OR REPLACE VIEW silver_genomic_report     AS SELECT * FROM delta_scan(getvariable('repo') || '/data/silver/genomic_report');
CREATE OR REPLACE VIEW silver_ecg_metadata       AS SELECT * FROM delta_scan(getvariable('repo') || '/data/silver/ecg_metadata');
CREATE OR REPLACE VIEW gold                      AS SELECT * FROM delta_scan(getvariable('repo') || '/data/gold/encounter_summary');
-- NOTE: silver.ingest_log only exists if you ran `python -m local.pipeline` (the CLI
-- path appends it). The Dagster path surfaces validation via @asset_check instead.
-- If you also have it on disk, uncomment:
-- CREATE OR REPLACE VIEW silver_ingest_log    AS SELECT * FROM delta_scan(getvariable('repo') || '/data/silver/ingest_log');

SELECT 'views ready — see schema pane in the sidebar →' AS status;


-- # 1. Headline numbers — what's in the corpus
--   (cell title: "Corpus at a glance")

SELECT
  (SELECT COUNT(*) FROM silver_patient)             AS patients,
  (SELECT COUNT(*) FROM silver_encounter)           AS encounters,
  (SELECT COUNT(*) FROM silver_observation)         AS observations,
  (SELECT COUNT(*) FROM silver_condition)           AS conditions,
  (SELECT COUNT(*) FROM silver_medication_request)  AS medication_requests,
  (SELECT COUNT(*) FROM silver_procedure)           AS procedures,
  (SELECT COUNT(*) FROM silver_soap_note)           AS soap_notes,
  (SELECT COUNT(*) FROM silver_imaging_study)       AS imaging_studies,
  (SELECT COUNT(*) FROM silver_genomic_report)      AS genomic_reports,
  (SELECT COUNT(*) FROM gold)                       AS gold_encounter_summary;


-- # 2. The Gold contract — what each row looks like
--   (cell title: "gold.encounter_summary — schema")

DESCRIBE SELECT * FROM gold;


-- # 3. Encounter mix — what kinds of visits are in the corpus
--   (cell title: "Encounter types — top 10")

SELECT encounter_type, COUNT(*) AS n
FROM gold
GROUP BY 1
ORDER BY n DESC
LIMIT 10;


-- # 4. Patient demographics — who's in the cohort
--   (cell title: "Demographics — gender × race × ethnicity")

SELECT gender, race, ethnicity, COUNT(*) AS patients
FROM silver_patient
GROUP BY 1, 2, 3
ORDER BY patients DESC
LIMIT 20;


-- # 5. Top conditions — the chronic disease landscape
--   (cell title: "Top 20 active conditions across the corpus")

WITH unnested AS (
  SELECT UNNEST(active_conditions) AS condition
  FROM gold
  WHERE active_conditions IS NOT NULL
)
SELECT condition, COUNT(*) AS encounter_appearances
FROM unnested
GROUP BY 1
ORDER BY encounter_appearances DESC
LIMIT 20;


-- # 6. Top medications — what's being prescribed
--   (cell title: "Top 20 active medications")

WITH unnested AS (
  SELECT UNNEST(active_medications) AS medication
  FROM gold
  WHERE active_medications IS NOT NULL
)
SELECT medication, COUNT(*) AS encounter_appearances
FROM unnested
GROUP BY 1
ORDER BY encounter_appearances DESC
LIMIT 20;


-- # 7. Longitudinal span — encounters per year, proving the data is real-shaped
--   (cell title: "Encounters per year — longitudinal view")

SELECT
  date_trunc('year', encounter_date)::DATE AS year,
  COUNT(*) AS encounters,
  COUNT(DISTINCT patient_id) AS patients_seen
FROM gold
GROUP BY 1
ORDER BY 1;


-- # 8. Problem-list complexity — co-morbidity buckets
--   (cell title: "Active-condition load per encounter")

SELECT
  CASE
    WHEN LENGTH(active_conditions) = 0           THEN '0 conditions'
    WHEN LENGTH(active_conditions) BETWEEN 1 AND 5  THEN '1–5'
    WHEN LENGTH(active_conditions) BETWEEN 6 AND 10 THEN '6–10'
    WHEN LENGTH(active_conditions) BETWEEN 11 AND 20 THEN '11–20'
    ELSE '21+'
  END AS condition_bucket,
  COUNT(*) AS encounters,
  ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM gold), 1) AS pct
FROM gold
GROUP BY 1
ORDER BY MIN(LENGTH(active_conditions));


-- # 9. Vitals distribution — the structured vitals struct, in action
--   (cell title: "Vital signs — median, p10, p90")

SELECT
  approx_quantile(recent_vitals.heart_rate, 0.1)        AS hr_p10,
  approx_quantile(recent_vitals.heart_rate, 0.5)        AS hr_p50,
  approx_quantile(recent_vitals.heart_rate, 0.9)        AS hr_p90,
  approx_quantile(recent_vitals.bp_systolic, 0.5)       AS sbp_p50,
  approx_quantile(recent_vitals.bp_diastolic, 0.5)      AS dbp_p50,
  approx_quantile(recent_vitals.temperature, 0.5)       AS temp_p50,
  COUNT(*) FILTER (WHERE recent_vitals.heart_rate IS NOT NULL) AS encounters_with_hr
FROM gold;


-- # 10. Imaging — what modalities the cohort got
--   (cell title: "Imaging modalities (FHIR-authoritative, ADR-013)")

SELECT
  imaging.modality       AS modality,
  imaging.body_site_display AS body_site,
  COUNT(*)               AS encounters
FROM gold
WHERE imaging.has_imaging
GROUP BY 1, 2
ORDER BY encounters DESC
LIMIT 15;


-- # 11. Pick a demo patient — most conditions, has a SOAP note
--   (cell title: "Pick a demo patient")

SELECT
  patient_id,
  COUNT(*)                                   AS encounters,
  MAX(LENGTH(active_conditions))             AS max_active_conditions,
  COUNT(*) FILTER (WHERE soap_note_text IS NOT NULL) AS soap_notes
FROM gold
GROUP BY 1
HAVING max_active_conditions >= 5 AND soap_notes >= 3
ORDER BY encounters DESC
LIMIT 5;


-- # 12. One patient's journey — encounters over time
--   (cell title: "One patient — encounter timeline")
--   Edit the patient_id below to one from cell 11 if you want a different anchor.

SELECT
  encounter_date::DATE                        AS date,
  patient_age                                 AS age,
  encounter_type,
  LENGTH(active_conditions)                   AS n_conditions,
  LENGTH(active_medications)                  AS n_meds,
  LEFT(COALESCE(soap_note_text, ''), 80)      AS soap_preview
FROM gold
WHERE patient_id = '0074596f-5fd0-7965-db0f-cce71c81567d'
ORDER BY encounter_date
LIMIT 25;


-- # 13. As-of-date problem list — how it grows over time (ADR-014)
--   (cell title: "As-of-date conditions evolve over time")

SELECT
  encounter_date::DATE      AS date,
  patient_age               AS age,
  active_conditions
FROM gold
WHERE patient_id = '0074596f-5fd0-7965-db0f-cce71c81567d'
  AND LENGTH(active_conditions) > 0
ORDER BY encounter_date
LIMIT 10;


-- # 14. One full SOAP note — the demo centerpiece
--   (cell title: "Read a SOAP note — real readable clinical text")
--   In the result pane, click the cell to expand. Or copy/paste into a markdown viewer.

SELECT
  encounter_date::DATE  AS date,
  patient_age           AS age,
  encounter_type,
  soap_note_text
FROM gold
WHERE patient_id = '0074596f-5fd0-7965-db0f-cce71c81567d'
  AND soap_note_text IS NOT NULL
ORDER BY encounter_date
LIMIT 1;


-- # 15. SOAP note keyword search — RAG-style query without RAG
--   (cell title: "Search SOAP notes — keyword grep")

SELECT
  patient_id,
  encounter_date::DATE       AS date,
  LEFT(soap_note_text, 200)  AS preview
FROM gold
WHERE soap_note_text ILIKE '%diabetes%'
  AND soap_note_text ILIKE '%hypertension%'
LIMIT 5;


-- # 16. Data completeness — what's populated vs sparse (per the contract)
--   (cell title: "Corpus coverage — what's populated")

SELECT
  COUNT(*)                                                       AS total_encounters,
  COUNT(soap_note_text)                                          AS with_soap_note,
  COUNT(*) FILTER (WHERE LENGTH(active_conditions) > 0)          AS with_conditions,
  COUNT(*) FILTER (WHERE LENGTH(active_medications) > 0)         AS with_medications,
  COUNT(*) FILTER (WHERE recent_vitals.heart_rate IS NOT NULL)   AS with_vitals,
  COUNT(*) FILTER (WHERE imaging.has_imaging)                    AS with_imaging,
  COUNT(*) FILTER (WHERE LENGTH(recent_labs) > 0)                AS with_labs
FROM gold;


-- # 17. Lineage — Silver versions baked into Gold (corpus_manifest)
--   (cell title: "Provenance — which Silver versions built this Gold row")

SELECT
  encounter_id,
  silver_versions
FROM gold
LIMIT 1;


-- # 18. Validation log — ingest_log audit trail (CLI-built corpora only)
--   (cell title: "silver.ingest_log — every Silver write's quality check")
--   Skip this cell if Silver was built by Dagster (validation surfaces in the
--   asset-check panel of the UI instead — see ADR-016).

-- SELECT
--   ingest_timestamp,
--   "table",
--   row_count,
--   passed,
--   failed_checks
-- FROM silver_ingest_log
-- ORDER BY ingest_timestamp DESC, "table"
-- LIMIT 20;


-- # 19. Cross-layer join — proving the medallion shape pays off
--   (cell title: "Top conditions for a single patient — pure SQL, no Spark")

SELECT
  c.code AS snomed_code,
  c.display AS condition,
  MIN(c.onset_date)::DATE AS first_onset,
  COUNT(*) AS occurrences
FROM silver_condition c
WHERE c.patient_id = '0074596f-5fd0-7965-db0f-cce71c81567d'
GROUP BY 1, 2
ORDER BY first_onset;


-- # 20. Corpus shape, end-to-end
--   (cell title: "Final sanity — corpus span, contract pinned in manifest")

SELECT
  COUNT(*)                       AS encounter_summary_rows,
  COUNT(DISTINCT patient_id)     AS distinct_patients,
  MIN(encounter_date)            AS earliest_encounter,
  MAX(encounter_date)            AS latest_encounter,
  ROUND(AVG(LENGTH(active_conditions)), 2)  AS avg_conditions_per_encounter,
  ROUND(AVG(LENGTH(active_medications)), 2) AS avg_meds_per_encounter
FROM gold;

-- The corpus contract version lives in the manifest (read from the file):
-- SELECT * FROM read_json_auto(getvariable('repo') || '/data/gold/_metadata/corpus_manifest.json');
