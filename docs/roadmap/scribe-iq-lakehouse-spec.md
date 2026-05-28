# scribe-iq-lakehouse — Project Spec

**Repo:** `scribe-iq-lakehouse`
**Status:** Implementation-ready
**Execution environments:** Microsoft Fabric (Bronze + Silver) + Local Python (Gold)

---

## 1. Purpose

A production-pattern healthcare data lakehouse built on Synthea Coherent —
the richest publicly available synthetic longitudinal patient dataset. Ingests
the full multimodal dataset (FHIR, SOAP notes, ECG, DICOM, genomics), builds
a medallion architecture in Delta Lake, and produces a governed clinical corpus
for downstream AI workloads.

**Feeds two downstream projects:**
- `scribe-iq` — clinical RAG and generative AI workflows
- `clinical-bert-pipeline` — discriminative NLP, Phase 3 inference enrichment

**Ollama Gold generation is a separate spec** — this spec covers Bronze through
Silver and the corpus handoff contract. Gold generation follows once this ships.

### Architectural signals

| Signal | How it shows |
|---|---|
| Healthcare data engineering | FHIR R4 ingestion, medallion architecture, clinical data modeling |
| Fabric / enterprise platform | OneLake, Spark notebooks, Auto Loader, Delta Lake |
| Streaming simulation | Partitioned file replay through Auto Loader, CDC-enabled Silver |
| Data governance | Schema validation, lineage tracking, corpus manifest, audit trail |
| Multimodal data | FHIR + SOAP notes + ECG metadata + DICOM headers + genomic flags |
| Production judgment | DICOM metadata extracted, pixel deferred; genomics flagged with honest limitation |
| Honest data modeling | `data_limitation` column in genomic table — constraint is a first-class field |
| System thinking | Feeds scribe-iq and clinical-bert-pipeline — not a standalone demo |

---

## 2. Source Dataset — Synthea Coherent

**Location:** `s3://synthea-open-data/coherent/` (AWS Open Data, no credentials needed)
**Size:** 9GB
**Format:** FHIR R4 bundles (JSON), one bundle per patient

### What it contains

| Data type | FHIR resources | Format | This spec |
|---|---|---|---|
| Patient demographics | `Patient` | Structured JSON | Silver ✓ |
| Encounters | `Encounter` | Structured JSON | Silver ✓ |
| Conditions | `Condition` | Structured JSON | Silver ✓ |
| Observations / vitals / labs | `Observation` | Structured JSON | Silver ✓ |
| Medications | `MedicationRequest` | Structured JSON | Silver ✓ |
| Procedures | `Procedure` | Structured JSON | Silver ✓ |
| SOAP clinical notes | `DocumentReference` + `Binary` (Base64) | Text via decode | Silver ✓ |
| ECG metadata | `DiagnosticReport` + `Observation` | Structured JSON | Silver ✓ |
| ECG waveform signals | `Binary` | SBML/waveform | Bronze only → roadmap Phase 3 |
| MRI DICOM images | `ImagingStudy` + DICOM headers | DICOM binary | Silver ✓ (metadata via pydicom stop_before_pixels) |
| Genomic data | `DiagnosticReport` + `Binary` | VCF-like | Silver ✓ (report metadata + flag only) |

**DICOM note:** Header metadata extracted via `pydicom` with `stop_before_pixels=True` —
modality, body part, study description, study date, series info. No pixel data loaded.
Fast and lightweight. Enriches Gold `encounter_summary` directly.

**Genomics note:** Synthea Coherent genomics models familial inheritance simulation,
not clinically actionable variants (no BRCA, CYP2D6, HLA typing).
Metadata flag (`has_genomic_report`, `gene_panel_name`, `report_date`) is
useful for encounter context. Full VCF parsing deferred — low clinical signal
on synthetic inheritance data. Real genomic pipeline requires real variant data.

### Key FHIR structure insight

SOAP notes and ECG signals are **Base64 encoded inside Binary FHIR resources**,
linked to encounters via `DocumentReference`. Extraction is a decode step,
not a separate file format to parse. No CCDA extraction needed.

```
Patient (1)
  └── Encounter (many)
        ├── Condition
        ├── Observation (vitals, labs, ECG metadata)
        ├── MedicationRequest
        ├── Procedure
        ├── DiagnosticReport (ECG findings, genomic reports)
        └── DocumentReference → Binary (Base64 SOAP note text)
```

---

## 3. Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    SOURCE                                           │
│   AWS S3  s3://synthea-open-data/coherent/                         │
│   (open data, no credentials, ~9GB FHIR JSON bundles)              │
└──────────────────────────┬──────────────────────────────────────────┘
                           │  Fabric Copy Pipeline or S3 Shortcut
┌──────────────────────────▼──────────────────────────────────────────┐
│                    BRONZE — OneLake                                 │
│   Raw landing, append-only, no transforms                           │
│                                                                     │
│   fhir/                                                             │
│     cohort=A/patient_001.json ... patient_500.json                  │
│     cohort=B/patient_501.json ... patient_1000.json                 │
│   dicom/          (landed + header metadata extracted to Silver)    │
│   genomics/       (landed + report metadata extracted to Silver)    │
│   ecg_signals/    (landed, waveform not processed — roadmap)        │
│   _metadata/      (manifest: file count, size, ingest timestamp)    │
└──────────────────────────┬──────────────────────────────────────────┘
                           │  Auto Loader (Structured Streaming)
                           │  Partition replay → simulated stream
┌──────────────────────────▼──────────────────────────────────────────┐
│                    SILVER — Delta Tables (Fabric)                   │
│   Validated, typed, CDC-enabled, encounter-linked                   │
│                                                                     │
│   Core clinical entities                                            │
│     silver.patient           silver.encounter                       │
│     silver.condition         silver.observation                     │
│     silver.medication_request silver.procedure                      │
│                                                                     │
│   Notes and signals                                                 │
│     silver.soap_note         (decoded from Binary, encounter-linked)│
│     silver.ecg_metadata      (DiagnosticReport + Observation)       │
│                                                                     │
│   Reference                                                         │
│     silver.document_reference silver.diagnostic_report              │
│     silver.imaging_study      (DICOM header metadata via pydicom,  │
│                                stop_before_pixels — no pixel data)  │
│     silver.genomic_report     (report metadata + flag only,        │
│                                Synthea inheritance sim noted)       │
│                                                                     │
│   Lineage                                                           │
│     silver.ingest_log        (file → table, row counts, timestamps) │
└──────────────────────────┬──────────────────────────────────────────┘
                           │  Corpus export (Python, local or Fabric)
┌──────────────────────────▼──────────────────────────────────────────┐
│                    GOLD — Corpus Handoff                            │
│   Denormalized, AI-ready, Ollama generation input                   │
│                                                                     │
│   gold.encounter_summary     (patient + encounter + conditions +    │
│                               meds + vitals + SOAP note)            │
│   gold.corpus_manifest       (lineage: which Silver records fed     │
│                               which Gold rows, generation metadata) │
│                                                                     │
│   ── Ollama generation (separate spec) ──────────────────────────   │
│   gold.synthetic_dialogue    (LLM-generated, grounded in SOAP)      │
│   gold.synthetic_note_unstructured  (LLM-generated)                 │
│   gold.entity_annotations    (ClinicalBERT NER output, Phase 3)     │
└─────────────────────────────────────────────────────────────────────┘
```

### Execution environment split

| Layer | Where | Why |
|---|---|---|
| Bronze landing | Fabric | S3 shortcut or Copy Pipeline, OneLake storage |
| Silver transforms | Fabric Spark notebooks | Delta Lake, Auto Loader, CDC |
| Gold denormalization | Fabric or local | Either works, same Delta format |
| Ollama generation | Local only | No GPU/Ollama in Fabric |
| delta-rs local mirror | Local Python | Same Delta format, Fabric trial fallback |

---

## 4. Repository Structure

```
scribe-iq-lakehouse/
│
├── fabric/
│   ├── notebooks/
│   │   ├── 00_setup.ipynb           # OneLake paths, library installs, config
│   │   ├── 01_bronze_ingest.ipynb   # S3 → OneLake landing pipeline
│   │   ├── 02_silver_patient.ipynb  # Patient + demographics
│   │   ├── 03_silver_encounter.ipynb # Encounter + linked resources
│   │   ├── 04_silver_clinical.ipynb  # Condition, Observation, Medication, Procedure
│   │   ├── 05_silver_soap_notes.ipynb # DocumentReference + Binary decode
│   │   ├── 06_silver_ecg.ipynb       # DiagnosticReport + ECG Observation metadata
│   │   ├── 07_silver_imaging.ipynb   # ImagingStudy metadata (DICOM, no pixels)
│   │   ├── 08_silver_genomics.ipynb  # Genomic DiagnosticReport metadata
│   │   ├── 09_gold_encounter_summary.ipynb # Denormalized Gold table
│   │   └── 10_corpus_export.ipynb    # Export for Ollama generation
│   │
│   ├── pipelines/
│   │   └── master_pipeline.json      # Fabric Pipeline orchestrating notebooks
│   │
│   └── config/
│       └── lakehouse_config.json     # OneLake paths, table names, thresholds
│
├── local/
│   ├── ingest/
│   │   ├── __init__.py
│   │   ├── download.py              # S3 → local Bronze (no-sign-request)
│   │   ├── bronze_landing.py        # Partition by cohort, write Delta via delta-rs
│   │   └── streaming_sim.py         # Auto Loader simulation via watchdog
│   │
│   ├── transforms/
│   │   ├── __init__.py
│   │   ├── fhir_parser.py           # Core FHIR bundle extraction using fhir.resources
│   │   ├── silver_patient.py
│   │   ├── silver_encounter.py
│   │   ├── silver_clinical.py       # Condition, Observation, Medication, Procedure
│   │   ├── silver_soap_notes.py     # Base64 decode + text extraction
│   │   ├── silver_ecg.py            # ECG metadata extraction
│   │   ├── silver_imaging.py        # DICOM metadata from FHIR ImagingStudy
│   │   └── silver_genomics.py       # Genomic report metadata
│   │
│   ├── gold/
│   │   ├── __init__.py
│   │   ├── encounter_summary.py     # Denormalized Gold table builder
│   │   └── corpus_manifest.py       # Lineage tracking
│   │
│   ├── validation/
│   │   ├── __init__.py
│   │   ├── schema_registry.py       # Expected schemas per Silver table
│   │   └── validate.py              # Row counts, null checks, referential integrity
│   │
│   └── pipeline.py                  # Local end-to-end orchestration
│
├── tests/
│   ├── fixtures/
│   │   └── sample_bundle.json       # 5-patient FHIR bundle for unit tests
│   ├── test_fhir_parser.py
│   ├── test_silver_soap_notes.py
│   ├── test_silver_ecg.py
│   ├── test_gold_encounter_summary.py
│   └── test_validation.py
│
├── docs/
│   ├── index.md                     # MkDocs home (mirrors campus-rag pattern)
│   ├── REVIEWER_GUIDE.md
│   ├── ARCHITECTURE.md
│   ├── DATA_DICTIONARY.md           # Every Silver table, every column, types
│   ├── CORPUS_CONTRACT.md           # Schema contract for Ollama Gold generation
│   ├── STREAMING_DESIGN.md          # Auto Loader simulation pattern explained
│   └── PRODUCTION_NOTES.md          # Honest production seams
│
├── schemas/
│   ├── silver_patient.json          # JSON Schema for validation
│   ├── silver_encounter.json
│   ├── silver_soap_note.json
│   ├── silver_ecg_metadata.json
│   └── gold_encounter_summary.json
│
├── .github/
│   └── workflows/
│       ├── ci.yml                   # lint, unit tests, sample bundle validation
│       └── validate_schemas.yml     # schema contract validation on PR
│
├── mkdocs.yml
├── docker-compose.yml               # Local Delta Lake + validation runner
├── pyproject.toml
├── requirements.txt
└── README.md
```

---

## 5. Component Specifications

### 5.1 Bronze Landing

**Fabric approach (primary):**

```python
# Notebook 01_bronze_ingest.ipynb
# Option A: Fabric S3 Shortcut (zero-copy, preferred)
#   Create shortcut in Fabric UI → External Sources → S3
#   Path: s3://synthea-open-data/coherent/
#   No credentials needed (open data)

# Option B: Copy Pipeline
#   Source: HTTP / S3 open data endpoint
#   Sink: OneLake Files section
#   Partitioned by cohort (split patient list into A/B/C batches)
```

**Local approach (fallback / delta-rs mirror):**

```python
# local/ingest/download.py
# aws s3 sync s3://synthea-open-data/coherent/fhir/ data/bronze/fhir/ --no-sign-request
# Partition output: data/bronze/fhir/cohort=A/, cohort=B/, cohort=C/
# Write partition manifest to data/bronze/_metadata/manifest.json
```

**Bronze rules:**
- Append-only, never modify
- Raw files as-is, no parsing
- Partition by cohort for streaming simulation
- Log file count, size, ingest timestamp to `_metadata/`

---

### 5.2 Auto Loader / Streaming Simulation

**In Fabric:**

```python
# Notebook 02+ — reads from Bronze using Auto Loader
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation",
              f"{BRONZE_PATH}/_schemas/fhir/")
      .load(f"{BRONZE_PATH}/fhir/"))

# Process each file as it "arrives"
# Simulate stream by dropping cohort partitions sequentially
```

**Local simulation:**

```python
# local/ingest/streaming_sim.py
# Uses watchdog to monitor data/bronze/fhir/
# Triggers silver transform when new cohort partition lands
# Writes checkpoint to data/bronze/_checkpoints/
```

**Why this matters — document clearly in STREAMING_DESIGN.md:**

| Simulated pattern | Production equivalent |
|---|---|
| Cohort partition drop | New patient batch from EHR export |
| Auto Loader file trigger | EventHub / Kafka message |
| Checkpoint directory | Consumer offset |
| Silver MERGE | CDC UPSERT from change event |
| `enableChangeDataFeed` | Downstream CDC consumers |

---

### 5.3 FHIR Parser — `fhir_parser.py`

Core extraction logic. Used by all Silver notebooks/scripts.

```python
from fhir.resources.bundle import Bundle
import base64

class FHIRBundleParser:
    """
    Parses a Synthea Coherent FHIR R4 bundle JSON.
    Extracts all resource types into typed dicts.
    """

    def parse_bundle(self, bundle_json: dict) -> dict:
        """Returns dict of resource_type → list of extracted records"""

    def extract_patient(self, resource) -> dict:
        """Demographics, identifiers, birth date, gender, address"""

    def extract_encounter(self, resource) -> dict:
        """Type, period, status, class, provider reference"""

    def extract_condition(self, resource) -> dict:
        """Code, display, onset, clinical status, encounter ref"""

    def extract_observation(self, resource) -> dict:
        """Code, value, unit, effective date, category, encounter ref"""

    def extract_medication_request(self, resource) -> dict:
        """Medication code, display, dosage, route, authored date"""

    def extract_procedure(self, resource) -> dict:
        """Code, display, performed period, encounter ref"""

    def extract_soap_note(self, binary_resource, doc_ref) -> dict:
        """
        Base64 decode Binary resource → plain text SOAP note.
        Links to encounter via DocumentReference.subject + context.
        Returns: patient_id, encounter_id, note_text, note_date,
                 section_counts (S/O/A/P detected), char_count
        """
        raw = base64.b64decode(binary_resource.data).decode("utf-8")
        return self._parse_soap_sections(raw, doc_ref)

    def extract_ecg_metadata(self, diagnostic_report, observations) -> dict:
        """
        ECG DiagnosticReport: conclusion, status, effective date.
        Linked Observations: heart rate, QRS, rhythm finding codes.
        Does NOT extract Binary waveform signal — roadmap.
        Returns: patient_id, encounter_id, finding, rhythm,
                 heart_rate_bpm, report_date, has_waveform_binary
        """

    def extract_imaging_study(self, resource, dicom_binary=None) -> dict:
        """
        Two-pass extraction:
        Pass 1 — FHIR ImagingStudy resource:
          patient_id, encounter_id, modality, body_site,
          series_count, instance_count, started_date,
          study_description, status, dicom_binary_id

        Pass 2 — pydicom header (if Binary available):
          study_date, series_description, slice_thickness,
          rows, columns, manufacturer, institution,
          pixel_spacing, magnetic_field_strength

        stop_before_pixels=True — never loads image data.
        Returns merged dict from both passes.
        Pixel extraction and segmentation: roadmap Phase 3.
        """
        fhir_meta = self._extract_imaging_from_fhir(resource)
        if dicom_binary:
            dicom_meta = self._extract_dicom_headers(dicom_binary)
            return {**fhir_meta, **dicom_meta}
        return fhir_meta

    def _extract_dicom_headers(self, binary_data: bytes) -> dict:
        """
        import pydicom, io
        ds = pydicom.dcmread(io.BytesIO(binary_data),
                             stop_before_pixels=True)
        Extract tags: StudyDate, Modality, BodyPartExamined,
        StudyDescription, SeriesDescription, SliceThickness,
        Rows, Columns, Manufacturer, MagneticFieldStrength
        """

    def extract_genomic_report(self, resource) -> dict:
        """
        Metadata flag extraction only.
        Synthea Coherent genomics = familial inheritance simulation.
        NOT clinically actionable variants (no BRCA, CYP2D6, HLA).
        Document this limitation explicitly in every output row.

        Returns:
          patient_id, encounter_id, report_date,
          gene_panel_name, result_summary,
          has_pathogenic_variant (boolean from report text),
          family_history_flag (boolean),
          binary_id (for future VCF extraction),
          data_note: "Synthea simulated inheritance — not clinical variants"

        Full VCF parsing deferred to roadmap Phase 4.
        Requires real clinical genomic data for actionable signal.
        """
```

---

### 5.4 Silver Table Schemas

#### `silver.patient`

| Column | Type | Source | Notes |
|---|---|---|---|
| patient_id | string | `Patient.id` | FHIR UUID |
| birth_date | date | `Patient.birthDate` | |
| gender | string | `Patient.gender` | |
| race | string | `Patient.extension` | US Core race extension |
| ethnicity | string | `Patient.extension` | US Core ethnicity extension |
| state | string | `Patient.address[0].state` | |
| city | string | `Patient.address[0].city` | |
| zip | string | `Patient.address[0].postalCode` | |
| deceased | boolean | `Patient.deceasedBoolean` | |
| deceased_date | date | `Patient.deceasedDateTime` | |
| ingest_timestamp | timestamp | pipeline | |
| source_file | string | pipeline | Bronze file path |

#### `silver.encounter`

| Column | Type | Source | Notes |
|---|---|---|---|
| encounter_id | string | `Encounter.id` | |
| patient_id | string | `Encounter.subject.reference` | FK to patient |
| type_code | string | `Encounter.type[0].coding[0].code` | SNOMED |
| type_display | string | `Encounter.type[0].coding[0].display` | |
| class_code | string | `Encounter.class.code` | AMB, IMP, ER |
| start_date | timestamp | `Encounter.period.start` | |
| end_date | timestamp | `Encounter.period.end` | |
| status | string | `Encounter.status` | |
| provider_id | string | `Encounter.participant[0].individual` | |
| reason_code | string | `Encounter.reasonCode[0].coding[0].code` | |
| reason_display | string | `Encounter.reasonCode[0].coding[0].display` | |
| ingest_timestamp | timestamp | pipeline | |

#### `silver.soap_note`

| Column | Type | Source | Notes |
|---|---|---|---|
| note_id | string | `DocumentReference.id` | |
| patient_id | string | `DocumentReference.subject` | |
| encounter_id | string | `DocumentReference.context.encounter` | |
| note_date | timestamp | `DocumentReference.date` | |
| note_text | string | `Binary.data` (Base64 decoded) | Full SOAP text |
| has_subjective | boolean | parser | S section detected |
| has_objective | boolean | parser | O section detected |
| has_assessment | boolean | parser | A section detected |
| has_plan | boolean | parser | P section detected |
| char_count | integer | parser | |
| word_count | integer | parser | |
| binary_id | string | `Binary.id` | Reference to source |
| ingest_timestamp | timestamp | pipeline | |

#### `silver.ecg_metadata`

| Column | Type | Source | Notes |
|---|---|---|---|
| ecg_id | string | `DiagnosticReport.id` | |
| patient_id | string | `DiagnosticReport.subject` | |
| encounter_id | string | `DiagnosticReport.encounter` | |
| report_date | timestamp | `DiagnosticReport.effectiveDateTime` | |
| status | string | `DiagnosticReport.status` | |
| conclusion | string | `DiagnosticReport.conclusion` | Text finding |
| rhythm | string | `Observation` (LOINC 8893-1) | Sinus / Afib / etc |
| heart_rate_bpm | integer | `Observation` (LOINC 8867-4) | |
| pr_interval_ms | integer | `Observation` (LOINC 8625-6) | If present |
| qrs_duration_ms | integer | `Observation` (LOINC 8625-2) | If present |
| has_waveform | boolean | `Binary` reference exists | Signal available |
| waveform_binary_id | string | `Binary.id` | For future extraction |
| ingest_timestamp | timestamp | pipeline | |

#### `silver.imaging_study`

FHIR ImagingStudy metadata + DICOM header metadata (no pixel data).

| Column | Type | Source | Notes |
|---|---|---|---|
| study_id | string | `ImagingStudy.id` | |
| patient_id | string | `ImagingStudy.subject` | |
| encounter_id | string | `ImagingStudy.encounter` | |
| started_date | timestamp | `ImagingStudy.started` | |
| status | string | `ImagingStudy.status` | |
| modality | string | `ImagingStudy.series[0].modality` | MR, CT, US |
| body_site | string | `ImagingStudy.series[0].bodySite` | SNOMED code |
| body_site_display | string | `ImagingStudy.series[0].bodySite.display` | Human readable |
| series_count | integer | `ImagingStudy.numberOfSeries` | |
| instance_count | integer | `ImagingStudy.numberOfInstances` | |
| study_description | string | DICOM tag `StudyDescription` | via pydicom |
| series_description | string | DICOM tag `SeriesDescription` | via pydicom |
| study_date | date | DICOM tag `StudyDate` | via pydicom |
| manufacturer | string | DICOM tag `Manufacturer` | via pydicom |
| magnetic_field_strength | float | DICOM tag `MagneticFieldStrength` | MRI only |
| slice_thickness_mm | float | DICOM tag `SliceThickness` | |
| rows | integer | DICOM tag `Rows` | Image dimensions |
| columns | integer | DICOM tag `Columns` | Image dimensions |
| dicom_binary_id | string | `Binary.id` | For Phase 3 pixel extraction |
| dicom_extracted | boolean | pipeline | True if pydicom headers read |
| ingest_timestamp | timestamp | pipeline | |

**pydicom extraction note:** `stop_before_pixels=True` on every read.
Binary data decoded from FHIR, passed to pydicom via `io.BytesIO`.
Never writes pixel data to Silver. Fast — header read only.

#### `silver.genomic_report`

Report-level metadata only. Flags for encounter context.

| Column | Type | Source | Notes |
|---|---|---|---|
| report_id | string | `DiagnosticReport.id` | |
| patient_id | string | `DiagnosticReport.subject` | |
| encounter_id | string | `DiagnosticReport.encounter` | |
| report_date | timestamp | `DiagnosticReport.effectiveDateTime` | |
| status | string | `DiagnosticReport.status` | |
| gene_panel_name | string | `DiagnosticReport.code.display` | |
| result_summary | string | `DiagnosticReport.conclusion` | Text summary |
| has_pathogenic_variant | boolean | parser | From conclusion text |
| family_history_flag | boolean | `DiagnosticReport.extension` | |
| binary_id | string | `Binary.id` | VCF data reference |
| data_limitation | string | pipeline | Always: "Synthea simulated inheritance — not clinical variants" |
| ingest_timestamp | timestamp | pipeline | |

**Important:** `data_limitation` column is always populated with the
limitation note. Any downstream consumer reading this table sees
the constraint. Not a hidden gotcha — an explicit data contract.



The denormalized table that feeds Ollama generation.

| Column | Type | Source | Notes |
|---|---|---|---|
| summary_id | string | generated UUID | |
| patient_id | string | silver.patient | |
| encounter_id | string | silver.encounter | |
| patient_age | integer | calculated | At time of encounter |
| patient_gender | string | silver.patient | |
| encounter_type | string | silver.encounter | |
| encounter_date | date | silver.encounter | |
| active_conditions | array[string] | silver.condition | Display names |
| active_medications | array[string] | silver.medication_request | Display names |
| recent_vitals | struct | silver.observation | HR, BP, temp, O2 |
| recent_labs | array[struct] | silver.observation | Name, value, unit |
| procedures | array[string] | silver.procedure | This encounter |
| soap_note_text | string | silver.soap_note | If exists, else null |
| soap_note_id | string | silver.soap_note | Lineage reference |
| ecg_finding | string | silver.ecg_metadata | If exists, else null |
| ecg_rhythm | string | silver.ecg_metadata | If exists, else null |
| has_ecg | boolean | silver.ecg_metadata | |
| imaging | struct | silver.imaging_study | See struct below |
| has_genomics | boolean | silver.genomic_report | |
| genomic_summary | string | silver.genomic_report | result_summary if exists |
| created_timestamp | timestamp | pipeline | |
| silver_versions | struct | lineage | Row versions from Silver |

**`imaging` struct schema:**
```json
{
  "has_imaging": true,
  "modality": "MR",
  "body_site_display": "Brain structure",
  "study_description": "MRI Brain Without Contrast",
  "study_date": "2021-03-15",
  "series_count": 3,
  "dicom_binary_id": "binary-abc123"
}
```

This struct is directly usable as Ollama grounding context.
Generated notes can accurately reference imaging studies
because the metadata confirms they occurred.
Example grounded sentence: *"MRI brain without contrast obtained,
results pending at time of this note."*

---

### 5.5 CDC Configuration

Enable on all Silver tables immediately:

```sql
-- Run in Fabric Spark notebook after table creation
ALTER TABLE silver.patient
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE silver.encounter
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE silver.soap_note
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Repeat for all Silver tables
```

Downstream consumers (Gold generation, scribe-iq) can read:

```python
spark.read.format("delta") \
     .option("readChangeFeed", "true") \
     .option("startingVersion", 0) \
     .table("silver.soap_note")
```

---

### 5.6 Validation — `validate.py`

Runs after each Silver table write. Fails pipeline if thresholds breached.

**Checks per table:**

```python
VALIDATION_RULES = {
    "silver.patient": {
        "min_rows": 100,
        "required_non_null": ["patient_id", "birth_date", "gender"],
        "unique_keys": ["patient_id"],
    },
    "silver.soap_note": {
        "min_rows": 50,
        "required_non_null": ["note_id", "patient_id", "note_text"],
        "min_char_count": 100,         # notes under 100 chars are suspect
        "required_sections_pct": 0.8,  # 80% should have all 4 SOAP sections
    },
    "silver.ecg_metadata": {
        "min_rows": 10,
        "required_non_null": ["ecg_id", "patient_id", "report_date"],
        "heart_rate_range": [30, 250],  # physiological bounds check
    },
}
```

Validation results written to `silver.ingest_log` with timestamp, table,
row counts, pass/fail status, and any failed checks.

---

### 5.7 Corpus Contract — `CORPUS_CONTRACT.md`

The handoff schema between this lakehouse and the Ollama generation spec.
Defines exactly what `gold.encounter_summary` guarantees to the generation pipeline.

```
Required fields (always present):
  patient_id, encounter_id, patient_age, patient_gender,
  encounter_type, encounter_date, active_conditions,
  active_medications

Optional fields (present if data exists):
  recent_vitals, recent_labs, procedures,
  soap_note_text, ecg_finding, ecg_rhythm

Generation pipeline must handle null optional fields gracefully.
A summary with no soap_note_text uses structured fields only for grounding.
A summary with soap_note_text uses it as the primary generation anchor.
```

This contract is committed to the repo and versioned. Breaking changes require
a version bump. Both `scribe-iq` and `clinical-bert-pipeline` reference it.

---

## 6. Fabric Notebook Sequence

### `00_setup.ipynb`

```python
# Install libraries
%pip install fhir.resources delta-spark

# Define paths
BRONZE_PATH = "abfss://lakehouse@onelake.dfs.fabric.microsoft.com/bronze"
SILVER_PATH = "abfss://lakehouse@onelake.dfs.fabric.microsoft.com/silver"
GOLD_PATH   = "abfss://lakehouse@onelake.dfs.fabric.microsoft.com/gold"

# Spark config
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
```

### `01_bronze_ingest.ipynb`

```python
# Option A: S3 Shortcut already configured in Fabric UI
# Verify files are accessible
files = mssparkutils.fs.ls(f"{BRONZE_PATH}/fhir/")
print(f"Found {len(files)} FHIR bundles in Bronze")

# Write ingest manifest
manifest = {
    "ingest_timestamp": datetime.now().isoformat(),
    "file_count": len(files),
    "total_size_mb": sum(f.size for f in files) / 1024 / 1024,
    "cohorts": ["A", "B", "C"]
}
# Write to _metadata/
```

### `05_silver_soap_notes.ipynb` — most important notebook

```python
from transforms.fhir_parser import FHIRBundleParser
import base64

parser = FHIRBundleParser()

# Read FHIR bundles via Auto Loader (streaming)
raw_df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.schemaLocation",
                  f"{BRONZE_PATH}/_schemas/")
          .load(f"{BRONZE_PATH}/fhir/"))

# UDF: extract SOAP notes from each bundle
@udf(returnType=ArrayType(soap_note_schema))
def extract_notes_udf(bundle_json):
    bundle = json.loads(bundle_json)
    return parser.extract_soap_notes_from_bundle(bundle)

notes_df = (raw_df
    .withColumn("notes", extract_notes_udf(col("value")))
    .select(explode("notes").alias("note"))
    .select("note.*")
    .withColumn("ingest_timestamp", current_timestamp()))

# Write to Silver with MERGE (upsert on note_id)
(notes_df.writeStream
    .format("delta")
    .option("checkpointLocation", f"{BRONZE_PATH}/_checkpoints/soap_notes/")
    .foreachBatch(lambda df, id: merge_soap_notes(df))
    .start())
```

### `09_gold_encounter_summary.ipynb`

```python
# Denormalize Silver tables into Gold encounter summary
# Join: encounter + patient + conditions + meds + vitals + labs + SOAP + ECG

encounter_summary = (
    spark.table("silver.encounter").alias("enc")
    .join(spark.table("silver.patient").alias("pat"),
          "patient_id", "left")
    .join(conditions_agg, "encounter_id", "left")
    .join(medications_agg, "encounter_id", "left")
    .join(vitals_agg, "encounter_id", "left")
    .join(labs_agg, "encounter_id", "left")
    .join(soap_notes.alias("soap"), "encounter_id", "left")
    .join(ecg_metadata.alias("ecg"), "encounter_id", "left")
    .join(imaging_flag, "encounter_id", "left")
    .join(genomics_flag, "encounter_id", "left")
    .select(gold_encounter_summary_columns)
)

# Write to Gold Delta table
(encounter_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{GOLD_PATH}/encounter_summary/"))
```

---

## 7. CI/CD

### `ci.yml` — runs on every PR

```yaml
jobs:
  lint:
    - ruff check local/ tests/
    - black --check local/ tests/

  test:
    - pytest tests/ -v
    # Uses 5-patient fixture bundle, no S3 access needed
    # Validates: FHIR parsing, Base64 decode, schema compliance,
    #            SOAP section detection, ECG metadata extraction

  validate_schemas:
    - python -m local.validation.validate --fixture
    # Runs validation rules against fixture data
    # Ensures schema contract is not broken on PR
```

### `validate_schemas.yml` — schema contract guard

```yaml
on:
  pull_request:
    paths:
      - schemas/**
      - local/validation/schema_registry.py

jobs:
  schema_guard:
    - python scripts/check_schema_breaking_changes.py
    # Fails if required fields are removed from corpus contract
    # Notifies: scribe-iq and clinical-bert-pipeline depend on this schema
```

---

## 8. Production Notes — `PRODUCTION_NOTES.md`

| Capability | Current | Production path |
|---|---|---|
| Ingestion trigger | Manual cohort drop / Auto Loader file watch | FHIR R4 subscription → Azure EventHub → Fabric Eventstream |
| Change tracking | `enableChangeDataFeed` on Silver | Full CDC with SCD Type 2 for patient record updates |
| Source | Synthea Coherent S3 (synthetic) | BAA-backed EHR FHIR API endpoint |
| PHI handling | No PHI — synthetic data only | De-identification pipeline before Bronze landing |
| ECG waveform processing | Metadata only (Silver) | NeuroKit2 signal processing → feature extraction → time series ML |
| DICOM pixel processing | Header metadata only (Silver, stop_before_pixels) | MONAI pixel extraction → segmentation → radiomics |
| Genomic processing | Report metadata flag only (Silver) | Real clinical variant data required — BRCA/CYP2D6/HLA pipelines |
| Auth | None (open data) | Managed Identity, RBAC per OneLake table |
| Observability | ingest_log table | Fabric monitoring + Azure Monitor alerts |
| Multi-tenant | Single workspace | Workspace-per-tenant with shortcut isolation |

---

## 17. Roadmap

Every roadmap item is documented honestly — what it requires,
why it is deferred, and what value it adds when built.

### Phase 2 — Ollama Gold Generation (this weekend + M5 full run)

**What:** LLM-based synthetic dialogue generation grounded in
Silver SOAP notes and Gold encounter_summary.

**Initial run:** 200 notes from a stratified sample of the
1,500-patient Synthea Coherent cohort. ~2.5 hours locally.
Task A (dialogue) only. Layer 1+2 validation. Fast plan mode.

**Full corpus run:** 6,300 notes, both tasks, full QwQ validation.
~55 hours across multiple overnight runs on higher-memory hardware.

**Why Task B deferred:** Unstructured note generation adds ~8 sec/note.
At 200 notes it saves 27 minutes. At 6,300 notes it saves 14 hours.
Worth doing properly with full validation on capable hardware.

**Context:** scribe-iq currently has 19 patients (dev corpus).
After lakehouse Silver: 1,500 patients available.
After 200 Ollama notes: patient-linked dialogue layer added.
After M5 full run: complete production corpus, scribe-iq migration.

**Status:** Deferred. `gold/generation/` module not yet built; spec will be
added back when implementation starts. `gold.encounter_summary` corpus
contract is the handoff interface when it does land.

---

### Phase 3 — ECG Waveform Feature Extraction

**What:** Parse SBML-based waveform signals from Bronze Binary resources.
Extract clinical features: heart rate variability, QRS morphology,
ST segment changes, AF detection.

**Why deferred:** Requires signal processing expertise and libraries
(NeuroKit2, WFDB) distinct from the NLP/data engineering work.
ECG metadata in Silver already captures the clinically relevant
summary (rhythm, rate, conclusion) for corpus grounding.

**What it unlocks:**
- Time series ML for cardiac event prediction
- Multimodal clinical AI — ECG signal + note text + structured data
- A legitimate cardiology AI portfolio piece

**Technical path:**
```python
# NeuroKit2 extraction from decoded Binary
import neurokit2 as nk

def extract_ecg_features(waveform_bytes: bytes) -> dict:
    signal = parse_sbml_to_array(waveform_bytes)
    signals, info = nk.ecg_process(signal, sampling_rate=500)
    return {
        "heart_rate_mean": info["ECG_Rate_Mean"],
        "hrv_rmssd": nk.hrv_time(signals)["HRV_RMSSD"],
        "qrs_duration_ms": info["ECG_QRS_Duration"],
        "af_probability": info.get("AF_Probability", None),
    }
```

**Data:** Already in Bronze (`ecg_signals/`). `has_waveform` and
`waveform_binary_id` in `silver.ecg_metadata` are the pointers.

**Prerequisite:** Verify Synthea Coherent ECG waveform format.
SBML models may produce non-standard signal arrays.

---

### Phase 4 — DICOM Pixel Extraction and Imaging ML

**What:** Full pixel extraction from DICOM MRI files.
Segmentation, radiomics feature extraction, imaging-text multimodal models.

**Why deferred:** GPU required for segmentation inference.
Header metadata already provides clinical context for corpus.
Pixel processing is a separate ML engineering workstream.

**What it unlocks:**
- Brain MRI segmentation (Synthea Coherent is CVD-focused — brain imaging)
- Imaging + clinical note multimodal models
- Radiology report generation grounded in actual image features
- Strong signal for healthcare imaging ML roles

**Technical path:**
```python
import pydicom
import monai

# Phase 3 adds pixel loading
ds = pydicom.dcmread(dicom_path)   # without stop_before_pixels
pixel_array = ds.pixel_array        # numpy array

# MONAI segmentation
from monai.transforms import LoadImage, Compose
from monai.networks.nets import UNet
```

**Data:** Already in Bronze (`dicom/`). `dicom_binary_id` in
`silver.imaging_study` is the pointer back to source.

**Prerequisite:** GPU environment. MONAI models for brain MRI.
Verify DICOM format consistency across Synthea Coherent patients.

---

### Phase 5 — Real Clinical Genomic Data Pipeline

**What:** Replace Synthea simulated inheritance with real
clinical genomic datasets. VCF parsing, variant annotation,
pharmacogenomics, clinical actionability scoring.

**Why deferred:** Synthea Coherent genomics is inheritance simulation —
no clinically actionable variants (no BRCA, CYP2D6, HLA typing).
Building a VCF pipeline on simulated inheritance data produces
a technically complete pipeline with clinically meaningless output.
This requires real data to be worth building.

**What it unlocks:**
- Pharmacogenomics — drug-gene interactions from CYP variants
- Hereditary cancer risk — BRCA1/2 pathogenicity
- Population genomics — allele frequency analysis
- Direct clinical decision support input

**Real data sources (when credentialed):**
```
ClinVar       — variant pathogenicity annotations (public)
gnomAD        — population allele frequencies (public)
PharmGKB      — drug-gene interactions (free academic)
UK Biobank    — real genomic cohort (credentialed, competitive)
TCGA          — cancer genomics (dbGaP credentialing)
```

**Technical path:**
```python
# VCF parsing with cyvcf2
import cyvcf2

def parse_vcf_variants(vcf_path: str) -> list[dict]:
    vcf = cyvcf2.VCF(vcf_path)
    variants = []
    for variant in vcf:
        variants.append({
            "chrom": variant.CHROM,
            "pos": variant.POS,
            "ref": variant.REF,
            "alt": str(variant.ALT[0]),
            "qual": variant.QUAL,
            "filter": variant.FILTER,
            "gene": variant.INFO.get("GENE", None),
        })
    return variants

# Variant annotation with ClinVar lookup
# Pharmacogenomics with PharmGKB API
```

**Prerequisite:** Real genomic data access. dbGaP or equivalent
credentialing process. This is a multi-week project on its own.

---

### Phase 6 — Streaming Production Architecture

**What:** Replace simulated streaming with real event-driven ingestion.
FHIR R4 subscriptions → Azure EventHub → Fabric Eventstream →
Structured Streaming → Silver MERGE.

**Why deferred:** Requires a live FHIR server emitting change events.
Synthea is a static dataset — no source system to subscribe to.

**What it unlocks:**
- Near real-time patient data in Silver (seconds vs minutes)
- SCD Type 2 patient record versioning
- Event-driven Gold refresh
- Production-ready architecture for EHR integration

**Technical path:**
```
Azure Health Data Services (FHIR server)
  → FHIR R4 subscription on Patient/Encounter/Condition
  → Azure EventHub (change events as FHIR JSON)
  → Fabric Eventstream
  → Structured Streaming job
  → Silver MERGE with WHEN MATCHED / WHEN NOT MATCHED
  → CDC change feed to downstream
```

**Portfolio note:** Document this architecture diagram in
PRODUCTION_NOTES.md now. The streaming simulation pattern
in Phase 1 is explicitly designed to map 1:1 to this.

---

### Roadmap Summary Table

| Phase | What | Deferred because | Unlocks | Effort |
|---|---|---|---|---|
| 2 | Ollama Gold generation | Separate spec, ~18hr run | Full corpus for AI | 1 weekend |
| 3 | ECG waveform features | Signal processing specialty | Cardiac time series ML | 1-2 weeks |
| 4 | DICOM pixel + imaging ML | GPU required, separate ML workstream | Radiology AI, multimodal | 2-4 weeks |
| 5 | Real genomic pipeline | Need real data, VCF on synthetic = low value | Pharmacogenomics, cancer risk | 3-6 weeks + credentialing |
| 6 | Real streaming architecture | No live FHIR source | Production EHR integration | 1-2 weeks + Azure setup |

---

### What is NOT on the roadmap and why

**OMOP CDM conversion:** Converting Silver to OMOP Common Data Model
is a legitimate healthcare data engineering pattern. Decided against
it for this project — OMOP is a warehouse pattern, the lakehouse
serves AI workloads better with denormalized Gold tables.
Worth a separate project if targeting clinical research roles.

**HL7 v2 ingestion:** Legacy hospital messaging format.
Synthea doesn't emit HL7 v2. Out of scope for this dataset.
Relevant for hospital system integration roles — document as
a known gap for production healthcare data engineering.

**SMART on FHIR app layer:** Application-level FHIR queries.
Scribe IQ is the app layer. The lakehouse is the data layer.
Clear separation maintained.

---

## 9. Implementation Sequence for Claude Code

### Session 1 — Repo scaffold + FHIR parser foundation

```
1. Init repo structure, pyproject.toml, requirements.txt
2. Download 5-patient sample bundle from S3 (no-sign-request)
   aws s3 cp s3://synthea-open-data/coherent/fhir/ tests/fixtures/
             --no-sign-request --recursive --max-keys 5
3. local/transforms/fhir_parser.py
   - FHIRBundleParser class
   - extract_patient, extract_encounter
   - extract_soap_note (Base64 decode + SOAP section detection)
   - extract_ecg_metadata
   - extract_imaging_study (metadata only)
   - extract_genomic_report (metadata only)
4. tests/fixtures/sample_bundle.json (5 patients)
5. tests/test_fhir_parser.py
6. tests/test_silver_soap_notes.py
Goal: parser handles all resource types, tests pass on fixture data
```

### Session 2 — Local Bronze + Silver pipeline

```
1. local/ingest/download.py — S3 sync with cohort partitioning
2. local/ingest/bronze_landing.py — write Delta via delta-rs
3. local/ingest/streaming_sim.py — watchdog-based Auto Loader sim
4. local/transforms/silver_patient.py
5. local/transforms/silver_encounter.py
6. local/transforms/silver_clinical.py (Condition, Observation, Med, Procedure)
7. local/transforms/silver_soap_notes.py
8. local/transforms/silver_ecg.py
9. local/transforms/silver_imaging.py
10. local/transforms/silver_genomics.py
11. local/validation/schema_registry.py + validate.py
12. local/pipeline.py — end-to-end orchestration
Goal: full pipeline runs locally on 5-patient fixture, all Silver tables written
```

### Session 3 — Gold layer + corpus contract

```
1. local/gold/encounter_summary.py — denormalize Silver → Gold
2. local/gold/corpus_manifest.py — lineage tracking
3. schemas/ — JSON Schema files for all Silver + Gold tables
4. docs/CORPUS_CONTRACT.md — handoff schema for Ollama spec
5. tests/test_gold_encounter_summary.py
6. Run full pipeline on small cohort (50 patients), validate Gold output
Goal: Gold encounter_summary populated, corpus contract documented
```

### Session 4 — Dagster local orchestration (ADR-015, ADR-016)

```
1. orchestration/partitions.py — cohort partitions (from cohort_labels)
2. orchestration/resources.py  — LakehousePlatform as a Dagster resource (LAKEHOUSE_PLATFORM)
3. orchestration/assets.py     — Silver multi_asset (parse-once → 10 tables),
                                 gold_encounter_summary + corpus_manifest assets
4. orchestration/checks.py     — @asset_check per Silver table wrapping validate_table()
5. orchestration/sensors.py    — Bronze cohort sensor (Auto Loader analogue, §5.2)
6. orchestration/definitions.py — Definitions(assets, asset_checks, resources, sensors)
7. tests/test_dagster_defs.py  — materialize on fixture; checks pass; Definitions loads
8. dagster + dagster-webserver in pyproject [dev]; ADR-015/016 written
Goal: `dagster dev` shows the medallion asset graph; per-cohort backfill works;
      assets reuse the pure transforms — zero duplicate logic (third execution surface
      alongside the CLI and the Fabric notebooks). local/pipeline.py CLI kept for CI.
```

### Session 5 — Fabric notebooks

```
1. fabric/notebooks/00_setup.ipynb
2. fabric/notebooks/01_bronze_ingest.ipynb (S3 shortcut or Copy Pipeline)
3. fabric/notebooks/02_silver_patient.ipynb
4. fabric/notebooks/03_silver_encounter.ipynb
5. fabric/notebooks/04_silver_clinical.ipynb
6. fabric/notebooks/05_silver_soap_notes.ipynb (Auto Loader streaming)
7. fabric/notebooks/06_silver_ecg.ipynb
8. fabric/notebooks/07_silver_imaging.ipynb
9. fabric/notebooks/08_silver_genomics.ipynb
9. fabric/notebooks/09_gold_encounter_summary.ipynb
10. fabric/notebooks/10_corpus_export.ipynb
11. Enable CDC on all Silver tables
12. Run full pipeline in Fabric — capture screenshots
Goal: Full pipeline running in Fabric, all tables populated, CDC enabled
```

### Session 6 — CI, docs, screenshots, README

```
1. .github/workflows/ci.yml
2. .github/workflows/validate_schemas.yml
3. docs/ — all markdown files
4. mkdocs.yml — MkDocs site (mirrors campus-rag pattern)
5. PRODUCTION_NOTES.md
6. README.md — reviewer guide table, architecture diagram, quick start
7. Capture Fabric screenshots:
   - OneLake file explorer showing Bronze structure
   - Silver Delta tables in Fabric lakehouse view
   - Notebook run showing row counts per table
   - Gold encounter_summary sample rows
8. Export notebooks from Fabric as .ipynb files
9. Push everything to GitHub
Goal: Public repo, Fabric screenshots captured before trial expires
```

---

## 10. README Structure

```
# scribe-iq-lakehouse

Badge row: CI | Python | Delta Lake | Fabric | Synthea

One-line: Production-pattern healthcare data lakehouse on Synthea Coherent.

## Reviewer guide (same table pattern as campus-rag)

## What this shows (signals table)

## Architecture diagram

## Data sources (Synthea Coherent attribution)

## Medallion layers (Bronze / Silver / Gold table list)

## Streaming simulation (Auto Loader pattern explained briefly)

## Quick start
  Local: python local/pipeline.py
  Fabric: Run notebooks 00 → 10 in sequence

## Silver tables (link to DATA_DICTIONARY.md)

## Corpus contract (link to CORPUS_CONTRACT.md)
  "Gold encounter_summary feeds scribe-iq and clinical-bert-pipeline"

## Fabric execution (screenshots)

## Production seams (link to PRODUCTION_NOTES.md)

## Roadmap
  Phase 1 — Bronze + Silver + Gold (this repo) ✓
  Phase 2 — Ollama Gold generation (separate spec)
  Phase 3 — ECG waveform feature extraction
  Phase 4 — DICOM pixel extraction + imaging ML
  Phase 5 — Real clinical genomic data pipeline

## Production seams (link to PRODUCTION_NOTES.md)

## Related projects
  scribe-iq — consumes Gold corpus for RAG + generative workflows
  clinical-bert-pipeline — consumes Gold corpus for NLP inference (Phase 3)
```

---

## 11. Fabric Trial — Priority Capture Checklist

Before trial expires, ensure these are captured permanently:

```
□ All notebooks exported as .ipynb and committed to repo
□ Screenshots: OneLake file browser (Bronze structure)
□ Screenshots: Lakehouse Silver tables view
□ Screenshots: Notebook run output with row counts
□ Screenshots: Gold encounter_summary in Fabric SQL endpoint
□ Screenshots: CDC change feed query result
□ fabric/config/lakehouse_config.json with actual OneLake paths
□ README Fabric section written with screenshots embedded
□ docs/ARCHITECTURE.md updated with actual Fabric workspace structure
```

Local delta-rs pipeline is the fallback once trial expires.
The Delta format is identical — same notebooks, different storage path.

---

## 12. Downstream Connections

### → scribe-iq

```python
# scribe-iq corpus loader reads Gold encounter_summary
# Replaces current data_prep/ pipeline
# Schema contract: docs/CORPUS_CONTRACT.md
```

### → clinical-bert-pipeline (Phase 3)

```python
# ClinicalBERT NER model runs inference on gold.soap_note_text
# Writes entity annotations back to gold.entity_annotations
# Annotated notes available for RAG retrieval in scribe-iq
```

### → Ollama Gold generation (next spec)

```python
# Reads gold.encounter_summary
# Uses soap_note_text as grounding anchor
# Generates synthetic_dialogue + synthetic_note_unstructured
# Writes back to gold layer with generation audit record
```

---

*Document version: 3.0 — May 2026*
*Added: DICOM header metadata extraction (pydicom stop_before_pixels),*
*genomic report metadata with honest limitation field,*
*updated Silver schemas for imaging_study and genomic_report,*
*updated Gold imaging struct, comprehensive 6-phase roadmap,*
*explicit reasoning for each deferral decision*
*Status: Ready for implementation via Claude Code*
*Ollama Gold generation spec: separate document, follows this spec*

---

## 13. Local Spark + Ollama notes

Local pipeline work runs on Apple Silicon (MPS). Key configs:

```python
# Spark local mode for delta-rs pipeline
spark = (SparkSession.builder
    .master("local[10]")
    .config("spark.driver.memory", "24g")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .getOrCreate())
```

Ollama on Apple Silicon:
- llama3.1:8b runs at ~45 tokens/sec on MPS
- 6,000 notes × 500 tokens ÷ 45 tok/s ÷ 3600 = ~18 hours generation
- Ollama Gold generation pipeline MUST be resumable with checkpointing
- Run overnight across multiple sessions
- See Ollama spec (separate document) for checkpoint design

---

## 14. Fabric DevOps, Observability, and Production Engineering

This section defines everything required to run the lakehouse pipeline
as a production-grade system in Fabric — not just notebooks that work once.

### 14.1 Fabric Workspace Structure

```
Workspace: scribe-iq-lakehouse
│
├── Lakehouses
│   ├── bronze_lakehouse       # Raw landing zone
│   ├── silver_lakehouse       # Validated Delta tables
│   └── gold_lakehouse         # Corpus + AI-ready outputs
│
├── Notebooks
│   ├── 00_setup
│   ├── 01_bronze_ingest
│   ├── 02_silver_patient ... 08_silver_genomics
│   ├── 09_gold_encounter_summary
│   └── 10_corpus_export
│
├── Pipelines
│   ├── master_pipeline        # Full Bronze → Silver → Gold
│   ├── bronze_ingest_pipeline # Bronze only, schedulable
│   ├── silver_refresh_pipeline # Silver only, triggered
│   └── gold_refresh_pipeline  # Gold only, on Silver completion
│
├── Data Activator
│   └── pipeline_health_alerts # Alert on row count drops, failures
│
├── Semantic Model
│   └── lakehouse_metrics      # Power BI dataset for monitoring dashboard
│
└── Reports
    └── pipeline_health_dashboard # Live monitoring report
```

---

### 14.2 Pipeline Orchestration

Master pipeline in Fabric Data Factory pattern:

```
master_pipeline
│
├── Activity: ValidateBronzeFiles
│   Type: Notebook (00_setup)
│   On fail: Send alert → stop pipeline
│
├── Activity: IngestBronze
│   Type: Notebook (01_bronze_ingest)
│   On fail: Send alert → stop pipeline
│   On success: Log row count to ingest_log
│
├── Activity: SilverPatientEncounter (parallel group 1)
│   ├── Notebook: 02_silver_patient
│   └── Notebook: 03_silver_encounter
│   Wait for all: true
│
├── Activity: SilverClinicalData (parallel group 2)
│   ├── Notebook: 04_silver_clinical
│   ├── Notebook: 05_silver_soap_notes
│   └── Notebook: 06_silver_ecg
│   Depends on: SilverPatientEncounter
│   Wait for all: true
│
├── Activity: SilverModalityData (parallel group 3)
│   ├── Notebook: 07_silver_imaging
│   └── Notebook: 08_silver_genomics
│   Depends on: SilverPatientEncounter
│   Wait for all: true
│
├── Activity: ValidateSilver
│   Type: Notebook (validation)
│   Depends on: SilverClinicalData, SilverModalityData
│   On fail: Send alert → stop pipeline
│
├── Activity: BuildGold
│   Type: Notebook (09_gold_encounter_summary)
│   Depends on: ValidateSilver
│
└── Activity: ExportCorpus
    Type: Notebook (10_corpus_export)
    Depends on: BuildGold
    On success: Send success notification
```

Parallel groups 2 and 3 run simultaneously — Silver clinical,
SOAP notes, ECG, imaging, and genomics all process in parallel
after patient and encounter tables are ready.

---

### 14.3 Observability Stack

#### Ingest Log Table — `silver.ingest_log`

Every pipeline activity writes a record:

```sql
CREATE TABLE silver.ingest_log (
    log_id          STRING,
    pipeline_run_id STRING,       -- Fabric pipeline run ID
    activity_name   STRING,
    target_table    STRING,
    source_file     STRING,
    rows_written    BIGINT,
    rows_updated    BIGINT,
    rows_failed     BIGINT,
    duration_seconds INT,
    status          STRING,       -- SUCCESS / FAILURE / PARTIAL
    error_message   STRING,
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP,
    spark_app_id    STRING,       -- for log correlation
    notebook_version STRING       -- git commit hash
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

#### Pipeline Metrics Table — `silver.pipeline_metrics`

Aggregate metrics per pipeline run:

```sql
CREATE TABLE silver.pipeline_metrics (
    run_id              STRING,
    run_timestamp       TIMESTAMP,
    total_patients      BIGINT,
    total_encounters    BIGINT,
    total_soap_notes    BIGINT,
    total_ecg_records   BIGINT,
    gold_summaries      BIGINT,
    validation_passed   BOOLEAN,
    pipeline_duration_s INT,
    bronze_size_gb      DOUBLE,
    silver_size_gb      DOUBLE,
    gold_size_gb        DOUBLE
)
USING DELTA;
```

#### Row Count Drift Detection

After each Silver write, compare against previous run:

```python
def check_row_count_drift(table_name, current_count,
                           threshold_pct=0.10):
    """
    Fail if row count drops more than threshold vs last run.
    Protects against silent data loss.
    """
    prev = get_last_run_count(table_name)
    if prev and current_count < prev * (1 - threshold_pct):
        raise DataQualityException(
            f"{table_name}: row count dropped "
            f"{prev} → {current_count} "
            f"({(prev-current_count)/prev*100:.1f}%)"
        )
    log_metric(table_name, current_count)
```

#### Spark Metrics

Each notebook captures and logs:

```python
# After each major write operation
metrics = {
    "records_written": df.count(),
    "partitions": df.rdd.getNumPartitions(),
    "avg_record_size_bytes": ...,
    "duration_ms": end_time - start_time,
    "spark_app_id": spark.sparkContext.applicationId,
}
log_to_ingest_log(**metrics)
```

---

### 14.4 Data Quality Framework

Four layers of quality checks, run in sequence:

```
Layer 1 — Schema validation     Does the data match expected types?
Layer 2 — Completeness          Are required fields populated?
Layer 3 — Referential integrity Do FKs resolve across tables?
Layer 4 — Business rules        Are values in valid clinical ranges?
```

#### Quality Rules per Table

```python
QUALITY_RULES = {
    "silver.patient": {
        "schema": patient_schema,
        "not_null": ["patient_id", "birth_date", "gender"],
        "unique": ["patient_id"],
        "rules": [
            ("birth_date <= current_date()", "No future birth dates"),
        ]
    },
    "silver.soap_note": {
        "schema": soap_note_schema,
        "not_null": ["note_id", "patient_id", "note_text"],
        "referential": {"patient_id": "silver.patient"},
        "rules": [
            ("char_count >= 100", "Notes under 100 chars suspect"),
            ("char_count <= 50000", "Notes over 50k chars suspect"),
        ],
        "aggregate": {
            "soap_completeness_pct": ("has_all_sections", 0.80),
        }
    },
    "silver.ecg_metadata": {
        "not_null": ["ecg_id", "patient_id", "report_date"],
        "referential": {"patient_id": "silver.patient",
                        "encounter_id": "silver.encounter"},
        "rules": [
            ("heart_rate_bpm BETWEEN 30 AND 250 OR "
             "heart_rate_bpm IS NULL",
             "Heart rate outside physiological range"),
        ]
    },
    "gold.encounter_summary": {
        "not_null": ["summary_id", "patient_id", "encounter_id"],
        "referential": {
            "patient_id": "silver.patient",
            "encounter_id": "silver.encounter"
        },
        "rules": [
            ("patient_age BETWEEN 0 AND 130", "Age out of range"),
            ("array_size(active_conditions) >= 0",
             "Conditions array malformed"),
        ]
    }
}
```

#### Quality Report

Written to `silver.quality_report` after each run:

```json
{
  "run_id": "abc123",
  "timestamp": "2026-05-24T10:00:00",
  "tables": {
    "silver.soap_note": {
      "row_count": 6287,
      "null_violations": 0,
      "referential_violations": 2,
      "rule_violations": 14,
      "soap_completeness_pct": 0.91,
      "quality_score": 0.997,
      "passed": true
    }
  },
  "overall_passed": true
}
```

---

### 14.5 Git and Version Control for Fabric

Fabric notebooks connect to GitHub via Git integration.
Every notebook commit includes:

```
fabric/notebooks/05_silver_soap_notes.ipynb

Commit message format:
  feat(silver): add SOAP note Base64 extraction
  fix(ecg): handle missing heart_rate observation
  chore(pipeline): update row count thresholds
```

Notebook version (git commit hash) is logged to `ingest_log`
on every run — full lineage from code version to data output.

#### Branch Strategy

```
main          production-ready notebooks, protected
develop       integration branch
feature/*     individual notebook development
hotfix/*      urgent production fixes
```

Pull request required to merge to main.
CI validates notebook syntax and schema contracts on every PR.

---

### 14.6 Environment Configuration

```python
# fabric/config/lakehouse_config.json
{
  "workspace": "scribe-iq-lakehouse",
  "environment": "production",       # production / staging / dev
  "bronze_lakehouse": "bronze_lakehouse",
  "silver_lakehouse": "silver_lakehouse",
  "gold_lakehouse": "gold_lakehouse",
  "onelake_path": "abfss://...",
  "validation": {
    "min_soap_notes": 5000,
    "min_patients": 1000,
    "row_count_drift_threshold": 0.10,
    "soap_completeness_threshold": 0.80
  },
  "alerts": {
    "email": "sandeep@...",
    "on_failure": true,
    "on_quality_fail": true,
    "on_success": false
  },
  "streaming": {
    "trigger_interval": "30 seconds",
    "max_files_per_trigger": 10,
    "checkpoint_location": "bronze/_checkpoints/"
  }
}
```

Separate config files per environment — dev/staging/production.
Never hardcode paths in notebooks.

---

### 14.7 Monitoring Dashboard — Power BI / Fabric Report

Build a live pipeline health dashboard reading from
`silver.pipeline_metrics` and `silver.ingest_log`.

**Dashboard pages:**

**Page 1 — Pipeline Health**
```
KPI cards:
  Last run status (SUCCESS / FAILURE)
  Last run timestamp
  Total patients ingested
  Total SOAP notes extracted
  Pipeline duration (seconds)

Charts:
  Row counts over time per Silver table (line chart)
  Pipeline duration trend (bar chart)
  Quality score per table per run (heatmap)
```

**Page 2 — Data Quality**
```
Table: quality_report latest run
  Table name | Row count | Violations | Quality score | Pass/Fail

Chart: Quality score trend over runs
Chart: Violation breakdown by rule type
```

**Page 3 — Corpus Readiness**
```
Gold encounter_summary stats:
  Total summaries
  % with SOAP notes
  % with ECG data
  % with imaging
  % with genomics
  Condition distribution (bar chart)
  Encounter type distribution (pie chart)
  Patient age distribution (histogram)
```

**Page 4 — Streaming Simulation**
```
Auto Loader metrics:
  Files processed per cohort
  Records per batch
  Checkpoint progress
  Lag (simulated — cohorts remaining)
```

---

### 14.8 Alerting — Fabric Data Activator

Configure Data Activator rules on `silver.pipeline_metrics`:

```
Rule 1: Pipeline Failure Alert
  Trigger: status = 'FAILURE'
  Action: Email + Teams notification
  Message: "scribe-iq-lakehouse pipeline failed — {error_message}"

Rule 2: Row Count Drop Alert
  Trigger: row_count_drift > 10%
  Action: Email notification
  Message: "Row count dropped {pct}% in {table_name}"

Rule 3: Quality Gate Failure
  Trigger: quality_score < 0.95
  Action: Email notification
  Message: "Quality gate failed for {table_name}: {score}"

Rule 4: Long Running Pipeline
  Trigger: pipeline_duration_s > 3600
  Action: Email notification
  Message: "Pipeline running >1hr — investigate"
```

---

### 14.9 Security

```
OneLake RBAC:
  bronze_lakehouse   READ: pipeline service principal only
                     WRITE: pipeline service principal only
  silver_lakehouse   READ: downstream consumers, BI service principal
                     WRITE: pipeline service principal only
  gold_lakehouse     READ: scribe-iq app, clinical-bert app
                     WRITE: pipeline service principal only

Secrets:
  No credentials in notebooks
  All keys via Fabric Environment Variables or Key Vault reference
  DagsHub token (if used) via environment variable

Data classification:
  All data: SYNTHETIC — clearly labeled in lakehouse metadata
  No PHI tagging required (synthetic data only)
  Document production PHI path in PRODUCTION_NOTES.md
```

---

## 15. Fabric Demo Plan

### 15.1 What Makes a Successful Demo

A successful Fabric demo for this project proves three things:

1. **The pipeline runs** — not just screenshots of tables, but evidence of data flowing Bronze → Silver → Gold
2. **Production patterns are real** — DevOps, observability, quality checks are visible and working
3. **The output is useful** — Gold `encounter_summary` is clearly the input to something real (Scribe IQ, BERT)

Every demo asset should be capturable before the trial expires.

---

### 15.2 Screenshots Checklist

Capture in this order — earlier items are foundations for later ones.

**Workspace and structure**
```
□ Fabric workspace overview showing all items
  (Lakehouses, Notebooks, Pipelines, Reports)
□ Bronze lakehouse Files view — cohort partition structure visible
  bronzw/fhir/cohort=A/, cohort=B/, cohort=C/
□ Silver lakehouse Tables view — all Silver Delta tables listed
□ Gold lakehouse Tables view — encounter_summary visible
```

**Bronze ingestion**
```
□ S3 shortcut config in Fabric (showing open data connection)
□ Bronze Files view after ingest — file count, folder structure
□ Notebook 01 output cell — file count, total size, manifest written
```

**Silver pipeline — key notebooks**
```
□ Notebook 05 (SOAP notes) — streaming output showing records processed
  Must show: records written count, schema, sample rows
□ Notebook 06 (ECG metadata) — output showing ECG records extracted
□ Auto Loader checkpoint directory in Bronze
□ Silver soap_note table — sample rows with decoded note text visible
□ Silver ecg_metadata table — heart_rate, rhythm, conclusion visible
□ CDC enabled — show TBLPROPERTIES with changeDataFeed = true
```

**Data quality**
```
□ Quality report notebook output — table with pass/fail per table
□ silver.quality_report table — rows visible
□ silver.ingest_log table — pipeline run rows with durations
```

**Gold layer**
```
□ Gold encounter_summary — sample rows
  Must show: patient_id, conditions array, soap_note_text snippet,
             ecg_finding, has_imaging, has_genomics columns
□ Gold corpus_manifest — lineage rows visible
□ Notebook 09 output — row count, join stats
```

**Pipeline orchestration**
```
□ Master pipeline canvas — all activities and dependencies visible
□ Pipeline run result — green checkmarks on all activities
□ Pipeline run detail — duration per activity
□ Parallel activities visible in run timeline
```

**Monitoring dashboard**
```
□ Pipeline Health page — KPI cards populated with real run data
□ Data Quality page — quality scores table
□ Corpus Readiness page — charts showing data distribution
```

**Git integration**
```
□ Fabric Git integration settings — GitHub repo connected
□ Notebook showing last commit hash in header
□ Commit history view in Fabric
```

---

### 15.3 Demo Script — Walkthrough Narrative

Use this script for any recorded demo or live walkthrough.

**Opening (30 seconds)**
> "This is scribe-iq-lakehouse — a production-pattern healthcare data
> lakehouse built on Synthea Coherent, the richest publicly available
> synthetic longitudinal patient dataset. It ingests 9GB of FHIR data
> including SOAP notes, ECG metadata, DICOM, and genomics, processes
> it through a medallion architecture in Microsoft Fabric, and produces
> a governed corpus that feeds two downstream AI projects."

**Workspace overview (1 minute)**
- Show workspace: three lakehouses, 11 notebooks, 4 pipelines, 1 report
- Point out the separation: Bronze (raw), Silver (validated), Gold (AI-ready)
- Show Git integration — every notebook is version-controlled

**Bronze layer (1 minute)**
- Show S3 shortcut to Synthea Coherent open data
- Show cohort partitions — explain this is the streaming simulation anchor
- Open Bronze Files view — show file count, sizes

**Streaming simulation (1 minute)**
- Explain Auto Loader pattern
- Show Notebook 01 — run it live or show last output
- Watch files land in Bronze with ingest log entry written

**Silver SOAP notes extraction (2 minutes)**
- This is the centerpiece — spend time here
- Open Notebook 05 — walk through Base64 decode logic
- Show the streaming query — explain readStream + foreachBatch
- Show silver.soap_note table — click a row, show decoded SOAP text
- Explain the S/O/A/P section detection
- Show CDC enabled on the table

**Silver ECG metadata (1 minute)**
- Open silver.ecg_metadata — show rhythm, heart_rate_bpm, conclusion
- Explain: waveform Binary landed in Bronze, metadata extracted to Silver
- Point to roadmap: waveform processing is Phase 3

**Data quality (1 minute)**
- Open quality_report output
- Show all tables passing
- Show ingest_log — run durations, row counts
- Show row count drift check passing

**Gold layer (1 minute)**
- Open gold.encounter_summary
- Show a row: patient age, conditions array, SOAP note snippet, ECG finding
- "This is the input to the Ollama generation pipeline and to Scribe IQ"
- Show corpus_manifest — lineage back to source Silver records

**Pipeline orchestration (1 minute)**
- Open master_pipeline canvas
- Show parallel activity groups
- Show last successful run — green checkmarks, duration per activity

**Monitoring dashboard (1 minute)**
- Open Power BI report
- Show Pipeline Health KPIs — last run status, row counts
- Show Corpus Readiness — SOAP note coverage, ECG coverage

**Closing (30 seconds)**
> "The Gold encounter_summary feeds Scribe IQ's RAG layer directly,
> replacing the current heuristic corpus. It also feeds the
> clinical-bert-pipeline for discriminative NLP. The Ollama generation
> spec builds on top of this — using the SOAP notes as grounding
> anchors for synthetic dialogue generation."

**Total: ~10 minutes**

---

### 15.4 Video Capture Plan

**Tool:** OBS Studio or QuickTime screen recording

**What to record:**

```
Video 1 — Pipeline Run (3-4 min)
  Start: master_pipeline canvas idle
  Action: Trigger pipeline run
  Record: Activity completion in real time
  End: All green, pipeline_metrics row written
  Purpose: Proves the pipeline actually runs

Video 2 — SOAP Note Extraction (2 min)
  Start: Bronze FHIR JSON file open — show raw Base64
  Action: Run Notebook 05 cell by cell
  Record: Decode step, section detection output,
          final silver.soap_note row with readable text
  Purpose: The technically interesting moment

Video 3 — Streaming Simulation (2 min)
  Start: Only cohort=A in Bronze
  Action: Drop cohort=B partition manually
  Record: Auto Loader picking up new files, Silver row count incrementing
  End: CDC change feed query showing new rows
  Purpose: Demonstrates streaming pattern live

Video 4 — Dashboard Walkthrough (2 min)
  Start: Power BI report Pipeline Health page
  Action: Click through all 4 pages, explain each chart
  End: Corpus Readiness page — show SOAP note coverage %
  Purpose: Observability story

Video 5 — Gold → Downstream (1 min)
  Start: gold.encounter_summary table
  Action: Filter to a patient, show all columns
  End: Open CORPUS_CONTRACT.md in browser alongside
  Purpose: Shows the handoff to Ollama/Scribe IQ is real
```

**Recording tips:**
- Use 1440p or 4K — Fabric UI is detail-rich, low res looks bad
- Record audio narration live — easier than adding voiceover later
- Keep each video under 4 minutes — most viewers watch the first 2 min
- Upload to a video host (unlisted is fine), embed link in README

---

### 15.5 Trial Expiry — What Survives

When the Fabric trial ends, the portfolio evidence that persists:

```
Survives trial expiry          Lost when trial expires
──────────────────────────    ──────────────────────────
All notebooks in GitHub        Live Fabric workspace
All screenshots in docs/       Running Delta tables
Video recordings uploaded      Power BI reports
README with Fabric section     Auto Loader checkpoints
Architecture diagrams
PRODUCTION_NOTES.md
```

**Before trial expires — non-negotiable:**
```
□ All 11 notebooks exported from Fabric and committed to GitHub
□ All screenshots captured (checklist in 15.2)
□ All 5 videos recorded and uploaded
□ pipeline_metrics and quality_report table samples saved as CSV
  and committed to docs/sample_data/
□ Gold encounter_summary sample (50 rows) saved as JSON
  and committed to docs/sample_data/
□ Power BI report exported as PDF and committed to docs/
□ README Fabric section fully written with screenshots and video links
```

The sample data files in docs/ mean any reviewer can see the actual
output of the pipeline without needing Fabric access.

---

### 15.6 Local Fallback Demo — Polars Lite

After trial expires, the Polars lite pipeline provides
a runnable demo. No JVM, no cloud account, no Spark.

```bash
# Clone repo
git clone https://github.com/sandeep-jay/scribe-iq-lakehouse

# Install lite dependencies only — 8 packages, no JVM
pip install -r requirements-lite.txt

# Download 50-patient sample from Synthea Coherent open data
python local/pipeline_lite.py --cohort sample

# Inspect Gold output via DuckDB
python -c "
import duckdb
duckdb.query(
  \"SELECT patient_id, patient_age, active_conditions, \"
  \"soap_note_text FROM delta_scan('data/gold/encounter_summary') \"
  \"LIMIT 3\"
).show()
"
```

Runs in ~3 minutes on any modern laptop.
Same Delta schema as Fabric output.
Same corpus contract for downstream AI consumers.
Proves architecture is platform-independent by design.

**requirements-lite.txt:**
```
polars>=0.20
duckdb>=0.10
deltalake>=0.17
fhir.resources>=7.0
pydicom>=2.4
requests>=2.31
pydantic>=2.0
rich>=13.0
```

---

## 16. Platform Abstraction Layer

Full specification — engine-agnostic design for multi-cloud portability.

### Design principle

Apache Arrow is the interchange format between all engines.
Transforms return PyArrow tables. Platform handles read/write.
Polars, Spark, DuckDB all speak Arrow natively — zero serialization.

```
Transform function
  └── returns pa.Table (PyArrow)
        ├── LocalLitePlatform  → Polars.from_arrow() → delta-rs write
        ├── LocalSparkPlatform → spark.createDataFrame(arrow) → Delta write
        └── FabricPlatform     → spark.createDataFrame(arrow) → Delta write
```

### Abstract interface — `local/platform/base.py`

```python
from abc import ABC, abstractmethod
import pyarrow as pa

class LakehousePlatform(ABC):

    @abstractmethod
    def storage_path(self, layer: str, table: str) -> str:
        """
        Fabric:     abfss://lakehouse@onelake.dfs.fabric.microsoft.com/layer/table
        Databricks: dbfs:/mnt/lakehouse/layer/table
        AWS:        s3://bucket/lakehouse/layer/table
        GCP:        gs://bucket/lakehouse/layer/table
        Local:      data/layer/table
        """

    @abstractmethod
    def read_bronze_fhir(self, cohort: str = None) -> list[dict]:
        """Returns list of parsed FHIR bundle dicts"""

    @abstractmethod
    def write_silver(self, table: str,
                      data: pa.Table, mode: str = "merge"):
        """Writes PyArrow table to Silver Delta table"""

    @abstractmethod
    def read_silver(self, table: str) -> pa.Table:
        """Reads Silver Delta table as PyArrow"""

    @abstractmethod
    def write_gold(self, table: str, data: pa.Table):
        """Writes PyArrow table to Gold Delta table"""

    @abstractmethod
    def log_metric(self, table: str, metric: str, value):
        """Platform-appropriate metric logging"""

    @abstractmethod
    def send_alert(self, severity: str, message: str):
        """
        Fabric:     Data Activator
        Databricks: Databricks Alerts
        AWS:        SNS
        GCP:        Cloud Monitoring
        Local:      print only
        """

    @abstractmethod
    def get_spark_session(self):
        """None for LocalLitePlatform — no Spark"""
```

### Platform implementations

```
local/platform/
  base.py           Abstract interface (build now)
  factory.py        Env var router (build now)
  local_lite.py     Polars + DuckDB + delta-rs (build week 2)
  local_spark.py    PySpark local[10] (build week 2)
  fabric.py         Fabric implementation (build this week)
  databricks.py     Stub + migration notes (roadmap)
  aws.py            Stub + migration notes (roadmap)
  gcp.py            Stub + migration notes (roadmap)
```

### Factory — `local/platform/factory.py`

```python
import os

def get_platform() -> LakehousePlatform:
    p = os.getenv("LAKEHOUSE_PLATFORM", "local_lite")
    platforms = {
        "fabric":       "local.platform.fabric.FabricPlatform",
        "databricks":   "local.platform.databricks.DatabricksPlatform",
        "aws":          "local.platform.aws.AWSPlatform",
        "gcp":          "local.platform.gcp.GCPPlatform",
        "local_spark":  "local.platform.local_spark.LocalSparkPlatform",
        "local_lite":   "local.platform.local_lite.LocalLitePlatform",
    }
    module_path, class_name = platforms[p].rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)()
```

One env var. Zero code changes to migrate.

### Migration stub pattern

```python
# local/platform/databricks.py

class DatabricksPlatform(LakehousePlatform):
    """
    Databricks implementation — migration stub.

    Migration notes from Fabric:
      mssparkutils    → dbutils
      abfss://onelake → dbfs:// or s3:// mount
      Fabric Pipeline → Databricks Workflows YAML
      Data Activator  → Databricks SQL Alerts / PagerDuty
      Auto Loader     → identical cloudFiles format, no changes
      Delta Lake      → identical API, no changes
      MLflow          → native, better than Fabric implementation

    All transforms in local/transforms/ require zero changes.
    Estimated migration effort: 2-3 days.
    """

    def storage_path(self, layer: str, table: str) -> str:
        raise NotImplementedError(
            "Databricks: f'dbfs:/mnt/{mount}/{layer}/{table}'"
        )
```

### Engine comparison matrix (benchmark target)

| Capability | Polars lite | Spark local | Fabric | Databricks | AWS | GCP |
|---|---|---|---|---|---|---|
| Setup | 2 min | 15 min | 30 min | 45 min | 60 min | 60 min |
| Dependencies | pip only | JVM + pip | Cloud trial | Cloud account | AWS account | GCP account |
| Bronze ingest | ✓ | ✓ | ✓ | roadmap | roadmap | roadmap |
| Silver | ✓ | ✓ | ✓ | roadmap | roadmap | roadmap |
| Gold | ✓ | ✓ | ✓ | roadmap | roadmap | roadmap |
| Streaming | — | ✓ | ✓ | roadmap | roadmap | roadmap |
| CDC | — | ✓ | ✓ | roadmap | roadmap | roadmap |
| Cost (1k pts) | $0 | $0 | trial | TBD | TBD | TBD |

---

*Document version: 5.0 — May 2026*
*Final: locked weekend plan, 200-note portfolio corpus,*
*M5 full corpus June, corpus migration deferred, MASTER_PLAN.md*
*Status: READY FOR EXECUTION*
