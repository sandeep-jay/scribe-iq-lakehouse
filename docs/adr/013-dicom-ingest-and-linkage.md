# ADR-013: DICOM ingest, FHIR↔DICOM linkage, and header extraction

**Date:** 2026-05-27
**Status:** Accepted
**Supersedes the ingest assumption in:** ADR-006 (which still governs `stop_before_pixels`)
**Deciders:** Sandeep Jayaprakash

## Context

ADR-006 specified pydicom header extraction with `stop_before_pixels=True` and assumed the
DICOM bytes arrive embedded as FHIR `Binary` resources. Inspecting the real Coherent dataset
showed otherwise: DICOM lives in a **separate S3 prefix** (`coherent/unzipped/dicom/`, ~10 GB,
299 `.dcm` files), not in the FHIR bundles. As a result `silver.imaging_study` had its six
DICOM-header columns 100% null (`dicom_extracted = 0`) even though 3,752 FHIR ImagingStudy
rows existed. The user asked to ingest DICOM and populate the headers, and to also land the
`csv/` prefix as a first-class ingest artifact (not otherwise processed).

Two further realities surfaced on inspection of an actual file:
1. **Linkage key.** The DICOM file name is `{given}_{family}_{patientUUID}{StudyInstanceUID}.dcm`;
   the FHIR `ImagingStudy.identifier[].value` is `urn:oid:{StudyInstanceUID}`. The
   StudyInstanceUID is the exact join key.
2. **Synthetic placeholders.** Descriptive header tags are placeholder literals —
   `StudyDescription = "UNKNOWN"`, `Manufacturer = "UNKNOWN"`, `Modality = "OT"`. The
   genuinely real tags are `StudyDate`, `Rows`/`Columns` (256×256), and `SliceThickness`.

## Decision

**Ingest.** `download_assets()` syncs `dicom/` (→ `<bronze>/dicom/`) and `csv/`
(→ `<bronze>/csv/`) via the same idempotent `aws s3 sync`, writing
`_metadata/assets_manifest.json`. CSV is landed for reference only — nothing reads it.

**Linkage, with the parser kept pure.** `imaging_study_uid(resource)` reads the UID from the
FHIR identifier; `study_uid_from_filename(name)` reads it from the `.dcm` name. A new
`local/ingest/dicom_index.py::DicomIndex` (ingest layer — I/O lives here) maps UID → path and
returns bytes. `parse_bundle(bundle, dicom_resolver=index.read)` takes the resolver as a
**callback**, so the transform layer performs no file I/O and imports no platform code
(ADR-002 preserved) — the same injection pattern as Gold's `silver_versions`.

**Extraction semantics (extends ADR-006).**
- `stop_before_pixels=True` always; pixel data is never materialized.
- Coherent placeholder tokens (`UNKNOWN`, `ANONYMOUS`, …) normalize to `None` — they are
  absent-data markers, not values; storing them would pollute the Gold corpus.
- DICOM `DA` dates (`YYYYMMDD`) are reformatted to ISO `YYYY-MM-DD` for date coercion.
- **FHIR stays authoritative for `modality`** (specific `MR`/`DX`/`CT`); the DICOM file's
  generic `OT` is intentionally not read.
- `dicom_binary_id` stores the **StudyInstanceUID**, never the file name — names embed the
  patient's name and are identifier-bearing (ADR-010).
- A malformed file raises `InvalidDicomError`, which is caught per-study: FHIR metadata
  stands and `dicom_extracted` stays `False`. One bad file never aborts the run.

**Partial coverage is expected.** Only ~299 of ~3,752 studies have a DICOM file;
`dicom_extracted` flags which rows carry header data.

## Consequences

- **Positive:** `rows`, `columns`, `slice_thickness_mm`, `study_date` (and
  `magnetic_field_strength` where present) are now populated for studies with DICOM; the
  Gold `imaging` struct is richer for those encounters. The pure-parser architecture and the
  `stop_before_pixels` guarantee are both preserved. Ingest is reproducible
  (`download_assets`) and lineage-tracked.
- **Accepted limitation:** descriptive fields (`study_description`, `series_description`,
  `manufacturer`) are placeholder `UNKNOWN` in Coherent and therefore land as `null` — the
  spec's aspirational `"MRI Brain Without Contrast"` description does not exist in the data.
  This is documented in [CORPUS_CONTRACT.md](../CORPUS_CONTRACT.md).
- **Cost:** ~10 GB of `.dcm` files now live in Bronze for header-only immediate value. The
  pixel data is retained deliberately for a future Phase 3 (MONAI) pipeline; it is never
  read today.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|--------|------|------|-------------|
| Resolver callback into a pure parser (chosen) | Parser stays I/O-free (ADR-002); reuses Gold's injection pattern | One extra indirection | Best fit; keeps transform isolation |
| Read DICOM inside the parser | Fewer moving parts | Forces file I/O / paths into `local/transforms/` (breaks ADR-002) | Violates platform isolation |
| Ranged header-only S3 reads (~hundreds of MB) | ~100× less storage | Truncation risk mid-header; discards pixels needed for Phase 3 | User wants DICOM as a retained ingest asset; full read is robust |
| Store `UNKNOWN` literally in Silver | Faithful to raw bytes | Placeholder strings pollute the Gold corpus / grounding | Normalize to null instead |
| Let DICOM overwrite FHIR `modality` | One code path | Replaces specific `MR` with generic `OT` — worse data | FHIR modality is authoritative |
