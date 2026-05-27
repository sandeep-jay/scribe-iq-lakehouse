# ADR-006: DICOM stop_before_pixels metadata extraction

**Date:** 2026-05-27
**Status:** Accepted
**Deciders:** Sandeep Jayaprakash

## Context

Synthea Coherent includes DICOM MRI files for a subset of patients. Full pixel
extraction requires GPU infrastructure for meaningful segmentation and would add
significant complexity and runtime to the pipeline. However, DICOM header metadata
(modality, body part, study description, manufacturer, field strength) provides
clinically useful context for Gold encounter_summary without pixel processing.

## Decision

Extract DICOM metadata using `pydicom.dcmread(io.BytesIO(data), stop_before_pixels=True)`
in every case. Never load pixel data in this pipeline phase. Preserve `dicom_binary_id`
in silver.imaging_study as a forward pointer for Phase 4 pixel extraction. Document
this decision explicitly in PRODUCTION_NOTES.md and in every DICOM-related code comment.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|--------|------|------|-------------|
| Full pixel extraction | Complete imaging pipeline | Requires GPU, MONAI, significant complexity | Out of scope for Phase 1 |
| Skip DICOM entirely | Simplest | Loses imaging metadata from Gold | Reduces corpus richness |
| Metadata only (current) | Fast, no GPU, clinically useful | Pixel ML deferred to Phase 4 | Right trade-off for portfolio scope |

## Consequences

**Positive:**
- Imaging metadata enriches Gold encounter_summary (modality, body_site, study_description)
- Fast extraction — header read only, no pixel loading
- dicom_binary_id preserved for Phase 4 without re-ingestion

**Negative:**
- Pixel-level ML (segmentation, radiomics) is explicitly deferred to Phase 4
- Phase 4 requires GPU environment and MONAI — separate workstream

**Neutral:**
- Matches production pattern: metadata extraction often precedes pixel processing in real pipelines

## Implementation notes
- `local/transforms/fhir_parser.py` — _extract_dicom_headers() with stop_before_pixels=True
- `local/transforms/silver_imaging.py` — Silver table writer
- `fabric/notebooks/07_silver_imaging.ipynb` — Fabric extraction
- Phase 4 path: `pydicom.dcmread(path)` without stop_before_pixels + MONAI segmentation
- dicom_binary_id in silver.imaging_study is the Phase 4 entry point
