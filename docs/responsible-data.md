# Healthcare Data & Responsible Handling

The project's governing principle: **state every limitation precisely, pair it with the reason
and the production-grade alternative, and treat the limitation itself as the signal.** Knowing
where synthetic data, the FHIR model, or a trial capacity stops being trustworthy — and modeling
that boundary as a first-class column, a contract note, or a roadmap item rather than hiding it —
is the difference between a demo and a system.

## Synthetic-only, no PHI

The lakehouse is built entirely on [Synthea Coherent](https://registry.opendata.aws/synthea-coherent-data/)
— the richest public synthetic longitudinal FHIR dataset (1,278 patients, MIT). There is **no
real patient information** anywhere in the repo, the tests, or the published corpus. This is a
feature, not a caveat: it is exactly why the whole pipeline is laptop-reproducible and safe to
open-source, and why a reviewer can `git clone`, run all 129 tests, and rebuild the corpus with
zero credentials. Tests run against a single synthetic fixture bundle
(`core/tests/fixtures/sample_bundle.json`), never real data.

## PHI-safe by construction ([ADR-010](adr/010-phi-safe-logging.md))

Even on synthetic data, the logging discipline is production-grade. Logs **never** contain
patient or encounter identifiers, or bundle filenames. Identifier-bearing values are redacted to
a non-reversible `ref:<hash>` before they reach a log line, via `core.redaction.redact()`. The
habit is the point — the same code on real PHI would not leak identifiers.

## Limitations, named (from the [Corpus Contract](CORPUS_CONTRACT.md))

!!! note "Medications are a forward `status=active` approximation — not a point-in-time timeline"
    `active_medications` carries forward any `status=active` med from its authoring date (good for
    chronic/ongoing meds). But FHIR carries no medication *stop* date, so a med active at a *past*
    encounter and later stopped cannot be reconstructed on those past encounters. **Conditions, by
    contrast, are temporally precise** (onset + abatement gated, [ADR-014](adr/014-problem-list-as-of-date.md)).
    A true med timeline needs the CSV `STOP` column — deliberately out of scope ([ADR-013](adr/013-dicom-ingest-and-linkage.md)).
    This is the page's clearest demonstration of clinical-data-temporality judgment.

!!! note "Genomics is synthetic — and the limitation is a first-class column ([ADR-007](adr/007-genomic-data-limitation.md))"
    Synthea models familial inheritance *simulation*, not clinically actionable variants — no real
    BRCA, CYP2D6, or HLA typing, no pathogenic variants. Rather than footnote this, every
    `silver.genomic_report` row carries a mandatory `data_limitation` column (100% populated):
    *"Synthea simulated inheritance — not clinical variants."* The constraint is modeled **as
    data**, not prose.

!!! note "`has_ecg` is always false"
    Coherent has no ECG `DiagnosticReport`s in the FHIR bundles (ECG is Binary waveform data,
    roadmap Phase 3). The `has_ecg` / `ecg_*` fields exist for forward compatibility and are
    honestly empty rather than silently dropped.

!!! note "Imaging: metadata yes, study description no"
    FHIR imaging metadata (modality, body site, series count) is present for all 3,752 studies;
    the 298 with a downloaded DICOM file additionally carry real header dimensions via
    `pydicom stop_before_pixels=True` — **pixel data is never loaded** ([ADR-006](adr/006-dicom-stop-before-pixels.md)).
    But Coherent's descriptive DICOM tags are placeholder `"UNKNOWN"`, normalized to null
    ([ADR-013](adr/013-dicom-ingest-and-linkage.md)) — so there's no human-readable study
    description to ground on. Use `modality` + `body_site_display`.

!!! note "Vitals coverage is partial (~14% of encounters)"
    Only encounters with linked vital-sign observations populate `recent_vitals`. Blood pressure
    is parsed from the Silver `components_json` (systolic LOINC 8480-6 / diastolic 8462-4).

## Fabric-trial scope

The Fabric tier ran **green end-to-end on F4 trial capacity against a 100-patient sample**
(notebooks 00–10); the full 1,280-bundle re-run is pending. Framed honestly: the architecture is
validated end-to-end on real Fabric infrastructure (F4 capacity) at sample scale — the remaining
work is a full-scale re-run, not a design question — and it sits alongside a LocalLite tier that
*is* validated on the full dataset.

## FHIR & multimodal ingestion, in brief

| Data type | FHIR resource(s) | Silver handling |
|---|---|---|
| Demographics, encounters | `Patient`, `Encounter` | Structured → typed tables |
| Conditions, observations, meds, procedures | `Condition`, `Observation`, `MedicationRequest`, `Procedure` | Structured; codes kept as strings (never cast to int) |
| SOAP notes | `DocumentReference` + `Binary` (Base64) | **Decode**, not parse ([ADR-005](adr/005-fhir-binary-decode.md)); S/A/P (no Objective in Coherent) |
| Imaging | `ImagingStudy` + DICOM headers | Metadata via `pydicom stop_before_pixels` ([ADR-006](adr/006-dicom-stop-before-pixels.md)/[013](adr/013-dicom-ingest-and-linkage.md)) |
| Genomics | `DiagnosticReport` + `Binary` | Report metadata + `data_limitation` flag only ([ADR-007](adr/007-genomic-data-limitation.md)) |

Clinical codes (SNOMED, LOINC, ICD) are always stored as `str`, never cast to numeric — a small
but load-bearing healthcare-data correctness rule.
