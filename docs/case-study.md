# Engineering Case Study

A data-platform case study: the problem, the constraints that shaped it, the decisions that
mattered, and the result — each decision linked to its ADR.

## Problem

Downstream clinical-AI projects ([scribe-iq](https://sandeep-jay.github.io/scribe-iq/),
clinical-bert-pipeline) need a **governed, realistic clinical corpus** to build against. Synthea
Coherent is the richest public synthetic dataset, but it ships as raw, deeply nested, multimodal
FHIR R4 — verbose JSON with Base64-embedded notes, DICOM headers, and genomic reports. It is not
queryable, not typed, and has no contract a consumer could pin to.

## Constraints

- **Laptop-reproducible *and* enterprise-credible.** A reviewer must be able to clone and run the
  whole thing with no cloud account; the same design must also stand up on Microsoft Fabric.
- **Synthetic only, honest throughout.** No PHI; and where the synthetic data or the FHIR model
  is thin, say so in the data, not in a footnote.
- **A real handoff, not a dump.** Downstream teams need a stable, versioned interface — not a
  schema that shifts every rebuild.

## Decisions that mattered

| Decision | Why | ADR |
|---|---|---|
| **Medallion (Bronze → Silver → Gold)** | Separate raw landing, typed/validated entities, and the denormalized AI-ready corpus | [012](adr/012-gold-encounter-summary.md) |
| **Decode, don't parse, SOAP notes** | Notes are Base64 inside FHIR `Binary` — a decode step, not a new format | [005](adr/005-fhir-binary-decode.md) |
| **DICOM headers without pixels** | `stop_before_pixels=True` — fast, lightweight, right for encounter context | [006](adr/006-dicom-stop-before-pixels.md) · [013](adr/013-dicom-ingest-and-linkage.md) |
| **Limitation as a first-class column** | Synthetic genomics flagged in `data_limitation`, never silently presented as real | [007](adr/007-genomic-data-limitation.md) |
| **As-of-date problem lists** | `active_conditions`/`active_medications` scoped to each encounter date — temporally honest | [014](adr/014-problem-list-as-of-date.md) |
| **Generated-first docs + a versioned contract** | A contract test gates code/JSON-Schema/docs so the handoff can't drift | [011](adr/011-generated-first-docs.md) · [012](adr/012-gold-encounter-summary.md) |
| **Independent per-platform implementations** | Each tier engine-native; compatibility by schema parity, not a leaky shared abstraction | [022](adr/022-platform-independent-implementations.md) |
| **Medallion as a Dagster asset graph** | Cohort partitions, validation as asset checks, a file sensor — orchestration as a first-class surface | [015](adr/015-dagster-local-orchestration.md) · [016](adr/016-dagster-asset-graph.md) |

The architecture **evolved**: it started as one platform-abstraction layer with an Arrow
interchange ([ADR-002](_archive/adr/002-platform-abstraction.md)/[004](_archive/adr/004-arrow-interchange.md)),
and — once Spark-on-Fabric met a Python-bridge parsing path that fought the abstraction — was
deliberately replaced by **two independent engine-native tiers emitting one contract**
([ADR-022](adr/022-platform-independent-implementations.md)). Recognizing when an abstraction is
costing more than it saves is the senior move here; the story is told in [Design Notes](design-notes.md).

## Result

- **`gold.encounter_summary`** — 143,946 rows (one per encounter), contract **v1.1.0**,
  semver + test-gated, with a lineage manifest.
- **Built end-to-end on the full 1,278-patient dataset** on a laptop: 10 Silver tables in
  **2m19s** with **0 validation failures**, then Gold in **~6.5s**.
- **Proven on Microsoft Fabric** — green end-to-end on F4 against a 100-patient sample
  (notebooks 00–10); full re-run pending.
- **129 tests**, fixture-only (no cloud/network); generated docs-as-test; pre-commit security
  scanning (detect-secrets, gitleaks, bandit, semgrep).

See the numbers in [Benchmarks](BENCHMARKS.md), the handoff in the [Corpus Contract](CORPUS_CONTRACT.md),
and the parity story in [Engine Parity](platforms/parity.md).
