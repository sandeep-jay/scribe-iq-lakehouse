# scribe-iq-lakehouse

Production-pattern healthcare data lakehouse on [Synthea Coherent](https://registry.opendata.aws/synthea-coherent-data/)
(~1,500 synthetic patients). Fabric-first medallion architecture: **Bronze → Silver → Gold**,
feeding `scribe-iq` (clinical RAG) and `clinical-bert-pipeline` (NLP).

> **Status:** under active development. This README is a stub — the full reviewer guide,
> architecture diagram, and Fabric screenshots land in Session 5. See
> [`docs/roadmap/scribe-iq-lakehouse-spec.md`](docs/roadmap/scribe-iq-lakehouse-spec.md)
> for the complete spec.

## Architecture at a glance

- **Platform abstraction** ([ADR-002](docs/adr/002-platform-abstraction.md)): all cloud I/O
  goes through `local/platform/`. One env var (`LAKEHOUSE_PLATFORM`) selects Fabric,
  local-lite (Polars + delta-rs), Databricks, AWS, or GCP — transforms never change.
- **Pure transforms** ([ADR-004](docs/adr/004-arrow-interchange.md)): `local/transforms/`
  is engine-agnostic Python operating on dicts, returning Apache Arrow tables. No Spark,
  Delta, or platform imports.
- **Honest data modeling**: genomic `data_limitation` is a first-class column
  ([ADR-007](docs/adr/007-genomic-data-limitation.md)); DICOM headers extracted without
  pixel data ([ADR-006](docs/adr/006-dicom-stop-before-pixels.md)).
- **PHI-safe by construction**: logs never contain patient identifiers — bundle
  references are redacted to a non-reversible `ref:<hash>`
  ([ADR-010](docs/adr/010-phi-safe-logging.md)). Synthetic data, production discipline.

## Source data

`s3://synthea-open-data/coherent/unzipped/fhir/` — 1,281 FHIR R4 bundles, AWS Open Data,
no credentials required. Synthetic data only; no PHI.

## Quick start

```bash
pip install -r requirements.txt
python -m pytest          # 43 tests, no cloud / Fabric needed
```

The FHIR parser runs against any Coherent bundle:

```python
import json
from local.transforms.fhir_parser import FHIRBundleParser

bundle = json.load(open("path/to/patient.json"))
records = FHIRBundleParser().parse_bundle(bundle)
#  -> {"patient": [...], "encounter": [...], "soap_note": [...], ...}
```

## Layout

| Path | Purpose |
|------|---------|
| `local/platform/` | Cloud/engine abstraction (the only place platform code lives) |
| `local/transforms/` | Pure FHIR → record transforms (return Arrow tables) |
| `fabric/notebooks/` | Fabric execution notebooks 00–10 (Session 4) |
| `tests/` | Unit tests + synthetic `sample_bundle.json` fixture |
| `docs/adr/` | Architecture Decision Records |

## License

MIT. Built on Synthea Coherent synthetic data (no real patient information).
