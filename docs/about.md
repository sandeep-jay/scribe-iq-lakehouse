# About & Notice

## What this is

`scribe-iq-lakehouse` is a **portfolio engineering artifact** — a production-pattern healthcare
data lakehouse built to be reviewed, run, and reasoned about. It is the data-platform layer
beneath a small family of clinical-AI projects (see [Downstream & Portfolio](portfolio.md)).

## Data & privacy notice

Built entirely on [Synthea Coherent](https://registry.opendata.aws/synthea-coherent-data/)
**synthetic** data (AWS Open Data, no credentials required). It contains **no real patient
information** — no PHI. Genomic content is simulated inheritance, not clinical variants
([Responsible Data](responsible-data.md)). Not for clinical decision-making.

## License

MIT. The synthetic source dataset is published by the Synthea project under its own open terms.

## Source & companion work

- **Code:** [github.com/sandeep-jay/scribe-iq-lakehouse](https://github.com/sandeep-jay/scribe-iq-lakehouse)
- **Downstream consumer:** [scribe-iq](https://sandeep-jay.github.io/scribe-iq/) (clinical-documentation AI)
- **Companion repo:** [`fabric-lakehouse-hls-readmission`](https://github.com/sandeep-jay/fabric-lakehouse-hls-readmission)
  — a Databricks demo migrated to Fabric (different narrative, no code dependency)

## How the docs are maintained

- The [Data Dictionary](DATA_DICTIONARY.md) and the Gold JSON Schema are **generated from code**
  and verified by a CI gate — they cannot drift ([ADR-011](adr/011-generated-first-docs.md)).
- Decisions are recorded as [ADRs](adr/README.md); the [Architecture](ARCHITECTURE.md) page
  tracks the as-built system; the [Changelog](changelog.md) records what changed.

## Contact

Sandeep Jayaprakash — [github.com/sandeep-jay](https://github.com/sandeep-jay).
