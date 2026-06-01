# Reviewer Guide

Review this as an **engineering artifact**, not a product. The goal of this page is to get you
to the evidence quickly and to be honest, up front, about what is fully built versus
in-progress.

## Pick a depth

=== "Not technical?"

    **New to data engineering?** In one line: this turns raw hospital-style data into one clean,
    trusted dataset for AI — built to run on a laptop and in the cloud, on synthetic data only.

=== "90 seconds"

    1. Read the [Home](index.md) hero + the **What this shows** evidence table.
    2. Look at the one [system diagram](index.md#how-it-fits-together): raw FHIR → medallion →
       a versioned Gold contract → downstream AI.
    3. Takeaway: this is a governed clinical **data product**, built twice (laptop + Fabric)
       against one contract, with limitations modeled as first-class.

=== "10 minutes"

    1. [Engineering Case Study](case-study.md) — problem → decisions → result.
    2. [Corpus Contract](CORPUS_CONTRACT.md) — the versioned, test-gated Gold handoff.
    3. One ADR that shows judgment: [ADR-022 (independent per-platform impls)](adr/022-platform-independent-implementations.md)
       or [ADR-014 (problem-list as-of-date)](adr/014-problem-list-as-of-date.md).

=== "30 minutes (hands-on)"

    ```bash
    git clone https://github.com/sandeep-jay/scribe-iq-lakehouse && cd scribe-iq-lakehouse
    python -m venv .venv && source .venv/bin/activate
    pip install -e ".[local,dev]"
    pytest                                   # 129 tests — no cloud / Fabric / network
    python -m core.scripts.demo_walkthrough  # one patient: Bronze → Parse → Silver → Gold
    ```

    No credentials, no cloud. The tests run against a single synthetic fixture bundle. The
    walkthrough renders one patient end-to-end, ending in a `gold.encounter_summary` row with
    the SOAP note as readable clinical text. Full run: [Runbook](RUNBOOK.md).

## What's real vs in-progress

Stated plainly so the evidence isn't oversold.

| Area | Status |
|---|---|
| LocalLite tier (`core/`) — Bronze → Silver → Gold | ✅ **Run end-to-end on all 1,278 patients** (143,946 Gold rows, 0 validation failures) |
| Dagster asset graph (local orchestration) | ✅ Built — cohort partitions, `validate_table` as asset checks, file sensor |
| Gold corpus contract (v1.1.0) | ✅ Versioned + test-gated (schema/JSON-Schema/docs can't drift) |
| Fabric tier (`fabric/`) — Spark-native, notebooks 00–10 | ✅ **Green end-to-end on F4 against a 100-patient sample**; full 1,280-bundle re-run pending |
| Fabric Data Factory pipeline + Power BI Direct Lake | 🚧 In progress (demo deliverables) |
| Ollama note/dialogue generation → `scribe-iq` corpus loop | 🗺️ Roadmap (not built) — Gold is the input; the generation pipeline is the next-gen corpus path |
| ECG waveform processing · Databricks/AWS tiers | 🗺️ Roadmap (scoped, not built) |

The Fabric tier is **validated end-to-end on real Fabric infrastructure (F4 capacity) at sample scale** — the remaining work there is a
full-scale re-run, not a design question. Pair it with the LocalLite full-run numbers in
[Benchmarks](BENCHMARKS.md).

## If you only read one thing per concern

| Concern | Read |
|---|---|
| Data modeling / data-as-a-product | [Corpus Contract](CORPUS_CONTRACT.md) |
| Engine portability / architecture judgment | [ADR-022](adr/022-platform-independent-implementations.md) + [Engine Parity](platforms/parity.md) |
| Healthcare-data judgment & limitations | [Responsible Data](responsible-data.md) |
| Interesting problems solved | [Design Notes](design-notes.md) |
| Operations & reproducibility | [Runbook](RUNBOOK.md) + [Benchmarks](BENCHMARKS.md) |
| The downstream story | [Downstream & Portfolio](portfolio.md) |
