# ADR-001: Fabric-first development approach

**Date:** 2026-05-27
**Status:** Accepted
**Deciders:** Sandeep Jayaprakash

## Context

Active job hunt targeting healthcare AI and data engineering roles. Microsoft Fabric
trial is active with ~15-20 days remaining. Fabric provides the full enterprise data
platform story (Delta Lake, Spark, Auto Loader, CDC, Power BI) in a single environment.
The alternative was building locally first using Polars/DuckDB and migrating to Fabric
later, but this risks trial expiring before Fabric screenshots are captured.

## Decision

Build the medallion pipeline in Fabric first (Bronze + Silver + Gold notebooks).
Implement the local Polars lite tier in parallel as bandwidth allows, but treat it
as non-blocking. Capture Fabric screenshots throughout — evidence of a running system
on the enterprise platform is the portfolio priority.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|--------|------|------|-------------|
| Local Polars first | No trial pressure, faster iteration | No Fabric screenshots if trial expires | Trial window is finite |
| Both simultaneously | Full coverage | Context switching overhead | Trial makes Fabric the forcing function |
| Fabric only | Maximum focus | Platform lock-in for reviewer demo | Local lite needed for post-trial demo |

## Consequences

**Positive:**
- Fabric screenshots and evidence captured before trial expires
- Enterprise platform signals visible to hiring reviewers
- Auto Loader streaming simulation runs on real Spark infrastructure

**Negative:**
- Local lite tier is secondary priority — built after Fabric phase
- Any Fabric-specific bugs discovered late in trial window

**Neutral:**
- Platform abstraction layer (ADR-002) makes this reversible

## Implementation notes
- LAKEHOUSE_PLATFORM=fabric for all Fabric notebook work
- Screenshots checklist: docs/roadmap/scribe-iq-lakehouse-spec.md section 15.2
- Local fallback: LAKEHOUSE_PLATFORM=local_lite after trial expires
