# ADR-003: Polars + DuckDB for local lite tier

**Date:** 2026-05-27
**Status:** Accepted
**Deciders:** Sandeep Jayaprakash

## Context

After the Fabric trial expires, the repo needs a runnable local demo for portfolio
reviewers who don't have a cloud account. The options are: PySpark local mode (requires
JVM), Databricks Community Edition (requires account), or a pure Python stack.
Portfolio reviewers need to clone and run in under 5 minutes with no account setup.

## Decision

Use Polars + DuckDB + delta-rs as the local lite tier. No JVM, no cloud account.
`pip install -r requirements-lite.txt` (8 packages) and `python local/pipeline_lite.py`.
Polars handles DataFrame operations, DuckDB handles SQL analytics, delta-rs handles
Delta Lake read/write in pure Python. The same Arrow interchange format (ADR-004)
means transforms run identically — only the platform layer changes.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|--------|------|------|-------------|
| PySpark local mode | Identical to Fabric Spark | JVM required (~500MB), slow startup | Barriers too high for reviewer quick start |
| Pandas + CSV | Maximum simplicity | No Delta Lake, loses medallion story | Delta format is the portfolio signal |
| Polars + delta-rs | No JVM, fast, Delta native | Different API from Spark | Abstraction layer hides the difference |

## Consequences

**Positive:**
- Reviewer quick start: clone → pip install → run (3 minutes on any laptop)
- Same Delta schema as Fabric output — architecture story holds
- DuckDB allows SQL analytics on Delta tables without Spark

**Negative:**
- Streaming simulation is watchdog-based, not Auto Loader (different implementation)
- Polars and Spark have different APIs — must be abstracted behind the platform layer

**Neutral:**
- requirements-lite.txt is a separate file from requirements.txt (Spark + Fabric)

## Implementation notes
- `local/platform/local_lite.py` — LocalLitePlatform implementation
- `local/pipeline_lite.py` — end-to-end orchestration, no Spark
- `requirements-lite.txt` — polars, duckdb, deltalake, fhir.resources, pydicom, requests, pydantic, rich
- `local/ingest/streaming_sim.py` — watchdog-based Auto Loader simulation
