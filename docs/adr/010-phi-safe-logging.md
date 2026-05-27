# ADR-010: PHI-safe logging via redaction

**Date:** 2026-05-27
**Status:** Accepted
**Deciders:** Sandeep Jayaprakash

## Context

The ingest and pipeline layers log per-file warnings when a bundle cannot be read
(e.g. `"Skipping unreadable bundle: <filename>"`). Synthea Coherent filenames embed a
synthetic patient name and the patient UUID — `Abe604_Frami345_<uuid>.json`. On
synthetic data this is harmless, but this is a production-pattern lakehouse: the same
code path ingesting a real EHR export would emit patient names / MRNs / UUIDs into log
sinks, violating the project security rule (CLAUDE.md, healthcare-data skill:
*"never log patient_id, encounter_id, or any PHI"*) and faking compliance rather than
modeling it honestly.

Pure transforms already avoid logging identifiers (the parser logs counts only). The
gap was specifically the filename-bearing warnings in `pipeline.py` and `local_lite.py`.

## Decision

Route any identifier-bearing value through `local.redaction.redact()` before logging it.
`redact()` returns a stable, non-reversible reference — `"ref:<10-hex>"` from a truncated
SHA-256 digest — so the same file is traceable across log lines without exposing who it
belongs to. Applied to all "skipping unreadable bundle" warnings. The FHIR parser's
per-bundle DEBUG summary logs extracted **counts** only, never identifiers, and its
decode-failure warnings carry no IDs.

Logging policy, codified: log messages must never contain `patient_id`, `encounter_id`,
note text, raw bundle filenames, or any other identifier/PHI. Identifier-bearing values
are redacted; everything else (counts, table names, cohort labels, metrics) logs freely.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|--------|------|------|-------------|
| Log raw filenames | Simplest, directly debuggable | Leaks name + UUID; breaks PHI rule on real data | Fakes compliance — exactly what the spec says not to do |
| Drop the filename entirely | No leak | Can't tell which file failed | Loses operational traceability |
| Redacted `ref:<hash>` (chosen) | No leak, still correlatable across lines | One indirection to map ref → file | Best balance; production-correct |
| Structured logging filter/redactor | Centralized enforcement | Heavier; overkill at current scope | Revisit if log volume/sinks grow |

## Consequences

**Positive:**
- Logs are safe to ship to any sink even when the pipeline runs on real PHI.
- Failures remain traceable: the same file yields the same `ref:` across the run.
- Demonstrates PHI-conscious engineering — a portfolio signal, not a hidden gotcha.

**Negative:**
- Operators must map a `ref:` back to a file out-of-band (re-hash filenames to match).

**Neutral:**
- Synthetic data means there is no real PHI today; the redaction is a production seam
  that is correct in advance rather than retrofitted.

## Implementation notes
- `local/redaction.py` — `redact()` helper; `tests/test_redaction.py` (stability,
  non-leakage, distinctness, empty input).
- Applied in `local/pipeline.py` and `local/platform/local_lite.py` (read + iter paths).
- `local/transforms/fhir_parser.py` — module logging-policy note + counts-only DEBUG line.
- Related: ADR-007 (genomic data_limitation), the broader honest-limitations posture.
