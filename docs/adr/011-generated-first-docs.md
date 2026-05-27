# ADR-011: Generated-first documentation

**Date:** 2026-05-27
**Status:** Accepted
**Deciders:** Sandeep Jayaprakash

## Context

The project needs documentation that *evolves with the codebase* rather than rotting.
The spec (§4) lists a target doc set (REVIEWER_GUIDE, ARCHITECTURE, DATA_DICTIONARY,
CORPUS_CONTRACT, STREAMING_DESIGN, PRODUCTION_NOTES, MkDocs). The risk with several of
these — especially the data dictionary and the corpus contract — is that a hand-written
copy of the schemas silently diverges from the code as tables change.

## Decision

Adopt a **generated-first** documentation strategy with three tiers:

- **Living** (hand-maintained, change often): HANDOFF, CHANGELOG, ADRs (append-only),
  ARCHITECTURE (as-built — distinct from the frozen spec).
- **Generated** (derived from code, never hand-edited): DATA_DICTIONARY is rendered from
  `local/transforms/registry.py` schemas + `local/validation/schema_registry.py` rules by
  `scripts/gen_data_dictionary.py`. A test (`tests/test_docs_generated.py`) asserts the
  committed file matches the generator output, so it cannot drift. API reference will use
  mkdocstrings over the existing docstrings (Session 5).
- **Synthesized** (written once at a milestone): REVIEWER_GUIDE, full README,
  PRODUCTION_NOTES, STREAMING_DESIGN — Session 5.

Supporting practices: ADRs are immutable (supersede, never edit); diagrams are
diagrams-as-code (Mermaid, render on GitHub, diff in git); contracts are verified by tests
(the forthcoming CORPUS_CONTRACT gets a schema-conformance test with the Gold layer);
`docs/BENCHMARKS.md` records real run metrics as a regression baseline.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|--------|------|------|-------------|
| Hand-write all docs | Full control, no tooling | Drift is near-certain as schemas change | Defeats "evolves with the project" |
| Generated-first (chosen) | Code-mirroring docs can't drift; verified by tests | A little tooling up front | Best fit; matches the repo's production-pattern ethos |
| All docs in Session 5 | Less work mid-build | Dictionary/contract stale through Sessions 2–4; no living baseline | Loses the value of evolving docs |

## Consequences

**Positive:**
- DATA_DICTIONARY is always correct (test-enforced); changing a schema forces a regen.
- ARCHITECTURE/BENCHMARKS give reviewers an honest, current view at any commit.
- Demonstrates documentation engineering — a portfolio signal.

**Negative:**
- Contributors must run the generator (or CI will flag a stale dictionary).

**Neutral:**
- CORPUS_CONTRACT + its conformance test are deferred to the Gold session, since a
  contract test is only meaningful once `gold.encounter_summary` exists.

## Implementation notes
- `scripts/gen_data_dictionary.py` (`render()` pure + `--check` for CI) → `docs/DATA_DICTIONARY.md`.
- `tests/test_docs_generated.py` — fails if the dictionary is stale or misses a table.
- `docs/ARCHITECTURE.md` (as-built + Mermaid), `docs/BENCHMARKS.md` (real run metrics).
- Decision recorded in HANDOFF and the project memory `doc-strategy-generated-first`.
- Related: ADR-009 (schemas/registry that feed the generator), ADR-004 (Arrow schemas).
