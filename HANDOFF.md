# HANDOFF — Session 0
**Date:** 2026-05-27
**Repo:** scribe-iq-lakehouse
**Branch:** main

---

## Session summary
This is the bootstrap session. No code exists yet — only planning documentation.
Claude Code infrastructure has been set up: CLAUDE.md, session protocol, skills,
commands, hooks, security tooling, and 7 ADRs are now in place.
The repo is ready for Session 1: implement repo scaffold + FHIR parser foundation.

---

## Current state

**Working:**
- Claude Code configuration complete (.claude/skills, commands, rules, hooks)
- Global ~/.claude/CLAUDE.md and security deny rules in place
- ~/claude-os/ skill library created (7 skills, templates, init.sh)
- docs/adr/ created with ADRs 001-007
- Pre-commit security toolchain configured

**In progress:**
- Nothing — clean slate, ready for Session 1

**Blocked:**
- Fabric workspace creation (manual step — do first on Friday night)
- S3 shortcut setup in Fabric UI (manual — requires workspace to exist)

---

## Test status
```
No tests yet — repo scaffold not yet created.
```

---

## Next session — start here

**First task:** Init repo structure per spec section 4, then implement local/transforms/fhir_parser.py
**Read first:** docs/roadmap/scribe-iq-lakehouse-spec.md sections 1-5 (architecture + component specs)
**Decision needed:** None — spec is locked, begin execution

---

## Open decisions

| Decision | Options | Recommendation | Status |
|----------|---------|----------------|--------|
| Fabric Bronze ingestion | S3 Shortcut vs Copy Pipeline | S3 Shortcut (zero-copy, preferred) | Deferred to Session 4 |
| Dev cohort size | 5 / 20 / 50 patients | 20 patients for Fabric dev runs | Open |

---

## Key state
```
LAKEHOUSE_PLATFORM=local_lite (default until Fabric workspace created)
Fabric workspace: NOT YET CREATED
S3 shortcut: NOT YET CONFIGURED
Silver tables written: none
Gold tables written: none
Fabric trial: ~15 days remaining as of 2026-05-27
M5 Max: arriving ~June 2, 2026
```

---

## Session 1 goals (from spec section 9)
1. Init repo structure (pyproject.toml, requirements.txt, all directories)
2. Download 5-patient sample fixture from S3 (no-sign-request)
3. local/transforms/fhir_parser.py — FHIRBundleParser class, all extract_* methods
4. tests/fixtures/sample_bundle.json
5. tests/test_fhir_parser.py — full coverage
6. local/platform/base.py — abstract interface
7. local/platform/factory.py — env var router
8. End: HANDOFF.md + CHANGELOG.md + commit

## Files changed this session
- CLAUDE.md — created (project Claude Code configuration)
- HANDOFF.md — created (this file, session bootstrap)
- CHANGELOG.md — created
- .gitignore — created
- docs/adr/README.md + 001-007 — created
- .claude/settings.json — updated with hooks + security rules
- .claude/skills/ — 3 skill files installed
- .claude/commands/ — 4 slash commands created
- .claude/rules/ — 2 path-scoped rule files created
- .claude/hooks/scan-secrets.sh — created
- .pre-commit-config.yaml — created
- .semgrep/healthcare.yml — created

## ADRs written this session
- ADR-001: Fabric-first development approach
- ADR-002: Platform abstraction layer
- ADR-003: Polars + DuckDB for local lite tier
- ADR-004: Arrow as transform interchange format
- ADR-005: FHIR Binary Base64 decode for SOAP notes
- ADR-006: DICOM stop_before_pixels extraction
- ADR-007: Genomic data_limitation as first-class column
