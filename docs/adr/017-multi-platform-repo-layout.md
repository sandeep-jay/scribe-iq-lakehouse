# ADR-017: Multi-platform repo layout — `core/` + per-platform domains

**Date:** 2026-05-28 (amended 2026-05-29 — see ADR-022)
**Status:** Accepted
**Deciders:** Sandeep Jayaprakash

> **Amendment (2026-05-29):** ADR-022 superseded ADR-002 and ADR-004 — each
> platform tier is now an independent end-to-end implementation, not a thin
> wrapper around a shared transform layer. The repo *layout* this ADR defines
> still stands; only the dependency story changed: `fabric/` no longer imports
> transform / Gold / validation logic from `core/`. The one-way dependency
> rule below now applies narrowly (utilities like `core.redaction` if needed)
> rather than to the transform contract. See ADR-022.

## Context

ADR-002 established the `LakehousePlatform` abstraction so transforms run unchanged on any backend. That solved the *logic* portability problem but left the *file organisation* problem unsolved: the original `local/` directory was holding two roles at once.

1. **Platform-agnostic kernel** — transforms, gold logic, validation, ingest, redaction, the platform interface itself. Shared by every backend.
2. **One specific platform implementation** — `LocalLitePlatform` (Polars + DuckDB + delta-rs), the post-trial fallback.

Adding `fabric.py`, `databricks.py`, `aws.py` into the same `local/platform/` directory would conflate the two roles further, and notebooks/scripts/tests for each platform would scatter across the repo. Once the Fabric trial expires and Databricks/AWS reference implementations follow, the conflation becomes load-bearing.

This ADR was written immediately before Session 5's Fabric implementation — the cheapest moment to fix layout. Full planning in [docs/roadmap/multi-platform-reorg.md](../roadmap/multi-platform-reorg.md).

## Decision

Two-domain monorepo:

```
core/        Platform-agnostic kernel + LocalLite implementation + Dagster + CLI + tests + scripts + docs
fabric/      Fabric-specific: platform impl + notebooks + environments + deploy + tests + docs
databricks/  (future) Same shape as fabric/
aws/         (future) Same shape as fabric/
```

**One-way dependency rule:** `fabric/` (and every future sibling) imports from `core/`. `core/` never imports from `fabric/`, `databricks/`, or `aws/`. Enforced by:

- The `.claude/rules/transforms.md` lint guidance.
- A `core-pr-tests.yml` CI step that greps for forbidden imports.
- The wheel-as-library deployment model (ADR-018) — Fabric Environment doesn't even have `fabric/` on its Python path, so a stray import would fail loudly.

**LocalLite stays inside `core/`** because it's the default fallback and the only impl that runs without a cloud account. If symmetry becomes important later (when Databricks lands), `core/platform/local_lite.py` can be promoted to a top-level `local/` sibling without disturbing anything else.

**Dagster lives at `core/orchestration/dagster/`** — local-only per ADR-015. Future Fabric Data Factory pipeline specs go under `fabric/data_factory/`.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| Keep `local/` as-is, add `local/platform/fabric.py` | Minimal disruption | Conflates agnostic kernel with one platform impl; only gets worse with each new platform | Compounds debt |
| Light touch: `local/` keeps the agnostic kernel, move `local_lite.py` to a new top-level `platforms/local_lite/` | Less import churn | `local/` name remains confusing once Fabric/Databricks/AWS land | Doesn't fix the core naming issue |
| Polyrepo split (`-core`, `-fabric`, `-databricks`, `-aws`) | Cleanest deploy isolation, independent release cadence | Heavy overhead for cross-cutting changes; four repos for a solo portfolio | Wrong tradeoff for project size |
| Three top-level dirs: `core/` (agnostic only) + `local/` (LocalLite + Dagster + CLI) + `fabric/` | Symmetric treatment of every platform | User chose "two folders, one for fabric and one for local(core)" — bundle local with core | Honors user's explicit preference |
| **Current approach** | Self-contained domains, one-way dependency, scales to Databricks/AWS without re-litigating | One-time migration cost for imports + tests | Best long-term shape |

## Consequences

**Positive:**
- A reviewer reads one folder and sees a platform's complete story (impl + notebooks + tests + docs + deploy).
- Adding Databricks or AWS does not touch `core/` files.
- Notebook imports are stable across platforms: every notebook writes `from core.transforms.X import Y` regardless of where it runs.
- `core/` ships as a wheel (ADR-018) — every platform consumes a versioned library, not source files.

**Negative:**
- One-time migration churn: every import statement in the repo was rewritten (`from local.X` → `from core.X`), every test moved, `pyproject.toml` updated, two ADRs and CLAUDE.md kept aligned.
- Future symmetry question: if Databricks demands its own `local-symmetric` treatment, `core/platform/local_lite.py` may need promoting to a sibling. Acceptable — that change is mechanical and won't touch transforms.

## Related

- ADR-001 (Fabric-first)
- ADR-002 (platform abstraction — what this ADR operationalizes structurally)
- ADR-015 (Dagster local orchestration)
- ADR-018 (CI/CD monorepo — the deployment half of this layout)
- docs/roadmap/multi-platform-reorg.md (full planning doc)
