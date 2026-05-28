# ADR-018: Monorepo CI/CD — `core/` as a wheel, per-platform deploy workflows

**Date:** 2026-05-28
**Status:** Accepted
**Deciders:** Sandeep Jayaprakash

## Context

ADR-017 settled the repo's directory layout (`core/` + `fabric/` + future siblings). What it left open was how each platform actually *deploys* from this layout, given that:

- **Fabric** deploys via `fabric-cicd` + Fabric REST API; notebooks sync via Fabric Git Integration (workspace ↔ git folder).
- **Databricks** (future) deploys via Asset Bundles (`databricks.yml`) and the `databricks bundle deploy` CLI.
- **AWS** (future) deploys via CDK/Terraform with OIDC-assumed roles.

Each platform has its own secrets, release cadence, packaging convention, and operator. A naive monorepo CI would either run every platform's workflow on every push (waste + cross-pollination risk) or hand-pick which to run (fragile).

The shared `core/` logic is consumed by all of them, so it must arrive on each platform in a form that platform can install — not as raw source files.

## Decision

**`core/` is a library; each platform directory is a deployment manifest.**

1. **`core/` builds to a versioned wheel** (`scribe_iq_lakehouse_core-X.Y.Z-py3-none-any.whl`) on every push to `main` and on every `v*` tag. The wheel is uploaded as a GitHub Actions artifact and (on tagged releases) attached to a GitHub Release.

2. **Each platform tier has its own deploy workflow**, triggered by path-filtered pushes to `main`:
   - `.github/workflows/core-pr-tests.yml` — runs on every PR; lints + tests + enforces "no cross-domain imports in `core/`".
   - `.github/workflows/core-build.yml` — builds and publishes the wheel.
   - `.github/workflows/fabric-deploy.yml` — triggers on `core/**` or `fabric/**` changes; runs fabric contract tests, builds the wheel, uploads to a Fabric Environment via REST, and runs `fabric-cicd` against `fabric/notebooks/`.
   - `.github/workflows/databricks-deploy.yml` and `aws-deploy.yml` — disabled templates until those tiers exist.

3. **Per-platform secrets isolation via GitHub Environments.** Each platform has its own environment (`fabric-prod`, `databricks-prod`, `aws-prod`). A workflow targeting `fabric-prod` cannot read Databricks secrets even if compromised. Production environments are restricted to `main`.

4. **Fabric Git Integration targets `/fabric/notebooks/`** (subfolder, not repo root). The workspace sees only notebooks; `core` arrives via the Fabric Environment, not via Git. This is the clean separation that makes the monorepo viable for Fabric.

## Alternatives considered

| Option | Pros | Cons | Why rejected |
|---|---|---|---|
| Single deploy workflow for all platforms | Simpler to write | Couples platforms; one platform's failure or secret rotation blocks all others; cross-platform secret leakage | Brittle |
| Polyrepo (`-core` published to PyPI, separate platform repos) | Maximum isolation | Heavy overhead for solo dev; cross-cutting changes touch four repos | See ADR-017 |
| Editable install in Fabric workspace (`pip install -e ./core`) at top of every notebook | No wheel-publishing step | Fragile, slow, depends on Git Integration syncing `core/` into the workspace (which it shouldn't); breaks if `core/` and notebooks drift | Rejected in the planning Q&A |
| **Wheel + per-platform workflows + GitHub Environments** | Mirrors production lakehouse patterns; isolates secrets and release cadence; scales to Databricks/AWS by adding workflows | Two workflows must coordinate (wheel build + Fabric deploy); upload step needs Fabric REST helper | Right tradeoff |

## Consequences

**Positive:**
- A reviewer can read `.github/workflows/` and immediately see which platforms exist and how each one ships.
- Compromise of one platform's secrets does not expose others.
- Adding Databricks = one new workflow file + one `databricks.yml` in `databricks/`. No change to existing workflows or to `core/`.
- The wheel is a stable, versioned interface — Fabric notebooks can pin to a specific `core` version if a breaking change lands on `main`.

**Negative:**
- Two coordinated artefacts per Fabric deploy (wheel upload to Environment + notebook sync via Git Integration). Documented in `fabric/docs/DEPLOYMENT.md`.
- Fabric Environment update is asynchronous — `upload_wheel.py` must poll or wait for environment publication before notebooks see the new version.
- Solo-developer overhead: maintaining one workflow per platform. Mitigated by the disabled-template pattern (`databricks-deploy.yml.disabled`, `aws-deploy.yml.disabled`).

## Related

- ADR-001 (Fabric-first)
- ADR-002 (platform abstraction)
- ADR-017 (repo layout — the structural half of this decision)
- fabric/docs/DEPLOYMENT.md (operational runbook)
- docs/roadmap/multi-platform-reorg.md (full planning doc)
