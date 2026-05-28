# Fabric Deployment

How the Fabric tier is deployed and how the runtime gets `core`.

## Overview

The Fabric tier is two halves moving in lockstep:

1. **Code (notebooks)** — synced from `fabric/notebooks/` into the Fabric workspace via **Fabric Git Integration** (workspace ↔ git folder).
2. **Library (core wheel)** — built from `core/` on every push, uploaded into a **Fabric Environment** that the workspace's notebooks attach to.

Notebooks `import core.*`; the `core` package comes from the Environment, not from the git-synced files. This is the same library-vs-deployment split documented in [ADR-018](../../docs/adr/018-ci-cd-monorepo.md).

```
┌──────────────────────────────┐
│        GitHub (main)         │
│                              │
│  core/  ───┐                 │
│            │ build wheel     │
│  fabric/   │                 │
│   ├─ notebooks/  ──┐         │
│   └─ deploy/       │         │
└────────────┬───────┼─────────┘
             │       │
             ▼       ▼
   ┌─────────────────────────┐
   │  Fabric Environment     │
   │  (scribe-iq-lakehouse)  │
   │  └─ core-X.Y.Z.whl      │  ◄── uploaded by upload_wheel.py
   └────────────┬────────────┘
                │ attached to
                ▼
   ┌─────────────────────────┐
   │  Fabric Workspace       │
   │  └─ notebooks/          │  ◄── synced by Git Integration
   └─────────────────────────┘
```

## Fabric Git Integration

Configure the workspace to sync from `/fabric/notebooks/` on `main`:

1. Workspace settings → Git integration → Connect.
2. Repository: `sandeep-jay/scribe-iq-lakehouse`
3. Branch: `main`
4. Git folder: `/fabric/notebooks`
5. Sync direction: Bidirectional.

The workspace then sees only notebooks. `core/` is invisible to the workspace and arrives via the Environment instead.

## Service Principal (one-time setup)

A Service Principal is required for CI to talk to the Fabric REST API. Stored as GitHub Environment `fabric-prod`:

- `FABRIC_TENANT_ID`
- `FABRIC_CLIENT_ID`
- `FABRIC_CLIENT_SECRET`
- `FABRIC_WORKSPACE_ID`
- `FABRIC_LAKEHOUSE_ID`
- `FABRIC_ENVIRONMENT_ID`

Grant the Service Principal **Contributor** on the workspace.

## CI flow

`.github/workflows/fabric-deploy.yml` runs on every push to `main` touching `core/**` or `fabric/**`:

1. Checkout repo, install deps.
2. Run `pytest fabric/tests/`.
3. Build `core/` wheel.
4. Run `python fabric/deploy/upload_wheel.py --wheel core/dist/*.whl --environment-id $FABRIC_ENVIRONMENT_ID`.
5. Run `fabric-cicd --config fabric/deploy/fabric_cicd_config.yml`.
6. Trigger a smoke run of `fabric/notebooks/05_silver_soap_notes.ipynb`.

## Local dev

For local notebook development without deploying:

```bash
LAKEHOUSE_PLATFORM=local_lite python -m core.surfaces.cli.pipeline --with-gold
```

The `LAKEHOUSE_PLATFORM` env var dispatches the factory to `LocalLitePlatform` (Polars + DuckDB + delta-rs) — same transforms, no Fabric account needed. Notebooks can be edited in VS Code with the Jupyter extension and pushed via Git Integration when ready.

## Status

This document is a stub created during the multi-platform reorg (Session 5 prep). Real workspace IDs, Service Principal setup, and a verified end-to-end run land in Session 5 alongside the FabricPlatform implementation.
