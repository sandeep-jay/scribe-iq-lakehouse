# Multi-platform repo reorganization

> Planning doc for the `local/` → `core/` + `fabric/` split, with CI/CD model
> for Fabric now and Databricks/AWS later. Implemented in Session 5 prep.

## Context

Before implementing the Fabric tier (Session 5), the repo needs a layout that scales cleanly to Fabric now and Databricks/AWS later — without each new platform "muddying" the cross-platform core, and with a deployment model that respects each platform's native CI/CD tooling.

Today's pain point is **naming + deployment readiness**, not architecture. ADR-002's platform-abstraction design is already sound (pure transforms in `local/transforms/`, `LakehousePlatform` interface in `local/platform/base.py`, factory in `local/platform/factory.py`). But two things compound debt as new platforms arrive:

1. The directory `local/` does two jobs at once: it holds the **platform-agnostic core** that every platform shares (transforms, gold logic, validation, ingest, redaction, platform interface), AND it holds **one specific platform implementation** (`LocalLitePlatform`).
2. There is **no deployment model** documented. Fabric will need `fabric-cicd` to push notebooks; Databricks (future) needs Asset Bundles; AWS (future) needs CDK/Terraform. Each platform has its own secrets, release cadence, and packaging requirement — but they all share `core/` logic.

The companion repo `fabric-lakehouse-hls-readmission` (Databricks→Fabric migration, CSV-first) **stays separate** per MASTER_PLAN. Different portfolio narrative, different ingestion path; cross-link via README; no code dependency either direction.

**Decisions (locked in):**

- **Monorepo + `core/` published as a versioned wheel.** Each platform tier is a deployment manifest that consumes the wheel — mirrors real production library-vs-deployment patterns.
- **Fabric Git Integration targets `/fabric/notebooks/`** (subfolder, not repo root). Fabric workspace sees only notebooks; `core/` is delivered as a wheel to a Fabric Environment.
- **One folder per platform**, each fully self-contained (impl + notebooks + tests + docs + scripts + CI manifests).

Intended outcome: two domains today (`core/`, `fabric/`), each self-contained, with `.github/workflows/` orchestrating per-platform deployment via path filters. Future `databricks/` and `aws/` slot in as siblings, each owning its own deploy workflow.

---

## Target layout

```
scribe-iq-lakehouse/
├── README.md                          # Top-level: links to both domains + readmission repo
├── CHANGELOG.md
├── HANDOFF.md
├── CLAUDE.md
├── pyproject.toml                     # Declares `core` as a package; testpaths for both domains
│
├── docs/
│   ├── adr/                           # Repo-wide ADRs
│   └── roadmap/                       # MASTER_PLAN, spec, this doc
│
├── .github/
│   └── workflows/
│       ├── core-build.yml             # Tests + builds wheel on every push to main
│       ├── core-pr-tests.yml          # Runs core tests on every PR
│       ├── fabric-deploy.yml          # Triggered by changes in core/** or fabric/**
│       ├── databricks-deploy.yml      # Stub (commented-out) — added when databricks/ exists
│       └── aws-deploy.yml             # Stub (commented-out) — added when aws/ exists
│
├── core/                              # ┌─ Platform-agnostic + local execution. Builds to a wheel.
│   ├── pyproject.toml                 # │ (optional: separate package, or single root pyproject)
│   ├── platform/
│   │   ├── base.py                    # │ LakehousePlatform interface
│   │   ├── factory.py                 # │ get_platform() — reads LAKEHOUSE_PLATFORM
│   │   └── local_lite.py              # │ LocalLitePlatform (Polars + DuckDB)
│   ├── transforms/                    # │ Pure transforms — shared by ALL platforms
│   │   ├── registry.py
│   │   ├── fhir_parser.py
│   │   ├── silver_*.py
│   │   └── schema_utils.py
│   ├── gold/
│   ├── ingest/
│   ├── validation/
│   ├── redaction.py
│   ├── preview.py
│   ├── orchestration/
│   │   └── dagster/                   # │ Local-only per ADR-015
│   ├── surfaces/
│   │   └── cli/pipeline.py
│   ├── tests/
│   ├── scripts/
│   └── docs/
│
├── fabric/                            # ┌─ Fabric-specific everything
│   ├── platform.py                    # │ FabricPlatform(LakehousePlatform)
│   ├── notebooks/                     # │ Fabric Git Integration syncs THIS folder
│   │   ├── 00_setup.ipynb
│   │   ├── 01_bronze_ingest.ipynb
│   │   ├── 05_silver_soap_notes.ipynb (demo centerpiece)
│   │   └── 09_gold_encounter_summary.ipynb
│   ├── environments/
│   │   └── lakehouse_env.yml          # │ Fabric Environment spec (Python deps + wheel ref)
│   ├── data_factory/                  # │ Future: Fabric Data Factory pipeline JSON
│   ├── deploy/
│   │   ├── fabric_cicd_config.yml     # │ fabric-cicd configuration
│   │   └── upload_wheel.py            # │ Helper: upload core wheel to Fabric Environment via REST
│   ├── tests/
│   │   ├── test_fabric_platform.py    # │ Contract tests vs LakehousePlatform interface
│   │   └── test_notebooks_parse.py    # │ Notebooks compile cleanly
│   ├── docs/
│   │   ├── DEPLOYMENT.md              # │ Workspace setup, Git Integration target, secrets
│   │   ├── SCREENSHOTS.md             # │ Capture checklist
│   │   └── screenshots/
│   └── scripts/
│       └── capture_lineage.py
│
├── data/                              # Runtime, gitignored
├── dagster_home/                      # Runtime, gitignored
└── schemas/                           # Shared JSON schemas
```

**Future siblings of `fabric/`** (not created now, but layout is decided):

```
databricks/
  platform.py
  notebooks/
  databricks.yml                  # Asset Bundle definition
  cluster_configs/
  tests/  docs/  scripts/
aws/
  platform.py
  glue_jobs/
  stepfn/
  cdk/                            # IaC
  tests/  docs/  scripts/
```

---

## CI/CD design

### Build & publish `core/` as a wheel

`.github/workflows/core-build.yml`:
- **Trigger:** push to `main` touching `core/**`, OR tag `v*`.
- **Steps:** install deps → `pytest core/tests/` → `python -m build core/` → upload wheel as artifact → on tagged release, publish to GitHub Releases (and optionally a private PyPI / GitHub Packages registry).
- **Output:** `scribe_iq_lakehouse_core-X.Y.Z-py3-none-any.whl` attached to the release.

Versioning: semver via `core/pyproject.toml`, bumped on release tags (`v0.5.0` style).

### PR safety: `core/` tests on every PR

`.github/workflows/core-pr-tests.yml`:
- **Trigger:** every PR.
- **Steps:** `pytest core/tests/ -v` + ruff + mypy. Fast feedback loop independent of platform deploys.

### Fabric deploy

`.github/workflows/fabric-deploy.yml`:
- **Trigger:** push to `main` with changes in `core/**` OR `fabric/**`.
- **Secrets** (GitHub Environment `fabric-prod`): `FABRIC_TENANT_ID`, `FABRIC_CLIENT_ID`, `FABRIC_CLIENT_SECRET` (Service Principal), `FABRIC_WORKSPACE_ID`, `FABRIC_LAKEHOUSE_ID`, `FABRIC_ENVIRONMENT_ID`.
- **Steps:**
  1. Checkout repo.
  2. Build `core/` wheel (re-uses `core-build.yml` as a reusable workflow, or rebuilds locally).
  3. Run `fabric/tests/` (contract tests + notebook-parses-cleanly).
  4. Upload wheel to Fabric Environment via `fabric/deploy/upload_wheel.py` (uses Fabric REST API + Service Principal token).
  5. Run `fabric-cicd` against `fabric/notebooks/` to push notebooks into the Fabric workspace.
  6. Trigger Fabric pipeline run (smoke test).

**Important:** Fabric Git Integration is configured workspace-side to sync from `/fabric/notebooks/` on a branch (typically `main`). The `fabric-deploy.yml` workflow is the **wheel-and-environment** half of deployment; the **notebook sync** half is handled by Fabric Git Integration directly. The workflow ensures the Environment has the right wheel before notebooks try to import `core`.

### Future: Databricks deploy

`.github/workflows/databricks-deploy.yml` (added when `databricks/` exists):
- **Trigger:** push to `main` with `core/**` or `databricks/**` changes.
- **Secrets:** `DATABRICKS_HOST`, `DATABRICKS_TOKEN`.
- **Steps:** build wheel → `databricks bundle validate` → `databricks bundle deploy --target prod` (reads `databricks/databricks.yml` which references the wheel).

### Future: AWS deploy

`.github/workflows/aws-deploy.yml` (added when `aws/` exists):
- **Trigger:** push to `main` with `core/**` or `aws/**` changes.
- **Auth:** OIDC role assumption to AWS (no long-lived secrets).
- **Steps:** build wheel → `cdk synth` → `cdk deploy` (wheel bundled into Lambda layer / Glue job ZIP).

### Per-platform secrets isolation

Each platform's secrets live in a separate **GitHub Environment** (`fabric-prod`, `databricks-prod`, `aws-prod`) with environment protection rules (manual approval for production, restricted to `main`). Cross-platform leakage is prevented by GitHub's environment-scoping — `fabric-deploy.yml` cannot read Databricks secrets even if compromised.

---

## Why this shape

- **`core/` is a library, not a deployable.** That single decision is what makes the monorepo viable — every platform consumes the wheel rather than reaching into source files. Same pattern used by every real-world lakehouse platform team.
- **One-way dependency.** `fabric/` imports from `core` (via the wheel at deploy time, via source at dev time). `core/` never imports from `fabric/`. Enforced by lint rule + the wheel itself (Fabric environment doesn't have `fabric/` on its Python path).
- **Each platform dir is "everything related"** — impl + notebooks + tests + docs + scripts + deploy manifests. A reviewer reads one folder and sees that platform's full deployment story.
- **Fabric Git Integration sees `/fabric/notebooks/` only.** Notebooks sync natively; everything else is delivered via the wheel + Environment. Clean separation between "what Fabric renders" and "what Fabric uses as a library."
- **Dagster lives in `core/orchestration/dagster/`** — local-only per ADR-015. Fabric Data Factory definitions go to `fabric/data_factory/` when needed.
- **`core/` bundles the LocalLite impl** pragmatically — LocalLite is the default fallback and the only impl that runs without a cloud account. If symmetry matters later, `core/platform/local_lite.py` promotes to a `local/` sibling without disturbing anything else.

---

## Migration steps

Order matters — each step keeps the test suite green.

1. **Create directory skeleton** for `core/` and `fabric/` (empty dirs + `__init__.py`).

2. **`git mv` existing files** (preserves history):
   - `local/platform/*`        → `core/platform/*`
   - `local/transforms/*`      → `core/transforms/*`
   - `local/gold/*`            → `core/gold/*`
   - `local/ingest/*`          → `core/ingest/*`
   - `local/validation/*`      → `core/validation/*`
   - `local/redaction.py`      → `core/redaction.py`
   - `local/preview.py`        → `core/preview.py`
   - `local/pipeline.py`       → `core/surfaces/cli/pipeline.py`
   - `orchestration/*`         → `core/orchestration/dagster/*`
   - `tests/*`                 → `core/tests/*`
   - `scripts/gen_*.py`        → `core/scripts/`
   - `scripts/demo_*.py`       → `core/scripts/`

3. **Bulk-update imports**:
   - `from local.` → `from core.`
   - `import local.` → `import core.`
   - Factory strings: `"local.platform.local_lite.LocalLitePlatform"` → `"core.platform.local_lite.LocalLitePlatform"`; `"local.platform.fabric.FabricPlatform"` → `"fabric.platform.FabricPlatform"`. Future-correct the databricks/aws/gcp entries.

4. **Update `pyproject.toml`**:
   - Package discovery: `[tool.setuptools.packages.find]` includes both `core` and `fabric` (or use namespace-package layout if `core/pyproject.toml` is a separate file).
   - `[tool.pytest.ini_options] testpaths = ["core/tests", "fabric/tests"]`
   - Build backend config for wheel: `[build-system]` + `[project] name = "scribe-iq-lakehouse-core"` (so the published wheel name is unambiguous).
   - CLI entry point updated to `core.surfaces.cli.pipeline:main`.

5. **Update `CLAUDE.md` and `.claude/rules/*`**:
   - "Key files" paths updated to `core/...` and `fabric/...`.
   - `.claude/rules/transforms.md` — `core/transforms/` references; add rule: "core never imports from fabric/databricks/aws".
   - `.claude/rules/notebooks.md` — Fabric notebook imports become `from core.transforms.{module} import ...`.

6. **Create `fabric/` placeholders** (ready for Session 5):
   - `fabric/platform.py` — stub `FabricPlatform(LakehousePlatform)` with `NotImplementedError` on each method, matching the interface.
   - `fabric/notebooks/` empty.
   - `fabric/environments/lakehouse_env.yml` stub.
   - `fabric/deploy/upload_wheel.py` stub.
   - `fabric/deploy/fabric_cicd_config.yml` stub.
   - `fabric/tests/test_fabric_platform.py` — contract scaffold.
   - `fabric/docs/DEPLOYMENT.md` stub describing the Service Principal setup, Git Integration target, wheel upload flow.

7. **Create `.github/workflows/`**:
   - `core-pr-tests.yml` (runs immediately).
   - `core-build.yml` (runs on tags + main).
   - `fabric-deploy.yml` (skeleton — full implementation happens in Session 5 alongside `FabricPlatform`).
   - `databricks-deploy.yml.disabled` and `aws-deploy.yml.disabled` as commented-out templates (so the pattern is visible to reviewers but doesn't run).

8. **Add ADRs**:
   - `docs/adr/017-multi-platform-repo-layout.md` — documents the `core/` + `fabric/` (+ future `databricks/`, `aws/`) layout, one-way dependency rule, and core-as-wheel decision. References ADR-002.
   - `docs/adr/018-ci-cd-monorepo.md` — documents the monorepo + path-filtered workflows + per-platform GitHub Environments model. References ADR-001 (Fabric-first) and ADR-017.

9. **Update README.md**:
   - Top-of-file architecture diagram showing `core/` (wheel) → consumed by `fabric/`, future `databricks/`, future `aws/`.
   - "Deployment" section linking to each platform's `docs/DEPLOYMENT.md`.
   - "See also" → `fabric-lakehouse-hls-readmission` with one-line description.

10. **Update HANDOFF.md + CHANGELOG.md** per session protocol.

---

## Critical files to modify

| Path | Change |
|---|---|
| `local/platform/factory.py` → `core/platform/factory.py` | Update import strings; `fabric.platform.FabricPlatform` lives outside core |
| `orchestration/definitions.py` → `core/orchestration/dagster/definitions.py` | `from core.transforms...`, `from core.platform.factory...` |
| `orchestration/assets.py` → `core/orchestration/dagster/assets.py` | Same import rewrite |
| `local/pipeline.py` → `core/surfaces/cli/pipeline.py` | Imports + entry point name in `pyproject.toml` |
| `pyproject.toml` | Package discovery, build config, testpaths, entry points, wheel name |
| `CLAUDE.md` | "Key files" section, project structure paragraph |
| `.claude/rules/transforms.md` | Path refs, cross-domain import ban |
| `.claude/rules/notebooks.md` | Notebook import pattern → `from core.transforms...` |
| `README.md` | Architecture diagram + deploy section + readmission cross-link |
| `docs/adr/017-multi-platform-repo-layout.md` | NEW |
| `docs/adr/018-ci-cd-monorepo.md` | NEW |
| `.github/workflows/core-pr-tests.yml` | NEW |
| `.github/workflows/core-build.yml` | NEW |
| `.github/workflows/fabric-deploy.yml` | NEW (skeleton; completed in Session 5) |

---

## Reuse — what NOT to rewrite

- **`LakehousePlatform` interface** (`core/platform/base.py`) — methods unchanged.
- **`get_platform()` factory** — only the import strings change.
- **Pure transforms, validation, gold logic, redaction** — move only; no logic changes.
- **Tests** — move only; imports updated by bulk search-replace.

---

## Verification

After the migration, before committing:

1. **No cross-domain leaks:**
   ```bash
   ! grep -r "from fabric\|import fabric" core/ --include="*.py"
   ! grep -r "from local\." . --include="*.py"   # all should be 'core.' now
   ```

2. **Test suites green:**
   ```bash
   pytest core/tests/ -v
   pytest fabric/tests/ -v   # contract-stub tests only at this stage
   ```

3. **Factory dispatch works:**
   ```bash
   LAKEHOUSE_PLATFORM=local_lite python -c "from core.platform.factory import get_platform; print(type(get_platform()).__name__)"
   # → LocalLitePlatform
   ```

4. **Dagster definitions load:**
   ```bash
   dagster definitions list -m core.orchestration.dagster.definitions
   ```

5. **CLI entry point runs:**
   ```bash
   python -m core.surfaces.cli.pipeline --cohort A --layer silver
   ```

6. **Wheel builds cleanly:**
   ```bash
   python -m build --wheel core/
   ls core/dist/scribe_iq_lakehouse_core-*-py3-none-any.whl
   ```

7. **Wheel installs into a fresh venv and imports correctly:**
   ```bash
   python -m venv /tmp/check && /tmp/check/bin/pip install core/dist/scribe_iq_lakehouse_core-*.whl
   /tmp/check/bin/python -c "from core.platform.factory import get_platform; from core.transforms import silver_patient"
   ```

8. **Generated-docs check passes** (script now lives at `core/scripts/`):
   ```bash
   python core/scripts/gen_data_dictionary.py --check
   ```

9. **Fabric stub raises cleanly** (not implemented yet):
   ```bash
   LAKEHOUSE_PLATFORM=fabric python -c "from core.platform.factory import get_platform; get_platform()"
   # → NotImplementedError with clear message
   ```

10. **GitHub Actions syntax check** (no run, just parse):
    ```bash
    yamllint .github/workflows/
    # or push to a feature branch and confirm Actions UI parses workflows
    ```

11. **Git history preserved** for all moves:
    ```bash
    git log --follow --stat core/transforms/silver_patient.py | head
    # should show history pre-dating the move
    ```

Once verified, Session 5 (Fabric implementation) starts on a clean foundation:
- Implement `fabric/platform.py` against the interface.
- Build notebooks in `fabric/notebooks/` (imported `from core.transforms...`).
- Complete `fabric-deploy.yml` (wheel upload + fabric-cicd).
- Configure Fabric workspace Git Integration → `/fabric/notebooks/`.
- Capture screenshots before trial expires.