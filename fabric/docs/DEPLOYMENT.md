# Fabric Deployment

Operator runbook for standing up the Fabric tier from scratch and keeping it
synced with `main`. Captures the exact UI paths, the gotchas, and the env-var
contract that `core` + the CI workflow rely on.

## Architecture

The Fabric tier is two halves moving in lockstep:

1. **Code (notebooks)** — synced from `fabric/notebooks/` into the Fabric workspace via **Fabric Git Integration** (workspace ↔ git folder).
2. **Library (core wheel)** — built from `core/` on every push, uploaded into a **Fabric Environment** that the workspace's notebooks attach to.

Notebooks `import core.*`; the `core` package comes from the Environment, not from the git-synced files. This is the library-vs-deployment split documented in [ADR-018](../../docs/adr/018-ci-cd-monorepo.md).

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

---

## One-time setup — step by step

Execute in order. Each step has a "you'll know it worked when" verification line.

### 1. Workspace + Lakehouse

1. **+ New workspace** → name `scribe_iq_lakehouse_fabric` (or your preference) → region **Central US** (or your nearest) → assign to a Fabric trial / capacity → **Apply**.
2. From the workspace landing page → **+ New item** → **Lakehouse** → name `scribe_iq_lakehouse_fabric`. **Schema-enabled** option ON (default for new lakehouses; required by the storage layout `Tables/<layer>/<table>` used in [`fabric/platform.py`](../platform.py)'s `storage_path`).

✅ **Verify:** lakehouse opens to an empty `Tables/` and `Files/` view.

### 2. Fabric Environment

The Environment is the Python runtime that every notebook attaches to. It holds Spark version, public PyPI deps, and the `core` wheel.

1. Workspace → **+ New item** → **Environment** → name **`scribe-iq-lakehouse-env`** (must match [`fabric/environments/lakehouse_env.yml`](../environments/lakehouse_env.yml) so the CI config lines up).
2. Top toolbar → **Runtime** → **1.3 (Spark 3.5, Delta 3.2)**.
3. **External repositories** in the Environment's inner left nav (click the **≡** hamburger if you only see a single content pane). Toolbar → **+ Add library** → for each of these, set Source = PyPI and add one at a time (the toolbar's "+ Add library" button stays clickable between adds — it doesn't disappear):

   | Library | Min version |
   |---|---|
   | `pyarrow` | `15.0` |
   | `pydicom` | `2.4` |
   | `python-dateutil` | `2.9` |
   | `boto3` | `1.34` *(anonymous S3 client for the public Synthea Coherent bucket — used by `01_bronze_ingest`)* |

   *Note: `pyarrow` and `python-dateutil` often already ship in the runtime — check **Built-in libraries** first; the explicit pin is harmless either way.*

4. **Publish** (top right) — Fabric rebuilds a Spark image (~2–5 min). Wait for green.
5. Open the lakehouse → top bar → **Environment** dropdown → select `scribe-iq-lakehouse-env`. This is how notebooks pick up the runtime.

✅ **Verify:** Environment page shows "Published" with the 4 packages in External repositories. Lakehouse top bar shows the environment attached.

### 3. Capture IDs into `.env`

Every Fabric identifier lives in env vars — never hardcoded in committed files. Template + capture instructions are in [`.env.example`](../../.env.example) at the repo root.

```bash
cp .env.example .env          # gitignored
```

Fill in `.env` with these three GUIDs from the Fabric URL bar:

| Variable | URL path |
|---|---|
| `FABRIC_WORKSPACE_ID` | `/groups/<GUID>` (when on workspace landing) |
| `FABRIC_LAKEHOUSE_ID` | `/lakehouses/<GUID>` (when inside the lakehouse) |
| `FABRIC_ENVIRONMENT_ID` | `/sparkenvironments/<GUID>` (when inside the environment) |

Then source it for the current shell:

```bash
set -a; source .env; set +a
echo $FABRIC_WORKSPACE_ID    # smoke test
```

✅ **Verify:** `echo` prints a GUID, not the literal placeholder.

### 4. Build + upload the core wheel — Path A (manual UI, no Azure)

The simplest path for development. No Service Principal needed — that's only for CI automation.

```bash
.venv/bin/python -m pip install build
.venv/bin/python -m build --wheel --outdir dist/
```

Produces `dist/scribe_iq_lakehouse-0.1.0-py3-none-any.whl` (~100 KB).

Then in Fabric: Environment → **Custom libraries** (left rail, below External repositories) → **+ Upload** → pick the wheel → Save → **Publish** (top right). Wait for green again (~2–5 min).

✅ **Verify:** Custom libraries shows `scribe_iq_lakehouse-0.1.0-py3-none-any.whl` with status Saved; publish history's most recent entry is Succeeded.

### 5. Build + upload — Path B (REST automation, needs Service Principal)

Run this once locally to validate the round-trip before relying on CI. Skippable if Path A works and you're not pushing for CI yet.

#### 5a. Register the Service Principal

1. **Azure Portal** → **Entra ID** → **App registrations** → **+ New registration** → name `fabric-scribe-iq-deploy` → leave redirect URI blank → **Register**.
2. App **Overview** → copy:
   - **Application (client) ID** → goes in `.env` as `FABRIC_CLIENT_ID`
   - **Directory (tenant) ID** → `.env` as `FABRIC_TENANT_ID`
3. Left rail → **Certificates & secrets** → **+ New client secret** → 24 months → **Add**.
4. Copy the **Value** column **immediately** (only shown once) → `.env` as `FABRIC_CLIENT_SECRET`.

#### 5b. Grant Contributor on the workspace

> **Gotcha:** "Manage access" is **NOT in Workspace settings**. It's on the workspace landing page itself.

1. Close Workspace settings if open. Go to the workspace landing page.
2. Top-right toolbar (near **Share**) → **people icon** labeled **Manage access**. *(Alternative: workspace name → **⋯** menu → Manage access.)*
3. **+ Add people or groups** → search for `fabric-scribe-iq-deploy` → role **Contributor** → **Add**.

#### 5c. Run the REST upload

```bash
set -a; source .env; set +a
.venv/bin/python -m pip install -e ".[fabric]"
.venv/bin/python fabric/deploy/upload_wheel.py \
  --wheel dist/scribe_iq_lakehouse-0.1.0-py3-none-any.whl \
  --environment-id "$FABRIC_ENVIRONMENT_ID"
```

Expected output:
```
[upload_wheel] acquiring Fabric token (tenant=xxxxxxxx…)
[upload_wheel] uploading scribe_iq_lakehouse-0.1.0-py3-none-any.whl → env <env-id>
[upload_wheel] publishing environment
[upload_wheel] waiting for publish to complete
[upload_wheel] publish succeeded
```

✅ **Verify:** same as Path A — Custom libraries shows the wheel, publish state Succeeded.

### 6. Fabric Git Integration

Wires the workspace ↔ `/fabric/notebooks/` so notebook commits flow both ways.

1. Workspace settings → **Git integration** → **Connect**.
2. Repository: `<your-github-org>/scribe-iq-lakehouse`
3. Branch: `main`
4. Git folder: `/fabric/notebooks` *(not the repo root — only notebooks live in workspace; `core/` arrives via Environment)*
5. Sync direction: **Bidirectional**.

✅ **Verify:** workspace shows a Git status badge. First sync is empty until Phase 4 lands notebooks.

---

## Why Azure DevOps (not GitHub Git Integration)

GitHub is the canonical public-facing repo. **Azure DevOps mirrors GitHub**
and is the Git provider Fabric Git Integration is wired to.

We didn't pick this for fun. On the Fabric trial tenant available to us,
the **"Users can sync workspace items with GitHub repositories"** tenant
setting is blocked at the admin level — Fabric Git Integration → Provider
list shows GitHub greyed-out / "disabled by your administrator," with no
way to flip it as a trial tenant admin. **Azure DevOps Git Integration is
unaffected** — listed and connectable from the same workspace, same MS
account.

The chosen arrangement:

```
GitHub                          Azure DevOps                Fabric
├─ canonical public repo  →     ├─ mirror of GitHub      ─→  ├─ Git Integration
├─ shows on portfolio           ├─ Fabric pulls from here     │   reads from DevOps
├─ all PRs / CI / history       └─ kept in sync via import    └─ workspace items
                                                                  round-trip here
```

**Keeping the mirror in sync:** when GitHub gets a new commit, refresh
DevOps via Repos → Files → Import (or set up a periodic sync job; for a
solo-dev project a manual re-import every few commits is fine).
Alternatively, configure `git push` locally to push to both remotes — see
[Phase 6 of the Azure DevOps walkthrough](#) (left as a one-time setup).

**If you later move off the trial** (paid F-SKU, different tenant): GitHub
Git Integration may become available and the DevOps mirror can be retired.
The repo content is identical between the two; only the Fabric Git
Integration provider would flip.

## CI flow

[`.github/workflows/fabric-deploy.yml`](../../.github/workflows/fabric-deploy.yml) **is currently
disabled (manual-trigger only).**

Reason: the workflow path (GitHub Actions → Service Principal → Fabric REST
API → notebook + wheel deploy) duplicates what Fabric Git Integration +
manual wheel upload already does on the DevOps path. Plus we never
registered the Service Principal, so every auto-trigger run was failing
with credential errors. Trigger changed to `workflow_dispatch` to stop the
noise; file kept for reference.

**To re-enable later** (if you set up Service Principal + secrets):

1. Register Service Principal in Entra ID; grant Contributor on the Fabric
   workspace.
2. Add `fabric-prod` GitHub Environment with these secrets:
   - `FABRIC_TENANT_ID`, `FABRIC_CLIENT_ID`, `FABRIC_CLIENT_SECRET`
   - `FABRIC_WORKSPACE_ID`, `FABRIC_LAKEHOUSE_ID`, `FABRIC_ENVIRONMENT_ID`
3. In `fabric-deploy.yml` change `on:` back to:
   ```yaml
   on:
     push:
       branches: [main]
       paths: ["core/**", "fabric/**", ".github/workflows/fabric-deploy.yml"]
     workflow_dispatch:
   ```

When enabled, the workflow:
1. Installs `[local,dev,orchestration,fabric]` + `build` + `fabric-cicd`.
2. Runs `pytest fabric/tests/` (5 contract tests pass; the `@pytest.mark.fabric` test runs against the real workspace).
3. Builds the core wheel.
4. Uploads via `fabric/deploy/upload_wheel.py` (MSAL Service Principal → REST PUT to `/environments/{env}/staging/libraries` → POST `/publish` → poll until `Success`).
5. Triggers `fabric-cicd deploy + smoke-run` against notebook 05.

---

## Local dev (no Fabric account)

For notebook development without deploying:

```bash
LAKEHOUSE_PLATFORM=local_lite .venv/bin/python -m core.surfaces.cli.pipeline --with-gold
```

The `LAKEHOUSE_PLATFORM` env var dispatches the factory to `LocalLitePlatform` (Polars + delta-rs) — same transforms, no Fabric account needed. Notebooks can be edited in VS Code with the Jupyter extension and pushed via Git Integration when ready.

---

## S3 ingest (no AWS account)

Notebook `01_bronze_ingest` uses `boto3` in anonymous mode (`Config(signature_version=UNSIGNED)`) to pull from the public `s3://synthea-open-data/coherent/` bucket directly into `Files/bronze/fhir/cohort=*/`. No AWS account, no IAM user, no S3 shortcut required. See [docs/roadmap/fabric-execution-plan.md](../../docs/roadmap/fabric-execution-plan.md) Phase 3 decision.

---

## Gotchas (things to save the next person 30 min)

- **Lakehouse must be schema-enabled.** The default for new lakehouses since 2024, but verify — the storage path layout in `fabric/platform.py` depends on it (`Tables/<layer>/<table>`).
- **External repositories ≠ Built-in libraries.** Built-in is read-only (shows what ships with the runtime). PyPI deps go in External repositories.
- **"+ Add library" is the only path** — you add libraries one at a time, but the button stays clickable between adds. The toolbar also has **Import YML** if you want bulk, but the schema is conda-style (not the same as our `lakehouse_env.yml`).
- **"Manage access" lives on the workspace landing page**, not in Workspace settings. Trips up everyone the first time.
- **Service Principal client secret is shown only once** — at creation, in the "Value" column. Copy it immediately or you'll have to delete + recreate.
- **Environment publish takes 2–5 min** for each change (new library OR new wheel). Plan for it; don't refresh anxiously.
- **The wheel includes both `core/` and `fabric/` packages.** That's correct — notebooks need `fabric.platform.FabricPlatform` to be importable too; the factory dispatches there for `LAKEHOUSE_PLATFORM=fabric`.

---

## Provisioned reference (current deployment)

| Resource | Name | Region |
|---|---|---|
| Workspace | `scribe_iq_lakehouse_fabric` | Central US |
| Lakehouse | `scribe_iq_lakehouse_fabric` | Central US |
| Environment | `scribe-iq-lakehouse-env` | Runtime 1.3, Spark 3.5, Delta 3.2 |

GUIDs live in your local `.env` and the `fabric-prod` GitHub Environment secrets — never in this doc.
