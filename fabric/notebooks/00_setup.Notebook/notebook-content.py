# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 00 — Setup verification
#
# **Purpose.** First-run sanity check for the Fabric tier. Runs zero transforms and writes zero data — verifies only that the runtime is healthy enough for notebooks 01–10 to succeed.
#
# **Inputs.** None.
#
# **Outputs.** None (no Delta writes, no ingest_log row). Just a printed checklist + a tiny `boto3` listing as proof the public S3 bucket is reachable.
#
# **Dependencies (Environment `scribe-iq-lakehouse-env`).**
# - `scribe-iq-lakehouse-core` wheel (uploaded via Custom libraries) — provides `core.*` + `fabric.*`
# - `pyarrow>=15.0`, `pydicom>=2.4`, `python-dateutil>=2.9` (from External repositories)
# - `boto3>=1.34` (anonymous S3 client for the Synthea Coherent public bucket)
#
# **What it checks (4 gates).**
# 1. Wheel installed: `from core.platform.factory import get_platform` + `from fabric.platform import FabricPlatform` both succeed.
# 2. Spark + notebookutils healthy: active SparkSession + workspace ID readable.
# 3. FabricPlatform: `storage_path(...)` returns a valid OneLake `abfss://` URI; `Files/bronze/` is reachable via `mssparkutils.fs.ls`.
# 4. S3 anonymous access: `boto3.client('s3', config=Config(signature_version=UNSIGNED))` can list `synthea-open-data/coherent/`.
#
# Capture screenshot **00_workspace_overview** of this notebook's final output cell.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture context
#
# This notebook validates the deployment contract documented across these ADRs and runbooks:
#
# - **[ADR-001](../../docs/adr/001-fabric-first.md)** — Fabric-first development; the runtime this notebook depends on.
# - **[ADR-002](../../docs/adr/002-platform-abstraction.md)** — `LakehousePlatform` interface; `FabricPlatform` is one implementation, dispatched by the factory on `LAKEHOUSE_PLATFORM=fabric`.
# - **[ADR-017](../../docs/adr/017-multi-platform-repo-layout.md)** — `core/` ships as a wheel; notebooks `import core.*` from the wheel, never reach into source.
# - **[ADR-018](../../docs/adr/018-ci-cd-monorepo.md)** — the wheel arrives via `fabric/deploy/upload_wheel.py` (manual via UI today; CI later).
# - **[ADR-019](../../docs/adr/019-silver-merge-idempotency.md)** — `FabricPlatform._write_delta` mirrors LocalLite's target-dedup guard; exercised in notebooks 02+, this notebook only verifies the class loads.
#
# **Operator runbook:** [fabric/docs/DEPLOYMENT.md](../docs/DEPLOYMENT.md). If any cell below fails, that doc lists the setup step that was likely missed.
#
# **8-cell template:** this notebook follows the structure mandated by [`.claude/rules/notebooks.md`](../../.claude/rules/notebooks.md), with cells 4–8 adapted for setup verification (no Delta write, no `silver.ingest_log` row — those concepts don't apply to a setup check).

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os

os.environ["LAKEHOUSE_PLATFORM"] = "fabric"

from core.platform.factory import get_platform
from fabric.platform import FabricPlatform

platform = get_platform()
assert isinstance(platform, FabricPlatform), (
    f"Expected FabricPlatform, got {type(platform).__name__} — "
    "check LAKEHOUSE_PLATFORM env var and core/platform/factory.py mapping."
)
print(f"✓ wheel installed; factory returned {type(platform).__name__}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Verification approach
#
# Four independent gates, each producing a `(name, passed, detail)` tuple. We run all four (don't short-circuit on first failure) so the operator sees the full picture in one run.
#
# 1. **Spark + notebookutils.** Pull the active `SparkSession` (Fabric injects it as the global `spark`) and the workspace ID via `mssparkutils.env.getWorkspaceId()`. If either is missing, the runtime isn't a Fabric runtime — abort.
# 2. **FabricPlatform URI builder.** Call `platform.storage_path("silver", "patient")` and verify the returned URI matches the schema-enabled OneLake shape `abfss://<ws>@onelake.dfs.fabric.microsoft.com/<lh>.Lakehouse/Tables/silver/patient`.
# 3. **OneLake Files reachable.** `mssparkutils.fs.ls` against `Files/`. If this throws, the lakehouse isn't attached to the notebook — fix via the lakehouse top bar dropdown.
# 4. **S3 anonymous access.** Build a `boto3` client with `Config(signature_version=UNSIGNED)` and list the `synthea-open-data` bucket's `coherent/` prefix. If this fails, `boto3` isn't installed in the Environment — re-check External repositories.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from dataclasses import dataclass

import notebookutils.mssparkutils as msu
from pyspark.sql import SparkSession


@dataclass
class Check:
    name: str
    passed: bool
    detail: str


results: list[Check] = []

# Gate 1 — Spark + workspace.
try:
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    workspace_id = msu.env.getWorkspaceId()
    results.append(
        Check("spark+workspace", True, f"spark={spark.version} workspace={workspace_id[:8]}…")
    )
except Exception as err:  # noqa: BLE001
    results.append(Check("spark+workspace", False, f"{type(err).__name__}: {err}"))

# Gate 2 — FabricPlatform.storage_path URI shape.
try:
    uri = platform.storage_path("silver", "patient")
    ok = uri.startswith("abfss://") and ".Lakehouse/Tables/silver/patient" in uri
    results.append(Check("storage_path", ok, uri))
except Exception as err:  # noqa: BLE001
    results.append(Check("storage_path", False, f"{type(err).__name__}: {err}"))

# Gate 3 — OneLake Files reachable.
try:
    files_root = platform.storage_path("bronze", "").rsplit("/", 1)[0]  # Files/bronze
    entries = msu.fs.ls(files_root.rsplit("/", 1)[0])  # Files/
    results.append(Check("onelake_files", True, f"Files/ listed {len(entries)} entries"))
except Exception as err:  # noqa: BLE001
    results.append(Check("onelake_files", False, f"{type(err).__name__}: {err}"))

# Gate 4 — boto3 anonymous S3 listing.
try:
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config

    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    resp = s3.list_objects_v2(Bucket="synthea-open-data", Prefix="coherent/", MaxKeys=5)
    sample = [obj["Key"] for obj in resp.get("Contents", [])]
    results.append(
        Check("s3_anonymous", bool(sample), f"listed {len(sample)} keys; first: {sample[0] if sample else 'none'}")
    )
except Exception as err:  # noqa: BLE001
    results.append(Check("s3_anonymous", False, f"{type(err).__name__}: {err}"))

for r in results:
    print(f"{'✓' if r.passed else '✗'} {r.name:<20} {r.detail}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation
#
# All four gates must pass. If any failed, the detail string in the cell above points at the fix — usually one of:
# - **wheel installed but stale**: rebuild via `python -m build --wheel` and re-upload to Custom libraries.
# - **lakehouse not attached**: open the lakehouse top bar → Environment dropdown → pick `scribe-iq-lakehouse-env`.
# - **boto3 import error**: External repositories missing `boto3>=1.34` — add via `+ Add library`, Publish, retry.
#
# The Spark `display()` below summarises the same results as a sortable table — that's the cell to screenshot for `00_workspace_overview`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary_df = spark.createDataFrame(
    [(r.name, r.passed, r.detail) for r in results],
    schema="name STRING, passed BOOLEAN, detail STRING",
)
display(summary_df)

failed = [r.name for r in results if not r.passed]
assert not failed, f"Setup gates failed: {failed} — see DEPLOYMENT.md"
print(f"\nAll {len(results)} gates passed — Fabric tier ready for notebooks 01–10.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Setup notebooks don't write to silver.ingest_log (no ingest happened).
# The presence of a green run + the screenshot of the display() cell above
# is the audit trail. Subsequent notebooks (01–10) DO write ingest_log rows
# via core.validation, per the 8-cell template.
print("00_setup complete — next: 01_bronze_ingest.ipynb")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
