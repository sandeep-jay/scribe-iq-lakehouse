# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 01 — Bronze ingest: Synthea Coherent → OneLake
#
# **Purpose.** Pull Synthea Coherent FHIR bundles from the AWS Open Data
# public S3 bucket into the Fabric lakehouse Bronze layer. This is the
# self-contained-pipeline entry point: every downstream notebook (02–10)
# reads from the output of this one.
#
# **Source.** `s3://synthea-open-data/coherent/unzipped/fhir/*.json`
# (~1,278 patient bundles, ~4.6 GB total, public bucket — anonymous access
# via boto3 `Config(signature_version=UNSIGNED)`).
#
# **Output.** `Files/bronze/fhir/cohort={A,B,C}/*.json` in OneLake.
# Cohort partitioning is round-robin on the sorted key list, so each cohort
# is a representative slice rather than an alphabetic one — matches the
# local-tier `core.ingest.download.partition_into_cohorts` semantics.
#
# A small `Files/bronze/_metadata/ingest_manifest.json` is also written
# with source URI, ingestion timestamp, per-cohort file counts, and total
# byte count for downstream lineage.
#
# **Scale knob.** `SAMPLE_SIZE` in cell 3 controls cost vs completeness:
#   - `None` → full 1,278-patient corpus (~10–20 min, ~4.6 GB).
#   - `100`  → fast demo sample (~30 s, ~350 MB).
#
# Capture screenshot **01_bronze_ingest.png** of the final per-cohort
# count cell.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Architecture
#
# - **[ADR-022](../../docs/adr/022-platform-independent-implementations.md)** —
#   Fabric ingests directly via boto3 + `mssparkutils.fs.put`; no shared
#   `core.ingest` import (core uses the AWS CLI `s3 sync` locally — different
#   engine, same source).
# - **Cohort partitioning** mirrors the local-tier round-robin so the
#   `cohort=*` folder layout is identical to what notebook 00's S3 gate
#   already validated and what notebooks 02–07 read with
#   `platform.read_bronze_bundles_spark()`.
# - **Anonymous S3** — the bucket is public; no credentials, no Service
#   Principal, no `.env`. Validated as a gate in notebook 00.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import time
from datetime import UTC, datetime

import boto3
import notebookutils.mssparkutils as msu
from botocore import UNSIGNED
from botocore.config import Config

from fabric.platform import FabricPlatform

# --- config (the only knobs that matter) ------------------------------------
BUCKET = "synthea-open-data"
SOURCE_PREFIX = "coherent/unzipped/fhir/"
COHORTS = ("A", "B", "C")  # round-robin partition labels
SAMPLE_SIZE: int | None = 100  # set to None for the full ~1,278 corpus
# ---------------------------------------------------------------------------

platform = FabricPlatform()
spark = platform.get_spark_session()  # used by the validation cell's bundle read
platform.ensure_env()  # resolve workspace + lakehouse IDs from Spark conf
fhir_root = platform.storage_path("bronze", "fhir")  # abfss://.../Files/bronze/fhir
s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

mode_str = f"sample={SAMPLE_SIZE}" if SAMPLE_SIZE else "full"
print(f"Source : s3://{BUCKET}/{SOURCE_PREFIX}")
print(f"Target : {fhir_root}/cohort={{{','.join(COHORTS)}}}")
print(f"Mode   : {mode_str}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1 — List Coherent FHIR bundles in S3
#
# Page through the public bucket and collect every `.json` key under the
# FHIR prefix. Sort by key so cohort assignment is deterministic across
# re-runs. If `SAMPLE_SIZE` is set, take the first N keys after sorting.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

paginator = s3.get_paginator("list_objects_v2")
keys: list[tuple[str, int]] = []
for page in paginator.paginate(Bucket=BUCKET, Prefix=SOURCE_PREFIX):
    for obj in page.get("Contents", []):
        if obj["Key"].endswith(".json"):
            keys.append((obj["Key"], int(obj["Size"])))

keys.sort(key=lambda kv: kv[0])
if SAMPLE_SIZE is not None:
    keys = keys[:SAMPLE_SIZE]

total_bytes = sum(sz for _, sz in keys)
print(f"Bundles to fetch: {len(keys):,}")
print(f"Total size      : {total_bytes / 1e9:.2f} GB")
print(f"First key       : {keys[0][0] if keys else '(none)'}")
print(f"Last key        : {keys[-1][0] if keys else '(none)'}")
assert keys, f"No .json keys found under s3://{BUCKET}/{SOURCE_PREFIX}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Download + write to OneLake (round-robin cohorts)
#
# For each S3 key, download the bundle, assign it to a cohort by
# round-robin index, and write it under
# `Files/bronze/fhir/cohort=<label>/<basename>` with `mssparkutils.fs.put`
# (overwrite=True so this notebook is idempotent — re-running replaces in
# place rather than erroring on existing files).
#
# Progress is reported every 50 bundles. Failures are logged but don't
# abort the run; the manifest's `file_count` is the truth at the end.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cohort_counts: dict[str, int] = dict.fromkeys(COHORTS, 0)
bytes_written = 0
errors: list[tuple[str, str]] = []
t_start = time.time()

for i, (key, size) in enumerate(keys):
    label = COHORTS[i % len(COHORTS)]
    basename = key.rsplit("/", 1)[-1]
    dest = f"{fhir_root}/cohort={label}/{basename}"
    try:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        msu.fs.put(dest, body.decode("utf-8"), True)  # overwrite=True (idempotent)
        cohort_counts[label] += 1
        bytes_written += size
    except Exception as err:  # noqa: BLE001 — per-bundle resilience, partition continues
        errors.append((key, f"{type(err).__name__}: {err}"))

    if (i + 1) % 50 == 0 or (i + 1) == len(keys):
        elapsed = time.time() - t_start
        rate = (i + 1) / elapsed if elapsed > 0 else 0.0
        print(f"  {i + 1:>5,}/{len(keys):,}  ·  {bytes_written / 1e9:.2f} GB  ·  {rate:.1f} bundles/s")

elapsed = time.time() - t_start
print(f"\nDownloaded {sum(cohort_counts.values()):,} bundles in {elapsed / 60:.1f} min")
if errors:
    print(f"  ({len(errors)} failures — see Cell 7)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation — per-cohort counts + sample bundle + manifest write
#
# 1. Read back the OneLake directory listing and count `.json` per cohort
#    (truth-from-storage, not from the in-memory counter).
# 2. Pull one bundle, parse it, count `resourceType`s — sanity check that
#    what landed is actually FHIR.
# 3. Write `Files/bronze/_metadata/ingest_manifest.json` with provenance
#    (source URI, ingest timestamp, per-cohort counts, byte total) —
#    same shape as `core.ingest.download.IngestManifest`.

# METADATA ********************

# META {
# META   "language": "markdown",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Truth-from-storage cohort counts
written_counts: dict[str, int] = {}
for label in COHORTS:
    try:
        entries = msu.fs.ls(f"{fhir_root}/cohort={label}")
        written_counts[label] = sum(1 for e in entries if e.name.endswith(".json"))
    except Exception:  # noqa: BLE001 — empty cohort if dir doesn't exist yet
        written_counts[label] = 0

total_landed = sum(written_counts.values())
print("Per-cohort file counts (from OneLake):")
for label, n in written_counts.items():
    print(f"  cohort={label}: {n:,}")
print(f"  total      : {total_landed:,}")

# 2. Sample bundle resource-type counts (proves it's parseable FHIR).
# Best-effort — wrapped so the manifest write below always runs even if the
# sample bundle is unreadable. Reads via Spark wholetext to bypass
# mssparkutils.fs.head's silent truncation on large bundles.
sample_label = next((label for label, n in written_counts.items() if n > 0), None)
if sample_label:
    try:
        sample_entry = next(
            e for e in msu.fs.ls(f"{fhir_root}/cohort={sample_label}") if e.name.endswith(".json")
        )
        sample_text = spark.read.text(sample_entry.path, wholetext=True).first()["value"]
        sample_bundle = json.loads(sample_text)
        resource_counts: dict[str, int] = {}
        for entry in sample_bundle.get("entry", []):
            rtype = entry.get("resource", {}).get("resourceType")
            if rtype:
                resource_counts[rtype] = resource_counts.get(rtype, 0) + 1
        print(f"\nSample bundle: {sample_entry.name}")
        print(f"  resources: {sum(resource_counts.values()):,} across {len(resource_counts)} types")
        for rtype, n in sorted(resource_counts.items(), key=lambda kv: -kv[1])[:8]:
            print(f"    {rtype:<24} {n:>5}")
    except Exception as err:  # noqa: BLE001
        print(f"\n(skipped sample histogram: {type(err).__name__}: {err})")

# 3. Write the ingest manifest (same shape as core/ingest/download.IngestManifest)
manifest = {
    "source": f"s3://{BUCKET}/{SOURCE_PREFIX}",
    "ingested_at": datetime.now(UTC).isoformat(),
    "file_count": total_landed,
    "total_bytes": bytes_written,
    "cohorts": written_counts,
    "sample_size": SAMPLE_SIZE,
    "platform": platform.name,
    "errors": [{"key": k, "reason": r} for k, r in errors[:25]],  # cap to keep manifest small
}
manifest_path = platform.files_path("bronze/_metadata/ingest_manifest.json")
msu.fs.put(manifest_path, json.dumps(manifest, indent=2), True)
print(f"\nWrote manifest: {manifest_path}")

# Failure summary (don't fail the cell — Bronze is best-effort, downstream
# notebooks will fail loudly if too little data landed)
if errors:
    print(f"\n{len(errors)} download failures (first 5):")
    for k, r in errors[:5]:
        print(f"  {k}  →  {r}")

assert total_landed > 0, "No bundles landed — check S3 prefix and OneLake permissions"
platform.log_metric("bronze.fhir", "file_count", total_landed)
print(f"\n01_bronze_ingest complete — {total_landed:,} bundles in {fhir_root} — next: 02_silver_patient")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
