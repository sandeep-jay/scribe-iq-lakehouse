# Rules: fabric/notebooks/

These rules apply when editing any file under `fabric/notebooks/`.

## Source format — Fabric `.Notebook/` only

Notebooks live in Fabric's native source format:

```
fabric/notebooks/
└── <NN>_<name>.Notebook/
    ├── notebook-content.py    # Python with # CELL / # MARKDOWN magic comments
    └── .platform              # JSON metadata (type, displayName, logicalId)
```

`.ipynb` files are **not used** — `notebook-content.py` is the single source of
truth (ADR-021). Fabric Git Integration recognizes this layout natively;
notebooks committed to git appear in the workspace after a Sync.

Edit `notebook-content.py` directly in any text editor — cells are delimited
by `# CELL ********************` (code) or `# MARKDOWN ********************`
(markdown). Each cell ends with a `# METADATA ********************` block
declaring its language. For interactive editing, use the Fabric notebook UI
(round-trips via Git Integration).

## Spark-native pattern (ADR-022)

Every Silver notebook (02–07) is pure-Spark — no `applyInPandas`, no
`core.transforms.*` import, no `pa.Table` round-trip:

```python
from datetime import UTC, datetime
from fabric.platform import FabricPlatform
from fabric.transforms.registry import REGISTRY

TABLE = "patient"
platform = FabricPlatform()                        # ADR-022: no factory, no env var
spark = platform.get_spark_session()
ingest_ts = datetime.now(UTC)
spec = REGISTRY[TABLE]

bundles_df = platform.read_bronze_bundles_spark()  # (path, value) text DataFrame
silver_df = spec.build(bundles_df, ingest_ts)      # Spark-native from_json + project
platform.write_silver_spark(TABLE, silver_df, mode="merge")  # Delta MERGE + CDC + ADR-019 guard
```

Multi-table notebooks (04 clinical, 07 ecg+genomics) cache `bundles_df`
and loop over their table list — one `from_json` parse fans out to every
builder.

Gold (09) reads each Silver as a Spark DataFrame and calls
`fabric.gold.encounter_summary.build_encounter_summary(silver_dict, ...)`
which returns a Spark DataFrame; no driver-side Polars step.

Validation (08) uses `fabric.validation.validate.validate_table(name, df)`
— a single `.agg()` per table.

## Cell template

Each notebook uses cells in this rough order (count varies — clarity over
rigid count). All Silver notebooks (02–07) follow the same shape:

1. **Markdown** — title, purpose, I/O, scale, screenshot filename
2. **Markdown** — architecture (ADR refs, native engine strategy)
3. **Code** — imports + `FabricPlatform()` + spark + table constants
4. **Markdown** — step 1 description (read bundles distributed)
5. **Code** — `platform.read_bronze_bundles_spark()` → `bundles_df`
6. **Markdown** — step 2 description (build + MERGE)
7. **Code** — `spec.build(...)` + `platform.write_silver_spark(...)`
8. **Markdown** — validation
9. **Code** — `platform.read_silver_spark(...)` + count + `display()` + `log_metric`

## Paths
Never hardcode `abfss://` paths — always `platform.storage_path()`.

## Imports
Never import from `core.transforms.*` / `core.gold.*` / `core.validation.*`
in a Fabric notebook — Fabric is an independent end-to-end implementation
(ADR-022). Use `fabric.transforms.*` / `fabric.gold.*` / `fabric.validation.*`.

## Notebook 05 (SOAP notes) — special rule
**Demo centerpiece.** The validation cell MUST render a *decoded* SOAP note
in `display()` / `print()` output. A reviewer must see readable clinical
text — not a Base64 blob — in the notebook output. This is the
highest-priority screenshot of the project.

## Screenshot rule
Capture screenshots as you run — don't batch at the end. Evidence of a
running Fabric notebook is the priority during the trial window.