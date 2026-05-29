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

## Distributed Spark pattern (ADR-020)

Every Silver notebook (02–07) is **Spark-distributed**:

```python
import os; os.environ["LAKEHOUSE_PLATFORM"] = "fabric"
from core.platform.factory import get_platform
from core.transforms.registry import SILVER_TABLES
from fabric.spark_helpers import (
    make_partition_parser, pa_to_spark_schema, read_fhir_bundles_distributed,
)
from pyspark.sql import functions as F

platform = get_platform()
spark = platform.get_spark_session()

# 1. Read FHIR bundles as a partitioned Spark DataFrame
bundles_df = read_fhir_bundles_distributed(spark, platform.storage_path("bronze", "fhir"))

# 2. Distributed parse + build via applyInPandas — each executor runs the
#    pure-Python FHIRBundleParser + build_silver_<table> on its partition
spec = SILVER_TABLES[TABLE]
parse_udf = make_partition_parser(TABLE, spec.build, ingest_ts)
silver_df = bundles_df.groupBy(F.spark_partition_id()).applyInPandas(
    parse_udf, schema=pa_to_spark_schema(spec.schema)
)

# 3. Spark-native Delta MERGE (CDC + ADR-019 dedup guard)
platform.write_silver_spark(TABLE, silver_df, mode="merge")
```

Multi-table silver notebooks (04 clinical, 07 ecg+genomics) cache
`bundles_df` and run one `applyInPandas` pipeline per output table.

Gold notebook (09) uses Spark for reads + writes but does the global
denormalization on the driver via `build_encounter_summary` (Polars). See
ADR-020 §"When applyInPandas vs driver-side compute" for the trade.

## Cell template

Each notebook uses cells in this order (count varies by notebook — clarity
over rigid count):

1. **Markdown** — title, purpose, I/O, scale, screenshot filename
2. **Markdown** — architecture context (ADR refs, distributed strategy)
3. **Code** — imports + platform + spark + table constants
4. **Markdown** — step 1 description (read bundles distributed)
5. **Code** — `read_fhir_bundles_distributed(...)` → `bundles_df`
6. **Markdown** — step 2 description (parse + build via applyInPandas)
7. **Code** — `make_partition_parser` + `applyInPandas` → `silver_df`
8. **Markdown** — step 3 description (Spark-native MERGE)
9. **Code** — `platform.write_silver_spark(...)`
10. **Markdown** — validation
11. **Code** — `platform.read_silver_spark(...)` + count + `display()`
12. **Code** — `platform.log_metric(...)` + "next:" pointer

## Paths
Never hardcode abfss:// paths — always `platform.storage_path()`.

## Notebook 05 (SOAP notes) — special rule
**Demo centerpiece.** Cell 11 (or equivalent code cell after validation
prints) MUST render a *decoded* SOAP note via `displayHTML(...)`. A reviewer
must see readable clinical text — not a Base64 blob — in the notebook
output. This is the highest-priority screenshot of the project.

## Screenshot rule
Capture screenshots as you run — don't batch at the end. Evidence of a
running Fabric notebook is the priority during the trial window.