# Rules: fabric/notebooks/

These rules apply when editing any file in fabric/notebooks/.

## 8-cell documentation template (required for every notebook)
Every notebook must have these cells in order:
  Cell 1: Markdown — what this notebook does, I/O, dependencies
  Cell 2: Markdown — architecture context, ADR references
  Cell 3: Code — imports + platform setup (LAKEHOUSE_PLATFORM=fabric)
  Cell 4: Markdown — transform approach explanation
  Cell 5: Code — transform execution
  Cell 6: Markdown — validation approach
  Cell 7: Code — display() + row count + quality checks
  Cell 8: Code — log to silver.ingest_log

## Import pattern
```python
import os
os.environ["LAKEHOUSE_PLATFORM"] = "fabric"
from local.platform.factory import get_platform
from local.transforms.{module} import {transform_function}
platform = get_platform()
```

## Paths
Never hardcode abfss:// paths — always platform.storage_path().

## Validation cell (required)
Every notebook must assert a minimum row count before logging success:
```python
count = spark.table(f"silver.{table_name}").count()
assert count >= MIN_ROWS, f"Row count {count} below minimum {MIN_ROWS}"
display(spark.table(f"silver.{table_name}").limit(5))
```

## Notebook 05 (SOAP notes) — special rule
This is the demo centerpiece. It MUST show a decoded SOAP note in display() output.
A reviewer must be able to see readable clinical text in the notebook output cells.

## Screenshot rule
Capture screenshots of notebook output as you go — don't batch at the end.
Evidence of a running Fabric notebook is the priority before trial expires.
