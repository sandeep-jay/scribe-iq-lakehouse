"""Spark helpers — bridge ``core/transforms`` with distributed Spark on Fabric.

The medallion's transforms (``core.transforms.silver_*``) deliberately stay
platform-agnostic (ADR-002): they take ``list[dict]`` records and return
``pa.Table``. To exploit Spark parallelism on Fabric without duplicating those
transforms, we wrap them in ``applyInPandas`` UDFs — each Spark executor
receives a partition of bundles, parses them with the same pure-Python
``FHIRBundleParser``, calls the same ``build_silver_*`` builder, and returns
a pandas DataFrame to the Spark planner.

ADR-020 covers the trade-offs (when applyInPandas vs full Spark SQL).

All imports of pyspark / delta-spark / notebookutils are deferred to call time
so this module imports cleanly outside Fabric (contract tests, local dev).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING, Any

import pyarrow as pa

if TYPE_CHECKING:
    import pandas as pd  # noqa: F401
    from pyspark.sql import DataFrame, SparkSession  # noqa: F401
    from pyspark.sql.types import StructType  # noqa: F401


# pa.Type str representations → Spark type names. Silver schemas in this
# project serialize complex types (struct/list) as JSON strings via
# build_arrow_table, so flat mapping is sufficient.
_PA_TO_SPARK: dict[str, str] = {
    "string": "string",
    "int8": "byte",
    "int16": "short",
    "int32": "integer",
    "int64": "long",
    "uint8": "short",
    "uint16": "integer",
    "uint32": "long",
    "uint64": "long",
    "float": "float",
    "double": "double",
    "bool": "boolean",
    "date32[day]": "date",
    "timestamp[ns]": "timestamp",
    "timestamp[us]": "timestamp",
    "timestamp[ns, tz=UTC]": "timestamp",
    "timestamp[us, tz=UTC]": "timestamp",
}


def pa_to_spark_schema(pa_schema: pa.Schema) -> StructType:
    """Convert a (flat) ``pa.Schema`` to a Spark ``StructType``.

    Unknown pa types fall back to ``string`` — safe because the project's
    Silver builders serialize nested data as JSON strings, never raw structs.
    """
    from pyspark.sql.types import (
        BooleanType,
        ByteType,
        DateType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        ShortType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    type_objs: dict[str, Any] = {
        "string": StringType(),
        "byte": ByteType(),
        "short": ShortType(),
        "integer": IntegerType(),
        "long": LongType(),
        "float": FloatType(),
        "double": DoubleType(),
        "boolean": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType(),
    }

    fields = []
    for f in pa_schema:
        spark_name = _PA_TO_SPARK.get(str(f.type), "string")
        fields.append(StructField(f.name, type_objs[spark_name], nullable=True))
    return StructType(fields)


def read_fhir_bundles_distributed(
    spark: SparkSession,
    fhir_root: str,
    *,
    num_partitions: int | None = None,
) -> DataFrame:
    """Read every FHIR bundle under ``fhir_root/cohort=*/*.json`` as a Spark DataFrame.

    Returns a DataFrame with columns ``path`` (input file URI) and ``value``
    (raw JSON text). Each row is one FHIR bundle. Partitioned across executors
    so downstream ``applyInPandas`` parses in parallel.

    Args:
        spark: Active SparkSession (Fabric injects as global ``spark``).
        fhir_root: abfss URI under which ``cohort=*`` partitions live
            (e.g. from ``platform.storage_path("bronze", "fhir")``).
        num_partitions: Override Spark's default partition count for the read.
            Useful when bundles are large or executor count is high; default
            lets Spark pick based on file sizes.

    Returns:
        Spark DataFrame, schema ``path: string, value: string``.
    """
    from pyspark.sql import (
        functions as F,  # noqa: N812 - F is the standard alias for pyspark.sql.functions
    )

    df = (
        spark.read.text(f"{fhir_root}/cohort=*/*.json", wholetext=True)
        .withColumn("path", F.input_file_name())
        .select("path", "value")
    )
    if num_partitions is not None:
        df = df.repartition(num_partitions)
    return df


def make_partition_parser(
    table_name: str,
    builder: Callable[[list[dict], datetime], pa.Table],
    ingest_ts: datetime,
) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """Build an ``applyInPandas`` UDF that parses bundles into one Silver table.

    The returned function takes one Spark partition (as ``pd.DataFrame`` with
    ``path`` + ``value`` columns), parses each bundle with the pure-Python
    ``FHIRBundleParser``, extracts records for ``table_name``, stamps
    ``source_file``, calls the typed ``build_silver_*`` builder, and returns
    a pandas DataFrame matching the Spark schema (which the caller derives
    via :func:`pa_to_spark_schema`).

    Args:
        table_name: Logical Silver table name (key into ``parse_bundle`` output).
        builder: ``build_silver_<table>`` function from ``core.transforms``.
            Must accept ``(list[dict], datetime) -> pa.Table``.
        ingest_ts: Single ingest timestamp stamped on every row of every
            partition. Pass the same value across the whole notebook run.

    Returns:
        A function suitable for ``DataFrame.groupBy(...).applyInPandas(fn, schema)``.
    """

    def parse_partition(pdf: pd.DataFrame) -> pd.DataFrame:
        from core.transforms.fhir_parser import FHIRBundleParser

        parser = FHIRBundleParser()
        records: list[dict] = []
        for _, row in pdf.iterrows():
            try:
                bundle = json.loads(row["value"])
            except (json.JSONDecodeError, ValueError):
                # Per-row resilience — corrupt bundle skipped, partition continues
                continue
            src = str(row["path"]).rsplit("/", 1)[-1]
            for r in parser.parse_bundle(bundle).get(table_name, []):
                r["source_file"] = src
                records.append(r)
        # Pure-Python builder returns pa.Table; hand pandas back to Spark planner
        return builder(records, ingest_ts).to_pandas()

    return parse_partition
