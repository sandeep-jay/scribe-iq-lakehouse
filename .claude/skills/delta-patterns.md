# Skill: Delta Lake Patterns

Rules for working with Delta Lake tables in scribe-iq-lakehouse.
Apply for all Silver and Gold table work.

## Core rules
1. CDC always enabled on Silver and Gold tables:
```sql
ALTER TABLE silver.soap_note
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

2. MERGE preferred over overwrite for Silver tables:
```python
from delta.tables import DeltaTable

def merge_silver(spark, new_df, target_path, merge_key):
    if DeltaTable.isDeltaTable(spark, target_path):
        dt = DeltaTable.forPath(spark, target_path)
        (dt.alias("target")
           .merge(new_df.alias("source"),
                  f"target.{merge_key} = source.{merge_key}")
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute())
    else:
        new_df.write.format("delta").save(target_path)
```

3. Never partition by high-cardinality columns.
   Good: date, cohort, specialty. Bad: patient_id, encounter_id, UUID.

4. Z-ORDER on frequently filtered columns after writes:
```sql
OPTIMIZE silver.soap_note ZORDER BY (patient_id, encounter_id);
```

5. Schema evolution — always explicit:
```python
df.write.format("delta").option("mergeSchema", "true").mode("append").save(path)
```

6. Checkpoint location must be unique per stream.
7. Vacuum default retention is 7 days — don't reduce below that.

## Medallion rules
Bronze: Append-only, never modify. Raw files as-is. Log to _metadata/.
Silver: Validated, typed, CDC-enabled. MERGE not overwrite. ingest_timestamp on every row.
Gold: Denormalized for AI. Corpus contract defines schema. Lineage back to Silver.

## Platform paths — never hardcode
```python
# Good
path = platform.storage_path("silver", "soap_note")

# Bad — violates ADR-002
path = "abfss://lakehouse@onelake.dfs.fabric.microsoft.com/silver/soap_note"
```

## Streaming (Fabric Auto Loader)
```python
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", f"{BRONZE}/_schemas/")
      .load(f"{BRONZE}/fhir/"))
```
Simulate stream by dropping cohort partitions sequentially.

## delta-rs (local lite tier)
```python
from deltalake import write_deltalake, DeltaTable
dt = DeltaTable(path)
df = pl.from_arrow(dt.to_pyarrow())
write_deltalake(path, df.to_arrow(), mode="append")
```

## Quality gates after every Silver write
- Row count >= minimum threshold
- Required columns non-null
- Referential integrity (FKs resolve)
- Clinical range checks (heart rate 30-250, age 0-130)
- Row count drift < 10% vs last run

## Common gotchas
- DeltaTable.isDeltaTable() before first MERGE
- Auto Loader schema inference can fail on first run — provide hint schema
- CDC readChangeFeed requires startingVersion or startingTimestamp
