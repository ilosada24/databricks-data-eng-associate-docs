# Section 2: Data Ingestion and Loading

## Exam Objectives

- Enable and detail data ingestion patterns: batch, streaming, and incremental loading; import from local files, Lakeflow Connect standard connectors, and Lakeflow Connect managed connectors
- Use the COPY INTO command to incrementally load files from cloud object storage (ADLS/S3/GCS) into Unity-Catalog–governed tables
- Use Auto Loader with schema enforcement and schema evolution in batch modes (directory listing or file notification) to land data into Unity-Catalog–governed tables
- Configure Lakeflow Connect to reliably ingest data from diverse enterprise sources into Unity-Catalog–governed tables
- Use JDBC/ODBC or REST clients in notebooks to land data into cloud storage or directly into Unity-Catalog–governed tables, usually orchestrated and scheduled with Lakeflow Jobs
- Prioritize between Auto Loader, Lakeflow Connect (standard and managed connectors), partner connectors, and other ingestion methods based on technical requirements
- Ingest semi-structured and unstructured data (JSON, nested data) via Lakeflow Connect and other managed connectors into Unity-Catalog–governed Delta tables

---

## 2.1 Ingestion Patterns

### Batch ingestion

Data is loaded in discrete, scheduled intervals. The entire dataset (or a partition) is processed at once.

- **When to use:** Data arrives in bulk at predictable times (daily files, nightly database dumps)
- **Tools:** COPY INTO, Lakeflow Jobs with SQL/PySpark notebooks, JDBC reads

### Streaming ingestion

Data is processed continuously as it arrives, with very low latency.

- **When to use:** Real-time pipelines, event streams, CDC (Change Data Capture)
- **Tools:** Auto Loader (Structured Streaming), Lakeflow Spark Declarative Pipeline streaming tables

### Incremental loading

New data is appended or merged without reprocessing historical data. Tracks "high watermark" or uses file tracking.

- **When to use:** Growing datasets where full reloads are too expensive
- **Tools:** Auto Loader (checkpointing), COPY INTO (tracks loaded files), Lakeflow Connect (connector-managed offsets)

---

## 2.2 Auto Loader

Auto Loader (`cloudFiles` format) is Databricks' scalable, exactly-once file ingestion framework built on Structured Streaming.

### File discovery modes

| Mode | Mechanism | Best for |
|------|-----------|----------|
| **Directory listing** (default) | Scans source path; tracks processed files in checkpoint | Smaller volumes (<1M files/day), no cloud notification setup |
| **File notification** | Subscribes to cloud events (S3 EventBridge, Azure Event Grid, GCS Pub/Sub) | High-volume, millions of files/day; lower latency |

```python
# Enable file notification mode
.option("cloudFiles.useNotifications", "true")
```

**File notification** requires cloud infrastructure setup (event queue, notification subscription) but provides lower discovery latency and scales to very high file volumes without listing overhead.

### Core syntax

```python
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")                          # source file format
    .option("cloudFiles.schemaLocation", "/ckpt/schema_infer")    # schema inference store
    .option("cloudFiles.inferColumnTypes", "true")                # infer non-string types
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")    # schema evolution policy
    .option("pathGlobFilter", "*.json")                           # filename pattern filter
    .option("recursiveFileLookup", "true")                        # scan subdirectories
    .option("maxFilesPerTrigger", 1000)                           # max files per micro-batch
    .option("maxBytesPerTrigger", "10g")                          # max bytes per micro-batch
    .load("/Volumes/main/landing/raw_files/events/")
 .writeStream
    .option("checkpointLocation", "/ckpt/events_bronze")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)                                    # batch-like: all files, then stop
    .table("main.bronze.events"))
```

### Supported source formats

| Format | `cloudFiles.format` value | Notes |
|--------|--------------------------|-------|
| JSON | `json` | Supports nested objects/arrays |
| CSV | `csv` | Needs `header`, `delimiter` options |
| Parquet | `parquet` | Schema inferred from files |
| Avro | `avro` | Schema from files or registry |
| ORC | `orc` | |
| Text | `text` | Each line = one row |
| Binary files | `binaryFile` | Returns path, content, length, modificationTime |
| Delta | Use `spark.readStream.format("delta")` | Not cloudFiles |

### Schema inference and `schemaLocation`

Auto Loader infers schema from a **sample of files** and persists it to `schemaLocation`. On restart it reuses the stored schema — no re-inference overhead.

- `schemaLocation` can be the same directory as `checkpointLocation` (different subdirs)
- With `cloudFiles.inferColumnTypes = true`: infers `INT`, `DOUBLE`, `TIMESTAMP`, etc.
- With `cloudFiles.inferColumnTypes = false` (default): all columns inferred as `STRING`

### Schema evolution modes

| Mode | Behavior on new column in source |
|------|----------------------------------|
| `addNewColumns` **(default)** | New columns added to schema; stream continues |
| `rescue` | Unknown columns written to `_rescued_data` (STRING containing JSON fragment) |
| `failOnNewColumns` | Stream fails; requires manual schema update and stream restart |
| `none` | New columns silently ignored (lost) |

**The `_rescued_data` column** (rescue mode): captures any data that doesn't fit the schema:
- New columns not in schema
- Values that can't be cast to the expected type
- Stored as a JSON string fragment in `_rescued_data`

```python
# Read rescued data
df.select("_rescued_data").show()
# {"new_field": "value", "another_new_field": 123}
```

### Trigger modes for writeStream

| Trigger | Behavior | Use case |
|---------|----------|----------|
| `trigger(availableNow=True)` | Processes all backlog in multiple micro-batches, then stops. **Preferred over `once`.** | Scheduled batch-like runs |
| `trigger(once=True)` | Legacy: one micro-batch, then stops | Deprecated in favor of `availableNow` |
| `trigger(processingTime="5 minutes")` | Runs a micro-batch every N minutes | Continuous micro-batch streaming |
| `trigger(continuous="1 second")` | True continuous processing, ~1s latency | Low-latency streaming (experimental) |
| No trigger | Runs as fast as possible | Real-time streaming |

### Auto Loader metadata columns

Auto Loader automatically adds a `_metadata` struct column:

| Column | Content |
|--------|---------|
| `_metadata.file_path` | Full path of the source file |
| `_metadata.file_name` | File name only |
| `_metadata.file_size` | File size in bytes |
| `_metadata.file_modification_time` | Last modified timestamp |

```python
# Capture source file metadata in bronze table
(spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/ckpt/schema")
    .load("/landing/")
 .withColumn("source_file",  col("_metadata.file_path"))
 .withColumn("ingestion_time", current_timestamp())
 .writeStream
    .option("checkpointLocation", "/ckpt/bronze")
    .table("main.bronze.events"))
```

### Throttling ingestion

```python
# Limit micro-batch size to avoid overwhelming downstream
.option("maxFilesPerTrigger", 100)     # max 100 files per trigger
.option("maxBytesPerTrigger", "1g")    # max 1 GB per trigger
```

### Auto Loader checkpoint behavior

- Checkpoint stores: list of processed files, schema, stream state
- If checkpoint is deleted → stream restarts from scratch (reprocesses all files)
- Checkpoint is tied to a specific source path; changing source path requires new checkpoint
- Multiple streams reading from the same source require separate checkpoint locations

---

## 2.3 COPY INTO

`COPY INTO` is a **SQL command** that idempotently loads files from cloud storage into a Delta table, tracking which files have already been loaded.

### Syntax

```sql
-- JSON
COPY INTO main.bronze.events
FROM 'abfss://container@account.dfs.core.windows.net/landing/events/'
FILEFORMAT = JSON
FORMAT_OPTIONS ('inferSchema' = 'true', 'mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- CSV with header
COPY INTO main.bronze.sales
FROM 's3://my-bucket/data/sales/'
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header'      = 'true',
    'inferSchema' = 'true',
    'delimiter'   = ','
);

-- Parquet with credential
COPY INTO main.bronze.transactions
FROM (SELECT *, _metadata.file_path AS source_file
      FROM 's3://my-bucket/parquet/')
FILEFORMAT = PARQUET;

-- Load only specific files using pattern
COPY INTO main.bronze.events
FROM 's3://bucket/events/'
FILEFORMAT = JSON
PATTERN = '*.json'
FORMAT_OPTIONS ('inferSchema' = 'true');
```

### Key behaviors

| Behavior | Detail |
|----------|--------|
| **Idempotent** | Re-running does not reload already-loaded files |
| **File tracking** | Metadata stored in the Delta table's transaction log |
| **Schema** | Target table must exist before running (pre-create or use `inferSchema`) |
| **Supported formats** | CSV, JSON, Parquet, Avro, ORC, text, binary |
| **VALIDATE mode** | `COPY INTO ... VALIDATE ALL` — validates files without loading |

```sql
-- Validate without loading (check for schema issues first)
COPY INTO main.bronze.events
FROM 's3://bucket/events/'
FILEFORMAT = JSON
VALIDATE ALL;
```

### COPY_OPTIONS reference

| Option | Description |
|--------|-------------|
| `mergeSchema` | Merge source schema into target (add new columns) |
| `force` | `true` = re-load ALL files including previously loaded ones |

```sql
-- Force reload all files (re-process everything)
COPY INTO main.bronze.events
FROM 's3://bucket/events/'
FILEFORMAT = JSON
COPY_OPTIONS ('force' = 'true');
```

### Auto Loader vs COPY INTO

| | Auto Loader | COPY INTO |
|--|-------------|-----------|
| Interface | Structured Streaming (Python/Scala/SQL) | SQL |
| Scale | Millions of files | Thousands of files |
| Schema inference | Full inference + evolution | Basic inference |
| Schema evolution | Yes (`cloudFiles.schemaEvolutionMode`) | Limited (`mergeSchema`) |
| Checkpointing | Separate checkpoint directory | Via Delta transaction log |
| Orchestration | Runs as a streaming query | Runs as a SQL task in a job |
| File discovery | Directory listing or cloud events | Directory listing only |
| Best for | Production incremental pipelines, high volume | Simpler batch loads, SQL-centric workflows |

---

## 2.4 Lakeflow Connect

Lakeflow Connect is a managed ingestion service that connects enterprise data sources to Unity-Catalog–governed Delta tables **without custom code**.

### Standard connectors vs managed connectors

| | Standard connectors | Managed connectors |
|--|--------------------|--------------------|
| Who manages | Databricks manages the connector runtime | Databricks fully manages + scales the pipeline |
| Typical sources | SaaS apps: Salesforce, Workday, Google Analytics, HubSpot, Marketo | Enterprise DBs: Oracle, SAP, PostgreSQL, MySQL, Kafka, SQL Server |
| Latency | Near-real-time to hourly | Near-real-time |
| Schema evolution | Yes | Yes |
| CDC support | Limited | Yes (for supported databases) |
| Setup | Low-code UI configuration | Low-code UI configuration |
| Target | UC-governed Delta tables | UC-governed Delta tables |

### When to choose Lakeflow Connect over Auto Loader

- Source is a **SaaS application or relational database** (not cloud object storage files)
- You need **CDC** (Change Data Capture) from a relational database
- You want **no custom Spark code** — fully managed pipeline
- The connector handles **API auth, pagination, rate limits, schema mapping** automatically
- You need **governance** (lineage, audit) from source to UC without custom code

### Semi-structured and nested data ingestion

Lakeflow Connect managed connectors handle:
- **JSON nested objects** → mapped to `STRUCT` columns in Delta
- **Arrays** → mapped to `ARRAY` columns
- **Schema evolution** for nested fields handled automatically

```sql
-- Access nested fields after ingestion from a managed connector
SELECT
    event_id,
    payload.user.id       AS user_id,
    payload.user.email    AS email,
    explode(payload.tags) AS tag
FROM main.bronze.events;

-- Flatten a nested struct
SELECT
    event_id,
    payload.*            -- expands all fields in payload struct
FROM main.bronze.events;
```

### Ingestion from partner connectors

Partner connectors (Fivetran, Airbyte, Stitch, etc.) can write directly to Unity Catalog-governed tables using Databricks' partner connect. Choose partner connectors when:
- The source is not available as a Lakeflow Connect connector
- Your organization already has a partner tool contract
- Advanced transformation logic is needed at ingest time

---

## 2.5 JDBC/ODBC and REST Ingestion in Notebooks

For sources without a Lakeflow Connect connector, use JDBC, ODBC, or REST API calls from notebooks, scheduled via Lakeflow Jobs.

### JDBC read — parallel full load

```python
df = (spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://host:5432/mydb")
    .option("dbtable", "public.orders")
    .option("user",     dbutils.secrets.get("my-scope", "db-user"))
    .option("password", dbutils.secrets.get("my-scope", "db-password"))
    .option("numPartitions", 8)            # creates 8 parallel read tasks
    .option("partitionColumn", "order_id") # INTEGER or DATE column to partition on
    .option("lowerBound", "1")             # min value of partition column
    .option("upperBound", "1000000")       # max value of partition column
    .load())

df.write.mode("overwrite").saveAsTable("main.bronze.orders")
```

> **Note:** `numPartitions` + `partitionColumn` + `lowerBound` + `upperBound` are all required together for parallel JDBC reads. Without them, the read is single-threaded.

### JDBC incremental read (high-watermark pattern)

```python
from pyspark.sql.functions import max as spark_max

# Read last watermark from target table
last_updated = (spark.table("main.bronze.orders")
                .agg(spark_max("updated_at"))
                .collect()[0][0])

# If table is empty, use a far-past date
if last_updated is None:
    last_updated = "1970-01-01 00:00:00"

# Read only new/changed records
df = (spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://host:5432/mydb")
    .option("query", f"SELECT * FROM orders WHERE updated_at > '{last_updated}'")
    .option("user",     dbutils.secrets.get("scope", "user"))
    .option("password", dbutils.secrets.get("scope", "password"))
    .load())

df.write.mode("append").saveAsTable("main.bronze.orders")
```

### REST API ingestion

```python
import requests

token = dbutils.secrets.get("scope", "api-token")
response = requests.get(
    "https://api.example.com/v1/records",
    headers={"Authorization": f"Bearer {token}"},
    params={"since": "2024-01-01", "limit": 1000}
)
response.raise_for_status()

data = response.json()["records"]
df = spark.createDataFrame(data)
df.write.mode("append").saveAsTable("main.bronze.api_records")
```

### Databricks Secrets (always use for credentials)

```python
# Retrieve a secret (never hardcode credentials)
password = dbutils.secrets.get(scope="my-scope", key="db-password")

# List available scopes
dbutils.secrets.listScopes()

# List keys in a scope
dbutils.secrets.list("my-scope")

# Secrets are REDACTED in notebook output — printing them shows [REDACTED]
print(password)  # prints: [REDACTED]
```

---

## 2.6 Importing Data from Local Files

For small, ad-hoc datasets:

1. **Databricks UI → Catalog → Add data → Upload files** — uploads to a UC Volume path
2. **Notebook file upload widget** — drag-and-drop in notebook cells
3. **Databricks CLI:** `databricks fs cp local_file.csv /Volumes/main/uploads/`

```python
# Read after uploading to a Volume
df = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Volumes/main/landing/uploads/myfile.csv"))

df.write.mode("overwrite").saveAsTable("main.bronze.my_data")
```

---

## 2.7 Ingestion Method Selection Guide

| Requirement | Recommended method |
|-------------|-------------------|
| New files in S3/ADLS/GCS, high volume, continuous | **Auto Loader** |
| Files in cloud storage, SQL-only workflow, modest volume | **COPY INTO** |
| SaaS source (Salesforce, Workday, Google Analytics) | **Lakeflow Connect standard connector** |
| Enterprise database with CDC (Oracle, SAP, PostgreSQL) | **Lakeflow Connect managed connector** |
| Any JDBC database, custom incremental logic | **JDBC in notebook + Lakeflow Jobs** |
| REST API with custom pagination/auth | **REST in notebook + Lakeflow Jobs** |
| JSON/nested data from enterprise system | **Lakeflow Connect managed connector** |
| File source, schema changes frequent, millions of files | **Auto Loader with `addNewColumns` + file notification** |
| Partner BI tool or ETL platform already in use | **Partner connector** |
| Small CSV file, one-time load | **UI upload → COPY INTO or spark.read** |

---

## 2.8 Lakeflow Spark Declarative Pipelines (LDP) for Ingestion

LDP (Lakeflow Spark Declarative Pipelines, formerly Delta Live Tables) is the recommended framework for building production ingestion pipelines.

### LDP concepts

| Concept | Description |
|---------|-------------|
| **Streaming table** | Delta table populated by a streaming source (Auto Loader, Kafka) |
| **Materialized view** | Stored, auto-refreshed query result |
| **Expectations** | Data quality constraints applied at ingest |
| **LIVE namespace** | LDP tables referenced via `dlt.read()` or `LIVE.table_name` in SQL |
| **Pipeline mode** | `triggered` (runs on schedule) or `continuous` (always-on) |

### Minimal pipeline

```python
import dlt
from pyspark.sql.functions import col, current_timestamp

@dlt.table(
    name="bronze_events",
    comment="Raw events from landing zone",
    table_properties={"quality": "bronze"}
)
def bronze_events():
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", "/ckpt/bronze_schema")
            .load("/Volumes/main/landing/raw/events/"))

@dlt.expect_or_drop("non_null_event_id", "event_id IS NOT NULL")
@dlt.expect("valid_amount", "amount > 0")
@dlt.table(
    name="silver_events",
    comment="Cleaned events",
    table_properties={"quality": "silver"}
)
def silver_events():
    return (dlt.read_stream("bronze_events")
            .withColumn("ingested_at", current_timestamp())
            .filter(col("event_type").isNotNull()))
```

### SQL syntax equivalent

```sql
-- Bronze streaming table
CREATE OR REFRESH STREAMING TABLE bronze_events
COMMENT "Raw events from landing zone"
AS SELECT * FROM cloud_files('/Volumes/main/landing/raw/events/', 'json');

-- Silver with expectations
CREATE OR REFRESH STREAMING TABLE silver_events (
  CONSTRAINT non_null_event_id EXPECT (event_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amount      EXPECT (amount > 0)
)
AS SELECT *, current_timestamp() AS ingested_at
FROM STREAM(LIVE.bronze_events)
WHERE event_type IS NOT NULL;
```

### LDP expectations reference

| Decorator / Constraint | On violation |
|------------------------|-------------|
| `@dlt.expect` / `EXPECT` | Keep row, record metric (warning) |
| `@dlt.expect_or_drop` / `EXPECT ... ON VIOLATION DROP ROW` | Drop violating row |
| `@dlt.expect_or_fail` / `EXPECT ... ON VIOLATION FAIL UPDATE` | Fail the pipeline update |

### `dlt.read()` vs `dlt.read_stream()`

| | `dlt.read()` | `dlt.read_stream()` |
|--|-------------|---------------------|
| Processing | Batch (full table scan each update) | Streaming (only new rows) |
| Use for | Materialized views, dimension lookups | Streaming tables, append-only sources |
| Target | Materialized view | Streaming table |

### Pipeline modes

| Mode | Behavior | Use case |
|------|----------|----------|
| **Triggered** | Runs on schedule or manual trigger; processes all pending data then stops | Cost-effective batch workloads |
| **Continuous** | Always running; processes data as it arrives | Low-latency streaming |

---

## 2.9 Structured Streaming Deep Dive

### Output modes

| Mode | Behavior | Use with |
|------|----------|----------|
| `append` (default) | Only new rows since last trigger are written | Append-only sources (Auto Loader, Kafka); cannot use with aggregations that update |
| `complete` | Entire result table is rewritten every trigger | Aggregations (`groupBy`); output = full aggregate result |
| `update` | Only rows that changed since last trigger are written | Aggregations where you only need changed results; more efficient than complete |

```python
# Append mode (default for streaming to Delta)
query = df.writeStream.outputMode("append").table("target")

# Complete mode (required for running aggregations)
agg = df.groupBy("region").count()
query = agg.writeStream.outputMode("complete").table("region_counts")

# Update mode (only changed aggregates)
query = agg.writeStream.outputMode("update").table("region_counts")
```

### Watermarks (late data handling)

Watermarks tell Spark how long to wait for late-arriving data before finalizing a window.

```python
from pyspark.sql.functions import window, col

# Accept data up to 10 minutes late
windowed = (spark.readStream
    .format("delta")
    .table("main.bronze.events")
    .withWatermark("event_time", "10 minutes")     # watermark column + threshold
    .groupBy(window("event_time", "5 minutes"))    # tumbling window
    .count())

windowed.writeStream.outputMode("append").table("main.silver.event_counts")
```

**Key watermark behaviors:**
- Data arriving **within** the watermark threshold is included in the window
- Data arriving **after** the threshold is dropped (too late)
- Without watermark: Spark keeps all state forever → memory grows unbounded
- With watermark: Spark can clean up old state → bounded memory

### foreachBatch pattern

`foreachBatch` lets you apply **arbitrary batch operations** to each micro-batch of a stream — useful for MERGE, JDBC writes, or multi-table writes.

```python
def upsert_to_silver(batch_df, batch_id):
    """Apply MERGE logic to each micro-batch."""
    batch_df.createOrReplaceTempView("updates")
    batch_df._jdf.sparkSession().sql("""
        MERGE INTO main.silver.orders AS target
        USING updates AS source
        ON target.order_id = source.order_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/ckpt/schema")
    .load("/landing/orders/")
 .writeStream
    .foreachBatch(upsert_to_silver)
    .option("checkpointLocation", "/ckpt/silver_orders")
    .trigger(availableNow=True)
    .start())
```

**Use `foreachBatch` when:**
- You need MERGE (upsert) logic on a streaming source
- You need to write to multiple tables per micro-batch
- You need to call an external API per batch
- You need to write to a JDBC sink

### Exactly-once semantics

| Component | Guarantees |
|-----------|-----------|
| **Checkpoint** | Tracks which data has been processed (no re-processing on restart) |
| **Idempotent sink** | Delta Lake writes are atomic — partial writes are rolled back |
| **Checkpoint + Delta sink** | Together provide **exactly-once** end-to-end |

> **Exam tip:** Exactly-once requires both a checkpoint (source side) and an idempotent sink (target side). Delta Lake is an idempotent sink by design.

---

## 2.10 Common Exam Traps

| Trap | Correct answer |
|------|---------------|
| "Auto Loader can read Delta files" | False — use `spark.readStream.format("delta")`, not `cloudFiles` |
| "COPY INTO re-loads files if run twice" | False — COPY INTO is idempotent (skips already-loaded files) unless `force=true` |
| "Auto Loader schema inference happens every run" | False — inferred schema is persisted to `schemaLocation` and reused |
| "`availableNow=True` is the same as `once=True`" | Not exactly — `availableNow` uses multiple micro-batches (more efficient for large backlogs) |
| "File notification mode requires no cloud setup" | False — requires cloud event infrastructure (EventBridge, Event Grid, Pub/Sub) |
| "JDBC parallel read works with just `numPartitions`" | False — also needs `partitionColumn`, `lowerBound`, `upperBound` |
| "`append` output mode works with aggregations" | False — aggregations need `complete` or `update` output mode |
| "Watermarks guarantee no late data is lost" | False — data arriving after the watermark threshold is intentionally dropped |
| "`foreachBatch` only works with Delta sinks" | False — `foreachBatch` can write to any sink (JDBC, API, multi-table, etc.) |
