# Section 1: Databricks Intelligence Platform

## Exam Objectives

- Understand the core components of the Databricks Data Intelligence Platform: architecture, Delta Lake, and Unity Catalog
- Understand Databricks compute services: characteristics, limitations, cost models, and select the most suitable option for each workload use case

---

## 1.1 Platform Architecture

The Databricks Data Intelligence Platform is built on three pillars:

```
┌─────────────────────────────────────────────────────────────┐
│            Databricks Data Intelligence Platform             │
├────────────────┬──────────────────┬─────────────────────────┤
│   Delta Lake   │  Apache Spark    │     Unity Catalog        │
│  open storage  │  unified compute │  governance & discovery  │
└────────────────┴──────────────────┴─────────────────────────┘
              Cloud object storage (S3 / ADLS / GCS)
```

### Workspace

The workspace is the primary environment where users interact with Databricks:
- Notebooks, files, and Git folders (Repos)
- Clusters and SQL Warehouses
- Lakeflow Jobs (orchestration)
- Lakeflow Spark Declarative Pipelines (LDP)
- Unity Catalog browser

Multiple workspaces can be attached to **a single Unity Catalog metastore** — this is how cross-workspace governance works. One metastore per region per Databricks account.

### Databricks Runtime (DBR)

Every cluster runs a specific DBR version. DBR bundles:
- Apache Spark
- Delta Lake
- Python, R, Scala, Java
- Pre-installed ML/data science libraries (in ML Runtime)
- Optimizations: Photon, Delta optimizations, Databricks-specific APIs

| Runtime | Use case |
|---------|----------|
| **Standard DBR** | General data engineering |
| **DBR ML** | Machine learning (adds MLflow, TensorFlow, PyTorch, etc.) |
| **Photon DBR** | Vectorized SQL/DataFrame acceleration |
| **Light DBR** | Lightweight jobs, faster startup |

> **Exam tip:** Serverless compute automatically runs the latest supported DBR — you cannot pin a version.

---

## 1.2 Delta Lake

Delta Lake is the open-source storage layer that brings **ACID transactions** to cloud object storage. All Databricks-managed tables use Delta Lake by default.

### Core capabilities

| Capability | Description |
|------------|-------------|
| **ACID transactions** | Serializable isolation; concurrent reads/writes without corruption |
| **Scalable metadata** | Transaction log (`_delta_log/`) tracks all changes; scales to billions of files |
| **Schema enforcement** | Rejects writes that don't match the table schema (by default) |
| **Schema evolution** | Opt-in: `mergeSchema`, `overwriteSchema` write options |
| **Time travel** | Query historical versions by version number or timestamp |
| **Unified batch + streaming** | Same table can be read/written by both batch and streaming |
| **DML support** | `UPDATE`, `DELETE`, `MERGE INTO` on Parquet-based storage |

### Transaction log (`_delta_log/`)

- Located at `<table_path>/_delta_log/`
- JSON files (`000000000000000000N.json`) recording every transaction: add/remove file, metadata change, protocol change
- Every 10 commits → **checkpoint file** (Parquet) for faster log replay
- Powers: time travel, `DESCRIBE HISTORY`, `RESTORE`, concurrent writes

```
_delta_log/
  ├── 0000000000000000000.json  (commit 0 — table creation)
  ├── 0000000000000000001.json  (commit 1 — insert)
  ├── 0000000000000000010.checkpoint.parquet  (checkpoint after 10 commits)
  └── _last_checkpoint           (pointer to latest checkpoint)
```

### Key Delta commands

```sql
-- View full history of a table
DESCRIBE HISTORY main.silver.orders;
-- Returns: version, timestamp, operation, operationParameters, userEmail

-- Time travel: query by version
SELECT * FROM main.silver.orders VERSION AS OF 5;

-- Time travel: query by timestamp
SELECT * FROM main.silver.orders TIMESTAMP AS OF '2024-06-01T00:00:00';

-- Restore to a previous state (in-place, creates new commit)
RESTORE TABLE main.silver.orders TO VERSION AS OF 5;
RESTORE TABLE main.silver.orders TO TIMESTAMP AS OF '2024-06-01';

-- Compact small files into larger ones (target ~1GB per file)
OPTIMIZE main.silver.orders;
OPTIMIZE main.silver.orders WHERE order_date >= '2024-01-01';  -- partition predicate

-- Z-ORDER (legacy — prefer Liquid Clustering for new tables)
OPTIMIZE main.silver.orders ZORDER BY (customer_id, order_date);

-- Remove unreferenced files older than retention threshold
VACUUM main.silver.orders;                   -- default: 7-day retention
VACUUM main.silver.orders RETAIN 168 HOURS; -- explicit 7-day retention
VACUUM main.silver.orders RETAIN 0 HOURS DRY RUN;  -- preview what would be deleted

-- Show table properties
DESCRIBE TABLE EXTENDED main.silver.orders;
DESCRIBE DETAIL main.silver.orders;  -- size, numFiles, partitionColumns, etc.
```

> **Warning:** `VACUUM` with retention < 7 days breaks time travel and can cause concurrent reader failures. Never set to 0 in production.

### Delta DML operations

#### INSERT INTO

```sql
-- Append rows
INSERT INTO main.silver.orders VALUES ('o1', 'c1', 100.0, '2024-01-01');

-- Append from SELECT
INSERT INTO main.silver.orders
SELECT order_id, customer_id, amount, order_date FROM staging.orders;

-- Overwrite: replaces ALL data (like overwrite mode)
INSERT OVERWRITE main.silver.orders
SELECT * FROM staging.orders;
```

#### UPDATE

```sql
-- Update matching rows
UPDATE main.silver.orders
SET status = 'cancelled', updated_at = current_timestamp()
WHERE order_id = 'o123';

-- Update with join condition
UPDATE main.silver.orders o
SET o.customer_name = c.full_name
FROM main.silver.customers c
WHERE o.customer_id = c.customer_id;
```

#### DELETE

```sql
-- Delete specific rows
DELETE FROM main.silver.orders WHERE status = 'test';

-- Delete with subquery
DELETE FROM main.silver.orders
WHERE order_id IN (SELECT order_id FROM staging.cancelled_orders);
```

#### MERGE INTO (Upsert)

The most important DML operation for incremental pipelines.

```sql
-- Standard upsert: update existing rows, insert new ones
MERGE INTO main.silver.orders AS target
USING staging.orders_updates AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN
    UPDATE SET
        target.amount     = source.amount,
        target.status     = source.status,
        target.updated_at = current_timestamp()
WHEN NOT MATCHED THEN
    INSERT (order_id, customer_id, amount, status, created_at)
    VALUES (source.order_id, source.customer_id, source.amount, source.status, current_timestamp());

-- Full SCD Type 1 (overwrite): also delete rows not in source
MERGE INTO target t USING source s ON t.id = s.id
WHEN MATCHED     THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;

-- Insert only (skip if key exists)
MERGE INTO target t USING source s ON t.id = s.id
WHEN NOT MATCHED THEN INSERT *;
```

**Key MERGE facts for the exam:**
- `UPDATE SET *` updates ALL columns in target from source
- `INSERT *` inserts ALL columns from source
- `WHEN NOT MATCHED BY SOURCE` is for deleting rows not present in source (SCD Type 1 full sync)
- Multiple `WHEN MATCHED` clauses allowed (with different conditions)

### Liquid Clustering vs Z-Ordering vs Partitioning

| | Partitioning | Z-Ordering | Liquid Clustering |
|--|-------------|------------|-------------------|
| Column cardinality | Low only (< ~1000 values) | Any | Any |
| Change clustering cols | Requires full rewrite | Re-run OPTIMIZE | `ALTER TABLE` (no rewrite) |
| Small file risk | High (high cardinality) | Low | Low |
| Incremental | No | No | **Yes** |
| Query benefit | Partition pruning | Co-location in files | Co-location + pruning |
| Recommended | Legacy | Legacy | **Yes (new tables)** |

```sql
-- Create table with Liquid Clustering
CREATE TABLE main.silver.events CLUSTER BY (event_date, user_id);

-- Add clustering to existing table (no rewrite needed)
ALTER TABLE main.silver.events CLUSTER BY (event_date, region);

-- Apply clustering incrementally (only clusters new/modified files)
OPTIMIZE main.silver.events;

-- Remove clustering
ALTER TABLE main.silver.events CLUSTER BY NONE;
```

### Table constraints

Delta Lake supports table constraints for data quality enforcement at the storage layer.

```sql
-- NOT NULL constraint (enforced on all writes)
ALTER TABLE main.silver.orders ALTER COLUMN order_id SET NOT NULL;

-- CHECK constraint (custom boolean expression)
ALTER TABLE main.silver.orders ADD CONSTRAINT valid_amount CHECK (amount > 0);
ALTER TABLE main.silver.orders ADD CONSTRAINT valid_status CHECK (status IN ('active', 'closed', 'pending'));

-- List constraints
DESCRIBE TABLE EXTENDED main.silver.orders;   -- shows constraints in table properties

-- Drop constraint
ALTER TABLE main.silver.orders DROP CONSTRAINT valid_amount;
```

> Constraints are **enforced on write** — existing violating data is not checked retroactively.

### Delta table cloning

```sql
-- Deep clone: copies data files + metadata; fully independent copy
CREATE TABLE main.dev.orders_copy DEEP CLONE main.silver.orders;

-- Deep clone to a specific version (useful for testing)
CREATE TABLE main.dev.orders_backup DEEP CLONE main.silver.orders VERSION AS OF 10;

-- Shallow clone: copies only metadata; references original data files
-- Reads from original; writes create new files in clone location
CREATE TABLE main.dev.orders_shallow SHALLOW CLONE main.silver.orders;
```

| | Deep clone | Shallow clone |
|--|-----------|---------------|
| Data files | Copied | Referenced (not copied) |
| Storage cost | Higher | Low (metadata only) |
| Independence | Fully independent | Reads from source files |
| Use case | Backup, migration, isolated testing | Quick testing, schema exploration |

### Change Data Feed (CDF)

Change Data Feed records **row-level changes** (inserts, updates, deletes) to a Delta table, enabling downstream consumers to process only what changed.

```sql
-- Enable CDF on a table
ALTER TABLE main.silver.orders
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = true);

-- Create table with CDF enabled from the start
CREATE TABLE main.silver.events
(event_id STRING, amount DOUBLE, status STRING)
TBLPROPERTIES ('delta.enableChangeDataFeed' = true);

-- Read changes between versions
SELECT * FROM table_changes('main.silver.orders', 2, 5);

-- Read changes between timestamps
SELECT * FROM table_changes('main.silver.orders', '2024-01-01', '2024-01-02');
```

**CDF adds metadata columns to the change output:**

| Column | Values |
|--------|--------|
| `_change_type` | `insert`, `update_preimage`, `update_postimage`, `delete` |
| `_commit_version` | Delta version number of the change |
| `_commit_timestamp` | Timestamp of the change |

```python
# Read CDF as a streaming source (incremental CDC processing)
changes = (spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("main.silver.orders"))

# Filter for specific change types
inserts = changes.filter(col("_change_type") == "insert")
updates = changes.filter(col("_change_type") == "update_postimage")
```

> **Use cases:** Propagate incremental changes downstream, audit trail, CDC pipelines without full table scans.

### Delta table properties reference

| Property | Default | Purpose |
|----------|---------|---------|
| `delta.enableChangeDataFeed` | `false` | Enable row-level change tracking |
| `delta.logRetentionDuration` | `interval 30 days` | How long transaction log entries are kept |
| `delta.deletedFileRetentionDuration` | `interval 7 days` | How long deleted files are kept (VACUUM threshold) |
| `delta.enablePredictiveOptimization` | `inherit` | Enable auto OPTIMIZE/VACUUM |
| `delta.minReaderVersion` / `delta.minWriterVersion` | `1` / `2` | Protocol version (higher = more features, less backward compat) |

```sql
-- View all table properties
SHOW TBLPROPERTIES main.silver.orders;

-- Set a specific property
ALTER TABLE main.silver.orders
SET TBLPROPERTIES ('delta.logRetentionDuration' = 'interval 45 days');
```

### `CREATE OR REPLACE TABLE` vs `CREATE TABLE IF NOT EXISTS`

| Statement | Behavior |
|-----------|----------|
| `CREATE TABLE t (...)` | Fails if table exists |
| `CREATE TABLE IF NOT EXISTS t (...)` | Does nothing if table exists (no error, no overwrite) |
| `CREATE OR REPLACE TABLE t (...)` | Drops and recreates table if it exists — **replaces data + schema** |
| `CREATE OR REPLACE TABLE t AS SELECT ...` | Idempotent full refresh — always produces a fresh table |

> **Exam tip:** `CREATE OR REPLACE TABLE` is idempotent and used for Gold layer full refreshes. `CREATE TABLE IF NOT EXISTS` is a no-op guard — it does NOT update the table if it already exists.

### Schema enforcement vs schema evolution

**Schema enforcement (default):** Writes that don't match the table schema fail with `AnalysisException`.

**Schema evolution (opt-in):**
```python
# Append new columns from source DataFrame
df.write.option("mergeSchema", "true").mode("append").saveAsTable("main.silver.orders")

# Overwrite and replace schema entirely
df.write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("main.silver.orders")
```

```sql
-- SQL equivalent
SET spark.databricks.delta.schema.autoMerge.enabled = true;
INSERT INTO main.silver.orders SELECT * FROM staging.orders;
```

---

## 1.3 Unity Catalog

Unity Catalog (UC) is the unified governance layer for all data and AI assets in Databricks.

### Three-level namespace

```
metastore  (one per region per Databricks account)
  └── catalog
        └── schema  (= database)
              ├── table
              ├── view
              ├── materialized view
              ├── streaming table
              ├── volume  (unstructured files — replaces DBFS mounts)
              └── function
```

```sql
-- Fully qualified reference
SELECT * FROM my_catalog.my_schema.my_table;

-- Set default context
USE CATALOG my_catalog;
USE SCHEMA  my_schema;
SELECT * FROM my_table;   -- resolves to my_catalog.my_schema.my_table
```

### UC Volumes

Volumes are Unity Catalog objects for managing **unstructured and semi-structured files** (not Delta tables). They replace DBFS mounts.

| Type | Storage location | Data lifecycle |
|------|-----------------|---------------|
| **Managed volume** | UC-managed storage root | Deleted with volume |
| **External volume** | User-specified cloud path | Not deleted on DROP |

```sql
-- Create managed volume
CREATE VOLUME main.landing.raw_files;

-- Create external volume
CREATE EXTERNAL VOLUME main.landing.raw_files
LOCATION 's3://my-bucket/landing/';

-- Access in code
spark.read.format("json").load("/Volumes/main/landing/raw_files/events/")
dbutils.fs.ls("/Volumes/main/landing/raw_files/")
```

### Key UC features

- **Data lineage:** Column-level and table-level lineage tracked automatically across notebooks, jobs, and queries
- **Audit logs:** All data access events stored in `system.access.audit` (queryable via SQL)
- **Tags:** Add metadata tags to catalogs, schemas, tables, columns for discoverability
- **Delta Sharing:** Share data externally via open protocol without copying data
- **ABAC policies:** Row-level filtering and column masking (see Section 7)

---

## 1.4 Compute Services

### All-Purpose Clusters

**Purpose:** Interactive, multi-user development (notebooks, exploration, ad-hoc analysis).

| Characteristic | Detail |
|----------------|--------|
| Lifecycle | Persist until manually terminated or auto-terminated after idle timeout |
| Sharing | Multiple users can attach notebooks simultaneously |
| Cost model | Billed **while running** — idle time accrues cost |
| Autoscaling | Supported: set min/max workers; scales based on workload |
| Auto-termination | Configurable idle timeout (e.g., 60 minutes); set to avoid runaway costs |

**Cluster configuration options:**

```
Driver type:   Controls driver node (pick larger for driver-heavy workloads)
Worker type:   Controls worker nodes (pick compute/memory-optimized per workload)
Min workers:   Minimum for autoscaling (set > 0 to keep cluster warm)
Max workers:   Maximum for autoscaling
Auto-terminate: Minutes of idle before cluster is terminated
Spark config:  Key=value pairs injected as Spark conf properties
Init scripts:  Shell scripts run during cluster startup (install drivers, set env vars)
Libraries:     Maven/PyPI/DBFS libraries installed on cluster
```

**Limitation:** Shared state can cause interference between users; not suitable for production jobs.

### Job Clusters

**Purpose:** Automated, isolated execution of Lakeflow Jobs tasks.

| Characteristic | Detail |
|----------------|--------|
| Lifecycle | Created at task start → **terminated when task completes** |
| Sharing | Exclusive to the job run (no multi-user sharing) |
| Cost model | Billed **only during job execution** — no idle cost |
| Autoscaling | Supported |
| Configuration | Defined per-task in the job; each task can have its own cluster config |

**Advantage over All-Purpose:** Lower cost and better isolation. Each run starts clean.

### SQL Warehouses

**Purpose:** SQL analytics, BI dashboards, Databricks SQL editor, partner BI tools (Tableau, Power BI).

| Tier | Key difference | Cost model |
|------|---------------|------------|
| **Classic** | Original; shared Spark infrastructure | DBU/hour while running |
| **Pro** | Adds query federation, enhanced caching, Lakeflow Declarative Pipelines via SQL | DBU/hour while running |
| **Serverless** | Instant startup (<5s), fully managed by Databricks, per-query billing | DBU/second of execution only |

**Warehouse sizing:** T-shirt sizes (X-Small → 4X-Large). Larger warehouses run more parallel queries; auto-stop configurable.

**Serverless SQL Warehouse** is preferred when:
- Fast startup required (no warm-up delay)
- Sporadic usage (avoid paying for idle time)
- No infrastructure management desired

### Serverless Compute (Notebooks & Workflows)

**Purpose:** Run notebooks and Lakeflow Jobs tasks without managing cluster infrastructure.

| Characteristic | Detail |
|----------------|--------|
| Startup time | Near-instant (seconds vs minutes) |
| Management | Databricks fully manages sizing and scaling |
| Cost model | Billed per second of **active execution** only |
| DBR version | Automatically kept current by Databricks |
| Limitations | No custom init scripts, no instance pools, no SSH |

**Use when:** Production pipelines where cluster management overhead is undesirable.

### Instance Pools

**Purpose:** Reduce cluster startup latency by maintaining a pool of idle VMs.

- Clusters request instances from the pool instead of provisioning from scratch
- Pool idle instances billed at a **reduced rate** (instance cost only, no DBU)
- Each pool has a configurable idle instance timeout (instances returned to cloud after timeout)
- Useful when many short-lived job clusters start frequently

### Photon Engine

- Vectorized query engine written in C++ — available as a DBR variant (e.g., `15.4.x-photon-scala2.12`)
- **No code changes required** — drop-in acceleration
- Billed at higher DBU rate, but faster execution reduces total cost for SQL-heavy workloads

| Photon accelerates | Photon does NOT accelerate |
|--------------------|---------------------------|
| Spark SQL queries | Python UDFs (row-by-row Python) |
| DataFrame API operations | RDD operations |
| Aggregations, joins, scans | ML model training (scikit-learn, etc.) |
| `MERGE INTO`, `UPDATE`, `DELETE` | Pandas UDFs (Arrow path, not JVM) |
| File scans: Delta, Parquet, CSV, JSON | Arbitrary Python/Scala code |

> **Exam tip:** Photon accelerates **SQL/DataFrame** workloads only. Python UDFs and RDD operations bypass Photon entirely.

### Instance Pool vs Serverless

| | Instance Pool | Serverless Compute |
|--|--------------|-------------------|
| Startup time | Reduced (pre-provisioned VMs) | Near-instant (seconds) |
| Management | You manage pool size and idle timeout | Databricks manages everything |
| Custom config | Full control (init scripts, libraries) | No custom init scripts, no SSH |
| Cost model | Idle instances billed at reduced rate | Billed per second of execution only |
| Best for | Many short-lived job clusters with custom config | Zero-ops production pipelines |

---

## 1.5 Compute Selection Decision Guide

| Scenario | Best compute |
|----------|-------------|
| Data engineer exploring data interactively | All-Purpose cluster |
| Nightly ETL batch job, minimize cost | Job cluster (Serverless preferred) |
| Data analyst running SQL via Databricks SQL | Serverless SQL Warehouse |
| BI tool connecting via JDBC/ODBC | SQL Warehouse (Pro or Serverless) |
| Notebook job with no cluster management | Serverless compute |
| Multiple short jobs, reduce startup time | Instance Pool |
| Complex SQL aggregations, speed up | Photon-enabled cluster |
| Multiple users sharing interactive exploration | All-Purpose with autoscaling |
| Unit testing locally, connecting to remote Spark | Databricks Connect |

---

## 1.6 Common Exam Traps

| Misconception | Correct understanding |
|---------------|----------------------|
| "Serverless = unlimited compute" | Serverless means Databricks manages infrastructure; you still pay per DBU of execution |
| "VACUUM removes old versions for time travel" | VACUUM removes **unreferenced files**; time travel versions are available until VACUUM removes the files they point to |
| "Z-ORDER replaces partitioning" | Z-ORDER and partitioning are complementary; Z-ORDER works **within** partitions |
| "All-Purpose and Job clusters have same cost" | All-Purpose clusters idle cost; Job clusters don't — use Job clusters for production |
| "`OPTIMIZE` applies Liquid Clustering automatically" | Liquid Clustering is applied **only when `OPTIMIZE` is run** — it's not applied on every write |
| "Deep clone and shallow clone behave the same" | Deep clone = independent copy of data; Shallow clone = metadata only, shares source data files |
| "Schema enforcement prevents all bad data" | Schema enforcement checks column names/types but NOT value validity — use constraints for that |
| "CDF is enabled by default" | False — must explicitly set `delta.enableChangeDataFeed = true` |
| "`CREATE TABLE IF NOT EXISTS` updates the table" | False — it's a no-op if the table exists; use `CREATE OR REPLACE TABLE` for idempotent recreation |
| "Photon accelerates Python UDFs" | False — Photon only accelerates SQL/DataFrame operations, not Python UDFs or RDD ops |
