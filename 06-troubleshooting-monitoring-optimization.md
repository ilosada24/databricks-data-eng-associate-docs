# Section 6: Troubleshooting, Monitoring, and Optimization

## Exam Objectives

- Identify trends in job performance using the Lakeflow Jobs run history view to compare current execution times against historical baselines
- Use the Lakeflow Jobs UI to monitor pipeline health by interpreting job statuses, viewing DAG-based task graphs to spot upstream blockers, and tracking pipeline run times and failure rates
- Identify common performance bottlenecks such as data skew, shuffling, and disk spilling by interpreting stage-level metrics in the Spark UI
- Understand the features of Liquid Clustering and predictive optimization
- Diagnose cluster startup failures, library conflicts, and out-of-memory issues

---

## 6.1 Monitoring with the Lakeflow Jobs UI

### Run history view

**Access:** Job → **Runs tab**

Columns visible per run:
- **Start time**, **End time**, **Duration**
- **Status:** `Succeeded` | `Failed` | `Running` | `Skipped` | `Timed out` | `Cancelled`
- **Triggered by:** `Scheduled` | `Manual` | `File arrival` | `Table update` | `Repair`
- Per-task breakdown with individual durations and statuses

**Using run history for performance trending:**

| Signal | What it means |
|--------|--------------|
| Current run duration 2× median | Performance regression — check data volume growth or code change |
| Increasing failure rate on same task | Upstream data schema change, API quota, or external dependency failure |
| Duration spikes correlate with weekdays vs weekends | Load-dependent issue — may need autoscaling adjustment |
| Task skipped when no failure before it | "Run if" condition was false — not an error |
| All tasks skipped | Trigger fired but `max_concurrent_runs=1` constraint prevented a new run |

### DAG task graph view (per run)

**Access:** Runs tab → click a specific run → task graph

Each task node shows:
- Color: green (success), red (failed), yellow (running), gray (skipped), orange (queued)
- Duration
- Compute used (cluster ID or serverless)
- Click node → View logs / View Spark UI

**Identifying the root cause of failures:**
1. Find the **first red node** (failed task without a failed predecessor)
2. All gray (skipped) tasks after it are blocked by that failure — don't investigate them
3. Fix the root cause → use **Repair Run** (not full rerun)

### LDP Pipeline monitoring

**Access:** Pipelines → select pipeline → Pipeline Details

| View | What it shows |
|------|--------------|
| **Event log** | Every pipeline event: update start/end, expectation violations, schema changes, errors |
| **Data quality** | Per-table: records passed, failed (warned), dropped per expectation |
| **Lineage graph** | Visual DAG of data flow from source to output tables |
| **Update history** | Each pipeline run with status, duration, records processed |

**Monitoring expectations in LDP:**
- Click on a table in the pipeline graph → "Data Quality" tab
- Shows: expectation name, pass rate, number of violations
- `@dlt.expect` violations: rows kept but counted as warnings
- `@dlt.expect_or_drop` violations: rows dropped (visible in metrics)
- `@dlt.expect_or_fail` violations: pipeline fails (check event log)

---

## 6.2 Spark UI — Stage-Level Metrics

**Access:** Running cluster → Spark UI button (port 4040), or from a completed job run → "View Spark UI" link.

### Spark UI tabs

| Tab | Key metrics | What to look for |
|-----|------------|-----------------|
| **Jobs** | Duration, # stages, # tasks | Which jobs are slow |
| **Stages** | Duration, shuffle read/write, task duration distribution | Skew, shuffle size, straggler tasks |
| **Tasks** | Per-task metrics: duration, GC time, shuffle read/write, spill | Individual task outliers |
| **SQL / DataFrame** | Query plan (parsed → analyzed → optimized → physical) | Unnecessary scans, missing predicate pushdown |
| **Executors** | Memory used/total, GC time, shuffle read/write bytes | Memory pressure, GC bottlenecks |
| **Storage** | Cached DataFrames/RDDs: fraction cached, size | Cache hit/miss |
| **Streaming** | Micro-batch duration, input rate, processing rate | Streaming lag |

### Understanding the query plan (SQL tab)

```
Physical Plan (bottom to top):
  Exchange (shuffle) ← bad if large
    ├── Sort
    │     └── Filter ← good if early in plan (predicate pushdown)
    │           └── Scan parquet (numOutputRows: 1M) ← scan early = good
    └── BroadcastExchange ← good (avoids shuffle on one side)
          └── Filter
                └── Scan parquet (numOutputRows: 10K) ← small = broadcastable
```

**What to look for in the SQL plan:**
- `Exchange` (shuffle): look for large shuffle sizes → may need broadcast or partition tuning
- Missing `Filter` near `Scan`: predicate pushdown isn't working → check partition columns
- `BroadcastExchange`: good — means broadcast join is being used
- `Sort Merge Join` vs `Broadcast Hash Join`: prefer broadcast for small tables
- `FileScan` with `PartitionFilters`: partition pruning working ✓
- `FileScan` with no `PartitionFilters`: full table scan → may need partition or clustering

### Identifying data skew

**Where to look:** Stages tab → click a stage → task duration histogram

**Symptom:**
```
Task duration distribution (ideally all similar):
  P25:  2s
  P50:  2s
  P75:  3s
  P99:  187s  ← severe skew (one task with massive data)
  Max:  187s
```

**Cause:** Join or groupBy key has highly unequal value distribution (e.g., 80% of rows have `customer_id = 'DEFAULT'`).

**Fixes:**
```python
from pyspark.sql.functions import broadcast, rand, concat, lit, col

# Fix 1: Broadcast the smaller table (eliminates shuffle entirely)
result = large_df.join(broadcast(small_df), on="customer_id")

# Fix 2: AQE skew join handling (automatic, enabled by default)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
# AQE splits skewed partitions and replicates the other side

# Fix 3: Manual salting (for extreme skew that AQE doesn't handle)
salt_n = 10
large_df = large_df.withColumn("salt", (rand() * salt_n).cast("int"))
large_df = large_df.withColumn("salted_key",
    concat(col("customer_id"), lit("_"), col("salt").cast("string")))

small_df = small_df.crossJoin(
    spark.range(salt_n).toDF("salt")
).withColumn("salted_key",
    concat(col("customer_id"), lit("_"), col("salt").cast("string")))

result = large_df.join(small_df, on="salted_key").drop("salt", "salted_key")
```

### Identifying shuffle bottlenecks

**Where to look:** Stages tab → Shuffle Read / Shuffle Write columns

**Symptom:** Stages with very high shuffle read/write (tens of GB+) take a long time.

**Cause:** Wide transformations (joins, `groupBy`, `repartition`, `orderBy`) trigger a shuffle — data moves across the network between executors.

**Fixes:**
```python
# 1. Reduce shuffle partitions for small data (default 200 is too many)
spark.conf.set("spark.sql.shuffle.partitions", "50")

# 2. AQE: automatically coalesce small shuffle partitions
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# 3. Broadcast small lookup tables (no shuffle needed for one side)
result = large_df.join(broadcast(lookup_df), on="id")

# 4. Increase autoBroadcastJoinThreshold if tables are slightly above default 10MB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))

# 5. Pre-partition data if joining repeatedly on the same key
df = df.repartition(200, col("customer_id"))
df.write.partitionBy("customer_id").saveAsTable("main.silver.orders_partitioned")
```

### Identifying disk spilling

**Where to look:** Stages tab → select a stage → Tasks view → Spill (Memory) and Spill (Disk) columns

**Symptom:** Non-zero values in Spill (Disk) column — tasks are writing to disk during shuffle.

**Cause:** Executor heap doesn't have enough memory to hold the shuffle data in-memory.

**Impact:** Disk I/O is 10–100× slower than RAM → stage takes much longer than expected.

**Fixes:**
- Increase `spark.executor.memory` (e.g., from `4g` to `8g`)
- Increase number of workers (smaller shuffle per executor)
- Reduce `spark.sql.shuffle.partitions` (more data per partition but fewer partitions → less overhead)
- Enable AQE (splits large partitions into smaller ones)

---

## 6.3 Adaptive Query Execution (AQE)

AQE re-optimizes query plans **at runtime** using actual data statistics collected during execution. Enabled by default in DBR 7.3+.

### AQE features

| Feature | What it does | Config key |
|---------|-------------|-----------|
| **Coalesce partitions** | Merges small shuffle partitions into larger ones (200 → fewer, right-sized) | `spark.sql.adaptive.coalescePartitions.enabled` |
| **Skew join splitting** | Detects and splits skewed partitions, replicates the other join side | `spark.sql.adaptive.skewJoin.enabled` |
| **Dynamic broadcast** | Converts sort-merge join to broadcast join at runtime if one side is small | `spark.sql.adaptive.localShuffleReader.enabled` |

```python
# Check AQE status
spark.conf.get("spark.sql.adaptive.enabled")  # "true" by default

# All AQE features individually
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

> **Exam tip:** With AQE enabled, you often don't need to manually tune `spark.sql.shuffle.partitions` — AQE will coalesce the 200 default partitions down to the right number. However, setting it lower (e.g., 50) still helps avoid the overhead of creating 200 initial partitions.

---

## 6.4 Liquid Clustering and Predictive Optimization

### Liquid Clustering

Liquid Clustering is Databricks' recommended data layout strategy for Delta tables (replaces partitioning and Z-Ordering for new tables).

**How it works:**
1. When `OPTIMIZE` runs, files are compacted and co-located by clustering column values
2. Only **new or modified files** are clustered (incremental — no full table rewrite)
3. Clustering is stored within the Delta files themselves (not in directory structure)
4. Queries filtering on clustering columns get **file skipping** — Databricks reads only relevant files

```sql
-- Create table with Liquid Clustering
CREATE TABLE main.silver.events CLUSTER BY (event_date, user_id);

-- Change clustering columns without rewriting data
ALTER TABLE main.silver.events CLUSTER BY (event_date, region);

-- Apply clustering (incremental — only touches new/changed files)
OPTIMIZE main.silver.events;

-- Disable clustering
ALTER TABLE main.silver.events CLUSTER BY NONE;

-- Check clustering info
DESCRIBE TABLE EXTENDED main.silver.events;
-- Look for: clusteringColumns in table properties
```

**Liquid Clustering advantages over partitioning:**

| | Partitioning | Liquid Clustering |
|--|-------------|-------------------|
| Column cardinality | Low only (< ~1000 distinct values) | Any (handles high cardinality) |
| Change strategy | Full table rewrite | `ALTER TABLE` + next `OPTIMIZE` |
| Small file problem | Yes (high cardinality) | No (OPTIMIZE compacts) |
| Multiple filter columns | Nested partitions needed | Up to 4 clustering columns |
| Incremental | No | Yes |

**When to use Liquid Clustering:**
- Columns frequently used in `WHERE` filters or `JOIN` conditions
- High-cardinality columns (e.g., `user_id`, `session_id`, `device_id`)
- Query patterns evolve over time (clustering cols can be changed)
- Tables that grow continuously with frequent writes

**Limitations:**
- Clustering is applied by `OPTIMIZE` only — not on every write
- New data written between `OPTIMIZE` runs is unclustered
- Takes multiple `OPTIMIZE` runs to fully cluster a large existing table

### Predictive Optimization

A Databricks-managed service that **automatically runs `OPTIMIZE` and `VACUUM`** on Unity Catalog tables based on usage patterns — eliminating the need to schedule maintenance jobs.

| Property | Detail |
|----------|--------|
| **Scope** | Unity Catalog managed tables only |
| **What it does** | Runs `OPTIMIZE` (with clustering) and `VACUUM` based on access patterns |
| **Intelligence** | Learns table access patterns; schedules maintenance when tables are less active |
| **Cost** | Uses Serverless compute; billed per operation |
| **Control** | Can be enabled/disabled at catalog, schema, or table level |

```sql
-- Enable for a specific table
ALTER TABLE main.silver.orders
SET TBLPROPERTIES ('delta.enablePredictiveOptimization' = 'enable');

-- Enable for all tables in a schema
ALTER SCHEMA main.silver
SET DBPROPERTIES ('delta.enablePredictiveOptimization' = 'enable');

-- Enable for all tables in a catalog
ALTER CATALOG main
SET DBPROPERTIES ('delta.enablePredictiveOptimization' = 'enable');

-- Disable
ALTER TABLE main.silver.orders
SET TBLPROPERTIES ('delta.enablePredictiveOptimization' = 'disable');
```

**With Predictive Optimization enabled:**
- No need to schedule `OPTIMIZE` or `VACUUM` jobs
- Databricks determines optimal timing based on table write frequency and query patterns
- Table is always in good shape without manual intervention

---

## 6.5 Diagnosing Cluster Issues

### Cluster startup failures

**Symptom:** Job fails at "Pending" stage; cluster never reaches "Running" status.

**Where to look:**
- Cluster UI → **Event log** tab (lifecycle events, error messages)
- Cluster UI → **Logging** tab → **Init scripts** (init script stdout/stderr)

| Cause | How to identify | Fix |
|-------|----------------|-----|
| Instance quota exceeded | Event log: "InsufficientCapacity" or quota error | Request quota increase; change instance type |
| Init script failure | Init script log: non-zero exit code, stack trace | Fix init script; test on smaller cluster first |
| Invalid Spark config | Driver logs: "SparkException" or "Configuration error" | Review Spark config syntax in cluster settings |
| VPC/network misconfiguration | Cluster stuck in "Pending" > 10 minutes with no error | Verify VPC, subnets, security groups, NAT gateway |
| Missing cloud permissions | IAM/managed identity error in event log | Grant required storage/cloud permissions to cluster role |

### Library conflicts

**Symptom:** `ImportError`, `ModuleNotFoundError`, `NoClassDefFoundError`, `ClassNotFoundException` in driver or executor logs.

| Type | Cause | Fix |
|------|-------|-----|
| Python dependency conflict | Library A requires `requests==2.28`; Library B requires `requests==2.31` | Pin all dependencies; use notebook-scoped `%pip install` |
| Init script vs cluster library | Library installed in init script conflicts with cluster-attached library | Standardize on one installation mechanism |
| JAR conflict | Two JARs provide the same class | Exclude transitive dependency in Maven config |
| Wrong Python version | Library built for Python 3.9, cluster runs 3.11 | Use DBR with matching Python version |

**Notebook-scoped library management:**
```python
# Install at notebook top — creates isolated environment for this notebook session
%pip install requests==2.31.0 scikit-learn==1.3.0

# Restart Python interpreter to apply new packages
dbutils.library.restartPython()

# Check installed packages
%pip list
```

**Installation precedence (later overrides earlier):**
1. Cluster-attached libraries (lowest priority)
2. Init script installations
3. `%pip install` in notebook (highest priority — session-scoped)

### Out-of-memory (OOM) issues

**Symptom:** `java.lang.OutOfMemoryError: Java heap space`, `OutOfMemoryError: GC overhead limit exceeded`, or `Container killed by YARN for exceeding memory limits`.

**Diagnosis:**

| Location | Symptom in logs | Cause |
|----------|----------------|-------|
| **Executor** | OOM in `Executor` stderr log | Partition too large, large join, shuffle spill |
| **Driver** | OOM in driver log | `collect()` on large DF, large schema introspection |
| **Container** | YARN/container killed message | JVM heap + off-heap exceeds container memory |

```python
# Common OOM trigger — never do this on large datasets
all_rows = large_df.collect()        # brings ALL data to driver RAM — avoid
df_pandas = large_df.toPandas()      # same problem

# Safe alternatives
# Sample only
sample = large_df.sample(0.01).toPandas()          # 1% sample
subset = large_df.limit(10000).toPandas()           # first 10K rows

# Write to table and read back later
large_df.write.saveAsTable("main.debug.result")
```

**Fixes by location:**

| OOM location | Fix |
|-------------|-----|
| Executor OOM | Increase `spark.executor.memory`; increase workers; reduce partition size |
| Driver OOM | Increase `spark.driver.memory`; avoid `collect()` on large DataFrames |
| Disk spill leading to OOM | Increase executor memory; reduce shuffle partitions; broadcast small tables |

---

## 6.6 Delta Cache vs Spark Cache

| | Delta Cache (Databricks Cache) | Spark In-Memory Cache |
|--|-------------------------------|----------------------|
| Storage | SSD on worker nodes | JVM heap memory |
| What's cached | Remote file data (S3/ADLS/GCS) blocks | Entire DataFrame in memory |
| Trigger | Automatic (transparent) | Manual: `df.cache()` / `df.persist()` |
| Survives cluster restart | Yes (SSD persists until recycled) | No (memory cleared) |
| Best for | Repeated reads of the same Delta table | Reusing a small DataFrame multiple times in a job |

```python
# Spark cache — manual
df.cache()             # cache in memory (memory only)
df.persist()           # cache in memory, spill to disk if needed
df.unpersist()         # release cache

# Check if cached
df.is_cached           # True/False

# Force cache materialization
df.cache().count()     # count() forces evaluation and fills cache
```

---

## 6.7 Quick Diagnosis Checklist

| Symptom | Where to look | Likely cause | Fix |
|---------|--------------|-------------|-----|
| Job takes 2× normal duration | Stages tab → task duration histogram | Data skew or shuffle bottleneck | AQE, broadcast join, or salting |
| Tasks show disk spill | Tasks tab → Spill columns | Executor out of memory | Increase executor memory |
| Pipeline fails, rest skipped | Job DAG → first red node | Root cause task failed | Fix root task → Repair Run |
| Cluster never starts | Cluster event log + init script logs | Quota, init script bug, VPC config | Check error message; fix specific cause |
| `ImportError` in notebook | Driver logs / notebook output | Library conflict | Reinstall with `%pip install` |
| Driver OOM | Driver logs | `collect()` on large DataFrame | Remove `collect()`; write to table instead |
| Executor OOM | Executor logs (Stages tab → task details) | Large partition or large join | Increase memory; increase shuffle partitions |
| Slow shuffle joins | SQL tab → query plan (look for Exchange) | Missing broadcast or too many partitions | Broadcast small table; tune shuffle partitions |
| Query not using partitions | SQL tab → FileScan (no PartitionFilters) | Predicate pushdown not working | Filter on partition column directly (not after cast) |
| LDP pipeline failing | Pipeline event log | Schema change, expectation failure, source unavailable | Check event log for specific error |
