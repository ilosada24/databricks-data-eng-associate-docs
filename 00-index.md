# Databricks Certified Data Engineer Associate — Study Guide

> **This guide covers the NEW exam version live on May 4, 2026 and after.**

**Format:** 45 multiple-choice questions · 90 minutes · USD 200 · No test aides  
**Validity:** 2 years · Recertification: retake the current live exam

---

## Exam Domains

| # | Domain | Key topics |
|---|--------|-----------|
| 1 | [Databricks Intelligence Platform](./01-databricks-intelligence-platform.md) | Delta Lake, UC, compute types, DBR, Liquid Clustering, DML, cloning |
| 2 | [Data Ingestion and Loading](./02-data-ingestion-and-loading.md) | Auto Loader, COPY INTO, LDP, Lakeflow Connect, JDBC, schema evolution |
| 3 | [Data Transformation and Modeling](./03-data-transformation-and-modeling.md) | PySpark joins/aggregations, UDFs, Spark tuning, AQE, Gold layer objects |
| 4 | [Working with Lakeflow Jobs](./04-working-with-lakeflow-jobs.md) | DAG tasks, triggers, control flow, task values, repair runs |
| 5 | [Implementing CI/CD](./05-implementing-cicd.md) | Databricks Repos, Declarative Automation Bundles, Databricks CLI |
| 6 | [Troubleshooting, Monitoring & Optimization](./06-troubleshooting-monitoring-optimization.md) | Spark UI, AQE, Liquid Clustering, predictive optimization, OOM/library failures |
| 7 | [Governance and Security](./07-governance-and-security.md) | Managed/external tables, GRANT/REVOKE/DENY, column masking, RLS, ABAC, lineage |

---

## New vs Old Exam — Key Differences (May 2026)

| Removed / de-emphasized | Added / new focus |
|------------------------|-------------------|
| Delta Sharing | Lakeflow Connect (standard + managed connectors) |
| Lakehouse Federation | COPY INTO explicitly in scope |
| DAB vs "traditional deployment" framing | Declarative Automation Bundles + full CI/CD domain |
| Spark UI as optimization only | Dedicated Troubleshooting & Monitoring domain |
| DDL/DML as standalone section | Column masking, row-level security, ABAC policies |
| — | Spark tuning parameters explicitly tested |
| — | Gold layer: materialized views, streaming tables, views |
| — | Data-driven triggers: file arrival, table update |
| — | AQE, Liquid Clustering, Predictive Optimization |
| — | Data lineage (system.access tables) |

---

## Master Quick-Reference

### Ingestion method selection

| Method | Volume | Source type | Schema evolution | Code required |
|--------|--------|-------------|-----------------|---------------|
| **Auto Loader** | Millions of files | Cloud object storage | Yes (cloudFiles) | PySpark |
| **COPY INTO** | Thousands of files | Cloud object storage | Limited | SQL only |
| **LDP (streaming table)** | High | Cloud storage, Kafka | Yes | Python/SQL (dlt) |
| **Lakeflow Connect (standard)** | Med–High | SaaS apps | Yes | None (UI config) |
| **Lakeflow Connect (managed)** | High | Enterprise DBs, CDC | Yes | None (UI config) |
| **JDBC in notebook** | Small–Med | Any JDBC database | Manual | PySpark |

### Compute type selection

| Scenario | Best choice |
|----------|------------|
| Interactive notebook exploration | All-Purpose cluster |
| Scheduled production job, lowest cost | Job cluster (or Serverless) |
| SQL analytics / BI dashboards | Serverless SQL Warehouse |
| No cluster management, fast startup | Serverless compute |
| Reduce cluster startup time for many short jobs | Instance Pool |
| Accelerate SQL-heavy workloads | Photon-enabled cluster |
| Local IDE connecting to Databricks Spark | Databricks Connect |

### Lakeflow Jobs trigger types

| Trigger | Fires when |
|---------|-----------|
| **Scheduled** | On time-based cron expression |
| **File arrival** | New file lands in a monitored cloud storage path |
| **Table update** | A monitored Delta table receives a new commit (write) |
| **Manual** | On-demand only |

### Spark tuning parameters

| Parameter | Default | What to change |
|-----------|---------|----------------|
| `spark.sql.shuffle.partitions` | **200** | Lower for small data (e.g., 50); raise for huge shuffles |
| `spark.default.parallelism` | 2× cores | Match total executor core count |
| `spark.executor.memory` | `1g` | Increase on executor OOM errors |
| `spark.driver.memory` | `1g` | Increase when collecting large DataFrames |
| `spark.sql.autoBroadcastJoinThreshold` | **10MB** | Increase to broadcast more lookup tables |
| `spark.sql.adaptive.enabled` | `true` | Leave enabled — AQE auto-tunes shuffle partitions |

### Unity Catalog privilege hierarchy

```
Metastore (admin controls)
  └─ CATALOG  → USE CATALOG  (required for any access)
       └─ SCHEMA   → USE SCHEMA   (required for table access)
            ├─ TABLE   → SELECT, MODIFY, ALL PRIVILEGES
            ├─ VIEW    → SELECT
            ├─ VOLUME  → READ VOLUME, WRITE VOLUME
            └─ FUNCTION → EXECUTE
```

```sql
-- Minimum read-only access pattern
GRANT USE CATALOG ON CATALOG my_catalog TO my_group;
GRANT USE SCHEMA  ON SCHEMA  my_catalog.my_schema TO my_group;
GRANT SELECT      ON SCHEMA  my_catalog.my_schema TO my_group;  -- all tables

-- Deny overrides grant (even from group membership)
DENY MODIFY ON TABLE my_catalog.my_schema.sensitive TO restricted_user;
```

### DML quick reference

```sql
-- Append rows
INSERT INTO table VALUES (...);
INSERT INTO table SELECT ... FROM source;

-- Update rows
UPDATE table SET col = val WHERE condition;

-- Delete rows
DELETE FROM table WHERE condition;

-- Upsert (most important for incremental pipelines)
MERGE INTO target t USING source s ON t.id = s.id
WHEN MATCHED     THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Replace all data
INSERT OVERWRITE table SELECT * FROM source;
```

### Auto Loader schema evolution modes

| Mode | On new column in source |
|------|------------------------|
| `addNewColumns` (default) | Add column, keep row |
| `rescue` | Save unknown data to `_rescued_data` column |
| `failOnNewColumns` | Fail the stream |
| `none` | Silently ignore new columns |

### Managed vs external table DROP behavior

| Table type | `DROP TABLE` result |
|------------|---------------------|
| Managed | Deletes **metadata + data files** (permanent!) |
| External | Deletes **metadata only** (data at LOCATION survives) |

### LDP expectations behavior

| Expectation | On violation |
|-------------|-------------|
| `@dlt.expect` / `EXPECT` | Keep row, record warning metric |
| `@dlt.expect_or_drop` / `EXPECT ... ON VIOLATION DROP ROW` | Drop row |
| `@dlt.expect_or_fail` / `EXPECT ... ON VIOLATION FAIL UPDATE` | Fail pipeline |

---

## Sample Question Answers (from PDF)

| Q | Answer | Rule |
|---|--------|------|
| 1 | **D** | `count_distinct("billing_id")` = unique invoices; `sum("amount_billed")` = total revenue |
| 2 | **B** | `GRANT SELECT ON SCHEMA` = read-only on all tables in schema |
| 3 | **A** | Delta Share grants external partners READ; internal teams get READ/WRITE via UC directly |
| 4 | **A** | `CREATE OR REPLACE TABLE` always creates or fully replaces (correct DDL for idempotent creation) |
| 5 | **D** | `INSERT INTO my_table VALUES (...)` is correct DML syntax to append a record |

---

## Top 20 Exam Traps

| # | Trap | Correct answer |
|---|------|---------------|
| 1 | "PySpark `.union()` deduplicates" | No — use `.distinct()` after |
| 2 | "SQL UNION ALL deduplicates" | No — SQL UNION deduplicates; UNION ALL keeps all |
| 3 | "DROP TABLE on external table deletes data" | No — only metadata deleted |
| 4 | "DROP TABLE on managed table is reversible" | No — data files permanently deleted |
| 5 | "REVOKE = DENY" | No — REVOKE removes grant; DENY actively blocks |
| 6 | "Auto Loader can read Delta files with `cloudFiles`" | No — use `format("delta")` for Delta source |
| 7 | "COPY INTO re-loads files on second run" | No — idempotent; use `force=true` to re-load |
| 8 | "AQE must be enabled manually" | No — on by default since DBR 7.3 |
| 9 | "Python UDFs are faster than SQL UDFs" | No — SQL UDFs are faster (Photon, JVM-native) |
| 10 | "Liquid Clustering is applied on every write" | No — only when `OPTIMIZE` runs |
| 11 | "A failed task causes downstream tasks to fail" | No — downstream tasks are Skipped, not failed |
| 12 | "Repair Run re-runs the entire job" | No — only failed and downstream tasks |
| 13 | "`databricks bundle validate` deploys the bundle" | No — validate only; deploy requires `bundle deploy` |
| 14 | "Serverless compute = unlimited, free scaling" | No — billed per second of execution |
| 15 | "Row filters can be bypassed with direct Spark reads" | No — UC enforces filters on all access paths |
| 16 | "VACUUM removes old versions for time travel" | VACUUM removes unreferenced files older than retention |
| 17 | "`availableNow=True` = `once=True`" | Similar but `availableNow` uses multiple micro-batches |
| 18 | "Deep clone and shallow clone both copy data" | No — shallow clone copies metadata only |
| 19 | "`GRANT SELECT ON SCHEMA` covers views too" | Yes — views in the schema are included |
| 20 | "File notification mode requires no cloud setup" | No — needs EventBridge/Event Grid/Pub/Sub configured |
