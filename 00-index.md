# Databricks Certified Data Engineer Associate — Study Guide

> **This guide covers the NEW exam version live on May 4, 2026 and after.**

**Format:** 45 multiple-choice questions · 90 minutes · USD 200 · No test aides  
**Validity:** 2 years · Recertification: retake the current live exam

---

## Exam-Day Strategy

| Tip | Detail |
|-----|--------|
| **Time budget** | 45 questions / 90 min = **2 minutes per question** — flag and move on if stuck |
| **First pass** | Answer everything you're confident about (~60-70% of questions) |
| **Second pass** | Return to flagged questions with remaining time |
| **Elimination** | Rule out 1-2 wrong options first — the exam loves plausible-sounding distractors |
| **"Best" questions** | When asked "which is the BEST approach", look for the most specific/efficient option |
| **Trap words** | Watch for "always", "never", "only", "all" — these are often in wrong answers |
| **Read all options** | Don't pick the first right-looking answer — there may be a better one |

---

## Exam Domains (Approximate Weights)

| # | Domain | ~Weight | Key topics |
|---|--------|---------|-----------|
| 1 | [Databricks Intelligence Platform](./01-databricks-intelligence-platform.md) | ~20% | Delta Lake, UC, compute types, DBR, Liquid Clustering, DML, cloning, CDF |
| 2 | [Data Ingestion and Loading](./02-data-ingestion-and-loading.md) | ~20% | Auto Loader, COPY INTO, LDP, Lakeflow Connect, JDBC, schema evolution, streaming |
| 3 | [Data Transformation and Modeling](./03-data-transformation-and-modeling.md) | ~18% | PySpark joins/aggregations, UDFs, Spark tuning, AQE, Gold layer, CTEs, PIVOT, SCD |
| 4 | [Working with Lakeflow Jobs](./04-working-with-lakeflow-jobs.md) | ~12% | DAG tasks, triggers, control flow, task values, repair runs, notebook orchestration |
| 5 | [Implementing CI/CD](./05-implementing-cicd.md) | ~10% | Databricks Repos, Declarative Automation Bundles, Databricks CLI |
| 6 | [Troubleshooting, Monitoring & Optimization](./06-troubleshooting-monitoring-optimization.md) | ~10% | Spark UI, AQE, Liquid Clustering, predictive optimization, OOM/library failures |
| 7 | [Governance and Security](./07-governance-and-security.md) | ~10% | Managed/external tables, GRANT/REVOKE/DENY, column masking, RLS, ABAC, lineage |

**Supplementary sections:**
| # | Section | Content |
|---|---------|---------|
| 8 | [dbutils and Notebook Features](./08-dbutils-and-notebook-features.md) | Widgets, file system, secrets, magic commands, display() |
| 9 | [Practice Questions](./09-practice-questions.md) | 35 multiple-choice questions with answers (5 per domain) |

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

### Streaming output modes

| Mode | Behavior | Use with |
|------|----------|----------|
| `append` | Only new rows (default) | Append-only sources, no aggregations |
| `complete` | Full result rewritten each batch | Aggregations (groupBy) |
| `update` | Only changed rows | Aggregations (more efficient than complete) |

### Temp view vs Global temp view

| | Temp view | Global temp view |
|--|-----------|-----------------|
| Scope | Session (one notebook) | Cluster-wide |
| Access | Direct name | `global_temp.` prefix required |
| Survives restart | No | No |

### coalesce() vs repartition()

| | `coalesce(n)` | `repartition(n)` |
|--|--------------|------------------|
| Shuffle | No (narrow) | Yes (wide) |
| Direction | Decrease only | Increase or decrease |
| Balance | Uneven | Even |

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
| 21 | "Global temp views are accessed by name directly" | No — must use `global_temp.view_name` prefix |
| 22 | "`coalesce()` can increase partitions" | No — `coalesce()` only decreases; `repartition()` for increase |
| 23 | "CTEs cache their intermediate results" | No — CTEs are syntactic sugar, inlined by optimizer |
| 24 | "CDF is enabled by default on Delta tables" | No — must set `delta.enableChangeDataFeed = true` |
| 25 | "`%run` passes parameters to the called notebook" | No — `%run` shares scope; use `dbutils.notebook.run()` for params |
| 26 | "`CREATE TABLE IF NOT EXISTS` updates existing tables" | No — it's a no-op if the table exists |
| 27 | "Photon accelerates Python UDFs" | No — Photon only accelerates SQL/DataFrame ops |
| 28 | "`dbutils.notebook.exit()` can return any data type" | No — only returns a string |
| 29 | "`%pip install` affects all notebooks on the cluster" | No — it's session-scoped (one notebook) |
| 30 | "`append` output mode works with streaming aggregations" | No — aggregations need `complete` or `update` mode |

---

## Official Databricks Documentation Links

- [Databricks Certified Data Engineer Associate — Exam Guide](https://www.databricks.com/learn/certification/data-engineer-associate)
- [Databricks Documentation Home](https://docs.databricks.com/)
- [Delta Lake Documentation](https://docs.databricks.com/delta/index.html)
- [Unity Catalog Overview](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Structured Streaming](https://docs.databricks.com/structured-streaming/index.html)
- [Lakeflow Declarative Pipelines](https://docs.databricks.com/workflows/delta-live-tables/index.html)
- [Lakeflow Jobs](https://docs.databricks.com/workflows/jobs/jobs.html)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
- [Databricks Repos (Git Folders)](https://docs.databricks.com/repos/index.html)
- [Spark UI and Debugging](https://docs.databricks.com/clusters/spark-ui.html)
- [Compute Configuration](https://docs.databricks.com/clusters/index.html)
- [Data Governance with Unity Catalog](https://docs.databricks.com/data-governance/index.html)
- [dbutils Reference](https://docs.databricks.com/dev-tools/databricks-utils.html)
- [Databricks SQL](https://docs.databricks.com/sql/index.html)
- [Databricks Free Training & Academy](https://www.databricks.com/learn/training)
