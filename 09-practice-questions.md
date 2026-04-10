# Section 9: Practice Questions

> 35 multiple-choice questions covering all 7 exam domains, with emphasis on new topics and common traps.

---

## Domain 1: Databricks Intelligence Platform (Q1–Q5)

### Q1
A data engineer needs to query a Delta table as it existed 3 days ago. Which statement is correct?

- A. `SELECT * FROM main.silver.orders TIMESTAMP AS OF '2024-06-01'`
- B. `SELECT * FROM main.silver.orders WHERE _timestamp = '2024-06-01'`
- C. `SELECT * FROM main.silver.orders@v3`
- D. `SELECT * FROM main.silver.orders VERSION AS OF current_date() - 3`

**Answer: A**
Time travel uses `TIMESTAMP AS OF` or `VERSION AS OF` syntax. Option B uses a nonexistent column. Option C is not valid syntax. Option D is not valid — `VERSION AS OF` takes an integer literal, not an expression.

---

### Q2
A team wants to track row-level changes in a Delta table so downstream consumers can process only inserts and updates. What must they enable?

- A. Delta Sharing
- B. Change Data Feed (CDF)
- C. Liquid Clustering
- D. Predictive Optimization

**Answer: B**
Change Data Feed (`delta.enableChangeDataFeed = true`) records `_change_type` (insert, update_preimage, update_postimage, delete) for each row modification, readable via `table_changes()`.

---

### Q3
A data engineer runs `VACUUM main.silver.orders RETAIN 168 HOURS`. Which files are removed?

- A. All files older than 168 hours
- B. All files not referenced by the current table version
- C. Unreferenced files older than 168 hours
- D. Transaction log files older than 168 hours

**Answer: C**
VACUUM removes data files that are both unreferenced by the current table version AND older than the retention threshold. It does NOT touch the transaction log (that's controlled by `delta.logRetentionDuration`).

---

### Q4
Which statement about `CREATE TABLE IF NOT EXISTS` is correct?

- A. It replaces the existing table with a new empty table
- B. It updates the schema of the existing table if it differs
- C. It does nothing if the table already exists
- D. It appends data to the existing table

**Answer: C**
`CREATE TABLE IF NOT EXISTS` is a no-op guard — it silently does nothing if the table exists. To replace, use `CREATE OR REPLACE TABLE`.

---

### Q5
A cluster is using Photon Runtime. Which operation will NOT benefit from Photon acceleration?

- A. `SELECT SUM(amount) FROM orders GROUP BY region`
- B. `MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET *`
- C. A Python UDF applied via `df.withColumn("result", my_udf(col("value")))`
- D. `SELECT * FROM orders WHERE order_date > '2024-01-01'`

**Answer: C**
Python UDFs execute row-by-row in the Python interpreter, bypassing the Photon engine entirely. SQL queries, DataFrame operations, and MERGE INTO are all Photon-accelerated.

---

## Domain 2: Data Ingestion and Loading (Q6–Q10)

### Q6
An Auto Loader stream encounters a source file with a new column not in the schema. The stream is configured with `cloudFiles.schemaEvolutionMode = "rescue"`. What happens?

- A. The stream fails and must be restarted
- B. The new column is added to the target table schema
- C. The unknown column data is stored in a `_rescued_data` column
- D. The row is dropped silently

**Answer: C**
In `rescue` mode, data from unknown columns (or values that can't be cast) is stored as a JSON fragment in the `_rescued_data` column. The stream continues without interruption.

---

### Q7
A data engineer wants to process all backlogged files in cloud storage using Auto Loader, then stop the stream. Which trigger should they use?

- A. `trigger(once=True)`
- B. `trigger(processingTime="0 seconds")`
- C. `trigger(availableNow=True)`
- D. `trigger(continuous="1 second")`

**Answer: C**
`availableNow=True` processes all available data in multiple micro-batches (more efficient for large backlogs), then stops. `once=True` is legacy and processes in a single micro-batch. `availableNow` is preferred.

---

### Q8
Which output mode is required when writing a streaming aggregation (groupBy + count) to a Delta table?

- A. `append`
- B. `complete`
- C. `update`
- D. Both B and C are valid

**Answer: D**
Streaming aggregations cannot use `append` mode because previous output rows may change. Both `complete` (rewrites entire result) and `update` (writes only changed rows) are valid for aggregations.

---

### Q9
A data engineer configures a `foreachBatch` function to MERGE streaming data into a Silver table. What advantage does `foreachBatch` provide over a standard `writeStream.table()` call?

- A. It enables exactly-once delivery without a checkpoint
- B. It allows applying arbitrary batch operations (like MERGE) to each micro-batch
- C. It eliminates the need for a Delta Lake target
- D. It processes data faster than the default streaming sink

**Answer: B**
`foreachBatch` gives access to each micro-batch as a regular DataFrame, enabling operations like MERGE, multi-table writes, or JDBC inserts that aren't possible with standard streaming sinks.

---

### Q10
A team needs to ingest CDC data from an Oracle database into Unity Catalog tables without writing custom code. Which ingestion method is most appropriate?

- A. Auto Loader with file notification mode
- B. COPY INTO with force=true
- C. Lakeflow Connect managed connector
- D. JDBC read in a scheduled notebook

**Answer: C**
Lakeflow Connect managed connectors handle enterprise database CDC (Oracle, SAP, PostgreSQL) with no custom code, including schema evolution and offset management.

---

## Domain 3: Data Transformation and Modeling (Q11–Q17)

### Q11
A data engineer writes `df1.union(df2)` in PySpark. Which SQL statement produces the same result?

- A. `SELECT * FROM df1 UNION SELECT * FROM df2`
- B. `SELECT * FROM df1 UNION ALL SELECT * FROM df2`
- C. `SELECT * FROM df1 INTERSECT SELECT * FROM df2`
- D. `SELECT * FROM df1 EXCEPT SELECT * FROM df2`

**Answer: B**
PySpark `.union()` keeps all rows including duplicates — equivalent to SQL `UNION ALL`. SQL `UNION` (without ALL) deduplicates.

---

### Q12
Which function type offers the best performance for custom transformation logic in Databricks?

- A. Python UDF
- B. Pandas UDF
- C. SQL UDF
- D. Lambda function in `.filter()`

**Answer: C**
SQL UDFs run natively in the JVM, are optimized by Spark's catalyst optimizer, and are accelerated by Photon. Python UDFs are slowest (row-by-row serialization). Pandas UDFs are faster than Python UDFs but still not JVM-native.

---

### Q13
A query uses a global temporary view. Which syntax correctly references it?

- A. `SELECT * FROM my_global_view`
- B. `SELECT * FROM temp.my_global_view`
- C. `SELECT * FROM global_temp.my_global_view`
- D. `SELECT * FROM system.my_global_view`

**Answer: C**
Global temporary views are stored in the `global_temp` database and must be referenced with that prefix. Using just the view name will fail.

---

### Q14
A data engineer wants to reduce the number of output files from 200 to 10 before writing a small DataFrame to a Delta table. Which approach avoids a shuffle?

- A. `df.repartition(10).write.saveAsTable("target")`
- B. `df.coalesce(10).write.saveAsTable("target")`
- C. `df.repartition(10, "id").write.saveAsTable("target")`
- D. `spark.conf.set("spark.sql.shuffle.partitions", "10")`

**Answer: B**
`coalesce(n)` is a narrow transformation — it reduces partitions by combining them without a shuffle. `repartition()` triggers a full shuffle. Setting `shuffle.partitions` only affects subsequent shuffles, not the current DataFrame's partition count.

---

### Q15
Which is the correct syntax for a PIVOT operation in SQL?

- A. `SELECT * FROM sales PIVOT (region FOR SUM(revenue))`
- B. `SELECT * FROM sales PIVOT (SUM(revenue) FOR region IN ('US', 'EU'))`
- C. `SELECT * FROM (SELECT region, product, revenue FROM sales) PIVOT (SUM(revenue) FOR region IN ('US', 'EU'))`
- D. `SELECT * FROM sales GROUP BY PIVOT(region)`

**Answer: C**
PIVOT requires: (1) a subquery selecting only the needed columns, (2) an aggregate function, (3) `FOR column IN (values)`. The subquery is necessary to limit which columns participate in the pivot.

---

### Q16
A data engineer needs to implement SCD Type 2 for a customer dimension. Which key characteristic distinguishes SCD Type 2 from SCD Type 1?

- A. SCD Type 2 uses MERGE INTO while SCD Type 1 uses INSERT OVERWRITE
- B. SCD Type 2 adds a new row for each change, preserving history with `is_current` and date columns
- C. SCD Type 2 only tracks inserts, not updates
- D. SCD Type 2 requires Change Data Feed to be enabled

**Answer: B**
SCD Type 2 preserves history by inserting new rows and marking old rows as inactive (`is_current = false`, `end_date`). SCD Type 1 overwrites in place, losing history.

---

### Q17
A CTE (Common Table Expression) in Spark SQL:

- A. Caches the intermediate result for reuse
- B. Creates a temporary view visible to other notebooks
- C. Is syntactic sugar — the query is inlined during optimization
- D. Requires the RECURSIVE keyword

**Answer: C**
CTEs in Spark SQL are syntactic sugar. The query planner inlines them — there is no caching or materialization. Spark SQL does not support recursive CTEs.

---

## Domain 4: Working with Lakeflow Jobs (Q18–Q22)

### Q18
A Lakeflow Job has 4 tasks: A → B → C → D. Task B fails. What is the status of tasks C and D?

- A. Failed
- B. Cancelled
- C. Skipped
- D. Queued

**Answer: C**
When a task fails, all downstream dependent tasks are marked as **Skipped** (not failed or cancelled). The root cause is the first failed task.

---

### Q19
A data engineer uses Repair Run on a failed job. Which tasks are re-executed?

- A. All tasks in the job
- B. Only the failed task
- C. The failed task and all downstream dependent tasks
- D. Only the tasks that were skipped

**Answer: C**
Repair Run re-executes the failed task and all its downstream dependents. Successfully completed tasks are not re-run.

---

### Q20
A pipeline must start processing as soon as a new file lands in S3, not on a fixed schedule. Which trigger type should be configured?

- A. Scheduled with a 1-minute cron interval
- B. File arrival trigger
- C. Table update trigger
- D. Manual trigger with API polling

**Answer: B**
File arrival triggers fire when new files are detected in a monitored cloud storage path — ideal for event-driven processing. A 1-minute cron wastes compute if no files arrive.

---

### Q21
A notebook in a Lakeflow Job needs to pass a computed value to a downstream task. Which API should the notebook use?

- A. `dbutils.widgets.set("key", value)`
- B. `dbutils.notebook.exit(value)`
- C. `dbutils.jobs.taskValues.set(key="key", value=value)`
- D. `spark.conf.set("key", str(value))`

**Answer: C**
`dbutils.jobs.taskValues.set()` stores a value that downstream tasks can read with `dbutils.jobs.taskValues.get()`. `dbutils.notebook.exit()` returns a value to a `dbutils.notebook.run()` caller, not to other job tasks.

---

### Q22
A data engineer wants to execute a helper notebook that defines shared functions and variables in the current notebook's scope. Which method should they use?

- A. `dbutils.notebook.run("./helpers", 300)`
- B. `%run ./helpers`
- C. `import helpers`
- D. `spark.sql("RUN NOTEBOOK './helpers'")`

**Answer: B**
`%run` executes the notebook in the **same scope** — all variables and functions become available in the calling notebook. `dbutils.notebook.run()` executes in an isolated scope and only returns a string.

---

## Domain 5: Implementing CI/CD (Q23–Q26)

### Q23
A data engineer runs `databricks bundle validate -t prod`. What does this command do?

- A. Deploys the bundle to the prod workspace
- B. Runs all tests defined in the bundle
- C. Validates the YAML configuration for the prod target without deploying
- D. Creates a pull request for the prod deployment

**Answer: C**
`databricks bundle validate` parses and validates the configuration — it checks for YAML syntax errors, missing references, and variable resolution. It does NOT deploy anything.

---

### Q24
In a Declarative Automation Bundle, what is the effect of setting `mode: development` on a target?

- A. Resources are deployed with smaller cluster sizes
- B. Resources are prefixed with the developer's username to prevent naming conflicts
- C. Resources are deployed to a sandbox workspace
- D. Schedules are permanently disabled

**Answer: B**
`mode: development` prefixes resource names with `[dev ${username}]`, allowing multiple developers to deploy without overwriting each other's resources. Schedules are paused by default but can be manually triggered.

---

### Q25
A CI/CD pipeline needs to promote the same codebase from staging to production, with different catalog names and cluster sizes. What DAB feature enables this?

- A. Git branches (one branch per environment)
- B. Bundle `variables` with per-target overrides
- C. Separate `databricks.yml` files per environment
- D. Manual editing of resource names before deploy

**Answer: B**
Variables defined at the bundle level with defaults can be overridden per target (dev/staging/prod). This enables a single codebase to deploy to multiple environments with different configurations.

---

### Q26
Which authentication method is recommended for CI/CD pipelines deploying Databricks bundles?

- A. Personal access tokens (PATs) stored in the bundle YAML
- B. OAuth browser login
- C. Service principal with token stored in CI/CD secret management
- D. Shared user account credentials

**Answer: C**
Service principals (machine identities) are the recommended authentication for CI/CD. Tokens should be stored in CI/CD secret management (GitHub Secrets, Azure Key Vault), never in YAML or code.

---

## Domain 6: Troubleshooting, Monitoring & Optimization (Q27–Q30)

### Q27
A Spark job stage shows the following task duration distribution: P50 = 3s, P99 = 180s. What does this indicate?

- A. The cluster is undersized
- B. Network latency between executors
- C. Data skew — one or more partitions have significantly more data
- D. Disk I/O failure on one node

**Answer: C**
When P99 is dramatically higher than P50 (60× in this case), it means a small number of tasks are processing much more data than others — the classic sign of data skew. Fixes include AQE skew join, broadcast join, or salting.

---

### Q28
An executor repeatedly fails with `java.lang.OutOfMemoryError: Java heap space`. The error occurs during a shuffle join. Which fix is most appropriate?

- A. Increase `spark.driver.memory`
- B. Increase `spark.executor.memory`
- C. Add more `%pip install` memory packages
- D. Switch to a SQL Warehouse

**Answer: B**
Executor OOM during shuffle means executors don't have enough heap to hold shuffle data. Increasing `spark.executor.memory` directly addresses this. Driver memory only helps if the OOM is on the driver side (e.g., `collect()`).

---

### Q29
Predictive Optimization is enabled for a schema. Which maintenance operations does it handle automatically?

- A. VACUUM and ANALYZE TABLE
- B. OPTIMIZE (with Liquid Clustering) and VACUUM
- C. RESTORE and OPTIMIZE
- D. DESCRIBE HISTORY and OPTIMIZE

**Answer: B**
Predictive Optimization automatically runs `OPTIMIZE` (including Liquid Clustering if configured) and `VACUUM` based on table access patterns. No manual scheduling needed.

---

### Q30
A data engineer sees non-zero values in the "Spill (Disk)" column of the Spark UI Tasks view. What does this indicate?

- A. Corrupt data on disk
- B. Executor memory is insufficient — shuffle data is being written to disk
- C. The Delta transaction log is too large
- D. Checkpoint files are being written

**Answer: B**
Disk spill means executor memory couldn't hold all the shuffle data in RAM, so some was written to disk. This is 10-100x slower than in-memory processing. Fix by increasing executor memory, adding workers, or broadcasting small tables.

---

## Domain 7: Governance and Security (Q31–Q35)

### Q31
A data engineer drops a managed table using `DROP TABLE main.silver.orders`. What happens?

- A. Only the table metadata is removed; data files remain
- B. Both metadata and data files are permanently deleted
- C. The table is moved to a recycle bin for 30 days
- D. The table becomes an external table at the original location

**Answer: B**
Dropping a managed table permanently deletes both the metadata in Unity Catalog AND the underlying data files in cloud storage. This is irreversible.

---

### Q32
A user `alice@company.com` belongs to the `analysts` group. The following grants exist:
```sql
GRANT SELECT ON TABLE main.silver.orders TO analysts;
DENY SELECT ON TABLE main.silver.orders TO `alice@company.com`;
```
Can Alice query `main.silver.orders`?

- A. Yes — the group GRANT overrides the individual DENY
- B. No — DENY always overrides GRANT, even from group membership
- C. Yes — only REVOKE can remove access, not DENY
- D. It depends on the order the statements were executed

**Answer: B**
DENY explicitly blocks a privilege and overrides all GRANTs, including those inherited from group membership. DENY is the strongest access control statement.

---

### Q33
Which combination of privileges is the minimum required for a group to read all tables in `main.silver`?

- A. `GRANT SELECT ON SCHEMA main.silver`
- B. `GRANT USE CATALOG ON CATALOG main` + `GRANT SELECT ON SCHEMA main.silver`
- C. `GRANT USE CATALOG ON CATALOG main` + `GRANT USE SCHEMA ON SCHEMA main.silver` + `GRANT SELECT ON SCHEMA main.silver`
- D. `GRANT ALL PRIVILEGES ON CATALOG main`

**Answer: C**
Reading tables requires three grants: `USE CATALOG` (to navigate into the catalog), `USE SCHEMA` (to navigate into the schema), and `SELECT` (to read the data). Missing any one of these blocks access.

---

### Q34
A column masking function uses `is_member('pii_admin')`. When is this function evaluated?

- A. Once when the mask is applied via ALTER TABLE
- B. At query time, for each query that accesses the masked column
- C. Once per day during a scheduled security scan
- D. Only when the table owner queries the table

**Answer: B**
`is_member()` is evaluated fresh at query time for every query that touches the masked column. This ensures access decisions reflect current group membership.

---

### Q35
Which statement about Unity Catalog row-level security (row filters) is true?

- A. Row filters can be bypassed by reading the table directly with Spark
- B. Row filters only apply to SQL Warehouse queries, not notebook queries
- C. Row filters are enforced at the UC layer for all access paths — SQL, Spark, BI tools
- D. Row filters require enabling Delta Sharing on the table

**Answer: C**
UC-enforced row filters apply regardless of access method — notebooks, SQL Warehouses, Spark DataFrames, JDBC/ODBC, and partner BI tools. They cannot be bypassed.

---

## Scoring Guide

| Score | Interpretation |
|-------|---------------|
| 32–35 correct | Exam-ready — strong across all domains |
| 28–31 correct | Good foundation — review missed domains |
| 24–27 correct | Needs more study — focus on weak areas |
| < 24 correct | Review all sections before attempting the exam |

---

## Official Databricks Documentation Links

- [Databricks Certified Data Engineer Associate — Exam Guide](https://www.databricks.com/learn/certification/data-engineer-associate)
- [Databricks Free Training & Academy](https://www.databricks.com/learn/training)
- [Databricks Practice Exams](https://www.databricks.com/learn/certification/data-engineer-associate#practice)
- [Databricks Documentation Home](https://docs.databricks.com/)
- [Databricks Community Edition (Free Tier)](https://community.cloud.databricks.com/)
