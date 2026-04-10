# Databricks Certified Data Engineer Associate — Study Roadmap

> **Exam format:** 45 multiple-choice questions | 90 minutes | $200 | No external aids
> **Passing:** ~70% (approximately 32/45)
> **Materials:** Two study guide directories + practice questions available in this project

---

## Overview

This roadmap is designed for **4 weeks of part-time study (~8–10 hours/week)**, totalling approximately **35 hours**. Each week blends theory reading with hands-on labs in Azure Databricks. The schedule prioritizes the highest-weighted domains first and leaves the final week for review and practice exams.

---

## Week 1 — Platform Foundations & Data Ingestion (~10 hours)

### Domain 1: Databricks Intelligence Platform (~20% of exam)

**Read (3h):**
- `databricks-data-eng-associate-docs/01-databricks-intelligence-platform.md`
- Cross-reference with `databricks-certified-associate/study-guide/01-databricks-intelligence-platform.md`

**Key topics to master:**
- Delta Lake: ACID transactions, transaction log, time travel (`VERSION AS OF`, `TIMESTAMP AS OF`), `RESTORE`
- `OPTIMIZE`, `VACUUM` (default 168h retention, cannot vacuum below retention)
- DML: `INSERT`, `UPDATE`, `DELETE`, `MERGE INTO`
- Liquid Clustering vs legacy partitioning/Z-ORDER
- Schema enforcement vs schema evolution (`mergeSchema`, `overwriteSchema`)
- Deep vs Shallow clones
- Unity Catalog: three-level namespace (`catalog.schema.table`)
- Compute types: All-Purpose clusters, Job clusters, SQL Warehouses, Serverless, Photon

**Hands-on in Azure Databricks (2h):**
- [ ] Create a managed Delta table, run `DESCRIBE HISTORY`, query by version/timestamp, then `RESTORE`
- [ ] Run `OPTIMIZE` with and without `ZORDER`, observe file compaction
- [ ] Set `VACUUM` retention to 0 hours (requires overriding safety check) and observe behavior
- [ ] Create a `MERGE INTO` statement (upsert pattern)
- [ ] Enable Liquid Clustering on a table and compare with partitioned table
- [ ] Explore Unity Catalog: create catalog > schema > table hierarchy, run `SHOW GRANTS`

---

### Domain 2: Data Ingestion & Loading (~20% of exam)

**Read (2.5h):**
- `databricks-data-eng-associate-docs/02-data-ingestion-and-loading.md`
- Cross-reference with `databricks-certified-associate/study-guide/02-data-ingestion-and-loading.md`

**Key topics to master:**
- Auto Loader: `cloudFiles` format, directory listing vs file notification mode
- Schema evolution modes: `addNewColumns`, `rescue`, `failOnNewColumns`, `none`
- `_rescued_data` column behavior
- Trigger modes: `trigger(availableNow=True)` vs `trigger(once=True)` vs continuous
- `COPY INTO`: idempotent loading, `VALIDATE` mode, `force` option
- Lakeflow Declarative Pipelines (LDP): streaming tables vs materialized views
- LDP expectations: `CONSTRAINT ... EXPECT`, `ON VIOLATION DROP ROW / FAIL UPDATE`
- Structured Streaming output modes: append, complete, update
- Watermarks for late-arriving data

**Hands-on in Azure Databricks (2.5h):**
- [ ] Upload sample CSV/JSON files to a Volume, ingest with Auto Loader using `cloudFiles`
- [ ] Test schema evolution: add a new column to source files and observe `addNewColumns` behavior
- [ ] Use `COPY INTO` to load data, then re-run to confirm idempotency
- [ ] Build a simple Lakeflow Declarative Pipeline with a streaming table and a materialized view
- [ ] Add an expectation (`ON VIOLATION DROP ROW`) and observe the pipeline quality metrics
- [ ] Write a Structured Streaming query with `trigger(availableNow=True)` and checkpoint

---

## Week 2 — Transformations, Jobs & Orchestration (~10 hours)

### Domain 3: Data Transformation & Modeling (~18% of exam)

**Read (2.5h):**
- `databricks-data-eng-associate-docs/03-data-transformation-and-modeling.md`
- Cross-reference with `databricks-certified-associate/study-guide/03-data-transformation-and-modeling.md`

**Key topics to master:**
- Medallion architecture: Bronze (raw) > Silver (cleaned) > Gold (aggregated)
- PySpark: joins (inner, left, anti, semi), `groupBy`, `agg`, window functions
- `union` vs `unionByName`, deduplication with `dropDuplicates`
- UDFs: Python UDFs (row-at-a-time, serialization overhead), Pandas UDFs (vectorized, Arrow-based)
- SQL: CTEs, `PIVOT`, higher-order functions (`TRANSFORM`, `FILTER`, `AGGREGATE`)
- SCD Type 1 vs Type 2 patterns
- Gold layer objects: views, materialized views, `CREATE TABLE AS SELECT`
- `coalesce()` vs `repartition()`
- Adaptive Query Execution (AQE): auto coalesce, skew join handling, dynamic broadcast

**Hands-on in Azure Databricks (3.5h):**
- [ ] Build a Bronze > Silver > Gold pipeline using notebooks
- [ ] Practice joins: inner, left outer, left anti, left semi — verify row counts
- [ ] Create a Python UDF and a Pandas UDF; compare execution times on a large dataset
- [ ] Write a `PIVOT` query and a higher-order function (`TRANSFORM`)
- [ ] Implement an SCD Type 2 using `MERGE INTO` with surrogate keys and effective dates
- [ ] Use `coalesce(1)` to write a single output file; use `repartition(10)` and observe shuffle
- [ ] Enable AQE (`spark.sql.adaptive.enabled = true`) and examine query plans with `.explain(mode="formatted")`

---

### Domain 4: Working with Lakeflow Jobs (~12% of exam)

**Read (1.5h):**
- `databricks-data-eng-associate-docs/04-working-with-lakeflow-jobs.md`
- Cross-reference with `databricks-certified-associate/study-guide/04-working-with-lakeflow-jobs.md`

**Key topics to master:**
- DAG-based task graphs: task dependencies and execution order
- Task types: notebook, SQL, pipeline, Python script, dbt
- Control flow: retries, `if/else` conditions, `ForEach` loops, task values (`dbutils.jobs.taskValues`)
- Trigger types: scheduled (cron), file arrival, table update, manual
- Repair runs: re-run only failed and dependent tasks
- `%run` (static, same context) vs `dbutils.notebook.run()` (separate context, returns string)
- Job-level config: timeouts, concurrent run policies, notifications

**Hands-on in Azure Databricks (2.5h):**
- [ ] Create a multi-task Job with a DAG: task A > task B + task C (parallel) > task D
- [ ] Use `dbutils.jobs.taskValues.set()` in one task and `.get()` in a downstream task
- [ ] Configure a scheduled trigger with a cron expression
- [ ] Intentionally fail a task, then use **Repair Run** to re-execute only the failed branch
- [ ] Compare `%run ./other_notebook` vs `dbutils.notebook.run("./other_notebook", 60)`
- [ ] Set up file arrival trigger on a Volume path (if available)

---

## Week 3 — CI/CD, Monitoring, Governance & Utilities (~10 hours)

### Domain 5: Implementing CI/CD (~10% of exam)

**Read (1.5h):**
- `databricks-data-eng-associate-docs/05-implementing-cicd.md`
- Cross-reference with `databricks-certified-associate/study-guide/05-implementing-cicd.md`

**Key topics to master:**
- Databricks Repos (Git Folders): clone, branch, commit, push, pull, PR workflow
- Databricks Asset Bundles (DAB): `databricks.yml` structure, variables, targets (dev/staging/prod)
- `mode: development` vs `mode: production` behavior
- Variable substitution: `${var.x}`, `${workspace.current_user.short_name}`
- Databricks CLI: `databricks bundle validate`, `deploy`, `run`, `destroy`
- CI/CD pipeline pattern: PR > validate > deploy staging > test > deploy prod
- Service principal authentication for automation

**Hands-on in Azure Databricks (1.5h):**
- [ ] Connect a Databricks Repo to a Git repository (GitHub/Azure DevOps)
- [ ] Create a branch, make changes, commit and push from the workspace
- [ ] Install the Databricks CLI locally, configure authentication with a PAT
- [ ] Create a minimal `databricks.yml` with one job resource and two targets (dev, prod)
- [ ] Run `databricks bundle validate` and `databricks bundle deploy -t dev`

---

### Domain 6: Troubleshooting, Monitoring & Optimization (~10% of exam)

**Read (1.5h):**
- `databricks-data-eng-associate-docs/06-troubleshooting-monitoring-optimization.md`
- Cross-reference with `databricks-certified-associate/study-guide/06-troubleshooting-monitoring-optimization.md`

**Key topics to master:**
- Lakeflow Jobs UI: run history, DAG visualization, performance trending
- Spark UI: stages tab, task metrics, shuffle read/write, spill (memory/disk)
- Identifying data skew: one task much slower than others, uneven partition sizes
- Skew fixes: broadcast join, AQE skew join, salting
- Shuffle bottlenecks: too many/few partitions, `spark.sql.shuffle.partitions`
- Disk spilling: symptoms (spill metrics > 0), causes (insufficient memory), fixes (more memory, fewer partitions, broadcast)
- `.explain()` modes: `simple`, `extended`, `codegen`, `cost`, `formatted`
- Delta Cache vs Spark Cache
- Predictive Optimization: auto `OPTIMIZE` and `VACUUM` for UC managed tables
- Liquid Clustering: incremental, no need for manual `OPTIMIZE ZORDER`

**Hands-on in Azure Databricks (2h):**
- [ ] Run a large join and examine the Spark UI: stages, tasks, shuffle read/write
- [ ] Force data skew (load a dataset where one key has 90% of rows), observe the slow task in Spark UI
- [ ] Fix skew with a broadcast join hint: `/*+ BROADCAST(small_table) */`
- [ ] Run `.explain(mode="formatted")` on a query and identify `BroadcastExchange` vs `SortMergeJoin`
- [ ] Enable Predictive Optimization on a UC managed table and check `system.storage.predictive_optimization_operations_history`

---

### Domain 7: Governance & Security (~10% of exam)

**Read (1.5h):**
- `databricks-data-eng-associate-docs/07-governance-and-security.md`
- Cross-reference with `databricks-certified-associate/study-guide/07-governance-and-security.md`

**Key topics to master:**
- Unity Catalog hierarchy: metastore > catalog > schema > table/view/volume/function
- Managed vs External tables: `DROP` behavior (managed deletes data, external keeps data)
- `GRANT`, `REVOKE`, `DENY` — **DENY always overrides GRANT** (even from group membership)
- Privilege inheritance: catalog > schema > table
- Key roles: account admin, metastore admin, catalog/schema/table owner
- Column-level masking: `ALTER TABLE ... ALTER COLUMN ... SET MASK function_name`
- Row-level security: `ALTER TABLE ... SET ROW FILTER function_name ON (columns)`
- ABAC policies: attribute-based access control, enforcement at all access paths
- Data lineage: automatic tracking, `system.access.table_lineage`
- Volumes: managed vs external, used for non-tabular files

**Hands-on in Azure Databricks (2h):**
- [ ] Create managed and external tables, `DROP` both, verify which data persists
- [ ] Create users/groups in Unity Catalog, `GRANT SELECT` to a group, `DENY` to a specific user in that group — confirm DENY wins
- [ ] Create a masking function that redacts a column for non-admin users using `is_member()`
- [ ] Create a row filter function and apply it to a table
- [ ] Query `system.access.audit` and `system.access.table_lineage` (if available in your workspace)

---

### Bonus — dbutils & Notebook Features

**Read (0.5h):**
- `databricks-data-eng-associate-docs/08-dbutils-and-notebook-features.md`

**Key topics (appears across multiple domains):**
- `dbutils.widgets`: text, dropdown, combobox, multiselect — `getArgument()` for job parameters
- `dbutils.fs`: `ls`, `cp`, `mv`, `rm` — works with Volumes paths (`/Volumes/...`)
- `dbutils.secrets`: `get()`, `list()`, `listScopes()` — values always `[REDACTED]` in output
- `dbutils.notebook.run()` vs `%run`
- Magic commands: `%sql`, `%python`, `%pip install` (session-scoped), `%sh` (driver only)
- `display()` (Databricks-specific) vs `.show()` (Spark native)

---

## Week 4 — Review, Practice Exams & Gap Filling (~8 hours)

### Day 1–2: Exam Traps Review (2h)

- [ ] Re-read the "Common Exam Traps" section at the end of each domain chapter
- [ ] Study the "Top 30 Exam Traps" in `databricks-data-eng-associate-docs/00-index.md`
- [ ] Study the "Top 20 Exam Traps" in `databricks-certified-associate/study-guide/00-index.md`
- [ ] Review all quick-reference tables in both index files

### Day 3: Practice Exam — First Pass (2h)

- [ ] Take the 35-question practice exam: `databricks-data-eng-associate-docs/09-practice-questions.md`
- [ ] Simulate exam conditions: set a 70-minute timer (matching the 2 min/question pace)
- [ ] Score yourself and identify weak domains

**Scoring guide:**
| Score | Assessment |
|-------|------------|
| < 24/35 | Review all domains, focus on fundamentals |
| 24–27 | Needs targeted study on weak areas |
| 28–31 | Good foundation, refine edge cases |
| 32–35 | Exam-ready |

### Day 4–5: Targeted Gap Filling (2.5h)

- [ ] For each question you got wrong, go back to the relevant domain chapter
- [ ] Re-do the hands-on lab for that domain
- [ ] Create flashcards or notes for tricky distinctions:
  - `trigger(once=True)` vs `trigger(availableNow=True)`
  - `VACUUM` retention rules and safety checks
  - `COPY INTO` vs Auto Loader decision criteria
  - Managed vs External table DROP behavior
  - `DENY` overrides `GRANT`
  - `%run` vs `dbutils.notebook.run()`
  - `mode: development` vs `mode: production` in DABs
  - AQE features: coalesce, skew join, dynamic broadcast

### Day 6: Practice Exam — Second Pass (1h)

- [ ] Retake the practice exam (or attempt questions in a different order)
- [ ] Target: 32+/35

### Day 7: Final Review (0.5h)

- [ ] Quick scan of all quick-reference tables
- [ ] Review any remaining weak areas
- [ ] Rest and prepare for exam day

---

## Study Calendar Summary

| Week | Focus | Hours | Domains |
|------|-------|-------|---------|
| **1** | Platform + Ingestion | ~10h | 1, 2 |
| **2** | Transformations + Jobs | ~10h | 3, 4 |
| **3** | CI/CD + Monitoring + Governance + dbutils | ~10h | 5, 6, 7, 8 |
| **4** | Review + Practice Exams + Gap Filling | ~8h | All |
| **Total** | | **~38h** | |

---

## Exam Day Tips

1. **Time management:** 2 minutes per question — flag difficult ones and come back
2. **Read all options** before selecting — traps often hide in subtle wording
3. **Watch for absolutes:** "always", "never", "only" — these are often wrong
4. **Delta Lake and Unity Catalog** are the backbone — if unsure, think about how these systems enforce correctness
5. **Code snippets in questions:** read carefully for syntax errors, wrong function names, or missing parameters
