# Section 4: Working with Lakeflow Jobs

## Exam Objectives

- Implement control flows (retries and conditional tasks such as branching and looping) using Lakeflow Jobs for pipeline orchestration
- Configure common tasks (notebook, SQL query, dashboard, pipeline tasks) and their dependencies using Lakeflow Jobs and its DAG-based task graph
- Implement job schedules using Lakeflow Jobs with an understanding of trigger types: scheduled, file arrival, and table update
- Choose between time-based and data-driven triggers based on data availability and pipeline dependencies

---

## 4.1 Lakeflow Jobs Overview

Lakeflow Jobs (formerly Databricks Workflows) is the native orchestration service for data pipelines on Databricks. A **job** is a collection of **tasks** connected in a **DAG (Directed Acyclic Graph)**.

```
Job: Daily ETL Pipeline
  ├── Task A: ingest_bronze (notebook)  ← no dependency, runs first
  │     └── depends_on: none
  ├── Task B: silver_orders (notebook)  ← depends on A
  │     └── depends_on: Task A
  ├── Task C: silver_customers (notebook) ← depends on A (runs in parallel with B)
  │     └── depends_on: Task A
  └── Task D: gold_revenue (SQL query)  ← depends on B and C
        └── depends_on: Task B, Task C
```

Each task can:
- Use different compute (job cluster, serverless, existing cluster)
- Have its own retry policy and timeout
- Pass values to downstream tasks
- Run in parallel with other tasks when dependencies allow

---

## 4.2 Task Types

| Task type | What it runs | Notes |
|-----------|-------------|-------|
| **Notebook** | A Databricks notebook (Python, SQL, Scala, R) | Most common; supports parameters, widgets |
| **SQL query** | A saved Databricks SQL query or file | Runs on SQL Warehouse |
| **Dashboard** | Refresh a Databricks AI/BI dashboard | Regenerates dashboard data |
| **Pipeline** | A Lakeflow Spark Declarative Pipeline (LDP) | Full/incremental refresh |
| **Python script** | A `.py` file from Repos/workspace/DBFS | No notebook overhead |
| **dbt** | dbt Cloud job or dbt Core project | SQL transformations |
| **Spark JAR** | Compiled Spark application JAR | Legacy/Java/Scala apps |
| **Spark Python (`.py`)** | Python file run as spark-submit | Standalone Spark app |

### Compute per task

Each task can independently specify:
```
Job cluster (new_cluster):   Created fresh, terminated on completion — preferred for cost
Existing cluster:            Connects to a running all-purpose cluster — faster, but idle cost
Serverless:                  Zero cluster management, instant startup — recommended
SQL Warehouse:               For SQL query / dashboard tasks
```

### Notebook task parameters

Parameters are passed to notebooks via **base_parameters** and read with `dbutils.widgets`:

```yaml
# In job YAML / bundle
- task_key: transform_silver
  notebook_task:
    notebook_path: /Repos/main/silver_transform
    base_parameters:
      catalog: "main"
      date:    "2024-01-15"
      env:     "prod"
```

```python
# In the notebook
catalog = dbutils.widgets.get("catalog")
date    = dbutils.widgets.get("date")
```

---

## 4.3 DAG-Based Task Graph

Tasks are wired together using **"Depends on"** relationships. Lakeflow Jobs executes tasks in topological order, running independent tasks in parallel automatically.

```
ingest_bronze ──────┬──► silver_orders    ──┐
                    │                        ├──► gold_revenue ──► refresh_dashboard
                    └──► silver_customers ──┘
```

### Execution behavior

- Tasks with no dependencies: run immediately (in parallel if multiple)
- A task runs only after **all** its dependencies complete successfully (by default)
- A failed task causes all downstream dependent tasks to be **Skipped**
- Skipped tasks are NOT the same as failed tasks — job result depends on whether required tasks failed

### Dependency in YAML (Declarative Automation Bundle)

```yaml
resources:
  jobs:
    daily_etl:
      name: Daily ETL Pipeline
      tasks:
        - task_key: ingest_bronze
          notebook_task:
            notebook_path: ./src/bronze_ingest.py
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            num_workers: 2

        - task_key: silver_orders
          depends_on:
            - task_key: ingest_bronze
          notebook_task:
            notebook_path: ./src/silver_orders.py
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            num_workers: 2

        - task_key: gold_revenue
          depends_on:
            - task_key: silver_orders
            - task_key: silver_customers   # waits for BOTH
          sql_task:
            query:
              query_id: "abc-query-id"
            warehouse_id: "xyz-warehouse-id"
```

### "Run if" conditions

Tasks can be configured to run only if a specific upstream task has a specific outcome:

| Run if condition | Description |
|-----------------|-------------|
| `ALL_SUCCESS` (default) | Run only if all dependencies succeeded |
| `ALL_DONE` | Run regardless of dependency outcome (succeeded, failed, or skipped) |
| `AT_LEAST_ONE_SUCCESS` | Run if at least one dependency succeeded |
| `ALL_FAILED` | Run only if all dependencies failed |
| `AT_LEAST_ONE_FAILED` | Run if at least one dependency failed |

```yaml
- task_key: send_failure_alert
  depends_on:
    - task_key: transform_silver
      outcome: "FAILED"   # only run if transform_silver failed
  notebook_task:
    notebook_path: ./src/alert.py
```

---

## 4.4 Control Flow: Retries and Conditional Tasks

### Retry policy

```yaml
- task_key: call_external_api
  notebook_task:
    notebook_path: ./src/api_call.py
  retry_on_timeout: true
  max_retries: 3                          # retry up to 3 times on failure
  min_retry_interval_millis: 60000        # wait 60s between retries
  timeout_seconds: 300                    # fail task after 5 minutes
```

**When retries help:**
- Transient network failures
- Rate-limited external APIs
- Cloud resource provisioning delays

**When retries don't help:**
- Bugs in code (will fail every time)
- Schema mismatch (deterministic failure)

### Conditional tasks (if/else branching)

An **if/else task** evaluates a boolean expression and routes execution to either the true or false branch.

```
check_row_count (notebook)
        │
    [if/else task: "{{tasks.check_row_count.values.row_count}} > 0"]
        │                           │
   True branch                 False branch
   process_data (notebook)     send_alert (notebook)
```

**Setting task values in a notebook:**
```python
# Produce a value that can be read by downstream tasks/if-else conditions
row_count = spark.sql("SELECT COUNT(*) FROM main.bronze.events").collect()[0][0]
dbutils.jobs.taskValues.set(key="row_count", value=row_count)
dbutils.jobs.taskValues.set(key="status", value="success")
```

**Reading task values in a downstream notebook:**
```python
row_count = dbutils.jobs.taskValues.get(
    taskKey="check_row_count",  # upstream task key
    key="row_count",            # value key set by upstream
    default=0,                  # fallback if key not found
    debugValue=100              # value to use when running notebook interactively
)
```

**If/else condition syntax:**
```
# Numeric comparison
{{tasks.check_row_count.values.row_count}} > 0

# String comparison
{{tasks.validate.values.status}} == "success"

# Boolean
{{tasks.check.values.is_valid}} == true
```

### Looping (ForEach task)

A **ForEach task** iterates over a list of inputs and runs a child task (or nested task cluster) once per item.

```yaml
- task_key: process_each_region
  for_each_task:
    inputs: '["US", "EU", "APAC", "LATAM"]'  # static list as JSON array
    concurrency: 3                             # run up to 3 iterations in parallel
    task:
      task_key: process_region_child
      notebook_task:
        notebook_path: ./src/process_region.py
        base_parameters:
          region: "{{input}}"         # {{input}} = current iteration value
```

**Dynamic inputs from upstream task:**
```yaml
for_each_task:
  inputs: "{{tasks.get_regions.values.regions}}"  # JSON array from task value
  concurrency: 2
```

**In the child notebook:**
```python
region = dbutils.widgets.get("region")  # receives current {{input}} value
print(f"Processing region: {region}")
```

---

## 4.5 Trigger Types

### Scheduled (time-based)

Fires on a cron expression — suitable when data arrives at predictable times.

**Quartz cron format:** `<second> <minute> <hour> <day-of-month> <month> <day-of-week>`

```
0 0 2 * * ?    → Every day at 02:00 UTC
0 0 * * * ?    → Every hour at :00
0 0/15 * * * ? → Every 15 minutes
0 0 2 ? * MON  → Every Monday at 02:00
0 0 2 1 * ?    → 1st of every month at 02:00
```

**Choose scheduled when:**
- Data arrives on a known schedule (nightly ETL, hourly batch)
- Downstream consumers expect fresh data at a specific time
- SLA is time-based ("data must be ready by 8 AM")

### File arrival (data-driven)

Fires when a **new file lands** in a monitored cloud storage path.

```
Monitored path: s3://my-bucket/landing/events/
Glob pattern:   *.json
Wait period:    60 seconds (debounce — waits to batch multiple arriving files)
```

**Behavior:**
- Scans the path periodically for new files
- Runs the job when new files are detected
- Does NOT trigger for files already present when trigger was created

**Choose file arrival when:**
- Upstream producers write files to cloud storage asynchronously
- Processing should start as soon as data is available
- Processing time varies and cron would waste compute

### Table update (data-driven)

Fires when a **monitored Delta table receives a new commit** (write operation).

```
Monitored table: main.silver.orders
Trigger fires:   When a new version is written to the table
```

**Choose table update when:**
- Upstream job writes to a Delta table and downstream job should start immediately after
- Fan-out pattern: multiple jobs watching one central table
- Cross-team pipeline chaining without tight coupling

### Manual

On-demand only. Triggered via UI button, API call, or CLI. No automatic scheduling.

---

## 4.6 Time-Based vs Data-Driven Trigger Decision Guide

| Scenario | Recommended trigger |
|----------|-------------------|
| "Ingest data every night at midnight" | **Scheduled** (`0 0 0 * * ?`) |
| "Process files as soon as they arrive in S3" | **File arrival** |
| "Start downstream job when upstream writes to silver table" | **Table update** |
| "Process data only on weekdays" | **Scheduled** (with day-of-week filter) |
| "Data arrives unpredictably, minimize latency" | **File arrival** or **Table update** |
| "Guarantee processing at a fixed time regardless of data availability" | **Scheduled** |
| "Trigger downstream pipeline only when upstream job succeeds" | **Task dependency within same job** |
| "Ad-hoc backfill or testing" | **Manual** |

---

## 4.7 Job-Level Configuration

### Timeouts

```yaml
resources:
  jobs:
    my_job:
      timeout_seconds: 7200    # 2-hour job-level timeout (kills the entire job)
      tasks:
        - task_key: slow_task
          timeout_seconds: 1800  # 30-minute task-level timeout
```

### Notifications

Configure email or webhook alerts for job events:

```yaml
resources:
  jobs:
    my_job:
      email_notifications:
        on_failure:
          - data-team@company.com
          - oncall@company.com
        on_success:
          - data-team@company.com
        on_start:
          - monitoring@company.com
      notification_settings:
        no_alert_for_skipped_runs: true   # don't alert if run was skipped
```

### Maximum concurrent runs

```yaml
resources:
  jobs:
    my_job:
      max_concurrent_runs: 1   # don't start a new run if previous is still running
```

### Job access control (permissions)

| Permission | What it allows |
|------------|---------------|
| **Can View** | See job definition and run history |
| **Can Manage Run** | Trigger runs, cancel runs, repair runs |
| **Can Manage** | Edit job definition, change settings, delete job |
| **Is Owner** | Full control including permission management |

---

## 4.8 Monitoring and Repair Runs

### Viewing run history

**Job UI → Runs tab** shows:
- Start time, end time, duration for each run
- Status: `Succeeded`, `Failed`, `Running`, `Skipped`, `Timed out`, `Cancelled`
- Triggered by: scheduled, manual, file arrival, table update
- Per-task breakdown with individual durations and status

**Identifying performance trends:**
- Compare current run duration vs historical median
- A 2× increase signals a performance regression
- Recurring failure on the same task → check for upstream data changes, OOM, or API quota

### Viewing the DAG task graph

Click on a specific run → task graph view:

```
[ingest_bronze ✓ 2m]
    │
    ├──[silver_orders ✗ FAILED 5m]
    │         └──[gold_revenue ⊘ SKIPPED]
    │
    └──[silver_customers ✓ 3m]
              └──[gold_revenue ⊘ SKIPPED — upstream failed]
```

**Spotting upstream blockers:**
- Failed task → all downstream tasks show as `SKIPPED`
- Find the **first red (failed) node** — that's the root cause
- Fix root cause, then use **Repair Run** (not full rerun)

### Repairing a failed run

**Repair Run** re-executes only the failed (and downstream) tasks — skips successfully completed tasks.

1. Job UI → Runs → select the failed run → **Repair run**
2. Optionally select which specific tasks to rerun
3. Databricks uses the **same run ID** — history stays consolidated

**Why repair instead of full rerun:**
- Saves time — skips completed expensive tasks
- Preserves task output values from succeeded tasks
- Maintains clean run history (one run ID per pipeline execution attempt)

---

## 4.9 Lakeflow Spark Declarative Pipelines (LDP) as a Task

An LDP pipeline runs as a task within a Lakeflow Job.

```yaml
- task_key: run_bronze_to_silver_pipeline
  pipeline_task:
    pipeline_id: "abc-pipeline-id"
    full_refresh: false   # incremental (default): process only new data
    # full_refresh: true  # reprocess ALL data from scratch
```

**Full refresh vs incremental:**

| Mode | Behavior | Use case |
|------|----------|----------|
| `full_refresh: false` | Process only new data since last run | Normal daily/hourly runs |
| `full_refresh: true` | Drop and reprocess all data from scratch | Schema change, data corruption fix, initial load |

---

## 4.10 Notebook Orchestration: %run vs dbutils.notebook.run()

Two ways to call one notebook from another — different behavior, frequently tested.

### `%run` — static inclusion

```python
# Call another notebook (like a Python import)
%run ./helpers/common_functions

# After %run, all variables/functions defined in that notebook are available here
result = my_helper_function(data)   # defined in common_functions notebook
```

### `dbutils.notebook.run()` — dynamic execution

```python
# Run a notebook in an isolated scope; returns a string result
result = dbutils.notebook.run(
    "./transform_silver",        # notebook path
    timeout_seconds=300,         # max execution time
    arguments={"catalog": "main", "date": "2024-01-15"}  # parameters
)
print(result)  # string returned by dbutils.notebook.exit() in the child

# In the child notebook:
dbutils.notebook.exit("success")   # return a value to the caller
```

### Comparison

| | `%run` | `dbutils.notebook.run()` |
|--|--------|--------------------------|
| Execution scope | **Shared** — variables flow into calling notebook | **Isolated** — separate scope, no variable sharing |
| Return value | None (shared scope, so variables are just available) | String (via `dbutils.notebook.exit()`) |
| Parameters | No — uses shared scope | Yes — `arguments` dict (read with `dbutils.widgets.get()`) |
| Path resolution | Relative to current notebook | Relative to current notebook or absolute |
| Error handling | Failure stops the calling notebook | Raises exception; can be caught with try/except |
| Use case | Shared utilities, constants, imports | Modular pipeline steps, dynamic orchestration |

> **Exam tip:** `%run` is like `import` — shares scope. `dbutils.notebook.run()` is like calling a subprocess — isolated, returns a string, accepts parameters.

---

## 4.11 Common Exam Traps

| Trap | Correct answer |
|------|---------------|
| "A failed task causes downstream tasks to fail" | False — downstream tasks are **Skipped**, not failed |
| "Repair Run re-runs the entire job" | False — it only re-runs failed and downstream tasks |
| "Table update trigger fires for any table change" | Only for Delta tables with new commits (writes) |
| "Max concurrent runs = 1 means jobs queue up" | False — when a run is in progress, new triggered runs are **skipped** (not queued) unless configured otherwise |
| "Task values are shared across all tasks in the job" | True, but a task must explicitly call `dbutils.jobs.taskValues.set()` to share values |
| "ForEach `concurrency: 1` = sequential processing" | True — concurrency=1 processes one item at a time |
| "File arrival trigger processes all existing files" | False — only files that arrive AFTER the trigger is activated |
| "`%run` passes parameters to the called notebook" | False — `%run` shares scope; use `dbutils.notebook.run()` for parameters |
| "`dbutils.notebook.run()` shares variables with the caller" | False — it runs in an isolated scope; only returns a string via `dbutils.notebook.exit()` |
