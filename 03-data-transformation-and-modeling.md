# Section 3: Data Transformation and Modeling

## Exam Objectives

- Implement data cleaning by reading bronze tables with PySpark/SQL, cleaning nulls, standardizing data types, and writing to new silver tables
- Combine DataFrames with operations such as inner join, left join, broadcast join, multiple keys, cross join, union, and union all
- Manipulate columns, rows, and table structures by adding, dropping, splitting, renaming column names, applying filters, and exploding arrays
- Perform data deduplication operations and aggregate operations on DataFrames: count, approximate count distinct, and mean, summary
- Understand the basic tuning parameters and re-measure performance
- Understand the difference between, and how to build, Gold layer objects: materialized views, views, streaming tables, and tables for BI and analytics teams in Unity Catalog
- Apply data quality checks and validation rules to ensure reliable Silver and Gold datasets

---

## 3.1 Medallion Architecture

```
[Bronze]             [Silver]                  [Gold]
Raw ingestion   →   Cleaned / validated   →   Aggregated / business-ready
─────────────────────────────────────────────────────────────────────
As-is from source   Nulls handled             Joins, aggregations
Append only         Types standardized         Materialized views
Full fidelity       Deduplication              Served to BI tools
Schema-on-read      Schema enforced            Streaming tables
```

| Layer | Purpose | Write mode | Schema enforcement |
|-------|---------|------------|--------------------|
| **Bronze** | Raw data, full fidelity, no transformation | Append | Schema-on-read or inferred |
| **Silver** | Cleaned, deduplicated, type-standardized | Append or MERGE | Enforced |
| **Gold** | Business aggregates, KPIs, wide tables | Overwrite or MERGE | Enforced, stable |

**Key principle:** Never transform data in place — always read from one layer and write to the next. This preserves full reprocessing capability.

---

## 3.2 Data Cleaning (Bronze → Silver)

### Handling nulls

```python
from pyspark.sql.functions import col, when, coalesce, lit, isnan

# Drop rows where any critical column is null
silver_df = bronze_df.dropna(subset=["order_id", "customer_id"])

# Fill nulls with defaults (by column name)
silver_df = bronze_df.fillna({
    "status":   "unknown",
    "quantity": 0,
    "amount":   0.0
})

# Conditional null replacement (more control)
silver_df = bronze_df.withColumn(
    "amount",
    when(col("amount").isNull() | isnan(col("amount")), lit(0.0))
    .otherwise(col("amount"))
)

# COALESCE: return first non-null value from list
silver_df = bronze_df.withColumn(
    "phone",
    coalesce(col("mobile_phone"), col("home_phone"), lit("unknown"))
)
```

### Standardizing data types

```python
from pyspark.sql.functions import (
    to_date, to_timestamp, trim, lower, upper,
    regexp_replace, lpad, col
)

silver_df = (bronze_df
    .withColumn("order_date",    to_date(col("order_date_str"), "yyyy-MM-dd"))
    .withColumn("created_at",    to_timestamp(col("created_at_str"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("amount",        col("amount_str").cast("double"))
    .withColumn("quantity",      col("quantity_str").cast("integer"))
    .withColumn("customer_name", trim(lower(col("customer_name"))))
    .withColumn("phone",         regexp_replace(col("phone"), "[^0-9]", ""))  # digits only
    .withColumn("zip_code",      lpad(col("zip_code"), 5, "0"))               # zero-pad to 5 chars
)
```

### Writing silver table

```python
# Append new records
(silver_df
    .write
    .mode("append")
    .option("mergeSchema", "true")      # allow new columns
    .saveAsTable("main.silver.orders"))

# Full overwrite (use for idempotent daily refreshes)
(silver_df
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")  # also replace schema
    .saveAsTable("main.silver.orders"))

# Write with partition (for large tables)
(silver_df
    .write
    .mode("append")
    .partitionBy("order_date")
    .saveAsTable("main.silver.orders"))
```

### Write modes reference

| Mode | Behavior |
|------|----------|
| `append` | Adds rows; never modifies existing data |
| `overwrite` | Replaces ALL data; preserves schema by default |
| `errorIfExists` | Fails if table already exists (default for new tables) |
| `ignore` | Does nothing if table exists |

---

## 3.3 Column and Row Manipulation

### Add, rename, drop columns

```python
from pyspark.sql.functions import col, concat_ws, lit, upper, lower

# Add new column
df = df.withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
df = df.withColumn("env_label", lit("production"))   # constant value

# Rename column
df = df.withColumnRenamed("cust_id", "customer_id")

# Drop one or more columns
df = df.drop("temp_col", "internal_flag", "debug_info")

# Select and reorder columns
df = df.select("order_id", "customer_id", "amount", "order_date")

# Select with expressions
df = df.selectExpr(
    "order_id",
    "customer_id",
    "amount * 1.10 AS amount_with_tax",
    "UPPER(status) AS status"
)
```

### Split a column

```python
from pyspark.sql.functions import split, regexp_extract

# Split on delimiter
df = (df
    .withColumn("parts",     split(col("full_address"), ", "))
    .withColumn("city",      col("parts")[0])
    .withColumn("zip_code",  col("parts")[1])
    .drop("parts"))

# Extract with regex
df = df.withColumn("area_code",
    regexp_extract(col("phone"), r"^\((\d{3})\)", 1))
```

### Filters

```python
# Equality / comparison
df_filtered = df.filter(col("status") == "active")
df_filtered = df.filter(col("amount") > 100)

# AND / OR conditions
df_filtered = df.filter(
    (col("amount") > 100) & (col("status").isin("active", "pending"))
)
df_filtered = df.filter(
    (col("region") == "US") | (col("region") == "CA")
)

# NOT
df_filtered = df.filter(~col("status").isin("test", "deleted"))

# NULL checks
df_with_null    = df.filter(col("email").isNull())
df_without_null = df.filter(col("email").isNotNull())

# SQL-style WHERE (equivalent to filter)
df_filtered = df.where("status = 'active' AND amount > 100")

# Between
df_filtered = df.filter(col("amount").between(10, 1000))

# Like / rlike
df_filtered = df.filter(col("email").like("%@databricks.com"))
df_filtered = df.filter(col("phone").rlike(r"^\+1"))
```

### Explode arrays

```python
from pyspark.sql.functions import explode, explode_outer, posexplode, flatten

# explode: one row per element; rows with null/empty arrays are dropped
df_exploded = df.withColumn("tag", explode(col("tags")))

# explode_outer: keeps rows with null/empty arrays (tag = null)
df_exploded = df.withColumn("tag", explode_outer(col("tags")))

# posexplode: adds position index alongside value
df_exploded = df.select("id", posexplode(col("tags")).alias("pos", "tag"))

# flatten: flatten array of arrays into one array
df = df.withColumn("all_tags", flatten(col("nested_tags")))
```

### Access and transform struct fields

```python
from pyspark.sql.functions import struct

# Access nested field
df = df.withColumn("city",    col("address.city"))
df = df.withColumn("country", col("address.country"))

# Expand all struct fields (explodes struct into top-level columns)
df = df.select("id", "address.*")

# Create a struct column
df = df.withColumn("location", struct(
    col("city").alias("city"),
    col("country").alias("country")
))
```

### Higher-order functions (arrays/maps)

```python
from pyspark.sql.functions import transform, filter as array_filter, aggregate, array_contains

# Transform: apply lambda to each array element
df = df.withColumn("amounts_doubled",
    transform(col("amounts"), lambda x: x * 2))

# Filter array: keep elements matching condition
df = df.withColumn("active_tags",
    array_filter(col("tags"), lambda x: x != "deleted"))

# Aggregate array to a single value
df = df.withColumn("total",
    aggregate(col("amounts"), lit(0), lambda acc, x: acc + x))

# Check if array contains value
df = df.filter(array_contains(col("tags"), "premium"))
```

---

## 3.4 Joins

### All join types

```python
# Inner join — only matching rows
result = orders.join(customers, on="customer_id", how="inner")

# Left (outer) join — all orders, matching customer data or null
result = orders.join(customers, on="customer_id", how="left")
# how="left_outer" is equivalent

# Right (outer) join — all customers, matching order data or null
result = orders.join(customers, on="customer_id", how="right")

# Full (outer) join — all rows from both sides
result = left_df.join(right_df, on="id", how="outer")
# how="full" and how="full_outer" are equivalent

# Cross join — cartesian product (every combination)
result = dates.crossJoin(products)   # ← use with extreme care; can explode data size

# Left semi join — rows from left that have a match in right (no right columns in result)
result = orders.join(valid_customers, on="customer_id", how="left_semi")

# Left anti join — rows from left with NO match in right
result = orders.join(cancelled_orders, on="order_id", how="left_anti")

# Multiple join keys
result = orders.join(payments,
    on=(orders.order_id == payments.order_id) & (orders.region == payments.region),
    how="inner")
```

### Broadcast join

Forces Spark to broadcast the smaller DataFrame to all executors, **eliminating the shuffle** entirely.

```python
from pyspark.sql.functions import broadcast

# Explicit broadcast hint
result = large_orders.join(broadcast(small_lookup), on="product_id", how="left")
```

**Auto-broadcast rules:**
```python
# Check current threshold (default: 10MB)
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

# Increase threshold to broadcast tables up to 50MB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))

# Disable auto-broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
```

> **When to use broadcast join:** When one DataFrame is significantly smaller (< auto-broadcast threshold) than the other. Common for lookup/dimension tables.

### Union operations

```python
# PySpark .union() — keeps ALL rows (including duplicates), like SQL UNION ALL
combined = df1.union(df2)
combined = df1.unionAll(df2)    # identical to .union() in PySpark

# To deduplicate after union (equivalent to SQL UNION)
combined = df1.union(df2).distinct()

# UnionByName — matches columns by name, not position (safer for different column orders)
combined = df1.unionByName(df2)
combined = df1.unionByName(df2, allowMissingColumns=True)  # fills missing cols with null
```

```sql
-- SQL UNION: deduplicates
SELECT order_id, amount FROM orders_2023
UNION
SELECT order_id, amount FROM orders_2024;

-- SQL UNION ALL: keeps duplicates (faster — no dedup)
SELECT order_id, amount FROM orders_2023
UNION ALL
SELECT order_id, amount FROM orders_2024;
```

> **Critical exam distinction:** PySpark `.union()` = SQL `UNION ALL` (keeps duplicates). SQL `UNION` deduplicates. Use `.distinct()` after PySpark `.union()` to get SQL UNION behavior.

---

## 3.5 Deduplication and Aggregations

### Deduplication

```python
# Remove exact duplicates across ALL columns
df_dedup = df.distinct()

# Remove duplicates based on specific key columns (keeps arbitrary first occurrence)
df_dedup = df.dropDuplicates(["order_id"])
df_dedup = df.dropDuplicates(["order_id", "line_item_id"])

# Keep the most recent record per key (deterministic, production-safe)
from pyspark.sql.functions import row_number, col
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(col("updated_at").desc())
df_dedup = (df
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn"))
```

### Aggregations

```python
from pyspark.sql.functions import (
    count, countDistinct, approx_count_distinct,
    sum, avg, mean, min, max,
    first, last,
    collect_list, collect_set,
    stddev, variance,
    percentile_approx
)

result = df.groupBy("department", "region").agg(
    count("*").alias("total_rows"),                              # count all rows
    count("customer_id").alias("non_null_customers"),           # count non-null
    countDistinct("customer_id").alias("unique_customers"),     # exact distinct count
    approx_count_distinct("customer_id", rsd=0.05).alias("approx_unique"),  # HyperLogLog
    sum("amount").alias("total_revenue"),
    avg("amount").alias("avg_order"),
    mean("amount").alias("mean_order"),                         # same as avg
    min("amount").alias("min_order"),
    max("amount").alias("max_order"),
    stddev("amount").alias("stddev_amount"),
    collect_list("product_id").alias("all_products"),           # array (keeps duplicates)
    collect_set("product_id").alias("unique_products"),         # array (removes duplicates)
    percentile_approx("amount", 0.5).alias("median_amount"),
    percentile_approx("amount", array(lit(0.25), lit(0.75))).alias("p25_p75")
)
```

> **`approx_count_distinct`** uses HyperLogLog algorithm — much faster and less memory than `countDistinct` on large datasets. `rsd=0.05` means 5% relative standard deviation (accuracy).

### Summary statistics

```python
# describe(): count, mean, stddev, min, max for numeric columns
df.describe("amount", "quantity").show()

# summary(): more statistics including percentiles
df.summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max").show()

# SQL equivalent
spark.sql("""
    SELECT
        COUNT(*)           AS count,
        AVG(amount)        AS mean,
        STDDEV(amount)     AS stddev,
        MIN(amount)        AS min,
        PERCENTILE(amount, 0.5) AS median,
        MAX(amount)        AS max
    FROM main.silver.orders
""")
```

### Window functions

Window functions compute values across a set of rows related to the current row.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, rank, dense_rank,
    sum, avg, min, max,
    lag, lead,
    first, last,
    percent_rank, ntile
)

# Window spec: partition by department, order by date
w = Window.partitionBy("department").orderBy("billing_date")

# Running total (sum over rows from start of partition to current row)
df = df.withColumn("running_total",
    sum("amount").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow)))

# Moving average (3-row window)
df = df.withColumn("moving_avg_3",
    avg("amount").over(w.rowsBetween(-2, 0)))

# Ranking
df = df.withColumn("row_num",    row_number().over(w))   # unique, no gaps, no ties
df = df.withColumn("rank",       rank().over(w))         # ties get same rank, gaps after
df = df.withColumn("dense_rank", dense_rank().over(w))   # ties get same rank, no gaps

# Lag/lead — access previous/next row values
df = df.withColumn("prev_amount", lag("amount", 1).over(w))   # previous row
df = df.withColumn("next_amount", lead("amount", 1).over(w))  # next row

# Partition-level aggregate without reducing rows
df = df.withColumn("dept_total",
    sum("amount").over(Window.partitionBy("department")))
```

---

## 3.6 User-Defined Functions (UDFs)

### Python UDF

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

# Define and register a UDF
@udf(returnType=StringType())
def classify_amount(amount):
    if amount is None:
        return "unknown"
    elif amount < 100:
        return "small"
    elif amount < 1000:
        return "medium"
    else:
        return "large"

# Apply the UDF
df = df.withColumn("amount_class", classify_amount(col("amount")))
```

### SQL UDF (preferred — Photon accelerated)

SQL UDFs run on the Spark optimizer and **can be accelerated by Photon**. Python UDFs cannot.

```python
# Register SQL UDF
spark.sql("""
    CREATE OR REPLACE FUNCTION main.utils.classify_amount(amount DOUBLE)
    RETURNS STRING
    RETURN CASE
        WHEN amount IS NULL THEN 'unknown'
        WHEN amount < 100   THEN 'small'
        WHEN amount < 1000  THEN 'medium'
        ELSE 'large'
    END
""")

# Use the SQL UDF
df = spark.sql("SELECT *, main.utils.classify_amount(amount) AS amount_class FROM main.silver.orders")
```

### Pandas UDF (vectorized — fast for Python logic)

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("double")
def multiply_by_tax(amounts: pd.Series) -> pd.Series:
    return amounts * 1.10

df = df.withColumn("amount_with_tax", multiply_by_tax(col("amount")))
```

### UDF performance comparison

| Type | Performance | Serialization | Photon support | Use case |
|------|-------------|--------------|----------------|----------|
| Python UDF | Slowest (row-by-row, Python GIL) | Row serialization to Python | No | Complex Python logic unavailable in SQL |
| Pandas UDF | Fast (vectorized, Apache Arrow) | Batch via Arrow | No | Pandas/NumPy operations |
| SQL UDF | Fastest (native JVM, optimizer-aware) | None (stays in JVM) | Yes | Logic expressible in SQL |
| Built-in functions | Fastest | None | Yes | Always prefer over UDFs when available |

> **Exam rule:** Always prefer built-in Spark SQL functions over UDFs. If custom logic is needed, prefer SQL UDF > Pandas UDF > Python UDF.

---

## 3.7 Spark Tuning Parameters

| Parameter | Purpose | Default | When to tune |
|-----------|---------|---------|-------------|
| `spark.sql.shuffle.partitions` | Partitions after shuffle (join/groupBy) | **200** | Lower for small data; raise for very large data |
| `spark.default.parallelism` | Default parallelism for RDD operations | 2× total cores | Match to executor core count |
| `spark.executor.memory` | Heap memory per executor | `1g` | Increase on OOM errors in executors |
| `spark.driver.memory` | Heap memory for driver | `1g` | Increase when `collect()` or large schema operations |
| `spark.sql.autoBroadcastJoinThreshold` | Max table size for auto-broadcast join | **10MB** | Increase if small lookup tables aren't being broadcast |

```python
# View current value
spark.conf.get("spark.sql.shuffle.partitions")

# Set at session level (affects current notebook/job only)
spark.conf.set("spark.sql.shuffle.partitions", "50")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))

# Set at cluster level (persists across all sessions on that cluster)
# Cluster UI → Advanced Options → Spark Config:
# spark.sql.shuffle.partitions 50
```

**Rule of thumb for shuffle partitions:**
- Target ~128–256 MB of data per partition after shuffle
- Small datasets (< 10 GB): 50–100 partitions
- Default 200 creates too many tiny partitions for small data → overhead

### Adaptive Query Execution (AQE)

AQE automatically optimizes queries at runtime based on actual data statistics (enabled by default in DBR 7.3+).

```python
# Check AQE status (on by default)
spark.conf.get("spark.sql.adaptive.enabled")   # "true"

# AQE capabilities:
# 1. Coalesce small shuffle partitions (reduces 200 → fewer larger partitions)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# 2. Convert sort-merge join to broadcast join at runtime (if one side is small)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

# 3. Handle data skew in joins automatically
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

> **Exam tip:** AQE reduces the need for manual tuning of `shuffle.partitions`. With AQE on, Spark coalesces the 200 partitions down to the right number automatically.

---

## 3.8 Gold Layer Objects in Unity Catalog

### Regular Tables

Managed or external Delta tables. Written explicitly by a job.

```sql
-- Full refresh (overwrite)
CREATE OR REPLACE TABLE main.gold.daily_revenue AS
SELECT order_date, SUM(amount) AS total_revenue, COUNT(*) AS order_count
FROM main.silver.orders
GROUP BY order_date;

-- Incremental append
INSERT INTO main.gold.daily_revenue
SELECT order_date, SUM(amount), COUNT(*)
FROM main.silver.orders
WHERE order_date = current_date() - 1
GROUP BY order_date;
```

### Views

Virtual tables — no stored data. Query is re-executed on every access.

```sql
CREATE OR REPLACE VIEW main.gold.active_customers AS
SELECT customer_id, email, region
FROM main.silver.customers
WHERE status = 'active';

-- Dynamic view: filter based on current user
CREATE OR REPLACE VIEW main.gold.my_orders AS
SELECT * FROM main.silver.orders
WHERE customer_email = current_user();
```

**Uses for views:**
- Logical layer for BI tools (hide complexity)
- Column filtering (expose only safe columns)
- Row-level filtering per user (dynamic views)
- No storage cost

### Materialized Views (in LDP pipelines)

Pre-computed stored result, **automatically refreshed** when upstream data changes.

```python
import dlt

@dlt.materialized_view(
    comment="Pre-aggregated daily revenue for BI",
    table_properties={"quality": "gold"}
)
def daily_revenue():
    return (dlt.read("silver_orders")
              .groupBy("order_date", "region")
              .agg({"amount": "sum", "order_id": "count"}))
```

```sql
-- SQL syntax in LDP
CREATE OR REFRESH MATERIALIZED VIEW main.gold.daily_revenue AS
SELECT order_date, region,
       SUM(amount)    AS total_revenue,
       COUNT(order_id) AS order_count
FROM LIVE.silver_orders
GROUP BY order_date, region;
```

**Key differences from view:**
- Data is stored (query result pre-computed)
- Reads are fast (no re-execution)
- Refreshed automatically by LDP pipeline
- Supports query acceleration features (Liquid Clustering)

### Streaming Tables (in LDP pipelines)

A Delta table continuously updated from a streaming source.

```python
import dlt

@dlt.table(name="bronze_clickstream")
def bronze_clickstream():
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load("/Volumes/main/landing/clickstream/"))
```

```sql
-- SQL syntax
CREATE OR REFRESH STREAMING TABLE bronze_clickstream
AS SELECT * FROM cloud_files('/Volumes/main/landing/clickstream/', 'json');
```

**Use when:** Data arrives continuously and you need the table to grow incrementally without reprocessing all history.

### Gold object comparison

| Object | Storage | Auto-refresh | Latency | Best for |
|--------|---------|-------------|---------|----------|
| **Table** | Yes | No (manual) | Batch | Custom refresh logic, full materialization |
| **View** | No | On-query | Real-time | Logical layer, no storage cost |
| **Materialized view** | Yes | Yes (pipeline) | Near-real-time | Pre-aggregated BI datasets, fast query |
| **Streaming table** | Yes | Continuous | Low | Real-time landing, continuous ingestion |

---

## 3.9 Data Quality Checks and Validation

### LDP Expectations (declarative data quality)

```python
import dlt

@dlt.table
@dlt.expect("valid_amount",     "amount > 0")                          # warn, keep row
@dlt.expect_or_drop("non_null_id", "order_id IS NOT NULL")             # drop bad row
@dlt.expect_or_fail("valid_status", "status IN ('active','closed')")   # fail pipeline
def silver_orders():
    return dlt.read_stream("bronze_orders")
```

**Multiple expectations:**
```python
@dlt.expect_all({
    "valid_amount":   "amount > 0",
    "non_null_order": "order_id IS NOT NULL",
    "valid_date":     "order_date >= '2020-01-01'"
})
@dlt.table
def silver_orders():
    return dlt.read_stream("bronze_orders")

# Drop if ANY expectation fails
@dlt.expect_all_or_drop({...})

# Fail if ANY expectation fails
@dlt.expect_all_or_fail({...})
```

### Manual quality checks in PySpark

```python
from pyspark.sql.functions import count, when, col

# Count nulls per column
null_counts = df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df.columns
])
null_counts.show()

# Assert no duplicates
total   = df.count()
deduped = df.dropDuplicates(["order_id"]).count()
assert total == deduped, f"Found {total - deduped} duplicate order_ids!"

# Check value ranges
invalid_amount = df.filter((col("amount") <= 0) | col("amount").isNull()).count()
assert invalid_amount == 0, f"{invalid_amount} rows with invalid amount"

# Check referential integrity
orphan_orders = orders.join(customers, on="customer_id", how="left_anti").count()
assert orphan_orders == 0, f"{orphan_orders} orders with no matching customer"

# Check for stale data (data freshness)
from pyspark.sql.functions import datediff, current_date, max as spark_max
max_date = df.agg(spark_max("order_date")).collect()[0][0]
staleness_days = df.select(datediff(current_date(), lit(str(max_date)))).collect()[0][0]
assert staleness_days < 2, f"Data is {staleness_days} days old — expected < 2"
```

### Quarantine pattern

```python
from pyspark.sql.functions import current_timestamp, lit

valid   = df.filter(col("amount") > 0)
invalid = df.filter(col("amount") <= 0)

# Enrich quarantine records with rejection reason
invalid = invalid.withColumn("rejection_reason", lit("amount <= 0")) \
                 .withColumn("rejected_at", current_timestamp())

valid.write.mode("append").saveAsTable("main.silver.orders")
invalid.write.mode("append").saveAsTable("main.quarantine.orders_rejected")
```

---

## 3.10 Common Exam Traps

| Trap | Correct answer |
|------|---------------|
| "PySpark `.union()` deduplicates" | False — `.union()` keeps all rows. Use `.distinct()` to deduplicate |
| "SQL UNION ALL deduplicates" | False — UNION ALL keeps duplicates; SQL UNION deduplicates |
| "Python UDFs are faster than SQL UDFs" | False — SQL UDFs are faster (Photon-enabled, JVM-native) |
| "Broadcast join requires calling `broadcast()` explicitly" | False — Spark auto-broadcasts if table size < `autoBroadcastJoinThreshold` |
| "`.dropDuplicates()` is deterministic" | Not fully — it keeps an arbitrary record when duplicates exist; use window function for deterministic "keep latest" |
| "AQE needs to be enabled manually" | False — AQE is on by default in DBR 7.3+ |
| "`approx_count_distinct` is always less accurate than `countDistinct`" | True, but it's much faster and scales better; `rsd` parameter controls accuracy |
| "Streaming table = materialized view" | False — streaming tables receive streaming data; materialized views are batch-refreshed aggregations |
