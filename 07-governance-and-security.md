# Section 7: Governance and Security

## Exam Objectives

- Differentiate between managed and external tables in Unity Catalog and perform basic operations (create, modify, delete, and convert between managed and external tables) on them
- Configure access controls using the UI and SQL by applying GRANT, REVOKE, and DENY privileges to principals (users, groups, and service principals) at appropriate levels of the security hierarchy
- Understand column-level masking and row-level security to restrict data visibility based on user groups
- Understand Unity Catalog ABAC policies to centrally control row-level filtering and column masking for sensitive data

---

## 7.1 Unity Catalog Architecture

Unity Catalog (UC) provides a single governance layer across all Databricks workspaces in an account.

### Namespace hierarchy

```
Databricks Account (cloud account)
  └── Metastore (one per region)
        ├── Catalog A
        │     ├── Schema (Database) 1
        │     │     ├── Table
        │     │     ├── View
        │     │     ├── Materialized View
        │     │     ├── Streaming Table
        │     │     ├── Volume (unstructured files)
        │     │     └── Function (UDF)
        │     └── Schema 2 ...
        ├── Catalog B ...
        └── System Catalog (system.access, system.information_schema, etc.)
```

**Key rules:**
- One metastore per region per account (shared by all workspaces in that region)
- Multiple workspaces can be attached to the same metastore → same governance, same data
- `workspace_catalog` is a special catalog representing the legacy workspace-local namespace

### System catalogs

Databricks provides read-only `system` catalogs for observability:

| System table | Content |
|-------------|---------|
| `system.access.audit` | All data access audit events (who accessed what, when) |
| `system.access.column_lineage` | Column-level lineage across tables |
| `system.access.table_lineage` | Table-level read/write lineage |
| `system.information_schema.tables` | Metadata about all tables in the account |
| `system.billing.usage` | DBU consumption details |

```sql
-- Query audit logs
SELECT user_identity.email, action_name, request_params.table_name, event_time
FROM system.access.audit
WHERE event_date = current_date()
  AND action_name IN ('commandSubmit', 'runCommand')
ORDER BY event_time DESC
LIMIT 100;

-- Check who read a specific table
SELECT user_identity.email, event_time
FROM system.access.audit
WHERE request_params LIKE '%main.silver.orders%'
  AND action_name = 'commandSubmit'
ORDER BY event_time DESC;
```

---

## 7.2 Managed vs External Tables

### Managed tables

Databricks owns **both the metadata and the data files**.

| Aspect | Behavior |
|--------|----------|
| Data location | UC metastore storage root (or catalog/schema default location) |
| `DROP TABLE` | Deletes **both metadata AND data files** — **irreversible** |
| Creation syntax | No `LOCATION` clause |
| Data movement on create | Data stays where written; no copy |
| Governance | Fully managed by Unity Catalog |
| Best for | Most use cases — simplest lifecycle management |

```sql
-- Create managed table (schema explicit)
CREATE TABLE main.silver.orders (
    order_id    STRING    NOT NULL,
    customer_id STRING,
    amount      DOUBLE,
    status      STRING,
    order_date  DATE
) USING DELTA;

-- CTAS — Create Table As Select (managed by default)
CREATE OR REPLACE TABLE main.gold.daily_revenue AS
SELECT order_date, SUM(amount) AS revenue, COUNT(*) AS order_count
FROM main.silver.orders
GROUP BY order_date;

-- Create with Liquid Clustering
CREATE TABLE main.silver.events CLUSTER BY (event_date, user_id);
```

### External tables

The user owns the **data files**; Databricks only owns the metadata.

| Aspect | Behavior |
|--------|----------|
| Data location | User-specified cloud path via `LOCATION` |
| `DROP TABLE` | Deletes **metadata only** — data files remain at location |
| Creation syntax | Requires `LOCATION` clause |
| Prerequisite | UC External Location (or Storage Credential) must exist |
| Governance | Metadata governed by UC; data access also controlled by cloud IAM |
| Best for | Data shared with external systems, data you can't move, legacy data |

```sql
-- Create external table (Delta at specified path)
CREATE TABLE main.external.raw_events
USING DELTA
LOCATION 'abfss://container@account.dfs.core.windows.net/raw/events/';

-- Create external table with explicit schema
CREATE TABLE main.external.sales_csv (
    sale_id   STRING,
    amount    DOUBLE,
    sale_date DATE
)
USING CSV
OPTIONS ('header' = 'true', 'delimiter' = ',')
LOCATION 's3://my-bucket/raw/sales/';

-- Register existing Delta files as external table
CREATE TABLE main.external.existing_delta
USING DELTA
LOCATION 's3://my-bucket/delta/existing_data/';
```

### Managed vs External — full comparison

| | Managed | External |
|--|---------|----------|
| Data files owned by | Databricks | User / external system |
| `DROP TABLE` deletes data | **Yes — data is gone** | No — only metadata removed |
| `LOCATION` clause | Not used | **Required** |
| Data moved on CREATE | No | No (register existing files) |
| Portability | Low (tied to metastore storage) | High (files exist independently) |
| UC governance | Full | Metadata only (data needs cloud perms too) |
| `UNDROP TABLE` | Not supported | Data still exists at location |

### Basic table operations

```sql
-- Add a column
ALTER TABLE main.silver.orders ADD COLUMN discount_pct DOUBLE;
ALTER TABLE main.silver.orders ADD COLUMNS (discount_pct DOUBLE, is_premium BOOLEAN);

-- Rename a column (Delta only)
ALTER TABLE main.silver.orders RENAME COLUMN discount TO discount_pct;

-- Change column type (Delta allows widening: INT→LONG, FLOAT→DOUBLE)
ALTER TABLE main.silver.orders ALTER COLUMN amount TYPE DOUBLE;

-- Add a comment to a table or column
COMMENT ON TABLE main.silver.orders IS 'Cleaned orders from bronze layer';
ALTER TABLE main.silver.orders ALTER COLUMN amount COMMENT 'Order total in USD';

-- Add table properties (custom key-value metadata)
ALTER TABLE main.silver.orders SET TBLPROPERTIES ('owner' = 'data-team', 'pii' = 'false');

-- Drop a table
DROP TABLE IF EXISTS main.silver.orders;    -- managed: deletes data + metadata
                                            -- external: deletes metadata only

-- Inspect table details
DESCRIBE TABLE main.silver.orders;
DESCRIBE TABLE EXTENDED main.silver.orders;  -- includes metadata, location, properties
DESCRIBE DETAIL main.silver.orders;          -- Delta-specific: size, numFiles, partitions
```

### Converting between managed and external

```sql
-- Managed → External: create new external table from managed data
CREATE TABLE main.silver.orders_ext
USING DELTA
LOCATION 's3://my-bucket/silver/orders/'
AS SELECT * FROM main.silver.orders;

-- Then optionally remove the managed version
DROP TABLE main.silver.orders;

-- External → Managed: copy data into UC-managed storage
CREATE TABLE main.silver.orders_managed
AS SELECT * FROM main.silver.orders_ext;

-- No in-place conversion — must create a new table
```

---

## 7.3 UC Volumes

Volumes are Unity Catalog objects for managing **files** (not Delta tables).

| Type | Location | DROP behavior |
|------|----------|--------------|
| **Managed volume** | UC-managed storage | Deletes files |
| **External volume** | User-specified path | Keeps files |

```sql
-- Create managed volume
CREATE VOLUME main.landing.raw_files;
-- Files stored at: /Volumes/main/landing/raw_files/

-- Create external volume
CREATE EXTERNAL VOLUME main.landing.raw_files
LOCATION 's3://bucket/landing/';

-- Access in PySpark
spark.read.format("json").load("/Volumes/main/landing/raw_files/events/")
dbutils.fs.ls("/Volumes/main/landing/raw_files/")

-- Grant access to volume
GRANT READ VOLUME  ON VOLUME main.landing.raw_files TO data_engineers;
GRANT WRITE VOLUME ON VOLUME main.landing.raw_files TO ingestion_jobs;
```

---

## 7.4 Access Control: GRANT, REVOKE, DENY

### Unity Catalog privilege hierarchy

```
Account
  └── Metastore                          ← metastore admin (account-level role)
        └── CATALOG     → USE CATALOG
              └── SCHEMA  → USE SCHEMA
                    ├── TABLE     → SELECT, MODIFY, ALL PRIVILEGES
                    ├── VIEW      → SELECT
                    ├── VOLUME    → READ VOLUME, WRITE VOLUME
                    ├── FUNCTION  → EXECUTE
                    └── SCHEMA    → CREATE TABLE, CREATE VIEW, ...
```

**Privilege inheritance:** A privilege granted at a higher level applies to all objects below it.
```sql
-- Granting SELECT on CATALOG = SELECT on ALL schemas/tables/views in catalog
GRANT SELECT ON CATALOG main TO analysts;  -- very broad — use with care
```

### Principals

| Principal | Description | Example |
|-----------|-------------|---------|
| **User** | Individual Databricks account (email) | `alice@company.com` |
| **Group** | Named collection of users + service principals | `data_engineers`, `analysts` |
| **Service principal** | Machine identity for automated workloads | `etl-service-principal` |

### Complete privileges reference

| Privilege | Securable object | What it grants |
|-----------|-----------------|----------------|
| `USE CATALOG` | Catalog | Navigate into the catalog (required for any catalog access) |
| `USE SCHEMA` | Schema | Navigate into the schema (required for any schema access) |
| `SELECT` | Table, View, Schema | Read data from tables/views |
| `MODIFY` | Table | INSERT, UPDATE, DELETE, MERGE on a table |
| `CREATE TABLE` | Schema | Create new tables in the schema |
| `CREATE VIEW` | Schema | Create new views in the schema |
| `CREATE SCHEMA` | Catalog | Create new schemas in the catalog |
| `CREATE FUNCTION` | Schema | Create UDFs in the schema |
| `CREATE VOLUME` | Schema | Create volumes in the schema |
| `ALL PRIVILEGES` | Any | All applicable privileges for that securable type |
| `READ VOLUME` | Volume | Read files from a UC Volume |
| `WRITE VOLUME` | Volume | Write files to a UC Volume |
| `EXECUTE` | Function | Call a user-defined function |
| `APPLY TAG` | Any | Attach tags to objects |
| `BROWSE` | Catalog, Schema | View metadata without accessing data |

### GRANT examples

```sql
-- Minimum read-only access to a single table
GRANT USE CATALOG ON CATALOG main TO analysts;
GRANT USE SCHEMA  ON SCHEMA  main.silver TO analysts;
GRANT SELECT      ON TABLE   main.silver.orders TO analysts;

-- Read-only access to all tables in a schema (current AND future)
GRANT USE CATALOG ON CATALOG main TO analysts;
GRANT USE SCHEMA  ON SCHEMA  main.silver TO analysts;
GRANT SELECT      ON SCHEMA  main.silver TO analysts;

-- Write access to a table
GRANT MODIFY ON TABLE main.silver.orders TO data_engineers;

-- Grant to a service principal (use backticks)
GRANT USE CATALOG ON CATALOG main TO `etl-service-principal`;
GRANT SELECT      ON SCHEMA  main.silver TO `etl-service-principal`;
GRANT MODIFY      ON TABLE   main.silver.orders TO `etl-service-principal`;

-- Grant everything on a catalog
GRANT ALL PRIVILEGES ON CATALOG main TO data_team;

-- Grant function execution
GRANT EXECUTE ON FUNCTION main.utils.classify_amount TO analysts;

-- Grant volume access
GRANT READ VOLUME  ON VOLUME main.landing.raw_files TO ingestion_team;
GRANT WRITE VOLUME ON VOLUME main.landing.raw_files TO ingestion_team;
```

### REVOKE examples

```sql
-- Remove a specific privilege
REVOKE SELECT ON TABLE main.silver.orders FROM analysts;

-- Revoke schema-level SELECT
REVOKE SELECT ON SCHEMA main.silver FROM contractors;

-- Revoke all privileges from a principal
REVOKE ALL PRIVILEGES ON CATALOG main FROM ex_employee;
```

### DENY

`DENY` explicitly **blocks** a privilege even if the principal has it via group membership or a higher-level GRANT. **DENY overrides GRANT.**

```sql
-- Block modification even if group has MODIFY
DENY MODIFY ON TABLE main.silver.orders TO `restricted_user@company.com`;

-- Block all access to a specific table for a group
DENY ALL PRIVILEGES ON TABLE main.silver.pii_data TO contractors;
```

> **DENY vs REVOKE:** REVOKE removes a previously granted privilege (neutral state). DENY actively blocks the privilege (overrides grants from other paths).

### Show grants

```sql
SHOW GRANTS ON TABLE main.silver.orders;
SHOW GRANTS ON SCHEMA main.silver;
SHOW GRANTS ON CATALOG main;
SHOW GRANTS TO analysts;                        -- all grants for a specific principal
```

---

## 7.5 Key Roles in Unity Catalog

| Role | Scope | How assigned | Typical responsibilities |
|------|-------|-------------|-------------------------|
| **Account admin** | Databricks account | Account console | Create metastores, manage users/groups |
| **Metastore admin** | Metastore | Assigned by account admin | Create catalogs, manage storage credentials, assign workspaces |
| **Catalog owner** | Catalog | Table owner or GRANT | CREATE SCHEMA, manage catalog-level privileges |
| **Schema owner** | Schema | `ALTER SCHEMA OWNER TO` | CREATE TABLE/VIEW, manage schema-level privileges |
| **Table owner** | Table | Creator by default | Modify table, manage table-level privileges, apply masks/filters |

```sql
-- Transfer ownership
ALTER TABLE main.silver.orders OWNER TO data_engineering_team;
ALTER SCHEMA main.silver OWNER TO data_engineering_team;
ALTER CATALOG main OWNER TO platform_team;
```

---

## 7.6 Column-Level Masking

Column masking hides or transforms sensitive column values at query time based on the **current user's group membership** — without changing underlying data.

### Implementation

```sql
-- Step 1: Create a masking function (stored as a UC function)
CREATE OR REPLACE FUNCTION main.security.mask_credit_card(cc_number STRING)
RETURNS STRING
RETURN CASE
    WHEN is_member('pci_access')  THEN cc_number               -- full number for authorized users
    ELSE concat('****-****-****-', right(cc_number, 4))        -- last 4 digits for others
END;

CREATE OR REPLACE FUNCTION main.security.mask_email(email STRING)
RETURNS STRING
RETURN CASE
    WHEN is_member('pii_admin')   THEN email
    WHEN is_member('pii_read')    THEN regexp_replace(email, '(^[^@]+)', '****')  -- ****@domain.com
    ELSE 'REDACTED'
END;

-- Step 2: Apply mask to a column
ALTER TABLE main.silver.customers
    ALTER COLUMN credit_card_number SET MASK main.security.mask_credit_card;

ALTER TABLE main.silver.customers
    ALTER COLUMN email SET MASK main.security.mask_email;

-- Remove mask
ALTER TABLE main.silver.customers
    ALTER COLUMN credit_card_number UNSET MASK;
```

**Behavior:**
- `pci_access` group members: see full credit card number
- Others: see `****-****-****-1234`

**Mask function requirements:**
- Takes the masked column value as its first argument (same type as column)
- Returns the same type as the input column
- Can use `is_member()`, `current_user()`, and lookup tables

---

## 7.7 Row-Level Security (RLS)

Row-level security filters which rows a user can see, determined at query time by their group membership.

### Implementation

```sql
-- Step 1: Create a row filter function
CREATE OR REPLACE FUNCTION main.security.region_filter(region STRING)
RETURNS BOOLEAN
RETURN is_member('global_read')                               -- global team: all rows
    OR is_member(concat('region_', lower(region)))            -- region-specific groups
    OR current_user() IN (
        SELECT manager_email FROM main.security.regional_managers
        WHERE managed_region = region
    );

-- Step 2: Apply filter to the table
ALTER TABLE main.silver.sales
    SET ROW FILTER main.security.region_filter ON (region);

-- Remove filter
ALTER TABLE main.silver.sales DROP ROW FILTER;
```

**Behavior:**
- `global_read` group: sees all rows (all regions)
- `region_us` group: sees only `region = 'US'` rows
- `region_eu` group: sees only `region = 'EU'` rows

**Row filter function requirements:**
- Arguments must match column(s) in the `ON (columns)` clause
- Must return `BOOLEAN`
- Can reference `current_user()`, `is_member()`, and sub-selects

---

## 7.8 Unity Catalog ABAC Policies

**ABAC (Attribute-Based Access Control)** combines row filters and column masks into a centralized, policy-driven access control model enforced at the Unity Catalog layer.

### ABAC vs Traditional GRANT/REVOKE

| Dimension | GRANT/REVOKE | ABAC (Row Filter + Column Mask) |
|-----------|-------------|--------------------------------|
| **Granularity** | Table, schema, catalog | Individual rows and column values |
| **Who defines** | Object owners / data stewards | Central security team |
| **Scope** | Static: "analysts can SELECT table X" | Dynamic: "analysts see rows WHERE their department matches" |
| **Enforcement** | Catalog-level check | Filter/mask applied to every query |
| **Transparency** | Users know they can/can't access | Applied transparently — users don't know the policy |
| **Scale** | Manageable for coarse-grained access | Required for PII, multi-tenant, regulated data |

### ABAC policy enforcement points

ABAC policies are enforced at the **Unity Catalog layer** — they apply regardless of:
- How the user connects: notebook, SQL Warehouse, BI tool (Tableau, Power BI), JDBC/ODBC
- What query the user runs (cannot be bypassed by clever SQL)
- Whether the user uses Spark, SQL, or a partner tool

> **Key exam point:** ABAC (row filters + column masks) are enforced even on direct Spark reads. This is a key advantage over legacy Hive/Spark security which could be bypassed.

### ABAC implementation pattern

```sql
-- 1. Create centralized policy functions in a security schema
CREATE SCHEMA IF NOT EXISTS main.security;

-- 2. Create masking function for PII columns
CREATE OR REPLACE FUNCTION main.security.mask_ssn(ssn STRING)
RETURNS STRING
RETURN CASE WHEN is_member('hr_pii') THEN ssn ELSE '***-**-****' END;

-- 3. Create row filter for multi-tenant isolation
CREATE OR REPLACE FUNCTION main.security.tenant_filter(tenant_id STRING)
RETURNS BOOLEAN
RETURN is_member('super_admin')
    OR tenant_id = (SELECT current_tenant_id FROM main.security.user_tenant_map
                    WHERE user_email = current_user());

-- 4. Apply to all relevant tables
ALTER TABLE main.silver.employees ALTER COLUMN ssn SET MASK main.security.mask_ssn;
ALTER TABLE main.silver.transactions SET ROW FILTER main.security.tenant_filter ON (tenant_id);

-- 5. All queries now go through the policy — no per-query changes needed
SELECT ssn FROM main.silver.employees;  -- HR sees full SSN; others see ***-**-****
```

### `is_member()` and `current_user()` in policies

```sql
is_member('group_name')      -- TRUE if current user belongs to that group
current_user()               -- returns current user's email as STRING
```

**Key behaviors:**
- `is_member()` checks **direct AND inherited** group membership (transitive)
- Works in filter functions, mask functions, and dynamic views
- Evaluated fresh on every query (not cached)

---

## 7.9 Data Lineage

Unity Catalog automatically tracks **column-level and table-level lineage** for all SQL and Python operations run in Databricks.

### What lineage captures

- **Table lineage:** which tables were read to produce which tables
- **Column lineage:** which source columns contributed to which output columns
- **Notebook/query lineage:** which notebook or query created or modified the data

### Viewing lineage

**UI:** Catalog Explorer → select table → **Lineage tab**

Shows:
- Upstream tables (sources this table was derived from)
- Downstream tables (tables that were derived from this table)
- Column-level mappings (trace a column back to its origin)

```sql
-- Query lineage programmatically
SELECT *
FROM system.access.table_lineage
WHERE target_table_name = 'main.gold.daily_revenue'
ORDER BY event_time DESC;

SELECT *
FROM system.access.column_lineage
WHERE target_table_name = 'main.gold.daily_revenue'
  AND target_column_name = 'total_revenue';
```

**Lineage supports:**
- Spark SQL operations
- PySpark DataFrame operations
- `MERGE INTO`, `INSERT INTO`, `CREATE TABLE AS SELECT`
- Does NOT capture: file reads/writes outside Delta, external tools writing directly to storage

---

## 7.10 Governance Quick Reference

### Minimum grants for read-only access

```sql
-- Read-only access to a single table
GRANT USE CATALOG ON CATALOG my_catalog TO my_group;
GRANT USE SCHEMA  ON SCHEMA  my_catalog.my_schema TO my_group;
GRANT SELECT      ON TABLE   my_catalog.my_schema.my_table TO my_group;

-- Read-only access to ALL tables in a schema (including future tables)
GRANT USE CATALOG ON CATALOG my_catalog TO my_group;
GRANT USE SCHEMA  ON SCHEMA  my_catalog.my_schema TO my_group;
GRANT SELECT      ON SCHEMA  my_catalog.my_schema TO my_group;
```

### Managed vs external DROP behavior

```sql
-- Managed table: DROP deletes metadata AND data files
DROP TABLE main.silver.managed_orders;
-- ⚠️  Data files are permanently deleted from cloud storage

-- External table: DROP deletes metadata ONLY
DROP TABLE main.external.legacy_orders;
-- ✓  Data files remain at the LOCATION path
```

### DENY vs REVOKE

```sql
-- REVOKE: removes a privilege (principal returns to neutral state)
REVOKE SELECT ON TABLE t FROM alice;
-- Effect: alice has no explicit grant — if she has group grants, those still apply

-- DENY: actively blocks the privilege (overrides all grants including group grants)
DENY SELECT ON TABLE t TO alice;
-- Effect: alice cannot SELECT even if her group has GRANT SELECT
```

---

## 7.11 Common Exam Traps

| Trap | Correct answer |
|------|---------------|
| "DROP TABLE on a managed table can be undone" | False — managed table data is permanently deleted |
| "DROP TABLE on an external table deletes the data files" | False — only metadata is deleted |
| "REVOKE is the same as DENY" | False — REVOKE removes a grant; DENY actively blocks even if group grants it |
| "GRANT SELECT on a schema grants SELECT on views too" | True — views are included in schema-level GRANT SELECT |
| "Column masking changes the underlying data" | False — masking is applied at query time; data unchanged in storage |
| "Row filters can be bypassed with a direct Spark read" | False — UC enforces row filters on all access paths including direct Spark |
| "A user needs USE CATALOG + USE SCHEMA to read a table" | True — both are required in addition to SELECT on the table |
| "Data lineage must be manually configured" | False — UC captures lineage automatically for SQL and PySpark operations |
| "One metastore can be shared by multiple workspaces" | True — all workspaces in the same region share one metastore |

---

## Official Databricks Documentation Links

- [Unity Catalog Overview](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Managed and External Tables](https://docs.databricks.com/data-governance/unity-catalog/create-tables.html)
- [Volumes](https://docs.databricks.com/data-governance/unity-catalog/create-volumes.html)
- [GRANT, REVOKE, DENY](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-grant.html)
- [Unity Catalog Privileges](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)
- [Column Masking](https://docs.databricks.com/data-governance/unity-catalog/column-masking.html)
- [Row-Level Security (Row Filters)](https://docs.databricks.com/data-governance/unity-catalog/row-filters.html)
- [Attribute-Based Access Control (ABAC)](https://docs.databricks.com/data-governance/unity-catalog/abac.html)
- [Data Lineage](https://docs.databricks.com/data-governance/unity-catalog/data-lineage.html)
- [Audit Logs](https://docs.databricks.com/admin/account-settings/audit-logs.html)
- [Service Principals](https://docs.databricks.com/admin/users-groups/service-principals.html)
