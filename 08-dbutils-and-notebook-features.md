# Section 8: dbutils and Notebook Features

## Exam Objectives

- Understand `dbutils` submodules and their common functions
- Know notebook magic commands and when to use each
- Understand `display()` vs `.show()` behavior

---

## 8.1 dbutils Overview

`dbutils` is a Databricks-specific utility object available in every notebook. It provides access to widgets, file system, secrets, notebook orchestration, and library management.

```python
# List all available submodules
dbutils.help()

# Get help for a specific submodule
dbutils.fs.help()
dbutils.widgets.help()
```

---

## 8.2 dbutils.widgets — Parameterized Notebooks

Widgets create input parameters for notebooks — used for interactive exploration and for passing parameters from Lakeflow Jobs.

### Widget types

| Method | Widget type | Description |
|--------|------------|-------------|
| `dbutils.widgets.text(name, default, label)` | Text box | Free-form text input |
| `dbutils.widgets.dropdown(name, default, choices, label)` | Dropdown | Single-select from list |
| `dbutils.widgets.combobox(name, default, choices, label)` | Combobox | Dropdown with free-form option |
| `dbutils.widgets.multiselect(name, default, choices, label)` | Multi-select | Multiple selections from list |

```python
# Create widgets
dbutils.widgets.text("catalog", "dev_catalog", "Target Catalog")
dbutils.widgets.dropdown("env", "dev", ["dev", "staging", "prod"], "Environment")
dbutils.widgets.combobox("date", "2024-01-01", ["2024-01-01", "2024-02-01"], "Run Date")
dbutils.widgets.multiselect("regions", "US", ["US", "EU", "APAC", "LATAM"], "Regions")

# Read widget values
catalog = dbutils.widgets.get("catalog")
env     = dbutils.widgets.get("env")
regions = dbutils.widgets.get("regions")   # returns comma-separated string: "US,EU"

# Remove widgets
dbutils.widgets.remove("catalog")    # remove one
dbutils.widgets.removeAll()          # remove all widgets
```

**How Lakeflow Jobs passes parameters:**
- Job `base_parameters` are mapped to widget names
- The notebook reads them with `dbutils.widgets.get("param_name")`
- Default value in widget is used when running interactively

```python
# Works both interactively (uses default) and in a job (uses job parameter)
dbutils.widgets.text("catalog", "dev_catalog")
catalog = dbutils.widgets.get("catalog")
# Interactive: "dev_catalog"
# Job with base_parameters {"catalog": "prod_catalog"}: "prod_catalog"
```

---

## 8.3 dbutils.fs — File System Operations

Access cloud storage and Volumes via a unified file system API.

| Method | Description |
|--------|-------------|
| `dbutils.fs.ls(path)` | List files/directories at path |
| `dbutils.fs.head(path, maxBytes)` | Read first N bytes of a file |
| `dbutils.fs.cp(src, dst, recurse=False)` | Copy files |
| `dbutils.fs.mv(src, dst, recurse=False)` | Move files |
| `dbutils.fs.rm(path, recurse=False)` | Delete files |
| `dbutils.fs.put(path, contents, overwrite=False)` | Write string to file |
| `dbutils.fs.mkdirs(path)` | Create directories |

```python
# List files in a Volume
files = dbutils.fs.ls("/Volumes/main/landing/raw_files/")
for f in files:
    print(f.name, f.size, f.modificationTime)

# Read first 1000 bytes of a file
content = dbutils.fs.head("/Volumes/main/landing/raw_files/sample.json", 1000)

# Copy a file
dbutils.fs.cp("/Volumes/main/landing/file.csv", "/Volumes/main/archive/file.csv")

# Delete a directory and all contents
dbutils.fs.rm("/Volumes/main/temp/scratch/", recurse=True)

# Write a small text file
dbutils.fs.put("/Volumes/main/temp/note.txt", "Hello World", overwrite=True)
```

> **Note:** `dbutils.fs` paths use the `dbfs:/` scheme by default. Volume paths (`/Volumes/...`) are the modern approach — avoid legacy `dbfs:/mnt/` paths.

### Legacy mounts (deprecated but may appear on exam)

```python
# Mount external storage (legacy — replaced by UC Volumes and External Locations)
dbutils.fs.mount(
    source="wasbs://container@account.blob.core.windows.net/",
    mount_point="/mnt/my_data",
    extra_configs={"fs.azure.account.key.account.blob.core.windows.net": key}
)

# Unmount
dbutils.fs.unmount("/mnt/my_data")

# List mounts
dbutils.fs.mounts()
```

> **Exam tip:** Mounts are legacy. The modern replacement is UC Volumes (for files) and External Locations (for storage credentials). However, you may see mount-related questions.

---

## 8.4 dbutils.secrets — Credential Management

Retrieve secrets stored in Databricks secret scopes — **never hardcode credentials**.

```python
# Get a secret value (always returns a string)
password = dbutils.secrets.get(scope="my-scope", key="db-password")
api_key  = dbutils.secrets.get(scope="my-scope", key="api-key")

# List all secret scopes
dbutils.secrets.listScopes()

# List all keys in a scope (shows key names, NOT values)
dbutils.secrets.list("my-scope")
```

**Key behaviors:**
- Secret values are **REDACTED** in notebook output — `print(password)` shows `[REDACTED]`
- Secrets are only accessible within the notebook execution context
- Scope-based: organize secrets by team, environment, or application
- Backed by Databricks-managed store or Azure Key Vault / AWS Secrets Manager

---

## 8.5 dbutils.notebook — Orchestration

```python
# Run another notebook with parameters (isolated scope)
result = dbutils.notebook.run(
    path="./transforms/silver_orders",
    timeout_seconds=600,
    arguments={"catalog": "main", "date": "2024-01-15"}
)
# Returns: the string passed to dbutils.notebook.exit() in the child notebook

# Exit the current notebook with a return value (string only)
dbutils.notebook.exit("success")
dbutils.notebook.exit(str(row_count))   # must be a string
```

> See Section 4.10 for the full `%run` vs `dbutils.notebook.run()` comparison.

---

## 8.6 dbutils.library — Library Management

```python
# Restart the Python interpreter (required after %pip install)
dbutils.library.restartPython()
```

Used after `%pip install` to ensure newly installed packages are available. All Python variables are cleared on restart.

---

## 8.7 Magic Commands

Magic commands are prefixed with `%` and control notebook cell behavior.

| Command | Description |
|---------|-------------|
| `%python` | Switch cell to Python |
| `%sql` | Switch cell to SQL |
| `%scala` | Switch cell to Scala |
| `%r` | Switch cell to R |
| `%md` | Render cell as Markdown |
| `%sh` | Execute shell command on the driver node |
| `%pip` | Install Python packages (notebook-scoped) |
| `%run` | Execute another notebook (shared scope) |
| `%fs` | Shorthand for `dbutils.fs` commands |

```python
# %pip — install packages (session-scoped, isolated)
%pip install requests==2.31.0 pandas==2.1.0

# %sh — run shell commands on the driver
%sh ls -la /Volumes/main/landing/raw_files/
%sh cat /etc/os-release

# %fs — file system operations (shorthand for dbutils.fs)
%fs ls /Volumes/main/landing/
%fs head /Volumes/main/landing/sample.json

# %run — execute another notebook (shared scope)
%run ./helpers/common_functions
```

**`%pip install` behavior:**
- Installs packages for the **current notebook session only**
- Overrides cluster-attached libraries if version conflicts
- Requires `dbutils.library.restartPython()` after install
- Packages are lost on cluster restart

---

## 8.8 display() vs .show() vs .toPandas()

| Method | Output | Where | Use case |
|--------|--------|-------|----------|
| `display(df)` | Rich HTML table with charts, formatting | Databricks notebooks only | Interactive exploration, visualization |
| `df.show(n)` | Plain text, truncated columns | Any Spark environment | Quick debug, CI/CD logs |
| `df.toPandas()` | Pandas DataFrame in driver memory | Any Python environment | Small datasets, Pandas operations |

```python
# display() — rich rendering (Databricks-specific)
display(df)                         # default: 1000 rows, scrollable
display(df.limit(100))              # limit rows before display

# .show() — plain text output
df.show()                           # default: 20 rows, truncated columns
df.show(50, truncate=False)         # 50 rows, full column width

# .toPandas() — brings ALL data to driver (dangerous for large DataFrames)
pdf = df.toPandas()                 # ⚠️ OOM risk if df is large
pdf = df.limit(10000).toPandas()    # safer: limit first
```

> **Exam tip:** `display()` is Databricks-specific and does NOT work outside Databricks notebooks. `.show()` works everywhere. `.toPandas()` collects all data to the driver — use `.limit()` first to avoid OOM.

---

## 8.9 Common Exam Traps

| Trap | Correct answer |
|------|---------------|
| "Secret values can be printed in notebook output" | False — `print(secret)` shows `[REDACTED]` |
| "`dbutils.widgets.get()` fails if no widget exists" | True — unless a job parameter or widget default provides the value |
| "`%sh` runs on all cluster nodes" | False — `%sh` runs on the **driver node only** |
| "`%pip install` affects all notebooks on the cluster" | False — it's session-scoped (one notebook only) |
| "`display()` works in any Python environment" | False — `display()` is Databricks-specific |
| "`dbutils.notebook.exit()` accepts any data type" | False — it only accepts a **string** argument |
| "Mounts are the recommended way to access cloud storage" | False — UC Volumes and External Locations are the modern replacement |
| "`%run` can pass parameters to the called notebook" | False — `%run` shares scope; use `dbutils.notebook.run()` for parameters |

---

## Official Databricks Documentation Links

- [dbutils Reference](https://docs.databricks.com/dev-tools/databricks-utils.html)
- [dbutils.widgets](https://docs.databricks.com/dev-tools/databricks-utils.html#widgets-utility-dbutilswidgets)
- [dbutils.fs](https://docs.databricks.com/dev-tools/databricks-utils.html#file-system-utility-dbutilsfs)
- [dbutils.secrets](https://docs.databricks.com/dev-tools/databricks-utils.html#secrets-utility-dbutilssecrets)
- [dbutils.notebook](https://docs.databricks.com/dev-tools/databricks-utils.html#notebook-utility-dbutilsnotebook)
- [Notebook Workflows](https://docs.databricks.com/notebooks/notebook-workflows.html)
- [Magic Commands](https://docs.databricks.com/notebooks/notebooks-use.html#mix-languages)
- [%pip install in Notebooks](https://docs.databricks.com/libraries/notebooks-python-libraries.html)
- [display() Function](https://docs.databricks.com/notebooks/visualizations/index.html)
