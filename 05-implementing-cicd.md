# Section 5: Implementing CI/CD

## Exam Objectives

- Manage your code development workflow within the Databricks workspace UI: creating and switching between branches in Databricks Repos, committing and pushing changes, and creating pull requests using Databricks Git integration
- Understand environment-specific configuration using Automation Bundle (formerly Databricks Asset Bundles) variables and overrides while promoting the same codebase across dev, test, and prod targets
- Deploy Declarative Automation Bundles (formerly Databricks Asset Bundles) to package, configure, and promote Lakeflow Jobs, Lakeflow Spark Declarative Pipelines, and other workspace assets across dev, test, and prod environments
- Understand the Databricks CLI to validate, deploy, and manage Declarative Automation Bundles and other workspace assets in automated CI/CD workflows

---

## 5.1 Databricks Repos (Git Folders)

Databricks Repos (also called **Git Folders**) integrates Git repositories directly into the Databricks workspace, enabling version-controlled development of notebooks, Python files, and other code.

### Supported Git providers

GitHub, GitHub Enterprise, GitLab, GitLab self-managed, Azure DevOps (Azure Repos), Bitbucket Cloud, Bitbucket Server, AWS CodeCommit.

### Git operations from the Workspace UI

| Operation | How to do it |
|-----------|-------------|
| **Clone a repo** | Workspace sidebar → Repos → Add repo → Enter Git URL |
| **Create a branch** | Repo file browser → branch name dropdown (top) → Create branch |
| **Switch branches** | Branch dropdown → select existing branch |
| **Stage + commit** | Branch dropdown → Git button → select changed files → commit message → Commit |
| **Push to remote** | After committing → Push button |
| **Pull from remote** | Git button → Pull |
| **Create pull request** | Git button → Create pull request → opens Git provider's PR UI in browser |
| **Resolve merge conflicts** | Git button → Resolve conflicts → edit files → mark resolved → commit |

### Supported file types in Repos

Databricks Repos supports any file tracked by Git:
- `.py` files (Python notebooks and scripts)
- `.sql` files
- `.yml` / `.yaml` files (bundle configs)
- `.ipynb` files (Jupyter notebooks)
- `.md`, `.txt`, `.json`, etc.

> **Note:** `.dbc` (Databricks notebook archive) files and legacy `.py` notebook format files are treated as notebooks, not scripts.

### Feature branch development workflow

```
main (prod-stable)
  │
  └── feature/add-silver-orders   ← developer works here
        │
        │  1. Develop & test in feature branch
        │  2. git commit -m "Add silver orders transform"
        │  3. git push origin feature/add-silver-orders
        │  4. Open PR → code review → CI tests pass
        │  5. Merge to main
        ▼
main (updated)
        │
        └── CD pipeline: databricks bundle deploy -t prod
```

### Best practices

- Work in **feature branches** — never commit directly to `main`
- Commit **code only** — don't commit cluster outputs or cell results
- Use **pull requests** with code review before merging to `main`
- Keep `databricks.yml` and resource YAML in version control
- Use `.gitignore` to exclude secrets, `.env` files, and `__pycache__`

---

## 5.2 Declarative Automation Bundles (DAB)

Declarative Automation Bundles (formerly **Databricks Asset Bundles / DAB**) package Databricks workspace assets into a version-controlled, deployable unit defined in YAML.

### What DAB packages

- **Lakeflow Jobs** (workflows)
- **Lakeflow Spark Declarative Pipelines** (LDP)
- **SQL queries**
- **Notebooks and Python files** (as source code, not binary)
- **Cluster configurations**
- **Dashboards**
- **Secrets (references only, not values)**

### Key capabilities

| Capability | Detail |
|------------|--------|
| **Single codebase** | Same code deploys to dev, test, prod via target overrides |
| **Variables** | Parameterize any value; overridden per target |
| **Validation** | `databricks bundle validate` catches config errors without deploying |
| **Idempotent deploy** | `databricks bundle deploy` only updates changed assets |
| **State tracking** | Bundle deployment state tracked in workspace |

### Bundle directory structure

```
my_project/
├── databricks.yml            # main bundle config (required)
├── resources/
│   ├── jobs/
│   │   └── daily_etl.yml     # job resource definition
│   └── pipelines/
│       └── bronze_pipeline.yml  # LDP pipeline definition
├── src/
│   ├── bronze_ingest.py      # source notebooks/scripts
│   ├── silver_transform.py
│   └── gold_aggregations.py
└── tests/
    ├── unit/
    │   └── test_transforms.py
    └── integration/
        └── test_pipeline.py
```

### `databricks.yml` — complete structure

```yaml
bundle:
  name: my_data_pipeline

# Reusable variables
variables:
  catalog:
    default: dev_catalog
    description: "Unity Catalog to write to"
  schema:
    default: dev_schema
  env:
    default: dev
  cluster_workers:
    default: 1
    description: "Number of worker nodes"

# Target environments
targets:
  dev:
    mode: development           # prefixes resources with ${workspace.current_user.short_name}
    default: true               # used when -t flag is omitted
    variables:
      catalog:          dev_catalog
      schema:           dev_schema
      env:              dev
      cluster_workers:  1
    workspace:
      host: https://adb-dev.azuredatabricks.net

  staging:
    mode: development
    variables:
      catalog:          staging_catalog
      schema:           staging_schema
      env:              staging
      cluster_workers:  2
    workspace:
      host: https://adb-staging.azuredatabricks.net

  prod:
    mode: production            # no username prefix; resources have exact names
    variables:
      catalog:          prod_catalog
      schema:           prod_schema
      env:              prod
      cluster_workers:  8
    workspace:
      host: https://adb-prod.azuredatabricks.net

# Resources included from separate files
include:
  - resources/jobs/daily_etl.yml
  - resources/pipelines/bronze_pipeline.yml
```

### Job resource YAML (`resources/jobs/daily_etl.yml`)

```yaml
resources:
  jobs:
    daily_etl_job:
      name: "Daily ETL [${var.env}]"

      schedule:
        quartz_cron_expression: "0 0 2 * * ?"
        timezone_id: "UTC"

      email_notifications:
        on_failure:
          - oncall@company.com

      max_concurrent_runs: 1

      tasks:
        - task_key: ingest_bronze
          notebook_task:
            notebook_path: ./src/bronze_ingest.py
            base_parameters:
              catalog: "${var.catalog}"
              schema:  "${var.schema}"
          job_cluster_key: shared_cluster

        - task_key: transform_silver
          depends_on:
            - task_key: ingest_bronze
          notebook_task:
            notebook_path: ./src/silver_transform.py
            base_parameters:
              catalog: "${var.catalog}"
          job_cluster_key: shared_cluster

      job_clusters:
        - job_cluster_key: shared_cluster
          new_cluster:
            spark_version:       "15.4.x-scala2.12"
            node_type_id:        "Standard_DS3_v2"
            num_workers:         ${var.cluster_workers}
            spark_conf:
              spark.sql.shuffle.partitions: "50"
```

### Variable substitution syntax

```yaml
# Reference a variable
catalog: ${var.catalog}
job_name: "My Job [${var.env}]"

# Reference workspace context
job_name: "${workspace.current_user.short_name}_etl"   # dev mode only
host: "${workspace.host}"

# Override at target level (target variable wins over bundle-level default)
targets:
  prod:
    variables:
      catalog: prod_catalog
      cluster_workers: 8
```

### Development mode vs Production mode

| | `mode: development` | `mode: production` |
|--|--------------------|--------------------|
| Resource naming | Prefixed with `[dev ${username}]` | Exact name as defined |
| Purpose | Each developer gets isolated resources | Shared production resources |
| Concurrent deploys | Multiple devs can deploy without conflict | One canonical deployment |
| Scheduling | Paused by default | Active as configured |

### Target-level overrides (beyond variables)

Any resource property can be overridden per target:

```yaml
targets:
  dev:
    resources:
      jobs:
        daily_etl_job:
          tasks:
            - task_key: ingest_bronze
              new_cluster:
                num_workers: 1    # smaller dev cluster

  prod:
    resources:
      jobs:
        daily_etl_job:
          tasks:
            - task_key: ingest_bronze
              new_cluster:
                num_workers: 8    # larger prod cluster
```

---

## 5.3 Databricks CLI

The Databricks CLI is a command-line tool for managing Databricks resources and deploying bundles in CI/CD pipelines.

### Installation

```bash
# Recommended: new unified CLI (v0.200+)
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# macOS (Homebrew)
brew tap databricks/tap
brew install databricks
```

### Authentication

```bash
# Interactive OAuth login (browser-based — for developers)
databricks auth login --host https://adb-xxxxx.azuredatabricks.net

# PAT (for service principals in CI/CD)
export DATABRICKS_HOST=https://adb-xxxxx.azuredatabricks.net
export DATABRICKS_TOKEN=dapixxx...

# Profile-based (~/.databrickscfg)
databricks auth login --host https://adb-xxx.azuredatabricks.net --profile prod
databricks bundle deploy -t prod --profile prod
```

### Bundle commands

| Command | What it does |
|---------|-------------|
| `databricks bundle validate` | Parse and validate `databricks.yml` — **does NOT deploy** |
| `databricks bundle validate -t prod` | Validate for a specific target |
| `databricks bundle deploy` | Deploy all resources to the default target |
| `databricks bundle deploy -t prod` | Deploy to `prod` target |
| `databricks bundle run <job_key>` | Trigger a deployed job run |
| `databricks bundle run <job_key> -t prod` | Run in prod target |
| `databricks bundle destroy` | Remove all deployed resources |
| `databricks bundle destroy -t prod` | Destroy prod resources |
| `databricks bundle summary` | Show deployed resource names and IDs |

```bash
# Typical CI/CD sequence
databricks bundle validate -t test                   # validate config
databricks bundle deploy -t test                     # deploy to test env
databricks bundle run daily_etl_job -t test          # run and wait
databricks bundle deploy -t prod                     # promote to prod
```

### Other useful CLI commands

```bash
# Workspace
databricks workspace ls /Repos/myuser/my_project
databricks workspace export /Repos/myuser/nb.py ./nb.py
databricks workspace import ./local.py /Repos/myuser/local.py

# Secrets
databricks secrets create-scope my-scope
databricks secrets put --scope my-scope --key db-password --string-value "secretvalue"
databricks secrets list --scope my-scope

# Clusters
databricks clusters list
databricks clusters start --cluster-id abc-123
databricks clusters get --cluster-id abc-123

# Jobs
databricks jobs list
databricks jobs get --job-id 12345
databricks jobs run-now --job-id 12345

# Pipelines
databricks pipelines list
databricks pipelines start --pipeline-id xyz-456
```

---

## 5.4 CI/CD Pipeline Pattern

### Standard CI/CD flow

```
Feature branch push
      │
      ▼
[CI — on pull_request]
  1. lint / format check
  2. pytest tests/unit/ (local or via Databricks Connect)
  3. databricks bundle validate -t staging
  4. databricks bundle deploy -t staging
  5. databricks bundle run integration_test_job -t staging
  6. ↓ all pass → PR approved → merge to main
      │
      ▼
[CD — on push to main]
  1. databricks bundle validate -t prod
  2. databricks bundle deploy -t prod
  ✓ Done — production updated
```

### GitHub Actions example

```yaml
# .github/workflows/ci-cd.yml
name: CI/CD Pipeline

on:
  pull_request:
    branches: [main]
  push:
    branches: [main]

jobs:
  ci:
    if: github.event_name == 'pull_request'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - uses: databricks/setup-cli@main  # installs Databricks CLI

      - name: Validate bundle (staging)
        env:
          DATABRICKS_HOST:  ${{ secrets.DATABRICKS_HOST_STAGING }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_STAGING }}
        run: databricks bundle validate -t staging

      - name: Deploy to staging
        env:
          DATABRICKS_HOST:  ${{ secrets.DATABRICKS_HOST_STAGING }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_STAGING }}
        run: databricks bundle deploy -t staging

      - name: Run integration tests
        env:
          DATABRICKS_HOST:  ${{ secrets.DATABRICKS_HOST_STAGING }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_STAGING }}
        run: databricks bundle run integration_test_job -t staging

  cd:
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: databricks/setup-cli@main

      - name: Deploy to production
        env:
          DATABRICKS_HOST:  ${{ secrets.DATABRICKS_HOST_PROD }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_PROD }}
        run: |
          databricks bundle validate -t prod
          databricks bundle deploy -t prod
```

> **Security:** Always use **service principals** (not personal PATs) for CI/CD authentication. Store tokens in CI/CD secret management (GitHub Secrets, Azure Key Vault, etc.), never in code or bundle YAML.

---

## 5.5 Secrets in Bundles

Secrets referenced in bundles are **never stored in YAML** — only references.

```yaml
# Reference a secret in a job parameter
tasks:
  - task_key: ingest
    notebook_task:
      notebook_path: ./src/ingest.py
      base_parameters:
        db_password: "{{secrets/my-scope/db-password}}"  # resolved at runtime
```

```python
# In the notebook — read via dbutils.secrets
password = dbutils.secrets.get(scope="my-scope", key="db-password")
```

---

## 5.6 Key Concepts Summary

| Concept | Key point |
|---------|-----------|
| Databricks Repos | Git-native versioning for notebooks; branches/commits/PRs from workspace UI |
| `databricks.yml` | Single source of truth for all bundle assets; version-controlled |
| `targets` | Named environments (dev/staging/prod) each with own variables and overrides |
| `variables` | Parameterize any value; default set at bundle level, overridden at target level |
| `mode: development` | Prefixes resource names with username — prevents dev/prod conflicts |
| `mode: production` | Resources have exact names as defined — for production deployments |
| `databricks bundle validate` | Catches YAML syntax errors and reference issues — **run before every deploy** |
| `databricks bundle deploy` | Idempotent — only updates what changed since last deploy |
| Service principal | Machine identity for CI/CD — never use personal PATs in pipelines |
| `databricks bundle destroy` | Removes all resources deployed by the bundle from the workspace |

---

## 5.7 Common Exam Traps

| Trap | Correct answer |
|------|---------------|
| "`databricks bundle validate` deploys the bundle" | False — validate only checks config; deploy does nothing |
| "`mode: development` is for lower environments only" | It can be used in any target; the key effect is username-prefixed resource names |
| "Bundle variables are the same as Spark config vars" | No — bundle variables are YAML-level substitution only; Spark config is separate |
| "Databricks Repos requires a separate license" | False — included with Databricks workspace |
| "`databricks bundle deploy` always redeploys everything" | False — it's idempotent and only updates changed resources |
| "Secrets can be stored in `databricks.yml`" | Never — only references (`{{secrets/scope/key}}`); actual values stay in Databricks Secrets |
