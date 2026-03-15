---
name: adf-bundle-assembler
description: Assemble all converted Databricks artifacts into a complete Databricks Asset Bundle (DABs) project ready for deployment
trigger_keywords:
  - DABs
  - bundle
  - databricks.yml
  - deploy
  - CI/CD
  - Asset Bundle
  - assemble
  - package
  - project
input: All converted artifacts from skills 2-6 (workflow YAML, notebooks, schedules)
output: Complete DABs project directory with databricks.yml, resources, notebooks, and CI/CD config
---

# ADF Bundle Assembler

Assemble all converted Databricks artifacts into a deployable Databricks Asset Bundle (DABs) project.

## What This Skill Does

1. **Collects** all converted artifacts (workflow YAML, notebooks, schedules, cluster configs)
2. **Generates** the `databricks.yml` bundle manifest with variables, targets, and resource includes
3. **Organizes** notebooks into the standard directory structure (ingestion, transformations, orchestration)
4. **Creates** per-job resource YAML files with task definitions, schedules, and compute
5. **Generates** CI/CD pipeline configuration (GitHub Actions)
6. **Produces** a deployment-ready project that can be deployed with `databricks bundle deploy`

## Output Directory Structure

```
<project_name>/
├── databricks.yml                    # Bundle manifest
├── resources/
│   ├── <pipeline1_name>.yml          # Job definition per ADF pipeline
│   ├── <pipeline2_name>.yml
│   └── <dlt_pipeline_name>.yml       # DLT pipeline definitions
├── src/
│   └── notebooks/
│       ├── ingestion/                # From adf-copy-converter
│       │   ├── copy_sales_data.py
│       │   └── copy_customer_data.py
│       ├── transformations/          # From adf-dataflow-converter
│       │   ├── sales_transform.py
│       │   └── customer_enrichment.py
│       └── orchestration/            # From adf-pipeline-converter
│           ├── wait_for_completion.py
│           ├── call_api.py
│           └── error_handler.py
├── .github/
│   └── workflows/
│       └── deploy.yml                # CI/CD pipeline
└── README.md                         # Migration summary (optional)
```

## Step-by-Step Instructions

### Step 1: Inventory All Converted Artifacts

Collect outputs from each converter skill:
- **adf-pipeline-converter**: Job YAML files + orchestration notebooks
- **adf-copy-converter**: Ingestion notebooks
- **adf-dataflow-converter**: DLT/transformation notebooks
- **adf-trigger-converter**: Schedule YAML sections + cluster configs
- **adf-expression-translator**: (embedded in above notebooks)

### Step 2: Generate databricks.yml

Use the template from `assets/databricks.yml.template` and customize:

```yaml
bundle:
  name: <project_name>

variables:
  catalog:
    description: "Unity Catalog name"
    default: "main"
  schema:
    description: "Schema name for migrated tables"
    default: "migration"

include:
  - resources/*.yml

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: ${var.dev_host}
    variables:
      catalog: "dev_catalog"
      schema: "dev_migration"

  staging:
    workspace:
      host: ${var.staging_host}
    variables:
      catalog: "staging_catalog"
      schema: "staging_migration"

  prod:
    mode: production
    workspace:
      host: ${var.prod_host}
    variables:
      catalog: "prod_catalog"
      schema: "prod_migration"
    run_as:
      service_principal_name: "sp-data-engineering"
```

**Customization rules:**
- `bundle.name` → derived from ADF factory name (sanitized to lowercase alphanumeric + hyphens)
- `variables` → include `catalog` and `schema` at minimum; add more if ADF had global parameters
- `include` → always `resources/*.yml`
- `targets` → generate dev (default) + prod at minimum; add staging if multiple ADF environments detected
- `run_as` → add for prod target (use service principal)

### Step 3: Generate Per-Job Resource YAML

For each ADF pipeline converted to a Databricks Workflow, create `resources/<job_name>.yml`:

```yaml
resources:
  jobs:
    sales_data_pipeline:
      name: "Sales Data Pipeline"
      description: "Migrated from ADF pipeline: SalesDataPipeline"

      # Schedule (from trigger converter)
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: "UTC"

      # Parameters (from pipeline parameters)
      parameters:
        - name: catalog
          default: ${var.catalog}
        - name: schema
          default: ${var.schema}
        - name: environment
          default: "dev"

      # Tasks (from pipeline converter)
      tasks:
        - task_key: copy_sales_data
          notebook_task:
            notebook_path: src/notebooks/ingestion/copy_sales_data.py
            base_parameters:
              catalog: "{{job.parameters.catalog}}"
              schema: "{{job.parameters.schema}}"

        - task_key: transform_sales
          depends_on:
            - task_key: copy_sales_data
          notebook_task:
            notebook_path: src/notebooks/transformations/sales_transform.py
            base_parameters:
              catalog: "{{job.parameters.catalog}}"
              schema: "{{job.parameters.schema}}"

      # Tags
      tags:
        source: "adf-migration"
        original_pipeline: "SalesDataPipeline"

      # Notifications
      email_notifications:
        on_failure: []  # TODO: Configure team email

      max_concurrent_runs: 1

      queue:
        enabled: true
```

For DLT pipelines, create `resources/<dlt_pipeline_name>.yml`:
```yaml
resources:
  pipelines:
    sales_transform_dlt:
      name: "Sales Transform DLT"
      description: "Migrated from ADF DataFlow: SalesTransformFlow"
      target: "${var.catalog}.${var.schema}"
      libraries:
        - notebook:
            path: src/notebooks/transformations/sales_transform.py
      continuous: false
      development: true
      channel: PREVIEW
```

### Step 4: Organize Notebooks

Move/verify all generated notebooks are in the correct locations:
- `src/notebooks/ingestion/` — all notebooks from `adf-copy-converter`
- `src/notebooks/transformations/` — all notebooks from `adf-dataflow-converter`
- `src/notebooks/orchestration/` — all stub notebooks from `adf-pipeline-converter`

Ensure each notebook has the Databricks notebook header:
```python
# Databricks notebook source
```

### Step 5: Generate CI/CD Pipeline

Use the template from `assets/deploy.yml.template`:

```yaml
name: Deploy Databricks Bundle

on:
  push:
    branches:
      - main
  pull_request:
    branches:
      - main

jobs:
  validate:
    name: Validate Bundle
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle validate
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}

  deploy-dev:
    name: Deploy to Dev
    needs: validate
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    environment: dev
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle deploy --target dev
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}

  deploy-prod:
    name: Deploy to Prod
    needs: deploy-dev
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    environment: prod
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle deploy --target prod
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST_PROD }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_PROD }}
```

### Step 6: Generate Migration Summary

Print a summary table:

```
Migration Summary
=================
ADF Factory: <factory_name>
Bundle Name: <bundle_name>
Generated: <timestamp>

Artifacts:
  Jobs (Workflows):     X
  DLT Pipelines:        Y
  Ingestion Notebooks:  A
  Transform Notebooks:  B
  Orchestration Notebooks: C
  Total Notebooks:      A+B+C

Complexity:
  Low:    X components
  Medium: Y components
  High:   Z components

Manual Review Required:
  - <component_name>: <reason>
  - ...

Deployment:
  1. cd <project_name>
  2. databricks bundle validate --target dev
  3. databricks bundle deploy --target dev
  4. databricks bundle run <job_name> --target dev
```

## Important Notes

- All `databricks.yml` variable references use `${var.name}` syntax
- Resource YAML files use `{{job.parameters.name}}` for task parameter references
- Notebook paths in YAML are relative to the bundle root
- The `run_as` field in prod target requires a service principal — leave as placeholder if unknown
- Secrets must be configured in Databricks before deployment (`dbutils.secrets.get()`)
- See `references/dabs_schema.md` for the complete DABs schema reference
