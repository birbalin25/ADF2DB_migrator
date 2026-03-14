# ADF-to-Databricks Migration Framework

An AI-assisted migration analysis pipeline that converts **Azure Data Factory (ADF)** ARM template exports into a comprehensive migration plan. The framework parses every ADF component, maps it to a Databricks equivalent, scores complexity, builds a dependency graph, and assigns migration phases and units using the Databricks Foundation Model API.

## How It Works

The framework runs as a two-step Databricks Workflow, where each notebook builds on the output of the previous one:

```
ARM Template (JSON)
        |
        v
+-------------------------------+
|  01  Component Mapping        |   Parse ARM template, expand all ADF
|      (Rule-based + LLM)       |   components, map to Databricks equivalents,
|                                |   score complexity (Low/Medium/High)
+-------------------------------+
        |  Component_Mapping table
        v
+-------------------------------+
|  02  Dependency Analyzer      |   Build directed dependency graph,
|      (Graph + LLM)            |   assign migration phases (topological),
|                                |   classify migration units, visualize
+-------------------------------+
        |  Dependency_Analysis table
        v
  Migration plan with phases, units,
  and dependency-ordered execution
```

## Project Structure

```
adf-to-databricks-migration/
├── notebooks/
│   ├── 01_adf_analyzer.py             # Dependencies + phases + units
│   └── 02_adf2db_mapping.py            # Parse + map + score
├── tests/
│   ├── __init__.py
│   ├── sample_arm_template.json        # Test fixture (realistic ADF factory)
│   ├── test_component_mapping.py       # 91 tests for notebook 01
│   └── test_dependency_analyzer.py     # 60 tests for notebook 02
├── discovery/
│   ├── export_adf_artifacts.ps1        # PowerShell script to export ADF
│   └── adf_to_pyspark_prompt.md        # Manual LLM prompt template
├── databricks.yml                      # Databricks Asset Bundles config
├── .github/workflows/deploy.yml        # CI/CD pipeline
├── CLAUDE.md                           # AI assistant project context
└── README.md                           # This file
```

## Getting Started

### Prerequisites

- A Databricks workspace with Unity Catalog enabled
- A Foundation Model API endpoint (default: `databricks-meta-llama-3-3-70b-instruct`)
- An ADF ARM template export (JSON)
- Databricks CLI installed (for DABs deployment)

### Step 1: Export Your ADF ARM Template

**Option A — ADF Studio UI:**
1. Open ADF Studio
2. Go to **Manage** > **ARM Template** > **Export ARM Template**
3. Download the JSON file

**Option B — PowerShell (bulk export):**
```powershell
./discovery/export_adf_artifacts.ps1 `
    -ResourceGroupName "my-rg" `
    -DataFactoryName "my-adf" `
    -OutputDir "./adf-export"
```

### Step 2: Upload the ARM Template

Upload the exported JSON to a Databricks Volume or DBFS path:

```
/Volumes/main/default/migration/arm_template.json
```

### Step 3: Deploy the Workflow

```bash
# Validate the bundle
databricks bundle validate --target dev

# Deploy to dev
databricks bundle deploy --target dev

# Run the job
databricks bundle run adf_migration_analysis --target dev
```

Or run each notebook individually from the Databricks workspace UI.

### Step 4: Review the Output

After the workflow completes, two Unity Catalog tables are available:

| Table | Contents |
|---|---|
| `Component_Mapping` | Every ADF component with its Databricks equivalent and complexity score |
| `Dependency_Analysis` | Dependency edges, migration phases, migration units, risk levels |

## Configuration

### Notebook Parameters

All parameters have sensible defaults and can be overridden via Databricks widgets or DABs `base_parameters`.

**Notebook 01 — Component Mapping:**

| Parameter | Default | Description |
|---|---|---|
| `input_mode` | `arm_template` | `arm_template` or `single_pipeline` |
| `input_path` | `/Volumes/main/default/migration/arm_template.json` | Path to ARM JSON |
| `factory_name` | _(auto-detected)_ | Override the factory name |
| `llm_endpoint` | `databricks-meta-llama-3-3-70b-instruct` | Foundation Model endpoint |
| `output_catalog` | `bircatalog` | Unity Catalog for output |
| `output_schema` | `birschema` | Schema for output |
| `output_table` | `Component_Mapping` | Table name |
| `output_write_mode` | `overwrite` | `overwrite` or `append` |

**Notebook 02 — Dependency Analyzer:**

Same as notebook 01 plus:

| Parameter | Default | Description |
|---|---|---|
| `render_graph` | `yes` | Generate dependency graph visualizations |

### DABs Targets

| Target | Workspace | Catalog |
|---|---|---|
| `dev` (default) | `https://adb-dev.azuredatabricks.net` | `dev_catalog` |
| `prod` | `https://adb-prod.azuredatabricks.net` | `prod_catalog` |

## ADF Component Coverage

The framework supports **14 ADF resource types** and **30+ activity subtypes**:

**Fully mapped (code generated):**
- Pipelines, Activities (Copy, ExecuteDataFlow, ForEach, IfCondition, Switch, Until, ExecutePipeline, Lookup, GetMetadata, WebActivity, Script, SqlServerStoredProcedure, Custom, Delete, Filter, Fail, WebHook, DatabricksNotebook, DatabricksSparkPython, DatabricksSparkJar, SynapseNotebook, AzureMLExecutePipeline, ExecuteSSISPackage, USql, HDInsight*)
- Datasets, LinkedServices, DataFlows, Triggers, IntegrationRuntimes, ChangeDataCaptures

**Skipped (instructions only):**
- Parameters, Variables, GlobalParameters (captured in pipeline YAML)
- Credentials, ManagedVirtualNetworks, ManagedPrivateEndpoints (manual setup steps)

## Testing

All tests are pure Python — no Spark, Databricks, or LLM dependencies required.

```bash
# Run all tests (151 total)
pytest tests/ -v

# Run only component mapping tests (91 tests)
pytest tests/test_component_mapping.py -v

# Run only dependency analyzer tests (60 tests, requires networkx)
pytest tests/test_dependency_analyzer.py -v
```

## CI/CD

The GitHub Actions workflow (`.github/workflows/deploy.yml`) runs on push/PR to `main`:

1. **Validate** — `databricks bundle validate --target dev`
2. **Test** — `pytest tests/ -v --tb=short`
3. **Deploy Dev** — `databricks bundle deploy --target dev` (push to main only)
4. **Deploy Prod** — `databricks bundle deploy --target prod` (after dev succeeds)

## Technology Stack (2026)

The generated code uses current Databricks capabilities:

- **Lakeflow Spark Declarative Pipelines (SDP)** — replaces DLT (`from pyspark import pipelines as dp`)
- **Serverless compute** — GA for notebooks, jobs, and SDP
- **Auto Loader file events mode** — replaces `cloudFiles.useIncrementalListing`
- **Lakeflow Connect** — GA connectors for Salesforce, Workday, SQL Server, ServiceNow
- **DABs** — 22+ resource types (job, pipeline, catalog, schema, volume, secret_scope, etc.)
- **Workflow triggers** — cron, file arrival, table update (with `wait_after_last_change_seconds`)
