# ADF-to-Databricks Migration Framework

An AI-assisted migration framework that converts **Azure Data Factory (ADF)** ARM template exports into a fully deployable **Databricks Asset Bundle (DABs)**. The framework operates in two phases: (1) analyze and plan using Databricks notebooks with LLM enrichment, then (2) generate all Databricks code using Claude Code skills.

## How It Works

### Phase 1 — Analysis ( Databricks Notebooks )

Two notebooks run on Databricks to analyze the ADF factory and produce a migration plan:

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

### Phase 2 — Code Generation (Claude Code Skills)

Using the analysis output and the original ARM template, Claude Code converts ADF components into a complete deployable DABs project:

```
ARM Template + Dependency_Analysis table
        |
        v
  ┌─────────────────────────────────────────────┐
  │  Claude Code Skills (prompt.txt workflow)    │
  │                                              │
  │  1. Parse & Inventory (adf-arm-parser)       │
  │  2. Pipelines → Workflow YAML                │
  │  3. Copy Activities → Ingestion Notebooks    │
  │  4. Data Flows → DLT Notebooks               │
  │  5. Expressions → Python/Spark SQL           │
  │  6. Triggers → Cron Schedules                │
  │  7. Integration Runtimes → Compute Config    │
  │  8. Control Flow → Orchestration Notebooks   │
  │  9. Assemble DABs Bundle                     │
  │ 10. Migration Report                         │
  └─────────────────────────────────────────────┘
        |
        v
  databricks_migration/
  ├── databricks.yml
  ├── resources/*.yml
  ├── src/notebooks/
  │   ├── ingestion/
  │   ├── transformations/
  │   └── orchestration/
  └── .github/workflows/deploy.yml
```

## Project Structure

```
ADF2DB_migrator/
├── notebooks/
│   ├── 01_adf_analyzer.py             # Dependencies + phases + units
│   └── 02_adf2db_mapping.py           # Parse + map + score
├── .claude/
│   └── skills/                        # Claude Code skills for code generation
│       ├── adf-arm-parser/            # Parse ARM templates into component inventory
│       ├── adf-expression-translator/ # ADF expressions → Python/Spark SQL
│       ├── adf-copy-converter/        # Copy activities → ingestion notebooks
│       ├── adf-dataflow-converter/    # Data Flows → DLT/PySpark notebooks
│       ├── adf-pipeline-converter/    # Pipelines → Workflow YAML (DABs)
│       ├── adf-trigger-converter/     # Triggers → cron schedules
│       └── adf-bundle-assembler/      # Assemble complete DABs bundle
├── tests/
│   ├── __init__.py
│   ├── sample_arm_template.json       # Test fixture (realistic ADF factory)
│   ├── test_component_mapping.py      # 91 tests for notebook 01
│   └── test_dependency_analyzer.py    # 60 tests for notebook 02
├── discovery/
│   ├── export_adf_artifacts.ps1       # PowerShell script to export ADF
│   └── adf_to_pyspark_prompt.md       # Manual LLM prompt template
├── prompt.txt                         # End-to-end migration prompt template
├── .github/workflows/ci.yml           # CI pipeline (test on push/PR)
├── CLAUDE.md                          # AI assistant project context
└── README.md                          # This file
```

## Getting Started

### Prerequisites

- A Databricks workspace with Unity Catalog enabled
- A Foundation Model API endpoint (default: `databricks-meta-llama-3-3-70b-instruct`)
- An ADF ARM template export (JSON)
- [Claude Code](https://docs.anthropic.com/en/docs/claude-code) CLI (for Phase 2 code generation)

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

### Step 3: Run the Analysis Notebooks

Run each notebook from the Databricks workspace UI, or import and execute them in order.

After the notebooks complete, two Unity Catalog tables are available:

| Table | Contents |
|---|---|
| `Component_Mapping` | Every ADF component with its Databricks equivalent and complexity score |
| `Dependency_Analysis` | Dependency edges, migration phases, migration units, risk levels |

### Step 4: Generate Databricks Code with Claude Code

Open Claude Code in this project directory and paste the contents of `prompt.txt`, customizing:

1. **ARM Template path** — local path to your exported ARM JSON
2. **Analyzer Output Table** — the fully qualified `Dependency_Analysis` table from Step 3
3. **Migration Phase** — which phase to convert (e.g., `Phase 1`)
4. **Target Configuration** — Unity Catalog, schema, and output directory

Claude Code will use the project skills to:
- Filter components by migration phase from the Dependency_Analysis table
- Convert each ADF component type to its Databricks equivalent
- Generate a complete DABs bundle with notebooks, workflow YAML, and CI/CD

### Step 5: Deploy

```bash
cd databricks_migration
databricks bundle validate --target dev
databricks bundle deploy --target dev
```

## Claude Code Skills

The `.claude/skills/` directory contains 7 specialized skills that power the code generation phase:

| Skill | Purpose |
|---|---|
| `adf-arm-parser` | Parse ARM template JSON into structured component inventory with Databricks mapping recommendations and complexity scores |
| `adf-expression-translator` | Translate ADF Expression Language functions and system variables to Python or Spark SQL equivalents |
| `adf-copy-converter` | Convert ADF Copy Activities with source/sink connectors, datasets, and linked services to Databricks ingestion notebooks |
| `adf-dataflow-converter` | Convert ADF Mapping Data Flows to Lakeflow Declarative Pipelines (DLT) or PySpark notebooks |
| `adf-pipeline-converter` | Convert ADF pipeline orchestration and control flow activities to Databricks Workflow job YAML (DABs format) |
| `adf-trigger-converter` | Convert ADF triggers and Integration Runtime configs to Databricks Workflow schedules and cluster configurations |
| `adf-bundle-assembler` | Assemble all converted artifacts into a complete Databricks Asset Bundle project ready for deployment |

### Phase-Based Migration

The code generation workflow respects migration phases from the analysis output:

- Only components matching the target phase are converted
- Cross-phase dependencies produce stub placeholders (`TODO: Available after Phase N migration`)
- If a phase contains only LinkedServices/Datasets (no pipelines), only ingestion notebooks and infrastructure are generated

## Configuration

### Notebook Parameters (Phase 1)

All parameters have sensible defaults and can be overridden via Databricks widgets.

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

### Code Generation Parameters (Phase 2)

Configured in `prompt.txt` before running with Claude Code:

| Parameter | Example | Description |
|---|---|---|
| ARM Template path | `/path/to/ARMTemplateForFactory.json` | Local path to exported ARM JSON |
| Analyzer Output Table | `catalog.schema.Dependency_Analysis` | Fully qualified table from Phase 1 |
| Migration Phase | `Phase 1` | Which phase to convert |
| Unity Catalog | `bircatalog` | Target catalog for generated code |
| Schema | `birschema` | Target schema for generated code |
| Output Directory | `/databricks_migration` | Where to write the DABs bundle |

### Generated Output Structure

```
databricks_migration/
├── databricks.yml                      # DABs config with dev/stage/prod targets
├── resources/
│   └── *.yml                           # One workflow YAML per pipeline
├── src/notebooks/
│   ├── ingestion/                      # Copy activity → Auto Loader / JDBC notebooks
│   ├── transformations/                # Data Flow → DLT notebooks
│   └── orchestration/                  # Control flow stubs (WebActivity, Lookup, etc.)
└── .github/workflows/deploy.yml        # CI/CD pipeline
```

### Code Generation Conventions

- All code uses Unity Catalog 3-level namespace (`catalog.schema.table`)
- All credentials via `dbutils.secrets.get(scope, key)` — never hardcoded
- Serverless compute by default (no cluster config unless needed)
- Every notebook starts with `# Databricks notebook source`
- Unsupported activities (SSIS, AzureML, USql) get `TODO` stubs

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

The GitHub Actions workflow (`.github/workflows/ci.yml`) runs on push/PR to `main`:

1. **Test** — `pytest tests/ -v --tb=short`

## Migration Report

After code generation, Claude Code produces a migration report containing:

- Total ADF components discovered vs. converted
- Number of generated notebooks and workflow jobs
- Warnings and manual migration items
- **Assumptions** — schema inference, column mappings, source formats, compute config
- **Automated decisions** — Auto Loader for file ingestion, serverless compute, DLT for Data Flows
- **Manual validation items** — external API integrations, custom scripts, unsupported activities
- **Migration risks** — schema drift, runtime dependencies, credential config, VNet requirements
