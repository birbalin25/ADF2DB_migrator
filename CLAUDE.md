# CLAUDE.md — Project Context for AI Assistants

## Project Overview

This is an **ADF-to-Databricks migration framework** with two phases:

1. **Analysis Phase** — Two Databricks notebooks that parse ADF ARM templates, map components to Databricks equivalents, build dependency graphs, and assign migration phases using LLM-assisted analysis.
2. **Code Generation Phase** — Seven Claude Code skills that convert ADF components into a complete deployable Databricks Asset Bundle (DABs), driven by a structured prompt (`prompt.txt`).

## Architecture

### Phase 1 — Analysis (Databricks Notebooks)

```
01_adf_analyzer.py           →  Dependency_Analysis table (21 columns)
02_adf2db_mapping.py         →  Component_Mapping table (12 columns)
```

Each notebook reads from DBFS/Volumes (ARM template JSON) and/or upstream Unity Catalog tables, calls the Databricks Foundation Model API for enrichment, and writes a Delta table.

### Phase 2 — Code Generation (Claude Code Skills)

```
ARM Template + Dependency_Analysis table
        ↓
  prompt.txt workflow (10 steps)
        ↓
  databricks_migration/          ← complete DABs bundle
  ├── databricks.yml
  ├── resources/*.yml            (workflow YAML per pipeline)
  ├── src/notebooks/
  │   ├── ingestion/             (Copy → Auto Loader / JDBC)
  │   ├── transformations/       (Data Flow → DLT)
  │   └── orchestration/         (control flow stubs)
  └── .github/workflows/deploy.yml
```

The code generation phase filters by migration phase, so only components in the target phase are converted. Cross-phase dependencies produce stub placeholders.

## Key Conventions

### Notebook Format
- **Header**: `# Databricks notebook source`
- **Markdown cells**: `# MAGIC %md` followed by markdown content
- **Cell separators**: `# COMMAND ----------`
- **Widgets**: `dbutils.widgets.text()` / `dbutils.widgets.dropdown()` at the top
- **LLM calls**: `mlflow.deployments.get_deploy_client("databricks")` → `client.predict()`
- **Output**: `spark.createDataFrame(rows).write.format("delta").mode(...).saveAsTable(fqn)`
- **Summary cell**: Counter-based statistics printed at the end

### Code Style
- Pure Python helper functions (no Spark dependency) for testability
- JSON prompts to the LLM, JSON responses parsed with `{}`/`}` boundary detection
- All functions are defined inline in the notebook (no cross-notebook imports)
- Error handling: individual component failures don't stop the processing loop

### Test Conventions
- Tests live in `tests/` as pure Python (no Spark, no Databricks, no LLM)
- Functions are replicated from notebooks into test files for isolation
- Fixtures use `sample_arm_template.json` (a realistic ADF factory with 14+ component types)
- Module-scoped fixtures for expensive parsing operations
- Test classes grouped by function: `TestExpansion`, `TestComplexityScoring`, `TestMergeLogic`, etc.

## File Map

| Path | Purpose |
|------|---------|
| `notebooks/01_adf_analyzer.py` | Build dependency graph, assign phases/units, visualize |
| `notebooks/02_adf2db_mapping.py` | Parse ARM, expand components, map to Databricks, score complexity |
| `prompt.txt` | End-to-end migration prompt template for Claude Code |
| `.claude/skills/adf-arm-parser/` | Skill: Parse ARM templates into component inventory |
| `.claude/skills/adf-expression-translator/` | Skill: ADF expressions → Python / Spark SQL |
| `.claude/skills/adf-copy-converter/` | Skill: Copy Activities → ingestion notebooks |
| `.claude/skills/adf-dataflow-converter/` | Skill: Data Flows → DLT / PySpark notebooks |
| `.claude/skills/adf-pipeline-converter/` | Skill: Pipelines → Workflow YAML (DABs) |
| `.claude/skills/adf-trigger-converter/` | Skill: Triggers → cron schedules |
| `.claude/skills/adf-bundle-assembler/` | Skill: Assemble complete DABs bundle |
| `tests/test_component_mapping.py` | 91 tests for notebook 01 |
| `tests/test_dependency_analyzer.py` | 60 tests for notebook 02 |
| `tests/sample_arm_template.json` | Shared test fixture — full ARM template |
| `.github/workflows/ci.yml` | CI: lint → test on push/PR |
| `discovery/export_adf_artifacts.ps1` | PowerShell to export ADF artifacts from Azure |
| `discovery/adf_to_pyspark_prompt.md` | Manual LLM prompt template for ad-hoc conversions |

## ADF Component Types

14 supported ADF resource types:
`Pipeline`, `Activity`, `Dataset`, `LinkedService`, `DataFlow`, `Trigger`, `IntegrationRuntime`, `ChangeDataCapture`, `Credential`, `GlobalParameter`, `ManagedVirtualNetwork`, `ManagedPrivateEndpoint`, `PrivateEndpointConnection`, `Parameter`, `Variable`

Skipped types (no code generation): `Parameter`, `Variable`, `GlobalParameter`, `Credential`, `ManagedVirtualNetwork`, `ManagedPrivateEndpoint`

## Output Table Schemas

### Component_Mapping (12 columns)
`analysis_timestamp`, `data_factory_name`, `adf_component_type`, `adf_component_name`, `parent_component`, `adf_subtype`, `databricks_equivalent`, `migration_recommendation`, `complexity_score`, `complexity_reason`, `adf_config_summary`, `key_metrics`

### Dependency_Analysis (21 columns)
`analysis_timestamp`, `data_factory_name`, `component_type`, `component_name`, `depends_on`, `depended_on_by`, `dependency_count`, `dependent_count`, `is_independent`, `migration_phase`, `phase_reason`, `migration_risk`, `migration_notes`, `inferred_domain`, `activity_types`, `migration_unit`, `migration_unit_name`, `migration_unit_id`, `migration_unit_members`, `migration_unit_phase_plan`, `migration_unit_reason`

## LLM Configuration

- **Default endpoint**: `databricks-meta-llama-3-3-70b-instruct`
- **Client**: `mlflow.deployments.get_deploy_client("databricks")`
- **Temperature**: 0.05 (near-deterministic)
- **Response format**: JSON with `{}`/`}` boundary parsing (handles markdown fences, preamble)

## Running Tests

```bash
# All 151 tests
pytest tests/ -v

# Individual test files
pytest tests/test_component_mapping.py -v    # 91 tests
pytest tests/test_dependency_analyzer.py -v  # 60 tests
```

No external dependencies needed (just `pytest`). Tests are completely offline — no Spark, Databricks, or LLM calls.

## Common Modification Patterns

### Adding a new ADF component type
1. Add ARM resource type to `TYPE_MAP` in notebooks 01 and 02
2. Add expansion logic in notebook 01 (subtype extraction)
3. Add mapping rule to `MAPPING_RULES` / `SUBTYPE_OVERRIDES` in notebook 01
4. Add complexity scoring rules in `rule_based_complexity()` in notebook 01
5. Add the component to `sample_arm_template.json` and write tests

### Adding a new activity subtype
1. Add to `MAPPING_RULES` dict under `("Activity", "NewSubtype")` in notebook 01
2. Add complexity scoring logic in `rule_based_complexity()` in notebook 01
3. Add metrics extraction in `extract_metrics()` if needed
4. Write a synthetic test in `TestSyntheticActivityComplexity`

### Changing the LLM prompt
- Notebook 01: `SYSTEM_PROMPT` (complexity enrichment)
- Notebook 02: `PHASE_SYSTEM_PROMPT` (phase reasoning), `UNIT_SYSTEM_PROMPT` (migration units)

## Code Generation Phase (prompt.txt)

### Workflow Steps

The `prompt.txt` template drives a 10-step conversion workflow:

1. **Parse & Inventory** — Parse ARM template, produce component inventory with complexity scores
2. **Pipelines → Workflow YAML** — Generate DABs job YAML with activity-to-task DAG mapping
3. **Copy Activities → Ingestion Notebooks** — Auto Loader for files, Spark JDBC for databases, Lakeflow Connect for SaaS
4. **Data Flows → DLT Notebooks** — Parse DFL script, generate `@dp.table` / `@dp.materialized_view` decorators
5. **Expression Translation** — `@pipeline().parameters.X` → `dbutils.widgets.get()`, `@concat` → f-string, etc.
6. **Triggers → Schedules** — ScheduleTrigger → quartz cron, TumblingWindow → periodic, BlobEvent → file_arrival
7. **Integration Runtimes → Compute** — Managed IR → serverless, Self-Hosted IR → VNet injection docs
8. **Orchestration Notebooks** — WebActivity → requests, Lookup → spark.read, Until → while-loop, etc.
9. **Assemble DABs Bundle** — `databricks.yml` with targets, parameterized variables, CI/CD
10. **Migration Report** — Statistics, assumptions, automated decisions, manual items, risks

### Activity-to-Task Mapping

| ADF Activity | Databricks Equivalent |
|---|---|
| Copy | Auto Loader / JDBC ingestion notebook |
| ExecuteDataFlow | DLT pipeline notebook |
| ForEach | `for_each_task` |
| IfCondition | `condition_task` |
| Switch | Chained `condition_task` |
| Until | Notebook with while loop |
| ExecutePipeline | `run_job_task` |
| WebActivity | `requests` notebook |
| Lookup | `spark.read` notebook |
| SetVariable | `taskValues` notebook |
| Wait | `time.sleep` notebook |
| Fail | `raise Exception` notebook |

### Phase-Based Filtering

The code generation respects migration phases from the Dependency_Analysis table:

```sql
SELECT component_type, component_name, depends_on, migration_unit
FROM catalog.schema.Dependency_Analysis
WHERE migration_phase = 'Phase N'
```

- Only components in the target phase are converted
- Cross-phase dependencies produce stub placeholders: `TODO: Available after Phase N migration`
- Phase with only LinkedServices/Datasets (no pipelines) → ingestion notebooks only, skip workflow YAML

### Code Generation Conventions

- Unity Catalog 3-level namespace (`catalog.schema.table`) everywhere
- Credentials via `dbutils.secrets.get(scope, key)` — never hardcoded
- Serverless compute by default (no cluster config unless needed)
- Every notebook starts with `# Databricks notebook source`
- Unsupported activities (SSIS, AzureML, USql) flagged with `TODO` stubs
- Python is the default language unless explicitly specified

### Using the Prompt

1. Copy `prompt.txt` and customize the input variables (ARM path, table name, phase, catalog/schema)
2. Paste into Claude Code in this project directory
3. Claude Code will invoke the relevant skills and generate all files to disk
4. Deploy: `cd databricks_migration && databricks bundle validate --target dev && databricks bundle deploy --target dev`
