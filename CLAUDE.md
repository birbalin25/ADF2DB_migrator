# CLAUDE.md — Project Context for AI Assistants

## Project Overview

This is an **ADF-to-Databricks migration framework** — a two-notebook Databricks pipeline that analyzes Azure Data Factory ARM templates, maps components to Databricks equivalents, and builds dependency graphs with migration phases using LLM-assisted analysis.

## Architecture

```
01_adf_analyzer.py           →  Dependency_Analysis table (21 columns)
02_adf2db_mapping.py         →  Component_Mapping table (12 columns)
```

Each notebook reads from DBFS/Volumes (ARM template JSON) and/or upstream Unity Catalog tables, calls the Databricks Foundation Model API for enrichment, and writes a Delta table.

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
| `tests/test_component_mapping.py` | 91 tests for notebook 01 |
| `tests/test_dependency_analyzer.py` | 60 tests for notebook 02 |
| `tests/sample_arm_template.json` | Shared test fixture — full ARM template |
| `databricks.yml` | DABs config: 2-task job (`component_mapping` → `dependency_analysis`) |
| `.github/workflows/deploy.yml` | CI/CD: validate → test → deploy-dev → deploy-prod |
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

## DABs Configuration

- Bundle name: `adf-to-databricks-migration`
- Variables: `catalog` (default: `main`), `schema` (default: `migration`)
- Targets: `dev` (default, `dev_catalog`), `prod` (`prod_catalog`)
- Job: `adf_migration_analysis` with 2 sequential tasks
- Compute: Serverless

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
