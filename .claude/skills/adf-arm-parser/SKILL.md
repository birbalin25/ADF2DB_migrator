---
name: adf-arm-parser
description: Parse ADF ARM template JSON into a structured component inventory with Databricks mapping recommendations and complexity scores
trigger_keywords:
  - ARM template
  - ADF export
  - migration assessment
  - inventory
  - analyze ADF
  - parse ADF
  - component mapping
input: ADF ARM template JSON (full factory export or single pipeline)
output: Structured inventory table with component types, subtypes, Databricks equivalents, complexity scores, and key metrics
---

# ADF ARM Parser

Parse an Azure Data Factory ARM template export and produce a complete component inventory with Databricks migration mappings.

## What This Skill Does

1. **Reads** the user's ADF ARM template JSON (full factory export or single pipeline)
2. **Expands** all resource types: pipelines → activities → parameters/variables, datasets, linked services, triggers, data flows, integration runtimes, CDC, credentials, global parameters, networking
3. **Maps** each component to its Databricks equivalent using the comprehensive mapping rules in `references/adf_mapping_rules.md`
4. **Scores** complexity (Low/Medium/High) based on 40+ rule-based factors
5. **Extracts** key metrics per component (activity counts, expression counts, connector types, etc.)
6. **Outputs** a structured inventory as a Markdown table and/or JSON file

## Step-by-Step Instructions

### Step 1: Locate the ARM Template

Ask the user for the path to their ADF ARM template JSON file. This is typically exported from Azure Portal → Data Factory → ARM template → Export template, or from source control.

The ARM template JSON has this structure:
```json
{
  "$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#",
  "contentVersion": "1.0.0.0",
  "parameters": { "factoryName": { "type": "string" } },
  "resources": [
    {
      "name": "[concat(parameters('factoryName'), '/PipelineName')]",
      "type": "Microsoft.DataFactory/factories/pipelines",
      "apiVersion": "2018-06-01",
      "properties": { ... }
    }
  ]
}
```

### Step 2: Parse All Resources

Read the ARM JSON and classify each resource using this type map:

| ARM Resource Type | Friendly Name |
|---|---|
| `Microsoft.DataFactory/factories/pipelines` | Pipeline |
| `Microsoft.DataFactory/factories/datasets` | Dataset |
| `Microsoft.DataFactory/factories/linkedservices` | LinkedService |
| `Microsoft.DataFactory/factories/triggers` | Trigger |
| `Microsoft.DataFactory/factories/dataflows` | DataFlow |
| `Microsoft.DataFactory/factories/integrationruntimes` | IntegrationRuntime |
| `Microsoft.DataFactory/factories/adfcdcs` | ChangeDataCapture |
| `Microsoft.DataFactory/factories/credentials` | Credential |
| `Microsoft.DataFactory/factories/globalParameters` | GlobalParameter |
| `Microsoft.DataFactory/factories/managedVirtualNetworks` | ManagedVirtualNetwork |
| `Microsoft.DataFactory/factories/managedVirtualNetworks/managedPrivateEndpoints` | ManagedPrivateEndpoint |
| `Microsoft.DataFactory/factories/privateEndpointConnections` | PrivateEndpointConnection |

**Name resolution**: Extract component names from ARM `name` field using pattern `[concat(parameters('factoryName'), '/ComponentName')]` — take the last segment after `/`.

### Step 3: Expand Pipeline Children

For each Pipeline resource, expand into child rows:
- **Activities**: Each activity in `properties.activities[]` becomes its own row with `parent_component = PipelineName`
- **Parameters**: Each entry in `properties.parameters` becomes a row
- **Variables**: Each entry in `properties.variables` becomes a row
- **Nested activities**: For ForEach, IfCondition (true/false branches), Switch (cases + default), and Until — recurse into inner activities

### Step 4: Map to Databricks Equivalents

For each component, look up the Databricks equivalent from `references/adf_mapping_rules.md`. The mapping uses:
1. **Primary key**: `(component_type, subtype)` — e.g., `(Activity, Copy)` → Auto Loader / COPY INTO
2. **Fallback**: `(component_type, *)` — generic mapping when subtype has no override
3. **Subtype detection**: From `properties.type` for activities, `properties.typeProperties.type` for datasets/linked services/triggers

### Step 5: Score Complexity

Apply rule-based complexity scoring. Score thresholds: 0-1 = Low, 2-4 = Medium, 5+ = High.

**Pipeline factors:**
- Activities > 10: +3, Activities > 4: +1
- Each ForEach/Until/Switch: +2
- Each IfCondition: +1
- Each ExecutePipeline: +2, Each ExecuteDataFlow: +1
- Expressions > 5: +1

**Activity factors:**
- ForEach/Until/Switch: +2, ExecutePipeline: +2, Custom: +3
- ExecuteSSISPackage: +4, AzureML*: +3, USql: +3, HDInsight*: +2
- Copy with staging: +1, Copy with column mappings: +1
- Expressions > 3: +1

**DataFlow factors:**
- Transformations >= 5: +3, >= 2: +1
- Has join: +1, Has pivot: +2, Has derived columns: +1

**Other component factors:**
- LinkedService with Key Vault: +1, parameterized: +1
- Dataset parameterized: +1, dynamic path: +1
- TumblingWindowTrigger: +2, CustomEventsTrigger: +2
- SelfHosted IR: +3
- CDC > 2 sources: +2, Microbatch mode: +1
- ManagedVirtualNetwork/PrivateEndpoint: +2

### Step 6: Extract Key Metrics

For each component, extract quantitative metrics as a JSON object:

**Pipeline**: activity_count, activity_types, parameter_count, variable_count, has_foreach, has_until, has_switch, has_ifcondition, has_execute_pipeline, has_dataflow, expression_count, max_dependency_depth

**Activity**: activity_type, has_retry, retry_count, dependency_count, expression_count. For Copy: has_staging, has_column_mapping, source_type, sink_type. For ForEach: is_sequential, inner_activity_count, batch_count. For ExecutePipeline: child_pipeline, wait_on_completion.

**DataFlow**: flow_type, source_count, sink_count, transformation_count, transformation_names, script_length, has_join, has_aggregate, has_pivot, has_derived_column

**Dataset**: dataset_type, linked_service, parameter_count, has_schema, has_dynamic_path

**LinkedService**: service_type, uses_key_vault, has_parameters, auth_type

**Trigger**: trigger_type, pipeline_count, has_retry_policy, frequency, interval

**IntegrationRuntime**: ir_type, node_size, node_count

**ChangeDataCapture**: source_count, target_count, mode

### Step 7: Output the Inventory

Present the inventory as a Markdown table with columns:
- `data_factory_name`
- `component_type`
- `component_name`
- `parent_component`
- `subtype`
- `databricks_equivalent`
- `complexity` (Low/Medium/High)
- `key_metrics` (summary)

Also write the full inventory as JSON to `<output_dir>/adf_inventory.json` with all fields including `complexity_reason`, `adf_config_summary`, and full `key_metrics`.

Present a **summary** at the end:
- Total components by type
- Complexity distribution (Low/Medium/High counts)
- Components requiring manual migration (SSIS, AzureML, USql)
- Recommended migration phases

## Important Notes

- This skill runs **locally** — it reads JSON files and writes inventory files. No Spark cluster needed.
- The inventory output is consumed by downstream skills (`adf-pipeline-converter`, `adf-copy-converter`, etc.)
- For expressions embedded in component configs, flag them for the `adf-expression-translator` skill
- Components of type Parameter, Variable, GlobalParameter, Credential, ManagedVirtualNetwork, ManagedPrivateEndpoint are tracked but typically don't need standalone code generation — they are handled as part of their parent's conversion
