---
name: adf-pipeline-converter
description: Convert ADF pipeline orchestration and control flow activities to Databricks Workflow job YAML (DABs format)
trigger_keywords:
  - pipeline
  - workflow
  - ForEach
  - IfCondition
  - Switch
  - Until
  - ExecutePipeline
  - control flow
  - orchestration
  - DAG
  - task dependency
input: ADF Pipeline JSON (from ARM template or standalone export)
output: Databricks Workflow YAML (DABs format) with task DAG, plus stub notebooks for complex logic
---

# ADF Pipeline Converter

Convert ADF pipeline orchestration and control flow to Databricks Workflow job YAML in DABs format.

## What This Skill Does

1. **Reads** ADF pipeline JSON (activities, parameters, variables, dependencies)
2. **Maps** each activity to a Databricks task type (see mapping table below)
3. **Builds** a task DAG from `dependsOn` relationships
4. **Converts** control flow (ForEach, IfCondition, Switch, Until) to native Workflow task types
5. **Translates** expressions using the `adf-expression-translator` skill patterns
6. **Generates** DABs-format YAML job definition + stub notebooks for tasks that need Python logic
7. **Handles** dependency conditions (Succeeded, Failed, Completed, Skipped)

## Activity → Task Type Mapping

| ADF Activity | Databricks Task Type | YAML Key | Notes |
|---|---|---|---|
| Pipeline (top-level) | Job | `jobs.<name>` | Top-level DABs resource |
| Copy | `notebook_task` | Notebook from `adf-copy-converter` | References generated ingestion notebook |
| ExecuteDataFlow | `notebook_task` | Notebook from `adf-dataflow-converter` | References generated DLT notebook |
| ExecutePipeline | `run_job_task` | `job_id` or `pipeline_id` | Reference child job + pass parameters |
| ForEach | `for_each_task` | `inputs`, `task` | `items` → `inputs` expression, `batchCount` → `concurrency` |
| IfCondition | `condition_task` | `condition`, `if`/`else` | Expression → Jinja condition |
| Switch | Chained `condition_task` | Multiple conditions | Each case → chained if/else |
| Until | `notebook_task` | While-loop notebook | Python `while` with condition + timeout |
| Wait | `notebook_task` | Sleep notebook | `time.sleep(seconds)` |
| SetVariable | `notebook_task` | taskValues notebook | `dbutils.jobs.taskValues.set()` |
| AppendVariable | `notebook_task` | taskValues notebook | Get + append + set |
| WebActivity | `notebook_task` | HTTP requests notebook | `requests.get/post()` with auth |
| Lookup | `notebook_task` | Spark read notebook | `spark.read` or `spark.sql()` |
| GetMetadata | `notebook_task` | Metadata notebook | `dbutils.fs.ls()` or `DESCRIBE` |
| Filter | `notebook_task` | Filter notebook | PySpark `.filter()` |
| Fail | `notebook_task` | Error notebook | `raise Exception(message)` |
| Script | `sql_task` or `notebook_task` | SQL or notebook | `spark.sql()` for SQL scripts |
| SqlServerStoredProcedure | `notebook_task` | JDBC notebook | `spark.read.jdbc()` with stored proc |
| AzureFunctionActivity | `notebook_task` | HTTP notebook | `requests` call to function endpoint |
| DatabricksNotebook | `notebook_task` | Direct reference | Update path to DABs-relative |
| DatabricksSparkPython | `spark_python_task` | Python file | Map `pythonFile` + `parameters` |
| DatabricksSparkJar | `spark_jar_task` | JAR reference | Map `mainClassName` + `parameters` |

## Step-by-Step Instructions

### Step 1: Parse Pipeline Structure

From the pipeline JSON, extract:
- `properties.activities[]` — flat list of activities
- `properties.parameters` — pipeline parameters → `job_parameters` in YAML
- `properties.variables` — pipeline variables → task value keys
- Activity `dependsOn[]` — build the task DAG edges

### Step 2: Build Task DAG

For each activity's `dependsOn`:
```json
{
  "activity": "PreviousTask",
  "dependencyConditions": ["Succeeded"]
}
```

Map to DABs YAML:
```yaml
tasks:
  - task_key: current_task
    depends_on:
      - task_key: previous_task
        outcome: success  # Succeeded → success, Failed → failure
```

**Dependency condition mapping:**
| ADF Condition | DABs `outcome` |
|---|---|
| `Succeeded` | `success` (default — can omit) |
| `Failed` | `failure` |
| `Completed` | (no outcome filter — always runs) |
| `Skipped` | `skipped` |

### Step 3: Convert Control Flow Activities

**ForEach → `for_each_task`:**
```yaml
tasks:
  - task_key: process_tables
    for_each_task:
      inputs: ${job.parameters.table_list}  # from items expression
      concurrency: 20  # from batchCount
      task:
        notebook_task:
          notebook_path: src/notebooks/orchestration/process_single_table.py
          base_parameters:
            item: "{{input}}"
```

**IfCondition → `condition_task`:**
```yaml
tasks:
  - task_key: check_rows
    condition_task:
      condition: "{{tasks.lookup_count.values.row_count}} > 0"
      if:
        - task_key: process_data
          notebook_task:
            notebook_path: src/notebooks/orchestration/process.py
      else:
        - task_key: send_alert
          notebook_task:
            notebook_path: src/notebooks/orchestration/alert.py
```

**Switch → chained `condition_task`:**
Convert each case to an if-else chain:
```yaml
tasks:
  - task_key: switch_env
    condition_task:
      condition: "{{job.parameters.env}} == 'dev'"
      if:
        - task_key: dev_processing
          notebook_task: ...
      else:
        - task_key: switch_env_prod
          condition_task:
            condition: "{{job.parameters.env}} == 'prod'"
            if:
              - task_key: prod_processing
                notebook_task: ...
            else:
              - task_key: default_processing
                notebook_task: ...
```

**Until → notebook with while loop:**
Generate a notebook:
```python
# Databricks notebook source
import time

timeout_seconds = 3600  # from Until timeout
poll_interval = 30
elapsed = 0

while elapsed < timeout_seconds:
    # Check condition (translated from ADF expression)
    result = dbutils.jobs.taskValues.get(taskKey="check_status", key="status")
    if result == "Complete":
        break
    time.sleep(poll_interval)
    elapsed += poll_interval

if elapsed >= timeout_seconds:
    raise TimeoutError("Until loop timed out")
```

### Step 4: Convert Parameters and Variables

**Pipeline parameters → Job parameters:**
```yaml
jobs:
  pipeline_name:
    parameters:
      - name: env
        default: dev
      - name: start_date
        default: ""
```

**Variables → Task values:**
Variables in ADF are mutable within a pipeline run. Map to `dbutils.jobs.taskValues`:
- `SetVariable` → `dbutils.jobs.taskValues.set(key="var_name", value=expr)`
- Read variable → `dbutils.jobs.taskValues.get(taskKey="set_var_task", key="var_name")`

### Step 5: Generate Output Files

For each pipeline, produce:

1. **Job YAML** at `resources/<pipeline_name>.yml`:
```yaml
resources:
  jobs:
    <pipeline_name>:
      name: <pipeline_name>
      tasks:
        - task_key: activity_1
          notebook_task:
            notebook_path: src/notebooks/orchestration/<notebook>.py
          depends_on: []
        - task_key: activity_2
          depends_on:
            - task_key: activity_1
          notebook_task:
            notebook_path: src/notebooks/ingestion/<notebook>.py
      parameters:
        - name: param1
          default: value1
      job_clusters:
        - job_cluster_key: default
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            num_workers: 1
```

2. **Stub notebooks** at `src/notebooks/orchestration/<activity_name>.py` for activities that need Python code (WebActivity, Lookup, Until loops, SetVariable, etc.)

### Step 6: Handle Edge Cases

- **Nested ForEach**: Inner ForEach in an outer ForEach → nested `for_each_task`
- **Activities with retry**: `policy.retry` → `retry_on_timeout: true` + `max_retries` in task
- **Timeout**: `policy.timeout` → `timeout_seconds` on the task
- **Activity outputs consumed downstream**: Generate `dbutils.jobs.taskValues.set()` at end of notebook

## YAML Output Format

The generated YAML follows DABs schema. See `references/workflow_yaml_schema.md` for the complete schema reference.

## Important Notes

- Each ADF pipeline becomes one Databricks Workflow Job
- Activities become tasks within that job
- The task DAG preserves the original execution order from `dependsOn`
- Expressions in conditions/parameters are translated using `adf-expression-translator` patterns
- Notebook tasks reference notebooks generated by other skills (copy-converter, dataflow-converter)
- For complex orchestration (deeply nested control flow), generate separate notebooks to keep YAML readable
