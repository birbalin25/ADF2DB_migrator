# ADF Control Flow → Databricks Patterns

## ForEach → for_each_task

### ADF Pattern
```json
{
  "name": "ForEachTable",
  "type": "ForEach",
  "typeProperties": {
    "isSequential": false,
    "items": {
      "value": "@pipeline().parameters.tableList",
      "type": "Expression"
    },
    "batchCount": 20,
    "activities": [
      {
        "name": "CopyTable",
        "type": "Copy",
        "typeProperties": {
          "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": "@concat('SELECT * FROM ', item().schema, '.', item().table)"
          },
          "sink": { "type": "ParquetSink" }
        }
      }
    ]
  }
}
```

### Databricks Pattern
```yaml
tasks:
  - task_key: for_each_table
    for_each_task:
      inputs: "{{job.parameters.table_list}}"
      concurrency: 20
      task:
        task_key: copy_table
        notebook_task:
          notebook_path: src/notebooks/ingestion/copy_table.py
          base_parameters:
            schema_name: "{{input.schema}}"
            table_name: "{{input.table}}"
```

Notebook `copy_table.py`:
```python
# Databricks notebook source
schema_name = dbutils.widgets.get("schema_name")
table_name = dbutils.widgets.get("table_name")

df = (spark.read
    .format("jdbc")
    .option("url", dbutils.secrets.get("scope", "jdbc_url"))
    .option("dbtable", f"{schema_name}.{table_name}")
    .load())

df.write.mode("overwrite").saveAsTable(f"catalog.schema.{table_name}")
```

## IfCondition → condition_task

### ADF Pattern
```json
{
  "name": "CheckRowCount",
  "type": "IfCondition",
  "dependsOn": [{ "activity": "LookupCount", "dependencyConditions": ["Succeeded"] }],
  "typeProperties": {
    "expression": {
      "value": "@greater(activity('LookupCount').output.firstRow.count, 0)",
      "type": "Expression"
    },
    "ifTrueActivities": [
      { "name": "ProcessData", "type": "Copy", "..." : "..." }
    ],
    "ifFalseActivities": [
      { "name": "LogEmpty", "type": "WebActivity", "..." : "..." }
    ]
  }
}
```

### Databricks Pattern
```yaml
tasks:
  - task_key: lookup_count
    notebook_task:
      notebook_path: src/notebooks/orchestration/lookup_count.py

  - task_key: check_row_count
    depends_on:
      - task_key: lookup_count
    condition_task:
      condition: "{{tasks.lookup_count.values.count}} > 0"
      if:
        - task_key: process_data
          notebook_task:
            notebook_path: src/notebooks/ingestion/process_data.py
      else:
        - task_key: log_empty
          notebook_task:
            notebook_path: src/notebooks/orchestration/log_empty.py
```

## Switch → Chained condition_task

### ADF Pattern
```json
{
  "name": "SwitchEnv",
  "type": "Switch",
  "typeProperties": {
    "on": { "value": "@pipeline().parameters.environment", "type": "Expression" },
    "cases": [
      { "value": "dev", "activities": [{ "name": "DevProcess", "..." : "..." }] },
      { "value": "staging", "activities": [{ "name": "StagingProcess", "..." : "..." }] },
      { "value": "prod", "activities": [{ "name": "ProdProcess", "..." : "..." }] }
    ],
    "defaultActivities": [{ "name": "DefaultProcess", "..." : "..." }]
  }
}
```

### Databricks Pattern
```yaml
tasks:
  - task_key: switch_env_dev
    condition_task:
      condition: "{{job.parameters.environment}} == 'dev'"
      if:
        - task_key: dev_process
          notebook_task:
            notebook_path: src/notebooks/orchestration/dev_process.py
      else:
        - task_key: switch_env_staging
          condition_task:
            condition: "{{job.parameters.environment}} == 'staging'"
            if:
              - task_key: staging_process
                notebook_task:
                  notebook_path: src/notebooks/orchestration/staging_process.py
            else:
              - task_key: switch_env_prod
                condition_task:
                  condition: "{{job.parameters.environment}} == 'prod'"
                  if:
                    - task_key: prod_process
                      notebook_task:
                        notebook_path: src/notebooks/orchestration/prod_process.py
                  else:
                    - task_key: default_process
                      notebook_task:
                        notebook_path: src/notebooks/orchestration/default_process.py
```

## Until → Notebook with while Loop

### ADF Pattern
```json
{
  "name": "WaitForCompletion",
  "type": "Until",
  "typeProperties": {
    "expression": {
      "value": "@equals(activity('CheckStatus').output.firstRow.status, 'Complete')",
      "type": "Expression"
    },
    "timeout": "0.01:00:00",
    "activities": [
      {
        "name": "CheckStatus",
        "type": "Lookup",
        "typeProperties": {
          "source": {
            "type": "AzureSqlSource",
            "sqlReaderQuery": "SELECT status FROM job_status WHERE job_id = '@{pipeline().parameters.jobId}'"
          }
        }
      },
      {
        "name": "WaitInterval",
        "type": "Wait",
        "dependsOn": [{ "activity": "CheckStatus", "dependencyConditions": ["Succeeded"] }],
        "typeProperties": { "waitTimeInSeconds": 60 }
      }
    ]
  }
}
```

### Databricks Pattern

Notebook `wait_for_completion.py`:
```python
# Databricks notebook source
import time

job_id = dbutils.widgets.get("job_id")
jdbc_url = dbutils.secrets.get("scope", "jdbc_url")

timeout_seconds = 3600  # 1 hour
poll_interval = 60
elapsed = 0

while elapsed < timeout_seconds:
    status_df = (spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("query", f"SELECT status FROM job_status WHERE job_id = '{job_id}'")
        .load())

    status = status_df.first()["status"]

    if status == "Complete":
        dbutils.jobs.taskValues.set(key="status", value="Complete")
        break

    time.sleep(poll_interval)
    elapsed += poll_interval

if elapsed >= timeout_seconds:
    raise TimeoutError(f"Timed out waiting for job {job_id} to complete")
```

YAML:
```yaml
tasks:
  - task_key: wait_for_completion
    notebook_task:
      notebook_path: src/notebooks/orchestration/wait_for_completion.py
      base_parameters:
        job_id: "{{job.parameters.job_id}}"
    timeout_seconds: 3600
```

## ExecutePipeline → run_job_task

### ADF Pattern
```json
{
  "name": "RunChildPipeline",
  "type": "ExecutePipeline",
  "typeProperties": {
    "pipeline": {
      "referenceName": "ChildPipeline",
      "type": "PipelineReference"
    },
    "waitOnCompletion": true,
    "parameters": {
      "env": { "value": "@pipeline().parameters.env", "type": "Expression" }
    }
  }
}
```

### Databricks Pattern
```yaml
tasks:
  - task_key: run_child_pipeline
    run_job_task:
      job_id: ${resources.jobs.child_pipeline.id}
      job_parameters:
        env: "{{job.parameters.env}}"
```

## WebActivity → Notebook with requests

### ADF Pattern
```json
{
  "name": "CallApi",
  "type": "WebActivity",
  "typeProperties": {
    "method": "POST",
    "url": "https://api.example.com/process",
    "headers": { "Content-Type": "application/json" },
    "body": { "id": "@{pipeline().parameters.recordId}" },
    "authentication": {
      "type": "MSI",
      "resource": "https://api.example.com"
    }
  }
}
```

### Databricks Pattern

Notebook `call_api.py`:
```python
# Databricks notebook source
import requests
import json

record_id = dbutils.widgets.get("record_id")
api_token = dbutils.secrets.get("scope", "api_token")

response = requests.post(
    "https://api.example.com/process",
    headers={
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}"
    },
    json={"id": record_id},
    timeout=120
)
response.raise_for_status()

result = response.json()
dbutils.jobs.taskValues.set(key="output", value=json.dumps(result))
```

## Dependency Condition Patterns

### Succeeded (default)
```yaml
depends_on:
  - task_key: previous_task
    # outcome defaults to success
```

### Failed (error handling)
```yaml
tasks:
  - task_key: main_task
    notebook_task: ...

  - task_key: error_handler
    depends_on:
      - task_key: main_task
        outcome: failure
    notebook_task:
      notebook_path: src/notebooks/orchestration/error_handler.py
```

### Completed (always runs)
```yaml
tasks:
  - task_key: main_task
    notebook_task: ...

  - task_key: cleanup
    depends_on:
      - task_key: main_task
        # No outcome specified = runs on any outcome
    notebook_task:
      notebook_path: src/notebooks/orchestration/cleanup.py
```

## Fail Activity → raise Exception

### ADF Pattern
```json
{
  "name": "FailPipeline",
  "type": "Fail",
  "typeProperties": {
    "message": "Validation failed: @{activity('Validate').output.firstRow.error}",
    "errorCode": "VALIDATION_ERROR"
  }
}
```

### Databricks Pattern

Notebook `fail_pipeline.py`:
```python
# Databricks notebook source
error_msg = dbutils.jobs.taskValues.get(taskKey="validate", key="error")
raise Exception(f"VALIDATION_ERROR: Validation failed: {error_msg}")
```
