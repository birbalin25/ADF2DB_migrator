# Databricks Workflow YAML Schema Reference (DABs Format)

## Job Definition

```yaml
resources:
  jobs:
    job_name:
      name: "Human-readable Job Name"
      description: "Job description"

      # Schedule (from adf-trigger-converter)
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: "UTC"
        pause_status: UNPAUSED

      # Job-level parameters
      parameters:
        - name: param_name
          default: "default_value"

      # Email notifications
      email_notifications:
        on_start: []
        on_success: ["team@company.com"]
        on_failure: ["team@company.com"]

      # Task definitions
      tasks:
        - task_key: task_name
          description: "Task description"

          # Task type (one of these)
          notebook_task:
            notebook_path: src/notebooks/path/notebook.py
            base_parameters:
              key: value
            source: WORKSPACE

          # OR
          spark_python_task:
            python_file: src/scripts/script.py
            parameters: ["arg1", "arg2"]

          # OR
          spark_jar_task:
            main_class_name: com.company.Main
            parameters: ["arg1"]

          # OR
          sql_task:
            query:
              query_id: "query-id"
            warehouse_id: "warehouse-id"

          # OR
          run_job_task:
            job_id: ${resources.jobs.child_job.id}
            job_parameters:
              key: value

          # OR
          for_each_task:
            inputs: "{{job.parameters.items_list}}"
            concurrency: 10
            task:
              notebook_task:
                notebook_path: src/notebooks/path/inner.py
                base_parameters:
                  item: "{{input}}"

          # OR
          condition_task:
            condition: "{{tasks.prev_task.values.result}} == 'true'"
            if:
              - task_key: true_branch
                notebook_task:
                  notebook_path: src/notebooks/path/true.py
            else:
              - task_key: false_branch
                notebook_task:
                  notebook_path: src/notebooks/path/false.py

          # Dependencies
          depends_on:
            - task_key: previous_task
              outcome: success  # success | failure | skipped (optional)

          # Compute
          job_cluster_key: default  # OR
          existing_cluster_id: "cluster-id"  # OR use serverless (omit both)

          # Retry & timeout
          retry_on_timeout: true
          max_retries: 2
          min_retry_interval_millis: 30000
          timeout_seconds: 3600

          # Libraries
          libraries:
            - pypi:
                package: "requests>=2.28.0"

      # Job clusters
      job_clusters:
        - job_cluster_key: default
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 2
            autoscale:
              min_workers: 1
              max_workers: 4
            spark_conf:
              "spark.databricks.delta.preview.enabled": "true"

      # Queue settings
      queue:
        enabled: true

      # Tags
      tags:
        source: "adf-migration"
        team: "data-engineering"

      # Max concurrent runs
      max_concurrent_runs: 1

      # Timeout
      timeout_seconds: 86400
```

## Task Value Passing

Tasks communicate via `dbutils.jobs.taskValues`:

**Setting values (in notebook):**
```python
dbutils.jobs.taskValues.set(key="row_count", value=df.count())
dbutils.jobs.taskValues.set(key="status", value="success")
dbutils.jobs.taskValues.set(key="output_path", value="/mnt/output/data.parquet")
```

**Reading values (in YAML conditions):**
```yaml
condition: "{{tasks.lookup_task.values.row_count}} > 0"
```

**Reading values (in downstream notebook):**
```python
row_count = dbutils.jobs.taskValues.get(taskKey="lookup_task", key="row_count")
```

**Passing as parameters:**
```yaml
tasks:
  - task_key: downstream
    notebook_task:
      base_parameters:
        input_count: "{{tasks.lookup_task.values.row_count}}"
```

## Serverless Compute

To use serverless compute (recommended), omit `job_cluster_key` and `existing_cluster_id`:

```yaml
tasks:
  - task_key: my_task
    notebook_task:
      notebook_path: src/notebooks/task.py
    # No cluster config = serverless
```

## Environment Configuration

```yaml
tasks:
  - task_key: my_task
    environment_key: default
    notebook_task:
      notebook_path: src/notebooks/task.py

environments:
  - environment_key: default
    spec:
      client: "1"
      dependencies:
        - requests>=2.28.0
        - azure-storage-blob>=12.0.0
```
