# Databricks Asset Bundles (DABs) Schema Reference

## databricks.yml Top-Level

```yaml
bundle:
  name: "bundle-name"          # Required. Lowercase, hyphens, alphanumeric
  cluster_id: "optional"       # Default cluster for dev
  compute_id: "optional"       # Default serverless compute
  git:
    origin_url: "https://github.com/org/repo"
    branch: "main"

variables:                      # Bundle-level variables
  var_name:
    description: "Human description"
    default: "default_value"
    type: "string"             # string, integer, boolean, complex
    lookup:                    # Dynamic lookup
      cluster: "cluster-name"

workspace:                      # Workspace defaults
  host: "https://adb-xxx.azuredatabricks.net"
  root_path: "/Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}"
  artifact_path: "${workspace.root_path}/artifacts"
  file_path: "${workspace.root_path}/files"
  state_path: "${workspace.root_path}/state"

include:                        # Resource file includes
  - "resources/*.yml"
  - "resources/**/*.yml"

artifacts:                      # Build artifacts (wheels, JARs)
  my_wheel:
    type: whl
    path: ./dist
    build: "python -m build"

targets:                        # Deployment targets
  dev:
    mode: development
    default: true
    workspace:
      host: "https://dev.azuredatabricks.net"
    variables:
      var_name: "dev_value"
  prod:
    mode: production
    workspace:
      host: "https://prod.azuredatabricks.net"
    run_as:
      service_principal_name: "sp-name"

resources:                      # Inline resources (or use include)
  jobs: {}
  pipelines: {}
  experiments: {}
  models: {}
  model_serving_endpoints: {}
  registered_models: {}
  schemas: {}
  quality_monitors: {}
  clusters: {}
  dashboards: {}
  volumes: {}
  apps: {}
```

## Resource Types

### Jobs (Workflows)
```yaml
resources:
  jobs:
    my_job:
      name: "Job Name"
      description: "Description"
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: "UTC"
        pause_status: UNPAUSED  # UNPAUSED or PAUSED
      trigger:
        file_arrival:
          url: "abfss://..."
        table_update:
          table_names: ["catalog.schema.table"]
          condition: ANY_UPDATED
        periodic:
          interval: 1
          unit: HOURS
      continuous:
        pause_status: UNPAUSED
      parameters:
        - name: param1
          default: "value"
      tasks: []
      job_clusters: []
      tags: {}
      max_concurrent_runs: 1
      timeout_seconds: 86400
      health:
        rules:
          - metric: RUN_DURATION_SECONDS
            op: GREATER_THAN
            value: 7200
      email_notifications:
        on_start: []
        on_success: []
        on_failure: []
      webhook_notifications:
        on_start: [{id: "webhook-id"}]
      notification_settings:
        no_alert_for_skipped_runs: true
      queue:
        enabled: true
      run_as:
        user_name: "user@domain.com"
      environments:
        - environment_key: default
          spec:
            client: "1"
            dependencies: ["requests"]
```

### Pipelines (DLT / Lakeflow Declarative)
```yaml
resources:
  pipelines:
    my_pipeline:
      name: "Pipeline Name"
      description: "Description"
      target: "catalog.schema"
      catalog: "catalog"
      libraries:
        - notebook:
            path: src/notebooks/transform.py
        - file:
            path: src/python/module.py
      configuration:
        "pipeline.param": "value"
      continuous: false
      development: true
      channel: PREVIEW  # PREVIEW or CURRENT
      photon: true
      edition: ADVANCED  # CORE, PRO, ADVANCED
      serverless: true
      clusters:
        - label: default
          autoscale:
            min_workers: 1
            max_workers: 4
```

### Schemas
```yaml
resources:
  schemas:
    my_schema:
      catalog_name: "${var.catalog}"
      name: "${var.schema}"
      comment: "Schema for migrated data"
      grants:
        - principal: "data-engineers"
          privileges:
            - ALL_PRIVILEGES
```

### Volumes
```yaml
resources:
  volumes:
    my_volume:
      catalog_name: "${var.catalog}"
      schema_name: "${var.schema}"
      name: "raw_files"
      volume_type: MANAGED  # MANAGED or EXTERNAL
      comment: "Raw file storage"
```

## Variable References

- `${var.name}` — bundle variable
- `${bundle.name}` — bundle name
- `${workspace.current_user.userName}` — deploying user
- `${resources.jobs.job_name.id}` — job ID (post-deploy)
- `${resources.pipelines.pipeline_name.id}` — pipeline ID

## Task Parameter References (in YAML)

- `{{job.parameters.name}}` — job-level parameter
- `{{tasks.task_key.values.key}}` — task value from previous task
- `{{input}}` — current item in for_each_task

## CLI Commands

```bash
# Validate bundle configuration
databricks bundle validate --target dev

# Deploy bundle to workspace
databricks bundle deploy --target dev

# Run a specific job
databricks bundle run my_job --target dev

# Run with parameter overrides
databricks bundle run my_job --target dev --params '{"catalog":"test"}'

# Destroy deployed resources
databricks bundle destroy --target dev

# Show bundle summary
databricks bundle summary --target dev
```
