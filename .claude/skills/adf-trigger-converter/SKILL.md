---
name: adf-trigger-converter
description: Convert ADF triggers and Integration Runtime configs to Databricks Workflow schedules and cluster configurations
trigger_keywords:
  - trigger
  - schedule
  - cron
  - tumbling window
  - event trigger
  - file arrival
  - integration runtime
  - cluster
  - compute
  - recurrence
input: Trigger JSON + IntegrationRuntime JSON from ARM template
output: Databricks job schedule YAML + cluster/compute configuration
---

# ADF Trigger Converter

Convert ADF triggers to Databricks Workflow schedules and Integration Runtime configs to cluster/compute configurations.

## What This Skill Does

1. **Reads** Trigger and IntegrationRuntime JSON from the ARM template
2. **Converts** schedule triggers to quartz cron expressions
3. **Maps** tumbling window triggers to periodic schedules or continuous jobs
4. **Converts** blob event triggers to file arrival triggers
5. **Maps** Integration Runtime configurations to Databricks compute settings
6. **Generates** schedule and cluster YAML sections for DABs job definitions

## Trigger Mapping

| ADF Trigger Type | Databricks Equivalent | YAML Section |
|---|---|---|
| ScheduleTrigger | `schedule.quartz_cron_expression` | `jobs.<name>.schedule` |
| TumblingWindowTrigger | Periodic schedule or `trigger.table_update` | `jobs.<name>.schedule` or `trigger` |
| BlobEventsTrigger | `trigger.file_arrival` | `jobs.<name>.trigger` |
| CustomEventsTrigger | REST API trigger (manual) | Documentation + webhook notebook |

## Step-by-Step Instructions

### Step 1: Parse Trigger JSON

Extract from each trigger:
- `properties.type` — trigger type
- `properties.typeProperties` — schedule/event configuration
- `properties.pipelines[]` — which pipelines this trigger activates
- `properties.pipeline` — (TumblingWindow) single pipeline reference

### Step 2: Convert ScheduleTrigger

ADF ScheduleTrigger uses a recurrence pattern:
```json
{
  "type": "ScheduleTrigger",
  "typeProperties": {
    "recurrence": {
      "frequency": "Day",
      "interval": 1,
      "startTime": "2024-01-01T06:00:00Z",
      "timeZone": "UTC",
      "schedule": {
        "hours": [6, 18],
        "minutes": [0, 30],
        "weekDays": ["Monday", "Wednesday", "Friday"]
      }
    }
  }
}
```

**Conversion to Quartz Cron:**

Quartz cron format: `seconds minutes hours day-of-month month day-of-week`

| ADF Frequency | Cron Pattern | Example |
|---|---|---|
| `Minute` (interval=15) | `0 */15 * * * ?` | Every 15 minutes |
| `Hour` (interval=1) | `0 0 * * * ?` | Every hour on the hour |
| `Day` (interval=1, hour=6) | `0 0 6 * * ?` | Daily at 6 AM |
| `Week` (Mon,Wed,Fri, hour=6) | `0 0 6 ? * MON,WED,FRI` | MWF at 6 AM |
| `Month` (day=1, hour=0) | `0 0 0 1 * ?` | First of each month at midnight |

**Conversion rules:**
1. If `schedule.hours` and `schedule.minutes` specified → use them directly
2. If `schedule.weekDays` specified → use day-of-week field, set day-of-month to `?`
3. If `schedule.monthDays` specified → use day-of-month field, set day-of-week to `?`
4. If only `frequency` + `interval` → compute the `*/interval` pattern

**Output YAML:**
```yaml
schedule:
  quartz_cron_expression: "0 0 6 * * ?"
  timezone_id: "UTC"
  pause_status: UNPAUSED
```

### Step 3: Convert TumblingWindowTrigger

ADF TumblingWindow triggers fire at fixed intervals with window boundaries:
```json
{
  "type": "TumblingWindowTrigger",
  "typeProperties": {
    "frequency": "Hour",
    "interval": 1,
    "startTime": "2024-01-01T00:00:00Z",
    "delay": "00:15:00",
    "maxConcurrency": 5,
    "retryPolicy": { "count": 3, "intervalInSeconds": 30 },
    "dependsOn": [
      { "type": "TumblingWindowTriggerDependencyReference", "referenceTrigger": { "referenceName": "OtherTrigger" } }
    ]
  }
}
```

**Conversion:**
- Simple periodic → `schedule.quartz_cron_expression` (same as ScheduleTrigger)
- With window parameters → Pass `window_start`/`window_end` as job parameters
- `delay` → Job schedule offset (note in comments)
- `maxConcurrency` → `max_concurrent_runs` in job config
- `retryPolicy` → `max_retries` + `min_retry_interval_millis` on tasks
- `dependsOn` → Document as job dependency (manual setup via job triggers)

**Output YAML:**
```yaml
schedule:
  quartz_cron_expression: "0 0 * * * ?"  # Hourly
  timezone_id: "UTC"
max_concurrent_runs: 5
parameters:
  - name: window_start
    default: ""
  - name: window_end
    default: ""
```

### Step 4: Convert BlobEventsTrigger

ADF BlobEventsTrigger fires when files are created/deleted in blob storage:
```json
{
  "type": "BlobEventsTrigger",
  "typeProperties": {
    "blobPathBeginsWith": "/container/raw/sales/",
    "blobPathEndsWith": ".csv",
    "ignoreEmptyBlobs": true,
    "events": ["Microsoft.Storage.BlobCreated"]
  }
}
```

**Conversion to file arrival trigger:**
```yaml
trigger:
  file_arrival:
    url: "abfss://container@account.dfs.core.windows.net/raw/sales/"
    min_time_between_triggers_seconds: 60
    wait_after_last_change_seconds: 30
```

**Notes:**
- `blobPathBeginsWith` → `url` (convert to `abfss://` format)
- `blobPathEndsWith` → Add as comment (file arrival doesn't filter by extension natively; use Auto Loader with `pathGlobFilter`)
- `ignoreEmptyBlobs` → Handle in the notebook with file size check
- `events` (BlobCreated only) → default file arrival behavior

### Step 5: Convert CustomEventsTrigger

CustomEventsTrigger uses Azure Event Grid — no direct Databricks equivalent.

Generate documentation:
```yaml
# CustomEventsTrigger: <trigger_name>
# ADF used Azure Event Grid custom events
#
# Databricks options:
# 1. Use a webhook-triggered job via REST API
#    POST https://<workspace>/api/2.1/jobs/run-now
# 2. Use Azure Functions as middleware:
#    Event Grid → Azure Function → Databricks REST API
# 3. Use Databricks Workflow with manual/API trigger
#
# Original event config:
#   scope: <event_grid_scope>
#   events: <event_types>
#   subjectBeginsWith: <filter>
```

### Step 6: Convert Integration Runtime

**Managed (Azure IR) → Serverless or Job Cluster:**

```json
{
  "type": "Managed",
  "typeProperties": {
    "computeProperties": {
      "location": "AutoResolve",
      "dataFlowProperties": {
        "computeType": "General",
        "coreCount": 8,
        "timeToLive": 10
      }
    }
  }
}
```

**Serverless (recommended):**
```yaml
# Serverless compute — no cluster config needed
# Omit job_cluster_key and existing_cluster_id from tasks
```

**Job cluster (if specific config needed):**
```yaml
job_clusters:
  - job_cluster_key: default_cluster
    new_cluster:
      spark_version: "15.4.x-scala2.12"
      num_workers: 1
      autoscale:
        min_workers: 1
        max_workers: 4  # Derived from coreCount / cores_per_worker
      node_type_id: "Standard_DS3_v2"  # General compute type
```

**Core count mapping:**
| ADF Compute Type | Core Count | Databricks Node Type | Workers |
|---|---|---|---|
| General, 8 cores | 8 | Standard_DS3_v2 (4 cores) | 2 |
| General, 16 cores | 16 | Standard_DS3_v2 | 4 |
| General, 32 cores | 32 | Standard_DS4_v2 (8 cores) | 4 |
| ComputeOptimized, 8 cores | 8 | Standard_F4 (4 cores) | 2 |
| MemoryOptimized, 8 cores | 8 | Standard_E4_v3 (4 cores) | 2 |

**Self-Hosted IR → VNet-injected workspace:**
```yaml
# Self-Hosted Integration Runtime: <ir_name>
# Requires Databricks workspace with VNet injection
#
# Setup steps:
# 1. Deploy Databricks workspace in customer VNet
# 2. Configure Private Link endpoints for data sources
# 3. Use Serverless Compute with Network Connectivity Config (NCC)
# 4. Or use Classic compute with VNet-injected clusters
#
# Serverless NCC configuration:
# - Create NCC in account console
# - Add private endpoints for required services
# - Attach NCC to workspace
```

### Step 7: Output

For each trigger, add the schedule/trigger section to the corresponding job YAML in `resources/<job_name>.yml`.

For Integration Runtimes, add cluster configuration to the job YAML or document serverless usage.

## Important Notes

- Quartz cron in Databricks uses 6 fields (with seconds): `sec min hour dom month dow`
- Day-of-week: `SUN=1` through `SAT=7` in quartz, or use `MON,TUE,WED,THU,FRI,SAT,SUN`
- Use `?` for either day-of-month or day-of-week (one must be `?`)
- Tumbling window `dependsOn` between triggers → job chaining via `run_job_task` or manual orchestration
- File arrival triggers work with Unity Catalog External Locations and Volumes
- Serverless compute is recommended for most workloads — simpler and faster startup
