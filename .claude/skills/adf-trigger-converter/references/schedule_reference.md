# Databricks Schedule & Trigger Reference

## Quartz Cron Expression Format

Format: `seconds minutes hours day-of-month month day-of-week`

| Field | Values | Special Characters |
|---|---|---|
| Seconds | 0-59 | `, - * /` |
| Minutes | 0-59 | `, - * /` |
| Hours | 0-23 | `, - * /` |
| Day-of-month | 1-31 | `, - * / ? L W` |
| Month | 1-12 or JAN-DEC | `, - * /` |
| Day-of-week | 1-7 or SUN-SAT | `, - * / ? L #` |

### Special Characters
- `*` — all values
- `?` — no specific value (used in day-of-month OR day-of-week)
- `-` — range (e.g., `MON-FRI`)
- `,` — list (e.g., `MON,WED,FRI`)
- `/` — increment (e.g., `0/15` = every 15 starting from 0)
- `L` — last (last day of month, last weekday)
- `W` — nearest weekday
- `#` — nth weekday (e.g., `FRI#2` = second Friday)

### Common Examples

| Schedule | Cron Expression |
|---|---|
| Every 15 minutes | `0 */15 * * * ?` |
| Every hour | `0 0 * * * ?` |
| Daily at 6 AM UTC | `0 0 6 * * ?` |
| Daily at 6 AM and 6 PM | `0 0 6,18 * * ?` |
| Weekdays at 8 AM | `0 0 8 ? * MON-FRI` |
| MWF at 9:30 AM | `0 30 9 ? * MON,WED,FRI` |
| First of month at midnight | `0 0 0 1 * ?` |
| Last day of month at 11 PM | `0 0 23 L * ?` |
| Every 6 hours | `0 0 */6 * * ?` |
| Every 30 minutes during business hours | `0 */30 8-17 ? * MON-FRI` |
| Sunday at 2 AM (weekly maintenance) | `0 0 2 ? * SUN` |

## ADF Recurrence → Quartz Cron Conversion

### Frequency-Based (Simple)

| ADF `frequency` | ADF `interval` | Quartz Cron |
|---|---|---|
| Minute | 1 | `0 * * * * ?` |
| Minute | 5 | `0 */5 * * * ?` |
| Minute | 15 | `0 */15 * * * ?` |
| Minute | 30 | `0 */30 * * * ?` |
| Hour | 1 | `0 0 * * * ?` |
| Hour | 2 | `0 0 */2 * * ?` |
| Hour | 6 | `0 0 */6 * * ?` |
| Hour | 12 | `0 0 */12 * * ?` |
| Day | 1 | `0 0 0 * * ?` (midnight) |
| Day | 1 (with hours=[6]) | `0 0 6 * * ?` |
| Week | 1 | `0 0 0 ? * MON` |
| Month | 1 | `0 0 0 1 * ?` |

### Schedule-Based (Complex)

**With `schedule.hours` and `schedule.minutes`:**
```
ADF: frequency=Day, interval=1, schedule.hours=[6,18], schedule.minutes=[0]
Cron: 0 0 6,18 * * ?
```

**With `schedule.weekDays`:**
```
ADF: frequency=Week, interval=1, schedule.weekDays=["Monday","Wednesday","Friday"], schedule.hours=[8]
Cron: 0 0 8 ? * MON,WED,FRI
```

**With `schedule.monthDays`:**
```
ADF: frequency=Month, interval=1, schedule.monthDays=[1,15], schedule.hours=[0]
Cron: 0 0 0 1,15 * ?
```

## File Arrival Trigger

```yaml
trigger:
  file_arrival:
    url: "abfss://container@account.dfs.core.windows.net/path/"
    min_time_between_triggers_seconds: 60
    wait_after_last_change_seconds: 30
```

**Parameters:**
- `url` — Cloud storage path to monitor (ADLS Gen2, S3, GCS)
- `min_time_between_triggers_seconds` — Minimum gap between triggers (default: 60)
- `wait_after_last_change_seconds` — Wait time after last file change before triggering (default: 60)

## Table Update Trigger

```yaml
trigger:
  table_update:
    table_names:
      - "catalog.schema.source_table"
    condition: ANY_UPDATED
    min_time_between_triggers_seconds: 60
```

## Continuous Trigger (Always Running)

```yaml
trigger:
  periodic:
    interval: 1
    unit: HOURS
```

## Job YAML with Schedule

```yaml
resources:
  jobs:
    daily_pipeline:
      name: "Daily Sales Pipeline"

      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: "UTC"
        pause_status: UNPAUSED

      max_concurrent_runs: 1

      parameters:
        - name: window_start
          default: ""
        - name: window_end
          default: ""

      tasks:
        - task_key: ingest
          notebook_task:
            notebook_path: src/notebooks/ingestion/sales.py

      queue:
        enabled: true

      email_notifications:
        on_failure: ["team@company.com"]
```

## Cluster Configuration

### Serverless (Default — Recommended)
```yaml
# No cluster config needed — tasks use serverless automatically
tasks:
  - task_key: my_task
    notebook_task:
      notebook_path: src/notebooks/task.py
```

### Job Cluster
```yaml
job_clusters:
  - job_cluster_key: default
    new_cluster:
      spark_version: "15.4.x-scala2.12"
      node_type_id: "Standard_DS3_v2"
      num_workers: 2
      autoscale:
        min_workers: 1
        max_workers: 8
      spark_conf:
        "spark.databricks.delta.preview.enabled": "true"
      custom_tags:
        "source": "adf_migration"

tasks:
  - task_key: my_task
    job_cluster_key: default
    notebook_task:
      notebook_path: src/notebooks/task.py
```

### Existing Cluster
```yaml
tasks:
  - task_key: my_task
    existing_cluster_id: "0123-456789-abcdef"
    notebook_task:
      notebook_path: src/notebooks/task.py
```

## Retry Configuration

```yaml
tasks:
  - task_key: my_task
    notebook_task:
      notebook_path: src/notebooks/task.py
    max_retries: 3
    min_retry_interval_millis: 30000  # 30 seconds
    retry_on_timeout: true
    timeout_seconds: 3600  # 1 hour
```
