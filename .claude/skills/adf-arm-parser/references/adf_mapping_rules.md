# ADF Component → Databricks Mapping Rules

Complete mapping from ADF component (type, subtype) pairs to Databricks equivalents with migration recommendations.

## Activity Mappings

| ADF Activity Type | Databricks Equivalent | Migration Recommendation |
|---|---|---|
| Copy | Auto Loader / COPY INTO / Spark JDBC read | Use Auto Loader for file sources (Parquet, CSV, JSON). COPY INTO for batch file loads. Spark JDBC for database sources. Lakeflow Connect for supported SaaS connectors. |
| ExecuteDataFlow | Lakeflow Declarative Pipeline (DLT) or PySpark Notebook | DLT for streaming/incremental. PySpark notebook for batch. Use `from pyspark import pipelines as dp`. |
| ExecutePipeline | Databricks Workflow Run Job Task | Map to `run_job_task` in DABs YAML. Pass parameters via `job_parameters`. |
| ForEach | Databricks Workflow `for_each_task` | Map `items` expression to `inputs`. `batchCount` → `concurrency`. Inner activities become the nested task. |
| IfCondition | Databricks Workflow `if_else_task` | Map `expression` to Jinja `condition`. True/false activities → `then`/`else` branches. |
| Switch | Chained `if_else_task` | Each `cases[]` entry → chained if/else. `defaultActivities` → final else. |
| Until | Notebook with `while` loop + Workflow retry | Map `expression` to Python condition. `timeout` → Workflow timeout. Consider `retry_on_timeout`. |
| Wait | `time.sleep()` in Notebook | `typeProperties.waitTimeInSeconds` → `time.sleep(seconds)`. |
| SetVariable | `dbutils.jobs.taskValues.set()` | Map `variableName` → task value key. `value` expression → Python value. |
| AppendVariable | `dbutils.jobs.taskValues.set()` with list append | Get existing list, append, set back. |
| WebActivity | Notebook with `requests` library | Map `method`, `url`, `headers`, `body`. Auth via `dbutils.secrets`. |
| Lookup | `spark.read` / `spark.sql()` in Notebook | Source query → Spark SQL. `firstRowOnly` → `.first()` or `.limit(1)`. |
| GetMetadata | `dbutils.fs.ls()` / Delta `DESCRIBE` | File metadata → `dbutils.fs.ls()`. Table metadata → `DESCRIBE DETAIL`. |
| Filter | PySpark `.filter()` / `WHERE` clause | `items` → input array. `condition` → filter expression. |
| Fail | `raise Exception()` / `dbutils.notebook.exit()` | Map `message` and `errorCode` to exception. |
| Script | `spark.sql()` or JDBC execute in Notebook | `scriptContent` → SQL statements. Multiple scripts → sequential execution. |
| Validation | DLT Expectations / `assert` statements | `minimumSize`, `timeout` → assertions or DLT expectations. |
| Delete | `dbutils.fs.rm()` | `dataset` reference → path resolution. `recursive` flag mapping. |
| SqlServerStoredProcedure | `spark.sql()` via JDBC or Notebook SQL cell | Map `storedProcedureName` + parameters. Consider `spark.read.jdbc` with pushdown. |
| AzureFunctionActivity | Notebook calling HTTP endpoint | `functionName`, `method`, `body` → `requests` call. Auth via Function Key in secrets. |
| WebHook | Notebook with async HTTP callback | Similar to WebActivity but with callback URL pattern. |
| Custom | Notebook Task or Python Script Task | `command`, `folderPath`, `resourceLinkedService` → notebook with subprocess or native Python. |
| HDInsightSpark | Databricks Notebook / Spark Submit Task | Map `rootPath`, `entryFilePath`, `arguments`. |
| HDInsightHive | Spark SQL / Databricks SQL Notebook | Convert HiveQL to Spark SQL. |
| HDInsightPig | PySpark Notebook | Rewrite Pig Latin as PySpark transformations. |
| HDInsightMapReduce | PySpark Notebook / Spark Submit Task | Rewrite MapReduce as PySpark. |
| HDInsightStreaming | Structured Streaming Notebook | Convert to `spark.readStream`. |
| DatabricksNotebook | Databricks Notebook Task (native) | Direct mapping. Update workspace paths to DABs-relative paths. |
| DatabricksSparkPython | Databricks Python Script Task | Map `pythonFile`, `parameters`. |
| DatabricksSparkJar | Databricks JAR Task | Map `mainClassName`, `parameters`, library references. |
| ExecuteSSISPackage | **Manual**: Refactor SSIS logic to PySpark / DLT | High complexity. Analyze SSIS package structure. Generate stub notebook with TODO markers. |
| AzureMLExecutePipeline | **Manual**: MLflow Model Serving / Databricks Model Serving | Map to MLflow tracking + model registry. Generate stub with migration notes. |
| AzureMLBatchExecution | Databricks Batch Inference Notebook / Model Serving | Map to batch inference pattern with MLflow. |
| AzureMLUpdateResource | MLflow Model Registry update | Map to `mlflow.register_model()` pattern. |
| USql | **Manual**: Spark SQL / PySpark Notebook | Deprecated. Rewrite U-SQL as Spark SQL. Generate stub with rewrite notes. |
| SynapseNotebook | Databricks Notebook Task | Direct mapping. Update Synapse-specific APIs to Databricks equivalents. |
| SynapseSparkJob | Databricks Spark Submit / JAR Task | Map Synapse Spark config to Databricks job cluster. |

## Top-Level Component Mappings

| ADF Component Type | Databricks Equivalent | Migration Recommendation |
|---|---|---|
| Pipeline | Databricks Workflow (Job) | Each pipeline → one Job in DABs YAML. Activities → task DAG. Parameters → `job_parameters`. |
| Dataset | Unity Catalog Table / Volume / External Location | SQL datasets → UC tables. File datasets → UC Volumes or External Locations. Parameters → runtime resolution. |
| LinkedService | Unity Catalog Connection + Databricks Secrets | Connection strings → UC Connections. Credentials → Secret Scopes. Key Vault refs → Secret Scope with Azure Key Vault backend. |
| DataFlow | Lakeflow Declarative Pipeline (DLT) or PySpark Notebook | DFL script → DLT `@dp.table` / `@dp.materialized_view` decorators. Complex flows → PySpark notebook. |
| Trigger | Databricks Workflow Schedule / File Arrival Trigger | Schedule → `quartz_cron_expression`. Tumbling window → `table_update` or continuous trigger. Blob events → `file_arrival`. |
| IntegrationRuntime | Serverless Compute / Job Cluster / VNet Cluster | Azure IR → Serverless or autoscale job cluster. Self-Hosted → VNet-injected workspace + Private Link. |
| ChangeDataCapture | DLT with Change Data Feed / Lakeflow Connect CDC | Map source/target pairs. CDC mode → `readStream` with CDC or Lakeflow Connect. |
| Credential | Databricks Secret Scope / UC Storage Credential | ManagedIdentity → workspace managed identity. ServicePrincipal → secret scope. |
| GlobalParameter | Workflow Job Parameter / Cluster Init Script / Widget | Map to `dbutils.widgets.get()` or job-level variables in DABs YAML. |
| ManagedVirtualNetwork | Databricks Workspace VNet Injection / Private Link | Infrastructure setup. Document requirements for workspace deployment. |
| ManagedPrivateEndpoint | Databricks Private Link / Serverless NCC | Map `groupId` (blob, dfs, sqlServer, vault) to corresponding Databricks private connectivity. |
| PrivateEndpointConnection | Databricks Private Link Connection | Infrastructure setup. Document Private Link configuration steps. |
| Parameter | `dbutils.widgets.get()` / Job parameter | Handled as part of parent Pipeline conversion. |
| Variable | `dbutils.jobs.taskValues` / Python variable | Handled as part of parent Pipeline conversion. |

## LinkedService Subtype Overrides

| LinkedService Type | Databricks Equivalent | Notes |
|---|---|---|
| AzureSqlDatabase | UC Connection (SQL Server) + Secret Scope | JDBC connection via `spark.read.format("jdbc")` |
| AzureBlobFS | UC External Location (ADLS Gen2) | `abfss://` paths. Service principal or managed identity auth. |
| AzureBlobStorage | UC External Location (Blob) | `wasbs://` paths. Storage account key or SAS in secrets. |
| AzureKeyVault | Databricks Secret Scope (Azure KV backend) | `dbutils.secrets.get(scope, key)` backed by Azure Key Vault. |
| CosmosDb | UC Connection + `azure-cosmos-spark` connector | Cosmos DB Spark connector with throughput control. |
| Salesforce | Lakeflow Connect (Salesforce GA) | Managed connector. No custom code needed. |
| RestService | Notebook with `requests` + retry logic | Map `url`, `authenticationType`, headers. |
| AzureSqlDW | UC Connection (Synapse/DW) + Synapse connector | `spark.read.format("sqldw")` or JDBC. |
| AzureDatabricks | Native Databricks workspace reference | No conversion needed — already on Databricks. |
| SqlServer | UC Connection (SQL Server) + JDBC | `spark.read.format("jdbc")` with SQL Server driver. |
| MySql | UC Connection + JDBC | MySQL JDBC driver. |
| PostgreSql | UC Connection + JDBC | PostgreSQL JDBC driver. |
| Oracle | UC Connection + JDBC | Oracle JDBC driver. |
| AzureDataLakeStore | UC External Location (ADLS Gen1) | **Migrate to Gen2** recommended. |
| AmazonS3 | UC External Location (S3) | IAM role or access key auth via instance profile. |

## Dataset Subtype Overrides

| Dataset Type | Databricks Equivalent | Notes |
|---|---|---|
| AzureSqlTable | UC Table reference | 3-level namespace: `catalog.schema.table` |
| SqlServerTable | UC Table reference | Via JDBC or Lakeflow Connect |
| Parquet | UC Volume / External Location (Parquet) | Auto Loader `cloudFiles.format=parquet` |
| DelimitedText | UC Volume / External Location (CSV) | Auto Loader `cloudFiles.format=csv` with delimiter config |
| Json | UC Volume / External Location (JSON) | Auto Loader `cloudFiles.format=json` |
| Avro | UC Volume / External Location (Avro) | Auto Loader `cloudFiles.format=avro` |
| Orc | UC Volume / External Location (ORC) | Auto Loader `cloudFiles.format=orc` |
| Excel | UC Volume + `openpyxl` or `pandas` | Read with pandas, convert to Spark DataFrame |
| Binary | UC Volume | `dbutils.fs` for binary file operations |
| Xml | UC Volume + `spark-xml` | `spark.read.format("com.databricks.spark.xml")` |
| CosmosDbSqlApiCollection | UC Table via Cosmos connector | `azure-cosmos-spark` connector |
| AmazonS3Object | UC External Location (S3) | `s3://` paths with instance profile |

## Trigger Subtype Overrides

| Trigger Type | Databricks Equivalent | Notes |
|---|---|---|
| ScheduleTrigger | `schedule.quartz_cron_expression` | Convert `recurrence` (frequency + interval + schedule) to quartz cron. |
| TumblingWindowTrigger | `trigger.table_update` or continuous job | `frequency` + `interval` → periodic schedule. `retryPolicy` → job retry config. `dependsOn` → job dependency. |
| BlobEventsTrigger | `trigger.file_arrival` | `blobPathBeginsWith` → path filter. `events` (created/deleted). |
| CustomEventsTrigger | REST API trigger (manual setup) | Document Event Grid integration. Generate webhook handler notebook. |

## Credential Subtype Overrides

| Credential Type | Databricks Equivalent | Notes |
|---|---|---|
| ManagedIdentity | Workspace Managed Identity | Configure in workspace admin settings |
| ServicePrincipal | Secret Scope (client_id + client_secret) | Store in Databricks secret scope |

## IntegrationRuntime Subtype Overrides

| IR Type | Databricks Equivalent | Notes |
|---|---|---|
| Managed | Serverless Compute / Autoscale Job Cluster | Default. Use serverless for most workloads. |
| SelfHosted | VNet-injected Workspace + Private Link | Requires network architecture. Document setup steps. |

## ManagedPrivateEndpoint groupId Overrides

| groupId | Databricks Equivalent | Notes |
|---|---|---|
| blob | Private Link to Storage (Blob) | Serverless NCC or VNet PE |
| dfs | Private Link to Storage (DFS/ADLS) | Serverless NCC or VNet PE |
| sqlServer | Private Link to SQL Server | Serverless NCC or VNet PE |
| vault | Private Link to Key Vault | Secret Scope AKV backend handles this |
