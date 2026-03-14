# Databricks notebook source
# MAGIC %md
# MAGIC # ADF → Databricks Component Mapping Generator
# MAGIC
# MAGIC **Purpose**: Parses an ADF ARM template (or single pipeline JSON), identifies every
# MAGIC component — Pipelines, Activities, Datasets, Linked Services, DataFlows, Triggers,
# MAGIC Integration Runtimes, Parameters, Variables, Change Data Captures, Credentials,
# MAGIC Global Parameters, Managed Virtual Networks, Managed Private Endpoints, and
# MAGIC Private Endpoint Connections — and produces a structured mapping to Databricks
# MAGIC equivalents with migration recommendations and complexity scores.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Supported Input Formats
# MAGIC
# MAGIC ### Option A — Full ADF ARM Template
# MAGIC
# MAGIC Standard export from ADF Studio (**Manage → ARM Template → Export ARM Template**).
# MAGIC ```json
# MAGIC {
# MAGIC   "$schema": "http://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#",
# MAGIC   "contentVersion": "1.0.0.0",
# MAGIC   "parameters": { "factoryName": { "type": "string" } },
# MAGIC   "resources": [
# MAGIC     { "type": "Microsoft.DataFactory/factories", ... },
# MAGIC     { "type": "Microsoft.DataFactory/factories/pipelines", ... },
# MAGIC     { "type": "Microsoft.DataFactory/factories/datasets", ... },
# MAGIC     { "type": "Microsoft.DataFactory/factories/linkedservices", ... },
# MAGIC     { "type": "Microsoft.DataFactory/factories/triggers", ... },
# MAGIC     { "type": "Microsoft.DataFactory/factories/dataflows", ... },
# MAGIC     { "type": "Microsoft.DataFactory/factories/integrationruntimes", ... }
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Option B — Single Pipeline Extract
# MAGIC
# MAGIC JSON from `Get-AzDataFactoryV2Pipeline | ConvertTo-Json -Depth 100`.
# MAGIC Accepts a single object or an array. Both `name`/`Name` and
# MAGIC `properties`/`Properties` casings are handled.
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "PipelineName",
# MAGIC   "properties": { "activities": [...], "parameters": {...}, "variables": {...} }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Output Columns
# MAGIC
# MAGIC | Column | Description |
# MAGIC |---|---|
# MAGIC | `adf_component_type` | Category: Pipeline, Activity, Dataset, LinkedService, DataFlow, Trigger, IntegrationRuntime, Parameter, Variable, ChangeDataCapture, Credential, GlobalParameter, ManagedVirtualNetwork, ManagedPrivateEndpoint, PrivateEndpointConnection |
# MAGIC | `adf_component_name` | Name of the ADF component |
# MAGIC | `parent_component` | Parent (e.g. pipeline name for activities/params/vars) |
# MAGIC | `adf_subtype` | ADF-specific subtype (e.g. Copy, ForEach, ScheduleTrigger, AzureSqlDatabase) |
# MAGIC | `databricks_equivalent` | Recommended Databricks service / feature |
# MAGIC | `migration_recommendation` | Detailed migration guidance |
# MAGIC | `complexity_score` | Low / Medium / High |
# MAGIC | `complexity_reason` | Justification for the complexity score |
# MAGIC | `adf_config_summary` | Key configuration details from the ADF definition |
# MAGIC | `key_metrics` | JSON blob of quantitative metrics (activity counts, expression counts, flags, etc.) — for downstream AI code generation |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# -- Input --
dbutils.widgets.dropdown("input_mode", "arm_template", ["arm_template", "single_pipeline"], "Input Mode")
dbutils.widgets.text("input_path", "/Volumes/main/default/migration/arm_template.json", "Path to JSON (DBFS / Volumes / workspace)")
dbutils.widgets.text("factory_name", "", "Data Factory Name (auto-detected if blank)")

# -- LLM --
dbutils.widgets.text("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct", "Foundation Model Endpoint")

# -- Output --
dbutils.widgets.text("output_catalog", "bircatalog", "Output Catalog")
dbutils.widgets.text("output_schema", "birschema", "Output Schema")
dbutils.widgets.text("output_table", "Component_Mapping", "Output Table")
dbutils.widgets.dropdown("output_write_mode", "overwrite", ["overwrite", "append"], "Write Mode")

input_mode         = dbutils.widgets.get("input_mode")
input_path         = dbutils.widgets.get("input_path")
factory_name_param = dbutils.widgets.get("factory_name").strip()
llm_endpoint       = dbutils.widgets.get("llm_endpoint")
output_catalog     = dbutils.widgets.get("output_catalog")
output_schema      = dbutils.widgets.get("output_schema")
output_table       = dbutils.widgets.get("output_table")
output_write_mode  = dbutils.widgets.get("output_write_mode")
output_fqn         = f"{output_catalog}.{output_schema}.{output_table}"

print(f"Input:  {input_mode} → {input_path}")
print(f"Output: {output_fqn} (mode={output_write_mode})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import json, re
from datetime import datetime, timezone
from collections import defaultdict
from typing import Any

import mlflow.deployments

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parse Input JSON

# COMMAND ----------

raw_text = dbutils.fs.head(input_path)
raw_json = json.loads(raw_text)

TYPE_MAP = {
    # --- Core pipeline & data components ---
    "Microsoft.DataFactory/factories/pipelines":           "Pipeline",
    "Microsoft.DataFactory/factories/datasets":            "Dataset",
    "Microsoft.DataFactory/factories/linkedservices":      "LinkedService",
    "Microsoft.DataFactory/factories/triggers":            "Trigger",
    "Microsoft.DataFactory/factories/dataflows":           "DataFlow",
    "Microsoft.DataFactory/factories/integrationruntimes": "IntegrationRuntime",
    # --- Additional ADF resource types ---
    "Microsoft.DataFactory/factories/adfcdcs":             "ChangeDataCapture",
    "Microsoft.DataFactory/factories/credentials":         "Credential",
    "Microsoft.DataFactory/factories/globalParameters":    "GlobalParameter",
    "Microsoft.DataFactory/factories/managedVirtualNetworks":                            "ManagedVirtualNetwork",
    "Microsoft.DataFactory/factories/managedVirtualNetworks/managedPrivateEndpoints":    "ManagedPrivateEndpoint",
    "Microsoft.DataFactory/factories/privateEndpointConnections":                        "PrivateEndpointConnection",
}

def _name(resource: dict) -> str:
    raw = resource.get("name") or resource.get("Name") or "unknown"
    m = re.search(r"'([^']+)'\s*\)\s*\]$", raw)
    if m:
        path = m.group(1).lstrip("/")
        return path.rsplit("/", 1)[-1] if "/" in path else path
    return raw.rsplit("/", 1)[-1] if "/" in raw else raw

def _props(r: dict) -> dict:
    return r.get("properties") or r.get("Properties") or {}

def parse_arm(template):
    factory, comps = "", []
    factory_props = {}
    for r in template.get("resources", []):
        rt = r.get("type", "")
        if rt == "Microsoft.DataFactory/factories":
            factory = _name(r)
            factory_props = _props(r)
            continue
        friendly = TYPE_MAP.get(rt)
        if friendly:
            comps.append({"type": friendly, "name": _name(r), "properties": _props(r)})

    # Extract global parameters from the factory resource itself
    gp = factory_props.get("globalParameters", {})
    for gp_name, gp_def in gp.items():
        comps.append({
            "type": "GlobalParameter",
            "name": gp_name,
            "properties": gp_def if isinstance(gp_def, dict) else {"type": "string", "value": gp_def},
        })
    return factory, comps

def parse_pipelines(data):
    if isinstance(data, dict):
        data = [data]
    return "", [{"type": "Pipeline", "name": p.get("name") or p.get("Name", "unknown"),
                 "properties": p.get("properties") or p.get("Properties", {})} for p in data]

if input_mode == "arm_template":
    detected_factory, components = parse_arm(raw_json)
else:
    detected_factory, components = parse_pipelines(raw_json)

factory_name = factory_name_param or detected_factory or "UnknownFactory"
print(f"Factory: {factory_name}  |  Top-level components: {len(components)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Expand into All Component Types
# MAGIC
# MAGIC Pipelines are expanded into Activities, Parameters, and Variables so each
# MAGIC gets its own mapping row.

# COMMAND ----------

all_items = []   # each: {type, name, parent, subtype, properties}

for comp in components:
    ctype = comp["type"]
    name  = comp["name"]
    props = comp["properties"]

    # ---- Top-level component ----
    subtype = ""
    if ctype == "LinkedService":
        subtype = props.get("type", "")
    elif ctype == "Dataset":
        subtype = props.get("type", "")
    elif ctype == "Trigger":
        subtype = props.get("type", "")
    elif ctype == "DataFlow":
        subtype = props.get("type", "MappingDataFlow")
    elif ctype == "IntegrationRuntime":
        subtype = props.get("type", "Managed")
    elif ctype == "ChangeDataCapture":
        policy = props.get("policy", {})
        subtype = policy.get("mode", "Microbatch") if isinstance(policy, dict) else "Microbatch"
    elif ctype == "Credential":
        subtype = props.get("type", "")
    elif ctype == "GlobalParameter":
        subtype = props.get("type", "string")
    elif ctype == "ManagedVirtualNetwork":
        subtype = "Managed"
    elif ctype == "ManagedPrivateEndpoint":
        subtype = props.get("groupId", "")
    elif ctype == "PrivateEndpointConnection":
        subtype = props.get("privateLinkServiceConnectionState", {}).get("status", "") if isinstance(props.get("privateLinkServiceConnectionState"), dict) else ""

    all_items.append({"type": ctype, "name": name, "parent": "", "subtype": subtype, "properties": props})

    # ---- Pipeline children ----
    if ctype == "Pipeline":
        # Activities (top-level + inner)
        for act in props.get("activities", []):
            act_type = act.get("type", "")
            all_items.append({"type": "Activity", "name": act.get("name", "unnamed"),
                              "parent": name, "subtype": act_type, "properties": act})
            # Inner activities of control-flow constructs
            tp = act.get("typeProperties", {})
            inner = []
            if act_type == "ForEach":
                inner = tp.get("activities", [])
            elif act_type == "IfCondition":
                inner = tp.get("ifTrueActivities", []) + tp.get("ifFalseActivities", [])
            elif act_type == "Switch":
                for case in tp.get("cases", []):
                    inner.extend(case.get("activities", []))
                inner.extend(tp.get("defaultActivities", []))
            elif act_type == "Until":
                inner = tp.get("activities", [])
            for child in inner:
                all_items.append({"type": "Activity", "name": child.get("name", "unnamed"),
                                  "parent": f"{name}/{act.get('name','')}",
                                  "subtype": child.get("type", ""), "properties": child})

        # Parameters
        for pname, pdef in props.get("parameters", {}).items():
            ptype = pdef.get("type", "string") if isinstance(pdef, dict) else "string"
            all_items.append({"type": "Parameter", "name": pname, "parent": name,
                              "subtype": ptype, "properties": pdef if isinstance(pdef, dict) else {"type": ptype}})

        # Variables
        for vname, vdef in props.get("variables", {}).items():
            vtype = vdef.get("type", "String") if isinstance(vdef, dict) else "String"
            all_items.append({"type": "Variable", "name": vname, "parent": name,
                              "subtype": vtype, "properties": vdef if isinstance(vdef, dict) else {"type": vtype}})

print(f"Total items to map: {len(all_items)}")
for t in sorted({i["type"] for i in all_items}):
    print(f"  {t}: {sum(1 for i in all_items if i['type'] == t)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Rule-Based Mapping Table
# MAGIC
# MAGIC Deterministic ADF → Databricks mapping used as the **baseline** before
# MAGIC LLM enrichment. Covers all ADF component types and activity subtypes.

# COMMAND ----------

# ---- Databricks equivalents keyed by (component_type, subtype) ----
# Falls back to component_type-only if subtype key is absent.

MAPPING_RULES: dict[tuple[str, str] | str, dict[str, str]] = {
    # -- Activities --
    ("Activity", "Copy"): {
        "equivalent": "Auto Loader / COPY INTO / Spark JDBC read",
        "recommendation": "Use Auto Loader for file-based ingestion (ADLS/S3). Use COPY INTO for batch SQL loads. Use Spark JDBC read for database sources. If source is SaaS, evaluate Lakeflow Connect.",
    },
    ("Activity", "ExecuteDataFlow"): {
        "equivalent": "Delta Live Tables (DLT) Pipeline or PySpark Notebook",
        "recommendation": "Convert Mapping Data Flow script to DLT with @dlt.table and @dlt.expect decorators. For complex flows, use PySpark notebooks. DLT provides built-in data quality and lineage.",
    },
    ("Activity", "ExecutePipeline"): {
        "equivalent": "Databricks Workflow Run Job Task",
        "recommendation": "Use 'Run Job' task type in Databricks Workflows to call child jobs. Pass parameters via base_parameters.",
    },
    ("Activity", "ForEach"): {
        "equivalent": "Databricks Workflow For Each Task",
        "recommendation": "Use native For Each task in Databricks Workflows. For sequential processing, set concurrency=1. For parallel, set to desired batch size.",
    },
    ("Activity", "IfCondition"): {
        "equivalent": "Databricks Workflow If/Else Condition Task",
        "recommendation": "Use If/Else condition task in Databricks Workflows. Condition expressions translate to Jinja-style expressions referencing task values.",
    },
    ("Activity", "Switch"): {
        "equivalent": "Databricks Workflow If/Else Condition Task (chained)",
        "recommendation": "Chain multiple If/Else tasks to replicate Switch cases. Consider refactoring complex branching into a single notebook with Python conditional logic.",
    },
    ("Activity", "Until"): {
        "equivalent": "Notebook with while-loop + Workflow retry policy",
        "recommendation": "Implement retry logic inside a notebook. Alternatively, use Workflow task-level retry with configurable count and interval.",
    },
    ("Activity", "WebActivity"): {
        "equivalent": "Python requests in a Notebook Task",
        "recommendation": "Replace with Python `requests` library in a Databricks notebook. Store auth tokens in Databricks Secrets. Consider using dbutils.secrets.get().",
    },
    ("Activity", "Lookup"): {
        "equivalent": "spark.read / spark.sql() in Notebook",
        "recommendation": "Use spark.read.format('jdbc') for database lookups or spark.sql() for Delta table queries. Share results via dbutils.jobs.taskValues.set().",
    },
    ("Activity", "GetMetadata"): {
        "equivalent": "dbutils.fs.ls() / Delta table DESCRIBE",
        "recommendation": "Use dbutils.fs.ls() for file metadata. Use DESCRIBE DETAIL for Delta table metadata. Use spark.catalog API for schema inspection.",
    },
    ("Activity", "SetVariable"): {
        "equivalent": "dbutils.jobs.taskValues.set()",
        "recommendation": "Use taskValues to pass data between Workflow tasks. Within a notebook, use standard Python variables.",
    },
    ("Activity", "AppendVariable"): {
        "equivalent": "dbutils.jobs.taskValues.set() with list append",
        "recommendation": "Accumulate values in a Python list within a notebook, then set as taskValue at the end.",
    },
    ("Activity", "Wait"): {
        "equivalent": "time.sleep() in Notebook",
        "recommendation": "Use Python time.sleep() for simple waits. For event-driven waits, consider file-arrival triggers or table-update triggers.",
    },
    ("Activity", "Validation"): {
        "equivalent": "DLT Expectations / assert statements",
        "recommendation": "Use @dlt.expect / @dlt.expect_or_fail for declarative validation. Use Python assert in notebooks for ad-hoc checks.",
    },
    ("Activity", "SqlServerStoredProcedure"): {
        "equivalent": "spark.sql() via JDBC or Notebook SQL cell",
        "recommendation": "Call stored procedures via JDBC connection in PySpark. Consider migrating procedure logic to Spark SQL or Python.",
    },
    ("Activity", "AzureFunctionActivity"): {
        "equivalent": "Notebook Task calling HTTP endpoint",
        "recommendation": "Call Azure Functions via Python requests from a notebook. Alternatively, refactor lightweight function logic directly into PySpark.",
    },
    ("Activity", "Delete"): {
        "equivalent": "dbutils.fs.rm()",
        "recommendation": "Use dbutils.fs.rm(path, recurse=True) for file deletion. For Delta table row deletion, use DELETE FROM or Delta MERGE.",
    },
    ("Activity", "Custom"): {
        "equivalent": "Notebook Task or Python Script Task",
        "recommendation": "Refactor Azure Batch custom activities into Databricks notebook tasks. Container-based workloads may need review for Spark compatibility.",
    },
    ("Activity", "HDInsightSpark"): {
        "equivalent": "Databricks Notebook / Spark Submit Task",
        "recommendation": "Direct migration — Databricks is a superset of HDInsight Spark. Update cluster configs and storage paths.",
    },
    ("Activity", "DatabricksNotebook"): {
        "equivalent": "Databricks Notebook Task (native)",
        "recommendation": "Already Databricks-native. Move notebook path to DABs-managed path. Update cluster to job cluster.",
    },
    ("Activity", "DatabricksSparkPython"): {
        "equivalent": "Databricks Python Script Task",
        "recommendation": "Migrate to native Python script task type in Databricks Workflows.",
    },
    ("Activity", "DatabricksSparkJar"): {
        "equivalent": "Databricks JAR Task",
        "recommendation": "Migrate to native JAR task type. Upload JARs to Unity Catalog Volumes.",
    },
    ("Activity", "Filter"): {
        "equivalent": "PySpark filter() / WHERE clause in Notebook",
        "recommendation": "Replace with DataFrame.filter() or spark.sql() WHERE clause. Typically used to filter arrays — use Python list comprehension or PySpark array functions.",
    },
    ("Activity", "Fail"): {
        "equivalent": "raise Exception() / dbutils.notebook.exit() in Notebook",
        "recommendation": "Use Python raise or sys.exit(1) to fail a task. Use dbutils.notebook.exit('{\"status\":\"failed\"}') for structured exit. Workflow detects non-zero exit as failure.",
    },
    ("Activity", "Script"): {
        "equivalent": "spark.sql() or JDBC execute in Notebook",
        "recommendation": "Execute SQL scripts via spark.sql() for Delta/Spark SQL. For external databases, use JDBC connection to run stored procedures or DDL statements.",
    },
    ("Activity", "WebHook"): {
        "equivalent": "Notebook Task with async HTTP callback",
        "recommendation": "Implement webhook pattern with Python requests + polling/callback. Use Databricks Workflows REST API for external systems to report back completion.",
    },
    ("Activity", "ExecuteSSISPackage"): {
        "equivalent": "Refactor SSIS logic to PySpark / DLT Notebooks",
        "recommendation": "SSIS packages require significant refactoring. Decompose SSIS data flows into PySpark notebooks. Use DLT for ETL pipelines. Migrate SSIS control flow to Workflow DAG. High complexity migration.",
    },
    ("Activity", "AzureMLExecutePipeline"): {
        "equivalent": "MLflow Model Serving / Databricks Model Serving",
        "recommendation": "Migrate Azure ML pipelines to MLflow-based workflows on Databricks. Use Feature Store, MLflow tracking, and Model Serving for end-to-end ML lifecycle.",
    },
    ("Activity", "AzureMLBatchExecution"): {
        "equivalent": "Databricks Batch Inference Notebook / Model Serving",
        "recommendation": "Replace Azure ML batch scoring with Databricks batch inference using mlflow.pyfunc.spark_udf() or Model Serving batch endpoints.",
    },
    ("Activity", "AzureMLUpdateResource"): {
        "equivalent": "MLflow Model Registry update",
        "recommendation": "Replace with MLflow Model Registry model version transitions (staging → production). Use Databricks Model Serving for deployment.",
    },
    ("Activity", "USql"): {
        "equivalent": "Spark SQL / PySpark Notebook",
        "recommendation": "Rewrite U-SQL scripts in Spark SQL or PySpark. U-SQL concepts (rowsets, extractors) map to DataFrames and spark.read. Azure Data Lake Analytics is deprecated.",
    },
    ("Activity", "SynapseNotebook"): {
        "equivalent": "Databricks Notebook Task",
        "recommendation": "Migrate Synapse notebook code to Databricks notebooks. PySpark API is compatible. Update storage paths from abfss:// and adjust Synapse-specific APIs.",
    },
    ("Activity", "SynapseSparkJob"): {
        "equivalent": "Databricks Spark Submit / JAR Task",
        "recommendation": "Migrate Synapse Spark job definitions to Databricks. Submit via Spark Submit task or JAR task in Workflows.",
    },
    ("Activity", "HDInsightHive"): {
        "equivalent": "Spark SQL / Databricks SQL Notebook",
        "recommendation": "Convert Hive queries to Spark SQL. Most HiveQL is directly compatible with Spark SQL. Use Delta tables instead of Hive external tables.",
    },
    ("Activity", "HDInsightPig"): {
        "equivalent": "PySpark Notebook",
        "recommendation": "Rewrite Pig Latin scripts in PySpark. Pig LOAD/STORE → spark.read/write. Pig FOREACH/GENERATE → DataFrame select/withColumn. Pig FILTER → DataFrame.filter().",
    },
    ("Activity", "HDInsightMapReduce"): {
        "equivalent": "PySpark Notebook / Spark Submit Task",
        "recommendation": "Refactor MapReduce jobs to PySpark. Map phase → flatMap/map transformations. Reduce phase → groupBy/agg. Submit legacy JARs via Spark Submit if needed.",
    },
    ("Activity", "HDInsightStreaming"): {
        "equivalent": "Structured Streaming Notebook",
        "recommendation": "Replace Hadoop Streaming with Spark Structured Streaming. Use readStream/writeStream with Auto Loader for file-based streaming or Kafka connector.",
    },

    # -- Top-level components --
    "Pipeline": {
        "equivalent": "Databricks Workflow (Job)",
        "recommendation": "Convert pipeline to a Databricks Workflow with task DAG. Define in Databricks Asset Bundles (YAML) for CI/CD. Map activity dependencies to task depends_on.",
    },
    "Dataset": {
        "equivalent": "Unity Catalog Table / Volume / External Location",
        "recommendation": "Managed Delta tables for structured data. Unity Catalog Volumes for files (CSV, Parquet, JSON). External Locations for governed cloud storage paths.",
    },
    "LinkedService": {
        "equivalent": "Unity Catalog Connection + Databricks Secrets",
        "recommendation": "Create UC Connections for databases/SaaS. Store credentials in Databricks Secrets backed by Azure Key Vault. Use secret scopes for access control.",
    },
    "DataFlow": {
        "equivalent": "Delta Live Tables (DLT) or PySpark Notebook",
        "recommendation": "Convert data flow script to DLT pipeline with @dlt.table decorators and @dlt.expect quality gates. For complex logic, use PySpark notebooks in medallion architecture.",
    },
    "Trigger": {
        "equivalent": "Databricks Workflow Schedule / File Arrival Trigger",
        "recommendation": "Map ScheduleTrigger to cron schedule. Map BlobEventsTrigger to file-arrival trigger. Map TumblingWindowTrigger to continuous or table-update trigger.",
    },
    "IntegrationRuntime": {
        "equivalent": "Serverless Compute / Job Cluster / Cluster in VNet",
        "recommendation": "Azure IR → serverless compute. Self-hosted IR → cluster in customer-managed VNet with Private Link. SSIS IR → migrate SSIS packages to Spark/notebooks.",
    },
    "Parameter": {
        "equivalent": "Databricks Workflow Job Parameter / dbutils.widgets",
        "recommendation": "Pipeline parameters → Workflow job parameters (base_parameters). Access via dbutils.widgets.get() in notebooks.",
    },
    "Variable": {
        "equivalent": "dbutils.jobs.taskValues / Python variable",
        "recommendation": "Pipeline variables (intermediate state) → dbutils.jobs.taskValues.set/get for cross-task sharing, or Python variables within a single notebook.",
    },
    "ChangeDataCapture": {
        "equivalent": "Delta Live Tables (DLT) with Change Data Feed / Lakeflow Connect CDC",
        "recommendation": "Use DLT with APPLY CHANGES INTO for CDC processing. Enable Change Data Feed on Delta tables. For SaaS source CDC, use Lakeflow Connect managed CDC connectors.",
    },
    "Credential": {
        "equivalent": "Databricks Secret Scope / Unity Catalog Storage Credential",
        "recommendation": "Managed Identity credentials → UC Storage Credentials with managed identity. Service Principal credentials → Databricks Secret Scope. User-assigned MI → workspace-level identity federation.",
    },
    "GlobalParameter": {
        "equivalent": "Databricks Workflow Job Parameter / Cluster Init Script / Notebook Widget",
        "recommendation": "Factory-wide global parameters → Workflow-level job parameters or shared config notebook. For environment-specific values, use Databricks Secrets or cluster environment variables.",
    },
    "ManagedVirtualNetwork": {
        "equivalent": "Databricks Workspace VNet Injection / Private Link",
        "recommendation": "Managed VNet → Databricks VNet injection for workspace deployment. Configure NSG rules and private DNS zones. Use serverless with network connectivity configurations.",
    },
    "ManagedPrivateEndpoint": {
        "equivalent": "Databricks Private Link / Private Endpoint / Serverless Network Connectivity",
        "recommendation": "Create Azure Private Endpoints for target services. Use Databricks serverless connector framework or configure custom private endpoints in the VNet-injected workspace.",
    },
    "PrivateEndpointConnection": {
        "equivalent": "Databricks Private Link Connection",
        "recommendation": "Migrate private endpoint connections to Databricks Private Link. Configure front-end and back-end private links for workspace and storage access.",
    },
}

# Subtype-specific overrides for LinkedService, Dataset, Trigger, IR
SUBTYPE_OVERRIDES = {
    # LinkedService subtypes
    ("LinkedService", "AzureSqlDatabase"): {
        "equivalent": "UC Connection (SQL Server) + Databricks Secret Scope",
        "recommendation": "Create a Unity Catalog Connection of type sqlserver. Use Lakeflow Connect for managed CDC or Spark JDBC for batch reads.",
    },
    ("LinkedService", "AzureBlobFS"): {
        "equivalent": "Unity Catalog External Location + Storage Credential",
        "recommendation": "Create a Storage Credential (service principal or managed identity) and External Location pointing to the ADLS container.",
    },
    ("LinkedService", "AzureBlobStorage"): {
        "equivalent": "Unity Catalog External Location + Storage Credential",
        "recommendation": "Create Storage Credential + External Location. Migrate connection to use managed identity where possible.",
    },
    ("LinkedService", "AzureKeyVault"): {
        "equivalent": "Databricks Secret Scope backed by Azure Key Vault",
        "recommendation": "Create a Databricks secret scope backed by the same Azure Key Vault instance. Access via dbutils.secrets.get(scope, key).",
    },
    ("LinkedService", "CosmosDb"): {
        "equivalent": "Spark Cosmos DB Connector + UC Connection",
        "recommendation": "Use the azure-cosmos-spark connector. Store connection strings in Databricks Secrets.",
    },
    ("LinkedService", "Salesforce"): {
        "equivalent": "Lakeflow Connect (Salesforce)",
        "recommendation": "Use Lakeflow Connect Salesforce connector for managed ingestion with CDC support.",
    },
    ("LinkedService", "RestService"): {
        "equivalent": "Python requests in Notebook + Databricks Secrets",
        "recommendation": "Call REST APIs from notebooks using the requests library. Store API keys in secret scopes.",
    },
    # Dataset subtypes
    ("Dataset", "SqlServerTable"): {
        "equivalent": "Unity Catalog Table (ingested via JDBC/Lakeflow Connect)",
        "recommendation": "Ingest SQL Server tables into UC managed Delta tables. Use Lakeflow Connect for CDC or Spark JDBC for batch.",
    },
    ("Dataset", "Parquet"): {
        "equivalent": "Unity Catalog Table (Delta) or Volume (raw Parquet)",
        "recommendation": "Convert Parquet to managed Delta table for ACID, time travel, and Z-ORDER. Keep raw Parquet in Volumes if needed for external consumers.",
    },
    ("Dataset", "DelimitedText"): {
        "equivalent": "Unity Catalog Volume (raw CSV) → Auto Loader → Delta Table",
        "recommendation": "Land CSV files in a Volume. Use Auto Loader with schema inference to ingest into a managed Delta table.",
    },
    ("Dataset", "Json"): {
        "equivalent": "Unity Catalog Volume (raw JSON) → Auto Loader → Delta Table",
        "recommendation": "Land JSON in a Volume. Use Auto Loader with cloudFiles.format=json for streaming ingestion.",
    },
    ("Dataset", "AzureSqlTable"): {
        "equivalent": "Unity Catalog Table (ingested via JDBC/Lakeflow Connect)",
        "recommendation": "Same as SqlServerTable. Use incremental JDBC reads with watermark column.",
    },
    # Trigger subtypes
    ("Trigger", "ScheduleTrigger"): {
        "equivalent": "Databricks Workflow Cron Schedule",
        "recommendation": "Convert ADF schedule to Quartz cron expression on the Workflow. Minimum interval is 10 seconds.",
    },
    ("Trigger", "TumblingWindowTrigger"): {
        "equivalent": "Databricks Continuous Trigger / Table-Update Trigger",
        "recommendation": "Use table-update trigger for event-driven on UC table changes. Use continuous trigger for fixed-interval processing.",
    },
    ("Trigger", "BlobEventsTrigger"): {
        "equivalent": "Databricks File Arrival Trigger",
        "recommendation": "Use file-arrival trigger to monitor ADLS/S3/GCS for new files. Checks approximately every 1 minute.",
    },
    ("Trigger", "CustomEventsTrigger"): {
        "equivalent": "Databricks Workflow triggered via REST API",
        "recommendation": "Use Databricks Jobs REST API to trigger workflows from external event systems (Event Grid, Kafka, etc.).",
    },
    # Credential subtypes
    ("Credential", "ManagedIdentity"): {
        "equivalent": "Unity Catalog Storage Credential (Managed Identity)",
        "recommendation": "Map to UC Storage Credential using Azure Managed Identity. Assign workspace identity or user-assigned MI. Simplest credential migration path.",
    },
    ("Credential", "ServicePrincipal"): {
        "equivalent": "Databricks Secret Scope (Service Principal)",
        "recommendation": "Store SP client ID and secret in Databricks Secret Scope backed by Key Vault. Use for UC connections and external service authentication.",
    },
    # ManagedPrivateEndpoint subtypes (by groupId)
    ("ManagedPrivateEndpoint", "blob"): {
        "equivalent": "Private Endpoint to Azure Blob Storage",
        "recommendation": "Create Private Endpoint from Databricks VNet to Blob Storage account. Configure private DNS zone for blob.core.windows.net.",
    },
    ("ManagedPrivateEndpoint", "dfs"): {
        "equivalent": "Private Endpoint to Azure Data Lake Storage",
        "recommendation": "Create Private Endpoint from Databricks VNet to ADLS Gen2 account. Configure private DNS zone for dfs.core.windows.net.",
    },
    ("ManagedPrivateEndpoint", "sqlServer"): {
        "equivalent": "Private Endpoint to Azure SQL Database",
        "recommendation": "Create Private Endpoint from Databricks VNet to SQL Server. Configure private DNS zone for database.windows.net.",
    },
    ("ManagedPrivateEndpoint", "vault"): {
        "equivalent": "Private Endpoint to Azure Key Vault",
        "recommendation": "Create Private Endpoint from Databricks VNet to Key Vault. Configure Databricks Secret Scope backed by the private Key Vault.",
    },
    # IR subtypes
    ("IntegrationRuntime", "Managed"): {
        "equivalent": "Serverless Compute or Job Cluster",
        "recommendation": "Use serverless compute for zero-management. Use job clusters with autoscale for cost control.",
    },
    ("IntegrationRuntime", "SelfHosted"): {
        "equivalent": "Cluster in Customer-Managed VNet + Private Link",
        "recommendation": "Deploy Databricks workspace with VNet injection. Use Private Link for secure connectivity to on-premises resources. High migration complexity.",
    },
}

def get_mapping(item: dict) -> dict:
    """Return {equivalent, recommendation} for a component."""
    key_specific = (item["type"], item["subtype"])
    if key_specific in SUBTYPE_OVERRIDES:
        return SUBTYPE_OVERRIDES[key_specific]
    if key_specific in MAPPING_RULES:
        return MAPPING_RULES[key_specific]
    if item["type"] in MAPPING_RULES:
        return MAPPING_RULES[item["type"]]
    return {
        "equivalent": f"Databricks equivalent TBD ({item['type']}/{item['subtype']})",
        "recommendation": "Manual review required — no standard mapping available for this component type.",
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5. Extract Quantitative Metrics per Component
# MAGIC
# MAGIC Captures key numerical indicators used for complexity assessment and
# MAGIC downstream AI code generation. Merged from the former Inventory Analyzer.

# COMMAND ----------

def extract_metrics(item: dict) -> dict:
    """Return a metrics dict for the component (used as key_metrics column)."""
    props = item["properties"]
    ctype = item["type"]
    sub   = item.get("subtype", "")
    m: dict[str, Any] = {}

    if ctype == "Pipeline":
        activities = props.get("activities", [])
        act_types = [a.get("type", "") for a in activities]
        m["activity_count"] = len(activities)
        m["activity_types"] = sorted(set(act_types))
        m["parameter_count"] = len(props.get("parameters", {}))
        m["variable_count"] = len(props.get("variables", {}))
        m["has_foreach"] = "ForEach" in act_types
        m["has_until"] = "Until" in act_types
        m["has_switch"] = "Switch" in act_types
        m["has_ifcondition"] = "IfCondition" in act_types
        m["has_execute_pipeline"] = "ExecutePipeline" in act_types
        m["has_dataflow"] = "ExecuteDataFlow" in act_types
        m["has_web_activity"] = "WebActivity" in act_types
        m["expression_count"] = _count_expressions(props)
        # Max dependency chain length
        dep_map = {a.get("name", ""): [d.get("activity", "") for d in a.get("dependsOn", [])] for a in activities}
        def _chain_len(name, visited=None):
            if visited is None:
                visited = set()
            if name in visited:
                return 0
            visited.add(name)
            parents = dep_map.get(name, [])
            if not parents:
                return 0
            return 1 + max(_chain_len(p, visited) for p in parents)
        m["max_dependency_depth"] = max((_chain_len(n) for n in dep_map), default=0)

    elif ctype == "Activity":
        m["activity_type"] = sub
        m["has_retry"] = props.get("policy", {}).get("retry", 0) > 0
        m["retry_count"] = props.get("policy", {}).get("retry", 0)
        m["dependency_count"] = len(props.get("dependsOn", []))
        m["expression_count"] = _count_expressions(props.get("typeProperties", {}))
        tp = props.get("typeProperties", {})
        if sub == "Copy":
            m["has_staging"] = tp.get("enableStaging", False)
            m["has_column_mapping"] = "translator" in tp or "mappings" in tp
            m["source_type"] = tp.get("source", {}).get("type", "")
            m["sink_type"] = tp.get("sink", {}).get("type", "")
        elif sub == "ExecuteDataFlow":
            m["dataflow_ref"] = tp.get("dataflow", {}).get("referenceName", "")
            m["compute_type"] = tp.get("computeType", "")
            m["core_count"] = tp.get("coreCount", 0)
        elif sub == "ForEach":
            m["is_sequential"] = tp.get("isSequential", False)
            m["inner_activity_count"] = len(tp.get("activities", []))
            m["batch_count"] = tp.get("batchCount", 0)
        elif sub == "ExecutePipeline":
            m["child_pipeline"] = tp.get("pipeline", {}).get("referenceName", "")
            m["wait_on_completion"] = tp.get("waitOnCompletion", False)
        elif sub == "IfCondition":
            m["true_branch_count"] = len(tp.get("ifTrueActivities", []))
            m["false_branch_count"] = len(tp.get("ifFalseActivities", []))

    elif ctype == "DataFlow":
        tp = props.get("typeProperties", {})
        m["flow_type"] = props.get("type", "unknown")
        m["source_count"] = len(tp.get("sources", []))
        m["sink_count"] = len(tp.get("sinks", []))
        m["transformation_count"] = len(tp.get("transformations", []))
        m["transformation_names"] = [t.get("name", "") for t in tp.get("transformations", [])]
        script = tp.get("script", "")
        m["script_length"] = len(script)
        m["has_join"] = "join(" in script.lower()
        m["has_aggregate"] = "aggregate(" in script.lower()
        m["has_pivot"] = "pivot(" in script.lower()
        m["has_derived_column"] = "derive(" in script.lower()

    elif ctype == "Dataset":
        m["dataset_type"] = props.get("type", "")
        m["linked_service"] = props.get("linkedServiceName", {}).get("referenceName", "")
        m["parameter_count"] = len(props.get("parameters", {}))
        m["has_schema"] = bool(props.get("schema"))
        m["has_dynamic_path"] = bool(re.search(r"@dataset\(\)", json.dumps(props.get("typeProperties", {}))))

    elif ctype == "LinkedService":
        m["service_type"] = props.get("type", "")
        tp = props.get("typeProperties", {})
        m["uses_key_vault"] = "AzureKeyVaultSecret" in json.dumps(tp)
        m["has_parameters"] = bool(props.get("parameters"))
        m["auth_type"] = "KeyVault" if m["uses_key_vault"] else ("ServicePrincipal" if "servicePrincipalId" in json.dumps(tp) else "ConnectionString")

    elif ctype == "Trigger":
        m["trigger_type"] = props.get("type", "")
        m["pipeline_count"] = len(props.get("pipelines", []))
        tp = props.get("typeProperties", {})
        m["has_retry_policy"] = "retryPolicy" in tp
        m["frequency"] = tp.get("recurrence", tp).get("frequency", "")
        m["interval"] = tp.get("recurrence", tp).get("interval", "")

    elif ctype == "IntegrationRuntime":
        m["ir_type"] = props.get("type", "")
        managed_props = props.get("managedIntegrationRuntimeProperties", props.get("typeProperties", {}))
        compute = managed_props.get("computeProperties", {})
        m["node_size"] = compute.get("nodeSize", "")
        m["node_count"] = compute.get("numberOfNodes", 0)

    elif ctype == "ChangeDataCapture":
        tp = props.get("typeProperties", {})
        m["source_count"] = len(tp.get("sourceConnectionsInfo", []))
        m["target_count"] = len(tp.get("targetConnectionsInfo", []))
        policy = props.get("policy", {})
        m["mode"] = policy.get("mode", "") if isinstance(policy, dict) else ""

    elif ctype == "Credential":
        m["credential_type"] = props.get("type", "")

    elif ctype == "GlobalParameter":
        m["param_type"] = props.get("type", "string") if isinstance(props, dict) else "string"
        m["value"] = str(props.get("value", ""))[:100] if isinstance(props, dict) else ""

    elif ctype == "ManagedPrivateEndpoint":
        m["group_id"] = props.get("groupId", "")
        m["fqdns"] = props.get("fqdns", [])

    return m

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Complexity Scoring
# MAGIC
# MAGIC Rule-based baseline + LLM enrichment for nuanced reasoning.

# COMMAND ----------

def _count_expressions(obj, depth=0) -> int:
    if depth > 20:
        return 0
    if isinstance(obj, str):
        return len(re.findall(r"@\{|@pipeline\(\)|@activity\(\)|@dataset\(\)|@linkedService\(\)|@concat\(|@utcNow\(", obj))
    if isinstance(obj, dict):
        return sum(_count_expressions(v, depth + 1) for v in obj.values())
    if isinstance(obj, list):
        return sum(_count_expressions(v, depth + 1) for v in obj)
    return 0


def rule_based_complexity(item: dict) -> tuple[str, str]:
    """Return (score, reason) based on deterministic rules."""
    props = item["properties"]
    ctype = item["type"]
    sub   = item["subtype"]
    score = 0
    reasons = []

    if ctype == "Pipeline":
        acts = props.get("activities", [])
        act_types = {a.get("type", "") for a in acts}
        n = len(acts)
        if n > 10:   score += 3; reasons.append(f"{n} activities")
        elif n > 4:  score += 1
        for ctrl in ("ForEach", "Until", "Switch"):
            if ctrl in act_types:
                score += 2; reasons.append(f"has {ctrl}")
        if "IfCondition" in act_types:
            score += 1; reasons.append("has IfCondition")
        if "ExecutePipeline" in act_types:
            score += 2; reasons.append("nested ExecutePipeline")
        if "ExecuteDataFlow" in act_types:
            score += 1; reasons.append("uses DataFlow")
        expr = _count_expressions(props)
        if expr > 5: score += 1; reasons.append(f"{expr} expressions")

    elif ctype == "Activity":
        if sub in ("ForEach", "Until", "Switch"):           score += 2; reasons.append(f"{sub} control flow")
        if sub == "ExecutePipeline":                         score += 2; reasons.append("calls child pipeline")
        if sub == "Custom":                                  score += 3; reasons.append("Custom/Azure Batch activity")
        if sub == "ExecuteSSISPackage":                      score += 4; reasons.append("SSIS package — requires full rewrite")
        if sub in ("AzureMLExecutePipeline", "AzureMLBatchExecution", "AzureMLUpdateResource"):
            score += 3; reasons.append(f"Azure ML activity ({sub}) — requires ML pipeline migration")
        if sub == "USql":                                    score += 3; reasons.append("U-SQL — deprecated service, requires rewrite to Spark SQL")
        if sub in ("HDInsightHive", "HDInsightPig", "HDInsightMapReduce", "HDInsightStreaming"):
            score += 2; reasons.append(f"HDInsight {sub} — migrate to native Spark")
        if sub == "WebHook":                                 score += 1; reasons.append("webhook callback pattern")
        tp = props.get("typeProperties", {})
        if sub == "Copy":
            if tp.get("enableStaging"):                      score += 1; reasons.append("staging enabled")
            if "translator" in tp:                           score += 1; reasons.append("column mappings")
        expr = _count_expressions(tp)
        if expr > 3: score += 1; reasons.append(f"{expr} dynamic expressions")

    elif ctype == "DataFlow":
        tp = props.get("typeProperties", {})
        tc = len(tp.get("transformations", []))
        script = tp.get("script", "")
        if tc >= 5:       score += 3; reasons.append(f"{tc} transformations")
        elif tc >= 2:     score += 1
        if "join(" in script.lower():      score += 1; reasons.append("has join")
        if "pivot(" in script.lower():     score += 2; reasons.append("has pivot")
        if "derive(" in script.lower():    score += 1; reasons.append("derived columns")

    elif ctype == "LinkedService":
        if sub == "SelfHosted": score += 3; reasons.append("self-hosted IR dependency")
        tp_str = json.dumps(props.get("typeProperties", {}))
        if "AzureKeyVaultSecret" in tp_str: score += 1; reasons.append("Key Vault credential")
        if props.get("parameters"):         score += 1; reasons.append("parameterized linked service")

    elif ctype == "Dataset":
        if props.get("parameters"):         score += 1; reasons.append("parameterized dataset")
        if re.search(r"@dataset\(\)", json.dumps(props.get("typeProperties", {}))):
            score += 1; reasons.append("dynamic path expression")

    elif ctype == "Trigger":
        if sub == "TumblingWindowTrigger":  score += 2; reasons.append("tumbling window")
        if sub == "CustomEventsTrigger":    score += 2; reasons.append("custom event trigger")
        if props.get("typeProperties", {}).get("retryPolicy"): score += 1; reasons.append("retry policy")

    elif ctype == "IntegrationRuntime":
        if sub == "SelfHosted":  score += 3; reasons.append("self-hosted IR — network changes required")

    elif ctype == "ChangeDataCapture":
        source_count = len(props.get("typeProperties", {}).get("sourceConnectionsInfo", []))
        if source_count > 2:  score += 2; reasons.append(f"{source_count} source connections")
        policy = props.get("policy", {})
        if isinstance(policy, dict) and policy.get("mode") == "Microbatch":
            score += 1; reasons.append("microbatch CDC mode")

    elif ctype == "Credential":
        if sub == "ServicePrincipal":  score += 1; reasons.append("service principal credential")

    elif ctype == "GlobalParameter":
        gp_type = props.get("type", "string") if isinstance(props, dict) else "string"
        if gp_type in ("Array", "Object", "array", "object"):
            score += 1; reasons.append(f"complex global param type: {gp_type}")

    elif ctype == "ManagedVirtualNetwork":
        score += 2; reasons.append("managed VNet — requires Databricks VNet injection planning")

    elif ctype == "ManagedPrivateEndpoint":
        score += 2; reasons.append("private endpoint — requires Databricks Private Link configuration")

    elif ctype == "PrivateEndpointConnection":
        score += 2; reasons.append("private endpoint connection — requires network architecture review")

    elif ctype == "Parameter":
        ptype = props.get("type", "string") if isinstance(props, dict) else "string"
        if ptype in ("array", "object"):    score += 1; reasons.append(f"complex type: {ptype}")

    # Map score → level
    if score >= 5:   level = "High"
    elif score >= 2: level = "Medium"
    else:            level = "Low"
    reason = "; ".join(reasons) if reasons else "Standard component — straightforward migration"
    return level, reason

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. LLM Enrichment (Databricks Foundation Model)
# MAGIC
# MAGIC For each **Medium** or **High** item the LLM refines the reason and
# MAGIC recommendation. **Low** items use the rule-based output directly to
# MAGIC save tokens and latency.

# COMMAND ----------

client = mlflow.deployments.get_deploy_client("databricks")

SYSTEM_PROMPT = """You are an ADF-to-Databricks migration expert. Given an ADF component
with its rule-based mapping and complexity score, refine the analysis.

Respond with EXACTLY this JSON (no markdown, no extra text):
{
  "complexity_score": "<Low|Medium|High>",
  "complexity_reason": "<concise reason, 1-2 sentences>",
  "migration_recommendation": "<actionable migration guidance, 2-3 sentences>"
}

Guidelines:
- Low: Direct 1-to-1 mapping, minimal effort.
- Medium: Requires some refactoring, configuration changes, or logic adaptation.
- High: Significant redesign, multiple components to re-architect, or infrastructure changes.
"""


def llm_enrich(item: dict, rule_score: str, rule_reason: str, mapping: dict) -> dict:
    """Ask the LLM to refine complexity + recommendation."""
    config_summary = _config_summary(item)
    user_msg = json.dumps({
        "adf_component_type": item["type"],
        "adf_component_name": item["name"],
        "adf_subtype": item["subtype"],
        "rule_based_score": rule_score,
        "rule_based_reason": rule_reason,
        "databricks_equivalent": mapping["equivalent"],
        "rule_based_recommendation": mapping["recommendation"],
        "config_summary": config_summary,
    }, default=str)
    try:
        resp = client.predict(
            endpoint=llm_endpoint,
            inputs={
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": user_msg},
                ],
                "max_tokens": 300,
                "temperature": 0.05,
            },
        )
        text = resp["choices"][0]["message"]["content"].strip()
        s, e = text.index("{"), text.rindex("}") + 1
        return json.loads(text[s:e])
    except Exception as exc:
        print(f"  LLM fallback for {item['name']}: {exc}")
        return {"complexity_score": rule_score, "complexity_reason": rule_reason,
                "migration_recommendation": mapping["recommendation"]}


def _config_summary(item: dict, max_len=800) -> str:
    """Compact, safe summary of component config for the LLM prompt."""
    props = item["properties"]
    parts = []
    if item["type"] == "Activity":
        parts.append(f"activity_type={item['subtype']}")
        tp = props.get("typeProperties", {})
        if item["subtype"] == "Copy":
            parts.append(f"source={tp.get('source',{}).get('type','?')}")
            parts.append(f"sink={tp.get('sink',{}).get('type','?')}")
            parts.append(f"staging={tp.get('enableStaging', False)}")
        elif item["subtype"] == "ForEach":
            parts.append(f"inner_activities={len(tp.get('activities',[]))}")
            parts.append(f"sequential={tp.get('isSequential', False)}")
        elif item["subtype"] == "ExecuteDataFlow":
            parts.append(f"dataflow={tp.get('dataflow',{}).get('referenceName','?')}")
            parts.append(f"cores={tp.get('coreCount',0)}")
    elif item["type"] == "Pipeline":
        acts = props.get("activities", [])
        parts.append(f"activities={len(acts)}")
        parts.append(f"types={sorted({a.get('type','') for a in acts})}")
        parts.append(f"params={list(props.get('parameters',{}).keys())}")
    elif item["type"] == "DataFlow":
        tp = props.get("typeProperties", {})
        parts.append(f"transformations={len(tp.get('transformations',[]))}")
        parts.append(f"sources={len(tp.get('sources',[]))}, sinks={len(tp.get('sinks',[]))}")
    elif item["type"] == "Dataset":
        parts.append(f"type={props.get('type','')}")
        parts.append(f"linked_service={props.get('linkedServiceName',{}).get('referenceName','')}")
        parts.append(f"params={list(props.get('parameters',{}).keys())}")
    elif item["type"] == "LinkedService":
        parts.append(f"type={props.get('type','')}")
    elif item["type"] == "Trigger":
        parts.append(f"type={props.get('type','')}")
        parts.append(f"pipelines={[p.get('pipelineReference',{}).get('referenceName','') for p in props.get('pipelines',[])]}")
    elif item["type"] == "Parameter":
        parts.append(f"param_type={props.get('type','string')}")
        parts.append(f"default={props.get('defaultValue','(none)')}")
    elif item["type"] == "Variable":
        parts.append(f"var_type={props.get('type','String')}")
    elif item["type"] == "IntegrationRuntime":
        parts.append(f"ir_type={props.get('type','')}")
    elif item["type"] == "ChangeDataCapture":
        tp = props.get("typeProperties", {})
        parts.append(f"sources={len(tp.get('sourceConnectionsInfo',[]))}")
        parts.append(f"targets={len(tp.get('targetConnectionsInfo',[]))}")
        policy = props.get("policy", {})
        if isinstance(policy, dict):
            parts.append(f"mode={policy.get('mode','')}")
    elif item["type"] == "Credential":
        parts.append(f"cred_type={props.get('type','')}")
    elif item["type"] == "GlobalParameter":
        parts.append(f"param_type={props.get('type','string')}")
        parts.append(f"value={str(props.get('value','(none)'))[:100]}")
    elif item["type"] == "ManagedVirtualNetwork":
        parts.append("managed_vnet=true")
    elif item["type"] == "ManagedPrivateEndpoint":
        parts.append(f"groupId={props.get('groupId','')}")
        parts.append(f"fqdns={props.get('fqdns',[])}")
    elif item["type"] == "PrivateEndpointConnection":
        state = props.get("privateLinkServiceConnectionState", {})
        if isinstance(state, dict):
            parts.append(f"status={state.get('status','')}")
    return "; ".join(str(p) for p in parts)[:max_len]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Generate Full Mapping

# COMMAND ----------

timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
rows = []

for i, item in enumerate(all_items):
    mapping = get_mapping(item)
    rule_score, rule_reason = rule_based_complexity(item)

    # LLM enrichment only for Medium/High to save cost
    if rule_score in ("Medium", "High"):
        print(f"[{i+1}/{len(all_items)}] LLM enriching {item['type']}/{item['name']} ({rule_score}) ...", end=" ")
        enriched = llm_enrich(item, rule_score, rule_reason, mapping)
        final_score  = enriched.get("complexity_score", rule_score)
        final_reason = enriched.get("complexity_reason", rule_reason)
        final_rec    = enriched.get("migration_recommendation", mapping["recommendation"])
        print(f"→ {final_score}")
    else:
        final_score  = rule_score
        final_reason = rule_reason
        final_rec    = mapping["recommendation"]

    rows.append({
        "analysis_timestamp":       timestamp,
        "data_factory_name":        factory_name,
        "adf_component_type":       item["type"],
        "adf_component_name":       item["name"],
        "parent_component":         item.get("parent", ""),
        "adf_subtype":              item["subtype"],
        "databricks_equivalent":    mapping["equivalent"],
        "migration_recommendation": final_rec,
        "complexity_score":         final_score,
        "complexity_reason":        final_reason,
        "adf_config_summary":       _config_summary(item),
        "key_metrics":              json.dumps(extract_metrics(item), default=str),
    })

print(f"\nMapping complete: {len(rows)} rows generated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write to Unity Catalog

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {output_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {output_catalog}.{output_schema}")

results_df = spark.createDataFrame(rows)
results_df.write.format("delta").mode(output_write_mode).saveAsTable(output_fqn)

written = spark.table(output_fqn).count()
print(f"Wrote {len(rows)} rows to {output_fqn} (mode={output_write_mode})")
print(f"Table now has {written} total rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Display Results

# COMMAND ----------

display(
    spark.table(output_fqn)
    .orderBy("migration_phase_order", "adf_component_type", "adf_component_name")
    if "migration_phase_order" in spark.table(output_fqn).columns
    else spark.table(output_fqn).orderBy("complexity_score", "adf_component_type", "adf_component_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Summary

# COMMAND ----------

from collections import Counter

comp_counter = Counter(r["adf_component_type"] for r in rows)
score_counter = Counter(r["complexity_score"] for r in rows)

print("=" * 70)
print(f"COMPONENT MAPPING SUMMARY — {factory_name}")
print("=" * 70)
print(f"Total mapped items: {len(rows)}")
print()
print("By ADF Component Type:")
for ctype, cnt in comp_counter.most_common():
    high = sum(1 for r in rows if r["adf_component_type"] == ctype and r["complexity_score"] == "High")
    med  = sum(1 for r in rows if r["adf_component_type"] == ctype and r["complexity_score"] == "Medium")
    print(f"  {ctype:22s}: {cnt:3d}  (High={high}, Medium={med})")
print()
print("By Complexity Score:")
for level in ["High", "Medium", "Low"]:
    cnt = score_counter.get(level, 0)
    print(f"  {level:8s}: {cnt}")
print()
print("High-Complexity Items:")
for r in rows:
    if r["complexity_score"] == "High":
        print(f"  [{r['adf_component_type']}] {r['adf_component_name']}")
        print(f"    → {r['databricks_equivalent']}")
        print(f"    Reason: {r['complexity_reason']}")
print()
print("Mapping Table (sample):")
print(f"{'ADF Component':<22} {'Name':<28} {'Databricks Equivalent':<50} {'Complexity'}")
print("-" * 110)
for r in rows[:20]:
    print(f"{r['adf_component_type']:<22} {r['adf_component_name']:<28} {r['databricks_equivalent']:<50} {r['complexity_score']}")
if len(rows) > 20:
    print(f"  ... and {len(rows) - 20} more rows in {output_fqn}")
