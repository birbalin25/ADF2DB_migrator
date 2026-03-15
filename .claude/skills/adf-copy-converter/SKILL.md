---
name: adf-copy-converter
description: Convert ADF Copy Activities with their source/sink connectors, datasets, and linked services to Databricks ingestion notebooks
trigger_keywords:
  - Copy activity
  - data movement
  - ingestion
  - Auto Loader
  - COPY INTO
  - JDBC
  - linked service
  - dataset
  - connector
  - data ingestion
input: Copy Activity JSON + associated Dataset + LinkedService definitions
output: PySpark ingestion notebook + Unity Catalog connection config
---

# ADF Copy Converter

Convert ADF Copy Activities to Databricks ingestion notebooks using Auto Loader, COPY INTO, Spark JDBC, or Lakeflow Connect.

## What This Skill Does

1. **Reads** Copy Activity JSON with its source/sink configuration
2. **Resolves** the Dataset → LinkedService chain to determine the actual data source/sink
3. **Selects** the optimal Databricks ingestion pattern (Auto Loader, COPY INTO, JDBC, Lakeflow Connect)
4. **Generates** a PySpark ingestion notebook with proper error handling and Unity Catalog integration
5. **Handles** column mappings, staging, dynamic paths, and expression parameters

## Ingestion Pattern Selection

| Source Type | Pattern | When to Use |
|---|---|---|
| Cloud files (Parquet/CSV/JSON/Avro/ORC) | **Auto Loader** | Incremental file ingestion from cloud storage |
| Cloud files (batch) | **COPY INTO** | One-time or periodic batch file loads |
| SQL databases (Azure SQL, SQL Server, etc.) | **Spark JDBC** | Direct database reads |
| Supported SaaS (Salesforce, Workday, etc.) | **Lakeflow Connect** | Managed connector — no custom code |
| REST APIs | **requests + DataFrame** | Custom API ingestion |
| NoSQL (Cosmos DB, MongoDB) | **Spark connector** | Native Spark connector |

## Step-by-Step Instructions

### Step 1: Resolve the Data Source Chain

A Copy activity references datasets, which reference linked services:

```
Copy Activity
  → source.type (e.g., "AzureSqlSource")
  → inputs[0].referenceName → Dataset
    → dataset.linkedServiceName → LinkedService
      → linkedService.type (e.g., "AzureSqlDatabase")
      → linkedService.typeProperties (connection details)
```

Extract:
- **Source type** from `typeProperties.source.type`
- **Sink type** from `typeProperties.sink.type`
- **Source query** from `typeProperties.source.sqlReaderQuery` (if SQL)
- **Column mappings** from `typeProperties.translator.mappings[]`
- **Staging config** from `typeProperties.stagingSettings` (if `enableStaging: true`)
- **Dataset schema** from dataset `properties.schema[]`
- **Dataset parameters** from dataset `properties.parameters`
- **Connection details** from linked service `typeProperties`

### Step 2: Generate Ingestion Notebook

Based on the source type, generate the appropriate notebook pattern.

**Pattern A: Auto Loader (cloud file sources)**
```python
# Databricks notebook source
# Auto Loader ingestion: <source_description>
# Converted from ADF Copy Activity: <activity_name>

checkpoint_path = "/Volumes/catalog/schema/checkpoints/<table_name>"

df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "<parquet|csv|json|avro|orc>")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    # Source-specific options
    .option("header", "true")  # CSV only
    .option("inferSchema", "true")
    .load("<source_path>"))

# Column mappings (from TabularTranslator)
df = df.select(
    col("source_col1").alias("target_col1"),
    col("source_col2").alias("target_col2"),
)

(df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable("catalog.schema.table_name"))
```

**Pattern B: COPY INTO (batch file loads)**
```python
# Databricks notebook source
spark.sql("""
    COPY INTO catalog.schema.table_name
    FROM '<source_path>'
    FILEFORMAT = <PARQUET|CSV|JSON|AVRO|ORC>
    FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
    COPY_OPTIONS ('mergeSchema' = 'true')
""")
```

**Pattern C: Spark JDBC (database sources)**
```python
# Databricks notebook source
jdbc_url = dbutils.secrets.get("scope", "jdbc_url")
jdbc_user = dbutils.secrets.get("scope", "jdbc_user")
jdbc_password = dbutils.secrets.get("scope", "jdbc_password")

df = (spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("user", jdbc_user)
    .option("password", jdbc_password)
    .option("dbtable", "<schema.table>")  # or .option("query", "<sql_query>")
    .option("fetchsize", "10000")
    .load())

df.write.mode("overwrite").saveAsTable("catalog.schema.table_name")
```

**Pattern D: Lakeflow Connect (managed connectors)**
```python
# Databricks notebook source
# Lakeflow Connect handles this natively
# Configure via Unity Catalog Connections UI or Terraform

# Connection: catalog.schema.connection_name
# Ingestion is managed — no custom code needed

# To query ingested data:
df = spark.table("catalog.schema.table_name")
```

**Pattern E: REST API sources**
```python
# Databricks notebook source
import requests
import json

api_url = dbutils.widgets.get("api_url")
api_token = dbutils.secrets.get("scope", "api_token")

response = requests.get(
    api_url,
    headers={"Authorization": f"Bearer {api_token}"},
    timeout=120
)
response.raise_for_status()

data = response.json()
df = spark.createDataFrame(data["value"])
df.write.mode("overwrite").saveAsTable("catalog.schema.table_name")
```

### Step 3: Handle Column Mappings

If the Copy activity has `TabularTranslator` mappings:
```json
{
  "translator": {
    "type": "TabularTranslator",
    "mappings": [
      { "source": { "name": "CustomerID" }, "sink": { "name": "customer_id" } },
      { "source": { "name": "OrderDate" }, "sink": { "name": "order_date" } }
    ]
  }
}
```

Generate:
```python
from pyspark.sql.functions import col

df = df.select(
    col("CustomerID").alias("customer_id"),
    col("OrderDate").alias("order_date"),
)
```

### Step 4: Handle Dynamic Paths and Expressions

If the dataset has parameterized paths:
```json
{
  "typeProperties": {
    "location": {
      "type": "AzureBlobFSLocation",
      "fileName": "@dataset().fileName",
      "folderPath": "@concat(dataset().year, '/', dataset().month)",
      "fileSystem": "container"
    }
  }
}
```

Translate expressions and generate:
```python
file_name = dbutils.widgets.get("fileName")
year = dbutils.widgets.get("year")
month = dbutils.widgets.get("month")

source_path = f"abfss://container@storage.dfs.core.windows.net/{year}/{month}/{file_name}"
```

### Step 5: Handle Staging

If `enableStaging: true`, the Copy activity uses a staging blob for data transfer. In Databricks, this is typically unnecessary (direct read), but for large JDBC reads:

```python
# For large JDBC reads, consider using temporary staging
df = (spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", table_name)
    .option("numPartitions", "8")
    .option("partitionColumn", "id")
    .option("lowerBound", "1")
    .option("upperBound", "1000000")
    .load())
```

### Step 6: Output

Write the generated notebook to `src/notebooks/ingestion/<activity_name>.py`.

Connector-specific details are in `references/connector_mapping.md`.
Ingestion pattern details are in `references/ingestion_patterns.md`.

## Important Notes

- Always use `dbutils.secrets.get()` for credentials — never hardcode
- Use Unity Catalog 3-level namespace for all table references
- Auto Loader with `availableNow=True` mimics batch behavior with schema evolution support
- For Lakeflow Connect-supported sources (Salesforce, Workday, SQL Server CDC, ServiceNow, Google Analytics), prefer the managed connector over custom code
- The `references/connector_mapping.md` file has detailed mappings for all 90+ ADF connectors
