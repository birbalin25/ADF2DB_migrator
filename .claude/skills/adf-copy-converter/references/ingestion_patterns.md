# Databricks Ingestion Patterns Reference

## Auto Loader (Recommended for File Sources)

Auto Loader incrementally processes new files as they arrive in cloud storage.

### Basic Pattern
```python
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")  # parquet|csv|json|avro|orc
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(source_path))

(df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable("catalog.schema.table"))
```

### CSV with Options
```python
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("delimiter", ",")
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", "false")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .load(source_path))
```

### JSON with Options
```python
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("multiLine", "true")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .load(source_path))
```

### File Event Mode (Fastest)
```python
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.useNotifications", "true")  # File events mode
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .load(source_path))
```

## COPY INTO (Batch File Loads)

### Basic Pattern
```sql
COPY INTO catalog.schema.target_table
FROM '/path/to/source/files'
FILEFORMAT = PARQUET
COPY_OPTIONS ('mergeSchema' = 'true');
```

### CSV with Options
```sql
COPY INTO catalog.schema.target_table
FROM '/path/to/source/files'
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header' = 'true',
    'delimiter' = ',',
    'inferSchema' = 'true',
    'mergeSchema' = 'true'
)
COPY_OPTIONS ('mergeSchema' = 'true');
```

### With Credential
```sql
COPY INTO catalog.schema.target_table
FROM 'abfss://container@account.dfs.core.windows.net/path'
FILEFORMAT = PARQUET;
-- Credentials managed via External Location / Storage Credential
```

## Spark JDBC (Database Sources)

### Basic Read
```python
jdbc_url = dbutils.secrets.get("scope", "jdbc_url")

df = (spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "schema.table_name")
    .option("user", dbutils.secrets.get("scope", "jdbc_user"))
    .option("password", dbutils.secrets.get("scope", "jdbc_password"))
    .load())
```

### Parallel Read (Large Tables)
```python
df = (spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "schema.table_name")
    .option("user", dbutils.secrets.get("scope", "jdbc_user"))
    .option("password", dbutils.secrets.get("scope", "jdbc_password"))
    .option("numPartitions", 8)
    .option("partitionColumn", "id")
    .option("lowerBound", 1)
    .option("upperBound", 1000000)
    .option("fetchsize", 10000)
    .load())
```

### Query Pushdown
```python
query = "(SELECT id, name, amount FROM sales WHERE date >= '2024-01-01') AS subquery"

df = (spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", query)
    .option("user", dbutils.secrets.get("scope", "jdbc_user"))
    .option("password", dbutils.secrets.get("scope", "jdbc_password"))
    .load())
```

### JDBC Write
```python
df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "schema.target_table") \
    .option("user", dbutils.secrets.get("scope", "jdbc_user")) \
    .option("password", dbutils.secrets.get("scope", "jdbc_password")) \
    .mode("append") \
    .save()
```

## Lakeflow Connect (Managed Connectors)

### Supported Sources (GA)
- Salesforce / Salesforce Service Cloud
- Workday
- SQL Server (CDC)
- ServiceNow
- Google Analytics

### Setup (via UC Connection)
```sql
-- Create connection
CREATE CONNECTION salesforce_conn
TYPE salesforce
OPTIONS (
    host 'https://myorg.my.salesforce.com',
    oauth_client_id SECRET('scope', 'sf_client_id'),
    oauth_client_secret SECRET('scope', 'sf_client_secret')
);

-- Create ingestion pipeline
CREATE OR REFRESH STREAMING TABLE catalog.schema.accounts
AS SELECT * FROM STREAM READ_FILES(
    'salesforce_conn.Account'
);
```

### Reading Ingested Data
```python
# Data lands in UC tables automatically
df = spark.table("catalog.schema.sf_accounts")
```

## Storage Path Patterns

| ADF Location Type | Databricks Path Format |
|---|---|
| `AzureBlobFSLocation` | `abfss://container@account.dfs.core.windows.net/folder/file` |
| `AzureBlobStorageLocation` | `wasbs://container@account.blob.core.windows.net/folder/file` |
| `AmazonS3Location` | `s3://bucket/folder/file` |
| `GoogleCloudStorageLocation` | `gs://bucket/folder/file` |
| `HttpServerLocation` | Download via `requests`, then process |
| `SftpLocation` | Download via SFTP library, then process |
| `HdfsLocation` | `hdfs://namenode:port/path` |
| `FileServerLocation` | Mount or stage to cloud storage first |

## Column Mapping Pattern

```python
from pyspark.sql.functions import col, lit, to_timestamp, to_date

# Direct column rename
df = df.select(
    col("SourceCol1").alias("target_col_1"),
    col("SourceCol2").cast("integer").alias("target_col_2"),
    col("DateCol").cast("timestamp").alias("event_timestamp"),
)

# Add constant columns
df = df.withColumn("_loaded_at", current_timestamp())
df = df.withColumn("_source", lit("adf_migration"))
```

## Error Handling Pattern

```python
try:
    df = spark.read.format("jdbc").option(...).load()
    row_count = df.count()
    df.write.mode("overwrite").saveAsTable("catalog.schema.table")

    dbutils.jobs.taskValues.set(key="status", value="success")
    dbutils.jobs.taskValues.set(key="rowsCopied", value=row_count)
except Exception as e:
    dbutils.jobs.taskValues.set(key="status", value="failed")
    dbutils.jobs.taskValues.set(key="error", value=str(e))
    raise
```
