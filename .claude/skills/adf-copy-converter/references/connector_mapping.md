# ADF Connector → Databricks Mapping (90+ Connectors)

## Tier 1: Native / Lakeflow Connect (GA)

| ADF Connector | Spark Format / Approach | Connection Options | Auth Method | Lakeflow Connect |
|---|---|---|---|---|
| AzureSqlDatabase | `jdbc` | `url`, `dbtable`, `user`, `password` | SQL Auth / AAD Token | Coming soon |
| AzureBlobFS (ADLS Gen2) | `cloudFiles` / direct path | `abfss://container@account.dfs.core.windows.net/path` | Service Principal / Managed Identity | N/A (native) |
| AzureBlobStorage | `cloudFiles` / direct path | `wasbs://container@account.blob.core.windows.net/path` | Storage Key / SAS Token | N/A (native) |
| CosmosDb | `cosmos.oltp` | `spark.cosmos.accountEndpoint`, `spark.cosmos.accountKey`, `spark.cosmos.database`, `spark.cosmos.container` | Account Key / AAD | No |
| CosmosDbMongoDbApi | `mongodb` | `spark.mongodb.input.uri` | Connection String | No |
| Salesforce / SalesforceV2 | Lakeflow Connect | UC Connection config | OAuth 2.0 | **Yes (GA)** |
| SalesforceServiceCloud / SalesforceServiceCloudV2 | Lakeflow Connect | UC Connection config | OAuth 2.0 | **Yes (GA)** |

## Tier 2: JDBC / Spark Connectors

| ADF Connector | Spark Format / Approach | Connection Options | Auth Method | Lakeflow Connect |
|---|---|---|---|---|
| SqlServer | `jdbc` | `url=jdbc:sqlserver://host:1433;database=db`, `dbtable` | SQL Auth | **Yes (CDC)** |
| AzureSqlDW (Synapse) | `sqldw` | `url`, `dbtable`, `tempDir`, `forwardSparkAzureStorageCredentials` | SQL Auth / AAD | No |
| MySql / AzureMySql | `jdbc` | `url=jdbc:mysql://host:3306/db`, `dbtable` | SQL Auth | No |
| PostgreSql / AzurePostgreSql | `jdbc` | `url=jdbc:postgresql://host:5432/db`, `dbtable` | SQL Auth | No |
| Oracle | `jdbc` | `url=jdbc:oracle:thin:@host:1521:SID`, `dbtable` | SQL Auth | No |
| Db2 | `jdbc` | `url=jdbc:db2://host:50000/db`, `dbtable` | SQL Auth | No |
| Teradata | `jdbc` | `url=jdbc:teradata://host/DATABASE=db`, `dbtable` | SQL Auth | No |
| Snowflake | `snowflake` | `sfUrl`, `sfDatabase`, `sfSchema`, `sfWarehouse` | User/Password / OAuth | No |
| AmazonRedshift | `redshift` | `url`, `dbtable`, `tempdir`, `aws_iam_role` | IAM Role | No |
| GoogleBigQuery | `bigquery` | `table`, `project`, `parentProject` | Service Account JSON | No |
| Informix | `jdbc` | `url=jdbc:informix-sqli://host:port/db` | SQL Auth | No |
| Netezza | `jdbc` | `url=jdbc:netezza://host:5480/db` | SQL Auth | No |
| Vertica | `jdbc` | `url=jdbc:vertica://host:5433/db` | SQL Auth | No |
| MariaDB | `jdbc` | `url=jdbc:mariadb://host:3306/db` | SQL Auth | No |
| Sybase (SAP ASE) | `jdbc` | `url=jdbc:sybase:Tds:host:5000/db` | SQL Auth | No |
| SapHana | `jdbc` | `url=jdbc:sap://host:30015` | SQL Auth | No |
| SapBw / SapBwOpenHub | `com.databricks.spark.sap` (partner) | SAP connection params | SAP Auth | No |
| SapTable / SapEcc / SapOpenHub | `com.databricks.spark.sap` (partner) | SAP RFC params | SAP Auth | No |

## Tier 3: REST / API Connectors

| ADF Connector | Spark Format / Approach | Connection Options | Auth Method | Lakeflow Connect |
|---|---|---|---|---|
| RestService | `requests` + `spark.createDataFrame()` | `url`, `method`, `headers` | Basic / OAuth / API Key / MSI | No |
| HttpServer | `requests` + `spark.createDataFrame()` | `url`, `method` | Basic / Anonymous / Client Cert | No |
| OData | `requests` (OData client) | `url`, `$filter`, `$select`, `$expand` | Basic / OAuth / MSI | No |
| Dynamics365 / DynamicsCrm | `requests` (Dataverse API) | `serviceUri`, `entityName` | Service Principal / MSI | No |
| DynamicsAx | `requests` (OData) | `serviceUri`, `aadResourceId` | Service Principal | No |
| ServiceNow / ServiceNowV2 | `requests` (REST API) or Lakeflow Connect | `url`, `table` | Basic / OAuth | **Yes (GA)** |
| Marketo | `requests` (REST API) | `endpoint`, `clientId`, `clientSecret` | OAuth 2.0 | No |
| Concur | `requests` (REST API) | `connectionProperties.host` | OAuth 2.0 | No |
| Hubspot | `requests` (REST API) | `connectionProperties.host` | API Key / OAuth | No |
| Jira | `requests` (REST API) | `host`, `username` | Basic Auth | No |
| Magento | `requests` (REST API) | `host` | Access Token | No |
| PayPal | `requests` (REST API) | `host`, `clientId`, `clientSecret` | OAuth 2.0 | No |
| QuickBooks | `requests` (REST API) | `endpoint`, `companyId` | OAuth 2.0 | No |
| Shopify | `requests` (REST API) | `host`, `accessToken` | API Key | No |
| Square | `requests` (REST API) | `connectionProperties.host` | OAuth 2.0 | No |
| Xero | `requests` (REST API) | `host` | OAuth 2.0 | No |
| Zoho | `requests` (REST API) | `connectionProperties.host` | OAuth 2.0 | No |
| GoogleAdWords | `requests` (Google Ads API) | `clientCustomerID`, `developerToken` | OAuth 2.0 | No |
| GoogleAnalytics | Lakeflow Connect or `requests` | `serviceAccountEmail`, `keyFilePath` | Service Account | **Yes** |
| Responsys (Oracle) | `requests` (REST API) | `endpoint`, `clientId`, `clientSecret` | OAuth 2.0 | No |
| Eloqua (Oracle) | `requests` (REST API) | `endpoint`, `username`, `password` | Basic Auth | No |
| AzureSearch | `requests` (Search REST API) | `url`, `key` | API Key | No |
| AzureTableStorage | `requests` (Table API) or `jdbc` | `connectionString`, `tableName` | Storage Key / SAS | No |
| CommonDataServiceForApps | `requests` (Dataverse API) | `serviceUri` | Service Principal | No |
| SharePointOnlineList | `requests` (Graph API) | `siteUrl`, `listName` | Service Principal | No |
| Office365 | `requests` (Graph API) | `office365TenantId` | Service Principal | No |
| Workday | Lakeflow Connect | UC Connection config | OAuth 2.0 | **Yes (GA)** |

## Tier 4: File System Connectors

| ADF Connector | Spark Format / Approach | Connection Options | Auth Method | Lakeflow Connect |
|---|---|---|---|---|
| Sftp | `spark.read` via SFTP library or `requests` | `host`, `port`, `userName` | Password / SSH Key | No |
| FtpServer | `spark.read` via staged download | `host`, `port`, `userName` | Basic / Anonymous | No |
| Hdfs | `spark.read` direct path | `hdfs://namenode:port/path` | Kerberos | No |
| AmazonS3 / AmazonS3Compatible | `cloudFiles` / direct path | `s3://bucket/path` | IAM Role / Access Key | N/A (native) |
| GoogleCloudStorage | `cloudFiles` / direct path | `gs://bucket/path` | Service Account | N/A (native) |
| FileServer | Mount or staged copy | Local path | Windows Auth / Basic | No |
| AzureDataLakeStore (Gen1) | **Migrate to Gen2** | `adl://account.azuredatalakestore.net/path` | Service Principal | No |
| OracleCloudStorage | `requests` + download | Object Storage URL | API Key | No |

## Tier 5: Specialized Connectors

| ADF Connector | Spark Format / Approach | Connection Options | Auth Method | Lakeflow Connect |
|---|---|---|---|---|
| MongoDb / MongoDbV2 / MongoDbAtlas | `mongodb` | `spark.mongodb.input.uri=mongodb://host/db.collection` | Connection String | No |
| Cassandra | `org.apache.spark.sql.cassandra` | `spark.cassandra.connection.host`, `table`, `keyspace` | Username/Password | No |
| HBase | `org.apache.hadoop.hbase.spark` | `hbase.zookeeper.quorum`, `tableName` | Kerberos | No |
| Couchbase | `requests` (N1QL API) | `connectionString`, `bucketName` | Username/Password | No |
| AmazonDynamoDB | `dynamodb` (EMR connector) | `tableName`, `region` | IAM Role / Access Key | No |
| AzureDataExplorer (Kusto) | `com.microsoft.kusto.spark.datasource` | `kustoCluster`, `kustoDatabase`, `kustoTable` | AAD App | No |
| Phoenix | `jdbc` | `url=jdbc:phoenix:zk_host:/hbase` | Kerberos | No |
| Drill | `jdbc` | `url=jdbc:drill:drillbit=host` | Plain / Kerberos | No |
| Presto | `jdbc` | `url=jdbc:presto://host:8080/catalog` | LDAP / Kerberos | No |
| Spark (Hive) | `jdbc` or native | `url=jdbc:hive2://host:10000/db` | Kerberos / LDAP | No |
| AzureDatabricks | Native — no conversion | Already on Databricks | Workspace token | N/A |
| Impala | `jdbc` | `url=jdbc:impala://host:21050/db` | LDAP / Kerberos | No |
| Greenplum | `jdbc` | `url=jdbc:pivotal:greenplum://host:5432;DatabaseName=db` | SQL Auth | No |

## Source Type → Spark Read Pattern Quick Reference

| ADF Source Type | Spark Read Pattern |
|---|---|
| `AzureSqlSource` | `spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", table)` |
| `SqlServerSource` | `spark.read.format("jdbc").option("url", jdbc_url).option("dbtable", table)` |
| `AzureSqlDWSource` | `spark.read.format("sqldw").option("url", url).option("dbtable", table)` |
| `ParquetSource` | `spark.read.format("parquet").load(path)` or Auto Loader |
| `DelimitedTextSource` | `spark.read.format("csv").option("header", True).load(path)` or Auto Loader |
| `JsonSource` | `spark.read.format("json").load(path)` or Auto Loader |
| `AvroSource` | `spark.read.format("avro").load(path)` or Auto Loader |
| `OrcSource` | `spark.read.format("orc").load(path)` or Auto Loader |
| `BinarySource` | `spark.read.format("binaryFile").load(path)` |
| `ExcelSource` | `pandas.read_excel(path)` → `spark.createDataFrame()` |
| `XmlSource` | `spark.read.format("com.databricks.spark.xml").load(path)` |
| `RestSource` | `requests.get()` → `spark.createDataFrame()` |
| `CosmosDbSqlApiSource` | `spark.read.format("cosmos.oltp").option(...)` |
| `MongoDbV2Source` | `spark.read.format("mongodb").option(...)` |
| `SalesforceSource` | Lakeflow Connect (managed) |
| `ODataSource` | `requests` with OData parameters |
| `DynamicsSource` | `requests` with Dataverse API |

## Sink Type → Spark Write Pattern Quick Reference

| ADF Sink Type | Spark Write Pattern |
|---|---|
| `ParquetSink` | `df.write.format("parquet").save(path)` or `.saveAsTable()` |
| `DelimitedTextSink` | `df.write.format("csv").option("header", True).save(path)` |
| `JsonSink` | `df.write.format("json").save(path)` |
| `AzureSqlSink` | `df.write.format("jdbc").option("dbtable", table).save()` |
| `SqlServerSink` | `df.write.format("jdbc").option("dbtable", table).save()` |
| `AzureSqlDWSink` | `df.write.format("sqldw").option("dbtable", table).save()` |
| `CosmosDbSqlApiSink` | `df.write.format("cosmos.oltp").option(...).save()` |
| `DeltaSink` | `df.write.format("delta").saveAsTable("catalog.schema.table")` |
| Any sink to UC table | `df.write.mode("overwrite").saveAsTable("catalog.schema.table")` |
