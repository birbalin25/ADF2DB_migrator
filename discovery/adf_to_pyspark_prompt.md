# AI-Assisted ADF to PySpark Conversion Prompt Template

Use this structured prompt with Claude or other LLMs to convert ADF pipeline definitions to Databricks code.

## Prompt

```
<adf_pipeline>
[Paste the full ARM template JSON for this pipeline here]
</adf_pipeline>

<adf_data_flow>
[If the pipeline has a Mapping Data Flow, paste the Data Flow script here]
</adf_data_flow>

<linked_services>
[Paste relevant linked service definitions (redact credentials)]
</linked_services>

<requirements>
Target platform: Databricks on Azure
Architecture: Medallion (Bronze / Silver / Gold)
Code style: PySpark with Delta Lake

Specific requirements:
- Use Unity Catalog: {catalog_name}.{schema}.{table}
- Use Databricks Secrets scope "{secret_scope}" for all credentials
- Add ingestion metadata columns (ingestion_ts, source) to Bronze
- Deduplicate on primary key in Silver layer
- Add DLT expectations for data quality (or assert statements if using notebooks)
- Use MERGE/UPSERT for Gold layer updates
- Include error handling and structured logging
- Pass metrics between tasks using dbutils.jobs.taskValues
- Generate the Databricks Asset Bundle (DAB) YAML for the workflow
</requirements>

Generate the following Databricks artifacts:
1. Bronze ingestion notebook (PySpark)
2. Silver/Gold transformation notebook (PySpark)
3. Alternative DLT pipeline definition
4. Validation notebook with data quality checks
5. DAB YAML job definition with task dependencies
```

## Tips for Best Results

1. **Include the full ARM JSON** - Don't summarize; the model needs activity types, schemas, and connection details
2. **Include Data Flow scripts** - These contain the actual transformation logic (joins, filters, aggregates)
3. **Specify naming conventions** - Catalog, schema, and table naming patterns for your org
4. **Iterate** - Use Databricks Assistant to refine the generated code in-notebook
5. **Review joins carefully** - ADF Data Flow join conditions may use different column names than expected
6. **Check type mappings** - Azure SQL types may need explicit casting in Spark
