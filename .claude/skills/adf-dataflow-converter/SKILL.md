---
name: adf-dataflow-converter
description: Convert ADF Mapping Data Flows to Databricks Lakeflow Declarative Pipelines (DLT) or PySpark notebooks
trigger_keywords:
  - data flow
  - mapping data flow
  - transformation
  - DLT
  - Delta Live Tables
  - Lakeflow Declarative Pipeline
  - join
  - aggregate
  - pivot
  - derived column
  - DFL
input: DataFlow JSON including typeProperties.script or scriptLines (the DFL script)
output: DLT notebook with @dp.table / @dp.materialized_view decorators, or PySpark notebook
---

# ADF DataFlow Converter

Convert ADF Mapping Data Flows to Databricks Lakeflow Declarative Pipelines (DLT) or PySpark notebooks.

## What This Skill Does

1. **Reads** DataFlow JSON from the ARM template (including the DFL script in `scriptLines` or `script`)
2. **Parses** the Data Flow Language (DFL) script to identify sources, sinks, and transformations
3. **Maps** each DFL transformation to its PySpark/DLT equivalent
4. **Generates** either a DLT notebook (using `from pyspark import pipelines as dp`) or a standard PySpark notebook
5. **Handles** complex transformations: joins, aggregates, pivots, window functions, conditional splits

## DFL Transformation → Spark/DLT Mapping

| ADF DFL Transform | PySpark Equivalent | DLT Equivalent |
|---|---|---|
| `source(...)` | `spark.read.format(...).load(...)` | `dp.read(table_name)` or `dp.read_stream(table_name)` |
| `sink(...)` | `df.write.saveAsTable(...)` | `@dp.table(name="...")` decorator |
| `filter(condition)` | `.filter(condition)` | `.filter(condition)` |
| `select(cols)` | `.select(col1, col2, ...)` | `.select(col1, col2, ...)` |
| `derive(col = expr)` | `.withColumn("col", expr)` | `.withColumn("col", expr)` |
| `aggregate(groupBy(...), agg(...))` | `.groupBy(...).agg(...)` | `.groupBy(...).agg(...)` |
| `join(right, on, type)` | `.join(right, on, type)` | `.join(right, on, type)` |
| `union(byName)` | `.unionByName(other)` | `.unionByName(other)` |
| `sort(col asc/desc)` | `.orderBy(col.asc())` | `.orderBy(col.asc())` |
| `pivot(groupBy, pivotCol, agg)` | `.groupBy(...).pivot(col).agg(...)` | `.groupBy(...).pivot(col).agg(...)` |
| `unpivot(cols, nameCol, valueCol)` | `stack()` expression | `stack()` expression |
| `window(partition, order, frame)` | `Window.partitionBy(...).orderBy(...)` | Same |
| `conditionalSplit(conditions)` | Multiple `.filter()` calls | Multiple `@dp.table` with filters |
| `flatten(col)` | `explode(col)` | `explode(col)` |
| `exists(right, on)` | `.join(right, on, "left_semi")` | `.join(right, on, "left_semi")` |
| `lookup(right, on)` | `.join(right, on, "left")` | `.join(right, on, "left")` |
| `surrogateKey(col)` | `monotonically_increasing_id()` | `monotonically_increasing_id()` |
| `alterRow(conditions)` | Delta MERGE statement | DLT `APPLY CHANGES` |
| `rank(partition, order)` | `row_number().over(window)` | `row_number().over(window)` |
| `stringify(col)` | `.cast("string")` | `.cast("string")` |
| `parse(col, format)` | `.cast(type)` or `to_timestamp()` | Same |

## Step-by-Step Instructions

### Step 1: Extract the DFL Script

The Data Flow script is in:
- `properties.typeProperties.scriptLines[]` — array of strings (join with newline)
- OR `properties.typeProperties.script` — single string

Also extract:
- `properties.typeProperties.sources[]` — source definitions with dataset references
- `properties.typeProperties.sinks[]` — sink definitions with dataset references
- `properties.typeProperties.transformations[]` — transformation names

### Step 2: Parse DFL Script Lines

DFL scripts follow the pattern:
```
input_stream transformation(parameters) ~> output_stream
```

Examples:
```
source(output(id as integer, name as string), allowSchemaDrift: true) ~> sourceData
sourceData filter(amount > 0) ~> filteredData
filteredData derive(full_name = concat(first_name, ' ', last_name)) ~> enrichedData
enrichedData, dimTable join(id == dim_id, joinType:'left') ~> joinedData
joinedData aggregate(groupBy(category), total = sum(amount), count = count()) ~> aggregated
aggregated sink(allowSchemaDrift: true) ~> outputSink
```

Parse each line to extract:
1. **Input stream(s)** — name(s) before the transformation keyword
2. **Transformation type** — the keyword (filter, derive, join, aggregate, etc.)
3. **Parameters** — the parenthesized content
4. **Output stream** — name after `~>`

### Step 3: Choose Output Format

**Use DLT (Lakeflow Declarative Pipelines)** when:
- The data flow is a straightforward ETL pipeline (source → transform → sink)
- Sources are Delta tables or streaming sources
- You want automatic data quality checks and lineage

**Use PySpark notebook** when:
- Complex control flow (conditional splits with many branches)
- External API calls within the flow
- The flow is a one-time batch transformation

### Step 4: Generate DLT Notebook

```python
# Databricks notebook source
# Converted from ADF Data Flow: <flow_name>

from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Source
@dp.table(
    name="source_data",
    comment="Ingested from <source_description>"
)
def source_data():
    return dp.read("catalog.schema.source_table")

# Transformations
@dp.table(
    name="filtered_data",
    comment="Filtered: amount > 0"
)
def filtered_data():
    return dp.read("source_data").filter(col("amount") > 0)

@dp.table(
    name="enriched_data",
    comment="Enriched with derived columns"
)
def enriched_data():
    return (dp.read("filtered_data")
        .withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
    )

# Join
@dp.table(
    name="joined_data",
    comment="Joined with dimension table"
)
def joined_data():
    enriched = dp.read("enriched_data")
    dim = dp.read("catalog.schema.dim_table")
    return enriched.join(dim, enriched.id == dim.dim_id, "left")

# Aggregation (final sink)
@dp.table(
    name="catalog.schema.output_table",
    comment="Aggregated metrics by category"
)
def output_table():
    return (dp.read("joined_data")
        .groupBy("category")
        .agg(
            sum("amount").alias("total"),
            count("*").alias("count")
        )
    )
```

### Step 5: Generate PySpark Notebook (Alternative)

```python
# Databricks notebook source
# Converted from ADF Data Flow: <flow_name>

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Source
source_data = spark.table("catalog.schema.source_table")

# Filter
filtered_data = source_data.filter(col("amount") > 0)

# Derived columns
enriched_data = filtered_data.withColumn(
    "full_name", concat(col("first_name"), lit(" "), col("last_name"))
)

# Join
dim_table = spark.table("catalog.schema.dim_table")
joined_data = enriched_data.join(dim_table, enriched_data.id == dim_table.dim_id, "left")

# Aggregate
output_data = (joined_data
    .groupBy("category")
    .agg(
        sum("amount").alias("total"),
        count("*").alias("count")
    ))

# Write to sink
output_data.write.mode("overwrite").saveAsTable("catalog.schema.output_table")
```

### Step 6: Handle Complex Transformations

**Conditional Split** — Multiple output streams based on conditions:

DFL:
```
source conditionalSplit(
    amount > 1000 -> 'highValue',
    amount > 100 -> 'medValue',
    true -> 'lowValue'
) ~> splitByValue
```

DLT:
```python
@dp.table(name="high_value_orders")
def high_value():
    return dp.read("source_data").filter(col("amount") > 1000)

@dp.table(name="med_value_orders")
def med_value():
    return dp.read("source_data").filter((col("amount") > 100) & (col("amount") <= 1000))

@dp.table(name="low_value_orders")
def low_value():
    return dp.read("source_data").filter(col("amount") <= 100)
```

**Window Functions**:

DFL:
```
source window(over(category, order_date asc),
    running_total = sum(amount),
    row_num = rowNumber()) ~> windowed
```

PySpark:
```python
window_spec = Window.partitionBy("category").orderBy("order_date")
windowed = source_data.withColumn(
    "running_total", sum("amount").over(window_spec)
).withColumn(
    "row_num", row_number().over(window_spec)
)
```

**Alter Row (SCD / Merge)**:

DFL:
```
source alterRow(
    insertIf(is_new == true()),
    updateIf(is_modified == true()),
    deleteIf(is_deleted == true())
) ~> upsertRows
```

DLT:
```python
dp.apply_changes(
    target="catalog.schema.target_table",
    source="source_stream",
    keys=["id"],
    sequence_by="modified_date",
    stored_as_scd_type=1
)
```

### Step 7: Output Files

Write generated notebooks to `src/notebooks/transformations/<dataflow_name>.py`.

## Important Notes

- **Technology**: Use `from pyspark import pipelines as dp` (2025+ Lakeflow Declarative Pipelines), NOT the old `import dlt` pattern
- DLT materialized views use `@dp.materialized_view` for aggregate/latest-state tables
- DLT streaming tables use `@dp.table` with `dp.read_stream()` for incremental processing
- Data quality expectations: `@dp.expect("description", "condition")` or `@dp.expect_or_drop`/`@dp.expect_or_fail`
- Reference `references/dfl_syntax.md` for DFL parsing details
- Reference `references/dlt_patterns.md` for DLT decorator patterns
