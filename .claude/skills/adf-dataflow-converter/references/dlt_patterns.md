# Lakeflow Declarative Pipeline (DLT) Patterns

## Technology Note (2025+)

Lakeflow Declarative Pipelines replaces the older "Delta Live Tables" naming. The new import:
```python
from pyspark import pipelines as dp
```

Replaces the old `import dlt` pattern.

## Basic Table Declaration

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(
    name="catalog.schema.table_name",
    comment="Description of the table"
)
def my_table():
    return spark.read.table("catalog.schema.source_table")
```

## Streaming Table (Incremental)

```python
@dp.table(
    name="catalog.schema.streaming_table",
    comment="Incrementally processed data"
)
def streaming_table():
    return dp.read_stream("catalog.schema.source_table")
```

## Materialized View (Aggregated/Latest State)

```python
@dp.materialized_view(
    name="catalog.schema.daily_summary",
    comment="Daily aggregated metrics"
)
def daily_summary():
    return (dp.read("catalog.schema.transactions")
        .groupBy("date", "category")
        .agg(
            sum("amount").alias("total"),
            count("*").alias("count")
        ))
```

## Data Quality Expectations

```python
@dp.table(name="catalog.schema.validated_data")
@dp.expect("valid_amount", "amount > 0")
@dp.expect("non_null_id", "id IS NOT NULL")
@dp.expect_or_drop("valid_email", "email LIKE '%@%'")
@dp.expect_or_fail("valid_date", "event_date <= current_date()")
def validated_data():
    return dp.read("catalog.schema.raw_data")
```

**Expectation types:**
- `@dp.expect` — warn but keep invalid rows
- `@dp.expect_or_drop` — silently drop invalid rows
- `@dp.expect_or_fail` — fail the pipeline if violated

## Multi-Table Pipeline

```python
from pyspark import pipelines as dp
from pyspark.sql.functions import *

# Bronze: Raw ingestion
@dp.table(name="catalog.schema.raw_transactions")
def raw_transactions():
    return (dp.read_stream("catalog.schema.source_transactions")
        .withColumn("_ingested_at", current_timestamp()))

# Silver: Cleaned and validated
@dp.table(name="catalog.schema.clean_transactions")
@dp.expect_or_drop("valid_amount", "amount > 0")
@dp.expect("non_null_customer", "customer_id IS NOT NULL")
def clean_transactions():
    return (dp.read("raw_transactions")
        .filter(col("status") != "cancelled")
        .withColumn("amount", col("amount").cast("decimal(18,2)")))

# Gold: Aggregated
@dp.materialized_view(name="catalog.schema.daily_revenue")
def daily_revenue():
    return (dp.read("clean_transactions")
        .groupBy("date", "region")
        .agg(sum("amount").alias("revenue")))
```

## Apply Changes (SCD / CDC)

```python
dp.apply_changes(
    target="catalog.schema.customers_scd",
    source="catalog.schema.customer_changes",
    keys=["customer_id"],
    sequence_by="modified_date",
    stored_as_scd_type=2,  # 1 for overwrite, 2 for history
    columns=["customer_id", "name", "email", "status"]
)
```

## Parameterized Tables

```python
@dp.table(name="catalog.schema.filtered_data")
def filtered_data():
    min_date = spark.conf.get("pipeline.min_date", "2024-01-01")
    return (dp.read("catalog.schema.source_data")
        .filter(col("event_date") >= min_date))
```

## Complex Join Pattern

```python
@dp.table(name="catalog.schema.enriched_orders")
def enriched_orders():
    orders = dp.read("catalog.schema.orders")
    customers = dp.read("catalog.schema.customers")
    products = dp.read("catalog.schema.products")

    return (orders
        .join(customers, orders.customer_id == customers.id, "left")
        .join(products, orders.product_id == products.id, "left")
        .select(
            orders.order_id,
            orders.order_date,
            customers.name.alias("customer_name"),
            products.product_name,
            orders.quantity,
            orders.unit_price,
            (orders.quantity * orders.unit_price).alias("total_amount")
        ))
```

## Window Function Pattern

```python
from pyspark.sql.window import Window

@dp.table(name="catalog.schema.ranked_sales")
def ranked_sales():
    window_spec = Window.partitionBy("region").orderBy(col("revenue").desc())

    return (dp.read("catalog.schema.sales_summary")
        .withColumn("rank", row_number().over(window_spec))
        .withColumn("running_total", sum("revenue").over(
            Window.partitionBy("region").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
        )))
```

## Conditional Split Pattern

In ADF, a ConditionalSplit produces multiple output streams. In DLT, create separate tables with different filters:

```python
@dp.table(name="catalog.schema.high_value_orders")
def high_value():
    return dp.read("catalog.schema.orders").filter(col("amount") > 1000)

@dp.table(name="catalog.schema.standard_orders")
def standard():
    return dp.read("catalog.schema.orders").filter(
        (col("amount") > 100) & (col("amount") <= 1000)
    )

@dp.table(name="catalog.schema.low_value_orders")
def low_value():
    return dp.read("catalog.schema.orders").filter(col("amount") <= 100)
```

## Pipeline Configuration (databricks.yml)

```yaml
resources:
  pipelines:
    my_pipeline:
      name: "My DLT Pipeline"
      target: "catalog.schema"
      libraries:
        - notebook:
            path: src/notebooks/transformations/my_dataflow.py
      configuration:
        pipeline.min_date: "2024-01-01"
      continuous: false
      development: true
      channel: PREVIEW
```
