# ADF Data Flow Language (DFL) Reference

## Script Structure

A DFL script is a sequence of transformation statements, each following:
```
[input_stream(s)] transformation_keyword(parameters) ~> output_stream
```

Lines are separated by newlines. Multi-line statements are common.

## Source Statement

```
source(
    output(
        col1 as integer,
        col2 as string,
        col3 as timestamp
    ),
    allowSchemaDrift: true,
    validateSchema: false,
    isolationLevel: 'READ_COMMITTED',
    format: 'table',
    store: 'sqlserver',
    schemaName: 'dbo',
    tableName: 'source_table',
    query: 'SELECT * FROM table WHERE active = 1'
) ~> sourceStream
```

**Key parameters:**
- `output(...)` — explicit schema definition
- `allowSchemaDrift` — accept columns not in schema
- `format` — 'table', 'parquet', 'delimited', 'json'
- `store` — 'sqlserver', 'azureBlobStorage', 'adls', etc.
- `query` — optional SQL query for database sources
- `wildcardPaths` — file glob patterns

## Sink Statement

```
inputStream sink(
    allowSchemaDrift: true,
    validateSchema: false,
    format: 'delta',
    store: 'adls',
    fileSystem: 'container',
    folderPath: 'output/path',
    truncate: true,
    umpiMapping: false,
    skipDuplicateMapInputs: true,
    skipDuplicateMapOutputs: true
) ~> sinkStream
```

**Key parameters:**
- `truncate` — clear target before write
- `umpiMapping` — auto column mapping
- `partitionBy` — partition columns
- `preCommands` / `postCommands` — SQL to run before/after

## Filter

```
inputStream filter(
    amount > 0 && status != 'cancelled'
) ~> filteredStream
```

Operators: `&&` (and), `||` (or), `!` (not), `==`, `!=`, `>`, `<`, `>=`, `<=`

## Select (Column Selection/Reorder)

```
inputStream select(
    mapColumn(
        customer_id = CustomerID,
        order_date = OrderDate,
        total_amount = TotalAmount
    ),
    skipDuplicateMapInputs: true
) ~> selectedStream
```

## Derived Column

```
inputStream derive(
    full_name = concat(first_name, ' ', last_name),
    year = year(order_date),
    amount_category = iif(amount > 1000, 'High', iif(amount > 100, 'Medium', 'Low'))
) ~> derivedStream
```

**Common DFL expression functions:**
- `concat(a, b, ...)` → `CONCAT(a, b, ...)`
- `iif(condition, true_val, false_val)` → `IF(condition, true_val, false_val)`
- `isNull(col)` → `col IS NULL`
- `toString(col)` → `CAST(col AS STRING)`
- `toInteger(col)` → `CAST(col AS INT)`
- `toTimestamp(col, format)` → `TO_TIMESTAMP(col, format)`
- `currentTimestamp()` → `CURRENT_TIMESTAMP()`
- `trim(col)`, `upper(col)`, `lower(col)` → same in Spark SQL
- `year(col)`, `month(col)`, `dayOfMonth(col)` → same in Spark SQL
- `round(col, scale)` → `ROUND(col, scale)`
- `abs(col)` → `ABS(col)`
- `length(col)` → `LENGTH(col)`
- `substring(col, start, len)` → `SUBSTRING(col, start, len)`
- `replace(col, old, new)` → `REPLACE(col, old, new)`
- `regexReplace(col, pattern, replacement)` → `REGEXP_REPLACE(col, pattern, replacement)`
- `sha2(col, bitLength)` → `SHA2(col, bitLength)`
- `md5(col)` → `MD5(col)`
- `coalesce(col1, col2, ...)` → `COALESCE(col1, col2, ...)`

## Aggregate

```
inputStream aggregate(
    groupBy(category, region),
    total_amount = sum(amount),
    avg_amount = avg(amount),
    order_count = count(),
    max_date = max(order_date),
    distinct_customers = countDistinct(customer_id)
) ~> aggregatedStream
```

## Join

```
leftStream, rightStream join(
    leftStream.id == rightStream.customer_id,
    joinType: 'left',
    broadcast: 'right'
) ~> joinedStream
```

**Join types:** `'inner'`, `'left'`, `'right'`, `'full'`, `'cross'`
**Broadcast hint:** `broadcast: 'left'|'right'|'off'`

## Union

```
stream1, stream2 union(byName: true) ~> unionedStream
```

## Sort

```
inputStream sort(
    asc(category, true),
    desc(amount, true)
) ~> sortedStream
```

## Pivot

```
inputStream pivot(
    groupBy(customer_id, year),
    pivotBy(quarter, ['Q1', 'Q2', 'Q3', 'Q4']),
    total = sum(amount)
) ~> pivotedStream
```

## Unpivot

```
inputStream unpivot(
    output(
        metric_name as string,
        metric_value as double
    ),
    ungroupBy(id, date),
    lateral: true,
    ignoreNullPivots: false,
    (revenue, cost, profit)
) ~> unpivotedStream
```

## Window

```
inputStream window(
    over(customer_id, order_date asc),
    running_total = sum(amount),
    row_num = rowNumber(),
    rank = rank()
) ~> windowedStream
```

**Window frame:** `over(partitionBy, orderBy [asc|desc], rangeBetween|rowsBetween(start, end))`

## Conditional Split

```
inputStream conditionalSplit(
    amount > 1000 -> 'highValue',
    amount > 100 -> 'medValue',
    true -> 'lowValue'
) ~> splitStream@(highValue, medValue, lowValue)
```

Output streams accessed as: `splitStream@highValue`, `splitStream@medValue`, `splitStream@lowValue`

## Flatten (Explode)

```
inputStream flatten(
    unroll(items),
    mapColumn(
        id,
        item_name = items.name,
        item_qty = items.quantity
    )
) ~> flattenedStream
```

## Exists / Lookup

```
mainStream, lookupStream exists(
    mainStream.id == lookupStream.id,
    negate: false,
    broadcast: 'right'
) ~> existsStream

mainStream, lookupStream lookup(
    mainStream.id == lookupStream.id,
    broadcast: 'right'
) ~> lookupResult
```

## Surrogate Key

```
inputStream surrogateKey(
    key = 1
) ~> withSurrogateKey
```

## Alter Row

```
inputStream alterRow(
    insertIf(true()),
    updateIf(is_modified == true()),
    deleteIf(is_deleted == true()),
    upsertIf(false())
) ~> upsertStream
```
