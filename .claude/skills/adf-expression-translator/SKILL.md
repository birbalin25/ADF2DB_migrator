---
name: adf-expression-translator
description: Translate ADF Expression Language functions and system variables to Python or Spark SQL equivalents
trigger_keywords:
  - expression
  - "@pipeline()"
  - "@activity()"
  - "@concat()"
  - "@utcNow()"
  - "@formatDateTime()"
  - dynamic content
  - ADF function
  - translate expression
input: ADF expression strings (individual or embedded in component JSON)
output: Python or Spark SQL equivalent expressions
---

# ADF Expression Translator

Translate Azure Data Factory Expression Language to Python or Spark SQL equivalents.

## What This Skill Does

1. **Parses** ADF expression strings (e.g., `@concat(pipeline().parameters.prefix, '_', formatDateTime(utcNow(), 'yyyyMMdd'))`)
2. **Translates** each function call to its Python or Spark SQL equivalent
3. **Handles** nested expressions, string interpolation (`@{...}`), and system variables
4. **Converts** ADF date format strings to Python strftime / Spark date_format patterns
5. **Returns** production-ready Python or Spark SQL code

## When to Use This Skill

- **Standalone**: User has specific ADF expressions they want translated
- **As utility**: Called by `adf-pipeline-converter`, `adf-copy-converter`, and `adf-dataflow-converter` when they encounter embedded expressions
- **Bulk translation**: User wants all expressions in an ARM template translated at once

## Step-by-Step Instructions

### Step 1: Identify Expressions

ADF expressions start with `@` and appear in:
- Activity `typeProperties` (queries, paths, URLs, conditions)
- Pipeline parameters default values
- Dataset `typeProperties` (dynamic table names, file paths)
- Trigger parameters
- Data flow script expressions

Patterns to detect:
- `@pipeline().parameters.X` — pipeline parameter reference
- `@activity('name').output.Y` — activity output reference
- `@dataset().X` — dataset parameter reference
- `@linkedService().X` — linked service property reference
- `@trigger().outputs.windowStartTime` — trigger output
- `@{expression}` — string interpolation (embedded in larger strings)
- `@concat(...)`, `@if(...)`, etc. — function calls

### Step 2: Translate Using the Expression Catalog

Refer to `references/expression_catalog.md` for the complete function-by-function mapping.

**Translation target selection:**
- If the expression will be used in a **PySpark notebook** → translate to Python
- If the expression will be used in a **SQL cell or Spark SQL query** → translate to Spark SQL
- If the user doesn't specify → default to Python

### Step 3: Handle Nested Expressions

ADF expressions can be deeply nested:
```
@concat(
  pipeline().parameters.prefix,
  '/',
  formatDateTime(utcNow(), 'yyyy'),
  '/',
  formatDateTime(utcNow(), 'MM'),
  '/',
  formatDateTime(utcNow(), 'dd')
)
```

Translate inside-out:
1. `utcNow()` → `datetime.now(timezone.utc)`
2. `formatDateTime(..., 'yyyy')` → `.strftime('%Y')`
3. `concat(...)` → f-string or `+` concatenation
4. Final: `f"{prefix}/{datetime.now(timezone.utc).strftime('%Y')}/{datetime.now(timezone.utc).strftime('%m')}/{datetime.now(timezone.utc).strftime('%d')}"`

For readability, assign intermediate variables:
```python
now = datetime.now(timezone.utc)
path = f"{prefix}/{now.strftime('%Y')}/{now.strftime('%m')}/{now.strftime('%d')}"
```

### Step 4: Handle String Interpolation

ADF uses `@{expression}` for inline expressions within strings:
```
wasbs://container@{pipeline().parameters.storageAccount}.blob.core.windows.net/path
```

Convert to Python f-string:
```python
storage_account = dbutils.widgets.get("storageAccount")
path = f"wasbs://container@{storage_account}.blob.core.windows.net/path"
```

Note: `@` without `{` in connection strings is literal (not an expression).

### Step 5: Convert Date Format Strings

ADF uses .NET-style format strings. Convert to Python strftime:

| ADF Format | Python strftime | Spark date_format |
|---|---|---|
| `yyyy` | `%Y` | `yyyy` |
| `yy` | `%y` | `yy` |
| `MM` | `%m` | `MM` |
| `dd` | `%d` | `dd` |
| `HH` | `%H` | `HH` |
| `mm` | `%M` | `mm` |
| `ss` | `%S` | `ss` |
| `fff` | `%f` (microseconds) | `SSS` |
| `tt` | `%p` | `a` |
| `yyyyMMdd` | `%Y%m%d` | `yyyyMMdd` |
| `yyyy-MM-dd` | `%Y-%m-%d` | `yyyy-MM-dd` |
| `yyyy-MM-ddTHH:mm:ss` | `%Y-%m-%dT%H:%M:%S` | `yyyy-MM-dd'T'HH:mm:ss` |

### Step 6: Output

Return the translated expression(s) as:
1. **Inline code** if translating a single expression
2. **Code block** with imports and helper variables if complex
3. **Translation table** if doing bulk translation (original → Python → Spark SQL)

Always include required imports at the top:
```python
from datetime import datetime, timezone
import json
```

## Important Notes

- ADF expressions are **case-insensitive** for function names (`@Concat` = `@concat`)
- `@pipeline().RunId` is a GUID — use `dbutils.widgets.get("run_id")` or generate with `str(uuid.uuid4())`
- `@activity('name').output` depends on the activity type — Copy outputs `rowsCopied`, `dataWritten`; Lookup outputs `firstRow` or `value` (array)
- Expressions inside `@{...}` are interpolated; bare `@function()` means the entire value is the expression result
- Some ADF functions have no direct Spark SQL equivalent (e.g., `@activity()` references) — use Python in those cases
