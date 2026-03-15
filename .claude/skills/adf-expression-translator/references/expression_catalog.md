# ADF Expression → Python/Spark SQL Catalog

Complete mapping of ADF Expression Language functions to Python and Spark SQL equivalents.

## System Variables

| ADF Expression | Python Equivalent | Spark SQL | Notes |
|---|---|---|---|
| `@pipeline().DataFactory` | `spark.conf.get("spark.databricks.workspaceUrl")` | N/A | Workspace identifier |
| `@pipeline().Pipeline` | `dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()` | N/A | Current notebook path |
| `@pipeline().RunId` | `dbutils.widgets.get("run_id")` or `str(uuid.uuid4())` | N/A | Unique run identifier |
| `@pipeline().TriggerType` | `dbutils.widgets.get("trigger_type")` | N/A | Pass as job parameter |
| `@pipeline().TriggerId` | `dbutils.widgets.get("trigger_id")` | N/A | Pass as job parameter |
| `@pipeline().TriggerName` | `dbutils.widgets.get("trigger_name")` | N/A | Pass as job parameter |
| `@pipeline().TriggerTime` | `dbutils.widgets.get("trigger_time")` | N/A | Pass as job parameter |
| `@pipeline().GroupId` | `dbutils.widgets.get("group_id")` | N/A | Batch group identifier |
| `@pipeline().parameters.X` | `dbutils.widgets.get("X")` | `${X}` | Job parameter |
| `@pipeline().globalParameters.X` | `dbutils.widgets.get("X")` | `${X}` | Global parameter (same mechanism) |
| `@activity('name').output` | `dbutils.jobs.taskValues.get(taskKey="name", key="output")` | N/A | Previous task output |
| `@activity('name').output.rowsCopied` | `dbutils.jobs.taskValues.get(taskKey="name", key="rowsCopied")` | N/A | Copy activity metric |
| `@activity('name').output.firstRow` | `dbutils.jobs.taskValues.get(taskKey="name", key="firstRow")` | N/A | Lookup first row |
| `@activity('name').output.value` | `dbutils.jobs.taskValues.get(taskKey="name", key="value")` | N/A | Lookup result array |
| `@dataset().X` | `dbutils.widgets.get("X")` | `${X}` | Dataset parameter |
| `@linkedService().X` | `dbutils.widgets.get("X")` | `${X}` | Linked service property |
| `@trigger().outputs.windowStartTime` | `dbutils.widgets.get("window_start")` | `${window_start}` | Tumbling window start |
| `@trigger().outputs.windowEndTime` | `dbutils.widgets.get("window_end")` | `${window_end}` | Tumbling window end |
| `@trigger().scheduledTime` | `dbutils.widgets.get("scheduled_time")` | `${scheduled_time}` | Schedule trigger time |
| `@trigger().startTime` | `dbutils.widgets.get("trigger_start_time")` | `${trigger_start_time}` | Trigger fire time |
| `@item()` | Loop variable (e.g., `item` in `for item in items`) | N/A | ForEach current item |
| `@itemIndex()` | `index` in `for index, item in enumerate(items)` | N/A | ForEach index |

## String Functions

| ADF Function | Python Equivalent | Spark SQL |
|---|---|---|
| `@concat(a, b, ...)` | `f"{a}{b}"` or `str(a) + str(b)` | `CONCAT(a, b, ...)` |
| `@substring(str, start, len)` | `str[start:start+len]` | `SUBSTRING(str, start+1, len)` |
| `@replace(str, old, new)` | `str.replace(old, new)` | `REPLACE(str, old, new)` |
| `@toLower(str)` | `str.lower()` | `LOWER(str)` |
| `@toUpper(str)` | `str.upper()` | `UPPER(str)` |
| `@trim(str)` | `str.strip()` | `TRIM(str)` |
| `@indexOf(str, sub)` | `str.find(sub)` | `INSTR(str, sub) - 1` |
| `@lastIndexOf(str, sub)` | `str.rfind(sub)` | `LENGTH(str) - INSTR(REVERSE(str), REVERSE(sub))` |
| `@startsWith(str, prefix)` | `str.startswith(prefix)` | `str LIKE CONCAT(prefix, '%')` |
| `@endsWith(str, suffix)` | `str.endswith(suffix)` | `str LIKE CONCAT('%', suffix)` |
| `@contains(str, sub)` | `sub in str` | `str LIKE CONCAT('%', sub, '%')` |
| `@split(str, delim)` | `str.split(delim)` | `SPLIT(str, delim)` |
| `@join(arr, delim)` | `delim.join(arr)` | `ARRAY_JOIN(arr, delim)` |
| `@length(str)` | `len(str)` | `LENGTH(str)` |
| `@equals(a, b)` | `a == b` | `a = b` |
| `@empty(str)` | `not str` or `str == ""` | `str IS NULL OR str = ''` |
| `@coalesce(a, b)` | `a if a is not None else b` | `COALESCE(a, b)` |
| `@guid()` | `str(uuid.uuid4())` | `UUID()` |
| `@nthIndexOf(str, sub, n)` | Custom: find nth occurrence | Custom SQL |
| `@formatNumber(num, fmt)` | `f"{num:fmt}"` | `FORMAT_NUMBER(num, precision)` |
| `@base64(str)` | `base64.b64encode(str.encode()).decode()` | `BASE64(str)` |
| `@base64ToBinary(b64)` | `base64.b64decode(b64)` | `UNBASE64(b64)` |
| `@base64ToString(b64)` | `base64.b64decode(b64).decode()` | `CAST(UNBASE64(b64) AS STRING)` |
| `@uriComponent(str)` | `urllib.parse.quote(str, safe='')` | `N/A (use Python UDF)` |
| `@uriComponentToString(enc)` | `urllib.parse.unquote(enc)` | `N/A (use Python UDF)` |
| `@dataUri(str)` | `f"data:text/plain;charset=utf-8;base64,{base64.b64encode(str.encode()).decode()}"` | N/A |
| `@dataUriToBinary(uri)` | `base64.b64decode(uri.split(',')[1])` | N/A |
| `@dataUriToString(uri)` | `base64.b64decode(uri.split(',')[1]).decode()` | N/A |
| `@string(value)` | `str(value)` | `CAST(value AS STRING)` |

## Math Functions

| ADF Function | Python Equivalent | Spark SQL |
|---|---|---|
| `@add(a, b)` | `a + b` | `a + b` |
| `@sub(a, b)` | `a - b` | `a - b` |
| `@mul(a, b)` | `a * b` | `a * b` |
| `@div(a, b)` | `a / b` | `a / b` |
| `@mod(a, b)` | `a % b` | `a % b` |
| `@min(a, b)` | `min(a, b)` | `LEAST(a, b)` |
| `@max(a, b)` | `max(a, b)` | `GREATEST(a, b)` |
| `@range(start, count)` | `list(range(start, start + count))` | `SEQUENCE(start, start + count - 1)` |
| `@rand(min, max)` | `random.randint(min, max)` | `FLOOR(RAND() * (max - min + 1)) + min` |
| `@float(value)` | `float(value)` | `CAST(value AS DOUBLE)` |
| `@int(value)` | `int(value)` | `CAST(value AS INT)` |
| `@bool(value)` | `bool(value)` | `CAST(value AS BOOLEAN)` |

## Date/Time Functions

| ADF Function | Python Equivalent | Spark SQL |
|---|---|---|
| `@utcNow()` | `datetime.now(timezone.utc).isoformat()` | `CURRENT_TIMESTAMP()` |
| `@utcNow('format')` | `datetime.now(timezone.utc).strftime(py_fmt)` | `DATE_FORMAT(CURRENT_TIMESTAMP(), spark_fmt)` |
| `@formatDateTime(dt, fmt)` | `dt.strftime(py_fmt)` | `DATE_FORMAT(dt, spark_fmt)` |
| `@parseDateTime(str, fmt)` | `datetime.strptime(str, py_fmt)` | `TO_TIMESTAMP(str, spark_fmt)` |
| `@addDays(dt, days)` | `dt + timedelta(days=days)` | `DATE_ADD(dt, days)` |
| `@addHours(dt, hours)` | `dt + timedelta(hours=hours)` | `dt + INTERVAL hours HOURS` |
| `@addMinutes(dt, mins)` | `dt + timedelta(minutes=mins)` | `dt + INTERVAL mins MINUTES` |
| `@addSeconds(dt, secs)` | `dt + timedelta(seconds=secs)` | `dt + INTERVAL secs SECONDS` |
| `@addToTime(dt, interval, unit)` | `dt + timedelta(**{unit: interval})` | `dt + INTERVAL interval unit` |
| `@subtractFromTime(dt, interval, unit)` | `dt - timedelta(**{unit: interval})` | `dt - INTERVAL interval unit` |
| `@dayOfMonth(dt)` | `dt.day` | `DAY(dt)` |
| `@dayOfWeek(dt)` | `dt.isoweekday()` | `DAYOFWEEK(dt)` |
| `@dayOfYear(dt)` | `dt.timetuple().tm_yday` | `DAYOFYEAR(dt)` |
| `@month(dt)` | `dt.month` | `MONTH(dt)` |
| `@year(dt)` | `dt.year` | `YEAR(dt)` |
| `@startOfDay(dt)` | `dt.replace(hour=0, minute=0, second=0, microsecond=0)` | `DATE_TRUNC('DAY', dt)` |
| `@startOfMonth(dt)` | `dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)` | `DATE_TRUNC('MONTH', dt)` |
| `@startOfHour(dt)` | `dt.replace(minute=0, second=0, microsecond=0)` | `DATE_TRUNC('HOUR', dt)` |
| `@ticks(dt)` | `int(dt.timestamp() * 10_000_000) + 621355968000000000` | N/A |
| `@getFutureTime(interval, unit)` | `datetime.now(timezone.utc) + timedelta(**{unit: interval})` | `CURRENT_TIMESTAMP() + INTERVAL interval unit` |
| `@getPastTime(interval, unit)` | `datetime.now(timezone.utc) - timedelta(**{unit: interval})` | `CURRENT_TIMESTAMP() - INTERVAL interval unit` |
| `@convertFromUtc(dt, tz)` | `dt.astimezone(ZoneInfo(tz))` | `FROM_UTC_TIMESTAMP(dt, tz)` |
| `@convertToUtc(dt, tz)` | `dt.replace(tzinfo=ZoneInfo(tz)).astimezone(timezone.utc)` | `TO_UTC_TIMESTAMP(dt, tz)` |
| `@convertTimeZone(dt, src_tz, dest_tz)` | Convert via UTC intermediary | `FROM_UTC_TIMESTAMP(TO_UTC_TIMESTAMP(dt, src_tz), dest_tz)` |

## Logical Functions

| ADF Function | Python Equivalent | Spark SQL |
|---|---|---|
| `@if(condition, trueVal, falseVal)` | `trueVal if condition else falseVal` | `IF(condition, trueVal, falseVal)` |
| `@and(a, b)` | `a and b` | `a AND b` |
| `@or(a, b)` | `a or b` | `a OR b` |
| `@not(a)` | `not a` | `NOT a` |
| `@greater(a, b)` | `a > b` | `a > b` |
| `@greaterOrEquals(a, b)` | `a >= b` | `a >= b` |
| `@less(a, b)` | `a < b` | `a < b` |
| `@lessOrEquals(a, b)` | `a <= b` | `a <= b` |
| `@equals(a, b)` | `a == b` | `a = b` |
| `@not(equals(a, b))` | `a != b` | `a != b` or `a <> b` |

## Collection Functions

| ADF Function | Python Equivalent | Spark SQL |
|---|---|---|
| `@length(collection)` | `len(collection)` | `SIZE(collection)` |
| `@contains(collection, item)` | `item in collection` | `ARRAY_CONTAINS(collection, item)` |
| `@empty(collection)` | `len(collection) == 0` | `SIZE(collection) = 0` |
| `@first(collection)` | `collection[0]` | `collection[0]` |
| `@last(collection)` | `collection[-1]` | `collection[SIZE(collection)-1]` |
| `@intersection(a, b)` | `list(set(a) & set(b))` | `ARRAY_INTERSECT(a, b)` |
| `@union(a, b)` | `list(set(a) \| set(b))` | `ARRAY_UNION(a, b)` |
| `@skip(collection, n)` | `collection[n:]` | `SLICE(collection, n+1, SIZE(collection))` |
| `@take(collection, n)` | `collection[:n]` | `SLICE(collection, 1, n)` |
| `@createArray(a, b, ...)` | `[a, b, ...]` | `ARRAY(a, b, ...)` |
| `@json(str)` | `json.loads(str)` | `FROM_JSON(str, schema)` |
| `@xml(str)` | `xml.etree.ElementTree.fromstring(str)` | N/A (use Python UDF) |
| `@xpath(xml, expr)` | `xml_tree.findall(expr)` | N/A (use Python UDF) |
| `@array(value)` | `[value]` if not list else `value` | `ARRAY(value)` |

## Conversion Functions

| ADF Function | Python Equivalent | Spark SQL |
|---|---|---|
| `@json(str)` | `json.loads(str)` | `FROM_JSON(str, schema)` |
| `@string(value)` | `str(value)` or `json.dumps(value)` | `CAST(value AS STRING)` |
| `@int(value)` | `int(value)` | `CAST(value AS INT)` |
| `@float(value)` | `float(value)` | `CAST(value AS DOUBLE)` |
| `@bool(value)` | `bool(value)` | `CAST(value AS BOOLEAN)` |
| `@binary(value)` | `value.encode()` | `CAST(value AS BINARY)` |
| `@decodeBase64(str)` | `base64.b64decode(str).decode()` | `CAST(UNBASE64(str) AS STRING)` |
| `@encodeUriComponent(str)` | `urllib.parse.quote(str, safe='')` | N/A |
| `@decodeUriComponent(str)` | `urllib.parse.unquote(str)` | N/A |
| `@decodeDataUri(uri)` | `base64.b64decode(uri.split(',')[1])` | N/A |

## ADF Date Format → Python strftime Conversion

| ADF Token | Python strftime | Spark date_format | Meaning |
|---|---|---|---|
| `yyyy` | `%Y` | `yyyy` | 4-digit year |
| `yy` | `%y` | `yy` | 2-digit year |
| `MMMM` | `%B` | `MMMM` | Full month name |
| `MMM` | `%b` | `MMM` | Abbreviated month |
| `MM` | `%m` | `MM` | 2-digit month |
| `M` | `%-m` | `M` | Month without leading zero |
| `dddd` | `%A` | `EEEE` | Full day name |
| `ddd` | `%a` | `EEE` | Abbreviated day name |
| `dd` | `%d` | `dd` | 2-digit day |
| `d` | `%-d` | `d` | Day without leading zero |
| `HH` | `%H` | `HH` | 24-hour (2-digit) |
| `H` | `%-H` | `H` | 24-hour (no leading zero) |
| `hh` | `%I` | `hh` | 12-hour (2-digit) |
| `h` | `%-I` | `h` | 12-hour (no leading zero) |
| `mm` | `%M` | `mm` | Minutes (2-digit) |
| `ss` | `%S` | `ss` | Seconds (2-digit) |
| `fff` | `%f` | `SSS` | Milliseconds |
| `tt` | `%p` | `a` | AM/PM |
| `zzz` | `%z` | `XXX` | UTC offset |
| `K` | `%z` | `XXX` | Time zone info |
