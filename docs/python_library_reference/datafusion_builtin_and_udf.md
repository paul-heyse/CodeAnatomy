Below is the **DataFusion UDF surface area** (focused on the **`datafusion` Python bindings**), organized by **UDF kind** + the **exact constructors / decorators / registration calls / invocation syntax**.

---

## Project policy (DataFusion-first compute)

- All expression-level compute should route through DataFusion UDFs or built-ins.
- Any remaining PyArrow compute usage must be centralized under
  `datafusion_engine.compute_ops` and treated as an internal implementation detail.

## 0) The 4 UDF families DataFusion exposes (Python)

DataFusion Python supports:

* **Scalar UDF** (row-wise semantics; batch-at-a-time execution over Arrow arrays)
* **Aggregate UDF / UDAF** (group-wise aggregation via an `Accumulator`)
* **Window UDF / UDWF** (partition-wise via a `WindowEvaluator`)
* **Table function / UDTF** (returns a *table provider*; invoked from `FROM`) ([Apache DataFusion][1])

---

## 1) Scalar UDF (UDF)

### 1.1 Define (function signature contract)

A scalar UDF is a Python callable that takes **one or more `pyarrow.Array`** and returns a **single `pyarrow.Array`** (DataFusion passes whole batches; your logic is row-wise). ([Apache DataFusion][1])

### 1.2 Create (function or decorator)

`udf` can be used as **a function** or **a decorator**; it also supports a **PyCapsule/Rust-backed** UDF object.

* **Function form**

  ```python
  from datafusion import udf
  import pyarrow as pa

  def is_null(arr: pa.Array) -> pa.Array:
      return arr.is_null()

  is_null_udf = udf(is_null, [pa.int64()], pa.bool_(), "stable", "is_null")
  ```
* **Decorator form**

  ```python
  from datafusion import udf
  import pyarrow as pa

  @udf([pa.int64()], pa.bool_(), "stable", "is_null")
  def is_null_udf(arr: pa.Array) -> pa.Array:
      return arr.is_null()
  ```

The documented call shapes are:
`udf(func, input_types, return_type, volatility, name=None)` **or** `@udf(input_types, return_type, volatility, name=None)` (and `func` may be a PyCapsule-backed ScalarUDF). ([Apache DataFusion][2])

### 1.3 Invoke (DataFrame/Expr syntax)

A `ScalarUDF` is **callable on expressions** and returns an expression:

```python
from datafusion import col
df.select(col("a"), is_null_udf(col("a")).alias("is_null"))
```

This works because `ScalarUDF.__call__(*args: Expr) -> Expr`. ([Apache DataFusion][3])

### 1.4 Register (so SQL can call it by name)

```python
ctx.register_udf(is_null_udf)
```

`SessionContext.register_udf(udf: ScalarUDF) -> None` ([Apache DataFusion][3])

### 1.5 PyCapsule / Rust-backed interop (optional)

* `ScalarUDF.from_pycapsule(obj)` and/or passing a PyCapsule to `udf(...)` is supported for Rust-exported UDFs. ([Apache DataFusion][2])

---

## 2) Aggregate UDF (UDAF)

### 2.1 Implement the `Accumulator` interface

You implement an `Accumulator` with **four required methods**:

* `update(*values: pyarrow.Array) -> None`
* `merge(states: list[pyarrow.Array]) -> None`
* `state() -> list[pyarrow.Scalar]`
* `evaluate() -> pyarrow.Scalar` ([Apache DataFusion][1])

### 2.2 Create (`udaf` function or decorator)

Creation supports function/decorator forms and Rust/PyCapsule forms:

* `udaf(accum, input_types, return_type, state_type, volatility, name=None)`
* `@udaf(input_types, return_type, state_type, volatility, name=None)` ([Apache DataFusion][2])

Example (function form, from the docs’ pattern):

```python
from datafusion import udaf, Accumulator, col
import pyarrow as pa
import pyarrow.compute as pc

class MyAcc(Accumulator):
    def __init__(self):
        self._sum = 0.0

    def update(self, a: pa.Array, b: pa.Array) -> None:
        self._sum += pc.sum(a).as_py() - pc.sum(b).as_py()

    def merge(self, states: list[pa.Array]) -> None:
        self._sum += pc.sum(states[0]).as_py()

    def state(self) -> list[pa.Scalar]:
        return [pa.scalar(self._sum)]

    def evaluate(self) -> pa.Scalar:
        return pa.scalar(self._sum)

my_udaf = udaf(MyAcc, [pa.float64(), pa.float64()], pa.float64(), [pa.float64()], "stable", "col_diff")
```

The method contracts and the `udaf(...)` pattern are documented. ([Apache DataFusion][1])

### 2.3 Invoke

An `AggregateUDF` is callable on `Expr` arguments and returns an `Expr`:

* `AggregateUDF.__call__(*args: Expr) -> Expr` ([Apache DataFusion][2])

Typically used inside aggregations / (also usable in window contexts per docs):

```python
df.aggregate([], [my_udaf(col("a"), col("b")).alias("col_diff")])
```

([Apache DataFusion][1])

### 2.4 Register (for SQL-by-name)

```python
ctx.register_udaf(my_udaf)
```

`SessionContext.register_udaf(udaf: AggregateUDF) -> None` ([Apache DataFusion][3])

### 2.5 PyCapsule / Rust-backed interop (optional)

* `AggregateUDF.from_pycapsule(obj)` and/or `udaf(pycapsule)` are supported. ([Apache DataFusion][2])

---

## 3) Window UDF (UDWF)

### 3.1 Implement `WindowEvaluator`

You implement a `WindowEvaluator` and provide one of:

* `evaluate(...)` (single-row)
* `evaluate_all(values: list[Array], num_rows: int) -> Array`
* `evaluate_all_with_rank(...)` (rank-aware)

…and you can control which path is used by overriding option methods like `uses_window_frame`, `supports_bounded_execution`, `include_rank`. ([Apache DataFusion][1])

### 3.2 Create (`udwf` function or decorator)

`udwf` supports function/decorator forms and PyCapsule forms:

* `udwf(func, input_types, return_type, volatility, name=None)`
* `@udwf(input_types, return_type, volatility, name=None)` ([Apache DataFusion][2])

### 3.3 Invoke

A `WindowUDF` is callable on `Expr` args and returns an `Expr`:

* `WindowUDF.__call__(*args: Expr) -> Expr` ([Apache DataFusion][2])

### 3.4 Register (for SQL-by-name)

```python
ctx.register_udwf(my_udwf)
```

`SessionContext.register_udwf(udwf: WindowUDF) -> None` ([Apache DataFusion][3])

### 3.5 PyCapsule / Rust-backed interop (optional)

* `WindowUDF.from_pycapsule(obj)` and/or passing a PyCapsule to `udwf(...)` is supported. ([Apache DataFusion][2])

---

## 4) Table functions (UDTF)

### 4.1 Contract (important limitations)

* UDTFs “take any number of `Expr` arguments, **but only literal expressions are supported**.”
* They must return a **Table Provider** (i.e., something DataFusion can treat as a table). ([Apache DataFusion][1])

### 4.2 Create (`udtf`)

A `TableFunction` supports a convenience creator `udtf(func, name)` (and related decorator helper). ([Apache DataFusion][2])

### 4.3 Register

```python
ctx.register_udtf(my_udtf)
```

`SessionContext.register_udtf(func: TableFunction) -> None` ([Apache DataFusion][3])

### 4.4 Invoke (SQL shape)

UDTFs are used like table-valued functions (conceptually `FROM my_udtf(...)`). The docs emphasize the “FROM-clause / table provider” model and registration step. ([Apache DataFusion][1])

### 4.5 Rust-backed UDTF interop

If you expose a Rust table function via PyO3, DataFusion’s docs describe exporting it via a **`PyCapsule`** and then registering it. ([Apache DataFusion][1])

---

## 5) Volatility (applies to all UDF kinds)

You can pass either `Volatility` enum values **or** strings:

* `"immutable"`, `"stable"`, `"volatile"`

Semantics (high leverage for optimization correctness):

* **Immutable**: same output for same input; can be inlined during planning
* **Stable**: same within a query, may differ across queries (e.g., `now()`)
* **Volatile**: may differ per evaluation (e.g., `random()`), cannot be evaluated during planning ([Apache DataFusion][2])

---

## 6) One-page summary of the “UDF API checklist”

**Constructors / decorators**

* `udf(...)` / `@udf(...)` → `ScalarUDF` ([Apache DataFusion][2])
* `udaf(...)` / `@udaf(...)` → `AggregateUDF` ([Apache DataFusion][2])
* `udwf(...)` / `@udwf(...)` → `WindowUDF` ([Apache DataFusion][2])
* `udtf(...)` → `TableFunction` ([Apache DataFusion][2])

**Registration (SessionContext)**

* `ctx.register_udf(ScalarUDF)` ([Apache DataFusion][3])
* `ctx.register_udaf(AggregateUDF)` ([Apache DataFusion][3])
* `ctx.register_udwf(WindowUDF)` ([Apache DataFusion][3])
* `ctx.register_udtf(TableFunction)` ([Apache DataFusion][3])

**Invocation (Expr)**

* `ScalarUDF(*Expr) -> Expr`, `AggregateUDF(*Expr) -> Expr`, `WindowUDF(*Expr) -> Expr` ([Apache DataFusion][2])
* `TableFunction(*Expr) -> table provider` (UDTF; literals only) ([Apache DataFusion][2])

---

## 7) Rust equivalents (if you’re mapping docs across languages)

DataFusion core (Rust) uses helper constructors like:

* `create_udf(...)` (scalar), `create_udwf(...)` (window), `create_udaf(...)` (aggregate), then register via `ctx.register_udf/register_udwf/register_udaf`. ([Apache DataFusion][4])

[1]: https://datafusion.apache.org/python/user-guide/common-operations/udf-and-udfa.html "User-Defined Functions — Apache Arrow DataFusion  documentation"
[2]: https://datafusion.apache.org/python/autoapi/datafusion/user_defined/index.html "datafusion.user_defined — Apache Arrow DataFusion  documentation"
[3]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[4]: https://datafusion.apache.org/library-user-guide/functions/adding-udfs.html "Adding User Defined Functions: Scalar/Window/Aggregate/Table Functions — Apache DataFusion  documentation"

### How to generate the *complete* list (with descriptions) for **your exact DataFusion build**

Because the function set is **version + feature** dependent, the only truly “complete” list is what your runtime reports.

**1) SQL (works in datafusion-cli, Rust `SessionContext::sql`, Python `ctx.sql`)**

```sql
SHOW FUNCTIONS;
SHOW FUNCTIONS LIKE '%date%';
```

`SHOW FUNCTIONS` returns a table that already includes **descriptions + signatures**, with columns like: `function_name`, `return_type`, `parameters`, `parameter_types`, `function_type`, `description`, `syntax_example`. ([Apache DataFusion][1])

**2) Information schema (for richer metadata joins)**

DataFusion exposes the same information via `information_schema` views for **routines** (functions) and **parameters** (arguments). ([Apache DataFusion][1])

> If your build has `information_schema` disabled, enable it via the session config `datafusion.catalog.information_schema` (documented in configuration settings). ([Apache DataFusion][2])

**3) Python snippet to export the full catalog to CSV / Parquet / Markdown**

```python
from datafusion import SessionContext
import pyarrow as pa
import pyarrow.csv as pacsv

ctx = SessionContext()

# (optional) if your build requires it:
# ctx.sql("SET datafusion.catalog.information_schema = true").collect()

batches = ctx.sql("SHOW FUNCTIONS").collect()
tbl = pa.Table.from_batches(batches)

pacsv.write_csv(tbl, "datafusion_show_functions.csv")

# If you want Markdown:
rows = tbl.to_pylist()
with open("datafusion_show_functions.md", "w") as f:
    f.write("# DataFusion Functions (SHOW FUNCTIONS)\n\n")
    for r in rows:
        f.write(f"- **{r['function_name']}** ({r['function_type']}) — {r['description']}  \n")
        f.write(f"  - return: `{r['return_type']}`  \n")
        f.write(f"  - params: `{r['parameters']}`  \n")
        f.write(f"  - example: `{r['syntax_example']}`\n")
```

DataFusion’s Python `DataFrame` is lazily evaluated and materializes on terminal ops like `collect()` / `to_pandas()`—useful for turning the result into an Arrow table/pandas frame for export. ([Apache DataFusion][3])

---

## Built-in SQL functions in DataFusion (cataloged by the official SQL reference)

DataFusion groups built-ins into: **Scalar**, **Aggregate**, **Window**, and **Special** functions. ([Apache DataFusion][4])

### 1) Scalar functions

#### Math

`abs, acos, acosh, asin, asinh, atan, atan2, atanh, cbrt, ceil, cos, cosh, cot, degrees, exp, factorial, floor, gcd, isnan, iszero, lcm, ln, log, log10, log2, nanvl, pi, pow, power, radians, random, round, signum, sin, sinh, sqrt, tan, tanh, trunc` ([Apache DataFusion][5])

#### Conditional

`coalesce, greatest, ifnull, least, nullif, nvl, nvl2` ([Apache DataFusion][5])

#### String

`ascii, bit_length, btrim, char_length, character_length, chr, concat, concat_ws, contains, ends_with, find_in_set, initcap, instr, left, length, levenshtein, lower, lpad, ltrim, octet_length, overlay, position, repeat, replace, reverse, right, rpad, rtrim, split_part, starts_with, strpos, substr, substr_index, substring, substring_index, to_hex, translate, trim, upper, uuid` ([Apache DataFusion][5])

#### Binary string

`decode, encode` ([Apache DataFusion][5])

#### Regular expression

`regexp_count, regexp_instr, regexp_like, regexp_match, regexp_replace` ([Apache DataFusion][5])

#### Time & date

`current_date, current_time, current_timestamp, date_bin, date_format, date_part, date_trunc, datepart, datetrunc, from_unixtime, make_date, make_time, now, to_char, to_date, to_local_time, to_time, to_timestamp, to_timestamp_micros, to_timestamp_millis, to_timestamp_nanos, to_timestamp_seconds, to_unixtime, today` ([Apache DataFusion][5])

#### Array / list (includes both `array_*` and `list_*` names)

`array_any_value, array_append, array_cat, array_concat, array_contains, array_dims, array_distance, array_distinct, array_element, array_empty, array_except, array_extract, array_has, array_has_all, array_has_any, array_indexof, array_intersect, array_join, array_length, array_max, array_min, array_ndims, array_pop_back, array_pop_front, array_position, array_positions, array_prepend, array_push_back, array_push_front, array_remove, array_remove_all, array_remove_n, array_repeat, array_replace, array_replace_all, array_replace_n, array_resize, array_reverse, array_slice, array_sort, array_to_string, array_union, arrays_overlap, cardinality, empty, flatten, generate_series, list_any_value, list_append, list_cat, list_concat, list_contains, list_dims, list_distance, list_distinct, list_element, list_empty, list_except, list_extract, list_has, list_has_all, list_has_any, list_indexof, list_intersect, list_join, list_length, list_max, list_ndims, list_pop_back, list_pop_front, list_position, list_positions, list_prepend, list_push_back, list_push_front, list_remove, list_remove_all, list_remove_n, list_repeat, list_replace, list_replace_all, list_replace_n, list_resize, list_reverse, list_slice, list_sort, list_to_string, list_union, make_array, make_list, range, string_to_array, string_to_list` ([Apache DataFusion][5])

#### Struct

`named_struct, row, struct` ([Apache DataFusion][5])

#### Map

`element_at, map, map_entries, map_extract, map_keys, map_values` ([Apache DataFusion][5])

#### Hashing

`digest, md5, sha224, sha256, sha384, sha512` ([Apache DataFusion][5])

#### Union (Arrow union / tagged union type)

`union_extract, union_tag` ([Apache DataFusion][5])

#### Other

`arrow_cast, arrow_metadata, arrow_typeof, get_field, version` ([Apache DataFusion][5])

---

### 2) Aggregate functions

#### General

`array_agg, avg, bit_and, bit_or, bit_xor, bool_and, bool_or, count, first_value, grouping, last_value, max, mean, median, min, percentile_cont, quantile_cont, string_agg, sum, var, var_pop, var_population, var_samp, var_sample` ([Apache DataFusion][6])

#### Statistical

`corr, covar, covar_pop, covar_samp, nth_value, regr_avgx, regr_avgy, regr_count, regr_intercept, regr_r2, regr_slope, regr_sxx, regr_sxy, regr_syy, stddev, stddev_pop, stddev_samp` ([Apache DataFusion][6])

#### Approximate

`approx_distinct, approx_median, approx_percentile_cont, approx_percentile_cont_with_weight` ([Apache DataFusion][6])

(Also: aggregates support `FILTER (WHERE ...)`, and select ones support `WITHIN GROUP (ORDER BY ...)`.) ([Apache DataFusion][6])

---

### 3) Window functions

* **All aggregate functions** can be used as window functions via `... OVER (...)`. ([Apache DataFusion][7])
* Plus these built-in **ranking** and **analytic** window functions:

**Ranking:** `cume_dist, dense_rank, ntile, percent_rank, rank, row_number` ([Apache DataFusion][7])
**Analytical:** `first_value, lag, last_value, lead, nth_value` ([Apache DataFusion][7])

---

### 4) Special functions (expansion)

`unnest` and `unnest(struct)` ([Apache DataFusion][8])

---

## Quick note on “inlining” and “pushdown”

Those are **optimizer behaviors**, not functions:

* “Inlining / constant folding” is influenced by whether a function is effectively stable/deterministic within the query (e.g., `current_date/current_time/now` are defined to be query-time stable). ([Apache DataFusion][5])
* “Pushdown” is about whether predicates/projections can be evaluated inside a scan (Parquet/CSV/etc). Use `EXPLAIN` to see whether a `Filter` stays above a scan or is pushed into it. ([Apache DataFusion][9])

---

If you tell me whether you want the catalog in **(a)** “SQL names only”, **(b)** “SQL names + 1-line descriptions”, or **(c)** “full overload table exactly like `SHOW FUNCTIONS`”, I can give you a ready-to-run export script tailored to that output shape.

[1]: https://datafusion.apache.org/user-guide/sql/information_schema.html "Information Schema — Apache DataFusion  documentation"
[2]: https://datafusion.apache.org/user-guide/configs.html?utm_source=chatgpt.com "Configuration Settings — Apache DataFusion documentation"
[3]: https://datafusion.apache.org/python/user-guide/dataframe/index.html?utm_source=chatgpt.com "DataFrames — Apache Arrow DataFusion documentation"
[4]: https://datafusion.apache.org/user-guide/sql/index.html "SQL Reference — Apache DataFusion  documentation"
[5]: https://datafusion.apache.org/_sources/user-guide/sql/scalar_functions.md.txt "datafusion.apache.org"
[6]: https://datafusion.apache.org/_sources/user-guide/sql/aggregate_functions.md.txt "datafusion.apache.org"
[7]: https://datafusion.apache.org/_sources/user-guide/sql/window_functions.md.txt "datafusion.apache.org"
[8]: https://datafusion.apache.org/_sources/user-guide/sql/special_functions.md.txt "datafusion.apache.org"
[9]: https://datafusion.apache.org/user-guide/explain-usage.html?utm_source=chatgpt.com "Reading Explain Plans — Apache DataFusion documentation"
