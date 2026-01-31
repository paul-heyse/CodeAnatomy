Below is a “programming-LLM-dense” deep dive on **DataFrame expressions in Python** for building **programmatic views** in Apache DataFusion: how to compose expressions, shape nested outputs, control schema, and register/reuse view plans.

---

## 1) View creation is just “name a lazy DataFrame plan”

### Two equivalent ways to register a view

**A) `SessionContext.register_view(name, df)`** — convenience API
The Python docs show `register_view` registers a `DataFrame` under a name, so you can query it later (via SQL or `ctx.table`). ([Apache DataFusion][1])

```python
from datafusion import SessionContext, col, literal

ctx = SessionContext()
df = ctx.from_pydict({"a": [1, 2, 3], "b": [10, 20, 30]}, name="t")

view_df = df.filter(col("a") > literal(1)).select("a", "b")
ctx.register_view("v1", view_df)

# later
df2 = ctx.table("v1")           # retrieve by name
out = df2.collect()
```

([Apache DataFusion][1])

**B) `df.into_view()` → `ctx.register_table(name, view)`** — more explicit / composable
The `DataFrame.into_view(temporary=False)` method converts a `DataFrame` into a `Table` object, which you then register as a table (and keep using the original DataFrame if you want). ([Apache DataFusion][2])

```python
from datafusion import SessionContext

ctx = SessionContext()
df = ctx.sql("SELECT 1 AS value")
view = df.into_view(temporary=False)
ctx.register_table("values_view", view)

ctx.sql("SELECT value FROM values_view").collect()
```

([Apache DataFusion][2])

### Critical mental model: DataFrames are lazy logical plans

A DataFrame is a lazily evaluated plan; execution happens only on terminal calls like `collect()`, `show()`, `to_pandas()`. This matters for views because a view is essentially *a stored plan*, not materialized data. ([Apache DataFusion][2])

---

## 2) Expression building primitives (what every view factory uses)

### 2.1 `col()` and `lit()` / `literal()`

`col("x")` creates a column expression; `lit(value)` creates a literal expression (types inferred from Python object). ([Apache DataFusion][3])

```python
from datafusion import col, lit

expr = (col("a") + lit(1)).alias("a_plus_1")
```

([Apache DataFusion][3])

### 2.2 Boolean composition uses bitwise ops (`& | ~`)

For boolean expression trees, use `&`, `|`, and `~` (not Python `and/or/not`). ([Apache DataFusion][3])

```python
pred = ((col("color") == lit("red")) | (col("color") == lit("green"))) & (col("weight") > lit(42))
```

([Apache DataFusion][3])

### 2.3 Aliasing and sort expressions

* Most expressions can be renamed with `.alias("name")`. ([Apache DataFusion][3])
* Any `Expr` can be turned into a `SortExpr` via `.sort(ascending=..., nulls_first=...)`. ([Apache DataFusion][4])

```python
sorted_df = df.sort(col("age").sort(ascending=False, nulls_first=False))
```

([Apache DataFusion][4])

### 2.4 CASE / conditional expressions (vectorized)

Use `datafusion.functions.case(...)` or `datafusion.functions.when(...)` which produce a `CaseBuilder` and chain `.when(...).otherwise(...)`. ([Apache DataFusion][4])

```python
import datafusion.functions as f
from datafusion import col, lit

df = df.select(
    "column_a",
    f.case(col("column_a"))
      .when(lit(1), lit("One"))
      .when(lit(2), lit("Two"))
      .otherwise(lit("Unknown"))
      .alias("label")
)
```

([Apache DataFusion][4])

---

## 3) The “view shaping” DataFrame methods you should know cold

The Python `DataFrame` API is your view-definition DSL. Key methods (all return new lazy DataFrames):

### Projection and computed columns

* `select(*exprs_or_names)` (accepts `Expr` or column names) ([Apache DataFusion][2])
* `with_column(name, expr_or_sqlstr)` and `with_columns(*exprs, **named_exprs)` (expressions or SQL strings parsed against schema) ([Apache DataFusion][2])
* `drop(*columns)` (note: case-sensitive; quotes stripped if present) ([Apache DataFusion][2])
* `with_column_renamed(old, new)` (supports case-sensitive rename via quoting styles) ([Apache DataFusion][2])

```python
from datafusion import col, lit

df2 = (
    df
    .with_column("b2", col("b") * lit(2))
    .with_columns(
        (col("a") + col("b")).alias("sum_ab"),
        ratio=(col("a") / col("b"))
    )
    .drop("temporary_col")
)
```

([Apache DataFusion][2])

### Filtering

`filter(...)` accepts either `Expr` predicates **or SQL predicate strings**. ([Apache DataFusion][2])

```python
df_filtered = df.filter(col("a") > lit(1))
df_filtered2 = df.filter("a > 1")
```

([Apache DataFusion][2])

### Joins

* `join(right, on=..., how=..., left_on=..., right_on=..., coalesce_duplicate_keys=True)`
* `join_on(right, *on_exprs, how=...)` for (in)equality predicates; equality predicates are optimized. ([Apache DataFusion][2])

Also: join supports `inner/left/right/full/semi/anti`. ([Apache DataFusion][5])

```python
joined = left.join(right, left_on="customer_id", right_on="id", how="left")
ineq = left.join_on(right, col("id") == col("other_id"), how="inner")
```

([Apache DataFusion][2])

**Advanced join behavior:** starting in `datafusion-python` 51.0.0, joins on same-name keys coalesce duplicate join columns by default (`coalesce_duplicate_keys=True`), which reduces ambiguity in post-join column selection. ([Apache DataFusion][5])

### Aggregation and distinctness

* `aggregate(group_by, aggs)` groups and aggregates. `group_by` can be empty for global aggregation. ([Apache DataFusion][2])
* `distinct()` removes duplicate rows. ([Apache DataFusion][2])

### Set operations

* `union(other, distinct=False)` / `union_distinct(other)` require identical schemas ([Apache DataFusion][2])
* `intersect(other)` / `except_all(other)` require identical schemas ([Apache DataFusion][2])

### Partitioning and caching (often matters for view reuse)

* `repartition(num)` and `repartition_by_hash(*exprs, num=...)` let you steer partitioning in the logical plan. ([Apache DataFusion][2])
* `cache()` materializes the DataFrame as a memory table (useful if the same view is referenced repeatedly in a workflow). ([Apache DataFusion][2])

### Debug / introspection

* `schema()` returns a PyArrow schema for contract checks. ([Apache DataFusion][2])
* `logical_plan()` vs `optimized_logical_plan()` to compare before/after optimizer. ([Apache DataFusion][2])
* `explain(verbose=False, analyze=False)` prints the plan and optionally runs it for metrics. ([Apache DataFusion][2])
* `execution_plan()` returns the physical plan. ([Apache DataFusion][2])

---

## 4) Nested / complex schema expression techniques (arrays + structs) for view outputs

### 4.1 Arrays: element access, slicing, and common array ops

Array columns support bracket indexing in Python (0-based), and (since DataFusion 49.0.0) slice syntax like `col("a")[1:3]`. ([Apache DataFusion][3])

```python
df = ctx.from_pydict({"a": [[1, 2, 3], [4, 5, 6]]})

df = df.select(
    col("a")[0].alias("first"),
    col("a")[1:3].alias("slice_1_3"),
)
```

([Apache DataFusion][3])

Useful built-ins demonstrated in the expressions doc:

* `array_empty(col("a"))` / `functions.empty` checks emptiness ([Apache DataFusion][3])
* `cardinality(col("a"))` counts elements ([Apache DataFusion][3])
* array concatenation / repetition via functions like `array_cat/array_concat/array_repeat` (see examples) ([Apache DataFusion][3])

### 4.2 Structs: dictionary-style field access

Struct columns can be accessed like dicts: `col("a")["size"]`. ([Apache DataFusion][3])

```python
df = ctx.from_pydict({"a": [{"size": 15, "color": "green"}, {"size": 10, "color": "blue"}]})
df = df.select(col("a")["size"].alias("a_size"))
```

([Apache DataFusion][3])

### 4.3 Constructing nested outputs: `named_struct`, `struct`, `make_array`, “list_*”

The Python `functions` module is extensive (math/string/temporal/array/list/window/agg/etc.). It includes:

* `named_struct(...)` (returns struct from name/value pairs) ([Apache DataFusion][6])
* `struct(...)` (struct from arguments) ([Apache DataFusion][6])
* `make_array(...)` / `make_list(...)` plus many `list_*` functions (append/slice/distinct/etc.) ([Apache DataFusion][6])

This is the core way to create complex schema views without dropping into SQL text. ([Apache DataFusion][7])

### 4.4 Programmatic explode: `unnest_columns`

If you want “explode array into rows” inside a view definition, `DataFrame.unnest_columns(*cols, preserve_nulls=True)` expands array elements into one row per element. ([Apache DataFusion][2])

```python
exploded = df.unnest_columns("items", preserve_nulls=False)
```

([Apache DataFusion][2])

---

## 5) Schema control for view contracts (casts + type inspection)

For view creation, “schema drift” is a real hazard. DataFusion gives you a few tools to harden schema:

* `df.cast({col_name: pyarrow.DataType})` to cast columns in place. ([Apache DataFusion][2])
* `datafusion.functions.arrow_cast(expr, "TYPE")` and `arrow_typeof(expr)` for expression-level casts / inspection. ([Apache DataFusion][6])
* `df.schema()` returns the output schema for validation. ([Apache DataFusion][2])

Practical “contract pattern” for view factories:

1. Build expressions
2. `df.schema()` check expected columns / types
3. Force key casts (IDs, timestamps) with `cast` or `arrow_cast`
4. Register view

---

## 6) Aggregations: advanced knobs that matter in view definitions

`df.aggregate(group_by, aggs)` is the canonical API, and the docs show:

* global aggregation when `group_by=[]` ([Apache DataFusion][8])
* grouping by multiple keys ([Apache DataFusion][8])
* “parameterized aggregates” such as `distinct`, `order_by`, `null_treatment`, and `filter` (examples include `array_agg(... distinct=True, filter=...)` and `avg(..., filter=...)`). ([Apache DataFusion][8])

Example patterns worth encoding into your view-builder:

```python
from datafusion import functions as f, col, lit
from datafusion.common import NullTreatment

agg = df.aggregate(
    group_by=[col('"Type 1"')],
    aggs=[
        f.array_agg(col('"Type 2"'), distinct=True, filter=col('"Type 2"').is_not_null()).alias("type2_list"),
        f.first_value(col('"Type 2"'), order_by=[col('"Attack"')], null_treatment=NullTreatment.IGNORE_NULLS).alias("first_non_null_type2"),
        f.avg(col('"Speed"'), filter=col('"Attack"') < lit(50)).alias("avg_speed_low_attack"),
    ]
)
```

([Apache DataFusion][8])

---

## 7) Window functions: advanced usage for view creation

### 7.1 Direct window expressions from `functions`

Window functions like `lag` can be used directly in `select`. ([Apache DataFusion][9])

```python
from datafusion import functions as f, col

df = df.select(
    col('"Name"'),
    col('"Speed"'),
    f.lag(col('"Speed"')).alias("prev_speed"),
)
```

([Apache DataFusion][9])

### 7.2 Controlling ordering and partitioning

Many window functions accept `partition_by=[...]` and `order_by=[...]` where order_by uses `Expr.sort(...)`. ([Apache DataFusion][9])

```python
ranked = df.select(
    col('"Type 1"'),
    col('"Attack"'),
    f.rank(
        partition_by=[col('"Type 1"')],
        order_by=[col('"Attack"').sort(ascending=True)]
    ).alias("rank")
)
```

([Apache DataFusion][9])

### 7.3 Custom window frames (rolling, bounded, etc.)

Use `Window` + `WindowFrame` and apply `.over(Window(...))` to aggregate expressions. ([Apache DataFusion][9])

```python
from datafusion.expr import Window, WindowFrame
from datafusion import functions as f, col

rolling = df.select(
    col('"Name"'),
    col('"Speed"'),
    f.avg(col('"Speed"'))
      .over(Window(window_frame=WindowFrame("rows", 2, 0), order_by=[col('"Speed"')]))
      .alias("rolling_avg_speed"),
)
```

([Apache DataFusion][9])

### 7.4 Null treatment in windows

For windowed aggregates like `last_value`, null handling is configurable via `NullTreatment` on the `Window` definition. ([Apache DataFusion][9])

### 7.5 The “builder” pattern for aggregate/window parameters

At the `Expr` level there are builder methods like `.filter(...)`, `.order_by(...)`, `.partition_by(...)`, `.window_frame(...)`, and `.over(window)` that create an `ExprFuncBuilder` (validated at `.build()` time). ([Apache DataFusion][4])

This is the API you use when you need SQL-like `FILTER` / `ORDER BY` semantics attached to an aggregate/window expression, but want to stay purely programmatic.

---

## 8) UDFs and advanced extensibility inside view definitions

### 8.1 Scalar UDFs: batch-at-a-time Arrow arrays

A scalar UDF is a Python function that takes one or more `pyarrow.Array` inputs and returns a `pyarrow.Array`. DataFusion encourages using PyArrow compute functions when possible to avoid costly Python object conversion. ([Apache DataFusion][10])

### 8.2 UDAFs (aggregate): implement an `Accumulator`

UDAFs require implementing `update`, `merge`, `state`, `evaluate` (example shown in docs). ([Apache DataFusion][10])

### 8.3 UDWFs (window): implement `WindowEvaluator`

UDWFs can implement different evaluation styles (`evaluate`, `evaluate_all`, `evaluate_all_with_rank`) depending on options like `uses_window_frame` and `supports_bounded_execution`. ([Apache DataFusion][10])

### 8.4 UDTFs (table functions): programmatic table-valued generators

User-defined table functions accept literal Expr arguments and return a TableProvider; they can be registered with `SessionContext.register_udtf()`. Rust-backed UDTFs can be exposed via PyO3 `PyCapsule`. ([Apache DataFusion][10])

**Why this matters for view creation:** UDTFs are the “escape hatch” for *parameterized view sources* (e.g., generate a table from a run_id, load a custom datasource, etc.) while keeping the rest of the view definition in the DataFrame/Expr world.

---

## 9) Spec-driven view factories: “max programmatic” patterns

### Pattern A: pure-Expr view factory + `transform`

`DataFrame.transform(func, *args)` exists explicitly for chaining reusable transformations—perfect for “view recipe libraries.” ([Apache DataFusion][2])

```python
from datafusion import col, lit, functions as f

def add_features(df):
    return df.with_columns(
        (col("a") + col("b")).alias("sum_ab"),
        is_big=(col("a") > lit(100))
    )

def filter_active(df):
    return df.filter(col("active") == lit(True))

view_df = (
    ctx.table("base")
      .transform(add_features)
      .transform(filter_active)
)

ctx.register_view("v_features", view_df)
```

([Apache DataFusion][2])

### Pattern B: controlled string parsing for user-provided fragments

If your “view spec” is partially textual (filters, projections), you can parse expression strings against a DataFrame’s schema via:

* `df.parse_sql_expr("a > 1")` → `Expr` ([Apache DataFusion][2])
* `select_exprs("a + 1 as a1", ...)` and `filter("a > 1")` accept strings too ([Apache DataFusion][2])

This gives you a hybrid: spec text → validated `Expr` → programmatic plan assembly.

---

## 10) What I’d encode as an “LLM view-builder surface area”

If you’re training/priming an expert Python programming LLM to build view definitions reliably, give it a canonical toolbox:

1. **Expression constructors:** `col`, `lit`, `functions.*`, CASE builder (`functions.case/when`), `.alias`, `.sort`. ([Apache DataFusion][3])
2. **View-safe DataFrame ops:** `select`, `filter`, `with_column(s)`, `aggregate`, `join/join_on`, `sort`, `limit`, `distinct`, `drop`, `cast`. ([Apache DataFusion][2])
3. **Nested access + shaping:** array `[]`/slices, struct `["field"]`, `named_struct`, `make_array`, `unnest_columns`. ([Apache DataFusion][3])
4. **Advanced analytics:** windows with `Window/WindowFrame`, partition/order, null treatment. ([Apache DataFusion][9])
5. **Extensions:** scalar UDF/UDAF/UDWF and (if needed) UDTFs. ([Apache DataFusion][10])
6. **Introspection hooks:** `schema`, `logical_plan`, `optimized_logical_plan`, `explain(analyze=...)`, `execution_plan`. ([Apache DataFusion][2])

---

If you want, I can turn this into an **“agent contract” mini-spec** for view creation (Python): a declarative `ViewSpec` schema → compilation into DataFrame ops (pure Expr), including nested schema outputs and strict schema validation (`df.schema()` checks + explicit casts), so your agents can generate view layers deterministically.

[1]: https://datafusion.apache.org/python/user-guide/common-operations/views.html "Registering Views — Apache Arrow DataFusion  documentation"
[2]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[3]: https://datafusion.apache.org/python/user-guide/common-operations/expressions.html "Expressions — Apache Arrow DataFusion  documentation"
[4]: https://datafusion.apache.org/python/autoapi/datafusion/expr/index.html "datafusion.expr — Apache Arrow DataFusion  documentation"
[5]: https://datafusion.apache.org/python/user-guide/common-operations/joins.html "Joins — Apache Arrow DataFusion  documentation"
[6]: https://datafusion.apache.org/python/autoapi/datafusion/functions/index.html "datafusion.functions — Apache Arrow DataFusion  documentation"
[7]: https://datafusion.apache.org/python/user-guide/common-operations/functions.html "Functions — Apache Arrow DataFusion  documentation"
[8]: https://datafusion.apache.org/python/user-guide/common-operations/aggregations.html "Aggregation — Apache Arrow DataFusion  documentation"
[9]: https://datafusion.apache.org/python/user-guide/common-operations/windows.html "Window Functions — Apache Arrow DataFusion  documentation"
[10]: https://datafusion.apache.org/python/user-guide/common-operations/udf-and-udfa.html "User-Defined Functions — Apache Arrow DataFusion  documentation"


([Past chat][1])([Past chat][2])([Past chat][3])([Past chat][4])([Past chat][4])

Below is a “spec → DataFusion DataFrame plan → registered view” pattern that’s **actually usable** for CPG-style pipelines, where you have multiple extraction layers (AST/CST/SCIP/symtable/bytecode), you want **nested output schemas**, and your spec is driven by **target variable mappings** (a small IR that decides *what each target column means* and *how to compute it* from available sources).

I’ll show:

1. **Spec model** (what you write)
2. **Compiler** (how you turn spec into DataFusion DataFrames)
3. A **CPG-relevant worked example** with nested structs + arrays of structs

---

## 0) Ground rules: what DataFusion-Python gives you for “spec compilation”

You can make the entire view layer programmatic because:

* A view is just a `DataFrame` registered under a name via `ctx.register_view(...)`. ([Apache DataFusion][5])
* A `DataFrame` is a *lazy logical plan*; you build it via transformations (`filter`, `join`, `select`, `aggregate`, etc.). ([Apache DataFusion][6])
* You can parse **SQL expression strings into typed expressions** against the current schema using `df.parse_sql_expr(...)`. ([Apache DataFusion][7])
* You can add many derived columns at once via `df.with_columns(...)`, including **SQL expression strings** (so your spec doesn’t have to be a Python AST). ([Apache DataFusion][7])
* You can compose pipelines as reusable “view transforms” using `df.transform(fn, ...)` (this is perfect for spec-driven factories). ([Apache DataFusion][7])
* You can build nested types:

  * `named_struct([(name, expr), ...])` → struct ([Apache DataFusion][8])
  * `make_array(*exprs)` / `make_list(*exprs)` → list/array ([Apache DataFusion][8])
  * `array_agg(expr, distinct=..., filter=..., order_by=...)` → list aggregated across rows, with a notable `distinct`+`order_by` limitation. ([Apache DataFusion][8])
* Joins are fully programmatic:

  * `df.join(... how=inner/left/right/full/semi/anti ...)` ([Apache DataFusion][7])
  * `df.join_on(right, *on_exprs, how=...)` for expression-based join conditions (including inequality). ([Apache DataFusion][7])
* You can explode list columns into rows via `df.unnest_columns(...)`. ([Apache DataFusion][7])

These are the building blocks you need for a “spec compiler.”

---

## 1) The spec you actually want for CPG: “target mappings among variables”

When you say “target mappings amongst variables,” the highest-leverage pattern is:

* You define **targets** (the columns you want in the final view)
* Each target is defined in terms of:

  * **source variables** (columns from AST/CST/SCIP/symtable/bytecode inputs)
  * **derived variables** (intermediate computations)
  * **resolution rules** (fallback precedence, normalization, coalescing, schema shaping)

That is: a *small expression DAG*, plus a *join plan*, plus a *grouping/aggregation plan*.

### 1.1 Minimal spec IR (good enough to start)

This IR is intentionally small but covers most CPG view patterns:

* `sources`: named inputs → DataFusion table names
* `joins`: edges describing how to combine sources
* `vars`: derived variables (DAG)
* `targets`: final projection
* `groups`: optional grouped aggregations that produce nested lists (array-of-structs)

Here’s a JSON-ish sketch (you can store as YAML too):

```python
CPG_NODE_VIEW_SPEC = {
  "name": "v_cpg_node_facts",

  "sources": {
    "ast": "ast_nodes",
    "cst": "cst_nodes",
    "scip": "scip_occurrences",
    "sym": "symtable_symbols",
    "bc":  "bytecode_instructions",
  },

  # join graph between sources (your “linked dataframes”)
  "joins": [
    # attach CST node id when available
    {"left": "ast", "right": "cst", "how": "left",
     "on": [("ast__span_id", "cst__span_id")]},

    # attach symtable facts by (file, scope, name) — illustrative
    {"left": "ast", "right": "sym", "how": "left",
     "on": [("ast__file", "sym__file"),
            ("ast__scope_id", "sym__scope_id"),
            ("ast__ident", "sym__ident")]},

    # attach bytecode facts by (file, lineno) — coarse but common
    {"left": "ast", "right": "bc", "how": "left",
     "on": [("ast__file", "bc__file"),
            ("ast__start_line", "bc__lineno")]},
  ],

  # variable DAG: derived columns (some are complex/nested)
  "vars": {
    # span struct: { file, start:{line,col}, end:{line,col} }
    "loc": {"struct": {
      "file": {"col": "ast__file"},
      "start": {"struct": {"line": {"col": "ast__start_line"}, "col": {"col": "ast__start_col"}}},
      "end":   {"struct": {"line": {"col": "ast__end_line"},   "col": {"col": "ast__end_col"}}},
    }},

    # “layer ids” helps unify AST/CST/bytecode/… under one node identity
    "layer_ids": {"struct": {
      "ast": {"col": "ast__node_id"},
      "cst": {"col": "cst__node_id"},
    }},

    # symtable payload (you’ll usually pack a lot more here)
    "sym_payload": {"struct": {
      "scope_id": {"col": "sym__scope_id"},
      "flags":    {"col": "sym__flags"},
      "is_local": {"col": "sym__is_local"},
    }},

    # bytecode payload per instruction row
    "bc_instr": {"struct": {
      "offset": {"col": "bc__offset"},
      "opname": {"col": "bc__opname"},
      "arg":    {"col": "bc__argrepr"},
    }},
  },

  # grouped aggregations producing nested arrays of structs
  "groups": [
    {
      "name": "bc_instrs_by_ast_node",
      "group_by": ["ast__node_id"],
      "aggs": {
        "bytecode_instrs": {
          "array_agg": {"expr": {"var": "bc_instr"}, "order_by": ["bc__offset"]}
        }
      }
    }
  ],

  # final output contract for the view
  "targets": {
    "node_id": {"col": "ast__node_id"},
    "kind":    {"col": "ast__kind"},
    "loc":     {"var": "loc"},
    "layer_ids": {"var": "layer_ids"},
    "sym":     {"var": "sym_payload"},
    "bytecode": {"col": "bytecode_instrs"},   # comes from the group output
  },
}
```

This is the “target mapping” idea: `targets.bytecode` does *not* point to a raw column; it points to a grouped array derived via a named variable and aggregation.

---

## 2) Compiler design: turning specs into DataFusion DataFrames

### 2.1 Namespacing sources: prefix columns so the spec is unambiguous

DataFusion joins can coalesce duplicate keys, but for spec compilation you’ll be happier if *every input column is namespaced*.

Approach:

1. load each source table
2. project/rename columns to `alias__colname`
3. do joins using these namespaced columns

(You can do this with `select_exprs(...)` or `with_columns(...)` string expressions; both are parsed into logical expressions. ([Apache DataFusion][7]))

### 2.2 Expression compilation: support a tiny subset, but make it extensible

You generally need:

* `{"col": "ast__file"}` → `col("ast__file")`
* `{"lit": 123}` → `lit(123)`
* `{"sql": "coalesce(a,b)"}` → `df.parse_sql_expr(...)` ([Apache DataFusion][7])
* `{"struct": {"field": expr, ...}}` → `functions.named_struct([...])` ([Apache DataFusion][8])
* `{"array": [expr, ...]}` → `functions.make_array(...)` ([Apache DataFusion][8])
* `{"array_agg": {"expr": <expr>, "order_by":[...], ...}}` → `functions.array_agg(...)` ([Apache DataFusion][8])
  (must be placed inside an aggregation plan)

### 2.3 Join plan compilation

* `df.join(...)` for equijoins / named join keys ([Apache DataFusion][7])
* `df.join_on(...)` if you need expression-based conditions (e.g., interval containment for span joins) ([Apache DataFusion][7])

### 2.4 Group compilation (nested list outputs)

* For “node → list of occurrences / instructions / refs,” you produce:

  * per-row struct (e.g., `bc_instr`)
  * group by `node_id`
  * aggregate via `array_agg(struct_expr, order_by=...)` ([Apache DataFusion][8])

If you later need to explode those arrays (for edge derivation), use `unnest_columns(...)`. ([Apache DataFusion][7])

---

## 3) Worked CPG example: AST + symtable + bytecode + SCIP

### 3.1 Where these layers come from (why the joins/mappings make sense)

* **symtable**: Python symbol tables are produced by the compiler from the AST “just before bytecode is generated,” and they encode identifier scoping. ([Python documentation][9])
* **bytecode**: the `dis` module provides programmatic inspection of CPython bytecode. ([Python documentation][10])
* **SCIP**: a language-agnostic code intelligence protocol with a protobuf schema; used to represent symbols + relationships to source locations. The repo explicitly points to its schema and bindings. ([GitHub][11])
  For debugging a `.scip` index file, Sourcegraph documents decoding it with `protoc --decode=scip.Index scip.proto`. ([Sourcegraph Help Center][12])
* **CPG framing**: a CPG merges syntactic/semantic/control/data properties into a property graph representation (common reference implementation: Joern). ([Joern Docs][13])

### 3.2 What the view is doing

We want a view that, for each AST node:

* emits a stable node identity (`node_id`)
* emits a nested location struct (`loc`)
* attaches **layer IDs** (AST/CST)
* attaches symbol/scoping facts (symtable)
* attaches local execution signals (bytecode instruction list)
* optionally attaches code-intel symbols/references (SCIP occurrences)

This is exactly what spec-driven view factories excel at: the shape is *stable* while the sources evolve.

---

## 4) Concrete “spec compiler” skeleton in Python (DataFusion-Python)

Below is a compact but real compilation pattern. It intentionally:

* uses DataFusion’s `with_columns(...)` and `parse_sql_expr(...)` to accept SQL fragments from spec ([Apache DataFusion][7])
* uses `named_struct`, `make_array`, `array_agg` for nested schema outputs ([Apache DataFusion][8])
* composes stages using `transform(...)` where it improves readability ([Apache DataFusion][7])

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Optional

import datafusion.functions as f
from datafusion import SessionContext, col, lit
from datafusion.expr import Expr


# ---------- spec helpers ----------

def compile_expr(df, env: Dict[str, Expr], spec: Dict[str, Any]) -> Expr:
    """
    Compile a tiny expression IR to a DataFusion Expr.
    - df is only used for parse_sql_expr when spec uses {"sql": "..."}.
    - env provides compiled vars and lets {"var": "..."} reference them.
    """
    if "col" in spec:
        return col(spec["col"])
    if "lit" in spec:
        return lit(spec["lit"])
    if "var" in spec:
        return env[spec["var"]]
    if "sql" in spec:
        # Parse SQL expression against current schema
        return df.parse_sql_expr(spec["sql"])
    if "struct" in spec:
        fields = []
        for name, child in spec["struct"].items():
            fields.append((name, compile_expr(df, env, child)))
        return f.named_struct(fields)  # struct with stable field names
    if "array" in spec:
        items = [compile_expr(df, env, child) for child in spec["array"]]
        return f.make_array(*items)

    raise ValueError(f"Unsupported expr spec: {spec}")


def apply_joins(base_df, right_dfs: Dict[str, Any], joins: List[Dict[str, Any]]):
    """
    Apply a list of equijoins described by ("left_col","right_col") pairs.
    """
    df = base_df
    for j in joins:
        right = right_dfs[j["right"]]
        how = j.get("how", "inner")
        left_cols, right_cols = zip(*j["on"])
        df = df.join(
            right,
            how=how,
            left_on=list(left_cols),
            right_on=list(right_cols),
        )
    return df


def compile_group(df, group_spec: Dict[str, Any], env: Dict[str, Expr]):
    """
    Compile a group producing new columns via aggregate expressions.
    Uses array_agg(...) to create list<struct<...>> columns.
    """
    group_by_exprs = [col(c) for c in group_spec["group_by"]]

    aggs = []
    for out_col, agg_spec in group_spec["aggs"].items():
        if "array_agg" in agg_spec:
            aa = agg_spec["array_agg"]
            elem_expr = compile_expr(df, env, aa["expr"])  # often {"var": "bc_instr"} etc
            order_by = aa.get("order_by")
            agg_expr = f.array_agg(elem_expr, order_by=order_by)
            aggs.append(agg_expr.alias(out_col))
        else:
            raise ValueError(f"Unsupported agg: {agg_spec}")

    return df.aggregate(group_by_exprs, aggs)


# ---------- a CPG-ish view factory ----------

def build_view(ctx: SessionContext, spec: Dict[str, Any]) -> None:
    # 1) Load sources (assume you already registered these as tables in DataFusion)
    sources = {k: ctx.table(v) for k, v in spec["sources"].items()}

    # 2) Choose a base (left-deep join tree for simplicity)
    base_name = "ast"
    df = sources[base_name]
    remaining = {k: v for k, v in sources.items() if k != base_name}

    # 3) Join graph
    df = apply_joins(df, remaining, spec.get("joins", []))

    # 4) Compile derived vars (expression DAG) — single-pass example
    env: Dict[str, Expr] = {}
    for var_name, var_spec in spec.get("vars", {}).items():
        env[var_name] = compile_expr(df, env, var_spec)

    # 5) Compile grouped nested arrays (e.g., bytecode_instrs_by_ast_node)
    #    Here we do it in-place: group -> join back
    for g in spec.get("groups", []):
        grouped = compile_group(df, g, env)

        # join back by group_by keys (assume single key for brevity)
        k = g["group_by"][0]
        df = df.join(grouped, how="left", left_on=[k], right_on=[k])

    # 6) Final projection according to targets
    target_exprs = []
    for out_name, out_spec in spec["targets"].items():
        expr = compile_expr(df, env, out_spec)
        target_exprs.append(expr.alias(out_name))

    df = df.select(*target_exprs)

    # 7) Register the resulting DataFrame as a view
    ctx.register_view(spec["name"], df)
```

**Where the spec uses built-in DataFusion capabilities:**

* `parse_sql_expr` turns SQL strings into expressions (great escape hatch for functions not wrapped in Python). ([Apache DataFusion][7])
* `with_columns` can accept either Exprs *or* SQL strings (often easiest for “mapping rules”). ([Apache DataFusion][7])
* `array_agg` builds nested `list<...>` columns during aggregation and supports `order_by` but not `distinct`+`order_by` together. ([Apache DataFusion][8])
* `named_struct` gives you stable, named structs (your nested schema contract). ([Apache DataFusion][8])

---

## 5) Extending the example to include SCIP (symbol occurrences) and “target mappings”

### 5.1 Typical mapping rule: `symbol_id` resolution

For a CPG, you often want a canonical `symbol_id` that can be populated from multiple sources:

* Prefer SCIP symbol string when present
* Else fall back to symtable qualified name
* Else fall back to “best effort” AST-derived name

You can express this as a spec mapping via SQL, and compile it via `parse_sql_expr`:

```python
"symbol_id": {"sql": "coalesce(scip__symbol, sym__qualified_name, ast__qualname)"}
```

`parse_sql_expr` will resolve it against the schema at compile time. ([Apache DataFusion][7])

### 5.2 SCIP occurrences as a nested list-of-struct per node

A common pattern:

1. shape a per-occurrence struct `occ = {symbol, role, range}`
2. group by `ast__node_id` (or span bucket)
3. `array_agg(occ order_by=...)` to get `occurrences: list<struct<...>>`

You can do this with the same aggregation machinery as bytecode. `array_agg` explicitly supports `order_by`. ([Apache DataFusion][8])

For the “what is SCIP” grounding: the SCIP repo explicitly includes the protobuf schema and bindings ([GitHub][11]), and the announcement blog notes the schema encodes relationships between symbols and source locations. ([Sourcegraph][14])

---

## 6) Why this is CPG-friendly (and how to evolve it)

A code property graph merges multiple program representations; the key to making it evolvable is:

* **Keep extracted tables “flat and lossless”** (AST nodes, bytecode instructions, symtable bindings, SCIP occurrences)
* **Use views as the “semantic interface layer”** that:

  * picks canonical IDs
  * normalizes spans
  * attaches nested payloads (struct/list-of-struct) for convenience
  * produces edge-ready tables (via `unnest_columns` when needed) ([Apache DataFusion][7])

This matches the way CPGs are described in the Joern ecosystem: a single intermediate representation that unifies multiple facets of program structure/semantics. ([Joern Docs][13])

---

## 7) Practical “next step” if you want this to feel *fully declarative*

If you like this direction, the highest ROI additions to the spec/compiler are:

1. **A dependency-resolving var DAG**

   * topologically sort `vars` so ordering isn’t manual
2. **Join-on expressions** for range containment (SCIP occurrence inside AST span)

   * compile `join_on` conditions (supports expression-based joins). ([Apache DataFusion][7])
3. **Schema contracts**

   * validate `df.schema()` after compilation; if drift occurs, fail fast or apply explicit casts
4. **Two-phase aggregation**

   * build `*_by_node` grouped subviews (bytecode, scip occs, maybe “call edges”), then join them back into `v_cpg_node_facts`
5. **A small function registry**

   * allow spec ops like `{"op":"named_struct", ...}` and `{"op":"array_agg", ...}` so you don’t rely on embedded SQL strings except for truly missing wrappers

If you paste your *actual* column schemas for (a) AST/CST node tables, (b) symtable extraction, (c) bytecode instruction tables, and (d) whatever SCIP tables you’re writing, I can rewrite the example into a **fully concrete spec + compiler** that outputs the nested CPG node/edge views you want (including canonical IDs and span-join strategy).

[1]: https://chatgpt.com/c/695b76ee-8914-832a-a607-4638e5d5a0ed "CPG implementation review"
[2]: https://chatgpt.com/c/69727693-4198-8327-a538-a491ad96d85c "Code Property Graphs for AI"
[3]: https://chatgpt.com/c/6967034e-2258-832e-836c-322797598517 "Complex Query Design for LLM"
[4]: https://chatgpt.com/c/6961cfa1-37ec-8330-97c2-b83d36322791 "Review of pyarrow approach"
[5]: https://datafusion.apache.org/python/user-guide/common-operations/views.html?utm_source=chatgpt.com "Registering Views — Apache Arrow DataFusion documentation"
[6]: https://datafusion.apache.org/python/user-guide/dataframe/index.html?utm_source=chatgpt.com "DataFrames — Apache Arrow DataFusion documentation"
[7]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[8]: https://datafusion.apache.org/python/autoapi/datafusion/functions/index.html "datafusion.functions — Apache Arrow DataFusion  documentation"
[9]: https://docs.python.org/3/library/symtable.html?utm_source=chatgpt.com "symtable — Access to the compiler's symbol tables"
[10]: https://docs.python.org/3/library/dis.html?utm_source=chatgpt.com "dis — Disassembler for Python bytecode"
[11]: https://github.com/sourcegraph/scip "GitHub - sourcegraph/scip: SCIP Code Intelligence Protocol"
[12]: https://help.sourcegraph.com/hc/en-us/articles/15045932124941-Decoding-SCIP-index-file?utm_source=chatgpt.com "Decoding SCIP index file"
[13]: https://docs.joern.io/code-property-graph/?utm_source=chatgpt.com "Code Property Graph"
[14]: https://sourcegraph.com/blog/announcing-scip?utm_source=chatgpt.com "SCIP - a better code indexing format than LSIF"


Below is a **fully worked, minimal-schema** set of examples implementing *each* “Section 7” ROI addition, end-to-end, in a way that stays **spec-driven** (targets are mappings over variables), supports **nested outputs**, and uses **rustworkx** for the var-DAG.

I’ll keep the schemas exactly in the “simple illustrative” spirit you said you prefer, but the patterns scale to your real ones.

---

## 0) Minimalistic input schemas (the five “layers”)

These are the only columns we’ll assume exist in the raw tables (pre-prefix). The compiler will namespace them (`ast__*`, `scip__*`, …) so the spec is unambiguous.

```python
import pyarrow as pa

AST_SCHEMA = pa.schema([
    pa.field("node_id", pa.int64(), nullable=False),
    pa.field("kind", pa.string(), nullable=False),
    pa.field("file", pa.string(), nullable=False),
    pa.field("start_line", pa.int32(), nullable=False),
    pa.field("start_col", pa.int32(), nullable=False),
    pa.field("end_line", pa.int32(), nullable=False),
    pa.field("end_col", pa.int32(), nullable=False),
    pa.field("span_id", pa.int64(), nullable=True),     # link to CST (optional)
    pa.field("scope_id", pa.int64(), nullable=True),    # link to symtable (optional)
    pa.field("ident", pa.string(), nullable=True),      # link to symtable (optional)
])

CST_SCHEMA = pa.schema([
    pa.field("node_id", pa.int64(), nullable=False),
    pa.field("span_id", pa.int64(), nullable=False),
])

SYM_SCHEMA = pa.schema([
    pa.field("file", pa.string(), nullable=False),
    pa.field("scope_id", pa.int64(), nullable=False),
    pa.field("ident", pa.string(), nullable=False),
    pa.field("qualified_name", pa.string(), nullable=True),
    pa.field("flags", pa.int64(), nullable=True),
    pa.field("is_local", pa.bool_(), nullable=True),
])

BC_SCHEMA = pa.schema([
    pa.field("file", pa.string(), nullable=False),
    pa.field("lineno", pa.int32(), nullable=False),
    pa.field("offset", pa.int32(), nullable=False),
    pa.field("opname", pa.string(), nullable=False),
    pa.field("argrepr", pa.string(), nullable=True),
])

SCIP_SCHEMA = pa.schema([
    pa.field("file", pa.string(), nullable=False),
    pa.field("start_line", pa.int32(), nullable=False),
    pa.field("start_col", pa.int32(), nullable=False),
    pa.field("end_line", pa.int32(), nullable=False),
    pa.field("end_col", pa.int32(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("role", pa.string(), nullable=True),  # keep as string for demo
])
```

---

## 1) The spec shape (targets driven by variable mappings + nested outputs)

This version uses a **function registry** style (`{"op": ...}`) so you don’t need embedded SQL except as a last resort.

```python
CPG_VIEW_SPEC = {
  "name": "v_cpg_node_facts",

  "sources": {
    "ast":  "ast_nodes",
    "cst":  "cst_nodes",
    "sym":  "symtable_symbols",
    "bc":   "bytecode_instructions",
    "scip": "scip_occurrences",
  },

  # optional: early type coercions (schema contracts start here)
  "coerce_inputs": {
    # if any upstream writer drifted, normalize here
    # "ast__node_id": pa.int64(), ...
  },

  # joins (equijoins and join_on range joins)
  "joins": [
    # AST ↔ CST by span_id
    {"type": "equi", "left": "ast", "right": "cst", "how": "left",
     "on": [("ast__span_id", "cst__span_id")]},

    # AST ↔ symtable by (file, scope_id, ident)
    {"type": "equi", "left": "ast", "right": "sym", "how": "left",
     "on": [("ast__file", "sym__file"), ("ast__scope_id", "sym__scope_id"), ("ast__ident", "sym__ident")]},

    # AST ↔ bytecode by (file AND lineno within AST [start_line,end_line]) using join_on
    {"type": "range_join", "left": "ast", "right": "bc", "how": "left",
     "predicates": [
       {"op": "eq", "a": {"op": "col", "name": "ast__file"}, "b": {"op": "col", "name": "bc__file"}},
       {"op": "between_inclusive",
        "x": {"op": "col", "name": "bc__lineno"},
        "lo": {"op": "col", "name": "ast__start_line"},
        "hi": {"op": "col", "name": "ast__end_line"}},
     ]},

    # AST ↔ SCIP by file + full range containment (join_on inequality)
    {"type": "range_join", "left": "ast", "right": "scip", "how": "left",
     "predicates": [
       {"op": "eq", "a": {"op": "col", "name": "ast__file"}, "b": {"op": "col", "name": "scip__file"}},
       {"op": "span_contains",
        "container": {
          "start_line": {"op": "col", "name": "ast__start_line"},
          "start_col":  {"op": "col", "name": "ast__start_col"},
          "end_line":   {"op": "col", "name": "ast__end_line"},
          "end_col":    {"op": "col", "name": "ast__end_col"},
        },
        "inner": {
          "start_line": {"op": "col", "name": "scip__start_line"},
          "start_col":  {"op": "col", "name": "scip__start_col"},
          "end_line":   {"op": "col", "name": "scip__end_line"},
          "end_col":    {"op": "col", "name": "scip__end_col"},
        }},
     ]},
  ],

  # vars (DAG) — nested structs built from other vars, not hand-ordered
  "vars": {
    "start_pos": {"op": "named_struct", "fields": [
      ("line", {"op": "col", "name": "ast__start_line"}),
      ("col",  {"op": "col", "name": "ast__start_col"}),
    ]},
    "end_pos": {"op": "named_struct", "fields": [
      ("line", {"op": "col", "name": "ast__end_line"}),
      ("col",  {"op": "col", "name": "ast__end_col"}),
    ]},
    "loc": {"op": "named_struct", "fields": [
      ("file",  {"op": "col", "name": "ast__file"}),
      ("start", {"op": "var", "name": "start_pos"}),
      ("end",   {"op": "var", "name": "end_pos"}),
    ]},

    "layer_ids": {"op": "named_struct", "fields": [
      ("ast", {"op": "col", "name": "ast__node_id"}),
      ("cst", {"op": "col", "name": "cst__node_id"}),
    ]},

    "sym_payload": {"op": "named_struct", "fields": [
      ("qualified_name", {"op": "col", "name": "sym__qualified_name"}),
      ("flags",          {"op": "col", "name": "sym__flags"}),
      ("is_local",       {"op": "col", "name": "sym__is_local"}),
    ]},

    "bc_instr": {"op": "named_struct", "fields": [
      ("offset", {"op": "col", "name": "bc__offset"}),
      ("opname", {"op": "col", "name": "bc__opname"}),
      ("arg",    {"op": "col", "name": "bc__argrepr"}),
    ]},

    "scip_range": {"op": "named_struct", "fields": [
      ("start", {"op": "named_struct", "fields": [
        ("line", {"op": "col", "name": "scip__start_line"}),
        ("col",  {"op": "col", "name": "scip__start_col"}),
      ]}),
      ("end", {"op": "named_struct", "fields": [
        ("line", {"op": "col", "name": "scip__end_line"}),
        ("col",  {"op": "col", "name": "scip__end_col"}),
      ]}),
    ]},

    "scip_occ": {"op": "named_struct", "fields": [
      ("symbol", {"op": "col", "name": "scip__symbol"}),
      ("role",   {"op": "col", "name": "scip__role"}),
      ("range",  {"op": "var", "name": "scip_range"}),
    ]},
  },

  # two-phase aggregations (build *_by_node then join back)
  "groups": [
    {
      "name": "v_bc_by_node",
      "group_by": ["ast__node_id"],
      "aggs": {
        "bytecode_instrs": {"op": "array_agg", "expr": {"op": "var", "name": "bc_instr"},
                            "order_by": [{"col": "bc__offset", "ascending": True}]}
      }
    },
    {
      "name": "v_scip_by_node",
      "group_by": ["ast__node_id"],
      "aggs": {
        "scip_occs": {"op": "array_agg", "expr": {"op": "var", "name": "scip_occ"},
                      "order_by": [{"col": "scip__start_line", "ascending": True},
                                   {"col": "scip__start_col", "ascending": True}]}
      }
    },

    # optional: “call edges” as evidence list (toy example via filtering SCIP role)
    {
      "name": "v_calls_by_node",
      "pre_filter": {"op": "eq", "a": {"op": "col", "name": "scip__role"}, "b": {"op": "lit", "value": "call"}},
      "group_by": ["ast__node_id"],
      "aggs": {
        "calls": {"op": "array_agg",
                  "expr": {"op": "named_struct", "fields": [
                    ("callee_symbol", {"op": "col", "name": "scip__symbol"}),
                    ("evidence",      {"op": "lit", "value": "scip.role==call"}),
                  ]}}
      }
    },
  ],

  "targets": {
    "node_id":     {"op": "col", "name": "ast__node_id"},
    "kind":        {"op": "col", "name": "ast__kind"},
    "loc":         {"op": "var", "name": "loc"},
    "layer_ids":   {"op": "var", "name": "layer_ids"},
    "sym":         {"op": "var", "name": "sym_payload"},
    "bytecode":    {"op": "col", "name": "bytecode_instrs"},
    "scip":        {"op": "col", "name": "scip_occs"},
    "calls":       {"op": "col", "name": "calls"},
  },
}
```

Key API facts this spec leans on:

* **`DataFrame.join_on`** supports in-equality predicates, and equality predicates are still optimized. ([Apache DataFusion][1])
* **`DataFrame.parse_sql_expr`** exists as an escape hatch (we’ll include it in the registry but use it only when needed). ([Apache DataFusion][1])
* **`functions.named_struct`** builds a stable named struct. ([Apache DataFusion][2])
* **`functions.array_agg`** exists (note the current Python wrapper limitation re: `distinct` + `order_by`). ([Apache DataFusion][2])

---

## 2) (ROI #1) Dependency-resolving var DAG using rustworkx

### 2.1 Extract var dependencies from expression specs

```python
def iter_var_refs(expr_spec):
    """Yield var names referenced by {'op':'var','name':...} recursively."""
    if isinstance(expr_spec, dict):
        if expr_spec.get("op") == "var":
            yield expr_spec["name"]
        # walk children
        for v in expr_spec.values():
            yield from iter_var_refs(v)
    elif isinstance(expr_spec, (list, tuple)):
        for v in expr_spec:
            yield from iter_var_refs(v)

def build_var_dep_graph(vars_spec):
    """
    Return adjacency edges dep -> user.
    If var 'loc' references var 'start_pos', we add edge start_pos -> loc.
    """
    edges = []
    for var_name, spec in vars_spec.items():
        for dep in set(iter_var_refs(spec)):
            if dep not in vars_spec:
                raise KeyError(f"Var '{var_name}' references unknown var '{dep}'")
            edges.append((dep, var_name))
    return edges
```

### 2.2 Topologically sort with rustworkx

Use either:

* `rustworkx.topological_sort(graph)` for a one-shot ordering ([Rustworkx][3])
* or `rustworkx.TopologicalSorter` if you want “compile in waves / parallelize ready nodes” (get_ready/done loop). ([Rustworkx][4])

```python
import rustworkx as rx

def topo_order_vars(vars_spec):
    edges = build_var_dep_graph(vars_spec)

    g = rx.PyDiGraph()
    # stable node indexing: add nodes in sorted var name order
    names = sorted(vars_spec.keys())
    idx = {name: g.add_node(name) for name in names}
    for dep, user in edges:
        g.add_edge(idx[dep], idx[user], None)

    # Option A: one-shot sort
    order_idx = rx.topological_sort(g)
    order = [g.get_node_data(i) for i in order_idx]
    return order

def topo_waves_vars(vars_spec):
    """
    Option B: wave-based compilation (useful if you want concurrency).
    """
    edges = build_var_dep_graph(vars_spec)

    g = rx.PyDiGraph()
    names = sorted(vars_spec.keys())
    idx = {name: g.add_node(name) for name in names}
    for dep, user in edges:
        g.add_edge(idx[dep], idx[user], None)

    sorter = rx.TopologicalSorter(g)  # get_ready/done loop is documented :contentReference[oaicite:6]{index=6}
    waves = []
    while sorter.is_active():
        ready = sorter.get_ready()
        waves.append([g.get_node_data(i) for i in ready])
        sorter.done(ready)
    return waves
```

That eliminates “manual ordering” in your spec: you can write `loc` before `start_pos` and it will still compile.

---

## 3) (ROI #2) join_on range containment (SCIP occurrence inside AST span)

### 3.1 A robust “span contains” boolean Expr builder

This compiles the containment semantics you typically want in source-coordinate systems.

```python
from datafusion import col, lit

def span_contains_expr(container, inner):
    """
    container/inner are dicts of Exprs:
      start_line,start_col,end_line,end_col -> Expr
    Returns Expr: inner range fully contained in container range.
    """
    c_sl, c_sc, c_el, c_ec = container["start_line"], container["start_col"], container["end_line"], container["end_col"]
    i_sl, i_sc, i_el, i_ec = inner["start_line"], inner["start_col"], inner["end_line"], inner["end_col"]

    start_ok = (i_sl > c_sl) | ((i_sl == c_sl) & (i_sc >= c_sc))
    end_ok   = (i_el < c_el) | ((i_el == c_el) & (i_ec <= c_ec))
    return start_ok & end_ok
```

### 3.2 Compile into `DataFrame.join_on(...)`

`join_on` explicitly exists for in-equality predicates and accepts *multiple* predicate Exprs. ([Apache DataFusion][1])

```python
def apply_range_join(df_left, df_right, predicates, how="left"):
    """
    predicates: list[Expr] — will be passed as *on_exprs to join_on
    """
    return df_left.join_on(df_right, *predicates, how=how)  # :contentReference[oaicite:8]{index=8}
```

This is the pattern you’ll use for:

* SCIP occurrence → AST node containment
* bytecode instruction line ranges → AST node line ranges
* CST span → AST span containment (if span_id link is absent)

---

## 4) (ROI #3) Schema contracts (validate df.schema; cast/fail fast)

### 4.1 Schema inspection is a first-class API

`DataFrame.schema()` returns the output `pyarrow.Schema` (names, types, nullability). ([Apache DataFusion][1])

### 4.2 Fast “contract check” + optional coercion

`DataFrame.cast(mapping)` casts one or more columns (top-level) to a given Arrow datatype. ([Apache DataFusion][1])

```python
import pyarrow as pa

def assert_schema(df, expected: pa.Schema, *, casts=None, strict=True):
    actual = df.schema()  # :contentReference[oaicite:11]{index=11}
    if actual.equals(expected, check_metadata=False):
        return df

    if casts:
        df = df.cast(casts)  # :contentReference[oaicite:12]{index=12}
        actual2 = df.schema()
        if actual2.equals(expected, check_metadata=False):
            return df

    if strict:
        raise ValueError(
            "Schema contract mismatch.\n"
            f"Expected: {expected}\n"
            f"Actual:   {actual}\n"
        )
    return df
```

**Practical CPG note:** nested schemas are easiest to keep stable by:

1. coercing **leaf** scalar columns first (`ast.start_line`, `scip.start_line`, etc.)
2. then building structs/lists from those coerced leaves (your nested `loc`, `scip_range` will inherit stable types)

---

## 5) (ROI #4) Two-phase aggregation (*_by_node views) + join-back

### 5.1 Why two-phase?

You almost always want a “wide node facts view”:

* 1 row per AST node
* nested payload columns:

  * `bytecode: list<struct<...>>`
  * `scip: list<struct<...>>`
  * maybe `calls: list<struct<...>>`

The clean way is:

1. build a **base joined rowset** (AST×CST×sym×bc×scip)
2. build **grouped subviews** keyed by `ast__node_id`
3. join those subviews back into the base node table

DataFusion’s DataFrame API supports:

* `aggregate(group_by, aggs)` ([Apache DataFusion][1])
* `array_agg(...)` aggregate function ([Apache DataFusion][2])
* and `named_struct(...)` for per-row struct payloads ([Apache DataFusion][2])

### 5.2 Build `v_bc_by_node` and `v_scip_by_node`

```python
import datafusion.functions as f

def build_bc_by_node(df, env):
    # df includes ast__node_id and bc__* columns
    bc_instr = env["bc_instr"]
    return (
        df.aggregate(
            group_by=["ast__node_id"],
            aggs=[f.array_agg(
                    bc_instr,
                    order_by=[{"col": "bc__offset", "ascending": True}]
                 ).alias("bytecode_instrs")]
        )
    )  # aggregate() exists and returns a DataFrame :contentReference[oaicite:16]{index=16}

def build_scip_by_node(df, env):
    scip_occ = env["scip_occ"]
    return (
        df.aggregate(
            group_by=["ast__node_id"],
            aggs=[f.array_agg(
                    scip_occ,
                    order_by=[{"col": "scip__start_line", "ascending": True},
                              {"col": "scip__start_col", "ascending": True}]
                 ).alias("scip_occs")]
        )
    )
```

> Note: the Python `array_agg` wrapper currently calls out that `distinct` and `order_by` can’t be used together (workaround: aggregate then sort). ([Apache DataFusion][2])

### 5.3 Join-back pattern

Use `join` (equijoin) to attach the aggregated arrays onto the 1-row-per-node table.

```python
def attach_grouped_payloads(base_nodes_df, bc_by_node, scip_by_node, calls_by_node=None):
    out = base_nodes_df
    out = out.join(bc_by_node, how="left", left_on="ast__node_id", right_on="ast__node_id")
    out = out.join(scip_by_node, how="left", left_on="ast__node_id", right_on="ast__node_id")
    if calls_by_node is not None:
        out = out.join(calls_by_node, how="left", left_on="ast__node_id", right_on="ast__node_id")
    return out
```

### 5.4 Turning nested lists into edge tables (optional)

When you want edge tables (e.g., call edges), explode `calls` (list-of-struct) into rows via `unnest_columns`. ([Apache DataFusion][1])

```python
def calls_edge_table(node_facts_df):
    # 1 row per (node_id, call_struct)
    exploded = node_facts_df.select("node_id", "calls").unnest_columns("calls", preserve_nulls=False)
    # calls is a struct: access its fields via ["field"] if needed
    return exploded.select(
        col("node_id").alias("src_node_id"),
        col("calls")["callee_symbol"].alias("dst_symbol"),
        col("calls")["evidence"].alias("evidence"),
    )
```

---

## 6) (ROI #5) Small function registry (avoid embedded SQL strings)

You want two registries:

* **Expr ops** (return a DataFusion `Expr`)
* **Agg ops** (return an aggregate `Expr` used inside `aggregate()`)

DataFusion provides:

* `named_struct` ([Apache DataFusion][2])
* `array_agg` ([Apache DataFusion][2])
* `DataFrame.parse_sql_expr` for last-resort fallback when you haven’t wrapped a function yet ([Apache DataFusion][1])

```python
import datafusion.functions as f
from datafusion import col, lit

class FuncRegistry:
    def __init__(self):
        self.expr_ops = {
            "col": lambda df, env, s: col(s["name"]),
            "lit": lambda df, env, s: lit(s["value"]),
            "var": lambda df, env, s: env[s["name"]],

            "named_struct": lambda df, env, s: f.named_struct([
                (name, compile_expr(df, env, sub)) for name, sub in s["fields"]
            ]),

            "eq": lambda df, env, s: compile_expr(df, env, s["a"]) == compile_expr(df, env, s["b"]),
            "ge": lambda df, env, s: compile_expr(df, env, s["a"]) >= compile_expr(df, env, s["b"]),
            "le": lambda df, env, s: compile_expr(df, env, s["a"]) <= compile_expr(df, env, s["b"]),
            "and": lambda df, env, s: _and_all([compile_expr(df, env, x) for x in s["args"]]),
            "or":  lambda df, env, s: _or_all([compile_expr(df, env, x) for x in s["args"]]),

            "between_inclusive": lambda df, env, s: (
                (compile_expr(df, env, s["x"]) >= compile_expr(df, env, s["lo"]))
                & (compile_expr(df, env, s["x"]) <= compile_expr(df, env, s["hi"]))
            ),

            "span_contains": lambda df, env, s: span_contains_expr(
                {k: compile_expr(df, env, v) for k, v in s["container"].items()},
                {k: compile_expr(df, env, v) for k, v in s["inner"].items()},
            ),

            # escape hatch (try not to use):
            "sql": lambda df, env, s: df.parse_sql_expr(s["expr"]),  # :contentReference[oaicite:22]{index=22}
        }

        self.agg_ops = {
            "array_agg": lambda df, env, s: f.array_agg(
                compile_expr(df, env, s["expr"]),
                order_by=s.get("order_by"),
                distinct=s.get("distinct", False),
                filter=compile_expr(df, env, s["filter"]) if "filter" in s else None,
            ),
        }

def _and_all(exprs):
    out = exprs[0]
    for e in exprs[1:]:
        out = out & e
    return out

def _or_all(exprs):
    out = exprs[0]
    for e in exprs[1:]:
        out = out | e
    return out


REG = FuncRegistry()

def compile_expr(df, env, spec):
    op = spec["op"]
    return REG.expr_ops[op](df, env, spec)

def compile_agg(df, env, spec):
    op = spec["op"]
    return REG.agg_ops[op](df, env, spec)
```

---

## 7) Putting it all together: compile + validate + register

This pipeline does:

1. load sources
2. namespace columns
3. apply joins (equi joins + join_on range joins)
4. compile vars via rustworkx topo order
5. build grouped subviews (`*_by_node`)
6. join them back
7. project targets
8. validate schema contracts
9. `register_view`

### 7.1 Required DataFusion API pieces

* `register_view` registers a DataFrame as a view. ([Apache DataFusion][5])
* `with_columns` lets you add many derived cols at once (Exprs or SQL strings). ([Apache DataFusion][1])
* `transform` is useful to chain “pipeline stages” cleanly. ([Apache DataFusion][1])

### 7.2 Compiler implementation

```python
from datafusion import SessionContext

def namespace_df(df, prefix: str):
    # rename all columns -> prefix__col
    names = [f.name for f in df.schema()]  # schema() exists :contentReference[oaicite:26]{index=26}
    return df.select(*[col(n).alias(f"{prefix}__{n}") for n in names])

def load_sources(ctx: SessionContext, sources_spec):
    out = {}
    for alias, table_name in sources_spec.items():
        out[alias] = namespace_df(ctx.table(table_name), alias)
    return out

def apply_joins(sources, joins_spec):
    df = sources["ast"]
    for j in joins_spec:
        right = sources[j["right"]]
        how = j.get("how", "inner")

        if j["type"] == "equi":
            left_on, right_on = zip(*j["on"])
            df = df.join(right, how=how, left_on=list(left_on), right_on=list(right_on))

        elif j["type"] == "range_join":
            preds = [compile_expr(df, {}, p) for p in j["predicates"]]
            df = df.join_on(right, *preds, how=how)  # join_on supports inequality :contentReference[oaicite:27]{index=27}
        else:
            raise ValueError(f"unknown join type: {j['type']}")
    return df

def compile_vars(df, vars_spec):
    order = topo_order_vars(vars_spec)  # rustworkx topo sort :contentReference[oaicite:28]{index=28}
    env = {}
    for name in order:
        env[name] = compile_expr(df, env, vars_spec[name])
    return env

def build_group(df, env, group_spec):
    if "pre_filter" in group_spec:
        pred = compile_expr(df, env, group_spec["pre_filter"])
        df = df.filter(pred)  # filter supports Expr or SQL string :contentReference[oaicite:29]{index=29} (filter is shown nearby)

    group_by = group_spec["group_by"]
    aggs = []
    for out_col, agg_spec in group_spec["aggs"].items():
        agg_expr = compile_agg(df, env, agg_spec).alias(out_col)
        aggs.append(agg_expr)

    return df.aggregate(group_by=group_by, aggs=aggs)  # aggregate() :contentReference[oaicite:30]{index=30}

def project_targets(df, env, targets_spec):
    exprs = []
    for out_name, e_spec in targets_spec.items():
        exprs.append(compile_expr(df, env, e_spec).alias(out_name))
    return df.select(*exprs)

def compile_and_register(ctx: SessionContext, spec: dict, expected_schema=None):
    sources = load_sources(ctx, spec["sources"])
    df = apply_joins(sources, spec["joins"])

    # optional: input coercions for stability
    if spec.get("coerce_inputs"):
        df = df.cast(spec["coerce_inputs"])  # cast() :contentReference[oaicite:31]{index=31}

    env = compile_vars(df, spec["vars"])

    # two-phase aggregates
    grouped = {}
    for g in spec.get("groups", []):
        grouped_df = build_group(df, env, g)
        grouped[g["name"]] = grouped_df
        ctx.register_view(g["name"], grouped_df)  # you can reuse these later :contentReference[oaicite:32]{index=32}

    # join back (by node_id)
    base_nodes = sources["ast"].select("ast__node_id", "ast__kind").distinct()
    out = base_nodes
    if "v_bc_by_node" in grouped:
        out = out.join(grouped["v_bc_by_node"], how="left",
                       left_on="ast__node_id", right_on="ast__node_id")
    if "v_scip_by_node" in grouped:
        out = out.join(grouped["v_scip_by_node"], how="left",
                       left_on="ast__node_id", right_on="ast__node_id")
    if "v_calls_by_node" in grouped:
        out = out.join(grouped["v_calls_by_node"], how="left",
                       left_on="ast__node_id", right_on="ast__node_id")

    # now rebuild derived vars on the join-back df if needed
    # (for the minimal demo, we project loc/layer_ids/sym from the big joined df instead;
    #  in real usage you typically compute them on the node base with targeted joins)
    out_full = project_targets(df, env, spec["targets"])

    if expected_schema is not None:
        out_full = assert_schema(out_full, expected_schema, strict=True)

    ctx.register_view(spec["name"], out_full)  # register_view :contentReference[oaicite:33]{index=33}
    return out_full
```

---

## 8) One concrete schema contract for `v_cpg_node_facts`

Here’s a minimal expected schema (nested structs + list-of-structs). You’d pass this to `compile_and_register(..., expected_schema=...)`.

```python
EXPECTED_NODE_FACTS = pa.schema([
    pa.field("node_id", pa.int64(), nullable=False),
    pa.field("kind", pa.string(), nullable=False),

    pa.field("loc", pa.struct([
        pa.field("file", pa.string(), nullable=False),
        pa.field("start", pa.struct([pa.field("line", pa.int32()), pa.field("col", pa.int32())])),
        pa.field("end",   pa.struct([pa.field("line", pa.int32()), pa.field("col", pa.int32())])),
    ])),

    pa.field("layer_ids", pa.struct([
        pa.field("ast", pa.int64()),
        pa.field("cst", pa.int64()),
    ])),

    pa.field("sym", pa.struct([
        pa.field("qualified_name", pa.string()),
        pa.field("flags", pa.int64()),
        pa.field("is_local", pa.bool_()),
    ])),

    pa.field("bytecode", pa.list_(pa.struct([
        pa.field("offset", pa.int32()),
        pa.field("opname", pa.string()),
        pa.field("arg", pa.string()),
    ]))),

    pa.field("scip", pa.list_(pa.struct([
        pa.field("symbol", pa.string()),
        pa.field("role", pa.string()),
        pa.field("range", pa.struct([
            pa.field("start", pa.struct([pa.field("line", pa.int32()), pa.field("col", pa.int32())])),
            pa.field("end",   pa.struct([pa.field("line", pa.int32()), pa.field("col", pa.int32())])),
        ])),
    ]))),

    pa.field("calls", pa.list_(pa.struct([
        pa.field("callee_symbol", pa.string()),
        pa.field("evidence", pa.string()),
    ]))),
])
```

---

## Notes on correctness + performance (still within the “minimal example” intent)

* `join_on` is the correct hammer for inequality joins (range containment / line interval overlap). It’s explicitly documented for that purpose. ([Apache DataFusion][1])
* For real-scale corpora, you’ll eventually want **bucketing** (e.g., file + line block) so range joins don’t behave like giant band joins. But the spec/compiler structure above is exactly where that optimization lives (you add a “bucket key” var + equijoin + post-filter containment).

---

If you want, paste just **your real column names** for the five tables (even if types differ), and I’ll mechanically adapt this into a **drop-in spec + compiler** that emits:

* `v_cpg_node_facts`
* `v_cpg_call_edges` (via `unnest_columns`) ([Apache DataFusion][1])
* `v_cpg_symbol_facts` (symtable + scip canonical symbol resolution) using the same var-DAG + registry patterns

[1]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[2]: https://datafusion.apache.org/python/autoapi/datafusion/functions/index.html "datafusion.functions — Apache Arrow DataFusion  documentation"
[3]: https://www.rustworkx.org/apiref/rustworkx.topological_sort.html "rustworkx.topological_sort - rustworkx 0.17.1"
[4]: https://www.rustworkx.org/apiref/rustworkx.TopologicalSorter.html "TopologicalSorter - rustworkx 0.17.1"
[5]: https://datafusion.apache.org/python/user-guide/common-operations/views.html?utm_source=chatgpt.com "Registering Views — Apache Arrow DataFusion documentation"
