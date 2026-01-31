Below is a Rust-centric, **LogicalPlan-first** deep dive aimed at a programming LLM who already “thinks in Python,” but needs the **exact DataFusion Rust primitives** for building plans programmatically (no SQL text). I’ll assume modern DataFusion (the current docs.rs line is `datafusion 52.x`). ([Docs.rs][1])

---

## 0) Mental model: what you’re constructing

### `LogicalPlan` is a typed relational operator DAG

A `LogicalPlan` is an enum whose variants are relational operators (e.g. `TableScan`, `Filter`, `Projection`, `Aggregate`, `Join`, …) that transform an input relation into an output relation, with an associated schema. Logical plans can be created via SQL, the DataFrame API, or **programmatically**. ([Docs.rs][1])

### `Expr` is the expression tree evaluated by plan nodes

Filters, projections, aggregate arguments, join conditions, etc. are represented as `Expr` trees such as `col("a") + lit(1)`, `col("x").gt(lit(5))`, function calls, casts, CASE, IN, subqueries, and so on. DataFusion explicitly recommends building `Expr` via fluent helpers like `col` and `lit`. ([Docs.rs][2])

### Arrow Schema vs DataFusion DFSchema

* Arrow `Schema`: column names + Arrow `DataType`.
* DataFusion `DFSchema`: Arrow schema **plus qualifiers** (catalog/schema/table) and other relational metadata. This matters for joins and ambiguous column names. ([Apache DataFusion][3])

---

## 1) The two “correct” ways to build a `LogicalPlan` in Rust

### Path A (idiomatic): `LogicalPlanBuilder` (or DataFrame API that delegates to it)

DataFusion docs: creating plans by instantiating `LogicalPlan` variants directly is possible, but **much easier** with `LogicalPlanBuilder`. ([Apache DataFusion][4])

Builder gives you a fluent “query AST” style that is extremely close to a Pythonic chain (`df.filter(...).select(...)...`) but produces a `LogicalPlan`. ([Docs.rs][5])

### Path B (low-level): construct enum variants directly (useful for custom frontends)

The DataFusion “Building Logical Plans” doc shows manual construction of `LogicalPlan::TableScan` then wrapping it in `LogicalPlan::Filter`, etc. This is primarily useful if you’re writing your own planner / frontend. ([Apache DataFusion][6])

You almost always start with Path A, then drop to Path B only when:

* you need a custom `LogicalPlan::Extension` node
* you are building a query language compiler and need exact node control.

---

## 2) Imports and where types live (what a “Rust LLM” needs to know)

You will see the same handful of modules repeatedly:

* `datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, Expr, JoinType, ...}`
* `datafusion::logical_expr::expr_fn::{col, lit, ...}` and/or other function namespaces
* `datafusion::arrow::datatypes::{Schema, DataType, Field, ...}`
* `datafusion_common::{DFSchema, DFSchemaRef, TableReference, ...}`

The “expr_fn” helpers are explicitly the preferred way to create expressions like columns/literals and various function-ish nodes. ([Docs.rs][7])

---

## 3) Leaves: what you can build plans *from*

You need a **leaf** logical plan (a relation) before you can apply relational transforms.

### 3.1 `table_scan(...)` helpers (schema-only; mostly for tests/docs)

DataFusion exposes helpers like:

* `table_scan(name, schema, projection)`
* `table_scan_with_filters(name, schema, projection, filters)`
* `table_scan_with_filter_and_fetch(...)`

These are “mostly used for testing and documentation” because they don’t connect to a real data source, just a schema + name. ([Docs.rs][8])

**This is the fastest way to prototype a planner** because you can generate plans without any `TableProvider` plumbing.

### 3.2 Real scans: `TableProvider` → `TableSource` → scan

In “real” DataFusion, leaves correspond to `TableProvider`s (Parquet, CSV, Delta, custom connector, etc.). Logical planning uses a `TableSource` abstraction so the logical layer doesn’t depend on physical providers. DataFusion provides:

* `DefaultTableSource` (adapts a `TableProvider` to a `TableSource`) ([Docs.rs][9])
* `provider_as_source(table_provider)` helper ([Docs.rs][10])

This is the bridge you want when building plans programmatically *and* tying them to real data.

---

## 4) Expressions (`Expr`) in Rust: the essential construction vocabulary

### 4.1 Core primitives

* `col("name")` → `Expr::Column(...)` (unresolved/qualified as needed; normalized per SQL identifier rules) ([Docs.rs][7])
* `lit(42)` → literal `Expr` ([Docs.rs][11])

### 4.2 Fluent binary ops and predicate builders

DataFusion’s `Expr` supports ergonomic building (methods / ops) such as:

* `col("a").lt(lit(1000))`
* `col("id").gt(lit(500))`
  The docs emphasize “fluent APIs” (`col`, `lit`, and methods like `alias`, `cast_to`, etc.). ([Docs.rs][2])

### 4.3 Aggregates and window functions: `ExprFunctionExt`

Aggregate/window expressions carry optional `ORDER BY`, `FILTER`, `DISTINCT`, and `NULL TREATMENT`. DataFusion provides `ExprFunctionExt` to configure these options in a builder style, e.g. `count(col("x")).filter(...).build()?`. ([Docs.rs][12])

### 4.4 Nested / complex outputs (structs, lists, etc.)

For nested outputs you’ll commonly produce expressions that return nested Arrow types (struct/list/map). DataFusion exposes nested function packages and functions such as `named_struct(args: Vec<Expr>) -> Expr`. ([Docs.rs][13])

*(Mechanically: these are just scalar function expressions that happen to return nested Arrow types.)* ([Docs.rs][2])

---

## 5) `LogicalPlanBuilder`: the “compiler frontend” API

### 5.1 Minimal example: scan → filter → project → build

This is straight from the builder docs:

```rust
use datafusion::logical_expr::{col, lit, table_scan, LogicalPlanBuilder};

fn plan_employee(schema: &datafusion::arrow::datatypes::Schema) -> datafusion::error::Result<datafusion::logical_expr::LogicalPlan> {
    let plan = table_scan(Some("employee"), schema, None)?
        .filter(col("salary").lt(lit(1000)))?
        .project(vec![col("last_name")])?
        .build()?;
    Ok(plan)
}
```

The docs show this exact pattern and explicitly show you can go back from `LogicalPlan` to a builder via `LogicalPlanBuilder::from(plan)`. ([Docs.rs][5])

### 5.2 Starting builders (leaf constructors)

The “Building Logical Plans” guide calls out typical constructors like:

* `empty` (no fields)
* `values` (literal tuples)
* `scan` (table scan) ([Apache DataFusion][4])

`values` is particularly useful if you’re building a query language and need “inline relations” (like SQL `VALUES`) with inferred or provided schema. ([Docs.rs][5])

### 5.3 Common transforms (the important ones)

The official guide calls out transforms such as `filter`, `limit`, `sort`, `distinct`, `join`, etc. ([Apache DataFusion][4])

The builder module also includes useful utilities such as:

* `union` and `union_by_name` (combine relations by position vs by column name) ([Docs.rs][14])
* `subquery_alias`, `validate_unique_names`, `requalify_sides_if_needed`, `unnest` / `unnest_with_options` ([Docs.rs][14])

---

## 6) A “realistic” end-to-end programmatic plan (join + aggregate + window + nested projection)

This section is what you usually want when building view-like plans from specs.

### 6.1 Join (equijoin keys vs expression `join_on`)

`LogicalPlanBuilder` supports:

* `join(...)` with explicit left/right key columns
* `join_on(...)` with arbitrary join expressions

Builder docs note DataFusion can identify and optimize equality predicates; `join_on` is often more concise, and there’s no perf difference from using `join` when the predicates are equijoins. ([Docs.rs][5])

```rust
use datafusion::logical_expr::{col, lit, Expr, JoinType, LogicalPlan, LogicalPlanBuilder};

fn join_plan(left: LogicalPlan, right: LogicalPlan) -> datafusion::error::Result<LogicalPlan> {
    let plan = LogicalPlanBuilder::from(left)
        .join(
            right,
            JoinType::Inner,
            (vec!["user_id"], vec!["id"]), // equijoin keys by name
            None,                          // optional additional filter Expr
        )?
        .build()?;
    Ok(plan)
}
```

### 6.2 Aggregate: schema ordering rule matters

The `Aggregate` operator’s output schema is **group expressions first**, then aggregate expressions. If you’re generating plans from specs, you must preserve this ordering expectation. ([Docs.rs][15])

```rust
use datafusion::logical_expr::{col, sum};

let plan = LogicalPlanBuilder::from(input)
    .aggregate(
        vec![col("country")],                          // group exprs
        vec![sum(col("revenue")).alias("rev_sum")],    // aggs
    )?
    .build()?;
```

*(The `sum` helper exists in the expression function ecosystem; your exact import path depends on whether you use `datafusion::logical_expr` re-exports or `datafusion_expr` directly.)* ([Docs.rs][7])

### 6.3 Window: output schema rule matters (input columns + window exprs)

The `Window` operator’s output schema is **input schema followed by window expressions**, and the `PARTITION BY` expressions do **not** appear as output columns. ([Docs.rs][16])

### 6.4 Nested output: `named_struct(...)`

To produce a nested column in your projection, build a nested expression:

```rust
use datafusion::logical_expr::{col, Expr};
use datafusion::functions::expr_fn::named_struct;

let nested: Expr = named_struct(vec![
    lit("id"), col("id"),
    lit("score"), col("score"),
]);
```

`named_struct` is explicitly a function that returns a struct from name/argument pairs. ([Docs.rs][13])

### 6.5 Putting it together: a “view plan builder” function

This is the pattern you want for programmatic view creation (spec → plan):

```rust
use datafusion::error::Result;
use datafusion::logical_expr::{col, lit, Expr, JoinType, LogicalPlan, LogicalPlanBuilder};
use datafusion::functions::expr_fn::named_struct;

pub fn build_view_like_plan(
    users: LogicalPlan,
    events: LogicalPlan,
) -> Result<LogicalPlan> {
    // JOIN users u ON u.id = e.user_id
    let joined = LogicalPlanBuilder::from(events)
        .join(users, JoinType::Inner, (vec!["user_id"], vec!["id"]), None)?
        .build()?;

    // WHERE event_type = 'purchase'
    let filtered = LogicalPlanBuilder::from(joined)
        .filter(col("event_type").eq(lit("purchase")))?;
    
    // SELECT user_id, named_struct('ts', ts, 'amount', amount) AS payload
    let payload: Expr = named_struct(vec![
        lit("ts"), col("ts"),
        lit("amount"), col("amount"),
    ]).alias("payload");

    let projected = filtered
        .project(vec![col("user_id"), payload])?
        .build()?;

    Ok(projected)
}
```

This is deliberately “pure plan building.” There’s no `SessionContext`, no SQL, no execution.

---

## 7) Schema computation, name/qualifier issues, and why your generated plans fail

### 7.1 Projection schema is computed from `Expr::to_field` (metadata matters)

When you project expressions, DataFusion computes output schema from expressions (and handles schema/field metadata). The helper `projection_schema(input, exprs)` documents this explicitly and is worth calling when validating generated plans. ([Docs.rs][17])

### 7.2 Column name uniqueness and qualification

When you join, you can easily produce ambiguous/unqualified references. The builder utilities include `requalify_sides_if_needed` and `validate_unique_names` to help keep schemas valid, especially for plans originating from systems without alias support (Substrait note in docs). ([Docs.rs][14])

### 7.3 DFSchema exists for a reason

If you’re writing a planner, you eventually need to reason in `DFSchema` rather than raw Arrow `Schema`, because `DFSchema` carries qualifiers that disambiguate columns across multiple inputs. ([Apache DataFusion][3])

---

## 8) From LogicalPlan to “something you can run”: `SessionState` and optimizer pipeline

Even if you build the plan yourself, DataFusion’s execution pipeline is:

**LogicalPlan → Analyzer rules → Optimizer rules → Physical plan → Execution** ([Docs.rs][18])

Key APIs:

* `SessionState::optimize(&plan) -> LogicalPlan` applies optimizer rules ([Docs.rs][19])
* `SessionState::create_physical_plan(&logical_plan)` creates an `ExecutionPlan` and **first calls optimize** ([Docs.rs][19])
* `SessionState::create_logical_plan(sql)` exists when you do want SQL planning (feature `"sql"`) ([Docs.rs][19])

This is important for “compiler frontends”: your output is a LogicalPlan, and then DataFusion’s optimizer/physical planner does the rest.

---

## 9) Rewriting / inspecting plans programmatically (critical for “spec compilers”)

### 9.1 `TreeNode` API for plans and expressions

DataFusion provides a unified `TreeNode` API for inspecting and rewriting plan and expression trees. ([Docs.rs][20])

### 9.2 LogicalPlan tree_node module: visit + rewrite

The `datafusion_expr::logical_plan::tree_node` docs enumerate the important traversal / rewrite entry points:

* Visiting: `LogicalPlan::visit`, `visit_with_subqueries`, `apply_expressions`, …
* Rewriting: `LogicalPlan::rewrite`, `map_children`, `map_expressions`, `rewrite_with_subqueries`, …
* Recreation helpers: `with_new_exprs`, `expressions` (these can clone a lot; slower) ([Docs.rs][21])

If you’re generating plans from a DSL, you almost always need:

* **validator passes** (walk plan; check invariants)
* **normalization passes** (rewrite expressions / add aliases)
* **pushdown passes** (custom optimizations in your domain)

---

## 10) Extensibility: when LogicalPlanBuilder is not enough

### 10.1 Custom logical operators: `LogicalPlan::Extension` + `UserDefinedLogicalNodeCore`

DataFusion’s `LogicalPlan` includes an `Extension` variant “to add custom logical operators.” ([Apache DataFusion][4])

To implement an extension node, you implement `UserDefinedLogicalNodeCore` (recommended) / `UserDefinedLogicalNode`. The trait includes important hooks used by optimization such as projection and limit pushdown capabilities. ([Docs.rs][22])

DataFusion’s own extension example (Ballista docs) walks through implementing a custom logical plan node using `UserDefinedLogicalNodeCore`. ([Apache DataFusion][23])

### 10.2 Custom UDFs (so your planner can emit function calls)

When your plan generator needs a domain function, you can add UDFs:

* Scalar UDF: implement `ScalarUDFImpl` (advanced API) ([Docs.rs][24])
* Aggregate UDF: implement `AggregateUDFImpl` ([Docs.rs][25])
* Window UDF: implement `WindowUDF` / UDWF APIs ([Docs.rs][26])

Then your DSL compiler can produce `Expr::ScalarFunction` / `Expr::AggregateFunction` / `Expr::WindowFunction` nodes referencing those UDFs. ([Docs.rs][27])

---

## 11) “Cheat sheet” for a programmatic planner implementation

### Minimal “query compiler” shape

1. **Resolve inputs** → create leaf scans

   * for prototyping: `table_scan(schema-only)` ([Docs.rs][8])
   * for real execution: `TableProvider` → `provider_as_source` → scan ([Docs.rs][10])

2. **Compile expressions**

   * columns/literals via `col`, `lit` ([Docs.rs][2])
   * function config via `ExprFunctionExt` ([Docs.rs][12])

3. **Assemble plan with `LogicalPlanBuilder`**

   * `filter`, `project`, `join`, `aggregate`, `sort`, `limit`, `distinct`, `union_by_name`, `unnest`… ([Apache DataFusion][4])

4. **Validate schema / naming**

   * use `projection_schema` mental model; enforce unique names/qualifiers ([Docs.rs][17])

5. **(Optional) normalize / rewrite**

   * `LogicalPlan::rewrite` / `map_expressions` (TreeNode APIs) ([Docs.rs][21])

6. **Hand off**

   * optimize / create physical plan via `SessionState` when executing ([Docs.rs][19])

---

## What I would generate next if you want it “LLM-agent ready”

If you tell me *one concrete view spec shape* you care about (e.g., “select columns + computed fields + joins + groupbys + nested struct outputs”), I can produce:

* a **typed `ViewSpec` Rust struct** (serde-ready) → `compile(spec) -> LogicalPlan`
* a **deterministic naming + aliasing policy** (so joins never collide)
* a **schema-contract validator** using `projection_schema` + `DFSchema` checks
* a **rewrite pass** (TreeNode) that normalizes expressions (casts, aliases, pushdowns)

All using the exact APIs above, with code that’s meant to be dropped into your repo.

[1]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html?utm_source=chatgpt.com "LogicalPlan in datafusion::logical_expr - Rust"
[2]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html?utm_source=chatgpt.com "Expr in datafusion::logical_expr - Rust"
[3]: https://datafusion.apache.org/library-user-guide/working-with-exprs.html?utm_source=chatgpt.com "Working with Expr s - Apache DataFusion"
[4]: https://datafusion.apache.org/library-user-guide/building-logical-plans.html?utm_source=chatgpt.com "Building Logical Plans — Apache DataFusion documentation"
[5]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.LogicalPlanBuilder.html?utm_source=chatgpt.com "LogicalPlanBuilder in datafusion::logical_expr - Rust"
[6]: https://datafusion.apache.org/_sources/library-user-guide/building-logical-plans.md.txt?utm_source=chatgpt.com "building-logical-plans.md.txt - Apache DataFusion"
[7]: https://docs.rs/datafusion/latest/datafusion/logical_expr/expr_fn/index.html?utm_source=chatgpt.com "datafusion::logical_expr::expr_fn - Rust"
[8]: https://docs.rs/datafusion/latest/datafusion/logical_expr/fn.table_scan.html?utm_source=chatgpt.com "table_scan in datafusion::logical_expr - Rust"
[9]: https://docs.rs/datafusion/latest/datafusion/datasource/default_table_source/struct.DefaultTableSource.html?utm_source=chatgpt.com "DefaultTableSource in datafusion::datasource"
[10]: https://docs.rs/datafusion/latest/datafusion/datasource/default_table_source/fn.provider_as_source.html?utm_source=chatgpt.com "provider_as_source in datafusion::datasource"
[11]: https://docs.rs/datafusion/latest/datafusion/logical_expr/fn.lit.html?utm_source=chatgpt.com "lit in datafusion::logical_expr - Rust"
[12]: https://docs.rs/datafusion-expr/latest/datafusion_expr/expr_fn/trait.ExprFunctionExt.html?utm_source=chatgpt.com "ExprFunctionExt in datafusion_expr::expr_fn - Rust"
[13]: https://docs.rs/datafusion/latest/datafusion/functions/expr_fn/fn.named_struct.html?utm_source=chatgpt.com "named_struct in datafusion::functions::expr_fn - Rust"
[14]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/builder/index.html?utm_source=chatgpt.com "datafusion_expr::logical_plan::builder - Rust"
[15]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Aggregate.html?utm_source=chatgpt.com "Aggregate in datafusion::logical_expr - Rust"
[16]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Window.html?utm_source=chatgpt.com "Window in datafusion::logical_expr - Rust"
[17]: https://docs.rs/datafusion/latest/datafusion/logical_expr/fn.projection_schema.html?utm_source=chatgpt.com "projection_schema in datafusion::logical_expr - Rust"
[18]: https://docs.rs/datafusion/latest/datafusion/?utm_source=chatgpt.com "datafusion - Rust"
[19]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html "SessionState in datafusion::execution::session_state - Rust"
[20]: https://docs.rs/datafusion/latest/datafusion/common/tree_node/trait.TreeNode.html?utm_source=chatgpt.com "TreeNode in datafusion::common::tree_node - Rust"
[21]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/tree_node/index.html?utm_source=chatgpt.com "datafusion_expr::logical_plan::tree_node - Rust"
[22]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.UserDefinedLogicalNodeCore.html?utm_source=chatgpt.com "UserDefinedLogicalNodeCore in datafusion::logical_expr"
[23]: https://datafusion.apache.org/ballista/user-guide/extensions-example.html?utm_source=chatgpt.com "Extensions Example — Apache DataFusion Ballista documentation"
[24]: https://docs.rs/datafusion-expr/latest/datafusion_expr/trait.ScalarUDFImpl.html?utm_source=chatgpt.com "ScalarUDFImpl in datafusion_expr - Rust"
[25]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.AggregateUDFImpl.html?utm_source=chatgpt.com "AggregateUDFImpl in datafusion::logical_expr - Rust"
[26]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.WindowUDF.html?utm_source=chatgpt.com "WindowUDF in datafusion::logical_expr - Rust"
[27]: https://docs.rs/datafusion/latest/datafusion/prelude/enum.Expr.html?utm_source=chatgpt.com "Expr in datafusion::prelude - Rust"

Below is a **gap analysis** of my prior write-up: key capabilities / APIs / integration points around **programmatic `LogicalPlan` construction in Rust** that I didn’t cover (or only touched lightly). I’ll keep this **LLM-agent-dense** and oriented around “what else do I need to know to build serious planners, not just toy plans,” using current docs for Apache DataFusion. ([Docs.rs][1])

---

## 1) You need the *full* LogicalPlan surface area (not just Filter/Project/Join/Agg/Window)

In practice, “view / query compilation” will eventually hit plan variants beyond the basics—especially if you want parity with SQL features or need to model system operations.

The `LogicalPlan` enum includes (among others): `Sort`, `Limit`, `Repartition`, `Union`, `TableScan`, `EmptyRelation`, `Subquery`, `SubqueryAlias`, `Values`, `Explain`, `Analyze`, `Extension`, `Distinct`, **`Dml`**, **`Ddl`**, `Copy`, `DescribeTable`, `Unnest`, **`RecursiveQuery`**, and `Statement`. ([Docs.rs][1])

**Why this matters for a programmatic planner**

* If you want to compile a DSL that supports “export results,” you’ll want `Copy`. ([Docs.rs][1])
* If you want to compile “CREATE VIEW/CREATE TABLE/INSERT,” you’ll produce `Ddl`/`Dml` plans (or run SQL planning which yields them). ([Docs.rs][1])
* If you want to “freeze a plan” at a particular parallelism boundary, you may need `Repartition` (logical “exchange”) explicitly. ([Docs.rs][1])
* If you want to represent recursive CTEs or graph-style recursion, you need `RecursiveQuery`. ([Docs.rs][1])

**Actionable addition:** your internal IR/spec should model these “non-relational-pure” operations (copy/ddl/dml/explain) as first-class nodes, not as afterthoughts.

---

## 2) `LogicalPlanBuilder` has critical features you likely want in DSL compilers

I previously focused on the “classic chain.” The builder has additional methods that matter a lot in programmatic construction:

### 2.1 `VALUES` as a first-class relation (+ schema control + binder types)

`LogicalPlanBuilder::values(...)` builds a relation from literal rows; `values_with_schema(...)` lets you provide a `DFSchema`. ([Docs.rs][2])

It also explicitly notes: if your values include **params/binders like `$1, $2, ...`**, you should supply `param_data_types`. ([Docs.rs][2])

**Why it matters:** DSLs often need to inject parameter tables (run-id tables, filtering ID lists, small dimension tables) without registering a physical table.

### 2.2 Recursive queries: `to_recursive_query(...)`

`LogicalPlanBuilder::to_recursive_query(name, recursive_term, is_distinct)` converts a plan into a recursive query (“recursive CTE”), choosing `UNION` vs `UNION ALL` semantics via `is_distinct`. ([Docs.rs][2])

**Why it matters:** if you’re doing reachability, dependency closure, hierarchy expansion, etc., you can compile to `RecursiveQuery` rather than doing external iterative orchestration.

---

## 3) You must think in *phases*: Unresolved plan → Analyzer → Optimizer → Physical planning

When you “build a LogicalPlan manually,” it can still be *un-analyzed* (e.g., types not coerced, subqueries not fully resolved, function rewrites not applied). DataFusion splits “make valid” vs “make fast.”

### 3.1 Analyzer rules are distinct from optimizer rules

`AnalyzerRule`s transform plans “to make the plan valid prior to the rest of the optimization process,” including resolving expressions (e.g. subquery references) and type coercion. ([Docs.rs][3])

A concrete built-in analyzer rule is `TypeCoercion`, which “performs type coercion by determining the schema and performing expression rewrites.” ([Docs.rs][4])

### 3.2 You can *add analyzer rules* and *optimizer rules* programmatically

`SessionContext` exposes `add_analyzer_rule(...)` and `add_optimizer_rule(...)` (and `remove_optimizer_rule(...)`). ([Docs.rs][5])

This is not academic: systems doing federation often intercept plans early with custom analyzer rules (before DataFusion’s default rules) to ensure generated plans are valid for their multi-engine execution model. ([Spice AI][6])

**Practical implication for DSL compilers**

* Treat your output as “initial plan.”
* Run analysis/optimization under a configured `SessionState` before serializing/executing or caching. ([Docs.rs][7])

---

## 4) Subqueries are a real feature set (and you should build them explicitly)

I previously mentioned “subqueries exist,” but didn’t give the Rust building primitives.

### 4.1 Subquery plan nodes + Expr constructors

`LogicalPlan` includes `Subquery` and subqueries are supported in SQL (including correlated subqueries). ([Docs.rs][1])

Rust constructors exist for subquery expressions:

* `scalar_subquery(Arc<LogicalPlan>) -> Expr` ([Docs.rs][8])
* `expr_fn::exists(Arc<LogicalPlan>) -> Expr` ([Docs.rs][9])

SQL docs note: `EXISTS` / `NOT EXISTS` and `IN` / `NOT IN` are subquery operators, and they call out correlated subquery behavior. ([Apache DataFusion][10])

### 4.2 “Plan construction” gotcha: logical support ≠ physical support (historically)

There have been versions/eras where certain non-correlated subqueries were not physically executable (“not implemented” class errors). This impacts DSL targets: you might need to rewrite subqueries to joins (dependent joins / semi-joins) if you need stable execution across versions. ([GitHub][11])

**Actionable addition:** if your DSL emits `EXISTS/IN`, include a rewrite pass option: `Subquery -> (semi|anti) join` when required.

---

## 5) DDL/DML/COPY as LogicalPlans (and how to *control* them)

This is big if you want “view creation / table materialization” to be programmatic and safe.

### 5.1 `SessionContext::sql(...)` will execute DDL/DML with in-memory defaults

Docs explicitly warn: `sql()` “implements DDL statements such as `CREATE TABLE` and `CREATE VIEW` and DML such as `INSERT INTO` with in-memory default implementations.” ([Docs.rs][5])

### 5.2 Use `sql_with_options` to *forbid* mutation

`sql_with_options(..., SQLOptions)` can disallow DDL (`with_allow_ddl(false)`) and errors if a plan tries to create a table. ([Docs.rs][5])

### 5.3 Execute a plan directly

If you’re building plans programmatically (not from SQL), you can still run them via `execute_logical_plan(plan)` returning a `DataFrame`. ([Docs.rs][5])

**Why it matters for a “programmatic view layer”**

* You can gate what kinds of plans can run (pure SELECT vs mutating) using options verification. ([Docs.rs][5])
* Your “view registry” can store *plan objects* and decide whether to allow execution in a given context.

---

## 6) Cost-based optimization depends on statistics; your TableSource implementation matters

I didn’t cover the “why is my plan slow / why did optimizer not do X” side.

DataFusion states its optimization uses a cost-based model relying on table/column statistics for selectivity estimates (affecting join/filter/projection cost decisions). ([Apache DataFusion][12])

There’s explicit discussion that some optimizer work (e.g., join reordering) wants access to table sizes/statistics via `TableScan` / `TableSource.statistics`. ([GitHub][13])

Config docs also mention behaviors that only work when DataFusion has accurate statistics (example: repartitioning heuristics for certain providers). ([Apache DataFusion][14])

**Actionable addition for DSL builders**

* If you’re wrapping data sources (custom `TableProvider` / `TableSource`), invest in correct stats; it directly impacts the quality of optimized plans. ([Apache DataFusion][12])

---

## 7) Plan/Expr serialization is a first-class integration point (proto vs Substrait)

I mentioned Substrait briefly but didn’t cover the “operational” choice of serialization formats.

### 7.1 DataFusion-native protobuf serialization: `datafusion_proto`

`datafusion_proto` can serialize/deserialize **LogicalPlans (including Expr)** and ExecutionPlans to bytes (via protobuf). ([Docs.rs][15])
It also warns: serialized forms are **not guaranteed compatible across DataFusion versions**. ([Docs.rs][15])

### 7.2 Portable cross-engine format: `datafusion-substrait` / Substrait

`datafusion-substrait` converts a DataFusion logical plan into a Substrait protobuf and back. ([Docs.rs][16])

**Practical design choice**

* If you need *exact fidelity* for all DataFusion plan features: protobuf is “full range but DataFusion specific.” ([Docs.rs][15])
* If you need *interop* (federation/pushdown/remote execution): Substrait can be preferable, accepting that not every plan shape may round-trip perfectly across systems. ([Docs.rs][16])

---

## 8) SQL is not “strings only”: `datafusion_sql` crate gives you parser + planner + unparser

If your “programmatic view system” still needs SQL interchange (logging, pushdown, debugging), the SQL crate matters.

`datafusion_sql` provides:

1. `DFParser` (SQL → AST)
2. `SqlToRel` (AST → `LogicalPlan`)
3. SQL **unparser** (Expr/LogicalPlan → SQL). ([Docs.rs][17])

The unparser module exists specifically for converting `Expr` to SQL text. ([Docs.rs][18])
And in the `datafusion` crate itself there’s `plan_to_sql`, which errors when a plan can’t be converted to SQL. ([Docs.rs][19])

This capability is used in real federated systems: the plan is parsed into a LogicalPlan, captured by a federation analyzer, and converted (via the unparser) into dialect-specific SQL for pushdown. ([Spice AI][6])

**Gap to incorporate:** If you want “programmatic plans” but also want observability and/or remote pushdown, include a `LogicalPlan -> SQL` path for the subset of nodes you support.

---

## 9) Extending SQL and planning is a supported workflow

If your DSL is “SQL-like but not SQL,” you can also extend DataFusion’s SQL parser/planner.

DataFusion documents an extension system to customize SQL parsing and planning (custom operators, types, constructs). ([Apache DataFusion][20])

Separately, `SessionContext` can register a `RelationPlanner` to customize SQL table-factor planning; planners are invoked in reverse registration order. ([Docs.rs][5])

**Why it matters:** you can hybridize:

* emit `LogicalPlan` directly for your DSL
* but still accept SQL fragments or extend SQL for end-user ergonomics.

---

## 10) Execution boundary objects matter for plan construction workflows

I didn’t emphasize the practical boundary:

`SessionContext` (high-level shared state) vs `SessionState` (per-query planning/execution) vs `TaskContext` (execution-only). ([Docs.rs][5])

This becomes important when you:

* want different rule sets per query,
* want to disable DDL in some contexts,
* want to serialize/deserialize plans using APIs requiring `TaskContext` (e.g., `datafusion_proto` examples use `ctx.task_ctx()`). ([Docs.rs][15])

---

## 11) “Plan rewriting performance” is a real concern (TreeNode vs cloning)

If you’re writing optimizer/analyzer rules or heavy rewrite passes for your DSL, you must care about the mechanics.

There’s an explicit concern that some optimizer passes clone entire plans/exprs and that the project has tracked work to avoid copying `LogicalPlan` during optimizer passes. ([GitHub][21])

**Translation for an agent:** If you naively implement rewrite passes by extracting all expressions (`LogicalPlan::expressions()`), rewriting, and reconstructing, you can accidentally create large clone overhead. Track and prefer the newer `TreeNode` rewrite patterns where possible. ([GitHub][22])

---

## 12) Physical optimizer hooks exist (important if you “own the engine”)

Even if your immediate focus is logical, serious systems add physical optimizations.

`PhysicalOptimizerRule` transforms one `ExecutionPlan` into another more efficient plan, and `SessionState::add_physical_optimizer_rule` can register additional physical optimizer rules. ([Docs.rs][23])

This matters if your “view layer” targets very specific IO layouts, runtime constraints, or custom `ExecutionPlan` extension operators.

---

# Concrete “what to add to your mental/implementation model”

If your goal is “programmatic plan construction as infrastructure,” extend your internal toolkit with:

1. **Complete node coverage**: include `Ddl/Dml/Copy/RecursiveQuery/Subquery/Repartition/Unnest/Explain/Analyze`. ([Docs.rs][1])
2. **Phase separation**: initial plan → analyzer rules (validity/type/subquery resolution) → optimizer rules (performance). ([Docs.rs][3])
3. **Rule injection points**: `add_analyzer_rule`, `add_optimizer_rule`, plus cost/stats awareness. ([Docs.rs][5])
4. **Serialization strategy**: `datafusion_proto` for fidelity (version-coupled), Substrait for interop. ([Docs.rs][15])
5. **SQL interchange** (optional but high leverage): `datafusion_sql` parser/planner/unparser + `plan_to_sql` for pushdown / debugging. ([Docs.rs][17])

---

If you want, I can turn this into an **agent-ready “LogicalPlan compiler cookbook”** (Rust) with:

* a `ViewSpec` IR,
* a compilation pipeline (`compile -> analyze -> optimize -> (serialize|register)`),
* a subquery lowering pass (EXISTS/IN → semi/anti joins),
* and “schema contract enforcement” hooks (to make view outputs stable across compiler changes).

[1]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html "LogicalPlan in datafusion_expr::logical_plan - Rust"
[2]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.LogicalPlanBuilder.html "LogicalPlanBuilder in datafusion::logical_expr - Rust"
[3]: https://docs.rs/datafusion/latest/datafusion/optimizer/analyzer/trait.AnalyzerRule.html?utm_source=chatgpt.com "AnalyzerRule in datafusion::optimizer::analyzer - Rust"
[4]: https://docs.rs/datafusion/latest/datafusion/optimizer/analyzer/type_coercion/struct.TypeCoercion.html?utm_source=chatgpt.com "TypeCoercion in datafusion::optimizer - type_coercion"
[5]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html "SessionContext in datafusion::execution::context - Rust"
[6]: https://spice.ai/blog/how-we-use-apache-datafusion-at-spice-ai?utm_source=chatgpt.com "How we use Apache DataFusion at Spice AI"
[7]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html?utm_source=chatgpt.com "SessionState in datafusion::execution::session_state - Rust"
[8]: https://docs.rs/datafusion/latest/datafusion/logical_expr/fn.scalar_subquery.html?utm_source=chatgpt.com "scalar_subquery in datafusion::logical_expr - Rust"
[9]: https://docs.rs/datafusion/latest/datafusion/logical_expr/expr_fn/fn.exists.html?utm_source=chatgpt.com "exists in datafusion::logical_expr::expr_fn - Rust"
[10]: https://datafusion.apache.org/user-guide/sql/subqueries.html?utm_source=chatgpt.com "Subqueries — Apache DataFusion documentation"
[11]: https://github.com/apache/arrow-datafusion/issues/5265?utm_source=chatgpt.com "Some In/Exists Subqueries will generate wrong PhysicalPlan"
[12]: https://datafusion.apache.org/library-user-guide/query-optimizer.html?utm_source=chatgpt.com "Query Optimizer — Apache DataFusion documentation"
[13]: https://github.com/apache/datafusion/issues/4625?utm_source=chatgpt.com "Make table statistics available to logical plan optimizer rules"
[14]: https://datafusion.apache.org/user-guide/configs.html?utm_source=chatgpt.com "Configuration Settings — Apache DataFusion documentation"
[15]: https://docs.rs/datafusion-proto/latest/datafusion_proto/?search= "datafusion_proto - Rust"
[16]: https://docs.rs/crate/datafusion-substrait/34.0.0?utm_source=chatgpt.com "datafusion-substrait 34.0.0"
[17]: https://docs.rs/datafusion-sql?utm_source=chatgpt.com "datafusion_sql - Rust"
[18]: https://docs.rs/datafusion-sql/latest/datafusion_sql/unparser/index.html?utm_source=chatgpt.com "datafusion_sql::unparser - Rust"
[19]: https://docs.rs/datafusion/latest/datafusion/sql/unparser/fn.plan_to_sql.html?utm_source=chatgpt.com "plan_to_sql in datafusion::sql::unparser - Rust"
[20]: https://datafusion.apache.org/library-user-guide/extending-sql.html?utm_source=chatgpt.com "Extending SQL Syntax — Apache DataFusion documentation"
[21]: https://github.com/apache/datafusion/issues/9637?utm_source=chatgpt.com "[EPIC] Stop copying `LogicalPlan` during OptimizerPasses"
[22]: https://github.com/apache/datafusion/issues/8913?utm_source=chatgpt.com "Epic: Unified TreeNode rewrite API · Issue #8913"
[23]: https://docs.rs/datafusion/latest/datafusion/physical_optimizer/trait.PhysicalOptimizerRule.html?utm_source=chatgpt.com "PhysicalOptimizerRule in datafusion::physical_optimizer"
