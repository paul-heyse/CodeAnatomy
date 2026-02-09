According to a document from **January 27, 2026** and the upstream DataFusion documentation, the planning deep dive should treat “planning” as a **deterministic compilation pipeline** producing inspectable plan artifacts (logical + physical) whose deltas explain correctness, I/O shape, and performance behavior. ([Apache DataFusion][1])

---

## 0) Scope, versions, and definitions

### 0.1 Scope boundary: “planning” vs “execution”

**Planning** (compile-time, plan-shape):

* SQL text / DataFrame transformations → **(bound) LogicalPlan** → AnalyzerRule passes → OptimizerRule passes → **ExecutionPlan** generation → PhysicalOptimizerRule passes. ([Docs.rs][2])
* Outputs: unoptimized logical, optimized logical, physical plan trees, plus explain renderings (indent/tree) and (optionally) serialized plan bytes. ([Apache DataFusion][3])

**Execution** (run-time, data + metrics):

* Physical plan evaluation on runtime env (threads, memory pool, disk spill, object store) producing RecordBatches/streams; `EXPLAIN ANALYZE` is *execution*, because it runs the plan and attaches metrics. ([Docs.rs][4])

Operationally: treat logical artifacts as **semantic + rewrite ground truth**; treat physical artifacts as **environment-parameterized execution topology** (partitioning, file groups, shuffles), thus non-portable across hardware/layout.  ([Apache DataFusion][5])

---

### 0.2 Version pinning strategy (engine concepts vs binding surfaces)

Pin **two independent version axes**:

1. **Core engine (Rust crates)**

* Docs.rs currently indexes `datafusion` **52.1.0**; the ASF dist directory contains `apache-datafusion-52.1.0` release artifacts. ([Docs.rs][6])

2. **Python bindings (`pip install datafusion`)**

* PyPI currently lists `datafusion` **51.0.0**, released **Jan 9, 2026**. ([PyPI][7])

**Doc structure implication**: maintain a strict separation:

* **Concept chapters**: planner stages, plan IR invariants, rewrite semantics, explain formats (largely stable across minor versions).
* **API chapters**: exact method names and plan object capabilities for:

  * Rust (`datafusion::prelude`, `SessionContext`, `SessionState`, optimizer traits)
  * Python (`datafusion.SessionContext`, `datafusion.DataFrame`, `datafusion.plan.LogicalPlan/ExecutionPlan`) ([Apache DataFusion][8])

**Rust-only vs Python-exposed** (planning-relevant deltas):

* Rust exposes **planner mutation hooks**: `SessionContext.add_analyzer_rule`, `SessionContext.add_optimizer_rule`, `SessionContext.remove_optimizer_rule`; plus physical-rule injection via `SessionState::add_physical_optimizer_rule`. ([Docs.rs][9])
* Python exposes **plan extraction + rendering + best-effort serialization** (`to_proto/from_proto`, `to_variant`, `inputs/children`, `partition_count`) but does not generally expose arbitrary optimizer-rule injection as a first-class Python API surface. ([Apache DataFusion][3])

---

### 0.3 Glossary (planning objects; minimal but semantically precise)

**SessionContext**

* “Main interface” for query construction/execution; owns catalog bindings, function registry, configuration/runtime handles; entrypoint for SQL and DataFrame planning. ([Docs.rs][9])
* Planning relevance: provides SQL parsing (`sql`, `parse_sql_expr`), logical-plan execution entrypoints (`execute_logical_plan`), and rule registration (analyzer/optimizer). ([Docs.rs][9])

**SessionState**

* Container for “all necessary state to plan and execute queries” (configuration, functions, runtime environment), factored beneath SessionContext for reuse/embedding. ([Docs.rs][10])
* Planning relevance: receives physical optimizer rule injection (`add_physical_optimizer_rule`), and is the object typically used by embedded systems implementing custom planning stacks. ([Docs.rs][11])

**Expr**

* Logical expression IR (columns, literals, scalar functions, predicates, projections). Display format is SQL-like but typed (e.g., literal renders include datatype), and is used by low-level explain (indent). ([Docs.rs][12])

**LogicalPlan**

* Tree of relational operators transforming an input relation into an output relation; produced by SQL planner / DataFrame API / programmatic construction. ([Docs.rs][4])
* Planning relevance: analyzer/optimizer operate over LogicalPlan; EXPLAIN/EXPLAIN ANALYZE are represented as logical plan variants (`Explain`, `Analyze`). ([Docs.rs][4])

**ExecutionPlan**

* Physical operator tree executed by the engine; incorporates partitioning/distribution/sort requirements and data-source scan topology. Physical plan is explicitly hardware/data-layout dependent. ([Apache DataFusion][5])

**AnalyzerRule**

* Pre-optimization logical rewrites that make plans *valid* (name resolution refinements, subquery reference shaping, type coercion). AnalyzerRules are semantically allowed to “change form” to reach a valid typed/bound plan; they are distinct from optimizer rules. ([Docs.rs][2])

**OptimizerRule**

* Semantic-preserving logical rewrites that produce an equivalent LogicalPlan with reduced cost (e.g., simplification, pushdowns). Debuggable via `EXPLAIN VERBOSE` which shows per-rule effects. ([Apache DataFusion][13])

**PhysicalOptimizerRule**

* Semantic-preserving physical rewrites: `ExecutionPlan -> ExecutionPlan` (e.g., enforce distribution/sorting, repartitioning decisions). ([Docs.rs][11])

---

## 1) The planning pipeline: from query to plan artifacts

### 1.1 End-to-end flow (compiler pipeline view)

DataFusion planning is a multi-stage compiler:

1. **Parse**

* SQL text → SQL AST via DataFusion’s SQL parser stack (`datafusion-sqlparser-rs` / `sqlparser` integration). ([GitHub][14])

2. **Analyze / bind (AnalyzerRules)**

* Resolve/normalize expressions into a valid bound logical form (type coercion, subquery reference shaping, etc.). ([Docs.rs][2])
* Hook point (Rust): `SessionContext.add_analyzer_rule(...)` to prepend/augment analyzer passes. ([Docs.rs][9])

3. **Logical optimization (OptimizerRules)**

* Apply semantic-preserving rewrite passes (projection/predicate pushdown, constant folding/simplification, etc.). Your attached guide’s pipeline summary matches this ordering (Analyzer → Optimizer → physical planning → physical optimizer). ([Apache DataFusion][15])
* Debug surface: `EXPLAIN VERBOSE` emits rule-by-rule plan deltas. ([Apache DataFusion][13])
* Hook point (Rust): `SessionContext.add_optimizer_rule(...)` / `remove_optimizer_rule(name)` for deterministic rulepack experiments. ([Docs.rs][9])

4. **Physical planning (LogicalPlan → ExecutionPlan)**

* Select physical operator implementations + compute distribution/partitioning requirements based on config/runtime and data-source capabilities. Physical plan is explicitly a function of hardware configuration and data organization. ([Apache DataFusion][5])

5. **Physical optimization (PhysicalOptimizerRules)**

* Rewrite physical DAG to satisfy distribution/sort requirements and improve parallelism / operator placement. ([Docs.rs][11])
* Hook point (Rust): `SessionState::add_physical_optimizer_rule(...)`. ([Docs.rs][11])

---

### 1.2 Plan artifacts you can extract (non-executing vs executing)

#### A) Non-executing artifacts (compile-only)

**SQL surface**:

* `EXPLAIN [VERBOSE] [FORMAT format] statement` renders logical + physical plan without running. ([Apache DataFusion][16])
* Constraint: `EXPLAIN VERBOSE` only supports `indent` format. ([Apache DataFusion][16])
* Output shape control: `datafusion.explain.format` (`indent` vs `tree`), plus `show_schema/show_statistics/show_sizes`, and `tree_maximum_render_width`. ([Apache DataFusion][17])

**Python surface (direct plan objects; no execution)**:

* Extract the three canonical plan layers directly from a DataFrame: unoptimized logical, optimized logical, physical. Your attached guide already enumerates these accessors. ([Apache DataFusion][18])

```python
from datafusion import SessionContext

ctx = SessionContext()
ctx.register_parquet("t", "s3://bucket/path/t.parquet")  # or local path
df = ctx.sql("SELECT a, sum(b) AS sb FROM t WHERE a > 10 GROUP BY a")

lp0 = df.logical_plan()              # unoptimized LogicalPlan
lp1 = df.optimized_logical_plan()    # optimized LogicalPlan
pp  = df.execution_plan()            # ExecutionPlan (physical), still not executed

df.explain(verbose=True, analyze=False)  # explain-only, no execution
```

#### B) Executing artifacts (runtime + metrics)

**SQL surface**:

* `EXPLAIN ANALYZE statement` runs the plan and prints the annotated physical plan with execution metrics; conceptually implemented via a logical-plan `Analyze` variant that “runs the actual plan” and prints metrics. ([Docs.rs][4])

**Python surface**:

```python
df.explain(verbose=True, analyze=True)   # triggers execution; emits metrics
```

(If you need “results + explain analyze” in a single pass, treat it as an application-level concern; DataFusion surfaces explain-analyze as a distinct execution mode rather than a side-channel attached to result collection. ([GitHub][19]))

---

### 1.3 What changes across plan layers (and why you care)

Let `P0` = unoptimized logical, `P1` = optimized logical, `P2` = physical.

**P0 (unoptimized logical)**

* Primary signal: SQL parser + analyzer decisions (resolved schema, implicit casts/type coercion, initial operator placement).
* Debug value: validate binding failures, type coercion surprises, subquery shape, projection naming before optimizer mutation. ([Docs.rs][2])

**P1 (optimized logical)**

* Primary signal: semantic-preserving rewrites applied; your attached guide emphasizes projection/filter pushdown landing here, making P1 the stable backbone for lineage and “what will be read” reasoning.
* Debug value: answer “why did this scan read extra columns?”, “which predicates became pushdown vs residual?”, “did simplification happen?”. Rule-by-rule diffs available via `EXPLAIN VERBOSE`. ([Apache DataFusion][13])

**P2 (physical / ExecutionPlan)**

* Primary signal: operator selection + parallelism topology + distribution/sort enforcement; incorporates CPU count and file organization and therefore can differ between environments. ([Apache DataFusion][5])
* Debug value: diagnose shuffles/repartitions (`RepartitionExec` insertions), enforced sorts, scan partition counts; validate that performance regressions correspond to topology changes rather than semantic plan changes. ([Apache DataFusion][5])

**Minimal production-grade plan bundle (planning-first)**

* Store `(P0, P1, P2)` plus `EXPLAIN FORMAT tree` (diff-friendly) and `EXPLAIN VERBOSE` (rule provenance) for any query whose stability/performance matters; gate regressions on P1 (semantic rewrites) and treat P2 as environment-sensitive telemetry. ([Apache DataFusion][17])

[1]: https://datafusion.apache.org/?utm_source=chatgpt.com "Apache DataFusion — Apache DataFusion documentation"
[2]: https://docs.rs/datafusion/latest/datafusion/optimizer/analyzer/trait.AnalyzerRule.html?utm_source=chatgpt.com "AnalyzerRule in datafusion::optimizer::analyzer - Rust"
[3]: https://datafusion.apache.org/python/autoapi/datafusion/plan/index.html?utm_source=chatgpt.com "datafusion.plan — Apache Arrow DataFusion documentation"
[4]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html?utm_source=chatgpt.com "LogicalPlan in datafusion_expr::logical_plan - Rust"
[5]: https://datafusion.apache.org/user-guide/explain-usage.html?utm_source=chatgpt.com "Reading Explain Plans — Apache DataFusion documentation"
[6]: https://docs.rs/crate/datafusion/latest?utm_source=chatgpt.com "datafusion 52.1.0"
[7]: https://pypi.org/project/datafusion/?utm_source=chatgpt.com "datafusion"
[8]: https://datafusion.apache.org/library-user-guide/using-the-dataframe-api.html?utm_source=chatgpt.com "Using the DataFrame API — Apache DataFusion documentation"
[9]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html "SessionContext in datafusion::execution::context - Rust"
[10]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html?utm_source=chatgpt.com "SessionState in datafusion::execution::session_state - Rust"
[11]: https://docs.rs/datafusion/latest/datafusion/physical_optimizer/trait.PhysicalOptimizerRule.html?utm_source=chatgpt.com "PhysicalOptimizerRule in datafusion::physical_optimizer"
[12]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Expr.html?utm_source=chatgpt.com "Expr in datafusion::logical_expr - Rust"
[13]: https://datafusion.apache.org/library-user-guide/query-optimizer.html?utm_source=chatgpt.com "Query Optimizer — Apache DataFusion documentation"
[14]: https://github.com/apache/datafusion-sqlparser-rs?utm_source=chatgpt.com "apache/datafusion-sqlparser-rs: Extensible SQL Lexer and ..."
[15]: https://datafusion.apache.org/blog/2025/06/15/optimizing-sql-dataframes-part-one/?utm_source=chatgpt.com "Optimizing SQL (and DataFrames) in DataFusion, Part 1"
[16]: https://datafusion.apache.org/user-guide/sql/explain.html?utm_source=chatgpt.com "EXPLAIN — Apache DataFusion documentation"
[17]: https://datafusion.apache.org/user-guide/configs.html?utm_source=chatgpt.com "Configuration Settings — Apache DataFusion documentation"
[18]: https://datafusion.apache.org/python/user-guide/dataframe/index.html?utm_source=chatgpt.com "DataFrames — Apache Arrow DataFusion documentation"
[19]: https://github.com/apache/datafusion/discussions/13096?utm_source=chatgpt.com "Getting results and explain analyze plan at the same time"

According to a document you provided in this thread (Jan 27, 2026) and the current upstream DataFusion docs, **`SessionContext` is the planning “environment boundary”**: it owns the *name-resolution universe* (catalog/schema/table providers), the *function universe* (built-ins + registered UDF/UDAF/UDWF/UDTF), and the *configuration universe* (parser/optimizer/physical-planner knobs). These three state classes are exactly what makes a plan *reproducible* vs *accidentally environment-dependent*. ([Apache DataFusion][1])

---

## 2) `SessionContext` as the planning environment

### 2.1 Construction: what `SessionContext` owns (Python + Rust) and why it matters for planning

#### Python constructor (binding surface)

`SessionContext(config: SessionConfig | None = None, runtime: RuntimeEnvBuilder | None = None)` is the Python entrypoint; **`config` changes plan shape**, **`runtime` changes feasibility + execution resources** (spill/memory/disk) but is still part of “plan environment” because many physical plan decisions depend on it. ([Apache DataFusion][2])

Canonical “explicit environment” pattern (agent-ready):

```python
from datafusion import SessionContext, SessionConfig, RuntimeEnvBuilder

runtime = (
    RuntimeEnvBuilder()
    .with_disk_manager_specified("/mnt/df-spill")
    .with_fair_spill_pool(8 * 1024**3)
)

config = (
    SessionConfig()
    .with_default_catalog_and_schema("datafusion", "public")  # pin name-resolution
    .with_information_schema(True)                            # enable df_settings/SHOW*
    .with_target_partitions(16)                               # physical parallelism target
    .with_repartition_joins(True)
    .with_repartition_aggregations(True)
    .with_repartition_windows(True)
)

ctx = SessionContext(config, runtime)
```

Key semantics (from the binding docs):

* `SessionConfig.with_default_catalog_and_schema(catalog, schema)` pins *unqualified identifier resolution*.
* `SessionConfig.with_information_schema(enabled)` enables metadata tables/`SHOW` commands needed for environment capture (below).
* `SessionConfig.with_target_partitions(n)` directly alters physical plan partitioning targets.

#### Rust constructor (engine surface)

Rust `SessionContext` is the same concept: main interface for planning and executing; it exposes **more planning mutation hooks** (analyzer/optimizer rule injection) than Python typically surfaces. ([Docs.rs][3])

Rust-specific planning levers are *methods on `SessionContext`* (e.g., `add_analyzer_rule`, `add_optimizer_rule`, `remove_optimizer_rule`) and are part of the environment contract because they change the optimizer pipeline deterministically. ([Docs.rs][4])

#### `SessionState`: the “state atom” behind `SessionContext`

In Rust, `SessionState` is explicitly “all the necessary state to plan and execute queries, such as configuration, functions, and runtime environment” and is obtainable via `ctx.state()`.([Docs.rs][4])
Implication: if you are embedding DataFusion in a service, **treat `SessionState` as the serializable conceptual unit** even when you keep a `SessionContext` handle for API ergonomics.

---

### 2.2 Catalogs / schemas / tables: deterministic name resolution and how to wire it

#### Default resolution model

DataFusion uses a 3-level namespace: `catalog.schema.table`. Python docs state a default catalog+schema exists (names vary across docs); therefore **do not rely on defaults**—pin them explicitly or always fully qualify table references. ([Apache DataFusion][5])

Concrete controls:

* **Pin defaults in Python**: `SessionConfig.with_default_catalog_and_schema(catalog, schema)`.
* **Inspect defaults at runtime** (no guessing): `SHOW TABLES`, `SHOW COLUMNS`, `SHOW ALL`, `information_schema.*` views. ([Apache DataFusion][6])

#### Catalog API (Python): create namespaces and register providers

Python `SessionContext` exposes:

* `ctx.catalog(name='datafusion') -> Catalog` and `ctx.catalog_names()` for enumerating what the planner can resolve.
* `ctx.register_catalog_provider(name, provider)` to mount a new catalog provider into the session (thereafter visible to planning).

Minimal “namespace wiring” (in-memory providers):

```python
from datafusion import SessionContext
from datafusion.catalog import Catalog, Schema
import pyarrow as pa

ctx = SessionContext()

cat = Catalog.memory_catalog()
sch = Schema.memory_schema()
cat.register_schema("raw", sch)        # schema namespace
ctx.register_catalog_provider("mycat", cat)

sch.register_table("events", pa.table({"x":[1,2,3]}))
ctx.sql("SELECT * FROM mycat.raw.events").show()
```

The catalog objects are first-class:

* `Catalog.memory_catalog()` / `Schema.memory_schema()` are explicit constructors.
* `Schema.register_table(name, table)` registers a table provider into that schema; the accepted “table” types include a DataFusion `Table`, a `TableProviderExportable`, a `DataFrame`, or a `pyarrow.dataset.Dataset`.

**Agent rule**: treat “dataset identity” as `catalog.schema.table`; never use bare `t` in generated SQL unless `with_default_catalog_and_schema` is set and captured.

#### Table registration primitives (Python): choose the one whose semantics you need

Your attached guide explicitly treats “registration primitives” as what defines dependency keys (tables) and scan behavior knobs (pruning/schema/sort order).

Planning-relevant registration calls and their precise signatures:

1. **Parquet as a named table** (stable table key; scan knobs are part of the environment)

* `ctx.register_parquet(name, path, table_partition_cols=None, parquet_pruning=True, file_extension='.parquet', skip_metadata=True, schema=None, file_sort_order=None)`
  Use when: you need a durable SQL-visible name (`ctx.table(name)` / SQL), and you want to explicitly control pruning and schema-inference behavior at registration time.

2. **Directory / multi-file dataset as one table**

* `ctx.register_listing_table(name, path, table_partition_cols=None, file_extension='.parquet', schema=None, file_sort_order=None)`
  Use when: “one logical table = many files” and you want the planner to build file groups.

3. **PyArrow Dataset as a named table**

* `ctx.register_dataset(name, dataset)`
  Use when: you already have a Dataset object (filesystem, partitioning, filters) and want DataFusion planning over it.

4. **In-memory RecordBatches as a named table**

* `ctx.register_record_batches(name, partitions)`
  Use when: you’re synthesizing tables in-process for testing or staging; note that in-memory tables commonly break some plan serialization workflows (treat as non-portable planning inputs).

5. **Generic registration entrypoint**

* `ctx.register_table(name, table)` accepts a `Table`, `TableProviderExportable`, `DataFrame`, or `pyarrow.dataset.Dataset`
  Use when: you want a single adapter point, especially when working with foreign providers exposed via PyCapsule.

6. **Views**

* `ctx.register_view(name, df)` registers a `DataFrame` as a view (SQL name binds to a logical plan)
  Use when: you want a named subplan whose expansion is part of planning.

7. **Removal**

* `ctx.deregister_table(name)` removes a table from the session (name-resolution changes immediately).

**Deprecated API note (binding)**: `register_table_provider(name, provider)` is documented as deprecated in favor of `register_table()`; agents should call `register_table` unless pinned to an older binding.

---

### 2.3 Function registries: built-ins + UDFs and how they enter planning

#### Built-in function discovery and environment capture

The function set is part of the planning environment because it affects parse/bind/typecheck.

* Discover via `SHOW FUNCTIONS [LIKE pattern]` or the information schema routines/parameters views.
* Capture alongside plan artifacts using `SHOW ALL` / `information_schema.df_settings` (see §2.4). ([Apache DataFusion][6])

#### Python UDF construction (vectorized Arrow contract)

Scalar UDFs are defined as Python functions taking one or more Arrow arrays and returning an Arrow array; created via `udf()` with explicit `(input_types, return_type, volatility)` signature. ([Apache DataFusion][1])

Example (Scalar UDF used in expression planning):

```python
import pyarrow as pa
from datafusion import SessionContext, udf, col

def is_null(arr: pa.Array) -> pa.Array:
    return arr.is_null()

is_null_udf = udf(is_null, [pa.int64()], pa.bool_(), "stable")

ctx = SessionContext()
df = ctx.from_pydict({"a":[1,None,3]})
df.select(col("a"), is_null_udf(col("a")).alias("is_null")).show()
```

This is the “planning-visible” effect: `is_null_udf(col("a"))` becomes an `Expr` node in the logical plan; the engine uses the declared types/volatility during planning.([Apache DataFusion][7])

#### Registering functions into the SQL planner namespace (Python)

To make a UDF callable *by name in SQL*, register it with the context:

* `ctx.register_udf(ScalarUDF)`
* `ctx.register_udaf(AggregateUDF)`
* `ctx.register_udwf(WindowUDF)`
* `ctx.register_udtf(TableFunction)`

This matters for planning because the SQL binder resolves function identifiers through the registry.

Aggregate + window UDF construction primitives are also documented:

* UDAF: `udaf()` + custom `Accumulator` (update/merge/state/evaluate).
* UDWF: `udwf()` + `WindowEvaluator`; evaluation strategy flags control which method the engine calls (planning-time capability contract).

Table function constraint (planning-time): only literal expressions are supported as arguments; UDTFs must return a TableProvider and then be registered into the session via `register_udtf`.

#### Rust UDF registration (engine-level contract)

Engine docs specify the canonical scalar UDF pipeline: implement `ScalarUDFImpl`, create `ScalarUDF`, then register with `SessionContext::register_udf` to invoke by name.
Similarly, custom table providers and table functions ultimately integrate via registration on `SessionContext` (they become resolvable table names / table functions inside plans).([Apache DataFusion][8])

---

### 2.4 Configuration knobs: how to set them and how to prove they influenced planning

#### Where config lives and how it is applied

Rust `SessionConfig` is key-value namespaced (`datafusion.*`) and passed to `SessionContext::new_with_config`; configuration affects planning and execution behavior. ([Apache DataFusion][5])

DataFusion supports setting configuration:

* programmatically (API),
* via environment variables,
* via SQL `SET ...` statements.

#### Python: planning-relevant SessionConfig methods (direct plan-shape levers)

From the Python API docs:

* default namespace controls:

  * `with_create_default_catalog_and_schema(enabled)`
  * `with_default_catalog_and_schema(catalog, schema)`
  * `with_information_schema(enabled)`
* physical planning / partitioning:

  * `with_target_partitions(n)`
  * repartition toggles: joins/aggs/file scans/windows/sorts

**Agent rule**: any config that can change scan pushdown, repartitioning, or parser behavior must be treated as *part of the plan fingerprint*.

#### Global config keys that directly affect planning semantics

From the engine configuration registry:

* SQL parser:

  * `datafusion.sql_parser.enable_ident_normalization` (case normalization when not quoted)
  * `datafusion.sql_parser.dialect` (dialect selection; impacts parsed AST/operators)
  * `datafusion.sql_parser.collect_spans` (record source spans into logical plan nodes)
* Catalog defaults:

  * `datafusion.catalog.default_catalog`, `datafusion.catalog.default_schema` (affect unqualified name resolution)
  * `datafusion.catalog.information_schema` (exposes `information_schema` tables)
* Explain outputs (plan artifact format):

  * `datafusion.explain.format` (`indent` vs `tree`)

#### “Prove it”: capture the effective session config as a plan artifact

DataFusion’s own metadata surfaces:

* `SHOW ALL` or `SELECT * FROM information_schema.df_settings;` to extract current session configuration options.

This is the same idea your attached guide recommends: embed `df_settings` / `SHOW ALL` outputs in every plan bundle so plan diffs can be attributed to config drift rather than query drift.

---

### 2.5 Object stores: why they’re part of the planning environment (Python binding surface)

If table locations are URLs, the planner/executor must resolve them through an object store registry. Python exposes:

* `ctx.register_object_store(schema: str, store: Any, host: str | None = None)`

This is environment state: without consistent store registration, the same SQL text may fail to plan/execute or may list different file sets depending on backing store behavior.

---

## Minimal agent checklist (SessionContext setup for reproducible planning)

1. Construct `SessionContext(SessionConfig, RuntimeEnvBuilder)` and pin:

   * default catalog+schema, information_schema, target_partitions, repartition toggles.
2. Register catalogs (`register_catalog_provider`) and tables (`register_parquet` / `register_listing_table` / `register_table`), avoiding implicit defaults.
3. Register UDFs/UDAFs/UDWFs/UDTFs on the context when SQL should resolve them by name.
4. Set any remaining `datafusion.*` keys via `SessionConfig.set(...)` or SQL `SET ...`; snapshot with `SHOW ALL` / `information_schema.df_settings`.

[1]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[2]: https://datafusion.apache.org/python/user-guide/configuration.html "Configuration — Apache Arrow DataFusion  documentation"
[3]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html "SessionContext in datafusion::execution::context - Rust"
[4]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html "SessionState in datafusion::execution::session_state - Rust"
[5]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[6]: https://datafusion.apache.org/user-guide/sql/information_schema.html "Information Schema — Apache DataFusion  documentation"
[7]: https://datafusion.apache.org/python/user-guide/common-operations/udf-and-udfa.html "User-Defined Functions — Apache Arrow DataFusion  documentation"
[8]: https://datafusion.apache.org/library-user-guide/custom-table-providers.html "Custom Table Provider — Apache DataFusion  documentation"

According to a document from **January 27, 2026** (your planning guide) and the current upstream DataFusion docs, the “front-end” layer is best treated as a **small set of canonical plan constructors** (SQL / DataFrame / LogicalPlanBuilder) that deterministically produce `LogicalPlan` (+ derived `ExecutionPlan`) artifacts you can snapshot, diff, and replay.  ([datafusion.apache.org][1])

---

## 3) Plan construction surfaces (front-ends)

### 3.1 SQL planner surface

#### 3.1.1 Canonical entrypoint: `ctx.sql(...) → DataFrame` (Python)

**Signature (Python):**

```python
sql(query: str,
    options: SQLOptions | None = None,
    param_values: dict[str, Any] | None = None,
    **named_params: Any) -> DataFrame
```

This API is *planning-first*: it **creates** a DataFrame from SQL text; execution occurs when you call a terminal method (`collect/show/to_pandas/execute_stream/...`). ([datafusion.apache.org][1])

**Planning-only extraction pattern (no execution):**

```python
df = ctx.sql("SELECT a, min(b) FROM t GROUP BY a")
lp0 = df.logical_plan()
lp1 = df.optimized_logical_plan()
pp  = df.execution_plan()
df.explain(verbose=True, analyze=False)  # prints, does not run
```

`logical_plan()`, `optimized_logical_plan()`, `execution_plan()`, and `explain(verbose, analyze)` are explicit DataFrame accessors. ([datafusion.apache.org][2])

**Orchestrator-style “PlanBundle” construction** (your guide’s reference pattern):

```python
df = ctx.sql(sql_text)
logical = df.optimized_logical_plan()
physical = df.execution_plan()
```



#### 3.1.2 SQL safety gating: `SQLOptions` + `sql_with_options`

If an agent must not mutate catalog state or write data, wrap SQL planning in explicit allowlists:

```python
from datafusion import SQLOptions

opts = (
    SQLOptions()
    .with_allow_ddl(False)
    .with_allow_dml(False)
    .with_allow_statements(False)
)

df = ctx.sql_with_options("SELECT ...", opts)
```

`SQLOptions` defaults to allowing DDL/DML/statements; `sql_with_options` validates the query against these options before producing the DataFrame. ([datafusion.apache.org][1])

#### 3.1.3 DDL/DML participation: **planned as statements; applied on execution**

The Python `SessionContext.sql` doc is explicit: it implements **DDL** (`CREATE TABLE`, `CREATE VIEW`) and **DML** (`INSERT INTO`) with **in-memory default implementation**. That implies **stateful side effects** at execution time (not at plan construction). ([datafusion.apache.org][1])

**DDL coverage and syntax (DataFusion SQL reference):**

* `CREATE EXTERNAL TABLE ... STORED AS ... LOCATION ...` registers an external location (filesystem/object store) as a named table; supports `PARTITIONED BY`, `WITH ORDER`, `OPTIONS(...)`. ([datafusion.apache.org][3])
* `CREATE TABLE ... AS SELECT | VALUES` creates an **in-memory** table from a query or values list. ([datafusion.apache.org][3])
* `CREATE VIEW ... AS statement` registers a view. ([datafusion.apache.org][3])
* DML includes `COPY {table|query} TO 'file' ...` and `INSERT INTO table_name VALUES (...) | query`. ([datafusion.apache.org][4])

**Agent rule (DDL/DML correctness):**

* Treat `df = ctx.sql("CREATE ...")` as a *deferred side effect*. Nothing is installed into the catalog until a terminal execution call occurs (e.g., `df.collect()`).
* Gate DDL/DML via `SQLOptions` unless the task explicitly requires catalog mutation / writes.

**Example: create external table, then query (Python):**

```python
ctx.sql("""
CREATE EXTERNAL TABLE taxi
STORED AS PARQUET
LOCATION '/mnt/nyctaxi/tripdata.parquet'
""").collect()  # executes DDL side-effect

df = ctx.sql("SELECT count(*) FROM taxi")
df.show()
```

(DDL semantics + syntax are defined in the SQL reference; note the execution requirement is implied by DataFusion’s lazy DataFrame model.) ([datafusion.apache.org][3])

**Streaming planning via UNBOUNDED sources:**
`CREATE UNBOUNDED EXTERNAL TABLE ...` marks a source as unbounded; DataFusion may fail plan generation if the query cannot be executed in streaming fashion. ([datafusion.apache.org][3])

#### 3.1.4 Parameter binding: **two distinct mechanisms** (Python 51.0.0+)

DataFusion-Python provides *named placeholder substitution* with `$name` plus a typed scalar path via `param_values`.

**(A) String / DataFrame substitution via `$name`**

```python
ctx.sql('SELECT "Name" FROM pokemon WHERE "Attack" > $val', val=75)
ctx.sql('SELECT "Name", $col FROM $df WHERE $col > $val',
        col='"Defense"', df=ctx.table("pokemon"), val=75)
```

Mechanics:

* Placeholder formatting is `$name`.
* For `DataFrame` arguments, DataFusion registers a **temporary view** under a generated table name.
* Not supported for SQL dialects `hive` and `mysql` (no named placeholders). ([datafusion.apache.org][5])

**Critical warning (DataFrame placeholders):** session must support temporary view registration; custom catalog/schema providers must ensure temporary views do not persist across contexts (leakage hazard). ([datafusion.apache.org][5])

**(B) Typed scalar substitution via `param_values` (avoid string conversion loss)**

```python
ctx.sql(
  'SELECT "Name", "Attack" FROM pokemon WHERE "Attack" > $val',
  param_values={"val": 75},
)
```

`param_values` casts Python objects into Arrow scalar values; parameters are limited to scalar-value positions (e.g., comparisons). ([datafusion.apache.org][5])

---

### 3.2 DataFrame API surface (plan builder)

#### 3.2.1 DataFrame = “plan object + composition API”; execution only on terminals

`collect()` explicitly states the core contract: mutations before `collect` only update a plan; `collect` triggers computation. ([datafusion.apache.org][2])

Key plan accessors:

* `logical_plan() -> LogicalPlan` (unoptimized)
* `optimized_logical_plan() -> LogicalPlan`
* `execution_plan() -> ExecutionPlan`
* `explain(verbose=False, analyze=False) -> None` (printed explain) ([datafusion.apache.org][2])

Your guide calls out these accessors as first-class orchestration surfaces (snapshot them). 

#### 3.2.2 DataFrame creation surfaces (Python): choose based on name-resolution needs

**Direct read** (no catalog name; best for single-use pipelines):

```python
df = ctx.read_parquet("file.parquet")
```

**Register then resolve** (stable dataset key; required for SQL name resolution / views / joins by table name):

```python
ctx.register_parquet("file", "file.parquet")
df = ctx.table("file")
```

([datafusion.apache.org][6])

Also: `create_dataframe_from_logical_plan(plan)` builds a DataFrame from a `LogicalPlan` object (primarily for deserialization / programmatic plan pipelines). ([datafusion.apache.org][1])

#### 3.2.3 Expression construction: `Expr` vs schema-parsed SQL strings

Most transformation APIs accept **either** an `Expr` **or** a SQL expression string parsed against the DataFrame schema:

* `filter(*predicates: Expr | str) -> DataFrame` with explicit examples:

  * `df.filter(col("a") > lit(1))`
  * `df.filter("a > 1")` ([datafusion.apache.org][2])

* `select(*exprs: Expr | str) -> DataFrame` (columns or expressions) ([datafusion.apache.org][2])

* `with_column(name: str, expr: Expr | str) -> DataFrame` where string expr is parsed against schema. ([datafusion.apache.org][2])

* `with_columns(...exprs / named_exprs...)` supports mixed Expr + SQL strings. ([datafusion.apache.org][2])

**Dedicated expr parser (Python): `DataFrame.parse_sql_expr(expr: str) -> Expr`**
Use this when you want a typed `Expr` object (for reuse / canonicalization / inspection) rather than embedding strings in multiple calls:

```python
pred = df.parse_sql_expr("a > 1")  # yields col("a") > lit(1)
df2 = df.filter(pred)
```

The parser is schema-aware (“processed against the current schema”). ([datafusion.apache.org][2])

#### 3.2.4 Plan-builder composition: transformations vs terminals

**Representative transformation surface (Python docs; non-exhaustive):**

* `filter`, `select`, `aggregate`, `join`, `join_on`, `sort`, `limit`, `distinct`, `repartition`, `repartition_by_hash`, `union*`, `with_column(s)`, `transform`. ([datafusion.apache.org][2])

`join_on` is important when you need inequality predicates; equality predicates are “correctly optimized.” ([datafusion.apache.org][2])

**Terminal surfaces (execute):**

* In-memory materialization: `collect() -> list[RecordBatch]`, `to_arrow_table()`, `to_pandas()`, etc. ([datafusion.apache.org][2])
* Streaming: `execute_stream() -> RecordBatchStream` (single partition) and `execute_stream_partitioned() -> list[RecordBatchStream]` (per partition). ([datafusion.apache.org][2])

**Agent rule (memory discipline):**

* Prefer `execute_stream[_partitioned]` when the agent is building operators over streams or when result cardinality is unknown/large; avoid `collect()` unless bounded. ([datafusion.apache.org][2])

#### 3.2.5 Bridging SQL ↔ DataFrame ↔ catalog objects (views)

Two canonical “convert plan to name” patterns:

**(A) Register a DataFrame as a view via context**

```python
ctx.register_view("v", df)     # now usable from SQL
ctx.sql("SELECT ... FROM v").collect()
```

([datafusion.apache.org][1])

**(B) Convert DataFrame into a Table, then register**

```python
view_tbl = df.into_view()
ctx.register_table("values_view", view_tbl)
ctx.sql("SELECT value FROM values_view").collect()
```

([datafusion.apache.org][2])

---

### 3.3 Programmatic logical-plan construction (Rust-level, conceptually central)

#### 3.3.1 Why bypass SQL/DataFrame?

Use programmatic construction when at least one holds:

1. You are compiling a non-SQL front-end (DSL / Substrait / custom IR) into DataFusion-native relational algebra.
2. You need deterministic plan generation **without** SQL side effects (notably DDL mutation).
3. You need to inject custom logical operators (`LogicalPlan::Extension`) in a structured way (SQL/DataFrame front-ends can’t express the node directly).

DataFusion’s optimizer guide explicitly lists three plan construction paths: `LogicalPlanBuilder`, the SQL planner (`datafusion-sql`), and the DataFrame API. ([datafusion.apache.org][7])

#### 3.3.2 `LogicalPlan` variants + `LogicalPlanBuilder`

The “Building Logical Plans” guide defines `LogicalPlan` as an enum with operator variants and an `Extension` variant for custom operators. ([datafusion.apache.org][8])

**Builder example (Rust):**

```rust
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::prelude::*;
use std::sync::Arc;

let builder = LogicalPlanBuilder::scan("person", table_source, None)?;
let plan = builder
    .filter(col("id").gt(lit(500)))?
    .build()?;

println!("{}", plan.display_indent_schema());
```

([datafusion.apache.org][8])

`LogicalPlanBuilder` exists because:

* It provides **typed, fallible** construction with invariant checks at each stage (`Result<...>`).
* It centralizes planner-normalized semantics (projection lists, join predicates, schema propagation), reducing ad-hoc `LogicalPlan::{Variant}` wiring. ([Docs.rs][9])

#### 3.3.3 Compile logical → physical (`SessionState::create_physical_plan`)

Logical plans cannot execute directly; they must be compiled into an `ExecutionPlan`. The recommended path is:

```rust
let physical_plan = ctx.state().create_physical_plan(&logical_plan).await?;
```

([datafusion.apache.org][8])

(If you only need planning artifacts, stop here and snapshot `DisplayableExecutionPlan::indent(...)` for diffs. The “Building Logical Plans” guide demonstrates exactly this.) ([datafusion.apache.org][8])

#### 3.3.4 Wrap a programmatic `LogicalPlan` back into a DataFrame (Rust)

In Rust, `DataFrame` is explicitly a wrapper around a `LogicalPlan` plus the `SessionState` required for execution; a low-level constructor exists:

```rust
use datafusion::dataframe::DataFrame;

// given: session_state: SessionState, plan: LogicalPlan
let df = DataFrame::new(session_state, plan);
```

This is the bridge when you want to reuse DataFrame execution utilities on a plan produced by a custom front-end. ([Docs.rs][10])

#### 3.3.5 Parse SQL expressions into `Expr` (Rust; feature-gated)

Rust DataFrame includes `parse_sql_expr` (crate feature `sql`) to translate SQL expression text into an `Expr` processed against schema:

```rust
let expr = df.parse_sql_expr("a > 1 and b in (1, 10)")?;
```

([Docs.rs][10])

This mirrors Python’s `DataFrame.parse_sql_expr` and is the preferred bridge when your agent receives filter/projection text but must operate in the `Expr` API. ([datafusion.apache.org][2])

#### 3.3.6 SQL planning without state mutation (Rust: avoid DDL side effects)

Both the Rust ecosystem and Python binding docs warn that `SessionContext::sql` implements DDL and view creation with in-memory default mutation; if that’s undesirable, use `SessionState::create_logical_plan()` (parse/plan only). ([GitHub][11])

This distinction matters when your agent must:

* **plan** `CREATE VIEW ...` for lineage analysis **without** installing it,
* parse DDL in untrusted SQL without mutating the session,
* run “planning as a service” where session state must remain immutable across requests.

---


[1]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[2]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[3]: https://datafusion.apache.org/user-guide/sql/ddl.html "DDL — Apache DataFusion  documentation"
[4]: https://datafusion.apache.org/user-guide/sql/dml.html "DML — Apache DataFusion  documentation"
[5]: https://datafusion.apache.org/python/user-guide/sql.html "SQL — Apache Arrow DataFusion  documentation"
[6]: https://datafusion.apache.org/python/user-guide/io/parquet.html "Parquet — Apache Arrow DataFusion  documentation"
[7]: https://datafusion.apache.org/library-user-guide/query-optimizer.html "Query Optimizer — Apache DataFusion  documentation"
[8]: https://datafusion.apache.org/library-user-guide/building-logical-plans.html "Building Logical Plans — Apache DataFusion  documentation"
[9]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.LogicalPlanBuilder.html "LogicalPlanBuilder in datafusion::logical_expr - Rust"
[10]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html "DataFrame in datafusion::dataframe - Rust"
[11]: https://github.com/apache/datafusion/issues/5291 "Improved support for \"User Defined Catalogs\" · Issue #5291 · apache/datafusion · GitHub"

According to a document from **January 27, 2026**, the **practical extraction core** for DataFusion logical plans (Python) is: **(1) structural walk via `LogicalPlan.inputs()`**, then **(2) operator introspection via `LogicalPlan.to_variant()`**, with explicit defensive recursion for **subplans embedded in subqueries/expressions**, and with `display_indent_schema()` used as a schema “oracle” when direct schema objects are not ergonomically available.  ([datafusion.apache.org][1])

---

## 4) LogicalPlan deep dive (structure, traversal, matching)

### 4.1 Minimal introspection surface (Python bindings): what you can *actually call*

`datafusion.plan.LogicalPlan` (Python) exposes the following plan-inspection primitives (agent-safe, no execution):

* `inputs() -> list[LogicalPlan]` (structural children)
* `to_variant() -> Any` (concrete operator payload)
* renderers: `display()`, `display_indent()`, `display_indent_schema()`, `display_graphviz()`
* serialization: `to_proto()`, `from_proto(ctx, bytes)` (with limitations; see below) ([datafusion.apache.org][1])

The attached guide explicitly treats these as the *stable API contract* for plan mining in Python. 

**Canonical “obtain plan object” pattern (planning-only):**

```python
df = ctx.sql("SELECT ...")             # plan builder
lp = df.optimized_logical_plan()       # LogicalPlan (recommended layer)
# lp = df.logical_plan()               # unoptimized LogicalPlan
```



---

### 4.2 LogicalPlan node taxonomy (what variants exist; what fields you mine)

Rust defines `LogicalPlan` as an enum wrapping operator structs; the current variant set includes (among others) **Projection, Filter, Window, Aggregate, Sort, Join, Repartition, Union, TableScan, EmptyRelation, Subquery, SubqueryAlias, Limit, Values, Explain, Analyze, Extension, Distinct, Dml, Ddl, Copy, DescribeTable, Unnest, RecursiveQuery**. ([Docs.rs][2])

Below is the *agent-relevant* subset: **(a) what operator means**, **(b) what to extract**, **(c) which concrete struct fields back the Python `to_variant()` payload** (payload mirrors the Rust structs closely per your guide).

#### 4.2.1 `TableScan` (root dependency + pushdown truth)

**Meaning:** leaf node producing rows from a `TableSource`.
**Fields (Rust):** `table_name`, `projection` (indices), `projected_schema`, `filters`, `fetch`. ([Docs.rs][3])
**Agent extraction:**

* dataset identity: `table_name` (normalize to your canonical `catalog.schema.table` key)
* executed column set: `projection` + `projected_schema`
* pushed filters: `filters` (scan-level predicate pushdown)
* scan-level row cap: `fetch` (limit pushdown)

**Python `to_variant()` usage:**

```python
v = lp.to_variant()
if type(v).__name__ == "TableScan":
    name = v.table_name
    proj = getattr(v, "projection", None)
    pushed = getattr(v, "filters", []) or []
    fetch = getattr(v, "fetch", None)
```

(Use `getattr` to tolerate non-exhaustive evolution.)

#### 4.2.2 `Projection` (output column → defining Expr)

**Meaning:** SELECT-list; evaluates `expr` list against input.
**Fields:** `expr: Vec<Expr>`, `input`, `schema`. ([Docs.rs][4])
**Agent extraction:**

* output column lineage: each `Expr`’s name is the output field name; store `Expr.canonical_name()` if available (for equivalence) and `Expr.display_name()` for schema-facing names (naming matters for downstream resolution).

#### 4.2.3 `Filter` (residual predicate; NOT scan pushdown)

**Meaning:** WHERE/HAVING-style boolean predicate evaluated above child.
**Fields:** `predicate`, `input`. Filter construction notes: aliases in predicate are removed in `try_new` (important for “why did my predicate lose aliases?” debugging). ([Docs.rs][5])
**Agent extraction:**

* residual predicate string form (use Expr canonical/print)
* required columns bump: `cols(predicate)` must be included in child-required set (for your required-col propagation pass)

#### 4.2.4 `Aggregate` (GROUP BY + aggregates; grouping-set edge cases)

**Fields:** `input`, `group_expr`, `aggr_expr`, `schema`. Also defines an internal grouping-id column constant used for grouping sets. ([Docs.rs][6])
**Agent extraction:**

* group keys: `group_expr`
* aggregate exprs: `aggr_expr`
* schema order invariant: output schema = group exprs followed by aggr exprs (useful when reconstructing projection lineage around aggregates)

#### 4.2.5 `Join` (two inputs + equijoin keys + optional non-equi filter)

**Fields:** `left`, `right`, `on: Vec<(Expr, Expr)>`, `filter: Option<Expr>`, `join_type`, `join_constraint`, `schema`, `null_equality`. ([Docs.rs][7])
**Agent extraction:**

* join type + constraint
* equijoin pairs (left_expr, right_expr)
* non-equi join filter (if present)
* required-col propagation: always include join key columns (+ join filter columns) split by provenance

Your guide’s Python extraction skeleton uses `v.on` and `v.filter` with `canonical_name()` to serialize stable join records. 

#### 4.2.6 `Sort` (ORDER BY + optional fetch)

**Fields:** `expr` (sort expressions), `input`, `fetch`. ([Docs.rs][8])
**Agent extraction:**

* sort keys (plus direction/null placement embedded in sort expr)
* `fetch` for “top-k” style operators (frequently paired with `Limit`)

#### 4.2.7 `Limit` (skip/fetch are *Expr*, not just integers)

**Fields:** `skip: Option<Box<Expr>>`, `fetch: Option<Box<Expr>>`, `input`. ([Docs.rs][9])
**Agent extraction:**

* treat `skip` / `fetch` as expressions: can be literals, params, or computed in some contexts; don’t assume integer at parse time.

#### 4.2.8 `Window` (OVER clause; output schema appends window expr outputs)

**Fields:** `input`, `window_expr`, `schema`. Output schema = input schema + window outputs; partition-by expressions are *not* output columns. ([Docs.rs][10])
**Agent extraction:**

* window expr list for required-col propagation: include any referenced columns from partition/order/frame expressions.

#### 4.2.9 `Repartition` (exchange in logical space)

**Fields:** `input`, `partitioning_scheme`. ([Docs.rs][11])
**Agent extraction:**

* treat as a “parallelism boundary” hint; required-col propagation still flows through; scheduler may use to align with physical partition count later.

#### 4.2.10 `Union` (variadic; schema compatibility rules)

Union is variadic and combines multiple inputs. Construction APIs include strict schema match, “loose types”, and “by name” variants. ([Docs.rs][12])
**Agent extraction:**

* required-col propagation applies identically to each input; correctness expects column alignment semantics (positional vs by-name differs if union-by-name used).

#### 4.2.11 `Subquery` / `SubqueryAlias` (must be traversed explicitly)

* `Subquery` holds a nested plan (`subquery`) and an `outer_ref_columns` list (correlation payload). ([Docs.rs][13])
* `SubqueryAlias` wraps an input plan and adds an alias with schema-qualified field names. ([Docs.rs][14])

**Critical traversal rule:** Rust explicitly notes `LogicalPlan::inputs()` **does not include subqueries**. ([Docs.rs][2])
Your attached guide calls subqueries the “gotcha” and requires scanning variant fields for embedded subplans beyond `inputs()`.

#### 4.2.12 `Explain` / `Analyze` (planning artifacts encoded as LogicalPlan nodes)

* `Explain`: outputs string representations of parts of the plan (implements SQL `EXPLAIN`).
* `Analyze`: runs the input and prints annotated physical plan with execution metrics (implements `EXPLAIN ANALYZE`). ([Docs.rs][2])
  Agents should treat these as *wrappers* whose child plan must be traversed for lineage; `Analyze` is execution-bearing.

---

### 4.3 Traversal & inspection (Python): tree-walk + variant match + subquery recursion

#### 4.3.1 Preorder walk (structural backbone): `inputs()`

```python
def walk_logical(lp):
    stack = [lp]
    while stack:
        node = stack.pop()
        yield node
        # structural children
        kids = node.inputs()  # list[LogicalPlan]
        stack.extend(reversed(kids))
```

`LogicalPlan.inputs()` is the official child accessor in Python. ([datafusion.apache.org][1])

#### 4.3.2 Variant inspection: `to_variant()` + `type(...).__name__`

This is the core operator discriminator endorsed in your guide. 

```python
def classify(lp):
    v = lp.to_variant()
    return type(v).__name__, v

for lp in walk_logical(root):
    variant, v = classify(lp)
    if variant == "TableScan":
        ...
    elif variant == "Filter":
        ...
```

Python’s `to_variant()` is explicitly “convert the logical plan into its specific variant.” ([datafusion.apache.org][1])

#### 4.3.3 Subquery recursion (mandatory if you want completeness)

Because `inputs()` does not guarantee to surface embedded subplans, add targeted recursion:

* if variant == `Subquery`: recurse into `v.subquery`
* if variant == `Explain`/`Analyze`: recurse into the wrapped plan field (name differs by binding/version; use reflective scan)

**Agent-safe reflective scan (plan-field discovery):**

```python
def looks_like_plan(x) -> bool:
    return hasattr(x, "inputs") and hasattr(x, "to_variant") and hasattr(x, "display_indent")

def extra_embedded_plans(v):
    # fast path: common field names
    for k in ("subquery", "plan", "input"):
        x = getattr(v, k, None)
        if looks_like_plan(x):
            yield x
    # slow path: scan attributes (avoid heavy dir() in hot loops if large plans)
    for k in getattr(v, "__dict__", {}).keys():
        x = getattr(v, k, None)
        if looks_like_plan(x):
            yield x

def walk_logical_complete(lp):
    stack = [lp]
    seen = set()  # optional: id()-based de-dupe
    while stack:
        node = stack.pop()
        if id(node) in seen:
            continue
        seen.add(id(node))
        yield node
        v = node.to_variant()
        stack.extend(reversed(node.inputs()))
        stack.extend(extra_embedded_plans(v))
```

This matches the explicit guidance in your doc: recurse into plan-like fields in addition to `inputs()`.

#### 4.3.4 Schema-aware inspection: `display_indent_schema()` as a fallback oracle

Your guide uses `display_indent_schema()` to reconstruct per-node schema evolution when direct schema objects are unavailable and aligns it with `display_indent()` via preorder position heuristics. ([datafusion.apache.org][1])

Minimal usage:

```python
indent_plan = lp.display_indent()
indent_schema = lp.display_indent_schema()
```

`display_indent_schema()` is a first-class method on Python `LogicalPlan`. ([datafusion.apache.org][1])

#### 4.3.5 Display + Graphviz: emitting machine-carriers for plan diffs

Python provides:

* `display_indent()` for stable-ish diffing in logs
* `display_graphviz()` for DOT graph output (renderable via Graphviz toolchain) ([datafusion.apache.org][1])

```python
dot = lp.display_graphviz()
with open("logical.dot", "w") as f:
    f.write(dot)
# shell: dot -Tsvg logical.dot > logical.svg
```

#### 4.3.6 Proto serialization (plan artifact; known limitation)

`LogicalPlan.to_proto()/from_proto(...)` exist in Python, but **tables created in-memory from record batches are not supported** for this serialization path. ([datafusion.apache.org][1])
(Operationally: prefer Substrait for portable plan artifacts; keep protobuf for same-engine debugging bundles.)

---

### 4.4 Traversal & matching (Rust): enum matching + TreeNode (preferred) + controlled rewrites

#### 4.4.1 Variant matching (direct enum match)

Rust `LogicalPlan` is an enum; canonical structure:

```rust
match plan {
  LogicalPlan::TableScan(ts) => { /* ts.table_name, ts.projection, ts.filters, ts.fetch */ }
  LogicalPlan::Filter(f) => { /* f.predicate */ }
  LogicalPlan::Join(j) => { /* j.on, j.filter, j.join_type */ }
  _ => {}
}
```

Variant set is enumerated in the `LogicalPlan` docs. ([Docs.rs][2])

**Non-exhaustive structs (forward-compat discipline):**
Operator structs like `Filter` / `Projection` are marked `#[non_exhaustive]`; you cannot destructure without `..` and should avoid constructing them directly (use `try_new`). ([Docs.rs][5])

#### 4.4.2 Structural children vs embedded subqueries

Rust `LogicalPlan::inputs()` returns only direct children and explicitly **does not include subqueries**. ([Docs.rs][2])
So: for full plan closure, either:

* explicitly handle `LogicalPlan::Subquery(sq)` and traverse `sq.subquery`, or
* use the TreeNode “with_subqueries” APIs (below) when appropriate. ([Docs.rs][2])

#### 4.4.3 TreeNode API (preferred): inspect and rewrite with recursion control

`LogicalPlan` implements `TreeNode`; the TreeNode trait provides:

* `apply` (top-down preorder)
* `transform_up` / `transform_down` / `transform_down_up`
* `rewrite` with `TreeNodeRewriter`
  with `TreeNodeRecursion` controlling early termination and pruning. ([Docs.rs][15])

**Inspect: collect expressions referenced by nodes**
The `LogicalPlan` docs show walking the plan and using `apply_expressions` to collect expressions. ([Docs.rs][2])

**Rewrite: transform a Filter predicate**
The `LogicalPlan` docs demonstrate `transform` to replace a Filter predicate and then validate via `display_indent()`. ([Docs.rs][2])

#### 4.4.4 `expressions()` vs TreeNode: avoid cloning pitfalls

`LogicalPlan::expressions()` returns expressions **non-recursively** and clones; the docs recommend using TreeNode APIs when possible. ([Docs.rs][2])

---

### 4.5 Display helpers (logical plan renderers; both Python and Rust)

**Python `LogicalPlan`:** `display()`, `display_indent()`, `display_indent_schema()`, `display_graphviz()` ([datafusion.apache.org][1])
**Rust `LogicalPlan`:** `display_indent`, `display_indent_schema`, `display_graphviz`, `display_pg_json` are part of the logical plan API surface. ([Docs.rs][2])

**When to use which:**

* `display_indent()` → loggable canonical-ish carrier for regression diffs (one operator per line, stable ordering)
* `display_indent_schema()` → schema evolution trace for required-col propagation, and for diagnosing name qualification after joins/aliases
* `display_graphviz()` → debugging complex join trees / subquery nests where indentation is insufficient
* `display_pg_json()` (Rust) → feed external plan viewers that expect structured JSON

---


[1]: https://datafusion.apache.org/python/autoapi/datafusion/plan/index.html "datafusion.plan — Apache Arrow DataFusion  documentation"
[2]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html "LogicalPlan in datafusion_expr::logical_plan - Rust"
[3]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.TableScan.html "TableScan in datafusion::logical_expr - Rust"
[4]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/struct.Projection.html "Projection in datafusion_expr::logical_plan - Rust"
[5]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/struct.Filter.html "Filter in datafusion_expr::logical_plan - Rust"
[6]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/struct.Aggregate.html "Aggregate in datafusion_expr::logical_plan - Rust"
[7]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/struct.Join.html "Join in datafusion_expr::logical_plan - Rust"
[8]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/struct.Sort.html "Sort in datafusion_expr::logical_plan - Rust"
[9]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/struct.Limit.html "Limit in datafusion_expr::logical_plan - Rust"
[10]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/struct.Window.html "Window in datafusion_expr::logical_plan - Rust"
[11]: https://docs.rs/datafusion/latest/datafusion/logical_expr/logical_plan/struct.Repartition.html "Repartition in datafusion::logical_expr::logical_plan - Rust"
[12]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Union.html "Union in datafusion::logical_expr - Rust"
[13]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Subquery.html "Subquery in datafusion::logical_expr - Rust"
[14]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/struct.SubqueryAlias.html "SubqueryAlias in datafusion_expr::logical_plan - Rust"
[15]: https://docs.rs/datafusion/latest/datafusion/common/tree_node/trait.TreeNode.html "TreeNode in datafusion::common::tree_node - Rust"

According to a document from **January 27, 2026**, the most operationally correct framing is: **binding + analyzer are the plan-validity phases** that must be treated as part of your “plan environment” and therefore must be *observable* (via plan output) and *controllable* (via session config / SQL planner knobs / prepared statements) for any agent that is expected to generate correct DataFusion plans repeatedly. ([Docs.rs][1])

---

## 5) Analyzer & binding (turning “names” into “bound” plans)

### 5.0 Phase boundary: `SqlToRel` binding vs AnalyzerRules

**Binding** in DataFusion is performed by the SQL planner/binder **`SqlToRel`**: it converts SQL AST → `LogicalPlan` and performs **name and type resolution** (“binding”), looking up tables/columns/functions via a **`ContextProvider`**. It explicitly **does not do type coercion or optimization**; those are subsequent passes. ([Docs.rs][2])

**Analyzer** then rewrites the (already bound) `LogicalPlan` to make it semantically valid (e.g., **type coercion**, subquery reference resolution). `AnalyzerRule` is explicitly defined as “make the plan valid prior to the rest of optimization.” ([Docs.rs][3])

**Agent invariant:** when debugging “why did it fail / why is the plan different,” partition causes into:

1. **Binding failure** (missing table/function, ambiguous/unknown column, wrong default catalog/schema, quoting/normalization mismatch)
2. **Analyzer rewrite** (implicit casts, join/union coercions, function rewrites)
3. **Optimizer rewrite** (pushdown, reordering) — *not this section*

---

## 5.1 Name resolution (tables / columns / views / CTE) — *what binds to what*

### 5.1.1 Identifier normalization (case-folding) is a *planning-time* rule

DataFusion’s SQL behavior: **unquoted identifiers are normalized to lowercase** (and config-controlled). The SQL reference warns that column names in queries are made lower-case; to access capitalized fields, **use double quotes**. ([datafusion.apache.org][4])

Config knob (global): `datafusion.sql_parser.enable_ident_normalization` (default `true`) “convert ident to lowercase when not quoted.” ([datafusion.apache.org][5])

Planner primitive: `object_name_to_table_reference(object_name, enable_normalization)` shows the exact normalization semantics, including “`['foo','Bar'] -> schema=foo, table=bar`” vs `['foo','"Bar"'] -> table=Bar`. ([Docs.rs][6])

**Agent rule (portable SQL generation):**

* If an identifier must preserve case / contains punctuation: always emit `"QuotedIdent"`.
* Otherwise: emit lowercase identifiers and rely on normalization.

### 5.1.2 Table name binding: `catalog.schema.table` + defaults

DataFusion binds table references against the session’s catalog hierarchy; SQL that omits qualifiers relies on **default catalog** and **default schema** settings. ([datafusion.apache.org][5])

Config keys (default binding universe):

* `datafusion.catalog.default_catalog` (default `datafusion`)
* `datafusion.catalog.default_schema` (default `public`)
  These “impact what SQL queries use if not specified.” ([datafusion.apache.org][5])

**Agent procedure: deterministic table binding (Python)**

```python
from datafusion import SessionContext, SessionConfig

cfg = (
    SessionConfig()
    .with_default_catalog_and_schema("datafusion", "public")  # pin resolution
    .with_information_schema(True)                            # allow SHOW*/df_settings
)
ctx = SessionContext(cfg)

# Assert bindable namespace:
ctx.sql("SHOW TABLES").show()
ctx.sql("SHOW COLUMNS FROM information_schema.df_settings").show()
```

(Defaults + info schema are session configuration concerns; the engine docs explicitly treat SessionContext as “state needed to create LogicalPlans such as the table definitions and function registries.”) ([Docs.rs][1])

### 5.1.3 Views and CTE are planner-scoped bindings (not “just macros”)

DataFusion’s SQL planner maintains a `PlannerContext` that stores state used to resolve **CTEs, Views, subqueries, and PREPARE statements**, including parameter data types and outer query schema; it also documents that cloning is used to handle CTE scoping (subqueries inherit outer CTEs but can define private CTEs). ([Docs.rs][7])

**Agent procedure: view binding + inspection (Python)**

```python
import pyarrow as pa
from datafusion import SessionContext

ctx = SessionContext()

# Build a typed input and register as a view name.
t = pa.Table.from_pydict({"Key": [1, 2], "v": [10, 20]})
df = ctx.from_arrow(t)
ctx.register_view("V", df)

# Binding requires quotes for Key because SQL identifiers are lowercased when unquoted
q = ctx.sql('SELECT "Key", v FROM V WHERE "Key" > 1')
print(q.logical_plan().display_indent_schema())
```

* `register_view` makes the name resolvable by the SQL binder (table-like reference).
* `display_indent_schema()` is the fastest “did the binder resolve what I meant?” oracle: it exposes qualifiers/field names after aliasing/expansion. ([datafusion.apache.org][8])

**CTE binding** is also pure planner context:

```sql
WITH x AS (SELECT a, max(b) AS b FROM t GROUP BY a)
SELECT a, b FROM x;
```

This is explicitly in the SELECT syntax reference and is resolved by the planner context machinery. ([datafusion.apache.org][4])

### 5.1.4 Column resolution and qualification strategy

Binding of column references is part of `SqlToRel`’s “name and type resolution.” ([Docs.rs][2])

**Agent rules (avoid ambiguity):**

* In joins, always qualify: `t1.col`, `t2.col` (or table aliases).
* For capitalized column names: `"Col"` (double quotes) per DataFusion SQL guidance. ([datafusion.apache.org][4])

**Inspection tactic:** after planning, check `SubqueryAlias` / `Join` / `Projection` nodes and their schema rendering:

```python
lp = ctx.sql("SELECT ...").logical_plan()
print(lp.display_indent_schema())  # validate qualification + resolved field names
```

(Using plan renderers as “binding proofs” is a core extraction strategy in your attached guide; bind correctness before downstream lineage/required-col computations.)

### 5.1.5 Function name binding uses the same ContextProvider as tables

`ContextProvider` is the abstraction through which `SqlToRel` resolves **table sources** and **function metadata**, without depending directly on catalog internals. It exposes `get_table_source(...)`, `get_function_meta(...)` (scalar), `get_aggregate_meta(...)`, `get_window_meta(...)`. ([Docs.rs][9])

**Agent implication:** if SQL planning says “function not found,” the fix is never “optimizer”; it is:

* register UDF/UDAF/UDWF/UDTF into the session **before** calling `ctx.sql(...)`, or
* supply a ContextProvider (Rust embedding) that returns that function meta.

---

## 5.2 Type coercion & expression normalization — where implicit casts appear, and how to observe them

### 5.2.1 Coercion is *not* binding; it is an AnalyzerRule pass

`SqlToRel` explicitly does **not** perform type coercion. ([Docs.rs][2])
Coercion is performed by the Analyzer phase: DataFusion’s planning overview states `LogicalPlan` is checked/rewritten by **AnalyzerRules** “such as type coercion.” ([Docs.rs][1])

Mechanism: `TypeCoercionRewriter` rewrites expressions to apply coercion; it has specialized logic for:

* `coerce_join`: join equality expressions stored as parallel `(left_expr, right_expr)` pairs; coercion must preserve alignment. ([Docs.rs][10])
* `coerce_union`: coerces inputs to a common schema after wildcard expansion. ([Docs.rs][10])

### 5.2.2 How to *force* and *see* implicit casts (Python)

Use typed Arrow inputs, then inspect the logical plan rendering. DataFusion’s Arrow IO surface provides `ctx.from_arrow(table)` for typed tables. ([datafusion.apache.org][8])

**Join key coercion example (Int32 ⋈ Int64)**

```python
import pyarrow as pa
from datafusion import SessionContext

ctx = SessionContext()

t1 = pa.Table.from_arrays([pa.array([1, 2], type=pa.int32()),
                           pa.array([10, 20], type=pa.int64())],
                          names=["k", "v"])
t2 = pa.Table.from_arrays([pa.array([1, 2], type=pa.int64()),
                           pa.array([100, 200], type=pa.int64())],
                          names=["k", "w"])

ctx.register_view("t1", ctx.from_arrow(t1))
ctx.register_view("t2", ctx.from_arrow(t2))

df = ctx.sql("SELECT * FROM t1 JOIN t2 ON t1.k = t2.k")
print(df.logical_plan().display_indent())         # look for CAST(...) around join keys
print(df.logical_plan().display_indent_schema())  # schema confirms coerced types
```

Interpretation: any `CAST` insertion you observe here is analyzer-driven type coercion (pre-optimizer).

**Union coercion example (heterogeneous input schemas)**

```python
df = ctx.sql("""
SELECT CAST(1 AS INT) AS x
UNION ALL
SELECT CAST(1.5 AS DOUBLE) AS x
""")
print(df.logical_plan().display_indent_schema())  # expect a common supertype in output schema
```

(Union coercion is explicitly a dedicated coercion path.) ([Docs.rs][10])

### 5.2.3 Coercion strictness control: ANSI mode is an execution+expression semantics switch

Config `datafusion.execution.enable_ansi_mode` changes casting/type coercion semantics, including “strict type coercion rules” (implicit casts between incompatible types disallowed) and error handling behavior. ([datafusion.apache.org][5])

**Agent policy suggestion:** if you are generating SQL from an LLM and want **fail-fast** semantics (no silent coercions), set ANSI mode in the session config and treat any error as a prompt to add explicit `CAST(...)`.

### 5.2.4 Expression normalization knobs that change inferred types before coercion

These are SQL parser/planning-time type decisions and thus change the bound plan:

* `datafusion.sql_parser.parse_float_as_decimal` (parse floats as decimal) ([datafusion.apache.org][5])
* `datafusion.sql_parser.map_string_types_to_utf8view` (map VARCHAR/CHAR/Text/String to Utf8View) ([datafusion.apache.org][5])
* `datafusion.sql_parser.collect_spans` (record source spans in logical plan nodes; improves error localization) ([datafusion.apache.org][5])

**Agent rule:** treat these as part of the plan fingerprint. If they change, coercion/cast structure can change without SQL text changes.

---

## 5.3 Parameterized expressions — how values become literals/typed params, and plan stability

### 5.3.1 SQL PREPARE / EXECUTE: *plan template* with placeholders

DataFusion SQL supports prepared statements: `PREPARE name(types...) AS <sql with $1, $2, ...>` then `EXECUTE name(values...)`. Types can be declared or inferred at execution. ([datafusion.apache.org][11])

**Core syntax**

```sql
PREPARE greater_than(INT) AS SELECT * FROM example WHERE a > $1;
EXECUTE greater_than(20);
```

Multiple positional args supported: `PREPARE greater_than(INT, DOUBLE) ... WHERE a > $1 AND b > $2;` ([datafusion.apache.org][11])

**Planner mechanics:** planner context explicitly tracks PREPARE parameter data types as part of its state. ([Docs.rs][7])

**Plan-stability implication (agent-critical):** prepared statements are the first-class way to separate **plan shape** (placeholder plan) from **runtime values**, enabling stable caching keys and repeated execution; your attached doc calls this out explicitly. 

### 5.3.2 DataFusion-Python parameterized SQL: two modes with different stability + safety properties

Python adds named placeholders `$name` (DataFusion-Python 51.0.0). It supports:

#### Mode A: **named_params** (string/DataFrame substitution)

* Converts Python objects to **string representations**.
* Special case: if a parameter is a `DataFrame`, DataFusion registers a **temporary view** with a generated name; requires temp-view support in your catalog/schema implementation. ([datafusion.apache.org][12])

```python
ctx.sql('SELECT "Name" FROM pokemon WHERE "Attack" > $val', val=75)
ctx.sql('SELECT "Name", $col FROM $df WHERE $col > $val',
        col='"Defense"', df=ctx.table("pokemon"), val=75)
```

Constraints:

* `$name` placeholder style does not work for `hive` and `mysql` dialects (no named placeholders). ([datafusion.apache.org][12])

**Agent rule:** treat this as **macro expansion** (powerful, but can affect SQL parsing/type inference). Use it for identifiers / DataFrame injection, not for high-fidelity numeric correctness.

#### Mode B: **param_values** (typed scalar substitution)

`param_values` avoids string conversion; values are cast into a **PyArrow ScalarValue** and are limited to scalar-value positions (e.g., comparisons), relying on the configured SQL dialect. ([datafusion.apache.org][12])

```python
ctx.sql(
  'SELECT "Name", "Attack" FROM pokemon WHERE "Attack" > $val',
  param_values={"val": 75},
)
```

**Agent rule:** use `param_values` whenever numeric fidelity matters (floats/decimals/timestamps) or when you want predictable type inference. ([datafusion.apache.org][12])

### 5.3.3 Plan stability and cache keys (what an agent should actually do)

You have three distinct regimes:

1. **Prepared statements (SQL PREPARE/EXECUTE)**

   * Stable plan template; runtime values applied at EXECUTE. ([datafusion.apache.org][11])
   * Best for repeated execution with varying parameters.

2. **Python `param_values`**

   * Typed substitution; still produces a planned query per call (values become part of the planned expression tree). The docs frame it as “similar to prepared statements” but implemented as parameter casting/substitution at the Python API boundary. ([datafusion.apache.org][12])
   * If you need stable fingerprints, normalize literal scalars out of your extracted `Expr.canonical_name()` strings (or prefer PREPARE).

3. **Python `named_params`**

   * String conversion can alter parsing / type inference; treat as non-stable unless you canonicalize aggressively. ([datafusion.apache.org][12])

Your attached guide explicitly positions “prepared statements / parameterized queries” as the official separation between plan shape and runtime values for orchestration and caching. 

---

### Minimal “binding + analyzer audit bundle” (agent output contract)

For any query the agent generates, persist:

1. **Session binding knobs**:

   * `datafusion.catalog.default_catalog/default_schema` ([datafusion.apache.org][5])
   * `datafusion.sql_parser.enable_ident_normalization`, dialect, parse_float_as_decimal, map_string_types_to_utf8view, collect_spans ([datafusion.apache.org][5])
   * `datafusion.execution.enable_ansi_mode` ([datafusion.apache.org][5])

2. **Binding proof**: `df.logical_plan().display_indent_schema()` (post-binding, post-analyzer, pre-optimizer notionally; compare against optimized for optimizer effects)

3. **Coercion proof**: scan plan text for `CAST` / schema changes across nodes; for joins/unions expect coercion behavior consistent with TypeCoercionRewriter semantics. ([Docs.rs][10])

4. **Parameter regime**: record whether the query used PREPARE/EXECUTE, Python `param_values`, or macro-style `$name` substitution; this determines whether literal values should be redacted during fingerprinting. ([datafusion.apache.org][11])

[1]: https://docs.rs/datafusion/latest/datafusion/ "datafusion - Rust"
[2]: https://docs.rs/datafusion-sql/latest/datafusion_sql/planner/struct.SqlToRel.html "SqlToRel in datafusion_sql::planner - Rust"
[3]: https://docs.rs/datafusion/latest/datafusion/optimizer/analyzer/trait.AnalyzerRule.html "AnalyzerRule in datafusion::optimizer::analyzer - Rust"
[4]: https://datafusion.apache.org/user-guide/sql/select.html "SELECT syntax — Apache DataFusion  documentation"
[5]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[6]: https://docs.rs/datafusion-sql/latest/datafusion_sql/planner/fn.object_name_to_table_reference.html "object_name_to_table_reference in datafusion_sql::planner - Rust"
[7]: https://docs.rs/datafusion-sql/latest/datafusion_sql/planner/struct.PlannerContext.html "PlannerContext in datafusion_sql::planner - Rust"
[8]: https://datafusion.apache.org/python/user-guide/io/arrow.html "Arrow — Apache Arrow DataFusion  documentation"
[9]: https://docs.rs/datafusion/latest/datafusion/sql/planner/trait.ContextProvider.html "ContextProvider in datafusion::sql::planner - Rust"
[10]: https://docs.rs/datafusion/latest/datafusion/optimizer/analyzer/type_coercion/struct.TypeCoercionRewriter.html "TypeCoercionRewriter in datafusion::optimizer::analyzer::type_coercion - Rust"
[11]: https://datafusion.apache.org/user-guide/sql/prepared_statements.html "Prepared Statements — Apache DataFusion  documentation"
[12]: https://datafusion.apache.org/python/user-guide/sql.html "SQL — Apache Arrow DataFusion  documentation"

According to a document from **January 27, 2026**, DataFusion’s planning pipeline treats **optimizer rules** as the stage that injects the highest-value *lineage + I/O-shape* rewrites (notably **projection + filter pushdown**) into the **optimized logical plan**, before physical planning. This is why optimized logical is the correct “backbone artifact” to diff and to mine for scan contracts. 

---

## 6) Logical optimization (OptimizerRule pipeline)

### 6.1 Mental model: optimizer = ordered rewrite pipeline over `LogicalPlan` + `Expr`

**Core model**: `LogicalPlan -> LogicalPlan` under a sequence of `OptimizerRule`s, iterated to a **fixpoint** (bounded by a max pass count). Rules may:

* rewrite expressions (e.g., constant folding / algebraic simplification),
* move operators (pushdown),
* normalize plan structure (remove redundant nodes),
* decorrelate / transform subqueries into join forms.

**Control knobs (runtime)**:

* `datafusion.optimizer.max_passes` (fixpoint attempt bound) and `datafusion.optimizer.skip_failed_rules` (error policy for rule failures). ([datafusion.apache.org][1])
  Operational semantics: `skip_failed_rules=false` makes optimizer failures hard-fail; `true` converts rule errors into warnings and continues. ([datafusion.apache.org][1])

**Provenance / debugging surface**: `EXPLAIN VERBOSE` prints the plan after each rule (logical + physical optimizer stages), giving you the **actual rulepack and ordering** for the running engine build. ([datafusion.apache.org][2])

---

### 6.2 How to *prove* each transformation: required artifacts + comparison operators

#### 6.2.1 Minimal artifact bundle (Python, non-executing)

```python
df = ctx.sql(sql)

p0 = df.logical_plan().display_indent()            # unoptimized logical
p1 = df.optimized_logical_plan().display_indent()  # optimized logical

# optional schema oracle (binding+rewrite validation)
s0 = df.logical_plan().display_indent_schema()
s1 = df.optimized_logical_plan().display_indent_schema()
```

The attached guide treats this `{p0,p1}` pair as the canonical proof that optimizer rewrites occurred and as the stable basis for lineage/scheduling extraction. 

#### 6.2.2 Rule-by-rule proof (SQL; strongest “ground truth”)

* `EXPLAIN VERBOSE <statement>` emits rows like: `initial_logical_plan`, `logical_plan after <rule_name>`, `initial_physical_plan`, `physical_plan after <rule_name>`, etc. ([datafusion.apache.org][3])
* `EXPLAIN VERBOSE` supports only `indent` format (do not attempt `FORMAT tree` with verbose). ([datafusion.apache.org][4])
* Default explain format is controlled by `datafusion.explain.format` (`indent` vs `tree`), but verbose overrides to indent-only. ([datafusion.apache.org][4])

**Agent pattern:**

```python
exp = ctx.sql("EXPLAIN VERBOSE " + sql)
exp.show()                 # human proof
# if you need machine proof: collect rows and persist `plan_type -> plan` mapping
```

---

### 6.3 High-value rule families (what they do, how to detect, what to assert)

#### 6.3.1 Projection pushdown (column pruning)

**Goal**: move column selection “down” so scans only materialize required columns; for columnar sources this reduces I/O, decode, and downstream vector bandwidth.

**Where it manifests (logical plan)**:

* `TableScan` variant fields include `projection` (indices) and `projected_schema`; this is the optimizer-visible signature of successful projection pushdown. 
* Explain text commonly shows `TableScan ... projection=[...]` when pushdown succeeded. 

**How to detect (Python; structured)**

```python
lp = df.optimized_logical_plan()

def find_scans(lp):
    out = []
    stack = [lp]
    while stack:
        n = stack.pop()
        v = n.to_variant()
        if type(v).__name__ == "TableScan":
            out.append(v)
        stack.extend(n.inputs())
    return out

for scan in find_scans(lp):
    # scan.projection == None implies "read all columns"
    print(scan.table_name, scan.projection, scan.projected_schema)
```

**Assertion contract (scheduler / lineage)**:

* `scan.projection is not None` ⇒ treat as **executed required columns truth** (but still compute your own required-cols for stability/regressions). 

**Failure modes you should explicitly test**:

* `SELECT *` patterns + downstream expressions that reference many columns ⇒ optimizer cannot prune.
* UDFs with broad input requirements or opaque expressions ⇒ column pruning may degrade.
* If projection pushdown regresses, your required-cols computation should detect the delta even if outputs remain correct. 

---

#### 6.3.2 Predicate pushdown (filter placement + scan-time filter contracts)

**Goal**: push predicates closer to the scan so the engine can reduce row volume earlier and enable scan-level pruning (row groups, partitions, late materialization).

**Two distinct “pushdown layers” you must separate**:

1. **Plan-level predicate placement (logical optimizer)**

   * `Filter` operators can be pushed downward (rule typically named `filter_push_down` in verbose output). ([datafusion.apache.org][3])
   * Scan nodes may carry pushed predicates as `partial_filters` in explain output (indicating predicates attached to scan). 

2. **Data source predicate capability (TableProvider contract)**

   * Providers can classify filters as Unsupported / Exact / Inexact; Inexact implies a residual correctness filter remains above the scan even if a predicate is applied at scan. (This is why “pushdown” is not boolean.) 

**How to detect pushdown status (agent rule)**

* Compare:

  * logical residual `Filter` nodes (post-optimizer),
  * scan `partial_filters` (logical explain),
  * and/or physical scan `predicate=` presence vs `FilterExec` above scan (physical explain). 

**Concrete test harness (SQL)**

```sql
EXPLAIN VERBOSE
SELECT * FROM t
WHERE (a > 10 AND b = 'x' AND expensive_udf(c) > 0);
```

Interpretation expectations:

* `a > 10` and `b = 'x'` frequently become scan-level `partial_filters` for many providers.
* `expensive_udf(c) > 0` may remain as a residual `Filter` unless the provider/executor supports it.

**Parquet-specific “late materialization” (not logical, but affects scan behavior)**:

* `datafusion.execution.parquet.pushdown_filters=true` applies filters during Parquet decoding (often described as late materialization). ([datafusion.apache.org][1])
  Treat this as **execution-layer filter pushdown**: it changes physical behavior and `EXPLAIN ANALYZE` metrics, but is not the same as logical `filter_push_down`. ([datafusion.apache.org][1])

---

#### 6.3.3 Constant folding / expression simplification (rewrite `Expr` to cheaper `Expr`)

**Goal**: reduce runtime compute by pre-evaluating constant subtrees and applying algebraic/boolean simplifications.

**Rule identity**:

* `simplify_expressions` is a dedicated optimizer pass for rewriting expressions (constant evaluation + algebraic simplifications). ([Docs.rs][5])

**How to prove folding happened**:

* Use `EXPLAIN VERBOSE` and check `logical_plan after simplify_expressions` becomes a literal expression where appropriate (DataFusion docs show examples where arithmetic + casts are reduced to a literal). ([datafusion.apache.org][2])

**UDF volatility is a first-class precondition**:

* Volatility determines optimization eligibility; define UDF volatility as strict as possible (Immutable > Stable > Volatile) to maximize simplification/folding opportunities. ([Docs.rs][6])

**Agent checklist**:

* If an expression *should* fold but doesn’t:

  * check volatility of any involved UDFs,
  * check implicit cast insertion (type coercion) changed the expression shape (visible in verbose output as `type_coercion` stage preceding simplification). ([datafusion.apache.org][3])

---

#### 6.3.4 Join reordering / join rewrite family (logical) vs join strategy selection (physical)

DataFusion advertises join reordering as an optimization feature (logical). ([datafusion.apache.org][7])
In practice, the **observable logical optimizer effects** in verbose output include rules such as:

* `reduce_cross_join` (turn cross join + filter into join),
* `reduce_outer_join` (outer join reduction where predicates imply null-rejection),
* subquery-to-join rewrites (`scalar_subquery_to_join`, `subquery_filter_to_join`, decorrelation passes). ([datafusion.apache.org][3])

**How to prove join rewrites**:

* `EXPLAIN VERBOSE` will show the join graph shape changing after these rule names (look at Join nodes and their predicates). ([datafusion.apache.org][3])

**Join strategy selection is *not* logical** (but users conflate it):

* Physical optimizer includes a `join_selection` stage (visible in verbose output as `physical_plan after join_selection`). ([datafusion.apache.org][3])
* Physical join algorithm preferences are configured via `datafusion.optimizer.prefer_hash_join`, `datafusion.optimizer.enable_piecewise_merge_join`, etc. ([datafusion.apache.org][1])
  Interpretation: logical rewrites decide *what join*, physical rewrites decide *how to execute the join*.

**Agent tactic for deterministic plan debugging**:

* When correctness is the issue → diff `p0` vs `p1` (logical).
* When performance is the issue → inspect physical changes and join algorithm knobs (`prefer_hash_join`, row estimates, memory limits). ([datafusion.apache.org][1])

---

#### 6.3.5 Common subexpression elimination (CSE) + redundant operator elimination

**Goal**: avoid recomputing the same expression multiple times; remove no-op filters/limits; collapse redundant projections.

**Rule identity**:

* `common_sub_expression_eliminate` is explicitly shown as a logical optimizer stage in verbose output. ([datafusion.apache.org][3])
* Related elimination rules appear in verbose output (e.g., `eliminate_filter`, `eliminate_limit`). ([datafusion.apache.org][3])

**How to detect (practically)**:

1. Write a query with repeated expensive expression:

   ```sql
   SELECT expensive_udf(a) AS x,
          expensive_udf(a) + 1 AS y
   FROM t;
   ```
2. Compare:

   * unoptimized logical: expression appears duplicated,
   * optimized logical: expression may be computed once and referenced (plan structure depends on version).

**Precondition**: volatility and determinism; volatile expressions should not be CSE’d across evaluation sites. ([Docs.rs][6])

---

### 6.4 Optimizer observability and rulepack extraction (agent-grade)

#### 6.4.1 Extract the effective rulepack and ordering (no guessing)

* Run `EXPLAIN VERBOSE` for a representative query and parse the `plan_type` column to obtain:

  * list of logical stages (`logical_plan after <name>`),
  * list of physical stages (`physical_plan after <name>`). ([datafusion.apache.org][3])
    This gives a **version-accurate**, engine-provided inventory.

#### 6.4.2 Lock down optimizer nondeterminism and failure behavior

* Set optimizer pass bound and failure policy explicitly:

  ```sql
  SET datafusion.optimizer.max_passes = 3;
  SET datafusion.optimizer.skip_failed_rules = false;
  ```

  ([datafusion.apache.org][1])

---

### 6.5 Rust-only: controlling / extending the logical optimizer pipeline (when default rules are insufficient)

Python bindings are primarily **consumer surfaces** (plan construction + explain + plan objects). For **optimizer mutation**, use Rust.

**SessionContext hooks**:

* `SessionContext::add_optimizer_rule(Arc<dyn OptimizerRule>)`
* `SessionContext::remove_optimizer_rule(name)` ([Docs.rs][8])

**Custom rewrite authoring workflow (canonical)**:

* Implement a rewrite over `Expr` using TreeNode transforms (example in DataFusion optimizer docs shows rewriting `BETWEEN` into range comparisons), then wrap in an OptimizerRule and register. ([datafusion.apache.org][3])

**Agent guidance**:

* Prefer custom logical rules when you need semantic rewrites (e.g., canonicalizing predicates for deterministic scheduling keys).
* Prefer config knobs when you need execution-policy shifts (e.g., hash vs sort-merge join preference). ([datafusion.apache.org][1])

---

## Quick “optimizer proof” checklist (what an agent should do per query)

1. Capture `p0 = df.logical_plan().display_indent()` and `p1 = df.optimized_logical_plan().display_indent()`; diff for structural proof. 
2. Capture `EXPLAIN VERBOSE <sql>` and persist `plan_type -> plan` rows; this is the authoritative rewrite trace. ([datafusion.apache.org][2])
3. For pushdowns, assert:

   * scan carries projection indices (`TableScan.projection`) and pushed filters (`partial_filters` / scan filters),
   * residual `Filter` nodes remain only when pushdown is partial/inexact. 
4. If optimizer behavior is unstable across runs, pin `datafusion.optimizer.max_passes` and set `skip_failed_rules=false` to surface failures early. ([datafusion.apache.org][1])

[1]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[2]: https://datafusion.apache.org/library-user-guide/query-optimizer.html?utm_source=chatgpt.com "Query Optimizer — Apache DataFusion documentation"
[3]: https://datafusion.apache.org/_sources/library-user-guide/query-optimizer.md.txt?utm_source=chatgpt.com "query-optimizer.md.txt - Apache DataFusion"
[4]: https://datafusion.apache.org/user-guide/sql/explain.html?utm_source=chatgpt.com "EXPLAIN — Apache DataFusion documentation"
[5]: https://docs.rs/datafusion/latest/datafusion/optimizer/simplify_expressions/simplify_exprs/index.html?utm_source=chatgpt.com "datafusion::optimizer::simplify_expressions::simplify_exprs"
[6]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Volatility.html?utm_source=chatgpt.com "Volatility in datafusion::logical_expr - Rust"
[7]: https://datafusion.apache.org/user-guide/features.html?utm_source=chatgpt.com "Features — Apache DataFusion documentation"
[8]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html?utm_source=chatgpt.com "SessionContext in datafusion::execution::context - Rust"

According to a document from **January 27, 2026** (your planning guide + addendum) and the current upstream DataFusion docs, **statistics/metadata are first-class planning inputs**: they directly parameterize (a) **scan pruning** (row group/page skip + file grouping) and (b) **join planning** (selectivity/cost estimates, join-side decisions, dynamic filters), and they are only “useful” if you can **(1) ensure they exist** and **(2) prove the engine is consuming them** via plan+settings artifacts. ([datafusion.apache.org][1])

---

## 7) Statistics & metadata as planning inputs

### 7.1 DataFusion’s statistics model: what is represented, where it is consumed

#### 7.1.1 Canonical container: `datafusion::common::Statistics`

Rust struct (re-exported across engine crates) has three fields:

* `num_rows: Precision<usize>`
* `total_byte_size: Precision<usize>`
* `column_statistics: Vec<ColumnStatistics>`
  and explicitly states values may be **optional/inexact**; `total_byte_size` is **output Arrow bytes**, not bytes scanned from storage. ([Docs.rs][2])

Key methods you should recognize (because plan nodes call these internally and custom providers must emulate semantics):

* `Statistics::new_unknown(schema)` → produce “unknown” stats for every column (the “flying blind” baseline). ([Docs.rs][2])
* `Statistics::to_inexact()` → downgrade any Exact stats when plan transforms destroy exactness (common after filter pushdown / partial pruning). ([Docs.rs][2])
* `Statistics::project(projection)` → column-prune stats (must stay index-consistent with `TableScan.projection`). ([Docs.rs][2])
* `Statistics::with_fetch(fetch, skip, n_partitions)` → adjust stats for `LIMIT/OFFSET`-style operators (relevant when proving TopK/limit pushdown effects). ([Docs.rs][2])

#### 7.1.2 `Precision<T>`: planning cannot assume “exact”

DataFusion’s optimizer documentation explicitly wraps each statistic in `Precision` “exact or estimated,” and uses this to reason about the reliability of cardinality estimates. ([datafusion.apache.org][1])

Agent rule: **any pipeline that consumes stats must propagate exactness**; never treat `num_rows` as an integer unless `Precision::Exact`.

#### 7.1.3 Column-level stats actually used for selectivity & pruning

DataFusion’s optimizer guide states `ColumnStatistics` includes (at least):

* null counts
* min/max
* sums (numeric)
* distinct counts
  and ties these directly to selectivity estimation and optimization decisions. ([datafusion.apache.org][1])

Where consumed:

* **Selectivity / cost**: DataFusion uses a cost based model relying on table+column stats, with boundary analysis (interval arithmetic) for predicates to infer selectivity (planning input to join/filter costing). ([datafusion.apache.org][1])
* **Pruning**: `PruningPredicate` uses min/max/null_count to prove “container can never match predicate” and skip entire containers such as Parquet row groups; API is general and supports non-Parquet containers if they implement `PruningStatistics`. ([Docs.rs][3])

---

### 7.2 Where statistics come from in lake-style sources (and the knobs that decide if they exist)

#### 7.2.1 ListingTable “registration-time statistics” (`collect_statistics`)

For the built-in `ListingTableProvider` path (CREATE EXTERNAL TABLE / directory-of-files tables), **statistics collection is done when the table is created**, is **config-gated**, and has **no effect after creation**:

* Config: `datafusion.execution.collect_statistics` default `true` (ListingTableProvider only). ([datafusion.apache.org][4])
* SQL guidance: if you don’t want the up-front cost, set it **before** `CREATE EXTERNAL TABLE`; otherwise DataFusion will read files to gather stats and “accelerate subsequent queries substantially.” ([datafusion.apache.org][5])
* Your addendum: emphasizes this is a planning artifact with measurable downstream plan quality impact, and notes version behavior change (default true in newer releases). ([datafusion.apache.org][4])

**Agent procedure (SQL)**

```sql
-- pin behavior BEFORE creating the table
SET datafusion.execution.collect_statistics = true;

CREATE EXTERNAL TABLE hits
STORED AS PARQUET
LOCATION '/data/hits/';  -- directory-of-files ListingTable path
```

Then snapshot `statistics_cache()` (CLI) or `EXPLAIN` stats outputs (embedded) to confirm stats exist (see §7.4). ([datafusion.apache.org][6])

#### 7.2.2 Parquet metadata-derived statistics (footer / row-group / page index / bloom filters)

DataFusion’s Parquet pruning pipeline is explicitly multi-stage:

1. read metadata (schema + locations + stats, optionally page stats + bloom filters)
2. prune by projection
3. prune by row-group stats + bloom filters
4. prune by page stats
5. read remaining byte ranges (“access plan”)
   and then (optional) row-level filter pushdown (“late materialization”). ([datafusion.apache.org][7])

This is controlled by session config keys (critical ones):

* `datafusion.execution.parquet.pruning=true` → row-group skipping via predicate + min/max metadata. ([datafusion.apache.org][4])
* `datafusion.execution.parquet.enable_page_index=true` → page-level metadata read (more pruning, more metadata cost). ([datafusion.apache.org][4])
* `datafusion.execution.parquet.bloom_filter_on_read=true` → use bloom filters on read (equality pruning accelerator). ([datafusion.apache.org][8])
* `datafusion.execution.parquet.pushdown_filters=false` default → row-level filter pushdown exists but not enabled by default (per pruning blog). ([datafusion.apache.org][4])
* `datafusion.execution.parquet.metadata_size_hint` → reduce object-store round trips for metadata fetch by reading last N bytes optimistically. ([datafusion.apache.org][4])
* `datafusion.execution.parquet.skip_metadata` → skip optional embedded schema metadata (schema-conflict workaround; can affect planning-time schema/metadata behavior). ([datafusion.apache.org][4])

**Agent procedure (Python)**

```python
from datafusion import SessionContext, SessionConfig

cfg = (
    SessionConfig()
    .with_parquet_pruning(True)          # python convenience wrapper
    .set("datafusion.execution.parquet.enable_page_index", "true")
    .set("datafusion.execution.parquet.bloom_filter_on_read", "true")
    .set("datafusion.execution.parquet.pushdown_filters", "false")  # enable only if validated
)
ctx = SessionContext(cfg)
```

`with_parquet_pruning(True)` is explicitly documented as enabling pruning predicates to skip row groups. ([datafusion.apache.org][9])

#### 7.2.3 Metadata/Statistics caches: “existence amplification” for remote object stores

DataFusion 50 introduced automatic caching of Parquet metadata (statistics, page indexes, etc.) for ListingTable scans, reducing repeated network round trips and showing an explicit `metadata_load_time` improvement in `EXPLAIN ANALYZE` when `datafusion.runtime.metadata_cache_limit` is nonzero. ([datafusion.apache.org][10])

CLI-visible cache functions (ListingTable):

* `metadata_cache()` shows cached metadata entries and includes `metadata_size_bytes`, `hits`, and `extra` (page index included). ([datafusion.apache.org][6])
* `statistics_cache()` shows cached per-file statistics; explicitly requires `datafusion.execution.collect_statistics` enabled. ([datafusion.apache.org][6])
* `list_files_cache()` shows cached LIST results scoped to tables (reduces relisting). ([datafusion.apache.org][6])

Your addendum frames these caches as observability primitives: verify stats exist before expecting good join/scan planning, and tune `meta_fetch_concurrency` for registration-time metadata reads. ([datafusion.apache.org][6])

---

### 7.3 Planning impact: scan pruning vs join planning

#### 7.3.1 Scan pruning (static plan-time + runtime-assisted)

**Static pruning signals you should extract from plans (no execution):**

* Optimized logical `TableScan` includes:

  * `projection=[...]` (column pruning)
  * `partial_filters=[...]` (scan-level pushed filters)
    These correlate strongly with Parquet “prune by projection” and “prune by row group stats” stages. ([datafusion.apache.org][7])

**Runtime-assisted pruning (execution-time):**

* Dynamic filter pushdown for hash joins adds runtime predicates at scan time; DataFusion 50 shows `predicate=DynamicFilterPhysicalExpr[...]` inside `DataSourceExec` in the execution plan output. ([datafusion.apache.org][10])
* Treat this as “late-binding”: static plan bundles cannot fully predict pruning effectiveness (your doc explicitly warns static plan bundles can’t fully predict scan pruning when dynamic filters are enabled). ([datafusion.apache.org][10])

**Stats-aware file grouping (Rust-level, scheduling relevant):**
Your guide notes `FileScanConfig` has helpers to **split file groups using min/max statistics** (parallelism while preserving sort/non-overlap constraints).
Config also exposes an experimental switch: `datafusion.execution.split_file_groups_by_statistics` (“pack & sort files with non-overlapping statistics”). ([datafusion.apache.org][8])

#### 7.3.2 Join planning: where stats change join cost/shape

DataFusion explicitly ties selectivity estimation to join/filter cost: cost model uses table/column stats; boundary analysis helps infer predicate selectivity to “find more efficient plans.” ([datafusion.apache.org][1])

What this means operationally:

* if `Statistics` are Unknown/Inexact at key operators, cost-based heuristics degrade, and join ordering/selection becomes more conservative.
* “join planning” is split across:

  * logical-level costing inputs (selectivity estimation / boundary analysis)
  * physical-level join selection / partitioning decisions (threshold knobs that reference estimated rows/bytes)

Concrete knobs that *directly* depend on stats quality:

* Perfect hash join uses **min/max key range** + **build_side.num_rows()** to decide if it can use the fast path (thresholds exposed as config). ([datafusion.apache.org][4])
* HashJoin single-partitioning uses estimated size/rows thresholds:

  * `datafusion.optimizer.hash_join_single_partition_threshold` (bytes)
  * `datafusion.optimizer.hash_join_single_partition_threshold_rows` (rows) ([datafusion.apache.org][4])
* Hash join IN-list dynamic filtering:

  * `datafusion.optimizer.hash_join_inlist_pushdown_max_size` explains IN-list pushdown can yield “better statistics pruning” and use scan-side bloom filters (explicitly coupling join planning to scan pruning metadata). ([datafusion.apache.org][4])

Agent consequence: if `collect_statistics=false` at table creation or Parquet pruning metadata is unavailable (no min/max, no bloom, no page index), these join knobs either cannot trigger or trigger suboptimally.

---

### 7.4 Observability: “is the optimizer flying blind?” (required plan+settings probes)

#### 7.4.1 Settings introspection (must be part of your plan bundle)

Your guide explicitly recommends snapshotting `information_schema.df_settings` / `SHOW ALL` as part of a plan fingerprint bundle.
At minimum record:

* `datafusion.execution.collect_statistics`
* parquet pruning keys (`parquet.pruning`, `enable_page_index`, `bloom_filter_on_read`, `pushdown_filters`, `metadata_size_hint`)
* `datafusion.runtime.metadata_cache_limit`
* join thresholds (`perfect_hash_join_*`, `hash_join_*`, dynamic filter toggles)

#### 7.4.2 Plan output: show stats, show sizes

Enable operator statistics printing for *physical plans*:

* `datafusion.explain.show_statistics` (default false)
* `datafusion.explain.show_sizes` (default true) ([datafusion.apache.org][4])

Agent procedure (SQL):

```sql
SET datafusion.explain.show_statistics = true;
SET datafusion.explain.show_sizes = true;

EXPLAIN SELECT ...;
```

Interpretation:

* If physical nodes show `Statistics: Unknown` / missing stats for scans/joins, your cost model inputs are absent.
* If partition sizes are extreme/skewed, file grouping and repartition heuristics may be operating without good size estimates.

#### 7.4.3 Cache introspection (ListingTable; CLI or equivalent embedding hooks)

If you can use DataFusion CLI, these are direct probes:

```sql
SELECT * FROM metadata_cache();
SELECT * FROM statistics_cache();   -- requires collect_statistics=true
SELECT * FROM list_files_cache();
```

`statistics_cache()` explicitly requires `datafusion.execution.collect_statistics` enabled and exposes per-file `num_rows` and `table_size_bytes` (as Precision-wrapped values), letting you confirm stats are populated before expecting join planning to improve. ([datafusion.apache.org][6])

#### 7.4.4 Execution proof: metadata_load_time + dynamic predicate presence

Use `EXPLAIN ANALYZE` to validate:

* metadata caching effectiveness (`metadata_load_time` drops when metadata cache is enabled) ([datafusion.apache.org][10])
* scan predicate tightening via dynamic filters (`predicate=DynamicFilterPhysicalExpr[...]` on scan) ([datafusion.apache.org][10])

---

### 7.5 Minimal “stats discipline” checklist (agent-grade)

1. **Before table creation** (ListingTable path): ensure `datafusion.execution.collect_statistics=true` if you want join/filter costing to improve; this is a one-time decision per table instance. ([datafusion.apache.org][4])
2. **For Parquet pruning**: keep `parquet.pruning=true`, `enable_page_index=true`, `bloom_filter_on_read=true` unless you have a measured regression; treat `pushdown_filters` as an opt-in validated feature. ([datafusion.apache.org][4])
3. **Prove stats exist**: enable `datafusion.explain.show_statistics=true` and diff physical plans across runs; “Unknown” stats implies your join thresholds and selectivity estimates are degraded. ([datafusion.apache.org][4])
4. **Prove pruning inputs exist**: `TableScan.partial_filters` + projection in optimized logical are necessary but not sufficient; confirm runtime pruning via `EXPLAIN ANALYZE` (metadata_load_time, dynamic predicates). ([datafusion.apache.org][10])
5. **Bundle everything**: `{p0 logical, p1 optimized logical, physical plan, df_settings snapshot}`; treat stats/caches/config as part of plan identity (not optional metadata).

[1]: https://datafusion.apache.org/library-user-guide/query-optimizer.html "Query Optimizer — Apache DataFusion  documentation"
[2]: https://docs.rs/datafusion/latest/datafusion/common/struct.Statistics.html "Statistics in datafusion::common - Rust"
[3]: https://docs.rs/datafusion/latest/datafusion/physical_optimizer/pruning/struct.PruningPredicate.html "PruningPredicate in datafusion::physical_optimizer::pruning - Rust"
[4]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[5]: https://datafusion.apache.org/user-guide/sql/ddl.html "DDL — Apache DataFusion  documentation"
[6]: https://datafusion.apache.org/user-guide/cli/functions.html "CLI Specific Functions — Apache DataFusion  documentation"
[7]: https://datafusion.apache.org/blog/2025/03/20/parquet-pruning/ "Parquet Pruning in DataFusion: Read Only What Matters - Apache DataFusion Blog"
[8]: https://datafusion.apache.org/_sources/user-guide/configs.md.txt "datafusion.apache.org"
[9]: https://datafusion.apache.org/python/autoapi/datafusion/index.html "datafusion — Apache Arrow DataFusion  documentation"
[10]: https://datafusion.apache.org/blog/2025/09/29/datafusion-50.0.0/ "Apache DataFusion 50.0.0 Released - Apache DataFusion Blog"

According to your planning guide, the **physical layer** is where you switch from “query algebra” to **execution topology**: you traverse the **`ExecutionPlan` DAG via `children()`**, use **`partition_count` as the coarse parallelism signal**, and extract scan work units from **`DataSourceExec file_groups={...}`** (usually via `display_indent()` / `EXPLAIN FORMAT indent`).

---

## 8) Physical planning: `ExecutionPlan` construction

### 8.1 What the physical plan is (ExecutionPlan = runnable operator DAG)

#### 8.1.1 ExecutionPlan contract (Rust, authoritative)

`ExecutionPlan` is the executable operator interface. The minimal mental model:

* `children()` provides the input operators (edges in the operator DAG)
* `execute(partition, TaskContext)` returns an **async stream of `RecordBatch`** for that output partition
* `properties()` exposes **output properties** used by optimizers (ordering, partitioning, equivalence)
* `required_input_distribution()` / `required_input_ordering()` express *input requirements* the planner/optimizer must satisfy (or the plan may be invalid / incorrect) ([Docs.rs][1])

Concretely, the trait signature includes `children`, `with_new_children`, and `execute(partition, context) -> RecordBatchStream` as required methods. ([Docs.rs][1])

#### 8.1.2 Partitioning semantics: “N partitions = N independent output streams”

Physical partitioning is an explicit type (`Partitioning`), e.g. `RoundRobinBatch(n)`, `Hash(exprs, n)`, `UnknownPartitioning(n)`. Each partition corresponds to calling `execute(i)` for `i in [0..n)`. ([Docs.rs][2])

---

### 8.2 Physical planning pipeline (where ExecutionPlans come from)

#### 8.2.1 “Default” pipeline (DataFusion engine)

DataFusion’s canonical compilation path is:

1. logical plan (possibly unoptimized)
2. analyzer + logical optimizer
3. **physical planner**: `LogicalPlan -> ExecutionPlan`
4. physical optimizer (distribution/order enforcement, join selection, pushdowns)

The DataFusion example `planner_api.rs` shows the two operationally relevant APIs:

* **One-shot**: `ctx.state().create_physical_plan(&logical_plan).await?` (optimizes then builds physical)
* **Step-by-step**: analyzer → optimizer → `query_planner.create_physical_plan(&optimized_logical_plan, &state)` ([apache.googlesource.com][3])

**Agent rule:** when debugging *physical planning* specifically, do step-by-step so you can attribute changes to the correct stage.

#### 8.2.2 Physical planner interface points (Rust)

There are two adjacent traits:

* `PhysicalPlanner`: converts a `LogicalPlan` to an `ExecutionPlan`; also converts `Expr -> PhysicalExpr`. ([Docs.rs][4])
* `QueryPlanner`: “planner used to add extensions to DataFusion logical and physical plans”; given a `LogicalPlan`, create an `ExecutionPlan`. ([Docs.rs][5])

**Practical implication:** if you need custom physical operator injection (e.g., replace a logical scan with a custom `ExecutionPlan`), this is the engine-level interception seam.

---

### 8.3 Translating logical operators → physical operators (the “real job”)

You should treat “translation” as a mapping from logical nodes to concrete `*Exec` implementations plus **physical properties**.

Representative mappings (not exhaustive, but covers >90% of plans):

* `LogicalPlan::TableScan` → a scan exec (often `DataSourceExec` wrapping a file/data source) ([Docs.rs][6])
* `Projection` → `ProjectionExec` (usually preserves ordering if it doesn’t reorder rows; ordering propagation is tracked via `output_ordering`) ([Docs.rs][7])
* `Filter` → `FilterExec` (preserves equivalence properties; doesn’t reorder rows) ([Docs.rs][7])
* `Aggregate` → `AggregateExec` variants (partial/final, hash/sort; join selection & stats may alter choice) ([Docs.rs][8])
* `Join` → join exec variants (hash join / sort-merge / nested loop). Build-side selection and partition mode are frequently delegated to physical optimizer rules (see `join_selection`). ([Docs.rs][8])
* `Sort` / `TopK` → `SortExec` / TopK-like variants; ordering requirements are tracked and enforced via `EnforceSorting` ([Docs.rs][8])
* `Limit` → limit exec variants; plus `LimitPushdown` can push limits down *in the physical plan* ([Docs.rs][8])
* `Repartition` (logical or required by distribution) → `RepartitionExec` / sort-preserving repartition variants ([Docs.rs][9])

---

### 8.4 Partitioning decisions, distribution requirements, and correctness

#### 8.4.1 “Target partitions” is a *planner-level* knob

`RepartitionExec` docs explicitly state the planner picks a target partition count and then uses exchange/repartition to reach it (DataFusion’s parallelism model). ([Docs.rs][9])

**Consequences for agents:**

* If you set `target_partitions=k`, expect repartition nodes to appear when upstream partitioning differs or is suboptimal.
* Repartitioning can destroy ordering unless explicitly preserved. ([Docs.rs][9])

#### 8.4.2 Distribution requirements are explicit (`Distribution`)

`Distribution` encodes required input distribution: `UnspecifiedDistribution`, `SinglePartition`, `HashPartitioned(keys)`. It also has `create_partitioning(partition_count)` to produce a satisfying `Partitioning`. ([Docs.rs][10])

Each `ExecutionPlan` can declare requirements for each child via `required_input_distribution()` (default: unspecified per child). ([Docs.rs][1])

**Enforcement mechanism (physical optimization, but required for correctness/performance):**
`EnforceDistribution` inspects distribution requirements and inserts `RepartitionExec` where necessary; it may also increase partition counts when beneficial and allowed by config. ([Docs.rs][8])

**Agent diagnostic:** if a join/agg is slow or spills, inspect whether required hash distribution is being satisfied *before* the operator; if not, you’ll see `RepartitionExec` inserted close to it.

#### 8.4.3 Ordering requirements matter for correctness (not “just optimization”)

DataFusion’s ordering analysis explicitly states: an operator’s **ordering requirement** describes how its input must be sorted **to compute the correct result**, and it is the planner’s job to ensure requirements are satisfied (implemented via `EnforceSorting`). ([Apache DataFusion][11])

Mechanically:

* `ExecutionPlanProperties.output_ordering()` describes per-partition ordering guarantees (or `None` = no guarantee) ([Docs.rs][7])
* `ExecutionPlan.required_input_ordering()` expresses required ordering per child input ([Docs.rs][1])
* `ExecutionPlan.maintains_input_order()` indicates whether an operator can reorder rows (important when reasoning about whether an ordering guarantee survives). ([Docs.rs][1])
* `equivalence_properties()` tracks “columns known equal” so the optimizer can avoid unnecessary sorts; incorrect propagation causes unnecessary resorting. ([Docs.rs][7])

**Agent correctness policy:** if your workload includes window functions, ORDER BY + LIMIT (TopK), or sort-merge joins, treat ordering requirements as correctness constraints, not “nice-to-have”.

---

### 8.5 File scan planning: projections, pushed filters, file groups (work units)

#### 8.5.1 TableProvider::scan is the physical planning boundary

For any table, physical planning delegates scan construction to the provider:

`TableProvider::scan(state, projection, filters, limit) -> ExecutionPlan`

* projection is column indices into `schema()`
* filters are boolean `Expr` AND-ed together, subject to pushdown classification via `supports_filters_pushdown`
* ExecutionPlan must scan partitions “in a streaming, parallelized fashion” ([Docs.rs][12])

**Agent rule:** if you need custom scan-time behavior (secondary indexes, manifest-driven file membership, custom pruning), you implement/override the provider scan to return a custom `ExecutionPlan`.

#### 8.5.2 File-backed scans: `FileScanConfig` → `DataSourceExec`

`FileScanConfig` is the “base config” for file scans; it is itself a `DataSource` used to build `DataSourceExec`. ([Docs.rs][13])

Critical fields (planning-relevant, not runtime trivia):

* `object_store_url`: URL prefix used to resolve `ObjectStore` instances (must be registered in `RuntimeEnv`) ([Docs.rs][13])
* `file_groups: Vec<FileGroup>`: **files grouped into partitions**; partitions can be read concurrently; files within a group are read sequentially ([Docs.rs][13])
* `limit`: scan-level limit pushdown (when set) ([Docs.rs][13])
* `output_ordering: Vec<LexOrdering>`: ordering guarantees derived from file order/sort metadata (enables sort avoidance) ([Docs.rs][13])

**Concurrency semantics (agent-critical):** “DataFusion may attempt to read each partition concurrently; files within a partition will be read sequentially” → `file_groups` is your coarse-grained work scheduling unit. ([Docs.rs][13])

#### 8.5.3 `partitioned_by_file_group`: “hash partitioned by partition columns” shortcut

When `partitioned_by_file_group=true`, file groups are organized by partition column values and `output_partitioning` returns hash partitioning on those partition columns, allowing the optimizer to skip hash repartitioning for joins/aggs on those columns; if file partitions exceed `target_partitions`, groups are round-robined down to `target_partitions`. ([Docs.rs][13])

**Agent use:** for partitioned lake datasets, this can be the difference between:

* `Scan -> (no repartition) -> HashAggregate/HashJoin`
  vs
* `Scan -> RepartitionExec(Hash) -> HashAggregate/HashJoin`

#### 8.5.4 Projection pushdown moved into FileSource (important for custom file sources)

Recent DataFusion upgrade notes: projection handling moved from `FileScanConfig` into `FileSource` via `try_pushdown_projection`, enabling format-specific projection pushdown (including struct-field access); `FileScanConfigBuilder::with_projection_indices` now returns `Result` because pushdown may fail. ([Apache DataFusion][14])

**Agent implication:** if you are building a custom file source or touching `FileScanConfigBuilder` directly, treat projection pushdown as a *fallible planning operation* and surface failures early (don’t silently fall back to full projection).

---

### 8.6 Programmatic inspection of physical structure (Python + Rust)

#### 8.6.1 Python: `ExecutionPlan` wrapper inspection primitives

The Python `datafusion.plan.ExecutionPlan` wrapper exposes:

* `children() -> list[ExecutionPlan]`
* `partition_count` (property)
* `display()` / `display_indent()`
* `to_proto()` / `from_proto()` (serialization, subject to engine/provider limits) ([Apache DataFusion][15])

**Agent-grade traversal + parallelism annotation**

```python
def walk_exec(plan):
    stack = [(plan, 0)]
    while stack:
        node, depth = stack.pop()
        yield node, depth
        kids = node.children()
        for k in reversed(kids):
            stack.append((k, depth + 1))

pp = df.execution_plan()
for node, d in walk_exec(pp):
    print("  " * d, node.partition_count, node.display().splitlines()[0])
```

**Scan work extraction (recommended by your guide):** traverse structure via `children()`, but parse scan details via `display_indent()` / `EXPLAIN FORMAT indent` because scan nodes commonly emit `file_groups`, `projection`, `predicate`, `file_type` only in their formatted display.

#### 8.6.2 Rust: properties + requirements are structured (do not parse strings)

In Rust, you should **read properties and requirements directly**:

* `plan.properties()` returns `PlanProperties`
* `ExecutionPlanProperties` trait gives:

  * `output_partitioning() -> &Partitioning`
  * `output_ordering() -> Option<&LexOrdering>`
  * `equivalence_properties()` (avoid unnecessary sorts) ([Docs.rs][7])
* `ExecutionPlan` provides:

  * `required_input_distribution() -> Vec<Distribution>`
  * `required_input_ordering() -> Vec<Option<OrderingRequirements>>`
  * `maintains_input_order() -> Vec<bool>` ([Docs.rs][1])

Skeleton:

```rust
use std::sync::Arc;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

fn dump(plan: Arc<dyn ExecutionPlan>, depth: usize) {
    let indent = "  ".repeat(depth);
    println!("{}{} {:?}", indent, plan.name(), plan.output_partitioning());
    if let Some(ord) = plan.output_ordering() {
        println!("{}  ordering={:?}", indent, ord);
    }
    println!("{}  req_dist={:?}", indent, plan.required_input_distribution());
    println!("{}  req_ord ={:#?}", indent, plan.required_input_ordering());

    for child in plan.children().into_iter() {
        dump(Arc::clone(child), depth + 1);
    }
}
```

(Here `children()` returns references to `Arc<dyn ExecutionPlan>`; cloning the `Arc` is the normal pattern.) ([Docs.rs][1])

---

### 8.7 “Why this matters” (agent assertions to enforce)

If your agent is generating or modifying DataFusion queries/plans, enforce these invariants at planning time:

1. **Parallelism truth**: `partition_count` (Python) / `output_partitioning().partition_count()` (Rust) drives downstream concurrency budgeting. ([Apache DataFusion][15])
2. **Scan work units**: `file_groups` are the physical “task shards”; never schedule per-file without checking whether the planner already grouped them. ([Docs.rs][13])
3. **Correctness via requirements**: if `required_input_ordering` / `required_input_distribution` are unmet, the plan is invalid in principle; rely on enforcement rules (`EnforceSorting`, `EnforceDistribution`) rather than assuming the input “happens to be OK.” ([Docs.rs][1])
4. **String parsing is last resort**: only Python lacks structured access to ordering/distribution; in Rust, treat `display_indent()` as a debug artifact, not a primary data carrier. ([Docs.rs][7])


[1]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html "ExecutionPlan in datafusion::physical_plan - Rust"
[2]: https://docs.rs/datafusion/latest/datafusion/physical_plan/enum.Partitioning.html "Partitioning in datafusion::physical_plan - Rust"
[3]: https://apache.googlesource.com/datafusion/%2B/refs/tags/47.0.0-rc1/datafusion-examples/examples/planner_api.rs "datafusion-examples/examples/planner_api.rs - datafusion - Git at Google"
[4]: https://docs.rs/datafusion/latest/datafusion/physical_planner/trait.PhysicalPlanner.html "PhysicalPlanner in datafusion::physical_planner - Rust"
[5]: https://docs.rs/datafusion/latest/datafusion/execution/context/trait.QueryPlanner.html "QueryPlanner in datafusion::execution::context - Rust"
[6]: https://docs.rs/datafusion/latest/datafusion/datasource/memory/struct.DataSourceExec.html "DataSourceExec in datafusion::datasource::memory - Rust"
[7]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlanProperties.html "ExecutionPlanProperties in datafusion::physical_plan - Rust"
[8]: https://docs.rs/datafusion/latest/datafusion/physical_optimizer/index.html "datafusion::physical_optimizer - Rust"
[9]: https://docs.rs/datafusion/latest/datafusion/physical_plan/repartition/struct.RepartitionExec.html "RepartitionExec in datafusion::physical_plan::repartition - Rust"
[10]: https://docs.rs/datafusion/latest/datafusion/physical_plan/enum.Distribution.html "Distribution in datafusion::physical_plan - Rust"
[11]: https://datafusion.apache.org/blog/2025/03/11/ordering-analysis/ "Using Ordering for Better Plans in Apache DataFusion - Apache DataFusion Blog"
[12]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html "TableProvider in datafusion::datasource - Rust"
[13]: https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.FileScanConfig.html "FileScanConfig in datafusion::datasource::physical_plan - Rust"
[14]: https://datafusion.apache.org/library-user-guide/upgrading.html "Upgrade Guides — Apache DataFusion  documentation"
[15]: https://datafusion.apache.org/python/autoapi/datafusion/plan/index.html "datafusion.plan — Apache Arrow DataFusion  documentation"

According to a document from **January 27, 2026**, the “physical layer” is treated as **execution hints** mined from the `ExecutionPlan` DAG (via `children()` + `partition_count`) and from `display_indent()` / `EXPLAIN FORMAT indent` for scan details; the **physical optimizer** is the stage that *rewrites that DAG* after physical planning.

---

## 9) Physical optimization (PhysicalOptimizerRule pipeline)

### 9.1 Boundary: logical vs physical optimization (what is allowed to change)

**Logical optimizer (`OptimizerRule`)**: rewrites `LogicalPlan` / `Expr` (algebraic equivalence; pushdowns; decorrelation).
**Physical optimizer (`PhysicalOptimizerRule`)**: rewrites `ExecutionPlan` (operator DAG), preserving relational semantics but changing:

* algorithm variants (e.g., join selection),
* partition topology (exchanges / repartitions),
* ordering enforcement (sort insertion/removal/pushdown),
* auxiliary nodes for global requirements tracking. ([Docs.rs][1])

**PhysicalOptimizerRule API (Rust; authoritative contract)**:

```rust
pub trait PhysicalOptimizerRule {
  fn optimize(&self, plan: Arc<dyn ExecutionPlan>, config: &ConfigOptions)
    -> Result<Arc<dyn ExecutionPlan>, DataFusionError>;
  fn name(&self) -> &str;
  fn schema_check(&self) -> bool;
}
```

`schema_check` is a rule-level declaration that the rewrite will not mutate the output schema (nullable changes can require disabling it). ([Docs.rs][1])

**Rule injection (Rust-only)**: register additional rules via `SessionState::add_physical_optimizer_rule`. ([Docs.rs][1])

---

### 9.2 “What rules ran?”: the only reliable method is `EXPLAIN VERBOSE`

**SQL surface (engine-truth):**

```sql
EXPLAIN [ANALYZE] [VERBOSE] [FORMAT format] <statement>;
```

* `EXPLAIN VERBOSE` prints intermediate plans **after each rule** and only supports `indent` formatting. ([Apache DataFusion][2])

**Rule-by-rule staging names you should expect in output** (example pattern):

* `initial_physical_plan`
* `physical_plan after aggregate_statistics`
* `physical_plan after join_selection`
* `physical_plan after coalesce_batches`
* `physical_plan after repartition`
* `physical_plan after add_merge_exec`
* …then `physical_plan` (final) ([Apache DataFusion][3])

**Python (agent-grade) way to capture those rows**:

```python
rows = ctx.sql("EXPLAIN VERBOSE " + sql).to_arrow_table()  # or .collect()
# persist columns: plan_type, plan
```

(Use `EXPLAIN` instead of `df.execution_plan()` when you need **pre/post** snapshots per physical rule; Python bindings typically expose only the final `ExecutionPlan`.) ([Apache DataFusion][2])

---

### 9.3 Enforcement rules: distribution + ordering are *correctness constraints*

The physical planner/optimizer relies on **declared properties and requirements** on `ExecutionPlan` nodes:

* output properties (`properties()`) feed optimizer decisions;
* input requirements (`required_input_distribution`, `required_input_ordering`) describe constraints that must hold for correctness (or for the selected algorithm variant). ([Docs.rs][4])

#### 9.3.1 Distribution enforcement (EnforceDistribution): when/why `RepartitionExec` appears

**Core idea**: insert “exchange” operators to satisfy input **distribution requirements** (e.g., `HashPartitioned(keys)` for hash joins/aggs; `SinglePartition` for global order/merge phases).

DataFusion exposes the enforcement primitive as `ensure_distribution(...)`:

* adds “data exchange” operators to satisfy distribution requirements;
* intended for **bottom-up traversal**;
* explicit admonition: if you rely on this, avoid inserting exchange operators “in other places” (double-insertion risk). ([Docs.rs][5])

**How `RepartitionExec` behaves (debug semantics, not trivia)**:

* With multiple input streams, output ordering becomes an arbitrary interleaving (unordered) **unless** `with_preserve_order` is used.
* Repartition uses spill channels (SpillPool) to handle memory pressure during repartitioning. ([Docs.rs][6])

**Plan signatures (what to look for)**:

* Parallelism boost: `RepartitionExec: partitioning=RoundRobinBatch(N)`
* Key-based shuffle: `RepartitionExec: partitioning=Hash([k1, k2, ...], N)`
* Global funnel: `CoalescePartitionsExec` or `RepartitionExec` driving to `SinglePartition` (often visible via `required_input_distribution: SinglePartition` failures in sanity checker). ([Docs.rs][6])

**Join-key order alignment (why keys may be reordered “mysteriously”)**
DataFusion includes physical join-key reordering to align children partitioning with parent join keys; see `adjust_input_keys_ordering` (“ordering of join keys … might not match children output partitioning; top-down process adjusts”). ([Docs.rs][7])
Config knob: `datafusion.optimizer.top_down_join_key_reordering=true|false`. ([Apache DataFusion][8])

#### 9.3.2 Ordering enforcement (EnforceSorting): when/why `SortExec` / `SortPreservingMergeExec` appears

**Correctness statement**: an operator’s **ordering requirement** specifies how its input must be sorted for correct results; the planner must ensure requirements are satisfied. ([Apache DataFusion][9])

EnforceSorting is not “just add SortExec”; it is a multi-step optimizer (ordering-dependent subrules) including:

1. **ensure_sorting**: remove unnecessary `SortExec` / `SortPreservingMergeExec`, add `SortExec` if required, adjust windows.
2. **parallelize_sorts**: optional; controlled by `repartition_sorts`; removes/rewrites partition unifiers around sorts.
3. **replace_with_order_preserving_variants**: collapse `SortExec + CoalescePartitionsExec` into `SortPreservingMergeExec`, and rewrite `SortExec + RepartitionExec` into order-preserving repartition forms where applicable.
4. **sort_pushdown**: push sorts deeper.
5. **replace_with_partial_sort**: rewrite to `PartialSortExec` when safe. ([Docs.rs][1])

**Tuning lever**: `datafusion.optimizer.repartition_sorts` controls whether EnforceSorting may parallelize sorts (subrule #2). ([Docs.rs][1])

---

### 9.4 Global requirements tracking: `OutputRequirementExec` is an optimization-time artifact

DataFusion has an `OutputRequirements` rule that temporarily inserts an auxiliary `OutputRequirementExec` to track global ordering/distribution requirements across rules, and removes it before execution. ([Docs.rs][10])

**Debug tell**: intermediate physical plans may show:

* `OutputRequirementExec: order_by=[...], dist_by=[...]`
* followed by inserted repartitions/sorts satisfying those requirements. ([Apache DataFusion Blog][11])

Agent rule: if `OutputRequirementExec` survives into the final runnable plan, treat it as a bug / invariant violation (it is documented as “non-executable helper; shouldn’t occur in final plan”). ([Docs.rs][10])

---

### 9.5 JoinSelection: physical algorithm/shape changes (and infinite-source accommodation)

`JoinSelection` is explicitly a **physical optimizer** rule:

* modifies a plan to accommodate infinite sources (avoid pipeline-breaking join variants),
* uses available statistical information to choose more performant join execution. ([Docs.rs][12])

**User-facing lever**: `datafusion.optimizer.prefer_hash_join=true|false` biases join algorithm preference at the physical layer (hash vs sort-merge tradeoff). ([Apache DataFusion][8])

---

### 9.6 “Why did it repartition / sort / shuffle?”: deterministic debug procedure

#### 9.6.1 Capture the three required artifacts

1. **Final physical plan** (Python plan object):

```python
pp = df.execution_plan()
physical_indent = pp.display_indent()
```

Your guide treats `ExecutionPlan.children()` / `partition_count` + `display_indent()` as the extraction backbone. 

2. **Per-rule physical delta trace** (SQL):

```sql
EXPLAIN VERBOSE <sql>;
```

Use `plan_type` rows to locate the first stage where `RepartitionExec` / `SortExec` appears. ([Apache DataFusion][2])

3. **Session knobs snapshot**:

```sql
SELECT * FROM information_schema.df_settings;
-- or SHOW ALL
```

(Needed because config toggles directly change physical optimizer behavior; especially repartition/sort/dynamic filters.) ([Apache DataFusion][8])

#### 9.6.2 If you see `RepartitionExec`, classify it by partitioning scheme

* `RoundRobinBatch(N)`:

  * usually inserted by physical optimizer to increase parallelism (`enable_round_robin_repartition=true`) and to reach `target_partitions`-scale parallelism. ([Apache DataFusion][8])
  * mitigation knobs: reduce `datafusion.execution.target_partitions`; disable `datafusion.optimizer.enable_round_robin_repartition`. ([Apache DataFusion][8])

* `Hash([keys], N)`:

  * inserted to satisfy `HashPartitioned(keys)` requirements for hash joins/aggs or for downstream distribution constraints enforced by EnforceDistribution. ([Docs.rs][5])
  * mitigation knobs: ensure upstream scan is already partitioned by those keys when possible (file partitioning / provider partitioning), or reduce repartition churn by aligning join-key ordering (`top_down_join_key_reordering`). ([Docs.rs][7])

* Consecutive repartitions (`RoundRobinBatch` then `Hash`):

  * known failure mode class; DataFusion’s own debugging write-up shows identifying EnforceDistribution as the rule that inserted redundant repartitions via `EXPLAIN VERBOSE` and intermediate plan inspection. ([Apache DataFusion Blog][11])

**Correctness note**: repartition is an “exchange” that can destroy ordering unless preserve-order is set; if you later see EnforceSorting insert sorts, this is often the immediate cause. ([Docs.rs][6])

#### 9.6.3 If you see `SortExec` / `SortPreservingMergeExec`, map it to an ordering requirement

* Identify the downstream operator that requires ordering (window variants, merge joins, TopK/sort pipelines).
* EnforceSorting may:

  * insert `SortExec` to satisfy ordering,
  * remove sorts when ordering is already satisfied (ordering analysis),
  * rewrite to `PartialSortExec` when input is partially sorted. ([Docs.rs][1])

**If you expected “no sort needed” but got a sort**:

* verify scan ordering metadata exists (provider declared ordering / file sort order);
* if ordering is lost, look for intervening repartitions (unordered interleaving) and order-preserving rewrites. ([Apache DataFusion][9])

#### 9.6.4 If you see “shuffle-like” behavior but no explicit shuffle operator

In DataFusion, shuffles materialize as exchange/repartition operators (`RepartitionExec`). The operator itself documents spill-based channels and FIFO ordering per (input, output) pair; absence of `RepartitionExec` lines generally means “no exchange,” not “no data movement.” ([Docs.rs][6])

---

### 9.7 Dynamic filters: physical optimizer can inject **runtime-derived** scan predicates

Config keys (physical optimizer → scan phase):

* `datafusion.optimizer.enable_dynamic_filter_pushdown`
* `...enable_join_dynamic_filter_pushdown`
* `...enable_topk_dynamic_filter_pushdown`
* `...enable_aggregate_dynamic_filter_pushdown` ([Apache DataFusion][8])

Dynamic filters are represented as physical expressions that reference operator state (e.g., TopK threshold or HashJoin build-side keys) and can produce predicates like `a >= <threshold>` or `b IN (...)`. ([Docs.rs][13])

**Debug**: inspect scan lines in physical plan / explain for `predicate=DynamicFilter...` or equivalent; treat this as *execution-dependent pruning* (plan text changes can occur with stats and rulepack changes even when SQL is identical). ([Docs.rs][13])

---

### 9.8 Programmatic inspection: “requirements vs properties” (Rust) vs “parse display” (Python)

#### Rust: read requirements/properties directly (no string parsing)

* `ExecutionPlan::required_input_distribution()` / `required_input_ordering()` indicate what must be satisfied. ([Docs.rs][4])
* `ExecutionPlanProperties` (via `properties()`) exposes output partitioning/ordering/equivalence used by enforcement rules. ([Docs.rs][4])

#### Python: structural traversal is available; properties/requirements usually are not

Your guide’s recommended extraction path is:

1. traverse physical DAG via `children()` / `partition_count`,
2. parse `display_indent()` (or `EXPLAIN FORMAT indent`) for operator-level attributes like `file_groups`, `projection`, `predicate`.

---


[1]: https://docs.rs/datafusion/latest/datafusion/physical_optimizer/trait.PhysicalOptimizerRule.html "PhysicalOptimizerRule in datafusion::physical_optimizer - Rust"
[2]: https://datafusion.apache.org/user-guide/sql/explain.html?utm_source=chatgpt.com "EXPLAIN — Apache DataFusion documentation"
[3]: https://datafusion.apache.org/_sources/library-user-guide/query-optimizer.md.txt "datafusion.apache.org"
[4]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html?utm_source=chatgpt.com "ExecutionPlan in datafusion::physical_plan - Rust"
[5]: https://docs.rs/datafusion/latest/datafusion/physical_optimizer/enforce_distribution/fn.ensure_distribution.html "ensure_distribution in datafusion::physical_optimizer::enforce_distribution - Rust"
[6]: https://docs.rs/datafusion/latest/datafusion/physical_plan/repartition/struct.RepartitionExec.html "RepartitionExec in datafusion::physical_plan::repartition - Rust"
[7]: https://docs.rs/datafusion/latest/datafusion/physical_optimizer/enforce_distribution/fn.adjust_input_keys_ordering.html?utm_source=chatgpt.com "adjust_input_keys_ordering in datafusion::physical_optimizer"
[8]: https://datafusion.apache.org/user-guide/configs.html?utm_source=chatgpt.com "Configuration Settings — Apache DataFusion documentation"
[9]: https://datafusion.apache.org/blog/2025/03/11/ordering-analysis/ "Using Ordering for Better Plans in Apache DataFusion - Apache DataFusion Blog"
[10]: https://docs.rs/datafusion/latest/datafusion/physical_optimizer/output_requirements/index.html "datafusion::physical_optimizer::output_requirements - Rust"
[11]: https://datafusion.blog.apache.org/2025/12/15/avoid-consecutive-repartitions/ "Optimizing Repartitions in DataFusion: How I Went From Database Noob to Core Contribution - Apache DataFusion Blog"
[12]: https://docs.rs/datafusion/latest/datafusion/physical_optimizer/join_selection/struct.JoinSelection.html?utm_source=chatgpt.com "JoinSelection in datafusion::physical_optimizer"
[13]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.PhysicalExpr.html?utm_source=chatgpt.com "PhysicalExpr in datafusion::physical_plan - Rust"

According to a document from **January 27, 2026** (your planning guide) and the upstream DataFusion docs, **EXPLAIN is a first-class, configurable planning API**: it is the canonical, stable carrier for (a) logical scan contracts (`projection`, `partial_filters`) and (b) physical scan work units (`file_groups`, `projection`, `predicate`)—and should be treated as a *regression artifact*, not “debug text.”

---

## 10) Plan introspection, debugging, and explainability

### 10.1 EXPLAIN as the canonical “plan UI”

#### 10.1.1 SQL syntax, modifiers, and observable semantics

**Syntax (DataFusion SQL):**

```sql
EXPLAIN [ANALYZE] [VERBOSE] [FORMAT format] statement
```

DataFusion defines this grammar explicitly and uses it as the canonical non-programmatic interface to produce plan artifacts. ([Apache DataFusion][1])

**Modifier semantics (agent contract):**

* `EXPLAIN …` (no `ANALYZE`): **planning-only**; returns a relation whose rows contain plan text (no execution). ([Apache DataFusion][1])
* `ANALYZE`: **executes** the query and returns an *annotated* plan with operator metrics; output is intentionally plan-centric (results discarded). ([Apache DataFusion][1])
* `VERBOSE`:

  * With `EXPLAIN` (no `ANALYZE`): returns **per-rule delta trace** (“plan after rule X”) for logical and physical optimizer pipelines; this is the authoritative “which rule inserted what” trace. ([Apache DataFusion][2])
  * With `EXPLAIN ANALYZE`: returns **per-partition metrics** rather than partition-aggregated metrics; aggregation vs per-partition is controlled by the `VERBOSE` keyword, not by `analyze_level`. ([Apache DataFusion][1])
* `FORMAT format`: selects a renderer for the returned plan text; if omitted, uses session config `datafusion.explain.format`. ([Apache DataFusion][3])

**Hard constraints (must not guess):**

* `EXPLAIN VERBOSE` only supports `indent` format. ([Apache DataFusion][3])
* `EXPLAIN ANALYZE` only supports `indent` format. ([Apache DataFusion][1])

#### 10.1.2 Output shape: `plan_type` / `plan` rows are a machine interface

`EXPLAIN` returns a **table** where `plan_type` partitions content (e.g., `logical_plan`, `physical_plan`, `Plan with Metrics`) and `plan` holds the serialized textual representation. The docs show this shape directly in examples. ([Apache DataFusion][3])

**Agent extraction pattern (Python, capture without stdout parsing):**

```python
exp = ctx.sql(f"EXPLAIN FORMAT INDENT {sql_text}")
t = exp.to_arrow_table()
# columns: plan_type, plan
```

This is preferable to `DataFrame.explain()` when you need **capturable** output (DataFrame.explain prints). ([Apache DataFusion][4])

#### 10.1.3 FORMAT choices: when to use each, and what you get

DataFusion documents four renderers:

* `FORMAT INDENT`: emits both logical + physical plans as **one operator per line**, hierarchy via indentation. ([Apache DataFusion][3])
  **Use:** golden text fixtures; regex-based extraction (`TableScan`, `DataSourceExec`); `VERBOSE`/`ANALYZE` compatibility. ([Apache DataFusion][3])

* `FORMAT TREE`: renders a compact tree diagram (modeled after DuckDB) that is optimized for **high-level physical topology inspection**. ([Apache DataFusion][3])
  **Use:** quick “what operators exist / in what shape” diffs; human triage of repartitions/sorts.

* `FORMAT PGJSON`: emits plan as Postgres-modeled JSON; explicitly intended to plug into existing plan visualization tooling (e.g., dalibo’s explain visualizer). ([Apache DataFusion][3])
  **Use:** structured parsing and downstream plan tooling without reverse-engineering `indent` text.

* `FORMAT GRAPHVIZ`: emits DOT language for Graphviz, providing a visual plan graph for topology debugging and visual diffs. ([Apache DataFusion][3])
  **Use:** join forests / deep pipelines where indentation is insufficient.

#### 10.1.4 Session-level explain controls (must be pinned for deterministic fixtures)

DataFusion exposes `datafusion.explain.*` controls:

* `datafusion.explain.format` (`indent` or `tree`) and `datafusion.explain.tree_maximum_render_width` (tree only). ([Apache DataFusion][5])
* `datafusion.explain.show_schema` (include schemas), `show_statistics` (operator stats for physical plans), `show_sizes` (partition sizes). ([Apache DataFusion][5])
* `datafusion.explain.logical_plan_only`, `physical_plan_only`. ([Apache DataFusion][5])
* `datafusion.explain.analyze_level` controls **which categories/levels** of metrics are displayed (`summary` vs `dev`), distinct from `VERBOSE` which controls aggregation granularity. ([Apache DataFusion][5])

**How to set (SQL, documented):**

```sql
SET datafusion.explain.show_statistics = 'true';
SET datafusion.explain.show_schema = 'true';
SET datafusion.explain.format = 'indent';
```

The configs guide explicitly documents `SET <key> = '<value>'` for configuration changes. ([Apache DataFusion][5])

**Harness invariant (from your guide):** always specify `FORMAT` or pin `datafusion.explain.format`, otherwise fixture drift occurs when defaults change.  ([Apache DataFusion][3])

#### 10.1.5 How to read plans: locate pushdowns and scan work units

**Logical scan contracts (optimized logical / explain logical_plan):**

* `TableScan: … projection=[...]` ⇒ projection pushdown (column pruning).
* `TableScan: … partial_filters=[...]` ⇒ scan-level pushed predicates (provider pushdown / partial pushdown). ([Apache DataFusion][1])

The Reading Explain Plans example shows both fields in the logical plan line. ([Apache DataFusion][1])
This is exactly the extraction target your guide calls out as “required columns” + “pushdown predicates.”

**Physical scan work units (physical_plan / DataSourceExec):**

* `DataSourceExec: file_groups={...}` ⇒ grouped file ranges, i.e., **scan shards** (scheduling units).
* `DataSourceExec: … projection=[...]` ⇒ executed column pruning at scan.
* `DataSourceExec: … predicate=...` ⇒ predicate actually applied during scan (may include dynamic filters). ([Apache DataFusion][1])

Reading Explain Plans shows the physical scan line with `file_groups`, `projection`, and `predicate` in `EXPLAIN ANALYZE` output. ([Apache DataFusion][1])
Your guide treats `file_groups` and scan `predicate/projection` as “truth for I/O scheduling.”

**Parallelism signal:** physical plans explicitly encode partitioned execution (“16 partitions” example) and the docs state DataSourceExec reads in parallel across partitions. ([Apache DataFusion][1])

**EXPLAIN ANALYZE metrics reading (scan-centric):**

* Scan-level metrics include `bytes_scanned`, plus pruning counters when predicate pushdown is enabled (`row_groups_pruned_statistics`, `row_groups_pruned_bloom_filter`, page index prune counts, etc.). ([Apache DataFusion][1])
  This is the canonical “prove pruning happened” carrier (not the logical plan alone).

---

### 10.2 Programmatic inspection APIs (Python)

#### 10.2.1 DataFrame plan accessors (no execution)

DataFusion Python explicitly exposes the three plan layers and DataFrame-level explain:

* `logical_plan() -> LogicalPlan` (unoptimized)
* `optimized_logical_plan() -> LogicalPlan`
* `execution_plan() -> ExecutionPlan`
* `explain(verbose=False, analyze=False) -> None` (prints; `analyze=True` runs + reports metrics)
* `parse_sql_expr(expr: str) -> Expr` (schema-aware SQL expr parser; useful for canonical predicate normalization) ([Apache DataFusion][4])

Your guide treats these as the minimal “plan bundle” primitives. 

**Agent usage (plan bundle construction):**

```python
df = ctx.sql(sql_text)

lp0 = df.logical_plan()
lp1 = df.optimized_logical_plan()
pp  = df.execution_plan()

# stable string carriers
lp1_txt = lp1.display_indent_schema()
pp_txt  = pp.display_indent()
```

Plan object renderers are stable enough for golden tests; schema rendering is the fast schema-oracle fallback. ([Apache DataFusion][6])

#### 10.2.2 Plan object methods: traversal + rendering + serialization

Python plan module defines the inspection surface:

**LogicalPlan**

* renderers: `display()`, `display_indent()`, `display_indent_schema()`, `display_graphviz()`
* traversal: `inputs()`
* structural matching hook: `to_variant()`
* serialization: `to_proto()` / `from_proto(ctx, bytes)` with limitation: in-memory tables from record batches not supported. ([Apache DataFusion][6])

**ExecutionPlan**

* traversal: `children()`
* rendering: `display()`, `display_indent()`
* coarse parallelism: `partition_count`
* serialization: `to_proto()` / `from_proto(ctx, bytes)` with the same in-memory limitation. ([Apache DataFusion][6])

These are exactly the APIs your guide builds on (`inputs()` + `to_variant()` for logical; `children()` + `partition_count` for physical).

#### 10.2.3 Capturable explain in Python: prefer SQL EXPLAIN over DataFrame.explain

`DataFrame.explain(verbose, analyze)` prints and returns `None`; use it for interactive sessions. ([Apache DataFusion][4])
For harnessed artifacts and machine parsing, use SQL EXPLAIN because it returns a DataFrame (capturable table with `plan_type/plan`). ([Apache DataFusion][1])

#### 10.2.4 Recommended “golden artifacts” for regression (minimum viable set)

Your guide’s recommended baseline is:

1. **Optimized logical** (semantic + pushdown truth backbone)
2. **Physical** (execution hints: partitions + file_groups + scan predicate/projection)
3. **Explain text** (stable extraction fixture; prefer explicit `FORMAT`)

**Concrete artifact set (agent checklist):**

* `optimized_logical_plan().display_indent_schema()` (diffable; schema-aware; stable across environments more than physical) ([Apache DataFusion][6])
* `execution_plan().display_indent()` (diffable physical topology + partitioning hints) ([Apache DataFusion][6])
* `EXPLAIN FORMAT INDENT <sql>` captured as `(plan_type, plan)` rows (capture-friendly; includes both logical+physical in indent mode) ([Apache DataFusion][3])

**High-leverage additions (when you need more than “one string”):**

* `EXPLAIN VERBOSE <sql>` (rule-by-rule provenance; required to answer “who inserted this repartition/sort?”) ([Apache DataFusion][2])
* `EXPLAIN FORMAT PGJSON <sql>` (machine-parsable structured plan; tool-friendly) ([Apache DataFusion][3])
* `EXPLAIN ANALYZE [VERBOSE] <sql>` with pinned `datafusion.explain.analyze_level` (metrics regression tracking; per-partition only when VERBOSE) ([Apache DataFusion][1])

---

[1]: https://datafusion.apache.org/user-guide/explain-usage.html "Reading Explain Plans — Apache DataFusion  documentation"
[2]: https://datafusion.apache.org/library-user-guide/query-optimizer.html "Query Optimizer — Apache DataFusion  documentation"
[3]: https://datafusion.apache.org/user-guide/sql/explain.html "EXPLAIN — Apache DataFusion  documentation"
[4]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[5]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[6]: https://datafusion.apache.org/python/autoapi/datafusion/plan/index.html "datafusion.plan — Apache Arrow DataFusion  documentation"

According to a document from **January 27, 2026**, DataFusion plan persistence should be treated as **two-tier**: **(a) “engine-internal” protobuf bytes** (`to_proto`) that are powerful but brittle/limited, and **(b) Substrait bytes** as the **portable** interchange + fingerprint layer, with physical plans treated as **regeneratable runtime hints** rather than stable cache keys.

---

## 11) Plan serialization & interchange

### 11.0 Artifact taxonomy (what you are serializing)

**DataFusion-native protobuf (“datafusion-proto” / Python `to_proto`)**

* Encodes **LogicalPlan (+ Expr)** and **ExecutionPlan (+ PhysicalExpr)** into protobuf bytes. ([Docs.rs][1])
* Explicitly **not version-stable**: “not guaranteed compatible across DataFusion versions.” ([Docs.rs][1])
* Intended for **same-engine** distribution / distributed query engines (send plans over network). ([Docs.rs][2])

**Substrait (portable IR)**

* Substrait is a cross-language plan format “designed for interoperability across different languages and systems.” ([Substrait][3])
* DataFusion advertises Substrait for passing plans across language/system boundaries. ([Apache DataFusion][4])
* DataFusion Python exposes first-class Substrait producer/consumer/serde. ([Apache DataFusion][5])

---

## 11.1 Protobuf plan serialization (DataFusion-native)

### 11.1.1 Python surface: `LogicalPlan/ExecutionPlan.to_proto()` + `from_proto(ctx, bytes)`

**APIs (Python)**

* `LogicalPlan.to_proto() -> bytes`
* `LogicalPlan.from_proto(ctx: SessionContext, data: bytes) -> LogicalPlan`
* `ExecutionPlan.to_proto() -> bytes`
* `ExecutionPlan.from_proto(ctx: SessionContext, data: bytes) -> ExecutionPlan` ([Apache DataFusion][6])

**Hard limitation (Python docs; non-negotiable)**

* “Tables created in memory from record batches are currently not supported.” ([Apache DataFusion][6])
  This matches the guidance in your doc: proto persistence cannot be your only artifact if you use `register_record_batches(...)` / `create_dataframe(...)` style in-memory sources.

#### A) LogicalPlan round-trip (agent-ready recipe)

**Goal**: store **optimized logical** protobuf bytes; later rehydrate into a DataFrame and continue planning/execution.

```python
from datafusion import SessionContext
from datafusion.plan import LogicalPlan

ctx = SessionContext()
ctx.register_parquet("t", "/data/t.parquet")

df = ctx.sql("SELECT a, sum(b) AS sb FROM t WHERE a > 10 GROUP BY a")
lp = df.optimized_logical_plan()

blob: bytes = lp.to_proto()

# --- later / elsewhere ---
ctx2 = SessionContext()
ctx2.register_parquet("t", "/data/t.parquet")   # MUST recreate name-resolution environment

lp2: LogicalPlan = LogicalPlan.from_proto(ctx2, blob)
df2 = ctx2.create_dataframe_from_logical_plan(lp2)
df2.show()
```

Key points:

* `from_proto` requires a `SessionContext` because decoding needs a planning environment (table/function resolution). ([Apache DataFusion][6])
* `SessionContext.create_dataframe_from_logical_plan(plan)` is the canonical “rehydrate a DataFrame wrapper around an existing LogicalPlan” API. ([Apache DataFusion][7])

#### B) ExecutionPlan round-trip (agent-ready recipe)

**Goal**: persist a physical plan (rarely appropriate across machines), rehydrate, then execute partitions manually.

```python
from datafusion import SessionContext
from datafusion.plan import ExecutionPlan

ctx = SessionContext()
ctx.register_parquet("t", "/data/t.parquet")
df = ctx.sql("SELECT * FROM t WHERE a > 10")
pp = df.execution_plan()

blob: bytes = pp.to_proto()

# --- later / elsewhere ---
ctx2 = SessionContext()
ctx2.register_parquet("t", "/data/t.parquet")

pp2: ExecutionPlan = ExecutionPlan.from_proto(ctx2, blob)

# Execute per partition (partition index in [0..pp2.partition_count))
for part in range(pp2.partition_count):
    stream = ctx2.execute(pp2, part)
    # consume RecordBatchStream (implementation-dependent consumption)
```

* `ExecutionPlan.partition_count` and `SessionContext.execute(plan, partitions:int)->RecordBatchStream` are exposed in Python. ([Apache DataFusion][6])
* ExecutionPlan serialization is explicitly blocked by the same in-memory-table constraint. ([Apache DataFusion][6])

### 11.1.2 Python limitation taxonomy (what breaks proto round-trip)

**(1) In-memory RecordBatch-backed tables**

* Explicitly unsupported for `to_proto/from_proto`. ([Apache DataFusion][6])

**(2) Custom functions + table providers (extension codec gap)**

* `PyLogicalPlan` “can currently only serialize or deserialize built in functions and table providers” and uses `DefaultLogicalExtensionCodec`; custom functions/providers (e.g., Iceberg integration) need a codec that can encode/decode them. ([GitHub][8])

**(3) Version skew**

* DataFusion proto bytes are not guaranteed to deserialize across DataFusion versions. ([Docs.rs][1])

**(4) Cache-key misuse**

* Protobuf serialization is *not canonical* (hashes of bytes are fragile) and “serialization stability across builds” is not guaranteed; do not treat raw protobuf bytes as a stable fingerprint. ([Protocol Buffers][9])

### 11.1.3 Rust surface: `datafusion-proto` crate (authoritative semantics; canonical function names)

The `datafusion-proto` crate documents:

* it serializes **LogicalPlans (+ Expr)** and **ExecutionPlans (+ PhysicalExpr)**
* **version compatibility is not guaranteed**
* binary format is **DataFusion-specific**, and points to Substrait for a standard encoding ([Docs.rs][1])

**Core functions (Rust; exact names)**

* `logical_plan_to_bytes(&LogicalPlan) -> Result<Bytes>`
* `logical_plan_from_bytes(bytes: &[u8], ctx: &TaskContext) -> Result<LogicalPlan>`
* `physical_plan_to_bytes(plan: Arc<dyn ExecutionPlan>) -> Result<Bytes>`
* `physical_plan_from_bytes(bytes: &[u8], ctx: &TaskContext) -> Result<Arc<dyn ExecutionPlan>>` ([Docs.rs][1])

**TaskContext requirement (recent API reality)**

* Upgrade guidance: physical plan serde expects **`TaskContext`**, not `SessionContext`, because it carries `RuntimeEnv` required by decode paths (`try_into_physical_plan`). ([Apache DataFusion][10])

**Extension codecs (custom nodes/providers/UDF hooks)**

* Logical plan serde exposes `Default Logical Extension Codec` + `Logical Extension Codec` trait. ([Docs.rs][11])
* Physical plan serde exposes `Default Physical Extension Codec`, `Composed Physical Extension Codec`, and `Physical Extension Codec` trait. ([Docs.rs][12])
  Implication: if your plan contains **Extension** nodes, foreign table providers, or custom physical exec nodes, you must supply codecs capable of encoding/decoding those nodes; otherwise round-trip fails or silently degrades.

---

## 11.2 Substrait (portable relational plan interchange)

### 11.2.1 Python `datafusion.substrait`: exact classes + call signatures

Python Substrait module provides: `Producer`, `Consumer`, `Serde`, and `Plan`. ([Apache DataFusion][5])

**SQL → Substrait bytes**

* `Serde.serialize_bytes(sql: str, ctx: SessionContext) -> bytes` ([Apache DataFusion][5])

**bytes → Plan**

* `Serde.deserialize_bytes(proto_bytes: bytes) -> Plan` ([Apache DataFusion][5])

**Plan → LogicalPlan**

* `Consumer.from_substrait_plan(ctx: SessionContext, plan: Plan) -> LogicalPlan` ([Apache DataFusion][5])

**LogicalPlan → Plan**

* `Producer.to_substrait_plan(logical_plan: LogicalPlan, ctx: SessionContext) -> Plan` ([Apache DataFusion][5])

**Plan → bytes**

* `Plan.encode() -> bytes` ([Apache DataFusion][5])

**File I/O**

* `Serde.serialize(sql, ctx, path)` and `Serde.deserialize(path)` for filesystem carriers. ([Apache DataFusion][5])

#### End-to-end roundtrip (directly matching upstream test usage)

The upstream Python tests demonstrate the full pipeline: SQL → Substrait → LogicalPlan → DataFrame → Substrait. ([Apache Git Repositories][13])

```python
import pyarrow as pa
from datafusion import SessionContext
from datafusion import substrait as ss

ctx = SessionContext()
batch = pa.RecordBatch.from_arrays([pa.array([1,2,3]), pa.array([4,5,6])], names=["a","b"])
ctx.register_record_batches("t", [[batch]])

substrait_bytes = ss.Serde.serialize_bytes("SELECT * FROM t", ctx)
substrait_plan  = ss.Serde.deserialize_bytes(substrait_bytes)
logical_plan    = ss.Consumer.from_substrait_plan(ctx, substrait_plan)

df = ctx.create_dataframe_from_logical_plan(logical_plan)
plan2 = ss.Producer.to_substrait_plan(df.logical_plan(), ctx)
bytes2 = plan2.encode()
```

### 11.2.2 When Substrait is the right artifact (selection criteria)

**Choose Substrait when:**

* You require **cross-language / cross-process** interchange with a standardized payload (Substrait’s core design point). ([Substrait][3])
* You want a **long-lived plan IR** that is *less coupled* to DataFusion’s internal protobuf schema/versioning (DataFusion itself points to Substrait as the “standard encoding” alternative to DataFusion-specific proto). ([Docs.rs][1])
* You need a **plan fingerprint** that is semantically closer to relational algebra than SQL strings (your doc’s recommended “primary fingerprint: Substrait bytes hash” pattern).

**But**: Substrait serialization is also protobuf-based; protobuf serialization is not canonical and not stable across builds, so fingerprints should use a **stable normalization policy** (e.g., decode + re-encode deterministically, strip/retain unknown fields consistently, include Substrait version + extension URIs in the hash envelope). ([Protocol Buffers][9])

### 11.2.3 Substrait limitations (DataFusion-specific reality)

**Logical plans: strongest support**

* Producer/Consumer operate on **LogicalPlan** in Python (by API). ([Apache DataFusion][5])

**Physical plans: incomplete / under development**

* DataFusion issue explicitly states serializing an `ExecutionPlan` to Substrait “is still not complete enough” / “only works for a small subset of plans.” ([GitHub][14])
* There is an EPIC tracking producer/consumer for physical plans (i.e., don’t assume parity). ([GitHub][15])

**Operational consequence (agent rule)**

* Use Substrait as the **logical** interchange/fingerprint layer; regenerate physical plans per environment (hardware, file layout, runtime config). This matches your guide’s “Substrait portable, proto best-effort” stance.

---

## Practical “agent contract” (minimum viable bundle for distributed orchestration)

If you must choose one canonical persistence envelope:

1. **Primary**: Substrait bytes (`Serde.serialize_bytes(sql, ctx)` or `Producer.to_substrait_plan(lp, ctx).encode()`) + hash. ([Apache DataFusion][5])
2. **Secondary debug**: `EXPLAIN FORMAT PGJSON` / `FORMAT INDENT` strings (human + machine triage).
3. **Best-effort**: `LogicalPlan.to_proto()` / `ExecutionPlan.to_proto()` only when sources are serializable and you control version skew; never assume cross-version decode. ([Apache DataFusion][6])

[1]: https://docs.rs/datafusion-proto/latest/datafusion_proto/?search= "datafusion_proto - Rust"
[2]: https://docs.rs/crate/datafusion-proto/latest "datafusion-proto 52.1.0 - Docs.rs"
[3]: https://substrait.io/ "Home - Substrait: Cross-Language Serialization for Relational Algebra"
[4]: https://datafusion.apache.org/user-guide/introduction.html "Introduction — Apache DataFusion  documentation"
[5]: https://datafusion.apache.org/python/autoapi/datafusion/substrait/index.html "datafusion.substrait — Apache Arrow DataFusion  documentation"
[6]: https://datafusion.apache.org/python/autoapi/datafusion/plan/index.html "datafusion.plan — Apache Arrow DataFusion  documentation"
[7]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[8]: https://github.com/apache/datafusion-python/issues/1181 "Serialize user defined functions and table providers via protobuf · Issue #1181 · apache/datafusion-python · GitHub"
[9]: https://protobuf.dev/programming-guides/serialization-not-canonical/ "Proto Serialization Is Not Canonical | Protocol Buffers Documentation"
[10]: https://datafusion.apache.org/library-user-guide/upgrading.html "Upgrade Guides — Apache DataFusion  documentation"
[11]: https://docs.rs/datafusion-proto/latest/datafusion_proto/logical_plan/index.html "datafusion_proto::logical_plan - Rust"
[12]: https://docs.rs/datafusion-proto/latest/datafusion_proto/physical_plan/index.html "datafusion_proto::physical_plan - Rust"
[13]: https://apache.googlesource.com/datafusion-python/%2B/refs/tags/48.0.0/python/tests/test_substrait.py "python/tests/test_substrait.py - datafusion-python - Git at Google"
[14]: https://github.com/apache/datafusion/issues/9299 "Can't serialize example `ExecutionPlan` to substrait · Issue #9299 · apache/datafusion · GitHub"
[15]: https://github.com/apache/datafusion/issues/5173 "[EPIC] Substrait: Add producer and consumer for physical plans · Issue #5173 · apache/datafusion · GitHub"

According to a document from **January 27, 2026** and the upstream DataFusion library docs / API, “extensibility” in planning reduces to **four programmable seams**: **(i)** SQL→`Expr` / SQL→`LogicalPlan` interception (planners), **(ii)** `LogicalPlan::Extension` (custom logical operators), **(iii)** logical + physical **rule pipelines** (rewrite passes), and **(iv)** datasource + function registries (`TableProvider`, UDFs) that parameterize binding + pushdowns.  ([datafusion.apache.org][1])

---

## 12) Extensibility: planning hooks you can build systems on

### 12.0 Hook selection: which extension point to use (decision procedure)

**Select exactly one “primary” hook per feature; compose only when forced.**

1. **Need custom SQL surface syntax** (new operator, new literal, new FROM clause element, new type):

* Implement **SQL extension planners** (`ExprPlanner`, `RelationPlanner`, `TypePlanner`) that intercept SQL AST during `SqlToRel`. ([datafusion.apache.org][1])
* Registration is explicit and ordered (LIFO for expr/relation; single active type planner). ([datafusion.apache.org][1])

2. **Need a new relational operator not representable as existing LogicalPlan nodes**:

* Implement a **custom logical node** and wrap it in `LogicalPlan::Extension`. `LogicalPlan` explicitly includes an `Extension` variant.  ([Docs.rs][2])
* Either (a) rewrite it away into standard operators before physical planning, or (b) supply a physical execution implementation. ([datafusion.apache.org][1])

3. **Need datasource intelligence / external indexes / manifest-driven file selection / custom pruning**:

* Implement a custom **`TableProvider`**: planning boundary is `scan(projection, filters, limit)` (and the newer `scan_with_args(ScanArgs)` for structured parameters). Your planner payoff is in pushdown classification + scan topology.  ([Docs.rs][3])

4. **Need custom computation** (scalar/aggregate/window/table functions):

* Implement UDFs and register into `SessionContext` / `SessionState` so the binder can resolve them; set **`Volatility`** precisely to unlock constant folding / CSE / inlining eligibility. ([datafusion.apache.org][4])

5. **Need domain-specific rewrites** (canonicalization, index substitution, semantic “lowering”, physical topology edits):

* Implement `OptimizerRule` / `PhysicalOptimizerRule` and register into the session. DataFusion’s docs frame operator extension as transforming `LogicalPlan` and `ExecutionPlan` with custom rules.  ([datafusion.apache.org][5])

---

## 12.1 Custom logical operators (`LogicalPlan::Extension`)

### 12.1.1 IR location and non-negotiable invariants

* `LogicalPlan` is an enum that includes `Extension(Extension)`; extension nodes are first-class LogicalPlan variants. ([Docs.rs][2])
* Optimizer compatibility requires the extension node to expose:

  * `inputs()` (children), `expressions()` (non-recursive), `schema()`, and **a constructor** `with_exprs_and_inputs(exprs, inputs)` used during rewrites. ([Docs.rs][6])
* If these are wrong/incomplete, you silently disable (or misapply) pushdowns and rule rewrites.

### 12.1.2 Implementing a custom logical node: `UserDefinedLogicalNodeCore` (recommended)

`UserDefinedLogicalNodeCore` is the minimal “optimizer-friendly” surface. Required methods: `name/inputs/schema/expressions/fmt_for_explain/with_exprs_and_inputs`; key provided defaults affect pushdown and must be overridden deliberately. ([Docs.rs][6])

**Skeleton (Rust; compile-time shape, not business logic):**

```rust
use std::sync::Arc;
use datafusion::common::{DFSchema, DFSchemaRef, Result};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MyExtNode {
    input: Arc<LogicalPlan>,
    schema: DFSchemaRef,
    exprs: Vec<Expr>, // any logical expressions your node semantically uses
}

impl UserDefinedLogicalNodeCore for MyExtNode {
    fn name(&self) -> &str { "MyExt" }

    fn inputs(&self) -> Vec<&LogicalPlan> { vec![self.input.as_ref()] }

    fn schema(&self) -> &Arc<DFSchema> { &self.schema }

    fn expressions(&self) -> Vec<Expr> { self.exprs.clone() }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MyExt: exprs={}", self.exprs.len())
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        // optimizer will call this during rewrites; preserve invariants
        Ok(Self {
            input: Arc::new(inputs.into_iter().next().expect("1 input")),
            schema: Arc::clone(&self.schema),
            exprs,
        })
    }

    // override provided methods below as needed (see §12.1.3)
}
```

Required-method list and the `with_exprs_and_inputs` rewrite contract are explicit in the API docs. ([Docs.rs][6])

### 12.1.3 Pushdown semantics across extension nodes (defaults are “block everything”)

The **provided methods** on `UserDefinedLogicalNodeCore` encode pushdown behavior:

* `prevent_predicate_push_down_columns()`:

  * Default returns **all output columns**, preventing any predicate from being pushed below the node. ([Docs.rs][6])
  * Override to return only columns whose predicates are **semantics-changing** if moved below (e.g., after a non-deterministic transform).

* `necessary_children_exprs(output_columns)`:

  * Default `None` ⇒ optimizer cannot compute required input columns for projection pushdown through the node. ([Docs.rs][6])
  * Override to map required output columns → required input columns per child (vector of vectors), enabling upstream projection pruning.

* `supports_limit_pushdown()`:

  * If `true`, `LIMIT` can be pushed below the node; if `false`, you may force full scans even when downstream requests few rows. ([Docs.rs][6])

**Operational rule**: for any extension node that is logically transparent (wraps a child without changing row semantics), explicitly:

* allow limit pushdown (`supports_limit_pushdown=true`),
* allow predicate pushdown except for computed columns,
* implement `necessary_children_exprs` so projection pushdown survives the node.

### 12.1.4 Making it runnable: “rewrite to standard” vs “custom physical exec”

DataFusion documents two implementation strategies for custom syntax/semantics:

1. rewrite to standard SQL / existing operators, or
2. create **custom logical + custom physical** nodes; both are required for end-to-end execution. ([datafusion.apache.org][1])

**If you keep `LogicalPlan::Extension` into physical planning**, you must provide an executable counterpart:

* implement `ExecutionPlan` for your operator (name/schema/properties/requirements/children/execute). ([Docs.rs][7])
* ensure `properties()` and `required_input_distribution/ordering` are correct; physical optimizer rules rely on them for enforcement.

---

## 12.2 Custom optimizer rules (logical + physical)

### 12.2.1 Logical optimizer rules (`OptimizerRule`)

DataFusion’s optimizer is explicitly a set of modular `OptimizerRule`s (logical) and `PhysicalOptimizerRule`s that rewrite plans while preserving results.  ([datafusion.apache.org][5])

**Trait surface (key methods):**

* `name() -> &str`
* optional recursion policy via `apply_order() -> Option<ApplyOrder>` (TopDown / BottomUp), otherwise rule handles recursion itself. ([Docs.rs][8])
* `rewrite(plan, config) -> Result<Transformed<LogicalPlan>>` returning `yes/no`. ([Docs.rs][9])

**Canonical “extension optimizer” pattern (DataFusion µWheel example):**

* identify a semantic pattern,
* consult external index,
* rewrite plan into an equivalent `LogicalPlan::TableScan` over a `MemTable` (plan-time materialization) or into a different scan strategy. ([datafusion.apache.org][10])

**Registration (Rust; session-level):**

* `SessionContext.add_optimizer_rule(...)` / `remove_optimizer_rule(name)` exist as direct methods. ([Docs.rs][11])
* If you need full control over rulepack ordering and composition, build via `SessionStateBuilder.with_optimizer_rule(s)` / `with_optimizer_rules(...)`. ([Docs.rs][12])

### 12.2.2 Physical optimizer rules (`PhysicalOptimizerRule`)

Physical rules transform `ExecutionPlan` trees after physical planning (distribution/order enforcement, join selection, limit pushdown, etc.). ([datafusion.apache.org][5])

**Trait surface (core):**

* `optimize(plan: Arc<dyn ExecutionPlan>, config: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>>`
* `name()`
* `schema_check()` toggles schema validation after rewrite (disable if nullable changes). ([Docs.rs][13])

**Registration (Rust):**

* `SessionStateBuilder.with_physical_optimizer_rule(s)` / `with_physical_optimizer_rules(...)` exist; session builder explicitly exposes `physical_optimizer_rules` and “with_*” methods. ([Docs.rs][12])

**Placement constraints**:

* If you introduce new distribution/order requirements, you must do so in a way compatible with the standard enforcement rules (`EnforceDistribution`, `EnforceSorting`) that inspect requirements and insert repartition/sort nodes. ([Docs.rs][14])

---

## 12.3 Custom `TableProvider` and scan-time planning

### 12.3.1 Boundary split: `TableSource` (planning) vs `TableProvider` (execution)

* `TableSource` is the planning-time subset: schema + pushdown capabilities; exists specifically to decouple logical planning from the execution engine. ([Docs.rs][15])
* `TableProvider` adds physical execution hooks (`scan` / insert semantics) and is where scan topology is constructed. ([Docs.rs][3])

### 12.3.2 The scan contract (what DataFusion will pass you)

`TableProvider::scan` receives:

* `projection: Option<Vec<usize>>` (indices into `schema()`) for projection pushdown,
* `filters: &[Expr]` (ANDed) **only if** you override `supports_filters_pushdown`,
* `limit: Option<usize>` for limit pushdown. ([Docs.rs][3])

**Critical footgun (filter columns not in projection):** DataFusion warns that a column may appear only in a pushed-down filter; the projection may omit it, but the provider must still evaluate the predicate correctly. ([Docs.rs][3])

**Structured scan (newer API):** `scan_with_args(ScanArgs) -> ScanResult` exists to pass projection/filters/limit plus evolving parameters like ordering preferences; override when you need ordering-aware scans. ([Docs.rs][16])

### 12.3.3 Pushdown truth table: `supports_filters_pushdown` → `TableProviderFilterPushDown`

To receive filters in `scan`, override `supports_filters_pushdown(filters: &[&Expr]) -> Vec<TableProviderFilterPushDown>`. ([Docs.rs][3])

`TableProviderFilterPushDown` is **three-valued**:

* `Unsupported`: provider can’t use predicate
* `Inexact`: provider can prune but may return extra rows; DataFusion re-applies a Filter above the scan
* `Exact`: provider guarantees predicate is fully applied; no residual filter added ([Docs.rs][17])

This exact “three-valued pushdown” is called out in your guide as the scheduler-critical semantics model. 

### 12.3.4 What scan capabilities change in plans (verification protocol)

Your guide treats EXPLAIN + plan objects as the canonical “pushdown witness”:

* logical `TableScan … projection=[…], partial_filters=[…]`
* physical scan (`DataSourceExec … file_groups=…, projection=[…], predicate=…`) for work-unit + predicate truth.  ([datafusion.apache.org][5])

**Agent verification loop (non-negotiable):**

1. Register provider/table.
2. Run `EXPLAIN VERBOSE` for at least one representative query.
3. Assert: scan node shows expected projection/predicate/fetch, and residual Filter exists iff any pushed predicate is `Inexact`. ([datafusion.apache.org][18])

### 12.3.5 Minimal custom provider wiring (Rust, end-to-end)

DataFusion’s “Custom Table Provider” guide is explicit: implement `TableProvider`, implement an `ExecutionPlan`, register the provider with `SessionContext`. ([datafusion.apache.org][18])

```rust
use std::sync::Arc;
use datafusion::execution::context::SessionContext;
use datafusion::datasource::TableProvider;

let ctx = SessionContext::new();
ctx.register_table("customers", Arc::new(my_provider))?;
let df = ctx.sql("SELECT id FROM customers").await?;
```

([datafusion.apache.org][18])

---

## 12.4 Function system and expression planning hooks

### 12.4.1 Function registration surfaces (planning-visible)

`SessionContext` has explicit registration/deregistration methods for:

* scalar UDF (`register_udf`), aggregate UDF (`register_udaf`), window UDF (`register_udwf`), table function (`register_udtf`) and corresponding deregistration calls. ([Docs.rs][19])

Binder implication: if a function name resolves, it becomes an `Expr` node in the LogicalPlan; volatility + signature affect downstream optimizations.

### 12.4.2 Scalar UDF: `ScalarUDFImpl` is a planning contract, not just “code to run”

`ScalarUDFImpl` methods encode planning-critical metadata:

* `signature()` returns argument typing + **Volatility**
* `return_type(arg_types)` and/or `return_field_from_args`
* `invoke_with_args(args: ScalarFunctionArgs) -> ColumnarValue`
* optional `coerce_types` for user-defined signatures
* optional `output_ordering` / `preserves_lex_ordering` to participate in ordering analysis
* interval/bounds methods used by selectivity / simplification logic (`evaluate_bounds`, etc.) ([Docs.rs][20])

Example in the API docs: create `ScalarUDF` from implementation, then create an `Expr` via `.call(...)`. ([Docs.rs][20])

```rust
let udf = ScalarUDF::from(AddOne::new());
let expr = udf.call(vec![col("a")]); // planning-time Expr node
```

([Docs.rs][20])

### 12.4.3 Volatility: optimizer eligibility gate

`Volatility` is a first-class enum (`Immutable | Stable | Volatile`) and determines eligibility for optimizations; docs explicitly recommend selecting the strictest possible volatility. ([Docs.rs][21])

Planning consequence:

* `Immutable` enables inlining/constant folding of calls like `abs(-1)` during planning (value computed and replaced). ([Docs.rs][22])

### 12.4.4 Aggregate / window / table UDFs (type + planning surface)

The UDF guide enumerates:

* Scalar (vectorized arrays),
* Window,
* Aggregate,
* Table functions returning a `TableProvider` used in the plan. ([datafusion.apache.org][4])

Table-function implication: UDTF is effectively a *parameterized datasource factory* that runs at planning time to produce a `TableProvider` for the scan. ([datafusion.apache.org][4])

### 12.4.5 SQL `CREATE FUNCTION`: `FunctionFactory` is mandatory plumbing

Your addendum correctly flags that SQL `CREATE FUNCTION` requires a configured factory; DataFusion exposes a `FunctionFactory` install point at the session/state layer.  ([Docs.rs][11])

Concrete API surfaces:

* `SessionContext.with_function_factory(Arc<dyn FunctionFactory>)` exists. ([Docs.rs][11])
* `SessionStateBuilder.with_function_factory(...)` exists as a builder method. ([Docs.rs][12])

### 12.4.6 “Custom expression planning”: `ExprPlanner` (SQL AST → DataFusion `Expr`)

`ExprPlanner` is the sanctioned hook to customize how SQL expressions are planned:

* custom operators (`->`, `@>`, etc),
* custom field access,
* custom aggregate/window handling. ([datafusion.apache.org][1])

Planner semantics:

* Register multiple planners; invoked in **reverse registration order** (last wins). Return `Original(...)` to delegate. ([datafusion.apache.org][1])
* Registration method is explicit (`ctx.register_expr_planner()`); `SessionStateBuilder.with_expr_planners(...)` exists for embedding.  ([datafusion.apache.org][1])

**Example (binary operator override; from docs):**

```rust
use datafusion_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};

#[derive(Debug)]
struct MyCustomPlanner;

impl ExprPlanner for MyCustomPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        match &expr.op {
            BinaryOperator::Arrow => Ok(PlannerResult::Planned(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(expr.left.clone()),
                right: Box::new(expr.right.clone()),
                op: Operator::StringConcat,
            }))),
            _ => Ok(PlannerResult::Original(expr)),
        }
    }
}
```

([datafusion.apache.org][1])

**When to use vs optimizer rewrite**:

* Use `ExprPlanner` when the *SQL surface* is nonstandard and must be recognized during `SqlToRel`.
* Use `OptimizerRule` when you can accept standard SQL/Expr formation and only need canonicalization/lowering after binding (e.g., rewrite certain function calls into `LogicalPlan::Extension` or into an indexed `TableScan`). ([datafusion.apache.org][1])

[1]: https://datafusion.apache.org/library-user-guide/extending-sql.html "Extending SQL Syntax — Apache DataFusion  documentation"
[2]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html "LogicalPlan in datafusion_expr::logical_plan - Rust"
[3]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html "TableProvider in datafusion::datasource - Rust"
[4]: https://datafusion.apache.org/library-user-guide/functions/adding-udfs.html "Adding User Defined Functions: Scalar/Window/Aggregate/Table Functions — Apache DataFusion  documentation"
[5]: https://datafusion.apache.org/library-user-guide/query-optimizer.html "Query Optimizer — Apache DataFusion  documentation"
[6]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.UserDefinedLogicalNodeCore.html "UserDefinedLogicalNodeCore in datafusion::logical_expr - Rust"
[7]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html "ExecutionPlan in datafusion::physical_plan - Rust"
[8]: https://docs.rs/datafusion/latest/datafusion/optimizer/optimizer/enum.ApplyOrder.html?utm_source=chatgpt.com "ApplyOrder in datafusion::optimizer::optimizer - Rust"
[9]: https://docs.rs/datafusion/latest/datafusion/optimizer/scalar_subquery_to_join/struct.ScalarSubqueryToJoin.html?utm_source=chatgpt.com "ScalarSubqueryToJoin in datafusion::optimizer"
[10]: https://datafusion.apache.org/library-user-guide/extending-operators.html "Extending Operators — Apache DataFusion  documentation"
[11]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html?utm_source=chatgpt.com "SessionContext in datafusion::execution::context - Rust"
[12]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionStateBuilder.html "SessionStateBuilder in datafusion::execution::session_state - Rust"
[13]: https://docs.rs/datafusion/latest/datafusion/physical_optimizer/trait.PhysicalOptimizerRule.html?utm_source=chatgpt.com "PhysicalOptimizerRule in datafusion::physical_optimizer"
[14]: https://docs.rs/datafusion/latest/datafusion/physical_optimizer/index.html?utm_source=chatgpt.com "datafusion::physical_optimizer - Rust"
[15]: https://docs.rs/datafusion-expr/latest/datafusion_expr/trait.TableSource.html "TableSource in datafusion_expr - Rust"
[16]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html "TableProvider in datafusion::catalog - Rust"
[17]: https://docs.rs/datafusion-expr/latest/datafusion_expr/enum.TableProviderFilterPushDown.html "TableProviderFilterPushDown in datafusion_expr - Rust"
[18]: https://datafusion.apache.org/library-user-guide/custom-table-providers.html "Custom Table Provider — Apache DataFusion  documentation"
[19]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html "SessionContext in datafusion::execution::context - Rust"
[20]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.ScalarUDFImpl.html "ScalarUDFImpl in datafusion::logical_expr - Rust"
[21]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Volatility.html?utm_source=chatgpt.com "Volatility in datafusion::logical_expr - Rust"
[22]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Volatility.html?search=&utm_source=chatgpt.com "\"\" Search - Rust"

According to a document from **January 27, 2026**, the orchestration-grade rule is: **treat `SessionConfig` + `RuntimeEnv` knobs as first-class planning inputs** and snapshot them alongside `(optimized_logical, physical)` because they directly affect partition topology, pushdown visibility, and explain artifacts.

---

## 13) Configuration knobs that materially change plan shapes

### 13.1 Configuration surfaces and precedence (Rust + SQL + Python)

#### 13.1.1 Rust (engine): `ConfigOptions` / env vars / SQL `SET`

DataFusion config is key-namespaced (`datafusion.*`) and can be set:

* **programmatically** via `ConfigOptions` (typed fields),
* via **environment variables** parsed by `ConfigOptions::from_env`,
* via **SQL** `SET key = 'value'`. ([Apache DataFusion][1])

Concrete (Rust):

```rust
use datafusion::common::config::ConfigOptions;

let mut config = ConfigOptions::new();
config.execution.target_partitions = 8; // typed field
```

([Apache DataFusion][1])

Concrete (env var):

```bash
DATAFUSION_EXECUTION_TARGET_PARTITIONS=8 ./your_program
```

([Apache DataFusion][1])

Concrete (SQL):

```sql
SET datafusion.execution.target_partitions = '8';
```

([Apache DataFusion][1])

#### 13.1.2 Python (binding): `SessionConfig` fluent modifiers + generic `set(key,value)`

Python `SessionContext(config=SessionConfig(...), runtime=RuntimeEnvBuilder(...))` is the standard configuration injection point. ([Apache DataFusion][2])

You have **two** configuration APIs in Python:

1. **Typed/fluent**:

```python
from datafusion import SessionConfig

cfg = (
  SessionConfig()
  .with_target_partitions(8)
  .with_repartition_joins(True)
  .with_repartition_aggregations(True)
  .with_repartition_windows(True)
)
```

([Apache DataFusion][2])

2. **Generic key/value** (covers any `datafusion.*` key without needing a dedicated fluent wrapper):

```python
cfg = (
  SessionConfig()
  .set("datafusion.execution.target_partitions", "8")
  .set("datafusion.execution.parquet.pruning", "true")
  .set("datafusion.optimizer.repartition_joins", "true")
)
```

Python API defines `SessionConfig.set(key: str, value: str) -> SessionConfig`. ([Apache DataFusion][3])

---

### 13.2 Partitioning targets (parallelism scalar → repartition topology)

#### 13.2.1 `datafusion.execution.target_partitions`

**Semantics**: “Number of partitions for query execution”; higher → more concurrency; default `0` meaning “defaults to CPU cores.” ([Apache DataFusion][1])

**Shape effects**:

* Physical plan `partition_count` changes broadly (scan partitions, intermediate `RepartitionExec`, final merges).
* Repartition-enabled operators (joins/aggs/windows/sorts/file_scans) will target this value and insert exchanges.

**Set it**:

```sql
SET datafusion.execution.target_partitions = '32';
```

([Apache DataFusion][1])

Python:

```python
cfg = SessionConfig().with_target_partitions(32)
```

([Apache DataFusion][2])

#### 13.2.2 Heuristic regimes (from DataFusion tuning guidance)

* Very small datasets (< ~1MB): set `target_partitions = 1` to avoid repartition overhead. ([Apache DataFusion][1])
* Tight memory limits: reduce `target_partitions` to allocate more memory per partition (FairSpillPool divides memory across partitions); optionally reduce `batch_size`. ([Apache DataFusion][1])

---

### 13.3 Repartition toggles (optimizer) → explicit `RepartitionExec` / `CoalescePartitionsExec` / merge nodes

These toggles are **plan-shape switches** because they decide whether the physical optimizer inserts exchanges to hit `target_partitions`.

#### 13.3.1 Join/Agg/Window/Sor­t repartition gates

* `datafusion.optimizer.repartition_joins` (default `true`): repartition on join keys to parallelize joins. ([Apache DataFusion][1])
* `datafusion.optimizer.repartition_aggregations` (default `true`): repartition on group keys to parallelize aggregates. ([Apache DataFusion][1])
* `datafusion.optimizer.repartition_windows` (default `true`): repartition on window partition keys. ([Apache DataFusion][1])
* `datafusion.optimizer.repartition_sorts` (default `true`): enables sort parallelization / better multithreaded shape; docs include the canonical rewrite from `SortExec + Coalesce + Repartition` → `SortPreservingMergeExec + SortExec + Repartition`. ([Apache DataFusion][1])

**Set (SQL)**:

```sql
SET datafusion.optimizer.repartition_joins = 'true';
SET datafusion.optimizer.repartition_aggregations = 'true';
SET datafusion.optimizer.repartition_windows = 'true';
SET datafusion.optimizer.repartition_sorts = 'true';
```

([Apache DataFusion][1])

**Set (Python)**:

```python
cfg = (
  SessionConfig()
  .with_repartition_joins(True)
  .with_repartition_aggregations(True)
  .with_repartition_windows(True)
  .with_repartition_sorts(True)
)
```

Python binding exposes `with_repartition_sorts` in `SessionConfig` (in addition to the join/agg/window toggles). ([Apache DataFusion][3])

#### 13.3.2 File scan repartition (intra-file parallelism)

* `datafusion.optimizer.repartition_file_scans` (default `true`): if enabled, the optimizer may repartition within scans (depends on source; special semantics for FileSource vs in-memory). ([Apache DataFusion][1])
* `datafusion.optimizer.repartition_file_min_size` (default `10,485,760` bytes): minimum total file size to perform file scan repartitioning. ([Apache DataFusion][1])

**Critical limitation**: tuning guidance notes this does not apply to user-defined sources and relies on accurate statistics. ([Apache DataFusion][1])

Python surface includes both `with_repartition_file_scans` and `with_repartition_file_min_size`. ([apache.googlesource.com][4])

#### 13.3.3 “Preserve file partitions” (avoid unnecessary hash exchanges)

* `datafusion.optimizer.preserve_file_partitions` (thresholded enablement): declares hash partitioning by hive partition columns when distinct partition values exceed threshold; affects whether downstream operators accept scan partitioning without extra repartition. ([Apache DataFusion][1])

#### 13.3.4 Subset satisfaction vs forced repartition (aggregation key supersets)

* `datafusion.optimizer.subset_repartition_threshold` (default `4`): controls whether existing partitioning that is a subset of required partitioning can be reused vs forcing repartition; docs include explicit example with `Hash([a])` satisfying `Hash([a,b])` above threshold. ([Apache DataFusion][1])

---

### 13.4 Pruning toggles (metadata read + row-group/page skip + late materialization)

#### 13.4.1 Table-creation statistics (ListingTable only): `datafusion.execution.collect_statistics`

* `datafusion.execution.collect_statistics` (default `true`): collects statistics when first creating a table; **no effect after the table is created**; applies to default `ListingTableProvider`. ([Apache DataFusion][1])

**Operational consequence**: you must set it **before** `CREATE EXTERNAL TABLE` if you want cost/selectivity improvements downstream.

#### 13.4.2 Parquet pruning/pushdown core switches

All below are in `datafusion.execution.parquet.*` and materially affect scan behavior, often visible as `DataSourceExec predicate=...` plus `EXPLAIN ANALYZE` pruning counters:

* `enable_page_index` (default `true`): reads Parquet Page Index metadata to reduce I/O / rows decoded. ([Apache DataFusion][1])
* `pruning` (default `true`): skip entire row groups using predicate + min/max metadata. ([Apache DataFusion][1])
* `bloom_filter_on_read` (default `true`): use available bloom filters during reads. ([Apache DataFusion][1])
* `pushdown_filters` (default `false`): apply filters during decoding (“late materialization”) to reduce rows decoded. ([Apache DataFusion][1])
* `reorder_filters` (default `false`): reorder pushed filters heuristically to minimize evaluation cost. ([Apache DataFusion][1])
* `force_filter_selections` (default `false`): force RowSelections when `pushdown_filters` is enabled. ([Apache DataFusion][1])
* `metadata_size_hint` (default `524288`): optimistic footer fetch to reduce object store reads. ([Apache DataFusion][1])
* `skip_metadata` (default `true`): skip optional embedded schema metadata (schema-conflict mitigation; can affect planning-time schema reconciliation). ([Apache DataFusion][1])

**Set (SQL)**:

```sql
SET datafusion.execution.parquet.pruning = 'true';
SET datafusion.execution.parquet.enable_page_index = 'true';
SET datafusion.execution.parquet.bloom_filter_on_read = 'true';
SET datafusion.execution.parquet.pushdown_filters = 'true';   -- opt-in; validate with EXPLAIN ANALYZE
```

([Apache DataFusion][1])

**Set (Python via key/value)**:

```python
cfg = (
  SessionConfig()
  .set("datafusion.execution.parquet.pruning", "true")
  .set("datafusion.execution.parquet.enable_page_index", "true")
  .set("datafusion.execution.parquet.pushdown_filters", "true")
)
```

([Apache DataFusion][3])

---

### 13.5 Optimizer enable/disable switches (rulepack gates + physical algorithm selection)

#### 13.5.1 Fixpoint behavior + failure policy

* `datafusion.optimizer.max_passes` (default `3`): optimizer fixpoint attempt bound. ([Apache DataFusion][1])
* `datafusion.optimizer.skip_failed_rules` (default `false` per config page excerpt): if `true`, optimizer continues after rule errors; if `false`, query fails on rule error. ([Apache DataFusion][1])

Use when: you want deterministic failures in CI plan regression; set `skip_failed_rules=false` and pin `max_passes`.

#### 13.5.2 Dynamic filter pushdown (runtime-derived predicates injected into scans)

* `datafusion.optimizer.enable_join_dynamic_filter_pushdown` (default `true`) and `enable_aggregate_dynamic_filter_pushdown` (default `true`) toggle whether runtime dynamic filters are pushed into file scan phase. ([Apache DataFusion][1])

Shape effects:

* physical `DataSourceExec predicate=...` may include dynamic predicates; your own guide flags scan predicate may be placeholder before execution and updated during execution (late-binding). ([Apache DataFusion][1])

#### 13.5.3 Join algorithm selection (physical plan variant)

Config page explicitly: physical planner chooses join algorithm based on statistics + join condition, and optimizer selection can be influenced via `datafusion.optimizer.*` keys. ([Apache DataFusion][1])

High-impact toggles:

* `datafusion.optimizer.prefer_hash_join` (default `true`): prefer HashJoin vs SortMergeJoin (memory trade). ([Apache DataFusion][1])
* `datafusion.optimizer.enable_piecewise_merge_join` (default `false`): allow experimental PiecewiseMergeJoin under specific join predicate shape. ([Apache DataFusion][1])
* `datafusion.optimizer.allow_symmetric_joins_without_pruning` (default `true`): controls symmetric hash joins for unbounded sources (buffer pruning constraints). ([Apache DataFusion][1])

**Set (SQL)**:

```sql
SET datafusion.optimizer.prefer_hash_join = 'false';
SET datafusion.optimizer.enable_piecewise_merge_join = 'true';
```

([Apache DataFusion][1])

#### 13.5.4 Sort pushdown (scan-order optimization; keeps Sort for correctness)

* `datafusion.optimizer.enable_sort_pushdown` (default `true`): push sort requirements down to data sources that can read in reverse order; returns **inexact ordering** and keeps a Sort operator for correctness, but enables early termination for TopK (ORDER BY … LIMIT N). ([Apache DataFusion][1])

---

### 13.6 Explain formatting + analyze detail levels (artifact determinism)

#### 13.6.1 EXPLAIN syntax + hard constraints

`EXPLAIN [ANALYZE] [VERBOSE] [FORMAT format] statement`. ([Apache DataFusion][5])

* `EXPLAIN VERBOSE` only supports `indent`. ([Apache DataFusion][5])
* `EXPLAIN ANALYZE` only supports `indent`. ([Apache DataFusion][5])
* If `FORMAT` omitted, EXPLAIN uses the format from config key `datafusion.explain.format`. ([Apache DataFusion][5])

**Artifact rule**: always set `FORMAT` explicitly in golden fixtures (or pin `datafusion.explain.format`) to eliminate drift.

#### 13.6.2 Explain config keys that affect emitted artifacts

* `datafusion.explain.format` (`indent` | `tree`) ([Apache DataFusion][1])
* `datafusion.explain.tree_maximum_render_width` (tree only) ([Apache DataFusion][1])
* `datafusion.explain.show_schema` / `show_statistics` / `show_sizes` (physical verbosity) ([Apache DataFusion][1])
* `datafusion.explain.logical_plan_only` / `physical_plan_only` ([Apache DataFusion][1])
* `datafusion.explain.analyze_level` (`summary` vs `dev`) controls EXPLAIN ANALYZE metric depth. ([Apache DataFusion][1])

**Set (SQL)**:

```sql
SET datafusion.explain.format = 'indent';
SET datafusion.explain.show_schema = 'true';
SET datafusion.explain.show_statistics = 'true';
SET datafusion.explain.analyze_level = 'dev';
```

([Apache DataFusion][1])

---

### 13.7 Planning profile template (settings + plan artifacts + reproducibility envelope)

#### 13.7.1 Snapshot the *effective* session settings (not your intended settings)

Use `SHOW ALL` or `information_schema.df_settings` (requires info schema enabled). ([Apache DataFusion][6])

```sql
-- enable info schema if needed
SET datafusion.catalog.information_schema = 'true';

-- snapshot all effective config values
SELECT * FROM information_schema.df_settings;
```

([Apache DataFusion][6])

#### 13.7.2 Minimal “PlanningProfile” schema (JSON envelope)

Persist one blob per planned query:

```json
{
  "profile_schema_version": 1,
  "engine": {
    "datafusion_build": "string (capture from deployment metadata)",
    "python_pkg_version": "importlib.metadata.version('datafusion')"
  },
  "session_settings": {
    "df_settings": [
      {"name": "datafusion.execution.target_partitions", "setting": "8"},
      {"name": "datafusion.optimizer.repartition_joins", "setting": "true"}
    ]
  },
  "plan_artifacts": {
    "sql": "…",
    "optimized_logical_indent_schema": "…",
    "physical_indent": "…",
    "explain_indent_rows": [
      {"plan_type": "logical_plan", "plan": "…"},
      {"plan_type": "physical_plan", "plan": "…"}
    ],
    "explain_verbose_rows": [
      {"plan_type": "logical_plan after …", "plan": "…"}
    ]
  }
}
```

#### 13.7.3 Reference implementation (Python) — generate profile deterministically

```python
import importlib.metadata as md
from datafusion import SessionContext

def plan_profile(ctx: SessionContext, sql: str) -> dict:
    # 1) settings snapshot (requires information_schema enabled in config)
    settings_tbl = ctx.sql("SELECT name, setting FROM information_schema.df_settings").to_arrow_table()

    # 2) core plan artifacts
    df = ctx.sql(sql)
    lp = df.optimized_logical_plan()
    pp = df.execution_plan()

    # 3) capturable explain (avoid df.explain() which prints)
    explain_tbl = ctx.sql(f"EXPLAIN FORMAT INDENT {sql}").to_arrow_table()
    verbose_tbl = ctx.sql(f"EXPLAIN VERBOSE {sql}").to_arrow_table()

    return {
        "profile_schema_version": 1,
        "engine": {"python_pkg_version": md.version("datafusion")},
        "session_settings": {
            "df_settings": [dict(zip(settings_tbl.column_names, row)) for row in settings_tbl.to_pylist()],
        },
        "plan_artifacts": {
            "sql": sql,
            "optimized_logical_indent_schema": lp.display_indent_schema(),
            "physical_indent": pp.display_indent(),
            "explain_indent_rows": explain_tbl.to_pylist(),
            "explain_verbose_rows": verbose_tbl.to_pylist(),
        },
    }
```

This template matches the orchestration premise in your guide: use **optimized logical** for stable semantics + pushdown truth, and **physical** + **EXPLAIN** for execution hints; then record config that materially changes both.

[1]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[2]: https://datafusion.apache.org/python/user-guide/configuration.html "Configuration — Apache Arrow DataFusion  documentation"
[3]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[4]: https://apache.googlesource.com/datafusion-python/%2B/refs/tags/51.0.0-rc2/python/datafusion/context.py "python/datafusion/context.py - datafusion-python - Git at Google"
[5]: https://datafusion.apache.org/user-guide/sql/explain.html "EXPLAIN — Apache DataFusion  documentation"
[6]: https://datafusion.apache.org/user-guide/sql/information_schema.html "Information Schema — Apache DataFusion  documentation"

According to a document from **January 27, 2026**, the production-grade posture is: **(a)** treat optimized logical as the stable “semantic backbone,” **(b)** treat physical as **execution hints** (environment-dependent), and **(c)** snapshot **EXPLAIN** + **effective session config** as first-class regression artifacts. 

---

## 14) Applied planning discipline for production systems

### 14.1 Plan determinism

#### 14.1.1 Default non-determinism model (ordering is not a free invariant)

DataFusion plans are **parallel** by default; row order is not a stable contract unless you explicitly request it (SQL `ORDER BY` / enforced ordering pipeline). Physical plans explicitly incorporate hardware + file layout, so “same SQL” can yield different physical topology (and thus different row emission interleavings).

Concrete “ordering destroyers” (operators that typically invalidate prior ordering guarantees unless an order-preserving variant is used):

1. **Exchange / repartition**

* `RepartitionExec` may interleave multiple input partitions; default behavior does not preserve row order across partitions. If you need to preserve order across repartition, you must opt into `with_preserve_order` (runtime cost tradeoff). ([Docs.rs][1])
* Config-level trade: `datafusion.optimizer.prefer_existing_sort` toggles whether DataFusion tries to preserve existing ordering (using `preserve_order` + `SortPreservingMergeExec`) vs maximizing parallelism even if it requires resorting later. ([datafusion.apache.org][2])
* Failure mode class (real): repartitioning sorted scan output destroys sort order; this has caused correctness/plan issues and is explicitly called out in issue reports. ([GitHub][3])

2. **Union / multi-input concatenation**

* Unless the operator explicitly requires/propagates ordering across inputs, the merged output ordering is not guaranteed. (Union ordering requirement work is tracked/discussed.) ([GitHub][4])

3. **Hash-based operators**

* Hash joins / hash aggregations are primarily distribution-driven; output order is an implementation artifact. Treat as nondeterministic ordering unless followed by `SortExec`.

4. **LIMIT without ORDER BY**

* `LIMIT` bounds cardinality, not order. Determinism requires `ORDER BY … LIMIT …` (TopK pattern) and validating ordering enforcement in the physical plan.

#### 14.1.2 Make ordering explicit (and keep it explicit through physical optimization)

**Correctness contract** (agent rule): if downstream consumers require deterministic ordering, your query must encode a **total order**:

```sql
SELECT ...
FROM ...
WHERE ...
ORDER BY key1 ASC, key2 ASC, stable_tiebreaker ASC
LIMIT 1000;
```

Then enforce that the physical plan contains:

* `SortExec` (or equivalent) for per-partition ordering
* `SortPreservingMergeExec` (or equivalent) for global merge across partitions (if multi-partition output is produced)

Ordering is not “just optimization”; DataFusion performs ordering analysis to determine whether operator ordering requirements are satisfied by inputs, and this analysis drives sort insertion/removal decisions. ([datafusion.apache.org][5])

**Performance-preserving ordering discipline (when inputs are already sorted):**

* If your source files are sorted (or your scan declares `file_sort_order`), set:

```sql
SET datafusion.optimizer.prefer_existing_sort = 'true';
```

This biases the optimizer towards preserving ordering rather than repartitioning and re-sorting. ([datafusion.apache.org][2])

If you control physical plan construction (Rust), prefer the order-preserving repartition path only when downstream operators can exploit ordering:

```rust
// expensive, but preserves ordering when it matters
let rp = RepartitionExec::try_new(input, partitioning)?.with_preserve_order();
```

([Docs.rs][1])

#### 14.1.3 Validate determinism with plan artifacts (no guessing)

**Primary validation surface:** `EXPLAIN FORMAT INDENT` (planning-only) + `EXPLAIN VERBOSE` (per-rule trace). `EXPLAIN VERBOSE` only supports `indent` format; `EXPLAIN ANALYZE` only supports `indent` format. ([datafusion.apache.org][6])

Plan reading targets:

* presence/position of `RepartitionExec` (shuffle/exchange)
* presence/position of `SortExec`, `SortPreservingMergeExec`
* whether sorting is removed/added by physical optimizer stages (`EnforceSorting`, `OutputRequirements`, etc.)

**Minimal debug workflow (“why did it repartition/sort?”):**

1. Capture per-rule trace:

```sql
EXPLAIN VERBOSE
SELECT ...;
```

2. Locate the first “physical_plan after <rule>” row that introduces `RepartitionExec` / `SortExec`. (This is the authoritative answer; don’t infer.) ([datafusion.apache.org][6])

**Rust structured validation** (preferred when available): use `ExecutionPlan.properties()` and inspect ordering/partitioning rather than string parsing. ([Docs.rs][7])

---

### 14.2 Plan regression harness (CI-grade)

#### 14.2.1 Golden artifacts: minimal set

From your guide, the agent-ready harness snapshots **EXPLAIN FORMAT INDENT** output (often with schema shown) as the canonical fixture, and treats physical plan fields like `file_groups` as scheduling hints. 

**Recommended minimum bundle per query**:

1. Optimized logical (schema-aware):

* `df.optimized_logical_plan().display_indent_schema()`

2. Physical:

* `df.execution_plan().display_indent()`

3. Capturable explain:

* `EXPLAIN FORMAT INDENT <sql>` table rows `(plan_type, plan)`

4. Effective config snapshot:

* `SHOW ALL` or `SELECT * FROM information_schema.df_settings` (see §14.2.3)

Also: always pin EXPLAIN format (or set `datafusion.explain.format`) to avoid fixture drift when defaults change. 

#### 14.2.2 Fixture determinism rules (avoid false positives)

**Rule A (pin plan-shape knobs):** set these explicitly in your harness `SessionConfig` / SQL `SET` preamble:

* `datafusion.execution.target_partitions` (parallelism scalar)
* repartition toggles (`datafusion.optimizer.repartition_*`)
* pruning toggles (`datafusion.execution.parquet.*`) if you assert scan predicate shape

(Otherwise `partition_count`, exchange placement, and scan predicate absorption can drift.)

**Rule B (treat physical as “hints,” not semantic truth):** physical plan can vary with hardware + file layout. Your guide explicitly warns this; use it for debugging and scheduling, not for semantic cache keys. 

**Rule C (use stable carriers):**

* `EXPLAIN FORMAT INDENT` and `display_indent_schema()` are the “string fixtures” with maximal signal density.  ([datafusion.apache.org][6])

#### 14.2.3 Config drift detection (hard gate)

DataFusion exposes two equivalent introspection surfaces:

* `SHOW ALL`
* `select * from information_schema.df_settings;` ([datafusion.apache.org][8])

**Harness rule:** every golden plan fixture is keyed by **(sql, effective_config)**. If config differs, either (a) reject as drift, or (b) re-baseline fixtures intentionally.

Python capture (agent-grade):

```python
def df_settings(ctx):
    # requires information_schema enabled in SessionConfig
    return ctx.sql("SELECT name, setting FROM information_schema.df_settings").to_arrow_table()
```

#### 14.2.4 Pushdown expectations tests (projection + predicates)

Your guide’s core extraction targets are explicit:

* Logical `TableScan` includes `projection=[...]` and `partial_filters=[...]`
* Physical `DataSourceExec` includes `file_groups={...}`, `projection=[...]`, `predicate=...`
  DataFusion’s “Reading Explain Plans” doc shows these exact fields in the EXPLAIN output. ([datafusion.apache.org][9])

**Test pattern 1: projection pushdown**

* Input: `(sql, expected_scan_cols_by_table)`
* Mechanism: parse logical plan row from `EXPLAIN FORMAT INDENT` and extract `TableScan … projection=[…]`
* Assert: projection is a subset of expected scan cols (or exact match if you want strictness)

**Test pattern 2: predicate pushdown**

* Input: `(sql, expected_pushed_predicates_by_table)`
* Mechanism:

  * logical: extract `partial_filters=[…]`
  * physical: extract `DataSourceExec … predicate=…`
* Assert:

  * pushed predicate present at scan
  * residual filter exists above scan iff pushdown is partial (compare `Filter` vs `partial_filters`, or `FilterExec` vs `DataSourceExec predicate`).

**Implementation sketch (capturable, no stdout scraping):**

```python
import re

def explain_rows(ctx, sql):
    return ctx.sql(f"EXPLAIN FORMAT INDENT {sql}").to_arrow_table().to_pylist()

def find_plan(rows, plan_type):
    return next(r["plan"] for r in rows if r["plan_type"] == plan_type)

SCAN_RE = re.compile(r"TableScan:\s+(?P<table>\S+)\s+.*projection=\[(?P<proj>[^\]]*)\].*partial_filters=\[(?P<pf>[^\]]*)\]")

def scan_contracts_from_explain_indent(logical_plan_text: str):
    out = []
    for line in logical_plan_text.splitlines():
        m = SCAN_RE.search(line)
        if m:
            out.append({
                "table": m.group("table"),
                "projection": [x.strip() for x in m.group("proj").split(",") if x.strip()],
                "partial_filters": m.group("pf").strip(),
            })
    return out
```

#### 14.2.5 Golden-test layout (recommended)

Directory structure (one file per query id):

* `queries/<id>.sql`
* `profiles/<id>.df_settings.json` (sorted by `name`)
* `plans/<id>.optimized_logical.indent_schema.txt`
* `plans/<id>.physical.indent.txt`
* `plans/<id>.explain.indent.jsonl` (rows: `{plan_type, plan}`)

Plus: optionally `EXPLAIN FORMAT PGJSON` and `GRAPHVIZ` artifacts for machine/visual diffing.  ([datafusion.apache.org][6])

[1]: https://docs.rs/datafusion/latest/datafusion/physical_plan/repartition/struct.RepartitionExec.html?utm_source=chatgpt.com "RepartitionExec in datafusion::physical_plan::repartition"
[2]: https://datafusion.apache.org/user-guide/configs.html?utm_source=chatgpt.com "Configuration Settings — Apache DataFusion documentation"
[3]: https://github.com/apache/arrow-datafusion/issues/8451?utm_source=chatgpt.com "Incorrect results due to repartitioning a sorted ParquetExec"
[4]: https://github.com/apache/datafusion/issues/9898?utm_source=chatgpt.com "Teach UnionExec to require its inputs sorted · Issue #9898"
[5]: https://datafusion.apache.org/blog/2025/03/11/ordering-analysis/?utm_source=chatgpt.com "Using Ordering for Better Plans in Apache DataFusion"
[6]: https://datafusion.apache.org/user-guide/sql/explain.html?utm_source=chatgpt.com "EXPLAIN — Apache DataFusion documentation"
[7]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html?utm_source=chatgpt.com "ExecutionPlan in datafusion::physical_plan - Rust"
[8]: https://datafusion.apache.org/user-guide/sql/information_schema.html?utm_source=chatgpt.com "Information Schema — Apache DataFusion documentation"
[9]: https://datafusion.apache.org/user-guide/explain-usage.html?utm_source=chatgpt.com "Reading Explain Plans — Apache DataFusion documentation"
