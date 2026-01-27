Below is a **feature catalog** of the plan-development surfaces in **Ibis + DataFusion** that are directly useful for **orchestration** (table-level deps *and* column/predicate lineage). It’s segmented into topic sections you can deep-dive independently.

---

## 1) Ibis IR as the “semantic plan” DAG

### 1.1 Immutable + lazy expression objects (why this is orchestration-gold)

* `ibis.expr.types.relations.Table` is explicitly an **immutable, lazy dataframe**: each method returns a new expression; nothing executes until you materialize (execute / export). This gives you a stable, purely-declarative graph to analyze. ([Ibis][1])
* “Plan” is inherent: expressions are *symbolic* and usually compile to SQL; the backend executes later. ([Ibis][1])

### 1.2 IR structure: `Expr` + `Node` / operations

* Ibis expressions are represented as a tree of `Expr` and `Node` objects; this is the intermediate representation you want to traverse for dependencies. ([Ibis][2])
* Nodes correspond to operations; many are in `ibis.expr.operations`. ([Ibis][3])

### 1.3 “Plan-time” APIs you’ll actually use for orchestration extraction

From `Table` / expression APIs:

* `schema()` → schema of the current expression (useful for column lineage constraints + validation) ([Ibis][1])
* `get_name()` → fully qualified table name (critical for dataset-key mapping) ([Ibis][1])
* `equals(other)` → *structural equivalence* (handy for canonicalization / CSE / plan fingerprinting at the IR layer) ([Ibis][1])
* `compile(limit=…, params=…, pretty=…)` and `to_sql(dialect=…, pretty=…)` → stable compilation artifact(s) ([Ibis][1])
* `unbind()` → normalize away backend-specific bound objects into `UnboundTable`-based expressions (useful for portability + deterministic plan hashing) ([Ibis][1])
* `visualize()` / `ibis.expr.visualize.to_graph(expr)` → renders the IR graph (debugging + regression snapshots) ([Ibis][1])

**Minimal “orchestration extraction” sketch (Ibis side)**:

```python
import ibis

t = ibis.table(schema={"a": "int64", "b": "int64"}, name="t", catalog="cat", database="db")
expr = (
    t.filter(t.a > 0)
     .mutate(c=t.a + t.b)
     .group_by(t.b)
     .aggregate(total=lambda s: s.c.sum())
)

deps_table_key = t.get_name()      # dataset identity input
required_schema = expr.schema()    # output contract
sql = expr.compile(pretty=True)    # compilation artifact
g = expr.visualize()               # IR graph (debug/snapshot)
```

([Ibis][1])

---

## 2) Ibis compilation pipeline (SQLGlot boundary + rewrite hooks)

### 2.1 SQLGlot-based compiler infrastructure

* Ibis generalized compilation to support **SQLGlot-based expressions**, not only raw string emitters. ([Ibis][3])
* The compiler decomposes a `SELECT` into pieces (`select_set`, `where`, `group_by`, `having`, `limit`, `order_by`, `distinct`) and assembles via a backend-specific translator. ([Ibis][3])

### 2.2 Why orchestration cares

* This is where you decide whether your *canonical plan artifact* is:

  * **Ibis IR** (most stable semantics),
  * **SQL / SQLGlot** (portable + analyzable),
  * or **DataFusion plan** (execution-realistic, optimizer-aware).
* You can treat compiled SQL as an *exchange format* into DataFusion’s SQL planner.

---

## 3) Ibis ↔ DataFusion backend integration surface

### 3.1 Connecting with a `SessionContext` (the “shared catalog” pattern)

* `ibis.datafusion.connect` is a wrapper around the backend `do_connect`; it accepts either:

  * a mapping `{table_name: path}` (*deprecated in 10.0*), or
  * a **DataFusion `SessionContext` instance** (recommended). ([Ibis][4])
* Example in docs shows: create `SessionContext`, create/register data in it, then `ibis.datafusion.connect(ctx)`, and Ibis can `list_tables()`. ([Ibis][4])

### 3.2 Backend-level compile

* `datafusion.Backend.compile(expr, limit, params, pretty)` compiles an Ibis expression to a **SQL string**. ([Ibis][4])

### 3.3 Catalog hierarchy support (relevant to dataset identity)

* The DataFusion backend exposes methods like `create_catalog`, `create_database`, etc., and Ibis explicitly maps “catalog/database/table” terminology across backends. ([Ibis][4])

---

## 4) DataFusion “native plan” building APIs (Python)

### 4.1 `SessionContext` as the stateful planning environment

`SessionContext` is the main interface for:

* creating DataFrames from sources,
* registering sources as tables,
* executing SQL to produce a DataFrame plan. ([Apache DataFusion][5])

### 4.2 Source registration primitives (these define *dataset dependencies*)

Key methods (Python bindings):

* `read_parquet(...)` with knobs like `parquet_pruning`, `skip_metadata`, `schema`, `file_sort_order` (these materially change scan + pruning behavior). ([Apache DataFusion][6])
* `register_parquet(...)`, `register_csv(...)`, `register_json(...)` (stable table names for dependency keys). ([Apache DataFusion][6])
* `register_dataset(name, pyarrow.dataset.Dataset)` (ties into Arrow Dataset partitioning). ([Apache DataFusion][6])
* `register_listing_table(name, path, file_extension, partition_cols, sort_order, …)` registers **many files as one logical table** (critical for file-group scheduling). ([Apache DataFusion][6])
* `register_object_store(schema, store, host=…)` injects object stores into resolution. ([Apache DataFusion][6])
* `register_view(name, df)` turns a DataFrame plan into a named view. ([Apache DataFusion][6])
* `sql(query, param_values=…, named_params=…)` supports parameter substitution and can execute DDL/DML with an in-memory default implementation. ([Apache DataFusion][6])
* UDF registration: `register_udf`, `register_udaf`, `register_udtf`, `register_udwf` (these are runtime deps that affect plan validity + hashing). ([Apache DataFusion][6])

### 4.3 DataFrame is “a plan”

* DataFusion DataFrames are lazy; before terminal ops, modifying a DataFrame just updates the plan; `collect()` triggers computation. ([Apache DataFusion][7])
* In the Rust docs, a `DataFrame` is described as a wrapper around a `LogicalPlan` plus execution state, with the typical workflow being build transforms then `collect`. ([Docs.rs][8])

---

## 5) DataFusion plan introspection & traversal (your orchestration backbone)

### 5.1 Plan layers you can request from a `DataFrame`

* `df.logical_plan()` → unoptimized `LogicalPlan` ([Apache DataFusion][7])
* `df.optimized_logical_plan()` → optimized `LogicalPlan` ([Apache DataFusion][7])
* `df.execution_plan()` → physical `ExecutionPlan` ([Apache DataFusion][7])
* `df.explain(verbose=False, analyze=False)` → print plan; `analyze=True` runs and reports metrics ([Apache DataFusion][7])
* `df.parse_sql_expr("a > 1")` → parse SQL predicate into a DataFusion `Expr` against current schema (useful for normalizing predicate logic). ([Apache DataFusion][7])

### 5.2 `LogicalPlan` / `ExecutionPlan` objects: structured APIs

Logical plan:

* `display()`, `display_graphviz()`, `display_indent()`, `display_indent_schema()` for rendering ([Apache DataFusion][9])
* `inputs()` returns child logical plans (tree traversal) ([Apache DataFusion][9])
* `to_variant()` converts to a “specific variant” (the hook you use to pattern-match operators programmatically) ([Apache DataFusion][9])
* `to_proto()/from_proto(ctx, bytes)` for serialization **with limitation**: tables created in memory from record batches aren’t supported. ([Apache DataFusion][9])

Physical plan:

* `children()` returns input execution plans (tree traversal) ([Apache DataFusion][9])
* `partition_count` exposes partitioning degree (scheduler hint) ([Apache DataFusion][9])
* `to_proto()/from_proto(ctx, bytes)` with the same in-memory-table limitation ([Apache DataFusion][9])

### 5.3 “Explain” plans contain lineage signals (projection, filters, files)

The DataFusion explain guide shows:

* Logical plan includes `TableScan ... projection=[...], partial_filters=[...]` (column + predicate lineage at scan). ([Apache DataFusion][10])
* Physical plan includes `DataSourceExec ... file_groups={...}, projection=[...], predicate=..., file_type=parquet` (file-group + predicate pushdown visibility). ([Apache DataFusion][10])
* Physical plans are hardware/data dependent; logical plans aren’t (important for determinism in orchestration). ([Apache DataFusion][10])

---

## 6) Substrait as a portable plan artifact (DataFusion Python)

* `datafusion.substrait` provides `Producer` (LogicalPlan → Substrait), `Consumer` (Substrait → LogicalPlan), and `Serde` for serialization. ([Apache DataFusion][11])
* The `datafusion` PyPI feature list explicitly calls out **serializing/deserializing query plans in Substrait format**. ([PyPI][12])

**Orchestration payoff**: Substrait can become your “plan fingerprint + interchange” layer (especially if you want cross-engine reproducibility).

---

## 7) Extensibility hooks that directly impact orchestration

### 7.1 Custom logical operators (engine extensions)

* DataFusion `LogicalPlan` is an enum of operators and includes an `Extension` variant to add custom logical operators. ([Apache DataFusion][13])
  This matters if you want to encode orchestration-specific operators directly into the plan graph.

### 7.2 Custom TableProviders (scan-time pruning + external indexes)

* Python-side support: implement `TableProvider` in Rust and expose a `FFI_TableProvider` via `PyCapsule` (DataFusion 43+), then register it with `SessionContext`. ([Apache DataFusion][14])
* Core semantics: `TableProvider::scan` returns an `ExecutionPlan` and is the key integration point; DataFusion explains this is “likely the most important” method. ([Apache DataFusion][15])
  This is the *native* place to implement “scan planning as orchestration,” e.g., consult an external index, prune files, enforce column requirements, etc.

---

## 8) Runtime / configuration knobs that feed scheduling decisions

Even if your orchestration is mostly “deps/lineage,” you often want to also schedule based on resource shape:

* `SessionConfig` exposes knobs like `with_parquet_pruning`, `with_target_partitions`, and repartition toggles (joins, aggregations, scans, windows). ([Apache DataFusion][6])
* `RuntimeEnvBuilder` exposes memory-pool and spill/disk-manager configuration (operational constraints can influence how aggressively you parallelize). ([Apache DataFusion][6])

---

## 9) Canonical integration patterns (Ibis plan ↔ DataFusion plan)

### Pattern A — **Ibis-first (semantic DAG) → DataFusion (optimizer + file groups)**

1. Build Ibis expression graph (stable semantics + rich API)
2. Compile to SQL (or SQLGlot)
3. Feed to DataFusion `ctx.sql(sql)`
4. Extract `optimized_logical_plan()` + `execution_plan()` for:

   * **table deps** (scans / views / providers),
   * **column lineage** (projection, filters),
   * **file-group scheduling** (DataSourceExec file_groups). ([Apache DataFusion][7])

```python
from datafusion import SessionContext
import ibis

ctx = SessionContext()
ctx.register_parquet("t", "path/to/t.parquet")

con = ibis.datafusion.connect(ctx)
t = con.table("t")

expr = t.filter(t.a > 0).select(t.a, t.b)
sql = expr.compile()

df = ctx.sql(sql)
lp = df.optimized_logical_plan()
pp = df.execution_plan()

print(lp.display_indent_schema())
```

([Apache DataFusion][6])

### Pattern B — **DataFusion-first (native DataFrame plan)**

Build via DataFusion DataFrame API; extract plans directly. This is the cleanest route if you want **engine-native operators** and avoid SQL as an intermediate. ([Apache DataFusion][7])

---


[1]: https://ibis-project.org/reference/expression-tables "expression-tables – Ibis"
[2]: https://ibis-project.org/posts/does-ibis-understand-sql/ "Does Ibis understand SQL? – Ibis"
[3]: https://ibis-project.org/concepts/internals "internals – Ibis"
[4]: https://ibis-project.org/backends/datafusion "datafusion – Ibis"
[5]: https://datafusion.apache.org/python/user-guide/basics.html "Concepts — Apache Arrow DataFusion  documentation"
[6]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[7]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[8]: https://docs.rs/datafusion/latest/datafusion/dataframe/struct.DataFrame.html "DataFrame in datafusion::dataframe - Rust"
[9]: https://datafusion.apache.org/python/autoapi/datafusion/plan/index.html "datafusion.plan — Apache Arrow DataFusion  documentation"
[10]: https://datafusion.apache.org/user-guide/explain-usage.html "Reading Explain Plans — Apache DataFusion  documentation"
[11]: https://datafusion.apache.org/python/autoapi/datafusion/substrait/index.html "datafusion.substrait — Apache Arrow DataFusion  documentation"
[12]: https://pypi.org/project/datafusion/25.0.0/?utm_source=chatgpt.com "DataFusion in Python"
[13]: https://datafusion.apache.org/library-user-guide/building-logical-plans.html "Building Logical Plans — Apache DataFusion  documentation"
[14]: https://datafusion.apache.org/python/user-guide/io/table_provider.html "Custom Table Provider — Apache Arrow DataFusion  documentation"
[15]: https://datafusion.apache.org/library-user-guide/custom-table-providers.html "Custom Table Provider — Apache DataFusion  documentation"

Below is a **canonical dependency model** that treats **Ibis IR**, **DataFusion optimized logical**, and **DataFusion physical** as *three complementary overlays* on a single scheduler graph.

The key idea is: **use Ibis as the semantic spec**, **DataFusion optimized logical as the engine-real relational algebra**, and **DataFusion physical as execution hints**—then reconcile into one normalized representation that your orchestrator can diff, cache, and schedule.

---

## 0) Ground truth: what each layer is “for”

### Ibis IR (semantic, backend-agnostic, stable)

* Ibis tables are **immutable + lazy** symbolic expressions that “typically” translate into SQL executed on a backend. This gives you a “pure” DAG to traverse before you even touch engine planning. ([Ibis][1])
* Ibis expressions are **a tree of `Expr` and `Node` objects** and can be visualized as a directed graph. ([Ibis][2])
* You can drop down to the operation layer via `expr.op()` and then reconstruct an expression via `.to_expr()` (useful for rewrites / normalization). ([Ibis][3])

### DataFusion optimized logical plan (engine-real logical algebra)

* In DataFusion Python, a `DataFrame` is explicitly “a representation of a logical plan” and you compose it then call `.collect()` to execute. ([Apache Arrow][4])
* You can get **optimized logical plan** (`df.optimized_logical_plan()`), traverse inputs (`LogicalPlan.inputs()`), and ask for structured variants (`LogicalPlan.to_variant()`). ([Apache DataFusion][5])

### DataFusion physical plan (execution DAG + file groups + partitions + metrics)

* Physical plans are accessible via `df.execution_plan()`; the returned `ExecutionPlan` has `children()` and `partition_count`, plus display helpers. ([Apache DataFusion][6])
* Physical planning can **absorb filters into scans** (e.g., `DataSourceExec` applying filters during scan; `FilterExec` absorbing what children can’t handle). ([Docs.rs][7])
* `EXPLAIN` output surfaces crucial orchestration hints (scan projections, pushed predicates, and physical `file_groups`). ([Apache DataFusion][8])

---

## 1) Canonical scheduler graph: the data model

You want a graph that can answer (at minimum):

1. **Dataset deps**: which *logical datasets* must exist before computing this one
2. **Column deps**: which *columns* are required from each upstream dataset
3. **Predicate deps**: which *filters* are semantically applied, and which are *pushed down* to scans
4. **Execution hints**: file groups, partitioning, metrics (advisory; not correctness)

A clean model is a **bipartite “relation ↔ operator” graph** with overlays per planning layer.

### 1.1 Core identities

```python
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional, FrozenSet, Mapping, Sequence, Literal

DatasetKind = Literal["table", "view", "file", "listing", "mem", "udf", "unknown"]

@dataclass(frozen=True)
class DatasetKey:
    """Canonical dataset identity used by the scheduler."""
    kind: DatasetKind
    # stable identity string; for tables: "catalog.schema.table" (or similar)
    name: str

    # Optional decomposition / routing
    catalog: Optional[str] = None
    schema: Optional[str] = None

    # For file/listing sources
    uri: Optional[str] = None  # e.g. "s3://bucket/prefix" or local path

    # Extra identity-relevant config (format, options)
    options: Mapping[str, Any] = field(default_factory=dict)

@dataclass(frozen=True)
class ColumnSet:
    cols: FrozenSet[str]

@dataclass(frozen=True)
class ExprCanon:
    """Canonical representation for expressions (predicates, computed columns)."""
    # Use one or more of these fields depending on which layer you got it from.
    sql: Optional[str] = None          # normalized SQL string (e.g., via SQLGlot)
    ibis_repr: Optional[str] = None    # stable-ish repr of ibis Node/Expr
    df_repr: Optional[str] = None      # repr() of DataFusion Expr / plan fragment
    fingerprint: Optional[str] = None  # pre-hashed canonical bytes (Substrait/proto)
```

### 1.2 Operator and edges

```python
OpKind = Literal[
    "scan", "projection", "filter", "aggregate", "join", "sort", "limit",
    "union", "distinct", "window", "subquery", "extension", "sink"
]

@dataclass(frozen=True)
class PlanNode:
    id: str
    kind: OpKind
    # optional schema snapshots
    output_schema: Optional[Mapping[str, str]] = None

    # operator-specific payload
    exprs: Sequence[ExprCanon] = ()
    join_type: Optional[str] = None

@dataclass(frozen=True)
class Edge:
    """Edge from upstream dataset to operator or operator to downstream dataset."""
    src: str
    dst: str
    required_cols: Optional[ColumnSet] = None
    predicate: Optional[ExprCanon] = None
    predicate_pushdown: Optional[Literal["none", "partial", "full"]] = None

@dataclass
class CanonicalGraph:
    datasets: dict[str, DatasetKey]   # dataset_id -> DatasetKey
    ops: dict[str, PlanNode]          # op_id -> PlanNode
    edges: list[Edge]

    # overlays (same node ids) capturing plan-layer provenance
    overlays: dict[str, Any] = field(default_factory=dict)
```

### 1.3 Why bipartite works well

* It cleanly separates **dataset identity** (scheduler-visible) from **operator structure** (lineage-visible).
* You can “collapse” operators away to get a pure dataset DAG for task ordering, while keeping operators for column/predicate derivations.

---

## 2) Layer extraction: what to pull from each plan representation

### 2.1 Ibis IR extraction (semantic overlay)

**Goal:** Identify *semantic sources* and *semantic column/predicate lineage* before engine rewrites.

Facts you can rely on:

* Ibis `Table` is immutable/lazy; the expression is the plan. ([Ibis][1])
* Ibis expressions are `Expr` + `Node` trees and can be visualized. ([Ibis][2])
* You can operate at the node layer (`expr.op()`) and reconstitute via `.to_expr()`. ([Ibis][3])

**Recommended semantics-first normalization:**

1. `expr = expr.unbind()` if you want to eliminate backend-specific bindings and force `UnboundTable`-based identity (Ibis supports unbound tables conceptually; `unbind` exists as a first-class method). ([Ibis][9])
2. Convert *semantic predicates* and *computed expressions* into a canonical form you control (e.g., SQLGlot AST) by round-tripping through `to_sql` / `compile` where appropriate (Ibis exposes these compilation methods on expressions/tables). ([Ibis][1])

**Minimal semantic lineage extraction strategy**

* Traverse `expr.op()` recursively.
* Track:

  * Base relations (`UnboundTable`, `DatabaseTable`, `MemTable`, etc.)
  * Column references (`Field`/`Column`-like nodes)
  * Filters (`Filter` nodes; join predicates; HAVING predicates)
  * Projection/rename expressions (output col -> expression tree)

In pseudocode (intentionally “agent-implementable”, not relying on undocumented internals beyond `expr.op()`):

```python
def ibis_semantic_overlay(expr) -> dict:
    op = expr.op()  # proven to exist in docs/posts :contentReference[oaicite:13]{index=13}

    # Walk node DAG, collect:
    sources: dict[str, dict] = {}     # source_id -> {dataset_key, cols, preds}
    col_lineage: dict[str, ExprCanon] = {}  # output_col -> expr canon
    predicates: list[ExprCanon] = []

    # ... implement a visitor with:
    # - node type checks (by class name)
    # - extract table name/schema when node is a relation
    # - extract referenced columns when node is a column op
    # - extract boolean expressions when node is filter/having/join condition
    #
    # For canonicalization: store ibis_repr=str(node) and/or sql=ibis.to_sql(expr_subtree)

    return {
        "sources": sources,
        "col_lineage": col_lineage,
        "predicates": predicates,
    }
```

**Important:** Ibis semantic lineage is *pre-optimizer* and may include columns/predicates that an engine later removes via pushdown/projection pruning. That’s expected—store it as the “spec”.

---

### 2.2 DataFusion optimized logical extraction (canonical relational overlay)

This is where you should treat DataFusion as “what will actually be executed logically” after rewrite rules.

You have:

* `df.optimized_logical_plan()` ([Apache DataFusion][5])
* `LogicalPlan.inputs()` for traversal and `LogicalPlan.to_variant()` for structured matching ([Apache DataFusion][6])
* Optional `display_indent_schema()` for debugging schema evolution through operators ([Apache DataFusion][6])

**Why this layer is canonical for orchestration**

* It already incorporates **projection pushdown** and can expose what each scan truly needs.
* It typically normalizes/rewrites predicate placement (some pushed into scans, some left as filters).

DataFusion’s own explain guide shows what you can extract at this layer:

* Logical `TableScan` surfaces:

  * `projection=[...]` (column pruning)
  * `partial_filters=[...]` (scan-level pushed filters) ([Apache DataFusion][8])

Example from DataFusion docs (logical plan):

* `TableScan: hits.parquet projection=[WatchID, ClientIP, URL], partial_filters=[starts_with(...)]` ([Apache DataFusion][8])

So your logical extractor should:

1. Traverse the optimized logical plan tree
2. Convert each node into a `PlanNode(kind=...)`
3. On `TableScan`:

   * identify the dataset (table name / file path)
   * record `required_cols` from `projection`
   * record pushed predicates from `partial_filters`

**Practical note:** `to_variant()` returns “the specific variant” but the exact Python object shape can vary by version. The robust approach is introspection + fallbacks:

```python
def df_logical_overlay(df):
    lp = df.optimized_logical_plan()  # :contentReference[oaicite:19]{index=19}

    def walk(plan):
        v = plan.to_variant()  # :contentReference[oaicite:20]{index=20}
        kind = type(v).__name__

        # Prefer attribute access; fallback to parsing plan.display_indent()
        # and/or using EXPLAIN FORMAT INDENT if you must.
        children = plan.inputs()  # :contentReference[oaicite:21]{index=21}

        return kind, v, children

    # build canonical op nodes + edges...
```

---

### 2.3 DataFusion physical extraction (execution-hints overlay)

Physical plans are *not* your correctness source, but they’re hugely valuable for scheduling:

* **file grouping / partitioning** for parallel work allocation
* whether predicates were absorbed into scans
* (optionally) operator metrics via `EXPLAIN ANALYZE`

From DataFusion docs (physical plan output):

* `DataSourceExec: file_groups={16 groups: ...}, projection=[...], predicate=..., file_type=parquet` ([Apache DataFusion][8])

DataFusion Python surfaces:

* `df.execution_plan()` (exists)
* `ExecutionPlan.children()`, `ExecutionPlan.partition_count`
* `ExecutionPlan.display_indent()` for a parseable representation ([Apache DataFusion][6])

And DataFusion’s physical-plan semantics explicitly allow filter absorption:

* A `DataSourceExec` may “absorb” filters to apply them during scan; a `FilterExec` may absorb what children can’t handle. ([Docs.rs][7])

**Physical overlay extraction strategy**

1. Compute `pp = df.execution_plan()` ([Apache DataFusion][10])
2. Capture:

   * `pp.partition_count` (coarse parallelism hint) ([Apache DataFusion][6])
   * `pp.display_indent()` (parse to identify scan nodes, file_groups, predicates, projections)
   * optionally `EXPLAIN ANALYZE` if you need cost/metrics (the docs show metrics appended to the plan) ([Apache DataFusion][8])

---

## 3) Reconciliation: merging the overlays into one scheduler graph

### 3.1 Dataset identity normalization

This is the *hardest* practical part. You need one `DatasetKey` that all three layers map onto.

**DataFusion naming comes from registration / catalog**

* DataFusion’s `SessionContext::register*` (or Python equivalents) define the name a SQL query refers to. ([Apache DataFusion][11])
* You can also register views from DataFrames (`register_view`) and then the name resolves as a table-like dependency. ([Apache DataFusion][12])

**Recommended rule:** build a single `DatasetResolver`:

```python
class DatasetResolver:
    def __init__(self, *, table_aliases=None, uri_normalizer=None):
        self.table_aliases = table_aliases or {}
        self.uri_normalizer = uri_normalizer or (lambda u: u)

    def from_ibis_table_name(self, name: str) -> DatasetKey:
        # name might already be catalog.schema.table or just table
        # apply alias mapping
        name = self.table_aliases.get(name, name)
        return DatasetKey(kind="table", name=name)

    def from_datafusion_scan(self, scan_name_or_path: str) -> DatasetKey:
        # if it looks like a file path: treat as file/listing
        if "/" in scan_name_or_path or scan_name_or_path.endswith(".parquet"):
            return DatasetKey(kind="file", name=scan_name_or_path, uri=self.uri_normalizer(scan_name_or_path))
        return DatasetKey(kind="table", name=self.table_aliases.get(scan_name_or_path, scan_name_or_path))
```

Then reconciliation is mostly “join on DatasetKey”.

---

### 3.2 Column reconciliation rules (semantic vs executed)

You will often see:

* **Ibis semantic required columns** = superset (what the user asked for)
* **DataFusion scan projection** = subset (what engine will read after pruning)

DataFusion’s explain examples show that pruning is real and explicit:

* logical `TableScan ... projection=[UserID]` reads only that column ([Apache DataFusion][8])
* and similarly in other queries the scan projection is enumerated ([Apache DataFusion][8])

**Rule of thumb**

* **Correctness:** rely on Ibis semantic lineage to validate “this task depends on upstream dataset X”
* **I/O scheduling:** rely on DataFusion scan `projection` to decide which columns need to be present / fetched / materialized
* Persist both as:

  * `required_cols.semantic`
  * `required_cols.executed`

In the canonical graph, store both on the same `Edge(dataset → scan_op)` as two overlays or two fields.

---

### 3.3 Predicate reconciliation rules (semantic vs pushdown)

You need to separate:

* **Semantic predicate**: filters/having/join conditions expressed by the user (Ibis)
* **Pushdown predicate**: what got applied at scan-time (DataFusion)

DataFusion shows:

* `TableScan ... partial_filters=[starts_with(...)]` in logical plan ([Apache DataFusion][8])
* physical `DataSourceExec ... predicate=starts_with(...)` ([Apache DataFusion][8])
* and the engine can absorb filters into scans as part of physical optimization ([Docs.rs][7])

**Canonical representation**

* Store **all semantic predicates** attached to the operator where they appear in Ibis IR.
* Store **scan-level pushdowns** attached specifically to the scan edge with `predicate_pushdown="partial|full"`:

  * `partial` when there are residual filters above scan
  * `full` when filter is fully absorbed and no residual remains

To compute pushdown status, compare:

* logical plan `Filter` operators vs scan `partial_filters`
* and/or physical plan showing `FilterExec` presence vs `DataSourceExec predicate=...`

---

### 3.4 Operator graph reconciliation (Ibis DAG vs DataFusion tree)

Ibis is an expression DAG; DataFusion plans are generally trees of operators (with explicit subquery nodes etc.). The reconciliation strategy that works in production:

1. Use **DataFusion optimized logical** as your **structural backbone** (operator ordering, join placement, projection pushdown results).
2. Attach **Ibis semantic payloads** by matching:

   * dataset identity (source tables)
   * output column names
   * (optional) SQL canonicalization via `ibis.to_sql()` / `compile()` to create a common normalized SQL string ([Ibis][1])
3. When DataFusion performs rewrites (e.g., merges projections), you don’t try to 1:1 align every node; instead you align at the **interface edges**:

   * per-source required columns
   * per-source pushed predicates
   * output schema and computed columns map

This gives you stable scheduling inputs even when the engine rewrites internal structure.

---

## 4) Fingerprinting + diffing: making it schedulable over time

Your scheduler needs stable hashes to:

* detect when a task’s “inputs contract” changed (deps/cols/preds)
* reuse cached artifacts when safe

### 4.1 Plan serialization options (and caveats)

**DataFusion protobuf bytes**

* `LogicalPlan.to_proto()` and `ExecutionPlan.to_proto()` exist in Python, but **tables created in memory from record batches are currently not supported**. ([Apache DataFusion][13])
  (There are open issues tracking this limitation.) ([GitHub][14])

**Substrait**

* DataFusion Python has `datafusion.substrait` with `Producer`, `Consumer`, `Serde`. ([Apache DataFusion][15])
* Substrait is explicitly an interoperability format for structured compute operations. ([Substrait][16])

**Recommendation**

* For “mostly stable” fingerprinting:

  * `semantic_fp = hash(normalize(ibis_expr.unbind()))`
  * `logical_fp = hash(substrait_plan_bytes)` (when feasible)
  * `physical_fp = hash(execution_plan.display_indent())` (treat as advisory / debug, not cache key)

---

## 5) End-to-end “PlanBundle → CanonicalGraph” pipeline (reference implementation sketch)

This is the high-leverage integration pattern:

1. Build Ibis expression (semantic DAG)
2. Compile to SQL
3. Feed SQL into DataFusion `SessionContext.sql(...)` (same context Ibis backend is bound to) ([Apache DataFusion][17])
4. Extract optimized logical + physical
5. Build canonical graph and persist as scheduling artifact

```python
from datafusion import SessionContext
import ibis

def build_plan_bundle(expr: "ibis.Expr", ctx: SessionContext):
    # 1) semantic
    semantic_expr = expr.unbind()  # stable-ish identity layer :contentReference[oaicite:41]{index=41}
    semantic_overlay = ibis_semantic_overlay(semantic_expr)

    # 2) compiled SQL bridge
    sql = semantic_expr.to_sql(pretty=False)  # method exists on table/expr API :contentReference[oaicite:42]{index=42}

    # 3) datafusion planning
    df = ctx.sql(sql)  # SessionContext.sql supports parameters etc. :contentReference[oaicite:43]{index=43}
    logical = df.optimized_logical_plan()      # :contentReference[oaicite:44]{index=44}
    physical = df.execution_plan()             # :contentReference[oaicite:45]{index=45}

    return {
        "semantic_expr": semantic_expr,
        "semantic_overlay": semantic_overlay,
        "sql": sql,
        "df": df,
        "logical": logical,
        "physical": physical,
    }

def build_canonical_graph(bundle, resolver: DatasetResolver) -> CanonicalGraph:
    # 1) backbone from DataFusion logical
    #    - walk logical.inputs() :contentReference[oaicite:46]{index=46}
    #    - inspect node variants via to_variant() :contentReference[oaicite:47]{index=47}
    #    - create ops + datasets + edges
    #
    # 2) attach semantic overlay:
    #    - map Ibis sources to DatasetKey via resolver
    #    - merge required cols / predicates
    #
    # 3) attach physical hints:
    #    - parse physical.display_indent() (file_groups, predicate) or use EXPLAIN output style :contentReference[oaicite:48]{index=48}
    #    - store partition_count :contentReference[oaicite:49]{index=49}

    return CanonicalGraph(datasets={}, ops={}, edges=[], overlays={})
```

---

## 6) Coverage checklist: make it “exhaustive enough” for real schedulers

To be confident you’ve captured “all orchestration-relevant plan constructs”, ensure you handle:

### Source shapes

* registered tables (catalog/schema/table)
* registered views (`register_view`) ([Apache DataFusion][12])
* file scans / listing scans (`TableScan: hits.parquet`) ([Apache DataFusion][8])
* in-memory sources (note `to_proto` limitation) ([Apache DataFusion][13])

### Operator classes (logical)

* Filter / Projection / Aggregate / Sort / Limit / Join / Union / Distinct / Window
* Subquery boundaries (scalar subqueries, correlated subqueries)
* Extension nodes (DataFusion supports extensibility; treat as `OpKind="extension"`)

### Predicate placement + pushdown

* scan `partial_filters` vs residual `Filter` nodes ([Apache DataFusion][8])
* physical absorption of predicates into `DataSourceExec` ([Docs.rs][7])

### Column lineage

* scan projection lists as executed truth ([Apache DataFusion][8])
* expression-derived output cols (projection/aggregate outputs)
* rename + alias propagation (needs explicit mapping output->input expr)

### Execution hints

* `file_groups` counts and segmentation (parallelism units) ([Apache DataFusion][8])
* `partition_count` (coarse degree of parallelism) ([Apache DataFusion][6])
* optional metrics via `EXPLAIN ANALYZE` ([Apache DataFusion][8])

---


[1]: https://ibis-project.org/reference/expression-tables?utm_source=chatgpt.com "Table expressions - Ibis"
[2]: https://ibis-project.org/posts/does-ibis-understand-sql/?utm_source=chatgpt.com "Does Ibis understand SQL?"
[3]: https://ibis-project.org/posts/udf-rewriting/?utm_source=chatgpt.com "Dynamic UDF Rewriting with Predicate Pushdowns - Ibis"
[4]: https://arrow.staged.apache.org/datafusion-python/generated/datafusion.DataFrame.html?utm_source=chatgpt.com "datafusion.DataFrame"
[5]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html?utm_source=chatgpt.com "datafusion.dataframe"
[6]: https://datafusion.apache.org/python/autoapi/datafusion/plan/index.html "datafusion.plan — Apache Arrow DataFusion  documentation"
[7]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html?utm_source=chatgpt.com "ExecutionPlan in datafusion::physical_plan - Rust"
[8]: https://datafusion.apache.org/user-guide/explain-usage.html "Reading Explain Plans — Apache DataFusion  documentation"
[9]: https://ibis-project.org/how-to/extending/unbound_expression?utm_source=chatgpt.com "Write and execute unbound expressions - Ibis"
[10]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[11]: https://datafusion.apache.org/library-user-guide/using-the-sql-api.html?utm_source=chatgpt.com "Using the SQL API — Apache DataFusion documentation"
[12]: https://datafusion.apache.org/python/user-guide/common-operations/views.html?utm_source=chatgpt.com "Registering Views — Apache Arrow DataFusion documentation"
[13]: https://datafusion.apache.org/python/autoapi/datafusion/plan/index.html?utm_source=chatgpt.com "datafusion.plan — Apache Arrow DataFusion documentation"
[14]: https://github.com/apache/datafusion-python/issues/898?utm_source=chatgpt.com "Add support for conversion of in memory tables to protobuf"
[15]: https://datafusion.apache.org/python/autoapi/datafusion/substrait/index.html?utm_source=chatgpt.com "datafusion.substrait"
[16]: https://substrait.io/?utm_source=chatgpt.com "Home - Substrait: Cross-Language Serialization for Relational ..."
[17]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html?utm_source=chatgpt.com "datafusion.context — Apache Arrow DataFusion documentation"

Below is the “implementation-grade” follow-on you asked for: **(1) a concrete `LogicalPlan.to_variant()` operator catalog + extraction rules + required-column propagation**, **(2) a robust physical-plan parser focused on `DataSourceExec`**, and **(3) a canonical expression normalizer using SQLGlot + optional Substrait bytes**.

---

## A) DataFusion LogicalPlan extraction: operator catalog + required-columns propagation

### A0) Planning surfaces you will use (Python)

* `df.optimized_logical_plan()` gives the optimizer-rewritten logical plan you should treat as “execution-real logical algebra”. ([Apache DataFusion][1])
* `LogicalPlan.inputs()` returns the direct child plans; `LogicalPlan.to_variant()` converts the plan node into its specific variant object. ([Apache DataFusion][1])
* DataFusion logical plans are a tree of relational operators (Projection, Filter, Join, …). ([Docs.rs][2])

**Critical nuance for completeness:** “inputs” traversal can miss **subplans embedded in expressions** (e.g., `EXISTS`, `IN (subquery)`), so you must separately scan expression trees for subquery references (DataFusion’s own optimizer guide calls this out conceptually and shows extracting subquery references from `Expr`). ([Apache DataFusion][3])

---

### A1) Variant catalog (25 variants) + extraction fields + OpKind mapping

DataFusion’s Rust `LogicalPlan` enum exposes 25 variants (Projection, Filter, Window, Aggregate, Sort, Join, Repartition, Union, TableScan, …). ([Docs.rs][2])
In Python, `to_variant()` yields an object whose attributes typically mirror the Rust struct fields (you should still code defensively).

Below is the catalog **you pattern-match on** and what you extract.

#### Leaf / source

**TableScan → `OpKind="scan"`**

* Fields: `table_name`, `source`, `projection`, `projected_schema`, `filters`, `fetch`. ([Docs.rs][4])
  Extraction:
* **DatasetKey**: from `table_name` (normalize via your resolver).
* **Executed required cols**: if `projection` is present (indices), map to names using `projected_schema`; else defer to required-cols propagation.
* **Scan-pushdown predicates**: `filters` are “expressions to be used as filters by the table provider” (this is *logical* pushdown, distinct from residual `Filter` nodes). ([Docs.rs][4])

**EmptyRelation → `OpKind="scan"` (synthetic leaf)**

* Fields: `produce_one_row`, `schema`. ([Docs.rs][5])

**Values → `OpKind="scan"` (synthetic leaf)**

* Fields: `schema`, `values`. ([Docs.rs][6])

#### Unary relational operators

**Projection → `OpKind="projection"`**

* Fields: `expr`, `input`, `schema`. ([Docs.rs][7])

**Filter → `OpKind="filter"`**

* Fields: `input`, `predicate`. ([Docs.rs][8])

**Aggregate → `OpKind="aggregate"`**

* Fields: `group_expr`, `aggr_expr`, `input`, `schema`. ([Docs.rs][9])

**Window → `OpKind="window"`**

* Fields: `input`, `window_expr`, `schema`. ([Docs.rs][10])

**Sort → `OpKind="sort"`**

* Fields: `expr`, `input`, `fetch`. ([Docs.rs][11])

**Limit → `OpKind="limit"`**

* Fields: `skip`, `fetch`, `input`. ([Docs.rs][12])

**Repartition → `OpKind="repartition"`**

* Fields: `input`, `partitioning_scheme`. ([Docs.rs][13])

**SubqueryAlias → `OpKind="subquery"`**

* Fields: `alias`, `input`, `schema`. ([Docs.rs][14])
  (For orchestration, alias affects column qualification and join predicate resolution; it should feed your DatasetKey / column resolution logic.)

**Distinct → `OpKind="distinct"`**

* Variants: `All`, `On`. ([Docs.rs][15])

**Analyze / Explain → `OpKind="extension"` (wrapper)**

* Analyze fields: `input`, `schema`, `verbose`. ([Docs.rs][16])
* Explain fields: `plan`, `schema`, `verbose`, `stringified_plans`, `explain_format`, etc. ([Docs.rs][17])
  (Usually treat as wrappers and recurse into the wrapped plan.)

**Unnest → `OpKind="projection"` (semantically an expand/unnest operator)**

* Fields include: `input`, `exec_columns`, `list_type_columns`, `struct_type_columns`, `dependency_indices`, `options`, `schema`. ([Docs.rs][18])

#### Binary / N-ary relational operators

**Join → `OpKind="join"`**

* Fields: `left`, `right`, `on`, `filter`, `join_type`, `join_constraint`, `schema`, `null_equality`. ([Docs.rs][19])

**Union → `OpKind="union"`**

* Fields: `inputs`, `schema`. ([Docs.rs][20])

#### Embedded-plan / advanced

**Subquery → `OpKind="subquery"`**

* Fields: `subquery`, `outer_ref_columns`, `spans`. ([Docs.rs][21])
  (You must incorporate `subquery` into traversal even if it doesn’t show up in `inputs()`.)

**RecursiveQuery → `OpKind="subquery"`**

* Fields: `name`, `static_term`, `recursive_term`, `is_distinct`. ([Docs.rs][22])

**Extension → `OpKind="extension"`**

* Field: `node`. ([Docs.rs][23])

#### Side-effect / sink (orchestration-critical)

**DmlStatement → `OpKind="sink"`**

* Fields: `input`, `op`, `output_schema`, `table_name`, `target`. ([Docs.rs][24])

**DdlStatement → `OpKind="sink"` (schema/catalog mutation)**

* Variants include create/drop table/view/catalog/function/index, etc. ([Docs.rs][25])

---

### A2) Required-columns propagation: the practical algorithm

You need *two* computations:

1. **Expression column-uses** (“intrinsic uses”) for each operator’s expressions (predicate, projection exprs, join keys, group_expr/aggr_expr, sort keys, …).
2. **Top-down required output cols** from the plan root to leaves, to prune what you ask of children.

Even if DataFusion already applied projection pushdown (so scans have `projection`), you still want this algorithm because:

* it gives you a **backend-independent** lineage signal you can compare against Ibis/SQLGlot,
* it lets you detect regressions when pushdown fails,
* it produces the “required cols per upstream input” artifact your orchestrator wants.

#### A2.1 Expression column extraction (robust + defensive)

DataFusion’s Python `Expr` exposes:

* `canonical_name()` (“complete string representation”) ([Apache DataFusion][26])
* `display_name()` (“schema display”) ([Apache DataFusion][26])

DataFusion explicitly recommends: use `canonical_name` for expression equivalence, `display_name` for schema naming. ([Apache DataFusion][3])

**Recommended extraction strategy (in order):**

1. Try structural walk using `Expr.to_variant()` / `Expr.variant_name()` (if the variant objects expose child Exprs).
2. Fallback to parsing `canonical_name()` and filtering tokens by **known child schema columns**.

That second step is much more reliable than “regex all identifiers” because you can filter by the input schema’s column set.

#### A2.2 Operator rules: compute child required cols from parent required cols

Let:

* `R_out(node)` = required output columns (names) demanded by parent
* `U(node)` = intrinsic columns used by this node’s own expressions (from A2.1)
* `proj_map` = for Projection-like nodes, mapping output_name -> Expr

Rules (key operators):

**Projection**

* Keep only projection expressions that produce columns in `R_out(node)` (if `R_out` is None / “all”, keep all).
* `R_in(child) = cols_used_by_kept_exprs ∪ U(node)`
  Fields: `expr`, `input`, `schema`. ([Docs.rs][7])

**Filter**

* `R_in(child) = R_out(node) ∪ cols_used(predicate)`
  Fields: `input`, `predicate`. ([Docs.rs][8])

**Aggregate**

* If `R_out` is known, keep only aggregate outputs needed; always keep any `group_expr` needed to form groups that are referenced downstream.
* `R_in(child) = cols_used(group_expr ∪ kept_aggr_expr)`
  Fields: `group_expr`, `aggr_expr`, `input`, `schema`. ([Docs.rs][9])

**Join**

* Split `R_out` into left vs right based on output schema qualification/field origin (if you can’t reliably map, conservatively assume both).
* Always include join keys: `cols_used(on)` and any `filter` columns.
* `R_in(left)  = left_output_cols_needed ∪ cols_used(join_keys_left) ∪ cols_used(join_filter_left)`
* `R_in(right) = right_output_cols_needed ∪ cols_used(join_keys_right) ∪ cols_used(join_filter_right)`
  Fields: `left`, `right`, `on`, `filter`, `join_type`, … ([Docs.rs][19])

**Sort**

* `R_in(child) = R_out(node) ∪ cols_used(sort_exprs)`
  Fields: `expr`, `input`, `fetch`. ([Docs.rs][11])

**Limit / Repartition / SubqueryAlias**

* Generally pass-through: `R_in(child) = R_out(node) ∪ U(node)`
  Fields for Limit: `skip`, `fetch`, `input`. ([Docs.rs][12])
  Fields for Repartition: `partitioning_scheme` may introduce `cols_used(partitioning_exprs)`. ([Docs.rs][13])
  Fields for SubqueryAlias: `alias`, `input`, `schema` (rename/qualify semantics). ([Docs.rs][14])

**Union**

* `R_in(each_input) = R_out(node)` (names aligned to union schema)
  Fields: `inputs`, `schema`. ([Docs.rs][20])

**TableScan**

* If plan already has `projection` indices, record them as executed required cols; else your computed `R_out(scan)` can drive a custom scan scheduler or be compared to pushdown results. ([Docs.rs][4])

---

### A3) Code skeleton: logical extraction + required columns (LLM-agent implementable)

```python
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

# --- public “planner artifacts” you want to persist ---
@dataclass(frozen=True)
class LogicalNodeRec:
    node_id: str
    variant: str
    fields: Dict[str, Any]           # raw extracted fields (safe for debugging)
    output_cols: Optional[List[str]] # if you can get schema columns
    inputs: List[str]                # child node_ids

@dataclass
class RequiredColsRec:
    required_out: Optional[Set[str]]
    required_in_by_child: Dict[str, Set[str]]

# -----------------------
# Helpers: safe reflection
# -----------------------
def _get(obj: Any, name: str, default=None):
    return getattr(obj, name, default)

def _variant_name(v: Any) -> str:
    return type(v).__name__

# -----------------------
# Expr column extraction
# -----------------------
def expr_cols(expr: Any, *, candidate_cols: Set[str]) -> Set[str]:
    """
    Best-effort column extraction from a DataFusion Expr.
    Strategy:
      1) attempt structural walk via to_variant() (if available)
      2) fallback: parse canonical_name() and intersect with candidate_cols
    """
    cols: Set[str] = set()

    # (A) structural attempt
    try:
        v = expr.to_variant()
        # Try common structural fields; recurse into any Expr-ish members.
        stack = [v]
        seen = set()
        while stack:
            cur = stack.pop()
            if id(cur) in seen:
                continue
            seen.add(id(cur))

            # common pattern: variant object has nested Expr fields
            for attr in dir(cur):
                if attr.startswith("_"):
                    continue
                val = getattr(cur, attr, None)
                if val is None:
                    continue
                # nested Expr
                if hasattr(val, "canonical_name") and hasattr(val, "to_variant"):
                    # this is likely an Expr wrapper
                    cols |= expr_cols(val, candidate_cols=candidate_cols)
                # list/tuple of Expr
                elif isinstance(val, (list, tuple)):
                    for it in val:
                        if hasattr(it, "canonical_name") and hasattr(it, "to_variant"):
                            cols |= expr_cols(it, candidate_cols=candidate_cols)
                # Column nodes sometimes expose a name directly
                elif attr in ("name", "column", "col") and isinstance(val, str) and val in candidate_cols:
                    cols.add(val)

        if cols:
            return cols
    except Exception:
        pass

    # (B) fallback: canonical_name tokenization
    try:
        s = expr.canonical_name()
        # strip DataFusion’s @N suffixes that show up in explain outputs
        import re
        tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", s)
        for t in tokens:
            if t in candidate_cols:
                cols.add(t)
        return cols
    except Exception:
        return set()

# -----------------------
# LogicalPlan traversal
# -----------------------
def extract_logical_tree(logical_plan: Any) -> Dict[str, LogicalNodeRec]:
    """
    Build a node table keyed by a synthetic node_id.
    Use LogicalPlan.inputs() and to_variant() as the primary traversal.
    """
    nodes: Dict[str, LogicalNodeRec] = {}
    counter = 0

    def visit(lp: Any) -> str:
        nonlocal counter
        node_id = f"lp{counter}"
        counter += 1

        v = lp.to_variant()            # variant object (Projection/Filter/TableScan/…) 
        variant = _variant_name(v)

        # child traversal
        children = lp.inputs()
        child_ids = [visit(c) for c in children]

        # best-effort schema columns (when available via string parsing)
        # If you can’t access schema directly, parse lp.display_indent_schema()
        output_cols = None
        try:
            schema_txt = lp.display_indent_schema()
            # parse columns from schema_txt (implementation omitted)
        except Exception:
            pass

        # capture raw fields we care about by convention
        fields = {}
        for k in ("input", "left", "right", "inputs", "expr", "predicate",
                  "group_expr", "aggr_expr", "window_expr", "on", "filter",
                  "table_name", "projection", "filters", "fetch", "schema", "alias"):
            if hasattr(v, k):
                fields[k] = getattr(v, k)

        nodes[node_id] = LogicalNodeRec(
            node_id=node_id, variant=variant, fields=fields,
            output_cols=output_cols, inputs=child_ids
        )
        return node_id

    visit(logical_plan)
    return nodes

# -----------------------
# Required columns pass
# -----------------------
def propagate_required_cols(
    nodes: Dict[str, LogicalNodeRec],
    root_id: str,
    schema_cols_by_node: Dict[str, Set[str]],
    root_required: Optional[Set[str]] = None,
) -> Dict[str, RequiredColsRec]:
    """
    Top-down required column propagation.
    schema_cols_by_node: known output columns for each plan node (best effort).
    """
    reqs: Dict[str, RequiredColsRec] = {}

    def walk(node_id: str, required_out: Optional[Set[str]]):
        rec = nodes[node_id]
        variant = rec.variant
        out_cols = schema_cols_by_node.get(node_id, set())

        # helper: candidate cols for expressions = child output cols (computed later per child)
        required_in_by_child: Dict[str, Set[str]] = {}

        # unary operators
        if len(rec.inputs) == 1:
            child = rec.inputs[0]
            child_cols = schema_cols_by_node.get(child, set())

            if variant == "Filter":
                pred = rec.fields.get("predicate")
                used = expr_cols(pred, candidate_cols=child_cols) if pred else set()
                required_in = (required_out or child_cols) | used

            elif variant == "Projection":
                # if required_out is None, treat as all output
                # NOTE: you'd want a projection-output-name -> Expr mapping here;
                # if unavailable, conservatively require all referenced columns.
                exprs = rec.fields.get("expr") or []
                used = set()
                for e in exprs:
                    used |= expr_cols(e, candidate_cols=child_cols)
                required_in = used

            elif variant == "Aggregate":
                group_expr = rec.fields.get("group_expr") or []
                aggr_expr = rec.fields.get("aggr_expr") or []
                used = set()
                for e in list(group_expr) + list(aggr_expr):
                    used |= expr_cols(e, candidate_cols=child_cols)
                required_in = used

            elif variant == "Sort":
                exprs = rec.fields.get("expr") or []
                used = set()
                for e in exprs:
                    # sort exprs might not be Expr directly in python; handle defensively
                    if hasattr(e, "expr"):
                        used |= expr_cols(e.expr(), candidate_cols=child_cols)
                    elif hasattr(e, "canonical_name"):
                        used |= expr_cols(e, candidate_cols=child_cols)
                required_in = (required_out or child_cols) | used

            else:
                # Limit / Repartition / Window / SubqueryAlias / Analyze / Explain default
                required_in = required_out or child_cols

            required_in_by_child[child] = required_in
            reqs[node_id] = RequiredColsRec(required_out=required_out, required_in_by_child=required_in_by_child)
            walk(child, required_in)

        # binary operators (Join)
        elif variant == "Join" and len(rec.inputs) == 2:
            left, right = rec.inputs
            lcols = schema_cols_by_node.get(left, set())
            rcols = schema_cols_by_node.get(right, set())

            on = rec.fields.get("on") or []
            join_filter = rec.fields.get("filter")

            used_left = set()
            used_right = set()

            # "on" is typically pairs; handle defensively
            for pair in on:
                if isinstance(pair, (list, tuple)) and len(pair) == 2:
                    used_left |= expr_cols(pair[0], candidate_cols=lcols)
                    used_right |= expr_cols(pair[1], candidate_cols=rcols)

            if join_filter is not None:
                used_left |= expr_cols(join_filter, candidate_cols=lcols)
                used_right |= expr_cols(join_filter, candidate_cols=rcols)

            # If you can map required_out columns to left/right provenance, do it here.
            left_need = (required_out or lcols) | used_left
            right_need = (required_out or rcols) | used_right

            required_in_by_child[left] = left_need
            required_in_by_child[right] = right_need
            reqs[node_id] = RequiredColsRec(required_out=required_out, required_in_by_child=required_in_by_child)
            walk(left, left_need)
            walk(right, right_need)

        else:
            # Union / multi-input, etc: propagate required_out to each input
            for child in rec.inputs:
                ccols = schema_cols_by_node.get(child, set())
                required_in_by_child[child] = required_out or ccols
                walk(child, required_out or ccols)
            reqs[node_id] = RequiredColsRec(required_out=required_out, required_in_by_child=required_in_by_child)

    walk(root_id, root_required)
    return reqs
```

This is intentionally “defensive reflection” because Python wrappers can differ by DataFusion version; the stable contract is *the variant catalog + field names* from the Rust side (above).

---

## B) Physical plan parser: extracting `DataSourceExec` file_groups / projection / predicate

### B0) Why physical parsing is separate

* DataFusion’s physical plan is hardware/data dependent and can differ across machines/configs. ([Apache DataFusion][27])
* But the physical plan exposes **file_groups** and scan-level **predicate/projection** in `DataSourceExec`, which is *exactly* what you need for “file-group scheduling”. The official explain-usage example shows `DataSourceExec: file_groups=…, projection=[…], predicate=…, file_type=parquet`. ([Apache DataFusion][28])

### B1) Getting the plan string (Python)

* `ExecutionPlan.display_indent()` prints an indented physical plan; `partition_count` gives partition parallelism. ([Apache DataFusion][1])

For fixtures, also use SQL `EXPLAIN` in `FORMAT indent` because:

* `EXPLAIN VERBOSE` and `EXPLAIN ANALYZE` only support `indent`. ([Apache DataFusion][29])
* The `indent` format prints one operator per line with indentation. ([Apache DataFusion][30])
* Config flags let you toggle schema/stats/sizes, and default explain format. ([Apache DataFusion][31])

### B2) Parsing strategy (robust)

Physical lines look like:

* `DataSourceExec: file_groups={16 groups: [[hits.parquet:0..923748528], ...]}, projection=[WatchID, ClientIP, URL], predicate=starts_with(...), file_type=parquet` ([Apache DataFusion][28])

You want:

* `file_groups_raw` (and optionally a parsed list of `(path, range)` tuples)
* `projection_cols`
* `predicate_raw` (and optionally normalized predicate)

**Key pitfall:** properties are comma-separated but contain nested `{…}` / `[…]` / `(…)`. So you need a **bracket-aware splitter** (state machine).

```python
import re
from dataclasses import dataclass
from typing import Dict, List, Optional

@dataclass(frozen=True)
class DataSourceExecRec:
    indent: int
    file_groups_raw: Optional[str]
    projection: Optional[List[str]]
    predicate_raw: Optional[str]
    file_type: Optional[str]
    extra: Dict[str, str]

_BRACKETS = {"{": "}", "[": "]", "(": ")"}
_CLOSE = {v: k for k, v in _BRACKETS.items()}

def split_top_level_commas(s: str) -> List[str]:
    out, buf = [], []
    stack = []
    for ch in s:
        if ch in _BRACKETS:
            stack.append(ch)
        elif ch in _CLOSE and stack and stack[-1] == _CLOSE[ch]:
            stack.pop()
        if ch == "," and not stack:
            out.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    if buf:
        out.append("".join(buf).strip())
    return out

def parse_kv_props(prop_str: str) -> Dict[str, str]:
    props = {}
    for part in split_top_level_commas(prop_str):
        if "=" in part:
            k, v = part.split("=", 1)
            props[k.strip()] = v.strip()
        else:
            # e.g. "file_groups{...}" variants; keep raw
            props[part.strip()] = ""
    return props

def parse_datasource_execs(physical_indent: str) -> List[DataSourceExecRec]:
    recs = []
    for line in physical_indent.splitlines():
        # Count leading spaces for hierarchy
        indent = len(line) - len(line.lstrip(" "))
        txt = line.strip()
        if not txt.startswith("DataSourceExec:"):
            continue

        rest = txt[len("DataSourceExec:"):].strip()
        props = parse_kv_props(rest)

        proj = None
        if "projection" in props:
            inner = props["projection"].strip()
            if inner.startswith("[") and inner.endswith("]"):
                proj = [c.strip() for c in inner[1:-1].split(",") if c.strip()]

        recs.append(
            DataSourceExecRec(
                indent=indent,
                file_groups_raw=props.get("file_groups"),
                projection=proj,
                predicate_raw=props.get("predicate"),
                file_type=props.get("file_type"),
                extra={k: v for k, v in props.items()
                       if k not in {"file_groups","projection","predicate","file_type"}},
            )
        )
    return recs
```

**Scheduler integration:**

* treat each `file_groups` group as a schedulable “work unit”
* map `DataSourceExecRec.projection` to your **executed** required-cols overlay
* store `predicate_raw` as “executed pushdown predicate”; reconcile with logical `TableScan.filters` and logical `Filter` residual predicates.

---

## C) Canonical expression normalizer: SQLGlot lingua franca + optional Substrait bytes

You need canonicalization for:

* **Plan fingerprinting** (cache keys / invalidation)
* **Predicate equivalence** (same filter written differently)
* **Computed column lineage** (output col depends on which upstream cols)

### C0) DataFusion-native canonical forms (always capture)

* `Expr.canonical_name()` returns a complete string representation. ([Apache DataFusion][26])
* DataFusion recommends `canonical_name` for equivalence and `display_name` for schema naming. ([Apache DataFusion][3])

This gives you a cheap, engine-consistent canonicalization layer even if SQLGlot parsing fails for some edge syntax.

### C1) Convert DataFusion LogicalPlan → SQL (to feed SQLGlot)

DataFusion Python exposes an **unparser**:

* `datafusion.unparser.Unparser.plan_to_sql(plan)` converts a logical plan to a SQL string, with selectable dialect and pretty formatting. ([Apache DataFusion][32])

This is the cleanest bridge from DataFusion plans into your existing SQLGlot-based tooling.

### C2) SQLGlot normalization pipeline (the “lingua franca”)

Key building blocks (all official SQLGlot APIs):

* `parse_one(sql, dialect=...)` parses SQL to AST; specifying dialect matters. ([GitHub][33])
* `normalize_identifiers(...)` standardizes identifier casing while preserving semantics. ([SqlGlot][34])
* `qualify(...)` rewrites AST to normalized/qualified tables + columns (prerequisite for many optimizations). ([SqlGlot][35])
* `qualify_tables(...)` fully qualifies table references (db/catalog). ([SqlGlot][36])
* `serde.dump(expr)` produces JSON-serializable AST payloads you can hash stably. ([SqlGlot][37])
* `lineage(column, sql_or_expr, …)` can build a column lineage graph per output column (useful for computed column dependency sets). ([SqlGlot][38])
* Optional: boolean normal-form checks via `normalize.normalized(...)` (CNF/DNF) to reduce predicate-diff noise. ([SqlGlot][39])

**Canonicalization recipe (practical):**

1. `ast = parse_one(sql, dialect="")` (or your chosen base dialect; many stacks just use SQLGlot’s generic) ([SqlGlot][40])
2. `ast = normalize_identifiers(ast, dialect=...)` ([SqlGlot][34])
3. `ast = qualify(ast, schema=..., db=..., catalog=...)` ([SqlGlot][35])
4. `ast = qualify_tables(ast, db=..., catalog=...)` ([SqlGlot][36])
5. `canon_json = serde.dump(ast)` and hash it ([SqlGlot][37])
6. For predicates: extract WHERE/HAVING/join conditions and optionally normalize CNF/DNF if you want stronger equivalence ([SqlGlot][39])
7. For computed column lineage: run `sqlglot.lineage.lineage(colname, ast, schema=..., sources=...)` ([SqlGlot][38])

### C3) Optional stable bytes: Substrait

DataFusion Python supports serializing SQL to Substrait bytes:

* `Serde.serialize_bytes(sql, ctx) -> bytes` ([Apache DataFusion][41])
* or go plan-first: `Producer.to_substrait_plan(logical_plan, ctx)` then `Plan.encode()` ([Apache DataFusion][41])
  Substrait is explicitly supported by DataFusion (including through the Python bindings). ([substrait.io][42])

**How to use this in your orchestrator:**

* `plan_fp_primary = hash(substrait_bytes)` (fast change detector across equivalent SQL strings)
* `plan_fp_secondary = hash(sqlglot_dump_json)` (human-debuggable, diffable)
* store both, and when they disagree you have a perfect “diagnostic trigger” to dump full artifacts.

---

## What I’d implement next (directly actionable)

1. A **schema extractor** for `LogicalPlan` nodes (best effort):

   * prefer structured schema access if exposed; otherwise parse `display_indent_schema()` into a column list. ([Apache DataFusion][1])
2. A **join provenance mapper** (which output cols come from left vs right) using qualified column naming + `SubqueryAlias` tracking. ([Docs.rs][19])
3. A **subquery-dependency sweep**:

   * walk expression trees and detect subquery variants; DataFusion’s optimizer docs show the pattern conceptually. ([Apache DataFusion][3])

If you want, I can now turn this into a single cohesive “`PlanBundle → CanonicalGraph`” module: concrete node/edge schemas, deterministic fingerprints, and a golden-test fixture suite built from `EXPLAIN FORMAT INDENT` outputs and `ExecutionPlan.display_indent()` snapshots. ([Apache DataFusion][29])

[1]: https://datafusion.apache.org/python/autoapi/datafusion/plan/index.html "datafusion.plan — Apache Arrow DataFusion  documentation"
[2]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html "LogicalPlan in datafusion::logical_expr - Rust"
[3]: https://datafusion.apache.org/library-user-guide/query-optimizer.html "Query Optimizer — Apache DataFusion  documentation"
[4]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.TableScan.html "TableScan in datafusion::logical_expr - Rust"
[5]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.EmptyRelation.html "EmptyRelation in datafusion::logical_expr - Rust"
[6]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Values.html "Values in datafusion::logical_expr - Rust"
[7]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Projection.html "Projection in datafusion::logical_expr - Rust"
[8]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Filter.html "Filter in datafusion::logical_expr - Rust"
[9]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Aggregate.html "Aggregate in datafusion::logical_expr - Rust"
[10]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Window.html "Window in datafusion::logical_expr - Rust"
[11]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Sort.html "Sort in datafusion::logical_expr - Rust"
[12]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Limit.html "Limit in datafusion::logical_expr - Rust"
[13]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Repartition.html "Repartition in datafusion::logical_expr - Rust"
[14]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.SubqueryAlias.html "SubqueryAlias in datafusion::logical_expr - Rust"
[15]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.Distinct.html "Distinct in datafusion::logical_expr - Rust"
[16]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Analyze.html "Analyze in datafusion::logical_expr - Rust"
[17]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Explain.html "Explain in datafusion::logical_expr - Rust"
[18]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Unnest.html "Unnest in datafusion::logical_expr - Rust"
[19]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Join.html "Join in datafusion::logical_expr - Rust"
[20]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Union.html "Union in datafusion::logical_expr - Rust"
[21]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Subquery.html "Subquery in datafusion::logical_expr - Rust"
[22]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.RecursiveQuery.html "RecursiveQuery in datafusion::logical_expr - Rust"
[23]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Extension.html "Extension in datafusion::logical_expr - Rust"
[24]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.DmlStatement.html "DmlStatement in datafusion::logical_expr - Rust"
[25]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.DdlStatement.html "DdlStatement in datafusion::logical_expr - Rust"
[26]: https://datafusion.apache.org/python/autoapi/datafusion/expr/index.html "datafusion.expr — Apache Arrow DataFusion  documentation"
[27]: https://datafusion.apache.org/user-guide/explain-usage.html?utm_source=chatgpt.com "Reading Explain Plans — Apache DataFusion documentation"
[28]: https://datafusion.apache.org/user-guide/explain-usage.html "Reading Explain Plans — Apache DataFusion  documentation"
[29]: https://datafusion.apache.org/user-guide/sql/explain.html "EXPLAIN — Apache DataFusion  documentation"
[30]: https://datafusion.apache.org/user-guide/sql/explain.html?utm_source=chatgpt.com "EXPLAIN — Apache DataFusion documentation"
[31]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[32]: https://datafusion.apache.org/python/autoapi/datafusion/unparser/index.html "datafusion.unparser — Apache Arrow DataFusion  documentation"
[33]: https://github.com/tobymao/sqlglot?utm_source=chatgpt.com "tobymao/sqlglot: Python SQL Parser and Transpiler"
[34]: https://sqlglot.com/sqlglot/optimizer/normalize_identifiers.html?utm_source=chatgpt.com "sqlglot.optimizer.normalize_identifiers API documentation"
[35]: https://sqlglot.com/sqlglot/optimizer/qualify.html?utm_source=chatgpt.com "sqlglot.optimizer.qualify API documentation"
[36]: https://sqlglot.com/sqlglot/optimizer/qualify_tables.html?utm_source=chatgpt.com "sqlglot.optimizer.qualify_tables API documentation"
[37]: https://sqlglot.com/sqlglot/serde.html?utm_source=chatgpt.com "sqlglot.serde API documentation"
[38]: https://sqlglot.com/sqlglot/lineage.html?utm_source=chatgpt.com "sqlglot.lineage API documentation"
[39]: https://sqlglot.com/sqlglot/optimizer/normalize.html?utm_source=chatgpt.com "sqlglot.optimizer.normalize API documentation"
[40]: https://sqlglot.com/sqlglot/dialects.html?utm_source=chatgpt.com "sqlglot.dialects API documentation"
[41]: https://datafusion.apache.org/python/autoapi/datafusion/substrait/index.html "datafusion.substrait — Apache Arrow DataFusion  documentation"
[42]: https://substrait.io/community/powered_by/?utm_source=chatgpt.com "Powered by Substrait"

Below is a “pattern-matching + lineage report” deep dive that assumes you want **machine-readable**, **diffable**, **scheduler-usable** artifacts from DataFusion **without parsing SQL** as the primary source of truth.

The core technique is:

* **LogicalPlan layer** (optimized) → correctness + lineage (scans, joins, projections, filters, aggregates)
* **ExecutionPlan layer** → execution-aware hints (partitions, file groups, scan predicates/projections as actually executed)

DataFusion Python gives you the primitives you need: `df.optimized_logical_plan()`, `LogicalPlan.inputs()`, `LogicalPlan.to_variant()`, and `df.execution_plan()` plus `ExecutionPlan.children()` and `partition_count`. ([Apache DataFusion][1])

---

## 1) Planning surfaces and why you should use *optimized* logical

### 1.1 The exact API entry points (Python)

From a `DataFrame` you can extract:

* **optimized logical**: `df.optimized_logical_plan()`
* **physical**: `df.execution_plan()`

From the plan objects:

* `LogicalPlan.inputs()` → child logical plans
* `LogicalPlan.to_variant()` → concrete operator struct (Projection / Filter / Join / TableScan / …)
* `ExecutionPlan.children()` → child execution plans
* `ExecutionPlan.partition_count` → physical parallelism degree (coarse) ([Apache DataFusion][1])

DataFusion explicitly frames a `LogicalPlan` as a node in a tree of relational operators producing an output relation from an input relation. ([Apache DataFusion][1])

### 1.2 Why optimized logical is the backbone for lineage

DataFusion’s pipeline is: Analyzer rules → **Optimizer rules (projection/filter pushdown)** → physical planning → physical optimizer rules. ([Docs.rs][2])
So the **optimized logical plan** already incorporates the most important lineage-relevant rewrites (notably **projection and filter pushdown**) while remaining stable enough to compare across runs.

---

## 2) The output you want: a machine-friendly lineage report schema

You want two “views” of the same query:

### 2.1 Logical lineage (correctness + semantics)

* **Scan dependencies**: which datasets are read (table name / provider)
* **Required columns per scan**: what the plan says each scan needs
* **Filter semantics**: residual filters vs scan-pushdown filters
* **Join semantics**: join type, join keys, optional join filter
* **Aggregate semantics**: group keys + aggregate expressions
* **Projection lineage**: output columns → defining expressions

### 2.2 Physical hints (execution)

* **Partition count** for each physical subtree (parallel scheduling heuristic)
* **File groups** for scan operators (your “work units”)
* **Predicate/projection actually applied** at scan (truth for I/O scheduling)
* Optional: metrics via `EXPLAIN ANALYZE` (if you choose to run it)

DataFusion’s “Reading Explain Plans” doc stresses that physical plans incorporate hardware + data organization and can differ across environments. Treat physical details as **hints**, not correctness. ([Apache DataFusion][3])

---

## 3) LogicalPlan pattern matching: `inputs()` + `to_variant()` as the extractor core

### 3.1 The two-phase walk

1. **Tree traversal**: recursively traverse `LogicalPlan.inputs()` (this is the structural backbone). ([Apache DataFusion][1])
2. **Variant introspection**: for each node, call `to_variant()` and pattern-match on `type(variant).__name__` (Projection/Filter/Join/TableScan/…). ([Apache DataFusion][1])

### 3.2 Variant fields: what you extract for lineage

The Python `to_variant()` object mirrors (closely) the Rust operator structs. For example, `TableScan` fields include `table_name`, `projection`, `projected_schema`, and `filters` (pushdown filters). ([Docs.rs][4])

That makes `TableScan` the single most important node for orchestration:

* `projection` is **projection pushdown**
* `filters` are **table-provider pushdown filters**
* `projected_schema` lets you map projection indices → column names ([Docs.rs][4])

### 3.3 Subqueries are the “gotcha”

`LogicalPlan.inputs()` walks the *plan tree*, but subqueries may be nested inside expressions. You should defensively scan expressions for embedded subplans (e.g., EXISTS/IN subquery). DataFusion’s optimizer docs describe extracting subquery references from expressions conceptually (even if you don’t rely on those internal helpers in Python). ([Docs.rs][5])

Practical rule: **if a variant has any fields that look like a plan (`subquery`, `plan`, etc.), explicitly recurse into those too**, in addition to `inputs()`.

---

## 4) Building “required columns per upstream dataset”: top-down + expression-use extraction

Even though DataFusion does projection pushdown automatically, you still want to compute required columns yourself because:

* it produces a stable lineage artifact (independent of pushdown success),
* it lets you unify with Ibis/SQLGlot lineage,
* it helps catch “silent regressions” when an optimizer rule stops applying.

### 4.1 Expression → referenced columns (robust approach)

DataFusion expressions have `canonical_name()` and DataFusion recommends it for expression equivalence / stable representation. ([Docs.rs][2])
In Python, you can use `expr.canonical_name()` as a fallback to extract referenced columns by intersecting tokens with the child schema’s column set (this is surprisingly effective and version-resilient).

### 4.2 Operator propagation rules (high-signal subset)

* **Filter**: child_required = parent_required ∪ cols(predicate)
* **Projection**: child_required = cols(expressions that produce parent_required outputs)
* **Aggregate**: child_required = cols(group_expr ∪ needed aggr_expr)
* **Join**: split parent_required by left/right provenance + always include join keys (+ optional join filter)
* **Sort/Window/Repartition**: child_required = parent_required ∪ cols(sort/window/partition exprs)
* **Union**: child_required = parent_required for each input

This works best if you can reliably get output schemas at each node. DataFusion Python provides `display_indent_schema()` for a best-effort schema trace when direct schema objects aren’t accessible. ([Apache DataFusion][1])

---

## 5) Turning `TableScan` into scheduler-grade “scan records”

### 5.1 Table identity and normalization

The `TableScan` struct has `table_name: TableReference` in Rust. ([Docs.rs][4])
You should normalize into a `DatasetKey` (your internal canonical dataset identity) via a resolver:

* map `catalog.schema.table` ↔ your dataset registry keys
* map file scans / URL tables to a stable URI form
* incorporate scan options if they affect semantics (format, partition cols, etc.)

### 5.2 Projection pushdown as “executed required columns”

DataFusion explicitly provides projection indices to `TableProvider::scan` as “projection pushdown” to only read needed columns. ([Docs.rs][6])
So from a `TableScan` node:

* if `projection` present → executed required cols = projection-mapped column names
* else → executed required cols = propagated required cols (your computed)

### 5.3 Pushdown filters

`TableScan.filters: Vec<Expr>` are “expressions to be used as filters by the table provider”. ([Docs.rs][4])
Record them separately from residual `Filter` nodes:

* pushdowns are “applied at scan”
* residual filters are “applied above scan”

This split is extremely valuable for file-level scheduling and index selection.

---

## 6) Producing a lineage report: reference implementation skeleton (logical)

Below is a practical skeleton that:

* traverses `LogicalPlan.inputs()`
* inspects `to_variant()`
* emits scan/projection/filter/join/aggregate records
* computes required columns top-down (with a token-based fallback)

```python
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

@dataclass
class ScanRec:
    node_id: str
    table_name: str
    projection_cols: Optional[List[str]]
    pushed_filters: List[str]  # canonical_name() strings
    required_cols_computed: List[str]  # from propagation

@dataclass
class JoinRec:
    node_id: str
    join_type: str
    on_pairs: List[Tuple[str, str]]    # left_expr, right_expr (canonical_name)
    join_filter: Optional[str]

@dataclass
class FilterRec:
    node_id: str
    predicate: str

@dataclass
class AggRec:
    node_id: str
    group_exprs: List[str]
    aggr_exprs: List[str]

@dataclass
class LineageReport:
    scans: List[ScanRec] = field(default_factory=list)
    joins: List[JoinRec] = field(default_factory=list)
    filters: List[FilterRec] = field(default_factory=list)
    aggs: List[AggRec] = field(default_factory=list)

def _tokens(s: str) -> Set[str]:
    import re
    return set(re.findall(r"[A-Za-z_][A-Za-z0-9_]*", s))

def _expr_cols(expr: Any, candidate_cols: Set[str]) -> Set[str]:
    # version-resilient fallback: canonical_name tokenization
    try:
        s = expr.canonical_name()
    except Exception:
        s = str(expr)
    return _tokens(s) & candidate_cols

def build_lineage_from_logical(df) -> LineageReport:
    lp = df.optimized_logical_plan()

    # You’ll usually want schema column sets per node.
    # If you can’t read schema directly, parse lp.display_indent_schema().
    # Here we keep it simple: you inject schema_by_node in real code.
    schema_by_node: Dict[str, Set[str]] = {}

    report = LineageReport()
    node_counter = 0

    def visit(plan) -> str:
        nonlocal node_counter
        node_id = f"lp{node_counter}"
        node_counter += 1

        v = plan.to_variant()
        variant = type(v).__name__

        children = plan.inputs()
        child_ids = [visit(c) for c in children]

        # ---- operator-specific extraction ----
        if variant == "TableScan":
            table_name = str(getattr(v, "table_name", ""))
            proj = getattr(v, "projection", None)
            projected_schema = getattr(v, "projected_schema", None)
            # If projected_schema is accessible, map indices -> names; else leave None
            projection_cols = None

            pushed = []
            for e in getattr(v, "filters", []) or []:
                try:
                    pushed.append(e.canonical_name())
                except Exception:
                    pushed.append(str(e))

            report.scans.append(ScanRec(
                node_id=node_id,
                table_name=table_name,
                projection_cols=projection_cols,
                pushed_filters=pushed,
                required_cols_computed=[],
            ))

        elif variant == "Filter":
            pred = getattr(v, "predicate", None)
            report.filters.append(FilterRec(
                node_id=node_id,
                predicate=(pred.canonical_name() if pred is not None else ""),
            ))

        elif variant == "Join":
            on_pairs = []
            for pair in getattr(v, "on", []) or []:
                if isinstance(pair, (list, tuple)) and len(pair) == 2:
                    on_pairs.append((
                        pair[0].canonical_name() if hasattr(pair[0], "canonical_name") else str(pair[0]),
                        pair[1].canonical_name() if hasattr(pair[1], "canonical_name") else str(pair[1]),
                    ))
            jf = getattr(v, "filter", None)
            report.joins.append(JoinRec(
                node_id=node_id,
                join_type=str(getattr(v, "join_type", "")),
                on_pairs=on_pairs,
                join_filter=(jf.canonical_name() if jf is not None else None),
            ))

        elif variant == "Aggregate":
            group_exprs = []
            for e in getattr(v, "group_expr", []) or []:
                group_exprs.append(e.canonical_name() if hasattr(e, "canonical_name") else str(e))
            aggr_exprs = []
            for e in getattr(v, "aggr_expr", []) or []:
                aggr_exprs.append(e.canonical_name() if hasattr(e, "canonical_name") else str(e))
            report.aggs.append(AggRec(
                node_id=node_id, group_exprs=group_exprs, aggr_exprs=aggr_exprs
            ))

        # NOTE: for completeness, also inspect variant fields for embedded subplans
        # and recurse into them explicitly.

        return node_id

    visit(lp)

    # In real code: run a required-column propagation pass and fill
    # ScanRec.required_cols_computed per scan node_id.

    return report
```

Key detail: this approach doesn’t depend on SQL parsing. It uses the plan tree (`inputs`) and structured variants (`to_variant`) that DataFusion exposes. ([Apache DataFusion][1])

---

## 7) ExecutionPlan traversal: `children()` + `partition_count` for execution-aware hints

### 7.1 The stable primitives

The Rust `ExecutionPlan` trait defines `children()` as the way to traverse the physical DAG. ([Docs.rs][7])
DataFusion Python’s `ExecutionPlan` wrapper exposes:

* `children()`
* `partition_count`
* display helpers and protobuf serialization (with limitations) ([Apache DataFusion][1])

### 7.2 What you pull from physical

Scheduler-grade physical hints include:

* **global / subtree parallelism**: `partition_count`
* **scan work partitioning**: `DataSourceExec file_groups={...}`
* **executed scan projection/predicate**: shown on `DataSourceExec` lines

DataFusion’s explain usage shows physical plans include file organization and can vary by hardware/data layout. ([Apache DataFusion][3])

### 7.3 Practical extraction path: display-based parsing for DataSourceExec

Physical scan nodes like `DataSourceExec` often expose `file_groups`, `projection`, `predicate`, and `file_type` in their indented display. The “Reading Explain Plans” example demonstrates exactly that format. ([Apache DataFusion][3])

So you typically:

1. Traverse the physical plan with `children()` for structure
2. Use `display_indent()` (or `EXPLAIN FORMAT indent`) as a reliable textual carrier for scan details ([Apache DataFusion][8])

---

## 8) Bridging to SQLGlot and plan hashing (optional but very useful)

### 8.1 Unparsing DataFusion logical → SQL

DataFusion provides an unparser that converts a `LogicalPlan` back to SQL. There’s a Python module `datafusion.unparser` for this, and in Rust `plan_to_sql` is explicitly “the opposite” of SQL→plan. ([Apache DataFusion][9])

This is ideal when you want to:

* normalize and hash expressions with SQLGlot,
* produce human-diffable artifacts for golden tests,
* reuse your existing SQLGlot lineage tooling.

### 8.2 Serialization caveat (proto)

Both logical and physical plan protobuf serialization note the limitation: **tables created in memory from record batches are not supported**. ([Apache DataFusion][1])
So don’t make protobuf the only persistence strategy if you rely heavily on in-memory sources; keep text/unparsed SQL / Substrait as alternates.

---

## 9) Recommended “best practice” report: merge logical lineage + physical hints

In practice you emit a single JSON-ish artifact with:

* `logical.scans[]`: dataset keys, computed required cols, pushed filters
* `logical.joins[]`: join type, join keys, join filter
* `logical.filters[]`: residual predicates
* `logical.aggregates[]`: group/aggregate expressions
* `physical.scans[]`: file_groups, executed projection/predicate, partition_count (per subtree)
* `fingerprints`: stable ids (e.g., SQLGlot AST hash + (optional) proto/substrait hash)

This is aligned with DataFusion’s own separation: logical plans capture query structure independent of hardware, while physical plans incorporate file organization and CPU count—useful for performance diagnosis and scheduling. ([Apache DataFusion][3])

---


[1]: https://datafusion.apache.org/python/autoapi/datafusion/plan/index.html?utm_source=chatgpt.com "datafusion.plan — Apache Arrow DataFusion documentation"
[2]: https://docs.rs/datafusion/latest/datafusion/?utm_source=chatgpt.com "datafusion - Rust"
[3]: https://datafusion.apache.org/user-guide/explain-usage.html?utm_source=chatgpt.com "Reading Explain Plans — Apache DataFusion documentation"
[4]: https://docs.rs/datafusion/latest/datafusion/logical_expr/logical_plan/struct.TableScan.html?utm_source=chatgpt.com "TableScan in datafusion::logical_expr::logical_plan - Rust"
[5]: https://docs.rs/datafusion/latest/datafusion/logical_expr/index.html?utm_source=chatgpt.com "datafusion::logical_expr - Rust"
[6]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html?utm_source=chatgpt.com "TableProvider in datafusion::catalog - Rust"
[7]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html?utm_source=chatgpt.com "ExecutionPlan in datafusion::physical_plan - Rust"
[8]: https://datafusion.apache.org/user-guide/sql/explain.html?utm_source=chatgpt.com "EXPLAIN — Apache DataFusion documentation"
[9]: https://datafusion.apache.org/python/autoapi/datafusion/unparser/index.html?utm_source=chatgpt.com "datafusion.unparser"
[10]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html?utm_source=chatgpt.com "LogicalPlan in datafusion_expr::logical_plan - Rust"

Below is a **drop-in module spec** that you can hand to an LLM coding agent and expect an implementation that:

1. **Pattern-matches DataFusion optimized LogicalPlans** via `LogicalPlan.inputs()` + `to_variant()` and emits a **machine-friendly lineage report** (deps + predicates + join keys + required columns per upstream scan).
2. Computes **required columns** using a top-down propagation pass, with a **schema oracle** that falls back to `display_indent_schema()` when you can’t reliably read schema objects.
3. Parses **physical plans** (`ExecutionPlan.display_indent()` and/or SQL `EXPLAIN FORMAT indent`) to extract **DataSourceExec file_groups / projection / predicate** and maps those file groups back to logical scans for **file-group scheduling**.

---

# 0) Minimal API surface you build against (Python)

DataFusion Python exposes the plan introspection primitives you need:

* `LogicalPlan.display_indent()`, `display_indent_schema()`, `inputs()`, `to_variant()` ([Apache DataFusion][1])
* `ExecutionPlan.display_indent()`, `partition_count`, protobuf serialization caveats ([Apache DataFusion][1])
* Expression APIs include `Expr.canonical_name()`, `display_name()`, `to_variant()`, `variant_name()` (useful for stable string forms and defensive parsing). ([Apache DataFusion][2])

DataFusion’s Rust docs also explicitly frame plans as **DAGs with embedded expressions**, so you should assume “expressions can hide subqueries” and build defensively. ([Docs.rs][3])

---

# 1) Drop-in module layout + core datamodel

### 1.1 File layout (single-module option)

`datafusion_lineage.py` (single file, no external deps besides `datafusion` + stdlib)

### 1.2 Core dataclasses (stable, JSON-friendly)

```python
# datafusion_lineage.py
from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Set, Tuple, Literal, Iterable

DatasetKind = Literal["table", "view", "file", "catalog", "schema", "function", "index", "unknown"]

@dataclass(frozen=True)
class DatasetKey:
    kind: DatasetKind
    name: str  # canonical identity (e.g. "catalog.schema.table" or "hits.parquet" or "s3://...")

@dataclass(frozen=True)
class ScanKey:
    """A stable handle for one logical scan node inside a plan."""
    plan_node_id: str
    dataset: DatasetKey

@dataclass(frozen=True)
class ExprCanon:
    """Canonical expression payload used in lineage artifacts."""
    df_canonical: str                 # Expr.canonical_name() or best-effort string :contentReference[oaicite:4]{index=4}
    df_variant: Optional[str] = None  # Expr.variant_name() if available :contentReference[oaicite:5]{index=5}

@dataclass
class LogicalScanRec:
    scan: ScanKey
    projected_cols_executed: Optional[List[str]] = None  # from TableScan.projection/projected_schema :contentReference[oaicite:6]{index=6}
    pushed_filters: List[ExprCanon] = field(default_factory=list)  # TableScan.filters :contentReference[oaicite:7]{index=7}
    required_cols_computed: Optional[List[str]] = None    # from propagation pass

@dataclass
class LogicalJoinRec:
    plan_node_id: str
    join_type: str
    on_pairs: List[Tuple[ExprCanon, ExprCanon]]           # Join.on pairs :contentReference[oaicite:8]{index=8}
    non_equi_filter: Optional[ExprCanon] = None           # Join.filter :contentReference[oaicite:9]{index=9}

@dataclass
class LogicalFilterRec:
    plan_node_id: str
    predicate: ExprCanon                                  # Filter.predicate :contentReference[oaicite:10]{index=10}

@dataclass
class LogicalProjectRec:
    plan_node_id: str
    exprs: List[ExprCanon]                                # Projection.expr :contentReference[oaicite:11]{index=11}

@dataclass
class LogicalAggregateRec:
    plan_node_id: str
    group_exprs: List[ExprCanon]                          # Aggregate.group_expr :contentReference[oaicite:12]{index=12}
    aggr_exprs: List[ExprCanon]                           # Aggregate.aggr_expr :contentReference[oaicite:13]{index=13}

@dataclass
class LogicalWindowRec:
    plan_node_id: str
    window_exprs: List[ExprCanon]                         # Window.window_expr :contentReference[oaicite:14]{index=14}

@dataclass
class DdlRec:
    plan_node_id: str
    ddl_kind: str
    outputs: List[DatasetKey] = field(default_factory=list)   # created/dropped objects
    inputs: List[DatasetKey] = field(default_factory=list)    # dependencies (e.g., CreateView.input) :contentReference[oaicite:15]{index=15}

@dataclass
class DmlRec:
    plan_node_id: str
    target: DatasetKey
    op: str
    inputs: List[DatasetKey] = field(default_factory=list)    # from DmlStatement.input :contentReference[oaicite:16]{index=16}

@dataclass
class PhysicalScanHint:
    dataset: DatasetKey
    file_groups_raw: Optional[str]
    projection: Optional[List[str]]
    predicate_raw: Optional[str]
    partition_count: Optional[int] = None

@dataclass
class LineageReport:
    # logical
    scans: List[LogicalScanRec] = field(default_factory=list)
    joins: List[LogicalJoinRec] = field(default_factory=list)
    filters: List[LogicalFilterRec] = field(default_factory=list)
    projects: List[LogicalProjectRec] = field(default_factory=list)
    aggregates: List[LogicalAggregateRec] = field(default_factory=list)
    windows: List[LogicalWindowRec] = field(default_factory=list)
    ddls: List[DdlRec] = field(default_factory=list)
    dmls: List[DmlRec] = field(default_factory=list)

    # physical (execution hints)
    physical_scans: List[PhysicalScanHint] = field(default_factory=list)

    # debug artifacts (golden fixtures)
    logical_indent: Optional[str] = None
    logical_indent_schema: Optional[str] = None
    physical_indent: Optional[str] = None

    def to_jsonable(self) -> Dict[str, Any]:
        return asdict(self)
```

---

# 2) Complete LogicalPlan variant catalog (handlers + fields + required-cols rule)

You pattern-match `type(lp.to_variant()).__name__` and extract the **same fields** the Rust structs expose.

## 2.1 Core variants you requested (operator specs)

### TableScan → “scan”

Rust struct fields: `table_name`, `source`, `projection`, `projected_schema`, `filters`, `fetch`. ([Docs.rs][4])
**Extraction:**

* dataset identity: `str(v.table_name)` (normalize in `DatasetResolver`)
* executed columns: if `projection` present → map indices via `projected_schema`
* pushed filters: `filters` are “expressions to be used as filters by the table provider” ([Docs.rs][4])
  **Required-cols rule (top-down):**
* `required_in(scan) = required_out(scan)`; compare with `projection` if present (pushdown worked).

### Projection → “projection”

Fields: `expr`, `input`, `schema`. ([Docs.rs][5])
**Required-cols rule:**

* If parent needs only subset of projection outputs, keep only those exprs; child required = union(cols_used(kept_exprs)).

### Filter → “filter”

Fields: `predicate`, `input`. ([Docs.rs][6])
**Required-cols rule:**

* child required = parent required ∪ cols_used(predicate).

### Join → “join”

Fields: `left`, `right`, `on`, `filter`, `join_type`, `join_constraint`, `schema`, `null_equality`. ([Docs.rs][7])
**Required-cols rule:**

* Split `parent_required` into left/right provenance (best effort; see schema oracle below).
* Always include join keys (`on` pairs) and optional `filter`.

### Aggregate → “aggregate”

Fields: `input`, `group_expr`, `aggr_expr`, `schema`. ([Docs.rs][8])
**Required-cols rule:**

* child required = cols_used(group_expr ∪ needed aggr_expr).
  Also note aggregate output schema is group exprs followed by aggr exprs. ([Docs.rs][8])

### Window → “window”

Fields: `input`, `window_expr`, `schema`. ([Docs.rs][9])
**Required-cols rule:**

* child required = parent required ∪ cols_used(window_expr).

### Sort → “sort”

Fields: `expr`, `input`, `fetch`. ([Docs.rs][10])
**Required-cols rule:**

* child required = parent required ∪ cols_used(sort keys).

### Limit → “limit”

Fields: `skip`, `fetch`, `input` (skip/fetch are Expr boxes). ([Docs.rs][11])
**Required-cols rule:**

* typically pass-through (limit doesn’t add column needs), but conservatively include cols_used(skip/fetch) in case they are expressions.

### SubqueryAlias → “alias”

Fields: `input`, `alias`, `schema`. ([Docs.rs][12])
**Required-cols rule:**

* pass-through required columns; but update qualifier mapping (alias) for downstream provenance.

### Union → “union”

Fields: `inputs`, `schema`. ([Docs.rs][13])
**Required-cols rule:**

* propagate `parent_required` to each input (names aligned by union schema).

### Distinct → “distinct”

Enum: `All(Arc<LogicalPlan>)` or `On(DistinctOn)`. ([Docs.rs][14])
For `DistinctOn`, fields: `on_expr`, `select_expr`, `sort_expr`, `input`, `schema`. ([Docs.rs][15])
**Required-cols rule:**

* `All`: pass-through
* `On`: child required = cols_used(on_expr ∪ select_expr ∪ sort_expr (if present))

### Repartition → “repartition”

Fields: `input`, `partitioning_scheme`. ([Docs.rs][16])
Partitioning variants include `Hash(Vec<Expr>, usize)` and `DistributeBy(Vec<Expr>)`. ([Docs.rs][17])
**Required-cols rule:**

* pass-through parent required, plus cols_used(partitioning exprs) when scheme contains expressions.

### Unnest → “unnest”

Fields: `input`, `exec_columns`, `list_type_columns`, `struct_type_columns`, `dependency_indices`, `schema`, `options`. ([Docs.rs][18])
**Required-cols rule:**

* If downstream needs specific output columns, use `dependency_indices` to determine which input columns are needed (this is unusually lineage-friendly). ([Docs.rs][18])

---

## 2.2 DML + DDL (sink semantics + deps)

### DmlStatement

Fields: `table_name`, `target`, `op`, `input`, `output_schema`. ([Docs.rs][19])
**Scheduler semantics:**

* Outputs: target table (`table_name`)
* Inputs: lineage extracted from `input` plan (recursive).
* Treat as **sink** op for orchestration; required columns are the columns demanded by the `input` plan producing rows to write.

### DdlStatement

Enum variants: CreateExternalTable, CreateMemoryTable, CreateView, CreateCatalogSchema, CreateCatalog, CreateIndex, DropTable, DropView, DropCatalogSchema, CreateFunction, DropFunction. ([Docs.rs][20])
Also exposes `inputs()` and `name()` methods (important for generic handling). ([Docs.rs][20])

Variant struct fields you should extract (minimum useful set):

* **CreateView**: `name`, `input`, `or_replace`, `definition`, `temporary`. ([Docs.rs][21])
* **CreateExternalTable**: `schema`, `name`, `location`, `file_type`, partition cols, `if_not_exists`, `or_replace`, `temporary`, `definition`, `order_exprs`, `options`, constraints, defaults. ([Docs.rs][22])
* **DropView**: `name`, `if_exists`, dummy `schema`. ([Docs.rs][23])
* **DropCatalogSchema**: `name`, `if_exists`, `cascade`, dummy `schema`. ([Docs.rs][24])

(If you can’t easily import every DDL struct type in Python, use `DdlStatement.name()` + `inputs()` + best-effort attribute access.)

---

# 3) Required-column propagation with a schema oracle (incl. `display_indent_schema()` fallback)

## 3.1 Why you need the schema oracle

For robust `cols_used(expr)` extraction, you want to intersect expression identifiers with **the actual column set** of the child relation. When schema objects aren’t directly introspectable in Python bindings, you fall back to `LogicalPlan.display_indent_schema()` which is explicitly available. ([Apache DataFusion][1])

## 3.2 Schema oracle strategy (ordered)

1. **Prefer variant schema fields** when present: `Projection.schema`, `Aggregate.schema`, `Join.schema`, `Union.schema`, `SubqueryAlias.schema`, `Unnest.schema`, `TableScan.projected_schema`, `DmlStatement.output_schema`. ([Docs.rs][5])
2. If schema object doesn’t expose names cleanly, parse `str(schema_obj)` with heuristics.
3. If still missing, parse `plan.display_indent_schema()` into a tree and align with `plan.display_indent()` (same indentation structure) to assign schema per node. ([Apache DataFusion][1])

## 3.3 Expression column extraction (DataFusion-native)

Use `Expr.canonical_name()` (stable, binding-friendly) and intersect tokens with candidate cols. ([Apache DataFusion][2])
Also available: `Expr.to_variant()` / `Expr.variant_name()` for deeper structural walkers when needed. ([Apache DataFusion][2])

## 3.4 Drop-in implementation (core pieces)

```python
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

def _expr_canon(e: Any) -> ExprCanon:
    # DataFusion Python Expr has canonical_name + variant_name :contentReference[oaicite:47]{index=47}
    try:
        s = e.canonical_name()
    except Exception:
        s = str(e)
    try:
        vn = e.variant_name()
    except Exception:
        vn = None
    return ExprCanon(df_canonical=s, df_variant=vn)

def _tokens(s: str) -> Set[str]:
    return set(_IDENT_RE.findall(s))

def cols_used(expr: Any, candidate_cols: Set[str]) -> Set[str]:
    if expr is None:
        return set()
    s = _expr_canon(expr).df_canonical
    # conservative: only count identifiers that are real columns in candidate set
    return _tokens(s) & candidate_cols

# ------------------------------
# Logical tree node (drop-in)
# ------------------------------
@dataclass
class LPNode:
    node_id: str
    variant: str
    v: Any               # variant payload
    children: List[str]  # node_ids
    indent_sig: str      # for aligning with display-based schema fallback

def build_lp_tree(root_lp: Any) -> Tuple[Dict[str, LPNode], str, str]:
    """
    Returns:
      nodes: node_id -> LPNode
      indent_plan: root_lp.display_indent()
      indent_schema: root_lp.display_indent_schema()
    """
    indent_plan = root_lp.display_indent()
    indent_schema = root_lp.display_indent_schema()  # explicitly provided :contentReference[oaicite:48]{index=48}

    nodes: Dict[str, LPNode] = {}
    counter = 0

    def visit(lp: Any) -> str:
        nonlocal counter
        node_id = f"lp{counter}"
        counter += 1
        v = lp.to_variant()  # LogicalPlan.to_variant() :contentReference[oaicite:49]{index=49}
        variant = type(v).__name__
        kids = [visit(c) for c in lp.inputs()]  # LogicalPlan.inputs() :contentReference[oaicite:50]{index=50}
        # signature used for best-effort alignment to display_* trees
        indent_sig = f"{variant}:{getattr(v,'table_name',getattr(v,'alias',''))}"
        nodes[node_id] = LPNode(node_id=node_id, variant=variant, v=v, children=kids, indent_sig=indent_sig)
        return node_id

    visit(root_lp)
    return nodes, indent_plan, indent_schema

# ------------------------------
# display_indent_schema parsing
# ------------------------------
@dataclass
class DisplayNode:
    variant: str
    raw_line: str
    cols: Optional[List[str]]
    children: List["DisplayNode"]

def _parse_cols_from_line(line: str) -> Optional[List[str]]:
    """
    Best-effort: extract bracketed schema-like segments and parse column names.
    Works for many formats, but remains heuristic by design.
    """
    # common: "... [a:Int32, b:Utf8]" or "... [a, b]"
    m = re.search(r"\[([^\]]+)\]", line)
    if not m:
        return None
    body = m.group(1)
    parts = [p.strip() for p in body.split(",") if p.strip()]
    cols = []
    for p in parts:
        # "a:Int32" -> "a"
        col = p.split(":")[0].strip()
        # sometimes "t.a" appears; keep last segment for required-col set
        cols.append(col.split(".")[-1])
    return cols or None

def parse_display_tree(indent_text: str) -> DisplayNode:
    """
    Parse DataFusion indent output into a tree by indentation.
    Assumes "one operator per line" with leading spaces.
    """
    lines = [ln.rstrip("\n") for ln in indent_text.splitlines() if ln.strip()]
    stack: List[Tuple[int, DisplayNode]] = []
    root = DisplayNode(variant="ROOT", raw_line="", cols=None, children=[])

    for ln in lines:
        indent = len(ln) - len(ln.lstrip(" "))
        raw = ln.strip()
        variant = raw.split(":", 1)[0] if ":" in raw else raw.split(" ", 1)[0]
        node = DisplayNode(variant=variant, raw_line=raw, cols=_parse_cols_from_line(raw), children=[])

        while stack and stack[-1][0] >= indent:
            stack.pop()
        parent = stack[-1][1] if stack else root
        parent.children.append(node)
        stack.append((indent, node))

    return root

def align_schema_to_lp(nodes: Dict[str, LPNode], indent_plan: str, indent_schema: str) -> Dict[str, Set[str]]:
    """
    Best-effort alignment:
      - parse display_indent() tree and display_indent_schema() tree
      - align by pre-order position + variant name
    """
    plan_tree = parse_display_tree(indent_plan)
    schema_tree = parse_display_tree(indent_schema)

    def preorder(n: DisplayNode) -> List[DisplayNode]:
        out = []
        for c in n.children:
            out.append(c)
            out.extend(preorder(c))
        return out

    plan_list = preorder(plan_tree)
    schema_list = preorder(schema_tree)

    # build a schema list in the same walk order; if lengths mismatch, align by index min
    k = min(len(plan_list), len(schema_list))

    # also build LP preorder list from our structural traversal
    # (note: nodes dict insertion order is traversal order in build_lp_tree above)
    lp_list = list(nodes.values())

    m = min(len(lp_list), k)
    schema_by_lp: Dict[str, Set[str]] = {}
    for i in range(m):
        lp = lp_list[i]
        sc = schema_list[i]
        if sc.cols:
            schema_by_lp[lp.node_id] = set(sc.cols)
    return schema_by_lp

# ------------------------------
# Required-column propagation
# ------------------------------
def propagate_required_cols(nodes: Dict[str, LPNode], root_id: str, schema_by_lp: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
    """
    Returns required_out per node_id.
    """
    required_out: Dict[str, Set[str]] = {}

    def rec(node_id: str, need: Optional[Set[str]]):
        n = nodes[node_id]
        v = n.v
        variant = n.variant
        out_cols = schema_by_lp.get(node_id, set())
        # If need is None, treat as "all output columns" (conservative)
        need_set = set(out_cols) if need is None else set(need)
        required_out[node_id] = need_set

        # unary helper
        def child_cols(child_id: str) -> Set[str]:
            return schema_by_lp.get(child_id, set())

        if variant == "TableScan":
            return

        if variant == "Filter":
            child = n.children[0]
            pred = getattr(v, "predicate", None)
            used = cols_used(pred, candidate_cols=child_cols(child))
            rec(child, need_set | used)
            return

        if variant == "Projection":
            child = n.children[0]
            exprs = getattr(v, "expr", []) or []
            used: Set[str] = set()
            # NOTE: ideally filter exprs by which outputs are needed; without output->expr mapping
            # in Python bindings, we conservatively union all used cols.
            for e in exprs:
                used |= cols_used(e, candidate_cols=child_cols(child))
            rec(child, used)
            return

        if variant == "Aggregate":
            child = n.children[0]
            gexpr = getattr(v, "group_expr", []) or []
            aexpr = getattr(v, "aggr_expr", []) or []
            used: Set[str] = set()
            for e in list(gexpr) + list(aexpr):
                used |= cols_used(e, candidate_cols=child_cols(child))
            rec(child, used)
            return

        if variant == "Join":
            left, right = n.children
            lcols = child_cols(left)
            rcols = child_cols(right)
            # join keys
            used_l: Set[str] = set()
            used_r: Set[str] = set()
            for pair in getattr(v, "on", []) or []:
                if isinstance(pair, (tuple, list)) and len(pair) == 2:
                    used_l |= cols_used(pair[0], candidate_cols=lcols)
                    used_r |= cols_used(pair[1], candidate_cols=rcols)
            jf = getattr(v, "filter", None)
            if jf is not None:
                # split is hard; conservatively add to both sides
                used_l |= cols_used(jf, candidate_cols=lcols)
                used_r |= cols_used(jf, candidate_cols=rcols)
            # provenance split is best-effort; default conservatively requests both sides
            rec(left, (need_set & lcols) | used_l if lcols else None)
            rec(right, (need_set & rcols) | used_r if rcols else None)
            return

        if variant == "Sort":
            child = n.children[0]
            used = set()
            for se in getattr(v, "expr", []) or []:
                # DataFusion Python exposes SortExpr.expr() per API docs :contentReference[oaicite:51]{index=51}
                try:
                    e = se.expr()
                except Exception:
                    e = getattr(se, "expr", None) or se
                used |= cols_used(e, candidate_cols=child_cols(child))
            rec(child, need_set | used)
            return

        if variant == "Limit":
            child = n.children[0]
            used = set()
            used |= cols_used(getattr(v, "skip", None), candidate_cols=child_cols(child))
            used |= cols_used(getattr(v, "fetch", None), candidate_cols=child_cols(child))
            rec(child, need_set | used)
            return

        if variant == "Repartition":
            child = n.children[0]
            used = set()
            ps = getattr(v, "partitioning_scheme", None)
            # Partitioning can contain expressions (Hash / DistributeBy) :contentReference[oaicite:52]{index=52}
            if ps is not None:
                try:
                    pv = ps  # python binding may already be structured
                    # fallback: string parse
                    used |= _tokens(str(pv)) & child_cols(child)
                except Exception:
                    pass
            rec(child, need_set | used)
            return

        if variant == "Union":
            for child in n.children:
                rec(child, need_set)
            return

        if variant == "Distinct":
            # Distinct is enum; python may expose inner via attributes; fallback: recurse all children
            for child in n.children:
                rec(child, need_set)
            return

        if variant == "SubqueryAlias":
            child = n.children[0]
            rec(child, need_set)
            return

        if variant == "Unnest":
            child = n.children[0]
            # if dependency_indices is accessible, use it; else pass-through
            rec(child, None)
            return

        # default: propagate to children conservatively
        for child in n.children:
            rec(child, need_set if need is not None else None)

    rec(root_id, None)
    return required_out
```

**What makes this “production hard” (and why the above is shaped this way):**

* Many DataFusion logical structs are `#[non_exhaustive]` in Rust (Projection / Filter / Aggregate / SubqueryAlias, etc.), meaning new fields may appear across versions; you must code with wildcard/defensive attribute access. ([Docs.rs][5])
* Physical planning can absorb filters into scans (`DataSourceExec` absorbing filters; `FilterExec` may disappear), so logical vs physical predicate placement will differ and must be stored as separate overlays. ([Docs.rs][25])

---

# 4) Physical DataSourceExec parser + mapping file_groups → logical scans

## 4.1 The physical “truth” you can parse

DataFusion’s explain guidance shows physical scan nodes like:

`DataSourceExec: file_groups={...}, projection=[...], predicate=..., file_type=parquet` ([Apache DataFusion][26])

You can obtain a comparable indented string via `ExecutionPlan.display_indent()` (Python). ([Apache DataFusion][1])

## 4.2 Parsing DataSourceExec robustly (bracket-aware)

```python
_BR_OPEN = {"{": "}", "[": "]", "(": ")"}
_BR_CLOSE = {v: k for k, v in _BR_OPEN.items()}

def split_top_level_commas(s: str) -> List[str]:
    out, buf = [], []
    stack: List[str] = []
    for ch in s:
        if ch in _BR_OPEN:
            stack.append(ch)
        elif ch in _BR_CLOSE and stack and stack[-1] == _BR_CLOSE[ch]:
            stack.pop()
        if ch == "," and not stack:
            out.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    if buf:
        out.append("".join(buf).strip())
    return out

def parse_kv_props(s: str) -> Dict[str, str]:
    props: Dict[str, str] = {}
    for part in split_top_level_commas(s):
        if "=" in part:
            k, v = part.split("=", 1)
            props[k.strip()] = v.strip()
    return props

def parse_physical_datasourceexecs(physical_indent: str) -> List[Dict[str, Any]]:
    recs = []
    for ln in physical_indent.splitlines():
        indent = len(ln) - len(ln.lstrip(" "))
        txt = ln.strip()
        if not txt.startswith("DataSourceExec:"):
            continue
        rest = txt[len("DataSourceExec:"):].strip()
        props = parse_kv_props(rest)

        proj = None
        if "projection" in props and props["projection"].startswith("["):
            inner = props["projection"].strip()[1:-1]
            proj = [c.strip() for c in inner.split(",") if c.strip()]

        recs.append({
            "indent": indent,
            "file_groups_raw": props.get("file_groups"),
            "projection": proj,
            "predicate_raw": props.get("predicate"),
            "file_type": props.get("file_type"),
            "raw": txt,
        })
    return recs
```

## 4.3 Mapping physical scans to logical scans

You typically map on a **dataset identity resolver**:

* Logical `TableScan.table_name` often resembles a file/table identifier. ([Docs.rs][4])
* Physical `DataSourceExec.file_groups` includes file paths like `hits.parquet:0..923...` ([Apache DataFusion][26])

**Best-effort mapping rules (works well in practice):**

1. Build candidate aliases for each logical scan dataset:

   * `dataset.name`
   * basename of `dataset.name` if it looks path-like
2. For each DataSourceExec record:

   * extract basenames from `file_groups_raw` via regex `r"([^/\\s\\]]+\\.parquet|[^/\\s\\]]+\\.csv|...)"` (format-dependent)
   * match to exactly one logical scan alias → map; if multiple, mark ambiguous and keep raw.

---

# 5) Putting it together: `extract_lineage(df)` end-to-end

```python
class DatasetResolver:
    def dataset_from_table_ref(self, table_ref: str) -> DatasetKey:
        # simplest: treat as "table" unless it looks like a file
        s = str(table_ref)
        if "/" in s or s.endswith((".parquet", ".csv", ".json")):
            return DatasetKey(kind="file", name=s)
        return DatasetKey(kind="table", name=s)

def extract_lineage(df, resolver: DatasetResolver) -> LineageReport:
    report = LineageReport()

    # ---- logical ----
    lp = df.optimized_logical_plan()
    nodes, indent_plan, indent_schema = build_lp_tree(lp)
    report.logical_indent = indent_plan
    report.logical_indent_schema = indent_schema

    schema_by_lp = align_schema_to_lp(nodes, indent_plan, indent_schema)
    root_id = "lp0"  # from build_lp_tree traversal
    required_out = propagate_required_cols(nodes, root_id, schema_by_lp)

    # walk nodes and emit records
    for node in nodes.values():
        v = node.v
        variant = node.variant

        if variant == "TableScan":
            ds = resolver.dataset_from_table_ref(getattr(v, "table_name", ""))
            scan = ScanKey(plan_node_id=node.node_id, dataset=ds)

            pushed = [_expr_canon(e) for e in (getattr(v, "filters", []) or [])]
            rec = LogicalScanRec(scan=scan, pushed_filters=pushed)

            # computed required cols
            rec.required_cols_computed = sorted(required_out.get(node.node_id, set())) or None

            report.scans.append(rec)

        elif variant == "Filter":
            report.filters.append(LogicalFilterRec(
                plan_node_id=node.node_id,
                predicate=_expr_canon(getattr(v, "predicate", None))
            ))

        elif variant == "Projection":
            exprs = [_expr_canon(e) for e in (getattr(v, "expr", []) or [])]
            report.projects.append(LogicalProjectRec(plan_node_id=node.node_id, exprs=exprs))

        elif variant == "Join":
            on_pairs = []
            for pair in (getattr(v, "on", []) or []):
                if isinstance(pair, (tuple, list)) and len(pair) == 2:
                    on_pairs.append((_expr_canon(pair[0]), _expr_canon(pair[1])))
            jf = getattr(v, "filter", None)
            report.joins.append(LogicalJoinRec(
                plan_node_id=node.node_id,
                join_type=str(getattr(v, "join_type", "")),
                on_pairs=on_pairs,
                non_equi_filter=_expr_canon(jf) if jf is not None else None,
            ))

        elif variant == "Aggregate":
            report.aggregates.append(LogicalAggregateRec(
                plan_node_id=node.node_id,
                group_exprs=[_expr_canon(e) for e in (getattr(v, "group_expr", []) or [])],
                aggr_exprs=[_expr_canon(e) for e in (getattr(v, "aggr_expr", []) or [])],
            ))

        # DML / DDL: typically appear as variant objects too; handle if present
        # DmlStatement fields in Rust: table_name, op, input, ... :contentReference[oaicite:59]{index=59}
        if variant == "DmlStatement":
            ds = resolver.dataset_from_table_ref(getattr(v, "table_name", ""))
            report.dmls.append(DmlRec(
                plan_node_id=node.node_id,
                target=ds,
                op=str(getattr(v, "op", "")),
                inputs=[],
            ))

        if variant == "DdlStatement":
            # DdlStatement enum lists variants + has name()/inputs() :contentReference[oaicite:60]{index=60}
            ddl_kind = getattr(v, "name", lambda: type(v).__name__)()
            report.ddls.append(DdlRec(plan_node_id=node.node_id, ddl_kind=str(ddl_kind)))

    # ---- physical hints ----
    pp = df.execution_plan()
    report.physical_indent = pp.display_indent()
    ds_execs = parse_physical_datasourceexecs(report.physical_indent)

    # map to logical scans (best-effort)
    # Here: simplest mapping uses basename matching against dataset names
    logical_by_basename: Dict[str, DatasetKey] = {}
    for srec in report.scans:
        name = srec.scan.dataset.name
        base = name.split("/")[-1]
        logical_by_basename[base] = srec.scan.dataset

    for rec in ds_execs:
        fg = rec.get("file_groups_raw") or ""
        # grab something like "hits.parquet" out of file_groups
        m = re.search(r"([^/\s\]]+\.(?:parquet|csv|json|avro))", fg)
        ds = logical_by_basename.get(m.group(1), DatasetKey(kind="unknown", name="unknown")) if m else DatasetKey(kind="unknown", name="unknown")
        report.physical_scans.append(PhysicalScanHint(
            dataset=ds,
            file_groups_raw=rec.get("file_groups_raw"),
            projection=rec.get("projection"),
            predicate_raw=rec.get("predicate_raw"),
            partition_count=pp.partition_count,  # ExecutionPlan.partition_count :contentReference[oaicite:61]{index=61}
        ))

    return report
```

---

# 6) Golden fixtures + operational checklist (what to persist for scheduling)

For every scheduled “task expression” (query / DataFrame), persist:

1. `optimized_logical_plan.display_indent()` and `.display_indent_schema()` ([Apache DataFusion][1])
2. `execution_plan.display_indent()` + `partition_count` ([Apache DataFusion][1])
3. Optional: SQL `EXPLAIN [ANALYZE] [VERBOSE] [FORMAT indent]` output for stable snapshots; DataFusion documents EXPLAIN syntax and format behavior. ([Apache DataFusion][27])

If you tune explain config, note config keys like `datafusion.explain.format` and `datafusion.explain.show_schema`. ([Apache DataFusion][28])

---

## The “why this is enough” for orchestration

* Logical extraction gives **stable dataset deps + required columns + semantic predicates**.
* Physical extraction gives **file_groups** (parallel work units) and confirms **what got pushed into scans**. ([Apache DataFusion][26])
* You can treat physical hints as advisory (they can vary by hardware/data), consistent with DataFusion’s own explain guidance. ([Apache DataFusion][26])

---


[1]: https://datafusion.apache.org/python/autoapi/datafusion/plan/index.html "datafusion.plan — Apache Arrow DataFusion  documentation"
[2]: https://datafusion.apache.org/python/autoapi/datafusion/expr/index.html "datafusion.expr — Apache Arrow DataFusion  documentation"
[3]: https://docs.rs/datafusion/latest/datafusion/?utm_source=chatgpt.com "datafusion - Rust"
[4]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/struct.TableScan.html "TableScan in datafusion_expr::logical_plan - Rust"
[5]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/struct.Projection.html "Projection in datafusion_expr::logical_plan - Rust"
[6]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/struct.Filter.html "Filter in datafusion_expr::logical_plan - Rust"
[7]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/struct.Join.html "Join in datafusion_expr::logical_plan - Rust"
[8]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/struct.Aggregate.html "Aggregate in datafusion_expr::logical_plan - Rust"
[9]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/struct.Window.html?utm_source=chatgpt.com "Window in datafusion_expr::logical_plan - Rust"
[10]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/struct.Sort.html "Sort in datafusion_expr::logical_plan - Rust"
[11]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/struct.Limit.html "Limit in datafusion_expr::logical_plan - Rust"
[12]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/struct.SubqueryAlias.html "SubqueryAlias in datafusion_expr::logical_plan - Rust"
[13]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/struct.Union.html "Union in datafusion_expr::logical_plan - Rust"
[14]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.Distinct.html "Distinct in datafusion_expr::logical_plan - Rust"
[15]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/struct.DistinctOn.html "DistinctOn in datafusion_expr::logical_plan - Rust"
[16]: https://docs.rs/datafusion/latest/datafusion/logical_expr/logical_plan/struct.Repartition.html "Repartition in datafusion::logical_expr::logical_plan - Rust"
[17]: https://docs.rs/datafusion/latest/datafusion/logical_expr/logical_plan/enum.Partitioning.html?utm_source=chatgpt.com "Partitioning in datafusion::logical_expr::logical_plan - Rust"
[18]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Unnest.html "Unnest in datafusion::logical_expr - Rust"
[19]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.DmlStatement.html "DmlStatement in datafusion::logical_expr - Rust"
[20]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.DdlStatement.html "DdlStatement in datafusion::logical_expr - Rust"
[21]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.CreateView.html?utm_source=chatgpt.com "CreateView in datafusion::logical_expr - Rust"
[22]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/struct.CreateExternalTable.html?utm_source=chatgpt.com "CreateExternalTable in datafusion_expr::logical_plan - Rust"
[23]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.DropView.html?utm_source=chatgpt.com "DropView in datafusion::logical_expr - Rust"
[24]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/struct.DropCatalogSchema.html?utm_source=chatgpt.com "DropCatalogSchema in datafusion_expr::logical_plan - Rust"
[25]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html?utm_source=chatgpt.com "ExecutionPlan in datafusion::physical_plan - Rust"
[26]: https://datafusion.apache.org/user-guide/explain-usage.html "Reading Explain Plans — Apache DataFusion  documentation"
[27]: https://datafusion.apache.org/user-guide/sql/explain.html?utm_source=chatgpt.com "EXPLAIN — Apache DataFusion documentation"
[28]: https://datafusion.apache.org/user-guide/configs.html?utm_source=chatgpt.com "Configuration Settings — Apache DataFusion documentation"

Below are the three remaining deep dives, written as if you’re handing them to a **Python-expert LLM programming agent** building an orchestration layer. I’m going to treat **Ibis IR** as the semantic DAG, **DataFusion logical** as the execution-real relational algebra DAG, and **DataFusion physical** as *execution hints* (file groups / partitions / pushdowns).

---

# 2) Ibis IR traversal & column lineage (required columns per upstream input)

## 2.1 What you’re traversing: `Expr` vs `Node`

Ibis’s user-facing objects are **expressions** (`Expr`) that are a thin abstraction carrying type/shape, and the actual computation graph is expressed by **operation nodes** (`Node` subclasses) defined mostly in `ibis.expr.operations`. ([Ibis][1])
Ibis explicitly frames the execution flow as “user builds expression → backend rewrites → compile → execute”, making IR traversal a first-class “plan-time” activity. ([Ibis][1])

Ibis also states directly that expressions are inherently represented as a tree of `Expr` and `Node` objects, and provides both `to_graph(expr)` and a stable pretty-printed textual representation showing `UnboundTable`, `Filter`, `Project`, etc. ([Ibis][2])

## 2.2 Canonicalization primitives: `unbind()` and `equals()`

These two are the “make IR usable for orchestration” APIs:

* `Table.unbind()` **rebuilds the expression on `UnboundTable`** rather than backend-bound objects, with docs showing executing an unbound expression on a *different* backend. ([Ibis][3])
  **Orchestration role:** canonicalize dataset identity so your lineage doesn’t depend on “duckdb table handle vs polars table handle” object identity.

* `Table.equals(other)` checks **structural equivalence** of expressions (not value equality) and is documented explicitly as such, with examples. ([Ibis][3])
  **Orchestration role:** dedupe/CSE at the IR layer (“these two tasks are the same plan”), and regression tests (“did my refactor preserve structure?”).

**Recommended canonicalization pipeline (Ibis-side):**

```python
def canonical_ibis(expr):
    # 1) normalize backend bindings
    expr_u = expr.unbind()  # backend-independent graph :contentReference[oaicite:5]{index=5}

    # 2) compute a stable identity / fingerprint:
    # - use expr_u.equals(other) for equivalence checks :contentReference[oaicite:6]{index=6}
    # - store repr(expr_u) / to_graph(expr_u) as debug artifact :contentReference[oaicite:7]{index=7}
    return expr_u
```

## 2.3 Traversal API choices: “supported-ish” vs “internal but powerful”

### Option A — “Supported-ish” structural artifacts (safe fallback)

* `to_graph(expr)` produces a dependency graph intended to show edges encode dependencies. ([Ibis][4])
* Pretty-print (`repr(expr)`) yields a plan-like textual IR (“r0 := UnboundTable … r1 := Filter … Project …”). ([Ibis][2])

This is great for **golden snapshots** and **human diffs**, but not ideal as your only machine-readable lineage source.

### Option B — `expr.op().args` (high-fidelity; treat as unstable internal API)

In practice, the most direct way to traverse the operation DAG is to drop from `Expr` to the underlying op via `expr.op()` and then inspect its inputs via `.args`. This pattern is used by Ibis maintainers/examples when compiling custom ops: `value, json_path = expr.op().args`. ([GitHub][5])
However, maintainers also note internal operations APIs have been unstable (they even removed docs about extending ops due to instability). ([GitHub][5])

**Practical stance for orchestration:**
Use `expr.op().args` for best results, but **wrap in strict version gates + fallbacks** (Option A or SQLGlot lineage after compilation).

## 2.4 Goal: “required columns per upstream input” from Ibis IR

You want an artifact like:

```json
{
  "sources": {
    "catalog.db.tableA": {"required_cols": ["x","y"], "predicates":[...], "reason": {...}},
    "catalog.db.tableB": {"required_cols": ["id"], "predicates":[...]}
  },
  "output": {"cols": [...], "computed_cols": {...}}
}
```

### 2.4.1 The minimal semantic model

At the IR layer you can view lineage as:

* **Base relations**: `UnboundTable` / backend tables / `memtable`
* **Row filters**: `Filter` nodes: predicate references columns
* **Projections**: `Project` / `Select` / `Mutate`: define output columns as expressions over upstream columns
* **Joins**: combine two relations with join predicates; provenance split matters
* **Aggregations / Windows**: “uses” include group keys + window keys + aggregate inputs

The “Does Ibis understand SQL?” post shows the core operators in the pretty IR (`UnboundTable`, `Filter`, `Project`) and is a good mental anchor for how Ibis expresses plan structure. ([Ibis][2])

### 2.4.2 Required-columns algorithm (Ibis semantic)

Use a two-pass approach (mirrors what you already do with SQLGlot/DataFusion):

#### Pass 1: build an expression dependency index

For every node:

* record upstream child nodes (from `.args`)
* record whether this node introduces output columns (projection-like)
* record predicates

#### Pass 2: propagate “needed outputs” top-down

Start from:

* columns requested by the “final consumer” (often all output cols or a subset)
* plus any columns referenced by predicates / order-by / join keys

Propagate:

* Projection-like nodes: needed inputs = columns referenced by the expressions that produce the needed outputs
* Filter-like nodes: needed inputs = needed outputs ∪ columns referenced by predicate
* Aggregate-like nodes: needed inputs = cols(group keys ∪ needed aggregates)
* Join-like nodes: split by provenance; always include join keys

### 2.4.3 Implementation skeleton (internal API + fallback artifacts)

```python
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, Set, Tuple, Iterable

@dataclass
class SourceUse:
    required_cols: Set[str] = field(default_factory=set)
    predicates: list[str] = field(default_factory=list)

def ibis_required_cols(expr) -> Dict[str, SourceUse]:
    expr = expr.unbind()  # canonicalize backend bindings :contentReference[oaicite:13]{index=13}

    # fallback debug artifact: stable-ish textual IR
    debug_ir = repr(expr)  # aligns with the documented pretty IR example :contentReference[oaicite:14]{index=14}

    uses: Dict[str, SourceUse] = {}

    def visit(e) -> Any:
        # best-effort op access
        try:
            op = e.op()  # used in practice; internal :contentReference[oaicite:15]{index=15}
        except Exception:
            return

        # conservative: traverse args and collect "column name" strings from Expr repr
        for a in getattr(op, "args", ()):
            if hasattr(a, "op"):
                visit(a)

        # TODO: pattern match op class name to extract:
        # - base table identity and column refs
        # - predicates
        # - projection expressions (output->input cols)
        #
        # You can also use debug_ir + to_graph(expr) for correlation / golden tests :contentReference[oaicite:16]{index=16}

    visit(expr)
    return uses
```

**Reality check / footgun:** because Ibis’s op APIs can change, your production-grade implementation should also store `debug_ir` (and optionally `to_graph`) as a *verifiable trace*, and optionally cross-check against SQLGlot lineage after compile (Ibis compiles via SQLGlot in modern versions). ([Ibis][2])

---

# 4) File-level scheduling in DataFusion (file_groups + scan projections + pushdown predicates)

This is where you turn a query into *parallel scan work units*.

## 4.1 Where the signals live: EXPLAIN + physical plan

DataFusion’s “Reading Explain Plans” doc shows the exact fields you want:

* Logical `TableScan` includes:

  * `projection=[...]` (column pruning)
  * `partial_filters=[...]` (pushed filters) ([Apache DataFusion][6])
* Physical `DataSourceExec` includes:

  * `file_groups={...}` (grouped file ranges)
  * `projection=[...]`
  * `predicate=...` (what is applied at scan)
  * `file_type=...` ([Apache DataFusion][6])

The same doc explicitly states physical plans incorporate hardware configuration and data organization and can differ across environments—so treat these as **execution hints** for scheduling. ([Apache DataFusion][6])

## 4.2 `register_listing_table` is the “directory of files as one table” primitive

From the Python `SessionContext` API:

`register_listing_table(name, path, table_partition_cols=None, file_extension='.parquet', schema=None, file_sort_order=None)`:

* registers **multiple files as a single table**
* can assemble files from locations in an `ObjectStore`
* supports **partition columns** and **file sort order**. ([Apache DataFusion][7])

This changes the scheduling surface from “one file scan” to “scan a table that expands into many files and produces `file_groups` in the physical plan.”

Contrast with:

* `register_parquet(... parquet_pruning=True, skip_metadata=True, ...)` — registers parquet, and exposes parquet pruning and metadata skipping knobs. ([Apache DataFusion][7])

## 4.3 Practical scheduling recipe: map logical scans → physical file_groups

### Step 1: ensure scans are file-backed (or listing-backed)

If you want file-level scheduling, prefer:

* `register_listing_table("t", "/dataset/prefix", file_extension=".parquet", table_partition_cols=[("p","int32"), ...])` ([Apache DataFusion][7])
  rather than ad-hoc `FROM 'hits.parquet'` paths scattered in SQL (still works, but harder to normalize identity).

### Step 2: use EXPLAIN FORMAT INDENT as your stable extraction fixture

`EXPLAIN [ANALYZE] [VERBOSE] [FORMAT format]` is documented; `VERBOSE` only supports `indent`. ([Apache DataFusion][8])
Also, explain-related config options exist (format, show_schema, show_statistics, show_sizes). ([Apache DataFusion][9])

### Step 3: extract three overlays per scan

From the explain example, you can extract:

1. **required columns** (logical `projection=[...]`) ([Apache DataFusion][6])
2. **pushdown predicates** (logical `partial_filters=[...]` and physical `predicate=...`) ([Apache DataFusion][6])
3. **file work units** (`DataSourceExec file_groups={...}`) ([Apache DataFusion][6])

### Step 4: schedule work units

DataFusion describes that “partitions” in the plan are run independently (roughly one core per partition), and certain operators exchange data between partitions. ([Apache DataFusion][6])
Your scheduler can:

* treat each `file_group` as a scan task
* fan out by group count
* respect `partition_count` (if you use `ExecutionPlan.partition_count`) as an upper bound for parallelism (or as a hint when deciding chunk size) ([Apache DataFusion][10])

## 4.4 DataSourceExec semantics: why file_groups are trustworthy scheduling units

DataFusion’s `DataSourceExec` is explicitly an `ExecutionPlan` that reads one or more files, and it implements common functionality such as applying projections and caching plan properties. ([Docs.rs][11])
That’s exactly what you want: a physical leaf operator whose internal grouping is “how DataFusion intends to scan the data.”

---

# 5) Plan fingerprinting + persistence (proto limitations vs Substrait)

You need fingerprinting/persistence for:

* cache reuse (don’t recompute same plan)
* distributed orchestration handoff (send “plan spec” to workers)
* determinism (stable identity across restarts)

## 5.1 DataFusion `to_proto()` / `from_proto()` (Python): powerful but limited

The DataFusion Python plan module provides:

* `LogicalPlan.to_proto()` / `LogicalPlan.from_proto(ctx, bytes)`
* `ExecutionPlan.to_proto()` / `ExecutionPlan.from_proto(ctx, bytes)`

…but with a hard limitation:

> “Tables created in memory from record batches are currently not supported.” ([Apache DataFusion][10])

This matters if you use:

* `register_record_batches(...)` (in-memory table) ([Apache DataFusion][7])
* `SessionContext.create_dataframe(partitions=...)` (RecordBatch partitions) ([Apache DataFusion][7])

**Operational implication:** `to_proto()` can’t be your only persistence artifact unless you constrain sources to file/table providers that are serializable.

## 5.2 DataFusion-proto (Rust) compatibility reality (important even in Python systems)

The `datafusion-proto` crate explicitly states:

* it serializes LogicalPlans (including Expr) and ExecutionPlans (including PhysicalExpr)
* **serialized forms are not guaranteed compatible across DataFusion versions**
* it is DataFusion-specific, and points to Substrait for a standard encoding. ([Docs.rs][12])

Even if you’re in Python, this is the correct mental model: **protobuf plan bytes are “engine-internal” artifacts**.

## 5.3 Substrait via `datafusion.substrait`: the better “distributed orchestration artifact”

DataFusion Python exposes a full Substrait module with:

* `Producer.to_substrait_plan(logical_plan, ctx) -> Plan`
* `Plan.encode() -> bytes`
* `Consumer.from_substrait_plan(ctx, plan) -> LogicalPlan`
* `Serde.serialize_bytes(sql, ctx) -> bytes` and deserialize variants ([Apache DataFusion][13])

**When Substrait is the better artifact**

* You need *cross-language* or *cross-process* interchange with a standards-based payload.
* You want a plan fingerprint that is less tied to DataFusion’s internal protobuf schema.
* You want to treat SQL as an input but store a structured representation (`serde.serialize_bytes(sql, ctx)`). ([Apache DataFusion][13])

**Important boundary:** Substrait support is strongest for *logical plans*. Physical plan Substrait serialization is explicitly “still under development” / incomplete in upstream discussions. ([GitHub][14])

So: use Substrait bytes primarily as your **logical plan** cache key / interchange artifact, and treat physical plans as runtime hints (regenerate per environment).

## 5.4 Recommended “PlanBundle fingerprint” strategy

Use **multi-layer fingerprints**, each with a clear purpose:

1. **Semantic fingerprint (Ibis)**

   * `expr_u = expr.unbind()`; store `repr(expr_u)`; structural dedupe via `expr_u.equals(other)` ([Ibis][3])

2. **Engine-logical fingerprint (DataFusion)**

   * Prefer Substrait bytes (`Serde.serialize_bytes(sql, ctx)` or `Producer.to_substrait_plan(plan).encode()`) ([Apache DataFusion][13])
   * Optionally also store DataFusion `LogicalPlan.to_proto()` bytes when sources permit; but do **not** assume version stability. ([Apache DataFusion][10])

3. **Execution-hint fingerprint (DataFusion physical)**

   * Store `ExecutionPlan.display_indent()` as a debugging signature, not as a cache key (physical can vary with hardware / file layout). ([Apache DataFusion][6])

---

## If you want the next step

The most “agent-ready” next increment is a **unified `LineageAndSchedulingBundle` schema** that merges:

* Ibis semantic required-cols per source
* DataFusion logical scan projection + partial_filters
* DataFusion physical file_groups + predicate
* fingerprints: ibis_structural + substrait_bytes (+ optional df_proto)

…and then a golden-test harness that snapshots:

* `repr(ibis_expr.unbind())` ([Ibis][2])
* `EXPLAIN FORMAT INDENT ...` output (with `datafusion.explain.show_schema=true`) ([Apache DataFusion][8])

[1]: https://ibis-project.org/concepts/internals "internals – Ibis"
[2]: https://ibis-project.org/posts/does-ibis-understand-sql/ "Does Ibis understand SQL? – Ibis"
[3]: https://ibis-project.org/reference/expression-tables "expression-tables – Ibis"
[4]: https://ibis-project.org/how-to/visualization/graphs "graphs – Ibis"
[5]: https://github.com/ibis-project/ibis/discussions/8774 "How to extend Backends with non-scalar functions · ibis-project ibis · Discussion #8774 · GitHub"
[6]: https://datafusion.apache.org/user-guide/explain-usage.html "Reading Explain Plans — Apache DataFusion  documentation"
[7]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[8]: https://datafusion.apache.org/user-guide/sql/explain.html "EXPLAIN — Apache DataFusion  documentation"
[9]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[10]: https://datafusion.apache.org/python/autoapi/datafusion/plan/index.html "datafusion.plan — Apache Arrow DataFusion  documentation"
[11]: https://docs.rs/datafusion/latest/datafusion/datasource/memory/struct.DataSourceExec.html?utm_source=chatgpt.com "DataSourceExec in datafusion::datasource::memory - Rust"
[12]: https://docs.rs/datafusion-proto/latest/datafusion_proto/?search=&utm_source=chatgpt.com "\"\" Search - Rust"
[13]: https://datafusion.apache.org/python/autoapi/datafusion/substrait/index.html "datafusion.substrait — Apache Arrow DataFusion  documentation"
[14]: https://github.com/apache/datafusion/issues/9299 "Can't serialize example `ExecutionPlan` to substrait · Issue #9299 · apache/datafusion · GitHub"

Below are the three remaining deep dives, written as if you’re handing them to a **Python-expert LLM programming agent** building an orchestration layer. I’m going to treat **Ibis IR** as the semantic DAG, **DataFusion logical** as the execution-real relational algebra DAG, and **DataFusion physical** as *execution hints* (file groups / partitions / pushdowns).

---

# 2) Ibis IR traversal & column lineage (required columns per upstream input)

## 2.1 What you’re traversing: `Expr` vs `Node`

Ibis’s user-facing objects are **expressions** (`Expr`) that are a thin abstraction carrying type/shape, and the actual computation graph is expressed by **operation nodes** (`Node` subclasses) defined mostly in `ibis.expr.operations`. ([Ibis][1])
Ibis explicitly frames the execution flow as “user builds expression → backend rewrites → compile → execute”, making IR traversal a first-class “plan-time” activity. ([Ibis][1])

Ibis also states directly that expressions are inherently represented as a tree of `Expr` and `Node` objects, and provides both `to_graph(expr)` and a stable pretty-printed textual representation showing `UnboundTable`, `Filter`, `Project`, etc. ([Ibis][2])

## 2.2 Canonicalization primitives: `unbind()` and `equals()`

These two are the “make IR usable for orchestration” APIs:

* `Table.unbind()` **rebuilds the expression on `UnboundTable`** rather than backend-bound objects, with docs showing executing an unbound expression on a *different* backend. ([Ibis][3])
  **Orchestration role:** canonicalize dataset identity so your lineage doesn’t depend on “duckdb table handle vs polars table handle” object identity.

* `Table.equals(other)` checks **structural equivalence** of expressions (not value equality) and is documented explicitly as such, with examples. ([Ibis][3])
  **Orchestration role:** dedupe/CSE at the IR layer (“these two tasks are the same plan”), and regression tests (“did my refactor preserve structure?”).

**Recommended canonicalization pipeline (Ibis-side):**

```python
def canonical_ibis(expr):
    # 1) normalize backend bindings
    expr_u = expr.unbind()  # backend-independent graph :contentReference[oaicite:5]{index=5}

    # 2) compute a stable identity / fingerprint:
    # - use expr_u.equals(other) for equivalence checks :contentReference[oaicite:6]{index=6}
    # - store repr(expr_u) / to_graph(expr_u) as debug artifact :contentReference[oaicite:7]{index=7}
    return expr_u
```

## 2.3 Traversal API choices: “supported-ish” vs “internal but powerful”

### Option A — “Supported-ish” structural artifacts (safe fallback)

* `to_graph(expr)` produces a dependency graph intended to show edges encode dependencies. ([Ibis][4])
* Pretty-print (`repr(expr)`) yields a plan-like textual IR (“r0 := UnboundTable … r1 := Filter … Project …”). ([Ibis][2])

This is great for **golden snapshots** and **human diffs**, but not ideal as your only machine-readable lineage source.

### Option B — `expr.op().args` (high-fidelity; treat as unstable internal API)

In practice, the most direct way to traverse the operation DAG is to drop from `Expr` to the underlying op via `expr.op()` and then inspect its inputs via `.args`. This pattern is used by Ibis maintainers/examples when compiling custom ops: `value, json_path = expr.op().args`. ([GitHub][5])
However, maintainers also note internal operations APIs have been unstable (they even removed docs about extending ops due to instability). ([GitHub][5])

**Practical stance for orchestration:**
Use `expr.op().args` for best results, but **wrap in strict version gates + fallbacks** (Option A or SQLGlot lineage after compilation).

## 2.4 Goal: “required columns per upstream input” from Ibis IR

You want an artifact like:

```json
{
  "sources": {
    "catalog.db.tableA": {"required_cols": ["x","y"], "predicates":[...], "reason": {...}},
    "catalog.db.tableB": {"required_cols": ["id"], "predicates":[...]}
  },
  "output": {"cols": [...], "computed_cols": {...}}
}
```

### 2.4.1 The minimal semantic model

At the IR layer you can view lineage as:

* **Base relations**: `UnboundTable` / backend tables / `memtable`
* **Row filters**: `Filter` nodes: predicate references columns
* **Projections**: `Project` / `Select` / `Mutate`: define output columns as expressions over upstream columns
* **Joins**: combine two relations with join predicates; provenance split matters
* **Aggregations / Windows**: “uses” include group keys + window keys + aggregate inputs

The “Does Ibis understand SQL?” post shows the core operators in the pretty IR (`UnboundTable`, `Filter`, `Project`) and is a good mental anchor for how Ibis expresses plan structure. ([Ibis][2])

### 2.4.2 Required-columns algorithm (Ibis semantic)

Use a two-pass approach (mirrors what you already do with SQLGlot/DataFusion):

#### Pass 1: build an expression dependency index

For every node:

* record upstream child nodes (from `.args`)
* record whether this node introduces output columns (projection-like)
* record predicates

#### Pass 2: propagate “needed outputs” top-down

Start from:

* columns requested by the “final consumer” (often all output cols or a subset)
* plus any columns referenced by predicates / order-by / join keys

Propagate:

* Projection-like nodes: needed inputs = columns referenced by the expressions that produce the needed outputs
* Filter-like nodes: needed inputs = needed outputs ∪ columns referenced by predicate
* Aggregate-like nodes: needed inputs = cols(group keys ∪ needed aggregates)
* Join-like nodes: split by provenance; always include join keys

### 2.4.3 Implementation skeleton (internal API + fallback artifacts)

```python
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, Set, Tuple, Iterable

@dataclass
class SourceUse:
    required_cols: Set[str] = field(default_factory=set)
    predicates: list[str] = field(default_factory=list)

def ibis_required_cols(expr) -> Dict[str, SourceUse]:
    expr = expr.unbind()  # canonicalize backend bindings :contentReference[oaicite:13]{index=13}

    # fallback debug artifact: stable-ish textual IR
    debug_ir = repr(expr)  # aligns with the documented pretty IR example :contentReference[oaicite:14]{index=14}

    uses: Dict[str, SourceUse] = {}

    def visit(e) -> Any:
        # best-effort op access
        try:
            op = e.op()  # used in practice; internal :contentReference[oaicite:15]{index=15}
        except Exception:
            return

        # conservative: traverse args and collect "column name" strings from Expr repr
        for a in getattr(op, "args", ()):
            if hasattr(a, "op"):
                visit(a)

        # TODO: pattern match op class name to extract:
        # - base table identity and column refs
        # - predicates
        # - projection expressions (output->input cols)
        #
        # You can also use debug_ir + to_graph(expr) for correlation / golden tests :contentReference[oaicite:16]{index=16}

    visit(expr)
    return uses
```

**Reality check / footgun:** because Ibis’s op APIs can change, your production-grade implementation should also store `debug_ir` (and optionally `to_graph`) as a *verifiable trace*, and optionally cross-check against SQLGlot lineage after compile (Ibis compiles via SQLGlot in modern versions). ([Ibis][2])

---

# 4) File-level scheduling in DataFusion (file_groups + scan projections + pushdown predicates)

This is where you turn a query into *parallel scan work units*.

## 4.1 Where the signals live: EXPLAIN + physical plan

DataFusion’s “Reading Explain Plans” doc shows the exact fields you want:

* Logical `TableScan` includes:

  * `projection=[...]` (column pruning)
  * `partial_filters=[...]` (pushed filters) ([Apache DataFusion][6])
* Physical `DataSourceExec` includes:

  * `file_groups={...}` (grouped file ranges)
  * `projection=[...]`
  * `predicate=...` (what is applied at scan)
  * `file_type=...` ([Apache DataFusion][6])

The same doc explicitly states physical plans incorporate hardware configuration and data organization and can differ across environments—so treat these as **execution hints** for scheduling. ([Apache DataFusion][6])

## 4.2 `register_listing_table` is the “directory of files as one table” primitive

From the Python `SessionContext` API:

`register_listing_table(name, path, table_partition_cols=None, file_extension='.parquet', schema=None, file_sort_order=None)`:

* registers **multiple files as a single table**
* can assemble files from locations in an `ObjectStore`
* supports **partition columns** and **file sort order**. ([Apache DataFusion][7])

This changes the scheduling surface from “one file scan” to “scan a table that expands into many files and produces `file_groups` in the physical plan.”

Contrast with:

* `register_parquet(... parquet_pruning=True, skip_metadata=True, ...)` — registers parquet, and exposes parquet pruning and metadata skipping knobs. ([Apache DataFusion][7])

## 4.3 Practical scheduling recipe: map logical scans → physical file_groups

### Step 1: ensure scans are file-backed (or listing-backed)

If you want file-level scheduling, prefer:

* `register_listing_table("t", "/dataset/prefix", file_extension=".parquet", table_partition_cols=[("p","int32"), ...])` ([Apache DataFusion][7])
  rather than ad-hoc `FROM 'hits.parquet'` paths scattered in SQL (still works, but harder to normalize identity).

### Step 2: use EXPLAIN FORMAT INDENT as your stable extraction fixture

`EXPLAIN [ANALYZE] [VERBOSE] [FORMAT format]` is documented; `VERBOSE` only supports `indent`. ([Apache DataFusion][8])
Also, explain-related config options exist (format, show_schema, show_statistics, show_sizes). ([Apache DataFusion][9])

### Step 3: extract three overlays per scan

From the explain example, you can extract:

1. **required columns** (logical `projection=[...]`) ([Apache DataFusion][6])
2. **pushdown predicates** (logical `partial_filters=[...]` and physical `predicate=...`) ([Apache DataFusion][6])
3. **file work units** (`DataSourceExec file_groups={...}`) ([Apache DataFusion][6])

### Step 4: schedule work units

DataFusion describes that “partitions” in the plan are run independently (roughly one core per partition), and certain operators exchange data between partitions. ([Apache DataFusion][6])
Your scheduler can:

* treat each `file_group` as a scan task
* fan out by group count
* respect `partition_count` (if you use `ExecutionPlan.partition_count`) as an upper bound for parallelism (or as a hint when deciding chunk size) ([Apache DataFusion][10])

## 4.4 DataSourceExec semantics: why file_groups are trustworthy scheduling units

DataFusion’s `DataSourceExec` is explicitly an `ExecutionPlan` that reads one or more files, and it implements common functionality such as applying projections and caching plan properties. ([Docs.rs][11])
That’s exactly what you want: a physical leaf operator whose internal grouping is “how DataFusion intends to scan the data.”

---

# 5) Plan fingerprinting + persistence (proto limitations vs Substrait)

You need fingerprinting/persistence for:

* cache reuse (don’t recompute same plan)
* distributed orchestration handoff (send “plan spec” to workers)
* determinism (stable identity across restarts)

## 5.1 DataFusion `to_proto()` / `from_proto()` (Python): powerful but limited

The DataFusion Python plan module provides:

* `LogicalPlan.to_proto()` / `LogicalPlan.from_proto(ctx, bytes)`
* `ExecutionPlan.to_proto()` / `ExecutionPlan.from_proto(ctx, bytes)`

…but with a hard limitation:

> “Tables created in memory from record batches are currently not supported.” ([Apache DataFusion][10])

This matters if you use:

* `register_record_batches(...)` (in-memory table) ([Apache DataFusion][7])
* `SessionContext.create_dataframe(partitions=...)` (RecordBatch partitions) ([Apache DataFusion][7])

**Operational implication:** `to_proto()` can’t be your only persistence artifact unless you constrain sources to file/table providers that are serializable.

## 5.2 DataFusion-proto (Rust) compatibility reality (important even in Python systems)

The `datafusion-proto` crate explicitly states:

* it serializes LogicalPlans (including Expr) and ExecutionPlans (including PhysicalExpr)
* **serialized forms are not guaranteed compatible across DataFusion versions**
* it is DataFusion-specific, and points to Substrait for a standard encoding. ([Docs.rs][12])

Even if you’re in Python, this is the correct mental model: **protobuf plan bytes are “engine-internal” artifacts**.

## 5.3 Substrait via `datafusion.substrait`: the better “distributed orchestration artifact”

DataFusion Python exposes a full Substrait module with:

* `Producer.to_substrait_plan(logical_plan, ctx) -> Plan`
* `Plan.encode() -> bytes`
* `Consumer.from_substrait_plan(ctx, plan) -> LogicalPlan`
* `Serde.serialize_bytes(sql, ctx) -> bytes` and deserialize variants ([Apache DataFusion][13])

**When Substrait is the better artifact**

* You need *cross-language* or *cross-process* interchange with a standards-based payload.
* You want a plan fingerprint that is less tied to DataFusion’s internal protobuf schema.
* You want to treat SQL as an input but store a structured representation (`serde.serialize_bytes(sql, ctx)`). ([Apache DataFusion][13])

**Important boundary:** Substrait support is strongest for *logical plans*. Physical plan Substrait serialization is explicitly “still under development” / incomplete in upstream discussions. ([GitHub][14])

So: use Substrait bytes primarily as your **logical plan** cache key / interchange artifact, and treat physical plans as runtime hints (regenerate per environment).

## 5.4 Recommended “PlanBundle fingerprint” strategy

Use **multi-layer fingerprints**, each with a clear purpose:

1. **Semantic fingerprint (Ibis)**

   * `expr_u = expr.unbind()`; store `repr(expr_u)`; structural dedupe via `expr_u.equals(other)` ([Ibis][3])

2. **Engine-logical fingerprint (DataFusion)**

   * Prefer Substrait bytes (`Serde.serialize_bytes(sql, ctx)` or `Producer.to_substrait_plan(plan).encode()`) ([Apache DataFusion][13])
   * Optionally also store DataFusion `LogicalPlan.to_proto()` bytes when sources permit; but do **not** assume version stability. ([Apache DataFusion][10])

3. **Execution-hint fingerprint (DataFusion physical)**

   * Store `ExecutionPlan.display_indent()` as a debugging signature, not as a cache key (physical can vary with hardware / file layout). ([Apache DataFusion][6])

---


[1]: https://ibis-project.org/concepts/internals "internals – Ibis"
[2]: https://ibis-project.org/posts/does-ibis-understand-sql/ "Does Ibis understand SQL? – Ibis"
[3]: https://ibis-project.org/reference/expression-tables "expression-tables – Ibis"
[4]: https://ibis-project.org/how-to/visualization/graphs "graphs – Ibis"
[5]: https://github.com/ibis-project/ibis/discussions/8774 "How to extend Backends with non-scalar functions · ibis-project ibis · Discussion #8774 · GitHub"
[6]: https://datafusion.apache.org/user-guide/explain-usage.html "Reading Explain Plans — Apache DataFusion  documentation"
[7]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[8]: https://datafusion.apache.org/user-guide/sql/explain.html "EXPLAIN — Apache DataFusion  documentation"
[9]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[10]: https://datafusion.apache.org/python/autoapi/datafusion/plan/index.html "datafusion.plan — Apache Arrow DataFusion  documentation"
[11]: https://docs.rs/datafusion/latest/datafusion/datasource/memory/struct.DataSourceExec.html?utm_source=chatgpt.com "DataSourceExec in datafusion::datasource::memory - Rust"
[12]: https://docs.rs/datafusion-proto/latest/datafusion_proto/?search=&utm_source=chatgpt.com "\"\" Search - Rust"
[13]: https://datafusion.apache.org/python/autoapi/datafusion/substrait/index.html "datafusion.substrait — Apache Arrow DataFusion  documentation"
[14]: https://github.com/apache/datafusion/issues/9299 "Can't serialize example `ExecutionPlan` to substrait · Issue #9299 · apache/datafusion · GitHub"

## Additional DataFusion planning surfaces worth wiring into an orchestration + lineage stack

### 1) Plan object inspection beyond `inputs()` / `to_variant()`

**LogicalPlan (Python) has richer rendering + serialization hooks than just `inputs()` / `to_variant()`:**

* `display_graphviz()` emits DOT for Graphviz, which is ideal for *stable-ish* visual snapshots and node/edge diff tooling (vs parsing indent text). ([Apache DataFusion][1])
* `display_indent()` and `display_indent_schema()` remain the most reliable “string fixtures” for golden tests and schema-oracle fallbacks. ([Apache DataFusion][1])
* `to_proto()` / `from_proto(ctx, bytes)` exist for both logical and physical plans, but **do not support tables created in memory from record batches** (this limitation is called out on the Python plan types). ([Apache DataFusion][1])

**ExecutionPlan (Python) similarly exposes:**

* `children()` (structural traversal), `display_indent()` (text fixture), and `partition_count` (physical parallelism hint). ([Apache DataFusion][1])

> Orchestration consequence: treat plan objects as providing **(a)** a traversal API, **(b)** multiple orthogonal renderers (`indent`, `graphviz`, `tree` via EXPLAIN), and **(c)** best-effort serialization with clear source-type constraints.

---

### 2) DataFrame lifecycle APIs that influence “planning-as-a-service”

A lot of “planning capability” in DataFusion is actually surfaced on the **DataFrame** object (because it *is* a plan builder).

Key methods you’ll likely want to snapshot/call from an orchestrator:

* `logical_plan()`, `optimized_logical_plan()`, `execution_plan()` are explicit first-class accessors. ([Apache Arrow][2])
* `explain(verbose=False, analyze=False)` exists (DataFrame-level alternative to SQL EXPLAIN). ([Apache Arrow][2])
* `cache()` inserts a cache boundary (important when you’re doing multi-step orchestration and want explicit reuse boundaries). ([Apache Arrow][2])
* `collect_partitioned()` preserves input partitioning on collection—useful when you want “same partitioning shape” diagnostics to line up with file_groups / partitions. ([Apache Arrow][2])
* Streaming surfaces exist:

  * `execute_stream()` / `execute_stream_partitioned()` (streaming execution)
  * `__arrow_c_stream__()` exports an Arrow C Stream and explicitly states it executes using streaming APIs and supports only *simple projections* when a requested schema is supplied (subset/reorder only; no computed expressions/renames). ([Apache DataFusion][3])

> Orchestration pattern: if your scheduler needs “don’t materialize everything”, treat DataFusion DataFrames as **streamable plans** and use `execute_stream*` / `__arrow_c_stream__()` as a first-class execution mode, not as an afterthought. ([Apache DataFusion][3])

---

### 3) EXPLAIN is a configurable planning API (not just debugging)

DataFusion’s docs explicitly position EXPLAIN as a way to see plans without running the query and point to DataFrame::explain as the programmatic equivalent. ([Apache DataFusion][4])

**SQL EXPLAIN surface area:**

* Syntax: `EXPLAIN [ANALYZE] [VERBOSE] [FORMAT format] statement` ([Apache DataFusion][5])
* `EXPLAIN VERBOSE` only supports `indent` format. ([Apache DataFusion][5])
* `FORMAT tree` exists and is described as modeled after DuckDB plans (often easier to diff structurally than indent). ([Apache DataFusion][5])
* `EXPLAIN ANALYZE` metrics detail can be controlled by `datafusion.explain.analyze_level` and per-partition metrics are available via `EXPLAIN ANALYZE VERBOSE`. ([Apache DataFusion][5])

**Session-level controls for EXPLAIN output** (high leverage for golden fixtures and orchestration metadata):

* `datafusion.explain.logical_plan_only` / `physical_plan_only` (limit output) ([Apache DataFusion][6])
* `datafusion.explain.show_statistics`, `show_sizes`, `show_schema` ([Apache DataFusion][6])
* `datafusion.explain.format` (`indent` vs `tree`) + `tree_maximum_render_width` ([Apache DataFusion][6])
* `datafusion.explain.analyze_level` (`summary` vs `dev`) ([Apache DataFusion][6])

**Important semantic boundary:** DataFusion’s “Reading Explain Plans” guide emphasizes the physical plan depends on hardware and data organization (files/CPUs) and thus can differ across environments—so treat physical plan artifacts as *execution hints*, not semantic truth. ([Apache DataFusion][4])

---

### 4) SessionContext / SessionConfig knobs that materially change plan shape

Python docs show `SessionContext` takes a `SessionConfig` and a `RuntimeEnvBuilder`, and demonstrate toggles that directly affect planning outcomes (catalog/schema defaults, target partitions, information schema, repartitioning, parquet pruning, and arbitrary config keys like parquet filter pushdown). ([Apache DataFusion][7])

Example knobs that are “planning-relevant” (not just performance):

* `with_target_partitions(N)` and repartition toggles (joins/aggs/windows) can insert/remove repartition operators and change physical partition counts. ([Apache DataFusion][7])
* `with_information_schema(True)` enables metadata tables/SHOW commands used for catalog-driven orchestration. ([Apache DataFusion][7])
* Parquet pruning / pushdown flags directly change scan nodes and what becomes `partial_filters` / scan predicates. ([Apache DataFusion][7])

At the “global config” level, there are optimizer and execution options that *change pushdown behavior and thus your scheduling surface*, such as enabling dynamic filter pushdown, sort pushdown, and various join/agg repartitioning behaviors. ([Apache DataFusion][6])

---

### 5) SQL parser + source-span capture (for deterministic canonicalization and provenance)

If you’re canonicalizing plans and want to map lineage back to SQL locations, the config surface includes:

* `datafusion.sql_parser.dialect` (Generic, Postgres, DuckDB, etc.) ([Apache DataFusion][6])
* `datafusion.sql_parser.enable_ident_normalization` (identifier case normalization) ([Apache DataFusion][6])
* `datafusion.sql_parser.collect_spans` to record source locations (“Span”) in logical plan nodes ([Apache DataFusion][6])
* Recursion limit and type parsing toggles (`parse_float_as_decimal`, etc.) that can change expression typing / AST shape. ([Apache DataFusion][6])

> For orchestration: “collect spans” is the missing glue between (a) plan nodes and (b) user-authored SQL, making it much easier to generate *actionable* lineage diagnostics that point back to the exact predicate/projection text. ([Apache DataFusion][6])

---

### 6) Catalog + metadata planning primitives: `information_schema` and SHOW commands

DataFusion supports ISO `information_schema` views and DataFusion-specific commands like `SHOW TABLES`, `SHOW COLUMNS`, `SHOW ALL`, and `SHOW FUNCTIONS`. ([Apache DataFusion][8])

Practical orchestration uses:

* `information_schema.tables` / `SHOW TABLES` for dataset discovery and dependency validation ([Apache DataFusion][8])
* `information_schema.columns` / `SHOW COLUMNS` for schema contracts and required-column validation ([Apache DataFusion][8])
* `information_schema.df_settings` / `SHOW ALL` to snapshot the *effective session planning config* as part of your plan fingerprint bundle ([Apache DataFusion][8])

---

### 7) Prepared statements + parameterized queries: plan reuse as a first-class planning feature

DataFusion SQL supports `PREPARE` / `EXECUTE` with typed or inferred parameters, explicitly described as enabling repeated execution efficiently. ([Apache DataFusion][9])

DataFusion Python adds **parameterized queries** with named placeholders (introduced in DataFusion-Python 51.0.0), explains string conversion caveats, and documents `param_values` for preserving exact Python-object values; it also notes DataFrame parameters may register temporary views in the SessionContext and warns about sessions needing temp-view support. ([Apache DataFusion][10])

> For orchestration: prepared statements / parameterized queries are the “official” way to separate **plan shape** from **runtime values**, which is exactly what you want for stable plan caching keys and incremental recomputation. ([Apache DataFusion][9])

---

### 8) Programmatic plan construction + plan rewriting ecosystems (often overlooked)

#### 8.1 Building logical plans directly (Rust-level, but defines the conceptual model)

DataFusion documents:

* `LogicalPlan` is an enum of operator variants and includes an `Extension` variant for custom logical operators. ([Apache DataFusion][11])
* `LogicalPlanBuilder` is the recommended way to programmatically build plans; the DataFrame API delegates to it. ([Apache DataFusion][11])
* Logical plans must be compiled to physical plans (ExecutionPlan), e.g. via `SessionState::create_physical_plan` (Rust). ([Apache DataFusion][11])

#### 8.2 Query optimizer as a plan transformation pipeline

The Query Optimizer docs emphasize:

* an extensive set of `OptimizerRule` and `PhysicalOptimizerRule` can rewrite plans/expressions ([Apache DataFusion][12])
* the optimizer can be run with an observer that sees the plan after each rule (great for generating “why did this schedule change?” traces). ([Apache DataFusion][12])
* expression naming has two canonical forms: `display_name` for schema naming and `canonical_name` for equivalence checks. ([Apache DataFusion][12])
* the TreeNode API is the recommended way (Rust-side) to recursively walk expressions/plans and find subqueries/joins; conceptually important even if you reimplement similar traversal in Python. ([Apache DataFusion][12])

#### 8.3 Extending SQL planning (custom operators/types/relations)

DataFusion documents a dedicated extension system where extension planners intercept SQL AST during `SqlToRel`:

* `ExprPlanner` (custom expressions/operators), `TypePlanner` (custom SQL types), `RelationPlanner` (custom FROM elements), including registration methods and planner precedence rules. ([Apache DataFusion][13])

#### 8.4 Extending operators via optimizer rules (logical and physical)

The “Extending Operators” guide explicitly frames operator extension as transforming `LogicalPlan` and `ExecutionPlan` using customized optimizer rules, and gives an example of rewriting a logical plan into a `LogicalPlan::TableScan` backed by a `MemTable`. ([Apache DataFusion][14])

---

### 9) TableProvider scan semantics (pushdown truth table matters for scheduling)

The Custom Table Provider guide is explicit about scan-time contracts:

* `TableProvider::scan` is “likely the most important” method and returns an `ExecutionPlan`. ([Apache DataFusion][15])
* `supports_filters_pushdown` can return per-filter pushdown classifications:

  * `Unsupported`, `Exact`, `Inexact`, and DataFusion will re-apply `Inexact` filters after the scan for correctness. ([Apache DataFusion][15])
* `scan` receives `projection`, `filters`, and `limit`—all of which are explicit pushdown opportunities. ([Apache DataFusion][15])
* The guide also points at `ListingTableProvider`, `FileFormat`, and `FileOpener` as the abstraction set for file-backed providers (directly relevant to file-group scheduling and custom pruning indexes). ([Apache DataFusion][15])

> Orchestration consequence: your “pushdown predicate” model should be *three-valued* (Unsupported / Exact / Inexact) rather than boolean, because `Inexact` implies **residual correctness filters remain above scan** even if you see scan-level predicate application. ([Apache DataFusion][15])

---

### 10) File-level scheduling: don’t ignore COPY/CREATE EXTERNAL TABLE options

Two often-missed planning surfaces that affect file scheduling:

1. **EXPLAIN outputs encode file work units** (`DataSourceExec` lines show file/range groupings; see Reading Explain Plans for how file counts/ranges appear and how physical plans depend on file organization). ([Apache DataFusion][4])
2. **Write paths introduce partition-parallel file emission**: the Format Options guide notes that when writing via `COPY ... PARTITIONED BY (...)`, DataFusion writes one Parquet file in parallel “for each partition in the query,” and options precedence can come from CREATE EXTERNAL TABLE / COPY options / session defaults. ([Apache DataFusion][16])

If you schedule “materialization steps” (writes) as tasks, you’ll want to snapshot:

* the *write* partitioning shape (logical + physical partitions),
* and the resolved format options (session defaults vs statement overrides). ([Apache DataFusion][16])

---

### 11) Plan interchange + canonicalization: Substrait + Unparser are first-class Python modules

**Substrait (Python)**

* `datafusion.substrait` provides:

  * `Serde.serialize_bytes()` / `deserialize_bytes()`
  * `Producer.to_substrait_plan(logical_plan, ctx)` + `Plan.encode()`
  * `Consumer.from_substrait_plan(ctx, plan)` ([Apache DataFusion][17])
* Substrait itself is defined as a format for describing compute operations on structured data designed for interoperability. ([Substrait][18])

**Unparser (Python)**

* `datafusion.unparser` provides `Unparser.plan_to_sql(plan)` plus selectable dialects and pretty-printing. ([Apache DataFusion][19])

> Orchestration implication: you can build a robust “plan bundle” with three orthogonal canonical forms:
>
> * LogicalPlan (structured) → Substrait bytes (portable fingerprint)
> * LogicalPlan (structured) → SQL (diffable, SQLGlot-friendly)
> * LogicalPlan/ExecutionPlan → indent/tree/graphviz renderings (golden fixtures + debugging)

Also note: Python plan protobuf serialization (`to_proto`) exists but is explicitly constrained by in-memory record-batch sources. ([Apache DataFusion][1])
And even at the Rust crate level, upgrade guides show serde API shifts (e.g., datafusion-proto physical plan serde expecting TaskContext rather than SessionContext), which is a practical reminder that protobuf-based persistence is more brittle than Substrait + SQL snapshots. ([Apache DataFusion][20])

---

## Practical “next wiring” checklist for your orchestrator

If you’re extending your current approach, the highest-impact additional signals to add to your bundle are:

1. **Graphviz DOT** for logical plans (`LogicalPlan.display_graphviz`) alongside indent fixtures. ([Apache DataFusion][1])
2. **EXPLAIN config snapshot** from `information_schema.df_settings` / `SHOW ALL` embedded in every plan bundle (so changes in pushdown flags explain scheduling changes). ([Apache DataFusion][8])
3. **EXPLAIN tree format** for structural diffs + `EXPLAIN VERBOSE` (indent-only) for full intermediate plans when debugging optimizer-driven rewrites. ([Apache DataFusion][5])
4. **Substrait bytes** as your primary plan fingerprint (`Serde.serialize_bytes` or `Producer.to_substrait_plan(...).encode()`) + Unparser SQL for human diffing. ([Apache DataFusion][17])
5. **Pushdown truth model** (Unsupported/Exact/Inexact) from provider capabilities, so file-level scheduling doesn’t over-assume scan-level predicates imply correctness. ([Apache DataFusion][15])
6. **Prepared/parameterized query surfaces** for stable plan reuse: PREPARE/EXECUTE and Python named parameters with `param_values` when type fidelity matters. ([Apache DataFusion][9])

[1]: https://datafusion.apache.org/python/autoapi/datafusion/plan/index.html "datafusion.plan — Apache Arrow DataFusion  documentation"
[2]: https://arrow.staged.apache.org/datafusion-python/generated/datafusion.DataFrame.html "datafusion.DataFrame — Apache Arrow DataFusion  documentation"
[3]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[4]: https://datafusion.apache.org/user-guide/explain-usage.html "Reading Explain Plans — Apache DataFusion  documentation"
[5]: https://datafusion.apache.org/user-guide/sql/explain.html "EXPLAIN — Apache DataFusion  documentation"
[6]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[7]: https://datafusion.apache.org/python/user-guide/configuration.html "Configuration — Apache Arrow DataFusion  documentation"
[8]: https://datafusion.apache.org/user-guide/sql/information_schema.html "Information Schema — Apache DataFusion  documentation"
[9]: https://datafusion.apache.org/user-guide/sql/prepared_statements.html "Prepared Statements — Apache DataFusion  documentation"
[10]: https://datafusion.apache.org/python/user-guide/sql.html "SQL — Apache Arrow DataFusion  documentation"
[11]: https://datafusion.apache.org/library-user-guide/building-logical-plans.html "Building Logical Plans — Apache DataFusion  documentation"
[12]: https://datafusion.apache.org/library-user-guide/query-optimizer.html "Query Optimizer — Apache DataFusion  documentation"
[13]: https://datafusion.apache.org/library-user-guide/extending-sql.html "Extending SQL Syntax — Apache DataFusion  documentation"
[14]: https://datafusion.apache.org/library-user-guide/extending-operators.html "Extending Operators — Apache DataFusion  documentation"
[15]: https://datafusion.apache.org/library-user-guide/custom-table-providers.html "Custom Table Provider — Apache DataFusion  documentation"
[16]: https://datafusion.apache.org/user-guide/sql/format_options.html "Format Options — Apache DataFusion  documentation"
[17]: https://datafusion.apache.org/python/autoapi/datafusion/substrait/index.html "datafusion.substrait — Apache Arrow DataFusion  documentation"
[18]: https://substrait.io/?utm_source=chatgpt.com "Home - Substrait: Cross-Language Serialization for Relational ..."
[19]: https://datafusion.apache.org/python/autoapi/datafusion/unparser/index.html "datafusion.unparser — Apache Arrow DataFusion  documentation"
[20]: https://datafusion.apache.org/library-user-guide/upgrading.html?utm_source=chatgpt.com "Upgrade Guides — Apache DataFusion documentation"

## Additional DataFusion planning capabilities worth modeling explicitly

What follows is a “second-pass” deep dive over DataFusion’s planning surface area—focused on **APIs and behaviors that materially affect plan inspection, lineage extraction, scheduling, caching, and distributed execution**, and that are easy to miss if you only wire `optimized_logical_plan()/to_variant()/inputs()` + `execution_plan()/children()`.

---

### 1) Plan presentation formats that remove the need to parse `indent` strings

If you’re still primarily scraping `display_indent()` / `EXPLAIN FORMAT INDENT`, DataFusion now offers **structured (or at least more machine-friendly) plan outputs** you can treat as first-class artifacts.

#### 1.1 SQL `EXPLAIN FORMAT` now has multiple “serialization-like” formats

The SQL docs enumerate several formats beyond `indent`/`tree`, including:

* `tree` (high-level structure)
* `indent` (one line per operator; includes both logical + physical)
* `pgjson` (Postgres JSON–modeled; designed to work with existing plan visualization tools like dalibo)
* `graphviz` (DOT language output) ([Apache DataFusion][1])

Also note:

* `EXPLAIN ANALYZE` only supports `indent` format, and `EXPLAIN VERBOSE` only supports `indent` format. ([Apache DataFusion][1])
* When you omit `FORMAT`, the format comes from `datafusion.explain.format`, so **your harness should always set the config or specify `FORMAT`** to avoid drift. ([Apache DataFusion][1])

**Why you should care:** `pgjson` is effectively “plan-as-JSON” without needing to reverse-engineer `indent` text, and `graphviz` is stable enough for visual diffs and topology checks.

#### 1.2 Rust-level plan renderers that matter even in Python systems

On the Rust `LogicalPlan`, there are dedicated display helpers:

* `display_pg_json()` and `display_graphviz()` exist as explicit renderers (and describe their intended usage). ([Docs.rs][2])

Even if Python bindings don’t surface every renderer directly, the SQL `EXPLAIN FORMAT PGJSON/GRAPHVIZ` route gives you those artifacts in a portable way. ([Apache DataFusion][1])

**Practical recommendation:**
In your plan bundle, store **both**:

* `EXPLAIN FORMAT PGJSON …` output (for machine parsing / tooling)
* `EXPLAIN FORMAT GRAPHVIZ …` output (for visual diffs / debugging)

---

### 2) Plan provenance and deterministic mapping back to SQL text

If you’re generating scheduling diagnostics (“this filter caused pruning”, “this join introduced a dynamic filter”), it’s far more useful when you can map plan fragments back to the originating SQL span.

#### 2.1 Source-span capture

There is a config option:

* `datafusion.sql_parser.collect_spans`: when enabled, DataFusion collects and records source locations (“Span”) into logical plan nodes. ([Apache DataFusion][3])

**Why it matters for orchestration:** you can attach “why” diagnostics to the precise sub-expression location, not just a stringified predicate.

#### 2.2 SQL parser normalization knobs that affect canonicalization

Also in config:

* dialect selection (`datafusion.sql_parser.dialect`)
* identifier normalization (`enable_ident_normalization`)
* recursion limit, and type-mapping flags ([Apache DataFusion][3])

If you fingerprint plans based on SQL text, these options can change AST/plan shape and must be captured in your plan bundle (e.g., `SHOW ALL` / `information_schema.df_settings` snapshots—if you use them).

---

### 3) SQL ↔ Plan round-tripping as a planning capability (not just debugging)

#### 3.1 Python unparser module (`datafusion.unparser`)

DataFusion Python exposes `Unparser.plan_to_sql(plan)` and dialect helpers (`Dialect.postgres()`, `Dialect.duckdb()`, etc.), plus `with_pretty()`. ([Apache DataFusion][4])

This lets you:

* extract logical plans structurally,
* modify/normalize them,
* then **emit canonical SQL** for SQLGlot lineage and/or human diffs.

#### 3.2 “Plan to SQL” is a strategic capability (federation + pushdown)

The DataFusion 40.0.0 release highlights converting `Expr` and `LogicalPlan` back to SQL as a core feature, explicitly noting use cases like predicate pushdown into other systems and query generation. ([Apache DataFusion][5])

**Orchestration payoff:** your canonical expression normalizer can choose from:

* DataFusion `Expr.canonical_name()` (engine-native stable strings)
* DataFusion `plan_to_sql()` (diffable + SQLGlot-compatible)
* Substrait bytes (portable structured fingerprint)

---

### 4) Plan reuse is an explicit first-class planning feature (PREPARE + Python named params)

#### 4.1 SQL prepared statements (`PREPARE` / `EXECUTE`)

DataFusion documents prepared statements and shows:

* typed parameters,
* inferred types,
* positional arguments with multiple params,
* repeated execution “in an efficient manner.” ([Apache DataFusion][6])

This is a planning surface because it separates **plan shape** from **runtime values**, enabling stable plan caching keys and repeatable execution.

#### 4.2 Python “parameterized queries” (named placeholders)

DataFusion Python adds named parameters (introduced in 51.0.0), with important behaviors:

* Python objects are converted to string representations
* DataFrame parameters are special-cased by registering a temporary view with a generated name
* Named placeholders don’t work for some SQL dialects (`hive`, `mysql`). ([Apache DataFusion][7])

**Orchestration implication:**
If you accept `ctx.sql(query, **params)` in your scheduling API, you must treat:

* parameter payloads,
* any temp-view registrations,
* and the configured dialect
  as part of the plan bundle / fingerprint story.

---

### 5) Plans as *objects* that can be created, injected, and executed partition-by-partition

This is one of the most important “planning capability clusters” for orchestration and distributed execution.

#### 5.1 Construct DataFrames from existing plans

Python `SessionContext` supports:

* `create_dataframe_from_logical_plan(plan)` → DataFrame from an existing `LogicalPlan`. ([Apache DataFusion][8])

This is the missing glue for “plan on coordinator, execute later” pipelines (especially when combined with Substrait).

#### 5.2 Execute physical plans directly (partitioned execution)

Python `SessionContext.execute(plan, partitions)` executes an `ExecutionPlan` and returns a record batch stream. ([Apache DataFusion][8])

This is effectively a “manual scheduler hook”: you can choose *which partitions* to run and when.

#### 5.3 DataFrame streaming execution

The Python DataFrame exposes:

* `execute_stream()` (single-partition stream)
* `execute_stream_partitioned()` (one stream per partition)
* `explain(verbose, analyze)` which can run the plan and report metrics ([Apache DataFusion][9])

**Orchestration payoff:** you can align your scheduler’s “work units” with physical partitions and stream results without forcing a full collect.

---

### 6) File-level scheduling is deeper than `file_groups` parsing

Most orchestrators stop at “parse `DataSourceExec.file_groups`”. DataFusion’s file planning stack has additional primitives that can change how file groups are formed and how pruning happens.

#### 6.1 FileGroup-aware splitting and repartitioning based on statistics (Rust-level, scheduling-relevant)

`FileScanConfig` provides helpers to **split file groups using min/max statistics** to enable parallelism while maintaining sort order and non-overlap constraints. ([Docs.rs][10])

This matters because your “file groups” are not just a static property—they can be transformed by target-partition settings and statistics-aware grouping.

Also note the `DataSource` trait on `FileScanConfig` supports repartitioning files by size (`repartitioned(...)`), which interacts with configs like `target_partitions` and `repartition_file_min_size`. ([Docs.rs][10])

#### 6.2 Dynamic filters change scan predicates *during execution*

DataFusion’s dynamic filters (blogged in 2025) show that:

* `DataSourceExec.predicate` can contain a placeholder (`true`) prior to execution,
* and is updated during execution (e.g., TopK / join-derived filters), enabling additional file/row skipping beyond static plan inspection. ([Apache DataFusion][11])

**Scheduling implication:**
A “static plan bundle” can’t fully predict scan pruning. Your scheduler should treat dynamic-filter-enabled scans as “late-binding”: still schedule file groups, but expect effective pruning to evolve during execution.

#### 6.3 Low-level Parquet pruning and user-supplied access plans

The DataFusion 40.0.0 release notes “Low Level APIs for Fast Parquet Access (indexing)” and introduces `ParquetAccessPlan` as a way for users to supply pruning info to skip decoding parts of files—explicitly positioned as useful alongside index information and object-store access optimization. ([Apache DataFusion][5])

This is highly relevant if you’re building external indexes (or CPG-like metadata stores) and want scan-time pruning beyond what the optimizer infers.

#### 6.4 Built-in metadata inspection for Parquet (CLI-level function, operationally useful)

The CLI ships a `parquet_metadata` table function that exposes row-group stats and sizes for Parquet files. ([Apache DataFusion][12])

Even if it’s “CLI-specific”, the concept is important: DataFusion has a first-class notion of exposing Parquet metadata for debugging and design decisions. If you build file-level schedulers, you can use the same metadata ideas (stats_min/max, row counts, compressed sizes) as inputs to grouping and pruning decisions.

---

### 7) Custom catalogs and schemas in Python and Rust (planning surface, not just metadata)

DataFusion Python explicitly supports **Catalogs written in Python or Rust**, with guidance on:

* implementing `CatalogProvider` in Python,
* exporting a Rust catalog via PyO3,
* and the subtlety that Python access may return the original Python object rather than a wrapper around a Rust wrapper. ([Apache DataFusion][13])

For orchestration:

* dataset identity resolution (`catalog.schema.table`) becomes a first-class mapping problem,
* and catalog-provider behavior must be captured in your plan bundle (especially if it influences table resolution, view expansion, and function availability).

---

### 8) Plan persistence and interchange: Substrait is “the portable layer”, proto is “best-effort”

#### 8.1 LogicalPlan / ExecutionPlan proto serialization limitations (Python)

Python plan docs are explicit:

* `LogicalPlan.to_proto()` / `from_proto(ctx, bytes)` and similar exist
* but **tables created in memory from record batches are currently not supported**. ([Apache DataFusion][14])

So proto persistence is not universally applicable if your workloads include in-memory tables (`create_dataframe`, `register_record_batches`, etc.).

#### 8.2 Substrait in Python

DataFusion Python supports serializing and deserializing query plans in Substrait format (noted as a feature) ([PyPI][15]), and exposes `datafusion.substrait` APIs (e.g., `Serde.serialize_bytes(sql, ctx)` in earlier work).

**Recommendation for plan bundles:**

* primary fingerprint: Substrait bytes hash
* secondary: canonical SQL (via unparser) + SQLGlot AST hash
* debug: `EXPLAIN FORMAT PGJSON` + `FORMAT GRAPHVIZ` snapshots
* best-effort: DF proto hashes when sources permit

---

## Concrete “completeness upgrades” to your existing PlanBundle

If you already store `(logical_indent, physical_indent, file_groups, substrait_hash, ibis_unbound_repr)`, the most meaningful additions are:

1. **`EXPLAIN FORMAT PGJSON`** snapshot as your primary machine-friendly plan extraction (no indent parsing). ([Apache DataFusion][1])
2. **`EXPLAIN FORMAT GRAPHVIZ`** snapshot for topology diffs / visual debugging. ([Apache DataFusion][1])
3. **Session-level config snapshot** of `datafusion.explain.*` and `datafusion.sql_parser.*` (spans/dialect/normalization), because they change plan output and canonicalization behavior. ([Apache DataFusion][3])
4. **Prepared/parameterized-query metadata**: record whether you used SQL `PREPARE`/`EXECUTE` or Python named params and whether DataFrame params created temp views. ([Apache DataFusion][6])
5. **Dynamic-filter awareness**: tag scans where runtime predicate updates are expected (don’t over-interpret the static predicate in `DataSourceExec`). ([Apache DataFusion][11])
6. **File-group shaping factors**: capture `target_partitions`, `repartition_file_min_size`, and whether file statistics collection is enabled for the listing provider (these affect group formation). ([Docs.rs][10])
7. If you’re building external indexes: model `ParquetAccessPlan`-style “user-supplied pruning plans” as a first-class input to scan scheduling. ([Apache DataFusion][5])


[1]: https://datafusion.apache.org/user-guide/sql/explain.html "EXPLAIN — Apache DataFusion  documentation"
[2]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html "LogicalPlan in datafusion_expr::logical_plan - Rust"
[3]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[4]: https://datafusion.apache.org/python/autoapi/datafusion/unparser/index.html "datafusion.unparser — Apache Arrow DataFusion  documentation"
[5]: https://datafusion.apache.org/blog/2024/07/24/datafusion-40.0.0/ "Apache DataFusion 40.0.0 Released - Apache DataFusion Blog"
[6]: https://datafusion.apache.org/user-guide/sql/prepared_statements.html "Prepared Statements — Apache DataFusion  documentation"
[7]: https://datafusion.apache.org/python/user-guide/sql.html "SQL — Apache Arrow DataFusion  documentation"
[8]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[9]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[10]: https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.FileScanConfig.html "FileScanConfig in datafusion::datasource::physical_plan - Rust"
[11]: https://datafusion.apache.org/blog/2025/09/10/dynamic-filters/ "Dynamic Filters: Passing Information Between Operators During Execution for 25x Faster Queries - Apache DataFusion Blog"
[12]: https://datafusion.apache.org/user-guide/cli/functions.html "CLI Specific Functions — Apache DataFusion  documentation"
[13]: https://datafusion.apache.org/python/user-guide/data-sources.html "Data Sources — Apache Arrow DataFusion  documentation"
[14]: https://datafusion.apache.org/python/autoapi/datafusion/plan/index.html "datafusion.plan — Apache Arrow DataFusion  documentation"
[15]: https://pypi.org/project/datafusion/25.0.0/ "datafusion · PyPI"
