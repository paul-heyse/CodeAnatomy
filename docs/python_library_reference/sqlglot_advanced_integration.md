# `sqlglot-advanced.md` — SQLGlot as compiler IR for qualification, normalization, lineage, and semantic diffs

> SQLGlot is a no-dependency SQL parser, transpiler, optimizer, and engine, with an emphasis on multi-dialect parsing/generation, AST inspection/transforms, and “optimizer as canonicalizer.” ([GitHub][1])
> Install note: `pip install "sqlglot[rs]"` enables the Rust tokenizer (faster); plain `pip install sqlglot` is supported but slower. ([GitHub][1])

---

## 0) Scope and invariants

### 0.1 SQLGlot’s role in your system

In your architecture, SQLGlot is the **compiler IR layer** that sits between:

* “relational intent” (Ibis IR / rule DSL) and
* “execution truth” (DataFusion SQL + planner behavior)

You use SQLGlot for:

* **analysis IR**: extract metadata, required columns, lineage, scopes
* **rewrite engine**: enforce deterministic SQL shapes and work around backend quirks
* **canonicalizer**: standardize semantically equivalent queries into a stable form for caching and incremental rebuild decisions

SQLGlot explicitly supports traversal and programmatic SQL building, and it provides an optimizer that rewrites SQL into a canonical AST. ([GitHub][1])

### 0.2 Dialect strategy

SQLGlot’s core rule: you must be explicit about **input dialect** and **output dialect**.

* For parsing, if you omit the dialect, SQLGlot uses the “SQLGlot dialect,” intended as a superset of supported dialects; omitting the true source dialect is a common reason parsing fails. ([GitHub][1])
* For generation/transpilation, if you don’t specify the target dialect, SQLGlot will generate in the default dialect, which may not match your engine. ([GitHub][1])

**Policy you should standardize:**

* Every parse must pass `read=<dialect>` or `dialect=<dialect>`
* Every generate must pass `write=<dialect>` or `dialect=<dialect>`
* Your build artifacts must record: `(read_dialect, write_dialect, identify, pretty, unsupported_level)` since these affect output stability and correctness ([GitHub][1])

### 0.3 Deterministic AST contract for caching and incremental rebuild

Your deterministic contract should be: **equivalent semantics → identical canonical AST → identical fingerprint**.

SQLGlot’s own recommended path for meaning-aware comparisons is: parse → canonicalize via optimizer rules → then compare. The “Are these SQL queries the same?” post shows canonicalization steps like expanding `*`, qualifying columns, simplifying boolean logic, and CNF conversion. ([Tobiko Data][2])

---

## 1) Mental model: parse → AST → transforms → generate

### 1.1 Parsing entrypoints

**Primary entrypoints**

* `sqlglot.parse_one(sql, dialect=...)` parses a single statement to an AST
* `sqlglot.parse(sql, dialect=...)` parses a SQL string into multiple AST nodes (statements)
* `sqlglot.transpile(sql, read=..., write=..., ...)` parses + rewrites + generates SQL in one shot ([GitHub][1])

**Parse errors**

* Parser errors raise `ParseError`. SQLGlot exposes structured syntax errors via `ParseError.errors`, including line/column and highlighted context. ([GitHub][1])

```python
import sqlglot
import sqlglot.errors

try:
    sqlglot.transpile("SELECT foo FROM (SELECT baz FROM t")
except sqlglot.errors.ParseError as e:
    # Structured list of parse errors (line/col/context)
    print(e.errors)
```

([GitHub][1])

**Error levels**
SQLGlot uses an `ErrorLevel` enum for “how strict to be” in various phases:

* `IGNORE`, `WARN`, `RAISE`, `IMMEDIATE` (raise on first error). ([SQLGlot][3])

This matters especially for generation/transpilation where “unsupported constructs” may either warn or raise depending on your enforcement policy. ([GitHub][1])

### 1.2 Expression tree model

An AST is a tree of `Expression` nodes. You can introspect it with:

* `repr(expr)` for the structural AST form (useful for debugging and golden snapshots) ([GitHub][1])
* expression helpers (`find_all`) to locate node types (tables, columns, selects) ([GitHub][1])

Example metadata traversal from the official README:

```python
from sqlglot import parse_one, exp

# print all column references
for column in parse_one("SELECT a, b + 1 AS c FROM d").find_all(exp.Column):
    print(column.alias_or_name)

# find all tables
for table in parse_one("SELECT * FROM x JOIN y JOIN z").find_all(exp.Table):
    print(table.name)
```

([GitHub][1])

### 1.3 SQL generation and stable formatting policy

Every expression can be rendered back to SQL:

* `expr.sql(dialect=..., pretty=True|False, identify=True|False, ...)` (common pattern)
* `sqlglot.transpile(..., pretty=True)` for one-shot conversions ([GitHub][1])

**Stability policy you should standardize**

* Use `pretty=False` for fingerprints and cache keys (minimize formatting variability).
* Use `pretty=True` for reviewer artifacts and debug bundles.
* Decide whether to force identifier quoting (`identify=True`) based on your engine needs (and store this choice in artifacts). ([GitHub][1])

### 1.4 Transpilation between dialects

Two canonical patterns:

**Parse then generate**

```python
from sqlglot import parse_one

expr = parse_one(sql, dialect="spark")
print(expr.sql(dialect="duckdb"))
```

([GitHub][1])

**One-shot transpile**

```python
import sqlglot
print(sqlglot.transpile(sql, read="duckdb", write="hive")[0])
```

([GitHub][1])

**Unsupported constructs**
SQLGlot may warn and proceed by default when generating unsupported SQL for a target dialect; the behavior can be changed by setting an `unsupported_level` that can raise an exception instead. ([GitHub][1])

---

## 2) API surface index

### 2.1 Parsing and generation

* `sqlglot.parse_one`, `sqlglot.parse`, `sqlglot.transpile` ([GitHub][1])
* `Expression.sql(...)` for output rendering ([GitHub][1])
* `sqlglot.errors.ErrorLevel` for strictness policies ([SQLGlot][3])

### 2.2 Expression construction, visitors, transforms

**Programmatic query building**
SQLGlot supports incremental expression building:

```python
from sqlglot import select, condition

where = condition("x=1").and_("y=1")
print(select("*").from_("y").where(where).sql())
```

([GitHub][1])

**Modifying a parsed tree**

```python
from sqlglot import parse_one
print(parse_one("SELECT x FROM y").from_("z").sql())
```

([GitHub][1])

**Recursive transforms**

```python
from sqlglot import exp, parse_one

tree = parse_one("SELECT a FROM x")

def transformer(node):
    if isinstance(node, exp.Column) and node.name == "a":
        return parse_one("FUN(a)")
    return node

print(tree.transform(transformer).sql())
```

([GitHub][1])

### 2.3 Dialects and dialect capability flags

SQLGlot defines a base `Dialect` class intended to be as universal as possible; each SQL variation is a Dialect subclass extending Tokenizer/Parser/Generator as needed. ([SQLGlot][4])

Dialect objects expose:

* `tokenizer_class`, `parser_class`, `generator_class` and many dialect capability flags. ([SQLGlot][5])

Generator behavior knobs (representative; shown in dialect docs) include:

* indentation/padding for pretty mode
* `normalize_functions` (upper/lower/disabled)
* `unsupported_level` and `max_unsupported`
* whether to preserve comments ([SQLGlot][6])

---

## 3) Qualification as a required normalization phase

### 3.1 `qualify.qualify` semantics and inputs

`sqlglot.optimizer.qualify.qualify(...)` rewrites an AST so that tables and columns are normalized and qualified; SQLGlot states this step is necessary for further optimizations. ([SQLGlot][7])

Key parameters you should treat as policy knobs:

* `schema`: used to infer column names and types
* `expand_stars`: expands `*` projections; docs explicitly warn this is necessary for most optimizer rules and not to disable unless you know what you’re doing ([SQLGlot][7])
* `expand_alias_refs`: expands references to aliases
* `infer_schema`: infer missing schema pieces
* `allow_partial_qualification`: whether partially qualifying is allowed
* `validate_qualify_columns`: validate column resolution
* `quote_identifiers` and `identify`: quoting behavior; `quote_identifiers` is described as important for correctness in case sensitive queries, but can be deferred ([SQLGlot][7])

Canonical usage:

```python
import sqlglot
from sqlglot.optimizer.qualify import qualify

schema = {"tbl": {"col": "INT"}}
expr = sqlglot.parse_one("SELECT col FROM tbl", dialect="duckdb")

qualified = qualify(expr, schema=schema, dialect="duckdb")
print(qualified.sql(dialect="duckdb"))
```

([SQLGlot][7])

### 3.2 Schema provisioning strategies

SQLGlot’s optimizer accepts schema in multiple shapes:

* `{table: {col: type}}`
* `{db: {table: {col: type}}}`
* `{catalog: {db: {table: {col: type}}}}` ([SQLGlot][8])

You can also use the `sqlglot.schema` module to manage schema objects; the schema API supports adding tables with a column mapping, and it can normalize identifiers according to the dialect. ([SQLGlot][9])

**Static schema**

* best for determinism, caching, and correctness gating
* enables type-sensitive canonicalization (`annotate_types`, `canonicalize`)

**Hybrid schema**

* provide schema for “core tables” (your materialized products)
* allow inference for volatile/temporary tables (`infer_schema=True`), but treat it as “debug mode” since it weakens determinism ([SQLGlot][7])

### 3.3 Canonical table aliasing and quoting policies

Two separate but related tools:

**(A) `qualify.qualify(..., canonicalize_table_aliases=...)`**

* uses canonical table aliases as part of the qualification pipeline (exposed at top-level qualify). ([SQLGlot][7])

**(B) `qualify_tables` for fully qualified tables and canonical source aliases**
`qualify_tables` can:

* add db/catalog qualifiers,
* expand join constructs like `(t1 JOIN t2) AS t` into a subquery form,
* optionally replace source aliases with canonical aliases like `_0`, `_1`, ... (`canonicalize_table_aliases=True`). ([SQLGlot][10])

```python
import sqlglot
from sqlglot.optimizer.qualify_tables import qualify_tables

expr = sqlglot.parse_one("SELECT 1 FROM tbl")
print(qualify_tables(expr, db="db").sql())
```

([SQLGlot][10])

### 3.4 Qualification smoke tests for backend compatibility

A robust “smoke test” policy for your compiler boundary:

1. Parse (`dialect=read_dialect`)
2. Run `qualify` with:

   * `expand_stars=True`
   * `validate_qualify_columns=True`
   * `sql=<original_sql>` so qualification errors can include highlighted context when available ([SQLGlot][8])
3. If qualification fails:

   * treat it as a compile-time error for rulepacks (not a runtime surprise)
   * persist the failing SQL, schema snapshot, and dialect in your repro bundle

---

## 4) Identifier normalization for cache-key stability

### 4.1 `normalize_identifiers` semantics and the case-sensitive escape hatch

`normalize_identifiers(expression, dialect=..., store_original_column_identifiers=...)` normalizes identifier case (upper/lower) while preserving dialect semantics, and is described as important for AST standardization. ([SQLGlot][11])

It supports a per-identifier escape hatch via a special comment:

```sql
SELECT a /* sqlglot.meta case_sensitive */ FROM table
```

([SQLGlot][11])

### 4.2 Dialect-dependent case folding

SQLGlot’s docs describe `normalize_identifiers` as reflecting how identifiers would be resolved by the engine for each dialect. In other words: “case folding” is dialect-specific and the normalization pass is trying to mirror that engine behavior. ([SQLGlot][11])

### 4.3 Storing original identifiers for human-friendly diagnostics

Use `store_original_column_identifiers=True` when you want:

* canonicalization for cache keys, but
* artifacts that retain “what the author wrote” for review

The flag is a first-class parameter on `normalize_identifiers`. ([SQLGlot][11])

---

## 5) Predicate normalization as an optimization enabler

### 5.1 `optimizer.normalize` and friends

SQLGlot’s `normalize` module provides:

* `normalize(expression, dnf=False, max_distance=128)` to rewrite into CNF by default or DNF if `dnf=True` ([SQLGlot][12])
* `normalized(expression, dnf=False)` to check whether an expression is already in CNF/DNF ([SQLGlot][12])
* `normalization_distance(expression, dnf=False, max_=inf)` which estimates cost of conversion ([SQLGlot][12])

Example (CNF):

```python
import sqlglot
from sqlglot.optimizer.normalize import normalize

expr = sqlglot.parse_one("(x AND y) OR z")
print(normalize(expr, dnf=False).sql())
```

This yields a CNF rewrite as shown in docs. ([SQLGlot][12])

### 5.2 Cost model and guardrails

`normalization_distance` is explicitly documented as an estimate of conversion cost and the docs note the conversion is exponential in complexity. ([SQLGlot][12])

The `normalize` function itself:

* rewrites connector nodes
* computes distance
* skips normalization if distance exceeds `max_distance` (logging the skip) ([SQLGlot][12])

This is exactly what you want in a production compiler pipeline: **attempt normalization only when it’s “cheap enough.”**

### 5.3 Pushdown interactions and DNF limitations

SQLGlot’s predicate pushdown implementation highlights an important limitation:

If predicates are in DNF, SQLGlot can only push down conditions that appear in **all** DNF blocks, and it can’t remove predicates from their original location. ([SQLGlot][13])

This is why your “policy engine” should generally prefer CNF for rewrite friendliness, and only use DNF when you know you need it.

### 5.4 Canonical predicate rewrite policy you should standardize

A pragmatic policy that works well for caching + pushdown:

* Extract the top-level `WHERE` / `JOIN ON` connector expression
* Compute `normalization_distance` (CNF) and only normalize when distance ≤ threshold
* Run `pushdown_predicates` after normalization to stabilize predicate placement
* Record whether normalization was applied and what distance was observed (this becomes a diagnostic artifact)

---

## 6) Semantic diff as the incremental rebuild core

### 6.1 Diff algorithm and outputs

SQLGlot’s diff docs describe how naive approaches fail, discuss the Myers diff algorithm as inspiration, and then explain the adoption of the Change Distiller algorithm for AST diffs. ([SQLGlot][14])

SQLGlot’s diff returns an “edit script” of AST operations.

A Tobiko blog post (by the SQLGlot ecosystem) describes the diff output as an edit script with five operation types:

* Insert
* Remove
* Move
* Update
* Keep ([Tobiko Data][15])

### 6.2 Diff after canonicalization pattern

Your reliable pattern is:

1. Parse
2. Qualify + normalize identifiers
3. Apply canonicalization and normalization passes
4. Diff the canonical ASTs

The “Are these SQL queries the same?” post demonstrates the value of canonicalization (expanding stars, qualifying columns, simplifying boolean logic, CNF conversion) to eliminate meaningless differences before comparison. ([Tobiko Data][2])

### 6.3 Build invalidation policy map

A minimal policy you can implement immediately (and refine later):

* **Non-breaking** candidate: only `Insert` operations that add new `SELECT` projections (with special checks for row-multiplying UDTFs)
* **Breaking**: any `Remove`, `Move`, `Update`, or an `Insert` that changes row cardinality or modifies existing projections

This exact conservative approach is described in the “Automatically detecting breaking changes” post: treat changes as breaking if any operation other than Insert/Keep appears, with additional checks for inserts that might change row counts. ([Tobiko Data][15])

### 6.4 Safety checks for rewrite drift

A practical guard:

* After each rewrite stage (qualify, normalize_identifiers, canonicalize, predicate normalization), compute `diff(previous, next)` and store the edit script.
* For passes intended to be “semantic no-ops” (formatting, identifier normalization under your chosen dialect semantics), assert that only “Keep” or “Update on metadata-only nodes” occurs (your own rule).

This is how you prevent your policy engine from introducing accidental semantic changes.

---

## 7) Column lineage graphs for contracts and projection pushdown

### 7.1 `sqlglot.lineage.lineage` inputs and behavior

`sqlglot.lineage.lineage(column, sql, schema=None, sources=None, dialect=None, scope=None, trim_selects=True, copy=True, **kwargs)` builds a lineage graph for a specific output column. ([SQLGlot][16])

Key behaviors from the docs:

* It parses the SQL (`maybe_parse`)
* It normalizes the target column identifier
* If `sources` is provided, it expands named queries into the expression
* If no scope is provided, it runs `qualify.qualify` internally with `validate_qualify_columns=False` and `identify=False` (and then builds a scope) ([SQLGlot][16])
* It only works for `SELECT`-style inputs and raises an error otherwise
* It raises an error if the requested column is not found in the query’s select list ([SQLGlot][16])

### 7.2 Required-column extraction for scan projection pushdown

There are two complementary techniques:

**(A) Lineage-derived required columns**

* For each output column, compute lineage nodes and collect referenced base columns by walking upstream.

**(B) Structural projection pushdown**
SQLGlot provides an optimizer pass:

* `pushdown_projections(expression, schema=None, remove_unused_selections=True, dialect=None)` which rewrites the AST to remove unused column projections, with an explicit example in docs. ([SQLGlot][17])

In your system:

* Use lineage to build “rule → required input columns” artifacts
* Use `pushdown_projections` to *enforce* projection minimization, so DataFusion reads fewer parquet columns.

### 7.3 Emitting lineage artifacts

Recommended artifact set per “rule output”:

* For each output column:

  * lineage graph (serialized)
  * set of base `(table, column)` dependencies
* For the whole query:

  * union of required base columns
  * list of derived columns and their select expression fingerprints

### 7.4 Handling CTEs, subqueries, and source mapping

SQLGlot’s lineage tooling relies on scoping. The scope system is explicitly described as traversing an expression by its “scopes,” where a scope represents the context of a `SELECT` and is used when you need more than the raw expression tree, such as resolving source names within subqueries. ([SQLGlot][18])

Additionally, if you need a query-level DAG, SQLGlot provides `planner.Step.from_expression`, which builds a DAG of steps from an expression and notes that tables and subqueries must be aliased for this to work. ([SQLGlot][19])

---

## 8) Rewrite rule pipeline as your SQL policy engine

### 8.1 Curated pass list

A robust default pipeline for your repo:

1. Parse (`read_dialect`)
2. `qualify` with `expand_stars=True`, schema attached
3. `normalize_identifiers` (store originals optional)
4. `annotate_types` (type inference)
5. `canonicalize` (type-sensitive canonical form)
6. Targeted CNF normalization for predicates (`normalize(..., max_distance=...)`)
7. `pushdown_predicates` and `pushdown_projections`
8. Join rewrites: `optimize_joins` (optional, see below)
9. Final SQL generation (`write_dialect`, `identify`, `pretty`)

Why this ordering:

* `canonicalize` explicitly relies on type inference via `annotate_types`. ([SQLGlot][20])
* Predicate normalization has guardrails and cost limits built in. ([SQLGlot][12])
* Pushdown passes stabilize where predicates and projections live in the tree. ([SQLGlot][13])

### 8.2 Using `optimize` as a baseline and customizing rules

`sqlglot.optimizer.optimize` rewrites an AST using a sequence of optimizer rules. It:

* accepts the schema in the same mapping forms described earlier,
* accepts `rules=` so you can supply your own ordered list,
* explicitly warns that many rules require `qualify` and you should not remove it unless you know what you’re doing. ([SQLGlot][8])

It also sets some internal defaults that matter (e.g., `isolate_tables=True` and `quote_identifiers=False` in its rule kwargs). ([SQLGlot][8])

Minimal example:

```python
from sqlglot.optimizer import optimize

optimized = optimize(sql_or_expr, schema=schema, dialect=read_dialect)
```

([SQLGlot][8])

### 8.3 High-leverage optimizer passes you can selectively enable

SQLGlot’s optimizer package enumerates a set of passes you can explicitly invoke or include in `rules`. ([SQLGlot][21])

The ones most relevant to your DataFusion-oriented workload:

**Predicate pushdown**

* `pushdown_predicates` pushes predicates into FROMs/JOINS; it has explicit examples and DNF caveats. ([SQLGlot][22])

**Projection pushdown**

* `pushdown_projections` removes unused inner projections and has explicit examples. ([SQLGlot][17])

**Join normalization**

* `optimize_joins` can remove cross joins when possible and reorder joins based on predicate dependencies; it includes an example showing cross join removal and reorder. ([SQLGlot][23])

  * Treat this as optional: it changes join structure and can affect planner choices downstream.

**Subquery normalization**

* `merge_subqueries` merges derived tables into the outer query and can merge CTEs if selected once. ([SQLGlot][24])
* `eliminate_subqueries` rewrites derived tables as CTEs and can deduplicate common subqueries. ([SQLGlot][25])

  * Choose one strategy and standardize it; mixing can oscillate.

**CTE cleanup**

* `eliminate_ctes` removes unused CTEs with a simple example. ([SQLGlot][26])

**Unused join elimination**

* `eliminate_joins` removes unused joins only when it knows the join condition doesn’t produce duplicate rows; it also skips when there are unqualified columns in the scope. ([SQLGlot][27])

  * This is “high reward, high caution”: enable only when your qualification policy is strict and you have good test coverage for row-count stability.

**Table isolation**

* `isolate_table_selects` exists as a pass that manipulates how multi-source scopes are isolated; it’s used to make other optimizations perform well. ([SQLGlot][28])

### 8.4 Dialect-specific rewrites for DataFusion reality

Even if you don’t have a perfect “DataFusion dialect” mapping, you still want a dialect-specific rewrite lane:

* Pick the closest write dialect you intend to execute and **lock it** (record in artifacts).
* Run a compatibility test suite:

  * canonical inputs → generated SQL → DataFusion parse/execute
* When DataFusion rejects syntax:

  * patch at the SQLGlot layer by rewriting the AST into an equivalent supported shape
  * keep rewrites in a dedicated “datafusion_policy.py” module and regression-test them with semantic diffs (so fixes don’t regress other workloads)

For enforcement, set generator behavior to raise on unsupported expressions (dialect docs describe `unsupported_level` and `max_unsupported`). ([SQLGlot][6])

---

## 9) Failure-mode playbooks

### 9.1 Qualification failures

Common root causes:

* Missing or incomplete schema (columns can’t be resolved)
* Ambiguous columns without enough context
* Disabling `expand_stars` (breaks downstream rules; docs warn against it) ([SQLGlot][7])

Playbook:

* Always qualify with `expand_stars=True`
* Provide schema for contracted tables
* When failing: persist SQL + schema snapshot + dialect + failure stack in a repro bundle

### 9.2 Dialect mismatch errors

Symptoms:

* “valid” SQL failing to parse
* incorrect output dialect SQL

Playbook:

* Always specify the parse dialect; SQLGlot emphasizes this as the most common issue. ([GitHub][1])
* Always specify the write dialect when generating SQL. ([GitHub][1])

### 9.3 Normalization explosion

Symptoms:

* normalization takes too long or produces huge predicates

Playbook:

* Always compute/limit `normalization_distance` and keep `max_distance` conservative
* Rely on `normalize`’s built-in skip behavior when distance is too high ([SQLGlot][12])

### 9.4 Lineage misattribution edge cases

Common pitfalls:

* lineage only works for SELECT; it raises when it can’t build lineage or can’t find the requested output column ([SQLGlot][16])
* missing schema or insufficient qualification can reduce accuracy
* heavy use of `SELECT *` without expansion reduces clarity

Playbook:

* Use qualified/canonicalized SQL for lineage extraction
* Prefer `trim_selects=True` to simplify the derived query to only relevant columns ([SQLGlot][16])
* If lineage is mission-critical, store and reuse a pre-built scope object (`scope=` parameter) to avoid subtle differences across runs ([SQLGlot][16])

---

## Appendices

## A) Canonical SQL shape spec for your repo

A practical “SQL shape” contract (derived from SQLGlot’s own canonicalization themes):

1. All tables have explicit aliases (no anonymous derived tables)
2. All `*` projections are expanded into explicit columns
3. All selected columns are fully qualified and consistently aliased
4. Identifiers are normalized according to the chosen dialect semantics
5. Predicates are normalized to CNF when cheap enough; otherwise preserved
6. Predicate and projection pushdown passes are applied to stabilize structure

The SQLGlot ecosystem explicitly uses star expansion and optimizer-based canonicalization to avoid fragile comparisons and to reason about changes. ([Tobiko Data][2])

## B) AST fingerprint recipe

You want a stable serialization that is insensitive to formatting and (ideally) node identity.

Two robust approaches:

**Approach 1: serialize the canonical AST to JSON payloads**
SQLGlot provides `sqlglot.serde.dump(expression)` returning a JSON-serializable list, and `sqlglot.serde.load(payloads)` to restore an Expression. ([SQLGlot][29])

Fingerprint steps:

1. canonicalize expression (your pass list)
2. `payloads = dump(expr)`
3. `fingerprint = sha256(json.dumps(payloads, sort_keys=True).encode()).hexdigest()`

**Approach 2: canonical SQL string**

* `expr.sql(dialect=write_dialect, pretty=False, identify=<policy>)`
* hash the resulting string

This is simpler but depends more heavily on generator stability and configuration; you must include generator settings in the cache key. ([GitHub][1])

## C) Golden tests for rewrite correctness

Your “rewrite test harness” should have three tiers:

1. **Parse/generate roundtrip**

   * canonical SQL → parse → `.sql(pretty=False)` should be stable under your dialect/config settings ([Tobiko Data][2])

2. **Semantic diff assertions**

   * For each rewrite pass, diff the pre/post AST and assert only the expected edit types occur.
   * Use the edit operation types Insert/Remove/Move/Update/Keep as your primitive signals. ([Tobiko Data][15])

3. **Lineage invariants**

   * For each canonical rule output column, lineage extraction should succeed and yield expected base dependencies.
   * Fail fast if lineage can’t find the column or can’t build a scope. ([SQLGlot][16])

[1]: https://raw.githubusercontent.com/tobymao/sqlglot/main/README.md "raw.githubusercontent.com"
[2]: https://tobikodata.com/blog/are-these-sql-queries-the-same "Are these SQL queries the same?"
[3]: https://sqlglot.com/sqlglot/errors.html "sqlglot.errors API documentation"
[4]: https://sqlglot.com/sqlglot/dialects.html "sqlglot.dialects API documentation"
[5]: https://sqlglot.com/sqlglot/dialects/dialect.html "sqlglot.dialects.dialect API documentation"
[6]: https://sqlglot.com/sqlglot/dialects/tsql.html "sqlglot.dialects.tsql API documentation"
[7]: https://sqlglot.com/sqlglot/optimizer/qualify.html "sqlglot.optimizer.qualify API documentation"
[8]: https://sqlglot.com/sqlglot/optimizer/optimizer.html "sqlglot.optimizer.optimizer API documentation"
[9]: https://sqlglot.com/sqlglot/schema.html "sqlglot.schema API documentation"
[10]: https://sqlglot.com/sqlglot/optimizer/qualify_tables.html "sqlglot.optimizer.qualify_tables API documentation"
[11]: https://sqlglot.com/sqlglot/optimizer/normalize_identifiers.html "sqlglot.optimizer.normalize_identifiers API documentation"
[12]: https://sqlglot.com/sqlglot/optimizer/normalize.html "sqlglot.optimizer.normalize API documentation"
[13]: https://sqlglot.com/sqlglot/optimizer/pushdown_predicates.html "sqlglot.optimizer.pushdown_predicates API documentation"
[14]: https://sqlglot.com/sqlglot/diff.html "sqlglot.diff API documentation"
[15]: https://tobikodata.com/blog/automatically-detecting-breaking-changes-in-sql-queries "Automatically detecting breaking changes in SQL queries"
[16]: https://sqlglot.com/sqlglot/lineage.html "sqlglot.lineage API documentation"
[17]: https://sqlglot.com/sqlglot/optimizer/pushdown_projections.html "sqlglot.optimizer.pushdown_projections API documentation"
[18]: https://sqlglot.com/sqlglot/optimizer/scope.html "sqlglot.optimizer.scope API documentation"
[19]: https://sqlglot.com/sqlglot/planner.html "sqlglot.planner API documentation"
[20]: https://sqlglot.com/sqlglot/optimizer/canonicalize.html "sqlglot.optimizer.canonicalize API documentation"
[21]: https://sqlglot.com/sqlglot/optimizer.html "sqlglot.optimizer API documentation"
[22]: https://sqlglot.com/sqlglot/optimizer/pushdown_predicates.html?utm_source=chatgpt.com "sqlglot.optimizer.pushdown_predicates API documentation"
[23]: https://sqlglot.com/sqlglot/optimizer/optimize_joins.html "sqlglot.optimizer.optimize_joins API documentation"
[24]: https://sqlglot.com/sqlglot/optimizer/merge_subqueries.html?utm_source=chatgpt.com "sqlglot.optimizer.merge_subqueries API documentation"
[25]: https://sqlglot.com/sqlglot/optimizer/eliminate_subqueries.html "sqlglot.optimizer.eliminate_subqueries API documentation"
[26]: https://sqlglot.com/sqlglot/optimizer/eliminate_ctes.html?utm_source=chatgpt.com "sqlglot.optimizer.eliminate_ctes API documentation"
[27]: https://sqlglot.com/sqlglot/optimizer/eliminate_joins.html "sqlglot.optimizer.eliminate_joins API documentation"
[28]: https://sqlglot.com/sqlglot/optimizer/isolate_table_selects.html "sqlglot.optimizer.isolate_table_selects API documentation"
[29]: https://sqlglot.com/sqlglot/serde.html "sqlglot.serde API documentation"


Below is the **follow-on `sqlglot-advanced.md` content** that covers the major “compiler-IR” features you’ll realistically want in your codebase but weren’t fully detailed previously: the **default optimizer rule stack and how to customize it**, the **internal qualification subsystem**, **type inference + type-driven canonicalization**, **subquery unnesting + decorrelation**, **subquery/CTE shape controls**, the **simplifier (with its safety metadata model)**, the **`transforms` module for dialect-compat rewrites (QUALIFY, UNNEST/EXPLODE, etc.)**, the **scope toolchain**, and the **executor harness for semantic golden tests**.

---

## 10) The default optimizer pipeline (`sqlglot.optimizer.optimize`) as a first-class compiler stage

### 10.1 The built-in `RULES` tuple (what `optimize()` runs, and in what order)

SQLGlot exposes a default ordered tuple of optimizer rules and uses it as the default `rules=` argument to `optimize()`. The `RULES` sequence is explicitly defined as:
`qualify → pushdown_projections → normalize → unnest_subqueries → pushdown_predicates → optimize_joins → eliminate_subqueries → merge_subqueries → eliminate_joins → eliminate_ctes → quote_identifiers → annotate_types → canonicalize → simplify`. ([SQLGlot][1])

This is **the actual canonicalization pipeline** you should treat as the baseline for:

* stable SQL shapes
* stable AST fingerprints
* stable diffs (incremental rebuild)

### 10.2 `optimize()` signature: db/catalog defaults + schema + dialect + rules

`optimize(expression, schema=None, db=None, catalog=None, dialect=None, rules=RULES, sql=None, **kwargs)` is documented as the primary entrypoint for rewriting an AST into “optimized form.” ([SQLGlot][1])

Key knobs you should standardize in your compiler boundary:

* `schema` (MappingSchema or dict mapping)
* `db`, `catalog` defaults (equivalent of `USE DATABASE` / `USE CATALOG`)
* `dialect` for parsing if `expression` is a SQL string
* `rules` (your curated pipeline)
* `sql` (original SQL string, used for better error reporting in some passes—see §12.3) ([SQLGlot][1])

### 10.3 How `**kwargs` are routed to passes (important for “policy injection”)

`optimize()` inspects each rule’s signature and passes only the kwargs it recognizes, so you can safely provide “global policy kwargs” without breaking other passes. ([SQLGlot][2])

**Practical implication:** you can define a single “policy dict” like:

```python
policy_kwargs = {
  "dialect": "duckdb",
  "identify": False,
  "expand_stars": True,
  "validate_qualify_columns": True,
  "leave_tables_isolated": True,  # merge_subqueries()
}
optimized = sqlglot.optimizer.optimize(expr, schema=schema, rules=my_rules, **policy_kwargs)
```

…and SQLGlot will feed the relevant keys to the relevant rules.

### 10.4 You should *explicitly pin your rule list*

Because optimizer behavior is a functional dependency of your cache keys, don’t rely on the default `RULES` implicitly. Treat `rules` as part of the “compiler version contract” and store it (or a hash of it) alongside build artifacts. ([SQLGlot][1])

---

## 11) Type inference + type-driven canonicalization (`annotate_types` + `canonicalize`)

### 11.1 `annotate_types`: inferred types become AST metadata

`annotate_types(expression, schema=None, expression_metadata=None, coerces_to=None, dialect=None, overwrite_types=True)` infers types and annotates the AST accordingly. It’s schema-aware and dialect-aware. ([SQLGlot][3])

Two important “power knobs”:

* `expression_metadata`: lets you override/extend type annotation logic per expression kind
* `coerces_to`: controls which types can be coerced to which (critical for stable coercion behavior across dialects) ([SQLGlot][3])

### 11.2 `canonicalize`: standard form that *depends on inferred types*

`canonicalize(expression, dialect=None)` converts SQL into a standard form and explicitly relies on type inference (many conversions are type-dependent). ([SQLGlot][4])

The implementation applies a stack of canonical rewrites including (representative examples):

* converting string `+` to concatenation when the inferred type is text
* replacing certain date function forms into explicit casts
* removing redundant casts when the input is already of the target type
* normalizing boolean contexts (“ensure bools”)
* removing redundant ascending order markers ([SQLGlot][4])

### 11.3 Minimum implementation pattern (you should standardize this ordering)

In a “compiler IR policy engine,” the type-aware order is typically:

1. `qualify(...)` (resolve columns / expand stars)
2. `annotate_types(...)`
3. `canonicalize(...)`
4. optionally `simplify(...)` (see §15)

This ordering matches SQLGlot’s own default optimizer ordering (annotate_types and canonicalize occur near the end, after structural rewrites). ([SQLGlot][1])

---

## 12) Qualification subsystem details you’ll want (beyond “call qualify”)

### 12.1 `qualify_columns` (fully qualify columns, expand stars, expand alias refs)

`qualify_columns(expression, schema, expand_alias_refs=True, expand_stars=True, infer_schema=None, allow_partial_qualification=False, dialect=None)` rewrites the AST so columns are fully qualified; the docs emphasize star expansion as required for many optimizer rules. ([SQLGlot][5])

Additional details that matter for real-world SQL:

* It uses `Scope` traversal and a `Resolver` to disambiguate columns. ([SQLGlot][5])
* There’s an explicit note that currently it only handles a single PIVOT/UNPIVOT operator. ([SQLGlot][5])

### 12.2 `qualify_tables` (fully qualify tables + enforce aliases; rewrite join-construct subqueries)

`qualify_tables` rewrites tables to be fully qualified and may expand join constructs like `(t1 JOIN t2) AS t` into a subquery form. ([SQLGlot][6])

### 12.3 `validate_qualify_columns`: turn “maybe ambiguous” into a hard compile-time error

`validate_qualify_columns(expression, sql=None)` raises `OptimizeError` if columns aren’t qualified / resolvable. It uses line/col metadata (and `highlight_sql` when you pass the original SQL string) to produce actionable error messages. ([SQLGlot][5])

**This is a key “make failures loud” primitive**: run it after qualification in your compiler stage so errors are deterministic and reproducible.

### 12.4 `qualify_outputs`: enforce “every output is aliased”

`qualify_outputs(scope_or_expression)` ensures output columns are aliased; this is extremely important for:

* stable lineage extraction
* stable column name contracts
* stable diff semantics (because rename-only changes show up cleanly) ([SQLGlot][5])

### 12.5 `quote_identifiers`: dialect-aware quoting pass

`quote_identifiers(expression, dialect=None, identify=True)` transforms identifiers that need quoting based on the dialect’s quoting rules. ([SQLGlot][5])

This is a **final-stage** pass if your “engine truth” dialect is case-sensitive or has reserved keywords in your schema.

### 12.6 `pushdown_cte_alias_columns` (CTE alias column semantics)

The qualification subsystem includes a helper that pushes CTE alias columns into the projection, motivated by dialects where CTE alias columns can be referenced in HAVING. ([SQLGlot][5])

Even if DataFusion doesn’t need this, it’s important if you parse/normalize SQL from other dialects and want consistent canonical shapes before transpiling.

---

## 13) Scope toolchain as an “analysis backbone” (beyond lineage)

### 13.1 What “scope” is

`Scope` represents the context of a SELECT statement and is used when you need more information than the raw expression tree—especially in the presence of subqueries and CTEs. SQLGlot exposes traversal helpers that return scopes rather than raw nodes. ([SQLGlot][7])

### 13.2 The scope APIs you’ll actually use

SQLGlot exports:

* `build_scope(expression)`
* `traverse_scope(expression)`
* `walk_in_scope`, `find_in_scope`, `find_all_in_scope` ([SQLGlot][8])

And `Scope` objects expose high-value properties you’ll want in your compiler artifacts:

* `selected_sources`, `references` (what tables/aliases are actually selected from)
* `external_columns` (correlation detection)
* `unqualified_columns` (ambiguity detection)
* `cte_sources`, `derived_tables`, `udtfs` etc. ([SQLGlot][9])

### 13.3 Scope traversal is not only SELECTs

`TRAVERSABLES = (exp.Query, exp.DDL, exp.DML)` in scope tooling indicates scope traversal is designed to operate across queries and also DDL/DML contexts. ([SQLGlot][9])

This matters if you canonicalize DataFusion DDL like `CREATE EXTERNAL TABLE` or `COPY` statements in the same pipeline (you might want different policies per statement kind).

---

## 14) Subquery unnesting and decorrelation (`unnest_subqueries`)

### 14.1 What `unnest_subqueries` does

`unnest_subqueries(expression)` rewrites certain predicate subqueries into joins:

* scalar subqueries → CROSS joins
* correlated/vectorized subqueries → rewritten with grouping so the join is not many-to-many ([SQLGlot][10])

The doc example shows converting a scalar subquery predicate into a LEFT JOIN with a grouped subquery and then filtering on the joined key. ([SQLGlot][10])

### 14.2 What triggers decorrelation

The implementation traverses scopes and:

* if `scope.external_columns` exist, it tries to decorrelate (convert external references into join keys)
* else if it’s a plain subquery scope, it tries to unnest it ([SQLGlot][10])

This gives you a principled way to:

* eliminate correlated subqueries that DataFusion might execute poorly,
* increase pushdown and join planning opportunities.

### 14.3 Important safety constraints you should encode

`unnest_subqueries` has explicit early exits for some shapes (e.g., if the subquery contains LIMIT/OFFSET, or certain OR conditions in correlation). ([SQLGlot][10])

**Implementation policy recommendation**

* Run `unnest_subqueries` as “optional but powerful”: enabled for rulepacks that benefit (e.g., where subquery filters appear), but disabled if you rely on correlated semantics that are hard to preserve.

---

## 15) Subquery and CTE shape controls: “compact” vs “isolated” query forms

SQLGlot has multiple passes that reshape queries. In your system, these are the core “SQL shape knobs” that affect DataFusion planner behavior and incremental rebuild stability.

### 15.1 `merge_subqueries`: inline derived tables / merge single-use CTEs

`merge_subqueries(expression, leave_tables_isolated=False)` merges derived tables into the outer query and merges CTEs that are selected from only once. ([SQLGlot][11])

It supports `leave_tables_isolated=True`, which prevents merging if it would create multiple table selects in a single query. ([SQLGlot][11])

**Why this matters**

* “merged” queries can improve pushdown opportunities and reduce nesting.
* “isolated” queries can improve readability and reduce planner complexity (and sometimes avoid backend bugs).

### 15.2 `isolate_table_selects`: enforce one-table-per-scope subqueries when useful

`isolate_table_selects(expression, schema=None, dialect=None)` wraps tables in subqueries under some conditions so scopes become more isolated. It explicitly requires tables to have an alias and tells you to run `qualify_tables` first if aliases are missing. ([SQLGlot][12])

This is a good lever when:

* some optimizer passes become easier/safer with isolated sources,
* you want stable intermediate boundaries for diagnostics.

### 15.3 `eliminate_subqueries`: rewrite derived tables as CTEs (with dedup)

`eliminate_subqueries(expression)` rewrites derived tables into CTEs and can deduplicate common subqueries. ([SQLGlot][13])

This is a *caching-friendly* rewrite: it tends to create explicit reusable named subplans.

### 15.4 How these relate to `optimize()` defaults

All of these reshaping passes are included in SQLGlot’s default optimizer `RULES` ordering. ([SQLGlot][1])
So if you want to “pin behavior,” you either:

* accept the defaults and pin your SQLGlot version, or
* provide your own explicit `rules` list and treat it as part of your compiler contract.

---

## 16) Simplification (`simplify`) as a *semantic* canonicalizer (with guardrails)

### 16.1 What `simplify()` is for

`simplify(expression, constant_propagation=False, coalesce_simplification=False, dialect=None)` rewrites the AST to simplify expressions (boolean/math and more). The docs show `TRUE AND TRUE → TRUE`. ([SQLGlot][14])

### 16.2 The two “power knobs”

* `constant_propagation`: enables constant propagation simplifications
* `coalesce_simplification`: tries to remove COALESCE calls (can help some analyses but makes queries more verbose or changes intended null-handling readability) ([SQLGlot][14])

### 16.3 The “FINAL” metadata model (why simplify is safer than ad-hoc transforms)

The simplifier uses a `FINAL` meta marker to prevent simplifying parts of the tree where exact syntactic equivalence matters (classic example: GROUP BY keys must match projections). The implementation explicitly avoids simplifying GROUP BY expressions by marking relevant nodes. ([SQLGlot][14])

**How you use this**

* Treat `simplify()` as the last semantic-canonicalization stage (after structural rewrites).
* If you have “don’t touch this expression” areas (e.g., signature-critical hash expressions), you can adopt the same pattern: attach a meta flag and prune in a custom pass (SQLGlot’s simplify shows the model).

### 16.4 Dependency note: timedelta simplification uses `dateutil`

SQLGlot notes that it uses `dateutil` to simplify literal timedelta expressions; if unavailable, certain simplifications won’t occur. ([SQLGlot][15])

---

## 17) `sqlglot.transforms` — dialect compatibility rewrites you will use in practice

The optimizer is about general canonicalization; `sqlglot.transforms` is a **grab bag of targeted rewrites** that often exist because dialects differ and some constructs need to be rewritten into equivalent supported forms.

### 17.1 `eliminate_qualify`: convert QUALIFY into subquery + WHERE

`eliminate_qualify(expression)` rewrites SELECTs containing QUALIFY into equivalent subqueries filtered outside, explicitly handling the fact that some dialects don’t allow window functions in WHERE and may need alias projections. ([SQLGlot][16])

**Why you care**

* If your upstream SQL uses QUALIFY (common for “winner selection”), but DataFusion doesn’t support it, this transform is the canonical bridge.
* Even if you don’t ingest QUALIFY directly, it’s a useful normalization target for portability.

### 17.2 UNNEST/EXPLODE interop: `unnest_to_explode` and `explode_projection_to_unnest`

For nested list handling (very relevant to your codebase’s explode/list patterns):

* `unnest_to_explode(expression, unnest_using_arrays_zip=True)` converts cross join UNNEST into LATERAL VIEW EXPLODE-style forms and supports multi-array unnest via ARRAYS_ZIP (or raises if disabled). It also chooses between EXPLODE/POSEXPLODE/INLINE depending on offset and multi-expr. ([SQLGlot][16])
* `explode_projection_to_unnest(index_offset=0)` returns a transform that converts explode/posexplode projections into unnests. ([SQLGlot][16])

**Implementation pattern**

* Put these into your “dialect policy lane” so you can:

  * normalize to UNNEST shape internally (for analysis),
  * generate dialect-specific explode shapes when necessary.

### 17.3 `any_to_exists`: rewrite ANY into array EXISTS-style

`any_to_exists(expression)` rewrites Postgres ANY comparisons into Spark EXISTS-style lambdas for array expressions (queries are explicitly not handled by this transform). ([SQLGlot][16])

Even if you don’t target Spark, it’s instructive: SQLGlot has many transforms that rewrite “rare operator X” into “more universal operator Y,” which is exactly what you’ll sometimes need when DataFusion lacks a construct.

### 17.4 Join-normal-form transforms (useful when backends don’t support specific join kinds)

`eliminate_full_outer_join(expression)` converts a query with a single FULL OUTER JOIN into a UNION of LEFT/RIGHT join forms. ([SQLGlot][17])

There are other join rewrites (e.g., semi/anti join elimination) present in transforms; use them when your engine’s join support differs from source SQL. ([SQLGlot][16])

### 17.5 `preprocess`: chaining transforms into generation

`preprocess(transforms, generator=None)` creates a composite generator transform that chains a sequence of transformations and then renders SQL, integrating with generator dispatch. ([SQLGlot][16])

**Why this is architecturally important**

* You can define a “DataFusion generation policy” as a chain:

  * eliminate_qualify → unnest normalization → unnest compatibility transforms → etc.
* Then bind it as part of your generator configuration so every `.sql(dialect=...)` call passes through a consistent transform lane.

---

## 18) Planner DAG + executor harness for semantic golden tests (rewrite safety)

### 18.1 Planner DAG: `Step.from_expression`

SQLGlot’s planner can build a DAG of Steps from an expression; it explicitly requires tables/subqueries to be aliased. ([SQLGlot][18])

This is useful in your system for:

* extracting subquery dependency graphs (CTE DAGs),
* computing “which subplans changed” for incremental rebuild decisions.

### 18.2 In-memory executor: `sqlglot.executor.execute` for rewrite correctness tests

SQLGlot includes a (not-fast) Python executor designed to run queries against Python tables. It exposes:
`execute(sql | Expression, schema=None, dialect=None, tables=None) -> Table`. ([SQLGlot][19])

It:

* calls `optimize(..., leave_tables_isolated=True, ...)`
* builds a `Plan`
* executes via `PythonExecutor` ([SQLGlot][19])

**How you use it**

* For rewrite pass validation: execute original SQL and rewritten SQL on the same small synthetic tables and assert result equivalence.
* This is the closest thing to a “semantic unit test” you can have without depending on DataFusion runtime behavior.

---

## 19) Schema module deeper: MappingSchema, normalization, depth, and identifier semantics

### 19.1 `MappingSchema` is the concrete schema implementation you’ll use

`MappingSchema` is a `Schema` implementation backed by nested mappings and supports multiple nesting forms (table→cols, db→table→cols, catalog→db→table→cols). It also supports a “visible columns” map (same nesting structure) and dialect-aware type mapping + identifier normalization. ([SQLGlot][20])

### 19.2 `ensure_schema` and `normalize_name`

* `ensure_schema(schema, **kwargs)` wraps dict schemas into a `MappingSchema` automatically. ([SQLGlot][20])
* `normalize_name(identifier, dialect=None, is_table=False, normalize=True)` parses identifiers and normalizes them using the dialect’s `normalize_identifier`; it also sets an `is_table` meta flag (notably used for dialect-specific rules). ([SQLGlot][20])

### 19.3 Schema depth and supported table qualifiers

The abstract `Schema` interface includes a `supported_table_args` property (“this”, “db”, “catalog”, etc.) and the schema enforces that table identifiers match its depth. ([SQLGlot][20])

This is directly relevant if you standardize fully qualified names in your artifacts (`catalog.db.table`) and want qualification and validation to behave deterministically.

---

## 20) Dialect customization and generator policy controls (how you make SQLGlot “yours”)

### 20.1 Custom dialects are explicitly supported

SQLGlot’s dialect docs describe how each SQL variation is a Dialect subclass extending Tokenizer/Parser/Generator, and they include a “custom dialect” implementation template. ([SQLGlot][21])

**Why you care**

* If DataFusion’s SQL shape is “close to DuckDB but with quirks,” you can either:

  * keep using a nearest dialect and maintain a transforms lane, or
  * create a custom dialect that encodes those quirks so generation is stable.

### 20.2 Function name normalization and preserving originals

Dialect settings include:

* `NORMALIZE_FUNCTIONS` (upper/lower/disabled)
* `PRESERVE_ORIGINAL_NAMES` (preserve function name in node metadata for roundtripping) ([SQLGlot][22])

These matter if:

* you want stable canonical output (`NORMALIZE_FUNCTIONS="upper"` is common),
* but still want the original function text preserved for debug bundles or “diff intent” (`PRESERVE_ORIGINAL_NAMES=True`).

### 20.3 Identifier normalization strategy

Dialects expose a `NormalizationStrategy` enum (lowercase/uppercase/case-sensitive/case-insensitive) that underlies how identifiers are treated. ([SQLGlot][23])

This is why your parse/generate dialect selection must be pinned: it directly affects identifier normalization and therefore cache keys.

---

## 21) Putting these additions into your “SQL policy engine” (minimal implementable scaffold)

If you want a single, repeatable pipeline that uses these advanced pieces:

```python
import sqlglot
from sqlglot.optimizer import optimize
from sqlglot.optimizer.qualify_columns import validate_qualify_columns
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.canonicalize import canonicalize
from sqlglot.optimizer.simplify import simplify
from sqlglot.transforms import eliminate_qualify

def compile_sql_policy(sql: str, *, schema: dict, read: str, write: str) -> str:
    # 1) Parse (strictly pin dialect)
    expr = sqlglot.parse_one(sql, dialect=read)

    # 2) Optional dialect-compat transforms on the *incoming* shape
    # (example: if upstream uses QUALIFY)
    expr = eliminate_qualify(expr)

    # 3) Optimizer canonicalization (pin your rule list)
    expr = optimize(expr, schema=schema, dialect=read)

    # 4) Make ambiguity fatal (requires original sql for highlighting)
    validate_qualify_columns(expr, sql=sql)

    # 5) Type-driven canonicalization (if you don’t use full optimize rules)
    expr = annotate_types(expr, schema=schema, dialect=read)
    expr = canonicalize(expr, dialect=read)
    expr = simplify(expr, dialect=read)

    # 6) Generate for engine dialect (pin identify/pretty knobs)
    return expr.sql(dialect=write, pretty=False, identify=False)
```

This is intentionally conservative:

* `optimize()` gives you the default “canonical SQL” rule stack (and you can replace it with your own `rules=` if you want full control). ([SQLGlot][1])
* `validate_qualify_columns(sql=...)` turns ambiguous resolution into deterministic compile-time errors with highlighted SQL snippets. ([SQLGlot][5])
* `eliminate_qualify` and other transforms are the “dialect-compat policy lane.” ([SQLGlot][16])

---


[1]: https://sqlglot.com/sqlglot/optimizer/optimizer.html "sqlglot.optimizer.optimizer API documentation"
[2]: https://sqlglot.com/sqlglot/optimizer/optimizer.html?utm_source=chatgpt.com "sqlglot.optimizer.optimizer API documentation"
[3]: https://sqlglot.com/sqlglot/optimizer/annotate_types.html "sqlglot.optimizer.annotate_types API documentation"
[4]: https://sqlglot.com/sqlglot/optimizer/canonicalize.html "sqlglot.optimizer.canonicalize API documentation"
[5]: https://sqlglot.com/sqlglot/optimizer/qualify_columns.html "sqlglot.optimizer.qualify_columns API documentation"
[6]: https://sqlglot.com/sqlglot/optimizer/qualify_tables.html?utm_source=chatgpt.com "sqlglot.optimizer.qualify_tables API documentation"
[7]: https://sqlglot.com/sqlglot/optimizer/scope.html?utm_source=chatgpt.com "sqlglot.optimizer.scope API documentation"
[8]: https://sqlglot.com/sqlglot/optimizer.html "sqlglot.optimizer API documentation"
[9]: https://sqlglot.com/sqlglot/optimizer/scope.html "sqlglot.optimizer.scope API documentation"
[10]: https://sqlglot.com/sqlglot/optimizer/unnest_subqueries.html "sqlglot.optimizer.unnest_subqueries API documentation"
[11]: https://sqlglot.com/sqlglot/optimizer/merge_subqueries.html "sqlglot.optimizer.merge_subqueries API documentation"
[12]: https://sqlglot.com/sqlglot/optimizer/isolate_table_selects.html "sqlglot.optimizer.isolate_table_selects API documentation"
[13]: https://sqlglot.com/sqlglot/optimizer/eliminate_subqueries.html?utm_source=chatgpt.com "sqlglot.optimizer.eliminate_subqueries API documentation"
[14]: https://sqlglot.com/sqlglot/optimizer/simplify.html "sqlglot.optimizer.simplify API documentation"
[15]: https://sqlglot.com/?utm_source=chatgpt.com "sqlglot API documentation"
[16]: https://sqlglot.com/sqlglot/transforms.html "sqlglot.transforms API documentation"
[17]: https://sqlglot.com/sqlglot/transforms.html?utm_source=chatgpt.com "sqlglot.transforms API documentation"
[18]: https://sqlglot.com/sqlglot/planner.html?utm_source=chatgpt.com "sqlglot.planner API documentation"
[19]: https://sqlglot.com/sqlglot/executor.html "sqlglot.executor API documentation"
[20]: https://sqlglot.com/sqlglot/schema.html "sqlglot.schema API documentation"
[21]: https://sqlglot.com/sqlglot/dialects.html "sqlglot.dialects API documentation"
[22]: https://sqlglot.com/sqlglot/dialects/dialect.html?utm_source=chatgpt.com "sqlglot.dialects.dialect API documentation"
[23]: https://sqlglot.com/sqlglot/dialects/dialect.html "sqlglot.dialects.dialect API documentation"

## 22) Parsing and tokenization control plane

### 22.1 Strictness knobs and actionable error payloads

SQLGlot’s parsing stack is built to accumulate errors and then raise (or warn) depending on your configured error policy. The parser surfaces structured error details (including merged errors / highlights), and the public API emphasizes that syntax errors are highlighted and incompatibilities can warn or raise based on configuration. ([GitHub][1])

**Implementation pattern (you should standardize):**

* Pick a strictness profile per surface:

  * **compiler pipeline** (rulepack build): fail fast
  * **interactive tooling** (dev, investigation): warn and recover when possible

You’ll use `sqlglot.errors.ErrorLevel` (e.g., `RAISE`, `IMMEDIATE`) to enforce this policy and keep it in your “SQL policy contract” along with dialects and optimizer rule list. 

### 22.2 Position metadata + “highlighted SQL” requires the original SQL string

Several optimizer components accept the original SQL string specifically so error messages can include highlighting. For example, `optimizer.optimize(..., sql=<original>)` documents that highlighting requires the expression to have position metadata from parsing, and if `sql` isn’t provided you won’t get highlighted errors. ([SQLGlot][2])

**Implementation rule:** whenever you parse input SQL that might fail qualification/validation, carry the original SQL string through your pipeline and pass it into optimizer/validation stages that accept it.

### 22.3 “Templated SQL” is not SQL: render first, then parse

SQLGlot parses SQL, not templating languages. If upstream SQL contains Jinja-style placeholders, you must render them (or strip them) before parsing. This is a common failure mode in systems that ingest templated SQL. ([Stack Overflow][3])

**Practical policy for your codebase:**

* Add a pre-parse “templating sanitization” stage:

  * either render parameters
  * or replace them with typed literals / bind parameters your dialect supports

### 22.4 Rust tokenizer acceleration and robustness toggles

SQLGlot supports an optional Rust tokenizer (`sqlglot[rs]`) for speed.
Like any alternate lexer implementation, edge-case tokenization bugs can exist. For example, there are reported failures around nested comment patterns in the Rust tokenizer (with workarounds shown in issue discussions). ([GitHub][4])

**Implementation posture:**

* Keep a “tokenizer mode” knob in your toolchain (fast vs conservative).
* In CI, include at least one test lane that exercises parsing under the tokenizer mode you plan to ship.

### 22.5 Dialect mapping as a first-class system responsibility

Large systems often have “dialect identity” mismatches (engine names that don’t map cleanly to SQLGlot dialects). Real-world projects hit this (example: Superset dialect mapping issues). ([GitHub][5])

**Implementation pattern:**

* Maintain your own `engine_name -> sqlglot dialect` map.
* If an engine is unknown:

  * map to the closest supported dialect **or**
  * create a custom dialect shim (see §31).

---

## 23) Generation policy and reproducible SQL emission

### 23.1 The generator is configurable: stabilize quoting, casing, and comments

Dialect generator classes expose key generation options that directly affect determinism: `pretty`, `identify`, and identifier normalization. Dialect docs describe `identify` as accepting `False`, `True`, and `'safe'` (quote only identifiers that are case insensitive), and also provide a `normalize` option for identifier normalization. ([SQLGlot][6])

SQLGlot’s generator also has a `comments` knob to preserve comments in output SQL. ([SQLGlot][7])

**Implementation rule (cache keys vs reviewer SQL):**

* Cache key emission:

  * `pretty=False`
  * `identify=<pinned policy>`
  * `comments=False` (unless comments are semantically meaningful in your org)
* Reviewer artifacts:

  * `pretty=True`
  * `comments=True` (optional; best for preserving author intent)

### 23.2 “Unsupported constructs” must be enforced, not merely warned

SQLGlot exposes generator policies for unsupported constructs (warn vs raise) via strictness configuration (`ErrorLevel` / “unsupported level” behavior is a documented and common configuration axis). 

**Implementation rule:**

* For DataFusion execution, set unsupported behavior to **raise** in the final emission stage so you don’t ship SQL that looks valid but won’t run.

### 23.3 Dialect generator `TRANSFORMS` is the hook for “rewrite-at-generate-time”

Dialect classes commonly define a `TRANSFORMS` mapping and import `sqlglot.transforms.preprocess`, showing that dialect output frequently depends on a transform lane embedded into generation. ([SQLGlot][6])

**Best-in-class design for your system:**

* Keep *analysis canonical form* in one internal dialect shape.
* Apply *engine compatibility transforms* at the final generate stage via a pinned transform chain (see §26–§27).

---

## 24) AST node contracts you’ll rely on: `Expression` shape, args, metadata, and safe mutation

### 24.1 Node structure is “args + arg_types”

Every AST node is a subclass of `Expression`, and expression types define their expected argument structure (commonly via `arg_types`). The recommended way to understand node shape is to inspect the expression definitions themselves; this is also how SQLGlot’s author advises users to discover node attributes. ([SQLGlot][8])

**Implementation pattern (for your tooling + LLM catalogs):**

* Build an “AST schema catalog” by enumerating expression classes and extracting `arg_types`.
* Use that to generate LLM-friendly “node shape docs” (Join args, Select args, etc.).

### 24.2 Mutation model: transform first, copy when you need safety

SQLGlot supports recursive `transform()` patterns for rewriting trees (core usage in the README), and the optimizer itself is implemented as a pipeline of transforms.

**Rule of thumb for your codebase:**

* Prefer `expr.transform(fn)` for deterministic rewrites.
* Use `copy()` before heavy transforms if you need to preserve the original AST for debug bundles.

### 24.3 Metadata and comments are part of the AST surface

SQLGlot uses meta/comments to encode behavior (e.g., case sensitivity markers; simplifier “FINAL” semantics). Identifier normalization explicitly includes a case-sensitivity escape hatch via metadata comments.
Generator configuration also decides whether comments are preserved in emitted SQL. ([SQLGlot][7])

**Implementation pattern:**

* Treat `meta/comments` as part of your reproducibility bundle:

  * preserve them in “human artifacts”
  * generally drop them from “cache key emission” unless your pipeline intentionally uses comment-based directives

---

## 25) Serde: stable AST serialization for caching, diffs, and artifact bundles

### 25.1 `sqlglot.serde.dump` / `load` are the canonical AST serialization tools

SQLGlot provides a Serde module that:

* dumps an `Expression` into a JSON-serializable list
* loads that list back into an `Expression` ([SQLGlot][9])

**Minimal implementation pattern:**

```python
import json, hashlib
import sqlglot
from sqlglot.serde import dump, load

expr = sqlglot.parse_one("SELECT a FROM t", dialect="duckdb")
payload = dump(expr)  # JSON-serializable

fingerprint = hashlib.sha256(
    json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
).hexdigest()

expr2 = load(payload)  # reconstruct Expression
```

(Serde dump/load behavior is documented.) ([SQLGlot][9])

### 25.2 When to prefer Serde over hashing SQL strings

* Prefer **Serde** when you want fingerprints that are less sensitive to generator quirks.
* Prefer **SQL string hashing** when you want “fingerprint matches exactly what DataFusion will parse” and you pin generator options in the key.

In your pipeline, it’s often best to store *both*:

* Serde payload for structural identity
* emitted SQL for execution identity

---

## 26) Transform library: compatibility rewrites you’ll actually deploy

`sqlglot.transforms` is where many “dialect quirks” are handled as AST-to-AST rewrites. For your codebase (DataFusion execution + cross-dialect ingestion), these transforms become an explicit “compatibility lane.”

### 26.1 `move_ctes_to_top_level`: normalize CTE placement for dialect legality

Some dialects only allow top-level CTE definitions. SQLGlot provides a transformation that moves nested CTEs to the top level to produce syntactically valid SQL in those dialects. ([SQLGlot][10])

**When you use it:**

* If you ingest SQL from tools that nest CTEs under subqueries and you need to transpile to a dialect that forbids it.
* As a “normalize before transpiling” step, so your policy engine doesn’t depend on dialect-specific CTE rules.

### 26.2 `eliminate_semi_and_anti_joins`: normalize to EXISTS/NOT EXISTS

SQLGlot includes a transform to convert SEMI/ANTI joins into EXISTS forms. ([SQLGlot][10])

**Why you care:**

* Some dialects don’t support SEMI/ANTI joins directly.
* Even when supported, EXISTS forms can be more stable for certain rewrite analyses (e.g., predicate locality).

### 26.3 `eliminate_full_outer_join`: normalize FULL OUTER JOIN via UNION

SQLGlot can rewrite a query with a single FULL OUTER JOIN into a UNION of LEFT/RIGHT join forms. ([SQLGlot][11])

**Use case:**

* If your target dialect lacks FULL OUTER JOIN support (or you want a single canonical form across engines).

### 26.4 Recursive CTE hygiene: `add_recursive_cte_column_names`

SQLGlot includes a transform that fills recursive CTE column names based on projection output names. ([SQLGlot][10])

**Use case:**

* Cross-dialect recursive CTE portability.
* Stabilizing output column naming for lineage/diff.

### 26.5 Boolean normalization hook: `ensure_bools`

There is a transform to convert numeric expressions used as conditions into explicit boolean comparisons (e.g., `x` → `x <> 0`). ([SQLGlot][10])

**Use case:**

* Canonicalizing “truthy numeric” dialect behaviors into explicit boolean logic.

---

## 27) Pushdown and structural rewrites as *compiler primitives*

### 27.1 Projection pushdown: `pushdown_projections`

`pushdown_projections(expression, schema=None, remove_unused_selections=True, dialect=None)` removes unused inner projections and has explicit examples in the docs. ([SQLGlot][12])

**Your canonical use:**

* After qualification/star expansion, run projection pushdown to minimize columns scanned from Parquet (important for DataFusion performance).

### 27.2 Predicate pushdown: `pushdown_predicates`

SQLGlot’s `pushdown_predicates` rewrites ASTs to push predicates into inner scopes. The docs show it can replace the outer predicate with `WHERE TRUE` after pushing down, which is useful for stabilizing query structure for later comparisons and for letting the engine prune earlier. ([SQLGlot][13])

**Dialect-specific gotcha:**
The implementation checks dialect-specific behavior for UNNEST/join semantics (example: Athena/Presto requiring CROSS JOIN in some cases). ([SQLGlot][13])

**Policy implication:** always pass the intended dialect when running predicate pushdown, and treat this pass as part of the dialect-aware pipeline.

---

## 28) Lineage in production: scope caching, sources expansion, and query-wide dependency extraction

### 28.1 `lineage()` accepts `sources` and a pre-built `scope`

`sqlglot.lineage.lineage(...)` supports:

* `sources`: mapping of queries that lineage can expand into
* `scope`: a pre-created scope object to use instead of rebuilding it
* `trim_selects`: simplify selects to relevant columns ([SQLGlot][14])

This is the core interface you’ll build on.

### 28.2 Caching scopes for repeated lineage calls is a real pattern

Systems that compute lineage across many columns cache the `(query, scope)` pair, then pass the cached scope into repeated lineage calls to avoid rebuilding scopes and requalifying each time. SQLMesh’s lineage code does this explicitly (cache query + scope; call sqlglot lineage with `copy=False`). ([SQLMesh][15])

**Implementation pattern for your rulepacks:**

1. Canonicalize query once.
2. Build scope once.
3. For each output column:

   * call `lineage(col, sql=query, scope=scope, copy=False, ...)`

### 28.3 Query-wide “required columns” extraction

Two complementary approaches:

* **Lineage aggregation**: run lineage on each projected output; union the base columns.
* **Projection pushdown**: apply `pushdown_projections` to trim intermediate selects automatically, then traverse remaining `Column` nodes.

Both approaches should be applied on **qualified + canonicalized** SQL to avoid ambiguity and to stabilize results. ([SQLGlot][12])

---

## 29) Semantic diff in production: matchings, known limitations, and how to harden it

### 29.1 Diff is AST-based and emits an edit script

SQLGlot supports semantic diff of expressions (documented and demonstrated in the repo) and returns a sequence of actions to transform one expression into another. ([GitHub][1])

### 29.2 `matchings` parameter: seeding correspondences for better diffs

The diff implementation supports providing “pre-matched node pairs” (`matchings`) to guide the algorithm. ([GitHub][16])

**Why you care:**

* In large queries with many repeated subexpressions, seeding matchings can prevent unstable “node pairing” decisions that cause noisy diffs.

### 29.3 Known diff edge cases exist; treat diff as “strong signal,” not a proof

There are documented bug reports where diff considers two expressions equivalent even though a window construct differs (illustrating that semantic diff can have blind spots). ([GitHub][17])

**Hardening pattern (best-in-class):**

* Diff **after canonicalization** (qualify → normalize identifiers → simplify → canonicalize).
* Add domain-specific structural invariants:

  * explicitly compare window specs for winner-selection queries
  * explicitly compare join graph signatures for relationship builds
* Use the executor harness (§30) for semantic equivalence tests on small fixtures when diffs are ambiguous.

---

## 30) Semantic golden tests without a database: SQLGlot executor and planner

### 30.1 The SQLGlot engine exists for correctness testing, not performance

SQLGlot includes a Python SQL execution engine; it’s explicitly not meant to be fast, but can execute queries against Python dictionaries/tables. ([SQLGlot][18])

**Best use in your system:**

* Unit tests for rewrite correctness:

  * execute original SQL and rewritten SQL on a small deterministic dataset
  * assert same results (or same multiset of rows)

### 30.2 Executor configuration: identify/normalize/pretty

The executor tooling exposes generator-like knobs (`pretty`, `identify`, `normalize`) for SQL generation in execution contexts. ([SQLGlot][19])

**Test harness rule:** keep these knobs pinned in tests so output is stable across environments.

---

## 31) Dialect authoring: implementing a DataFusion dialect shim (when transforms aren’t enough)

### 31.1 Dialects are extensible (Tokenizer/Parser/Generator)

SQLGlot’s dialect framework is explicitly designed to bridge dialect variations with extensible Tokenizer/Parser/Generator components. ([SQLGlot][20])

Dialect docs (example: Hive) show that a dialect class exposes:

* Tokenizer settings (quotes, identifiers, keywords)
* Parser function mappings and dialect quirks
* Generator transforms and feature flags (including `TRANSFORMS`) ([SQLGlot][6])

### 31.2 “Transforms lane” vs “custom dialect” decision table

Use a custom dialect shim when:

* DataFusion has recurring syntax constraints that you repeatedly patch with transforms
* you want `expr.sql(dialect="datafusion")` to be stable and enforceable

Use a transforms lane when:

* DataFusion is close enough to an existing dialect and only needs a few targeted rewrites (QUALIFY elimination, UNNEST normalization, CTE placement, etc.) ([SQLGlot][10])

**Implementation sketch:**

* Subclass the nearest dialect (often DuckDB/Postgres-like).
* Override generator `TRANSFORMS` via `preprocess([...])` to apply your compatibility transforms consistently. ([SQLGlot][6])

---


[1]: https://github.com/tobymao/sqlglot?utm_source=chatgpt.com "tobymao/sqlglot: Python SQL Parser and Transpiler"
[2]: https://sqlglot.com/sqlglot/optimizer/optimizer.html?utm_source=chatgpt.com "sqlglot.optimizer.optimizer API documentation"
[3]: https://stackoverflow.com/questions/74860652/sqlglot-is-throwing-an-errow-when-trying-to-parse-a-parameter-in-a-sql-statement?utm_source=chatgpt.com "SQLGlot is throwing an errow when trying to parse a ..."
[4]: https://github.com/tobymao/sqlglot/issues/3950?utm_source=chatgpt.com "Failed to parse oracle /* /* */ comment · Issue #3950"
[5]: https://github.com/apache/superset/issues/36068?utm_source=chatgpt.com "SQL Parsing Error for Unmapped SQLGLOT_DIALECTS in ..."
[6]: https://sqlglot.com/sqlglot/dialects/hive.html "sqlglot.dialects.hive API documentation"
[7]: https://sqlglot.com/sqlglot/generator.html?utm_source=chatgpt.com "sqlglot.generator API documentation"
[8]: https://sqlglot.com/sqlglot/expressions.html?utm_source=chatgpt.com "sqlglot.expressions API documentation"
[9]: https://sqlglot.com/sqlglot/serde.html?utm_source=chatgpt.com "sqlglot.serde API documentation"
[10]: https://sqlglot.com/sqlglot/transforms.html "sqlglot.transforms API documentation"
[11]: https://sqlglot.com/sqlglot/transforms.html?utm_source=chatgpt.com "sqlglot.transforms API documentation"
[12]: https://sqlglot.com/sqlglot/optimizer/pushdown_projections.html?utm_source=chatgpt.com "sqlglot.optimizer.pushdown_projections API documentation"
[13]: https://sqlglot.com/sqlglot/optimizer/pushdown_predicates.html?utm_source=chatgpt.com "sqlglot.optimizer.pushdown_predicates API documentation"
[14]: https://sqlglot.com/sqlglot/lineage.html?utm_source=chatgpt.com "sqlglot.lineage API documentation"
[15]: https://sqlmesh.readthedocs.io/en/stable/_readthedocs/html/sqlmesh/core/lineage.html?utm_source=chatgpt.com "sqlmesh.core.lineage API documentation - Read the Docs"
[16]: https://github.com/tobymao/sqlglot/blob/main/sqlglot/diff.py?utm_source=chatgpt.com "diff.py - tobymao/sqlglot"
[17]: https://github.com/tobymao/sqlglot/issues/4192?utm_source=chatgpt.com "sqlglot.diff Bug for exp.Window Differences · Issue #4192"
[18]: https://sqlglot.com/?utm_source=chatgpt.com "sqlglot API documentation"
[19]: https://sqlglot.com/sqlglot/executor/python.html?utm_source=chatgpt.com "sqlglot.executor.python API documentation"
[20]: https://sqlglot.com/sqlglot/dialects.html?utm_source=chatgpt.com "sqlglot.dialects API documentation"
