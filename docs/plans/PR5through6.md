Below are **PR‑05** and **PR‑06** in the same “scope item” style as your `datafusion_engine_migration_plan.md`, but with **much denser technical narrative** and **no target file list**, per your request. 

---

## PR‑05 — Convert remaining plan builders (normalize / extract / cpg) to Ibis expressions

**Description**
Complete the “declarative plan lane” migration by rewriting any remaining plan builders in `normalize/`, `extract/`, and `cpg/` so they **return Ibis IR (table expressions)** (wrapped in your `IbisPlan` / `RelPlan` containers), instead of producing Arrow tables early, constructing ad‑hoc engine plans, or embedding bespoke procedural logic in these layers. This corresponds to the “Normalize, Extract, and CPG Plan Builders in Ibis” objective. 

This PR is the point where the project becomes *meaningfully* rules/relationship driven, because:

* every “dataset derivation” is now an **Ibis expression template**,
* relspec rules can **compose** these templates,
* SQLGlot tooling can analyze the full logical plan graph,
* and DataFusion can optimize/execute without you writing SQL strings.

---

### Code patterns (key snippets only)

#### 1) Canonical “plan builder” signature → returns IbisPlan (not Arrow)

```python
from dataclasses import dataclass
from ibis.expr.types import Table as IbisTable

@dataclass(frozen=True)
class PlanInputs:
    # stable accessors to registered tables (by dataset id)
    tables: dict[str, IbisTable]
    # (optional) prebuilt param tables, e.g. for list filters
    param_tables: dict[str, IbisTable]

def build_normalized_spans(inputs: PlanInputs) -> "IbisPlan":
    raw = inputs.tables["cst_spans_raw"]
    files = inputs.tables["repo_files"]
    expr = (
        raw.join(files, predicates=[raw.file_id == files.file_id], how="inner")
           .select(
               raw.span_id,
               raw.file_id,
               raw.line_start,
               raw.col_start,
               raw.line_end,
               raw.col_end,
               # ...derived columns...
           )
    )
    return IbisPlan(expr=expr, ordering=Ordering.unordered())
```

#### 2) Deterministic dedupe / “winner selection” via window + row_number

(you must do this anywhere “pick best candidate” exists)

```python
import ibis

w = ibis.window(
    group_by=[t.anchor_id, t.role],      # partition keys
    order_by=[t.score.desc(), t.source_rank.asc(), t.candidate_id.asc()],  # tie-breakers
)

ranked = t.mutate(_rn=ibis.row_number().over(w))
winners = ranked.filter(ranked._rn == 0).drop("_rn")
```

#### 3) “Evidence split” pattern (filter by kind + project contract columns)

```python
defs = events.filter(events.kind == "def").select("code_unit_id", "symbol", "event_id")
uses = events.filter(events.kind == "use").select("code_unit_id", "symbol", "event_id")
joined = defs.join(uses, predicates=[defs.code_unit_id == uses.code_unit_id], how="inner")
```

(This is the exact kind of conversion PR‑05 should finish everywhere.) 

#### 4) Optional inputs must compile to empty tables *with the correct schema*

```python
def empty_table(schema: ibis.Schema) -> ibis.expr.types.Table:
    # use a backend-friendly empty memtable pattern; schema must be explicit
    return ibis.memtable([], schema=schema)

t = inputs.tables.get("optional_dataset") or empty_table(OPTIONAL_SCHEMA)
```

#### 5) Complex transforms must be routed through the “hybrid kernel bridge”

(keep the plan declarative; do the “hard part” as DF UDF/UDTF or Arrow fallback)

```python
# shape: ibis expr → (optional) batch materialize → pyarrow.compute transform → memtable back
out = hybrid.apply_kernel(expr, kernel="explode_list_column", args={...})
```

---

### Narrative (dense technical intent + function-level guidance)

#### A) What PR‑05 is *really* doing

Right now, any remaining “plan builders” that aren’t expressed as Ibis IR create three systemic problems:

1. **They are not optimizable** by SQLGlot/DataFusion in the same global plan context
   If normalize/extract/cpg steps materialize Arrow early (or build bespoke plans), you’ve cut the plan graph, which prevents:

   * join reordering,
   * predicate pushdown into scans,
   * projection pruning,
   * and cost-based join strategy selection.

2. **They aren’t portable** across engines
   Your relspec compiler is converging on “engine agnostic” definitions. The moment a plan builder returns a concrete engine object (or Arrow table), you’ve violated the boundary.

3. **They block inference-driven extensibility**
   Your target state is: “add a relationship rule / edge kind, and the system recomputes what’s needed.”
   You don’t get that if half the derivations are implicit procedural code.

So PR‑05’s job is: *close the gap*—ensure everything that can be a relational operation becomes a relational expression.

---

#### B) Normalize plan builders → strict relational derivations

Normalization is where you convert “extractor-specific representations” into **join keys** and **stable IDs**.

PR‑05 should enforce these constraints in normalize builders:

1. **No Python loops over rows**

   * Anything row-wise should be represented as:

     * an Ibis expression, or
     * a DataFusion UDF/UDTF, or
     * an Arrow compute kernel invoked via the hybrid bridge.

2. **Stable ID generation must be expressed relationally**

   * Preferred: a deterministic “hash of canonicalized fields” expressed as Ibis ops.
   * If hashing primitives differ across engines, define a single canonical “id kernel” in the hybrid bridge:

     * input: record batches with canonicalized columns
     * output: `id64` / `id128` columns
   * Then treat `id64/id128` as contract outputs from normalize tables, not “runtime incidental”.

3. **Span normalization must be join-driven**

   * Any mapping from (line, col) → byte offsets should be computed via joins against a “line index” table (per file).
   * Ambiguity arises when:

     * tabs vs spaces or different newline normalization,
     * multi-byte encodings (UTF‑8) where “column” isn’t byte offset,
     * CST “synthetic” nodes (implied tokens) don’t correspond to real source slices.
   * Your normalize outputs must therefore add:

     * `confidence` (or `span_quality`) fields,
     * and preserve original coordinates for later debugging.

4. **Bytecode CFG/DFG plan builders**
   Bytecode-derived graphs often include:

   * nested/array-like structures (basic block sequences, edge lists),
   * explode/unnest transforms,
   * and multi-stage unions.

   The rule: keep the *shape* relational (tables of nodes/edges), and treat:

   * list explosions,
   * interval alignment,
   * or “decode instruction arg” logic
     as hybrid kernels (DataFusion UDTF if you implement, Arrow fallback otherwise).

---

#### C) Extract plan builders → “evidence shaping”, not “computation”

Extraction is the “ingress” of facts from AST/CST/SCIP/etc. It’s fine that the raw parsing is procedural, but **extract plan builders** should only do:

* projection,
* canonical renaming,
* splitting tables by “kind”,
* and applying repository/file filters (via PR‑06 params).

The only acceptable computations here are *schema shaping* computations, e.g.:

* deriving `qualified_name_candidates` from CST token segments (still relational string ops),
* normalizing file paths,
* standardizing symbol string forms for joins.

Everything else belongs in normalize or relspec rules.

---

#### D) CPG plan builders → should collapse into relspec rule outputs

Your `cpg/` layer should increasingly become:

* **node/edge emission from relationship datasets**, not bespoke joins.

But pragmatically, PR‑05 can:

1. convert remaining relationship plan builders into Ibis IR (so DataFusion can run them), and
2. move them into relspec as *named rules* where possible.

Key idea: once relationship derivations are Ibis expressions, the *same rules* can be reused to emit:

* edge tables,
* property tables,
* diagnostics tables.

So in PR‑05, any `cpg/relationship_plans.py` style artifacts should become either:

* `relspec` rules (preferred), or
* thin wrappers that just call the relspec compiler and return the rule output expression.

---

#### E) Required updates to the DataFusion adapter / SQLGlot tooling (don’t skip)

As you convert these plan builders, you will introduce expression constructs that *must* be supported consistently by:

* **SQLGlot compilation** (for diagnostics & optimization)
* **DataFusion df_builder** (for runtime SQL-free execution)

The most common “new constructs” PR‑05 will introduce:

* window functions (row_number / dense_rank),
* `CASE WHEN` (coalesce-style scoring),
* struct/array ops (if you represent candidates as list columns),
* string canonicalization functions.

Policy:

* if the DataFusion adapter doesn’t support a construct, **fail fast** with a “translator unsupported node” error that prints:

  * the SQLGlot subtree,
  * the source rule name,
  * and a suggested fallback (execute via backend SQL only in dev/debug).

---

### Implementation checklist (PR‑05)

* [ ] Inventory *all* remaining plan builders in normalize/extract/cpg and classify:

  * (a) purely relational → rewrite to Ibis directly
  * (b) relational + needs explode/interval align/ID hashing → route those pieces through hybrid kernel bridge
  * (c) truly non-relational → keep procedural, but move out of plan builders (should be rare)
* [ ] For each builder:

  * [ ] Rewrite as `IbisTable` expression chain (`select`, `filter`, `mutate`, `join`, `aggregate`, `union`, `order_by`)
  * [ ] Wrap in `IbisPlan(expr=..., ordering=...)`
  * [ ] Apply contract projection at the end (explicit column list; explicit aliases)
* [ ] Replace any “winner selection” logic with window-based ranking + deterministic tie-breakers
* [ ] Enforce optional-input behavior: missing datasets compile to schema-correct empty tables
* [ ] Add/extend SQLGlot snapshot diagnostics for each converted builder
* [ ] Extend DataFusion df_builder translator for any newly used SQLGlot/Ibis nodes
* [ ] Run end-to-end: parquet outputs (schemas + column names + row counts) unchanged

**Status**: Proposed → ready to implement

---

---

## PR‑06 — Parameterization everywhere (Ibis params + memtable-join for list params)

**Description**
Eliminate all SQL string interpolation / ad-hoc conditional query assembly by standardizing on:

* **Ibis scalar parameters** for thresholds / toggles / limits, and
* **memtable + join** patterns for “list parameters” (file subsets, symbol subsets, allowlists/denylists, etc.).

This corresponds directly to “Parameterization and Safe Execution (Ibis Params)” and the explicit “memtable + join” approach in your migration plan. 

This PR is not just about safety—it is what makes your pipeline **cacheable**, **replayable**, and **plan-hash stable**:

* the compiled rule plan stays the same,
* only bound param values change at runtime.

---

### Code patterns (key snippets only)

#### 1) Scalar parameter templates (never inline Python values into expressions)

```python
import ibis

threshold = ibis.param("int64").name("min_score")   # stable name matters at scale
expr = table.filter(table.score >= threshold)

result = con.execute(expr, params={threshold: 10})
```

(Exactly aligned with your plan doc’s intent.) 

Also note: Ibis’s param binding is explicitly designed as a mapping `{ScalarParamExpr: value}`. 

#### 2) List parameter filtering (canonical “memtable + join”, not `IN (...)`)

```python
ids_tbl = ibis.memtable({"file_id": file_ids}).distinct()

# prefer semi-join semantics if your backend supports it; otherwise inner-join + select distinct
filtered = t.join(ids_tbl, predicates=[t.file_id == ids_tbl.file_id], how="semi")
```

#### 3) Optional list param (empty list must compile to “empty result”, not full scan)

```python
ids_tbl = ibis.memtable({"file_id": file_ids})
filtered = t.join(ids_tbl, predicates=[t.file_id == ids_tbl.file_id], how="inner")

# If file_ids is empty, ids_tbl is empty => filtered is empty (correct semantics).
```

#### 4) Binding parameters in the SQLGlot→DataFusion lane (no SQL strings, but you *must* substitute)

When your runtime executes via:

* `Ibis expr -> SQLGlot AST -> DataFusion DataFrame ops`
  you need a param binder that rewrites SQLGlot nodes before df_builder executes.

SQLGlot’s `transform()` visitor is the correct primitive for this style of rewrite:

```python
from sqlglot import exp

def bind_params(ast: exp.Expression, values: dict[str, object]) -> exp.Expression:
    def _visit(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Parameter):
            name = node.name  # (or node.this depending on dialect)
            return exp.Literal.string(values[name])  # or numeric literal
        return node
    return ast.transform(_visit)
```

(SQLGlot supports AST-wide transforms explicitly; this is the intended pattern for programmatic rewrites.) 

---

### Narrative (dense technical intent + function-level guidance)

#### A) Why PR‑06 is mandatory for the “rules engine” endgame

Your target architecture depends on these properties:

1. **Plan templates are stable**
   Rule compilation must produce a deterministic plan graph so you can:

   * hash it (cache key),
   * diff it (migration regression),
   * store it in run bundles,
   * and reason about it independent of a specific repo run.

2. **Runtime variability must not change the plan structure**

   * Filtering to a subset of files must not re-render the plan in structurally different ways.
   * Avoiding SQL strings is not just “cleaner”—it prevents you from accidentally creating unbounded plan variants.

Scalar params solve this for single values; list params are the real footgun—hence memtable+join.

---

#### B) Scalar parameters: strict rules

Scalar parameters must be used for:

* thresholds (“min_score”, “max_candidates”),
* boolean toggles (“include_experimental_edges”),
* runtime knobs used inside expressions (“confidence_floor”).

**Do not**:

* bake literal thresholds into expressions,
* or use Python `if` to build different expression shapes.

Instead:

* keep one expression template,
* bind values at execution.

Practically:

* every rule that depends on a runtime scalar should accept a `ParamSpec` reference (not a raw value),
* and the engine runner should take responsibility for binding.

---

#### C) List parameters: why “memtable + join” is the correct canonical form

Using `col.isin(list)` (or SQL `IN (...)`) creates problems:

* huge SQL literals (or huge expression trees),
* poor engine planning (especially if the list is long),
* and it destroys plan-hash stability because list length changes the plan rendering.

Using a memtable:

* produces a stable plan shape (a join against a small table),
* is engine-native (hash join / broadcast join),
* and is easy to cache/register per run.

**Best practice for list params in your system**:

* represent list params as a first-class “param table dataset” registered into the backend/session
* and referenced by name in expressions/rules.

That means:

* PR‑06 should introduce a `ParamTableRegistry` (even if very small) that:

  * builds an in-memory Arrow table (or Ibis memtable),
  * registers it under a stable deterministic name,
  * and exposes an Ibis table expression handle to the rule layer.

---

#### D) Parameter binding in the DataFusion SQL-free execution lane (critical integration point)

If you rely on `con.execute(expr, params=...)`, Ibis will handle binding for backends that support it. But your key differentiator is:

> Ibis → SQLGlot AST → DataFusion DataFrame ops (no SQL strings)

That means *you* need to ensure parameter values are applied before translating to DataFusion ops, otherwise your df_builder sees unresolved parameter nodes.

There are two robust patterns:

**Pattern 1 (recommended): SQLGlot AST substitution pre-translation**

* compile Ibis expr → SQLGlot
* qualify/optimize (still parameterized)
* bind params (rewrite parameter nodes to literals)
* translate bound SQLGlot → DataFusion DataFrame

This is where SQLGlot `transform()` is the correct low-level primitive. 

**Pattern 2: translator-level binding**

* keep AST parameter nodes
* df_builder has a `params` map and translates parameters directly to `datafusion.functions.lit(value)` expressions

Pattern 1 is simpler to reason about and debug (because you can snapshot the post-bind SQLGlot tree), and it aligns with your “diagnostics artifact” philosophy.

---

#### E) Dedupe + winner selection interactions (PR‑05 dependency)

List params affect winner selection because “candidate universe” changes. Your pipeline must preserve:

* deterministic ordering inside partitions,
* and deterministic scoring tie-breakers,
  independent of which subset is selected.

So PR‑06 must also standardize:

* “source_rank” fields,
* stable tie-break columns,
* and guarantee the memtable join doesn’t introduce duplicates (hence `.distinct()` on the param memtable).

---

### Implementation checklist (PR‑06)

* [ ] Define a parameter model:

  * [ ] `ScalarParamSpec(name, ibis_dtype, default, required)`
  * [ ] `ListParamSpec(name, key_schema, distinct=True, empty_semantics="empty_result")`
* [ ] Create a param registry API used by Hamilton entrypoints to assemble runtime params
* [ ] Replace any SQL-string formatting or conditional SQL building with Ibis params + bindings
* [ ] Replace all list filters (`isin`, `IN`) with memtable+join

  * [ ] enforce `.distinct()` on param tables by default
  * [ ] define empty-list semantics explicitly (should typically yield empty result)
* [ ] Add a SQLGlot param binder pass (AST rewrite) in the DataFusion execution pathway
* [ ] Add a “no SQL interpolation” guardrail:

  * [ ] grep-based test (no f-strings into SQL)
  * [ ] runtime assertion that compiled SQLGlot contains no raw “IN (…many…)” for list filters
* [ ] Ensure params are recorded in run bundles/manifests for reproducibility

**Status**: Proposed → ready to implement

---

If you want the next increment after PR‑06, I’d do a **small “param-table caching + backend registration policy” follow-up** (often folded into PR‑06) that decides:

* whether list param tables are registered per-rule, per-run, or per-session,
* and how they are named so SQLGlot lineage remains stable and human-readable.


## PR‑06b — Param-table caching + backend registration policy (stable names + stable lineage)

This is the natural “tightening” follow-up to PR‑06: once you standardize on **memtable + join** for list params, you must decide (a) *where* those param tables live (per-rule/per-run/per-session) and (b) how they’re named/registered so **SQLGlot lineage stays stable** and the engine can reuse them efficiently. Your migration plan already points at the key idea: for large “IN lists”, create an in-memory relation and (when you need stable names) explicitly materialize names via `create_table`/`create_view`.  

### Description

Add a **ParamTableRegistry** layer that:

1. Implements a **registration scope policy**:

* **PER_RUN (default)**: param tables registered once per run/session and reused across all rules.
* **PER_SESSION (service mode)**: param tables namespaced per request/run and cleaned up.
* **PER_RULE (escape hatch only)**: param tables embedded as `ibis.memtable(...)` inside a single expression (allowed only for *tiny* lists; discouraged for anything large).

2. Implements a **stable naming policy** for SQLGlot lineage and human readability:

* A fixed schema like `params` (or `__params`) containing stable tables/views such as:

  * `params.file_allowlist`
  * `params.symbol_allowlist`
  * `params.rule_allowlist`
* The rule graph should reference these names consistently so lineage doesn’t explode.

3. Implements **param-table caching**:

* Param tables are created once per run and stored in-memory; rules join against them.
* Produce a `ParamSignature` (hash of canonical values) so:

  * plan templates remain stable,
  * but materialization caches can vary by param values.

---

## Code patterns (key snippets only)

### 1) Policy objects (scope + naming)

```python
from dataclasses import dataclass
from enum import Enum

class ParamTableScope(Enum):
    PER_RUN = "per_run"
    PER_SESSION = "per_session"
    PER_RULE = "per_rule"  # discouraged: only for tiny lists

@dataclass(frozen=True)
class ParamTablePolicy:
    scope: ParamTableScope = ParamTableScope.PER_RUN
    catalog: str = "codeintel"
    schema: str = "params"
    prefix: str = "p_"  # final table: p_file_allowlist
```

### 2) Canonical name builder (SQLGlot‑friendly)

```python
import re

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def param_table_name(policy: ParamTablePolicy, logical_name: str) -> str:
    # logical_name: "file_allowlist" / "symbol_allowlist"
    if not _IDENT.match(logical_name):
        raise ValueError(f"invalid param logical name: {logical_name!r}")
    return f"{policy.prefix}{logical_name}"
```

### 3) Param signature (separate from plan signature)

```python
import hashlib, json

def param_signature(*, logical_name: str, values: list[object]) -> str:
    # canonicalize: stable sort + stable json encoding
    payload = {"name": logical_name, "values": sorted(values)}
    s = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()
```

### 4) DataFusion: register the param table into a dedicated schema

DataFusion’s catalog API supports a clean `catalog.schema.table` hierarchy, and you can register in-memory tables into schemas (and deregister them when reusing names). 

```python
from datafusion.catalog import Catalog, Schema
import pyarrow as pa

def ensure_param_schema(ctx, *, catalog_name: str, schema_name: str):
    # One-time: attach catalog + schema to the session context (if not already)
    # Uses Catalog.memory_catalog / Schema.memory_schema patterns.
    ...

def register_param_arrow_table(ctx, *, catalog: str, schema: str, table_name: str, arrow: pa.Table):
    cat = ctx.catalog(catalog)
    sch = cat.schema(schema)
    # Optional: if overwriting, deregister first (Schema supports deregister_table).
    # sch.deregister_table(table_name)
    sch.register_table(table_name, arrow)
```

### 5) Ibis: stable-name strategy (avoid “giant IN lists”)

Ibis’ recommended robust pattern for large variable-length sets is **memtable + join**, and it explicitly notes you should `create_table/create_view` when you need stable naming (memtable naming constraints). 

```python
import ibis

def filter_by_param_table(t, *, param_t, key: str):
    # Prefer SEMI join if supported by backend; otherwise INNER + select distinct
    return t.join(param_t, predicates=[t[key] == param_t[key]], how="semi")
```

---

## Narrative (dense “how to get it exactly right”)

### A) The core tension you’re resolving

PR‑06 gives you *how to express list filters safely*; PR‑06b decides *how to operationalize them* so you preserve:

1. **Plan template stability** (compile once, execute many)
   If you embed list values directly into expressions (`IN (...)` or large literal lists), you create:

* huge expression trees,
* unstable plan signatures,
* unstable SQLGlot lineage,
* and you destroy your ability to cache compiled artifacts meaningfully.

2. **Lineage readability**
   When SQLGlot inspects the graph, you want to see:

* `... JOIN params.p_file_allowlist ...`
  not:
* a 10,000‑literal `IN` expression or a synthetic anonymous memtable.

3. **Engine reuse**
   Registering param tables once per run lets every rule reuse them without repeated construction.

---

### B) Recommended policy: **PER_RUN default**, **PER_SESSION for services**, **PER_RULE only as escape hatch**

#### PER_RUN (default)

* Assumption: one DataFusion `SessionContext` per run (CLI / batch pipeline).
* You create param tables exactly once per run, register them into `codeintel.params.*`, and reuse everywhere.
* SQLGlot lineage is stable/human-readable because the table name is stable (values are data, not plan text).

This is the sweet spot for your architecture.

#### PER_SESSION (service mode)

If you keep a long-lived session and serve multiple requests/runs, stable names collide. You have two good options:

**Option 1: Run‑scoped schema**

* Create schema per run: `params_run_<run_id>`
* Register tables there.
* For stable lineage, also create stable *views* (or stable aliases) in `params`:

  * `params.p_file_allowlist → SELECT * FROM params_run_<run_id>.p_file_allowlist`
    If views are awkward in your current adapter, you can approximate by:
* deregistering and re-registering `params.p_file_allowlist` for each request (safe if single-threaded per ctx, or ctx-per-request).

**Option 2: Unique table names + stable SQL alias**

* Register physical table name includes short run id: `p_file_allowlist__<rid>`
* In rule compilation, always alias it to `params.p_file_allowlist` at the SQLGlot layer (so lineage stays stable).
  This requires your SQLGlot qualification/optimizer layer to preserve aliasing consistently.

#### PER_RULE (discouraged)

* Only acceptable for **tiny** lists (think: 1–50 IDs), where you don’t care about plan cache or lineage blow-up.
* It’s still useful as an emergency fallback when:

  * you don’t have access to a session registry in a particular layer,
  * or you’re doing a quick dev-only ad hoc filter.

But PER_RULE should *never* be the default because it makes plans value-dependent.

---

### C) Naming: make the param tables *first-class “datasets”*

This is the “programmatic rules engine” move:

* Treat param tables as **datasets** with contracts:

  * `params_file_allowlist(file_id: int64)` etc.
* They are registered like any other dataset, but their source is:

  * a runtime value bundle, not a parquet path.

That means:

* the compiler can validate “joins use the correct key type”,
* the DAG can include “param table nodes” (materializers or debug outputs),
* and your run bundle can record the param signature + row counts.

For SQLGlot lineage stability, you want consistent, boring names:

* schema: `params`
* tables: `p_file_allowlist`, `p_symbol_allowlist`, etc.

DataFusion supports clean namespacing using `catalog.schema.table` and catalog-aware registration. 

---

### D) Caching: decouple *plan signature* from *param signature*

Your relspec plan signature should not vary with runtime param values; otherwise caches don’t hit. Your **cache key** should be something like:

* `cache_key = (plan_signature, runtime_profile_fingerprint, param_signature_bundle)`

Where:

* `plan_signature` = canonicalized RelPlan / SQLGlot AST hash (stable)
* `param_signature_bundle` = hash of all param signatures used by that rule (stable per run)

This also integrates perfectly with your run bundle approach:

* record the param signatures and row counts per param table for reproduction.

---

## Implementation checklist (PR‑06b)

### 1) Add the registry + policy surface

* [ ] Implement `ParamTablePolicy(scope, catalog, schema, prefix)` and thread it into your `ExecutionContext`.
* [ ] Implement `ParamTableRegistry` with:

  * `register_list_param(name, arrow_table, key_cols, signature)`
  * `get_ibis_table(name)` / `get_qualified_name(name)`
  * `debug_dump()` (names, schemas, row counts, signatures)

### 2) DataFusion integration (catalog-aware registration)

* [ ] Ensure the `params` schema exists at session init (or lazily on first param registration).
* [ ] Register Arrow tables into `catalog.schema` using schema API and stable table names. 
* [ ] Implement overwrite behavior for PER_SESSION reuse:

  * deregister old table name before re-registering.
* [ ] Emit diagnostics: `param_table_registered(name, rows, signature)`.

### 3) Ibis integration (stable handles)

* [ ] Ensure rules can reference param tables as stable Ibis table expressions (by name).
* [ ] Standardize list filtering in one helper: `semi_join_to_param_table(...)`.
* [ ] Enforce `.distinct()` / canonical ordering for param table creation where appropriate. 

### 4) Compiler + SQLGlot lineage stability

* [ ] Ensure SQLGlot qualification sees `catalog.schema.table` (or consistent schema.table) for param tables.
* [ ] Add a regression guard: compiled lineage must include `params.*` table references rather than literal IN lists.

### 5) Reproducibility

* [ ] Add param signatures + row counts to run bundles / manifests.
* [ ] Add an option to dump param table contents (small lists) into repro bundles for debugging.

### Watchouts

* **Concurrency**: if you’re in service mode, avoid sharing a mutable `SessionContext` across concurrent requests unless your policy namespaces tables per request (or you allocate ctx per request).
* **Empty lists**: ensure semantics are explicit and correct (usually “empty list → empty result”). The memtable join pattern naturally achieves this if the param table is empty.
* **Key types**: the param table key column type must exactly match the dataset key type; enforce with contracts early, not at runtime.

---

If you want, the next increment after this is to treat param tables as **first-class Hamilton nodes** (so `params.p_file_allowlist` can be explicitly materialized/debugged and shows up in the same manifest + caching system as every other dataset)—but PR‑06b is the prerequisite that makes that clean.


## PR‑06c — Param tables as first‑class Hamilton nodes (materializable + manifest-visible + cacheable)

This PR turns list/scalar params into **explicit Hamilton nodes** so they become: (a) **debuggable artifacts**, (b) **manifest/run‑bundle visible**, and (c) **eligible for caching/materialization policies** exactly like any other dataset. This is a direct continuation of PR‑06/PR‑06b and fits the migration-plan style you referenced.  

---

### Description

Right now, even after PR‑06b, parameter tables can still feel “out-of-band” because they’re created in the execution layer (or registry) and then *used* by rules.

To make the system truly inference-driven and reproducible, param tables must be treated as **first-class datasets** in the DAG:

* **Inputs**: runtime param values (from CLI/config/service request)
* **Derived datasets**: Arrow tables representing list params (e.g., file allowlists)
* **Side-effects (optional)**: write param tables to Parquet for inspection/replay
* **Registration**: register param tables into DataFusion `catalog.schema` so rules can join by stable names
* **Observability**: record param signatures, row counts, schema fingerprints, and optionally contents snapshot in the run bundle

This achieves the key goal: **any change to parameter semantics is visible and testable**, and any agent can request “materialize params” outputs to understand what the run did.

---

## Scope 1 — ParamBundle node (canonical, typed input surface)

### Code patterns

**1) Define a single structured param bundle**

```python
from dataclasses import dataclass
from typing import Sequence

@dataclass(frozen=True)
class ParamBundle:
    # scalar params
    min_score: int = 0
    max_candidates: int = 50

    # list params
    file_allowlist: tuple[int, ...] = ()
    symbol_allowlist: tuple[str, ...] = ()
```

**2) Hamilton input node for params**

```python
def param_bundle(run_config: "RunConfig") -> ParamBundle:
    # parse/validate; keep as pure function
    return run_config.params
```

### Narrative (dense)

* This makes params a **typed boundary** instead of a free-form dict.
* It enforces “no ad hoc runtime knobs in random modules”—all params flow from **one canonical node**, so DAG inference and repro bundles stay coherent.
* Treat this as an **API**: backward compatibility matters, and changes should be reflected in manifest schema.

### Implementation checklist

* [ ] Introduce `ParamBundle` dataclass (or equivalent typed structure).
* [ ] Create Hamilton node `param_bundle`.
* [ ] Ensure `RunConfig` (or equivalent) has a stable serialization form for bundling.

---

## Scope 2 — ParamTableSpec nodes (declarative param→table mapping)

### Code patterns

**1) Param table specs as declarative objects**

```python
from dataclasses import dataclass
import pyarrow as pa

@dataclass(frozen=True)
class ParamTableSpec:
    logical_name: str               # e.g. "file_allowlist"
    key_col: str                    # e.g. "file_id"
    schema: pa.Schema               # e.g. pa.schema([("file_id", pa.int64())])
    empty_semantics: str = "empty_result"  # or "no_filter"
    distinct: bool = True
```

**2) Hamilton node that emits all specs**

```python
def param_table_specs() -> tuple[ParamTableSpec, ...]:
    return (
        ParamTableSpec("file_allowlist", "file_id", pa.schema([("file_id", pa.int64())])),
        ParamTableSpec("symbol_allowlist", "symbol", pa.schema([("symbol", pa.large_string())])),
    )
```

### Narrative (dense)

* This is the “rules/registry” concept applied to params: **specs are static**, values are dynamic.
* Specs become the place to encode:

  * join key name/type,
  * empty-list semantics (critical),
  * and constraints like distinctness.
* This is also where you standardize “human readable lineage names” (the `logical_name` becomes the canonical dataset identity).

### Implementation checklist

* [ ] Add `ParamTableSpec`.
* [ ] Add Hamilton node `param_table_specs`.
* [ ] Ensure this spec list is captured in run bundle (as JSON + schema fingerprints).

---

## Scope 3 — Build Arrow param tables as dataset nodes (pure, deterministic)

### Code patterns

**1) Construct Arrow tables with explicit schema**

```python
import pyarrow as pa

def build_param_table(spec: ParamTableSpec, values: list[object]) -> pa.Table:
    arr = pa.array(values, type=spec.schema.field(spec.key_col).type)
    t = pa.table({spec.key_col: arr}, schema=spec.schema)
    if spec.distinct:
        # distinct via compute (engine-independent)
        import pyarrow.compute as pc
        t = t.take(pc.unique_indices(t[spec.key_col]))
    return t
```

**2) Hamilton node: emit mapping of logical_name → Arrow table + signature**

```python
from dataclasses import dataclass
import hashlib, json

@dataclass(frozen=True)
class ParamTableArtifact:
    logical_name: str
    table: pa.Table
    signature: str
    rows: int

def param_tables_arrow(param_bundle: ParamBundle, specs: tuple[ParamTableSpec, ...]) -> dict[str, ParamTableArtifact]:
    out = {}
    for spec in specs:
        values = list(getattr(param_bundle, spec.logical_name))
        t = build_param_table(spec, values)
        sig_payload = {"name": spec.logical_name, "values": values}
        sig = hashlib.sha256(json.dumps(sig_payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        out[spec.logical_name] = ParamTableArtifact(spec.logical_name, t, sig, t.num_rows)
    return out
```

### Narrative (dense)

* This makes param tables **pure derivations** in the DAG (no engine calls, no side-effects).
* It ensures:

  * explicit schema (no inference surprises),
  * engine-independent distinct semantics (Arrow compute),
  * stable signatures (for caching and for run bundle “what changed?” diffs).
* Crucially, it decouples:

  * **plan signature** (rule logic) from
  * **param signature** (runtime values).

### Implementation checklist

* [ ] Implement Arrow table construction with explicit schema coercion.
* [ ] Enforce `.distinct()` behavior in Arrow, not backend-specific SQL.
* [ ] Compute and store `ParamTableArtifact.signature`.
* [ ] Add a test: empty list yields `rows=0` and correct schema.

---

## Scope 4 — Register param tables into DataFusion as stable named tables (controlled side-effect)

### Code patterns

**1) A single “registration” node that mutates the session**

```python
def register_param_tables_df(
    df_ctx: "SessionContext",
    param_tables_arrow: dict[str, ParamTableArtifact],
    policy: "ParamTablePolicy",
) -> dict[str, str]:
    """
    Returns mapping logical_name -> fully qualified registered name.
    Side-effect: registers tables into df_ctx catalog/schema.
    """
    ensure_param_schema(df_ctx, catalog=policy.catalog, schema=policy.schema)

    mapping = {}
    for logical_name, art in param_tables_arrow.items():
        table_name = f"{policy.prefix}{logical_name}"   # p_file_allowlist
        register_param_arrow_table(
            df_ctx, catalog=policy.catalog, schema=policy.schema, table_name=table_name, arrow=art.table
        )
        mapping[logical_name] = f"{policy.catalog}.{policy.schema}.{table_name}"
    return mapping
```

**2) Provide Ibis handles by name (so rules can join cleanly)**

```python
import ibis

def param_tables_ibis(
    ibis_con: "ibis.BaseBackend",
    df_param_name_map: dict[str, str],
) -> dict[str, ibis.expr.types.Table]:
    out = {}
    for logical_name, fqname in df_param_name_map.items():
        # backend should resolve schema-qualified tables; or you provide a helper that splits fqname
        out[logical_name] = ibis_con.table(fqname.split(".")[-1], schema=fqname.split(".")[-2])
    return out
```

### Narrative (dense)

* Registration is a **controlled side-effect** and should be isolated to one or two nodes:

  * it makes it trivial to reason about “what touched the engine state”.
* Registering into a dedicated `params` schema yields:

  * stable SQLGlot lineage (you’ll see `params.p_file_allowlist`),
  * predictable join performance (small hash tables),
  * and easy inspection (list tables are visible to `SHOW TABLES` and info schema).
* In service mode (PER_SESSION policy), this node is where you enforce:

  * schema-per-run isolation or
  * overwrite/deregister semantics.

### Implementation checklist

* [ ] Add `register_param_tables_df` node and thread it into the pipeline.
* [ ] Ensure catalog/schema exist before registration.
* [ ] Decide overwrite behavior (deregister then register) for per-session reuse.
* [ ] Emit diagnostics event per table registered: rows, signature, schema fingerprint.
* [ ] Ensure rules never call “register memtable” themselves—rules only see `param_tables_ibis`.

---

## Scope 5 — Param tables as materializable debug outputs (Parquet + replay)

### Code patterns

**1) Optional materializer node: write param tables to Parquet**

```python
from pathlib import Path

def write_param_tables_parquet(
    work_dir: Path,
    param_tables_arrow: dict[str, ParamTableArtifact],
) -> dict[str, str]:
    out = {}
    for logical_name, art in param_tables_arrow.items():
        ds_dir = work_dir / "params" / logical_name
        ds_dir.mkdir(parents=True, exist_ok=True)
        # simplest: one file per param table (they’re small)
        path = ds_dir / "part-0.parquet"
        import pyarrow.parquet as pq
        pq.write_table(art.table, path)
        out[logical_name] = str(ds_dir)
    return out
```

**2) Optional replay mode: register Parquet param datasets instead of in-memory**

```python
def register_param_tables_from_parquet(df_ctx, param_table_paths: dict[str, str], policy: ParamTablePolicy):
    for logical_name, ds_dir in param_table_paths.items():
        table_name = f"{policy.prefix}{logical_name}"
        df_ctx.register_parquet(table_name, ds_dir)  # or listing provider with options
```

### Narrative (dense)

* This is the key operational win: param tables become **debuggable artifacts**.
* When a downstream join seems “wrong”, you can:

  * request param table materialization,
  * inspect it as Parquet,
  * replay the run with the same param datasets even if upstream config code changes.
* This also allows “param-driven incremental runs”: persist param sets used for a set of rules and replay them later.

### Implementation checklist

* [ ] Add a `write_param_tables_parquet` materializer node.
* [ ] Add a “replay param tables from parquet” mode gated by config.
* [ ] Ensure schema fingerprints are identical between in-memory and parquet reads.

---

## Scope 6 — Manifest + run bundle integration (params become first-class provenance)

### Code patterns

**1) Add params block to manifest**

```python
def manifest_params_block(param_tables_arrow: dict[str, ParamTableArtifact]) -> dict:
    return {
        name: {"rows": art.rows, "signature": art.signature, "schema": art.table.schema.to_string()}
        for name, art in param_tables_arrow.items()
    }
```

**2) Include param table artifacts in run bundle**

* `params/specs.json`
* `params/signatures.json`
* optional `params/<name>/part-0.parquet` (if materialized)

### Narrative (dense)

* This is what makes the pipeline *auditable*: you can answer “what filters were applied?” as a manifest query.
* It also enables cache policies:

  * rule output caching keyed by `(plan_signature, param_signature_bundle)`.
* You should treat param tables as “inputs” for provenance purposes even though they are generated from config; they control effective dataset semantics and must be recorded.

### Implementation checklist

* [ ] Add param table signatures/row counts to manifest writer.
* [ ] Add param spec dump + param signature dump to run bundle.
* [ ] Optional: include materialized param parquet contents if a debug flag is enabled.

---

## Scope 7 — Make rule compilation consume param tables as declared datasets (no “memtable inline”)

### Code patterns

**1) PlanInputs contains `param_tables`**

```python
@dataclass(frozen=True)
class PlanInputs:
    tables: dict[str, ibis.expr.types.Table]
    param_tables: dict[str, ibis.expr.types.Table]
```

**2) List filtering is always a semi-join**

```python
def apply_file_allowlist(t, inputs: PlanInputs):
    p = inputs.param_tables["file_allowlist"]
    return t.join(p, predicates=[t.file_id == p.file_id], how="semi")
```

### Narrative (dense)

* This is what stabilizes SQLGlot lineage and plan signatures:

  * the query always references `params.p_file_allowlist`, regardless of list size.
* It also makes param usage analyzable:

  * “which rules depend on file_allowlist?” becomes a lineage query.
* This is the final removal of “parameter handling scattered in rule code”.

### Implementation checklist

* [ ] Update the plan compiler / rule runtime so `PlanInputs` includes param tables.
* [ ] Replace any remaining inline `ibis.memtable(list)` usage inside rule compilation with references to `inputs.param_tables`.
* [ ] Add a validation: rules must declare which param tables they require (so Hamilton can infer dependencies cleanly).

---

# Final notes / watchouts (important)

1. **Empty-list semantics must be explicit**
   Some param lists should mean “empty result” (allowlist), others might mean “no filter” (optional filter). Encode this in `ParamTableSpec.empty_semantics` and apply it consistently (e.g., bypass join if “no_filter” and list empty).

2. **Concurrency and session lifetimes**
   If you run multi-request service mode, you must avoid name collisions in a shared session. The clean answer is either:

* one `SessionContext` per request/run, or
* run-scoped schema naming + stable view aliasing.

3. **Type alignment is non-negotiable**
   Param table schema must match join key types exactly. Enforce at construction time, not after weird join failures.

4. **Plan vs param signatures**
   Don’t poison plan caching with param values. Param tables should vary at runtime, but the plan template stays constant.

---

If you want, the next logical PR after this is: **“param dependency inference”** — automatically derive which rules require which param tables by SQLGlot lineage scanning (table reference extraction), so Hamilton can wire param nodes into the DAG without manual dependency declarations.

## PR‑06d — Param dependency inference (SQLGlot lineage scan → auto-wired Hamilton deps)

**Description**
Make param-table dependencies **fully automatic** by inferring, for each rule, which `params.*` tables it references via **SQLGlot AST table-reference extraction**. Then use that inferred dependency map to wire the correct **param-table Hamilton nodes** (from PR‑06c) into the DAG *without manual per-rule declarations*.

This PR directly builds on your migration plan’s intent to use **SQLGlot for analysis/optimization/lineage** (and to keep runtime SQL-free via an Ibis→SQLGlot→DataFusion adapter).

It also aligns with the typed relational IR concept that already recognizes `"param"` as a first-class op kind—this PR effectively operationalizes that idea by making param usage discoverable even when rule authors don’t explicitly annotate it.

---

### Code patterns (key snippets only)

### 1) Extract *table references* from the optimized SQLGlot AST

SQLGlot supports traversing an AST and collecting `Table` nodes using `find_all(exp.Table)`—that’s the correct primitive for stable metadata extraction.

```python
from dataclasses import dataclass
from sqlglot import exp

@dataclass(frozen=True)
class TableRef:
    catalog: str | None
    schema: str | None
    name: str

def extract_table_refs(sqlglot_ast: exp.Expression) -> set[TableRef]:
    refs: set[TableRef] = set()
    for t in sqlglot_ast.find_all(exp.Table):
        # SQLGlot stores parts like: catalog/db/name (dialect dependent).
        # Use a helper to normalize; be defensive for None.
        refs.add(
            TableRef(
                catalog=getattr(t, "catalog", None) or t.args.get("catalog"),
                schema=getattr(t, "db", None) or t.args.get("db"),
                name=t.name,
            )
        )
    return refs
```

**Critical detail:** run this extraction **after** your SQLGlot `qualify()` + `optimize()` pass so schema/db fields are as complete as possible (and CTE/alias confusion is reduced). This is already the planned diagnostics flow.

---

### 2) Identify param tables using your policy (`schema="params"`, prefix like `p_`)

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class ParamDep:
    logical_name: str          # e.g. "file_allowlist"
    table_name: str            # e.g. "p_file_allowlist"
    schema: str                # "params"
    catalog: str | None        # usually your default catalog

def infer_param_deps(
    table_refs: set[TableRef],
    *,
    param_schema: str,
    param_prefix: str,
) -> set[ParamDep]:
    out: set[ParamDep] = set()
    for r in table_refs:
        if r.schema != param_schema:
            continue
        if not r.name.startswith(param_prefix):
            continue
        logical = r.name[len(param_prefix):]
        out.add(ParamDep(logical, r.name, r.schema, r.catalog))
    return out
```

---

### 3) Validate inferred deps against the declared ParamTableSpec registry

If a rule references a param table you haven’t declared/spec’d, you want a **compile-time** failure (not a runtime “table not found”).

```python
class ParamSpecRegistry:
    def has(self, logical_name: str) -> bool: ...

def validate_param_deps(rule_name: str, deps: set[ParamDep], registry: ParamSpecRegistry) -> None:
    missing = sorted(d.logical_name for d in deps if not registry.has(d.logical_name))
    if missing:
        raise ValueError(f"Rule {rule_name} references undeclared param tables: {missing}")
```

---

### 4) Store deps into rule diagnostics + build a reverse index (param → rules)

```python
@dataclass(frozen=True)
class RuleDependencyReport:
    rule_name: str
    param_tables: tuple[str, ...]     # logical names
    dataset_tables: tuple[str, ...]   # (optional) non-param table refs

def build_param_reverse_index(reports: list[RuleDependencyReport]) -> dict[str, tuple[str, ...]]:
    rev: dict[str, list[str]] = {}
    for r in reports:
        for p in r.param_tables:
            rev.setdefault(p, []).append(r.rule_name)
    return {p: tuple(sorted(rs)) for p, rs in rev.items()}
```

---

### 5) Hamilton wiring strategy: infer → compute union → activate only needed param nodes

Because this inference happens *before* execution, you can compute which param tables are required for the requested outputs/rules and pass that set into your param-table builder nodes.

```python
@dataclass(frozen=True)
class ActiveParamSet:
    active: frozenset[str]   # logical names

def active_param_set(selected_rules: tuple[str, ...], dep_reports: dict[str, RuleDependencyReport]) -> ActiveParamSet:
    needed: set[str] = set()
    for rn in selected_rules:
        needed.update(dep_reports[rn].param_tables)
    return ActiveParamSet(frozenset(needed))
```

Then in PR‑06c’s param table construction node, you add a gate:

* build/register only those specs whose `logical_name` is in `ActiveParamSet.active`.

This keeps the DAG inference-driven: rules imply deps; deps imply param nodes.

---

### Narrative (dense technical intent + “why this is the right abstraction”)

#### A) Why SQLGlot table-reference extraction is the correct inference mechanism

In your target architecture, rules are authored programmatically (Ibis/RelOps), but the compiler’s “truth surface” is the **SQLGlot AST**, because:

* It is the shared IR used for optimization/lineage/validation gates.
* It is also the bridge into the SQL-free DataFusion adapter path (Ibis → SQLGlot → DataFusion DataFrame).

So, using SQLGlot AST introspection to infer dependencies is superior to:

* scanning source code text,
* trying to infer from Ibis objects (which can be backend-specific),
* or relying on rule authors to keep explicit dep lists up to date.

SQLGlot explicitly supports metadata extraction by traversing the AST and collecting `exp.Table` nodes via `find_all(exp.Table)`; this is designed for exactly this kind of lineage/policy tooling.

#### B) Why this must run after `qualify()` and `optimize()`

A naive scan of unqualified ASTs will misclassify:

* CTE names (appear like tables),
* aliased subqueries,
* and objects resolved via search path defaults.

Your plan already establishes `to_sqlglot(expr) → qualify(schema_map) → optimize(schema_map)` as the canonical diagnostics pipeline.

If you infer param deps on the **optimized qualified AST**, then:

* real tables tend to carry `db/schema` consistently,
* and you can reliably treat “schema == params” as the discriminator.

#### C) How this changes Hamilton orchestration (the key “inference-driven” win)

Once you have `(rule → param tables)` inferred at compile-time, the Hamilton driver factory can:

1. Resolve which rules are needed for the requested outputs (existing behavior).
2. Compute `ActiveParamSet = union(param_deps for selected rules)`.
3. Include PR‑06c’s param-table nodes, gated by that set.
4. Run compilation/execution with full reproducibility:

   * dependency map stored in diagnostics/run bundle,
   * param signatures recorded (from PR‑06c/PR‑06b),
   * and plan signatures remain stable.

Net effect: nobody ever manually edits “rule X depends on param Y”—the rule’s own query structure defines it.

#### D) Relationship to typed RelOps (`RelOpKind = ... "param"`)

Your migration plan already includes `"param"` as a typed relational op kind in the rule IR.

This PR gives you a clean, best-of-both-worlds posture:

* **If rule authorship already emits `ParamOp(logical_name=...)`:** treat that as an explicit, compile-time dependency (fast path).
* **Else:** infer deps from SQLGlot table refs (fallback path).

Over time you can enforce: “rules must use ParamOp for list filters”, and keep SQLGlot inference as a validation backstop.

#### E) Failure modes and how this PR avoids them

1. **False positives from CTEs named like `p_file_allowlist`**
   Fix: only treat a reference as a param table if its resolved schema/db equals `params` (post-qualify). If schema is missing, treat as “unknown” and do not infer param dependency.

2. **False negatives when param tables are embedded inline as `IN (...)`**
   Fix: PR‑06/06b already standardizes list params as `memtable + join` and encourages stable naming via create_table/create_view where needed.
   This PR essentially “enforces” that practice: if a rule uses `IN (...)`, it won’t show a `params.*` table reference and thus won’t activate the param nodes.

3. **Backend differences in qualification behavior**
   Fix: run inference on the SQLGlot AST you snapshot as diagnostics (the exact same artifact you store for migrations). That keeps inference consistent across execution modes.

---

### Implementation checklist (PR‑06d)

* [ ] Add `extract_table_refs(sqlglot_ast) -> set[TableRef]` using SQLGlot `find_all(exp.Table)` traversal.
* [ ] Add `infer_param_deps(table_refs, policy) -> set[ParamDep]` with schema/prefix matching.
* [ ] Integrate inference into your relspec diagnostics pipeline *after* `qualify()` + `optimize()`; store per-rule dependency reports alongside other SQLGlot artifacts.
* [ ] Validate inferred deps against your ParamTableSpec registry; fail fast on missing spec.
* [ ] Add a reverse index (param table → rules) for debugging/manifests/run bundles.
* [ ] Update Hamilton driver build process:

  * [ ] compute selected rules for requested outputs
  * [ ] compute `ActiveParamSet` union
  * [ ] gate PR‑06c param-table nodes using `ActiveParamSet` so only needed param tables are built/registered
* [ ] Add tests:

  * [ ] rule with `params.p_file_allowlist` join infers dependency
  * [ ] rule with CTE named like a param table does *not* infer dependency (when schema absent)
  * [ ] rule with literal `IN (...)` does *not* infer (and triggers a policy warning)
* [ ] Add manifest/run bundle entries:

  * [ ] `rule_param_deps.json` (rule → param tables)
  * [ ] `param_rule_reverse_index.json` (param → rules)

---

If you want one more “best-in-class” refinement after PR‑06d: add a **compiler gate** that rejects list filtering unless it comes from a declared param table (or a declared ParamOp), so you can prevent regressions back into inline `IN (...)` expressions and keep plan hashing + lineage stable.


## PR‑06e — Compiler gate: reject list filtering unless it’s via declared ParamTable (or ParamOp)

**Description**
Add a **hard compiler validation gate** that **rejects** any rule plan that implements “list filtering” (membership tests) using **inline literal lists** (e.g., `col IN (1,2,3)` / `col.isin([…])`) or other value-dependent constructs, unless the membership set is sourced from:

1. a **declared param table** (e.g., `params.p_file_allowlist` via semi-join / join), or
2. an explicit **ParamOp** in your typed relational IR (which the compiler lowers to the param-table join form).

This gate prevents regressions back into inline `IN (...)` expressions, which are poisonous to:

* plan signature stability,
* SQLGlot lineage readability,
* run-bundle reproducibility,
* and engine performance for large sets.

This directly advances the migration plan goals of: “no handwritten SQL”, “parameterization via Ibis params”, and “stable naming via create_view/create_table” where needed. 

---

### Code patterns (key snippets only)

#### 1) Policy/config: strict by default, optional dev escape hatch

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class ListFilterGatePolicy:
    # Strict mode: reject any literal IN-list (recommended).
    reject_literal_inlist: bool = True

    # Optional dev-only escape hatch:
    # allow IN-list only when it is <= this size AND marked explicitly.
    allow_small_literal_inlist_max: int = 0  # 0 means disabled
```

#### 2) SQLGlot AST validator: detect membership predicates and classify their RHS

```python
from sqlglot import exp

class ListFilterGateError(ValueError):
    pass

def validate_no_inline_inlists(
    *,
    rule_name: str,
    sg_ast: exp.Expression,
    param_schema: str = "params",
    param_prefix: str = "p_",
    policy: ListFilterGatePolicy = ListFilterGatePolicy(),
) -> None:
    """
    Rejects `IN (...)` / `IN [literal list]` patterns unless RHS is a subquery sourced from params.*
    or the plan was produced from ParamOp lowering (which should not generate literal lists).
    """
    for node in sg_ast.find_all(exp.In):
        rhs = node.expression  # right side of IN

        # CASE A: IN (SELECT ... FROM params.p_* ...)
        if isinstance(rhs, (exp.Subquery, exp.Select)):
            if _subquery_is_from_param_table(rhs, param_schema=param_schema, param_prefix=param_prefix):
                continue
            raise ListFilterGateError(
                f"[list-filter-gate] Rule '{rule_name}' uses IN-subquery not sourced from declared param tables; "
                f"use params.{param_prefix}<name> join/semijoin instead. Offending: {node.sql(dialect='')}"
            )

        # CASE B: IN (<literal tuple/array>)
        if isinstance(rhs, (exp.Tuple, exp.Array)):
            lit_count = sum(1 for e in rhs.expressions if isinstance(e, exp.Literal))
            if policy.allow_small_literal_inlist_max and lit_count <= policy.allow_small_literal_inlist_max:
                # still require explicit marker upstream if you want this; otherwise keep disabled
                continue
            if policy.reject_literal_inlist:
                raise ListFilterGateError(
                    f"[list-filter-gate] Rule '{rule_name}' contains literal IN-list ({lit_count} literals). "
                    f"Use a declared param table (params.{param_prefix}<name>) or ParamOp lowering. "
                    f"Offending: {node.sql(dialect='')}"
                )
```

Helper (classification): “is RHS sourced from params?”

```python
from sqlglot import exp

def _subquery_is_from_param_table(q: exp.Expression, *, param_schema: str, param_prefix: str) -> bool:
    for t in q.find_all(exp.Table):
        schema = t.args.get("db")  # db == schema in SQLGlot naming
        name = t.name
        if schema == param_schema and name.startswith(param_prefix):
            return True
    return False
```

#### 3) Gate integration point: run after qualify/optimize (your canonical diagnostics path)

```python
def compile_rule_to_sqlglot(rule, ibis_expr, schema_map):
    sg = con.compiler.to_sqlglot(ibis_expr)
    sg = qualify(sg, schema=schema_map)
    sg = optimize(sg, schema=schema_map)

    validate_no_inline_inlists(
        rule_name=rule.name,
        sg_ast=sg,
        param_schema=param_policy.schema,
        param_prefix=param_policy.prefix,
        policy=list_filter_gate_policy,
    )
    return sg
```

#### 4) Optional: “ParamOp lowering” ensures the preferred shape never emits IN-lists

```python
# Pseudocode IR:
# FilterOp(predicate=InSet(lhs=col("file_id"), rhs=ParamRef("file_allowlist")))
#
# Lowering:
# t SEMI JOIN params.p_file_allowlist ON t.file_id = params.p_file_allowlist.file_id
```

---

### Narrative (dense technical intent + micro-level “why”)

#### A) What this gate protects (and why it’s worth being strict)

In this architecture, rules are compiled into reusable, analyzable plan templates. Inline `IN` lists break that contract in four ways:

1. **Plan hashing becomes value-dependent**
   If a list is embedded as literals, the compiled AST changes when the list changes; your “plan signature” stops being a plan signature and becomes a “plan+values signature”.

2. **Lineage becomes unreadable and brittle**
   Instead of seeing stable dependencies (`JOIN params.p_file_allowlist`), lineage sees enormous literal blobs. This destroys the ability to reason about why a rule depends on a particular filter set.

3. **Execution quality is inconsistent**
   Engines may treat large literal `IN` lists differently depending on size thresholds (rewrite to hash set, linear scan, etc.). You want *engine-consistent behavior*, which you get by using real relations (param tables) and joins.

4. **It blocks your “param dependency inference”**
   PR‑06d relies on inferring param dependencies via table references. Inline lists have no table reference. The gate ensures you never regress into patterns that disable the inference machinery.

So: the gate is not just “style enforcement”—it preserves the central architectural invariants.

---

#### B) Why SQLGlot is the right enforcement layer

You have multiple authoring paths:

* pure Ibis expressions,
* typed RelOps → Ibis,
* potentially some transitional rule encodings.

SQLGlot sits at the convergence point:

* everything compiles to SQLGlot for diagnostics/optimization/lineage in your plan,
* and it’s a stable place to apply global correctness gates before execution.

Enforcing at SQLGlot means:

* you catch violations regardless of authoring path,
* you can emit precise error messages with the offending subtree,
* and you can store the “failing AST” in run diagnostics.

---

#### C) “Allowed” membership semantics under this gate

After this PR lands, you have exactly two acceptable ways to express list filtering:

1. **Param table join / semi-join (preferred)**

* `t SEMI JOIN params.p_file_allowlist ON t.file_id = params.p_file_allowlist.file_id`
* This is stable, optimizable, and supports PR‑06c/06d/manifest visibility.

2. **ParamOp (authoring convenience, compiled to #1)**

* Rule authors express “filter by allowlist” as a ParamOp.
* Compiler lowers it to the join form.
* Keeps rule authoring ergonomic but preserves the invariant.

Everything else is rejected (by default).

---

#### D) Why we reject “IN (SELECT …)” unless it selects from params.*

Even though `IN (SELECT …)` can be a valid relational idiom, allowing it generally creates a loophole:

* people can “smuggle” dynamic lists through arbitrary subqueries,
* which defeats the param table “first-class dataset” semantics and makes params invisible to manifests.

So we permit `IN (SELECT …)` only if its FROM contains `params.p_*` and is therefore semantically equivalent to the param-table join form.

You still *prefer* generating explicit semi-joins, but allowing this specific shape prevents false positives if some compiler path emits `IN (SELECT ...)` syntactically.

---

#### E) Optional dev-only escape hatch (explicitly discouraged)

There are rare cases where a small, truly static enumeration is reasonable (e.g., a 3-value mode flag). If you want, you can allow a small literal list threshold, but it should be:

* **disabled in production** (`allow_small_literal_inlist_max = 0`), and
* only enabled in dev with a policy flag, ideally with an explicit marker in rule metadata (“static literal allowlist intended”).

Otherwise the gate should remain strict.

---

### Implementation checklist (PR‑06e)

**Compiler enforcement**

* [ ] Add `ListFilterGatePolicy` to config (default strict).
* [ ] Implement `validate_no_inline_inlists()` on SQLGlot AST.
* [ ] Integrate the gate into the canonical compilation pipeline **after** qualify/optimize and **before** execution. 
* [ ] Emit a “fix hint” in the error message:

  * “Use `params.p_<name>` semi-join” or “Use ParamOp(…)”

**ParamOp pathway**

* [ ] If ParamOp exists: ensure lowering always yields semi-join against `params.p_*`.
* [ ] If ParamOp is not yet in all authoring paths: implement a small “ParamRef predicate” helper so rule authors don’t regress.

**Tests**

* [ ] Rule with `t.filter(t.file_id.isin([1,2,3]))` ⇒ **fails** with clear error.
* [ ] Rule joining `params.p_file_allowlist` ⇒ **passes**.
* [ ] Rule using `IN (SELECT file_id FROM params.p_file_allowlist)` ⇒ **passes** (if you allow that shape).
* [ ] Rule using `IN (SELECT … FROM non-params-table)` ⇒ **fails**.

**Diagnostics**

* [ ] On failure: store the optimized SQLGlot AST (and optionally the subtree) in diagnostics output to make fixes fast.
* [ ] Add a run-bundle summary flag: `list_filter_gate: "strict"` and a count of violations (should be 0 in a clean run).

---

### Watchouts / edge cases (important)

* **Optimizer rewrites**: If your SQLGlot optimize pass ever rewrites a join into a literal IN-list (rare, but possible in some optimizers), validate both:

  * pre-optimize (author intent),
  * and post-optimize (final AST).
    In practice, start with post-optimize only; add the pre-optimize check if you see unexpected rewrites.

* **CTEs named like params**: Your param dependency inference already requires schema-qualified `params.*`. Maintain that invariant: the gate should only classify param tables using `schema == "params"` + `name.startswith("p_")`.

* **Multi-column membership**: If you ever use tuple membership (`(a,b) IN (...)`), disallow entirely unless you model it as a join against a 2-column param table. (Same invariant, same fix.)

---

If you want the next follow-on after PR‑06e: add a *compiler auto-fix* mode (diagnostics-only) that rewrites trivial inline `IN` literal lists into a generated param table spec + param join, then emits a suggested patch in the diagnostics bundle (not applied automatically). This can speed migration while keeping the invariant strict.



