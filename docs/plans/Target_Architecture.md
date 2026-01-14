Below is a **target (end‑state) architecture** that keeps your **CPG outputs identical** (same Parquet artifacts), but makes the implementation **maximally modular, declarative, and rule/relationship-driven**, while using **PyArrow + compute + Acero**, **DataFusion**, **Ibis**, **SQLGlot**, and **Hamilton** in the most “best‑in‑class” way.

I’m grounding this in what I see in your *partway-through DataFusion integration* codebase: you already have the right “center of gravity” (rule objects, contracts, policies, kernels, resolver modes). The biggest upgrade is to **make DataFusion the primary lazy plan executor** (not a side bridge), and make Ibis/SQLGlot the **compiler frontend**, while PyArrow/Acero becomes the **Arrow-native kernel + streaming + fallback lane**.

---

## 1) Where you are now (what’s already “correct” directionally)

From the zip, you’ve already built the hard parts correctly:

* A **rule registry** concept (`relspec/rules/*`) that makes relationships declarative rather than hard-coded.
* A **contract/spec layer** (`schema_spec/system.py`, contract catalogs, dataset catalogs) that lets rules talk in names/columns rather than concrete intermediate schemas.
* A **policy layer** (dedupe strategies, tie-breakers, confidence/evidence defaults).
* A **kernel lane** that can run either Arrow kernels or DataFusion kernels depending on runtime (`arrowdsl.compute.kernels.resolve_kernel` → `datafusion_engine.kernels`). This is exactly the right “escape hatch” pattern.
* A **filesystem-backed resolver** (`FilesystemPlanResolver` using Ibis dataset registration) so the system can operate on Parquet scans rather than only in-memory.

The main architectural friction is:

* DataFusion is currently “off to the side” (custom SQLGlot→DataFusion translation) rather than the core execution backend.
* There’s still a tendency to **materialize intermediate results** (or to convert plans to memtables) in places where you ideally want **lazy views** and **engine-optimized joins**.

---

## 2) Target end-state: one declarative rule graph, multiple execution lanes

### The guiding principle

**Everything is a rule that compiles to a lazy relational plan**.

You want three lanes:

1. **Relational lane (primary): DataFusion**

   * Used for *joins, filters, projections, aggregations, window functions, dedupe/winner selection*.
   * Operates primarily on **Parquet/Arrow datasets** via scans.
   * Produces outputs either as **views** or **materialized parquet datasets**.

2. **Arrow kernel lane (secondary): PyArrow compute + Acero**

   * Used for:

     * operations that are faster/cleaner in Arrow compute,
     * operations not expressible (or not stable) in the relational executor,
     * streaming/recordbatch transformations when you explicitly want `to_reader()` behavior.
   * Acero’s “declaration is a blueprint; execution chooses sink” model is exactly what you want for modularity and separating compile vs execute. 

3. **Compiler/IR lane: Ibis + SQLGlot**

   * Rules are authored as **(a)** a structured join/relationship spec and/or **(b)** Ibis expressions.
   * SQLGlot is the canonical AST for:

     * dependency extraction (lineage),
     * query normalization/canonicalization,
     * portability checks,
     * diagnostics.
   * This matches your existing direction: treat SQLGlot as compiler IR, not incidental output. 

---

## 3) Best-in-class use of each library (clear “jobs to be done”)

### PyArrow (data objects + compute)

**Job:** the physical data model and the fastest “vectorized math” substrate.

* Use Arrow Tables/RecordBatches as the interchange between layers.
* Inside UDFs, never convert to Python scalars unless absolutely required; prefer `pyarrow.compute` (this is explicitly a performance rule in the DataFusion UDF guidance). 
* Keep your “expression vs scalar” distinction sharp (`pa.scalar` vs `pc.scalar`), because it matters for pushing expressions into dataset scans / Acero / compute expression trees. 

### PyArrow Acero (+ your ArrowDSL)

**Job:** a streaming execution / kernel pipeline mechanism and a reliable fallback executor.

* Use Acero when you want:

  * streaming readers (`to_reader()`),
  * Arrow-native pushdown scans and “small predictable” pipelines,
  * a fallback execution lane for rules that don’t compile cleanly to DataFusion.
* Keep the DSL small, runtime knobs in runtime profiles, not scattered through node code. 

### DataFusion

**Job:** the primary *relational* execution engine and optimizer.

* Use DataFusion for heavy relationship work:

  * multi-join queries,
  * window functions for dedupe/winner selection,
  * large parquet scans,
  * predicate pushdown,
  * join reorder + parallelism tuning.
* Collect statistics at table creation time for better planning. 
* Treat UDF volatility (immutable/stable/volatile) as part of your contract, because the optimizer uses it. 

### Ibis

**Job:** rule authoring frontend (composable relational expressions) + portable type semantics.

* In your current codebase you compile rules to Ibis expressions already. The best upgrade is: **run those expressions on the DataFusion backend** rather than DuckDB-for-execution + DataFusion-bridge-for-some-cases.
* Ibis **does have a DataFusion backend** and can connect directly (including passing a DataFusion `SessionContext`). ([Ibis][1])
  This is the cleanest way to eliminate your SQLGlot→DataFusion manual translation layer.

### SQLGlot

**Job:** the canonical AST + rewriting + introspection toolkit.

* Use SQLGlot for:

  * canonicalizing rule SQL for stable fingerprints,
  * extracting dependencies to build the Hamilton DAG automatically,
  * query linting (missing columns, ambiguous refs),
  * building “explainable” plan artifacts in run bundles.

### Hamilton

**Job:** inference-driven orchestration + “only materialize what’s requested”.

* The DAG should be generated from:

  * dataset contracts,
  * rule registry dependencies (from SQLGlot lineage),
  * output requests.
* Hamilton already supports data-driven expansion patterns; you can generate nodes dynamically from a registry table instead of hand-writing nodes. 

---

## 4) The target architecture (modules and responsibilities)

Here’s the architecture I’d implement given your current direction.

### A) Specs: the single source of truth

**1) DatasetSpec / ContractSpec registry**

* Required columns + types
* Optional/extra columns allowed
* Ordering / determinism requirements (canonical sorts, stable IDs)
* Dedupe requirements

**2) Rule registry**
A rule is the *only* way new logic enters the system.

Rule types:

* `ExtractRule` → produces base tables (AST/CST/SCIP/etc.)
* `NormalizeRule` → makes tables join-ready (byte offsets, IDs, schema evolution)
* `RelationshipRule` → joins tables, resolves ambiguity, outputs relationship datasets
* `EmissionRule` → turns relationship datasets into CPG node/edge/property tables

Each rule defines:

* inputs (dataset refs + minimal required columns)
* relational plan spec (Ibis expression *or* structured join graph)
* dedupe/winner policy
* output contract + kind claims (edge kind / node kind)

### B) Compiler: Rule → LogicalPlan (engine-agnostic) → Backend plan

**Compilation outputs per rule:**

* `CompiledRule` containing:

  * `ibis_expr` (authoring-level IR)
  * `sqlglot_expr` (canonical compiler IR)
  * rendered SQL string (dialect pinned)
  * optional: DataFusion logical plan / Substrait bytes (more below)
  * `output_schema` (inferred + validated against contract)
  * dependency list (datasets referenced)

**Why this matters**

* It gives you *full inference-driven orchestration*:

  * dependencies come from lineage, not from handwired DAG edges.
* It gives you *schema inference without intermediate schema declarations*:

  * only required props are validated; intermediate schemas are inferred by the engine + validated by contracts.

### C) Execution: a “resolver” that never forces materialization unless asked

The execution engine should support three resolution modes:

1. **View mode (default)**

* Register base datasets as DataFusion tables.
* Register each compiled rule output as a DataFusion **view** (or an Ibis view backed by DataFusion).
* Nothing is executed until an output is requested.

2. **Materialize mode (debug / caching / large DAG)**

* For selected nodes:

  * execute the plan,
  * write parquet,
  * register the parquet path as the dataset for downstream nodes.
* This is your existing “FilesystemPlanResolver” concept—keep it, but make DataFusion the executor.

3. **Kernel fallback mode**

* If a rule contains an op that DataFusion/Ibis can’t express:

  * run a PyArrow compute/Acero pipeline for that op (or whole rule),
  * materialize the result (table or parquet),
  * re-enter the relational lane.

---

## 5) How to get the “join patterns as logical statements tied to variable names”

Yes—this is feasible and it’s exactly what you get if you make **the rule registry the join truth**.

### The best-in-class representation

Represent relationship rules as a **join graph**:

* Nodes: named dataset aliases (e.g., `cst_callsites`, `scip_occurrences`, `ast_defs`)
* Edges: join predicates (usually equi-joins + optional disambiguation predicates)
* Each join edge has:

  * join keys (left_cols, right_cols)
  * optional additional predicates (span containment, same-file, same-module)
  * a “candidate scoring” expression
  * a winner selection policy

Then compile that join graph into:

* Ibis (for authoring and type checking),
* SQLGlot (for canonicalization and lineage),
* DataFusion (for execution).

This yields your ideal property:

* You **never hand-specify intermediate schemas**.
* You only specify:

  * what input datasets exist (contracts),
  * what relationship is being asserted (join graph + scoring),
  * what output columns must exist (contract),
  * how ambiguity is resolved (winner policy).

---

## 6) Winner selection / dedupe as first-class relational primitives

To be “rules-based” and not ad hoc, make winner selection a *required block* in relationship rules.

### Preferred: relational winner selection

Implement as:

* partition by ambiguity group keys
* order by (score desc, confidence desc, rule_priority asc, provenance tie-breakers)
* keep row_number = 1

This maps cleanly to DataFusion window execution and is deterministic if your tie-breakers are complete.

### Fallback: kernel lane

If you can’t express something relationally, fall back to:

* Arrow compute sort + group
* or DataFusion kernel helpers (you already started these)

---

## 7) Substrait as the “plan artifact” layer (optional but extremely powerful)

If you want your rule system to be **fully modular and replayable**, you want a stored plan format.

DataFusion supports Substrait producer/consumer and positions Substrait as the interchange for submitting plans across systems. 

**Best-in-class pattern:**

* Compile rule → canonical SQLGlot AST → SQL string
* Ask DataFusion to produce Substrait bytes
* Store those bytes in your run bundle
* Re-run the exact same plan later even if the rule authoring layer evolves

This becomes your “compiled artifact cache”.

---

## 8) “Policy hooks” and curated function surface (advanced, but worth calling out)

If you want a truly *controlled*, modular system, you also want a controlled function surface:

* In Rust embedding, DataFusion has `SessionStateBuilder` hooks to curate registries (`with_scalar_functions`, `with_aggregate_functions`, etc.). 
* Your addendum notes that stock Python bindings don’t expose a turnkey FunctionFactory install; doing this “for real” typically requires a small Rust/PyO3 bridge. 

**Practical architecture stance:**

* Start with Python UDF registration (fast iteration).
* If you later want a locked-down function policy surface, plan for a Rust extension as a “phase 2”.

---

## 9) Concrete target: what I would refactor towards in *your* current codebase

You asked for a blank-slate target, but grounded in your direction—this is the most leverage you can get:

### Step 1: Make DataFusion the primary Ibis backend

* Replace “DuckDB execution backend” with **Ibis DataFusion backend** (and pass your configured `SessionContext`). ([Ibis][1])
* This immediately makes:

  * filesystem scans,
  * joins,
  * window dedupe,
  * parquet writing
    happen in the intended engine.

### Step 2: Delete (or demote) manual SQLGlot→DataFusion translation

Your `datafusion_engine/df_builder.py` translator is fine as a bridge, but it’s a long-term maintenance cost. Once Ibis runs on DataFusion directly, you no longer need to maintain translation coverage for SQL constructs.

Keep SQLGlot for:

* canonicalization
* diagnostics
* lineage extraction

### Step 3: Promote “views-first” execution everywhere

* Relationship compilation should register **views** unless explicitly materialized.
* The filesystem resolver becomes a *materialization policy* rather than the default executor.

### Step 4: Push kernel operations into the relational plan wherever possible

* If you can express dedupe/winner selection in Ibis/DataFusion, do it there.
* Keep Arrow kernels for:

  * truly custom vector operations,
  * span normalization quirks,
  * rare transforms that don’t belong in SQL.

(DataFusion UDF guidance is explicit: keep UDFs Arrow-native by using `pyarrow.compute` internally. )

### Step 5: Make rule dependencies purely derived from lineage

* Use SQLGlot lineage extraction to build the Hamilton DAG edges.
* Then the Hamilton layer becomes “select outputs + choose materializers”, not “manually define wiring”.

---

## 10) The “maximally modular, declarative” target in one sentence

**A run is:**

> Register input datasets → compile rule registry into lazy DataFusion views using Ibis/SQLGlot → materialize only requested outputs (and chosen debug checkpoints) → emit CPG parquet artifacts, with every ambiguity decision driven by explicit winner-selection policies.

That’s the best achievable blend of:

* inference-driven orchestration (Hamilton),
* inference-driven schema/materialization (contracts + engine inference),
* maximal composability (rules),
* high-performance joins (DataFusion),
* Arrow-native correctness/performance escape hatch (PyArrow compute + Acero).

---

If you want, I can turn this into a **concrete “target package map”** (what modules to keep, what to merge, what to delete) specifically for your current repo, including:

* the *new* “DataFusion-backed Ibis backend factory” shape,
* how relationship rules should register as views vs parquet,
* and exactly where Substrait plan snapshots belong in your run bundle format.

[1]: https://ibis-project.org/backends/datafusion "datafusion – Ibis"
