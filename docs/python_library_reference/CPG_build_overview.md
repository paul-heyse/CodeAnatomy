Below is a **mechanistic, “do-this-then-that” guide** for building a **Code Property Graph (CPG)** once you already have:

* an **AST dataset** (semantic tree)
* a **CST dataset** (concrete tree / tokens / trivia)
* an **AST↔CST join** (by byte-span, so every AST node can be tied to concrete text)
* a **first-pass SCIP mapping** (SCIP occurrences/symbols anchored to syntax spans)

The key idea: **CPG is not “one graph.”** It’s a **composition** of multiple subgraphs that you build in layers: **Syntax (AST/CST) → Symbols/Scopes (SCIP + binder) → Control flow (CFG) → Data flow (DFG) → Interprocedural stitching (calls/returns) → Property enrichment**.

---

## 0) Starting point: what you already have (treat this as your “ground truth”)

Assume your joined datasets already give you, per file:

1. **CST nodes/tokens** with exact byte spans into the original file bytes (so you can slice the source losslessly).
2. **AST nodes** with:

   * stable intra-file IDs (or a stable traversal index),
   * node kind + fields,
   * a byte span (either native or derived),
   * a join back to CST token ranges.
3. **SCIP occurrences** (definitions/references) already mapped to syntax spans, so each occurrence is attached to *some* AST/CST anchor node; SCIP itself is “symbols + occurrences + relationships” emitted by scip-python/Pyright. 

From here forward, **the only coordinate system that matters is byte offsets**. If anything in later stages can’t be tied back to byte spans, it’s not “index-grade.”

---

## 1) Decide your graph contract up front (otherwise you’ll rebuild everything)

A property graph needs two base relations:

### 1.1 Node contract

Every node must have:

* `node_id` (globally unique)
* `label` (node type: `AST_NODE`, `CST_NODE`, `TOKEN`, `SYMBOL`, `SCOPE`, `BINDING`, `CFG_BLOCK`, `CFG_EDGE_NODE`, `DF_DEF`, `DF_USE`, …)
* `file_id` (nullable for “global” nodes like external package symbols)
* `span_start`, `span_end` (nullable for purely logical nodes like `CFG_BLOCK`, but preferred whenever possible)
* `props` (a JSON/STRUCT map: kind, name, operator, literal value, etc.)

### 1.2 Edge contract

Every edge must have:

* `edge_id` (unique)
* `src`, `dst`
* `label` (edge type: `AST_CHILD`, `CST_CHILD`, `COVERS`, `DEF`, `REF`, `CFG_NEXT`, `CFG_TRUE`, `CFG_FALSE`, `REACHES`, `CALLS`, …)
* `props` (ordering index, field name, confidence, etc.)

**Do not try to squeeze this into “one edge type + properties.”** You want explicit edge labels because they become your query primitives.

> If you prototype in NetworkX: use a **MultiDiGraph** so you can represent multiple parallel edges between the same nodes (e.g., `AST_CHILD` and `CFG_NEXT` between the same pair) without collisions. 

---

## 2) Phase 1 — Canonical identity: make IDs stable before you add semantics

Mechanistically:

1. **Create `file_id`** as a stable hash of repo-relative path (plus repo/version if you shard).

2. **Create stable IDs for syntax nodes**:

   * `ast_node_id = f"{file_id}:AST:{ast_local_id}"`
   * `cst_node_id = f"{file_id}:CST:{cst_local_id}"`
   * `tok_id = f"{file_id}:TOK:{tok_index}"`

3. **Create stable IDs for semantic nodes**:

   * `symbol_id = f"SCIP:{symbol_string}"` (or include package/name/version for external symbols, which SCIP encodes for stability). 
   * `scope_id = f"{file_id}:SCOPE:{scope_local_id}"`
   * `binding_id = f"{scope_id}:BIND:{name}:{binding_ordinal}"`

4. **Record a “span contract” row per file**: encoding/newline/indent + a content hash so you can detect drift. (Bytes-first CST parsing is the typical way to preserve encoding fidelity.) 

At the end of Phase 1, you can answer: **“Given an ID, what file/span does it correspond to?”** If you can’t, stop and fix IDs now.

---

## 3) Phase 2 — Build the syntax backbone subgraph (AST + CST + the join)

This is purely structural: you’re creating the “shape” that everything else hangs on.

### 3.1 AST subgraph (semantic tree)

Mechanistically:

* Insert a node row for every AST node: `label="AST_NODE"`, `props.kind = ast_class_name`, plus any normalized fields you store (e.g., `name`, `op`, `ctx`).
* Insert edges:

  * `AST_CHILD(src=parent, dst=child, props={field, index})`

You want **field-aware edges**, not just “child index,” because later queries depend on “this Name is the function name vs this Name is an argument.”

### 3.2 CST/token subgraph (concrete surface)

Mechanistically:

* Insert CST nodes and/or tokens (depending on your CST representation).
* Insert edges:

  * `CST_CHILD(parent → child)`
  * `CST_TOKEN(node → tok)` or `TOKEN_NEXT(tok_i → tok_{i+1})`

### 3.3 AST↔CST anchoring edges

Your join result becomes edges like:

* `COVERS(ast_node → token_range)` **or** `COVERS(ast_node → cst_node)`
* `PRIMARY_TOKEN(ast_node → tok)` for “best representative token” (useful for UI and symbol anchoring)
* `TEXT_RANGE(ast_node.props = {start,end})` already exists, but you still want explicit cover edges for traversal.

This is the moment you “lock in” the invariant:

> Every semantic thing we add later (symbol, def-use, call, etc.) must be able to point to **either an AST node** or a **token span** (and therefore back to source bytes).

---

## 4) Phase 3 — Attach SCIP: turn “occurrences” into graph edges

SCIP gives you: **symbols** and **occurrences (defs/refs)** anchored to ranges. 
You’ve already mapped occurrences to syntax spans; now you materialize them into graph primitives.

### 4.1 Create SYMBOL nodes

For every unique SCIP symbol string:

* Node: `label="SYMBOL"`, `node_id=symbol_id`
* Props: `symbol`, `kind` (function/class/var), `package`, `version`, `doc`, etc. (whatever you decode)

### 4.2 Create DEF/REF edges from syntax to symbols

For each SCIP occurrence:

1. Find its anchor syntax node:

   * prefer an AST node if the occurrence range falls inside exactly one “semantic” AST node (e.g., `FunctionDef.name`, `ClassDef.name`, `Name(Load)`),
   * otherwise fall back to the token span.

2. Materialize:

   * `DEFINES(anchor → SYMBOL, props={role, scip_occ_id})` if occurrence is a definition
   * `REFERS_TO(anchor → SYMBOL, props={role, scip_occ_id})` if occurrence is a reference

That’s it. No CFG, no DFG yet—just symbol linking.

### 4.3 Create “symbol-to-definition-site” convenience edges

You will constantly want “jump from symbol to defining syntax.” Add:

* `SYMBOL_DEF(SYMBOL → anchor)` for definitions

This is how you make “go to definition” a single hop in your CPG.

---

## 5) Phase 4 — Build scopes + bindings (the “missing middle” between symbols and dataflow)

Even with SCIP, you still need **a scope/binding model** because:

* Python has *implicit* bindings (comprehensions, exception handlers, `with … as`, imports, parameters, pattern matching targets, etc.).
* You’ll have cases where SCIP is missing/ambiguous (dynamic code, partial indexing, generated code).
* Dataflow is fundamentally about **definitions reaching uses**, which requires a binding abstraction.

### 5.1 Create SCOPE nodes

Mechanistically, define scope boundaries using AST structure:

* module scope
* class scope
* function scope
* lambda scope
* comprehension scope (important!)

Create `SCOPE` nodes, each pointing to its “owner” AST node:

* `OWNS_SCOPE(ast_owner → scope)`
* `PARENT_SCOPE(scope_parent → scope_child)`

### 5.2 Create BINDING nodes (name slots in a scope)

For each scope:

* scan for binding-introducing constructs:

  * function params
  * assignment targets
  * import aliases
  * `for` targets
  * `with … as x`
  * `except … as e`
  * pattern matching targets
  * `global`/`nonlocal` declarations (affect resolution rules)

For each binding, create:

* node `BINDING(scope_id, name, kind, flags={global/nonlocal})`
* edge `DECLARES(scope → binding)`

### 5.3 Link binding defs/refs (bridge to SCIP when available)

Now link syntax sites to bindings:

* For definition sites (Store context):

  * `BINDS_DEF(ast_store_node → binding)`
* For use sites (Load context):

  * `BINDS_USE(ast_load_node → binding)`

If SCIP symbol exists for that Name:

* add `BINDING_SYMBOL(binding → SYMBOL)` (this lets you unify name-based and symbol-based worlds).

At this point, you can answer:

* “What scope does this Name belong to?”
* “Which binding does this Name resolve to?”
* “Which symbol does this binding correspond to (if any)?”

That’s the prerequisite for CFG/DFG to mean anything.

---

## 6) Phase 5 — Build the CFG (Control Flow Graph) per code unit

A CPG without CFG is basically “syntax + symbols.” CFG is what turns it into “program behavior.”

### 6.1 Choose your CFG granularity (be explicit)

You have two viable approaches:

**A) Statement-level CFG (fast, good first cut)**

* Nodes represent statements (or statement blocks).
* Edges represent possible next statement execution.

**B) Expression-aware CFG (more correct, more work)**

* Nodes include expression evaluation order, short-circuit, exceptions, etc.
* Needed if you want precise def-use on subexpressions.

Most systems do A first, then selectively refine to B for “hot” constructs (calls, boolean short-circuit, comprehensions, augmented assignment).

### 6.2 CFG representation primitives

Mechanistically create:

* `CFG_BLOCK` nodes (basic blocks)
* `CFG_EDGE` edges with labels:

  * `CFG_NEXT` (fallthrough)
  * `CFG_TRUE` / `CFG_FALSE` (branches)
  * `CFG_EXC` (exceptional flow)
  * `CFG_RETURN`, `CFG_RAISE`, `CFG_BREAK`, `CFG_CONTINUE`

And a containment mapping:

* `CONTAINS_CFG(code_unit_ast → cfg_entry_block)`
* `CFG_CONTAINS(cfg_block → ast_stmt_node, props={index})`

### 6.3 The actual CFG construction algorithm (structured-to-graph lowering)

Per code unit (module/function/lambda):

1. Create `entry_block`, `exit_block`.
2. Walk statements in order, threading a “current block” pointer.
3. For each control construct, emit blocks and edges:

**If**

* current → cond_block
* cond_block → then_block (`CFG_TRUE`)
* cond_block → else_block (`CFG_FALSE`) (or directly to merge if no else)
* then/else → merge_block (`CFG_NEXT`)

**While/For**

* current → header_block
* header_block → body_block (`CFG_TRUE`)
* header_block → loop_exit (`CFG_FALSE`)
* body_block → header_block (`CFG_NEXT`)
* `break` jumps to loop_exit, `continue` jumps to header_block

**Try/Except/Finally**

* normal flow threads as usual
* add `CFG_EXC` edges from “may-raise” regions to handler entry blocks
* ensure `finally` executes on both normal and exceptional paths (this is where CFG gets tricky)

You don’t have to be perfect on day one, but you must be *mechanically consistent*: every construct produces blocks/edges in a deterministic way.

---

## 7) Phase 6 — Build DFG/PDG: definitions, uses, and “reaches” edges

Now that you have:

* bindings (what a name *means*)
* CFG (how execution *flows*)

…you can build dataflow.

### 7.1 Define what counts as a DEF and what counts as a USE

Mechanistically:

* A **DEF** is any syntax site that assigns to a binding:

  * parameters at function entry
  * assignment targets
  * loop targets
  * import aliases
  * `with … as`
  * `except … as`
* A **USE** is any site that reads from a binding:

  * `Name(Load)`
  * capturing a variable in a closure
  * implicit uses (e.g., `global` scope reads)

Create explicit nodes if you want (recommended for analysis):

* `DF_DEF` node (points to AST assignment site + RHS expression)
* `DF_USE` node (points to AST use site)

And link:

* `DEFINES_BINDING(df_def → binding)`
* `USES_BINDING(df_use → binding)`

### 7.2 Compute REACHES edges (classic reaching-definitions)

Mechanistically:

1. For each CFG block, compute:

   * `gen[block]`: defs created in the block
   * `kill[block]`: prior defs of same binding overwritten (strong update for locals)
2. Run standard forward dataflow to compute:

   * `in[block]`: reaching defs at entry
   * `out[block]`: reaching defs at exit
3. For each use site in a block:

   * for each reaching def of the same binding in `in[block]` (and any earlier in-block defs), emit:

     * `REACHES(df_def → df_use, props={binding, certainty})`

This is the first time your CPG can answer:

* “Where could this variable value have come from at this use?”

### 7.3 Add control dependence edges (PDG “control side”)

Once you have CFG, compute control dependencies via post-dominators:

* for each branch node, statements in the “then” region get `CONTROL_DEP(cond → stmt)` (and similarly for else)
* this gives you a full **Program Dependence Graph** (data deps + control deps)

---

## 8) Phase 7 — Call graph stitching: from call sites to possible callees

Now you connect intra-procedural graphs into an interprocedural graph.

### 8.1 Create CALLSITE nodes (or reuse AST Call nodes)

For each `ast.Call` (or equivalent), create:

* node `CALLSITE` (props: arity, kwarg names, is_method_call, etc.)
* edge `CALLSITE_AT(callsite → ast_call_node)`

### 8.2 Resolve callees

Mechanistically, your resolution stack is:

1. **SCIP-resolved symbol** for the call target (best case)
2. **Binder + simple heuristics** (e.g., direct name call in same scope)
3. **Fallback “unknown callee”** node (keep the edge, mark confidence low)

For each candidate callee:

* `CALLS(callsite → SYMBOL, props={confidence, reason})`
* optionally `CALLS_DEF(callsite → callee_def_anchor)` via `SYMBOL_DEF` hop

You should expect multiple edges for dynamic dispatch; that’s why a multigraph representation is useful. 

### 8.3 (Optional) actual→formal parameter edges

If you want CPG-style argument wiring:

* For each argument expression node:

  * `ARG_OF(arg_expr → callsite, props={position|kw})`
* For each callee parameter binding:

  * `BINDS_ARG(arg_expr → param_binding, props={position|kw, confidence})`

This becomes the backbone for interprocedural def-use later.

---

## 9) Phase 8 — Materialize + query: turning “many derived datasets” into “one usable CPG”

At this point you have:

* Syntax subgraph
* Symbol/binding graph
* CFG
* DFG/PDG
* Call graph edges

Now you make it *usable*:

### 9.1 Store in a property-graph-friendly shape

Two-table model:

* `nodes(node_id, label, file_id, span_start, span_end, props)`
* `edges(edge_id, src, dst, label, props)`

This is easy to:

* load into DuckDB
* export to Parquet/Arrow
* hydrate into NetworkX for ad-hoc traversal

### 9.2 Build “views” (pre-joined query surfaces)

Don’t force every query to join 7 tables. Create stable views like:

* `v_symbol_defs(symbol_id → file/span → snippet)`
* `v_callgraph(callsite → callee symbol → callee def)`
* `v_defuse(binding → defs → uses → paths)`
* `v_cfg(function → blocks → edges)`

These are what you expose to agents.

---

## 10) Validation: how you know you didn’t build nonsense

You want invariant checks that are cheap and brutal:

1. **Span coverage checks**

   * Every `DEFINES/REFERS_TO` anchor must have a span that covers the SCIP occurrence range.
2. **CFG sanity**

   * Every code unit has exactly one entry and exit block.
   * Every block is reachable from entry (or explain why not).
3. **DFG sanity**

   * Every `DF_USE` must have ≥0 reaching defs; if 0, it’s either uninitialized or a missing binding/symbol link (record it).
4. **Call graph sanity**

   * Every resolved callee symbol must have a definition anchor (or be external), otherwise your symbol table is inconsistent.

---

## The “mental picture” to keep you aligned

When you’re doing this right, you can narrate any edge in your graph as a concrete mechanism:

* “This `REFERS_TO` edge exists because a SCIP occurrence range was mapped to this AST `Name(Load)` node, then normalized into a symbol reference edge.” 
* “This `CFG_TRUE` edge exists because lowering an `if` statement always creates a condition block with two outgoing edges labeled true/false.”
* “This `REACHES` edge exists because reaching-definitions analysis on the CFG proved this definition can flow to that use without being killed.”

That’s what makes a CPG *mechanistic* rather than “a pile of edges.”

---

If you want, I can turn this into a **concrete build recipe** with:

* an explicit **node/edge label taxonomy** (Python-focused),
* minimal pseudo-IR for Python evaluation order (the exact part that makes CFG/DFG correct),
* and a **golden-fixture test suite** that asserts CFG/DFG edges on a handful of micro-programs (ifs/loops/try/finally/comprehensions/augassign).

Below is a **concrete, Python-focused build recipe** you can treat as a **contract** for your post-join pipeline:

1. **explicit node/edge label taxonomy**,
2. **minimal pseudo-IR for Python evaluation order** (so CFG/DFG are mechanically correct),
3. a **golden fixture suite** (pytest + snapshot JSON) that asserts CFG + reaching-defs + key evaluation-order facts for: **ifs, loops, try/finally, with, comprehensions, augassign**.

Everything is phrased as: **what nodes/edges you emit, in what order, and why**.

---

# 1) Concrete build recipe (the pipeline you implement)

You already have: **AST↔CST joined** (byte spans) + **SCIP occurrences mapped to syntax spans**.

From there, build the CPG in **four deterministic compilation passes**:

## Pass A — Graph backbone (syntax + SCIP surface)

**Inputs:** AST nodes, CST/tokens, AST↔CST cover join, SCIP symbols+occurrences
**Outputs:** `nodes[]`, `edges[]` for syntax and symbol linking

Mechanistically:

1. Emit `AST_NODE`, `CST_NODE`, `TOKEN` nodes with `span_start/span_end` (byte offsets).
2. Emit `AST_CHILD(field,index)` edges and CST edges.
3. Emit `COVERS(ast → token_range)` (or ast→cst) edges from your join.
4. Emit `SYMBOL` nodes for every SCIP symbol string.
5. For each SCIP occurrence: pick an anchor (AST node preferred, else token span) and emit:

   * `DEFINES(anchor → SYMBOL)` if definition
   * `REFERS_TO(anchor → SYMBOL)` if reference
   * and (optional but highly practical) `SYMBOL_DEF(SYMBOL → anchor)` for defs.

SCIP anchoring is only “attachment” here; you do **not** need CFG/DFG yet.

---

## Pass B — Scopes + bindings (the “resolution substrate”)

**Inputs:** AST + (optional) SCIP
**Outputs:** `SCOPE`, `BINDING` nodes and `BINDS_*` edges

Mechanistically:

1. Create `SCOPE` nodes for: module / function / class / lambda / comprehension.
2. Create `PARENT_SCOPE` edges and `OWNS_SCOPE(ast_owner → scope)` edges.
3. In each scope, scan for binding-introducing constructs (params, assigns, for-targets, with-as, except-as, imports, pattern targets). Emit `BINDING` nodes and `DECLARES(scope → binding)`.
4. Resolve every `Name(Load/Store/Del)` to a `BINDING` (per Python rules + scope chain). Emit:

   * `BINDS_DEF(name_store → binding)` for defs
   * `BINDS_USE(name_load → binding)` for uses
5. If a binding corresponds to a SCIP symbol, connect: `BINDING_SYMBOL(binding → SYMBOL)`.

This pass is what makes DFG meaningful: DFG is “defs/uses of *bindings* along *control flow*”.

---

## Pass C — Lower to minimal Eval-IR + CFG (this is the correctness hinge)

**Inputs:** AST + scopes/bindings + spans
**Outputs:** `CFG_BLOCK` nodes, `EVAL_STEP` nodes, CFG edges, evaluation-order edges

Mechanistically:

1. For each **code unit** (module/function/lambda/comprehension body), compile its AST to a **linearized Eval-IR** that respects Python evaluation order.
2. Partition Eval-IR into `CFG_BLOCK`s (basic blocks).
3. Emit:

   * `CFG_*` edges between blocks (control transfer)
   * `STEP_IN_BLOCK(block → step, order=i)`
   * `EVAL_NEXT(step_i → step_{i+1})` for sequencing **inside** a block (this is the piece most CPGs handwave; you’re making it explicit)

Python’s evaluation-order constraints you must enforce are in the language reference:

* “Python evaluates expressions from left to right.” ([Python documentation][1])
* Assignment evaluates **RHS before LHS**. ([Python documentation][1])
* Augmented assignment evaluates the **target first**, **only once**, then RHS, then writes back (and LHS-before-RHS is explicitly called out). ([Python documentation][2])
* `try…finally`: the `finally` clause runs “on the way out” even for return/break/continue; return in finally overrides prior return. ([Python documentation][3])
* `with`: step-by-step semantics (evaluate expr; load enter/exit; call enter; assign target; run suite; call exit with exc or None). ([Python documentation][3])
* Comprehensions: executed in a separate implicitly nested scope (except leftmost iterable evaluated in enclosing scope), and the for/if clauses nest left-to-right. ([Python documentation][1])

Those rules drive your IR lowering, and your tests will lock them in.

---

## Pass D — DFG (reaching definitions over the CFG, driven by bindings)

**Inputs:** CFG blocks + EVAL_STEP stream + bindings
**Outputs:** `REACHES` edges (and optional DEF/USE nodes)

Mechanistically:

1. From `EVAL_STEP`s, extract:

   * DEF sites (store to a binding / store to attribute / store to subscript)
   * USE sites (load of a binding / attribute / subscript)
2. Run classic forward reaching-defs on the CFG:

   * `gen[block]`, `kill[block]`, compute `in/out`
3. For each USE site, connect every reaching DEF of the same “storage location” via:

   * `REACHES(def_step → use_step, props={slot, certainty})`

You now have the core CPG properties: **syntax + symbols + control flow + data flow**.

---

# 2) Python-focused node/edge label taxonomy (explicit contract)

This is a **minimal but sufficient** taxonomy that supports the passes above and is testable.

## 2.1 Node labels

### Syntax layer

* `FILE`

  * props: `path`, `hash`
* `TOKEN`

  * props: `tok_kind`, `text` (optional), `index`
  * span: byte range
* `CST_NODE`

  * props: `kind`
  * span
* `AST_NODE`

  * props: `kind`, plus normalized fields (e.g., `name`, `op`, `ctx`)
  * span

### Symbols / binding layer

* `SYMBOL` (SCIP symbol)

  * props: `scip_symbol`, `kind`, `package`, `version`…
* `SCOPE`

  * props: `scope_kind` ∈ {`module`,`function`,`class`,`lambda`,`comprehension`}
* `BINDING`

  * props: `name`, `binding_kind` (param/local/nonlocal/global/import/target…), `flags`

### Control / evaluation layer

* `CFG_BLOCK`

  * props: `unit_id`, `block_kind` ∈ {`entry`,`exit`,`normal`,`loop_header`,`merge`,`finally`,`except_handler`,…}
* `EVAL_STEP` (**your minimal pseudo-IR instruction**)

  * props:

    * `op` ∈ {`LOAD_NAME`,`STORE_NAME`,`LOAD_ATTR`,`STORE_ATTR`,`LOAD_SUBSCR`,`STORE_SUBSCR`,
      `CALL`,`BINOP`,`UNOP`,`COMPARE`,`BRANCH`,`JUMP`,`RETURN`,`RAISE`,
      `ENTER_WITH`,`EXIT_WITH`,`ITER_INIT`,`ITER_NEXT`,`PHI`(optional)}
    * `unit_id`
    * `anchor_ast_id` (optional but recommended)
    * `text` (optional: source slice for debugging)
  * span: typically the anchor span (e.g., the `Name`, `Call`, `Subscript`, statement)

> The key design choice: **evaluation order is represented as explicit nodes and edges** (`EVAL_STEP` + `EVAL_NEXT`). That lets you test Python semantics directly instead of inferring them from AST shape.

---

## 2.2 Edge labels

### Syntax edges

* `AST_CHILD(parent → child, props={field,index})`
* `CST_CHILD(parent → child, props={field,index})`
* `CST_TOKEN(cst → token, props={index})`
* `COVERS(ast → token|cst, props={kind})`
* `PRIMARY_TOKEN(ast → token)` (optional convenience)

### SCIP symbol edges

* `DEFINES(anchor → SYMBOL, props={scip_occ_id})`
* `REFERS_TO(anchor → SYMBOL, props={scip_occ_id})`
* `SYMBOL_DEF(SYMBOL → anchor)` (defs only)

### Scope/binding edges

* `OWNS_SCOPE(ast_owner → scope)`
* `PARENT_SCOPE(scope_parent → scope_child)`
* `DECLARES(scope → binding)`
* `BINDS_DEF(name_store → binding, props={role})`
* `BINDS_USE(name_load → binding, props={role})`
* `BINDING_SYMBOL(binding → SYMBOL)` (optional)

### CFG / Eval edges

* `UNIT_ENTRY(unit_ast → cfg_block_entry)`
* `UNIT_EXIT(unit_ast → cfg_block_exit)`
* `CFG_NEXT(src_block → dst_block)`
* `CFG_TRUE(src_block → dst_block, props={cond_step})`
* `CFG_FALSE(src_block → dst_block, props={cond_step})`
* `CFG_EXC(src_block → handler_block, props={may_raise_step})`
* `CFG_FINALLY(src_block → finally_block, props={reason:return|break|continue|raise})`
* `STEP_IN_BLOCK(block → step, props={order})`
* `EVAL_NEXT(step_i → step_{i+1})`

### Dataflow edges

* `STEP_DEF(step → binding|slot, props={slot_kind})`
* `STEP_USE(step → binding|slot, props={slot_kind})`
* `REACHES(def_step → use_step, props={slot, certainty})`
* `CONTROL_DEP(cond_step → step|block)` (optional PDG)

### Calls (optional but usually immediate)

* `CALLS(call_step → SYMBOL, props={confidence,reason})`
* `ARG_OF(arg_step → call_step, props={pos|kw})`
* `BINDS_ARG(arg_step → binding_param, props={confidence})`

---

# 3) Minimal pseudo-IR for Python evaluation order (the exact lowering rules)

Think of this as compiling AST into a tiny “instruction tape” whose semantics are **Python’s**, not “whatever order your AST walker yields”.

## 3.1 Core IR concepts

* **Value temps**: `t0, t1, …` (not graph nodes; just IDs inside step props)
* **Storage slots**:

  * `BindingSlot(binding_id)` for locals/params/etc.
  * `AttrSlot(obj_temp, attr_name)`
  * `SubscrSlot(obj_temp, index_temp)` (note: index_temp is the *evaluated* index value)

Every `EVAL_STEP` op either:

* produces a temp (e.g., `CALL`, `LOAD_NAME`, `LOAD_SUBSCR`)
* consumes temps (e.g., `BINOP`, `STORE_SUBSCR`)
* transfers control (`BRANCH`, `JUMP`, `RETURN`, `RAISE`)

## 3.2 Expression lowering rules (selected, minimal-but-correct)

### Names

* `Name(Load)`:

  * `LOAD_NAME name → t`
  * emit `STEP_USE(step → binding(name))`
* `Name(Store)`:

  * defs happen via statement lowering (assignment), not directly as an expression

### Calls

Lower `f(a, b, *c, **d)` as:

1. lower `f` → `t_f`
2. lower positional args left-to-right → `t_a, t_b, …`
3. lower starargs/kwargs expressions in their textual order
4. `CALL t_f (args…) → t_ret`

Language ref guarantees left-to-right evaluation generally, and explicitly shows call evaluation order as part of the evaluation-order examples. ([Python documentation][1])

### Boolean short-circuit (`and` / `or`)

Lower `e1 and e2`:

1. eval `e1 → t1`
2. `BRANCH t1 → (true: eval e2) (false: result=t1)`
3. merge result temp (you can model with a `PHI` step or just a merge convention)

This is CFG-sensitive: you cannot model `and/or` as a plain `BINOP` if you want correct CFG/DFG.

### Subscript (read)

Lower `obj[idx]`:

1. eval `obj → t_obj`
2. eval `idx → t_idx` (expression list evaluated left-to-right in general) ([Python documentation][1])
3. `LOAD_SUBSCR t_obj, t_idx → t_val`
   (semantically corresponds to `__getitem__`) ([Python documentation][1])

### Assignment expression evaluation order constraint

The language ref states: expressions evaluate left-to-right, but **during assignment** the RHS is evaluated before the LHS. ([Python documentation][1])
So for `target = expr`, your lowering must:

1. lower RHS first
2. then evaluate target “address” and store

### Augmented assignment (this is the notorious one)

The language ref is explicit: augmented assignment:

* evaluates the **target first**
* evaluates it **only once**
* evaluates RHS after LHS target evaluation
* performs operation, then writes back. ([Python documentation][2])

Lower `obj[idx()] += rhs()` as:

1. **Evaluate target address exactly once**

   * eval `obj → t_obj`
   * eval `idx() → t_idx`  (**one call only**)
   * (optional) compute “slot” = `SubscrSlot(t_obj, t_idx)`
2. **Read old value**

   * `LOAD_SUBSCR t_obj, t_idx → t_old`
3. **Evaluate RHS**

   * eval `rhs() → t_rhs`
4. **Compute new value**

   * `BINOP "+=" t_old, t_rhs → t_new`  (you can model in-place attempt vs fallback in props)
5. **Write back using same evaluated slot**

   * `STORE_SUBSCR t_obj, t_idx, t_new`

That is exactly what your `EVAL_STEP` stream and your tests will enforce.

---

## 3.3 Statement lowering rules (CFG shaping)

### If statement

Lower:

1. eval test expr steps
2. terminate block with `BRANCH(test_temp)`
3. emit then-block and else-block
4. connect to merge-block with `CFG_NEXT`

### For loop (minimal form)

Lower `for x in xs: body` as:

1. eval `xs → t_iterable`
2. `ITER_INIT t_iterable → t_iter`
3. loop_header:

   * `ITER_NEXT t_iter → (has_item?, item_temp)`
   * `BRANCH(has_item)`
4. on true:

   * `STORE_NAME x = item_temp` (a def)
   * body
   * jump back to header
5. on false:

   * fallthrough after loop

### try/finally (the “on the way out” rule)

The language ref: finally runs even when leaving via return/break/continue/exception; return in finally overrides. ([Python documentation][3])

Lower:

* Any terminator inside try-body (`RETURN`, `BREAK`, `CONTINUE`, `RAISE`) becomes:

  1. set a “pending exit reason” + any “pending return value”
  2. `JUMP finally_entry`
* finally block:

  * execute steps
  * if finally itself terminates (return/break/continue/raise), that wins
  * else “resume” the pending reason:

    * if pending return: `RETURN pending_value`
    * if pending raise: re-raise
    * etc.

### with statement (desugar to try/finally with enter/exit)

The language ref gives the precise step order for `with EXPR as TARGET: SUITE`. ([Python documentation][3])
Lower:

1. eval `EXPR → t_mgr`
2. load `__enter__`, `__exit__` (can be implicit in your model, or explicit steps)
3. `ENTER_WITH t_mgr → t_enter_ret` (call enter)
4. if `as TARGET`: bind target from `t_enter_ret` (def)
5. execute suite
6. `EXIT_WITH t_mgr, exc_info_or_none` (call exit) on both normal and exceptional paths

For CFG: this is naturally a try/finally region; `EXIT_WITH` is in cleanup.

### Comprehension scope

Comprehensions run in a separate implicitly nested scope (except the leftmost iterable expression evaluated in the enclosing scope), and the for/if clauses nest left-to-right. ([Python documentation][1])
Lower:

1. evaluate leftmost iterable in enclosing scope
2. enter new `SCOPE(comprehension)`
3. iterate; assign target(s) inside comp scope; evaluate element expression; append/yield
4. exit comp scope

This is what prevents loop targets from “leaking” to the outer scope.

---

# 4) Golden fixture suite (pytest + snapshot JSON)

You want two kinds of tests:

1. **structure asserts** (cheap invariants; excellent signal)
2. **golden snapshots** (freeze a normalized CFG/DFG surface; detect drift)

Below is a “contract pack” structure you can drop into your repo (or adapt):

```
cpg_contract/
  normalize.py
  selectors.py
  expectations.py
tests/
  fixtures/
    01_if_reaching_defs.py
    02_loop_break_continue.py
    03_try_finally_return.py
    04_with_as_binding.py
    05_comp_scope_no_leak.py
    06_augassign_subscript_eval_once.py
  goldens/
    01_if_reaching_defs.json
    02_loop_break_continue.json
    03_try_finally_return.json
    04_with_as_binding.json
    05_comp_scope_no_leak.json
    06_augassign_subscript_eval_once.json
  test_cpg_goldens.py
```

## 4.1 Fixture programs (micro, surgical)

### `01_if_reaching_defs.py`

```python
def f(a):
    if a:
        x = 1
    else:
        x = 2
    return x
```

Asserts:

* CFG has two incoming paths into the return block.
* DFG: both defs of `x` reach the return-use of `x`.

---

### `02_loop_break_continue.py`

```python
def f(xs):
    s = 0
    for x in xs:
        if x < 0:
            continue
        s = s + x
        if s > 10:
            break
    return s
```

Asserts:

* CFG has `CONTINUE` edge back to loop header.
* CFG has `BREAK` edge out of loop.
* DFG: `s=0` reaches `return s` (empty loop case), and `s=s+x` also reaches `return s`.

---

### `03_try_finally_return.py`

```python
def f():
    try:
        return g()
    finally:
        h()
```

Asserts:

* CFG contains an edge from the try-body return path into finally (`CFG_FINALLY`).
* Evaluation order: `CALL g` happens before `CALL h` (since return expr must be computed before finally runs “on the way out”). ([Python documentation][3])

---

### `04_with_as_binding.py`

```python
def f():
    with cm() as x:
        use(x)
```

Asserts:

* DFG: def of `x` (from enter result) reaches use of `x` in `use(x)`.
* CFG executes cleanup (`EXIT_WITH` / `__exit__`) after suite. ([Python documentation][3])

---

### `05_comp_scope_no_leak.py`

```python
def f():
    x = 10
    ys = [x for x in range(3)]
    return x, ys
```

Asserts:

* Two distinct bindings named `x` exist: one in function scope, one in comprehension scope.
* `return x` uses the **outer** binding (no leak), while the element expression uses the **inner** binding. ([Python documentation][1])

---

### `06_augassign_subscript_eval_once.py`

```python
def f(obj, idx, rhs):
    obj[idx()] += rhs()
```

Asserts:

* Exactly **one** `CALL idx` step exists.
* `CALL idx` occurs before `CALL rhs`.
* The `STORE_SUBSCR` step reuses the evaluated index temp (i.e., no second idx call).
* Lowering respects “target evaluated once; LHS before RHS” per augmented assignment semantics. ([Python documentation][2])

---

## 4.2 Normalization contract (so goldens are stable)

Golden snapshots should not depend on ephemeral internal IDs. Normalize to **selectors**:

* For `CFG_BLOCK`: use an anchor label:

  * `ENTRY`, `EXIT`, else first anchored statement span `(line,col)` plus kind
* For `EVAL_STEP`: use `(op, anchor_span, text_slice)` (text slice optional, but very useful)
* For `BINDING`: use `(scope_path, name)`
* For `REACHES`: endpoints become the normalized `EVAL_STEP` selectors

### Example normalized JSON shape

Each golden file stores only what you care about:

```json
{
  "cfg_edges": [
    ["ENTRY", "If@2:4", "CFG_NEXT"],
    ["If@2:4", "Assign@3:8", "CFG_TRUE"],
    ["If@2:4", "Assign@5:8", "CFG_FALSE"],
    ["Assign@3:8", "Return@6:4", "CFG_NEXT"],
    ["Assign@5:8", "Return@6:4", "CFG_NEXT"],
    ["Return@6:4", "EXIT", "CFG_RETURN"]
  ],
  "reaches": [
    ["DEF(x)@3:8", "USE(x)@6:11"],
    ["DEF(x)@5:8", "USE(x)@6:11"]
  ],
  "eval_order_facts": [
    ["CALL(idx)@2:4", "CALL(rhs)@2:14", "EVALS_BEFORE"]
  ]
}
```

---

## 4.3 Pytest harness skeleton (snapshot + update mode)

### `tests/test_cpg_goldens.py`

```python
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

# Your project hook: must return a graph-like object with nodes/edges
# and enough props to normalize CFG + EVAL_STEP + REACHES surfaces.
from your_cpg_builder import build_cpg

from cpg_contract.normalize import normalize_cpg_surface

ROOT = Path(__file__).resolve().parent
FIXTURES = ROOT / "fixtures"
GOLDENS = ROOT / "goldens"

UPDATE = os.getenv("CPG_SNAPSHOT_UPDATE") == "1"

@pytest.mark.parametrize("name", [
    "01_if_reaching_defs",
    "02_loop_break_continue",
    "03_try_finally_return",
    "04_with_as_binding",
    "05_comp_scope_no_leak",
    "06_augassign_subscript_eval_once",
])
def test_cpg_goldens(name: str) -> None:
    src = (FIXTURES / f"{name}.py").read_text(encoding="utf-8")
    g = build_cpg(src, filename=f"{name}.py")

    surface = normalize_cpg_surface(g, src)

    golden_path = GOLDENS / f"{name}.json"
    if UPDATE:
        golden_path.write_text(json.dumps(surface, indent=2, sort_keys=True) + "\n")
        return

    expected = json.loads(golden_path.read_text(encoding="utf-8"))
    assert surface == expected
```

### `cpg_contract/normalize.py` (core idea; adapt to your graph model)

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

@dataclass(frozen=True)
class EdgeRec:
    src: str
    dst: str
    label: str

def normalize_cpg_surface(g: Any, src: str) -> dict[str, Any]:
    """
    Extract a stable surface for goldens:
      - cfg_edges: (src_block_anchor, dst_block_anchor, label)
      - reaches:   (def_selector, use_selector)
      - eval_order_facts: minimal ordering assertions (e.g., CALL idx before CALL rhs)
    """
    # --- 1) build selector maps ---
    block_sel = _block_selectors(g, src)
    step_sel = _step_selectors(g, src)
    binding_sel = _binding_selectors(g)

    # --- 2) collect CFG edges ---
    cfg_edges: list[list[str]] = []
    for e in g.edges(label_prefix="CFG_"):
        s = block_sel.get(e.src, e.src)
        d = block_sel.get(e.dst, e.dst)
        cfg_edges.append([s, d, e.label])
    cfg_edges.sort()

    # --- 3) collect REACHES edges ---
    reaches: list[list[str]] = []
    for e in g.edges(label="REACHES"):
        s = step_sel.get(e.src, e.src)
        d = step_sel.get(e.dst, e.dst)
        reaches.append([s, d])
    reaches.sort()

    # --- 4) minimal eval-order facts (optional but recommended) ---
    eval_facts = _extract_eval_facts(g, step_sel)

    return {
        "cfg_edges": cfg_edges,
        "reaches": reaches,
        "eval_order_facts": eval_facts,
    }

def _block_selectors(g: Any, src: str) -> dict[str, str]:
    # Example: ENTRY/EXIT or first-statement anchor in block props
    out: dict[str, str] = {}
    for n in g.nodes(label="CFG_BLOCK"):
        kind = n.props.get("block_kind")
        if kind == "entry":
            out[n.id] = "ENTRY"
        elif kind == "exit":
            out[n.id] = "EXIT"
        else:
            out[n.id] = n.props.get("anchor", n.id)
    return out

def _step_selectors(g: Any, src: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for n in g.nodes(label="EVAL_STEP"):
        op = n.props.get("op", "STEP")
        anchor = n.props.get("anchor", "")
        name = n.props.get("name")  # e.g. callee name if you record it
        if op == "CALL" and name:
            out[n.id] = f"CALL({name})@{anchor}"
        elif op in ("LOAD_NAME", "STORE_NAME") and name:
            out[n.id] = f"{op}({name})@{anchor}"
        else:
            out[n.id] = f"{op}@{anchor}"
    return out

def _binding_selectors(g: Any) -> dict[str, str]:
    out: dict[str, str] = {}
    for n in g.nodes(label="BINDING"):
        scope = n.props.get("scope_path", "?")
        name = n.props.get("name", "?")
        out[n.id] = f"{scope}::{name}"
    return out

def _extract_eval_facts(g: Any, step_sel: dict[str, str]) -> list[list[str]]:
    """
    Pull a tiny set of "ordering facts" from EVAL_NEXT edges,
    e.g. first CALL(idx) precedes CALL(rhs) in augassign fixture.
    """
    facts: list[list[str]] = []
    # This is intentionally minimal: your fixture-specific tests can also assert counts.
    # You may also choose to omit this and rely on goldens for EVAL_NEXT sequences.
    return facts
```

That’s the skeleton: your real implementation decides how nodes/edges are stored and how anchors are computed; the harness just requires you to *expose enough props to normalize*.

---

# 5) What “done” looks like (operational definition)

You can call your CPG build “mechanistically correct (v1)” when all of the following are true:

* **AugAssign subscript** fixture proves: LHS target evaluated once; LHS-before-RHS; correct sequencing of idx/rhs; store uses cached index. ([Python documentation][2])
* **try/finally** fixture proves: return path flows into finally before exiting; finally dominates exit on that path. ([Python documentation][3])
* **with** fixture proves: enter/assign/suite/exit ordering is correct and cleanup runs. ([Python documentation][3])
* **comprehension** fixture proves: comp scope is separate; no name leak; leftmost iterable evaluated in outer scope (at least structurally). ([Python documentation][1])
* **if/loop** fixtures prove: CFG edges + reaching-def edges match expectations (including RHS-before-LHS for plain assignment where relevant). ([Python documentation][1])

Once these are green, you have a **test-locked semantics core**. From there, expanding to richer CPG surfaces (exceptions edges, interprocedural arg binding, attribute/subscript aliasing, call resolution confidence scoring, etc.) becomes incremental rather than existential.

If you want the next increment after this pack: I can provide a **fully specified normalization schema** (exact selector rules for every node type you’ll snapshot) and a **complete set of starter goldens** for the six fixtures above (so you can copy/paste them without generating first).

[1]: https://docs.python.org/3/reference/expressions.html "6. Expressions — Python 3.14.2 documentation"
[2]: https://docs.python.org/3/reference/simple_stmts.html "7. Simple statements — Python 3.14.2 documentation"
[3]: https://docs.python.org/3/reference/compound_stmts.html "8. Compound statements — Python 3.14.2 documentation"
