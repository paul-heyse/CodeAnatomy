# GRAPH_RELATIONSHIP_COMPILATION_GUIDE — “Abstract Operations” Semantics for CPG Relationships

This guide documents, in an **exhaustive, implementation-aligned way**, the **abstract operations** this system is intended to perform when compiling **relationships** and emitting a **Python Code Property Graph (CPG)**.

It is written so that **any engineer or AI agent** can:
- understand what each evidence source “means”,
- understand what each join/selection produces (the *new insight*),
- understand why ambiguity exists and how we treat it,
- and evaluate whether a code change preserves the intended semantics.

> **Scope of this guide**
> - Focuses on: *extraction evidence meaning* → *normalization* → *relationship compilation* → *edge/node emission*
> - Assumes “Ultimate Python CPG” design goals: syntax + symbols + scope/binding + diagnostics + flow + optional runtime overlay
> - Treats the pipeline as **a relational compiler**: evidence tables → relationship tables → graph tables.

---

## Table of contents

1. Core mental model: evidence tables → relationship tables → CPG tables  
2. Evidence sources and what they mean (AST, CST/LibCST, SCIP, symtable, bytecode/dis, tree-sitter)  
3. Canonical normalization rules (byte spans, stable IDs, path identity)  
4. Relationship compilation: rule types, Acero execution semantics, ordering & determinism  
5. Relationship families (symbol resolution, calls, imports, scopes/bindings, bytecode anchoring, flow, diagnostics)  
6. Ambiguity: where it comes from, how we represent it, how we rank/select winners  
7. Edge emission rules: from relationships to CPG edges (symbol_roles-driven, call fallback, flow layers)  
8. Validation & contracts: how the registry is enforced at compile time  
9. Debugging and change evaluation: run bundles, intermediate materializers, failure modes  
10. Appendix: canonical columns, confidence taxonomy, and “superior evidence” policy

---

# 1) Core mental model: “relational compiler for code intelligence”

## 1.1 The system is a compiler pipeline
The CPG pipeline behaves like a compiler whose “program” is your repository, and whose intermediate representations are **Arrow tables**.

There are three stable representation layers:

### A) Evidence tables (raw facts; per source)
- Produced independently per extractor.
- Each table expresses facts in the *native semantics* of that extractor.
- Evidence tables may be incomplete, ambiguous, or missing entirely.

### B) Normalized tables (join-ready facts)
- All evidence is converted to a single coordinate system: **byte spans**.
- Stable IDs are computed.
- Normalized tables have predictable join keys.

### C) Relationship tables (cross-source linkages)
- Created by compiling relationship rules into deterministic plans.
- Each relationship table encodes a specific “insight” that does not exist in any single evidence source alone.

### D) Final CPG tables (graph IR)
- Nodes, edges, and properties are emitted deterministically.
- Relationships are *consumed* and projected into final graph edges (and sometimes new nodes).

---

## 1.2 “New insight” is generated only by linkages
A single source gives partial truth:
- CST tells you the code text and structure.
- AST tells you semantic structure (with some loss of formatting).
- SCIP tells you symbol identities across files/modules (when available).
- symtable tells you lexical scope/binding facts (compiler truth).
- bytecode tells you evaluation order and actual control flow structure.

The CPG becomes “best in class” when we compute *relationships* like:
- “This CST name reference is the same thing as this SCIP symbol occurrence”
- “This name reference reads a binding from an outer scope”
- “This bytecode instruction corresponds to this AST node”
- “This callsite resolves to this symbol (else fallback to qualified name)”

The goal is to make these linkages:
- **explicit**
- **contracted**
- **ambiguity-preserving**
- **deterministic**

---

# 2) Evidence sources: what they mean (granular semantics)

This section defines each evidence source as an information model. The rest of the guide treats these sources as the raw inputs to relationship compilation.

---

## 2.1 CST: LibCST (Concrete Syntax Tree) evidence

### What CST means
LibCST parses Python source into a syntax tree that **retains formatting and concrete tokens** (including whitespace and comments). It’s the best lens for:
- exact source spans
- syntactic forms (e.g., attribute access vs name, call syntax)
- faithful anchoring for UI and diffs

### Metadata providers (what they give you)
LibCST’s metadata system can supply:
- byte spans via `ByteSpanPositionProvider` (preferred for our CPG)【412:4†libcst.md†L5-L7】【412:4†libcst.md†L19-L40】
- scope analysis via `ScopeProvider`
- qualified name candidates via `QualifiedNameProvider` and `FullyQualifiedNameProvider`【412:4†libcst.md†L19-L40】
- repo-level name resolution context via `FullRepoManager` (important for package roots)【412:4†libcst.md†L26-L31】

### Core CST-derived entity types (conceptual)
We treat certain syntactic constructs as *CPG-relevant atoms*:

1. **Token spans**
   - the smallest anchoring units (byte spans of identifiers, operators, keywords)
2. **Name references**
   - occurrences of identifiers in expression/statement context (`x`, `foo`, `bar`)
3. **Attribute accesses**
   - `obj.attr` where `attr` is an identifier anchored to a span
4. **Definitions**
   - function/class/variable definitions (where an identifier is introduced)
5. **Imports**
   - `import x` / `from m import y as z` plus aliasing spans
6. **Calls**
   - `callee(args...)` with a “callee span” (name or attribute expression)

### Why CST is essential
- It provides the **most reliable anchoring spans** for human-facing graph features.
- It supports extracting “callsite callee shape” (NAME vs ATTRIBUTE vs SUBSCRIPT vs CALL-returned-call), which is essential for resolution heuristics.

### CST limitations (causes of ambiguity)
- CST can’t definitively resolve what a name “means” across modules.
- Qualified name providers may return multiple candidates for a name reference (imports, shadowing, aliasing).
- Some constructs are dynamic (e.g., `getattr(x, name)`), and CST cannot resolve them without extra evidence.

---

## 2.2 AST: Python `ast` evidence

### What AST means
Python’s built-in `ast` module produces an Abstract Syntax Tree that captures the semantic structure of code while discarding formatting and some lexical details.

AST is best for:
- semantic structure (function, class, control constructs)
- “container boundaries” (module/class/function bodies)
- bridging to compiler artifacts (symtable and bytecode both attach naturally to AST-level scopes)

### Core AST-derived entity types (conceptual)
1. **Def nodes**: `FunctionDef`, `AsyncFunctionDef`, `ClassDef`, `Lambda`, comprehensions
2. **Statements/expressions**: used for span containment and structural grouping
3. **Owner mapping**: a scope-producing AST node “owns” a lexical scope and a code object

### Why AST is essential
- symtable is *line-based* and tied to compiler scope structure; anchoring symtable scopes to AST is the reliable bridge【412:10†python_ast_libraries_and_cpg_construction.md†L22-L27】【412:10†python_ast_libraries_and_cpg_construction.md†L44-L56】
- bytecode code objects correspond closely to AST scope owners (functions/lambdas/comprehensions)

### AST limitations (causes of ambiguity)
- AST spans can be broader than CST identifier spans, and multiple AST nodes may cover the same token span.
- Some “semantic owners” can be difficult to map exactly (decorators, nested defs, typing meta-scopes).

---

## 2.3 SCIP: Symbol index evidence

### What SCIP means
SCIP indexes code into:
- **Documents** (files)
- **Symbols** (global, unique identifiers)
- **Occurrences** (symbol appearances in a document range)
- plus optional docs/metadata

An occurrence has:
- `symbol` (string identifier of the symbol)
- `symbol_roles` (bitmask of roles: definition/import/read/write/etc.)
- `range` (location within the document)【412:0†scip_python_overview.md†L1-L15】

SCIP is the best lens for:
- cross-file symbol identity
- definitions vs references vs imports
- read/write roles (when accurately emitted)

### Key SCIP role bits (used downstream)
The guide states typical role bits:
- 1 = Definition
- 2 = Import
- 4 = Write access
- 8 = Read access【412:0†scip_python_overview.md†L11-L15】

(We treat `symbol_roles` as a bitmask; an occurrence can have multiple bits set.)

### Why SCIP is “superior evidence” for symbol identity
- Its symbol strings are designed to be globally unique (encode language/package/module)【412:0†scip_python_overview.md†L38-L40】
- When SCIP data exists and span alignment succeeds, it is our **highest-confidence resolver** for names and call targets.

### SCIP limitations (causes of ambiguity)
- Range coordinates are typically line/character-based and must be normalized to bytes.
- There may be multiple occurrences overlapping a span (nested references, different granularities).
- The index may be missing, partial, or stale; many repos won’t have SCIP.

---

## 2.4 symtable: compiler symbol table evidence

### What symtable means
Python’s `symtable` module exposes the compiler’s lexical scope analysis:
- scopes (module/function/class + typing/annotation meta-scopes)
- symbols (names) per scope with flags (local/global/free/nonlocal/param/imported/etc.)

symtable is the best lens for:
- **lexical binding truth**: which name is local vs free vs global
- scope hierarchy: nested scopes
- parameter inventories (compiler truth)

### A critical invariant: symtable must be anchored to byte spans
Symtable is line-based; our join system is bytes-first:
- “Symtable is **line-based**. Your CPG is **bytes-first**. So symtable facts are only ‘index-grade’ once you **anchor** them to AST/CST spans.”【412:10†python_ast_libraries_and_cpg_construction.md†L22-L27】

### Why symtable is “superior evidence” for lexical binding
- It reflects compiler decisions (what’s local/free/global) not heuristics.
- It is the glue that makes bytecode def/use events meaningful (ties LOAD/STORE events to bindings)【412:3†python_ast_libraries_and_cpg_construction.md†L1-L39】

### symtable limitations (causes of ambiguity)
- Anchoring a symtable scope to an AST node can be ambiguous (multiple defs with same name, decorators, nested constructs).
- Typing/annotation meta-scopes can be hard to locate in syntax; may require heuristic anchoring with lower confidence【412:10†python_ast_libraries_and_cpg_construction.md†L55-L56】.

---

## 2.5 Bytecode / `dis`: compiled instruction evidence

### What bytecode evidence means
The CPython compiler produces code objects and bytecode instructions. `dis` can decode instructions, jumps, and exception regions.

Bytecode evidence is best for:
- **evaluation order**
- **control flow graph (CFG)** that is correct for `try/finally/with`
- static def/use events (`LOAD_*` / `STORE_*`)
- building a data flow graph (DFG) via reaching definitions, when paired with bindings

The guide explicitly frames bytecode as the substrate for CFG/DFG and “evaluation order”【412:13†python_ast_libraries_and_cpg_construction.md†L1-L3】.

### Key join: bytecode ↔ syntax anchoring
To use bytecode in a CPG, instructions must be anchored back to syntax:
- build instruction byte spans from `dis` positions
- join those spans to CST tokens and/or the smallest covering AST node
- emit a `BYTECODE_ANCHOR(instr_id → ast_node_id)` linkage【412:13†python_ast_libraries_and_cpg_construction.md†L17-L31】

### Why bytecode is “superior evidence” for CFG correctness
The guide calls out that try/finally/with are hard to get right from AST alone; bytecode + exception table makes CFG mechanical【412:13†python_ast_libraries_and_cpg_construction.md†L44-L47】.

### Where ambiguity comes from
- instruction position ranges can be coarse, mapping to multiple AST nodes
- some instructions may lack precise source mapping
- attribute/subscript operations are object-sensitive; static interpretation is heuristic at first【412:13†python_ast_libraries_and_cpg_construction.md†L61-L63】

---

## 2.6 Tree-sitter: parser + diagnostics evidence (optional)

### What tree-sitter means (in our context)
Tree-sitter is a fast, incremental parsing framework that produces a CST over **bytes** and supports:
- robust error recovery (`is_error`, `is_missing`)
- incremental invalidation via `changed_ranges`
- structural queries via `QueryCursor`

Node diagnostic semantics include:
- `is_missing` (parser recovery inserted tokens)
- `is_error`, `has_error`
- byte ranges and point ranges【412:5†tree-sitter.md†L35-L49】

### Why tree-sitter is “superior evidence” for diagnostics & partial code
LibCST and Python AST tend to assume syntactically valid code; tree-sitter is often more tolerant and can produce structured partial trees and error nodes.

### Incrementality model (important for IDE-like use cases)
- You must call `Tree.edit(...)` then parse with `old_tree` to compute `changed_ranges` correctly【412:1†tree-sitter_advanced.md†L12-L14】【412:2†tree-sitter_advanced.md†L24-L28】.

---

# 3) Canonical normalization rules (what makes joins possible)

This system treats normalization as the step that makes all evidence “join-ready”.

## 3.1 Path identity
Normalize all file identities to:
- `repo_root` + `relative_path` (preferred)
- `file_id` as stable hash over repo+path (or similar stable recipe)

## 3.2 Canonical coordinate system: byte spans
Every anchored entity gets:
- `bstart`: inclusive start byte
- `bend`: exclusive end byte

Rationale:
- tree-sitter ranges are byte-native【412:5†tree-sitter.md†L46-L49】
- LibCST can produce byte spans (ByteSpanPositionProvider)【412:4†libcst.md†L5-L7】
- symtable and SCIP are not byte-native and must be mapped/anchored to bytes【412:10†python_ast_libraries_and_cpg_construction.md†L22-L27】

## 3.3 Stable IDs
Every anchorable entity gets a stable ID based on:
- `file_id`
- `bstart, bend`
- `kind` (and optionally a discriminant like “name text” if two kinds can share span)

Stable IDs are the backbone of:
- deterministic dedupe
- deterministic edge emission
- run-to-run diffability

## 3.4 Schema shaping for join readiness
Normalization may:
- coerce types to canonical Arrow dtypes
- ensure required join keys exist
- create “dimension tables” (e.g., qualified names) for consistent referencing

---

# 4) Relationship compilation: rule types, Acero semantics, ordering & determinism

## 4.1 Relationship rules are declarative
A RelationshipRule describes:
- input datasets
- join keys / join type
- optional scoring logic
- output dataset name
- output contract

The compiler turns rules into executable plans.

## 4.2 Acero execution model (why the DSL exists)
Key “axioms” for DSL authors:
- declarations define a blueprint; ExecPlan is created at execution, and the sink depends on the execution method【416:4†arrow_acero_dsl_guide.md†L22-L27】
- ExecPlan is push-based【416:4†arrow_acero_dsl_guide.md†L25-L26】
- `to_table()` is an explicit “accumulate everything” sink【416:4†arrow_acero_dsl_guide.md†L1-L13】
- dataset scans are recommended for pushdown in production【416:4†arrow_acero_dsl_guide.md†L14-L18】

## 4.3 Ordering is not guaranteed — and we must enforce it
Arrow/Acero do not guarantee stable row order in many operations:
- “Row order is rarely guaranteed; if you need stable ordering, you must create and enforce it.”【416:9†arrow_acero_dsl_guide.md†L1-L7】
- hash join has no predictable output order【416:9†arrow_acero_dsl_guide.md†L56-L60】

This matters for:
- stable IDs derived from row order (avoid!)
- “keep first” dedupe semantics
- snapshot testing / diffing
- reproducibility across machines/threads

**Abstract operation requirement:** any stage that relies on ordering must:
1) create explicit ordering keys, and
2) sort by them before winner selection/dedupe.

## 4.4 Compute expressions: symbolic vs material scalars
When building compute expressions for filters/projections:
- `pa.scalar(...)` produces a material scalar value
- `pc.scalar(...)` produces an expression literal for compute/dataset expressions【416:11†pyarrow-advanced.md†L1-L14】

Field references (`pc.field` / `ds.field`) are symbolic until bound to schema【416:11†pyarrow-advanced.md†L20-L23】.

**Abstract operation requirement:** relationship compilation must treat filters/projections as expression trees, not eager Python computations.

---

# 5) Relationship families: what we compute, why it matters, and how ambiguity works

This section enumerates the intended relationship operations as a set of **abstract join/selection transforms**.

Each relationship has:
- Inputs
- Join type
- Output insight
- Ambiguity sources
- Ranking/winner selection policy

---

## 5.A) Symbol resolution (CST ↔ SCIP): “what does this name refer to?”

### A1) `rel_name_ref_occurrence` (CST name refs ↔ SCIP occurrences)
**Inputs**
- `cst_name_refs_norm` (file_id, name_ref_id, bstart, bend, name_text, context flags)
- `scip_occurrences_norm` (file_id, occ_id, bstart, bend, symbol, symbol_roles)

**Join type**
- **Interval-align join** within a file:
  - candidate if occurrence span intersects (or is contained by) the name_ref span

**Output insight**
- attaches a globally unique `symbol` to a syntactic name reference
- attaches role semantics (`symbol_roles`) that later emit edges like defines/references/reads/writes/imports

SCIP occurrence fields (symbol + roles + range) are the authoritative source of identity and roles【412:0†scip_python_overview.md†L1-L15】.

**Ambiguity sources**
- multiple SCIP occurrences may overlap the same CST span (nested occurrences, different granularities)
- span mismatch due to coordinate conversion errors (line/col → bytes)
- generated or partial SCIP indexes

**Candidate ranking (superior options)**
Prefer candidates that:
1) are fully contained within the name_ref span (strongest alignment)
2) have the smallest span (more precise occurrence)
3) match expected role bits given CST context (e.g., store vs load)
4) tie-break deterministically by `(occ_bstart, occ_bend, symbol)`.

**Winner selection**
- If the output dataset is intended to be **single-symbol per name_ref**, dedupe by `name_ref_id` with tie-breakers above.
- If multi-symbol ambiguity should be preserved, keep all candidates and group by `ambiguity_group_id = hash(name_ref_id)`.

---

### A2) `rel_attr_ref_occurrence` (CST attribute refs ↔ SCIP occurrences)
Same as A1, but for attribute identifiers inside `obj.attr`.

**New insight**
- allows edges for attribute usage to be symbol-resolved when SCIP has a qualified symbol for the attribute (common for modules/types in typed code).

**Ambiguity sources**
- attribute resolution depends on type; static symbolization may be partial
- multiple candidates from different provider layers

---

### A3) Roles-to-edges: turning `symbol_roles` into semantic edges
This is not a join, but a deterministic projection:
- if `symbol_roles` has Definition bit → `PY_DEFINES_SYMBOL`
- else → `PY_REFERENCES_SYMBOL`
- if Import bit → `PY_IMPORTS_SYMBOL`
- if Read bit → `PY_READS_SYMBOL`
- if Write bit → `PY_WRITES_SYMBOL`

SCIP role bitmask semantics are explicit in the overview【412:0†scip_python_overview.md†L11-L15】.

**Ambiguity sources**
- Some occurrences may set multiple bits (e.g., import+reference).
- Some ecosystems may not reliably set read/write bits; treat roles as “best effort” with confidence.

---

## 5.B) Imports: “what symbol does this import introduce?”

### B1) `rel_import_occurrence` (CST import alias spans ↔ SCIP import occurrences)
**Inputs**
- `cst_imports_norm` (import_id, alias spans, module span, imported name)
- `scip_occurrences_norm` where `symbol_roles` has Import bit【412:0†scip_python_overview.md†L11-L15】

**Join type**
- interval-align of alias span to occurrence span

**Output insight**
- `import_id → symbol` mapping (import edge)
- optional: track aliasing (`asname`) and original module spec

**Ambiguity sources**
- wildcard imports
- re-exports
- partial SCIP index

**Superior options**
- Prefer explicit import occurrences with Import bit set.
- If missing, fall back to LibCST qualified name provider candidates (lower confidence).

---

## 5.C) Call resolution: “what does this call invoke?”

Call resolution is inherently ambiguous in Python; the goal is to preserve candidates and prefer the strongest evidence.

### C1) `rel_callsite_occurrence` (CST callsites ↔ SCIP occurrences)
**Inputs**
- `cst_callsites_norm` (callsite_id, callee_span, callee_shape, args spans)
- `scip_occurrences_norm`

**Join type**
- interval-align: candidate if occurrence lies within callee_span

**Output insight**
- callsite → symbol edges (high confidence when SCIP resolves)

**Ambiguity sources**
- callee expression may be complex (call-returned-call, subscripts)
- multiple occurrences within callee span (e.g., `pkg.mod.fn` has several identifiers)
- resolution may refer to attribute vs base name

**Superior candidates**
Rank by:
1) occurrences anchored closest to the syntactic callee head
2) definition/reference role expectations
3) symbol specificity (prefer function/method symbols over module symbols when call parentheses apply)
4) fallback: smallest span and deterministic ties.

### C2) `rel_callsite_qname_candidates` (CST callsites ↔ qualified name candidates)
**Inputs**
- `cst_callsites_norm`
- LibCST `QualifiedNameProvider` / `FullyQualifiedNameProvider` candidates【412:4†libcst.md†L19-L40】

**Join type**
- key join on callee expression node id or span (depending on representation)

**Output insight**
- produces a set of possible “qualified name” targets for the callsite:
  - (callsite_id, qname, provider, confidence)

**Ambiguity sources**
- LibCST can return multiple qualified names for the same reference
- package root configuration matters; `use_pyproject_toml=True` influences FQNs【412:4†libcst.md†L26-L31】

**Why QNAME is lower confidence than SCIP**
- QNAME is a candidate list, not a globally unique symbol identity
- Python allows runtime mutation and shadowing that defeat static candidate enumeration

### C3) Call edge emission policy (preference + fallback)
Deterministic policy:
1) emit `PY_CALLS_SYMBOL` edges when `rel_callsite_symbol` exists (SCIP-resolved)
2) else emit `PY_CALLS_QNAME` edges from QNAME candidates
3) optionally retain both as layered edges if configured, but mark `origin` and confidence.

---

## 5.D) Scopes and bindings: “what binding slot is this name using?”

This family links AST/CST occurrences to symtable scopes and binding slots.

### D1) `rel_scope_owner` (symtable scope ↔ AST owner anchor)
**Inputs**
- `py_sym_scopes` + `py_sym_scope_edges`
- AST function/class/module spans

**Join type**
- name+lineno match, then best-span selection
- as documented: match `FunctionDef/AsyncFunctionDef` by (name, lineno), pick smallest span if ambiguous【412:10†python_ast_libraries_and_cpg_construction.md†L53-L56】

**Output insight**
- creates explicit SCOPE nodes and OWNS_SCOPE edges:
  - `OWNS_SCOPE(ast_owner → scope)` and `PARENT_SCOPE(parent_scope → child_scope)`【412:10†python_ast_libraries_and_cpg_construction.md†L39-L61】

**Ambiguity sources**
- decorators can shift line numbers
- nested defs with same names in different blocks
- meta scopes (typing/annotation) require heuristic anchoring【412:10†python_ast_libraries_and_cpg_construction.md†L55-L56】

**Superior options**
- exact match on lineno + name is highest
- smallest containing span is preferred
- if heuristic anchoring is needed, lower confidence but preserve edge for usefulness.

### D2) `rel_binding_slots` (symtable symbols → binding nodes)
**Inputs**
- `py_sym_symbols` per scope
- derived `py_sym_bindings` inventory

**Abstract operation**
- deterministically create one binding slot per `(scope_id, name)` (compiler truth)【412:10†python_ast_libraries_and_cpg_construction.md†L83-L87】

**Output insight**
- binding nodes represent “storage slots” in a scope:
  - local variable slots
  - parameters
  - free variables (captured)
  - globals
  - imported names

This is how we make name references and bytecode def/use talk about the same conceptual thing.

### D3) `rel_name_ref_binding` (CST name refs → binding slots)
**Inputs**
- CST name refs + containing scope_id (from span containment)
- symtable binding slots and resolution edges

**Join type**
- containment join to find the name’s containing lexical scope
- then lookup/resolve binding by `(scope_id, name)` including nonlocal/free/global resolution

**Output insight**
- name references become binding-anchored; enables:
  - `BINDS_DEF` (definition site)
  - `USES_BINDING` (use site)
  - `READS/WRITES` classification at binding level (later merged with SCIP)

**Ambiguity sources**
- dynamic name resolution (`globals()`, `locals()`)
- runtime mutation
- `from module import *`
- if symtable is missing or not anchored, fallback must be used (lower confidence).

### D4) Symtable improves call stitching when SCIP is missing
The guide explicitly notes symtable helps rank call candidates:
- classify `f` as local/global/free/imported
- improve heuristics when SCIP missing
- rank candidate callees and set confidence【412:3†python_ast_libraries_and_cpg_construction.md†L81-L89】

---

## 5.E) Bytecode anchoring + flow layers: CFG/DFG grounded in dis + bindings

### E1) `rel_instr_anchor` (bytecode instruction → AST/CST anchor)
**Inputs**
- `py_bc_instructions` with position spans mapped to bytes
- AST nodes / CST tokens spans

**Join type**
- interval-align: instruction span contained by AST node span → choose smallest
- optional secondary: exact token coverage

**Output insight**
- makes every instruction navigable back to syntax and (transitively) to SCIP and symtable facts【412:13†python_ast_libraries_and_cpg_construction.md†L17-L31】

### E2) CFG construction (from blocks + edges)
**Abstract operation**
- produce CFG block nodes and CFG edges from bytecode decoding:
  - `py_bc_blocks` → CFG_BLOCK nodes
  - `py_bc_cfg_edges` → CFG_NEXT/TRUE/FALSE/EXC edges【412:13†python_ast_libraries_and_cpg_construction.md†L33-L43】

**New insight**
- correct control flow, including exception edges, grounded in compiler output.

### E3) Def/use event extraction (instruction classification)
**Abstract operation**
- classify instruction rows into event rows:
  - STORE_* ⇒ DEF_SLOT
  - LOAD_* ⇒ USE_SLOT
  - and best-effort for attributes/subscripts【412:13†python_ast_libraries_and_cpg_construction.md†L57-L63】

The guide suggests a derived table `py_bc_defuse_events` with explicit columns including `event_kind`, `space`, `name`, and confidence【412:14†python_ast_libraries_and_cpg_construction.md†L1-L14】.

### E4) Def/use → binding resolution (symtable glue)
**Abstract operation**
- for each def/use event, resolve which binding slot it affects using symtable resolution:
  - map code_unit → scope_id
  - resolve binding using `(scope_id, name)` with FAST/DEREF/GLOBAL semantics
  - emit DEFINES_BINDING / USES_BINDING edges
The guide calls this the “glue that makes dis-based DFG coherent”【412:3†python_ast_libraries_and_cpg_construction.md†L1-L39】.

### E5) DFG (reaching definitions) over CFG
**Abstract operation**
1) build CFG
2) build def/use events
3) resolve events to binding slots
4) run reaching definitions
5) emit `REACHES(def_site → use_site)` edges【412:14†python_ast_libraries_and_cpg_construction.md†L15-L26】

**Ambiguity sources**
- object-sensitive memory ops (attr/subscript)
- exceptional control flow complicates reaching defs (but CFG helps)

**Superior options**
- explicit slot ops (`LOAD_FAST/STORE_FAST`) high confidence (1.0)
- heuristic memory ops lower confidence (e.g., 0.6)【412:14†python_ast_libraries_and_cpg_construction.md†L10-L14】

### E6) Bytecode calls stitched to SCIP (optional interprocedural layer)
The guide suggests:
- identify call instructions
- anchor to callee expression via BYTECODE_ANCHOR
- use SCIP reference edges from that syntax node to a SYMBOL
- emit `CALLS(callsite → SYMBOL)` with high confidence when resolved【412:13†python_ast_libraries_and_cpg_construction.md†L69-L77】

---

## 5.F) Diagnostics layer: tree-sitter + SCIP + pipeline diagnostics

### F1) Tree-sitter error nodes
**Abstract operation**
- parse with tree-sitter, extract nodes where:
  - `is_error` or `is_missing` or `has_error`
- store their `start_byte/end_byte` and `start_point/end_point` for UI and join alignment【412:5†tree-sitter.md†L44-L49】

**New insight**
- allows CPG to represent:
  - parse errors as graph nodes
  - impact ranges for incremental invalidation

### F2) Incremental invalidation (optional advanced mode)
**Abstract operation**
- call `Tree.edit` then parse with `old_tree` and compute `changed_ranges`【412:1†tree-sitter_advanced.md†L12-L14】
- lift each changed range to a stable container node and rerun only impacted query packs.

This is how the pipeline supports IDE-like “reindex only what changed”.

---

# 6) Ambiguity: representation, ranking, and why some options are superior

## 6.1 Where ambiguity comes from (taxonomy)
1) **Span ambiguity**
   - multiple candidates overlap a span
   - coordinate mismatch creates false overlaps or misses

2) **Name ambiguity**
   - same identifier text refers to different things depending on scope/imports
   - dynamic rebinding

3) **Call ambiguity**
   - Python allows runtime replacement of functions/attributes
   - method resolution depends on runtime type

4) **Flow ambiguity**
   - object-sensitive operations collapse to heuristic “attr slot” nodes
   - exceptional flow introduces multiple feasible reaching paths

5) **Index incompleteness**
   - SCIP missing or partial
   - symtable anchoring missing for meta scopes
   - parse errors

## 6.2 How we represent ambiguity
Every relationship dataset that can produce multiple candidates must include:
- `confidence` (0..1)
- optional `score` (for ranking among candidates)
- `ambiguity_group_id` for grouping alternatives (often derived from the left entity id)
- `origin` / `provider` / `rule_name` metadata for explainability

## 6.3 “Superior evidence” policy (ranking precedence)
When multiple sources can answer the same question, prefer in this order:

1) **SCIP symbol identity** (cross-file, unique)  
   - best for symbol edges and call edges when aligned successfully【412:0†scip_python_overview.md†L1-L15】

2) **symtable binding classification** (compiler truth)  
   - best for lexical scoping and name binding, and it improves call heuristics when SCIP missing【412:3†python_ast_libraries_and_cpg_construction.md†L81-L89】

3) **bytecode control flow** (compiler truth for CFG)  
   - best for try/with/finally semantics and evaluation order【412:13†python_ast_libraries_and_cpg_construction.md†L44-L47】

4) **LibCST qualified-name candidates**  
   - good fallback when SCIP missing; may be multiple candidates【412:4†libcst.md†L19-L40】

5) **Heuristics** (string matching, local-only)  
   - used only as last resort; must be confidence-tagged.

## 6.4 Winner selection: deterministic, contract-driven
Because row order is not guaranteed in Acero joins【416:9†arrow_acero_dsl_guide.md†L56-L60】, winner selection must always:
1) establish explicit sort keys, then
2) keep-first after sort (or score-based top-1), then
3) emit deterministic ties.

---

# 7) Edge emission: how relationship datasets become final CPG edges

This layer consumes relationship outputs and emits edges under the “Ultimate kind registry” contracts.

## 7.1 Symbol-role-driven edges
Given relationship `(syntax_id → symbol, symbol_roles)`:
- emit `PY_DEFINES_SYMBOL` if Definition bit set
- emit `PY_IMPORTS_SYMBOL` if Import bit set
- emit `PY_READS_SYMBOL` if Read bit set
- emit `PY_WRITES_SYMBOL` if Write bit set
- else emit `PY_REFERENCES_SYMBOL`【412:0†scip_python_overview.md†L11-L15】

## 7.2 Call edges: prefer SCIP, fallback to QNAME
- Primary: `PY_CALLS_SYMBOL` from callsite→symbol relationships
- Fallback: `PY_CALLS_QNAME` from qualified-name candidates
- Mark edges with `origin` (scip vs qname) and confidence.

## 7.3 Scope/binding edges
- `OWNS_SCOPE`, `PARENT_SCOPE` from symtable scope relationships【412:10†python_ast_libraries_and_cpg_construction.md†L39-L61】
- `DECLARES(scope → binding)` and def/use edges from binding resolution
- optional: parameter binding initialization edges (BINDS_DEF at entry)【412:3†python_ast_libraries_and_cpg_construction.md†L42-L76】

## 7.4 Flow edges
- CFG edges from bytecode tables【412:13†python_ast_libraries_and_cpg_construction.md†L33-L43】
- DFG reaching definitions edges from def/use resolution + reaching defs【412:14†python_ast_libraries_and_cpg_construction.md†L15-L26】

## 7.5 Diagnostics edges
- parse error nodes (tree-sitter) and diagnostic edges to affected syntax spans or file nodes.

---

# 8) Validation & contracts: how we prevent semantic drift

## 8.1 Single source of truth: Ultimate kind registry
`cpg/kinds_ultimate.py` defines:
- NodeKind/EdgeKind enums
- required properties per kind
- derivation manifest (expected extractor / join keys / id recipe / confidence)

## 8.2 Contract validation at relationship compilation time
The relspec compiler includes a validator hook that enforces:
- when a relationship dataset feeds edge kind X, its output contract provides all `required_props` for edge kind X,
- either as real columns or declared `virtual_fields`.

This prevents silent drift when refactoring relationship outputs.

---

# 9) Debugging & change evaluation: how to prove a change is aligned

## 9.1 The “run bundle” workflow
When making changes:
1) run pipeline on a small fixture repo
2) write normalized inputs (Parquet) and relationship outputs
3) write run bundle:
   - config snapshot
   - schema fingerprints
   - registry + contract snapshots
   - row counts per dataset

This gives reproducible diffs and lets you see where semantics changed.

## 9.2 Intermediate materializers (critical for join debugging)
Requestable outputs should include:
- normalized evidence datasets (by source)
- each relationship output dataset
- final CPG nodes/edges/props

When a join fails, inspect:
- coordinate systems (are bstart/bend aligned?)
- path identity
- candidate overlap statistics

---

# 10) Appendix

## 10.1 Canonical join keys (recommended)
- Intra-file interval joins: `(file_id, bstart, bend)`
- Symbol joins: `symbol`
- Scope/binding joins: `scope_id`, `(scope_id, name)`
- Callsite joins: `callsite_id`, `callee_span` for span align, or `callee_expr_id` for structural join

## 10.2 Confidence scale (recommended)
- 1.0: compiler truth / explicit slot ops / SCIP resolved
- 0.8: symtable-anchored but heuristic span match
- 0.6: heuristic attribute/subscript def/use events【412:14†python_ast_libraries_and_cpg_construction.md†L10-L14】
- 0.4: QNAME candidates without SCIP
- 0.2: string/heuristic fallbacks

## 10.3 Why “bytes-first” is non-negotiable
- tree-sitter uses byte offsets directly【412:5†tree-sitter.md†L46-L49】
- LibCST can produce byte spans【412:4†libcst.md†L5-L7】
- symtable is only index-grade once anchored to bytes【412:10†python_ast_libraries_and_cpg_construction.md†L22-L27】
This is the only coordinate system that makes cross-source joins reliable across encodings and incremental edits.

---

## References (repo docs)
- `python_ast_libraries_and_cpg_construction.md` (symtable + dis + CFG/DFG algorithms)  
- `scip_python_overview.md` (symbol_roles, occurrences model)  
- `libcst.md` / `libcst-advanced.md` (metadata providers, byte spans, qualified names)  
- `tree-sitter.md` / `tree-sitter_advanced.md` (diagnostics, incrementality, byte semantics)  
- `arrow_acero_dsl_guide.md` (Acero ExecPlan model, ordering semantics)  
- `pyarrow-advanced.md` (compute expression semantics, dataset scanning model)  
