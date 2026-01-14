Below are **PR‑05** and **PR‑06** in the same “scope item” style as your `datafusion_engine_migration_plan.md`, but with **much denser technical narrative** and **no target file list**, per your request. 

According to a document in your repo (`datafusion_engine_migration_plan.md`; timestamp metadata wasn’t provided in the snippet), PR‑05 is essentially **Scope 12: “Normalize, Extract, and CPG Plan Builders in Ibis”**—i.e., replacing the remaining ad‑hoc plan builders with **Ibis expression builders** while preserving output invariants and handling optional inputs correctly. 

You’re right that the earlier PR‑05 notes were “directionally correct but underspecified.” Below is the missing “zero‑thinking required” context: **rules to apply**, **functional design target**, **why** each rule exists, and exactly what is meant by:

* **“stable ID generation must be expressed relationally”**
* **“span normalization must be join-driven against a line index”**

I’ll make this concrete enough that a Python expert can implement without having to infer what we mean.

---

# 1) PR‑05 functional design target

PR‑05 is not “convert code to Ibis” in the abstract. It is specifically:

### Target state

Every dataset derivation step in **normalize/**, **extract/** (post-parse shaping), and **cpg/** (relationship-derived outputs) becomes:

1. **A pure, declarative Ibis table expression template**

   * Inputs: named datasets (Ibis tables) + param tables
   * Output: an Ibis expression (lazy)

2. **Contract-first**

   * The plan explicitly *projects* the contract columns and aliases them into canonical names
   * It does not leak “accidental extra columns” as part of the dataset contract.

3. **Deterministic-by-construction**

   * If the dataset needs canonical determinism, the expression includes explicit sorting keys / winner selection logic, rather than “whatever the engine happens to return.”

4. **Join-ready**

   * Normalization produces canonical join keys (especially byte spans + stable ids) so relspec rules can join without bespoke glue code.

This aligns exactly with the migration plan’s definition of PR‑05/Scope 12: “Convert normalize/extract/CPG plan builders to Ibis expressions, preserving dataset schemas and output invariants.” 

---

# 2) “Stable ID generation must be expressed relationally”

## 2.1 What “stable ID” means here

A **stable ID** is an identifier that:

* is **deterministic** across runs given the same repo content + same normalization rules,
* is **independent of row order** or partitioning,
* is **portable** across execution lanes (Ibis/DataFusion/Arrow),
* and is derived from **semantic identity**, not incidental computation.

Examples:

* syntax anchor node ID = identity of *(file_id, span_start_byte, span_end_byte, kind)*
* edge ID = identity of *(src_id, dst_id, edge_kind, anchor span if applicable)*
* binding ID = identity of *(scope_id, name)* (compiler-truth inventory) 

## 2.2 What “expressed relationally” means (the non-negotiable rule)

**Stable IDs must be produced as columns by a plan**, not assigned after the fact by Python control flow.

Concretely, this means:

✅ Allowed:

* `stable_id = stable_hash64(kind, file_id, bstart, bend, extra…)` computed via:

  * an engine builtin hash function, or
  * a DataFusion UDF (vectorized), or
  * an Arrow kernel that is invoked as part of a hybrid “relational → kernel → relational” block (still treated as part of the compiled plan output).

❌ Not allowed:

* `enumerate(rows)` / “row_number used as identity”
* “append a UUID per row in Python”
* “take engine row order and assign sequential IDs”

### Why it must be relational

Because Arrow/Acero and parallel engines do not guarantee stable row ordering unless you enforce it, and ordering can change with threading and versions. Your DSL doc explicitly calls out that ordering guarantees are *not stable* under threading and that determinism must be encoded as a contract, not developer memory. 

If you generate IDs procedurally after materialization:

* the same logical dataset can produce different IDs across runs,
* downstream joins break,
* and run bundles can’t be diffed meaningfully.

## 2.3 The minimal rules for stable ID recipes

You should standardize these rules globally (and then enforce them via a “finalize gate” or compiler validator):

### Rule ID‑1: IDs must be a function of *semantic identity keys*

Pick keys that define “same fact,” similar to your DSL’s dedupe guidance: choose keys that represent semantic identity, not incidental structure. 

Examples of semantic identity keys:

* **Syntax anchor**: `(file_id, bstart, bend, kind, subkind?)`
* **Callsite**: `(file_id, callee_bstart, callee_bend, call_paren_bstart, call_paren_bend)` (if you need call-paren stability)
* **Symbol node**: `(scip_symbol_string)`
* **Binding slot**: `(scope_id, name)` 
* **Edge**: `(src_id, dst_id, edge_kind, anchor_span?)`

### Rule ID‑2: IDs must not depend on row order

No use of:

* implicit record batch order,
* `row_number()` as identity (you *can* use row_number for winner selection, but not for ID).

### Rule ID‑3: Normalize end semantics before hashing

Before hashing:

* normalize spans to the same convention (start inclusive, end exclusive),
* normalize any “kind” strings (canonical enum value),
* normalize strings (ensure consistent casing and encoding policies).

### Rule ID‑4: Use a single canonical hash implementation

This is the practical “best-in-class” approach:

* define `stable_hash64` and `stable_hash128` as *named kernel functions* in your hybrid kernel bridge
* provide:

  * a DataFusion UDF implementation (preferred runtime),
  * an Arrow kernel fallback implementation.

Then in Ibis you only ever call:

* `f.stable_hash64(...)` (function registry entry)
  and the compiler determines whether that compiles to:
* DataFusion builtin hash (if you decide), or
* DataFusion UDF, or
* fallback.

This ensures:

* identical IDs across engines,
* and one place to audit hash behavior.

### Rule ID‑5: Emit both “natural keys” and “hashed id” during migration

During the PR‑05 migration period, output datasets should include:

* the stable ID column, **and**
* the natural key columns used to generate it (at least in debug mode).

This makes debugging *dramatically* easier when you later discover span normalization mistakes.

---

# 3) “Span normalization must be join-driven against a line index”

This is the other critical PR‑05 hinge, and it’s the piece that makes cross-source joins actually work.

## 3.1 Why span normalization exists (the bytes-first invariant)

Multiple upstream sources describe locations differently:

* symtable is line-based (line/col)
* AST often provides lineno/end_lineno + col offsets
* SCIP occurrences use ranges in line/character space
* LibCST can give byte spans directly
* tree-sitter is byte-native

But your CPG joins need a single coordinate system, and your design target is **bytes-first**, because byte spans are the best join key across tools and languages.

Your CPG construction guide states the invariant explicitly:

> Symtable is **line-based**. Your CPG is **bytes-first**. So symtable facts are only “index-grade” once you **anchor** them to AST/CST spans. 

Span normalization is the step that converts “line/col-ish” spans into **canonical byte spans**.

## 3.2 What a “line index” is (precise definition)

A **line index** is a dataset that, for every file, provides a mapping:

> **(file_id, line_no)** → **line_start_byte_offset**

Optionally also:

* line_end_byte_offset
* line_text (or line_bytes)
* per-line encoding features used for col→byte conversion

### Minimal line index schema

`file_line_index`:

* `file_id: int64` (or stable file id)
* `line_no: int32` (choose a canonical base: 0-based or 1-based; I recommend 0-based internally)
* `line_start_byte: int64`
* `line_end_byte: int64` (exclusive)
* `line_text: string` (optional but extremely useful for col→byte conversion and validation)
* `newline_kind: string` (optional diagnostic: LF/CRLF/none-last-line)

### Why this table exists at all

Because you will normalize **millions** of spans across a repo. You don’t want to:

* repeatedly scan file contents per span,
* run Python loops per span,
* or re-derive line starts independently in every extractor.

Instead you compute line starts once per file, store them, and then all span normalization is a **join + arithmetic** problem.

That is what “join-driven” means.

## 3.3 What “join-driven span normalization” means mechanically

### Inputs

You’ll usually have “raw span” tables with some variant of:

* `file_id`
* `start_line`, `start_col`
* `end_line`, `end_col`
* `coord_system` metadata (more on this below)

### Output

A normalized span table with:

* `bstart`, `bend` (bytes, canonical)
* `span_quality` / `confidence`
* “raw coords preserved” columns (for debugging)

### The join-driven computation (single-line case)

To normalize a span:

1. join raw spans to `file_line_index` on `(file_id, start_line)`
2. compute `bstart = line_start_byte + col_to_byte(line_text, start_col, coord_system)`
3. join raw spans to `file_line_index` on `(file_id, end_line)`
4. compute `bend = line_start_byte + col_to_byte(line_text, end_col, coord_system)`

This is relational because:

* the file contents are not re-read inside the normalization for each span,
* the per-line reference data is brought in via joins,
* and the rest is vector arithmetic/UDF.

### “col_to_byte” is the one hard part

Different tools define “col” differently (and that’s where span normalization commonly goes wrong).

You should treat this as a **kernel** (vectorized UDF):

* input: `line_text` (string) and `col` (int)
* output: `byte_offset_into_line` (int)

The join-driven part provides the `line_text`. The kernel converts col semantics into bytes.

---

# 4) Where span normalization goes wrong (common failure modes + consequences)

This is the context you explicitly asked for: what breaks, why, and what the downstream blast radius is.

## 4.1 Failure mode: line numbering base mismatch

Different sources may use:

* 0-based lines (common in LSP-like systems),
* 1-based lines (common in Python lineno).

If you join `(file_id, line_no)` with the wrong base:

* you compute bstart/bend on the wrong line,
* and spans “look plausible” but are wrong.

**Consequence**

* AST↔CST joins by byte span fail or mis-join
* SCIP occurrence linking attaches symbols to the wrong identifiers
* your stable IDs become permanently wrong (because they’re hashed from bstart/bend)

This is catastrophic because it produces *confidently wrong graph edges*, not just missing data.

## 4.2 Failure mode: end position convention mismatch (inclusive vs exclusive)

Some sources treat `end_col` as:

* exclusive end,
* inclusive end,
* or “end of token” vs “end of node”.

If you don’t normalize end semantics, you get:

* off-by-one errors on bends
* overlaps that shouldn’t exist
* missing containment relationships

**Consequence**

* interval alignment joins (e.g., name token ↔ SCIP occurrence) become unstable
* dedupe keys that include span_end change spuriously
* callsite/callee anchoring becomes inconsistent

## 4.3 Failure mode: “col” is not bytes (UTF‑8 vs codepoints vs UTF‑16)

This is the most common subtle bug.

Possible col meanings:

* byte offset into UTF‑8 bytes
* Unicode codepoint index
* UTF‑16 code unit index (LSP often uses this)
* “display columns” (tab expansion) — less common but real in some tools

If you assume `col` is bytes but it’s actually codepoints (or UTF‑16), multi-byte characters cause drift.

**Consequence**

* ASCII-only repos look fine
* repos with emoji / non-Latin identifiers produce broken cross-source joins
* symbol resolution coverage drops unpredictably

## 4.4 Failure mode: newline normalization mismatches

If one source reads files with universal newlines (CRLF→LF) and another treats raw bytes:

* line_start_byte differs
* col offsets refer to different underlying strings

**Consequence**

* widespread join failure between sources for CRLF files
* “mostly works” until you hit Windows-origin files, then everything breaks

## 4.5 Failure mode: tabs treated inconsistently

If a tool defines col as “visual columns” (tabs expand to 8) and you treat it as “character index”:

* offsets drift after tabs

**Consequence**

* wrong spans in indented code blocks
* especially damaging for leading whitespace tokens and decorators

---

# 5) How to prevent span normalization errors (design + validation rules)

## 5.1 Span normalization rules (the concrete checklist)

### Rule SPAN‑1: Every raw span dataset must declare its coordinate system

Add columns like:

* `line_base`: 0 or 1
* `col_unit`: `"byte"` | `"codepoint"` | `"utf16"`
* `end_is_exclusive`: bool

This avoids “tribal knowledge” and makes normalization explicit.

### Rule SPAN‑2: Normalize to one canonical span convention

Canonical:

* line_base → 0-based internal (or 1-based, but pick one)
* end_is_exclusive → true
* bstart/bend are UTF‑8 byte offsets into the canonical text representation

### Rule SPAN‑3: Use the line index for line_start_byte; never recompute in the row loop

Even if you *could* compute bstart by scanning the whole string each time, don’t.
Join to `file_line_index` and add offsets.

This is the “join-driven” rule.

### Rule SPAN‑4: “col_to_byte” must be centralized and tested

Implement `col_to_byte(line_text, col, col_unit)` as:

* DataFusion UDF for runtime
* Arrow kernel fallback (for correctness testing and non-DF lanes)

### Rule SPAN‑5: Preserve raw coords + emit span_quality flags

Every normalized span row should include:

* raw line/col values
* `span_quality`: enum like `OK`, `OUT_OF_RANGE`, `LINE_MISSING`, `BAD_COL_UNIT`, `NEGATIVE`, `REVERSED`
* optional `confidence` float

This makes debugging and telemetry straightforward.

## 5.2 Validation gates (what to measure and when to fail)

PR‑05 should introduce (or require) these checks:

1. **Bounds check**

* 0 ≤ bstart ≤ bend ≤ file_byte_length

2. **Substring sanity check (sampled)**

* for a sample of spans, slice the canonical file text and check:

  * it is non-empty when expected
  * it matches an identifier regex when the span claims to be an identifier token

3. **Join coverage metrics**

* percent of CST name refs that find at least one SCIP occurrence
* percent of symtable scopes anchored to AST nodes
  (this matters because symtable is line-based and must become byte-anchored to be “index-grade”) 

4. **Deterministic dedupe gate**
   Any relationship dataset that does “winner selection” must obey the DSL’s determinism contract:

* explicit keys
* explicit tie-breakers
* total ordering (no ties left)
  The DSL doc makes clear that determinism must be enforced as a contract because ordering guarantees can break under threading and because order-dependent aggregations exist. 

---

# 6) Putting it together: PR‑05 “rules we apply” (the final checklist)

This is the compact “do this everywhere” rule set for the PR.

## Rule group A — Plan builders are expression templates

* Plan builders return `IbisPlan(expr=..., ordering=...)`, not Arrow tables.
* No Python loops over rows.
* No “materialize to inspect then re-enter” unless routed through the hybrid kernel bridge.

(That’s exactly the intent of Scope 12.) 

## Rule group B — Stable IDs are columns, derived from semantic identity

* Every dataset that emits CPG-relevant entities must emit stable IDs as columns.
* IDs are computed by deterministic hash over semantic identity keys.
* IDs must not depend on row order or engine partitioning.
* IDs must be computed in the plan (or via a registered UDF/kernel that is invoked as part of the plan).

## Rule group C — Span normalization is a dedicated normalization pass

* Build `file_line_index` once from canonical file text.
* All line/col spans normalize via joins to `file_line_index`.
* Column-to-byte conversion is centralized (`col_to_byte`) and coordinate system is explicit.

## Rule group D — Determinism and winner selection are explicit

* If the dataset has a dedupe policy: enforce canonical ordering + tie-breakers explicitly.
* Do not rely on engine “natural order.”
* The need for explicit determinism is a foundational DSL requirement (threading breaks ordering guarantees; deterministic dedupe must be standardized). 

---

# 7) What you should add to PR‑05 as explicit “context docs” for implementers

If you want to remove all remaining ambiguity for your team/agents, PR‑05 should include a short internal doc (or module-level docstrings) that defines:

1. **Canonical coordinate system**

* what a byte span is, what it indexes into (canonical file text), and the end-exclusivity rule

2. **`file_line_index` contract**

* schema + how it is built + line numbering base

3. **Stable ID recipes per entity family**

* anchors, symbols, scopes/bindings, edges
* which natural keys are used

4. **A “span normalization failure playbook”**

* typical symptoms:

  * SCIP join coverage collapses
  * AST↔CST join collapses
* the metrics to inspect
* how to identify which coordinate system mismatch is occurring

This ensures future changes don’t silently break the core join substrate.

---

If you want, I can now convert the above into a **PR‑05 sub-PR stack** (05a line index + span normalization, 05b stable id kernel + function registry, 05c normalize builder conversions, 05d cpg builder conversions) in the same migration-plan format you’re using, so it’s directly executable by the team. 


Below is a **PR‑05 sub‑PR stack** in the same “migration-plan” format as your `datafusion_engine_migration_plan.md` (with dense “why”, key code patterns, and executable checklists). 

This stack is explicitly the “Scope 12” continuation—moving **normalize/extract/CPG plan builders** into **Ibis expressions** while preserving **schemas + invariants**. 

---

# PR‑05a: Line index + join-driven span normalization (byte offsets)

## Description (what changes)

Introduce a **canonical line-index dataset** per file and refactor all span normalization to be:

* **join-driven** (raw spans ↔ line_index)
* **unit-aware** (line/col → byte offsets)
* **fully relational at the plan level** (no “read file text per-row” inside join rules)

This provides the single, audited mechanism for producing **join-ready byte spans** that all later rules depend on.

### What “line index” means (precise definition)

A **line index** is a table derived from *the file bytes* with one row per (file_id, line_no):

* `file_id: u64` (or stable file key)
* `line_no0: i32` (0-based line index, canonical)
* `line_start_byte: i64` (byte offset in file to the first byte of that line)
* optional `line_end_byte: i64` (exclusive)
* optional `line_text: large_string` (decoded line; useful for col→byte conversion when columns are codepoint-based)
* optional `newline_kind: enum` / `has_crlf: bool` (diagnostics)

Once you have `line_start_byte`, any line/col span can become a byte span as:

* `bstart = line_start_byte + col_to_byte_offset(line_text, col_start, col_unit)`
* `bend   = line_start_byte + col_to_byte_offset(line_text, col_end,   col_unit)`

and now every downstream relationship rule can use **(file_id, bstart, bend)** as join keys.

## Why this exists (functional intent + consequences)

Span alignment is *the* “keystone join primitive” for CPG binding:

* CST nodes, AST nodes, bytecode blocks, and SCIP occurrences all need to converge on shared span coordinates to link identity and meaning.
* If spans are inconsistent, you get silent failure modes: joins that drop rows, incorrect candidate rankings, wrong winner selection, or “phantom ambiguity”.

This PR makes span normalization:

* deterministic and centrally auditable,
* cheap (one scan per file to build line index),
* relational (joins and projections), and therefore portable across engines.

It also aligns with the DSL’s determinism ethos: **row order is rarely guaranteed**, and many operations destroy ordering—so if you derive *anything* (including IDs) from implicit order, you get drift. 

## Common ways span normalization goes wrong (and why it matters)

These are the failure modes you’re protecting against:

1. **CRLF vs LF mismatch**

* If one extractor treats CRLF as 2 bytes and another normalizes to LF, your byte offsets diverge.
* Result: interval joins miss, “same node” fails to unify across sources.

2. **0-based vs 1-based line numbers**

* Some ecosystems are 1-based for line numbers. If you don’t canonicalize, everything shifts by one line.

3. **Column unit mismatch**

* Python `ast` columns are typically “character index” (codepoint-ish).
* Other sources might be UTF-8 byte offsets.
* If you treat codepoints as bytes, any non-ASCII yields wrong offsets and join failures.

4. **End-exclusive vs end-inclusive spans**

* Normalize to one convention (strongly recommend **end-exclusive** for intervals).
* If you don’t, you’ll get off-by-one interval containment errors.

5. **Missing `end_*`**

* Some extractors provide start-only. You must define fallback heuristics (e.g., bend=bstart, or bstart+1 for tokens) and carry confidence/diagnostic flags.

## Code patterns (key snippets only)

### 1) Line index build surface (dataset contract)

```python
# normalize/contracts.py-like (conceptual)
LineIndexContract = {
  "file_id": "uint64",
  "line_no0": "int32",
  "line_start_byte": "int64",
  # optional but recommended:
  "line_text": "large_string",
}
```

### 2) Join-driven normalization pattern (Ibis)

```python
raw = raw_spans  # file_id, start_line, start_col, end_line, end_col, col_unit, ...
li  = line_index # file_id, line_no0, line_start_byte, line_text

# Join start line
s = raw.join(
  li,
  predicates=[raw.file_id == li.file_id, raw.start_line0 == li.line_no0],
).mutate(
  bstart = li.line_start_byte + col_to_byte(li.line_text, raw.start_col, raw.col_unit)
)

# Join end line (separate alias)
li2 = li.view()
out = s.join(
  li2,
  predicates=[s.file_id == li2.file_id, s.end_line0 == li2.line_no0],
).mutate(
  bend = li2.line_start_byte + col_to_byte(li2.line_text, s.end_col, s.col_unit)
)
```

### 3) `col_to_byte` must be a kernel/UDF (engine-agnostic)

This is exactly the kind of “hard to express purely relationally” operation that belongs in the **hybrid kernel bridge**: prefer DataFusion built-ins; else Python UDF; else Arrow fallback. 

DataFusion’s guidance: **UDFs should operate on Arrow arrays and use `pyarrow.compute.*`** for performance and correctness. 

```python
# pseudo-pattern
def col_to_byte(line_text_arr: pa.Array, col_arr: pa.Array, unit_arr: pa.Array) -> pa.Array:
    # vectorized conversion (codepoint→byte) implemented Arrow-native
    ...

# register as DataFusion scalar UDF (or keep as Arrow fallback kernel)
```

## Implementation checklist (directly executable)

* [ ] Define canonical `line_no0` convention (0-based) + canonical newline policy (no implicit normalization unless explicitly encoded).
* [ ] Add dataset: `file_line_index` (produced from repo file bytes; stored as parquet alongside extracted datasets).
* [ ] Add a **single normalization function** `normalize_spans(raw_spans, line_index)` that:

  * [ ] converts all line numbers to `*_line0`
  * [ ] computes `(bstart, bend)` end-exclusive
  * [ ] emits `span_confidence`, `span_diag_flags` (optional but strongly recommended)
* [ ] Refactor each extractor’s “raw span table” to include:

  * `start_line`, `start_col`, `end_line`, `end_col`
  * `col_unit` (enum: `"codepoint" | "utf8_byte" | ...`)
  * `span_source` (enum: AST/CST/SCIP/BC/SYMTABLE)
* [ ] Add regression fixtures:

  * [ ] CRLF file
  * [ ] non-ASCII identifier file (e.g., `π = 1`)
  * [ ] tabs and mixed indentation file
* [ ] Add a **diagnostic join coverage report**:

  * percent of raw spans that successfully join to line index
  * percent with non-null bstart/bend
  * distribution of `col_unit`

## Status

Pending.

---

# PR‑05b: Stable ID kernel + function registry (relational IDs, engine-safe)

## Description (what changes)

Implement a single, centralized **stable ID generation kernel** and expose it through:

* the ExprIR→Ibis function registry (so builders use it like any other expression), and
* the kernel bridge (so it runs as built-in / DataFusion UDF / Arrow fallback consistently).

This PR makes the rule “**stable id generation must be expressed relationally**” enforceable in practice.

## What “stable id generation must be expressed relationally” means

It means:

* **No ID is produced by “row order”** (`row_number()`, enumerate batches, etc.)
* IDs are computed as a deterministic function of **semantic identity columns** via projection:

  * `id = stable_hash64(col_a, col_b, ..., type_tags, null_sentinels)`
* If IDs depend on implicit ordering, they will drift because joins/aggregations frequently destroy order (hash join in particular). 

This is also essential for:

* caching/materialization correctness,
* stable diffs and manifests,
* rule output reproducibility across machines/threads. 

## Why this exists (functional intent)

Your CPG build is only “rules-based” if identity is rules-based.

Stable IDs are the glue across:

* normalized datasets,
* relationship outputs,
* emitted node/edge tables.

If an ID drifts because it came from incidental order, you cannot reliably:

* dedupe/winner-select,
* compare manifests,
* reproduce run bundles.

## Code patterns (key snippets only)

### 1) Stable ID as an expression macro (Ibis-level)

```python
# ibis_engine/functions.py (conceptual)
def stable_id64(*cols: ibis.Expr) -> ibis.IntegerValue:
    # implemented by:
    # - DF builtin if available
    # - else DF UDF
    # - else Arrow fallback kernel
    return call_kernel("stable_id64", cols)
```

### 2) Use it everywhere IDs are needed

```python
nodes = nodes.mutate(
  node_id = stable_id64(nodes.repo_id, nodes.file_id, nodes.node_kind, nodes.bstart, nodes.bend)
)
edges = edges.mutate(
  edge_id = stable_id64(edges.src_id, edges.dst_id, edges.edge_kind, edges.provenance_rank)
)
```

### 3) Determinism gates: canonical ordering + dedupe

If any downstream step uses “keep first”, you must enforce canonical sort first. The DSL guide is explicit that “first” semantics are order-dependent and output ordering can drift (especially under threading). 

So the stable-ID rule pairs naturally with:

* canonical sort primitives (`sort_indices` + `take`) and
* explicit dedupe specs (keys + tie-breakers + winner strategy). 

## Implementation checklist

* [ ] Specify ID recipes as contracts:

  * stable inputs (typed)
  * null sentinel policy
  * string normalization policy (e.g., NFC? exact bytes?—must be fixed)
* [ ] Implement `stable_id64` kernel in the kernel bridge:

  * [ ] attempt DataFusion built-in (if you standardize on one)
  * [ ] else register a DataFusion scalar UDF (Arrow-native arrays; prefer `pyarrow.compute`) 
  * [ ] else Arrow fallback kernel
* [ ] Register it in the ExprIR→Ibis function registry (so all builders can call it) 
* [ ] Add “no row-order IDs” lint gate:

  * [ ] forbid usage of row_number/auto_increment patterns in normalize/cpg plan builders
* [ ] Add golden tests:

  * [ ] same input → same IDs across two runs
  * [ ] shuffled input ordering → same IDs

## Status

Pending.

---

# PR‑05c: Convert normalize builders to Ibis expressions (span + ids + bytecode normalization)

## Description (what changes)

Refactor the `normalize/*` plan builders to build **Ibis expressions** rather than ad-hoc logic, while preserving:

* schema contracts and column names,
* deterministic invariants (ordering metadata where needed),
* optional-input behavior (missing inputs yield empty tables).

This is directly aligned to “Scope 12” target files. 

## Why normalize is special (dense rationale)

Normalize is the “make it join-ready” stage:

* It converts raw extractor outputs into canonical keys:

  * `file_id` normalization
  * span normalization (PR‑05a)
  * stable IDs (PR‑05b)
  * canonical typing/provenance columns
* If normalize is not fully declarative, rule compilation cannot safely infer dependencies or validate contracts.

Normalize is also where you must be ruthless about determinism:

* Many transforms require dedupe/winner-selection.
* Any time you do winner-selection, you must enforce ordering keys first. 

## Code patterns (key snippets only)

### 1) Normalize builder signature pattern

```python
def build_normalized_spans(raw_spans: ibis.Table, line_index: ibis.Table) -> ibis.Table:
    # join-driven span normalization + stable ids
    ...
    return normalized
```

### 2) Optional inputs → empty tables contract

```python
def maybe_table(name: str, schema: pa.Schema) -> ibis.Table:
    # if dataset missing, return ibis.memtable(empty_arrow_table(schema))
    ...
```

### 3) Kernel bridge usage for hard ops

Span conversions and interval alignment are explicitly in-scope for the kernel bridge. 

```python
expr = expr.mutate(
  bstart = call_kernel("col_to_byte", expr.line_text, expr.start_col, expr.col_unit)
)
```

## Implementation checklist

* [ ] Convert these builders to Ibis expression builders:

  * `normalize/plan_builders.py`
  * `normalize/bytecode_cfg_plans.py`
  * `normalize/bytecode_dfg_plans.py`
  * `normalize/types_plans.py` 
* [ ] Ensure each builder:

  * [ ] returns an Ibis table expression (not materialized Arrow table)
  * [ ] declares or preserves ordering metadata (unordered by default)
  * [ ] applies stable ID projection via `stable_id64`
  * [ ] uses PR‑05a span normalization outputs where spans are needed
* [ ] Add schema parity tests:

  * [ ] columns match exactly
  * [ ] metadata semantics preserved (ordering tags, determinism tier if encoded)
* [ ] Add row-count sanity checks at key junctions:

  * e.g., `#normalized_spans >= #raw_spans_joinable` (with diagnostics explaining any drop)

## Status

Pending.

---

# PR‑05d: Convert CPG relationship plan builders to Ibis expressions (join + winner selection → edges)

## Description (what changes)

Refactor `cpg/relationship_plans.py` (and any remaining join-heavy CPG builders) into Ibis expression builders that:

* consume normalized datasets (spans + IDs already canonical),
* execute joins/winner selection deterministically, and
* emit relationship datasets / edge tables with exact output schema contracts.

This is the last major step in “Scope 12”. 

## Why this is the payoff PR

Once PR‑05a/b/c are in:

* relationship rules become “just joins + policies”
* ambiguity handling becomes centralized ranking + dedupe contracts
* the CPG edge emission becomes stable and reproducible

This is also where ordering/dedupe policy must be consistently enforced. The DSL guide gives the canonical primitive set:

* stable sort via `sort_indices + take`
* dedupe strategies like KEEP_FIRST_AFTER_SORT / KEEP_BEST_BY_SCORE
* final canonical sort after grouping because group output ordering is not guaranteed. 

## Code patterns (key snippets only)

### 1) Relationship builder shape

```python
def build_callsite_symbol_edges(callsites: ibis.Table, scip_occ: ibis.Table) -> ibis.Table:
    candidates = callsites.join(
        scip_occ,
        predicates=[
          callsites.file_id == scip_occ.file_id,
          # span overlap / containment is usually a kernel (interval align) or explicit predicate
        ],
    )

    ranked = candidates.mutate(score = score_call_candidate(candidates))

    winners = dedupe_by_policy(
        ranked,
        keys=("callsite_id",),
        order_by=("score DESC", "confidence DESC", "span_len ASC"),
        strategy="KEEP_FIRST_AFTER_SORT",
    )

    return winners.select(
      "src_node_id", "dst_symbol_id", "edge_kind", "confidence", "provenance"
    )
```

### 2) Prefer kernel bridge for interval alignment + explode

This PR assumes PR‑04 kernel bridge patterns (built-in → UDF/UDTF → Arrow fallback) are available and used explicitly. 

### 3) Output must match schema contracts, always

This is consistent with the migration plan’s constraints (“preserve output schemas, column names, metadata semantics”). 

## Implementation checklist

* [ ] Convert `cpg/relationship_plans.py` to Ibis expression builders
* [ ] Ensure relationship builders:

  * [ ] never perform ad-hoc materialization mid-plan
  * [ ] use normalized spans + stable IDs as join keys
  * [ ] enforce canonical ordering before any KEEP_FIRST* policy
  * [ ] emit confidence/provenance fields needed for diagnostics
* [ ] Add “join coverage” diagnostics:

  * percent of callsites with a resolved symbol edge
  * percent that fall back to qualified-name edges
  * ambiguity distributions (N candidates before winner selection)
* [ ] Add parity harness:

  * compare edge row counts and key distributions before/after conversion

## Status

Pending.

---

# PR‑05 overall sequencing + merge strategy (how the stack fits)

* **PR‑05a** must land first because it creates the canonical join keys (byte spans).
* **PR‑05b** next because it defines the only allowed ID mechanism and plugs into the function registry/kernel bridge.
* **PR‑05c** then converts normalize builders and ensures all downstream datasets already carry stable IDs + normalized spans.
* **PR‑05d** finally converts CPG relationship builders, which now become mostly “pure join logic + policy”.

This matches the migration plan’s intent: convert plan builders to Ibis while preserving schema invariants and optional-input behavior. 

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



