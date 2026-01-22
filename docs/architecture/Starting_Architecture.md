
[![Open-Sourcing the Code Property Graph Specification](https://tse2.mm.bing.net/th/id/OIP.HMhlfG1dt3-mPyWxhHj0dwHaDu?cb=defcachec2\&pid=Api)](https://blog.shiftleft.io/open-sourcing-the-code-property-graph-specification-30238d66a541?utm_source=chatgpt.com)

# Inference-driven CPG Build Spec

A blank-slate technical specification for building a “best-in-class” Code Property Graph (CPG) pipeline that:

* **Orchestrates by inference** (only outputs are requested; intermediates are inferred) using **Apache Hamilton**
* **Executes transformations** as **PyArrow Acero** plans (plus PyArrow compute + dataset IO)
* **Integrates** Python **AST + CST + SCIP** into a single, queryable **property-graph-as-tables** output

This document is written so a strong Python engineer with *no prior background* in PyArrow, Hamilton, or CPGs can implement it end-to-end.

---

## 1) What “fully inference-driven” means in this design

### 1.1 Goal

You only specify:

1. **Inputs available** (AST/CST/SCIP datasets; maybe others later)
2. **Output products** you want (e.g., `cpg_nodes`, `cpg_edges`)
3. **Declarative relationship rules** (join keys, anchoring rules, invariants)
4. **Declarative transformations** (what to compute, not how to stage)

…and the system:

* Infers the minimal execution plan needed to produce the requested outputs
* Infers intermediate schemas from Arrow at runtime
* Enforces **schema + determinism** only at the **finalization boundary**

### 1.2 Key architectural constraint you must accept

**Acero is excellent at streaming relational-style transforms** (scan/filter/project/hashjoin/aggregate/order_by), but **not every “code intelligence” relationship is naturally a pure relational equality join**.

So, the design explicitly supports two computation modes:

1. **Relational mode (Acero)**: equality joins, projections, filters, aggregations, unions, deduping patterns
2. **Anchoring mode (per-file algorithms)**: interval containment (“smallest AST node that covers this range”), token-to-span alignment, fallbacks when spans don’t match exactly

This is how you keep the *target state* (inference-driven, minimal intermediate specs) without forcing everything into unnatural joins.

---

## 2) Core principles and invariants

### 2.1 Canonical coordinate system: bytes-first spans

All cross-system joins (AST↔CST↔SCIP) depend on a shared addressing system:

* **`file_id`**: stable identifier for a file in a snapshot
* **`byte_start`, `byte_end`**: half-open span `[start, end)` in UTF-8 bytes

Why bytes-first:

* Python AST offsets can be byte-based for multibyte UTF-8, so mixing char offsets and byte offsets silently breaks joins.
* LibCST can provide **byte spans** (not just line/col), enabling exact matching and stable IDs.

### 2.2 “Everything is a set unless finalized”

Acero and Arrow do not guarantee stable ordering across many operations:

* Hash joins do not preserve input ordering and may produce non-predictable order.
* Grouped ops may not guarantee stable output order when multithreaded.

Therefore:

* **No intermediate dataset may rely on row order**
* Deterministic ordering is created **only** at “finalize”

### 2.3 “Finalize gate” is mandatory

Every materialized output passes through a **Finalize Gate** that:

* Aligns schema
* Enforces invariants
* Canonicalizes ordering (stable sort)
* Dedupes
* Emits error rows + stats

This is explicitly recommended by your internal Arrow/Acero guidance as a best practice for robust pipelines.

### 2.4 Ambiguity is a first-class data product

Especially with CST name resolution:

* Qualified-name resolution can legitimately return multiple candidates; ambiguity is expected and should be stored as multiple rows, not “forced” into a scalar.

This design *never discards ambiguity*; it represents it in edges with confidence + grouping identifiers.

---

## 3) Output model: “Property graph as Arrow tables”

### 3.1 Minimum viable “fully complete” CPG outputs

You will produce **three** datasets (Parquet via `pyarrow.dataset.write_dataset` is recommended):

1. `cpg_nodes`
2. `cpg_edges`
3. `cpg_props` (optional but strongly recommended for extensibility)

This avoids constantly changing schemas when you add new node/edge attributes.

### 3.2 Required schemas (contracts)

#### `cpg_nodes` (contract)

Primary purpose: store graph vertices with stable identity + minimal common properties.

Required columns:

* `snapshot_id: large_string`
* `node_id: large_string` (stable, unique within snapshot)
* `node_kind: dictionary<values=string>` (or int enum)
* `file_id: large_string` (nullable for global nodes like packages)
* `path: large_string` (nullable)
* `byte_start: int64` (nullable)
* `byte_end: int64` (nullable)
* `name: large_string` (nullable; e.g., identifier text)
* `origin: dictionary<string>` (e.g., `ast`, `cst`, `scip`, `derived`)
* `confidence: float32` (nullable; default 1.0)

Recommended extras (still “core”):

* `lang: dictionary<string>` (e.g., `python`)
* `symbol_id: large_string` (nullable; if this node represents/anchors a SCIP symbol)
* `ast_kind: dictionary<string>` (nullable)
* `cst_kind: dictionary<string>` (nullable)

#### `cpg_edges` (contract)

Required columns:

* `snapshot_id: large_string`
* `edge_id: large_string`
* `edge_kind: dictionary<string>` (or int enum)
* `src_node_id: large_string`
* `dst_node_id: large_string`
* `file_id: large_string` (nullable)
* `path: large_string` (nullable)
* `byte_start: int64` (nullable)
* `byte_end: int64` (nullable)
* `origin: dictionary<string>`
* `confidence: float32` (nullable)

Recommended extras:

* `ambiguity_group_id: large_string` (nullable; for “N candidates” sets)
* `role_bits: int32` (nullable; for SCIP roles)

#### `cpg_props` (contract)

Purpose: add properties without changing node/edge schemas.

Required columns:

* `snapshot_id: large_string`
* `entity_kind: dictionary<string>` (`node` or `edge`)
* `entity_id: large_string` (`node_id` or `edge_id`)
* `key: large_string`
* One of:

  * `value_string: large_string`
  * `value_int64: int64`
  * `value_float64: float64`
  * `value_bool: bool`
  * `value_bytes: large_binary`
* `origin: dictionary<string>`
* `confidence: float32`

---

## 4) Inputs: what to ingest from AST, CST, SCIP

You will create **six** primary input datasets (these are “facts,” not “specs”):

### 4.1 `files`

One row per source file.

Columns:

* `snapshot_id`
* `file_id` (stable hash of normalized repo-relative path)
* `path`
* `lang` (e.g., python)
* `text_utf8` OR `bytes_utf8` (pick one; bytes is preferred)
* Optional: `line_offsets` (list<int64>) for fast line/col→byte conversions

### 4.2 `ast_nodes`

One row per Python AST node.

Required columns:

* `snapshot_id`
* `file_id`, `path`
* `ast_node_id` (stable within snapshot)
* `ast_kind` (e.g., `FunctionDef`, `Call`)
* `byte_start`, `byte_end`
* `parent_ast_node_id` (nullable)
* Optional: `fields_json` (compact structured metadata)

**Span extraction rule:** use `lineno/col_offset/end_lineno/end_col_offset`, then map to byte offsets using file bytes. Be careful about UTF-8 multibyte behavior.

### 4.3 `cst_nodes`

One row per LibCST node (or only “interesting nodes” if volume is too high).

Required columns:

* `snapshot_id`
* `file_id`, `path`
* `cst_node_id`
* `cst_kind`
* `byte_start`, `byte_end`
* `parent_cst_node_id`

LibCST spans are start-inclusive and end-exclusive and use 1-indexed lines, 0-indexed columns.

### 4.4 `cst_tokens` (strongly recommended)

Tokens provide the most reliable anchoring surface for names and operators.

Required columns:

* `snapshot_id`
* `file_id`, `path`
* `token_id`
* `token_kind` (identifier, keyword, operator, string, number, indent/dedent, newline)
* `byte_start`, `byte_end`
* `text` (token literal)

### 4.5 `scip_occurrences`

One row per SCIP occurrence.

Required columns:

* `snapshot_id`
* `file_id`, `path`
* `occ_id`
* `symbol` (SCIP symbol string)
* `role_bits` (bitmask; includes Definition/Reference/etc.)
* `byte_start`, `byte_end` (converted from SCIP range)
* Optional: `enclosing_symbol` (if present)

### 4.6 `scip_symbols`

One row per SCIP symbol information (metadata).

Required columns:

* `snapshot_id`
* `symbol` (primary key)
* `symbol_kind` (if available)
* `display_name` / `name` (if available)
* Optional: `docstring`, `signature`, relationships

> Practical note: If you’re unsure how to decode SCIP files, Sourcegraph documents decoding an `index.scip` protobuf with `protoc` tooling (good onboarding reference). ([GitHub][1])

---

## 5) Relationship rules: the “centralized join logic” you asked for

Yes: it is feasible (and ideal) to centralize join patterns as logical statements **as long as you normalize into equality joins**.

### 5.1 The Relationship Registry

Implement a central registry that defines:

* Canonical keys (e.g., `span_key = (file_id, byte_start, byte_end)`)
* Foreign-key relationships and their cardinalities
* Allowed join types and fallbacks
* Ambiguity policies

#### `JoinRule` (code object spec)

```python
@dataclass(frozen=True)
class JoinRule:
    name: str
    left_relation: str
    right_relation: str
    join_type: Literal["inner", "left outer", "right outer", "full outer",
                       "left semi", "right semi", "left anti", "right anti"]
    left_keys: list[str]          # column names OR named key macros
    right_keys: list[str]
    cardinality: Literal["1:1", "1:n", "n:1", "n:n", "unknown"]
    requires_exact_span_match: bool
    ambiguity_grouping: Optional[str]  # e.g. "cst_name_resolution"
```

#### Acceptance criteria

* Every join used anywhere in the system must correspond to one `JoinRule`
* `JoinRule` is sufficient to compile into a `HashJoinNodeOptions`

This is directly supported by Acero hash join options accepting key fields (strings or expressions) and standard join types.

### 5.2 Span anchoring rules (non-relational fallback)

When spans don’t match exactly (common across AST vs CST), you need a deterministic fallback algorithm that is still “inference-driven” (no manual intermediate schema spec).

Define a standard “anchoring API”:

#### `AnchorRule` (code object spec)

```python
@dataclass(frozen=True)
class AnchorRule:
    name: str
    source_relation: str        # e.g. scip_occurrences
    target_relation: str        # e.g. cst_tokens or cst_nodes
    strategy: Literal["exact", "smallest_covering", "nearest_left", "nearest_right"]
    max_distance_bytes: int
```

**Example rules**

* `scip_occurrence → cst_token`: `smallest_covering` (prefer identifiers)
* `ast_node → cst_node`: `smallest_covering`
* `cst_token → ast_node`: `smallest_covering`

This creates *derived mapping tables* (facts) like:

* `map_ast_to_cst(ast_node_id, cst_node_id, confidence)`
* `map_scip_to_token(occ_id, token_id, confidence)`

These become equality join surfaces for the rest of the pipeline.

---

## 6) Execution architecture: Hamilton + Acero “plans as nodes”

### 6.1 Hamilton’s role

Hamilton builds a DAG from Python functions; you then request outputs, and Hamilton infers the upstream work required.

You will use the Hamilton `driver.Builder()` pattern with:

* `.with_modules(...)`
* optional `.with_config(...)`
* optional `.with_materializers(...)`
* optional `.with_cache()` for reuse of intermediates ([hamilton.apache.org][2])

### 6.2 Key design decision

**Hamilton nodes should mostly return “plan objects” (Acero Declarations), not materialized tables.**

* A `pyarrow.acero.Declaration` represents an unconstructed ExecPlan node (and can reference other Declarations as inputs).
* `Declaration.to_reader()` executes and returns a streaming `RecordBatchReader`.
* `Declaration.to_table()` executes and collects results into a table, adding a sink node and blocking until completion.

This is exactly your target: the DAG is an inferred composition of **plans**, and only output nodes execute.

### 6.3 Why streaming matters

Collecting to a table accumulates everything in memory (both your internal notes and Arrow docs emphasize this behavior via the “collect into a table” semantics).

So:

* Use `.to_reader()` for most outputs and stream-write to Parquet
* Reserve `.to_table()` for tests or small derived products

---

## 7) PyArrow/Acero building blocks you will use (and where)

This section is intentionally “cookbook-like” so a new engineer can map specs to library calls.

### 7.1 Datasets + scan nodes

Preferred IO: `pyarrow.dataset.Dataset` backed by Parquet.

* Read: `pyarrow.dataset.dataset(path, format="parquet")`
* Write: `pyarrow.dataset.write_dataset(table_or_batches, base_dir=..., format="parquet", partitioning=...)`

Plan scan:

* `acero.Declaration("scan", acero.ScanNodeOptions(dataset, **scanner_kwargs))`

ScanNodeOptions notes:

* It can apply pushdown filters/projections to file readers **but does not construct filter/project nodes**; you can supply the same expression to scan and to a downstream filter/project node.
* It supports `require_sequenced_output` and `implicit_ordering` flags; when `implicit_ordering=True`, batches are augmented to enable stable ordering for simple plans.

### 7.2 Filter

* `acero.Declaration("filter", acero.FilterNodeOptions(filter_expression), inputs=[prev])`

Filter expression:

* Must be a `pyarrow.compute.Expression` returning boolean.

### 7.3 Project (add/drop/compute columns)

* `acero.Declaration("project", acero.ProjectNodeOptions(expressions, names), inputs=[prev])`

Expressions must be scalar expressions evaluated elementwise.

### 7.4 Hash join

* `acero.Declaration("hashjoin", acero.HashJoinNodeOptions(...), inputs=[left, right])`

Keys can be column names or field expressions; standard join types are supported.

**Ordering warning:** joins don’t preserve ordering; don’t rely on row order.

### 7.5 Aggregate

You can use:

* `acero.AggregateNodeOptions` for Acero aggregation (GROUP BY style) ([Apache Arrow][3])
* or `table.group_by(...).aggregate(...)` if you already have a table (but note output order is not stable with threads) ([Apache Arrow][4])

**Pipeline-breaker warning:** aggregate nodes can fully materialize data in memory (documented as a pipeline breaker in Arrow C++ docs) ([Apache Arrow][5]).

### 7.6 Order by (avoid except in finalize)

Acero’s order_by:

* Accumulates all data, sorts, and does **not** support larger-than-memory sort.

So: do not use `order_by` in the middle of the pipeline; use your Finalize Gate instead.

### 7.7 Deterministic sorting primitive

Use:

* `pyarrow.compute.sort_indices(table, sort_keys=[...])` which returns indices defining a **stable sort** ([Apache Arrow][6])
* then `table.take(indices)` to reorder

This is also the “stable ordering” pattern referenced in your internal guidance.

---

## 8) The Finalize Gate (mandatory component)

### 8.1 `FinalizeGate` (code object spec)

```python
class FinalizeGate:
    """
    Input: a RecordBatchReader or Table + a TableContract
    Output: (final_table, error_table, stats_table)
    """

    def finalize_reader(self, reader: pa.RecordBatchReader, contract: TableContract) -> FinalizedArtifact: ...
    def finalize_table(self, table: pa.Table, contract: TableContract) -> FinalizedArtifact: ...
```

### 8.2 Functional requirements

Must implement the following steps (in order):

1. **Schema alignment**

   * Add missing contract columns as nulls
   * Drop extra columns (or keep them only if contract allows)
   * Cast types to contract types (safe casting policy configurable)
2. **Invariant checks**

   * Primary key non-null
   * Referential integrity optional (emit as error rows, don’t crash by default)
3. **Canonical dedupe**

   * Deduplicate on `(primary_key)` choosing deterministic winner
4. **Canonical ordering**

   * Stable sort by `contract.sort_keys` using `pc.sort_indices` + `take`
5. Emit:

   * `final_table`
   * `error_table` (bad rows with reason codes)
   * `stats_table` (counts, null rates, join drop rates)

This “finalize gate” pattern (schema alignment + invariants + canonical ordering and dedupe + reporting) is explicitly described in your Arrow/Acero design guidance.

---

## 9) Building the CPG: required node/edge types and how to derive them

### 9.1 Minimal node kinds (best-in-class baseline)

You should implement at least these `node_kind` values:

1. `FILE`
2. `AST_NODE`
3. `CST_NODE`
4. `TOKEN`
5. `SYMBOL` (from SCIP)
6. `CALLSITE` (derived; anchored to CST Call nodes)
7. `IMPORT` (derived; anchored to CST import nodes)

You can add more later (SCOPE, TYPE, CFG blocks, etc.) without schema changes.

### 9.2 Minimal edge kinds (best-in-class baseline)

You should implement at least:

#### Structural edges

* `FILE_CONTAINS` (FILE → AST/CST roots)
* `AST_PARENT_OF` (AST parent → AST child)
* `CST_PARENT_OF` (CST parent → CST child)

#### Cross-representation alignment edges

* `AST_ALIGNS_TO_CST` (ast_node → cst_node) via anchoring rule
* `TOKEN_BELONGS_TO_CST` (token → cst_node)

#### Semantic edges (SCIP)

* `DEFINES_SYMBOL` (syntax anchor → SYMBOL) when role includes Definition
* `REFERENCES_SYMBOL` (syntax anchor → SYMBOL) when not Definition
* `IMPORTS_SYMBOL` (import syntax → SYMBOL) when role includes Import (if present in your SCIP toolchain)

#### Call graph edges (approximate but useful)

* `CALLS_SYMBOL` (CALLSITE → SYMBOL) when the callee expression span resolves to a SCIP symbol occurrence

### 9.3 How to choose the “syntax anchor” node

For each SCIP occurrence, you want a stable syntax node to attach semantics to.

Preferred anchor order:

1. `TOKEN` (identifier token) that matches the occurrence span
2. else `CST_NODE` (smallest covering)
3. else `AST_NODE` (smallest covering)
4. else attach directly to `FILE` with low confidence

This is where ambiguity handling matters.

---

## 10) The CPG pipeline: derived products and their dependencies

Below is a “blank slate” dependency graph (not code; just product logic). Hamilton will implement it via inferred orchestration.

### 10.1 Derived mapping tables (facts)

1. `map_ast_to_cst`

   * Inputs: `ast_nodes`, `cst_nodes`
   * Output: `(ast_node_id, cst_node_id, confidence)`
2. `map_scip_to_token`

   * Inputs: `scip_occurrences`, `cst_tokens`
   * Output: `(occ_id, token_id, confidence)`
3. `map_token_to_cst`

   * Inputs: `cst_tokens`, `cst_nodes`
   * Output: `(token_id, cst_node_id, confidence)`

### 10.2 Derived CPG nodes

1. `cpg_file_nodes` from `files`
2. `cpg_ast_nodes` from `ast_nodes`
3. `cpg_cst_nodes` from `cst_nodes`
4. `cpg_token_nodes` from `cst_tokens`
5. `cpg_symbol_nodes` from `scip_symbols`
6. `cpg_callsite_nodes` (derived)
7. `cpg_import_nodes` (derived)

Final:

* `cpg_nodes = union(all node subsets) → FinalizeGate(contract=cpg_nodes)`

### 10.3 Derived CPG edges

1. `ast_edges` from `ast_nodes` parent field
2. `cst_edges` from `cst_nodes` parent field
3. `alignment_edges` from mapping tables
4. `semantic_edges` from:

   * `scip_occurrences` joined to `map_scip_to_token` joined to `cpg_token_nodes` joined to `cpg_symbol_nodes`
5. `call_edges` from:

   * `callsite_nodes` anchored to callee tokens joined to SCIP symbol occurrences

Final:

* `cpg_edges = union(all edge subsets) → FinalizeGate(contract=cpg_edges)`

---

## 11) How Hamilton nodes should be structured

### 11.1 Design pattern: “plans are data”

Each Hamilton function returns one of:

* `pa.Table` (only for small/extraction steps)
* `pa.dataset.Dataset` (as a handle)
* `acero.Declaration` (preferred)
* `pa.RecordBatchReader` (for streaming output)
* `FinalizedArtifact` (after finalize gate)

### 11.2 Required core Hamilton modules (files)

1. `contracts.py`

   * defines `TableContract`s for `cpg_nodes`, `cpg_edges`, `cpg_props`, and all “fact datasets”
2. `relationships.py`

   * builds `RelationshipRegistry` containing JoinRules + AnchorRules
3. `plans.py`

   * compiles `TargetSpec → acero.Declaration`
4. `execution.py`

   * executes Declarations (`to_reader`) and materializes outputs
5. `finalize.py`

   * FinalizeGate implementation
6. `extract_ast.py`, `extract_cst.py`, `extract_scip.py`

   * extraction into Arrow tables/datasets

### 11.3 Driver construction

Use Hamilton Builder with modules; optionally use materializers/caching. ([hamilton.apache.org][2])

---

## 12) Plan compilation: the “single logical Acero object” aspiration

### 12.1 What’s feasible today

For each output target (e.g., `cpg_edges`), it is feasible to compile:

* A **single `acero.Declaration` graph** that scans required inputs and produces the output.

This is exactly what `Declaration` is for: declaring nodes that will become an ExecPlan, then running them via `to_reader` or `to_table`.

### 12.2 What’s *not* reliably feasible in pure Acero alone

* Multi-sink “one plan produces multiple final outputs” is not the ergonomic default in the Python `Declaration.to_table/to_reader` API (it implicitly adds a single sink per execution).
* Non-equality joins (interval containment) need anchoring preprocessing.

**Best-in-class workaround**

* Compile one Declaration per output
* Share the same underlying Parquet datasets for sources
* Use Hamilton caching/materialization to avoid recomputing expensive anchoring tables

### 12.3 Substrait: optional future-proofing

Arrow recommends Substrait as a preferred plan interchange mechanism, helping portability and longevity. ([Apache Arrow][7])

However, be aware:

* Substrait expressions are bound to schema, while PyArrow compute expressions are unbound; serialization requires schemas and can fail if functions don’t match.

So:

* Use **Acero Declaration graphs as the primary runtime plan**
* Optionally export Substrait later as a serialization format once your schema observation layer is mature

---

## 13) Minimal “correctness” test suite (must-have)

### 13.1 Contract tests (per dataset)

* Can write dataset to Parquet and read back as `pyarrow.dataset.Dataset`
* Finalized output matches `TableContract.schema` exactly
* Primary keys are unique and non-null

### 13.2 Determinism tests

Given same `snapshot_id` and inputs:

* `cpg_nodes` and `cpg_edges` must be byte-for-byte identical Parquet outputs (or at least row-for-row identical after canonical sort)
* Enforce canonical sort using stable sort indices ([Apache Arrow][6])

### 13.3 Ambiguity preservation tests

* If LibCST name resolution yields multiple candidates, output must contain multiple edges with same `ambiguity_group_id` (not “pick one”).

---

## 14) Implementation checklist (the shortest path to “best-in-class”)

### Phase A — Foundations

* [ ] Implement Arrow dataset store (read/write)
* [ ] Implement TableContract + FinalizeGate
* [ ] Implement bytes-first span normalization utilities

### Phase B — Extraction

* [ ] `files`
* [ ] `ast_nodes`
* [ ] `cst_nodes` + `cst_tokens` (LibCST metadata providers as needed)
* [ ] `scip_occurrences` + `scip_symbols` (protobuf decode guidance) ([GitHub][1])

### Phase C — Mapping / anchoring facts

* [ ] `map_scip_to_token`
* [ ] `map_ast_to_cst`
* [ ] `map_token_to_cst`

### Phase D — CPG outputs

* [ ] `cpg_nodes` union + finalize
* [ ] `cpg_edges` union + finalize
* [ ] optional `cpg_props`

### Phase E — Inference-driven orchestration

* [ ] Hamilton DAG where outputs are requested by name
* [ ] Most nodes return `acero.Declaration` until finalization
* [ ] Streaming execution using `Declaration.to_reader()` and dataset writer

---

## 15) Summary: why this matches your target state

* **Hamilton** gives you inference-driven orchestration (only compute what the requested outputs require). ([hamilton.apache.org][2])
* **Acero Declarations** give you composable, mostly-streaming relational execution graphs (plans are first-class).
* **Finalize Gate** is the key to “no intermediate schema specification”: schemas can float through execution and only harden at output boundaries.
* **Ambiguity-first modeling** ensures robustness for real-world code intelligence (don’t collapse multiple candidates into a fake single truth).

---

If you want, I can extend this spec with a **concrete enumeration** of recommended `node_kind`/`edge_kind` values tailored specifically to “rich CPGs for Python” (classes/functions/parameters/calls/imports/attributes/typing edges), but the above is the minimal architecture + contracts + library mapping needed to implement the fully inference-driven approach you described.

[1]: https://github.com/sourcegraph/scip/blob/main/scip.proto?utm_source=chatgpt.com "scip/scip.proto at main · sourcegraph/scip"
[2]: https://hamilton.apache.org/concepts/builder/ "Builder - Hamilton"
[3]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.AggregateNodeOptions.html?utm_source=chatgpt.com "pyarrow.acero.AggregateNodeOptions — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.TableGroupBy.html?utm_source=chatgpt.com "pyarrow.TableGroupBy — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/cpp/acero/user_guide.html?utm_source=chatgpt.com "Acero User's Guide — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.sort_indices.html?utm_source=chatgpt.com "pyarrow.compute.sort_indices — Apache Arrow v22.0.0"
[7]: https://arrow.apache.org/docs/cpp/acero/user_guide.html "Acero User’s Guide — Apache Arrow v22.0.0"

Below is a **concrete, code-friendly enumeration** of recommended `node_kind` and `edge_kind` values for a **rich Python CPG** that covers **classes, functions, parameters, calls, imports, attributes, and typing**—and still fits the “inference-driven + ambiguity-tolerant” architecture we discussed.

I’m assuming the “property graph as tables” output shape you already accepted:

* `cpg_nodes(snapshot_id, node_id, node_kind, file_id, path, byte_start, byte_end, name, origin, confidence, …)`
* `cpg_edges(snapshot_id, edge_id, edge_kind, src_node_id, dst_node_id, file_id, path, byte_start, byte_end, origin, confidence, …)`
* `cpg_props(snapshot_id, entity_kind, entity_id, key, value_*, origin, confidence, …)`

And that all “anchors” use **byte spans** as canonical evidence coordinates (UTF-8 safe).

---

## 0) Naming conventions for kinds

Use simple, stable string enums:

* Node kinds: `PY_*`
* Edge kinds: `PY_*`
* Integration kinds (cross-representation / mapping): `XREF_*`

This keeps a clean separation between:

* your Python semantic graph, and
* the glue edges that reconcile AST/CST/SCIP.

---

## 1) Node kinds

### 1.1 Repository / file / module layer

These provide the “container hierarchy” every other node hangs off.

1. **`PY_FILE`**

* Meaning: a source file artifact
* Anchor: whole file (`byte_start=0`, `byte_end=len(file_bytes)`)
* Key props:

  * `path`
  * `module_fqn` (optional enrichment)
  * `language="python"`
* Edges:

  * `PY_CONTAINS` → top-level defs (classes, functions, imports)

2. **`PY_MODULE`** *(optional but recommended)*

* Meaning: logical module identity (esp. packages)
* Anchor: either same as file span, or null span (module as conceptual)
* Key props:

  * `module_fqn` if you compute it
* Why: lets you model `import pkg.mod` vs file paths cleanly

3. **`PY_PACKAGE`** *(optional but recommended)*

* Meaning: package identity (directory / import namespace)
* Key props: package name, root path

> If you don’t want `PY_MODULE` and `PY_PACKAGE` initially, you can still build a good CPG with only `PY_FILE`.

---

### 1.2 Symbol layer (SCIP-backed “semantic identity”)

4. **`PY_SYMBOL`**

* Meaning: a resolved semantic symbol (from SCIP symbol string)
* Anchor: no span required (symbol is global identity)
* Key props:

  * `scip_symbol` (the full symbol string)
  * `display_name`
  * `symbol_kind` (function, class, method, field, etc. if present)
* Why this matters:

  * It’s your *semantic join surface* across files.

---

### 1.3 Definition layer (classes, functions, parameters)

These are the core “things that exist”.

5. **`PY_CLASS_DEF`**

* Meaning: a class definition
* Anchor: class header span (or whole class statement span)
* Key props:

  * `name`
  * `is_dataclass` (optional derived)
  * `decorator_count` (optional)
* Typical source:

  * CST is best for precise spans + decorators; AST can also do it

6. **`PY_FUNCTION_DEF`**

* Meaning: `def f(...): ...` (top-level function)
* Anchor: function header span (or whole function statement span)
* Key props:

  * `name`
  * `is_async`
  * `decorator_count`
* Source:

  * CST for anchors and signature surface; AST for semantic structure if needed

7. **`PY_METHOD_DEF`** *(optional; can be derived as `PY_FUNCTION_DEF` with an owner edge)*

* Meaning: a function definition that is owned by a class
* Anchor: same as function def
* If you keep this separate:

  * It reduces downstream “is method?” checks
* If you don’t:

  * just emit `PY_FUNCTION_DEF` and connect it to the class via `PY_MEMBER_OF`.

8. **`PY_PARAMETER`**

* Meaning: a function parameter (including `*args`, `**kwargs`, positional-only)
* Anchor: parameter token/span if possible; otherwise function header span + index
* Key props:

  * `param_name`
  * `param_kind` in `{posonly, pos, varpos, kwonly, varkw}`
  * `has_default: bool`
  * `has_annotation: bool`
  * `position_index: int`

9. **`PY_DECORATOR`**

* Meaning: decorator expression `@decorator(...)` or `@decorator`
* Anchor: decorator expression span
* Key props:

  * `decorator_text` (optional)
* Why:

  * decorators can be calls, attributes, names; it’s useful to treat them as first-class nodes.

---

### 1.4 Reference/expression layer (names, attributes, calls, imports)

These represent “code usage sites” (edges will connect them to semantic meaning).

10. **`PY_NAME_REF`**

* Meaning: an identifier usage in code (`x`, `foo`, etc.)
* Anchor: identifier token span
* Key props:

  * `name`
  * `ctx` in `{read, write, del, unknown}` (best-effort)
* Notes:

  * Some resolvers produce multiple candidate qualified names; **store sets, not scalars**.

11. **`PY_ATTRIBUTE_ACCESS`**

* Meaning: attribute syntax `obj.attr`
* Anchor: the full expression span
* Key props:

  * `attr_name` (string)
  * `is_chained: bool` (optional: `a.b.c`)
* Why:

  * attribute access is extremely common and central to call resolution in Python.

12. **`PY_CALLSITE`**

* Meaning: a call expression `f(...)`, `obj.m(...)`, `pkg.f(...)`, etc.
* Anchor: full `Call` expression span
* Key props:

  * `arg_count_positional`
  * `arg_count_keyword`
  * `has_starargs`, `has_kwargs`
  * `callee_shape` in `{name, attribute, subscript, lambda, unknown}` (optional)

13. **`PY_IMPORT_STMT`**

* Meaning: an import statement (`import x`, `from x import y`)
* Anchor: full import statement span
* Key props:

  * `import_kind` in `{import, import_from}`
  * `module_text` (the `from` module if present)
  * `level` (relative import level, if you track it)

14. **`PY_IMPORT_ALIAS`**

* Meaning: a single imported name binding, e.g. `import numpy as np` or `from x import y as z`
* Anchor: the alias token span
* Key props:

  * `local_name` (`np`, `z`)
  * `imported_name` (`numpy`, `y`)
  * `is_star: bool`

---

### 1.5 Typing layer (annotations as expressions + resolved meaning)

Key stance: **type hints are syntax expressions**; model them as such, then optionally link to `PY_SYMBOL`.

15. **`PY_TYPE_EXPR`**

* Meaning: a type annotation expression (e.g. `list[int]`, `Foo | None`, `typing.Optional[Foo]`)
* Anchor: annotation span
* Key props:

  * `expr_text` (optional; useful for debugging)
  * `is_stringized: bool` (if you handle postponed evaluation)
* Notes:

  * You can attach multiple “type referents” to a type expression.

16. **`PY_TYPE_ARG`** *(optional, if you want deep typing structure)*

* Meaning: a single type argument inside a generic type expression
* Anchor: span of the argument expression
* Why:

  * Helpful if you want a graph like `list[T]` → contains `T`.

---

## 2) Edge kinds

### 2.1 Structural containment and ownership

1. **`PY_CONTAINS`**

* `(PY_FILE|PY_MODULE|PY_CLASS_DEF|PY_FUNCTION_DEF) -> (defs/usages within)`
* Use for:

  * file contains class/function/import
  * class contains method
  * function contains local usage sites (optional)

2. **`PY_MEMBER_OF`**

* `PY_FUNCTION_DEF -> PY_CLASS_DEF`
* Meaning: function is a member/method of a class (even if you didn’t emit `PY_METHOD_DEF`)

3. **`PY_HAS_PARAMETER`**

* `PY_FUNCTION_DEF -> PY_PARAMETER`
* Required for “rich” function signature modeling

4. **`PY_HAS_DECORATOR`**

* `(PY_CLASS_DEF|PY_FUNCTION_DEF) -> PY_DECORATOR`

---

### 2.2 Import graph

5. **`PY_HAS_IMPORT_ALIAS`**

* `PY_IMPORT_STMT -> PY_IMPORT_ALIAS`

6. **`PY_IMPORTS_SYMBOL`**

* `PY_IMPORT_ALIAS -> PY_SYMBOL`
* Preferred source: SCIP occurrences where `symbol_roles` includes the Import bit (if available), otherwise best-effort via name resolution. The roles are a bitmask (Definition/Import/Write/Read/etc.).

7. **`PY_IMPORTS_MODULE`** *(optional)*

* `PY_IMPORT_STMT -> PY_MODULE` or `PY_SYMBOL` (if you treat modules as symbols)
* Meaning: import statement references a module namespace.

8. **`PY_ALIASES`**

* `PY_IMPORT_ALIAS -> PY_SYMBOL` *(or -> PY_MODULE)*
* Meaning: “local_name is an alias of imported entity”
* Useful for binding-level edges if you add `PY_BINDING` later.

---

### 2.3 Calls

9. **`PY_CALLSITE_CALLEE`**

* `PY_CALLSITE -> (PY_NAME_REF|PY_ATTRIBUTE_ACCESS)`
* Meaning: syntactic callee expression
* Why:

  * you can build call resolution in two stages:

    1. callee syntax
    2. callee resolves to symbol(s)

10. **`PY_CALLS_SYMBOL`**

* `PY_CALLSITE -> PY_SYMBOL`
* Meaning: semantic call edge (resolved)
* Ambiguity:

  * if there are multiple candidates, emit multiple edges and group them (see `ambiguity_group_id` below).

11. **`PY_CALL_ARG`**

* `PY_CALLSITE -> PY_NAME_REF` *(or a future `PY_EXPR` node family)*
* Meaning: call argument usage site (optional but enables arg→param mapping)

12. **`PY_ARG_MATCHES_PARAM`** *(optional, high value)*

* `PY_NAME_REF (arg expr) -> PY_PARAMETER`
* Meaning: “this argument position/name maps to that parameter”
* Implementation note:

  * You can do conservative matching (positional index and keyword name)
  * This is still very useful without full runtime binding.

---

### 2.4 References and symbol roles (reads/writes/defs)

These are essential for any “code intelligence” graph.

13. **`PY_DEFINES_SYMBOL`**

* `(PY_CLASS_DEF|PY_FUNCTION_DEF|PY_PARAMETER|PY_IMPORT_ALIAS|PY_NAME_REF) -> PY_SYMBOL`
* Source:

  * SCIP occurrence with Definition bit set (`symbol_roles & Definition != 0`)

14. **`PY_REFERENCES_SYMBOL`**

* `(PY_NAME_REF|PY_ATTRIBUTE_ACCESS|PY_CALLSITE|PY_IMPORT_ALIAS) -> PY_SYMBOL`
* Source:

  * SCIP occurrence without Definition bit (or explicit Reference role if you track it)

15. **`PY_READS_SYMBOL`**

* `PY_NAME_REF -> PY_SYMBOL`
* Source:

  * SCIP `symbol_roles` bitmask includes ReadAccess

16. **`PY_WRITES_SYMBOL`**

* `PY_NAME_REF -> PY_SYMBOL`
* Source:

  * SCIP `symbol_roles` includes WriteAccess

> You don’t need perfect “read vs write” to be useful, but when SCIP provides it, it’s worth emitting because it unlocks def-use style analysis later.

---

### 2.5 Attributes

17. **`PY_ATTRIBUTE_BASE`**

* `PY_ATTRIBUTE_ACCESS -> PY_NAME_REF` *(or -> another `PY_ATTRIBUTE_ACCESS` for chains)*
* Meaning: the base expression of an attribute access (`obj` in `obj.attr`)

18. **`PY_ATTRIBUTE_RESOLVES_TO_SYMBOL`**

* `PY_ATTRIBUTE_ACCESS -> PY_SYMBOL`
* Meaning: semantic resolution of attribute access (method, field, property)
* Ambiguity:

  * multiple edges allowed (dynamic dispatch / monkey patching / incomplete typing)

19. **`PY_MEMBER_ACCESS`** *(optional, for class membership semantics)*

* `PY_CLASS_DEF -> PY_SYMBOL`
* Meaning: class “has member symbol” (can be derived after you’ve linked methods/fields)

---

### 2.6 Classes and inheritance

20. **`PY_INHERITS_FROM_SYMBOL`**

* `PY_CLASS_DEF -> PY_SYMBOL`
* Meaning: base class relationship
* Source:

  * type expression in class bases + resolution

21. **`PY_INHERITS_FROM_CLASS`** *(optional)*

* `PY_CLASS_DEF -> PY_CLASS_DEF`
* Meaning: base class resolved to a class def in same snapshot (when resolvable)
* Implement by:

  * base resolves to a `PY_SYMBOL`
  * `PY_SYMBOL` crosswalk resolves to a local `PY_CLASS_DEF`

---

### 2.7 Typing edges (annotations + resolution)

22. **`PY_ANNOTATES_PARAM`**

* `PY_PARAMETER -> PY_TYPE_EXPR`

23. **`PY_ANNOTATES_RETURN`**

* `PY_FUNCTION_DEF -> PY_TYPE_EXPR`

24. **`PY_ANNOTATES_VAR`** *(optional if you add var/binding nodes)*

* `PY_NAME_REF|PY_BINDING -> PY_TYPE_EXPR`

25. **`PY_TYPE_REFERS_TO_SYMBOL`**

* `PY_TYPE_EXPR -> PY_SYMBOL`
* Meaning: a type annotation references a semantic symbol (`Foo` in `Foo | None`)
* Ambiguity:

  * multiple referents allowed; do not collapse

26. **`PY_TYPE_ARG_OF`** *(optional deep typing)*

* `PY_TYPE_ARG -> PY_TYPE_EXPR` (or reversed)
* Meaning: captures generic relationships (`T` is an arg of `list[T]`)

---

## 3) Ambiguity and candidate sets: how to represent without losing information

### 3.1 Never collapse “multiple candidates” into one

LibCST-style qualified name resolution can produce **multiple candidates**, and this must remain a set, not a scalar.
Similarly, qualified names are returned as sets of `(name, source)` values (not a single string).

### 3.2 Two recommended representations

Pick one and standardize it:

**Option A: Edges with grouping**

* Emit multiple edges like `PY_CALLS_SYMBOL` or `PY_ATTRIBUTE_RESOLVES_TO_SYMBOL`
* Add:

  * `ambiguity_group_id` (same for all candidates from one resolution event)
  * `confidence` per candidate

**Option B: Properties as sets**

* Store candidate lists on the usage node in `cpg_props`, e.g.:

  * `key="callee_candidates"`, `value_string` repeated rows or stored as bytes/json
* Still emit `PY_CALLS_SYMBOL` edges only when SCIP gives a precise symbol

I recommend **Option A** for graph-native queries.

---

## 4) Stable IDs for these kinds (so the enumeration is implementable)

For any **span-anchored** node (defs/usages/calls/imports/types), use a deterministic ID derived from:

* `path`, `byte_start`, `byte_len`, `kind`
  This exact “byte span keyed stable ID” rule is a proven indexer stance: byte spans are canonical and slice raw evidence without re-tokenization.

For any **span-less** node like `PY_SYMBOL`, use:

* `hash(snapshot_id + scip_symbol_string)`.

---

## 5) Minimal “rich Python CPG” set to implement first

If you want the tightest set that still feels “rich” (your categories covered), implement:

### Nodes

* `PY_FILE`, `PY_CLASS_DEF`, `PY_FUNCTION_DEF`, `PY_PARAMETER`
* `PY_CALLSITE`, `PY_IMPORT_STMT`, `PY_IMPORT_ALIAS`
* `PY_ATTRIBUTE_ACCESS`, `PY_NAME_REF`
* `PY_TYPE_EXPR`
* `PY_SYMBOL`

### Edges

* `PY_CONTAINS`, `PY_MEMBER_OF`, `PY_HAS_PARAMETER`
* `PY_HAS_IMPORT_ALIAS`, `PY_IMPORTS_SYMBOL`, `PY_ALIASES`
* `PY_CALLSITE_CALLEE`, `PY_CALLS_SYMBOL`
* `PY_DEFINES_SYMBOL`, `PY_REFERENCES_SYMBOL`, `PY_READS_SYMBOL`, `PY_WRITES_SYMBOL`
* `PY_ATTRIBUTE_BASE`, `PY_ATTRIBUTE_RESOLVES_TO_SYMBOL`
* `PY_ANNOTATES_PARAM`, `PY_ANNOTATES_RETURN`, `PY_TYPE_REFERS_TO_SYMBOL`
* `PY_INHERITS_FROM_SYMBOL`

That set gives you exactly what you asked for: **classes/functions/parameters/calls/imports/attributes/typing edges**—with ambiguity preserved.

---

If you want, I can also provide a “derivation recipe” per kind (e.g., which CST node types produce `PY_CALLSITE`, how to extract `callee_shape`, and how to map SCIP occurrences to `PY_NAME_REF`/`PY_ATTRIBUTE_ACCESS` anchors) in the same strict, implementable style as this enumeration.



Below is an implementable “derivation recipe” for the **Python-focused node/edge kinds** we’ve been discussing—specifically showing:

* which **LibCST node types** emit each **CPG node_kind**
* how to compute **byte-accurate anchors/spans**
* how to compute **callee_shape** for `PY_CALLSITE`
* how to map **SCIP occurrences → (PY_NAME_REF / PY_ATTRIBUTE_ACCESS / …) anchors** deterministically (even under ambiguity)

I’m going to assume this core design choice (it’s the key that makes everything joinable and inference-friendly):

## Canonical coordinate system: byte spans

**All anchors/spans in the CPG are stored as `(start_byte, end_byte)` in UTF‑8 bytes**, end-exclusive.

* LibCST can produce this directly via `ByteSpanPositionProvider`: it yields a `CodeSpan(start, length)` where offsets/length are **bytes**, not characters, and excludes node-owned whitespace. ([LibCST][1])
* CPython `ast` location fields (`col_offset`, `end_col_offset`) are also specified as **UTF‑8 byte offsets** for the first/last token that generated the node. ([Python documentation][2])
* SCIP occurrences provide `(line, character-offset)` ranges plus a `position_encoding`; you must convert those offsets into UTF‑8 byte spans (recipe below). `Occurrence.range` is commonly a list of 3 or 4 ints (either `[line, start, end]` or `[start_line, start_char, end_line, end_char]`).

This is what makes “SCIP ↔ CST” joins reliable.

---

## Base inputs you must materialize (minimal)

### A) `cst_anchors` (from LibCST + metadata)

You will parse each file with LibCST and compute metadata at least for:

* `ByteSpanPositionProvider` (for byte spans) ([LibCST][1])
* `ParentNodeProvider` (to find enclosing constructs; optional but highly recommended) ([LibCST][1])
* `ExpressionContextProvider` (LOAD/STORE/DEL for Names/Attributes/Subscripts; helps when SCIP is missing or for fallback) ([LibCST][1])
* optionally `QualifiedNameProvider` / `FullyQualifiedNameProvider` (for *fallback* disambiguation; expect sets of candidates) ([LibCST][1])

**Row schema (minimum):**

* `file_path: string`
* `anchor_id: string` (stable; see below)
* `anchor_kind: enum` (e.g., `NAME`, `ATTRIBUTE`, `CALL`, `IMPORT_ALIAS`, `FUNCTION_DEF_NAME`, `CLASS_DEF_NAME`, `PARAM_NAME`, `TYPE_EXPR`, …)
* `start_byte: int64`
* `end_byte: int64`
* `cst_type: string` (e.g., `"Name"`, `"Attribute"`, …)
* `text_hint: string?` (e.g., identifier text for `Name`, evaluated import strings, etc.)
* `parent_anchor_id: string?` (optional, but extremely useful)

**Stable ID rule (recommended):**
`anchor_id = f"{file_path}:{start_byte}:{end_byte}:{anchor_kind}"`

### B) `scip_occurrences` (from SCIP index)

For each SCIP `Document`:

* `file_path: string` (`Document.relative_path`)
* `occ_id: string` (stable: `f"{file_path}:{i}"` where `i` is occurrence index in the document)
* `symbol: string`
* `symbol_roles: int` (bitmask)
* `range_raw: list[int]`
* `position_encoding: enum/int` (from `Document.position_encoding`)
* plus derived:

  * `start_byte: int64`
  * `end_byte: int64`

SCIP notes:

* `symbol_roles` is a bitmask: Definition=1, Import=2, WriteAccess=4, ReadAccess=8 (and others).
* `Occurrence` includes `symbol`, `symbol_roles`, and `range`.

---

## Converting SCIP ranges → UTF‑8 byte spans

### 1) Normalize `Occurrence.range` to a 4-tuple

SCIP ranges are commonly either:

* `[start_line, start_char, end_line, end_char]`, or
* `[line, start_char, end_char]` meaning `end_line = start_line`

**Normalization:**

* If `len(r)==3`: `(sl, sc, el, ec) = (r[0], r[1], r[0], r[2])`
* If `len(r)==4`: `(sl, sc, el, ec) = (r[0], r[1], r[2], r[3])`
* Else: treat as invalid and drop (or log)

### 2) Convert `(line, char_offset)` → UTF‑8 byte offset

You must respect `Document.position_encoding`. A practical conversion approach (works even under UTF‑16) is in the SCIP notes: convert code-unit offsets to Python string indices per-line, then slice. 

**Implementable recipe (per file):**

1. Read file text as `doc_text: str` (correct disk encoding).
2. Build `lines = doc_text.splitlines(keepends=True)`
3. Precompute `line_start_byte[i]` for each line `i` in UTF‑8 bytes:

   * `line_start_byte[0]=0`
   * `line_start_byte[i+1]=line_start_byte[i] + len(lines[i].encode("utf-8"))`
4. Define helper `code_unit_offset_to_py_index(line: str, off: int, position_encoding) -> int` exactly like the pattern shown (handle UTF‑16 by encoding to `utf-16-le` and slicing `off*2` bytes).
5. For a `(line_i, char_off)`:

   * `py_idx = code_unit_offset_to_py_index(lines[line_i], char_off, position_encoding)`
   * `byte_off_in_line = len(lines[line_i][:py_idx].encode("utf-8"))`
   * `abs_byte_off = line_start_byte[line_i] + byte_off_in_line`

Do that for both `(sl, sc)` and `(el, ec)` to get `(start_byte, end_byte)`.

---

## Core join: mapping SCIP occurrences → CST anchors

This is the central “semantic binding” step.

### Candidate set

For each occurrence `(file_path, occ_start_byte, occ_end_byte)` consider all `cst_anchors` rows in the same file where:

* **COVER**: `anchor.start_byte <= occ_start_byte` AND `occ_end_byte <= anchor.end_byte`

In Arrow/Acero terms this is a filter predicate after a (file_path) grouping join.

### Selection rule (deterministic)

From covering anchors choose the “best” one:

1. **Minimum span length**: `(anchor.end_byte - anchor.start_byte)` smallest wins
2. If tie: prefer `anchor_kind` in this priority order:

   * `NAME` (exact token anchors)
   * `IMPORT_ALIAS`
   * `PARAM_NAME`
   * `FUNCTION_DEF_NAME` / `CLASS_DEF_NAME`
   * `ATTRIBUTE_ATTR_NAME` (if you materialize separately)
   * `ATTRIBUTE` (whole expression)
   * `CALL` (whole call)
   * fallback: anything else
3. If still tie: lowest `start_byte`

This produces:

* `occ_to_anchor(occ_id → anchor_id)`

### Why this works

* SCIP `Occurrence.range` is usually token-level (identifier) while LibCST `Name` spans are also token-level; so the smallest-cover rule naturally maps semantics to tokens.
* For cases where SCIP anchors a token inside a larger CST node (e.g., `Attribute` expression), the smallest cover still picks the inner `Name` token if you include it.

---

# Derivation recipes by kind

Below, each kind includes: **(1) CST triggers**, **(2) fields**, **(3) edges implied**, and **(4) how SCIP attaches**.

---

## `PY_NAME_REF`

### CST triggers

Emit a `PY_NAME_REF` anchor for **every** `libcst.Name` node (even when you don’t know yet if it’s a def/ref/import).
Expression context metadata is available for `Name` nodes (LOAD/STORE/DEL) but note LibCST may omit context for some cases. ([LibCST][1])

### Anchor/span

* `span = ByteSpanPositionProvider(Name)` ([LibCST][1])
* `name = Name.value`

### Fields (minimum)

* `name: string`
* `expr_ctx: enum?` from `ExpressionContextProvider` if present (LOAD/STORE/DEL). ([LibCST][1])
* `syntactic_role_hint: enum?` (optional; computed via parent inspection)

### SCIP attachment

After `occ_to_anchor`:

* if `symbol_roles & Definition != 0`: this name occurrence is a **definition site**
* else it is a **reference / read / write / import** depending on bits (ReadAccess/WriteAccess/Import).

You do **not** need to decide def/ref at CST time. SCIP decides it.

---

## `PY_ATTRIBUTE_ACCESS`

### CST triggers

Emit `PY_ATTRIBUTE_ACCESS` for each `libcst.Attribute` expression.
Expression context metadata exists for `Attribute` nodes (STORE/LOAD/DEL). ([LibCST][1])

Also note: LibCST represents `.attr` as a `Name`, but ExpressionContextProvider **does not assign context to the `attr` Name** (to mirror CPython ast where `attr` is a string). ([LibCST][1])
So: treat the attribute’s `.attr` **Name token** as a separate `PY_NAME_REF` anchor (you’ll still get spans for it).

### Anchor/span

* `attr_expr_span = ByteSpan(Attribute)` ([LibCST][1])
* `base_span = ByteSpan(Attribute.value)`
* `attr_name_span = ByteSpan(Attribute.attr)` (this is a `Name` node)

### Fields (minimum)

* `chain_len: int` (flatten nested `Attribute`)
* `attr_terminal: string` (rightmost attribute name; from `.attr.value`)
* `is_store/load/del: enum?` from `ExpressionContextProvider` on the `Attribute` node ([LibCST][1])

### Edges you should emit syntactically

* `PY_ATTRIBUTE_BASE` : `PY_ATTRIBUTE_ACCESS -> (node for base expr)`
* `PY_ATTRIBUTE_ATTR_TOKEN` : `PY_ATTRIBUTE_ACCESS -> (PY_NAME_REF for Attribute.attr)`

### SCIP attachment (important)

SCIP occurrences for attribute references usually bind to the **token span** (the `.attr` name), not the full `obj.attr` expression. Your join will map the occurrence to the `PY_NAME_REF` for `.attr` (smallest-cover).

To recover “this reference happened as an attribute access”, you do:

* Find enclosing `PY_ATTRIBUTE_ACCESS` whose `attr_name_span` covers the occurrence span.
* Add derived edge: `PY_NAME_REF --PY_IS_ATTR_MEMBER_OF--> PY_ATTRIBUTE_ACCESS` (or store `enclosing_attribute_access_id` on the name ref)

This gives you both:

* token-level semantic binding (SCIP → `PY_NAME_REF`)
* expression-level structural context (`PY_ATTRIBUTE_ACCESS`)

---

## `PY_CALLSITE`

### CST triggers

Emit `PY_CALLSITE` for each `libcst.Call`. ([LibCST][3])
LibCST `Call`:

* `func: BaseExpression` (often `Name` or `Attribute`) ([LibCST][3])
* `args: Sequence[Arg]` supporting positional/keyword/star args ([LibCST][3])
  LibCST `Arg` includes:
* `keyword: Name | None`
* `star: Literal['', '*', '**']` ([LibCST][3])

### Anchor/span

* `call_span = ByteSpan(Call)` ([LibCST][1])
* `func_span = ByteSpan(Call.func)` (callee expression span)

### `callee_shape` extraction (deterministic)

Inspect the runtime type of `Call.func`:

* if `isinstance(func, libcst.Name)`: `callee_shape="NAME"`
* elif `isinstance(func, libcst.Attribute)`: `callee_shape="ATTRIBUTE"`
* elif `isinstance(func, libcst.Subscript)`: `callee_shape="SUBSCRIPT"`
* elif `isinstance(func, libcst.Call)`: `callee_shape="CALL_RESULT"` (rare but valid: `f()()`)
* elif `isinstance(func, libcst.Lambda)`: `callee_shape="LAMBDA"`
* else: `callee_shape=f"OTHER:{type(func).__name__}"`

This is grounded in the fact `Call.func` is a `BaseExpression`, commonly `Name`/`Attribute`. ([LibCST][3])

### Arg-shape fields (minimum)

From `Call.args` (each an `Arg`): ([LibCST][3])

* `n_args_total = len(args)`
* `n_keyword = count(arg.keyword is not None)`
* `n_star_1 = count(arg.star == '*')`
* `n_star_2 = count(arg.star == '**')`
* `keyword_names = [arg.keyword.value for arg in args if arg.keyword]`

### Structural edges (syntactic)

* `PY_CALLSITE_CALLEE_EXPR`: `PY_CALLSITE -> (node for Call.func)`
* `PY_CALLSITE_HAS_ARG`: optional per-arg nodes, or store arg summary on callsite

### SCIP attachment: deriving `PY_CALLS_SYMBOL`

This is the most important semantic edge for call graphs.

**Goal:** connect `PY_CALLSITE` to the resolved symbol of the callee (when SCIP can resolve it).

**Recipe:**

1. For each `PY_CALLSITE`, compute the sub-span for its `func` expression (`func_span`).
2. Collect SCIP occurrences in the same file whose `(start_byte,end_byte)` is **contained in** `func_span` and whose `symbol_roles` indicate a non-definition reference (typically ReadAccess).
3. Pick the “best callee occurrence” by:

   * smallest span length
   * prefer occurrences mapped to `PY_NAME_REF` over larger nodes
4. Emit:

   * `PY_CALLS_SYMBOL`: `PY_CALLSITE -> SCIP_SYMBOL(symbol)`
   * optionally `PY_CALLSITE_CALLEE_TOKEN`: `PY_CALLSITE -> PY_NAME_REF(anchor_id)` for the token that carried the symbol

This is robust even when `callee_shape="ATTRIBUTE"`: SCIP will typically bind the attribute member token; you still recover the callsite.

---

## `PY_FUNCTION_DEF`

### CST triggers

Emit `PY_FUNCTION_DEF` for each `libcst.FunctionDef`.

LibCST `FunctionDef` fields include:

* `name: Name`
* `params: Parameters`
* `returns: Annotation | None`
* `decorators: Sequence[Decorator]` ([LibCST][3])

### Anchor/span

Recommended: anchor the def node on the **function name token** (stable, small, aligns with SCIP def occurrence):

* `name_span = ByteSpan(FunctionDef.name)`
  Optionally also store:
* `def_span = ByteSpan(FunctionDef)` for “body extraction” / enclosing range comparisons

### Fields (minimum)

* `name: string`
* `is_async: bool` (if you also handle `AsyncFunctionDef` separately or inspect `asynchronous` field)
* `has_return_annotation: bool`
* `n_decorators: int`

### SCIP attachment

SCIP will have an occurrence with `symbol_roles` containing Definition for the function symbol. You map that occurrence to the `PY_NAME_REF` at the `FunctionDef.name` span; then you can:

* either attach the symbol to the `PY_FUNCTION_DEF` node
* or treat the `PY_NAME_REF` as the “definition anchor” and link `PY_FUNCTION_DEF -> PY_NAME_REF`

Both work; I recommend:

* `PY_FUNCTION_DEF --PY_DEF_NAME--> PY_NAME_REF`
* `PY_NAME_REF --PY_DEFINES_SYMBOL--> SCIP_SYMBOL`

---

## `PY_PARAMETER`

### CST triggers

Emit parameter nodes by traversing `FunctionDef.params` (a `libcst.Parameters`). ([LibCST][3])

LibCST `Parameters` includes:

* `params: Sequence[Param]`
* `posonly_params: Sequence[Param]`
* `kwonly_params: Sequence[Param]`
* `star_arg: Param | ParamStar | MaybeSentinel`
* `star_kwarg: Param | MaybeSentinel` ([LibCST][3])

LibCST `Param` includes:

* `name: Name`
* `annotation: Annotation | None`
* `default: BaseExpression | None` ([LibCST][3])

### Param kind classification (deterministic)

Create one `PY_PARAMETER` per `Param` and classify `param_kind` by where it came from:

* if in `posonly_params`: `POSONLY`
* elif in `params`: `POS_OR_KW`
* elif equals `star_arg` and is a `Param`: `VAR_POS` (the `*args` parameter itself)
* elif in `kwonly_params`: `KWONLY`
* elif equals `star_kwarg` (a `Param`): `VAR_KW` (the `**kwargs` parameter itself)

Also emit `PY_PARAM_STAR_SENTINEL` if `star_arg` is a `ParamStar` and you want to preserve the bare `*` sentinel (optional). ([LibCST][3])

### Anchors/spans

* anchor each parameter on `Param.name` token span

### Edges

* `PY_HAS_PARAMETER`: `PY_FUNCTION_DEF -> PY_PARAMETER` (with `param_index`, `param_kind`)
* `PY_PARAM_NAME`: `PY_PARAMETER -> PY_NAME_REF` (token)

### SCIP attachment

SCIP will often mark parameter names as Definition occurrences too. Your occ→anchor join will attach symbols to the `PY_NAME_REF` token; you can lift that onto the `PY_PARAMETER`.

---

## `PY_CLASS_DEF`

### CST triggers

Emit `PY_CLASS_DEF` for each `libcst.ClassDef`.

LibCST `ClassDef` includes:

* `name: Name`
* `bases: Sequence[Arg]`
* `decorators: Sequence[Decorator]` ([LibCST][3])

### Anchors/spans

* anchor on `ClassDef.name` token span
* optionally store `class_span = ByteSpan(ClassDef)` as well

### Base classes

Each `bases[i]` is a LibCST `Arg`; use `Arg.value` as the base expression. (Same `Arg` type as call args; it supports keywords/star; but bases typically use positional.) ([LibCST][3])

Emit:

* `PY_TYPE_EXPR` node for each base expression span (recommended)
* structural edge: `PY_CLASS_BASE_EXPR`: `PY_CLASS_DEF -> PY_TYPE_EXPR`

Then semantic resolution:

* Map SCIP occurrences inside that base expr span to symbols; emit:

  * `PY_INHERITS_FROM_SYMBOL`: `PY_CLASS_DEF -> SCIP_SYMBOL`

---

## `PY_IMPORT_STMT` and `PY_IMPORT_ALIAS`

### CST triggers

* `PY_IMPORT_STMT` from `libcst.Import` and `libcst.ImportFrom`
* `PY_IMPORT_ALIAS` from each `libcst.ImportAlias` in the statement

LibCST:

* `Import.names: Sequence[ImportAlias]` ([LibCST][3])
* `ImportFrom` is `from x import y` ([LibCST][3])
* `ImportAlias` has:

  * `name: Attribute | Name`
  * `asname: AsName | None`
  * `evaluated_name: str`
  * `evaluated_alias: str | None` ([LibCST][3])

### Anchors/spans

* `PY_IMPORT_STMT` anchored to the statement node span (`Import` / `ImportFrom`)
* `PY_IMPORT_ALIAS` anchored to `ImportAlias` node span (or to the local alias token, if present—either works; choose one and be consistent)

### Fields

For `PY_IMPORT_ALIAS`:

* `imported: str = ImportAlias.evaluated_name` ([LibCST][3])
* `local_alias: str? = ImportAlias.evaluated_alias` ([LibCST][3])

### SCIP attachment

SCIP occurrences with Import bit set correspond to imports. Import=2 is a known role bit. 

Emit edges:

* `PY_IMPORTS_SYMBOL`: `PY_IMPORT_ALIAS -> SCIP_SYMBOL` for occurrences mapped into the alias span with `Import` role.

---

# Practical “enclosing construct” recovery (how you connect token-anchors to higher nodes)

Because SCIP binds primarily to **tokens**, you need a consistent rule to connect token anchors to the larger structural nodes (`PY_CALLSITE`, `PY_ATTRIBUTE_ACCESS`, `PY_FUNCTION_DEF`, …).

You can do this purely by spans, no AST required:

### Build `enclosing_index` per file

For each structural node you emit (callsite, attribute_access, function_def, class_def, import_stmt, …) store its `(start_byte,end_byte)`.

### Enclosing lookup rule

Given a token anchor span `(a_start,a_end)`, find the smallest enclosing structural node span that covers it. Same selection rule as earlier: minimal covering span wins.

This gives:

* `name_ref.enclosing_callsite_id?`
* `name_ref.enclosing_attribute_access_id?`
* `name_ref.enclosing_function_def_id?`
* etc.

This is what lets you derive:

* call graph edges (`CALLSITE -> SYMBOL`) using the callee token occurrence
* “attribute read/write” on the specific attribute access expression
* “import binds local alias” relationships

---

# Where ambiguity lives (and how this design handles it)

### CST-only ambiguity

LibCST `QualifiedNameProvider` can return **multiple possible qualified names** for the same node (e.g., conditional imports). ([LibCST][1])
So CST alone is not enough to decide a unique target.

### SCIP resolves most ambiguity

SCIP occurrences name the resolved `symbol` directly, and provides roles (definition/import/read/write).

**Therefore the recommended policy is:**

* Use LibCST for **structure**
* Use SCIP for **semantic identity + roles**
* Join them on **byte spans**

This gives you the “inference-driven” modifiability you want: you can add new derived edges/nodes by writing new span-joins, without changing intermediate schemas by hand.

---

## If you want one “golden test” for the whole joining mechanism

Pick a file containing all three patterns:

```py
from pkg import mod as m
class C(Base): 
    def f(self, x: T) -> R:
        return m.g(self.a, x, **kw)
```

You should see all of these succeed:

* SCIP Definition occurrence for `C` maps to the `Name("C")` token anchor
* `Base` in bases maps to a token in the base expr span → `PY_INHERITS_FROM_SYMBOL`
* `f` Definition maps to the function name token
* `x` parameter Definition maps to param name token
* `m.g(...)` emits a `PY_CALLSITE` with `callee_shape="ATTRIBUTE"`
* SCIP callee occurrence maps to the `.g` token (a `Name` inside the `Attribute`)
* your enclosure rule maps that token to the enclosing `PY_CALLSITE`, producing `PY_CALLS_SYMBOL`

---

If you want, I can extend this again with a similarly strict recipe for **control-flow and data-flow** nodes (e.g., `PY_IF`, `PY_FOR`, `PY_WITH`, `PY_ASSIGN`, `PY_RETURN`) and the minimal additional edges needed to make those useful for CPG-style traversals (dominance-lite, def-use hints) while still keeping SCIP as the semantic authority.

[1]: https://libcst.readthedocs.io/en/latest/metadata.html "Metadata — LibCST  documentation"
[2]: https://docs.python.org/3/library/ast.html "ast — Abstract syntax trees — Python 3.14.2 documentation"
[3]: https://libcst.readthedocs.io/en/latest/nodes.html "Nodes — LibCST  documentation"


According to documents from **January 10, 2026**, here is a **blank‑slate, implementable file-by-file build blueprint** for an **inference-driven CPG builder** that combines:

* **Apache Hamilton** for inference-driven orchestration (targets determine what runs; materializers for I/O; caching/telemetry hooks)
* **PyArrow Dataset + Acero** for *plan-first* relational execution with pushdown, joins, filtering/projection, and a centralized “finalize gate” for schema alignment + determinism + invariants
* **PyArrow compute** only via a curated “kernel lane” (never ad hoc in DAG nodes)
* **AST + LibCST + SCIP** as input fact sources aligned on **byte spans** (not character offsets)

---

## 0) Repository layout (the full set of Python files)

This is the **minimal “full system”** that an expert Python engineer can build against without prior Arrow/Hamilton/CPG background.

```
src/codeintel_cpg/
  __init__.py
  config.py
  types.py

  arrowdsl/
    __init__.py
    runtime.py
    plan.py
    expr.py
    queryspec.py
    joins.py
    kernels.py
    contracts.py
    finalize.py
    dataset_io.py
    runner.py

  extract/
    __init__.py
    repo_scan.py
    ast_extract.py
    cst_extract.py
    scip_extract.py
    symtable_extract.py
    bytecode_extract.py

  normalize/
    __init__.py
    spans.py
    schema_infer.py
    ids.py

  relspec/
    __init__.py
    model.py
    registry.py
    compiler.py

  cpg/
    __init__.py
    kinds.py
    schemas.py
    build_nodes.py
    build_edges.py
    build_props.py

  hamilton/
    __init__.py
    driver_factory.py
    modules/
      __init__.py
      inputs.py
      extraction.py
      normalization.py
      cpg_build.py
      outputs.py

  storage/
    __init__.py
    parquet.py
    ipc.py

  obs/
    __init__.py
    manifest.py
    stats.py
    repro.py

  graph/
    __init__.py
    rustworkx_adapter.py
```

---

## 1) `config.py` — build configuration and policy knobs

### Purpose

Centralize *every knob* that affects determinism, schema tolerance, and runtime execution.

### Public API (signatures only)

```python
@dataclass(frozen=True)
class BuildConfig:
    repo_root: str
    out_dir: str
    scip_index_cmd: list[str]          # e.g. ["scip-python", "index", ...]
    python_version_tag: str            # e.g. "py311"
    strictness: Literal["strict", "tolerant"]
    determinism: Literal["canonical", "stable_set", "best_effort"]

    # Arrow
    use_threads: bool
    preserve_order_on_write: bool
    schema_promote: Literal["permissive", "default"]

    # Provenance/observability
    emit_provenance: bool
    emit_repro_artifacts: bool
```

### Key library calls it influences

* `pyarrow.dataset.write_dataset(..., preserve_order=...)`
* `pa.unify_schemas([...], promote_options="permissive")`
* `Declaration.to_reader(use_threads=ctx.use_threads)`

---

## 2) `types.py` — shared “semantic” types

### Purpose

Keep type aliases and small dataclasses that are used everywhere.

### Public API

```python
FileId = int
NodeId = int
EdgeId = int

@dataclass(frozen=True)
class ByteSpan:
    file_id: FileId
    start: int
    end: int

@dataclass(frozen=True)
class RepoFile:
    file_id: FileId
    path: str
    content_bytes: bytes
```

---

# arrowdsl (the core of “all-in on Arrow + Acero”)

This is the layer that enforces:

* plans are *declared* (`Plan`) and executed centrally (`runner.run_pipeline`)
* nodes do **not** call `pc.*` directly

---

## 3) `arrowdsl/runtime.py` — ExecutionContext + determinism tiers

### Purpose

Single place to define runtime knobs that should **never** be embedded ad hoc in pipeline code (threads, provenance, determinism tier).

### Public API

```python
class DeterminismTier(str, Enum):
    CANONICAL = "canonical"
    STABLE_SET = "stable_set"
    BEST_EFFORT = "best_effort"

@dataclass(frozen=True)
class ExecutionContext:
    use_threads: bool
    determinism: DeterminismTier
    provenance: bool
```

### Key supporting doc constraints

* “Execution knobs belong in a runtime profile (`use_threads`, etc.), not in ad hoc node code.”

---

## 4) `arrowdsl/plan.py` — Plan wrapper around Acero Declarations

### Purpose

A tiny IR that:

* holds an `acero.Declaration` (or a table/reader thunk)
* exposes **one** execution surface: `to_reader()` or `to_table()`
* makes it easy to prove equivalence of output surfaces in tests

### Public API

```python
TableThunk = Callable[[], pa.Table]
ReaderThunk = Callable[[ExecutionContext], pa.RecordBatchReader]

@dataclass(frozen=True)
class Plan:
    decl: acero.Declaration | None = None
    table_thunk: TableThunk | None = None
    reader_thunk: ReaderThunk | None = None

    def to_reader(self, *, ctx: ExecutionContext) -> pa.RecordBatchReader: ...
    def to_table(self, *, ctx: ExecutionContext) -> pa.Table: ...
```

### Key library calls

* `acero.Declaration(...).to_reader(use_threads=...)`
* `acero.Declaration(...).to_table(use_threads=...)` (explicit “accumulate everything” sink)
* Guardrail: `to_table()` is memory-expensive; prefer streaming for large data

---

## 5) `arrowdsl/expr.py` — expression macros (pushdown-friendly)

### Purpose

Return **Expressions**, not arrays, so the same predicate/projection vocabulary can be used for:

* Dataset scan pushdown
* Acero `filter` / `project` nodes

### Public API

```python
class E:
    @staticmethod
    def field(name: str) -> pc.Expression: ...
    @staticmethod
    def scalar(value: Any) -> pc.Expression: ...

    @staticmethod
    def eq(col: str, value: Any) -> pc.Expression: ...
    @staticmethod
    def ge(col: str, value: Any) -> pc.Expression: ...
    @staticmethod
    def and_(*exprs: pc.Expression) -> pc.Expression: ...
    @staticmethod
    def or_(*exprs: pc.Expression) -> pc.Expression: ...
    @staticmethod
    def in_(col: str, values: Sequence[Any]) -> pc.Expression: ...
```

### Key library calls / gotchas

* Use `pc.scalar(...)` when you need an **Expression**, not `pa.scalar(...)` which returns a Scalar
* Expressions are combined with `& | ~` (not Python `and/or/not`)

---

## 6) `arrowdsl/queryspec.py` — QuerySpec (predicate + pushdown + projection)

### Purpose

Standardize the “one query, three call sites” pattern:

* pushdown pruning predicate
* semantic correctness predicate
* projection definition

### Public API

```python
@dataclass(frozen=True)
class ProjectionSpec:
    base_cols: tuple[str, ...]
    computed: dict[str, pc.Expression] = field(default_factory=dict)

@dataclass(frozen=True)
class QuerySpec:
    predicate: pc.Expression | None
    pushdown_predicate: pc.Expression | None
    projection: ProjectionSpec
```

### Key library calls

* Dataset scanner accepts:

  * `columns` as list **or dict mapping `{new_name: expression}`**
  * and allows “special fields” (provenance)
* Build scanner via `dataset.scanner(filter=expr, columns=[...])`

---

## 7) `arrowdsl/dataset_io.py` — open datasets, inspect schema, scan profiles

### Purpose

Everything related to reading/writing datasets lives here (not scattered).

### Public API

```python
@dataclass(frozen=True)
class ScanProfile:
    batch_size: int
    batch_readahead: int
    fragment_readahead: int
    use_threads: bool

def open_dataset(path: str) -> ds.Dataset: ...
def inspect_schema(path: str) -> pa.Schema: ...
def make_scanner(dataset: ds.Dataset, spec: QuerySpec, *, profile: ScanProfile) -> ds.Scanner: ...
def write_dataset(table_or_batches: Any, path: str, *, preserve_order: bool) -> None: ...
```

### Key library calls

* Schema inspect + permissive schema finish:

  * `ds.FileSystemDatasetFactory(...).inspect()`
  * `factory.finish(schema=pa.unify_schemas([...], promote_options="permissive"))`
* Scanner signature (important knobs):

  * `ds.Scanner.from_dataset(dataset, columns=..., filter=..., batch_size=..., batch_readahead=..., fragment_readahead=..., use_threads=...)`
* Useful stats:

  * `Dataset.count_rows(filter=...)`
  * `Dataset.get_fragments(filter=...)`
* Writer:

  * `ds.write_dataset(..., preserve_order=...)`

---

## 8) `arrowdsl/joins.py` — JoinSpec compiled to Acero hashjoin

### Purpose

Make joins **data-driven objects** that compile into an Acero plan. This is where your “join patterns as logical statements” live.

### Public API

```python
@dataclass(frozen=True)
class JoinSpec:
    join_type: Literal["inner", "left outer", "right outer", "full outer", "semi", "anti"]
    left_keys: tuple[str, ...]
    right_keys: tuple[str, ...]
    left_output: tuple[str, ...]
    right_output: tuple[str, ...]
    # optional: suffixes, null fill policy, etc.

def hash_join(left: Plan, right: Plan, spec: JoinSpec) -> Plan: ...
```

### Key library calls (this is the exact “join command + key args” you asked for)

* `acero.Declaration("hashjoin", acero.HashJoinNodeOptions(...), inputs=[left, right])` with:

  * `join_type=...`
  * `left_keys=[...]`, `right_keys=[...]`
  * `left_output=[...]`, `right_output=[...]`
* Follow with projection/renaming:

  * `acero.Declaration("project", acero.ProjectNodeOptions(expressions=[...], names=[...]), inputs=[...])`

### Determinism note

* Acero explicitly says **hash join has no predictable output order**
  So **you do not treat join output as ordered** unless you explicitly canonical-sort in finalize.

---

## 9) `arrowdsl/kernels.py` — the blessed eager compute primitives

### Purpose

This is the **only** place `pyarrow.compute` is used (plus maybe `finalize.py`). Nodes should never call `pc.*` directly.

### Public API

```python
def canonical_sort(table: pa.Table, *, sort_keys: list[tuple[str, str]]) -> pa.Table: ...
def dedupe_keep_first_after_sort(table: pa.Table, *, keys: list[str], tie_breakers: list[tuple[str, str]]) -> pa.Table: ...
def explode_list(table: pa.Table, *, list_col: str, parent_cols: list[str]) -> pa.Table: ...
```

### Key library calls

* Canonical sort is *stable*:

  * `pc.sort_indices(table, sort_keys=[("col","ascending"), ...])`
  * `table.take(idx)`
* “Explode list” pattern (repeat parent rows + flatten list):

  * `pc.list_parent_indices(table[list_col])`
  * `pc.list_flatten(table[list_col])`
  * `pc.take(table[parent_col], indices)`

---

## 10) `arrowdsl/contracts.py` — Contract, invariants, dedupe, canonical sort spec

### Purpose

Define **what output must look like** (schema + invariants + determinism), and treat it as immutable/versioned.

### Public API

```python
InvariantFn = Callable[[pa.Table], pc.Expression]   # returns boolean mask expression

@dataclass(frozen=True)
class SortKey:
    column: str
    order: Literal["ascending", "descending"] = "ascending"

@dataclass(frozen=True)
class DedupeSpec:
    keys: tuple[str, ...]
    tie_breakers: tuple[SortKey, ...]
    strategy: Literal["KEEP_FIRST_AFTER_SORT", "KEEP_BEST_BY_SCORE", "COLLAPSE_LIST", "KEEP_ARBITRARY"]

@dataclass(frozen=True)
class Contract:
    name: str
    schema: pa.Schema
    key_fields: tuple[str, ...]
    required_non_null: tuple[str, ...]
    invariants: tuple[InvariantFn, ...] = ()
    dedupe: DedupeSpec | None = None
    canonical_sort: tuple[SortKey, ...] = ()
```

### Source alignment

* This shape is directly recommended in the DSL guide

---

## 11) `arrowdsl/finalize.py` — schema align + invariants + determinism gate

### Purpose

One centralized correctness boundary:

* schema align/cast
* invariants → route bad rows to errors table
* canonical sort (Tier A)
* dedupe policy
* emit structured artifacts

### Public API

```python
@dataclass(frozen=True)
class FinalizeResult:
    good: pa.Table
    errors: pa.Table
    alignment: pa.Table
    stats: pa.Table

def finalize(table: pa.Table, *, contract: Contract, mode: str, ctx: ExecutionContext) -> FinalizeResult: ...
```

### Key library calls

* Align schemas:

  * `pa.unify_schemas([...], promote_options="permissive")` (when merging fragment schemas)
* Cast to contract schema:

  * `table.cast(contract.schema, safe=True/False)`
    Note: `Table.cast` requires names **and order** to match the schema
* Canonical sort:

  * `pc.sort_indices` + `table.take`

---

## 12) `arrowdsl/runner.py` — run_pipeline(plan → post-kernels → finalize)

### Purpose

A **single function** prevents execution logic from scattering and enforces the “Plan + post steps” discipline.

### Public API

```python
PostFn = Callable[[pa.Table, ExecutionContext], pa.Table]

def run_pipeline(
    *,
    plan: Plan,
    post: Iterable[PostFn],
    contract: Contract,
    mode: Literal["strict","tolerant"],
    ctx: ExecutionContext,
) -> FinalizeResult: ...
```

### Key rule (non-negotiable)

* “Nodes never call `pc.*` directly; they only compose `Plan` + `post` steps.”

---

# extract (AST/CST/SCIP → Arrow facts)

Your extraction modules produce **fact tables** (Arrow) with:

* stable IDs
* byte spans
* enough attributes to build CPG nodes/edges later

---

## 13) `extract/repo_scan.py` — enumerate Python source files

### Purpose

Create the foundational dataset: files, file IDs, raw bytes.

### Public API

```python
def scan_repo_files(repo_root: str) -> pa.Table:
    """Columns: file_id, path, content_bytes, maybe repo_id/commit."""
```

### Recommended output columns

* `file_id: int64` (stable hash of repo+path)
* `path: string`
* `content_bytes: binary`

---

## 14) `extract/ast_extract.py` — Python AST facts

### Purpose

Emit a row per AST node with:

* node kind
* parent id
* (start,end) byte span
* key attributes (name, op, literal kind, etc.)

### Key library calls

* `ast.parse(source, filename=..., type_comments=True)` (and/or `feature_version=`)
* Node locations: `lineno`, `col_offset`, `end_lineno`, `end_col_offset`
  These offsets are **UTF‑8 byte offsets**, not character counts

### Public API

```python
def build_ast_nodes(files: pa.Table) -> pa.Table:
    """Columns: ast_id, file_id, parent_ast_id, ast_kind, span_start, span_end, attrs(struct)"""
```

### Implementation note you must follow

Because AST offsets are *line-based* and use UTF‑8 byte offsets, you should:

* precompute `line_start_byte_offsets` per file
* translate `(lineno, col_offset)` → absolute `span_start` and similarly for end offsets

---

## 15) `extract/cst_extract.py` — LibCST facts + precise spans + contexts

### Purpose

LibCST gives:

* concrete syntax (tokens, formatting, parentheses)
* precise byte spans (via metadata)
* expression context (Load/Store/Del)

### Key library calls

* Parse:

  * `libcst.parse_module(source_text)`
* Metadata:

  * `libcst.MetadataWrapper(module)` then resolve providers
  * `ByteSpanPositionProvider` → `(start,end)` byte offsets
  * `ExpressionContextProvider` → Load/Store/Del context for expressions

### Public API

```python
def build_cst_anchors(files: pa.Table) -> pa.Table:
    """Columns: cst_anchor_id, file_id, anchor_kind, span_start, span_end, ctx, attrs"""

def build_cst_calls(files: pa.Table) -> pa.Table:
    """Columns: callsite_id, file_id, span_start, span_end, callee_shape, args_sig, receiver_span?, ..."""
```

### Key design rule

Do **not** store LibCST nodes long-term; metadata wrapper can copy nodes and invalidate identity comparisons. Use stable IDs derived from spans (and any stable-id helpers you define).

---

## 16) `extract/scip_extract.py` — SCIP symbols + occurrences

### Purpose

SCIP provides:

* symbol identity + definitions
* occurrences (references) with roles (Definition/Import/Write/Read, etc.)

### Key library calls / behavior

* SCIP “occurrence” ranges:

  * `[startLine, startCol, endCol]` or `[startLine, startCol, endLine, endCol]`
* `symbol_roles` is a bitset (Definition, Import, WriteAccess, ReadAccess, Generated, Test, etc.)

### Public API

```python
def run_scip_index(repo_root: str, cmd: list[str]) -> str:
    """Returns path to emitted .scip file."""

def parse_scip(path: str) -> tuple[pa.Table, pa.Table]:
    """Returns (symbols_table, occurrences_table)."""
```

### Span normalization requirement

SCIP column units depend on `position_encoding`; you must normalize all spans into **byte spans** so they join to AST/CST consistently.

---

## 17) `extract/symtable_extract.py` — symbol tables for scope relationships

### Purpose

Python’s `symtable` helps build:

* scope graph (module/class/function scopes)
* local/free/cell/global variable classifications (useful for dataflow edges)

### Key library calls

* `symtable.symtable(source, filename, compile_type="exec")`

### Public API

```python
def build_symtable_facts(files: pa.Table) -> pa.Table:
    """Columns: scope_id, file_id, scope_kind, name, parent_scope_id, symbols(list/struct)"""
```

---

## 18) `extract/bytecode_extract.py` — optional bytecode facts

### Purpose

Useful for:

* call targets in dynamic constructs
* control-flow approximations when AST/CST are ambiguous

### Key library calls

* `compile(source, filename, mode="exec")`
* `dis.get_instructions(code_obj)`

### Public API

```python
def build_bytecode_facts(files: pa.Table) -> pa.Table:
    """Columns: instr_id, file_id, offset, opname, argrepr, span_start?, span_end?"""
```

---

# normalize (unify spans, schemas, IDs)

---

## 19) `normalize/spans.py` — unify all source spans into byte spans

### Purpose

Create a consistent join key: `(file_id, span_start, span_end)`.

### Public API

```python
def normalize_ast_spans(ast_nodes: pa.Table, files: pa.Table) -> pa.Table: ...
def normalize_cst_spans(cst_tables: pa.Table) -> pa.Table: ...
def normalize_scip_spans(occ: pa.Table, files: pa.Table, position_encoding: str) -> pa.Table: ...
```

### Key constraint

AST uses UTF‑8 byte offsets for `col_offset` fields; LibCST ByteSpan provider also emits byte offsets—so **byte spans are the correct universal currency**.

---

## 20) `normalize/schema_infer.py` — inference-driven schema materialization

### Purpose

Handle ambiguous/missing columns without forcing intermediate schema specifications everywhere.

### Public API

```python
def infer_schema_for_dataset(dataset: ds.Dataset, *, promote: str) -> pa.Schema: ...
def align_table_to_schema(table: pa.Table, schema: pa.Schema, *, add_missing_nulls: bool) -> pa.Table: ...
```

### Key library calls

* `pa.unify_schemas([...], promote_options="permissive")`
* `table.cast(target_schema, safe=...)` (names+order must match)

---

## 21) `normalize/ids.py` — stable IDs for nodes/edges

### Purpose

Turn spans + kinds into stable identifiers.

### Public API

```python
def make_node_id(file_id: int, kind: str, span_start: int, span_end: int, salt: int = 0) -> int: ...
def make_edge_id(src: int, dst: int, kind: str, salt: int = 0) -> int: ...
```

---

# relspec (your “logical statements” that compile to Acero)

This is the layer that makes your pipeline **target-driven** and join patterns declarative.

---

## 22) `relspec/model.py` — declarative relationship + dataset specs

### Purpose

Represent “logical datasets” and “logical relationships” without hardcoding physical schemas.

### Public API

```python
@dataclass(frozen=True)
class DatasetRef:
    name: str            # e.g. "scip_occurrences"
    path: str            # dataset location
    contract: Contract

@dataclass(frozen=True)
class RelationshipRule:
    name: str
    left: str            # dataset name
    right: str
    join: JoinSpec
    # plus: predicates, projections, and “required for target X” tags
```

---

## 23) `relspec/registry.py` — central rule registry

### Purpose

One place to declare:

* which input datasets exist
* which relationships exist
* which outputs (targets) require which rules

### Public API

```python
@dataclass(frozen=True)
class BuildTargets:
    want_cpg_nodes: bool
    want_cpg_edges: bool
    want_callgraph: bool
    want_symbol_index: bool

def load_registry(cfg: BuildConfig) -> tuple[list[DatasetRef], list[RelationshipRule]]: ...
```

---

## 24) `relspec/compiler.py` — compile relationship rules to a single Plan

### Purpose

Transform a list of logical relationship rules into an Acero plan DAG (or multiple subplans) plus finalize contracts.

### Public API

```python
def compile_targets_to_plan(
    *,
    datasets: dict[str, ds.Dataset],
    rules: list[RelationshipRule],
    targets: BuildTargets,
    ctx: ExecutionContext,
) -> Plan:
    """Returns an Acero-backed plan that yields the contract schema for the requested target."""
```

### Key library calls

* Build scan nodes:

  * `acero.Declaration("scan", acero.ScanNodeOptions(dataset, columns=..., filter=...))`
* Apply semantic filter:

  * `acero.Declaration("filter", acero.FilterNodeOptions(predicate), inputs=[...])`
* Join rules:

  * `acero.Declaration("hashjoin", acero.HashJoinNodeOptions(...), inputs=[...])`

---

# cpg (final assembly into node/edge tables)

---

## 25) `cpg/kinds.py` — node_kind / edge_kind enums (your canonical vocabulary)

### Purpose

Centralize the enumerations you previously requested (and that everything else references).

### Public API

```python
class NodeKind(str, Enum): ...
class EdgeKind(str, Enum): ...
```

---

## 26) `cpg/schemas.py` — Arrow schemas for CPG outputs

### Purpose

Define the canonical Arrow schemas for:

* nodes
* edges
* node properties (optional normalized table)

### Public API

```python
def node_schema() -> pa.Schema: ...
def edge_schema() -> pa.Schema: ...
def node_props_schema() -> pa.Schema: ...
```

### Key Arrow constraints

* Schema must be enforceable by `table.cast(schema)` (so name/order must match)

---

## 27) `cpg/build_nodes.py` — derive CPG nodes from normalized facts

### Purpose

Create *semantic* nodes from AST/CST/SCIP sources.

### Public API

```python
def build_cpg_nodes(
    *,
    files: pa.Table,
    ast_nodes: pa.Table,
    cst_anchors: pa.Table,
    scip_symbols: pa.Table,
    symtable: pa.Table,
) -> pa.Table:
    """Returns nodes table (NodeId, NodeKind, file_id, span_start/end, properties)."""
```

### Key command patterns

* Do not “loop build” nodes in Python; use Arrow transforms:

  * `Table.group_by([...]).aggregate([...])` for rollups where needed
  * canonical sort at end for determinism

---

## 28) `cpg/build_edges.py` — derive edges (relationships) from normalized facts

### Purpose

Generate edges like:

* AST parent/child
* refers_to (SCIP occurrence → symbol)
* callsite → resolved callee (CST call → SCIP symbol)
* import edges, attribute edges, typing edges, etc.

### Public API

```python
def build_cpg_edges(
    *,
    nodes: pa.Table,
    ast_nodes: pa.Table,
    cst_calls: pa.Table,
    scip_occ: pa.Table,
    scip_symbols: pa.Table,
) -> pa.Table:
    """Returns edges table (EdgeId, src, dst, EdgeKind, properties)."""
```

### Key command patterns

* Span join patterns should compile via `relspec` → `acero hashjoin` (for equality joins on IDs)
* For span overlaps (range joins), use a kernel lane function (since Acero doesn’t natively do interval joins in this DSL)

  * implement overlap join as:

    * sort by file_id + start
    * two-pointer sweep in Python over batches **only if necessary**
    * or pre-bucket spans and do equality joins on buckets (preferred)

---

## 29) `cpg/build_props.py` — normalize “properties” into a struct or side-table

### Purpose

Keep node/edge schemas stable while allowing extensible properties.

### Public API

```python
def attach_node_properties(nodes: pa.Table, *, props: pa.Table) -> pa.Table: ...
def attach_edge_properties(edges: pa.Table, *, props: pa.Table) -> pa.Table: ...
```

### Key Arrow call patterns

* prefer `struct` columns or a normalized `(id, key, value)` property table depending on query needs

---

# hamilton (orchestration without contaminating compute logic)

Hamilton’s job here:

* wire **inputs → outputs** based on targets
* materialize and cache results
* expose telemetry/inspection

---

## 30) `hamilton/driver_factory.py` — build a Driver (modules, config, caching, materializers)

### Purpose

One canonical way to create a Hamilton Driver.

### Public API

```python
def build_driver(*, modules: list[object], cfg: dict, cache_path: str | None) -> driver.Driver: ...
```

### Key library calls

* Materializers pattern:

  * `driver.Builder().with_modules(...).with_materializers(*mats).build()`
* Caching:

  * `.with_cache(path=..., default_behavior=..., log_to_file=True)`
* Split metadata/results stores (production pattern):

  * `.with_cache(metadata_store=..., result_store=...)`

---

## 31) `hamilton/modules/inputs.py` — DAG external inputs (repo_root, config, targets)

### Public API

```python
def build_config(...) -> BuildConfig: ...
def build_targets(...) -> BuildTargets: ...
def execution_context(cfg: BuildConfig) -> ExecutionContext: ...
```

---

## 32) `hamilton/modules/extraction.py` — extraction nodes (return Arrow tables / datasets)

### Rule

These nodes may return:

* an Arrow `pa.Table`, or
* a dataset path, or
* a `Plan` (preferred if large and downstream is relational)

### Public API

```python
def files__table(cfg: BuildConfig) -> pa.Table: ...
def ast_nodes__table(files__table: pa.Table) -> pa.Table: ...
def cst_anchors__table(files__table: pa.Table) -> pa.Table: ...
def scip_symbols__table(cfg: BuildConfig) -> pa.Table: ...
def scip_occurrences__table(cfg: BuildConfig) -> pa.Table: ...
```

---

## 33) `hamilton/modules/normalization.py` — normalized facts

### Public API

```python
def norm_ast_nodes__table(ast_nodes__table: pa.Table, files__table: pa.Table) -> pa.Table: ...
def norm_scip_occ__table(scip_occurrences__table: pa.Table, files__table: pa.Table) -> pa.Table: ...
```

---

## 34) `hamilton/modules/cpg_build.py` — targets produce final outputs

### Public API

```python
def cpg_nodes__table(...) -> pa.Table: ...
def cpg_edges__table(...) -> pa.Table: ...
```

### Key Hamilton execution calls (what an engineer runs)

* `Driver.execute(final_vars, inputs=..., overrides=...)`
* Validate and snapshot graph:

  * `validate_execution(...)`, `export_execution(...)`
* Introspection for registry tooling:

  * `list_available_variables(tag_filter=...)`

---

## 35) `hamilton/modules/outputs.py` — materialization to Parquet/IPC

### Purpose

Attach materializers to save outputs without modifying core functions.

### Key Hamilton call

* `with_materializers(...)` and then `execute([...])` runs saving nodes too

---

# storage / obs / repro

---

## 36) `storage/parquet.py` — standard write/read functions

### Key library calls

* `ds.write_dataset(..., preserve_order=...)`
* Use `Scanner.to_reader()` and persist batches incrementally rather than `to_table()` for large outputs

---

## 37) `storage/ipc.py` — fast local artifacts (optional but great for debugging)

### Key library calls

* `pa.ipc.IpcWriteOptions(..., compression="zstd", use_threads=True, ...)`

---

## 38) `obs/manifest.py` — run manifest (inputs, versions, row counts)

### Purpose

Make every build reproducible and inspectable.

### Key counts

* `Dataset.count_rows(filter=...)`

---

## 39) `obs/stats.py` — structured stats tables

### Purpose

Finalize already emits `(good, errors, alignment, stats)`; this module just standardizes how you persist/merge them.

---

## 40) `obs/repro.py` — minimal repro dataset extraction

### Purpose

Given an errors table, extract the smallest dataset slice to reproduce.

### Key call patterns

* use provenance columns (filename/fragment indices) if you include them in scans (special fields)

---

# graph (optional)

---

## 41) `graph/rustworkx_adapter.py` — convert nodes/edges tables to rustworkx graph

### Purpose

For algorithms that are hard/slow in pure relational form (reachability, dominators, SCC, etc.).

### Key library calls

* Construct graph and run algorithms like `topological_sort`, `is_directed_acyclic_graph`, etc. (and note multigraph caveats)

---

# 2) The “join patterns as logical statements” question: feasibility & how it lands in files

Yes—**within this architecture**, joins can be fully extracted into declarative objects:

* `relspec/model.py` holds `RelationshipRule( left="cst_calls", right="scip_symbols", join=JoinSpec(...))`
* `relspec/compiler.py` compiles that to:

  * scan left plan
  * scan right plan
  * `acero.Declaration("hashjoin", acero.HashJoinNodeOptions(...), inputs=[...])`
* determinism is handled later:

  * because join output order is not predictable
  * finalize applies canonical sort via stable `sort_indices + take` when tier=CANONICAL

This is exactly the “single consolidated plan + centralized rule set + Hamilton used mostly for target selection/materialization” target you described.

---

# 3) Practical “key commands cheat sheet” (by operation)

These are the exact **PyArrow commands + key args** your implementer will keep using:

### Dataset scanning (pushdown)

* `ds.dataset(path, format="parquet")`
* `dataset.scanner(columns=..., filter=..., use_threads=..., batch_size=...)`
* or `ds.Scanner.from_dataset(dataset, columns=..., filter=..., batch_size=..., batch_readahead=..., fragment_readahead=..., use_threads=...)`

### Acero scan + filter + project

* `acero.Declaration("scan", acero.ScanNodeOptions(dataset, columns=[...], filter=...))`
* `acero.Declaration("filter", acero.FilterNodeOptions(predicate), inputs=[...])`
* `acero.Declaration("project", acero.ProjectNodeOptions(expressions=[...], names=[...]), inputs=[...])`

### Joins

* `acero.Declaration("hashjoin", acero.HashJoinNodeOptions(join_type=..., left_keys=..., right_keys=..., left_output=..., right_output=...), inputs=[left, right])`

### Execute plan

* `decl.to_reader(use_threads=True)` or `decl.to_table(use_threads=True)`

### Canonical determinism

* `pc.sort_indices(table, sort_keys=[("col","ascending"), ...])` (stable) + `table.take(indices)`

### Schema inference/alignment

* `pa.unify_schemas([...], promote_options="permissive")`
* `table.cast(schema, safe=...)` (names+order must match)

### LibCST spans/context

* `libcst.MetadataWrapper(module).resolve(ByteSpanPositionProvider)`
* `...resolve(ExpressionContextProvider)`

### SCIP occurrences

* Parse `occurrence.range` as 3- or 4-int list
* Interpret `symbol_roles` bitset

### Hamilton orchestration

* Build:

  * `driver.Builder().with_modules(...).with_materializers(...).build()`
  * `.with_cache(path=..., default_behavior=..., log_to_file=True)`
* Run:

  * `dr.execute(final_vars, inputs=..., overrides=...)`

---

If you want the next step, I can take **this exact file set** and produce a **CPG-focused “relationship rule registry” starter pack**: a concrete list of `RelationshipRule` objects for the most important Python joins (CST callsites ↔ SCIP symbols, AST defs ↔ SCIP definitions, imports ↔ modules, attributes ↔ qualified names), each with the precise join keys and the output column contracts.


According to a document from **January 10, 2026**, here is a **CPG-focused “relationship rule registry” starter pack**: a concrete list of `RelationshipRule` objects covering the four “most important Python joins” you called out—**(1) CST callsites ↔ SCIP symbols, (2) AST defs ↔ SCIP definitions, (3) imports ↔ modules, (4) attributes ↔ qualified names**—with **precise join keys** and **output column contracts** expressed in an implementable way.

This is designed to be **blank-slate**, and to work with the “contracts + Acero hashjoin + kernel-lane post steps” pattern (Acero join via `HashJoinNodeOptions(left_keys/right_keys, left_output/right_output, join_type=...)`) shown in your DSL guide. 

---

## 0) Shared primitives these rules assume

### 0.1 Canonical coordinate system for joins: `(path, byte_start, byte_end)`

All cross-system joins become tractable if you normalize everything to **repo-relative `path`** plus **UTF-8 byte offsets**.

**CST (LibCST):** use `ByteSpanPositionProvider` → it yields `CodeSpan(start_offset_bytes, length_bytes)` and those offsets are explicitly in **bytes** and exclude surrounding whitespace. 

**AST (CPython ast):** `col_offset` / `end_col_offset` are **UTF-8 byte offsets**; end positions can be missing, so plan for `end_*` nullable. 

**SCIP:** occurrence `range` character-unit semantics depend on `Document.position_encoding`; normalize ranges and convert into `(byte_start, byte_end)` during SCIP ingestion.

### 0.2 Stable IDs everywhere (so relationships don’t depend on row order)

For CST-derived entities (calls, defs, import sites, attribute sites), use stable IDs like:

`"{path}:{start_offset_bytes}:{length_bytes}:{kind}"`

This is explicitly recommended as the stable-ID pattern in your LibCST notes.

### 0.3 SCIP SymbolRole bit constants (for filters)

Use the SymbolRole bitmask constants:

* `Definition = 1`
* `Import = 2`
* `WriteAccess = 4`
* `ReadAccess = 8`

These are spelled out in your SCIP overview.

### 0.4 “Contract” objects define output schemas + invariants + dedupe + canonical ordering

Each rule below names an **output contract**. This is aligned to your DSL principle that `Contract` is the single place to declare schema, keys, non-null invariants, dedupe policy, and canonical sort. 

---

## 1) RelationshipRule object shape (minimal, implementable)

This is the *shape* assumed by the registry below (not full code; just the fields you must support):

* `name: str`
* `left_dataset: str`
* `right_dataset: str | None` (for explode-only rules)
* `kind: Literal["hash_join", "interval_align", "explode", "dim_build"]`
* `join_type: str | None` (e.g., `"inner"`, `"left outer"`)
* `left_keys: tuple[str, ...] | None`
* `right_keys: tuple[str, ...] | None`
* `right_filter: str | None` (expressible as an Acero filter expression)
* `output_dataset: str`
* `output_contract_name: str`
* `output_columns: list[str]` (projection list)
* `notes: str` (including any kernel-lane needs)

Where `kind="hash_join"` is compiled into an Acero `hashjoin` node using `HashJoinNodeOptions(left_keys, right_keys, left_output, right_output, join_type=...)`. 

---

## 2) Starter pack: concrete RelationshipRule list

Below is the registry. I’m including a few “bridge rules” because **AST defs ↔ SCIP definitions** is most reliable as a 2-hop mapping (AST → CST def header, then CST def name → SCIP definition occurrence), given AST name-token spans are not first-class.

### Group A — CST callsites ↔ SCIP symbols

---

### **RR_CALLSITE__SCIP_SYMBOL_BY_CALLEE_SPAN_EXACT**

**Goal:** link each CST callsite’s callee token span to the SCIP occurrence covering that same span.

* **kind:** `hash_join`
* **left_dataset:** `cst_callsites`
* **right_dataset:** `scip_occurrences`
* **join_type:** `"inner"`
* **left_keys:** `("path", "callee_bstart", "callee_bend")`
* **right_keys:** `("path", "occ_bstart", "occ_bend")`
* **right_filter (required):**

  * exclude definitions: `(symbol_roles & 1) == 0`
  * prefer references/reads: `((symbol_roles & 8) != 0) OR (symbol_roles == 0)`
  * (SymbolRole bits per SCIP overview)
* **output_dataset:** `rel_callsite_symbol`
* **output_contract:** `rel_callsite_symbol_v1`
* **output columns (projection):**

  * from left: `call_id, path, call_bstart, call_bend, callee_bstart, callee_bend, callee_syntax, qname_candidates`
  * from right: `symbol, symbol_roles`
  * computed: `resolution_method="SPAN_EXACT"`, `confidence=1.0`

**Output contract: `rel_callsite_symbol_v1`**

* **Schema**

  * `call_id: string` (non-null)
  * `path: string` (non-null)
  * `call_bstart: int32` (non-null)
  * `call_bend: int32` (non-null)
  * `callee_bstart: int32` (non-null)
  * `callee_bend: int32` (non-null)
  * `symbol: string` (non-null)
  * `symbol_roles: int32` (non-null)
  * `resolution_method: string` (non-null)
  * `confidence: float32` (non-null)
  * `callee_syntax: string` (nullable)
  * `qname_candidates: list<struct<name: string, source: string>>` (nullable)

    * (QualifiedNameProvider yields a *set* of `(name, source)` candidates; store as list and explode later if needed.) 
* **key_fields:** `(call_id, symbol)`
* **required_non_null:** `(call_id, path, callee_bstart, callee_bend, symbol, resolution_method, confidence)`
* **dedupe:** keys `(call_id, symbol)`, tie-breakers `(confidence DESC)`, strategy `KEEP_FIRST_AFTER_SORT` 
* **canonical_sort:** `(path ASC, call_bstart ASC, call_bend ASC, symbol ASC)`

**Notes**

* This rule assumes you extracted `callee_bstart/callee_bend` using LibCST `ByteSpanPositionProvider` (bytes, whitespace excluded). 

---

### **RR_CALLSITE__SCIP_SYMBOL_FALLBACK_CONTAINED_BEST**

**Goal:** handle the real-world case where span-exact doesn’t match because the “callee span” and SCIP occurrence span differ slightly (attribute chains, parentheses, etc.).

* **kind:** `interval_align` (kernel lane)
* **left_dataset:** `cst_callsites`
* **right_dataset:** `scip_occurrences`
* **predicate (not hash-joinable):**

  * `path` equality AND
  * `occ_bstart >= callee_bstart AND occ_bend <= callee_bend`
  * choose the “best” candidate per call as:

    1. smallest `(occ_bend - occ_bstart)`
    2. then prefer `ReadAccess` over none
* **output_dataset:** `rel_callsite_symbol`
* **output_contract:** `rel_callsite_symbol_v1` (same as above)
* **computed:** `resolution_method="CONTAINED_BEST"`, `confidence=0.6`

**Notes**

* This runs in kernel-lane because Acero’s built-in join is hashjoin on equality keys (per your DSL guide’s integrated scan/filter/join/project pattern). 

---

### Group B — AST defs ↔ SCIP definitions (via CST def alignment)

---

### **RR_AST_DEF__CST_DEF_BY_STMT_BSTART**

**Goal:** align AST definition nodes to CST definition nodes using **statement start byte offset** (so AST can “inherit” CST name-token spans and stable def IDs).

* **kind:** `hash_join`
* **left_dataset:** `ast_defs`
* **right_dataset:** `cst_defs`
* **join_type:** `"inner"`
* **left_keys:** `("path", "def_kind", "stmt_bstart")`
* **right_keys:** `("path", "def_kind", "stmt_bstart")`
* **output_dataset:** `rel_astdef_cstdef`
* **output_contract:** `rel_astdef_cstdef_v1`
* **output columns:**

  * `ast_def_id, cst_def_id, path, def_kind, stmt_bstart`
  * computed: `resolution_method="STMT_BSTART_EXACT"`, `confidence=1.0`

**Why byte offsets are valid here**

* AST `col_offset` values are UTF-8 byte offsets, so `stmt_bstart = line_start_byte_offset + col_offset` is in the same “byte space” as LibCST’s `ByteSpanPositionProvider`. 

**Output contract: `rel_astdef_cstdef_v1`**

* **Schema**

  * `ast_def_id: string` (non-null)
  * `cst_def_id: string` (non-null)
  * `path: string` (non-null)
  * `def_kind: string` (non-null)  *(e.g., "FUNCTION" | "ASYNC_FUNCTION" | "CLASS")*
  * `stmt_bstart: int32` (non-null)
  * `resolution_method: string` (non-null)
  * `confidence: float32` (non-null)
* **key_fields:** `(ast_def_id, cst_def_id)`
* **required_non_null:** all except none
* **canonical_sort:** `(path ASC, stmt_bstart ASC, def_kind ASC)`

---

### **RR_CST_DEFNAME__SCIP_DEFINITION_OCCURRENCE_EXACT**

**Goal:** map a CST definition’s **name-token span** to the SCIP occurrence that has the `Definition` role.

* **kind:** `hash_join`
* **left_dataset:** `cst_defs`
* **right_dataset:** `scip_occurrences`
* **join_type:** `"inner"`
* **left_keys:** `("path", "name_bstart", "name_bend")`
* **right_keys:** `("path", "occ_bstart", "occ_bend")`
* **right_filter:** `(symbol_roles & 1) != 0` (Definition)
* **output_dataset:** `rel_cstdef_symbol`
* **output_contract:** `rel_cstdef_symbol_v1`
* **output columns:**

  * from left: `cst_def_id, path, def_kind, name_bstart, name_bend`
  * from right: `symbol, symbol_roles`
  * computed: `resolution_method="DEFNAME_SPAN_EXACT"`, `confidence=1.0`

**Output contract: `rel_cstdef_symbol_v1`**

* **Schema**

  * `cst_def_id: string` (non-null)
  * `path: string` (non-null)
  * `def_kind: string` (non-null)
  * `name_bstart: int32` (non-null)
  * `name_bend: int32` (non-null)
  * `symbol: string` (non-null)
  * `symbol_roles: int32` (non-null)
  * `resolution_method: string` (non-null)
  * `confidence: float32` (non-null)
* **key_fields:** `(cst_def_id, symbol)`
* **required_non_null:** all
* **dedupe:** keys `(cst_def_id, symbol)`, tie-breakers `(confidence DESC)`, `KEEP_FIRST_AFTER_SORT` 
* **canonical_sort:** `(path ASC, name_bstart ASC, name_bend ASC, symbol ASC)`

---

### **RR_AST_DEF__SCIP_DEFINITION_SYMBOL_VIA_CST**

**Goal:** produce the direct “AST defs ↔ SCIP definitions” mapping your prompt asked for.

* **kind:** `hash_join`
* **left_dataset:** `rel_astdef_cstdef` *(output of RR_AST_DEF__CST_DEF_BY_STMT_BSTART)*
* **right_dataset:** `rel_cstdef_symbol` *(output of RR_CST_DEFNAME__SCIP_DEFINITION_OCCURRENCE_EXACT)*
* **join_type:** `"inner"`
* **left_keys:** `("cst_def_id",)`
* **right_keys:** `("cst_def_id",)`
* **output_dataset:** `rel_astdef_symbol`
* **output_contract:** `rel_astdef_symbol_v1`
* **output columns:**

  * `ast_def_id, cst_def_id, path, def_kind, symbol, symbol_roles`
  * computed:

    * `resolution_method="VIA_CST_DEFNAME_SPAN"`
    * `confidence = min(left.confidence, right.confidence)` *(or just 1.0)*

**Output contract: `rel_astdef_symbol_v1`**

* **Schema**

  * `ast_def_id: string` (non-null)
  * `cst_def_id: string` (non-null)
  * `path: string` (non-null)
  * `def_kind: string` (non-null)
  * `symbol: string` (non-null)
  * `symbol_roles: int32` (non-null)
  * `resolution_method: string` (non-null)
  * `confidence: float32` (non-null)
* **key_fields:** `(ast_def_id, symbol)`
* **required_non_null:** all
* **canonical_sort:** `(path ASC, ast_def_id ASC, symbol ASC)`

---

### Group C — Imports ↔ modules

---

### **RR_IMPORT_ALIAS__MODULE_BY_ABSOLUTE_FQN**

**Goal:** connect an import site to an internal module (if present in the repo manifest).

* **kind:** `hash_join`
* **left_dataset:** `cst_import_aliases`
* **right_dataset:** `module_manifest`
* **join_type:** `"left outer"` *(so external imports remain as “unresolved internal module”; you may still create an external module node later)*
* **left_keys:** `("target_module_fqn",)`
* **right_keys:** `("module_fqn",)`
* **output_dataset:** `rel_import_module`
* **output_contract:** `rel_import_module_v1`
* **output columns:**

  * from left: `import_alias_id, path, src_module_id, src_module_fqn, import_kind, target_module_fqn, imported_name, alias, is_star, relative_level`
  * from right: `module_id as dst_module_id, path as dst_path`
  * computed: `resolution_method="MODULE_FQN_EXACT"`, `confidence=1.0 if dst_module_id not null else 0.0`

**How to compute `target_module_fqn`**

* Resolve relative imports using LibCST helpers like `get_absolute_module_for_import(current_module, import_node)`; this is explicitly called out as the recommended approach in your LibCST notes.

**Output contract: `rel_import_module_v1`**

* **Schema**

  * `import_alias_id: string` (non-null)
  * `path: string` (non-null)
  * `src_module_id: string` (non-null)
  * `src_module_fqn: string` (non-null)
  * `import_kind: string` (non-null) *(IMPORT | IMPORT_FROM)*
  * `target_module_fqn: string` (non-null)
  * `dst_module_id: string` (nullable) *(null => external module or missing manifest)*
  * `dst_path: string` (nullable)
  * `imported_name: string` (nullable)
  * `alias: string` (nullable)
  * `is_star: bool` (non-null)
  * `relative_level: int32` (non-null)
  * `resolution_method: string` (non-null)
  * `confidence: float32` (non-null)
* **key_fields:** `(import_alias_id, target_module_fqn)`
* **required_non_null:** `(import_alias_id, path, src_module_id, src_module_fqn, import_kind, target_module_fqn, resolution_method, confidence)`
* **canonical_sort:** `(path ASC, import_alias_id ASC, target_module_fqn ASC)`

---

### **RR_IMPORT_ALIAS_NAME__SCIP_IMPORT_OCCURRENCE_EXACT**

**Goal:** connect import alias token spans to SCIP occurrences with the `Import` role.

* **kind:** `hash_join`
* **left_dataset:** `cst_import_aliases`
* **right_dataset:** `scip_occurrences`
* **join_type:** `"inner"`
* **left_keys:** `("path", "import_name_bstart", "import_name_bend")`
* **right_keys:** `("path", "occ_bstart", "occ_bend")`
* **right_filter:** `(symbol_roles & 2) != 0` (Import)
* **output_dataset:** `rel_import_symbol`
* **output_contract:** `rel_import_symbol_v1`
* **output columns:** `import_alias_id, path, symbol, symbol_roles, resolution_method="IMPORTNAME_SPAN_EXACT", confidence=1.0`

**Output contract: `rel_import_symbol_v1`**

* **Schema**

  * `import_alias_id: string` (non-null)
  * `path: string` (non-null)
  * `symbol: string` (non-null)
  * `symbol_roles: int32` (non-null)
  * `resolution_method: string` (non-null)
  * `confidence: float32` (non-null)
* **key_fields:** `(import_alias_id, symbol)`
* **required_non_null:** all
* **canonical_sort:** `(path ASC, import_alias_id ASC, symbol ASC)`

---

### Group D — Attributes ↔ qualified names

This uses LibCST’s QualifiedNameProvider, which can return a **set of possible qualified names** with `name` and `source`, i.e., ambiguity is a first-class output. 

---

### **RR_ATTRIBUTE__QUALIFIED_NAME_CANDIDATES_EXPLODE**

**Goal:** convert `qname_candidates: list<struct<name, source>>` into one row per `(attribute_site, qualified_name_candidate)`.

* **kind:** `explode` (kernel lane)
* **left_dataset:** `cst_attribute_accesses`
* **right_dataset:** `None`
* **output_dataset:** `rel_attribute_qname_candidates`
* **output_contract:** `rel_attribute_qname_candidates_v1`
* **explode spec:**

  * explode column: `qname_candidates`
  * output columns per exploded element:

    * `qname = qname_candidates.name`
    * `qname_source = qname_candidates.source`
  * (QualifiedNameProvider produces a set; store as list and explode deterministically by sorting by `(qname_source, qname)` before explode.) 

**Output contract: `rel_attribute_qname_candidates_v1`**

* **Schema**

  * `attr_id: string` (non-null)
  * `path: string` (non-null)
  * `expr_bstart: int32` (non-null)
  * `expr_bend: int32` (non-null)
  * `attr_name_bstart: int32` (non-null)
  * `attr_name_bend: int32` (non-null)
  * `qname: string` (non-null)
  * `qname_source: string` (non-null)
  * `resolution_method: string` (non-null) *(="QNP_EXPLODE")*
  * `confidence: float32` (non-null) *(optional: source-weighted)*
* **key_fields:** `(attr_id, qname, qname_source)`
* **required_non_null:** all
* **canonical_sort:** `(path ASC, attr_name_bstart ASC, attr_name_bend ASC, qname_source ASC, qname ASC)`

---

### **RR_ATTRIBUTE_QNAME__QNAME_DIM_LOOKUP**

**Goal:** link attribute sites to a normalized qualified-name dimension (so graph nodes for qualified names are stable and deduped).

* **kind:** `hash_join`
* **left_dataset:** `rel_attribute_qname_candidates`
* **right_dataset:** `dim_qualified_names`
* **join_type:** `"inner"`
* **left_keys:** `("qname",)`
* **right_keys:** `("qname",)`
* **output_dataset:** `rel_attribute_qname`
* **output_contract:** `rel_attribute_qname_v1`
* **output columns:**

  * from left: `attr_id, path, expr_bstart, expr_bend, attr_name_bstart, attr_name_bend, qname, qname_source, confidence`
  * from right: `qname_id`

**Output contract: `rel_attribute_qname_v1`**

* **Schema**

  * `attr_id: string` (non-null)
  * `path: string` (non-null)
  * `expr_bstart: int32` (non-null)
  * `expr_bend: int32` (non-null)
  * `attr_name_bstart: int32` (non-null)
  * `attr_name_bend: int32` (non-null)
  * `qname_id: string` (non-null)
  * `qname: string` (non-null)
  * `qname_source: string` (non-null)
  * `confidence: float32` (non-null)
* **key_fields:** `(attr_id, qname_id)`
* **required_non_null:** all
* **canonical_sort:** `(path ASC, attr_id ASC, qname_id ASC)`

---

## 3) One required “dim build” rule to make the above work

### **RR_DIM_QUALIFIED_NAMES_BUILD**

**Goal:** build `dim_qualified_names` as the distinct set of qualified names referenced by attributes (and later callsites, imports, etc.).

* **kind:** `dim_build` (kernel lane)
* **input_dataset:** `rel_attribute_qname_candidates`
* **grouping:** distinct `qname`
* **output_dataset:** `dim_qualified_names`
* **output_contract:** `dim_qualified_names_v1`
* **computed fields:**

  * `qname_id = stable_hash(qname)` *(or `"qname:" + qname` if you want human-readable IDs)*

**Output contract: `dim_qualified_names_v1`**

* **Schema**

  * `qname_id: string` (non-null)
  * `qname: string` (non-null)
* **key_fields:** `(qname_id,)`
* **required_non_null:** all
* **canonical_sort:** `(qname ASC)`

---

## 4) Practical notes on feasibility + why these keys are “best starter defaults”

* The “exact span” joins are feasible because:

  * LibCST can produce **byte-accurate spans** (`start_offset_bytes`, `length_bytes`). 
  * Python AST uses **byte offsets** for columns. 
  * SCIP ranges can be normalized and interpreted under `Document.position_encoding`, then converted into bytes once per file during ingestion.

* You *will* still want a kernel-lane fallback for callsites in real code, because “callee expression span” doesn’t always line up perfectly with “occurrence span.” That’s why I included `RR_CALLSITE__SCIP_SYMBOL_FALLBACK_CONTAINED_BEST`.

* Treat QualifiedNameProvider as producing **sets** (ambiguity is normal), not a single string. That’s why the attribute→qualified-name relationship is modeled as explode + dimension lookup. 

---

If you want, I can now extend this registry starter pack with **the analogous rules for**:

* **CST name refs ↔ SCIP occurrences** (general name resolution),
* **CST callsites ↔ qualified names** (QNP candidates for call callee),
* **SCIP symbol ↔ module manifest parsing** (to attach `symbol → defining module/file` edges),
* plus a “winner selection” policy (score-based vs keep-first-after-sort) consistent with your DSL’s dedupe strategies. 


According to a document from **January 10, 2026** (your project docs snapshot), here is the **extended RelationshipRule registry starter pack** covering:

1. **CST name refs ↔ SCIP occurrences** (general name resolution)
2. **CST callsites ↔ qualified names** (QualifiedNameProvider candidates for callee)
3. **SCIP symbol ↔ module manifest** (symbol → defining module/file edges)
4. A standardized **winner selection policy** consistent with your DSL’s sanctioned dedupe strategies (`KEEP_FIRST_AFTER_SORT`, `KEEP_BEST_BY_SCORE`, `COLLAPSE_LIST`, `KEEP_ARBITRARY`).

I’m going to keep this in the same “registry spec” form you’ve been using: **each rule specifies join keys, filters, output dataset, and output contract** (schema + keys + dedupe + canonical sort).

---

## A) Assumed input fact datasets (minimal columns)

These rules assume you already have (or will create) the following input “fact” datasets:

### `cst_name_refs` (LibCST reference index)

From LibCST you should emit a **reference index** (“variable accesses + referents”) with at least:

* `name_ref_id: string` (stable ID = `{path}:{byte_start}:{byte_len}:{kind}` recommended)
* `path: string` (repo-relative POSIX)
* `name: string`
* `bstart: int32`, `bend: int32` (byte span from `ByteSpanPositionProvider`)
* `expr_ctx: string?` (LOAD/STORE/DEL when available; note contexts can be missing)

### `cst_callsites` (LibCST call-site index)

LibCST call-site index should contain **callee candidates (qualified names) + arg-shape** with at least:

* `call_id: string`
* `path: string`
* `call_bstart, call_bend: int32`
* `callee_bstart, callee_bend: int32` (span of callee expression)
* `callee_dotted: string?` (cheap syntactic dotted string; `Call.func` often `Name|Attribute`)
* `callee_qnames: list<struct<name: string, source: string>>`

  * because `QualifiedNameProvider` returns a **set** of `(name, source)` candidates; store as list `{name, source}` (dedup+sorted)

### `scip_occurrences`

From SCIP each occurrence includes:

* `symbol: string`, `symbol_roles: int` bitmask, and `range` location

After normalizing ranges to byte spans, store at least:

* `occ_id: string`
* `path: string` (document relative path)
* `occ_bstart, occ_bend: int32`
* `symbol: string`
* `symbol_roles: int32`

  * where `Definition=1`, `Import=2`, `WriteAccess=4`, `ReadAccess=8` (etc.)

### `scip_symbols` (optional but recommended dimension)

* `symbol: string` (PK)
* symbol metadata fields if available (display name, doc, relationships, etc.)

### `module_manifest` (from LibCST FullyQualifiedNameProvider)

You want per-file module identity:

* `path: string`
* `module_fqn: string` (repo-absolute; FullyQualifiedNameProvider)
* `module_id: string` (stable hash of `module_fqn`)
* `file_sha256: string?` (helpful for incremental builds)

LibCST explicitly calls out `module_fqn` as “optional but high-value” and resolved by FullyQualifiedNameProvider on the `Module` node.

---

## B) New contracts used in these additional rules

I’m defining these contract names (you’d implement them as `Contract` objects per your DSL guidance) and specifying the minimal schema + keys + dedupe + canonical ordering.

> Reminder from the DSL guide: contracts are **immutable/versioned** and each contract declares determinism + dedupe policy.

### Contract: `rel_name_symbol_v1`

Maps a CST `Name` reference to a SCIP occurrence/symbol.

**Schema**

* `name_ref_id: string` (non-null)
* `path: string` (non-null)
* `bstart: int32` (non-null)
* `bend: int32` (non-null)
* `name: string` (non-null)
* `expr_ctx: string?`
* `occ_id: string` (non-null)
* `symbol: string` (non-null)
* `symbol_roles: int32` (non-null)
* `resolution_method: string` (non-null)
* `confidence: float32` (non-null)
* `score: float32` (non-null)  *(for winner selection; see policy section)*

**key_fields**

* `(name_ref_id, symbol)` (ambiguity preserved)

**dedupe**

* strategy: `KEEP_FIRST_AFTER_SORT`
* keys: `(name_ref_id, symbol)`
* tie_breakers: `(score DESC, occ_id ASC)` (stable total order)

**canonical_sort**

* `(path ASC, bstart ASC, bend ASC, name_ref_id ASC, symbol ASC)`

---

### Contract: `rel_name_symbol_winner_v1`

Single “best” symbol per `name_ref_id` (optional derived target).

**Schema**

* `name_ref_id, path, bstart, bend, name, expr_ctx`
* `symbol, symbol_roles`
* `resolution_method, confidence, score`

**key_fields**

* `(name_ref_id,)` (one row per name ref)

**dedupe**

* strategy: `KEEP_BEST_BY_SCORE` (defined below)

**canonical_sort**

* `(path ASC, bstart ASC, bend ASC, name_ref_id ASC)`

---

### Contract: `rel_callsite_qname_candidates_v1`

Callsite → QualifiedNameProvider candidate set (ambiguity preserved).

**Schema**

* `call_id: string` (non-null)
* `path: string` (non-null)
* `call_bstart, call_bend: int32` (non-null)
* `callee_bstart, callee_bend: int32` (non-null)
* `callee_dotted: string?`
* `qname: string` (non-null)
* `qname_source: string` (non-null)
* `resolution_method: string` (non-null) *(="QNP_CALLEE")*
* `confidence: float32` (non-null)
* `score: float32` (non-null)

**key_fields**

* `(call_id, qname, qname_source)`

**dedupe**

* `KEEP_FIRST_AFTER_SORT` on `(call_id,qname,qname_source)` (dedup extraction noise)

**canonical_sort**

* `(path ASC, call_bstart ASC, call_bend ASC, qname_source ASC, qname ASC)`

---

### Contract: `rel_callsite_qname_winner_v1`

Optional derived: pick one best qualified name candidate for callsite.

**key_fields**

* `(call_id,)`

**dedupe**

* `KEEP_BEST_BY_SCORE`

---

### Contract: `rel_symbol_defsite_v1`

Symbol → defining file/span derived from SCIP definition occurrences.

**Schema**

* `symbol: string` (non-null)
* `path: string` (non-null)
* `def_occ_id: string` (non-null)
* `def_bstart, def_bend: int32` (non-null)
* `resolution_method: string` (non-null) *(="SCIP_DEFINITION_OCC")*
* `confidence: float32` (non-null)
* `score: float32` (non-null)

**key_fields**

* `(symbol, path, def_bstart, def_bend)` (preserve multiplicity if multiple defsites exist)

**dedupe**

* `KEEP_FIRST_AFTER_SORT` on exact duplicates

**canonical_sort**

* `(symbol ASC, path ASC, def_bstart ASC, def_bend ASC)`

---

### Contract: `rel_symbol_module_v1`

Symbol → defining module/file edges (module_manifest join).

**Schema**

* `symbol: string` (non-null)
* `path: string` (non-null)
* `module_fqn: string` (nullable) *(null for non-importable or missing)*
* `module_id: string` (nullable)
* `resolution_method: string` (non-null)
* `confidence: float32` (non-null)
* `score: float32` (non-null)

**key_fields**

* `(symbol,)` *(for “winner” flavor)* or `(symbol, module_id)` for ambiguity-preserving flavor

---

## C) Additional RelationshipRule objects

### Group 1 — CST name refs ↔ SCIP occurrences (general name resolution)

#### RR_NAME_REF__SCIP_OCCURRENCE_SPAN_EXACT

**Goal:** map each CST `Name` token span to the SCIP occurrence at the same span.

* **kind:** `hash_join`
* **left_dataset:** `cst_name_refs`
* **right_dataset:** `scip_occurrences`
* **join_type:** `"inner"` *(match-only; see optional unresolved rule below)*
* **left_keys:** `("path", "bstart", "bend")`
* **right_keys:** `("path", "occ_bstart", "occ_bend")`
* **right_filter:** none (include defs/imports/reads/writes; `symbol_roles` carries meaning)
* **output_dataset:** `rel_name_symbol`
* **output_contract:** `rel_name_symbol_v1`
* **output_columns:**

  * from left: `name_ref_id, path, bstart, bend, name, expr_ctx`
  * from right: `occ_id, symbol, symbol_roles`
  * computed:

    * `resolution_method="SPAN_EXACT"`
    * `confidence=1.0`
    * `score=1.0` *(base score; policy refines)*

**Notes**

* SCIP defines vs references using `symbol_roles & 1` (Definition bit) etc.

---

#### RR_NAME_REF__SCIP_OCCURRENCE_UNRESOLVED (optional but very useful)

**Goal:** produce a debugging stream of CST names that didn’t map to any SCIP occurrence.

* **kind:** `hash_join`
* **join_type:** `"left anti"` (or `"left outer"` then filter where `symbol is null`)
* **left_keys/right_keys:** same as above
* **output_dataset:** `rel_name_unresolved`
* **output_contract:** `rel_name_unresolved_v1` (simple contract: `name_ref_id, path, bstart, bend, name, expr_ctx, reason`)

This stream is extremely helpful for tolerant mode observability (and for measuring SCIP coverage).

---

#### RR_NAME_REF__SYMBOLINFO_ENRICH (optional)

**Goal:** enrich name mappings with symbol metadata (docstring, relationships, etc.) when present.

* **kind:** `hash_join`
* **left_dataset:** `rel_name_symbol`
* **right_dataset:** `scip_symbols`
* **join_type:** `"left outer"`
* **left_keys:** `("symbol",)`
* **right_keys:** `("symbol",)`
* **output_dataset:** `rel_name_symbol_enriched`
* **output_contract:** `rel_name_symbol_enriched_v1` (extends v1 with metadata fields)

SymbolInformation lookup by symbol is explicitly described as a consumer pattern.

---

### Group 2 — CST callsites ↔ qualified names (QNP candidates for callee)

LibCST’s indexing guidance explicitly calls for a call-site index with **callee candidates (qualified names)** and emphasizes that qualified-name resolution is a **set** (ambiguity-first).

#### RR_CALLSITE__QNAME_CANDIDATES_EXPLODE

**Goal:** explode `callee_qnames[]` into one row per candidate.

* **kind:** `explode` (kernel lane)
* **left_dataset:** `cst_callsites`
* **explode_column:** `callee_qnames`
* **output_dataset:** `rel_callsite_qname_candidates`
* **output_contract:** `rel_callsite_qname_candidates_v1`
* **output_columns:**

  * `call_id, path, call_bstart, call_bend, callee_bstart, callee_bend, callee_dotted`
  * plus exploded:

    * `qname = callee_qnames.name`
    * `qname_source = callee_qnames.source`
  * computed:

    * `resolution_method="QNP_CALLEE"`
    * `confidence=0.5` *(base; policy refines by source)*
    * `score=<computed>` (see policy)

**Notes**

* QualifiedNameProvider returns a **set** of `(name, source)`; store as list, dedup+sorted before exploding so results are deterministic.

Also: keep `callee_dotted` as a cheap syntactic name layer (Call.func often Name|Attribute) to help debugging and fallbacks.

---

#### RR_CALLSITE_QNAME__DIM_QUALIFIED_NAMES_BUILD

**Goal:** build the dimension table of qualified names referenced by callsites.

* **kind:** `dim_build` (kernel lane)
* **input_dataset:** `rel_callsite_qname_candidates`
* **distinct_on:** `qname`
* **output_dataset:** `dim_qualified_names`
* **output_contract:** `dim_qualified_names_v1`
* **computed:**

  * `qname_id = stable_hash(qname)` or `"qname:" + qname`

This mirrors the “qualified-name value encoding” guidance (store names + sources; treat ambiguity as first-class).

---

#### RR_CALLSITE_QNAME__DIM_LOOKUP

**Goal:** attach `qname_id` so CPG can link callsites to qualified-name nodes (or to symbol candidates later).

* **kind:** `hash_join`
* **left_dataset:** `rel_callsite_qname_candidates`
* **right_dataset:** `dim_qualified_names`
* **join_type:** `"inner"`
* **left_keys:** `("qname",)`
* **right_keys:** `("qname",)`
* **output_dataset:** `rel_callsite_qname`
* **output_contract:** `rel_callsite_qname_v1` (same as candidates, plus `qname_id`)

---

#### RR_CALLSITE_QNAME__WINNER (optional)

**Goal:** choose one “best” qualified name per callsite for downstream heuristics (not replacing SCIP-resolved symbols).

* **kind:** `winner_select` (kernel lane; implemented via your dedupe policy)
* **input_dataset:** `rel_callsite_qname`
* **output_dataset:** `rel_callsite_qname_winner`
* **output_contract:** `rel_callsite_qname_winner_v1`
* **winner_policy:** `KEEP_BEST_BY_SCORE` (see policy section)

---

### Group 3 — SCIP symbol ↔ module manifest parsing (symbol → defining module/file edges)

This is best done via **definition occurrences**, not by trying to parse symbol strings. Your SCIP overview shows how definition occurrences identify where symbols are defined and includes `doc.relative_path` (your `path`) for each definition.

#### RR_SCIP_SYMBOL__DEFSITE_FROM_DEFINITION_OCCS

**Goal:** create “symbol defined in file/span” facts.

* **kind:** `filter_project` (plan lane; no join required)
* **input_dataset:** `scip_occurrences`
* **filter:** `(symbol_roles & 1) != 0` (Definition)
* **output_dataset:** `rel_symbol_defsite`
* **output_contract:** `rel_symbol_defsite_v1`
* **output_columns:**

  * `symbol`
  * `path`
  * `def_occ_id = occ_id`
  * `def_bstart = occ_bstart`, `def_bend = occ_bend`
  * computed:

    * `resolution_method="SCIP_DEFINITION_OCC"`
    * `confidence=1.0`
    * `score=1.0`

**Optional enhancement**
If SCIP gives `enclosing_range`, you can store the “definition body span” for extraction (doc mentions preferring `occ.enclosing_range` when present).

---

#### RR_SYMBOL_DEFSITE__MODULE_MANIFEST_JOIN

**Goal:** attach module identity to each symbol based on the file that contains its definition.

* **kind:** `hash_join`
* **left_dataset:** `rel_symbol_defsite`
* **right_dataset:** `module_manifest`
* **join_type:** `"left outer"` *(symbols might be in files that aren’t importable modules)*
* **left_keys:** `("path",)`
* **right_keys:** `("path",)`
* **output_dataset:** `rel_symbol_module`
* **output_contract:** `rel_symbol_module_v1`
* **output_columns:**

  * `symbol`
  * `path`
  * `module_fqn`, `module_id`
  * computed:

    * `resolution_method="DEF_PATH_TO_MODULE_FQN"`
    * `confidence = 1.0 if module_id not null else 0.3`
    * `score = 1.0 if module_id not null else 0.3`

**Justification**
LibCST guidance calls out `module_fqn` as “optional but high-value” and derived via FullyQualifiedNameProvider on the `Module` node.

---

#### RR_SYMBOL__MODULE_WINNER (optional)

If `rel_symbol_module` can contain multiple candidates (rare, but possible under multiple manifests or overlays), produce a winner.

* **kind:** `winner_select` (kernel lane)
* **input_dataset:** `rel_symbol_module`
* **output_contract:** `rel_symbol_module_winner_v1`
* **key_fields:** `(symbol,)`
* **policy:** `KEEP_BEST_BY_SCORE`

---

## D) Winner selection policy (score-based vs keep-first-after-sort)

Your Arrow/Acero DSL guide is very explicit: dedupe is **keys + tie-breakers + winner strategy**, and only a small set of strategies should exist. 

### D.1 Sanctioned strategies

Use exactly these (as the DSL guide recommends):

* `KEEP_FIRST_AFTER_SORT` (Tier A / canonical): canonical sort then first-wins
* `KEEP_BEST_BY_SCORE` (Tier A/B): compute score then choose max/min
* `COLLAPSE_LIST` (Tier A/B): preserve multiplicity as an explicit list
* `KEEP_ARBITRARY` (Tier C): allowed only for best-effort outputs

### D.2 Canonical (Tier A) algorithmic requirements

When determinism tier is CANONICAL:

* You must create a stable total order using **stable** `sort_indices + take` (canonical sort)
* If you use “first”/“last” aggregation, it is **order-dependent**, so you must define order first
* Grouped output ordering can drift under threading, so either:

  * run group_by deterministically, or
  * canonical-sort again after aggregation

### D.3 Recommended scoring model (stable, explainable)

To enable `KEEP_BEST_BY_SCORE` consistently, standardize a score computation based only on stable fields:

#### Score components (examples)

**(a) Resolution method weight**

* `SPAN_EXACT (SCIP span match)`: `+1.00`
* `CONTAINED_BEST`: `+0.60`
* `QNP_CALLEE`: `+0.50`
* `QNP_REFERENCE`: `+0.45`
* `SCOPE_PROVIDER`: `+0.55` *(if you later add ScopeProvider referents)*

**(b) SCIP role preference**
If an occurrence has a role:

* Definition bit → `+0.10`
* Import bit → `+0.05`
* Read/Write bits → `+0.02` (or none)

Role bits and their meaning are described in your SCIP overview.

**(c) QualifiedNameProvider source preference**
QualifiedNameProvider returns `(name, source)` and source indicates import/local/builtin semantics; encode source weight:

* local: `+0.20`
* import: `+0.15`
* builtin: `+0.05`
* unknown: `+0.00`

(You store `{name, source}` candidates as suggested.)

#### Output column

Every relationship output that might need winner selection should include:

* `score: float32`
* `confidence: float32` (human-facing; can mirror score or be simpler)

### D.4 How to implement `KEEP_BEST_BY_SCORE` deterministically

Even though the guide characterizes it as “order-independent choice,” the easiest Tier-A implementation is:

1. canonical sort by `(keys..., score DESC, tie_breakers...)`
2. `KEEP_FIRST_AFTER_SORT` on `keys`

This yields the same result as “max score” *if* your score+tie-breakers define a total order. It also leverages the guide’s canonical primitives directly.

If you want a purely “max(score)” semantics, the alternative is:

* group_by(keys).aggregate(max(score))
* filter rows where score==max
* if still multiple, apply tie-breaker canonical sort and keep first

Either is fine; the first is simpler and matches the DSL’s “canonical sort then controlled first-wins” approach.

### D.5 When to use which strategy (recommended defaults)

* **Ambiguity-preserving relationship tables** (edges/candidates):

  * keys include candidate (e.g., `(call_id, qname, source)`), so you don’t collapse meaning
  * strategy: `KEEP_FIRST_AFTER_SORT` just to remove true duplicates
  * this matches the “ambiguity is first-class” stance

* **Winner tables** (optional downstream conveniences):

  * keys are just the source site (e.g., `(call_id,)` or `(name_ref_id,)`)
  * strategy: `KEEP_BEST_BY_SCORE` with documented scoring

* **Never** use `KEEP_ARBITRARY` for contract outputs meant for caching/snapshots; reserve it for best-effort exploratory tables.

---

## E) Summary of the “additional rules” you requested

You now have added:

1. **CST name refs ↔ SCIP occurrences**

   * `RR_NAME_REF__SCIP_OCCURRENCE_SPAN_EXACT` (+ optional unresolved/enrich)

2. **CST callsites ↔ qualified names**

   * `RR_CALLSITE__QNAME_CANDIDATES_EXPLODE`
   * `RR_CALLSITE_QNAME__DIM_QUALIFIED_NAMES_BUILD`
   * `RR_CALLSITE_QNAME__DIM_LOOKUP` (+ optional winner)

3. **SCIP symbol ↔ module manifest parsing**

   * `RR_SCIP_SYMBOL__DEFSITE_FROM_DEFINITION_OCCS`
   * `RR_SYMBOL_DEFSITE__MODULE_MANIFEST_JOIN` (+ optional symbol-module winner)

4. **Winner selection policy** consistent with the DSL guide’s sanctioned strategies and canonical determinism rules.

---

If you’d like, the next extension I’d recommend is a small set of **“CPG edge emission rules”** that consume *these relationship datasets* and deterministically emit the final CPG edges:

* `PY_DEFINES_SYMBOL`, `PY_REFERENCES_SYMBOL`, `PY_READS_SYMBOL`, `PY_WRITES_SYMBOL`, `PY_IMPORTS_SYMBOL` driven entirely by `symbol_roles` bits
* plus call edges that prefer `callsite→symbol` (SCIP) and fall back to `callsite→qualified_name` (QNP).



According to a document reviewed on **January 10, 2026**, here is a **small, CPG-focused set of deterministic edge-emission rules** that consume the relationship datasets we’ve already defined and emit **final CPG edges**, including:

* `PY_DEFINES_SYMBOL`
* `PY_REFERENCES_SYMBOL`
* `PY_READS_SYMBOL`
* `PY_WRITES_SYMBOL`
* `PY_IMPORTS_SYMBOL`

**driven entirely by `symbol_roles` bits** (SCIP), plus:

* **call edges** that **prefer** `callsite → symbol` (SCIP) and **fall back** to `callsite → qualified_name` (LibCST QualifiedNameProvider)

This is grounded in:

* SCIP `symbol_roles` bitmask semantics (Definition=1, Import=2, Write=4, Read=8)
* LibCST’s guidance to treat qualified names as **sets** of `(name, source)` candidates and to emit a call-site index with “callee candidates”
* Your Arrow/Acero DSL rules for deterministic dedupe strategies and canonical sort (stable `sort_indices + take`)

---

## 0) Prerequisites and invariants assumed by these rules

### 0.1 Relationship inputs consumed

These edge-emission rules assume the following relationship datasets already exist (from your registry packs):

* `rel_name_symbol` (CST name refs ↔ SCIP occurrences/symbols)
* `rel_import_symbol` (CST import alias ↔ SCIP import occurrence/symbol)
* `rel_callsite_symbol` (CST callsite ↔ SCIP symbol via callee span match / fallback)
* `rel_callsite_qname` (CST callsite ↔ qualified-name candidates, with `qname_id` via `dim_qualified_names`)

### 0.2 Node identity conventions (so edges don’t need extra joins)

To keep edge emission “pure” (no extra join to `cpg_nodes` needed), adopt these node ID conventions:

* `PY_NAME_REF.node_id = name_ref_id`
* `PY_IMPORT_ALIAS.node_id = import_alias_id`
* `PY_CALLSITE.node_id = call_id`
* `PY_SYMBOL.node_id = symbol` *(SCIP symbol strings are designed to be unique and stable)*
* `PY_QUALIFIED_NAME.node_id = qname_id`

LibCST explicitly recommends stable IDs based on `(path, byte_start, byte_len, kind)` and then hashing if you want compactness.

---

## 1) One shared contract for all emitted edges

Define a single contract used by all “edge emission” rules so unioning is trivial and deterministic.

### Contract: `cpg_edges_py_v1`

**Schema (minimum)**

* `edge_id: string` (non-null)
* `edge_kind: string` (non-null)
* `src_node_id: string` (non-null)
* `dst_node_id: string` (non-null)
* `path: string` (non-null)
* `bstart: int32` (non-null)
* `bend: int32` (non-null)
* `origin: string` (non-null) *(e.g., `"scip"`, `"qnp"`)*
* `resolution_method: string` (non-null)
* `confidence: float32` (non-null)
* `score: float32` (non-null)

Optional columns (nullable but very useful):

* `symbol_roles: int32` *(SCIP-derived edges)*
* `qname_source: string` *(QNP-derived edges; source indicates local/import/builtin etc.)*
* `ambiguity_group_id: string` *(group candidate sets; for QNP fallback, set to `call_id`)*

**Key (semantic identity)**

* `keys = (edge_kind, src_node_id, dst_node_id, path, bstart, bend)`

This matches your DSL guidance that edge keys should reflect semantic identity, and for “call-site facts” the key should include span/file context (not just `(src,dst,kind)`).

**Dedupe**

* strategy: `KEEP_FIRST_AFTER_SORT`
* tie-breakers (must form total order in CANONICAL):

  * `score DESC`
  * `confidence DESC`
  * `resolution_method ASC`
  * `dst_node_id ASC` *(final tie breaker)*

Your DSL explicitly standardizes winner strategies and requires stable, meaningful tie-breakers.

**Canonical sort**

* `(path ASC, bstart ASC, bend ASC, edge_kind ASC, src_node_id ASC, dst_node_id ASC)`

**Why this contract exists**
Threading and grouped ops don’t guarantee stable ordering, so determinism must be enforced as a contract and implemented via canonical primitives (stable sort + controlled dedupe).

---

## 2) Edge emission rules for symbol roles

SCIP says:

* if Definition bit is set, that occurrence is where the symbol is defined
* otherwise it is a usage/reference (and may also be an import/read/write)

We leverage exactly that.

> **Bit constants**: Definition=1, Import=2, WriteAccess=4, ReadAccess=8

### ER_PY_DEFINES_SYMBOL

**Purpose:** emit `PY_DEFINES_SYMBOL` edges from definition occurrences.

* **input_dataset:** `rel_name_symbol`
* **filter:** `(symbol_roles & 1) != 0` (Definition)
* **output_dataset:** `edges_py_defines_symbol`
* **output_contract:** `cpg_edges_py_v1`
* **mapping**

  * `edge_kind = "PY_DEFINES_SYMBOL"`
  * `src_node_id = name_ref_id`
  * `dst_node_id = symbol`
  * `path = path`
  * `bstart,bend = bstart,bend` *(token span evidence)*
  * `origin="scip"`
  * `resolution_method = rel_name_symbol.resolution_method` (e.g., `"SPAN_EXACT"`)
  * `confidence, score` propagate from relationship table
  * `symbol_roles = symbol_roles`

---

### ER_PY_REFERENCES_SYMBOL

**Purpose:** emit `PY_REFERENCES_SYMBOL` edges for every non-definition occurrence.

* **input_dataset:** `rel_name_symbol`
* **filter:** `(symbol_roles & 1) == 0` *(not a definition)*
  SCIP explicitly frames “not definition” as usage/reference (or import)
* **output_dataset:** `edges_py_references_symbol`
* **output_contract:** `cpg_edges_py_v1`
* **mapping**

  * `edge_kind = "PY_REFERENCES_SYMBOL"`
  * `src_node_id = name_ref_id`
  * `dst_node_id = symbol`
  * evidence span: `bstart,bend`
  * `origin="scip"`
  * `symbol_roles` retained

**Important note (intentional duplication)**
A single occurrence may generate both:

* `PY_REFERENCES_SYMBOL` and `PY_READS_SYMBOL` (or `PY_WRITES_SYMBOL`, `PY_IMPORTS_SYMBOL`)
  This is normal in rich CPGs and lets queries choose generic vs specialized semantics.

---

### ER_PY_READS_SYMBOL

**Purpose:** emit `PY_READS_SYMBOL` edges for occurrences with read role.

* **input_dataset:** `rel_name_symbol`
* **filter:** `(symbol_roles & 8) != 0` (ReadAccess)
* **output_dataset:** `edges_py_reads_symbol`
* **output_contract:** `cpg_edges_py_v1`
* **mapping**

  * `edge_kind="PY_READS_SYMBOL"`
  * `src_node_id=name_ref_id`
  * `dst_node_id=symbol`
  * `origin="scip"`
  * keep `symbol_roles`

---

### ER_PY_WRITES_SYMBOL

**Purpose:** emit `PY_WRITES_SYMBOL` edges for occurrences with write role.

* **input_dataset:** `rel_name_symbol`
* **filter:** `(symbol_roles & 4) != 0` (WriteAccess)
* **output_dataset:** `edges_py_writes_symbol`
* **output_contract:** `cpg_edges_py_v1`
* **mapping**

  * `edge_kind="PY_WRITES_SYMBOL"`
  * `src_node_id=name_ref_id`
  * `dst_node_id=symbol`
  * `origin="scip"`

---

### ER_PY_IMPORTS_SYMBOL

**Purpose:** emit `PY_IMPORTS_SYMBOL` edges anchored to import-alias nodes (not generic `Name` nodes).

* **input_dataset:** `rel_import_symbol`
* **filter:** `(symbol_roles & 2) != 0` (Import)
* **output_dataset:** `edges_py_imports_symbol`
* **output_contract:** `cpg_edges_py_v1`
* **mapping**

  * `edge_kind="PY_IMPORTS_SYMBOL"`
  * `src_node_id = import_alias_id`
  * `dst_node_id = symbol`
  * evidence span:

    * **recommended:** use the import-alias span (name token span if available; else alias node span)
  * `origin="scip"`
  * `symbol_roles` retained

---

## 3) Call edges: prefer SCIP symbol, fall back to QNP qualified names

LibCST guidance strongly supports:

* creating a **call-site index** with **callee candidates (qualified names)** and arg-shape signature
* treating qualified-name resolution as a **set** of `{name, source}` candidates (store as list and keep ambiguity)

### ER_PY_CALLS_SYMBOL

**Purpose:** emit call graph edges using SCIP resolution when available.

* **input_dataset:** `rel_callsite_symbol`
* **output_dataset:** `edges_py_calls_symbol`
* **output_contract:** `cpg_edges_py_v1`
* **mapping**

  * `edge_kind="PY_CALLS_SYMBOL"`
  * `src_node_id = call_id`
  * `dst_node_id = symbol`
  * evidence span:

    * `bstart,bend = call_bstart, call_bend` *(use the call expression as evidence)*
  * `origin="scip"`
  * `resolution_method` and `confidence/score` propagate
  * optionally include `symbol_roles`

---

### ER_DIM_CALLSITES_WITH_SCIP_SYMBOL

**Purpose:** build a “resolved callsites set” used to suppress QNP fallback when SCIP succeeded.

* **kind:** `dim_build` (distinct)
* **input_dataset:** `rel_callsite_symbol`
* **output_dataset:** `dim_callsites_resolved_by_scip`
* **schema**

  * `call_id: string` (non-null)
* **key_fields:** `(call_id,)`
* **canonical_sort:** `(call_id ASC)`

This is a small “set dimension” table.

---

### ER_PY_CALLS_QUALIFIED_NAME_FALLBACK

**Purpose:** for callsites with **no** SCIP-resolved callee, emit edges to **qualified-name candidates** (ambiguity-preserving).

* **input_dataset:** `rel_callsite_qname`
* **anti-join suppression:** remove call_ids present in `dim_callsites_resolved_by_scip`

  * equality key: `call_id`
  * join type: left anti (or left outer then `where resolved.call_id is null`)
* **output_dataset:** `edges_py_calls_qname_fallback`
* **output_contract:** `cpg_edges_py_v1`
* **mapping**

  * `edge_kind="PY_CALLS_QUALIFIED_NAME"`
  * `src_node_id = call_id`
  * `dst_node_id = qname_id`
  * evidence span: `bstart,bend = call_bstart, call_bend`
  * `origin="qnp"`
  * `resolution_method="QNP_CALLEE_FALLBACK"`
  * `confidence/score` from `rel_callsite_qname` (usually lower than SCIP symbol)
  * `qname_source = qname_source`
  * `ambiguity_group_id = call_id` *(so all candidates are queryable as one set)*

**Why “candidate set” is correct**
LibCST explicitly treats qualified name resolution as potentially **multiple candidates**; ambiguity must be preserved, not collapsed to a scalar.

> Optional extra: If you *also* want a single fallback winner, create a separate table `edges_py_calls_qname_winner` using the DSL’s `KEEP_BEST_BY_SCORE` strategy, but keep the candidate edges as the primary “truth set.” Your DSL sanctions a small set of winner strategies, including `KEEP_BEST_BY_SCORE`.

---

## 4) Final assembly rule: union + finalize deterministically

### ER_CPG_EDGES_PY_UNION_FINALIZE

**Purpose:** combine these edge streams into the final `cpg_edges` output.

* **inputs:**

  * `edges_py_defines_symbol`
  * `edges_py_references_symbol`
  * `edges_py_reads_symbol`
  * `edges_py_writes_symbol`
  * `edges_py_imports_symbol`
  * `edges_py_calls_symbol`
  * `edges_py_calls_qname_fallback`
* **operation:** union/concat in a controlled order (order doesn’t matter; finalize will canonicalize)
* **finalize:** apply contract `cpg_edges_py_v1`:

  * canonical sort using stable `sort_indices + take`
  * dedupe according to `KEEP_FIRST_AFTER_SORT` and tie-breakers
  * final canonical sort

This is necessary because ordering can drift under threading and grouping, so determinism must be enforced as a contract, not assumed.

---

## 5) Quick “edge semantics” reference (what each emitted edge means)

Driven by `symbol_roles` bits:

* `PY_DEFINES_SYMBOL`: occurrence has Definition bit set
* `PY_REFERENCES_SYMBOL`: occurrence is not a definition (may also be import/read/write)
* `PY_READS_SYMBOL`: occurrence has ReadAccess bit set
* `PY_WRITES_SYMBOL`: occurrence has WriteAccess bit set
* `PY_IMPORTS_SYMBOL`: occurrence has Import bit set

Calls:

* `PY_CALLS_SYMBOL`: SCIP resolved callee symbol (best)
* `PY_CALLS_QUALIFIED_NAME`: no SCIP callee symbol; emit QNP candidate set (fallback, ambiguity-preserving)

---

A “maximally complete / best‑in‑class” Python CPG is not a single graph so much as a **stack of interlocking subgraphs** that all share a **canonical evidence anchor** (file + byte-span), and that preserve **ambiguity** (multiple candidates) instead of forcing premature resolution. The important move is to treat each tool’s output as an *evidence lens*:

* **LibCST** = highest‑fidelity *concrete* syntax + rich metadata (byte spans, scope, qualified names, optional types)
* **Python `symtable`** = compiler‑truth for scope & binding classification
* **Python `dis` / bytecode** = execution‑truth for CFG + def/use (DFG scaffolding)
* **SCIP** = cross‑file symbol identity + definition/reference/import/read/write roles + symbol relationships
* **tree‑sitter** = error‑tolerant parsing + explicit *parse recovery diagnostics* + incrementality
* **inspect** (optional overlay) = runtime object facts and signatures, with strict sandboxing and “no-eval by default” policies

Below is the “best‑in‑class set” I would target as *named CPG products* (you can materialize them as one unified node/edge store with subgraph tags, or as separate graphs sharing IDs).

---

## 0) The non‑negotiable foundation: evidence anchors + stable IDs

**Make byte spans canonical everywhere.** LibCST explicitly supports `ByteSpanPositionProvider` and recommends treating byte spans as the canonical “evidence slicing” coordinate system (store line/col for UI, bytes for joins/snippets).

**tree‑sitter also gives byte offsets natively** (`start_byte/end_byte`) and surfaces parse recovery states (`is_missing`, `is_error`, `has_error`).

**SCIP ranges are line/char**, but occurrences are defined by `(symbol, symbol_roles, range)` and can be normalized to byte spans via your existing span normalization step (repo text index). The key point is: SCIP gives you “this span corresponds to this symbol with these roles.”

Best‑in‑class CPGs use:

* `anchor_id = hash(path, bstart, bend, kind)` (your existing philosophy)
* Every node and edge that refers to code points at an `anchor_id`
* Every “semantic claim” includes `{source: <tool>, confidence, ambiguity_group_id?}`

---

## 1) CPG‑Syntax: Lossless Concrete Syntax Graph (CSG)

**Goal:** be able to answer formatting/trivia questions and drive rewrite/codemod tooling, while still being joinable to semantics.

**Primary source:** LibCST

* Parse + traversal to emit syntax constructs, *including trivia ownership* and precise spans.
* Store both line/col and byte spans (LibCST position providers support both).

**Node kinds (representative):**

* `PY_MODULE_CST`, `PY_CLASS_CST`, `PY_FUNCTION_CST`, `PY_IMPORT_CST`, `PY_CALL_CST`, `PY_ATTRIBUTE_CST`
* Literal/detail nodes: `PY_STRING_LITERAL`, `PY_FSTRING_PART`, `PY_NUMBER_LITERAL`
* Trivia nodes (if you want true “best in class”): `PY_COMMENT`, `PY_WHITESPACE`, `PY_NEWLINE` (LibCST can preserve/regenerate source via `Module.code` and immutable transforms).

**Edge kinds:**

* `CST_PARENT_OF`, `CST_NEXT_TOKEN` (optional), `CST_OWNS_TRIVIA`, `CST_HAS_BYTE_SPAN`

**Why this matters:** it’s the only way to be *fully round‑trip faithful* while still attaching semantic overlays.

---

## 2) CPG‑ParseDiagnostics: Error‑Tolerant Parse Graph

**Goal:** still produce a useful graph when code is partially broken, incomplete, or mid-edit.

**Primary source:** tree‑sitter
tree‑sitter nodes carry:

* `is_missing` (recovery inserted tokens)
* `is_error` / `has_error`
* byte spans and points

**What to model:**

* `TS_NODE` for every named node you care about, plus:

  * `TS_ERROR_NODE` (or a `props.is_error`)
  * `TS_MISSING_TOKEN` (or a `props.is_missing`)
* `PARSE_CONTAINS_ERROR(file → error_node)`
* `PARSE_RECOVERY_INSERTED(file → missing_token_node)`

**Best‑in‑class addition:** incremental reindex hooks. tree‑sitter supports `Tree.edit(...)`, `Parser.parse(..., old_tree=...)`, and `changed_ranges` for minimal invalidation windows.

This becomes your “editor‑grade CPG refresh loop” even if your final offline build uses LibCST+SCIP.

---

## 3) CPG‑ScopeBinding: Lexical Scopes + Bindings Graph

**Goal:** be correct about Python’s scoping rules (locals vs globals vs nonlocals vs frees, annotation/type scopes, etc.) and unify *all* def/use views (syntax + bytecode + SCIP) on a single “binding slot model.”

**Primary source:** `symtable` (compiler truth)
Symtable gives table kinds (3.13+ includes modern typing scopes) such as:

* `MODULE`, `FUNCTION`, `CLASS`
* `ANNOTATION`, `TYPE_ALIAS`, `TYPE_PARAMETERS`, `TYPE_VARIABLE`

And per-symbol classification flags:

* `is_referenced`, `is_assigned`, `is_imported`, `is_annotated`
* `is_parameter`, `is_global`, `is_declared_global`, `is_nonlocal`, `is_local`, `is_free`
* namespace facts via `is_namespace`, `get_namespaces`

**Node kinds:**

* `SCOPE` (with subtype: module/function/class/annotation/type_params/type_alias/type_var)
* `BINDING` (name + scope_id + category: param/local/global/nonlocal/free/import)
* `BINDING_DEF_SITE`, `BINDING_USE_SITE` (anchor-based)
* (Optional) `NAMESPACE` nodes when symtable reports namespace behavior

**Edge kinds:**

* `OWNS_SCOPE`, `PARENT_SCOPE`
* `SCOPE_BINDS(binding → scope)`
* `RESOLVES_TO(binding → outer_binding)` (for free/nonlocal/global resolution)
* `BINDS_DEF(anchor → binding)`, `BINDS_USE(anchor → binding)`

**Best‑in‑class move:** this graph is the glue that makes bytecode DFG and syntax “name refs” talk about the **same identity**.

---

## 4) CPG‑Symbols: Cross‑File Symbol Identity Graph

**Goal:** give every definition and reference a stable, global symbol identity across files and dependencies.

**Primary source:** SCIP
SCIP occurrences have:

* `symbol` (unique identifier string)
* `symbol_roles` bitmask (definition/import/write/read/etc.)
* `range` (location)

SCIP also provides `SymbolInformation` entries (for many symbols) with documentation and relationships like “extends” / “overrides.”

**Node kinds:**

* `SCIP_SYMBOL`
* `SCIP_OCCURRENCE`
* `SCIP_DOCUMENT` (per file)
* `SCIP_DIAGNOSTIC` (when present in documents; documents may contain diagnostics)

**Edge kinds:**

* `PY_DEFINES_SYMBOL(anchor → scip_symbol)` when `symbol_roles` has Definition bit
* `PY_REFERENCES_SYMBOL(anchor → scip_symbol)` when it’s a reference
* `PY_IMPORTS_SYMBOL(anchor → scip_symbol)` when Import bit set
* `PY_READS_SYMBOL` / `PY_WRITES_SYMBOL` based on role bits
* `SCIP_EXTENDS(symbol → symbol)` / `SCIP_OVERRIDES(symbol → symbol)` when present in symbol info

**Best‑in‑class behavior:** never collapse ambiguity early—store all candidate resolutions, then apply deterministic winner selection (as you’re already doing in your relationship registry).

---

## 5) CPG‑QualifiedNames: Name Resolution Candidates Graph

**Goal:** represent “likely referents” even when SCIP is missing or incomplete.

**Primary source:** LibCST metadata providers
LibCST provides:

* `ScopeProvider` (defs/refs without re‑implementing scoping)
* `QualifiedNameProvider` (possible qualified names, import-aware)
* `FullyQualifiedNameProvider` for absolute module-qualified names
* `ExpressionContextProvider` for Load/Store/Delete classification

**Node kinds:**

* `QNAME` (qualified name candidate, with source: QualifiedNameProvider vs FullyQualifiedNameProvider)
* `NAME_REF` / `ATTRIBUTE_ACCESS` / `CALLSITE` anchors

**Edges:**

* `NAME_REF_CANDIDATE_QNAME(name_ref → qname)`
* `CALLSITE_CANDIDATE_QNAME(callsite → qname)`
* `ATTR_CANDIDATE_QNAME(attr → qname)`
* `EXPR_CONTEXT(name_ref → {LOAD|STORE|DEL})` as a property/edge

This subgraph is your **SCIP fallback** and your **ambiguity preservation** mechanism.

---

## 6) CPG‑FlowCFG: Execution‑Grounded Control Flow Graph

**Goal:** eliminate guesswork in Python control flow (especially `try/except/finally`, `with`, comprehensions) by grounding in *actual compiled control flow*.

**Primary source:** `dis` / bytecode
Your Python tooling guide explicitly frames:

* `DIS-2` as “Control flow: basic blocks, jump semantics, exception table parsing + edges”

And it cautions that CPython bytecode is an implementation detail with specialization/caches—so this layer should be version-stamped and treated as “execution lens,” not “language spec.”

**Node kinds:**

* `BC_CODE_UNIT` (code object / function/lambda/comprehension)
* `BC_INSTR`
* `CFG_BLOCK`
* `EXC_HANDLER` / `EXC_EDGE` (modeled from exception table)

**Edge kinds:**

* `CODE_UNIT_HAS_BLOCK`, `BLOCK_HAS_INSTR`
* `CFG_NEXT(block → block)` (normal edges)
* `CFG_EXC(block → handler_block)` (exception edges)

---

## 7) CPG‑FlowDFG: Dataflow / Def‑Use Graph (Reaching Definitions)

**Goal:** get a real DFG, not heuristic “maybe-use” edges, by using bytecode def/use events and a stack model, then running reaching defs over the CFG.

Your guide lays out a concrete plan:

* derive `py_bc_defuse_events` from instructions
* build CFG
* resolve slot def/use events to actual `BINDING` nodes using scope/binder
* run reaching defs and emit `REACHES(def_site → use_site)`

It also explicitly notes this makes exception‑heavy constructs stop being guesswork because the DFG is grounded in `dis + exception table`.

**Node kinds:**

* reuse `BC_INSTR` or introduce `DF_DEF_SITE` / `DF_USE_SITE`
* `BINDING` (from ScopeBinding graph)

**Edge kinds:**

* `DEFINES_BINDING(df_def → binding)`
* `USES_BINDING(df_use → binding)`
* `REACHES(df_def → df_use)` (your DFG edges)

This is where “best‑in‑class” Python CPGs separate from most static analyzers: they rarely do bytecode‑grounded DFG at scale.

---

## 8) CPG‑Types: Annotation + (Optional) Inferred Type Graph

**Goal:** expose types as a first-class graph so you can query “calls where arg type mismatches param type”, “return type flows”, etc.

**Sources:**

* **Annotations:** use `inspect.get_annotations(eval_str=False)` as a stable boundary for safe extraction (no string eval by default).
* **Optional inference:** LibCST `TypeInferenceProvider` (Pyre + Watchman required; treat as enrichment).

**Node kinds:**

* `TYPE_EXPR` (syntax form; from CST/AST)
* `TYPE` (normalized form; maybe string, maybe structured)
* `TYPEVAR`, `TYPE_PARAM_SCOPE` (tie into symtable’s typing scopes)

**Edges:**

* `HAS_ANNOTATION(def → type_expr)`
* `INFERRED_TYPE(node → type)` with `props.source={pyre|heuristic|runtime}`

Best‑in‑class detail: types must be **multi-valued** and evidence-tagged (annotation vs inferred vs runtime).

---

## 9) CPG‑RuntimeOverlay: Optional Runtime Object Graph

**Goal:** connect “static definitions” to “runtime realities” (wrapping, signatures, attributes, MRO, etc.), but only under explicit safe execution policies.

Your guide describes an “inspect → facts” contract designed to be deterministic and non-executing by default, highlighting safe enumeration (`getmembers_static`, `getattr_static`) and wrapper handling (`inspect.unwrap`, `__wrapped__`).

**Node kinds:**

* `INSPECT_OBJECT`
* `INSPECT_SIGNATURE`
* `INSPECT_SIGNATURE_PARAM`

**Edges:**

* `HAS_SIGNATURE(obj → signature)`
* `HAS_PARAM(signature → param)`
* `WRAPS(obj → obj)` (unwrap hop chain)
* `INSPECT_ANCHORS_AST(obj → anchor)` (best effort)

This overlay is optional, but if you have it, it’s a major differentiator for Python.

---

## 10) The “Ultimate” product: Unified Python CPG with layered certainty

If you want a single “maximally complete best-in-class” output, it’s this:

### A) One unified node store with subgraph tags

Every node has:

* `node_kind`, `node_id`
* `anchor_id?` (when tied to source)
* `source={libcst|ast|symtable|dis|scip|treesitter|inspect}`
* `confidence`, `ambiguity_group_id?`

### B) One unified edge store + deterministic winner policies

* Candidate edges are emitted and scored
* Final edges are chosen deterministically (your existing keep-first-after-sort pattern)

### C) “Truth precedence” (recommended)

* Cross-file symbol identity: **SCIP** (authoritative)
* Lexical scope classification: **symtable** (authoritative)
* Concrete spans/trivia: **LibCST** (authoritative)
* Broken/partial code parse: **tree‑sitter** (authoritative for diagnostics)
* Execution flow: **dis** (authoritative for CFG/DFG within a CPython version)
* Runtime overlay: **inspect** (optional enrichment, always labeled as runtime evidence)

---

## 11) Practical note for Python 3.14 (ops-grade, but impacts “best-in-class”)

If you do repo-scale parsing/indexing with multiprocess: plan for spawn/forkserver behavior (your LibCST ops notes explicitly call out default start-method drift, including POSIX forkserver since 3.14). That matters for determinism and cache strategy in a production indexer.

---

## If you want, I can make this fully “implementable” next

Two useful follow-ups that turn this into an engineering checklist:

1. A **concrete node_kind / edge_kind master enum** for *this* “Ultimate Python CPG” (including flow, type, diagnostics, runtime overlay) with required properties per kind.

2. A **source-to-subgraph derivation recipe**: for each node/edge kind, precisely which extractor emits it (LibCST provider X, SCIP field Y, symtable flag Z, dis table W), what the stable IDs are, and what confidence/ambiguity rules apply.



Below is a **concrete, drop‑in validator hook** that makes your **relspec compiler enforce** the **Ultimate CPG kind registry**:

* When a relationship rule produces an output dataset that *feeds CPG edges of kind X*, the compiler validates that the dataset’s **output Contract** contains **all `required_props` for those EdgeKinds** as either:

  1. **real output columns** in the contract schema, or
  2. **declared downstream/virtual props** on the contract (explicitly listed), which will be injected later by edge emission.

This is the missing enforcement link between:

* your **relationship registry + contracts**, and
* your **CPG edge kind requirements** in `cpg/kind_catalog.py`.

---

# 1) Add a small “virtual fields” capability to `Contract`

To support the “either as actual columns or required downstream props” requirement, the Contract must be able to declare “virtual props”.

## Patch `src/codeintel_cpg/arrowdsl/contracts.py`

Add the following fields + helper to your `Contract` dataclass (keep everything else unchanged):

```python
from dataclasses import dataclass, field
from typing import Dict, Optional, Sequence, Tuple

import pyarrow as pa

@dataclass(frozen=True)
class Contract:
    name: str
    schema: pa.Schema
    required_non_null: Tuple[str, ...] = ()
    # ... your existing fields: dedupe, canonical_sort, etc.

    # NEW: properties required by downstream consumers but not stored as columns
    # Example: origin="scip" injected by the edge emission stage.
    virtual_fields: Tuple[str, ...] = ()

    # Optional docs for virtual fields (helpful for debugging)
    virtual_field_docs: Optional[Dict[str, str]] = None

    def available_fields(self) -> Tuple[str, ...]:
        """
        Columns + declared virtual fields. Used by validators.
        """
        cols = tuple(self.schema.names) if self.schema is not None else ()
        return tuple(cols) + tuple(self.virtual_fields or ())
```

**Why this matters:** the validator can now strictly enforce that “downstream props” are **declared** (not implicit magic).

---

# 2) Create validator module: `src/codeintel_cpg/relspec/edge_contract_validation.py`

This module:

* loads edge kind requirements from `cpg/kind_catalog.py`,
* maps relationship output datasets → edge kinds,
* validates output contracts against required edge props.

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple, Union


# Default mapping: relationship output dataset -> edge kinds it can emit downstream.
# You can override this in config or add more datasets as you expand the registry.
DEFAULT_REL_OUTPUT_TO_EDGE_KINDS: Dict[str, Tuple[str, ...]] = {
    # name_ref_id + symbol + roles => defines/references/reads/writes
    "rel_name_symbol": (
        "PY_DEFINES_SYMBOL",
        "PY_REFERENCES_SYMBOL",
        "PY_READS_SYMBOL",
        "PY_WRITES_SYMBOL",
    ),
    # import_alias_id + symbol => import edges
    "rel_import_symbol": (
        "PY_IMPORTS_SYMBOL",
    ),
    # call_id + symbol => call edges (preferred)
    "rel_callsite_symbol": (
        "PY_CALLS_SYMBOL",
    ),
    # call_id + qname_id => call edges (fallback)
    "rel_callsite_qname": (
        "PY_CALLS_QNAME",
    ),
}


@dataclass(frozen=True)
class EdgeContractValidationConfig:
    """
    Configuration for validating relationship output contracts against CPG EdgeKind contracts.
    """
    dataset_to_edge_kinds: Mapping[str, Sequence[str]] = DEFAULT_REL_OUTPUT_TO_EDGE_KINDS

    # If True, we error when a dataset is mapped to edge kinds but has no contract_name.
    require_contract_name: bool = True

    # If True, we error if multiple rules produce same output_dataset with different contract_name.
    require_single_contract_per_output_dataset: bool = True

    # If True, we error when an edge kind is unknown in the Ultimate registry.
    error_on_unknown_edge_kind: bool = True


def _load_edge_kind_required_props() -> Dict[str, Set[str]]:
    """
    Load edge kind requirements from codeintel_cpg.cpg.kind_catalog.

    Returns:
      { "EDGE_KIND_NAME": {"required_prop_a", ...}, ... }
    """
    from codeintel_cpg.cpg.kind_catalog import edge_kind_required_props

    return edge_kind_required_props()


def _stringify_edge_kinds(edge_kinds: Sequence[Any]) -> Tuple[str, ...]:
    out: List[str] = []
    for ek in edge_kinds:
        if ek is None:
            continue
        if hasattr(ek, "value"):  # Enum
            out.append(str(ek.value))
        else:
            out.append(str(ek))
    return tuple(out)


def _contract_available_fields(contract: Any) -> Set[str]:
    """
    Returns available fields for validation:
      - schema column names
      - contract.virtual_fields (if present)
      - contract.available_fields() (if present)
    """
    # Prefer explicit helper if present
    if hasattr(contract, "available_fields") and callable(contract.available_fields):
        try:
            return set(contract.available_fields())
        except Exception:
            pass

    fields: Set[str] = set()

    schema = getattr(contract, "schema", None)
    if schema is not None and hasattr(schema, "names"):
        try:
            fields |= set(schema.names)
        except Exception:
            pass

    virtual = getattr(contract, "virtual_fields", None)
    if virtual:
        try:
            fields |= set(virtual)
        except Exception:
            pass

    return fields


def validate_relationship_output_contracts_for_edge_kinds(
    *,
    rules: Sequence[Any],
    contract_catalog: Any,
    config: Optional[EdgeContractValidationConfig] = None,
) -> None:
    """
    Validates that whenever a relationship output dataset feeds a CPG edge kind X, the dataset's
    output contract provides all required_props for that edge kind.

    "Provides" means:
      - the prop exists as a column in contract.schema, OR
      - the prop is declared in contract.virtual_fields (downstream injected).

    Raises ValueError with a readable multi-error report.
    """
    cfg = config or EdgeContractValidationConfig()
    required_props_by_edge_kind = _load_edge_kind_required_props()

    # Group rules by output_dataset
    rules_by_out: Dict[str, List[Any]] = {}
    for r in rules:
        out_ds = getattr(r, "output_dataset", None)
        if out_ds:
            rules_by_out.setdefault(str(out_ds), []).append(r)

    errors: List[str] = []

    # Validate each output dataset that maps to some edge kinds
    for out_ds, edge_kinds in cfg.dataset_to_edge_kinds.items():
        edge_kinds_s = _stringify_edge_kinds(edge_kinds)

        # If no rules produce this dataset, skip (you might not enable all rules in all runs)
        out_rules = rules_by_out.get(out_ds, [])
        if not out_rules:
            continue

        # Determine contract_name(s) used by the rules producing this output dataset
        contract_names = sorted({str(getattr(r, "contract_name", "") or "") for r in out_rules})
        contract_names = [c for c in contract_names if c]  # drop empty

        if cfg.require_contract_name and not contract_names:
            errors.append(
                f"[relspec] Output dataset '{out_ds}' is mapped to edge kinds {list(edge_kinds_s)} "
                f"but the producing rules have no contract_name set."
            )
            continue

        if cfg.require_single_contract_per_output_dataset and len(set(contract_names)) > 1:
            errors.append(
                f"[relspec] Output dataset '{out_ds}' has inconsistent contract_name across rules: {contract_names}. "
                f"All rules producing the same output dataset must share a single contract."
            )
            continue

        if not contract_names:
            continue  # allowed if require_contract_name=False

        contract_name = contract_names[0]

        # Get contract from catalog
        if not hasattr(contract_catalog, "get"):
            errors.append(f"[relspec] Contract catalog has no .get(); cannot validate '{contract_name}'.")
            continue

        contract = contract_catalog.get(contract_name)
        if contract is None:
            errors.append(
                f"[relspec] Missing contract '{contract_name}' in contract catalog (needed for '{out_ds}')."
            )
            continue

        available_fields = _contract_available_fields(contract)

        # Now validate required props for each edge kind
        for ek in edge_kinds_s:
            if ek not in required_props_by_edge_kind:
                if cfg.error_on_unknown_edge_kind:
                    errors.append(
                        f"[relspec] Unknown edge kind '{ek}' referenced by dataset_to_edge_kinds for '{out_ds}'. "
                        "Not found in the edge kind catalog."
                    )
                continue

            req = required_props_by_edge_kind[ek]
            missing = sorted([p for p in req if p not in available_fields])

            if missing:
                errors.append(
                    f"[relspec] Contract '{contract_name}' for output dataset '{out_ds}' does not provide "
                    f"required edge props for edge kind '{ek}': missing={missing}. "
                    f"Available fields={sorted(list(available_fields))}. "
                    f"Fix by either (a) adding columns to the relationship output schema, or "
                    f"(b) declaring these as Contract.virtual_fields if injected downstream."
                )

    if errors:
        raise ValueError("Edge contract validation failed:\n" + "\n".join(f" - {e}" for e in errors))
```

---

# 3) Wire the validator hook into `relspec/compiler.py`

In your `RelationshipRuleCompiler.compile_registry(...)`, add an optional `contracts` parameter and call the validator **before** compiling.

## Patch `src/codeintel_cpg/relspec/compiler.py`

Add imports near the top:

```python
from .edge_contract_validation import (
    EdgeContractValidationConfig,
    validate_relationship_output_contracts_for_edge_kinds,
)
```

Then update your `RelationshipRuleCompiler.compile_registry` signature and add the hook.

### Example patch (minimal, safe)

```python
class RelationshipRuleCompiler:
    def __init__(self, resolver: Any):
        self._resolver = resolver

    def compile_registry(
        self,
        rules: Sequence[Any],
        *,
        ctx: Any,
        contracts: Any = None,
        edge_validation: Optional[EdgeContractValidationConfig] = None,
    ) -> Dict[str, Any]:
        """
        Compile a set of relationship rules into executable outputs.

        New: if `contracts` is provided, validates that relationship output contracts
        satisfy required edge props for the edge kinds those datasets feed.
        """
        # --- NEW VALIDATION HOOK ---
        if contracts is not None:
            validate_relationship_output_contracts_for_edge_kinds(
                rules=rules,
                contract_catalog=contracts,
                config=edge_validation or EdgeContractValidationConfig(),
            )

        # ... existing compilation logic below (unchanged) ...
        return self._compile_rules_to_outputs(rules=rules, ctx=ctx)

    def _compile_rules_to_outputs(self, rules: Sequence[Any], *, ctx: Any) -> Dict[str, Any]:
        # your existing compile implementation
        ...
```

This is intentionally small: it does not change your plan compilation logic.

---

# 4) Make your existing relationship output contracts declare downstream props

Now that validation is strict, you need to declare virtual props like `origin` that are injected later.

In your `hamilton/modules/cpg_build.py` `relationship_contracts()` where you register:

* `rel_name_symbol_v1`
* `rel_import_symbol_v1`
* `rel_callsite_symbol_v1`
* `rel_callsite_qname_v1`

Add `virtual_fields=("origin",)` to each contract **if `origin` is required by the edge kinds they feed**.

For example:

```python
cc.register(
    Contract(
        name="rel_name_symbol_v1",
        schema=rel_name_symbol_schema,
        required_non_null=("name_ref_id", "symbol"),
        virtual_fields=("origin",),  # NEW: injected downstream by edge emitter
        # ... existing dedupe/canonical_sort ...
    )
)
```

Do the same for:

* `rel_import_symbol_v1`
* `rel_callsite_symbol_v1`
* `rel_callsite_qname_v1`

If you also inject `resolution_method` downstream (instead of storing it), declare it too.

---

# 5) Update the Hamilton node that calls `compile_registry`

In `hamilton/modules/cpg_build.py`, update `compiled_relationship_outputs` to pass `relationship_contracts`:

```python
def compiled_relationship_outputs(
    relationship_registry: RelationshipRegistry,
    relspec_resolver: Any,
    relationship_contracts: ContractCatalog,  # add dep
    ctx: ExecutionContext,
) -> Dict[str, Any]:
    compiler = RelationshipRuleCompiler(resolver=relspec_resolver)
    return compiler.compile_registry(
        relationship_registry.rules(),
        ctx=ctx,
        contracts=relationship_contracts,
        edge_validation=EdgeContractValidationConfig(),  # optional override
    )
```

Now the DAG fails early if a contract drifts from what your CPG edge kinds require.

---

## What this gives you (why it’s “the final enforcement link”)

* Your **edge kind catalog** (required props) becomes *enforced*, not aspirational.
* Any refactor that removes a column like `symbol_roles` or forgets to declare `origin` as virtual will fail at the relationship compilation boundary—**before** graph build.
* This keeps the pipeline inference-driven but still contract-safe.

---

If you want the next tightening step: I can extend this validator to also check **type correctness** (e.g., `symbol_roles` must be int, `score` float) by comparing `prop_catalog` value types against `pyarrow.Schema.field(prop).type`, and to validate that your edge emission stage actually injects every declared `virtual_fields` (so “virtual” can’t silently become “missing”).

