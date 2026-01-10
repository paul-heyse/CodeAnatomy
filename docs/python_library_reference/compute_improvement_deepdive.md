
## Acero: what it is (and what it isn’t)

**Acero** is Arrow’s **streaming execution engine**: you express work as an **execution plan** and Acero runs it over **streams of RecordBatches** (batched, pipeline-style). In Python, this shows up as `pyarrow.acero`, where you build an ExecPlan via the `Declaration` interface. It’s explicitly described as **experimental** in the Arrow docs. ([Apache Arrow][1])

Why you care (especially for your “best-in-class compute” goal): if you “just call compute kernels one after the other” you often force **full materialization of intermediates**; Acero exists to execute multi-step computations in a streaming/batched way that avoids that pattern. ([Apache Arrow][2])

Acero is also explicitly **not**:

* an optimizer (no SQL planner / no cost-based rewrite; it does what you tell it), and
* not distributed (it can be used *inside* distributed systems, but doesn’t orchestrate a cluster). ([Apache Arrow][3])

---

## The mental model you should internalize

### 1) Plans are graphs of nodes

At the C++ level, an ExecPlan is a graph of **ExecNode** operators; batches flow through edges; sources produce batches; sinks consume them. ([Apache Arrow][3])

### 2) In Python you author “Declarations”

A `pyarrow.acero.Declaration` represents an **unconstructed exec node** plus its upstream declarations (inputs). You specify:

* a **factory name** like `"scan"`, `"filter"`, `"project"`, etc.
* an **options object** (a subclass of `ExecNodeOptions`)
* optionally, **inputs** (other `Declaration`s). ([Apache Arrow][4])

### 3) Expressions are Arrow compute expressions

Filter/project/join keys are expressed with Arrow **compute Expressions** (field refs, literals, compute functions, comparisons). The docs describe creating expressions using `pyarrow.compute.field()` and `pyarrow.compute.scalar()` and then combining them with comparisons/operators. ([Apache Arrow][5])

---

## The “authoring surface” in PyArrow: node factories you actually use

The Python Acero API lists these key option types / node factories: `table_source`, `scan`, `filter`, `project`, `aggregate`, `order_by`, and `hashjoin`. ([Apache Arrow][1])

Below is what each one *means*, the gotchas, and the correct wiring pattern.

---

# A) Sources

## 1) `table_source`: start from an in-memory Table

Use this when you already have a `pa.Table` in memory.

* Factory name: `"table_source"`
* Options: `TableSourceNodeOptions(table)` ([Apache Arrow][6])

```python
import pyarrow as pa
import pyarrow.acero as acero

source = acero.Declaration(
    "table_source",
    acero.TableSourceNodeOptions(my_table),
)
```

## 2) `scan`: start from a Dataset (and get pushdown)

Use this when you want Acero to read from files via `pyarrow.dataset.Dataset`.

* Factory name: `"scan"`
* Options: `ScanNodeOptions(dataset, **kwargs)` ([Apache Arrow][7])

### The important bits (pushdown + ordering)

`ScanNodeOptions` can apply **pushdown projections/filters** to file readers *to reduce IO*, but it **does not** automatically create filter/project nodes for final semantics—you generally supply the same expressions again in downstream nodes. ([Apache Arrow][7])

It also has ordering-related knobs:

* `implicit_ordering=True` augments output batches with fragment/batch indices “to enable stable ordering for simple ExecPlans”
* `require_sequenced_output=True` yields batches sequentially “like single-threaded” ([Apache Arrow][7])

```python
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow.acero as acero

dataset = ds.dataset("/path/to/parquet_dir", format="parquet")

filter_expr = pc.field("kind") == pc.scalar("call")

scan = acero.Declaration(
    "scan",
    acero.ScanNodeOptions(
        dataset,
        # kwargs are Scanner.from_dataset-style options (columns/filter/batch_size/etc.)
        filter=filter_expr,              # pushdown attempt
        columns=["kind", "src", "dst"],  # pushdown projection attempt
        implicit_ordering=True,          # stable-ish ordering for simple plans
        require_sequenced_output=False,  # set True if you need strict sequencing
    ),
)
```

---

# B) Row-wise transforms

## 3) `filter`: keep rows where expression is true

* Factory name: `"filter"`
* Options: `FilterNodeOptions(filter_expression)`
* Filter expression must be boolean. ([Apache Arrow][8])

```python
import pyarrow.compute as pc
import pyarrow.acero as acero

flt = acero.Declaration(
    "filter",
    acero.FilterNodeOptions(pc.field("score") > pc.scalar(0)),
    inputs=[scan],   # or [source]
)
```

## 4) `project`: compute/rearrange columns (elementwise only)

* Factory name: `"project"`
* Options: `ProjectNodeOptions(expressions, names=None)` ([Apache Arrow][9])

Key rule: project expressions must be **scalar (elementwise) expressions**—one output value per input row, independent of other rows. If you need group-wise behavior, that’s `aggregate`. ([Apache Arrow][9])

Also: if you don’t supply `names`, Arrow will use string representations of expressions. ([Apache Arrow][9])

```python
import pyarrow.compute as pc
import pyarrow.acero as acero

proj = acero.Declaration(
    "project",
    acero.ProjectNodeOptions(
        expressions=[
            pc.field("src"),
            pc.field("dst"),
            pc.add(pc.field("weight"), pc.scalar(1)),
        ],
        names=["src", "dst", "weight_plus_1"],
    ),
    inputs=[flt],
)
```

---

# C) Aggregation

## 5) `aggregate`: scalar aggregates or SQL-like GROUP BY (hash aggregates)

* Factory name: `"aggregate"`
* Options: `AggregateNodeOptions(aggregates, keys=None)` ([Apache Arrow][10])

Important semantics:

* Acero supports **scalar** aggregates and **hash (GROUP BY)** aggregates. ([Apache Arrow][10])
* Each aggregation is described by a **tuple**: *target column(s)*, function name, options object, output field name. Targets can be:

  * a single field ref (unary agg),
  * an empty list (nullary agg),
  * or a list of fields (n-ary agg). ([Apache Arrow][10])

Also (C++ engine behavior that matters to performance expectations): by default, aggregate is a **pipeline breaker** (needs to accumulate input before producing output). ([Apache Arrow][11])

```python
import pyarrow.compute as pc
import pyarrow.acero as acero

agg = acero.Declaration(
    "aggregate",
    acero.AggregateNodeOptions(
        keys=[pc.field("src")],
        aggregates=[
            # (target, func_name, func_options, output_name)
            (pc.field("dst"), "count", None, "out_degree"),   # example shape
            (pc.field("weight_plus_1"), "sum", None, "w_sum"),
        ],
    ),
    inputs=[proj],
)
```

*(Function names/options must match what your Arrow build supports; in practice you’ll standardize a small set and snapshot-test them.)*

---

# D) Joins

## 6) `hashjoin`: relational joins with explicit controls

* Factory name: `"hashjoin"`
* Options: `HashJoinNodeOptions(...)` ([Apache Arrow][12])

The Python docs enumerate:

* join types: `"left semi"`, `"right semi"`, `"left anti"`, `"right anti"`, `"inner"`, `"left outer"`, `"right outer"`, `"full outer"` ([Apache Arrow][12])
* keys can be column-name strings, field expressions, or lists of those ([Apache Arrow][12])
* optional `left_output` / `right_output` column selection
* suffixes for name collisions
* an optional residual `filter_expression` applied to matching rows ([Apache Arrow][12])

```python
import pyarrow.acero as acero
import pyarrow.compute as pc

left = acero.Declaration("table_source", acero.TableSourceNodeOptions(left_table))
right = acero.Declaration("table_source", acero.TableSourceNodeOptions(right_table))

join = acero.Declaration(
    "hashjoin",
    acero.HashJoinNodeOptions(
        join_type="inner",
        left_keys=["symbol_id"],
        right_keys=["symbol_id"],
        left_output=["symbol_id", "src"],
        right_output=["dst", "kind"],
        output_suffix_for_left="_l",
        output_suffix_for_right="_r",
        filter_expression=(pc.field("kind") == pc.scalar("call")),
    ),
    inputs=[left, right],
)
```

---

# E) Ordering

## 7) `order_by`: force a new ordering (but it accumulates)

* Factory name: `"order_by"`
* Options: `OrderByNodeOptions(sort_keys=..., null_placement=...)` ([Apache Arrow][13])

The docs are very explicit: this node currently works by **accumulating all data**, sorting, then emitting; **larger-than-memory sort is not supported**. ([Apache Arrow][13])

```python
import pyarrow.acero as acero

ordered = acero.Declaration(
    "order_by",
    acero.OrderByNodeOptions(
        sort_keys=[("src", "ascending"), ("dst", "ascending")],
        null_placement="at_end",
    ),
    inputs=[agg],
)
```

---

## Composition patterns you’ll use constantly

### 1) Linear pipelines: `Declaration.from_sequence`

This convenience method wires “a simple sequence of nodes” by appending each declaration to the next declaration’s inputs. ([Apache Arrow][4])

```python
plan = acero.Declaration.from_sequence([source, proj, flt, agg])
```

### 2) Multi-input nodes: pass `inputs=[left, right]`

Joins are the primary example (also union exists in C++ but isn’t part of the Python surface list shown on the API page). ([Apache Arrow][1])

---

## Execution surface: how you run a plan

### `to_table(use_threads=True)`

* Implicitly adds a **sink node** to collect results into a table.
* Creates an ExecPlan, starts it, blocks until finished, returns a `pyarrow.Table`. ([Apache Arrow][4])
* `use_threads=False` keeps CPU work on the calling thread (I/O may still be multi-threaded via the I/O executor). ([Apache Arrow][4])

```python
result_table = plan.to_table(use_threads=True)
```

### `to_reader(use_threads=True)`

Runs the declaration and returns a `pyarrow.RecordBatchReader`. ([Apache Arrow][4])

```python
reader = plan.to_reader(use_threads=True)
for batch in reader:
    ...
```

**Rule of thumb for your architecture:** use `to_reader()` when you truly want streaming downstream; use `to_table()` when you want “one materialization point” that you can validate/cache/hand off (which matches your “finalize gate” philosophy).

---

## The “Acero-native” best practices that matter for your project

1. **Write plans as fused pipelines**: scan/table_source → project/filter → (join/aggregate/order_by) → to_table/to_reader. That’s the unit that’s easiest to standardize + snapshot-test and aligns with “do work in Arrow, not Python loops.” ([Apache Arrow][1])

2. **Always understand what pushes down vs what doesn’t**:

   * `scan` can push projection/filter *into file readers*, but you still need explicit filter/project nodes for final semantics. ([Apache Arrow][7])

3. **Keep project expressions elementwise**:

   * Anything group/stateful goes into `aggregate`. ([Apache Arrow][9])

4. **Be explicit about determinism** when it matters:

   * If stable ordering matters, you either:

     * rely on scan’s `implicit_ordering` / `require_sequenced_output` where applicable, and/or
     * apply `order_by` (knowing it’s an accumulator). ([Apache Arrow][7])

---

## Optional: Substrait as an alternate “authoring surface”

If you ever want to generate plans outside of the `Declaration` API (or share plans across engines), PyArrow also supports executing Substrait plans and reading results as a `RecordBatchReader` via `pyarrow.substrait.run_query`. ([Apache Arrow][14])

This is *not required* for your Acero-first approach, but it’s the “escape hatch” if you want a standardized IR for plans.

---


[1]: https://arrow.apache.org/docs/python/api/acero.html "Acero - Streaming Execution Engine — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/11.0/cpp/streaming_execution.html "Acero: A C++ streaming execution engine — Apache Arrow v11.0.0"
[3]: https://arrow.apache.org/docs/cpp/acero/overview.html "Acero Overview — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.Declaration.html "pyarrow.acero.Declaration — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Expression.html?utm_source=chatgpt.com "pyarrow.dataset.Expression — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.TableSourceNodeOptions.html?utm_source=chatgpt.com "pyarrow.acero.TableSourceNodeOptions - Apache Arrow"
[7]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.ScanNodeOptions.html "pyarrow.acero.ScanNodeOptions — Apache Arrow v22.0.0"
[8]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.FilterNodeOptions.html "pyarrow.acero.FilterNodeOptions — Apache Arrow v22.0.0"
[9]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.ProjectNodeOptions.html "pyarrow.acero.ProjectNodeOptions — Apache Arrow v22.0.0"
[10]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.AggregateNodeOptions.html "pyarrow.acero.AggregateNodeOptions — Apache Arrow v22.0.0"
[11]: https://arrow.apache.org/docs/cpp/api/acero.html "Streaming Execution (Acero) — Apache Arrow v22.0.0"
[12]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.HashJoinNodeOptions.html "pyarrow.acero.HashJoinNodeOptions — Apache Arrow v22.0.0"
[13]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.OrderByNodeOptions.html "pyarrow.acero.OrderByNodeOptions — Apache Arrow v22.0.0"
[14]: https://arrow.apache.org/docs/python/api/substrait.html?utm_source=chatgpt.com "Substrait — Apache Arrow v22.0.0"

Absolutely — this is *the* workhorse pattern for turning “row facts” into “edge facts” without Python loops.

Below is an exhaustive deep dive of the **list explode** primitive you called out in `compute_revamp.md`: using `pc.list_flatten` + `pc.list_parent_indices` + `pc.take` to turn `src_id + list<dst_id>` into an edges table.

---

## 1) Why “list explode” is the canonical edge-builder

In graph construction, the most common shape is:

* **one parent row** (a function / symbol / file / module / node)
* **zero-to-many children** (callees / references / imports / edges)
* plus optional per-edge attributes (callsite span, confidence, kind, etc.)

If you store those relationships as Arrow **`list<T>` columns**, you get:

* compact columnar representation,
* cheap scan/filter/project on the parent rows,
* and then one fast “unnest” step to produce edges.

Your revamp doc explicitly frames it as the replacement for “iterate rows → append dicts” style edge building.

---

## 2) The three kernels: exact semantics you must internalize

### `pc.list_flatten(lists)`

“Flatten list values”: removes the **top level** of list nesting (or recursively flattens if requested). It supports “list-like type (lists, list-views, and fixed-size lists)”. **Top-level null list values do not emit anything**. ([Apache Arrow][1])

In C++ terms: it appends **all values in the list child array (including nulls)**, but **discards null parent lists**. ([Apache Arrow][2])

### `pc.list_parent_indices(lists)`

Returns, for every emitted child value, the **index of the parent row** it came from. Input must be “list-like or list-view”. ([Apache Arrow][3])

In Arrow’s compute reference, `list_parent_indices` outputs **Int64**. ([Apache Arrow][2])

Important edge case: indices for **null lists can still appear** if they are “non-empty null lists”; and for **list-view**, unused child values can show up as nulls depending on view usage. ([Apache Arrow][2])

### `pc.take(values, indices)`

This is the replication step: for each index `i` in `indices`, the `indices[i]`-th element of `values` is appended to the output. ([Apache Arrow][2])

---

## 3) The base algorithm (one list column)

This is the exact pattern from your revamp doc, with a few production-grade additions (type hints, chunk normalization hook, and a “zero edges” safe path).

```python
from __future__ import annotations
import pyarrow as pa
import pyarrow.compute as pc

def explode_src_dst(
    src_ids: pa.Array | pa.ChunkedArray,
    dst_lists: pa.Array | pa.ChunkedArray,   # list<T> / large_list<T> / list_view<T> / fixed_size_list<T>
) -> pa.Table:
    # 1) For each emitted child element, which parent row did it come from?
    parent_idx = pc.list_parent_indices(dst_lists)   # Int64 indices (Arrow compute ref)
    # 2) Flatten the child values
    dst_flat = pc.list_flatten(dst_lists)            # child values; null parent lists emit nothing
    # 3) Repeat src_id to match flattened children
    src_rep = pc.take(src_ids, parent_idx)

    return pa.table({"src_id": src_rep, "dst_id": dst_flat})
```

This deletes entire classes of Python loops for call edges, import edges, reference edges, etc. 

---

## 4) Exploding *with extra columns* (what you’ll actually do in CodeIntel)

In practice your “parent rows” usually carry scalar context you want on every edge:

* `repo_id`, `commit_id`, `file_id`
* `src_symbol_id` (same as `src_id`)
* `kind` (call/import/ref)
* maybe a `src_span` or `src_node_id`

You replicate those scalars with the **same `parent_idx`**:

```python
def explode_edges_table(
    t: pa.Table,
    *,
    src_col: str,
    dst_list_col: str,
    repeat_cols: tuple[str, ...] = (),      # scalar cols to repeat per edge
) -> pa.Table:
    # (Optional) reduce chunk overhead if you tend to have many tiny chunks
    # t = t.combine_chunks()

    dst_lists = t[dst_list_col]
    parent_idx = pc.list_parent_indices(dst_lists)
    dst_flat   = pc.list_flatten(dst_lists)

    out = {
        "src_id": pc.take(t[src_col], parent_idx),
        "dst_id": dst_flat,
    }
    for c in repeat_cols:
        out[c] = pc.take(t[c], parent_idx)

    return pa.table(out)
```

---

## 5) The critical “graph reality”: per-edge attributes are often list-aligned

Common call-edge shape:

* `callee_ids: list<int64>`
* `callsite_spans: list<struct<start:int32,end:int32>>`
* `call_kind: list<utf8>` (optional)
* `confidence: list<float32>` (optional)

These are **list-aligned per parent row**: every child edge has a span, kind, etc.

### Explode list-aligned columns

You flatten each list column separately. But you **must validate** they align (lengths match per row), otherwise you can’t safely zip them.

Use `pc.list_value_length`:

* emits the length for each non-null list
* emits null for null lists ([Apache Arrow][4])

```python
def explode_edges_with_aligned_lists(
    t: pa.Table,
    *,
    src_col: str,
    dst_list_col: str,
    aligned_list_cols: tuple[str, ...],  # each list col must have same per-row length as dst_list_col
    repeat_cols: tuple[str, ...] = (),
) -> tuple[pa.Table, pa.Table]:
    dst_lists = t[dst_list_col]
    parent_idx = pc.list_parent_indices(dst_lists)

    # --- validate per-row list lengths match (tolerant mode produces error rows)
    dst_len = pc.list_value_length(dst_lists)
    bad = None
    for c in aligned_list_cols:
        clen = pc.list_value_length(t[c])
        eq = pc.equal(dst_len, clen)  # nulls propagate: null == null -> null
        eq = pc.fill_null(eq, False)  # treat null as mismatch by default policy
        bad = pc.or_(pc.invert(eq), bad) if bad is not None else pc.invert(eq)

    # Build error rows at parent-row granularity
    bad_rows = t.filter(bad) if bad is not None else t.slice(0, 0)

    # Filter to good parent rows before explode (so alignment is guaranteed)
    good_parent = t.filter(pc.invert(bad)) if bad is not None else t

    dst_lists_g = good_parent[dst_list_col]
    parent_idx_g = pc.list_parent_indices(dst_lists_g)

    out = {
        "src_id": pc.take(good_parent[src_col], parent_idx_g),
        "dst_id": pc.list_flatten(dst_lists_g),
    }
    for c in repeat_cols:
        out[c] = pc.take(good_parent[c], parent_idx_g)

    # Flatten aligned per-edge list columns
    for c in aligned_list_cols:
        out[c] = pc.list_flatten(good_parent[c])

    good_edges = pa.table(out)
    return good_edges, bad_rows
```

**Why filter parent rows *before* explode?** Because once you explode you lose the clean “one row = one problem” mapping; handling alignment errors is vastly simpler at the parent row level.

---

## 6) Null semantics (this is where people get bitten)

### Parent list is null vs empty list

* `list_flatten`: “Null list values do not emit anything to the output.” ([Apache Arrow][1])
  So a **null list** behaves like “produce zero edges” — which might be **wrong** for your semantics if null means “unknown / failed extraction”.

**Best practice:** treat `is_null(dst_lists)` as an *error at finalize gate*, not as “no edges”.

### Child elements can be null

C++ semantics: `list_flatten` appends all child values, including nulls; it only discards *null parent lists*. ([Apache Arrow][2])
So you can get edges where `dst_id` is null.

**Policy choices:**

* Drop `dst_id is null` edges (common).
* Or keep them but route to an `error_edges` table with `error_code="NULL_DST"`.

### The weird corner: “non-empty null lists”

Arrow notes: `list_parent_indices` can still include indices of null lists “if they are non-empty null lists”. ([Apache Arrow][2])
You can make your explode robust by explicitly filtering edges where the parent list is valid:

```python
parent_valid = pc.is_valid(dst_lists)          # per parent row
parent_valid_rep = pc.take(parent_valid, parent_idx)
# then filter edges with parent_valid_rep == True
```

This is usually overkill if you control your types (and you should), but it’s a great “defensive” helper knob.

---

## 7) Recursive flattening (rarely needed for edges, but good to know)

`pc.list_flatten(..., recursive=True)` will flatten nested lists until a non-list array is formed. ([Apache Arrow][1])

For graphs, you almost always want **one-level** explode:

* adjacency lists are “list of neighbors”, not “list of list of neighbors”.
* recursive flattening can silently change meaning (and destroy the parent mapping you intended).

So: default `recursive=False`, and only enable it when the *data model* is truly nested.

---

## 8) Performance & throughput notes (the “speed” part)

Even though you said memory isn’t the concern, these still matter for throughput:

* **Compute kernels operate on Arrow buffers in C++**; the explode pattern is three linear passes over buffers (parent indices, flatten, take).
* **Chunking matters**: many small chunks = overhead per kernel invocation. Your revamp doc explicitly recommends `combine_chunks()` when memory isn’t a concern.
* **Compute `parent_idx` once** and reuse it to replicate multiple scalar columns (cheap win).
* Prefer “explode after pushdown”: use Acero/dataset scan to reduce parent rows and columns *before* exploding (otherwise you explode work you’ll later drop).

---

## 9) Where this sits in your “list explode + Acero plan + finalize gate” pipeline

Acero doesn’t currently give you a clean “unnest” node equivalent, and `project` expressions are elementwise (explode changes row count), so the pragmatic pattern is:

1. **Acero plan**: scan/filter/project minimal columns (including the list column).
2. **Materialize** (table or reader→table batches).
3. **Explode** using these kernels.
4. **Finalize gate**: schema align, invariant checks, error routing (strict/tolerant).

That keeps the “explode” logic as one reusable helper (and prevents calc sprawl).

---

## 10) Recommended “standard helper” surface (so you don’t re-implement it 12 times)

If I were baking this into `src/codeintel/build/tabular/arrow_compute.py`, I’d make **one** canonical API:

* `explode_edges(t, src_col, dst_list_col, repeat_cols=..., aligned_list_cols=..., policy=...) -> (good_edges, error_rows)`
* policies include:

  * how to treat null parent lists (`ERROR` vs `EMPTY`)
  * how to treat null child values (`DROP` vs `ERROR`)
  * alignment enforcement for list-aligned columns (`STRICT` vs `TOLERANT`)

That single helper tends to collapse a lot of bespoke edge-builder code (exactly as you noted in the revamp doc).

---


[1]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.list_flatten.html "pyarrow.compute.list_flatten — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/cpp/compute.html "Compute Functions — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.list_parent_indices.html "pyarrow.compute.list_parent_indices — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.list_value_length.html?utm_source=chatgpt.com "pyarrow.compute.list_value_length — Apache Arrow v22.0.0"

### What the “finalize gate” is supposed to buy you

Your revamp doc’s intent is: **every pipeline produces Arrow tables “best-effort,” and then one shared finalize gate does correctness + diagnostics**, so you don’t sprinkle bespoke checks everywhere. In strict mode it fails fast; in tolerant mode it **never throws**, and instead computes a **columnar `is_valid` mask** and splits into **`(good_table, error_table)`**, where the error table includes **`row_id` + `error_code` + key fields**.

That implies a single, reusable function with four jobs (exactly as listed): **schema align/cast**, **vectorized invariant checks**, **dedupe policy**, and **emit alignment + error artifacts**.

---

## 1) The contract: what inputs/outputs look like

### Inputs (minimum viable)

A finalize gate should accept:

* `table: pa.Table`
* `target_schema: pa.Schema` (what this dataset *must* look like after finalization)
* `key_fields: list[str]` (what you’ll copy into error rows for debugging)
* `mode: "strict" | "tolerant"` (behavior switch)

Optionally:

* `required_non_null: list[str]`
* `invariants: list[InvariantSpec]`
* `dedupe: DedupeSpec`
* `combine_chunks: bool` (since you’re optimizing for throughput)

### Outputs (recommended)

Return a single object (dataclass) containing:

* `good: pa.Table` (guaranteed schema + invariants)
* `errors: pa.Table` (row-level failures; always well-typed)
* `alignment: pa.Table` (1-row report of schema operations)
* `stats: pa.Table` (counts by error_code / stage)

Even if you don’t persist `alignment`/`stats` initially, designing the interface around them makes “fault tolerance as a structured artifact” real.

---

## 2) Phase A: Schema align + cast (make “shape” deterministic)

### 2.1 Column presence + ordering

Decide the policy:

* **Drop extras** (default for contract purity), or
* **Keep extras** (if you want pass-through debug columns), but *don’t* let downstream depend on them.

### 2.2 Add missing columns

For each missing field `f` in `target_schema`, add a null-filled column of exactly that type.

### 2.3 Cast types

You have two main tools:

* **Per-column cast** with `pyarrow.compute.cast(arr, target_type, safe=...)` (safe defaults to `True`). ([Apache Arrow][1])
* **Whole-table cast** with `Table.cast(target_schema, safe=..., options=...)` *but names and order must match the schema*. ([Apache Arrow][2])

For “finalize gate” I strongly prefer **per-column casts** because:

* you can produce **better alignment reports** (“col X cast int32→int64”),
* you can choose `safe=True` vs targeted `CastOptions`, and
* you can (in tolerant mode) catch/record failures at the column level without losing everything.

CastOptions exists if you need to allow certain “unsafe” conversions explicitly. ([Apache Arrow][3])

---

## 3) Phase B: Invariant checks (all vectorized, no Python row loops)

### 3.1 Build a single validity mask

The simplest “core” invariant is “required columns must be non-null” using `pc.is_valid`, which emits true iff the value is non-null. ([Apache Arrow][4])

You then combine it with any additional invariant masks (ranges, allowed sets, relational constraints, etc.) into one boolean `good_mask`.

### 3.2 Compute `row_id` deterministically

Use `pyarrow.arange(0, n)` to create a row index column you can attach to errors. ([Apache Arrow][5])

### 3.3 Split good vs bad

Use `pc.filter` to filter tables by boolean masks. ([Apache Arrow][6])

To extract bad row indices, `pc.indices_nonzero(bad_mask)` is extremely convenient. ([Apache Arrow][7])

### 3.4 Assign error codes without loops

For “one error code per row” (priority order), use nested `pc.if_else` (condition → choose left vs right). ([Apache Arrow][8])

When masks can contain nulls, normalize with `pc.fill_null(mask, False)` (or `True`, depending on policy). ([Apache Arrow][9])

---

## 4) Phase C: Dedupe policy (determinism lives here)

Even if upstream work is highly parallel and order is unstable, you can make outputs deterministic here by:

* sorting once (if you care),
* then applying a canonical “keep first row by key” grouped aggregation (your revamp doc already sketches this pattern elsewhere).

The finalize gate should own dedupe because:

* it’s part of “contract compliance,” and
* duplicates are often “data quality failures” worth reporting.

---

## 5) Phase D: Emit artifacts (the “structured fault tolerance” part)

### 5.1 Error table schema

Minimum error table columns:

* `row_id: int64` (original input row index)
* `error_code: string` (stable enum)
* `stage: string` (e.g., `"schema" | "invariant" | "dedupe"`)
* `key fields...` (copied from input; whatever helps locate the bad record)

This aligns directly with your doc’s requirement (`row_id` + `error_code` + key fields).

### 5.2 Alignment report (1-row)

One row containing (at least):

* counts: `num_rows_in`, `num_rows_good`, `num_rows_bad`
* `missing_cols_added: list<string>`
* `extra_cols_dropped: list<string>`
* `casts_applied: list<struct<col:string, from:string, to:string, safe:bool>>`

### 5.3 Stats table

`error_code → count` is a huge operational win for debugging and regression tests.

---

## 6) Reference implementation (repo-ready core)

This is a “golden” finalize gate skeleton that implements:

* schema align/cast
* required-non-null invariants
* a couple edge-like invariants (no null keys, no self-edge)
* strict vs tolerant behavior
* `good`/`errors` split + alignment/stats

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

import pyarrow as pa
import pyarrow.compute as pc


@dataclass(frozen=True)
class FinalizeResult:
    good: pa.Table
    errors: pa.Table
    alignment: pa.Table
    stats: pa.Table


def _ensure_columns_and_order(t: pa.Table, target: pa.Schema) -> tuple[pa.Table, list[str], list[str]]:
    missing: list[str] = []
    extra: list[str] = []

    target_names = [f.name for f in target]
    t_names = set(t.column_names)

    # Add missing columns
    for f in target:
        if f.name not in t_names:
            missing.append(f.name)
            t = t.append_column(f.name, pa.nulls(t.num_rows, type=f.type))

    # Drop extras (policy: drop)
    extra = [c for c in t.column_names if c not in target_names]
    if extra:
        keep = [c for c in t.column_names if c in target_names]
        t = t.select(keep)

    # Reorder to target schema
    t = t.select(target_names)
    return t, missing, extra


def _cast_to_schema(t: pa.Table, target: pa.Schema, *, safe: bool) -> tuple[pa.Table, list[tuple[str, pa.DataType, pa.DataType, bool]]]:
    casts: list[tuple[str, pa.DataType, pa.DataType, bool]] = []
    arrays = []
    names = []

    for f in target:
        col = t[f.name]
        if not col.type.equals(f.type):
            casts.append((f.name, col.type, f.type, safe))
            # compute.cast: safe defaults True; explicit here for clarity
            col = pc.cast(col, f.type, safe=safe)  # :contentReference[oaicite:13]{index=13}
        arrays.append(col)
        names.append(f.name)

    return pa.Table.from_arrays(arrays, names=names), casts


def finalize_edges_like(
    t: pa.Table,
    *,
    target_schema: pa.Schema,
    key_fields: Sequence[str],
    required_non_null: Sequence[str],
    mode: str,  # "strict" | "tolerant"
    combine_chunks: bool = True,
) -> FinalizeResult:
    if combine_chunks:
        t = t.combine_chunks()

    n_in = t.num_rows
    row_id = pa.arange(0, n_in)  # :contentReference[oaicite:14]{index=14}

    # ---- Phase A: schema align/cast
    try:
        t2, missing, extra = _ensure_columns_and_order(t, target_schema)
        t3, casts = _cast_to_schema(t2, target_schema, safe=True if mode == "strict" else True)
        # (Policy: even tolerant mode uses safe=True casts; if this fails, we treat it as a schema-stage error artifact.)
    except Exception as e:
        if mode == "strict":
            raise
        # tolerant fallback: emit a dataset-level error row (row_id = -1)
        err = pa.table(
            {
                "row_id": pa.array([-1], type=pa.int64()),
                "error_code": pa.array(["SCHEMA_ALIGN_OR_CAST_FAILED"], type=pa.string()),
                "stage": pa.array(["schema"], type=pa.string()),
                "message": pa.array([str(e)], type=pa.string()),
            }
        )
        empty_good = pa.Table.from_batches([], schema=target_schema)
        alignment = pa.table(
            {
                "num_rows_in": pa.array([n_in], pa.int64()),
                "num_rows_good": pa.array([0], pa.int64()),
                "num_rows_bad": pa.array([n_in], pa.int64()),
                "missing_cols_added": pa.array([missing], type=pa.list_(pa.string())),
                "extra_cols_dropped": pa.array([extra], type=pa.list_(pa.string())),
                "casts_applied": pa.array([[]], type=pa.list_(pa.string())),
            }
        )
        stats = pa.table({"error_code": pa.array(["SCHEMA_ALIGN_OR_CAST_FAILED"]), "count": pa.array([1], pa.int64())})
        return FinalizeResult(empty_good, err, alignment, stats)

    # ---- Phase B: invariants (vectorized)
    # required non-null columns
    good_mask = pa.array([True] * n_in)
    for c in required_non_null:
        m = pc.is_valid(t3[c])  # true iff non-null :contentReference[oaicite:15]{index=15}
        m = pc.fill_null(m, False)  # treat null as invalid :contentReference[oaicite:16]{index=16}
        good_mask = pc.and_(good_mask, m)

    # example edge invariants:
    # 1) no self-edges (src_id != dst_id) if those columns exist
    if "src_id" in t3.column_names and "dst_id" in t3.column_names:
        neq = pc.not_equal(t3["src_id"], t3["dst_id"])
        neq = pc.fill_null(neq, False)
        good_mask = pc.and_(good_mask, neq)

    bad_mask = pc.invert(good_mask)

    # error_code: priority order (minimal version)
    # (You’ll typically build several bad_* masks and then nested if_else them.)
    null_key_bad = pa.array([False] * n_in)
    for c in required_non_null:
        null_key_bad = pc.or_(null_key_bad, pc.invert(pc.fill_null(pc.is_valid(t3[c]), False)))

    error_code = pc.if_else(  # :contentReference[oaicite:17]{index=17}
        null_key_bad,
        "NULL_REQUIRED_FIELD",
        pa.scalar(None, type=pa.string()),
    )

    # ---- Split
    good = pc.filter(t3, good_mask)  # filter supports table-like inputs :contentReference[oaicite:18]{index=18}

    bad_idx = pc.indices_nonzero(bad_mask)  # :contentReference[oaicite:19]{index=19}
    errors_cols = {
        "row_id": pc.take(row_id, bad_idx),
        "error_code": pc.take(error_code, bad_idx),
        "stage": pa.array(["invariant"] * int(bad_idx.length()), type=pa.string()),
    }
    for k in key_fields:
        if k in t3.column_names:
            errors_cols[k] = pc.take(t3[k], bad_idx)

    errors = pa.table(errors_cols)

    # ---- Phase C: dedupe (optional): apply to good only (not shown here)

    # ---- Phase D: artifacts
    alignment = pa.table(
        {
            "num_rows_in": pa.array([n_in], pa.int64()),
            "num_rows_good": pa.array([good.num_rows], pa.int64()),
            "num_rows_bad": pa.array([errors.num_rows], pa.int64()),
            "missing_cols_added": pa.array([missing], type=pa.list_(pa.string())),
            "extra_cols_dropped": pa.array([extra], type=pa.list_(pa.string())),
            "casts_applied": pa.array([[f"{c}:{str(fr)}->{str(to)} safe={s}" for (c, fr, to, s) in casts]], type=pa.list_(pa.string())),
        }
    )

    # stats: error_code counts
    if errors.num_rows:
        grp = errors.group_by(["error_code"]).aggregate([("row_id", "count")])
        grp = grp.rename_columns(["error_code", "count"])
        stats = grp
    else:
        stats = pa.table({"error_code": pa.array([], pa.string()), "count": pa.array([], pa.int64())})

    if mode == "strict" and errors.num_rows:
        raise ValueError(f"Finalize strict mode: {errors.num_rows} invalid rows")

    return FinalizeResult(good=good, errors=errors, alignment=alignment, stats=stats)
```

This matches the doc’s core behavior: **strict mode fails fast**, **tolerant mode emits `(good, errors)` based on a columnar mask**, and consolidate contract enforcement in one place.

---

## 7) Two “next-level” refinements worth standardizing

### A) Multi-error rows (one record can violate multiple invariants)

Instead of choosing a single `error_code` per row, build **one error table per invariant** using `indices_nonzero`, attach `error_code="INV_X"`, then `pa.concat_tables([...])`. It’s still fully vectorized and dramatically improves debugging.

### B) Stage-aware reporting

Make `stage` explicit (`"schema" | "invariant" | "dedupe"`) so you can answer:

* “Are we failing mostly due to schema drift or due to data validity?”
* “Did a refactor change validity rates?”

---


[1]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.cast.html?utm_source=chatgpt.com "pyarrow.compute.cast — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/python/generated/pyarrow.Table.html?utm_source=chatgpt.com "pyarrow.Table — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.CastOptions.html?utm_source=chatgpt.com "pyarrow.compute.CastOptions — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.is_valid.html?utm_source=chatgpt.com "pyarrow.compute.is_valid — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/dev/python/generated/pyarrow.arange.html?utm_source=chatgpt.com "pyarrow.arange — Apache Arrow v23.0.0.dev234"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.filter.html?utm_source=chatgpt.com "pyarrow.compute.filter — Apache Arrow v22.0.0"
[7]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.indices_nonzero.html?utm_source=chatgpt.com "pyarrow.compute.indices_nonzero — Apache Arrow v22.0.0"
[8]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.if_else.html?utm_source=chatgpt.com "pyarrow.compute.if_else — Apache Arrow v22.0.0"
[9]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.fill_null.html?utm_source=chatgpt.com "pyarrow.compute.fill_null — Apache Arrow v22.0.0"

Below is a deep dive on **PyArrow Dataset scanning + pushdown** as the “free speed lever” in your compute revamp: **do less IO, earlier** (prune files/row-groups, read fewer columns, stream batches with good readahead), and only then hand results to Acero / compute kernels.

---

## 1) The mental model: Dataset → Fragments → Scanner → Batches

* `ds.dataset(...)` is the high-level entry point: it can crawl directories, understand partitioned layouts, normalize schemas, and is explicitly designed for **predicate pushdown + projection + parallel reading**. ([Apache Arrow][1])
* A `Dataset` is a collection of **fragments** (often files / row-group addressable units). Partitioning can accelerate queries that touch only some partitions/files. ([Apache Arrow][2])
* A `Scanner` is the “materialized scan operation”: it binds `columns/filter/batch_size/readahead/threads` and produces RecordBatches / a Reader / a Table. ([Apache Arrow][3])

Key take-away: **“pushdown” lives at Scanner/Dataset level**. If you do `dataset.to_table()` with no args and then filter/project afterward, you’ve already paid the IO + decode cost you were trying to avoid.

---

## 2) Dataset construction: get discovery + partition semantics right

### 2.1 `ds.dataset(...)` (and why it’s worth standardizing)

The `ds.dataset(...)` API supports:

* single file, directory (recursive discovery), explicit file lists, union datasets, and even in-memory tables/batches/readers (creating an `InMemoryDataset`). ([Apache Arrow][1])
  It’s the API Arrow explicitly positions for “optimized reading with predicate pushdown (filtering rows), projection (selecting columns), parallel reading…” ([Apache Arrow][1])

### 2.2 Discovery knobs that matter in real repos

If you’re scanning directories, file discovery options can dominate “startup time”:

* **`exclude_invalid_files=True`** does per-file validation but “incur[s] IO for each file in a serial and single threaded fashion”; turning it off can be much faster but risks scan-time errors later. ([Apache Arrow][4])
* **`selector_ignore_prefixes`** (defaults like `['.', '_']`) avoids crawling temp/hidden files. ([Apache Arrow][4])
* **`partition_base_dir`** matters if your dataset is nested and you only want partition parsing under a certain root; files outside won’t get partition info. ([Apache Arrow][4])

These are exactly the knobs you want behind a single “dataset open” helper so you don’t re-learn these costs across every pipeline.

---

## 3) Pushdown #1: Partition pruning (skip whole files)

Arrow’s dataset guide shows the cleanest win:

* If your Parquet files are written partitioned (Hive-style `key=value` directories), Arrow can re-attach partition columns at scan time, and **filtering on partition keys can “avoid loading files altogether.”** ([Apache Arrow][5])

Example (conceptual): if your call-edges are stored under `kind=call/` vs `kind=import/`, a filter on `kind == "call"` can prune entire directories/files before any Parquet decoding.

Also note: `Dataset.get_fragments(filter=...)` can be used as a *planning/debug step* to see which fragments/files match, using either partition information or Parquet stats. ([Apache Arrow][2])

**CodeIntel implication:** if you control how you write your intermediate datasets, *partitioning is an architectural performance choice*, not a storage detail.

---

## 4) Pushdown #2: Row-group pruning (Parquet statistics) + fallback filtering

In the Scanner docs, Arrow is explicit about the filter behavior:

> `filter`: if possible, it’s pushed down using partition info or internal metadata (e.g., Parquet statistics); otherwise Arrow **filters loaded RecordBatches before yielding**. ([Apache Arrow][3])

So you get three tiers:

1. **Partition pruning** (skip files entirely)
2. **Row-group pruning** via Parquet statistics (skip chunks inside files)
3. **Post-read filtering** on batches (still vectorized, but you already paid IO+decode)

**Practical rule:** write filters that *can* prune: simple comparisons and conjunctions on real columns (and partition keys), not “filter after reading into Python.”

---

## 5) Pushdown #3: Projection (read fewer columns, earlier)

This is the other huge win: `Scanner.columns` isn’t just cosmetic.

* `columns` can be a list of names (preserving order/duplicates) **or** a dict `{new_name: expression}` for advanced projections. ([Apache Arrow][3])
* Crucially: projected columns “will be passed down … to avoid loading, copying, and deserializing columns that will not be required further down the compute chain.” ([Apache Arrow][3])

That sentence is the “free speed lever” in one line: you want your pipeline to decide *up front* which 6 columns it needs, not read 60 and drop 54 later.

### Special fields for observability (high value in pipelines)

Projections can reference:

* `__batch_index`, `__fragment_index`, `__last_in_fragment`, `__filename` ([Apache Arrow][3])

This is extremely useful for:

* emitting “file provenance” into tolerant-mode error tables,
* diagnosing skew (“one fragment produces 90% of bad rows”),
* building reproducible debug repros.

---

## 6) The Scanner control plane: every knob you should care about

You’ll standardize around:

```python
scanner = ds.Scanner.from_dataset(
    dataset,
    columns=...,              # list[str] or dict[str, Expression]
    filter=...,               # Expression
    batch_size=131_072,       # default
    batch_readahead=16,       # default
    fragment_readahead=4,     # default
    fragment_scan_options=...,# format-specific
    use_threads=True,
    cache_metadata=True,
    memory_pool=...,
)
```

All of these parameters (and their meanings) are documented in `pyarrow.dataset.Scanner`. ([Apache Arrow][3])

### What each knob does (and when to tune it)

* **`batch_size`** (default 131,072 rows): max row count per RecordBatch. Reduce if batches are too large / spiky; increase when overhead dominates and you’re throughput-bound. ([Apache Arrow][3])
* **`batch_readahead`**: number of batches to read ahead *within a file*. Increases RAM but can improve IO utilization. ([Apache Arrow][3])
* **`fragment_readahead`**: number of files/fragments to read ahead. Same tradeoff: more RAM, often better throughput. ([Apache Arrow][3])
* **`use_threads`**: “maximum parallelism” based on CPU cores. ([Apache Arrow][3])
* **`cache_metadata`**: caches metadata during scanning to speed up repeated scans. ([Apache Arrow][3])
* **`memory_pool`**: choose the allocator used for scan buffers (useful if you later want to instrument or constrain allocations). ([Apache Arrow][3])

### “Cheap planning” helpers you should use a lot

* `dataset.count_rows(filter=...)` uses the same pushdown semantics (partition/stats when possible), so you can cheaply compute “how big is this query?” before you run heavy downstream work. ([Apache Arrow][2])
* `dataset.get_fragments(filter=...)` to log “how many files will this touch?” using partition/stats pruning. ([Apache Arrow][2])

---

## 7) Output surfaces: streaming vs materialization

From the Scanner methods list:

* `to_reader()` → `RecordBatchReader`
* `to_batches()` → iterate RecordBatches
* `scan_batches()` → batches *with corresponding fragments* (handy for provenance)
* `to_table()` → materialize a `pa.Table` ([Apache Arrow][3])

**CodeIntel rule of thumb:**

* Use **`to_reader()` / `to_batches()`** when the next stage can stream (or you want bounded memory behavior).
* Use **`to_table()`** at a deliberate boundary (e.g., before your finalize gate).

---

## 8) Parquet-specific scan options: where IO performance is won

If you’re on Parquet (you almost certainly are), `ParquetFragmentScanOptions` is the main format-specific hook.

Notably:

* **`pre_buffer`** can coalesce reads and issue them in parallel using a background IO thread pool; it’s called out as helpful on **high-latency filesystems (S3/GCS)** and “prioritize speed over minimal memory usage.” ([Apache Arrow][6])
* There are also options for **page checksum verification**, **decryption**, and **reading Parquet logical types as Arrow extension types** (via `arrow_extensions_enabled`). ([Apache Arrow][6])

For your “speed first, memory not a concern” stance, `pre_buffer` is exactly the kind of knob you’d want to evaluate in benchmarks.

---

## 9) Interop boundary: when scanning should feed Acero vs “escape hatch” engines

Because your revamp is “Acero plan + explode + finalize gate”, the clean integration is:

* Use Dataset scanning to **prune** and **project** early (cheap wins).
* Then either:

  * feed the Dataset directly into an Acero `scan` node (so Acero continues the pipeline), or
  * scan → reader/table and do the rest in Arrow compute kernels / explode helpers / finalize gate.

Even if you later use DuckDB/DataFusion for more complex relational work, you still want **Dataset pushdown first**, because it shrinks the data *before* any engine does heavier transforms.

---

## 10) A “golden” helper you’ll want in your repo

If you only standardize one thing, make it a scanner builder:

```python
def make_scanner(dataset, *, columns, filter, batch_size=131_072,
                 batch_readahead=16, fragment_readahead=4,
                 fragment_scan_options=None,
                 use_threads=True, cache_metadata=True, memory_pool=None):
    return ds.Scanner.from_dataset(
        dataset,
        columns=columns,
        filter=filter,
        batch_size=batch_size,
        batch_readahead=batch_readahead,
        fragment_readahead=fragment_readahead,
        fragment_scan_options=fragment_scan_options,
        use_threads=use_threads,
        cache_metadata=cache_metadata,
        memory_pool=memory_pool,
    )
```

Because it’s the one place you can enforce:

* pushdown-friendly filter/projection conventions,
* default readahead tuned for your workloads,
* standardized provenance columns (`__filename`, etc.), and
* consistent “planning telemetry” (`get_fragments`, `count_rows`) logging.

All of the semantics above come straight from the Dataset/Scanner APIs. ([Apache Arrow][3])

---


[1]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.dataset.html "pyarrow.dataset.dataset — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Dataset.html "pyarrow.dataset.Dataset — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html "pyarrow.dataset.Scanner — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.FileSystemFactoryOptions.html "pyarrow.dataset.FileSystemFactoryOptions — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/dataset.html "Tabular Datasets — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.ParquetFragmentScanOptions.html "pyarrow.dataset.ParquetFragmentScanOptions — Apache Arrow v22.0.0"

Below is a “compute vocabulary” deep dive in the exact sense you want for CodeIntel: **a small, standardized subset of Arrow expressions + kernels that you use everywhere**, so pipelines become *plan → execute → finalize* with minimal bespoke code.

---

## 0) Two vocabularies you need to standardize: Expressions vs Kernels

### A) **Expressions (deferred)**

Used for **Dataset pushdown** and **Acero filter/project** authoring. An `Expression` is “a logical expression to be evaluated against some input.” You build them with `pc.field()` / `pc.scalar()`, compare with `<, <=, ==, ...`, and combine with `& | ~` (not `and/or/not`).

Key point: `pc.scalar()` is **not** `pyarrow.scalar()` — `pc.scalar()` returns an `Expression` suitable for compute predicates and dataset filtering.

### B) **Kernels (eager)**

Used for **finalization, QA masks, explode patterns, selection, normalization**, etc. Arrow compute “functions represent compute operations… implemented by one or several kernels” depending on input types, and functions are stored in a global registry.

**Your standard library should explicitly separate these:**

* `expr_*` helpers that return `pc.Expression`
* `k_*` helpers that operate on arrays/tables and return arrays/tables/masks

This single separation prevents the common failure mode: accidentally constructing *eager arrays* when you meant *pushdown expressions* (or vice versa).

---

## 1) Expression vocabulary (what you allow in scan/filter/project)

### 1.1 Field references (including nested)

Use `pc.field(...)` (or `ds.field(...)`) to reference a dataset column. Arrow notes it stores only the field name, and nested references are allowed by passing multiple names or a tuple.

```python
import pyarrow.compute as pc

f_kind = pc.field("kind")
f_span_start = pc.field(("span", "start"))   # nested struct field path
```

### 1.2 Scalars (literals) for expressions

Use `pc.scalar(...)` to create scalar expressions usable in filtering and compute expressions.

```python
call = pc.scalar("call")
```

### 1.3 Comparison + boolean composition (pushdown-friendly core)

Arrow explicitly supports:

* comparisons: `<, <=, ==, >=, >`
* boolean combination: `&` (and), `|` (or), `~` (not) — *not* Python `and/or/not`

```python
pred = (pc.field("kind") == "call") & (pc.field("confidence") >= 0.8)
```

### 1.4 Membership predicates

Use `Expression.isin(...)` (which builds an `is_in(...)` expression). Arrow documents `.isin()` as a predicate creator and shows it producing an `is_in(...)` expression.

```python
pred = pc.field("edge_kind").isin(["call", "import"])
```

### 1.5 Expression methods worth standardizing

From the Expression API:

* `.cast(...)` (creates expression equivalent to calling cast)
* `.is_null(...)`, `.is_valid(...)`, `.is_nan()`
* `.to_substrait(...)` / `.from_substrait(...)` if you ever want serialized expressions

---

## 2) Kernel toolbox: the compute surface you should actually standardize

Arrow has *a lot* of kernels. Don’t “expose everything.” Standardize a **curated set** that covers 90% of your transforms and your QA/error-routing patterns.

Below is the catalog I’d standardize for your project (with the key kernels and the “why”):

---

## 2.1 Null handling + conditionals (the backbone of fault tolerance)

### `pc.is_valid`

Returns true iff a value is non-null.

### `pc.fill_null`

Replace each null in `values` with `fill_value` (scalar or array).

### `pc.coalesce`

Select the first non-null value row-wise across multiple inputs.

### `pc.if_else`

Choose between `left` and `right` based on a boolean condition; nulls in the condition “will be promoted to the output.”

### `pc.case_when`

Multi-branch conditional (your “error_code priority ladder” becomes clean).

**Standard helpers you want:**

* `k_valid_mask(required_cols)` → combined mask
* `k_coalesce(*cols)` → your canonical null-fallback
* `k_error_code(mask→code priority)` using `case_when` or nested `if_else`

---

## 2.2 Type normalization + safe casting (schema compliance)

### `pc.cast(arr, target_type, safe=True, options=...)`

`safe=True` checks for overflow/unsafe conversions.

### `CastOptions`

Exists for finer control; Arrow exposes safe/unsafe constructors.

**Standard helpers you want:**

* `k_safe_cast(col, dtype)` (always `safe=True`)
* `k_cast_or_null(col, dtype)` (tolerant mode: replace cast-fail rows with nulls + error mask; implement with finalize-gate policy)
* `expr_cast(field, dtype)` using `Expression.cast(...)` when authoring plans

---

## 2.3 Set membership + categorical speedups

### `pc.is_in(values, value_set=..., skip_nulls=...)`

Membership test against a set of values.

### `pc.dictionary_encode`

Dictionary-encode array; does nothing if already dictionary. Useful for low-cardinality strings (kinds, labels).

### `pc.unique`

Distinct values; null is considered a distinct value.

**Standard helpers you want:**

* `k_is_in(col, python_values)` (wrap `SetLookupOptions` consistently; stable null policy)
* `k_dict_encode_if_worth_it(col)` (optional optimization gate for repeated group/join keys)

---

## 2.4 Selection, indexing, and “mask-driven routing” (QA + explode plumbing)

### `pc.filter(input, selection_filter, null_selection_behavior=...)`

Filters input by boolean mask; null behavior is controlled by options.

### `pc.take(data, indices, boundscheck=True)`

Select rows by integer indices; if an index is null, output is null at that position.

**Boundscheck warning:** `TakeOptions` documents that disabling boundscheck can make out-of-bounds behavior “undefined (the process may crash).”

### `pc.indices_nonzero(values)`

Returns indices where value is neither zero/false/null (super useful for turning masks into index vectors).

### `pc.replace_with_mask`

Replace positions selected by a boolean mask; important for “tolerant” pipelines that patch values instead of dropping rows.

**Standard helpers you want:**

* `k_split_good_bad(table, good_mask)` → `(good, bad_idx, bad_rows)`
* `k_take_rows(table, idx)` (single place to enforce `boundscheck=True`)
* `k_error_rows_from_mask(table, mask, code, key_fields)` (your finalize-gate building block)

---

## 2.5 Nested types: lists + structs (critical for CodeIntel metadata)

### Lists

* `pc.list_flatten(lists, recursive=False)` flattens list-like arrays; **null list values do not emit anything**.
* `pc.list_parent_indices(lists)` emits parent indices for each child value.
* `pc.list_value_length(lists)` emits length for each non-null list; null lists emit null.
* `pc.list_slice(...)` for per-row list slicing (useful for top-k edges, truncation policies).
* `pc.list_element(lists, index)` to grab the nth element per row (handy for “primary callee” heuristics).

### Structs

* `pc.make_struct(*args, field_names=...)` wraps arrays into a StructArray.
* `pc.struct_field(values, indices=...)` extracts struct children by index (recursively).

**Standard helpers you want:**

* `k_explode(src_col, dst_list_col, repeat_cols=..., aligned_list_cols=...)`
* `k_list_len(col)` (normalize null semantics: treat null list as error vs empty)
* `k_make_struct_named({...})` (consistent field naming + nullability)

---

## 2.6 Strings + regex + normalization (common in symbol/path hygiene)

Useful core kernels:

* `pc.match_substring_regex(strings, pattern, ...)` emits true iff regex matches; null inputs emit null.
* `pc.replace_substring_regex(strings, pattern, replacement, ...)` regex replace; nulls emit null.
* `pc.binary_join_element_wise(*strings, null_handling=...)` joins strings; has explicit null-handling policies.

**Standard helpers you want:**

* `k_norm_path(path)` (regex/replace slice/join; one canonical normalization pipeline)
* `k_regex_guarded_extract(...)` (tolerant mode: return extracted + error mask)

---

## 2.7 Sorting + determinism (your “make results stable” toolbox)

### `pc.sort_indices`

Computes indices that define a **stable sort**; nulls sort to end by default, configurable via `SortOptions`.

This is perfect for:

* stable dedupe (“sort by tie-breakers, then group/first”)
* reproducible outputs for golden snapshot tests

**Standard helpers you want:**

* `k_stable_sort_indices(table, sort_keys, null_placement=...)`
* `k_sort_table(table, sort_keys)` implemented as `take(table, sort_indices(...))`

---

## 2.8 Arithmetic: overflow and “checked” variants

Even basic arithmetic has sharp edges:

* `pc.add` notes integer results “wrap around on overflow” and suggests `add_checked` if you want overflow to return an error.

**Standard helpers you want:**

* `k_add_checked(...)` / `k_sub_checked(...)` etc (where correctness matters)
* `k_safe_divide(num, den)` (avoid div-by-zero with `if_else` + error mask)

---

## 2.9 Avoiding UDF sprawl (but knowing where it exists)

PyArrow supports user-defined compute functions, but Arrow marks the UDF API as **experimental**.

Given your throughput + determinism goals, the “best practice” is:

* **prefer built-in kernels**
* if you must UDF, isolate it behind a single helper and snapshot-test its behavior

---

## 3) The helper surface I’d actually create in your repo

### `tabular/expr_vocab.py` (plan-time)

* `E.field(name|path)` → expression
* `E.scalar(x)` → expression
* `E.in_(field, values)` → expression `.isin`
* `E.and_(*exprs)` / `E.or_(*exprs)` / `E.not_(expr)`
* `E.cast(expr, dtype)`
* `E.is_valid(expr)` / `E.is_null(expr)`

All of these are thin wrappers that enforce the “pushdown-safe subset.”

### `tabular/kernels.py` (run-time)

* **Masks + routing:** `valid_mask`, `split_good_bad`, `indices_nonzero`, `filter_table`, `take_rows`
* **Null tools:** `fill_null`, `coalesce`, `if_else`, `case_when`
* **Type tools:** `safe_cast`, `cast_with_error_mask`
* **Lists/structs:** `explode_edges`, `list_len`, `make_struct_named`, `struct_get`
* **Strings:** `regex_match`, `regex_replace`, `join_cols`
* **Determinism:** `stable_sort_indices`, `sort_table`

This is the library that will collapse most bespoke “calc sprawl.”

---

## 4) A few “golden rules” to enforce in CI

1. **Never use Python `and/or/not` with Expressions** (must be `& | ~`). Arrow calls this out explicitly.

2. **Don’t mix up scalars**

* `pyarrow.scalar()` is an in-memory Scalar
* `pyarrow.compute.scalar()` is an Expression for predicates/compute expressions

3. **Keep `take(boundscheck=True)`**
   Turning boundscheck off can crash the process if indices are out of bounds.

4. **Treat null list vs empty list as a policy choice**
   `list_flatten` drops null lists (emits nothing). That can silently hide extraction failures unless your finalize gate flags it.

---


Here’s the deep dive on **nested Arrow data modeling as a performance feature**—the core idea in `compute_revamp.md` is: *stop serializing semi-structured stuff into JSON blobs / Python objects; keep it in Arrow-native nested types (struct/list/map), and only flatten/serialize at boundaries*.

---

## 1) Why nested Arrow types beat JSON blobs in your pipeline

### The performance angle

* **JSON blobs force parse/serialize work** and destroy type information. Your doc explicitly calls JSON “expensive and brittle” and recommends `struct<...>` (or `map<...>`) for “extras.”
* **Arrow nested types stay columnar**: compute kernels operate on buffers; scanners can project/filter early; and you can validate/cast once at the finalize gate.

### The code-sprawl angle

If you store “extras” as a struct, you eliminate the “build dict → json.dumps per row” pattern and keep transformations in a small number of reusable kernels (build struct, extract field, flatten at boundary).

---

## 2) Struct-first metadata (“extras”): create, use, and flatten

Your revamp doc’s canonical move is: **collapse `extras_json` into a typed struct early**.

### 2.1 Creating a struct column (the canonical helper)

Use `pyarrow.compute.make_struct(...)` to wrap arrays/scalars into a `StructArray` and name the fields. The API supports `field_names`, `field_nullability`, and `field_metadata`. ([Apache Arrow][1])

```python
import pyarrow.compute as pc

extras = pc.make_struct(
    pc.field("repo_id"),
    pc.field("parse_version"),
    field_names=["repo_id", "parse_version"],
    # field_nullability=[False, True],        # optional
    # field_metadata=[..., ...],              # optional
)
```

That’s exactly the pattern your doc calls out.

**Practical recommendation for CodeIntel:** define a *single* stable “extras struct” per dataset contract, even if it’s sparse (many nulls). Sparse is fine; schema stability is the win.

---

### 2.2 Extracting struct fields (keep it vectorized)

Use `pyarrow.compute.struct_field(values, indices=...)` to extract nested children “recursively” by index. ([Apache Arrow][2])

```python
import pyarrow.compute as pc

repo_id = pc.struct_field(pc.field("extras"), indices=0)  # first field
```

(You can build a tiny helper that maps field-name → index from the schema so you never hardcode indices.)

---

### 2.3 Flattening structs at boundaries (not everywhere)

`Table.flatten()` turns **each struct column into one column per struct field**, leaving other columns unchanged. ([Apache Arrow][3])

This is ideal as a boundary tool:

* **Keep nested** internally (compute + QA + storage)
* **Flatten** only for:

  * UI/exports (CSV, JSON),
  * engines/consumers that don’t handle nested well,
  * or “presentation views” where wide columns are desirable.

```python
flat = table.flatten()
# extras.repo_id, extras.parse_version, ...
```

---

## 3) Lists: model “one-to-many” relationships without row loops

For graph workloads, lists are the natural intermediate representation:

* `callee_ids: list<int64>`
* `callsite_spans: list<struct<start:int32,end:int32>>`
* etc.

Your doc explicitly ties performance to “model the intermediate data correctly (lists/structs instead of Python objects / JSON blobs).”

### 3.1 The list family you should care about

#### `list_` (default) vs `large_list`

* `pa.large_list(value_type)` uses **64-bit offsets**; Arrow explicitly says you should prefer `list_()` unless you truly need > 2**31 elements. ([Apache Arrow][4])
* In practice for CodeIntel, **use `list_` almost always**.

#### `list_view` / `large_list_view` (important nuance)

Arrow exposes `list_view` types as an **alternative to ListType** and warns they “may not be supported by all Arrow implementations.” ([Apache Arrow][5])

The key semantic difference (from Arrow’s data model docs):

* **List**: offsets buffer only
* **ListView**: offsets **and sizes** buffers, allowing **out-of-order offsets** (i.e., “views” into an underlying values buffer). ([Apache Arrow][6])

**Implication:** list-view is great for “slicing/view” semantics, but for **persisted interchange** (Parquet datasets, cross-tool compatibility), default to **plain list** unless you have a specific reason.

### 3.2 Why you still care about list_view even if you don’t store it

Because many compute kernels accept “list-like” inputs, and Arrow explicitly positions list_view as a list-alternative type. In practice you’ll encounter list_view when:

* a library returns list views,
* you construct them deliberately for cheap slicing,
* or you do advanced buffer-level work.

So: your helpers should accept **list** *and* **list_view** inputs (type-check “list-like”), but your storage contracts should generally stick to **list**.

---

## 4) Map vs struct for “extras” and other semi-structured fields

Your doc suggests `struct<...>` or `map<...>` as the Arrow-native replacement for JSON strings.

### 4.1 Struct is the default for “known keys”

Use `struct` when:

* keys are known (even if most are null),
* you want stable schemas and easy downstream validation,
* you want to flatten to columns later.

Arrow’s struct type constructor is `pa.struct(fields)`; fields are named and are part of the type metadata. ([Apache Arrow][7])

### 4.2 Map is for true “dynamic keys”

Use `pa.map_(key_type, item_type, keys_sorted=False)` when:

* keys are not known ahead of time,
* you expect wide/variable metadata that would explode column count.

Map type docs: `pyarrow.map_` constructs a MapType, and MapType includes a `keys_sorted` property. ([Apache Arrow][8])

**But**: maps are harder to query like “normal columns.” From Arrow’s data model docs, `MapArray.keys` and `MapArray.items` are **flattened**, and to keep keys/items associated per row you need to regroup using the offsets (e.g., `ListArray.from_arrays(arr.offsets, arr.keys/items)`). ([Apache Arrow][6])

**Practical implication:** maps are great for storage of long-tail metadata, but *analytics often requires exploding them* (similar to list explode).

### 4.3 Recommended hybrid pattern for CodeIntel

* `extras: struct<...>` for stable, frequently-used fields (repo_id, parse_version, tool versions, confidence buckets, etc.)
* `extras_kv: map<string, string>` for “unknown/rare” fields you don’t want in the core schema

This gives you:

* stable contracts (struct),
* extensibility (map),
* and you can always “promote” a map key into the struct later once it becomes important.

---

## 5) Keep nested vs flatten: a boundary policy (not ad hoc decisions)

Here’s a concrete policy that prevents chaos:

### Keep nested when…

* the nested structure is used for **compute** (explode edges, validate aligned lists, manipulate spans),
* you want **schema stability** without creating 200 columns,
* you will store to Parquet and read back repeatedly (Arrow/Parquet handle nested well).

### Flatten when…

* you are exporting to JSON/CSV/UI,
* you’re feeding an engine/consumer that’s weak on nested types,
* you’re generating “presentation datasets” where each field should be its own top-level column (`Table.flatten()`). ([Apache Arrow][3])

This aligns perfectly with your revamp doc’s “only serialize to JSON at the boundary (export/UI).”

---

## 6) Schema evolution: unify schemas, add missing fields, promote types—deterministically

Nested types only pay off if you can **evolve contracts safely**.

### 6.1 `pyarrow.unify_schemas` (union-by-field-name)

`pa.unify_schemas(schemas, promote_options="default"|"permissive")` merges schemas by field name:

* `"default"`: only null unifies with another type
* `"permissive"`: types are promoted to a “greater common denominator”
  It also states the resulting schema inherits metadata from the **first input schema**. ([Apache Arrow][9])

This is your canonical “heterogeneous shards” tool.

### 6.2 Dataset-level schema inspection (fragment drift)

When you have many files/fragments, `FileSystemDatasetFactory.inspect(promote_options=...)` returns a common schema, with the same “default vs permissive” semantics. ([Apache Arrow][10])

### 6.3 The canonical evolution workflow

For “merge then compute” workflows:

```python
import pyarrow as pa

unified = pa.unify_schemas([t1.schema, t2.schema], promote_options="permissive")
t1a = t1.cast(unified)
t2a = t2.cast(unified)
merged = pa.concat_tables([t1a, t2a])
```

The core idea: **unify → cast → concat**, and then your finalize gate takes over to enforce the dataset contract.

---

## 7) What I would standardize in your repo (so nested stays clean)

### A) A tiny “nested modeling” module (`tabular/nested.py`)

* `make_extras_struct(table, spec)` → returns `StructArray` using `pc.make_struct` (with field metadata/nullability if you want) ([Apache Arrow][1])
* `get_struct_field(table_or_array, "extras", "repo_id")` → resolves index and calls `pc.struct_field` ([Apache Arrow][2])
* `flatten_for_export(table)` → `table.flatten()` and maybe rename columns consistently ([Apache Arrow][3])

### B) Contract rules for nested types

* For **lists** in contracts: prefer `list_` over `large_list` unless genuinely needed ([Apache Arrow][4])
* Avoid storing `list_view` / `large_list_view` in persisted datasets because they “may not be supported by all Arrow implementations” ([Apache Arrow][5])
* For **maps**: only for “dynamic metadata,” and build explicit “map explode” helpers if/when you need to query them heavily ([Apache Arrow][6])

---

## A concrete “CodeIntel-shaped” nested schema (example)

This is the kind of schema that makes your “list explode + finalize gate” flow trivial:

* `src_id: int64`
* `callee_ids: list<int64>`
* `callsite_spans: list<struct<start:int32, end:int32>>`
* `extras: struct<repo_id:int64, parse_version:int32, ...>` (nullable fields allowed)
* (optional) `extras_kv: map<string, string>`

Then your edge builder is:

1. scan/project only these columns
2. explode lists into edges
3. finalize (mask + error table)
4. flatten only if needed for exports

That’s exactly the modeling direction your revamp doc is pushing: *lists/structs instead of row loops and JSON blobs.*

---

If you want to continue sequentially after this: the next deep dive that usually unlocks everything is **“schema evolution + finalize gate rules specifically for nested columns”** (e.g., how you validate list-aligned columns, how you version struct fields, and what your promote/cast policies are for nested children).

[1]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.make_struct.html?utm_source=chatgpt.com "pyarrow.compute.make_struct — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.struct_field.html?utm_source=chatgpt.com "pyarrow.compute.struct_field — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/docs/python/generated/pyarrow.Table.html?utm_source=chatgpt.com "pyarrow.Table — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.large_list.html?utm_source=chatgpt.com "pyarrow.large_list — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.list_view.html?utm_source=chatgpt.com "pyarrow.list_view — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/docs/python/data.html?utm_source=chatgpt.com "Data Types and In-Memory Data Model - Apache Arrow"
[7]: https://arrow.apache.org/docs/python/generated/pyarrow.struct.html?utm_source=chatgpt.com "pyarrow.struct — Apache Arrow v22.0.0"
[8]: https://arrow.apache.org/docs/python/generated/pyarrow.map_.html?utm_source=chatgpt.com "pyarrow.map_ — Apache Arrow v22.0.0"
[9]: https://arrow.apache.org/docs/python/generated/pyarrow.unify_schemas.html?utm_source=chatgpt.com "pyarrow.unify_schemas — Apache Arrow v22.0.0"
[10]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.FileSystemDatasetFactory.html?utm_source=chatgpt.com "pyarrow.dataset.FileSystemDatasetFactory - Apache Arrow"


Here’s the “unlock everything” deep dive: **how to do schema evolution + finalize-gate rules when your columns are nested** (lists/structs/maps), without letting drift + ad-hoc fixes explode your codebase.

This is explicitly the direction your `compute_revamp.md` is pushing: **model intermediates as lists/structs/maps (not JSON/Python objects)** and route *all* correctness + diagnostics through one **finalize gate** that does schema align/cast + vectorized invariants + dedupe + error artifacts (strict vs tolerant).

---

## 1) What changes when schemas are nested

Arrow’s type system explicitly includes nested types (`list`, `map`, `struct`, …). ([Apache Arrow][1])
But the moment you go nested, schema evolution stops being “add a column” and becomes “change a *tree* of types.”

A “nested column” can drift in at least 4 ways:

1. **Top-level field presence** (missing/extra column)
2. **Top-level field type** (e.g., `int32 → int64`)
3. **Nested child presence** (struct field added/removed)
4. **Nested child type** (e.g., `extras.parse_version: int16 → int32`, or `callsite_spans: list<struct<…>>` evolves)

Your finalize gate already covers (1) and (2).
This deep dive is about (3) and (4): **how to evolve nested children without breaking everything, and how the finalize gate enforces rules + emits diagnostics.**

---

## 2) The core policy: “contracts evolve additively” + “casts are explicit”

### 2.1 Additive evolution by default

For nested structures, the safest “works forever” policy is:

* **Struct fields:** additive only (you can add new fields; you do not rename/remove/retcon types silently)
* **List element types:** do not change unless you *explicitly* define a promotion/cast rule
* **Map key/item types:** do not change (treat changes as a new column, or funnel to a stable “stringy” representation)

Why so strict? Because “permissive” auto-promotion can be convenient, but it can also silently change semantics. Arrow’s schema unification APIs explicitly distinguish strict vs permissive behavior. ([Apache Arrow][2])

### 2.2 All drift handling happens in one place

Your doc’s mandate is: *do not scatter drift handling across nodes; do it at the finalize gate.*

So: upstream nodes are allowed to be “best effort,” but they **must not** invent their own ad hoc schema fixes. They return Arrow data, then finalize gate normalizes.

---

## 3) Schema evolution mechanics: discover → unify → normalize

### 3.1 Discover the “actual” schema across files/fragments

If you’re reading from a directory of Parquet fragments, use the dataset factory inspection to get a **common schema**:

* `pyarrow.dataset.FileSystemDatasetFactory.inspect(promote_options="default"|"permissive")`

  * default: types must match exactly except null merges
  * permissive: types are promoted when possible ([Apache Arrow][3])

This is the correct pre-flight step for “do we have drift across shards?”

### 3.2 Unify multiple schemas in memory (same semantics)

For merging tables (e.g., plugin outputs, partitions, incremental runs), use:

* `pyarrow.unify_schemas(schemas, promote_options="default"|"permissive")` ([Apache Arrow][2])

Important details that matter operationally:

* Unification merges fields by name; type mismatches fail by default. ([Apache Arrow][2])
* The unified field inherits metadata from the schema where it was first defined, and field ordering is anchored to the first schema. ([Apache Arrow][2])

**Implication:** always pass your **canonical contract schema first** when unifying, so its metadata + ordering win.

### 3.3 Concatenate tables with controlled promotion

When concatenating, Arrow also supports a `promote_options` knob:

* `pyarrow.concat_tables(..., promote_options="permissive")` promotes types to a common denominator (beyond the default behavior). ([Apache Arrow][4])

**Recommendation:** don’t rely on `concat_tables` to do “schema evolution.” Instead:

1. unify schemas (with *your* policy),
2. normalize/cast each table to the contract (including nested),
3. concat.

---

## 4) Versioning strategy for nested “extras” structs

Your revamp doc explicitly recommends collapsing JSON into typed `struct<...>` and using `pc.make_struct`, `pc.struct_field`, and `table.flatten()` as needed.

### 4.1 The stable pattern: `extras` struct + explicit version markers

I’d standardize **two** version markers:

1. **Dataset contract version**: schema metadata (`schema.with_metadata({"contract": "...", "contract_version": "..."})`)
2. **Extras schema version**: inside the struct (`extras.extras_version: int16`)

Why both?

* Schema metadata is great for global contracts, but unification inherits metadata from the first schema, so you want “self-describing” payload too. ([Apache Arrow][2])
* The `extras_version` makes error tables + cross-run comparisons easier.

### 4.2 Struct evolution rules (strongly recommended)

For `extras: struct<...>`:

Allowed:

* **Add new nullable fields** (default null for older data)
* **Add new field metadata** (units, meaning, provenance)

Disallowed (treat as breaking change):

* Rename a field (that’s remove+add)
* Change a field’s type (unless you define an explicit allowed promotion)
* Change required↔optional semantics without bumping `extras_version`

Practical note: Arrow will happily represent “missing keys become null children” when constructing structs from Python dicts (good for additive evolution).

---

## 5) The hard part: casting and promoting *nested children*

### 5.1 Table.cast exists—but nested casting can be limited

`pyarrow.Table.cast(target_schema, safe=..., options=...)` exists. ([Apache Arrow][5])
However, **casting nested struct fields** has historically been incomplete. A common workaround is: extract children, cast them, rebuild the struct. ([Stack Overflow][6])

Also: projecting/selecting nested struct fields at read time has had limitations (people explicitly request nested projection support). ([GitHub][7])

**So your finalize gate should assume you may need to do nested casts manually** (at least for some shapes like list-of-struct).

### 5.2 The canonical “deep cast” pattern (struct/list/map)

You want a single helper that recursively transforms an Array/ChunkedArray from `actual_type → contract_type`.

Building blocks you’ll use:

* `pc.cast(..., safe=True)` for primitive casts ([Apache Arrow][5])
* `StructArray.from_arrays(...)` to rebuild structs from cast children ([Apache Arrow][8])

And remember: Arrow map is physically a list of `{key,item}` structs. ([Apache Arrow][9])
So “map casting” is really “cast the list-of-struct child buffers.”

#### Reference implementation sketch (recursive)

```python
import pyarrow as pa
import pyarrow.compute as pc

def deep_cast_array(arr: pa.Array | pa.ChunkedArray, target_type: pa.DataType) -> pa.Array | pa.ChunkedArray:
    t = arr.type

    # Fast path: identical
    if t.equals(target_type):
        return arr

    # List / LargeList / FixedSizeList
    if pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t):
        # Cast child values, then rewrap with original offsets
        # (For fixed_size_list, use its fixed size; for list/large_list, use offsets)
        # Note: chunked arrays require per-chunk handling.
        if isinstance(arr, pa.ChunkedArray):
            return pa.chunked_array([deep_cast_array(c, target_type) for c in arr.chunks], type=target_type)

        offsets = arr.offsets  # for list/large_list
        values_cast = deep_cast_array(arr.values, target_type.value_type)
        if pa.types.is_fixed_size_list(target_type):
            return pa.FixedSizeListArray.from_arrays(values_cast, target_type.list_size)
        else:
            return pa.ListArray.from_arrays(offsets, values_cast, type=target_type)

    # Struct
    if pa.types.is_struct(t):
        if isinstance(arr, pa.ChunkedArray):
            return pa.chunked_array([deep_cast_array(c, target_type) for c in arr.chunks], type=target_type)

        # Align by field name: missing fields → null, extra fields → dropped
        target_fields = {f.name: f for f in target_type}
        child_arrays = []
        child_fields = []
        for name, f in target_fields.items():
            if name in arr.type:
                child = arr.field(name)
                child_cast = deep_cast_array(child, f.type)
            else:
                child_cast = pa.nulls(len(arr), type=f.type)
            child_arrays.append(child_cast)
            child_fields.append(f)

        return pa.StructArray.from_arrays(child_arrays, fields=child_fields)

    # Map
    if pa.types.is_map(t):
        # Map is list<struct<key,item>>; cast its underlying list/struct
        if isinstance(arr, pa.ChunkedArray):
            return pa.chunked_array([deep_cast_array(c, target_type) for c in arr.chunks], type=target_type)

        # Convert to list-of-struct representation via offsets/keys/items and rebuild.
        # (Details vary; conceptually, deep-cast keys/items and reconstruct.)
        keys_cast = deep_cast_array(arr.keys, target_type.key_type)
        items_cast = deep_cast_array(arr.items, target_type.item_type)
        return pa.MapArray.from_arrays(arr.offsets, keys_cast, items_cast, type=target_type)

    # Primitive-ish fallback
    return pc.cast(arr, target_type, safe=True)
```

This is the exact kind of helper that prevents “nested casting hacks” from leaking into 20 pipeline nodes.

---

## 6) Promotion policy: make it explicit and small

You’ll still want a “permissive” mode sometimes (especially tolerant pipelines), but you should restrict it to **known-safe promotions**.

### 6.1 Recommended allowed promotions (nested-aware)

Here’s a policy that tends to work well in analytics/graph pipelines:

**Primitives**

* int widths: `int32 → int64` (allowed)
* uint widths: `uint32 → uint64` (allowed)
* float widths: `float32 → float64` (allowed)
* string: `string → large_string` (optional, usually fine)
* timestamp: only if unit/timezone semantics are identical (otherwise treat as breaking)

**Lists**

* `list<T> → list<T'>` only if `T → T'` is an allowed primitive promotion
* `list → large_list` treat as breaking unless you have a strong reason (it changes offset width and may affect interoperability)

**Structs**

* field additions allowed (nullable)
* field type change only if the field’s primitive promotion is allowed
* field order changes should not matter *if you always address by name*, but some APIs extract by index (e.g., `struct_field(indices=...)`), so do not rely on indices in production code. ([Apache Arrow][10])

**Maps**

* keys: do not promote (keep `string`)
* items: only promote if it’s a safe primitive promotion; otherwise consider `map<string, string>` for long-tail metadata

And remember: `unify_schemas(..., promote_options="permissive")` will promote types “to the greater common denominator.” ([Apache Arrow][2])
So you should treat permissive unification as *a suggestion*, then run through your own “allowed promotions” filter and fail (strict) / error-row (tolerant) when it tries something you don’t want.

---

## 7) Finalize-gate rules for nested columns

Your finalize gate contract is already clear: in tolerant mode it computes an `is_valid` mask and splits into `(good_table, error_table)` with `row_id` + `error_code` + key fields.
Now we add nested-specific checks that produce **structured errors**.

### 7.1 Nested finalize should be staged

I recommend structuring nested finalization into stages (each stage emits its own errors):

1. **Top-level schema align**
2. **Top-level casts**
3. **Nested casts** (deep cast helper)
4. **Nested invariants** (masks)
5. **Dedupe (if applicable)**
6. **Artifact emission** (alignment report + error stats)

### 7.2 Nested invariants for lists (graph edge builders)

These are the “must haves”:

#### A) Required list presence

Null list is *not* the same as empty list. For edge datasets, a null list often means “extractor failed,” while empty means “no edges.”
So enforce:

* if list column required: `is_valid(listcol)` must be true
* else: null list allowed (but maybe logged)

(You already know `list_flatten` drops null lists—so if you don’t validate, you can silently lose failures.)

#### B) List-aligned columns must match lengths

If you have aligned columns like:

* `callee_ids: list<int64>`
* `callsite_spans: list<struct<start,end>>`

Then per parent row:

* `len(callee_ids) == len(callsite_spans)` must hold

Use `pc.list_value_length` for each list column and compare, and flag any mismatch **before you explode**, because parent-row-level errors are far easier to diagnose. (This is the same philosophy as your tolerant finalize: errors stay structured, not ad hoc.)

#### C) Child-level validity (post-explode)

Decide your policy:

* `dst_id` null edge → drop or error
* `span` null → error (usually)
* negative/invalid spans → error

### 7.3 Nested invariants for structs (“extras” and similar)

For `extras: struct<...>`:

* If `extras` itself is required: `is_valid(extras)` must be true
* For required child fields: `is_valid(extras.<field>)` must be true
  (you can extract children either by name via schema mapping or by `pc.struct_field(indices=...)`). ([Apache Arrow][10])

**Also validate version semantics:**

* if `extras.extras_version` present, enforce `extras_version ∈ allowed_versions`
* optionally enforce “if extras_version >= 2 then field X must be non-null”

### 7.4 Nested invariants for maps (dynamic metadata)

Maps are physically “list of {key,item} structs.” ([Apache Arrow][9])
So your invariants are naturally “explode-ish”:

* keys non-null
* (optional) keys unique per row (requires grouping within each map row; you can implement, but it’s heavier—often you skip it unless it’s causing real bugs)

Also: map arrays expose keys/items as flattened buffers in some contexts, and you may need offsets to re-associate per-row segments (good to keep in mind if you do per-row QA).

---

## 8) Error model: nested-friendly error tables

Keep your existing requirement (“row_id + error_code + key fields”).
Add just enough nested context to make failures actionable without bloating:

Recommended extra columns:

* `stage` (`schema` | `cast` | `nested_cast` | `invariant` | `dedupe`)
* `column` (string)
* `detail` (string) — e.g., `"len(callee_ids)=3 len(callsite_spans)=2"`

Canonical nested error codes:

* `NULL_REQUIRED_LIST`
* `MISALIGNED_LIST_COLUMNS`
* `NULL_CHILD_VALUE`
* `NULL_REQUIRED_STRUCT`
* `NULL_REQUIRED_STRUCT_FIELD`
* `NESTED_CAST_FAILED`
* `MAP_KEY_NULL`

And keep stats:

* `error_code → count` (cheap and incredibly helpful)

---

## 9) One more practical note: nested Parquet/Arrow interoperability

Arrow and Parquet both support nested structs/lists, but conversion between representations can be complex (especially for deep nesting). ([Apache Arrow][11])
So for stability:

* keep nested contracts shallow where you can (struct of primitives, list of primitives, list of small structs)
* reserve “deep nesting” for where it genuinely buys you a lot

---

## 10) What I’d actually implement as your “nested finalize kit”

If I had to boil it down to the minimal helper surface that kills 80% of drift pain:

1. `unify_schemas_with_contract_first(contract_schema, schemas, promote="default"|"permissive")`
2. `deep_cast_table_to_contract(table, contract_schema)` (uses the recursive deep cast approach)
3. `validate_nested(table, specs) -> (good_mask, error_tables...)`

   * specs include list-alignment constraints and required struct fields
4. `finalize(table, contract, mode=strict|tolerant)` (your existing finalize gate, now calling 1–3)

This keeps *every* Hamilton node’s body simple: **plan → execute → finalize** and makes nested structure a feature, not a debugging nightmare.

If you want the next sequential deep dive after this: I’d do a **concrete “nested contract spec” DSL** (tiny dataclasses that declare “required struct fields”, “aligned list groups”, “allowed promotions”), plus a **pytest micro-fixture suite** that asserts each nested invariant produces the right `(good, errors)` split with deterministic `error_code` counts.

[1]: https://arrow.apache.org/docs/python/data.html?utm_source=chatgpt.com "Data Types and In-Memory Data Model - Apache Arrow"
[2]: https://arrow.apache.org/docs/python/generated/pyarrow.unify_schemas.html?utm_source=chatgpt.com "pyarrow.unify_schemas — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.FileSystemDatasetFactory.html?utm_source=chatgpt.com "pyarrow.dataset.FileSystemDatasetFactory - Apache Arrow"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.concat_tables.html?utm_source=chatgpt.com "pyarrow.concat_tables — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.Table.html?utm_source=chatgpt.com "pyarrow.Table — Apache Arrow v22.0.0"
[6]: https://stackoverflow.com/questions/69744720/pyarrow-table-cast-a-struct-within-a-listarray-column-to-a-new-schema?utm_source=chatgpt.com "PyArrow Table: Cast a Struct within a ListArray column to ..."
[7]: https://github.com/apache/arrow/issues/38615?utm_source=chatgpt.com "Allow projection of schemas/structs · Issue #38615"
[8]: https://arrow.apache.org/docs/python/generated/pyarrow.StructArray.html?utm_source=chatgpt.com "pyarrow.StructArray — Apache Arrow v22.0.0"
[9]: https://arrow.apache.org/docs/cpp/api/datatype.html?utm_source=chatgpt.com "Data Types — Apache Arrow v22.0.0"
[10]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.struct_field.html?utm_source=chatgpt.com "pyarrow.compute.struct_field — Apache Arrow v22.0.0"
[11]: https://arrow.apache.org/blog/2022/10/17/arrow-parquet-encoding-part-3/?utm_source=chatgpt.com "Arrow and Parquet Part 3: Arbitrary Nesting with Lists of ..."

Below is a deep dive on **joins in Acero** (PyArrow), with a focus on **`hashjoin`**: how to author it, how to control output shape, what nulls do, why ordering is non-deterministic, and when you should **punt** to another engine.

---

## 1) The Acero hash join surface in PyArrow

In Python, you configure hash join via:

* `acero.Declaration("hashjoin", options=acero.HashJoinNodeOptions(...), inputs=[left, right])`

`HashJoinNodeOptions` exposes these knobs: **join type**, **left/right keys**, **left/right output fields**, **name-collision suffixes**, and a **residual filter** applied after key matching. ([Apache Arrow][1])

### Join types you can request

`join_type` is one of:
`"left semi"`, `"right semi"`, `"left anti"`, `"right anti"`, `"inner"`, `"left outer"`, `"right outer"`, `"full outer"`. ([Apache Arrow][1])

### Key specification

`left_keys` / `right_keys` can be:

* a string column name,
* a field `Expression`,
* or a list of those. ([Apache Arrow][1])

### Output shaping

* If you don’t specify `left_output`/`right_output`, Acero outputs **all valid fields from both sides**. ([Apache Arrow][1])
* If you do specify them, each entry can be a string column name or a field expression. ([Apache Arrow][1])
* `output_suffix_for_left` / `output_suffix_for_right` disambiguate duplicate names. ([Apache Arrow][1])

### Residual filter

`filter_expression` is a **residual filter applied to matching rows** (i.e., after the key match). ([Apache Arrow][1])

---

## 2) “Golden” hash join pattern (authoring + correctness)

### The pattern you should standardize

1. **Pre-project** both sides to:

   * cast/normalize join keys,
   * select only the payload columns you need,
   * rename columns to avoid collisions up front (optional).
2. **Hash join** on simple (preferably named) key columns.
3. **Post-project** to compute derived columns / drop temporary columns.
4. **Finalize gate** (schema + invariants + deterministic sort if needed).

Here’s a representative plan skeleton:

```python
import pyarrow.acero as acero
import pyarrow.compute as pc

left_src = acero.Declaration("scan", options=..., inputs=...)
right_src = acero.Declaration("scan", options=..., inputs=...)

# 1) Pre-project (normalize keys + select payload)
left_p = acero.Declaration(
    "project",
    acero.ProjectNodeOptions(
        expressions=[
            pc.field("symbol_id").cast("int64"),   # or pre-cast earlier
            pc.field("src_file_id"),
            pc.field("src_span"),
        ],
        names=["symbol_id", "src_file_id", "src_span"],
    ),
    inputs=[left_src],
)

right_p = acero.Declaration(
    "project",
    acero.ProjectNodeOptions(
        expressions=[
            pc.field("symbol_id").cast("int64"),
            pc.field("symbol_kind"),
        ],
        names=["symbol_id", "symbol_kind"],
    ),
    inputs=[right_src],
)

# 2) Join
join = acero.Declaration(
    "hashjoin",
    acero.HashJoinNodeOptions(
        join_type="left outer",
        left_keys=["symbol_id"],
        right_keys=["symbol_id"],
        left_output=["symbol_id", "src_file_id", "src_span"],
        right_output=["symbol_kind"],
        output_suffix_for_left="_l",
        output_suffix_for_right="_r",
        # filter_expression=...  # optional residual filter
    ),
    inputs=[left_p, right_p],
)

# 3) Post-project (optional)
out = acero.Declaration(
    "project",
    acero.ProjectNodeOptions(
        expressions=[pc.field("symbol_id"), pc.field("src_file_id"), pc.field("symbol_kind")],
        names=["symbol_id", "src_file_id", "symbol_kind"],
    ),
    inputs=[join],
)

tbl = out.to_table(use_threads=True)
```

Everything above maps directly to the HashJoinNodeOptions semantics (types, keys, outputs, suffixes, residual filter). ([Apache Arrow][1])

---

## 3) Null behavior (and how to deal with it)

### What Acero does today

For join keys, **missing values (NA / null) do not match** in Acero’s join behavior (it matches the “na_matches = never” semantics in dplyr terms). ([GitHub][2])

### What you should do in CodeIntel

In a CodeIntel pipeline, null join keys are almost always “data quality failure,” not “meaningful category.” So the best practice is:

* **Finalize-gate invariant:** join keys must be non-null
* Route null-key rows to your error table *before* the join
* Keep the join key domain “clean”

### If you truly need nulls to match nulls

Do it explicitly by **normalizing keys before join**, e.g.:

* Create `key_is_null = is_null(key)`
* Create `key_filled = coalesce(key, SENTINEL)`
* Join on both `(key_is_null, key_filled)` so you avoid collisions.

This gives you “NA matches NA” deterministically without changing engine semantics.

---

## 4) Important limitation for nested payloads (lists) in hash join

As of Aug 15, 2024, there’s an open Acero issue stating **Hash Join does not support `ListType` in non-key fields** (i.e., list columns in the payload you want to carry through the join). ([GitHub][3])

**Why this matters for you:** your intermediate modeling uses lists heavily (before explode), so the safe rule is:

* **Do not attempt Acero joins while list payload columns are present.**
* Either:

  * explode first (so lists become edge rows), or
  * project/drop list columns before join, then reattach later via a separate keyed lookup (if feasible).

This is one of the clearest “keep it in Acero only if the shape is scalar-friendly” constraints today. ([GitHub][3])

---

## 5) Residual filter: what it’s good for (and what it isn’t)

`filter_expression` is described as a “residual filter applied to matching row.” ([Apache Arrow][1])

Use it for:

* extra predicates *after* equi-match (e.g., “only keep matches where `kind == 'call'`”)
* cleaning up match multiplicity when you know keys aren’t unique and you need a secondary condition

Do **not** treat it as a substitute for:

* non-equi joins (range joins),
* as-of joins,
* complex disjunctive join conditions.

It doesn’t reduce the fundamental work of building/probing the hash table; it filters after a candidate match exists.

---

## 6) Join ordering + determinism (the key truth)

### Hash join output order is not predictable

Arrow’s Acero ordering documentation is blunt: a node’s output should be marked unordered if non-deterministic, and **“a hash-join has no predictable output order.”** ([Apache Arrow][4])

So: **never rely on join output order** for:

* golden snapshots,
* deterministic IDs,
* reproducible downstream behavior.

### How to enforce deterministic output

You have two practical patterns:

#### Pattern A: Post-join explicit sort (recommended for snapshot stability)

1. Ensure you have deterministic tie-breakers:

   * join keys (`k1..kn`)
   * plus stable per-input row identifiers you carry through (e.g., `left_row_id`, `right_row_id`)
2. Sort by `(k1..kn, left_row_id, right_row_id, …)`.

If you sort outside Acero, use `pc.sort_indices`, which produces indices for a **stable sort**. ([Apache Arrow][5])

#### Pattern B: In-Acero `order_by`

Acero can establish a new ordering with an order-by node (but note it’s an accumulating operation). The ordering doc explicitly notes order-by “will emit a brand new ordering.” ([Apache Arrow][4])

For CodeIntel, Pattern A is often simpler because it fits your “finalize gate = one boundary” philosophy (sort right before finalization / caching / snapshotting).

---

## 7) When to keep joins inside Acero

Use Acero `hashjoin` when most of these are true:

* **Equi-join** on 1–N key columns (typical dimension lookups).
* **Keys are clean** (non-null, pre-cast to consistent types).
* Output payload columns are **scalar-friendly** (avoid list payload columns per the known limitation). ([GitHub][3])
* You can **project aggressively** (only the columns you need) using `left_output`/`right_output` to keep the join light. ([Apache Arrow][1])
* You don’t care about natural ordering (or you plan to sort explicitly afterward). ([Apache Arrow][4])

Also: Acero hash join has seen “substantial improvements” across recent Arrow C++ releases in **stability, memory efficiency, and parallel performance**, making it more usable at scale than it used to be. ([Apache Arrow][6])

---

## 8) When to punt (and what that implies)

Acero explicitly positions itself as an **embeddable execution building block**, not a full query engine “aiming to compete with DuckDB.” ([Apache Arrow][6])

So you should punt to DuckDB / DataFusion / etc. when you need capabilities where Acero is (currently) the wrong tool:

### Punt if you need…

* **Non-equi join semantics** (range/asof/interval joins), or sophisticated SQL join planning/rewrites.
* **Disk-spilling joins / out-of-core behavior** (Acero is improving, but it’s not “full engine with spill” territory by design). ([Apache Arrow][6])
* **Robust nested payload support** through the join (e.g., list columns you must preserve). ([GitHub][3])
* **Null-matches-null join behavior as a first-class knob** (Acero behaves as “never match nulls” today). ([GitHub][2])
* Complex multi-step relational pipelines where a full engine’s optimizer and feature set is simply cheaper than hand-authoring ExecPlan graphs.

### The “escape hatch” discipline (so it doesn’t cause sprawl)

Even when you punt:

* treat the external engine step as a single node boundary,
* return Arrow Tables (or readers),
* and still run the **same finalize gate** afterward.

That keeps your fault tolerance + determinism discipline consistent across engines.

---

If you want the next deep dive after joins, the natural one is: **“deterministic join test harness + regression benchmarks”** (i.e., golden fixtures that assert: key null policies, duplicate-key multiplicity, column selection, and stable sorting behavior across Arrow upgrades).

[1]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.HashJoinNodeOptions.html "pyarrow.acero.HashJoinNodeOptions — Apache Arrow v22.0.0"
[2]: https://github.com/apache/arrow/issues/41358 "[R] Support join \"na_matches\" argument · Issue #41358 · apache/arrow · GitHub"
[3]: https://github.com/apache/arrow/issues/43716 "[C++][Acero] Add support for `ListType` for non-key fields to Hash Joins · Issue #43716 · apache/arrow · GitHub"
[4]: https://arrow.apache.org/docs/cpp/api/acero.html "Streaming Execution (Acero) — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.sort_indices.html?utm_source=chatgpt.com "pyarrow.compute.sort_indices — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/blog/2025/07/18/recent-improvements-to-hash-join/ "Recent Improvements to Hash Join in Arrow C++ | Apache Arrow"

Below is the “make it a policy, not ad hoc code” deep dive for **aggregation + dedupe + determinism** in a PyArrow/Acero-first pipeline—grounded in your `compute_revamp.md` canonical pattern (`group_by(...).aggregate(first/hash_first)`, plus “sort once, then aggregate”) and the Arrow docs/issue history.

---

## 1) Grouped aggregation in PyArrow: what you’re really invoking

### 1.1 `Table.group_by(...).aggregate(...)` is “hash_* grouped aggregations”

PyArrow grouped aggregation is driven by **hash aggregation compute functions**. The Python docs show that passing `"sum"` in `aggregate` uses the **`hash_sum`** compute function, and you can supply multiple aggregations and options like `CountOptions`. ([Apache Arrow][1])

Under the hood (C++ docs), grouped aggregation functions are explicitly named **`hash_*`** and include things like `hash_sum`, `hash_min`, `hash_max`, `hash_list`, `hash_distinct`, `hash_first`, `hash_last`, `hash_one`, etc. ([Apache Arrow][2])

**Key implication:** when you pick an aggregation function, you are also picking its **determinism properties**. Some functions are order-independent (min/max/sum over integers), while others are explicitly **order-dependent** (`hash_first`, `hash_last`, `hash_list`) or even **arbitrary** (`hash_one`). ([Apache Arrow][2])

---

## 2) The canonical dedupe pattern you called out (and what it assumes)

Your revamp doc’s canonical dedupe is:

* choose a **key set**
* aggregate **every non-key column** with `"first"` (i.e., `hash_first`)
* optionally disable threads for determinism
  and/or do **“sort once by [keys…, tie_breaker…] then group_by(keys, use_threads=True).aggregate(first…)”** for speed + determinism. 

That is a great “center of gravity” because it forces every dataset to answer: *what defines uniqueness?* and *what’s the canonical representative row?*

But you need to be explicit about one crucial detail:

> **`hash_first` / “first” is order-dependent** — Arrow’s grouped aggregation docs explicitly note `hash_first`/`hash_last` results are “based on ordering of the input data.” ([Apache Arrow][2])

So “keep first” is only deterministic if you’ve defined what “first” means.

---

## 3) A dedupe policy should have three parts

### Part A — Keys: what defines “same fact”

You should pick keys based on *semantic identity*, not convenience:

* **Node tables**: `symbol_id` (or `(repo_id, symbol)` if you don’t have stable IDs yet)
* **Edge tables**:

  * simplest: `(src_id, dst_id, kind)`
  * for call edges, often better: `(src_id, dst_id, callsite_span)` if multiple calls to same callee should remain distinct
  * for “reference edges”: `(src_id, dst_id, ref_span, ref_kind)`

Rule: if collapsing duplicates would lose meaning, your key is too coarse.

### Part B — Tie-breakers: how to pick the canonical row *within a key*

This is where determinism is won.

Good tie-breakers are:

* **semantic and stable**: e.g., `confidence DESC, span_start ASC, span_end ASC`
* **derived from source**: file path + byte offset (stable across runs)
* **monotonic**: timestamps for “latest-wins” if that’s your intent (but beware clocks)

Bad tie-breakers:

* “whatever row happened to arrive first from parallel scan”
* Python hash or nondeterministic IDs

### Part C — Representative selection: how you choose “the winner”

You have 3 main families:

1. **Order-dependent selection** (“first/last”): fastest & simplest, but you *must* define order.
2. **Order-independent selection** (“min/max/best metric”): deterministic even under parallelism.
3. **Collapse instead of select** (“list/distinct/count”): preserve multiplicity explicitly.

---

## 4) Three concrete dedupe recipes (you should standardize all three)

### Recipe 1 — “Keep first” (simple, but order-dependent)

This is your `compute_revamp.md` pattern:

```python
def dedupe_keep_first(t, keys):
    non_keys = [c for c in t.column_names if c not in keys]
    aggs = [(c, "first") for c in non_keys]
    return t.group_by(keys, use_threads=False).aggregate(aggs)
```

Your doc calls out the same approach and explicitly recommends `use_threads=False` when determinism matters. 

Why `use_threads=False` helps: Arrow’s Table docs say that when `use_threads=True` (the default), **“no stable ordering of the output is guaranteed.”** ([Apache Arrow][3])

**When to use:** small/medium tables where correctness & reproducibility outweigh throughput, and you’ve established a deterministic row order (e.g., you sorted first, or you have deterministic scan order).

---

### Recipe 2 — “Keep best by tie-breakers” (deterministic, parallel-friendly)

If you want determinism while keeping parallelism, avoid order-dependent “first” as the *decision mechanism*.

A robust approach is:

1. compute a **winner score** per row (e.g., tuple-like score encoded into sortable components)
2. compute **min/max score per key** using `hash_min`/`hash_max` (order-independent)
3. join back to pick rows with that winning score

This stays deterministic even if aggregation runs in parallel, because min/max doesn’t depend on arrival order (unlike `hash_first`). The grouped aggregation list explicitly includes `hash_min`/`hash_max` and also notes `hash_one` is arbitrary (so avoid that when determinism matters). ([Apache Arrow][2])

**Practical encoding trick:** if your tie-breakers are multiple columns, build a single comparable “score” (e.g., a struct or a string key). If `hash_min` doesn’t accept your type, convert to something it does.

---

### Recipe 3 — “Collapse duplicates explicitly” (don’t pretend they don’t exist)

Sometimes dedupe is the wrong abstraction; you actually want “unique key → list of all payloads”.

Arrow has grouped aggregations:

* `hash_list` gathers grouped values into a list array
* `hash_distinct` gathers distinct values into a list
  ([Apache Arrow][2])

This is very powerful for graph pipelines:

* build adjacency lists deterministically (if you also impose ordering before list aggregation)
* preserve multiple callsites/spans per `(src_id, dst_id)`

But note: like `hash_first`, list aggregation’s internal order follows input ordering. So if you need stable list ordering, you should define it (sort first, or sort after by exploding and re-sorting).

---

## 5) Determinism: where nondeterminism *actually* comes from

### 5.1 Threading changes output order (documented)

`Table.group_by(..., use_threads=True)` explicitly: **no stable ordering of output**. ([Apache Arrow][3])

This aligns with your revamp doc’s warning and recommendation to turn threads off for deterministic outputs. 

### 5.2 Some aggregations depend on input ordering (documented)

Arrow’s grouped aggregation docs note for `hash_first`/`hash_last` (and related) that the **result is based on ordering of the input data**. ([Apache Arrow][2])

So if your input ordering is not stable, your selected representative row is not stable.

### 5.3 Acero nodes can destroy ordering (documented)

In Acero’s execution model, ordering can be marked non-deterministic; a **hash join has no predictable output order**, and the docs note that **hash-join or aggregation may destroy ordering** (even if they might establish a new ordering). ([Apache Arrow][4])

So even if you “think” you have stable scan order, a downstream join/aggregate can scramble it unless you explicitly re-order.

### 5.4 Issue history: ordering drift has been real

There’s a Python Arrow issue requesting guaranteed stable ordering for `group_by`, noting ordering changes observed in dev builds (13.0.0.dev) and that the behavior wasn’t officially documented at the time. ([GitHub][5])

As of current docs, the rule is explicit: **don’t assume stable ordering when threaded**. ([Apache Arrow][3])

---

## 6) “Sort once, then aggregate” — the correct way to think about it

Your revamp doc proposes:

1. sort once by `[keys..., tie_breaker...]`
2. group_by(keys, use_threads=True).aggregate(first…)


The core idea is right: **make “first” meaningful** by imposing order.

### How to implement “sort once” in Arrow-native way

Use `pc.sort_indices` (stable sort) then `pc.take` to reorder the table. Arrow’s compute docs explicitly say `sort_indices` returns indices defining a **stable sort**. ([Apache Arrow][2])

Conceptually:

```python
idx = pc.sort_indices(t, sort_keys=[("k1","ascending"), ("tie","descending")])
t_sorted = pc.take(t, idx)   # then group_by(keys).aggregate(first...)
```

### The subtlety

Sorting makes the *global* row order deterministic, but a parallel hash aggregation can still process data in parallel. For **order-dependent** aggregations (`first/last/list`), the safest “no surprises” rule is:

* If you need strict determinism with `first/last/list`, either:

  * run that stage with ordering-preserving settings (often implies `use_threads=False`), **or**
  * use an order-independent winner-selection strategy (Recipe 2)

So: keep your “sort once” guidance, but treat it as:

* “best effort fast determinism” in many pipelines
* not the strongest possible guarantee unless you also control execution ordering

---

## 7) The “determinism budget” (what you lock down vs let float)

You want a *policy knob*, not a debate every time.

### Tier 0 — Canonical deterministic (CI snapshots, caching, persisted contracts)

Lock down:

* **which row wins** during dedupe
* **final output ordering**
* (optionally) stable IDs derived from content

Mechanics:

* order-independent winner selection (Recipe 2) *or* `use_threads=False` + explicit stable sort
* explicit final sort on contract keys + tie-breakers

### Tier 1 — Stable set, unstable order (production throughput)

Lock down:

* the **set** of rows (semantic correctness)
  Allow:
* row order drift (sort only when needed for downstream expectations)

Mechanics:

* prefer order-independent aggregations (`min/max/count/sum`)
* avoid `first/last/list` unless you don’t care

### Tier 2 — Best-effort (exploration / interactive)

Allow:

* representative row may differ under duplicates

Mechanics:

* `hash_one` is explicitly “one arbitrary value” per group (biased toward non-null). ([Apache Arrow][2])

This is a *feature* for speed, but must never leak into contract outputs.

---

## 8) How this becomes a single finalize-gate “dedupe spec”

Your doc already places “dedupe policy” inside finalize gate. 

Make it explicit with a small spec:

* `keys: list[str]`
* `tie_breakers: list[(col, asc/desc)]`
* `mode: KEEP_FIRST | KEEP_BEST | COLLAPSE_LIST | NO_DEDUPE`
* `determinism: CANONICAL | STABLE_SET | BEST_EFFORT`
* `threads_ok: bool` (derived from determinism)

Then every dataset contract states its dedupe spec once, and every pipeline step calls:

> `finalize(table, contract, dedupe_spec=..., determinism=...)`

---

## 9) Minimal tests you should add (so this stays real)

1. **Duplicate fixture**: 2–3 duplicate rows per key with different payload; assert winner row is correct under Tier 0.
2. **Threaded vs unthreaded**: run group_by with `use_threads=True` and `False`; assert only Tier 0 enforces order/winner.
3. **Order-dependent aggregation safety**: fixture that proves `first` changes if ordering changes (unless you lock it down).
4. **Output-order snapshot**: always sort contract outputs under Tier 0 so golden snapshots don’t fail due to harmless drift.



[1]: https://arrow.apache.org/docs/python/compute.html "Compute Functions — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/cpp/compute.html "Compute Functions — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/docs/python/generated/pyarrow.Table.html "pyarrow.Table — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/cpp/api/acero.html "Streaming Execution (Acero) — Apache Arrow v22.0.0"
[5]: https://github.com/apache/arrow/issues/36709 "[Python] Guarantee that `group_by` has stable ordering. · Issue #36709 · apache/arrow · GitHub"

## 8) Chunking + throughput mechanics (avoid per-kernel overhead)

### First: what “chunking” actually is in PyArrow

In a `pa.Table`, **each column is a `ChunkedArray`**. Chunks usually come from:

* scanning datasets in batches (each `RecordBatch` becomes a chunk when materialized to a `Table`),
* concatenating tables,
* or incremental “append” style assembly.

This matters because many compute operations effectively become “**do the same kernel per chunk**,” so “lots of tiny chunks” turns into “lots of kernel dispatch + Python/C++ boundary overhead.”

Your `compute_revamp.md` calls this out explicitly as a throughput lever: “many small chunks = more overhead per kernel call” and suggests combining chunks when memory isn’t a concern.

---

# A) Chunk normalization: when and where to `combine_chunks()`

## What `combine_chunks()` *guarantees* (and what it doesn’t)

`Table.combine_chunks()` “combines the chunks this table has” and states: underlying chunks in each column are concatenated into **zero or one chunk**, *except* “to avoid buffer overflow, binary columns may be combined into multiple chunks” (max-length chunks). ([Apache Arrow][1])

For a single column, `ChunkedArray.combine_chunks()` “flatten[s] this ChunkedArray into a single non-chunked array.” ([Apache Arrow][2])

## Why `combine_chunks()` is a speed lever (even if memory isn’t a concern)

* Fewer chunks → fewer kernel invocations / less overhead
* Better cache locality (longer contiguous buffers)
* Less overhead in downstream “mask → filter → take → join → aggregate” sequences

But: combining chunks is **work**. It can be *surprisingly slow* if you do it repeatedly or when you already have one chunk; Arrow users have called out the cost in practice. ([GitHub][3])

## The rule that avoids calc sprawl

**Do not sprinkle `combine_chunks()` everywhere.** Standardize it as a **policy at a small number of boundaries**, typically:

### 1) Right after an expensive scan / join / concat *if* you’ll run lots of kernels next

Example: `scan.to_table()` returns a table whose columns are chunked by batch boundaries; if you then run a pile of compute kernels outside Acero, combining once can pay off.

### 2) Inside the finalize gate (recommended default)

Your revamp doc explicitly frames chunk normalization as a “throughput knob” and implies it should be standardized, not ad hoc.

I’d implement:

* `finalize(..., combine_chunks=True)` (default True for “throughput mode”)
* `finalize(..., combine_chunks=False)` for cases where you truly want streaming or want to preserve chunk boundaries for diagnostics.

### 3) Before large group_by / sort work done *outside* Acero

If you’re not using Acero’s aggregate/order nodes and instead use `pc.sort_indices`, `Table.group_by`, etc., combining first often reduces overhead.

## When you should *not* combine

* **You’re still streaming** and haven’t committed to materialization
* You only apply **one or two kernels** and then drop the table
* Your table has **huge binary/string columns**, where `combine_chunks()` may still leave multiple chunks and can be bandwidth-heavy ([Apache Arrow][1])
* You’re about to hand the data back into an engine that already works batch-wise (Acero / Dataset scanning)

## A practical “chunk policy” heuristic (what I’d standardize)

Since you’re optimizing for throughput, you can gate combining on observed chunkiness:

* combine if `max(num_chunks_per_column) > 8` **or**
* `avg_rows_per_chunk < 50_000` (or some threshold tied to scanner batch_size)

This avoids paying the cost when you already have coarse chunks.

---

# B) Batch sizing strategy for scanners/readers (throughput vs latency)

## The Scanner knobs that matter (and defaults)

`pyarrow.dataset.Scanner` documents:

* `batch_size` default **131,072** rows (max rows per scanned `RecordBatch`) ([Apache Arrow][4])
* `batch_readahead` default **16** (read ahead within a file) ([Apache Arrow][4])
* `fragment_readahead` default **4** (read ahead across files) ([Apache Arrow][4])
* `use_threads=True` for max parallelism ([Apache Arrow][4])

And it reiterates the pushdown semantics: filter pushdown when possible; otherwise it filters loaded batches before yielding. ([Apache Arrow][4])

## Throughput-first tuning guidance (CodeIntel-shaped)

### 1) Start with default batch_size and move in “powers of two”

* Narrow numeric tables: often benefit from *larger* batches (fewer batches/chunks → less overhead)
* Wide / nested / lots of strings: may prefer moderate batches to avoid giant transient allocations

**Heuristic:** treat `batch_size` as “how many rows you want per chunk when materialized.” If you intend to call `combine_chunks()` later anyway, you can still keep `batch_size` reasonably large to reduce the number of chunks you’re combining.

### 2) Readahead is a throughput knob, but it inflates “in-flight” memory

Arrow docs are explicit: increasing `batch_readahead` and `fragment_readahead` increases RAM usage but may improve IO utilization. ([Apache Arrow][4])

Given your stance (“memory isn’t a concern”), you can be more aggressive here, *but*:

* Too much readahead can worsen cache locality by flooding memory with soon-to-be-processed batches.
* If downstream is CPU-bound, excess IO readahead doesn’t help.

A good policy is:

* increase `fragment_readahead` first if you have many files,
* increase `batch_readahead` if individual files are large and latency is high.

### 3) If you care about “interactive latency,” shrink batches; if you care about “pipeline throughput,” grow batches

* Smaller batches → earlier first results, easier progress reporting, lower peak
* Larger batches → fewer dispatches, less Python overhead, faster end-to-end

In CodeIntel build pipelines, you almost always want **throughput**.

---

# C) Table vs RecordBatchReader lifecycles (streaming vs materialization)

## Key fact: `Table.to_batches()` and `Table.to_reader()` are **zero-copy views**

PyArrow explicitly notes:

* `Table.to_batches()` is zero-copy; it “merely exposes the same data under a different API.” ([Apache Arrow][1])
* `Table.to_reader()` is also zero-copy. ([Apache Arrow][1])

So “streaming” doesn’t have to mean “recompute”; you can *materialize once* and then stream a reader view if that helps structure downstream processing.

## Why keep a `RecordBatchReader` as long as possible (even if memory is fine)

`RecordBatchReader` is explicitly an iterator/stream of record batches. ([Apache Arrow][5])

Even when you *can* afford full materialization, streaming buys you:

* better CPU scheduling (pipeline overlap),
* fewer giant contiguous allocations (less allocator churn),
* and more consistent cache behavior (process one batch, drop it, move on).

It also reduces *accidental* chunk explosion: if you materialize early, then repeatedly concatenate and slice, you tend to manufacture many small chunks that you later have to “fix” with `combine_chunks()`.

## A clean, CodeIntel-friendly lifecycle policy

### Prefer streaming (Reader/Batches) when:

* your next stage can operate batch-wise (e.g., building intermediate arrays, emitting edge rows in batches, writing parquet in batches),
* you’re doing “map-style” transforms that don’t require global state.

### Prefer materializing to Table when:

* you need global operations (sort, certain joins outside Acero, some global dedupe patterns),
* you’re at a **contract boundary** (finalize gate, caching, snapshotting),
* you need to pass a single object across interfaces.

And remember: `RecordBatchReader.read_all()` exists as a clean “commit to table now” boundary (shown in the `Table.to_reader()` example). ([Apache Arrow][1])

---

# D) Putting it together: the “one boundary” pattern that prevents overhead creep

Your revamp doc’s intent is: **fused plan → minimal materialization → finalize gate**. Chunking policy is a key part of that standardization.

Here’s the operational pattern I’d standardize:

1. **Scan with a batch_size chosen for throughput** (often keep default initially) ([Apache Arrow][4])
2. Stay in **RecordBatchReader/batches** as long as possible
3. Materialize to `Table` only at:

   * “needs global state” points, or
   * the finalize gate
4. In finalize gate:

   * optionally `combine_chunks()` once (policy-driven) ([Apache Arrow][1])
   * then run invariants/dedupe/sorts

That gives you:

* far fewer kernel calls,
* fewer accidental chunk explosions,
* and one place to tune chunk behavior without editing 30 nodes.

---

If you want the next sequential deep dive after this, the most practical is **“thread pools + use_threads interactions with chunking”**: how scanner threading, Acero threading, and `Table.group_by(use_threads=...)` combine to affect both throughput and determinism (and how to set a single global policy per run mode).

[1]: https://arrow.apache.org/docs/python/generated/pyarrow.Table.html "pyarrow.Table — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/python/generated/pyarrow.ChunkedArray.html?utm_source=chatgpt.com "pyarrow.ChunkedArray — Apache Arrow v22.0.0"
[3]: https://github.com/apache/arrow/issues/37318?utm_source=chatgpt.com "[C++][Python] pyarrow.ChunkedArray.combine_chunks is ..."
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html "pyarrow.dataset.Scanner — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatchReader.html?utm_source=chatgpt.com "pyarrow.RecordBatchReader — Apache Arrow v22.0.0"

## 9) Thread pools + parallelism knobs in PyArrow/Acero

Your `compute_revamp.md` calls this out as one of the two “force fast, predictable kernels” levers: **explicitly set Arrow’s global CPU + I/O thread pools**, then rely on per-op/per-plan `use_threads` where you need determinism or to avoid oversubscription.

There are **three layers** to understand:

1. **Global CPU thread pool** (Arrow “parallel operations”)
2. **Global I/O thread pool** (dataset scanning + other I/O tasks)
3. **Per-execution `use_threads=`** (Scanner and Acero plan execution)

---

# A) Global thread pools

## A1) CPU pool: `pa.set_cpu_count(n)` / `pa.cpu_count()`

* `pa.set_cpu_count(count)` **sets the number of threads to use in parallel operations** (compute kernels and other CPU-parallel work). ([Apache Arrow][1])
* `pa.cpu_count()` returns the configured size, and Arrow determines the **startup default** by inspecting `OMP_NUM_THREADS` and `OMP_THREAD_LIMIT`; if unset, it defaults to the number of hardware threads. You can change it at runtime via `set_cpu_count()`. ([Apache Arrow][2])

This matches the revamp doc’s suggestion to “own Arrow’s CPU thread pool explicitly” and also notes the same OpenMP env vars for defaults.

### Practical meaning

If you set `pa.set_cpu_count(32)`, you’re effectively telling Arrow: “for CPU-parallel operations, assume you can use up to 32 threads” (and a lot of APIs will treat that as the max). ([Apache Arrow][1])

---

## A2) I/O pool: `pa.set_io_thread_count(n)` / `pa.io_thread_count()`

* `pa.set_io_thread_count(count)` **sets the number of threads to use for I/O operations**, and Arrow explicitly says **many operations, such as scanning a dataset, implicitly use this pool**. Count must be positive. ([Apache Arrow][3])
* `pa.io_thread_count()` returns the configured size, and Arrow notes it’s set at startup but can be modified at runtime with `set_io_thread_count()`. ([Apache Arrow][4])

So: **dataset scans use both knobs**:

* CPU knob for decode/compute parallelism (when enabled),
* I/O knob for async read / prefetch style tasks (when present in the stack). ([Apache Arrow][3])

---

# B) Per-execution threading

## B1) Dataset scanning: `Scanner(..., use_threads=...)`

`Scanner.from_dataset(..., use_threads=True)` documents:

> If enabled, then **maximum parallelism will be used determined by the number of available CPU cores**. ([Apache Arrow][5])

Interpretation for your runtime-control model:

* `use_threads=True` lets the scan stage exploit CPU parallelism (bounded by “available CPU cores,” which in practice you should treat as governed by your global CPU pool setting). ([Apache Arrow][5])
* `use_threads=False` is your “make scanning single-thread CPU” option (useful for deterministic debugging, or avoiding oversubscription when you’re already parallel elsewhere).

Also note: `Scanner.to_table()` is explicitly a **serial materialization** step (“serially materialize the Scan result in memory before creating the Table”). For throughput, prefer `to_reader()`/`to_batches()` and only materialize at a boundary (e.g., finalize gate). ([Apache Arrow][5])

---

## B2) Acero plan execution: `Declaration.to_table(use_threads=...)` / `to_reader(use_threads=...)`

This one is extremely crisp in the docs:

* `Declaration.to_table(use_threads=True)` runs the plan and collects to a table.
* If `use_threads=False`, **all CPU work is done on the calling thread**.
* **I/O tasks still happen on the I/O executor** and may be multi-threaded (but should not use significant CPU). ([Apache Arrow][6])

This is the cleanest “determinism / no-oversubscription” knob you have for Acero without changing the plan.

Your revamp plan snippet explicitly uses `scan.to_table(use_threads=True)` and calls out that `Declaration.to_table(use_threads=...)` controls CPU threading for the plan.

---

# C) Interactions and “gotchas” (the stuff you want as policy)

## C1) Oversubscription: the #1 real-world failure mode

If you run Hamilton with parallel execution *and* you let Arrow use “max threads everywhere,” you can end up with:

* many Hamilton workers
* each running Arrow ops with large CPU pools

That can tank throughput (context switching, cache thrash) even though each component is “parallel.”

**Policy fix:** pick *one* layer to own parallelism per run mode.

* Either: Hamilton parallelizes across nodes, while Arrow runs with smaller CPU pool.
* Or: Hamilton mostly serializes nodes, while Arrow uses a large CPU pool per node.

Your architecture is trending toward “few big fused Arrow plans,” which usually means **let Arrow be the parallel engine** and keep orchestration concurrency lower.

## C2) Determinism vs speed is a first-class runtime mode

Given `Declaration.to_table(use_threads=False)` runs CPU on the calling thread, you can use that as the “deterministic debug mode” without changing code shape. ([Apache Arrow][6])

Similarly, you can run dataset scanning with `use_threads=False` when you’re chasing nondeterministic behavior or contention. ([Apache Arrow][5])

---

# D) A repo-ready “own the runtime” pattern

Put this at your CLI entrypoint / service startup (once per process), and log it into your run metadata:

```python
import os
import pyarrow as pa

def configure_arrow_threads(*, cpu: int | None, io: int | None) -> None:
    # Log what Arrow thinks right now
    before_cpu = pa.cpu_count()
    before_io = pa.io_thread_count()

    if cpu is not None:
        pa.set_cpu_count(cpu)
    if io is not None:
        pa.set_io_thread_count(io)

    after_cpu = pa.cpu_count()
    after_io = pa.io_thread_count()

    # emit to logs / ops tables
    print(f"Arrow threads: cpu {before_cpu}->{after_cpu}, io {before_io}->{after_io}")
```

This is exactly the revamp doc’s “own CPU & I/O pools explicitly” recommendation (it even gives the same example values).

---

# E) Operational sizing policy (local dev vs CI vs single-host production)

These are **heuristics** (because the right answer depends on whether you’re CPU-bound, IO-bound, and whether *other* layers are parallelizing), but they’re the policies I’d standardize:

## (a) Local dev (interactive, don’t melt the laptop)

Goal: responsiveness + enough throughput to feel fast.

* **CPU pool**: ~50–75% of hardware threads (leave headroom for the OS + IDE + tests)
* **I/O pool**: moderate (often 8–32) since scans can benefit from concurrent reads/prefetch, but it’s rarely worth saturating the machine

Rationale: Arrow’s default CPU pool may be “all hardware threads,” but your dev machine is also running everything else. ([Apache Arrow][2])

## (b) CI (predictable, avoid flake from contention)

Goal: stable runtimes and fewer “randomly slow” runs.

* **CPU pool**: small fixed number (e.g., 2–8), or match the CI runner’s allocated cores
* **I/O pool**: small/moderate (4–16)

Also: consider setting `OMP_NUM_THREADS` / `OMP_THREAD_LIMIT` in CI so the default is sane even if some process forgets to call `set_cpu_count`. ([Apache Arrow][2])

## (c) Production single-host services (throughput + tail latency)

Goal: high sustained throughput without starving the server runtime (FastAPI/ASGI threads, background tasks, etc.).

* **CPU pool**: near available cores **minus headroom** (common: `N-1` or `N-2`)
* **I/O pool**:

  * local NVMe: often `~N` is plenty
  * remote/object storage or high latency FS: can be higher (concurrency helps), but validate via perf tests

Remember: `set_io_thread_count` is specifically for I/O operations and is used implicitly by dataset scans. ([Apache Arrow][3])

---

# F) Where these knobs show up in your “plan → execute → finalize” architecture

* **Scan**: `Scanner(... use_threads=...)` and the global IO pool matter most. ([Apache Arrow][5])
* **Acero plan execution**: `Declaration.to_table/use_threads` controls CPU threading for the plan, while I/O still goes through the I/O executor. ([Apache Arrow][6])
* **Finalize gate**: if you need deterministic debugging, you can run the plan with `use_threads=False` and keep the rest identical.

This keeps your runtime behavior governed by **a small set of explicit knobs** (exactly what your revamp doc is aiming for).

---


[1]: https://arrow.apache.org/docs/python/generated/pyarrow.set_cpu_count.html "pyarrow.set_cpu_count — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/python/generated/pyarrow.cpu_count.html "pyarrow.cpu_count — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/docs/python/generated/pyarrow.set_io_thread_count.html "pyarrow.set_io_thread_count — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.io_thread_count.html "pyarrow.io_thread_count — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html "pyarrow.dataset.Scanner — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.Declaration.html "pyarrow.acero.Declaration — Apache Arrow v22.0.0"

Here’s a technical design for the **internal “Arrow DSL” / helper architecture** that stops calc sprawl—exactly matching the end-state your `compute_revamp.md` describes: a tiny wrapper that returns either an **Acero `Declaration` (preferred)** or a **`pa.Table` fallback**, Hamilton nodes that are mostly **declare plan → execute → finalize(strict/tolerant)**, and a single module where **all compute idioms live** so the surface area stops expanding.

---

## 1) What “calc sprawl” looks like (and what the DSL must prevent)

Calc sprawl usually emerges when:

* every Hamilton node hand-writes bespoke `pc.*` sequences,
* “same operation” exists in 6 variants (slightly different null/type semantics),
* performance knobs (`combine_chunks`, `use_threads`) are sprinkled ad hoc,
* data quality checks are scattered and inconsistent.

Your doc’s cure is architectural: keep nodes tiny and move the “real work” into a shared Arrow layer: **Acero plans when possible**, **table fallbacks when not**, and **one finalize gate** for correctness + diagnostics.

---

## 2) The core contract: three-phase node shape

Every Hamilton target becomes:

1. **declare plan (pure)**: build a “plan object” (no IO, no execution)
2. **execute plan**: materialize to `Table` (or `RecordBatchReader` if you explicitly choose)
3. **finalize(strict/tolerant)**: schema align/cast + vectorized invariants + dedupe + artifacts (good/errors/alignment/stats)

That exact shape is spelled out in the revamp doc as the new default.

This alone kills most sprawl because it forces all variability into:

* plan authoring helpers
* post-plan kernels (explode/dedupe/etc.)
* finalize policies

---

## 3) The “tiny Arrow DSL”: what it should be (and what it must **not** be)

### What it is

A *small* typed layer that represents:

* a **backend**: Acero `Declaration` *or* eager `pa.Table` path
* a **plan graph**: scan/filter/project/join/aggregate/order (Acero primitives)
* an **execution context**: threads/memory_pool/chunk policy/determinism mode
* a **contract**: schema + invariants + dedupe spec + error schema

And returns a single standard result: `FinalizeResult(good, errors, alignment, stats)`.

### What it must not be

* a general optimizer
* a second SQL engine
* a place where node-specific business logic re-accumulates

The DSL should deliberately cover the 80% path and make “escape hatches” explicit (your doc suggests this pattern and even calls out a DataFusion escape hatch if you hit relational ceilings).

---

## 4) Recommended module layout (to stop sprawl by construction)

Put all Arrow compute “power tools” behind one import boundary.

### `src/codeintel/build/tabular/arrowdsl/`

**`expr.py`**

* tiny wrappers for plan-time expressions (field/scalar/in_/and_/or_/cast)
* purpose: avoid mixing eager Scalars vs Expression literals, and keep pushdown-safe subset

**`plan.py`**

* `Plan` abstraction (wraps `acero.Declaration` or eager `pa.Table` thunk)
* plan constructors: `scan_dataset`, `table_source`, `project`, `filter`, `hash_join`, `aggregate`, `order_by`
* “compile” method: returns `Declaration | TableThunk`

**`kernels.py`**

* all *row-count-changing* or hard-to-express operations live here:

  * `explode_edges(...)` (list_flatten + list_parent_indices + take)
  * `stable_sort(...)` (`sort_indices` + `take`)
  * `dedupe_keep_first(...)` + “sort once then first”
  * `safe_divide`, `coalesce_many`, `safe_cast`
  * struct helpers (`make_extras_struct`, `flatten_for_export`)
* this is the “compute idioms module” your doc explicitly says to centralize.

**`contracts.py`**

* `DatasetContract`: schema + required fields + nested alignment rules + dedupe spec + determinism tier
* `ErrorContract`: error table schema + canonical error_code enums

**`finalize.py`**

* `finalize(table, contract, mode, ctx) -> FinalizeResult`
* single place for schema align/cast, invariant masks, dedupe, artifacts (as your doc prescribes).

**`runtime.py`**

* `ExecutionContext`: `cpu_threads`, `io_threads`, `use_threads`, `combine_chunks`, `determinism_mode`
* centralizes the knobs (and logs them once)

This layout makes it *hard* to write bespoke calcs in random nodes because the obvious primitives are all in one place.

---

## 5) Key abstractions (typed) that keep the surface small

### 5.1 `ExecutionContext`

A frozen dataclass carried everywhere:

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class ExecutionContext:
    use_threads: bool                 # per-plan execution CPU threading
    combine_chunks: bool              # normalize chunking at finalize boundary
    determinism: str                  # "canonical" | "stable_set" | "best_effort"
```

(You can extend with memory_pool, IO/cpu pool sizes, etc., but keep it minimal.)

### 5.2 `Plan` as a sum type (Declaration-first, Table fallback)

Your doc’s explicit requirement: return either an Acero `Declaration` (preferred) or `pa.Table` fallback when Acero can’t express something.

So model it explicitly:

```python
from dataclasses import dataclass
from typing import Callable, Union
import pyarrow as pa
import pyarrow.acero as acero

TableThunk = Callable[[], pa.Table]

@dataclass(frozen=True)
class Plan:
    inner: Union[acero.Declaration, TableThunk]

    def execute(self, *, ctx: ExecutionContext) -> pa.Table:
        if isinstance(self.inner, acero.Declaration):
            return self.inner.to_table(use_threads=ctx.use_threads)
        return self.inner()
```

Key property: execution is **one place**. Nodes don’t decide “how to run” anymore.

### 5.3 `DatasetContract` drives finalize (and dedupe)

Contracts make “policy not ad hoc” real:

* schema + required cols
* nested alignment rules (list-aligned cols)
* dedupe spec (keys + tie-breakers + strategy)
* determinism tier

Then `finalize()` is just “apply contract.”

---

## 6) The *one* compositional pattern you standardize: “Plan → Kernels → Finalize”

Your doc calls out the “list explode + Acero plan + finalize gate” pattern as the actionable refactor that reveals most of the helper surface.

So bake it into a first-class helper:

```python
def run_pipeline(
    *,
    plan: Plan,
    post: list[Callable[[pa.Table], pa.Table]],  # explode, join lookups, etc.
    contract: DatasetContract,
    mode: str,  # "strict" | "tolerant"
    ctx: ExecutionContext,
) -> FinalizeResult:
    t = plan.execute(ctx=ctx)
    for fn in post:
        t = fn(t)
    return finalize(t, contract=contract, mode=mode, ctx=ctx)
```

Now every Hamilton node is basically:

```python
def call_edges__table(ctx: ExecutionContext, ...deps...) -> pa.Table:
    plan = plans.call_edges_scan(...)
    post = [kernels.explode_edges(...), kernels.dedupe(...)]
    return run_pipeline(plan=plan, post=post, contract=contracts.call_edges, mode="tolerant", ctx=ctx).good
```

Which matches “declare plan → execute → finalize” exactly.

---

## 7) Where the sprawl *actually* goes to die: the “idioms module”

The doc explicitly lists the compute idioms that should be centralized: explode list, dedupe, safe divide/coalesce, struct extras, invariant validators.

Treat those as **blessed primitives**:

### 7.1 Explode edges

Single canonical helper (with aligned-list validation and error routing).

### 7.2 Dedupe

Two canonical variants only:

* deterministic: sort once by `[keys..., tie_breakers...]` then `group_by(keys).aggregate(first...)` (your doc’s “best of both worlds”)
* fast: `group_by(keys, use_threads=...)` “first” with documented determinism mode

### 7.3 Safe arithmetic and null semantics

`safe_divide`, `coalesce_many`, `safe_cast` must be single-source-of-truth (otherwise every node reinvents error policies).

### 7.4 Struct extras

One helper to build the `extras` struct (avoid JSON blobs) and one helper to flatten only at export boundaries.

### 7.5 Invariants

Define invariants as composable mask builders (return `BooleanArray` + `error_code`), and let finalize assemble them into `(good, errors)`.

---

## 8) Enforcing the architecture (so it doesn’t drift back)

Two practical enforcement moves:

1. **“No raw pc.* in nodes” rule**

   * allow `pc.*` only inside `arrowdsl/expr.py` and `arrowdsl/kernels.py`
   * everything else imports from there

2. **Golden tests for helpers**

   * every helper gets a micro-fixture test: input table → (good/errors/stats)
   * dedupe tests prove determinism mode behaviors
   * explode tests prove alignment error routing

This is how you make “calc sprawl stops expanding” an invariant of the repo, not a hope.

---

## 9) Decision rubric: when do you fall back to `pa.Table`?

Keep Acero as the default because it covers scan/project/filter/hash_join/aggregate/order primitives (your doc explicitly calls this out).

Fallback to eager `pa.Table` only when:

* the operation changes row counts in ways Acero can’t express cleanly (explode/unnest)
* nested payload limitations force it
* you need a niche compute pattern that’s not available as an ExecNode

But the *key* is: fallback is still “inside the DSL,” not ad hoc in nodes.

---


