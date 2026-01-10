# Chapter 1 — ExecPlan mental model for DSL authors

This chapter is about **how Acero actually runs your plan**, so you can design an Arrow DSL that is (a) fast by default, (b) predictable about “streaming vs materializing,” and (c) doesn’t accidentally reintroduce Python-loop calc sprawl.

You already know the basics: `Declaration("scan"|"filter"|"project"|...)` and `to_table()` / `to_reader()`. We’ll recap that in ~2 minutes, then spend the rest on the parts that usually *aren’t* internalized but matter a lot for DSL architecture.

---

## 1) The ExecPlan model in one picture

Acero is a C++ streaming execution engine: you express computation as an **execution plan (`ExecPlan`)**, which is a directed graph of **operators (`ExecNode`)**, and batches of data flow along the edges. ([Apache Arrow][1])

In practice you author the plan as a graph of **Declarations**; at execution time Acero builds an ExecPlan where **each Declaration becomes an ExecNode**, and then Acero adds a **sink** depending on how you execute (e.g., “collect to table”). ([Apache Arrow][2])

**The mental model you want:**

* **Declarations** = a *blueprint* for an ExecPlan
* **ExecPlan** = runtime graph + schedulable tasks
* **ExecBatches / RecordBatches** = the units of work that flow through operators

---

## 2) Push-based execution is the “gotcha” that should shape your DSL

Acero’s ExecPlan is **push-based**: sources *push* batches downstream into consumer nodes. ([Apache Arrow][3])

But most real-world sources are naturally **pull-based** (iterators, readers). So a source node typically schedules tasks that repeatedly:

1. pull an item from the source, then
2. push it into the output node (via an “input received” style callback). ([Apache Arrow][3])

### Why this matters for DSL authors

A push-based engine changes how you should think about:

* **Streaming boundaries**: if you “collect to table” you’ve placed a sink that **forces full accumulation**.
* **Backpressure & task scheduling**: a node might stop scheduling tasks if downstream can’t accept more input.
* **Where “row-count-changing” ops belong**: anything that is awkward to express as a streaming push operator is a candidate for “kernel lane” outside Acero (your DSL should model this explicitly, not ad hoc).

---

## 3) Execution lifecycle: what actually happens when you call `to_table()` / `to_reader()`

When you call an execution method (like “to table”), Acero:

* creates a new ExecPlan from the Declaration graph,
* adds an appropriate sink,
* starts the plan,
* and then either **blocks until completion** (table) or returns a streaming reader handle (reader). ([Apache Arrow][4])

### `to_table()`: “simple but accumulates everything”

The C++ user guide explicitly calls out that collecting to a single table is easy, but **requires accumulating all results into memory**. ([Apache Arrow][2])

### `to_reader()`: “streaming output surface”

`to_reader()` gives you a `RecordBatchReader` so you can consume output incrementally instead of demanding one big table materialization (this is your core “streaming boundary primitive” in Python). ([Apache Arrow][5])

### `use_threads=`: the per-execution CPU threading gate

On Python `Declaration.to_table()` / `to_reader()`, `use_threads=False` means **all CPU work runs on the calling thread**; I/O tasks still run on the I/O executor and may be multi-threaded. ([Apache Arrow][5])

**DSL implication:** you should treat `use_threads` as part of an `ExecutionContext` / runtime profile, not something each node decides.

---

## 4) “Streaming” doesn’t mean “never accumulates”: pipeline breakers are real

Acero can work on “potentially infinite” streams in principle. ([Apache Arrow][1])
But **some nodes cannot produce output until they have accumulated enough input**. Arrow’s developer docs explicitly call these **“pipeline breakers”** and use sort as the canonical example. ([Apache Arrow][3])

Concrete examples you should internalize:

* **`order_by`**: the Python docs say it currently works by **accumulating all data, sorting, then emitting**; “larger-than-memory sort is not currently supported.” ([Apache Arrow][6])
* **`aggregate`**: the C++ Acero API docs state **“by default, the aggregate node is a pipeline breaker”** and must accumulate all input before producing output (unless you use specialized options like segment keys). ([Apache Arrow][4])

**DSL design consequence:** “use `to_reader()`” is not enough to guarantee bounded memory. Your DSL should model which plans are truly streaming vs which contain pipeline breakers (and therefore have materialization-like behavior).

---

## 5) The DSL boundary rule that keeps things sane

A clean rule that scales:

### Keep inside Acero (plan lane)

Operations that are:

* **row-count preserving** (filter/project),
* standard relational primitives (hash join),
* or aggregations where you accept pipeline-break behavior (and you’re explicit about it).

### Keep outside Acero (kernel lane)

Operations that are:

* **row-count changing** in ways you want to standardize as a helper (explode/unnest),
* hard to express as an ExecNode in Python today,
* or require custom error-routing semantics best done in your finalize gate.

This matches your overall “plan → execute → finalize” architecture: Acero does the fused bulk work, then kernels do the few transforms Acero doesn’t express cleanly, then finalize enforces contract + QA.

---

## 6) Running example (Pipeline A skeleton): scan → project → (reader vs table)

We’ll build the smallest useful plan:

* **source** (dataset scan or table source)
* **project** (select only columns we need)
* then run it as **streaming** (`to_reader`) and **materialized** (`to_table`) to show the difference.

### Option A: in-memory source (runnable anywhere)

```python
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.acero as acero

call_sites = pa.table({
    "repo_id": [1, 1, 1],
    "caller_id": [10, 10, 11],
    "callee_id": [20, 21, 20],
    "kind": ["call", "call", "import"],
    "confidence": [0.9, 0.7, 0.8],
    "span_start": [100, 200, 300],
    "span_end": [110, 215, 310],
})

src = acero.Declaration(
    "table_source",
    acero.TableSourceNodeOptions(call_sites),
)

proj = acero.Declaration(
    "project",
    acero.ProjectNodeOptions(
        expressions=[
            pc.field("repo_id"),
            pc.field("caller_id"),
            pc.field("callee_id"),
            pc.field("kind"),
            pc.field("confidence"),
        ],
        names=["repo_id", "caller_id", "callee_id", "kind", "confidence"],
    ),
    inputs=[src],
)
```

### Execute as a reader (streaming surface)

```python
reader = proj.to_reader(use_threads=True)
for batch in reader:
    # consume batches incrementally
    print(batch.num_rows)
```

### Execute as a table (materializing sink)

```python
tbl = proj.to_table(use_threads=True)
print(tbl.num_rows)
```

What this demonstrates:

* you author the same plan,
* you choose output behavior at the execution surface,
* and **`to_table()` is the explicit “accumulate everything” sink**. ([Apache Arrow][2])

### Option B: dataset scan (what you’ll use in production)

Use a dataset scan when you want pushdown (partition pruning, column projection at read time) and parallel scanning.

At execution time, it’s still the same conceptual flow: scan node → project/filter nodes → sink. ([Apache Arrow][2])

---

## 7) What to take away (as DSL author “axioms”)

1. **Declarations are a plan blueprint; the ExecPlan is created at execution** and a sink is added based on execution method. ([Apache Arrow][2])
2. **ExecPlan is push-based**; sources schedule tasks to pull from pull-based sources and push downstream. ([Apache Arrow][3])
3. **Streaming output surfaces can still hide pipeline breakers** (order_by, aggregate) that accumulate input. ([Apache Arrow][6])
4. **Execution knobs belong in a runtime profile** (`use_threads`, etc.), not in ad hoc node code. ([Apache Arrow][5])

These axioms are the foundation for a DSL that stays small and predictable.

---

## 8) Artifacts to ship with this chapter (so it’s executable)

### Doc page

* `docs/guide/ch01_execplan_internals.md`

  * basically this chapter + the runnable code snippets

### Tests

* `tests/arrowdsl/test_ch01_execplan_smoke.py`

  * builds a tiny plan using `table_source → project`
  * asserts:

    * `to_table()` returns expected schema/rows
    * `to_reader()` yields batches whose concatenation equals `to_table()`

### Bench

* `bench/arrowdsl/bench_ch01_reader_vs_table.py`

  * runs the same plan in two modes:

    * `to_table()`
    * `to_reader(); read_all()`
  * records elapsed time and output sizes (and optionally memory snapshots if you have a harness)

---


[1]: https://arrow.apache.org/docs/cpp/acero/overview.html?utm_source=chatgpt.com "Acero Overview — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/cpp/acero/user_guide.html?utm_source=chatgpt.com "Acero User's Guide — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/docs/developers/cpp/acero.html?utm_source=chatgpt.com "Developing Acero — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/cpp/api/acero.html?utm_source=chatgpt.com "Streaming Execution (Acero) — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.Declaration.html?utm_source=chatgpt.com "pyarrow.acero.Declaration — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.OrderByNodeOptions.html?utm_source=chatgpt.com "pyarrow.acero.OrderByNodeOptions — Apache Arrow v22.0.0"

# Chapter 2 — Ordering reality: where Acero destroys order and why it matters

Chapter 1 gave you the ExecPlan mental model. Now we hit the most common source of “why did my pipeline drift?” bugs:

> **In Acero (and in Arrow generally), row order is rarely a guaranteed property. If you need stable ordering, you must *create and enforce* it.**

This matters for:

* golden snapshot tests,
* caching/materialization correctness (stable IDs),
* and any “keep first” / “take first” semantics (dedupe, winner selection, etc.).

---

## 1) An ordering vocabulary for DSL authors

In Acero’s C++ API docs, output ordering is treated as a **property of an operator’s output**: if the order is non-deterministic, the output should be marked unordered; and Acero explicitly says **a hash join has no predictable output order**. ([Apache Arrow][1])

For a practical DSL, you want a vocabulary that matches how you write contracts:

### 1.1 Ordering levels

1. **UNORDERED**

   * No meaningful row order; do not snapshot, do not “first()”, do not derive stable IDs without a sort.

2. **IMPLICIT ORDER**

   * There is a meaningful order, but it’s not represented by a column (e.g., “row order in an in-memory table”). Acero calls out this “implicit” case explicitly. ([Apache Arrow][1])

3. **EXPLICIT ORDER(keys...)**

   * There is a declared sort order by specific keys (and tie-breakers), and the data is known to be in that order.

**DSL rule:** every plan should carry an `ordering` attribute, and every operator should be required to declare whether it preserves, destroys, or establishes ordering.

---

## 2) Where ordering comes from (and how fragile it is)

### 2.1 In-memory sources: “implicit” row order

If you feed Acero from an in-memory table (`table_source`), the order you inserted rows is usually meaningful—but it’s **implicit** (not encoded in any column). Acero’s docs describe this as the typical case for implicit ordering. ([Apache Arrow][1])

**DSL take:** treat this as `IMPLICIT`, not `EXPLICIT`, unless you add an explicit `row_id` column.

### 2.2 Dataset scans: ordering is configurable (but only helps “simple” plans)

If you use Acero’s `scan` node, you have two important knobs:

* `require_sequenced_output=True`: batches are yielded sequentially “like single-threaded.” ([Apache Arrow][2])
* `implicit_ordering=True`: scan output is augmented with fragment/batch indices “to enable stable ordering for simple ExecPlans.” ([Apache Arrow][2])

This is excellent for *scan → filter → project* pipelines, but it does **not** magically give you stable ordering after joins and aggregations (next section).

---

## 3) Where Acero destroys ordering (the big three)

### 3.1 Hash join: **no predictable output order**

Acero’s docs are explicit: **hash join has no predictable output order**. ([Apache Arrow][1])

So after `hashjoin`, your DSL must mark the output as `UNORDERED`, regardless of whether either side was ordered.

### 3.2 Group-by / aggregation under threading: output order is not stable

On the Python side, Arrow is equally direct: `Table.group_by(..., use_threads=True)` (the default) means **no stable ordering of the output is guaranteed**. ([Apache Arrow][3])

So even if the **set** of grouped rows is deterministic, the **row order** can drift.

### 3.3 “Order by” establishes order… by accumulating everything

You can always restore a deterministic ordering via:

* Acero `order_by` node, or
* a kernel-level stable sort (more on this below),

…but you should treat “ordering establishment” as a **policy decision** because it’s a throughput tradeoff (sorting tends to accumulate data).

---

## 4) Why this matters: “first”, dedupe, and any winner-takes-all logic

If your pipeline ever does:

* dedupe by “keep first,”
* “first occurrence wins,”
* “take first” aggregations,
* or builds stable IDs from “row position,”

then **ordering is part of correctness**, not cosmetics.

Your compiled revamp work already pushes dedupe and determinism into policy and finalize boundaries; ordering is the substrate that makes those policies meaningful. 

---

## 5) The canonical fix: enforce deterministic order explicitly

### 5.1 Stable sort indices is your workhorse

`pyarrow.compute.sort_indices` computes indices that define a **stable sort** of an input array/record batch/table. ([Apache Arrow][4])

**Canonical sorting pattern (kernel lane):**

1. compute stable sort indices,
2. `take` rows by those indices.

```python
import pyarrow.compute as pc

def canonical_sort(table, sort_keys):
    idx = pc.sort_indices(table, sort_keys=sort_keys)  # stable sort indices
    return table.take(idx)
```

(Use this at finalize boundaries, before snapshotting/caching, and before “first” semantics.)

### 5.2 Choosing canonical sort keys: make it a *total order*

A canonical sort must be able to break ties deterministically.

**Good canonical key set:**

* Primary identifiers (repo_id, symbol_id, edge endpoints, etc.)
* Plus tie-breakers that are stable:

  * `span_start`, `span_end`, `file_id`,
  * provenance fields (filename/fragment/batch indices) when you have them,
  * or a persistent `row_id` / content hash.

**Bad tie-breakers:**

* anything derived from parallel scheduling (“arrival order”),
* anything that can drift across machines/runs.

---

## 6) Pipeline A (progress): add hash join and prove why “raw order” is meaningless

We now extend the running example to include the join.

### 6.1 Build the join plan (in-memory example)

```python
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.acero as acero

call_sites = pa.table({
    "repo_id": [1, 1, 1, 1],
    "caller_id": [10, 10, 11, 10],
    "callee_id": [20, 21, 20, 20],
    "kind": ["call", "call", "import", "call"],
    "confidence": [0.9, 0.7, 0.8, 0.95],
    "span_start": [100, 200, 300, 120],
    "span_end": [110, 215, 310, 130],
    "file_id": [7, 7, 8, 7],
})

symbols = pa.table({
    "symbol_id": [20, 21],
    "symbol_kind": ["function", "function"],
    "fqname": ["pkg.mod.f", "pkg.mod.g"],
})

left = acero.Declaration("table_source", acero.TableSourceNodeOptions(call_sites))
right = acero.Declaration("table_source", acero.TableSourceNodeOptions(symbols))

join = acero.Declaration(
    "hashjoin",
    acero.HashJoinNodeOptions(
        join_type="left outer",
        left_keys=["callee_id"],
        right_keys=["symbol_id"],
        left_output=["repo_id","caller_id","callee_id","kind","confidence","span_start","span_end","file_id"],
        right_output=["symbol_kind","fqname"],
    ),
    inputs=[left, right],
)

out = join.to_table(use_threads=True)
```

The important thing isn’t whether `out` *looks* ordered—it’s that **you are not allowed to treat it as ordered**, because hash join output has no predictable order. ([Apache Arrow][1])

### 6.2 Canonicalize the output order (what your DSL should do before finalization)

```python
sort_keys = [
    ("repo_id", "ascending"),
    ("caller_id", "ascending"),
    ("callee_id", "ascending"),
    ("span_start", "ascending"),
    ("span_end", "ascending"),
    ("file_id", "ascending"),
    ("kind", "ascending"),
    ("confidence", "descending"),
]

out_canon = canonical_sort(out, sort_keys=sort_keys)
```

This gives you a stable “presentation / snapshot / cache” order regardless of how join emitted rows.

---

## 7) What your DSL should encode (so nobody has to remember this)

### 7.1 Every plan step declares ordering behavior

* `scan` can optionally emit `IMPLICIT` ordering + provenance (via `implicit_ordering=True`) for **simple** plans. ([Apache Arrow][2])
* `hashjoin` **forces** `UNORDERED`. ([Apache Arrow][1])
* `aggregate` generally forces `UNORDERED` unless you explicitly re-order after.
* `order_by` establishes `EXPLICIT(order_keys...)`.

### 7.2 Finalize gate owns canonical ordering for “CANONICAL determinism”

If your determinism tier is “canonical” (CI snapshots, persisted contracts), then finalize must:

* sort by contract-defined keys/tie-breakers using a stable sort, then
* apply dedupe/aggregation rules that depend on order.

Stable sort indices is the correct primitive for this. ([Apache Arrow][4])

---

## 8) Artifacts to ship with this chapter (executable)

### `tests/arrowdsl/test_ch02_ordering_is_unstable_without_sort.py`

A robust version that **doesn’t rely on nondeterminism happening** (no flakes):

* Run Pipeline A twice with different input row orders (same rows).
* Assert:

  * raw output may differ in row order (allowed),
  * canonical-sorted output is identical.

### `tests/arrowdsl/test_ch02_canonical_sort_keys.py`

* Assert the canonical sort key list for Pipeline A is exactly what the guide specifies (so contracts don’t drift).

### Optional: scan-order note test

If you use dataset scan options in your real pipelines:

* a small test that `ScanNodeOptions(implicit_ordering=True)` produces provenance columns you can use as tie-breakers for “simple ExecPlans.” ([Apache Arrow][2])

---

### Side note (advanced): writing datasets while preserving order

If you ever need to **preserve row order while writing a dataset** (even with threads), Arrow exposes `preserve_order=True` on `pyarrow.dataset.write_dataset`, explicitly warning it “may cause notable performance degradation.” ([Apache Arrow][5])
This is useful for a few “contract materialization” steps, but it’s not a substitute for canonical ordering in compute pipelines.

---


[1]: https://arrow.apache.org/docs/cpp/api/acero.html?utm_source=chatgpt.com "Streaming Execution (Acero) — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.ScanNodeOptions.html?utm_source=chatgpt.com "pyarrow.acero.ScanNodeOptions — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/docs/python/generated/pyarrow.TableGroupBy.html?utm_source=chatgpt.com "pyarrow.TableGroupBy — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.sort_indices.html?utm_source=chatgpt.com "pyarrow.compute.sort_indices — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html?utm_source=chatgpt.com "pyarrow.dataset.write_dataset — Apache Arrow v22.0.0"

# Chapter 3 — Determinism budget + dedupe policy (keys + tie-breakers)

Chapter 2 established the uncomfortable truth: **order is not a stable property unless you explicitly create it**. This chapter turns that into an operational policy:

> **Determinism is a budget**: you decide what must be stable (and when), and you encode that decision in your DSL as data contracts + dedupe specs—not ad hoc code.

We’ll do three things:

1. define a **Determinism Budget** (tiers you can put in a contract),
2. define a **Dedupe Policy** format (keys + tie-breakers + winner strategy),
3. implement **Pipeline B** (`out_degree_rollup`) in a way that is deterministic when required, and fast when not.

---

## 1) Determinism is 3 separate properties (don’t conflate them)

When someone says “I need deterministic output,” they usually mean a mix of these:

### 1.1 Stable set

The *set* of rows is the same (ignoring ordering).

### 1.2 Stable representative choice

If there are duplicates by your semantic key, you pick the same “winner” row every run.

This is where order-dependent aggregates matter: Arrow’s grouped aggregation docs explicitly call out that `hash_first`, `hash_first_last`, and `hash_last` are **based on ordering of the input data** (i.e., order-dependent). ([Apache Arrow][1])

### 1.3 Stable ordering

The output row order is the same.

Arrow documents that for grouped results, when `use_threads=True` (default), **no stable ordering of the output is guaranteed**. ([Apache Arrow][2])

Your DSL needs to model all three explicitly—because you can often get “stable set” cheaply, while “stable representative choice” and “stable ordering” have real cost.

---

## 2) The Determinism Budget: three tiers you’ll use everywhere

Use these tiers as a contract-level knob (and as a runtime profile input):

### Tier A — `CANONICAL` (snapshots, caching, contract outputs)

Guarantees:

* stable set ✅
* stable representative choice ✅
* stable ordering ✅

Implementation pattern:

* enforce a total order (canonical sort keys + tie-breakers),
* apply any order-dependent dedupe with that order,
* emit final sorted output.

### Tier B — `STABLE_SET` (production throughput, order irrelevant)

Guarantees:

* stable set ✅
* stable representative choice ❓ (only if you avoid order-dependent ops)
* stable ordering ❌

Implementation pattern:

* prefer order-independent aggregates (`min/max/sum/count`) and avoid `first/last/list` unless you don’t care.
* sort only when a downstream consumer requires it.

### Tier C — `BEST_EFFORT` (exploration / interactive)

Guarantees:

* stable set ❓
* stable representative choice ❌
* stable ordering ❌

Here you can use “arbitrary representative” semantics intentionally. Arrow’s grouped aggregation docs describe `hash_one` as returning **one arbitrary value per group** (biased toward non-null). ([Apache Arrow][1])

---

## 3) Why this policy must exist (and can’t be “folklore”)

Two reasons you can’t treat determinism as a vague preference:

1. **Threading breaks ordering guarantees.** `TableGroupBy` explicitly states that with `use_threads=True` there is no stable ordering guarantee. ([Apache Arrow][2])
2. **Order-dependent aggregations exist.** `hash_first` / `hash_last` are explicitly order-dependent. ([Apache Arrow][1])

Also: Arrow users have observed ordering changes in practice across versions / dev builds and asked for stable ordering guarantees. ([GitHub][3])

So: the DSL must enforce determinism *as a contract*, not as developer memory.

---

## 4) Dedupe policy = keys + tie-breakers + winner strategy

Dedupe is not a function call; it’s an explicit decision:

### 4.1 Keys: what defines “same fact”

**Rule:** keys should represent *semantic identity*.

Examples (CodeIntel-shaped):

* **Symbol node table**: `symbol_id`
* **File node table**: `repo_id, file_id`
* **Edge table (coarse)**: `src_id, dst_id, kind`
* **Call-site edge table (finer)**: `src_id, dst_id, file_id, span_start, span_end`

If you collapse duplicates and lose meaning, your key is too coarse.

### 4.2 Tie-breakers: how to pick the winner within a key

Tie-breakers must be:

* stable,
* meaningful,
* and sufficient to create a **total order** (no ties left).

Typical tie-breakers for call-site facts:

* `confidence DESC`
* `span_start ASC`, `span_end ASC`
* `file_id ASC`
* plus provenance fields if available (filename/fragment indices) or a stable `row_id` if you have one.

### 4.3 Winner strategy (the part that must be standardized)

You need only a small set of sanctioned strategies:

* `KEEP_FIRST_AFTER_SORT` (Tier A): canonical sort → choose first (order-dependent but controlled)
* `KEEP_BEST_BY_SCORE` (Tier A/B): compute score → choose max/min (order-independent choice)
* `COLLAPSE_LIST` (Tier A/B): keep all duplicates as list (explicitly preserve multiplicity)
* `KEEP_ARBITRARY` (Tier C): use “one arbitrary per group” semantics (`hash_one` conceptually) ([Apache Arrow][1])

---

## 5) What your DSL should encode (minimal types)

The goal is: **every dataset contract can declare its determinism and dedupe policy**.

```python
from dataclasses import dataclass
from enum import Enum
from typing import Literal, Sequence

class DeterminismTier(str, Enum):
    CANONICAL = "canonical"
    STABLE_SET = "stable_set"
    BEST_EFFORT = "best_effort"

@dataclass(frozen=True)
class SortKey:
    column: str
    order: Literal["ascending", "descending"] = "ascending"

@dataclass(frozen=True)
class DedupeSpec:
    keys: tuple[str, ...]
    tie_breakers: tuple[SortKey, ...]          # must form a total order in CANONICAL
    strategy: Literal[
        "KEEP_FIRST_AFTER_SORT",
        "KEEP_BEST_BY_SCORE",
        "COLLAPSE_LIST",
        "KEEP_ARBITRARY",
    ]
```

Then the finalize gate can do:

* if `determinism == CANONICAL`: canonical sort → dedupe strategy → final sort
* else: skip canonical ordering unless explicitly requested

---

## 6) Canonical primitives you must standardize (so nobody re-invents them)

### 6.1 Canonical sort = stable `sort_indices` + `take`

`pyarrow.compute.sort_indices` returns indices that define a **stable sort**. ([Apache Arrow][4])

```python
import pyarrow.compute as pc

def canonical_sort(table, sort_keys: list[tuple[str, str]]):
    idx = pc.sort_indices(table, sort_keys=sort_keys)  # stable
    return table.take(idx)
```

### 6.2 Dedupe Strategy A: “keep first after sort” (Tier A default)

This is the simplest canonical dedupe:

* **sort** by `(keys..., tie_breakers...)`
* then `group_by(keys).aggregate(first on all payload columns)`

Why this works: `hash_first` is explicitly **based on ordering of the input data**. ([Apache Arrow][1])
So if you define order, “first” becomes meaningful.

One more important point: grouped output ordering itself isn’t guaranteed when threaded. ([Apache Arrow][2])
So you still finish with a final canonical sort (or treat grouped output as unordered and sort again).

```python
def dedupe_keep_first_after_sort(t, *, keys, tie_breakers):
    # 1) stable total order
    t_sorted = canonical_sort(
        t,
        sort_keys=[(k, "ascending") for k in keys] + [(tb.column, tb.order) for tb in tie_breakers],
    )

    # 2) first-wins aggregation (payload cols only)
    non_keys = [c for c in t_sorted.column_names if c not in keys]
    aggs = [(c, "first") for c in non_keys]
    # note: group_by(use_threads=True) does not guarantee output ordering,
    # so we canonical-sort again after aggregation if Tier A.
    out = t_sorted.group_by(list(keys), use_threads=False).aggregate(aggs)  # stable debug-friendly

    # 3) final stable order for Tier A outputs
    return canonical_sort(out, sort_keys=[(k, "ascending") for k in keys])
```

This is the “boring but bulletproof” approach. It’s also easy to reason about in CI.

### 6.3 Dedupe Strategy B: “keep best by score” (Tier A/B, parallel-friendly)

If you want:

* deterministic winner selection **without** relying on input iteration order,
* and you want to keep multi-threaded group_by,

then you avoid `first/last` for selection, and instead choose winners via order-independent aggregates like `max/min`.

A robust pattern:

1. compute a **score** (a value you can `max` deterministically),
2. `group_by(keys).aggregate(max(score))`,
3. join back on `(keys, score)` to select winning rows,
4. tie-break if multiple rows share the same max score (rare; handle with an extra secondary score component).

This keeps “winner selection” deterministic even when output ordering isn’t.

---

## 7) Pipeline B: `out_degree_rollup` (deterministic metrics rollup)

Now we build Pipeline B off Pipeline A’s output (enriched call-sites):

**Goal:** `caller_id → out_degree`

Using `Table.group_by().aggregate(...)` (the documented grouped-aggregation API). ([Apache Arrow][2])

```python
def out_degree_rollup(enriched_calls):
    # count callee_id per caller_id (null callee_id should already be filtered by finalize gate)
    deg = enriched_calls.group_by(["caller_id"], use_threads=True).aggregate([
        ("callee_id", "count"),
    ])

    # Canonical output ordering (Tier A only)
    deg = canonical_sort(deg, sort_keys=[("caller_id", "ascending")])
    return deg
```

Notes:

* The *set* of grouped results should be stable, but ordering is not guaranteed in threaded mode, so canonical sorting is how you make the output snapshot-friendly. ([Apache Arrow][2])
* In `CANONICAL` tier, you enforce `caller_id ASC` ordering *always*, so downstream views and tests never drift.

---

## 8) How this integrates with the finalize gate (the intended architecture)

In the “plan → execute → finalize” world, dedupe and ordering are finalize-gate responsibilities:

* **Finalize (Tier A):**

  1. schema align + cast
  2. invariants
  3. canonical sort
  4. dedupe spec (if present)
  5. final canonical sort
  6. emit `(good, errors, stats, alignment)`

* **Finalize (Tier B):**

  1. schema align + cast
  2. invariants
  3. dedupe spec only if it’s order-independent (or if caller explicitly asks)
  4. no canonical sort unless requested

That keeps determinism “policy-driven” instead of “who remembered to sort here?”

---

## 9) Artifacts to ship with this chapter (executable)

### Doc page

* `docs/guide/ch03_determinism_budget_and_dedupe.md`

  * this chapter + the `DedupeSpec` + the two strategies + pipeline B example

### Tests

1. `tests/arrowdsl/test_ch03_dedupe_winner_is_stable.py`

   * create duplicates per key where only confidence differs
   * assert `KEEP_FIRST_AFTER_SORT` always chooses the expected winner
   * assert the output is in canonical order by keys

2. `tests/arrowdsl/test_ch03_rollup_is_stable.py`

   * run `out_degree_rollup` twice with different input order
   * assert canonical-sorted output tables are identical

### Bench

* `bench/arrowdsl/bench_ch03_dedupe_strategies.py`

  * generate synthetic duplicates
  * compare:

    * `KEEP_FIRST_AFTER_SORT` (single-thread group_by)
    * `KEEP_BEST_BY_SCORE` (multi-thread group_by + join-back)
  * record elapsed time and output row counts

---


[1]: https://arrow.apache.org/docs/cpp/compute.html "Compute Functions — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/python/generated/pyarrow.TableGroupBy.html?utm_source=chatgpt.com "pyarrow.TableGroupBy — Apache Arrow v22.0.0"
[3]: https://github.com/apache/arrow/issues/36709?utm_source=chatgpt.com "[Python] Guarantee that `group_by` has stable ordering."
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.sort_indices.html?utm_source=chatgpt.com "pyarrow.compute.sort_indices — Apache Arrow v22.0.0"


# Chapter 4 — Observability: provenance + structured artifacts (not print statements)

In Chapters 1–3 we treated Acero plans as “declare → execute → finalize,” and we made ordering/determinism an explicit policy. This chapter makes the third pillar real:

> **Every pipeline stage should produce *structured* observability artifacts as first-class outputs**—not ad hoc logs—so you can (a) debug fast, (b) regression-test behavior, and (c) extract minimal repros.

We’ll do this by standardizing:

* **provenance columns** (where did a row come from?),
* a **FinalizeResult** shape (`good`, `errors`, `alignment`, `stats`),
* and a **repro extractor** that can build a tiny dataset subset from your error table.

---

## 1) The “observable pipeline” contract

Your finalize gate already implies the contract we want: tolerant mode *never throws*; it computes a validity mask and splits into `(good_table, error_table)` where the error table includes `row_id + error_code + key fields`.

For the guide, we formalize a four-artifact output per pipeline:

1. **`good: pa.Table`** — contract-compliant rows
2. **`errors: pa.Table`** — row-level failures (typed; never a string blob)
3. **`alignment: pa.Table`** — 1-row schema/cast report
4. **`stats: pa.Table`** — counts by error code/stage

That exact shape is already recommended in your compiled doc.

---

## 2) Provenance: “where did this row come from?” (cheaply)

### 2.1 Scanner special fields (the secret weapon)

When scanning Arrow Datasets, the `columns` list (or dict projection) can reference special fields:

* `__batch_index` (batch within fragment)
* `__fragment_index` (fragment within dataset)
* `__last_in_fragment` (bool)
* `__filename` (source file name / fragment description)

These are explicitly documented in `pyarrow.dataset.Scanner`. ([Apache Arrow][1])

You already flagged these as “high value for observability” (error tables, skew diagnosis, reproducible repros).

### 2.2 Standardize provenance as *opt-in columns*

Make this a runtime/profile knob (e.g., `ctx.provenance=True`), but in practice you’ll want it **on by default for tolerant mode** and often on for CI.

**Recommended canonical provenance columns (renamed for downstream ergonomics):**

* `prov_filename`  ← `__filename`
* `prov_fragment_index` ← `__fragment_index`
* `prov_batch_index` ← `__batch_index`
* `prov_last_in_fragment` ← `__last_in_fragment`

### 2.3 How you actually add them (dataset scan)

Two good patterns:

#### Pattern A — include special fields directly as columns

```python
import pyarrow.dataset as ds

def scan_with_provenance(dataset: ds.Dataset, *, columns: list[str], filter_expr=None, ctx=None):
    cols = list(columns)
    if ctx and ctx.provenance:
        cols += ["__filename", "__fragment_index", "__batch_index", "__last_in_fragment"]
    return ds.Scanner.from_dataset(dataset, columns=cols, filter=filter_expr)
```

This works because Scanner explicitly allows those special fields in the projection list. ([Apache Arrow][1])

#### Pattern B — use `scan_batches()` for fragment-aware debugging

`Scanner.scan_batches()` yields **TaggedRecordBatch** values. In v22, the method is documented as returning an iterator of `TaggedRecordBatch`. ([Apache Arrow][1])
A `TaggedRecordBatch` is explicitly “a combination of a record batch and the fragment it came from,” and it exposes `record_batch` and `fragment`. ([Apache Arrow][2])

```python
scanner = ds.Scanner.from_dataset(dataset, columns=["repo_id", "caller_id", "__filename"])
for tagged in scanner.scan_batches():
    batch = tagged.record_batch
    fragment = tagged.fragment
    # debug: fragment info + the batch that produced errors
```

This is your “I need to reproduce exactly what was read” surface.

---

## 3) Structured artifacts: error tables, alignment reports, and stats

### 3.1 Error table schema (minimum viable)

Your compiled spec is exactly what you want:

* `row_id: int64`
* `error_code: string`
* `stage: string` (schema / invariant / dedupe)
* plus **key fields** copied from the row (whatever helps locate it)

**Observability upgrade:** always include provenance fields in `errors` if available:

* `prov_filename`, `prov_fragment_index`, `prov_batch_index`

This makes errors actionable without attaching a debugger.

### 3.2 Alignment report (1 row)

The alignment report should record:

* `num_rows_in/good/bad`
* `missing_cols_added`
* `extra_cols_dropped`
* `casts_applied` (col + from + to + safe)

This becomes the first thing you diff when a pipeline “mysteriously” changes.

### 3.3 Stats table

A simple `error_code → count` table is a high-leverage operational artifact (drift detection + regression tests).

---

## 4) What to log as *run metadata* (separate from row-level errors)

Row-level error tables are great, but you still need a 1-row “run manifest” that answers:

* what ran?
* with what contract version?
* with what runtime profile?
* on what inputs?
* how big was the scan?

### 4.1 Planning telemetry you should always capture

Two cheap “pre-flight” metrics from Dataset scanning:

* `dataset.count_rows(filter=...)` (estimate “how big is this query?”)
* `dataset.get_fragments(filter=...)` (how many files/fragments will be touched?)

Even if you don’t persist these for every stage, they’re the fastest way to see “this run unexpectedly touched 10× the files.”

### 4.2 A minimal run manifest schema

Keep it small and stable (JSON or one-row Arrow table):

```json
{
  "run_id": "...",
  "pipeline": "enrich_call_sites",
  "contract": "call_sites_enriched_v1",
  "contract_version": 1,
  "determinism_tier": "canonical",
  "provenance_enabled": true,
  "arrow_version": "...",
  "profile": "CI_STABLE",
  "scan_estimated_rows": 123456,
  "scan_fragments": 98
}
```

(Contract + profile + counts are usually enough to pinpoint “why did this drift?”)

---

## 5) Minimal repro extractor: errors → tiny dataset subset

This is the practical payoff: **given an error table, build the smallest possible input slice** you can hand to a developer (or a CI artifact) to reproduce the issue.

### 5.1 Level 1 repro (fastest): file subset

If you have `prov_filename`, you can immediately isolate to the files that produced errors.

```python
from __future__ import annotations
import pathlib
import pyarrow as pa
import pyarrow.dataset as ds

def extract_repro_files(
    *,
    errors: pa.Table,
    dataset_root: str,
    out_dir: str,
    filename_col: str = "prov_filename",
) -> None:
    outp = pathlib.Path(out_dir)
    outp.mkdir(parents=True, exist_ok=True)

    if filename_col not in errors.column_names:
        raise ValueError(f"Errors table missing {filename_col}; enable provenance.")

    files = errors[filename_col].to_pylist()
    uniq = sorted({f for f in files if f is not None})

    # Copy or re-materialize just those files into a repro directory.
    # (Here we materialize via a dataset read/write, so it works even if filenames are fragment descriptors.)
    repro_ds = ds.dataset(uniq, format="parquet")
    ds.write_dataset(repro_ds.to_table(), base_dir=str(outp), format="parquet")
```

Pros:

* extremely simple
* typically shrinks “1000 files” to “3 files”

Cons:

* might still include extra rows within those files.

### 5.2 Level 2 repro (tighter): fragment/batch subset

If you also captured `prov_fragment_index` / `prov_batch_index`, you can isolate further.

Practical approach:

1. Build a set of `(fragment_index, batch_index)` pairs from the errors.
2. Scan with `scan_batches()` and emit only the matching batches.

This is possible because:

* `Scanner.scan_batches()` yields `TaggedRecordBatch` (batch + fragment) ([Apache Arrow][1])
* and you can project `__fragment_index`/`__batch_index` into the batch itself. ([Apache Arrow][1])

(For the guide, I’d provide this as an “advanced repro mode” once you’ve standardized provenance fields.)

---

## 6) Pipeline A progress: emitting the four artifacts

At this point in the guide, Pipeline A (`enrich_call_sites`) should return **FinalizeResult**.

### 6.1 Example: tolerant pipeline wrapper

```python
from dataclasses import dataclass
import pyarrow as pa

@dataclass(frozen=True)
class FinalizeResult:
    good: pa.Table
    errors: pa.Table
    alignment: pa.Table
    stats: pa.Table
```

And your `finalize(...)` implementation should:

* align schema + cast
* compute invariant masks
* split to `(good, errors)`
* emit `alignment` + `stats`

Your compiled finalize gate spec already defines the minimum error schema and alignment/stats expectations; Chapter 4 is about **treating those as primary outputs, not debugging afterthoughts**.

---

## 7) Artifacts to ship with this chapter

### Doc page

* `docs/guide/ch04_observability_playbook.md`

### Tests

1. `tests/arrowdsl/test_ch04_provenance_fields_present.py`

   * build a tiny dataset scan with provenance enabled
   * assert columns exist (or your renamed `prov_*` exist)

2. `tests/arrowdsl/test_ch04_errors_have_keys_and_provenance.py`

   * deliberately create invalid rows (e.g., null required key)
   * assert:

     * `errors` has `row_id`, `error_code`, `stage`, key fields
     * provenance fields present when enabled ([Apache Arrow][1])

### Tooling

* `tools/arrowdsl/repro_extract.py`

  * CLI that:

    * reads `errors.parquet`
    * writes a repro dataset (Level 1 file subset by default; Level 2 batch subset optional)

---



[1]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html "pyarrow.dataset.Scanner — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.TaggedRecordBatch.html "pyarrow.dataset.TaggedRecordBatch — Apache Arrow v22.0.0"

# Chapter 5 — The Arrow DSL IR: keeping it tiny, typed, and stable

Chapters 1–4 gave you the execution mental model, the ordering/determinism realities, and the observability contract. Now we turn those into **code structure**.

This chapter’s deliverable is a **minimal internal IR** (“Arrow DSL”) that:

* keeps the helper surface *small* and *typed*,
* compiles primarily to **Acero `Declaration`** (plan lane),
* supports explicit **fallbacks** (kernel lane) without leaking ad hoc logic into nodes,
* routes all correctness + diagnostics through one **FinalizeResult** shape.

The end state: a Hamilton node becomes ~10 lines of declarative wiring, not 200 lines of bespoke `pc.*`.

---

## 0) What’s new vs recap

### What’s new (this chapter)

* A **typed IR** (`Plan`, `ExecutionContext`, `Contract`, `FinalizeResult`) that is stable across the repo.
* A strict separation between:

  * **plan lane** (Acero) and
  * **kernel lane** (row-count-changing or “awkward in Acero” ops)
* A single “runner” function: **plan → post-kernels → finalize**.

### Recap (brief)

* Acero runs a push-based ExecPlan; `to_table` vs `to_reader` defines the sink.
* Hash join/aggregate destroy ordering; determinism requires explicit sorting + policy.

---

## 1) Design constraints (the rules that prevent calc sprawl)

These constraints are what make the IR “best-in-class” in practice:

1. **Nodes don’t do compute.** Nodes declare plans and call a single runner.
2. **No raw `pc.*` in nodes.** If you see `pc.` outside `arrowdsl/expr.py` or `arrowdsl/kernels.py`, it’s a code smell.
3. **Acero is the default.** If a transform is expressible as scan/filter/project/join/aggregate/order, it belongs in plan lane.
4. **Kernel lane is explicit.** Explode/dedupe/sort/finalize are shared helpers and are the *only* place row-count changes happen (unless a plan node explicitly does it).
5. **Determinism is a contract.** Canonical ordering + dedupe spec live in `Contract`, not in “who remembered to sort.”

---

## 2) The IR: four core types

You only need four first-class types to stabilize everything:

* `ExecutionContext`: runtime knobs (threads, determinism tier, provenance, chunk policy)
* `Plan`: “something executable” (Declaration-first, with explicit fallbacks)
* `Contract`: schema + invariants + dedupe + canonical ordering rules
* `FinalizeResult`: standardized structured outputs (good/errors/alignment/stats)

### 2.1 `ExecutionContext` (runtime knobs belong here)

Keep it minimal. Don’t turn this into a config dump.

```python
from __future__ import annotations
from dataclasses import dataclass
from enum import Enum

class DeterminismTier(str, Enum):
    CANONICAL = "canonical"     # snapshots/caching
    STABLE_SET = "stable_set"   # throughput; ordering irrelevant
    BEST_EFFORT = "best_effort" # exploration

@dataclass(frozen=True)
class ExecutionContext:
    use_threads: bool           # per-plan CPU threading
    combine_chunks: bool        # finalize boundary chunk normalization
    provenance: bool            # include prov_* columns
    determinism: DeterminismTier
```

> Anything not used by *most* pipelines does not belong here (yet). Keep the IR stable.

---

### 2.2 `Plan`: a tiny sum-type (Declaration-first)

This is the critical “stop sprawl” move: **Plan is either an Acero Declaration or an explicit fallback**.

```python
from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Optional, Union
import pyarrow as pa
import pyarrow.acero as acero

TableThunk = Callable[[], pa.Table]
ReaderThunk = Callable[[], pa.RecordBatchReader]

@dataclass(frozen=True)
class Plan:
    # Exactly one of these should be set.
    decl: Optional[acero.Declaration] = None
    table_thunk: Optional[TableThunk] = None
    reader_thunk: Optional[ReaderThunk] = None

    # Optional metadata for debugging/observability
    label: str = ""

    def to_table(self, *, ctx: "ExecutionContext") -> pa.Table:
        if self.decl is not None:
            return self.decl.to_table(use_threads=ctx.use_threads)
        if self.table_thunk is not None:
            return self.table_thunk()
        if self.reader_thunk is not None:
            return self.reader_thunk().read_all()
        raise RuntimeError("Invalid Plan: no backend provided")

    def to_reader(self, *, ctx: "ExecutionContext") -> pa.RecordBatchReader:
        if self.decl is not None:
            return self.decl.to_reader(use_threads=ctx.use_threads)
        if self.reader_thunk is not None:
            return self.reader_thunk()
        if self.table_thunk is not None:
            return self.table_thunk().to_reader()  # zero-copy view
        raise RuntimeError("Invalid Plan: no backend provided")
```

**Why this is best-in-class:**

* You can keep 95% of work in Acero.
* When you must fall back (explode, some nested corner), you do it **inside the DSL**, not inside nodes.
* Your runtime knobs (`use_threads`) live in one place.

---

### 2.3 `Contract`: policy lives here (not in random nodes)

A `Contract` should answer four questions:

1. What schema must the output have?
2. What invariants must hold (and how do we report violations)?
3. What dedupe policy (if any) applies?
4. What canonical ordering (if any) is required under `CANONICAL` determinism?

Minimal shape:

```python
from dataclasses import dataclass
from typing import Callable, Sequence

import pyarrow as pa

# Invariants compile to masks + error metadata; keep them simple.
InvariantFn = Callable[[pa.Table], tuple[pa.Array, str]]  # (bad_mask, error_code)

@dataclass(frozen=True)
class SortKey:
    column: str
    order: str = "ascending"  # "ascending" | "descending"

@dataclass(frozen=True)
class DedupeSpec:
    keys: tuple[str, ...]
    tie_breakers: tuple[SortKey, ...]
    strategy: str  # e.g. "KEEP_FIRST_AFTER_SORT"

@dataclass(frozen=True)
class Contract:
    name: str
    schema: pa.Schema
    key_fields: tuple[str, ...]              # copied into errors for debugging
    required_non_null: tuple[str, ...]       # invariant core
    invariants: tuple[InvariantFn, ...] = ()
    dedupe: DedupeSpec | None = None
    canonical_sort: tuple[SortKey, ...] = ()
```

**Policy rule:** `Contract` objects are versioned and treated as immutable. The moment you let nodes modify “just this one thing,” you’re back to sprawl.

---

### 2.4 `FinalizeResult`: structured artifacts (always)

This is Chapter 4’s output contract as code:

```python
from dataclasses import dataclass
import pyarrow as pa

@dataclass(frozen=True)
class FinalizeResult:
    good: pa.Table
    errors: pa.Table
    alignment: pa.Table   # 1-row summary
    stats: pa.Table       # error_code counts
```

---

## 3) The runner: plan → post-kernels → finalize

This is the single function that prevents “execution logic” from scattering.

```python
from typing import Callable, Iterable

PostFn = Callable[[pa.Table, ExecutionContext], pa.Table]

def run_pipeline(
    *,
    plan: Plan,
    post: Iterable[PostFn],
    contract: Contract,
    mode: str,  # "strict" | "tolerant"
    ctx: ExecutionContext,
) -> FinalizeResult:
    t = plan.to_table(ctx=ctx)

    for fn in post:
        t = fn(t, ctx)

    return finalize(t, contract=contract, mode=mode, ctx=ctx)
```

* `post` is where you put **kernel lane** transforms: explode, dedupe, stable sort, etc.
* `finalize(...)` is your single correctness boundary (schema align/cast, invariant masks, error tables, determinism).

**Key point:** nodes never call `pc.*` directly; they only compose `Plan` + `post` steps.

---

## 4) Where expressions and kernels live (so the repo stays sane)

### 4.1 `arrowdsl/expr.py`: pushdown-safe expression macros

This module returns **Expressions**, not arrays. It’s the only place where “expression vocabulary” is curated.

Examples:

* `E.field("col")`
* `E.in_("kind", ["call","import"])`
* `E.and_(...)`, `E.or_(...)`
* `E.cast(...)`

### 4.2 `arrowdsl/kernels.py`: eager compute primitives

This module contains the handful of **blessed eager kernels**, including:

* `explode_edges(...)`
* `canonical_sort(...)` (stable sort indices + take)
* `dedupe_keep_first_after_sort(...)`
* `make_extras_struct(...)`
* `validate_aligned_lists(...)`

If a new computation pattern appears in node code, it becomes a `kernels.*` helper or it doesn’t ship.

---

## 5) Re-implement Pipeline A using only DSL primitives

Pipeline A: `enrich_call_sites` (call-site facts + symbol dimension).

### 5.1 Plan lane (Acero): scan/project/join/project

This example uses in-memory tables so it’s runnable. In production, `scan` would come from a Dataset with pushdown.

```python
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.acero as acero

def build_enrich_call_sites_plan(call_sites: pa.Table, symbols: pa.Table) -> Plan:
    left = acero.Declaration("table_source", acero.TableSourceNodeOptions(call_sites))
    right = acero.Declaration("table_source", acero.TableSourceNodeOptions(symbols))

    # Pre-project to normalize keys + reduce payload
    left_p = acero.Declaration(
        "project",
        acero.ProjectNodeOptions(
            expressions=[
                pc.field("repo_id"),
                pc.field("caller_id"),
                pc.field("callee_id"),
                pc.field("kind"),
                pc.field("confidence"),
                pc.field("span_start"),
                pc.field("span_end"),
                pc.field("file_id"),
            ],
            names=["repo_id","caller_id","callee_id","kind","confidence","span_start","span_end","file_id"],
        ),
        inputs=[left],
    )

    right_p = acero.Declaration(
        "project",
        acero.ProjectNodeOptions(
            expressions=[pc.field("symbol_id"), pc.field("symbol_kind"), pc.field("fqname")],
            names=["symbol_id","symbol_kind","fqname"],
        ),
        inputs=[right],
    )

    join = acero.Declaration(
        "hashjoin",
        acero.HashJoinNodeOptions(
            join_type="left outer",
            left_keys=["callee_id"],
            right_keys=["symbol_id"],
            left_output=["repo_id","caller_id","callee_id","kind","confidence","span_start","span_end","file_id"],
            right_output=["symbol_kind","fqname"],
        ),
        inputs=[left_p, right_p],
    )

    return Plan(decl=join, label="enrich_call_sites")
```

**Important:** This is still “plan lane”—even though it uses `pc.field(...)`. That’s okay; the IR rule is “no raw compute in *nodes*.” Plan builders are part of the DSL layer.

### 5.2 Kernel lane (post): optional canonical sort / dedupe / explode

For Pipeline A, you typically do **no explode** (it’s already row-level facts). But you might:

* apply dedupe on `(repo_id, caller_id, callee_id, file_id, span_start, span_end)` with tie-breakers,
* apply canonical ordering under `CANONICAL`.

Those become `post=[kernels.dedupe(...), kernels.canonical_sort(...)]` and are shared across pipelines.

### 5.3 Finalize gate enforces contract + emits artifacts

Your finalize gate provides:

* schema align/casts,
* invariant masks,
* error routing,
* canonical determinism behavior.

So Pipeline A becomes:

```python
def enrich_call_sites(
    call_sites: pa.Table,
    symbols: pa.Table,
    *,
    ctx: ExecutionContext,
) -> FinalizeResult:
    plan = build_enrich_call_sites_plan(call_sites, symbols)

    post = [
        # e.g. kernels.dedupe_keep_first_after_sort(...),
        # e.g. kernels.canonical_sort_if_canonical(...)
    ]

    return run_pipeline(
        plan=plan,
        post=post,
        contract=CONTRACT_ENRICHED_CALL_SITES,
        mode="tolerant",
        ctx=ctx,
    )
```

This is the core “IR payoff”: the node stays tiny, everything else is shared.

---

## 6) Hamilton node integration (the canonical shape)

In Hamilton, you usually want to return the **contract-compliant table** (`result.good`), and optionally persist `errors/stats` elsewhere.

```python
def enriched_call_sites__table(
    call_sites__table: pa.Table,
    symbols__table: pa.Table,
    execution_context: ExecutionContext,
) -> pa.Table:
    res = enrich_call_sites(call_sites__table, symbols__table, ctx=execution_context)
    return res.good
```

If you want full observability in DAG outputs, you can also expose:

* `enriched_call_sites__errors`
* `enriched_call_sites__stats`
* `enriched_call_sites__alignment`

…but the main win is that the compute logic doesn’t multiply.

---

## 7) Enforcing stability: the two guardrails that matter most

### 7.1 “No raw pc in nodes” guard

Add a cheap CI test that fails if nodes call `pyarrow.compute` directly outside the DSL layer. Start simple: grep the `src/codeintel/build/**` tree for `pyarrow.compute as pc` imports (allowlist the DSL modules).

### 7.2 Plan execution equivalence test

Prove your Plan wrapper isn’t hiding behavior:

* Build a small plan manually, run it.
* Build the same plan via DSL, run it.
* Assert resulting tables are identical after canonical sort.

This becomes your “IR stability” safety net.

---

## 8) Chapter 5 ship list (what goes in the repo)

### Code modules

* `arrowdsl/plan.py` — `Plan`, `TableThunk`, `ReaderThunk`, execution methods
* `arrowdsl/runtime.py` — `ExecutionContext`, `DeterminismTier`
* `arrowdsl/contracts.py` — `Contract`, `DedupeSpec`, contract instances for Pipeline A/B
* `arrowdsl/expr.py` — expression macros (minimal for now)
* `arrowdsl/kernels.py` — canonical sort + dedupe helpers (minimal for now)
* `arrowdsl/runner.py` — `run_pipeline(...)`
* `arrowdsl/finalize.py` — `FinalizeResult` + `finalize(...)` (existing, now called consistently)

### Tests

* `tests/arrowdsl/test_ch05_plan_exec_equivalence.py`
* `tests/arrowdsl/test_ch05_no_raw_pc_in_nodes.py` (simple guard)

---

## The one thing to be ruthless about

If you only enforce one discipline from this chapter, enforce this:

> **Every new computation pattern goes into `kernels.py` (or `expr.py`) before it’s allowed into the DAG.**

That’s the difference between “a DSL that stays tiny” and “a DSL that becomes another sprawling framework.”

# Chapter 6 — Expression macros and pushdown tactics (make pushdown repeatable)

Chapter 5 gave you a tiny, typed DSL IR (`Plan`, `ExecutionContext`, `Contract`, `FinalizeResult`). This chapter fills in the missing piece that makes the DSL *pleasant* and *fast*:

> A small set of **expression macros** + a **QuerySpec** that produces the same semantics across:
> **Dataset scanning (pushdown)** and **Acero filter/project nodes (correctness + portability)**.

The outcome: every pipeline gets “do less IO earlier” by default, and you stop rewriting slightly-different filters/projections all over the DAG.

---

## 0) What’s new vs recap

### What’s new

* A **pushdown-safe expression vocabulary** (one place, one style).
* A `QuerySpec` that emits:

  * **scan** `columns` + `filter` (pushdown attempted),
  * and equivalent **Acero** filter/project options (correctness + plan composability).
* Practical pushdown tactics: how to write predicates that actually prune fragments/row-groups.

### Recap (brief)

* Expressions are built from `pc.field()` / `pc.scalar()` and combined with `& | ~` (not `and/or/not`). ([Apache Arrow][1])
* Dataset scanning can push down filters using partition info or Parquet stats, otherwise it filters loaded record batches. ([Apache Arrow][2])

---

## 1) The core problem: “one query, three call sites”

Without a QuerySpec, you end up duplicating the same intent in three different places:

1. **Dataset scan**: `Scanner.from_dataset(..., columns=..., filter=...)`
2. **Acero plan**: `scan` node options + `filter` node + `project` node
3. **Fallback path** (table_source): `filter/project` nodes only

This duplication is where sprawl comes from:

* slightly different null semantics,
* slightly different projections (forgetting a required column),
* pushdown not attempted in some code paths,
* provenance fields missing in error tables.

So the DSL needs a single *source of truth* for “what query are we running?”

---

## 2) The expression model (rules you enforce in `arrowdsl/expr.py`)

### 2.1 How expressions are created (and the footguns)

Arrow’s dataset expression docs are explicit:

* Create scalars with `pyarrow.compute.scalar()`
* Create field references with `pyarrow.compute.field()`
* Compare with `<, <=, ==, >=, >`
* Combine with `&` (and), `|` (or), `~` (not)
* Use `.isin(...)` for membership tests
  …and **you can’t use Python `and/or/not`**. ([Apache Arrow][1])

That becomes your DSL rule: **never expose raw expression construction in nodes.**

### 2.2 A minimal macro surface (enough to cover 90% of pushdown)

```python
# src/.../arrowdsl/expr.py
from __future__ import annotations
import pyarrow.compute as pc

class E:
    # Field refs
    @staticmethod
    def col(name: str) -> pc.Expression:
        return pc.field(name)

    # Literals
    @staticmethod
    def lit(x) -> pc.Expression:
        return pc.scalar(x)

    # Predicates
    @staticmethod
    def eq(col: str, value) -> pc.Expression:
        return pc.field(col) == pc.scalar(value)

    @staticmethod
    def ge(col: str, value) -> pc.Expression:
        return pc.field(col) >= pc.scalar(value)

    @staticmethod
    def isin(col: str, values: list) -> pc.Expression:
        return pc.field(col).isin(values)

    # Boolean composition (explicit, so nobody uses and/or)
    @staticmethod
    def and_(*xs: pc.Expression) -> pc.Expression:
        out = xs[0]
        for x in xs[1:]:
            out = out & x
        return out

    @staticmethod
    def or_(*xs: pc.Expression) -> pc.Expression:
        out = xs[0]
        for x in xs[1:]:
            out = out | x
        return out

    @staticmethod
    def not_(x: pc.Expression) -> pc.Expression:
        return ~x
```

This macro layer is intentionally tiny. If a query needs a new primitive, you add it here once—so pushdown behavior and semantics remain consistent everywhere.

---

## 3) `QuerySpec`: one object that drives scan + plan

### 3.1 Why QuerySpec must output both scan args and plan nodes

Two critical facts you need to design around:

* `Scanner.from_dataset(..., columns=..., filter=...)` will attempt pushdown using partition info / Parquet stats, else filter record batches after reading. ([Apache Arrow][2])
* Acero’s `ScanNodeOptions` can also apply pushdown projections/filters, **but it does not construct the associated filter/project nodes to perform the final filtering/projection**—Arrow explicitly tells you that you may supply the same filter/projection to the scan node that you supply to filter/project nodes. ([Apache Arrow][3])

That second point is the key “advanced” insight: **scan pushdown is an optimization hint; filter/project nodes are the semantic contract.**

### 3.2 Data model for QuerySpec

You want three layers of intent:

1. **Pushdown**: what’s safe/useful to push into the scan
2. **Semantic**: what must be true of the output (full filter)
3. **Shape**: what columns the rest of the plan expects (projection)

A practical minimal spec:

```python
# src/.../arrowdsl/queryspec.py
from __future__ import annotations
from dataclasses import dataclass
import pyarrow.compute as pc

@dataclass(frozen=True)
class ProjectionSpec:
    # Either a list of source columns, or computed columns
    base_cols: tuple[str, ...]
    computed: tuple[tuple[str, pc.Expression], ...] = ()  # (name, expr)

@dataclass(frozen=True)
class QuerySpec:
    # Full semantic filter (may include things that don't prune well)
    predicate: pc.Expression | None

    # Optional subset used for fragment planning / pruning telemetry
    # (e.g., only partition-key comparisons)
    pushdown_predicate: pc.Expression | None

    projection: ProjectionSpec

    def scan_columns(self, *, provenance: bool) -> list[str] | dict[str, pc.Expression]:
        # Scanner columns can be a list[str] or dict[new_name: Expression] :contentReference[oaicite:5]{index=5}
        if self.projection.computed:
            cols: dict[str, pc.Expression] = {c: pc.field(c) for c in self.projection.base_cols}
            for name, expr in self.projection.computed:
                cols[name] = expr
            if provenance:
                cols.update({
                    "prov_filename": pc.field("__filename"),
                    "prov_fragment_index": pc.field("__fragment_index"),
                    "prov_batch_index": pc.field("__batch_index"),
                    "prov_last_in_fragment": pc.field("__last_in_fragment"),
                })
            return cols

        cols_list = list(self.projection.base_cols)
        if provenance:
            cols_list += ["__filename", "__fragment_index", "__batch_index", "__last_in_fragment"]
        return cols_list
```

Why we expose both `predicate` and `pushdown_predicate`:

* Scan-time `filter` guarantees correctness even without pushdown, but you still want a **cheap, reliable pruning telemetry signal** (how many fragments would we touch *if pruning works*?). `Dataset.get_fragments(filter=...)` explicitly supports fragment selection using partition expression or Parquet statistics. ([Apache Arrow][4])

---

## 4) Compiling QuerySpec to scan + plan (the repeatable pattern)

### 4.1 Scan control plane (Dataset/Scanner)

Scanner projection supports:

* list of column names, or
* dict `{new_column_name: expression}` for advanced projections, including special fields (`__filename`, `__fragment_index`, `__batch_index`, `__last_in_fragment`). ([Apache Arrow][2])

Scanner filtering:

* returns rows matching filter;
* attempts pushdown using partition info / Parquet stats;
* otherwise filters loaded record batches before yielding. ([Apache Arrow][2])

So the scanner call becomes completely standardized:

```python
import pyarrow.dataset as ds

def make_scanner(dataset: ds.Dataset, spec: QuerySpec, *, ctx) -> ds.Scanner:
    return ds.Scanner.from_dataset(
        dataset,
        columns=spec.scan_columns(provenance=ctx.provenance),
        filter=spec.predicate,
        use_threads=ctx.use_threads,
    )
```

### 4.2 Acero control plane (preferred in the DSL)

When you build an Acero plan, you usually want **both**:

* pushdown hints at the scan node,
* and explicit filter/project nodes (semantic contract + modular composition).

Acero’s docs:

* `ScanNodeOptions` can apply pushdown projections/filters, but doesn’t build the final filter/project nodes. ([Apache Arrow][3])
* `FilterNodeOptions` selects rows where the expression evaluates to true; expression must be boolean. ([Apache Arrow][5])
* `ProjectNodeOptions` computes scalar (elementwise) expressions to create/rearrange columns. ([Apache Arrow][6])

**Canonical compilation:**

```python
import pyarrow.acero as acero

def compile_to_acero_scan(spec: QuerySpec, *, dataset, ctx) -> acero.Declaration:
    scan_opts = acero.ScanNodeOptions(
        dataset,
        columns=spec.scan_columns(provenance=ctx.provenance),
        filter=spec.pushdown_predicate,  # optimization hint
        # other Scanner.from_dataset kwargs can be wired here
    )
    decl = acero.Declaration("scan", scan_opts)

    # Semantic filter (full predicate)
    if spec.predicate is not None:
        decl = acero.Declaration("filter", acero.FilterNodeOptions(spec.predicate), inputs=[decl])

    # Shape (projection) — ensure output columns and names are exactly what downstream expects
    # If you used dict projection at scan time, you can often skip this project node.
    # Keep it if you need renames or post-join computed fields.
    return decl
```

This is the “repeatable pushdown” pattern:

* **scan**: pruning/projection hints
* **filter/project nodes**: correctness + composability

---

## 5) Pushdown tactics that actually work

### 5.1 Start with the dataset layout truth

Arrow datasets are “split across multiple files,” and partitioning can accelerate queries that only touch some partitions (files). ([Apache Arrow][4])
So pushdown success is partly a **data layout choice**.

**Best practice:** write datasets partitioned by the things you filter on all the time (repo_id, kind, maybe major pipeline stage).

### 5.2 Prefer “prunable” predicates

From the Scanner docs, pushdown relies on partition information or internal metadata like Parquet statistics. ([Apache Arrow][2])

Heuristics that tend to prune well:

* equality/inequality comparisons on partition keys (`repo_id == 1`, `kind == "call"`)
* simple range predicates on columns with good statistics (`confidence >= 0.8`)

Heuristics that often **don’t** prune well:

* predicates that wrap columns in functions (`lower(kind) == "call"`)
* complex regex on non-partitioned string columns
* deeply nested boolean logic where only part is prunable (rewrite it)

### 5.3 Split “planning filter” from “semantic filter”

This is why `pushdown_predicate` exists in `QuerySpec`.

* Use `pushdown_predicate` for:

  * fragment planning (`get_fragments`)
  * explaining “why did we touch so many files?”
* Use `predicate` for:

  * correctness (the actual dataset subset you want)

`Dataset.get_fragments(filter=...)` explicitly supports matching fragments “either using the partition_expression or internal information like Parquet’s statistics.” ([Apache Arrow][4])

### 5.4 Make pushdown visible with preflight telemetry

In your runner (or scan helper), record:

```python
def scan_telemetry(dataset, spec):
    frags = list(dataset.get_fragments(filter=spec.pushdown_predicate))
    return {"fragments_selected": len(frags)}
```

Even if you can’t prove “pushdown happened inside Parquet,” you can prove “this predicate should prune fragments,” and you can budget based on `fragments_selected`.

---

## 6) Projection tactics: make columns a first-class performance lever

The Scanner docs explicitly say:

* columns can be list or dict mapping `{new_column_name: expression}`,
* special fields are allowed,
* and columns are passed down to avoid loading/copying/deserializing unused columns. ([Apache Arrow][2])

**Rules you standardize:**

1. Default to **thin scans**: only columns needed downstream.
2. Add provenance columns when `ctx.provenance=True`.
3. Use dict projection for computed columns *only when it reduces downstream cost* (e.g., you want one normalized key column, not 5 raw columns).

Also: `ProjectNodeOptions` requires scalar (elementwise) expressions, so keep computed columns there when they depend on other computed fields or you need to rename/reorder in a controlled way. ([Apache Arrow][6])

---

## 7) Running example: QuerySpec for Pipeline A scan

Let’s make Pipeline A’s left-side scan (“call_sites”) repeatable.

### 7.1 Define the QuerySpec

```python
from arrowdsl.expr import E
from arrowdsl.queryspec import QuerySpec, ProjectionSpec

call_sites_spec = QuerySpec(
    # semantic filter (correctness)
    predicate=E.and_(
        E.eq("kind", "call"),
        E.ge("confidence", 0.8),
    ),
    # pruning filter (planning + scan hint)
    pushdown_predicate=E.eq("kind", "call"),
    projection=ProjectionSpec(
        base_cols=("repo_id","caller_id","callee_id","kind","confidence","span_start","span_end","file_id"),
    ),
)
```

### 7.2 Use it for Dataset scan

```python
scanner = make_scanner(call_sites_dataset, call_sites_spec, ctx=ctx)
table = scanner.to_table()  # or to_reader() depending on your boundary
```

### 7.3 Use it for Acero scan (preferred in integrated plans)

```python
left_decl = compile_to_acero_scan(call_sites_spec, dataset=call_sites_dataset, ctx=ctx)
```

Now your *entire project* has one canonical way to represent “scan this dataset with these filters and columns.”

---

## 8) Artifacts to ship with Chapter 6 (so it’s executable)

### Code

* `src/.../arrowdsl/expr.py` — `E` macros
* `src/.../arrowdsl/queryspec.py` — `QuerySpec`, `ProjectionSpec`
* `src/.../arrowdsl/scan.py` — `make_scanner`, `scan_telemetry`
* `src/.../arrowdsl/acero_compile.py` — `compile_to_acero_scan`

### Tests

1. `tests/arrowdsl/test_ch06_expr_macros_equivalence.py`

   * Build a small `InMemoryDataset` and a `table_source` plan.
   * Use the same QuerySpec in both paths and assert results match after canonical sort.

2. `tests/arrowdsl/test_ch06_pushdown_smoke.py`

   * Create a tiny **partitioned dataset** (`kind=call/`, `kind=import/`).
   * Assert `len(list(dataset.get_fragments(filter=E.eq("kind","call")))) == 1` (or expected).
     This is grounded in the documented `get_fragments(filter=...)` behavior (partition expression / stats). ([Apache Arrow][4])

### Bench

* `bench/arrowdsl/bench_ch06_pushdown_vs_no_pushdown.py`

  * Compare scanning with a pruning predicate vs scanning without it, measuring fragment counts + elapsed.

---

### The one-liner that keeps this chapter honest

If the DSL doesn’t make it *easier* to do pushdown than not, it won’t happen.

So the default should be:

* **every scan** uses a QuerySpec,
* every QuerySpec has a pushdown_predicate (even if it’s `None`),
* every run records fragment-selection telemetry.

[1]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Expression.html "pyarrow.dataset.Expression — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html "pyarrow.dataset.Scanner — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.ScanNodeOptions.html "pyarrow.acero.ScanNodeOptions — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Dataset.html "pyarrow.dataset.Dataset — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.FilterNodeOptions.html "pyarrow.acero.FilterNodeOptions — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.ProjectNodeOptions.html "pyarrow.acero.ProjectNodeOptions — Apache Arrow v22.0.0"


# Chapter 7 — Scan control plane as named profiles (dev / CI / prod)

Chapter 6 gave you **QuerySpec** + expression macros so pushdown becomes repeatable. Now we make that *operationally real*:

> **Scanning is your “free speed lever,” but only if it’s centralized and policy-driven.**
> The moment every node picks its own `batch_size`, `readahead`, `use_threads`, and thread pools, you get performance drift, contention, and nondeterministic debugging pain.

This chapter defines a **scan control plane** with **named profiles** you can apply consistently in local dev, CI, and production—while still fitting the DSL shape: *declare → execute → finalize*.

---

## 0) What’s new vs recap

### New in this chapter

* A small **ScanProfile** dataclass (batch sizing, readahead, threading, format options).
* A small **RuntimeProfile** that bundles:

  * global Arrow thread pools (CPU + I/O),
  * per-scan `use_threads`,
  * per-plan `use_threads`,
  * determinism tier defaults.
* A single `make_scanner()` (or `scan_builder`) that every pipeline uses.
* A “scan telemetry” hook (fragment counts, estimated rows) that you record on every run.

### Recap (brief)

* Batch sizing is a throughput/latency tradeoff; start from defaults and tune deliberately.
  The docs recommend treating `batch_size` as “how many rows you want per chunk when materialized,” and increasing in powers of two for throughput cases.
* Readahead (`batch_readahead`, `fragment_readahead`) improves IO utilization at the cost of more in-flight memory; too much can harm cache locality.
* Prefer streaming surfaces (`to_reader()` / `to_batches()`) and only materialize to a table at deliberate boundaries (like finalize/caching).

---

## 1) The scan control plane: three knobs, three scopes

### 1.1 Global thread pools (process scope)

Your compiled guidance already anchors a key point: Arrow has global pools for CPU and I/O, and **dataset scanning implicitly uses the I/O thread pool**.

**Policy rule:** set these once at process startup (CLI entrypoint, worker init) and record them in run metadata.

### 1.2 Per-scan threading (Scanner scope)

Scanner has a `use_threads` knob that determines whether it uses maximum parallelism based on available CPU cores.

**Policy rule:** `Scanner.use_threads` must come from a profile, not from ad hoc node code.

### 1.3 Per-plan threading (Acero execution scope)

If you execute an Acero plan via `Declaration.to_table(use_threads=...)`, `use_threads=False` forces CPU work onto the calling thread while I/O still uses the I/O executor.

**Policy rule:** plan execution `use_threads` comes from the same runtime profile as scan `use_threads`, so you can avoid oversubscription.

---

## 2) ScanProfile: the minimal stable “knob bundle”

### 2.1 What belongs in ScanProfile

At minimum:

* `batch_size` (default 131,072 rows)
* `batch_readahead` (default 16)
* `fragment_readahead` (default 4)
* `use_threads` (default True)
* `cache_metadata` (default True)
* optional `fragment_scan_options` (format-specific, especially Parquet)

Those defaults and meanings are part of the documented Scanner control surface.

### 2.2 Why profiles are better than “tuning by instinct”

Your doc already frames a practical tuning stance:

* Start with default `batch_size`, then move in **powers of two** for throughput workloads.
* Increase `fragment_readahead` first when you have many files, and `batch_readahead` when files are large/high-latency.
* For high latency filesystems, Parquet `pre_buffer` is the kind of knob worth benchmarking (speed > minimal memory).

Those are “scan-profile decisions,” not pipeline-specific decisions.

### 2.3 Reference dataclass

```python
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Literal

@dataclass(frozen=True)
class ScanProfile:
    name: str

    # Scanner knobs
    batch_size: int = 131_072
    batch_readahead: int = 16
    fragment_readahead: int = 4
    use_threads: bool = True
    cache_metadata: bool = True

    # Optional: format-specific scan options (e.g., ParquetFragmentScanOptions)
    fragment_scan_options: Any | None = None

    # Optional: if you want to bias for “first results fast” vs “throughput”
    intent: Literal["latency", "throughput"] = "throughput"
```

---

## 3) RuntimeProfile: named presets that include thread pools + scan profile

Your earlier sizing guidance maps cleanly into three production realities:

* Local dev: CPU pool ~50–75% of hardware threads; I/O pool moderate (8–32).
* CI: CPU pool small fixed (2–8); I/O pool small/moderate (4–16).
* Production single host: CPU pool near cores minus headroom; I/O pool depends on storage latency (NVMe vs object storage).

Make those *named* runtime profiles so your pipeline behavior is predictable.

### 3.1 Reference dataclass

```python
from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class RuntimeProfile:
    name: str

    # Global Arrow pools (set once at process startup)
    cpu_threads: Optional[int]
    io_threads: Optional[int]

    # Per-plan execution (Acero) CPU threading
    plan_use_threads: bool

    # Scan profile bundle
    scan: ScanProfile

    # Determinism default for this runtime mode (ties into Chapter 3)
    determinism: str  # "canonical" | "stable_set" | "best_effort"
```

### 3.2 Suggested named profiles (the minimal set)

* `DEV_FAST`: high throughput while staying responsive
* `DEV_DETERMINISTIC`: minimal threads to make debugging stable
* `CI_STABLE`: fixed resources, stable runtimes
* `PROD_THROUGHPUT`: saturate the box without starving the service

**Key design rule:** keep profile count small. Every new profile is a new test matrix.

---

## 4) The “one sanctioned scanner builder” (so nothing drifts)

Your compiled doc already includes the canonical idea: **if you standardize one thing, make it a scanner builder**, because it’s where you enforce pushdown conventions, provenance columns, and planning telemetry.

### 4.1 `make_scanner()` wired to profiles

```python
from __future__ import annotations
import pyarrow.dataset as ds

def make_scanner(
    dataset: ds.Dataset,
    *,
    columns,
    filter_expr,
    profile: ScanProfile,
    memory_pool=None,
) -> ds.Scanner:
    return ds.Scanner.from_dataset(
        dataset,
        columns=columns,
        filter=filter_expr,
        batch_size=profile.batch_size,
        batch_readahead=profile.batch_readahead,
        fragment_readahead=profile.fragment_readahead,
        fragment_scan_options=profile.fragment_scan_options,
        use_threads=profile.use_threads,
        cache_metadata=profile.cache_metadata,
        memory_pool=memory_pool,
    )
```

### 4.2 Planning telemetry: fragments + rows

Two cheap “budget signals” you should log every run:

* `dataset.get_fragments(filter=...)` → how many files/fragments will be touched
* `dataset.count_rows(filter=...)` → rough query size before heavy work

These are explicitly recommended “cheap planning helpers.”

---

## 5) Streaming vs materialization: encode as profile policy

A recurring performance footgun is unintentional materialization.

Your compiled notes emphasize:

* Scanner exposes `to_reader()` / `to_batches()` / `scan_batches()` / `to_table()` and recommends using `to_table()` only at deliberate boundaries (finalize gate).
* Keep a `RecordBatchReader` as long as possible for scheduling/cache behavior, even if memory isn’t the concern.

**Profile rule:** decide whether a profile is “stream-first” vs “table-first” and apply that consistently.

Example convention:

* `DEV_FAST`, `PROD_THROUGHPUT`: stream-first (use readers until finalize)
* `CI_STABLE`: table-first (simpler tests; fewer moving parts)
* `DEV_DETERMINISTIC`: table-first + low threads

---

## 6) Oversubscription guardrail: one layer owns parallelism

Your compiled guidance states the classic failure mode:

If you let orchestration parallelize *and* Arrow runs at max threads, you get oversubscription and throughput collapse. The fix is to choose one layer to own parallelism per run mode.

**In this guide’s architecture**, you’re trending toward “few big fused Arrow plans,” so the default stance is:

* Let Arrow be the parallel engine.
* Keep orchestration concurrency lower.

That’s a runtime profile choice, not a code change.

---

## 7) Chapter 7 artifacts (so it’s executable)

### Code

* `arrowdsl/profiles.py`

  * defines `ScanProfile`, `RuntimeProfile`, and a small map `{name → profile}`
* `arrowdsl/scan.py`

  * `make_scanner(...)`
  * `scan_telemetry(...)`

### Tests

* `tests/arrowdsl/test_ch07_profiles_apply_correctly.py`

  * asserts that each profile sets the expected:

    * global thread pool values (via `pa.cpu_count()`/`pa.io_thread_count()` after config)
    * scanner knob values (batch_size/readahead/use_threads)
  * asserts provenance fields are enabled when the profile requests them (ties into Chapter 4)

### Bench

* `bench/arrowdsl/bench_ch07_scan_sweep.py`

  * parameter sweep over:

    * batch_size ∈ {64k, 128k, 256k, 512k}
    * fragment_readahead ∈ {4, 8, 16}
    * batch_readahead ∈ {16, 32}
  * records:

    * elapsed time
    * fragment count touched
    * output row count (sanity)
  * produces a JSON/CSV report you can use to pick default profile settings

---

### The “don’t let it regress” rule

Once profiles exist, **ban ad hoc scan knob tuning** in nodes. If someone needs a different knob, they either:

* add a new named profile (rare), or
* justify why the existing profile can’t support the workload.

That single discipline is what makes “scan control plane” real rather than theoretical.

# Chapter 8 — Phase 1–2 capstone: one golden pipeline + regression gates

Chapters 1–7 built the foundation:

* ExecPlan mental model (what Acero really does)
* Ordering realities (join/agg destroy order)
* Determinism budget + dedupe policy (explicit, contract-driven)
* Observability artifacts (good/errors/alignment/stats + provenance)
* A tiny typed Arrow DSL IR (Plan/Contract/ExecutionContext/FinalizeResult)
* Expression macros + QuerySpec (pushdown repeatable)
* Scan control plane profiles (dev/CI/prod)

This chapter is the **capstone**: we ship **one golden, end-to-end pipeline** and the **regression gates** that keep it best-in-class over time.

> The goal is not “a demo.” The goal is “something you can run in CI and production that prevents drift.”

---

## 0) What “golden” means here

A golden pipeline has:

1. **A single canonical entry point** (`run_enrich_call_sites(...)`)
2. **A fixed contract** (schema + invariants + dedupe + canonical ordering)
3. **A deterministic output mode** (Tier A / CANONICAL) suitable for snapshots and caching
4. **Structured observability outputs** always present in tolerant mode
5. **Regression gates**:

   * correctness (schema + invariants)
   * determinism (output stable under reorder / threaded execution)
   * performance (benchmark drift budgets)

---

## 1) Golden pipeline definition (Pipeline A): `enrich_call_sites`

### Inputs

* `call_sites` dataset (facts)
* `symbols` dataset (dimension)

### Output

* `enriched_call_sites` contract dataset:

  * canonical sorted order (Tier A)
  * deduped per contract spec
  * error rows captured with provenance

This pipeline is intentionally “real enough” to exercise:

* scan + pushdown
* join
* deterministic ordering
* finalize gate artifacts

---

## 2) Contract for the golden pipeline

### 2.1 Output schema (example)

Keep it minimal but realistic:

* `repo_id: int64`
* `caller_id: int64`
* `callee_id: int64`
* `kind: string`
* `confidence: float32`
* `span_start: int32`
* `span_end: int32`
* `file_id: int64`
* `callee_symbol_kind: string` (from symbols dim)
* `callee_fqname: string` (from symbols dim)

Plus optional provenance columns if your profile enables them.

### 2.2 Invariants (minimum)

* required non-null keys: `repo_id, caller_id, callee_id, file_id, span_start, span_end`
* span validity: `span_start <= span_end`
* confidence bounds: `0.0 <= confidence <= 1.0`

### 2.3 Dedupe spec

Semantic identity for call-site facts is usually:

* keys: `(repo_id, caller_id, callee_id, file_id, span_start, span_end, kind)`

Tie-breakers:

* `confidence DESC` (winner is highest-confidence duplicate)

Strategy:

* `KEEP_FIRST_AFTER_SORT` (canonical sort then first-wins)

### 2.4 Canonical ordering

Total order keys:

* `(repo_id, caller_id, callee_id, file_id, span_start, span_end, kind, confidence DESC)`

This is the order you snapshot, cache, and hand to downstream consumers.

---

## 3) The golden pipeline implementation (integrated DSL)

### 3.1 QuerySpecs (pushdown + semantics)

**Call sites**: filter to relevant kinds and confidence threshold.

```python
from arrowdsl.expr import E
from arrowdsl.queryspec import QuerySpec, ProjectionSpec

CALL_SITES_SPEC = QuerySpec(
    predicate=E.and_(E.eq("kind", "call"), E.ge("confidence", 0.0)),
    pushdown_predicate=E.eq("kind", "call"),
    projection=ProjectionSpec(
        base_cols=("repo_id","caller_id","callee_id","kind","confidence","span_start","span_end","file_id"),
    ),
)

SYMBOLS_SPEC = QuerySpec(
    predicate=None,
    pushdown_predicate=None,
    projection=ProjectionSpec(base_cols=("symbol_id","symbol_kind","fqname")),
)
```

### 3.2 Plan lane (Acero): scan + join + project

```python
import pyarrow.acero as acero
import pyarrow.compute as pc

def build_enrich_call_sites_plan(*, call_sites_ds, symbols_ds, ctx) -> Plan:
    left_scan = acero.Declaration(
        "scan",
        acero.ScanNodeOptions(
            call_sites_ds,
            columns=CALL_SITES_SPEC.scan_columns(provenance=ctx.provenance),
            filter=CALL_SITES_SPEC.pushdown_predicate,
        ),
    )
    # Semantic filter node (correctness)
    left = acero.Declaration("filter", acero.FilterNodeOptions(CALL_SITES_SPEC.predicate), inputs=[left_scan])

    right_scan = acero.Declaration(
        "scan",
        acero.ScanNodeOptions(
            symbols_ds,
            columns=SYMBOLS_SPEC.scan_columns(provenance=False),
            filter=None,
        ),
    )
    right = right_scan

    # Join
    join = acero.Declaration(
        "hashjoin",
        acero.HashJoinNodeOptions(
            join_type="left outer",
            left_keys=["callee_id"],
            right_keys=["symbol_id"],
            left_output=[
                "repo_id","caller_id","callee_id","kind","confidence","span_start","span_end","file_id",
                # include prov_* if present
                *([ "prov_filename","prov_fragment_index","prov_batch_index","prov_last_in_fragment" ] if ctx.provenance else []),
            ],
            right_output=["symbol_kind", "fqname"],
            output_suffix_for_left="",
            output_suffix_for_right="",
        ),
        inputs=[left, right],
    )

    # Post-join project: rename right payload to stable output names
    proj_exprs = [
        pc.field("repo_id"),
        pc.field("caller_id"),
        pc.field("callee_id"),
        pc.field("kind"),
        pc.field("confidence"),
        pc.field("span_start"),
        pc.field("span_end"),
        pc.field("file_id"),
        pc.field("symbol_kind"),
        pc.field("fqname"),
    ]
    proj_names = [
        "repo_id","caller_id","callee_id","kind","confidence","span_start","span_end","file_id",
        "callee_symbol_kind","callee_fqname",
    ]
    if ctx.provenance:
        proj_exprs += [pc.field("prov_filename"), pc.field("prov_fragment_index"), pc.field("prov_batch_index"), pc.field("prov_last_in_fragment")]
        proj_names += ["prov_filename","prov_fragment_index","prov_batch_index","prov_last_in_fragment"]

    out = acero.Declaration("project", acero.ProjectNodeOptions(proj_exprs, proj_names), inputs=[join])

    return Plan(decl=out, label="enrich_call_sites")
```

This is the “integrated” pattern you want:

* scan with pushdown hints
* explicit filter for correctness
* join inside Acero
* explicit projection to enforce stable column names

### 3.3 Kernel lane (post): dedupe + canonical sort (Tier A)

```python
from arrowdsl.kernels import dedupe_keep_first_after_sort, canonical_sort_if_canonical

def post_steps(contract):
    def _dedupe(t, ctx):
        if contract.dedupe is None:
            return t
        return dedupe_keep_first_after_sort(t, spec=contract.dedupe, ctx=ctx)

    def _canon_sort(t, ctx):
        return canonical_sort_if_canonical(t, sort_keys=contract.canonical_sort, ctx=ctx)

    return [_dedupe, _canon_sort]
```

### 3.4 Finalize gate: contract + artifacts

Finally:

```python
def run_enrich_call_sites(*, call_sites_ds, symbols_ds, contract, ctx, mode="tolerant"):
    plan = build_enrich_call_sites_plan(call_sites_ds=call_sites_ds, symbols_ds=symbols_ds, ctx=ctx)
    return run_pipeline(plan=plan, post=post_steps(contract), contract=contract, mode=mode, ctx=ctx)
```

---

## 4) Regression gates (what you ship with the guide)

The capstone is as much about tests/benches as it is about the pipeline.

### Gate 1 — Contract correctness gate

Asserts:

* output schema matches contract
* required columns non-null
* invariants enforced
* errors table captures violations

**Test:** `test_ch08_end_to_end_pipeline_a_contract.py`

### Gate 2 — Determinism gate (the drift killer)

Asserts:

* changing input order does not change canonical output under `CANONICAL`
* threaded vs unthreaded execution does not change canonical output (after canonical sort)

**Test:** `test_ch08_end_to_end_pipeline_a_determinism.py`

Mechanism:

1. Build a small dataset fixture with duplicates and unsorted rows
2. Run pipeline under:

   * `ctx.use_threads=True`
   * `ctx.use_threads=False`
3. Assert canonical sorted `good` output tables are identical

### Gate 3 — Observability gate

Asserts:

* tolerant mode always returns `FinalizeResult`
* `errors` has `row_id`, `error_code`, `stage`, key fields
* if provenance enabled, `errors` includes `prov_*` and `good` includes them too

**Test:** `test_ch08_end_to_end_pipeline_a_observability.py`

### Gate 4 — Performance regression gate (budgeted)

This is not “make CI slow.” It’s “catch 3× regressions.”

**Bench:** `bench_ch08_end_to_end.py`

* generates a deterministic synthetic dataset (e.g., 1M rows with fixed seed)
* runs pipeline under `PROD_THROUGHPUT` profile
* records:

  * elapsed time
  * output row count
  * number of fragments touched
* compares to a stored baseline with a tolerance (e.g., +20%)

**Artifacts:**

* `bench/baselines/enrich_call_sites.json`
* `bench/reports/<timestamp>.json`

**Budget discipline:**

* Use short benchmarks in CI (smaller dataset)
* Run full benchmarks nightly or on release branches

---

## 5) Fixture strategy (so tests are fast and meaningful)

### Tiny Parquet dataset generator (recommended)

Ship a generator that writes:

* partitioned `call_sites` dataset by `kind=call/import` (so pruning is testable)
* small `symbols` dataset

This lets you test:

* fragment pruning via `pushdown_predicate`
* provenance columns (`__filename`, etc.)
* join behavior

### In-memory fallback fixtures

For unit tests that don’t need scanning behavior, keep everything in-memory for speed and determinism.

---

## 6) “One command” execution (make the guide runnable)

Ship a CLI entrypoint:

* `python -m tools.arrowdsl.run_enrich_call_sites --profile CI_STABLE --mode tolerant --out ./artifacts/`

Outputs:

* `good.parquet`
* `errors.parquet`
* `alignment.parquet`
* `stats.parquet`
* `run_manifest.json`

This turns the guide from “concept” to “executable cookbook.”

---

## 7) Chapter 8 ship list

### Code

* `tools/arrowdsl/run_enrich_call_sites.py`
* `fixtures/generate_call_sites_dataset.py`
* `arrowdsl/pipelines/enrich_call_sites.py` (plan builder + runner glue)
* `arrowdsl/contracts/enriched_call_sites.py` (contract object)

### Tests

* `tests/arrowdsl/test_ch08_end_to_end_pipeline_a_contract.py`
* `tests/arrowdsl/test_ch08_end_to_end_pipeline_a_determinism.py`
* `tests/arrowdsl/test_ch08_end_to_end_pipeline_a_observability.py`

### Bench

* `bench/arrowdsl/bench_ch08_end_to_end.py`
* `bench/arrowdsl/baselines/enrich_call_sites.json`

---

## 8) The capstone “promise” (what you get if you implement this)

After Phase 1–2, you have:

* a small, stable Arrow DSL IR
* deterministic, contract-driven outputs (when required)
* structured observability artifacts always available
* a golden pipeline you can copy for every new dataset
* regression gates that prevent drift across Arrow upgrades, refactors, and threading changes

From here, Phase 3+ becomes much easier because you can add capabilities (nested modeling, explode, advanced joins/aggregations) without losing control of correctness or performance.
