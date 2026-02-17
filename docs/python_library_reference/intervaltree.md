
Below is a **feature catalog** of Python’s `intervaltree` library, clustered into semantically coherent sections and written in the same “surface area + power knobs + footguns” style as your PyArrow reference. 

---

# 0) Mental model (what the library *is*)

`intervaltree` provides:

* `Interval(begin, end, data)` objects representing **half-open intervals** **[begin, end)** (lower bound included, upper bound excluded). It is explicitly designed for “tagging text and time intervals” with string-slice semantics; **0-length intervals are null** and overlap nothing, so `end` must be strictly greater than `begin`. ([GitHub][1])
* `IntervalTree`: a **mutable, self-balancing** interval tree supporting point queries, overlap queries, and envelopment (“contained-by”) queries. The project also calls out **automatic AVL balancing**. ([GitHub][1])
* Query results are **sets of `Interval`**; if you need deterministic ordering, you sort them (typical pattern: `sorted(tree[point])`). ([GitHub][1])

---

# 1) Core objects and “data model” surface

## 1.1 `Interval(begin, end, data=None)` (record + payload)

**Primary fields / tuple behavior**

* `iv.begin`, `iv.end`, `iv.data` are the public data plane. ([GitHub][1])
* `begin, end, data = iv` tuple-unpacks. ([GitHub][1])

**Ordering semantics (affects sorting + some merge patterns)**

* Intervals are ordered by `begin`, then `end`, then `data`; if `data` is not orderable, it falls back to sorting by `type(data)`. (This matters if you do `sorted(tree)` or `sorted(tree[...])` and want deterministic results.) ([GitHub][2])

---

# 2) Construction / initialization

## 2.1 Build an `IntervalTree`

* Empty: `tree = IntervalTree()` ([GitHub][1])
* From an iterable of `Interval`: `tree = IntervalTree(intervals)` ([GitHub][1])
* From tuples: `IntervalTree.from_tuples([(begin, end), ...])` (tuples may omit `data` if you don’t need payloads). ([GitHub][1])

---

# 3) Insertions and bulk updates

## 3.1 Add intervals (single)

* Slice assignment: `tree[begin:end] = data` (idiomatic; matches the half-open semantics). ([GitHub][1])
* Explicit: `tree.add(interval)` ([GitHub][1])
* Convenience: `tree.addi(begin, end, data)` ([GitHub][1])

## 3.2 Bulk add

* `tree.update(iterable)` (this is the preferred bulk method; `extend(...)` was removed in v3 in favor of `update`). ([GitHub][1])
* Operator forms: `tree |= other_tree` (union-update). ([GitHub][1])

---

# 4) Deletions and range removal

## 4.1 Remove exact intervals

* `tree.remove(interval)` → raises `ValueError` if missing ([GitHub][1])
* `tree.discard(interval)` → no-op if missing ([GitHub][1])
* Shorthands:

  * `tree.removei(begin, end, data)`
  * `tree.discardi(begin, end, data)` ([GitHub][1])

## 4.2 Remove by overlap/envelopment

* `tree.remove_overlap(point)`
* `tree.remove_overlap(begin, end)` (removes all intervals overlapping the query range) ([GitHub][1])
* `tree.remove_envelop(begin, end)` (removes all intervals **fully contained** in the query range) ([GitHub][1])

## 4.3 Delete syntax sugar

* `del tree[point]` is documented as equivalent to `tree.remove_overlap(point)` in examples. ([GitHub][1])
* `tree.clear()` empties the tree. ([GitHub][1])

---

# 5) Query surface (the “read path” APIs)

## 5.1 Version note (important API drift)

* v3 removed `search(begin, end, strict)`; use:

  * `at(point)`
  * `overlap(begin, end)`
  * `envelop(begin, end)` ([GitHub][1])

## 5.2 Point queries (“what covers this coordinate?”)

* `tree[point]` → set of intervals containing `point` ([GitHub][1])
* `tree.at(point)` → same semantics ([GitHub][1])

## 5.3 Range overlap queries (“what intersects this span?”)

* `tree[begin:end]` → set of intervals that overlap the query range ([GitHub][1])
* `tree.overlap(begin, end)` → same semantics ([GitHub][1])

**Half-open semantics are operationally critical**: queries are over `begin ≤ x < end`. Example: querying `[2,4)` returns none if you only have intervals `[1,2)` and `[4,7)` because neither includes points in `[2,4)`. ([GitHub][1])

## 5.4 Envelopment queries (“what is fully contained?”)

* `tree.envelop(begin, end)` → intervals completely enveloped by the query range (i.e., contained-by it). ([GitHub][1])

---

# 6) Membership + predicate helpers

* Exact membership: `interval_obj in tree` (explicitly called out as **fastest**, O(1)). ([GitHub][1])
* `tree.containsi(begin, end, data)` (membership helper for tuple form) ([GitHub][1])
* “Does anything overlap?”:

  * `tree.overlaps(point)`
  * `tree.overlaps(begin, end)` ([GitHub][1])

---

# 7) Iteration, sizing, and boundary introspection

## 7.1 Iteration forms

* `for interval in tree:` (tree is iterable) ([GitHub][1])
* `tree.items()` (explicit items access) ([GitHub][1])

## 7.2 Size / emptiness

* `len(tree)`, `tree.is_empty()`, and `not tree` are all supported idioms. ([GitHub][1])

## 7.3 Coordinate extents (useful for “span mapping” systems)

* `tree.begin()` → begin coordinate of the leftmost interval
* `tree.end()` → end coordinate of the rightmost interval ([GitHub][1])

---

# 8) Set algebra (IntervalTree as a set-like container)

`IntervalTree` supports classic set operations via both **methods** and **operators**: ([GitHub][1])

## 8.1 Union

* `tree.union(iterable)` and `tree1 | tree2`
* In-place: `tree.update(iterable)` and `tree |= other_tree`

## 8.2 Difference

* `tree.difference(iterable)` and `tree1 - tree2`
* In-place: `tree.difference_update(iterable)` and `tree -= other_tree`

## 8.3 Intersection

* `tree.intersection(iterable)` and `tree1 & tree2`
* In-place: `tree.intersection_update(iterable)` and `tree &= other_tree`

## 8.4 Symmetric difference

* `tree.symmetric_difference(iterable)` and `tree1 ^ tree2`
* In-place: `tree.symmetric_difference_update(iterable)` and `tree ^= other_tree`

## 8.5 Comparisons

* subset/superset + rich comparisons: `issubset`, `issuperset`, `<=`, `>=`, `==` ([GitHub][1])

---

# 9) Restructuring / normalization (the “span surgery” toolbox)

This is the part you’ll likely lean on for *span mapping* and *non-overlapping canonicalization*.

## 9.1 `chop(begin, end, datafunc=None)`

* “Slice intervals and remove everything between `begin` and `end`,” producing up to two residual intervals per affected interval. ([GitHub][1])
* Optional `datafunc(iv, islower)` lets you rewrite payloads depending on which side survives (lower/upper). Example shape is shown in docs. ([GitHub][1])

## 9.2 `slice(point, datafunc=None)`

* Splits intervals at a specific point **without removing** anything; again optional data rewriting is supported. ([GitHub][1])

## 9.3 `split_overlaps(...)`

* “Slice at all interval boundaries” to eliminate overlaps by segmenting (optionally modifying `data`). ([GitHub][1])

## 9.4 Merge operations (coalescing)

* `merge_overlaps(...)`: “joins overlapping intervals into a single interval,” optionally merging data fields; methods that take `strict` now default `strict=True`. ([GitHub][1])
* `merge_equals(...)`: “joins intervals with matching ranges,” optionally merging data. ([GitHub][1])
* `merge_neighbors(...)`: joins adjacent intervals when their endpoints are within a threshold distance; can optionally merge overlaps and merge data fields. ([GitHub][1])

---

# 10) Copying, conversion, and serialization

* `IntervalTree(tree)` constructs a new tree that references the **same** `Interval` objects (structural copy, not deep copy). ([GitHub][1])
* `tree.copy()` produces a tree whose `Interval` objects are *shallow copies*. ([GitHub][1])
* Convert to plain containers:

  * `set(tree)`
  * `list(tree)` (and either can later be fed into `IntervalTree(...)`). ([GitHub][1])
* “Pickle-friendly” is explicitly called out as a feature. ([GitHub][1])

---

# 11) Performance/robustness notes you should treat as “API contracts”

* **Half-open everywhere**: both stored intervals and query spans follow `[begin, end)` semantics; this is the most important correctness invariant for any span system. ([GitHub][1])
* **No null intervals**: zero-length intervals are treated as null and overlap nothing; many user errors are “end == begin.” ([GitHub][1])
* **Self-balancing claim**: the project explicitly frames itself as “self-balancing” and mentions **AVL** (useful when you assume asymptotics for large interval corpora). ([GitHub][1])
* General interval tree complexity expectations (not specific guarantees): overlap queries are typically **O(log n + m)** where `m` is number of reported intervals; insert/delete **O(log n)**. ([Wikipedia][3])

---

If you want the *next increment* (analogous to your PyArrow doc), I can convert each section above into a “deep dive” that includes: exact argument semantics for `strict`, the reducer hooks for `merge_*` (data merge strategy), and a “span mapping playbook” (byte offsets vs (line,col), canonicalization strategy with `split_overlaps()` → `merge_neighbors()`, and deterministic sorting/caching patterns).

[1]: https://github.com/chaimleib/intervaltree "GitHub - chaimleib/intervaltree: A mutable, self-balancing interval tree. Queries may be by point, by range overlap, or by range containment."
[2]: https://github.com/chaimleib/intervaltree/blob/master/CHANGELOG.md?utm_source=chatgpt.com "intervaltree/CHANGELOG.md at master"
[3]: https://en.wikipedia.org/wiki/Interval_tree?utm_source=chatgpt.com "Interval tree"

Below is the “next increment” deep-dive (analogous to a PyArrow-style advanced note): **(A)** exact `strict` semantics, **(B)** the `merge_*` reducer hooks (data merge strategy, aliasing + determinism pitfalls), and **(C)** a **span-mapping playbook** (byte offsets vs `(line,col)`, `split_overlaps() → merge_neighbors()` canonicalization, deterministic sorting + caching).

I’m grounding signatures/behavior on **intervaltree tag `3.2.1`** (Dec 24, 2025)  and the upstream README for high-level invariants (half-open intervals, v3 “strict defaults True”). ([GitHub][1])

---

## A) `strict` semantics: what it *actually* gates (and what it doesn’t)

### A1) First invariant: everything is half-open `[begin, end)`

`intervaltree` is explicitly “string slice” semantics: intervals include the lower bound and exclude the upper bound; **0-length intervals are null** (so `end` must be strictly greater than `begin`). ([GitHub][1])
This matters because “touching” means `a.end == b.begin` and **there is no shared point**.

### A2) “strict” in v3: default is consistently `True`

Upstream notes that methods taking `strict` now default `strict=True` consistently (v3 cleanup). ([GitHub][1])

### A3) Where `strict` applies (3.2.1): merges, not queries

In **3.2.1**, `strict` shows up on:

* `merge_overlaps(..., strict=True)`
* `merge_neighbors(..., strict=True)`  (and verified by the 3.2.1 source signature)

It **does not** affect `at()`, `overlap()`, `envelop()`, or membership tests; those follow half-open semantics.

---

## A4) Exact semantics by method (3.2.1)

### `merge_overlaps(strict=True|False)`

Signature (3.2.1 source):
`merge_overlaps(self, data_reducer=None, data_initializer=None, strict=True)`

Operational condition (conceptually):

* Let `lower` be the last merged interval and `higher` be the next interval in sorted order.
* Overlap test: `higher.begin < lower.end`
* Touching adjacency: `higher.begin == lower.end`

Behavior:

* **strict=True (default):** merge only if **actual overlap** (`higher.begin < lower.end`). Adjacent touching intervals are *not* merged.
* **strict=False:** merge if overlap **or** adjacency (`higher.begin <= lower.end` in half-open terms).

This is exactly the “do we coalesce contiguous segments?” knob.

### `merge_neighbors(distance=1, strict=True|False)`

Signature (3.2.1 source):
`merge_neighbors(self, data_reducer=None, data_initializer=None, distance=1, strict=True)`

This is “merge by gap threshold,” implemented via arithmetic:

* `margin = higher.begin - lower.end`

Implications:

* Bounds must support subtraction (practically: **ints/floats**). If you store `(line, col)` tuples directly, this method will break.

Behavior:

* Candidate merge if `margin <= distance`

  * `margin == 0` → touching neighbors
  * `margin > 0` → separated by a gap
  * `margin < 0` → overlapping
* **strict=True (default):** merges only **discrete** neighbors within distance; if `margin < 0` (overlap), it *refuses* to merge that pair (starts a new “series”).
* **strict=False:** merges both neighbors **and** overlaps when `margin <= distance` (which is always true for overlaps since margin is negative).

Rule of thumb:

* Use **`merge_overlaps(strict=False)`** to coalesce *contiguous* segments.
* Use **`merge_neighbors(distance=k, strict=True)`** to “bridge small gaps” without accidentally collapsing overlaps.
* Use **`merge_neighbors(..., strict=False)`** if you want a very aggressive “glue everything close together” pass.

---

## B) `merge_*` reducer hooks: `data_reducer` / `data_initializer` (exact semantics + footguns)

### B1) Signatures (3.2.1)

* `merge_overlaps(data_reducer=None, data_initializer=None, strict=True)`
* `merge_neighbors(data_reducer=None, data_initializer=None, distance=1, strict=True)`
* `merge_equals(data_reducer=None, data_initializer=None)` (no `strict`)
  All three advertise “reduce-like” semantics, and in 3.2.1 the implementation indeed mirrors `functools.reduce` behavior.

### B2) What the reducer does

For a run of intervals being merged, the library maintains an accumulator `current_reduced_data` and, for each additional interval, performs:

```
current_reduced_data = data_reducer(current_reduced_data, new_data)
```

If `data_reducer is None`:

* the merged interval’s `.data` is forced to **`None`** (it “annihilates” data because it cannot guess a merge strategy).

### B3) How the initializer works (and when it aliases)

If `data_initializer is None` (default):

* The accumulator starts as **the first interval’s `.data` object**.
* That means the accumulator is **aliased** to an existing interval’s data.

If `data_initializer is not None`:

* The accumulator starts as a **shallow copy** of `data_initializer` (`copy.copy(data_initializer)`).
* Then it immediately reduces the first interval’s data into it.

#### Footgun: `data_initializer` without `data_reducer`

In 3.2.1 source, the initializer path calls `data_reducer(...)` immediately. If you pass `data_initializer` while leaving `data_reducer=None`, you’ll error. Treat it as a contract:

> If you supply `data_initializer`, you must supply `data_reducer`.

### B4) Determinism pitfalls: “equal under comparator” + set iteration

The merge routines sort intervals (`sorted(self.all_intervals)`) and then reduce in that order.

Intervals compare by `(begin, end, data)`; if data are not mutually orderable, comparison falls back to **type name** (so two different payloads of the same unorderable type compare “equal”). In that case:

* the sort’s relative order among “equal” items depends on the **input order**, and `all_intervals` is a **set** → iteration order can vary across runs (hash randomization).
* therefore, **your reduction order can become nondeterministic**, which matters if your reducer is non-commutative (e.g., “pick first,” “append in order,” etc.).

**Mitigation patterns:**

* Make `.data` **orderable** (e.g., tuple `(priority, stable_id, payload)`).
* Or make reducer commutative/associative (e.g., set union).
* Or normalize to a deterministic key before storing (store `data_key` + external lookup table).

### B5) High-leverage reducer recipes (span-mapping friendly)

#### 1) Accumulate annotations as a set (commutative, deterministic)

```python
def add_to_set(acc: set, x):
    acc.add(x)
    return acc

tree.merge_overlaps(
    strict=False,
    data_initializer=set(),
    data_reducer=add_to_set,
)
```

#### 2) Preserve a “best” annotation by priority (order-dependent unless priority total-orders)

```python
def pick_best(acc, x):
    return acc if acc.priority >= x.priority else x

tree.merge_overlaps(strict=False, data_reducer=pick_best)
```

If priorities can tie, include a stable tiebreaker.

#### 3) Merge dictionaries (watch shallow-copy + mutation)

```python
def merge_dict(acc: dict, x: dict):
    acc.update(x)
    return acc

tree.merge_equals(data_initializer={}, data_reducer=merge_dict)
```

If values are nested mutables, shallow copy may not be enough.

---

## C) Span mapping playbook (CodeIntel-ish): coordinates, canonicalization, determinism, caching

### C1) Pick one “primary coordinate axis” per file

**Strong recommendation:** store span bounds as **monotonic integers** per file.

Typical options:

* **UTF-8 byte offsets** (robust, fast, stable for tooling that already works in bytes)
* **Unicode codepoint offsets** (human-aligned but can disagree with external protocols)
* **Protocol-defined units** (e.g., if you’re interoperating with a system that defines offsets in a specific encoding)

Then derive everything else (line/col, UTF-16 positions, etc.) via side tables.

Why:

* All core operations work on comparable bounds; `merge_neighbors()` specifically needs subtraction.

### C2) `(line, col)` vs offsets: what to do

If you truly want `(line, col)`:

* **Do not use `merge_neighbors`** unless you wrap positions in a custom numeric type that supports subtraction.
* Safer: convert `(line,col)` ↔ offset using a newline index:

  * `line_starts = [0, ...]`
  * `offset = line_starts[line] + col`
  * Then store offsets in IntervalTree.

### C3) Data modeling for span mapping

Your `.data` should be:

* **hashable** (to live in sets and enable deterministic grouping)
* ideally **orderable** (to avoid nondeterministic reductions)
* small (avoid huge payload duplication if you do `split_overlaps()`)

Good pattern:

* `.data = (kind, stable_id, attrs)` where `attrs` is itself hashable (tuple/frozenset) or an interned reference key.

### C4) Canonicalization: `split_overlaps() → merge_neighbors()` (what it does and how to use it)

#### What `split_overlaps()` actually produces

`split_overlaps()` partitions the axis at **every boundary** present in the tree and rewrites intervals so **no interval crosses a boundary**.

Key nuance: it does **not** “union labels”; it **replicates** original interval data across the new atomic segments.

So after splitting, you get a “boundary-aligned segmentation” that is perfect for:

* building *atomic span units*,
* doing deterministic “segment joins,”
* and avoiding partial-overlap logic downstream.

But you may end up with fragmentation (many tiny adjacent segments).

#### Why `merge_neighbors()` after splitting

Once overlaps are eliminated, your fragmentation is typically **adjacent segments** that should be coalesced back into longer runs **when safe**.

However, **`merge_neighbors()` merges regardless of `.data` equality**; it’s purely geometric (+ reducer).

So the correct playbook is:

1. `tree.split_overlaps()`
2. **Group by a “merge key”** (usually the data key you consider equivalent)
3. Within each group, coalesce adjacency using `merge_neighbors(distance=0)` (or small distance if you tolerate gaps)

Concrete pattern:

```python
from collections import defaultdict
from intervaltree import IntervalTree

# 1) split
tree.split_overlaps()

# 2) bucket by merge-key (e.g., annotation identity)
buckets = defaultdict(IntervalTree)
for iv in tree:
    key = iv.data  # or normalize(iv.data)
    buckets[key].addi(iv.begin, iv.end, iv.data)

# 3) coalesce adjacency per key
for t in buckets.values():
    t.merge_neighbors(distance=0, strict=True)

# buckets now contains maximal contiguous runs per key
```

Notes:

* Use `distance=0` to merge only touching segments.
* Keep `strict=True` since after splitting you *shouldn’t* have overlaps; if you do, strict prevents accidental collapse and exposes bugs.

#### Alternative (often better): `merge_overlaps(strict=False)` for “touching” coalesce

If your bounds are not numeric (or you want to avoid subtraction), you can do per-key:

* `merge_overlaps(strict=False)` instead of `merge_neighbors(distance=0, ...)`
  because it merges adjacency using comparisons only.

### C5) Deterministic querying and stable outputs

All query results are sets. The README calls out that if ordering matters, **you must sort results**. ([GitHub][1])

For deterministic pipelines:

* Always do `sorted(hits, key=lambda iv: (iv.begin, iv.end, stable_key(iv.data)))`
* Ensure `stable_key` is stable across runs (no object ids, no unordered dict reprs).

### C6) Fast “best containing node” at a point (common in AST/CST mapping)

Pattern:

* `hits = tree.at(pos)`
* choose the **smallest span** (nearest container): `min(hits, key=lambda iv: iv.end - iv.begin)`
  This gives you the “most specific” interval containing the point (useful for “node at cursor”).

### C7) Caching patterns (mutation-safe)

`IntervalTree` is mutable; caching must be invalidated on writes.

A robust pattern:

* Maintain `tree_revision: int`
* Increment on any mutation (add/remove/merge/split/chop/slice)
* Cache key includes revision:

  * `cache[(tree_revision, point)] = sorted_hits`
  * `cache[(tree_revision, begin, end)] = sorted_hits`

If you do many point queries (e.g., cursoring through tokens), even a tiny LRU can pay off.

### C8) Range existence tests without materializing intervals

There’s an `overlaps_range(begin, end)` helper in 3.2.1 that answers “does anything overlap this span?” without returning all hits. Use it when you only need a boolean. (This is especially useful when a downstream pass is expensive.)

---

If you want the *next* increment after this: I can turn the span playbook into **three concrete “indexer-grade” recipes** tailored to your CodeIntel patterns:

1. **Byte-offset span index** for CST/AST nodes (nearest-node + containment queries)
2. **Occurrence span index** for SCIP occurrences (point-to-symbol + range-to-enclosing-def)
3. A deterministic **“segment normalizer”** that outputs canonical, stable, merge-key–coalesced spans suitable for embedding chunk boundaries and stable golden tests.

[1]: https://github.com/chaimleib/intervaltree "GitHub - chaimleib/intervaltree: A mutable, self-balancing interval tree. Queries may be by point, by range overlap, or by range containment."

Below are **three concrete, indexer-grade recipes** you can drop into CodeIntel as “span primitives.” They assume you store **half-open byte spans** `[begin, end)` everywhere, and treat `intervaltree` as an **in-memory accelerator** (build per file on demand / cache), while your canonical truth stays in Arrow/DuckDB.

---

# 1) Byte-offset span index for CST/AST nodes

**Goal:** `nearest-node-at(point)` + `enclosing-node-for(range)` + fast “what nodes overlap this edit span?”

## 1.1 Data model (minimal but sufficient)

Store a **single interval per syntax node**:

* `begin_b: int`, `end_b: int` (byte offsets in file, half-open)
* `node_id: int|str` (stable)
* `kind: str` (e.g., LibCST node type or AST node type)
* `depth: int` (optional but very helpful)
* `parent_id: int|str` (optional)
* `goid: str` (optional, if you already assign GOIDs for defs)

**Payload (`Interval.data`) should be orderable + hashable** for determinism:

```python
# orderable, hashable, cheap
NodeRef = tuple[int, int, str]  # (depth, node_id_int, kind_id_or_name)
```

If `node_id` isn’t an int, map it to a stable int in your node table.

## 1.2 Build pattern (per file)

Inputs: your `syntax_nodes_v1` dataset (Arrow/Polars) already computed from LibCST/ast.

```python
from intervaltree import IntervalTree

def build_syntax_tree(rows) -> IntervalTree:
    t = IntervalTree()
    for r in rows:  # row: (begin_b, end_b, depth, node_id_int, kind)
        b, e = r.begin_b, r.end_b
        if e <= b:  # drop null/degenerate
            continue
        t.addi(b, e, (r.depth, r.node_id_int, r.kind))
    return t
```

**Caching:** cache by `(repo_rev, file_id, syntax_rev)`; rebuild only when the underlying node table changes.

## 1.3 Query recipes

### A) Nearest node at a cursor point

Heuristic that works extremely well in practice: **smallest span wins**, then **deepest** (or vice versa; pick one and be consistent).

```python
def nearest_node_at(t: IntervalTree, pos_b: int):
    hits = t.at(pos_b)
    if not hits:
        return None
    # choose smallest interval; tie-break by greatest depth
    best = min(hits, key=lambda iv: ((iv.end - iv.begin), -iv.data[0], iv.data[1]))
    return best.data  # (depth, node_id, kind)
```

### B) Enclosing node for a selection range

Use `envelop(begin,end)` to get nodes fully containing the selection; choose the tightest.

```python
def enclosing_node_for(t: IntervalTree, begin_b: int, end_b: int):
    hits = t.envelop(begin_b, end_b)
    if not hits:
        return None
    best = min(hits, key=lambda iv: ((iv.end - iv.begin), -iv.data[0], iv.data[1]))
    return best.data
```

### C) Nodes overlapping an edit span (incremental invalidation)

```python
def overlapping_nodes(t, begin_b, end_b):
    # overlap returns nodes that intersect the edit span (useful for “what got touched?”)
    return sorted(t.overlap(begin_b, end_b), key=lambda iv: (iv.begin, iv.end, iv.data))
```

## 1.4 “Indexer-grade” refinements

* **Kind filtering:** maintain a set of “structural kinds” (FunctionDef/ClassDef/Call/Attribute/Name) and filter hits before selecting best.
* **Two-tier trees:** build one tree for “all nodes” and one for “def nodes” (defs only). Def-tree gives more stable UX for “jump to enclosing def.”
* **Edit locality:** if you’re doing LSP-like operations, keep a rolling cache of `pos_b → nearest_node` for the last N cursor positions; invalidate cache when file revision increments.

---

# 2) Occurrence span index for SCIP occurrences

**Goal:** `point → (symbol, role)` resolution, plus `range → enclosing def symbol` and “best symbol under cursor” with sane precedence.

## 2.1 Inputs you should expose (per file)

From your `scip_occurrences_v1` table (per document/file), you want at least:

* `begin_b`, `end_b`
* `symbol: str`
* `role: int|enum` (definition/reference/import/etc)
* `syntax_id` (optional crosswalk to your syntax node id)
* `enclosing_symbol` (optional, if you precompute)

### Payload design

Make it deterministic and sortable by precedence:

```python
OccRef = tuple[int, str]  # (role_rank, symbol)
```

Where `role_rank` is a **small int** computed from SCIP roles.

Example precedence (tune to taste):

* 0: definition
* 1: import/namespace/module ref
* 2: type ref
* 3: value ref
* 4: any

## 2.2 Build: one tree for all occurrences, plus a def-tree

```python
def build_occurrence_trees(occ_rows):
    occ_t = IntervalTree()
    def_t = IntervalTree()
    for r in occ_rows:  # (begin_b, end_b, symbol, role_rank, is_definition)
        if r.end_b <= r.begin_b:
            continue
        payload = (r.role_rank, r.symbol)
        occ_t.addi(r.begin_b, r.end_b, payload)
        if r.is_definition:
            def_t.addi(r.begin_b, r.end_b, payload)
    return occ_t, def_t
```

## 2.3 Point → best symbol under cursor

You typically want:

1. smallest span (token-level beats big span),
2. best role rank (definition > reference),
3. stable symbol tie-break.

```python
def best_symbol_at(occ_t: IntervalTree, pos_b: int):
    hits = occ_t.at(pos_b)
    if not hits:
        return None
    best = min(
        hits,
        key=lambda iv: (
            (iv.end - iv.begin),      # smallest token span
            iv.data[0],               # best role_rank
            iv.data[1],               # stable symbol
        ),
    )
    role_rank, symbol = best.data
    return role_rank, symbol
```

## 2.4 Range → enclosing definition (best-effort)

If you have an occurrence at `pos_b` (reference), find the tightest definition interval that **envelops** it. This gives you “enclosing def” *within the same file*.

```python
def enclosing_def_at(def_t: IntervalTree, begin_b: int, end_b: int):
    hits = def_t.envelop(begin_b, end_b)
    if not hits:
        return None
    best = min(hits, key=lambda iv: ((iv.end - iv.begin), iv.data[1]))
    return best.data  # (role_rank, symbol)
```

### Cross-file “go to definition”

Do *not* try to do that with intervaltree alone. Use:

* `symbol → (def_doc_id, def_begin_b, def_end_b)` map precomputed from SCIP (or derived from your `goid_crosswalk` tables),
  then:
* on navigation, load/build the destination file’s syntax/def trees lazily and return the node.

## 2.5 Indexer-grade refinements

* **Multi-hit UX:** return the full sorted hit list for ambiguous points (e.g., `Name` that is both a type and a value). Provide a deterministic ordering to clients.
* **Role bucketing:** maintain separate trees for `definitions`, `references`, `imports` if you frequently query by category; this avoids scanning a mixed hit set.
* **Stable tests:** emit a canonical “symbol-under-cursor” snapshot fixture using a fixed list of cursor points and compare `(symbol, role_rank)` results.

---

# 3) Deterministic segment normalizer

**Goal:** Take messy, overlapping spans (e.g., tags/labels/features) and produce a **canonical, disjoint segmentation** with **stable merge-key coalescing**—ideal for:

* embedding chunk boundaries,
* stable golden tests,
* consistent downstream joins.

This recipe is the workhorse for “span mapping at scale.”

## 3.1 The problem you’re actually solving

Inputs often contain:

* overlaps (multiple labels cover the same bytes),
* adjacency fragmentation,
* nondeterministic ordering if payload isn’t orderable.

You want output that is:

* **disjoint** (no overlaps),
* **maximally merged** for identical label-sets,
* deterministic across runs.

## 3.2 Output contract (recommended)

Emit segments as:

* `begin_b`, `end_b`
* `labels: tuple[LabelKey, ...]` sorted
* `labels_hash: str|int` (optional; stable fingerprint)
* `primary_label: LabelKey` (optional; if you need a single owner)

Where `LabelKey` is a stable, orderable tuple like:

```python
LabelKey = tuple[str, str]  # (kind, stable_id) e.g. ("goid", "F:pkg.mod.func#1")
```

## 3.3 Normalization pipeline (deterministic, overlap-safe)

### Step 0: Load intervals with *single-label* payloads

```python
def build_label_tree(spans) -> IntervalTree:
    t = IntervalTree()
    for s in spans:  # (begin_b, end_b, label_key)
        if s.end_b <= s.begin_b:
            continue
        # store as a 1-label tuple to keep payload orderable
        t.addi(s.begin_b, s.end_b, (s.label_key,))
    return t
```

### Step 1: Split on all boundaries (atomic segmentation)

This produces atomic segments, but you may now have *multiple intervals with identical (begin,end)* differing only by label.

```python
t.split_overlaps()
```

### Step 2: Collapse identical ranges by unioning labels (`merge_equals`)

After splitting, all overlaps are boundary-aligned, so you can safely combine same-range intervals:

```python
def union_labels(acc: tuple, x: tuple):
    # both are tuples of LabelKey; produce a sorted, deduped tuple
    out = set(acc)
    out.update(x)
    return tuple(sorted(out))

t.merge_equals(data_reducer=union_labels)
```

Now each `[begin,end)` appears at most once, with `data = (label_key_1, label_key_2, ...)`.

### Step 3: Coalesce adjacent segments with identical label sets

Adjacent means `[a,b)` then `[b,c)`.

You must merge *only when label sets are equal*. The safest way is bucket-by-data:

```python
from collections import defaultdict

def coalesce_adjacent_by_labels(t: IntervalTree) -> IntervalTree:
    buckets = defaultdict(IntervalTree)
    for iv in t:
        buckets[iv.data].addi(iv.begin, iv.end, iv.data)
    out = IntervalTree()
    for labels, bt in buckets.items():
        bt.merge_overlaps(strict=False)  # strict=False merges touching neighbors
        out |= bt
    return out
```

### Step 4: Canonical ordering for stable output

Always sort output segments explicitly:

```python
segments = sorted(out, key=lambda iv: (iv.begin, iv.end, iv.data))
```

## 3.4 Optional: pick a single “primary” label (stable)

If a downstream consumer needs one owner (e.g., “which function owns this byte range?”), apply a deterministic priority:

```python
KIND_PRIORITY = {"goid": 0, "symbol": 1, "subsystem": 2}

def pick_primary(labels: tuple[LabelKey, ...]) -> LabelKey:
    return min(labels, key=lambda k: (KIND_PRIORITY.get(k[0], 99), k[1]))
```

## 3.5 Embedding chunk boundary recipe (practical)

Once you have canonical segments:

* Group contiguous segments by `primary_label` (or by exact label-set if you want “multi-owner” chunks).
* Accumulate until you hit a max byte budget (e.g., 8–32KB) or semantic boundary (new function/class).
* Emit chunk spans as unions of segments (still half-open, still deterministic).

This gives you **stable chunking** across runs and small edits, which is exactly what you want for golden tests and embedding refresh minimization.

## 3.6 Test harness hook (golden-friendly)

Create a fixture per file:

* input spans (unordered)
* normalized segments (ordered)
* assert exact equality

If you also persist `labels_hash`, you can snapshot only `(begin,end,hash)` to reduce fixture size.

---

## How these three recipes compose (the CodeIntel “span stack”)

* **SyntaxTree**: “what syntax node is here / contains this”
* **OccTree (+ DefTree)**: “what SCIP symbol is here / enclosing def”
* **SegmentNormalizer**: “what’s the canonical segmentation of labels across bytes”

A very strong practical flow is:

1. use **OccTree** to resolve symbol(s) at cursor,
2. use **SyntaxTree** to pick the best syntactic container (def/class/call),
3. use **SegmentNormalizer** to build stable chunks for embeddings or summaries.

If you want, I can translate these into **repo-aligned artifacts**: proposed dataset contract schemas (`spans.syntax_nodes_v1`, `spans.scip_occurrences_v1`, `spans.normalized_segments_v1`), and Hamilton node boundaries (e.g., `t__spans__syntax_tree_cache`, `t__spans__occ_tree_cache`, `t__spans__segments_normalize`) that fit your existing “targets from DAG tags” convention.

Below is a **repo-aligned translation** of the three span recipes into:

1. **Dataset contract schemas** (Arrow-first, DuckDB-friendly) for

   * `spans.syntax_nodes_v1`
   * `spans.scip_occurrences_v1`
   * `spans.normalized_segments_v1`

2. **Hamilton node boundaries** (compute nodes + `t__` materializers + cache artifacts) that fit your “**targets from DAG tags**” convention.

I’m assuming your canonical identity columns already exist (or you’re moving toward them): `run_id`, `repo_id`, `file_id` (+ `rel_path` in a core files table). If your current repo uses different names, treat these as **aliases** and keep the *shape*.

---

# 0) Conventions (make span primitives composable)

## Coordinate system

* All spans are **byte offsets** into the file’s **raw bytes** with **half-open** semantics: `[begin_b, end_b)`.
* Invariants you enforce everywhere:

  * `0 <= begin_b < end_b`
  * spans are measured against the exact bytes that were parsed / indexed (no newline normalization drift).

## Determinism policy

* Any “set-like” collection stored in a cell must have a **canonical order** (sorted).
* Any “reduce/merge” must be stable under:

  * different ingestion ordering,
  * hash randomization,
  * parallel extraction.

## Partitioning / physical ordering

DuckDB doesn’t rely heavily on indexes; you get most wins from **sort order** + partition keys:

* Always materialize these tables ordered by:
  `repo_id, file_id, begin_b, end_b` (or `repo_id, rel_path, begin_b, end_b` if that’s your join key).

---

# 1) Dataset contract: `spans.syntax_nodes_v1`

### Purpose

A minimal, query-optimized projection of your CST/AST node table with **stable ids** and **byte spans** suitable for:

* nearest-node-at-point
* enclosing-node-for-range
* overlap queries for edits

### Primary key

* `(run_id, file_id, node_id)`

### Required columns (Arrow types)

* `run_id: large_string` (or `fixed_size_binary(16)` if you use UUID bytes)
* `repo_id: large_string` (or same as above)
* `file_id: large_string` (stable internal id; don’t use path as PK)
* `node_id: int64` (stable per file per run; deterministic numbering is ideal)
* `parent_node_id: int64?` (nullable; root nodes null)
* `kind: large_string` (LibCST node type / AST node type / normalized kind)
* `depth: int32` (root=0; deterministic; makes “nearest” selection faster)
* `begin_b: int64`
* `end_b: int64`

### Recommended optional columns (high-leverage)

* `is_def: bool` (true for def-like nodes: function/class/assignment target/etc.)
* `name: large_string?` (only when cheap; otherwise keep in a side table)
* `goid: large_string?` (your internal stable symbol/GOID, if already computed)
* `span_hash: uint64` (stable hash of `(begin_b,end_b,kind,node_id)` for drift tests)

### Contract invariants

* `end_b > begin_b`
* `(file_id,node_id)` unique within `(run_id,repo_id)`
* `depth` consistent with `parent_node_id` (child depth = parent depth + 1) **if** you populate parents
* You may have overlaps (AST/CST nesting), that’s expected.

### Physical layout / sort keys

* Sort: `repo_id, file_id, begin_b, end_b, depth`
* If you persist as Parquet/Iceberg: partition by `repo_id` (and optionally `file_id` if very large repos)

---

# 2) Dataset contract: `spans.scip_occurrences_v1`

### Purpose

A span-indexable, deterministic view of SCIP occurrences suitable for:

* point → best symbol (role precedence)
* range → enclosing definition (in-file)
* overlap queries for edits

### Primary key

* `(run_id, file_id, occ_id)`
  Where `occ_id` is a stable integer within the file (deterministic sort-based numbering).

### Required columns (Arrow types)

* `run_id: large_string`
* `repo_id: large_string`
* `file_id: large_string`
* `occ_id: int64`
* `begin_b: int64`
* `end_b: int64`
* `symbol: large_string`
* `role_flags: int64` (raw SCIP bitmask)
* `role_rank: int16` (derived precedence rank for cursor resolution)
* `is_definition: bool`

### Recommended optional columns

* `syntax_node_id: int64?` (nullable; crosswalk to `spans.syntax_nodes_v1.node_id` when available)
* `enclosing_def_symbol: large_string?` (nullable; fast path for “enclosing def” UX)
* `symbol_hash: uint64` (stable hash of symbol string for joins / compactness)
* `diagnostics: large_string?` (only if you carry SCIP ingestion errors)

### Contract invariants

* `end_b > begin_b`
* `role_rank` is fully deterministic from `role_flags` (same mapping across runs)
* `(file_id, occ_id)` unique within `(run_id,repo_id)`
* If you populate `syntax_node_id`, it must reference a real node in the same `(run_id,file_id)`

### Physical layout / sort keys

* Sort: `repo_id, file_id, begin_b, end_b, role_rank, symbol`

---

# 3) Dataset contract: `spans.normalized_segments_v1`

### Purpose

A canonical, **disjoint** segmentation of file bytes into maximal runs with a stable **label-set**. This is the table that makes:

* embedding chunk boundaries stable
* golden tests robust
* downstream joins deterministic

### Primary key

* `(run_id, file_id, segment_id)`
  Where `segment_id` is deterministic from ordering (row_number over `begin_b`).

### Required columns (Arrow types)

* `run_id: large_string`
* `repo_id: large_string`
* `file_id: large_string`
* `segment_id: int64`
* `begin_b: int64`
* `end_b: int64`

**Label set (choose one encoding; pick based on ergonomics):**

**Option A (simplest / fast):**

* `labels: list<large_string>` where each element is canonical `"kind:id"`
  Example: `"goid:F:pkg.mod.func#1"`, `"symbol:scip-python ..."`.
* `labels_hash: uint64` (hash of the ordered labels list; stable)

**Option B (more structured):**

* `labels: list<struct<kind: large_string, id: large_string>>`
* `labels_hash: uint64`

And for “single-owner” consumers:

* `primary_label: large_string` (canonical `"kind:id"`)
* `label_count: int16`

### Contract invariants (critical)

Per `(run_id,file_id)`:

* Segments are **disjoint** and ordered:

  * `begin_b` strictly increasing
  * `end_b > begin_b`
  * **no overlaps**
* Maximal merge property:

  * if two adjacent segments touch (`a.end_b == b.begin_b`) then `a.labels != b.labels`
* `labels` are:

  * deduped
  * sorted canonically
* `labels_hash` is stable and equals hash(canonical labels)

### Physical layout / sort keys

* Sort: `repo_id, file_id, begin_b`
* Partition: `repo_id` (and optionally file_id for huge repos)

---

# 4) Hamilton node boundaries (repo-aligned, “targets from DAG tags”)

You want three things in the DAG:

1. compute projections (`spans__*`)
2. deterministic normalization (`spans__segments_normalize`)
3. optional caches (interval trees) as artifacts (`spans__*_tree_cache`) with `t__` anchors

## 4.1 Suggested module layout

```
src/codeintel/build/hamilton/native/spans/
  __init__.py
  syntax_nodes.py
  scip_occurrences.py
  segments_normalize.py
  tree_cache.py
```

Rationale:

* keeps span primitives isolated
* makes it trivial to feature-flag caches and normalization

## 4.2 Node naming convention

* Compute nodes: `spans__<thing>_<vN>()`
* Materializers: `t__spans__<target>()` (your “target spec anchor”)
* Cache artifacts: `spans__<tree>_cache()` + `t__spans__<tree>_cache()` anchor

## 4.3 Node graph (high level)

### (A) Syntax nodes projection

* **Input (existing):** `syntax.nodes_v1` (or your current CST/AST node table)
* **Compute:** `spans__syntax_nodes_v1(syntax_nodes_v1, core_files_v1, span_config) -> ArrowTable`
* **Anchor/materialize:** `t__spans__syntax_nodes_v1(spans__syntax_nodes_v1) -> DatasetRef`

### (B) SCIP occurrences projection

* **Input (existing):** `scip.occurrences_v1` (raw ingested occurrences)
* **Compute:** `spans__scip_occurrences_v1(scip_occurrences_v1, core_files_v1, role_rank_policy) -> ArrowTable`
* **Anchor/materialize:** `t__spans__scip_occurrences_v1(...) -> DatasetRef`

### (C) Normalized segments

* **Inputs:** `spans.syntax_nodes_v1`, `spans.scip_occurrences_v1`, `span_label_policy`
* **Compute:** `spans__segments_normalize(spans__syntax_nodes_v1, spans__scip_occurrences_v1, span_label_policy) -> ArrowTable`
* **Anchor/materialize:** `t__spans__normalized_segments_v1(spans__segments_normalize) -> DatasetRef`

### (D) Optional caches (artifact targets)

* **Compute:** `spans__syntax_tree_cache(spans__syntax_nodes_v1, cache_config) -> ArrowTable (metadata) OR ArtifactMap`
* **Anchor/materialize:** `t__spans__syntax_tree_cache(...) -> ArtifactRef/DatasetRef`
* Similarly for occurrences:

  * `spans__occ_tree_cache(spans__scip_occurrences_v1, cache_config)`

---

# 5) Concrete node boundary specs (signatures + responsibilities)

Below I’m describing each node as a “contract” you can implement in your existing Hamilton + gateway patterns.

## 5.1 `spans__syntax_nodes_v1`

**Inputs**

* `syntax_nodes_v1`: Arrow/Polars table from parsing (canonical)
* `core_files_v1`: file identity + raw bytes length (optional but recommended for validation)
* `span_config`: policy object (e.g., include_parent_links, include_depth, include_goid)

**Responsibilities**

* Ensure `begin_b/end_b` exist and are bytes, not (line,col).
* Enforce invariants (drop or flag `end_b <= begin_b`).
* Compute `depth` deterministically if not provided (optional, but a big win).
* Emit only the columns in `spans.syntax_nodes_v1` contract.

**Output**

* Arrow Table matching `spans.syntax_nodes_v1`.

**t__ anchor**: `t__spans__syntax_nodes_v1`

* Tagged as `node_type="materialize"`, `target="spans.syntax_nodes_v1"`
* Calls your Arrow materializer (to Parquet/Iceberg/DuckDB depending on your stack).

---

## 5.2 `spans__scip_occurrences_v1`

**Inputs**

* `scip_occurrences_v1`: raw occurrences (may be SCIP-native range encoding)
* `core_files_v1`: for validation + mapping if you need doc_id → file_id
* `role_rank_policy`: mapping from SCIP role bitmask → `role_rank` + `is_definition`

**Responsibilities**

* Normalize ranges to `begin_b/end_b` bytes.
* Derive:

  * `role_rank` (stable)
  * `is_definition`
* Assign deterministic `occ_id`:

  * `occ_id = row_number() over (partition by file_id order by begin_b, end_b, role_rank, symbol)`
* Optionally attach `syntax_node_id` via an xref step (if you have it); otherwise keep nullable.

**Output**

* Arrow Table matching `spans.scip_occurrences_v1`.

**t__ anchor**: `t__spans__scip_occurrences_v1`

* Tagged target materialization.

---

## 5.3 `spans__segments_normalize`

**Inputs**

* `spans__syntax_nodes_v1`
* `spans__scip_occurrences_v1`
* `span_label_policy` (what label kinds to include + precedence)

**Responsibilities**

* Convert chosen inputs to canonical **label spans**:

  * from syntax nodes: `("goid", goid)` or `("node", node_id)` (policy)
  * from scip occurrences: `("symbol", symbol)` or `("symbol_hash", hash)` (policy)
* Run normalization pipeline:

  1. build intervaltree with single-label payloads
  2. `split_overlaps()`
  3. `merge_equals()` with union reducer (canonical sorted set)
  4. coalesce adjacency **per label-set**
* Emit disjoint segments with canonical `labels` list and `labels_hash`.
* Assign deterministic `segment_id` via ordering.

**Output**

* Arrow Table matching `spans.normalized_segments_v1`.

**t__ anchor**: `t__spans__normalized_segments_v1`

---

# 6) Cache targets (interval trees) as repo-aligned artifacts

You have two practical implementation choices. I’d pick **Option A** first; it plays nicest with your “targets from DAG tags” system.

## Option A: “Cache metadata table + files on disk” (recommended)

### Artifact layout

* Store pickled trees under a cache root:

  * `{cache_root}/{repo_id}/{run_id}/spans/syntax_tree/{file_id}.pkl`
  * `{cache_root}/{repo_id}/{run_id}/spans/occ_tree/{file_id}.pkl` (contains both occ + def trees)

### Hamilton outputs

* `spans__syntax_tree_cache(...) -> spans.syntax_tree_cache_v1` (metadata table)
* `spans__occ_tree_cache(...) -> spans.occ_tree_cache_v1` (metadata table)

Each metadata row:

* `run_id, repo_id, file_id`
* `cache_kind: large_string` (“syntax_tree”, “occ_tree”)
* `path: large_string`
* `sha256: fixed_size_binary(32)` (or large_string hex)
* `bytes_len: int64`
* `build_rev: int64` (monotonic; for invalidation)

This makes caches:

* reproducible,
* testable,
* and pluggable into your gateway/materialization system.

**t__ anchors**

* `t__spans__syntax_tree_cache`
* `t__spans__occ_tree_cache`

## Option B: Store blobs directly in DuckDB/Parquet

Feasible, but tends to be awkward and less portable. Only do this if your environment forbids filesystem caches.

---

# 7) “Targets from DAG tags” metadata (how to keep it aligned)

For each `t__` node, your docstring becomes the canonical target documentation. Keep it mechanically parseable.

Example docstring template (apply to each `t__spans__*`):

* What it materializes (`spans.*_v1`)
* Key invariants (half-open, determinism)
* Sort keys / partitioning guidance
* Upstream deps (syntax/scip versions)
* Version bump rules (what constitutes v2)

Tag payload shape (conceptual; adapt to your existing decorator wrapper):

* `domain="spans"`
* `target="spans.syntax_nodes_v1"`
* `node_type="materialize"`
* `contract="spans.syntax_nodes_v1"` (if you keep contract registry by name)

---

# 8) Minimal “definition of done” per artifact (so this ships cleanly)

## spans.syntax_nodes_v1

* [ ] Byte offsets validated against file length (at least `end_b <= file_nbytes`)
* [ ] Deterministic `node_id` and/or stable mapping strategy documented
* [ ] Sorted materialization

## spans.scip_occurrences_v1

* [ ] Deterministic `occ_id`
* [ ] Stable `role_rank` mapping covered by tests
* [ ] Sorted materialization

## spans.normalized_segments_v1

* [ ] Disjointness + maximal merge invariants asserted (per file)
* [ ] Canonical label sorting enforced
* [ ] Stable `labels_hash` tested

## tree caches (optional)

* [ ] Cache invalidation keyed by upstream dataset revision/hash
* [ ] Metadata table exists and is stable for golden tests

---

If you want the **next increment**, I can write this as a **repo-ready contract pack**:

* the three `pyarrow.schema(...)` objects (and matching DuckDB DDL if you want it),
* plus the Hamilton node skeletons (function signatures + tags + t__ anchors),
* and a tiny pytest “contract harness” that asserts:

  * span invariants,
  * determinism under shuffled input ordering,
  * and normalized-segment disjointness/maximality.

Below is a **repo-ready contract pack** you can paste in as-is (modulo minor import-path tweaks to match your repo). It includes:

* **3× `pyarrow.schema(...)`** objects
* **Matching DuckDB DDL** strings
* **Hamilton node skeletons** (compute nodes + `t__` anchors with tags)
* **Pytest contract harness** asserting:

  * span invariants,
  * determinism under shuffled input ordering,
  * normalized-segment disjointness + maximality

---

## 0) File layout (copy/paste targets)

```
src/codeintel/contracts/spans_contracts.py

src/codeintel/build/hamilton/native/spans/__init__.py
src/codeintel/build/hamilton/native/spans/policy.py
src/codeintel/build/hamilton/native/spans/syntax_nodes.py
src/codeintel/build/hamilton/native/spans/scip_occurrences.py
src/codeintel/build/hamilton/native/spans/segments_normalize.py

tests/contracts/test_spans_contracts.py
```

---

# 1) Contracts: PyArrow schemas + DuckDB DDL

```python
# src/codeintel/contracts/spans_contracts.py
from __future__ import annotations

import hashlib
import struct
from typing import Iterable, Mapping, Sequence

import pyarrow as pa
import pyarrow.compute as pc


# -------------------------
# Dataset names (canonical)
# -------------------------

SPANS_SYNTAX_NODES_V1 = "spans.syntax_nodes_v1"
SPANS_SCIP_OCCURRENCES_V1 = "spans.scip_occurrences_v1"
SPANS_NORMALIZED_SEGMENTS_V1 = "spans.normalized_segments_v1"


# ---------------------------------------
# Deterministic 64-bit hash (portable)
# ---------------------------------------

def blake2b_u64(data: bytes) -> int:
    """Return a deterministic uint64 from blake2b(data, digest_size=8)."""
    digest = hashlib.blake2b(data, digest_size=8).digest()
    return int.from_bytes(digest, byteorder="little", signed=False)


def stable_u64_from_parts(parts: Sequence[bytes]) -> int:
    h = hashlib.blake2b(digest_size=8)
    for p in parts:
        h.update(p)
        h.update(b"\x1f")  # unit-separator to avoid accidental concatenation ambiguity
    return int.from_bytes(h.digest(), byteorder="little", signed=False)


def hash_labels_u64(labels: Sequence[str]) -> int:
    """
    Hash a *canonical* labels sequence.

    Contract: caller provides labels already deduped + sorted.
    """
    h = hashlib.blake2b(digest_size=8)
    for s in labels:
        h.update(s.encode("utf-8"))
        h.update(b"\x1f")
    return int.from_bytes(h.digest(), byteorder="little", signed=False)


# -------------------------
# PyArrow schemas (v1)
# -------------------------

SCHEMA_SPANS_SYNTAX_NODES_V1 = pa.schema(
    [
        pa.field("run_id", pa.large_string(), nullable=False),
        pa.field("repo_id", pa.large_string(), nullable=False),
        pa.field("file_id", pa.large_string(), nullable=False),

        pa.field("node_id", pa.int64(), nullable=False),
        pa.field("parent_node_id", pa.int64(), nullable=True),

        pa.field("kind", pa.large_string(), nullable=False),
        pa.field("depth", pa.int32(), nullable=False),

        pa.field("begin_b", pa.int64(), nullable=False),
        pa.field("end_b", pa.int64(), nullable=False),

        # optional-but-useful (keep nullable=True so old producers can evolve safely)
        pa.field("is_def", pa.bool_(), nullable=True),
        pa.field("name", pa.large_string(), nullable=True),
        pa.field("goid", pa.large_string(), nullable=True),
        pa.field("span_hash", pa.uint64(), nullable=True),
    ]
)

SCHEMA_SPANS_SCIP_OCCURRENCES_V1 = pa.schema(
    [
        pa.field("run_id", pa.large_string(), nullable=False),
        pa.field("repo_id", pa.large_string(), nullable=False),
        pa.field("file_id", pa.large_string(), nullable=False),

        pa.field("occ_id", pa.int64(), nullable=False),

        pa.field("begin_b", pa.int64(), nullable=False),
        pa.field("end_b", pa.int64(), nullable=False),

        pa.field("symbol", pa.large_string(), nullable=False),

        pa.field("role_flags", pa.int64(), nullable=False),
        pa.field("role_rank", pa.int16(), nullable=False),
        pa.field("is_definition", pa.bool_(), nullable=False),

        # optional crosswalk / accelerators
        pa.field("syntax_node_id", pa.int64(), nullable=True),
        pa.field("enclosing_def_symbol", pa.large_string(), nullable=True),
        pa.field("symbol_hash", pa.uint64(), nullable=True),
    ]
)

SCHEMA_SPANS_NORMALIZED_SEGMENTS_V1 = pa.schema(
    [
        pa.field("run_id", pa.large_string(), nullable=False),
        pa.field("repo_id", pa.large_string(), nullable=False),
        pa.field("file_id", pa.large_string(), nullable=False),

        pa.field("segment_id", pa.int64(), nullable=False),

        pa.field("begin_b", pa.int64(), nullable=False),
        pa.field("end_b", pa.int64(), nullable=False),

        # canonical labels: sorted, deduped strings like "kind|id"
        pa.field("labels", pa.list_(pa.large_string()), nullable=False),
        pa.field("labels_hash", pa.uint64(), nullable=False),

        # optional convenience
        pa.field("primary_label", pa.large_string(), nullable=True),
        pa.field("label_count", pa.int16(), nullable=True),
    ]
)

SCHEMAS: Mapping[str, pa.Schema] = {
    SPANS_SYNTAX_NODES_V1: SCHEMA_SPANS_SYNTAX_NODES_V1,
    SPANS_SCIP_OCCURRENCES_V1: SCHEMA_SPANS_SCIP_OCCURRENCES_V1,
    SPANS_NORMALIZED_SEGMENTS_V1: SCHEMA_SPANS_NORMALIZED_SEGMENTS_V1,
}


# -------------------------
# DuckDB DDL (matching)
# -------------------------

DUCKDB_DDL: Mapping[str, str] = {
    SPANS_SYNTAX_NODES_V1: """
CREATE TABLE IF NOT EXISTS spans_syntax_nodes_v1 (
  run_id VARCHAR NOT NULL,
  repo_id VARCHAR NOT NULL,
  file_id VARCHAR NOT NULL,

  node_id BIGINT NOT NULL,
  parent_node_id BIGINT,

  kind VARCHAR NOT NULL,
  depth INTEGER NOT NULL,

  begin_b BIGINT NOT NULL,
  end_b BIGINT NOT NULL,

  is_def BOOLEAN,
  name VARCHAR,
  goid VARCHAR,
  span_hash UBIGINT
);
""".strip(),
    SPANS_SCIP_OCCURRENCES_V1: """
CREATE TABLE IF NOT EXISTS spans_scip_occurrences_v1 (
  run_id VARCHAR NOT NULL,
  repo_id VARCHAR NOT NULL,
  file_id VARCHAR NOT NULL,

  occ_id BIGINT NOT NULL,

  begin_b BIGINT NOT NULL,
  end_b BIGINT NOT NULL,

  symbol VARCHAR NOT NULL,

  role_flags BIGINT NOT NULL,
  role_rank SMALLINT NOT NULL,
  is_definition BOOLEAN NOT NULL,

  syntax_node_id BIGINT,
  enclosing_def_symbol VARCHAR,
  symbol_hash UBIGINT
);
""".strip(),
    SPANS_NORMALIZED_SEGMENTS_V1: """
CREATE TABLE IF NOT EXISTS spans_normalized_segments_v1 (
  run_id VARCHAR NOT NULL,
  repo_id VARCHAR NOT NULL,
  file_id VARCHAR NOT NULL,

  segment_id BIGINT NOT NULL,

  begin_b BIGINT NOT NULL,
  end_b BIGINT NOT NULL,

  labels VARCHAR[] NOT NULL,
  labels_hash UBIGINT NOT NULL,

  primary_label VARCHAR,
  label_count SMALLINT
);
""".strip(),
}


# -------------------------
# Schema alignment + validation
# -------------------------

def align_to_schema(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """
    Produce a table with exactly schema fields (order + types), creating null columns for missing optional fields.
    Raises if required fields are missing.
    """
    n = table.num_rows
    name_set = set(table.column_names)

    cols = []
    for field in schema:
        if field.name in name_set:
            col = table[field.name]
            if not col.type.equals(field.type):
                col = pc.cast(col, field.type)
            cols.append(col)
        else:
            if field.nullable:
                cols.append(pa.nulls(n, type=field.type))
            else:
                raise KeyError(f"Missing required field: {field.name}")

    out = pa.table(cols, schema=schema)
    return out


def validate_table_against_schema(table: pa.Table, schema: pa.Schema) -> None:
    """
    Validates:
    - all fields exist
    - types match (exact)
    - non-nullable fields contain no nulls
    """
    # exact field set
    missing = [f.name for f in schema if f.name not in table.column_names]
    if missing:
        raise AssertionError(f"Missing columns: {missing}")

    for field in schema:
        col = table[field.name]
        if not col.type.equals(field.type):
            raise AssertionError(f"Column {field.name} type {col.type} != expected {field.type}")

        if field.nullable is False and col.null_count != 0:
            raise AssertionError(f"Column {field.name} is non-nullable but has {col.null_count} nulls")
```

---

# 2) Span policies (label formatting, primary label pick)

```python
# src/codeintel/build/hamilton/native/spans/policy.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence


@dataclass(frozen=True)
class SpanConfig:
    compute_span_hash: bool = True
    drop_invalid_spans: bool = True  # drop end<=begin


@dataclass(frozen=True)
class SpanLabelPolicy:
    """
    Label encoding is string-based for portability and easy storage in Arrow/DuckDB.

    We strongly recommend delimiter="|" because SCIP symbols commonly contain ':'.
    """
    delimiter: str = "|"

    include_syntax_goid: bool = True
    include_syntax_node: bool = False  # fallback label kind when no goid

    include_scip_symbol: bool = True
    include_scip_definitions_only: bool = False

    # Primary label selection priority
    kind_priority: tuple[str, ...] = ("goid", "symbol", "node")


def format_label(kind: str, ident: str, delimiter: str = "|") -> str:
    return f"{kind}{delimiter}{ident}"


def pick_primary_label(labels: Sequence[str], kind_priority: Sequence[str], delimiter: str = "|") -> str | None:
    if not labels:
        return None

    pri = {k: i for i, k in enumerate(kind_priority)}
    def key(lbl: str) -> tuple[int, str]:
        k = lbl.split(delimiter, 1)[0]
        return (pri.get(k, 999), lbl)

    return min(labels, key=key)
```

---

# 3) Hamilton modules (compute nodes + `t__` anchors)

> These are intentionally “skeleton but runnable.” They don’t assume your internal `DatasetRef` types—your materializer can treat `t__*` as “target anchors” and persist the returned Arrow table.

## 3.1 `__init__.py`

```python
# src/codeintel/build/hamilton/native/spans/__init__.py
# Re-export modules so Hamilton discovery finds them.
from . import syntax_nodes as _syntax_nodes
from . import scip_occurrences as _scip_occurrences
from . import segments_normalize as _segments_normalize
```

## 3.2 Syntax nodes projection

```python
# src/codeintel/build/hamilton/native/spans/syntax_nodes.py
from __future__ import annotations

import struct
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from hamilton.function_modifiers import tag

from codeintel.contracts.spans_contracts import (
    SCHEMA_SPANS_SYNTAX_NODES_V1,
    align_to_schema,
    stable_u64_from_parts,
)
from .policy import SpanConfig


@tag(domain="spans", dataset="spans.syntax_nodes_v1", node_type="compute")
def spans__syntax_nodes_v1(
    syntax_nodes_v1: pa.Table,
    run_id: str,
    repo_id: str,
    span_config: SpanConfig = SpanConfig(),
) -> pa.Table:
    """
    Project/normalize syntax node spans into spans.syntax_nodes_v1.

    Requirements:
    - byte offsets [begin_b, end_b)
    - deterministic ids (node_id)
    """
    t = syntax_nodes_v1

    # Inject run/repo if not already present (common pattern in pipelines)
    if "run_id" not in t.column_names:
        t = t.append_column("run_id", pa.array([run_id] * t.num_rows, type=pa.large_string()))
    if "repo_id" not in t.column_names:
        t = t.append_column("repo_id", pa.array([repo_id] * t.num_rows, type=pa.large_string()))

    # Drop invalid spans if requested
    if span_config.drop_invalid_spans:
        mask = pc.greater(t["end_b"], t["begin_b"])
        t = t.filter(mask)

    # Ensure required columns exist; create defaults where safe
    if "depth" not in t.column_names:
        t = t.append_column("depth", pa.array([0] * t.num_rows, type=pa.int32()))
    if "kind" not in t.column_names:
        raise KeyError("syntax_nodes_v1 missing required column: kind")
    if "node_id" not in t.column_names:
        raise KeyError("syntax_nodes_v1 missing required column: node_id")
    if "file_id" not in t.column_names:
        raise KeyError("syntax_nodes_v1 missing required column: file_id")

    # Optional: compute a stable span hash
    if span_config.compute_span_hash and "span_hash" not in t.column_names:
        begin = t["begin_b"].to_pylist()
        end = t["end_b"].to_pylist()
        kind = t["kind"].to_pylist()
        node_id = t["node_id"].to_pylist()

        out_hash = []
        for b, e, k, nid in zip(begin, end, kind, node_id):
            # pack ints + stable strings
            out_hash.append(
                stable_u64_from_parts(
                    [
                        struct.pack("<q", int(b)),
                        struct.pack("<q", int(e)),
                        str(k).encode("utf-8"),
                        struct.pack("<q", int(nid)),
                    ]
                )
            )
        t = t.append_column("span_hash", pa.array(out_hash, type=pa.uint64()))

    # Align to contract schema
    t = align_to_schema(t, SCHEMA_SPANS_SYNTAX_NODES_V1)

    # Stable sort for materialization friendliness (optional but recommended)
    # (Arrow sort is stable enough for our purposes; your materializer may also sort)
    sort_keys = [("repo_id", "ascending"), ("file_id", "ascending"), ("begin_b", "ascending"), ("end_b", "ascending")]
    indices = pc.sort_indices(t, sort_keys=sort_keys)
    t = pc.take(t, indices)

    return t


@tag(domain="spans", node_type="materialize", target="spans.syntax_nodes_v1", contract="spans.syntax_nodes_v1")
def t__spans__syntax_nodes_v1(spans__syntax_nodes_v1: pa.Table) -> pa.Table:
    """
    Target anchor: spans.syntax_nodes_v1

    Materializer should persist this table (Parquet/Iceberg/DuckDB) using the target tag as the source of truth.
    """
    return spans__syntax_nodes_v1
```

## 3.3 SCIP occurrences projection (deterministic `occ_id`)

```python
# src/codeintel/build/hamilton/native/spans/scip_occurrences.py
from __future__ import annotations

import struct
from collections import defaultdict
from typing import Iterable

import pyarrow as pa
import pyarrow.compute as pc
from hamilton.function_modifiers import tag

from codeintel.contracts.spans_contracts import (
    SCHEMA_SPANS_SCIP_OCCURRENCES_V1,
    align_to_schema,
    stable_u64_from_parts,
)
from .policy import SpanConfig


@tag(domain="spans", dataset="spans.scip_occurrences_v1", node_type="compute")
def spans__scip_occurrences_v1(
    scip_occurrences_v1: pa.Table,
    run_id: str,
    repo_id: str,
    span_config: SpanConfig = SpanConfig(),
) -> pa.Table:
    """
    Project SCIP occurrences into spans.scip_occurrences_v1 with deterministic occ_id.

    Assumes input already has:
      - file_id
      - begin_b/end_b
      - symbol
      - role_flags
      - role_rank
      - is_definition

    If your upstream table uses different column names or non-byte ranges, adapt here.
    """
    t = scip_occurrences_v1

    if "run_id" not in t.column_names:
        t = t.append_column("run_id", pa.array([run_id] * t.num_rows, type=pa.large_string()))
    if "repo_id" not in t.column_names:
        t = t.append_column("repo_id", pa.array([repo_id] * t.num_rows, type=pa.large_string()))

    if span_config.drop_invalid_spans:
        mask = pc.greater(t["end_b"], t["begin_b"])
        t = t.filter(mask)

    # Deterministic occ_id: row_number over (repo_id,file_id) ordered by (begin,end,role_rank,symbol)
    required = ["repo_id", "file_id", "begin_b", "end_b", "role_rank", "symbol"]
    for c in required:
        if c not in t.column_names:
            raise KeyError(f"scip_occurrences_v1 missing required column: {c}")

    # Use Arrow sort + group assignment in Python for clarity and determinism.
    sort_keys = [
        ("repo_id", "ascending"),
        ("file_id", "ascending"),
        ("begin_b", "ascending"),
        ("end_b", "ascending"),
        ("role_rank", "ascending"),
        ("symbol", "ascending"),
    ]
    idx = pc.sort_indices(t, sort_keys=sort_keys)
    t_sorted = pc.take(t, idx)

    repo_ids = t_sorted["repo_id"].to_pylist()
    file_ids = t_sorted["file_id"].to_pylist()

    occ_ids = []
    counters: dict[tuple[str, str], int] = defaultdict(int)
    for r, f in zip(repo_ids, file_ids):
        k = (str(r), str(f))
        occ_ids.append(counters[k])
        counters[k] += 1

    t_sorted = t_sorted.append_column("occ_id", pa.array(occ_ids, type=pa.int64()))

    # Optional: compute symbol_hash if absent (helps joins + compactness)
    if "symbol_hash" not in t_sorted.column_names and "symbol" in t_sorted.column_names:
        syms = t_sorted["symbol"].to_pylist()
        out_hash = []
        for s in syms:
            out_hash.append(stable_u64_from_parts([str(s).encode("utf-8")]))
        t_sorted = t_sorted.append_column("symbol_hash", pa.array(out_hash, type=pa.uint64()))

    t_out = align_to_schema(t_sorted, SCHEMA_SPANS_SCIP_OCCURRENCES_V1)
    return t_out


@tag(domain="spans", node_type="materialize", target="spans.scip_occurrences_v1", contract="spans.scip_occurrences_v1")
def t__spans__scip_occurrences_v1(spans__scip_occurrences_v1: pa.Table) -> pa.Table:
    """Target anchor: spans.scip_occurrences_v1"""
    return spans__scip_occurrences_v1
```

## 3.4 Normalized segments (intervaltree-powered canonicalizer)

```python
# src/codeintel/build/hamilton/native/spans/segments_normalize.py
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, Sequence

import pyarrow as pa
import pyarrow.compute as pc
from hamilton.function_modifiers import tag
from intervaltree import IntervalTree

from codeintel.contracts.spans_contracts import (
    SCHEMA_SPANS_NORMALIZED_SEGMENTS_V1,
    align_to_schema,
    hash_labels_u64,
)
from .policy import SpanLabelPolicy, format_label, pick_primary_label


def _normalize_label_spans(label_spans: Sequence[tuple[int, int, str]]) -> list[tuple[int, int, tuple[str, ...]]]:
    """
    Core algorithm:
      1) build tree with data=(label,)
      2) split_overlaps -> boundary-aligned atomic pieces
      3) merge_equals with union reducer -> one interval per (begin,end) with multi-label payload
      4) coalesce adjacency *per identical label-set* -> maximal runs
    Output: sorted list of (begin,end,labels_tuple)
    """
    t = IntervalTree()
    for b, e, lbl in label_spans:
        if e <= b:
            continue
        t.addi(int(b), int(e), (lbl,))

    if not t:
        return []

    t.split_overlaps()

    def union_labels(acc: tuple[str, ...], x: tuple[str, ...]) -> tuple[str, ...]:
        # deterministic: dedupe + sort
        out = set(acc)
        out.update(x)
        return tuple(sorted(out))

    # collapse identical ranges into a single label-set
    t.merge_equals(data_reducer=union_labels)

    # coalesce adjacency per label-set
    buckets: dict[tuple[str, ...], IntervalTree] = defaultdict(IntervalTree)
    for iv in t:
        buckets[iv.data].addi(iv.begin, iv.end, iv.data)

    out = IntervalTree()
    for labels, bt in buckets.items():
        bt.merge_overlaps(strict=False)  # strict=False merges touching neighbors
        out |= bt

    segments = sorted(out, key=lambda iv: (iv.begin, iv.end, iv.data))
    return [(int(iv.begin), int(iv.end), tuple(iv.data)) for iv in segments]


@tag(domain="spans", dataset="spans.normalized_segments_v1", node_type="compute")
def spans__normalized_segments_v1(
    spans__syntax_nodes_v1: pa.Table,
    spans__scip_occurrences_v1: pa.Table,
    run_id: str,
    repo_id: str,
    label_policy: SpanLabelPolicy = SpanLabelPolicy(),
) -> pa.Table:
    """
    Build canonical disjoint segments with deterministic label-sets.

    Output table contains *only labeled segments* (gaps with no labels are omitted by design).
    """
    # Group label spans per file
    by_file: dict[str, list[tuple[int, int, str]]] = defaultdict(list)

    # Syntax labels
    if label_policy.include_syntax_goid and "goid" in spans__syntax_nodes_v1.column_names:
        rows = spans__syntax_nodes_v1.select(["file_id", "begin_b", "end_b", "goid"]).to_pylist()
        for r in rows:
            g = r.get("goid")
            if g:
                lbl = format_label("goid", str(g), delimiter=label_policy.delimiter)
                by_file[str(r["file_id"])].append((int(r["begin_b"]), int(r["end_b"]), lbl))
    if label_policy.include_syntax_node:
        rows = spans__syntax_nodes_v1.select(["file_id", "begin_b", "end_b", "node_id"]).to_pylist()
        for r in rows:
            lbl = format_label("node", str(r["node_id"]), delimiter=label_policy.delimiter)
            by_file[str(r["file_id"])].append((int(r["begin_b"]), int(r["end_b"]), lbl))

    # SCIP symbol labels
    if label_policy.include_scip_symbol:
        cols = ["file_id", "begin_b", "end_b", "symbol"]
        if label_policy.include_scip_definitions_only and "is_definition" in spans__scip_occurrences_v1.column_names:
            cols.append("is_definition")
        rows = spans__scip_occurrences_v1.select(cols).to_pylist()
        for r in rows:
            if label_policy.include_scip_definitions_only and not r.get("is_definition", False):
                continue
            sym = r.get("symbol")
            if sym:
                lbl = format_label("symbol", str(sym), delimiter=label_policy.delimiter)
                by_file[str(r["file_id"])].append((int(r["begin_b"]), int(r["end_b"]), lbl))

    # Normalize per file
    out_rows: list[dict] = []
    for file_id, spans in by_file.items():
        normalized = _normalize_label_spans(spans)
        # deterministic segment_id by begin/end order
        for seg_id, (b, e, labels_t) in enumerate(normalized):
            labels = list(labels_t)  # already sorted
            labels_hash = hash_labels_u64(labels)
            primary = pick_primary_label(labels, label_policy.kind_priority, delimiter=label_policy.delimiter)
            out_rows.append(
                {
                    "run_id": run_id,
                    "repo_id": repo_id,
                    "file_id": file_id,
                    "segment_id": seg_id,
                    "begin_b": b,
                    "end_b": e,
                    "labels": labels,
                    "labels_hash": labels_hash,
                    "primary_label": primary,
                    "label_count": len(labels),
                }
            )

    t_out = pa.Table.from_pylist(out_rows) if out_rows else pa.table(
        {f.name: pa.array([], type=f.type) for f in SCHEMA_SPANS_NORMALIZED_SEGMENTS_V1}, schema=SCHEMA_SPANS_NORMALIZED_SEGMENTS_V1
    )
    t_out = align_to_schema(t_out, SCHEMA_SPANS_NORMALIZED_SEGMENTS_V1)

    # Stable sort
    idx = pc.sort_indices(
        t_out,
        sort_keys=[("repo_id", "ascending"), ("file_id", "ascending"), ("begin_b", "ascending"), ("end_b", "ascending")],
    )
    t_out = pc.take(t_out, idx)
    return t_out


@tag(domain="spans", node_type="materialize", target="spans.normalized_segments_v1", contract="spans.normalized_segments_v1")
def t__spans__normalized_segments_v1(spans__normalized_segments_v1: pa.Table) -> pa.Table:
    """Target anchor: spans.normalized_segments_v1"""
    return spans__normalized_segments_v1
```

---

# 4) Pytest contract harness (invariants + determinism)

```python
# tests/contracts/test_spans_contracts.py
from __future__ import annotations

import random

import pyarrow as pa

from codeintel.contracts.spans_contracts import (
    SCHEMA_SPANS_NORMALIZED_SEGMENTS_V1,
    SCHEMA_SPANS_SCIP_OCCURRENCES_V1,
    SCHEMA_SPANS_SYNTAX_NODES_V1,
    validate_table_against_schema,
)
from codeintel.build.hamilton.native.spans.policy import SpanLabelPolicy
from codeintel.build.hamilton.native.spans.scip_occurrences import spans__scip_occurrences_v1
from codeintel.build.hamilton.native.spans.segments_normalize import spans__normalized_segments_v1
from codeintel.build.hamilton.native.spans.syntax_nodes import spans__syntax_nodes_v1


def _assert_span_invariants(table: pa.Table, begin_col: str = "begin_b", end_col: str = "end_b") -> None:
    begins = table[begin_col].to_pylist()
    ends = table[end_col].to_pylist()
    for b, e in zip(begins, ends):
        assert b is not None and e is not None
        assert int(e) > int(b), f"Invalid span: [{b}, {e})"


def _assert_disjoint_and_maximal(segments: pa.Table) -> None:
    # Allow gaps; forbid overlaps. If touching, labels must differ (maximality).
    rows = segments.select(["file_id", "begin_b", "end_b", "labels"]).to_pylist()
    rows.sort(key=lambda r: (r["file_id"], r["begin_b"], r["end_b"]))

    prev = None
    for r in rows:
        if prev is None or r["file_id"] != prev["file_id"]:
            prev = r
            continue

        assert r["begin_b"] >= prev["end_b"], (
            f"Overlapping segments in file {r['file_id']}: "
            f"prev[{prev['begin_b']},{prev['end_b']}) cur[{r['begin_b']},{r['end_b']})"
        )

        if r["begin_b"] == prev["end_b"]:
            assert r["labels"] != prev["labels"], "Adjacent segments must differ in labels (maximality)"

        prev = r


def _mk_syntax_nodes_input(run_id: str, repo_id: str) -> pa.Table:
    # Minimal synthetic syntax spans (nested) with one goid
    rows = [
        {"file_id": "f1", "node_id": 1, "parent_node_id": None, "kind": "Module", "depth": 0, "begin_b": 0, "end_b": 20, "goid": None},
        {"file_id": "f1", "node_id": 2, "parent_node_id": 1, "kind": "FunctionDef", "depth": 1, "begin_b": 0, "end_b": 10, "goid": "F|pkg.mod.func"},
        {"file_id": "f1", "node_id": 3, "parent_node_id": 2, "kind": "Name", "depth": 2, "begin_b": 1, "end_b": 5, "goid": None},
        {"file_id": "f1", "node_id": 4, "parent_node_id": 1, "kind": "ClassDef", "depth": 1, "begin_b": 12, "end_b": 18, "goid": "C|pkg.mod.Cls"},
    ]
    return pa.Table.from_pylist(rows)


def _mk_scip_occ_input(run_id: str, repo_id: str) -> pa.Table:
    # Two occurrences overlapping the function goid span and one elsewhere
    rows = [
        {"file_id": "f1", "begin_b": 1, "end_b": 2, "symbol": "sym:local_x", "role_flags": 0, "role_rank": 3, "is_definition": False},
        {"file_id": "f1", "begin_b": 0, "end_b": 4, "symbol": "sym:def_func", "role_flags": 1, "role_rank": 0, "is_definition": True},
        {"file_id": "f1", "begin_b": 13, "end_b": 14, "symbol": "sym:ClsRef", "role_flags": 0, "role_rank": 3, "is_definition": False},
    ]
    return pa.Table.from_pylist(rows)


def test_contract_projection_and_basic_invariants():
    run_id = "run-1"
    repo_id = "repo-1"

    syntax_in = _mk_syntax_nodes_input(run_id, repo_id)
    scip_in = _mk_scip_occ_input(run_id, repo_id)

    syntax_out = spans__syntax_nodes_v1(syntax_in, run_id=run_id, repo_id=repo_id)
    validate_table_against_schema(syntax_out, SCHEMA_SPANS_SYNTAX_NODES_V1)
    _assert_span_invariants(syntax_out)

    scip_out = spans__scip_occurrences_v1(scip_in, run_id=run_id, repo_id=repo_id)
    validate_table_against_schema(scip_out, SCHEMA_SPANS_SCIP_OCCURRENCES_V1)
    _assert_span_invariants(scip_out)


def test_occ_id_deterministic_under_shuffle():
    run_id = "run-1"
    repo_id = "repo-1"
    base = _mk_scip_occ_input(run_id, repo_id).to_pylist()

    def run_once(seed: int):
        rnd = random.Random(seed)
        rows = base[:]
        rnd.shuffle(rows)
        t = pa.Table.from_pylist(rows)
        out = spans__scip_occurrences_v1(t, run_id=run_id, repo_id=repo_id)
        # canonical representation
        return out.select(["file_id", "occ_id", "begin_b", "end_b", "role_rank", "symbol"]).to_pylist()

    a = run_once(1)
    b = run_once(2)
    c = run_once(3)
    assert a == b == c


def test_normalized_segments_deterministic_and_valid():
    run_id = "run-1"
    repo_id = "repo-1"

    syntax_in = _mk_syntax_nodes_input(run_id, repo_id)
    scip_in = _mk_scip_occ_input(run_id, repo_id)

    # Build projected inputs
    syntax = spans__syntax_nodes_v1(syntax_in, run_id=run_id, repo_id=repo_id)
    scip = spans__scip_occurrences_v1(scip_in, run_id=run_id, repo_id=repo_id)

    policy = SpanLabelPolicy(delimiter="|", include_syntax_goid=True, include_syntax_node=False, include_scip_symbol=True)

    def run_once(seed: int):
        rnd = random.Random(seed)

        s_rows = syntax.to_pylist()
        o_rows = scip.to_pylist()
        rnd.shuffle(s_rows)
        rnd.shuffle(o_rows)

        s_tbl = pa.Table.from_pylist(s_rows)
        o_tbl = pa.Table.from_pylist(o_rows)

        seg = spans__normalized_segments_v1(
            s_tbl,
            o_tbl,
            run_id=run_id,
            repo_id=repo_id,
            label_policy=policy,
        )
        validate_table_against_schema(seg, SCHEMA_SPANS_NORMALIZED_SEGMENTS_V1)
        _assert_span_invariants(seg)
        _assert_disjoint_and_maximal(seg)
        # canonical snapshot
        return seg.select(["file_id", "segment_id", "begin_b", "end_b", "labels", "labels_hash"]).to_pylist()

    a = run_once(10)
    b = run_once(11)
    c = run_once(12)
    assert a == b == c
```

---

## Notes you can apply immediately (zero design churn)

* **Delimiter:** this pack uses `kind|id` because SCIP symbols often contain `:`; you avoid parsing ambiguity.
* **Normalized segments omit unlabeled gaps** by design. If you want full file coverage (including empty-label segments), you can add a post-pass that fills gaps using file length (from `core_files_v1`) and `labels=[]`.
* The normalization algorithm is intentionally **commutative** (union + sorted) so it stays deterministic even if intervaltree internal ordering changes.

---

If you want the *next* increment after this pack: I can extend the harness into a **golden “span-normalizer fixture suite”** that:

* runs on a tiny real repo sample,
* produces stable snapshot files under `tests/golden/spans/...`,
* and includes regression tests for “small edit” scenarios (insert/delete ranges) to ensure your span indices behave correctly under incremental updates.

Below is a **golden “span-normalizer fixture suite”** that is repo-ready and does exactly what you asked:

* runs on a **tiny real repo sample** (checked into `tests/fixtures/spans_tiny_repo/`)
* **writes/compares stable snapshots** under `tests/golden/spans/spans_tiny_repo/...`
* includes **edit regressions** (insert/delete) that validate your span indices behave correctly under incremental changes

---

## 1) Tiny real repo sample (fixture)

### `tests/fixtures/spans_tiny_repo/tiny_pkg/__init__.py`

```python
"""Tiny fixture package for span-normalizer golden tests."""
```

### `tests/fixtures/spans_tiny_repo/tiny_pkg/alpha.py`

```python
def outer(x):
    def inner(y):
        return x + y
    return inner(1)

class Greeter:
    def greet(self, name):
        return f"hi {name}"
```

### `tests/fixtures/spans_tiny_repo/tiny_pkg/beta.py`

```python
from .alpha import outer, Greeter

async def arun():
    return outer(2)(3)

def run():
    g = Greeter()
    return outer(2)(3), g.greet("you")
```

---

## 2) Golden snapshots folder + workflow note

### `tests/golden/spans/spans_tiny_repo/README.md`

```md
# span-normalizer goldens (spans_tiny_repo)

This folder contains golden snapshots produced by `test_span_normalizer_golden.py`.

## Update goldens

Run:

    CODEINTEL_UPDATE_GOLDENS=1 pytest -k span_normalizer_golden -q

This will rewrite `*.segments.json` files under this directory.

## What’s in the snapshots?

Per-file canonical segments:
- begin_b/end_b (byte offsets, half-open)
- labels (sorted, deduped)
- labels_hash (stable)
- primary_label (stable priority choice)
```

---

## 3) Golden test suite + edit regressions

Create **one** test module:

### `tests/golden/spans/test_span_normalizer_golden.py`

```python
from __future__ import annotations

import ast
import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa

from codeintel.build.hamilton.native.spans.policy import SpanLabelPolicy
from codeintel.build.hamilton.native.spans.scip_occurrences import spans__scip_occurrences_v1
from codeintel.build.hamilton.native.spans.segments_normalize import spans__normalized_segments_v1
from codeintel.build.hamilton.native.spans.syntax_nodes import spans__syntax_nodes_v1


THIS_DIR = Path(__file__).resolve().parent
TESTS_DIR = THIS_DIR.parents[2]  # .../tests
FIXTURE_REPO = TESTS_DIR / "fixtures" / "spans_tiny_repo"
GOLDEN_DIR = THIS_DIR / "spans_tiny_repo"

UPDATE_GOLDENS = os.getenv("CODEINTEL_UPDATE_GOLDENS") == "1"

RUN_ID = "golden"
REPO_ID = "spans_tiny_repo"

PY_FILE_RE = re.compile(r".+\.py$", re.IGNORECASE)


# -----------------------------
# Minimal “real repo” extractor
# -----------------------------

@dataclass(frozen=True)
class _DefSpan:
    rel_path: str
    kind: str  # "FunctionDef" | "AsyncFunctionDef" | "ClassDef"
    qualname: str
    parent_qualname: str | None
    depth: int
    begin_b: int
    end_b: int
    name_begin_b: int
    name_end_b: int


def _iter_py_files(repo_root: Path) -> list[Path]:
    files: list[Path] = []
    for p in repo_root.rglob("*.py"):
        if "__pycache__" in p.parts:
            continue
        files.append(p)
    return sorted(files)


def _module_from_relpath(rel_path: str) -> str:
    p = Path(rel_path)
    parts = list(p.parts)
    if parts[-1] == "__init__.py":
        parts = parts[:-1]
    else:
        parts[-1] = parts[-1][:-3]  # strip .py
    return ".".join(parts)


def _build_line_index(text: str) -> tuple[list[str], list[int]]:
    # splitlines keepends so byte-length mapping stays correct
    lines = text.splitlines(keepends=True)
    starts: list[int] = []
    cur = 0
    for ln in lines:
        starts.append(cur)
        cur += len(ln.encode("utf-8"))
    return lines, starts


def _pos_to_byte(lines: list[str], starts: list[int], lineno: int, col: int) -> int:
    li = lineno - 1
    if li < 0:
        return 0
    if li >= len(lines):
        # clamp
        last = lines[-1] if lines else ""
        return (starts[-1] + len(last.encode("utf-8"))) if starts else 0
    prefix = starts[li]
    # col is in characters; convert to bytes
    return prefix + len(lines[li][:col].encode("utf-8"))


def _find_def_name_cols(line: str, start_col: int, kind: str, fallback_name: str) -> tuple[int, int]:
    sub = line[start_col:]
    if kind in ("FunctionDef", "AsyncFunctionDef"):
        m = re.search(r"\bdef\s+([A-Za-z_]\w*)", sub)
    else:
        m = re.search(r"\bclass\s+([A-Za-z_]\w*)", sub)

    if m:
        return (start_col + m.start(1), start_col + m.end(1))

    # fallback: literal name search
    idx = sub.find(fallback_name) if fallback_name else -1
    if idx >= 0:
        return (start_col + idx, start_col + idx + len(fallback_name))
    return (start_col, start_col)


class _DefCollector(ast.NodeVisitor):
    def __init__(self, rel_path: str, lines: list[str], starts: list[int]) -> None:
        self.rel_path = rel_path
        self.lines = lines
        self.starts = starts
        self.stack: list[str] = []
        self.out: list[_DefSpan] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> Any:
        self._handle_def(node, "FunctionDef")
        return None

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> Any:
        self._handle_def(node, "AsyncFunctionDef")
        return None

    def visit_ClassDef(self, node: ast.ClassDef) -> Any:
        self._handle_def(node, "ClassDef")
        return None

    def _handle_def(self, node: Any, kind: str) -> None:
        end_lineno = getattr(node, "end_lineno", None)
        end_col = getattr(node, "end_col_offset", None)
        if end_lineno is None or end_col is None:
            return

        qualname = ".".join(self.stack + [node.name])
        parent = ".".join(self.stack) if self.stack else None
        depth = len(self.stack)

        begin_b = _pos_to_byte(self.lines, self.starts, node.lineno, node.col_offset)
        end_b = _pos_to_byte(self.lines, self.starts, end_lineno, end_col)

        # compute name span on same def/class line
        line = self.lines[node.lineno - 1]
        name_begin_col, name_end_col = _find_def_name_cols(
            line=line,
            start_col=node.col_offset,
            kind=kind,
            fallback_name=node.name,
        )
        name_begin_b = _pos_to_byte(self.lines, self.starts, node.lineno, name_begin_col)
        name_end_b = _pos_to_byte(self.lines, self.starts, node.lineno, name_end_col)

        self.out.append(
            _DefSpan(
                rel_path=self.rel_path,
                kind=kind,
                qualname=qualname,
                parent_qualname=parent,
                depth=depth,
                begin_b=begin_b,
                end_b=end_b,
                name_begin_b=name_begin_b,
                name_end_b=name_end_b,
            )
        )

        self.stack.append(node.name)
        self.generic_visit(node)
        self.stack.pop()


def _extract_defs(repo_root: Path) -> list[_DefSpan]:
    out: list[_DefSpan] = []
    for file_path in _iter_py_files(repo_root):
        rel = file_path.relative_to(repo_root).as_posix()
        text = file_path.read_text(encoding="utf-8")
        lines, starts = _build_line_index(text)

        tree = ast.parse(text, filename=str(file_path))
        collector = _DefCollector(rel_path=rel, lines=lines, starts=starts)
        collector.visit(tree)
        out.extend(collector.out)

    # determinism: stable global order (used by tests + reproducible ids)
    out.sort(key=lambda d: (d.rel_path, d.begin_b, d.end_b, d.kind, d.qualname))
    return out


def _build_inputs_from_fixture(repo_root: Path) -> tuple[pa.Table, pa.Table]:
    """
    Build:
      - syntax_nodes_v1 input (def/class nodes only, with goid label)
      - scip_occurrences_v1 input (definition-name spans only)

    We set file_id = rel_path for simplicity in tests.
    """
    defs = _extract_defs(repo_root)

    # Assign deterministic node_id per file
    per_file: dict[str, list[_DefSpan]] = {}
    for d in defs:
        per_file.setdefault(d.rel_path, []).append(d)

    qual_to_id: dict[tuple[str, str], int] = {}
    syntax_rows: list[dict[str, Any]] = []
    occ_rows: list[dict[str, Any]] = []

    for rel_path, items in per_file.items():
        items.sort(key=lambda d: (d.begin_b, d.end_b, d.kind, d.qualname))
        # node ids
        for node_id, d in enumerate(items):
            qual_to_id[(rel_path, d.qualname)] = node_id

        module = _module_from_relpath(rel_path)

        for d in items:
            node_id = qual_to_id[(rel_path, d.qualname)]
            parent_id = qual_to_id.get((rel_path, d.parent_qualname)) if d.parent_qualname else None

            # goid and symbol strings: keep them delimiter-safe (we use delimiter="|")
            if d.kind in ("FunctionDef", "AsyncFunctionDef"):
                goid = f"F:{module}.{d.qualname}"
                sym = f"def:{module}.{d.qualname}"
                kind_norm = "FunctionDef" if d.kind == "FunctionDef" else "AsyncFunctionDef"
            else:
                goid = f"C:{module}.{d.qualname}"
                sym = f"class:{module}.{d.qualname}"
                kind_norm = "ClassDef"

            syntax_rows.append(
                {
                    "file_id": rel_path,
                    "node_id": node_id,
                    "parent_node_id": parent_id,
                    "kind": kind_norm,
                    "depth": d.depth,
                    "begin_b": d.begin_b,
                    "end_b": d.end_b,
                    "is_def": True,
                    "goid": goid,
                    "name": d.qualname.split(".")[-1],
                }
            )

            # “SCIP-like” definition occurrence at the *name token*
            occ_rows.append(
                {
                    "file_id": rel_path,
                    "begin_b": d.name_begin_b,
                    "end_b": d.name_end_b,
                    "symbol": sym,
                    "role_flags": 1,
                    "role_rank": 0,
                    "is_definition": True,
                }
            )

    syntax_in = pa.Table.from_pylist(syntax_rows) if syntax_rows else pa.table({})
    occ_in = pa.Table.from_pylist(occ_rows) if occ_rows else pa.table({})
    return syntax_in, occ_in


# -----------------------------
# Golden snapshot helpers
# -----------------------------

def _golden_path_for(rel_path: str) -> Path:
    # mirror fixture layout under GOLDEN_DIR and suffix with ".segments.json"
    p = GOLDEN_DIR / rel_path
    return p.with_suffix(".segments.json")


def _write_or_assert_golden(path: Path, obj: Any) -> None:
    import json

    text = json.dumps(obj, indent=2, sort_keys=True) + "\n"

    if UPDATE_GOLDENS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return

    if not path.exists():
        raise AssertionError(
            f"Missing golden snapshot: {path}\n"
            f"Generate with: CODEINTEL_UPDATE_GOLDENS=1 pytest -k span_normalizer_golden -q"
        )

    expected = path.read_text(encoding="utf-8")
    assert expected == text


def _compute_segments(repo_root: Path) -> dict[str, list[dict[str, Any]]]:
    """
    Runs the *real* span pipeline codepaths you added:
      spans__syntax_nodes_v1 -> spans__scip_occurrences_v1 -> spans__normalized_segments_v1
    on the tiny fixture repo.

    Returns: rel_path -> list of canonical segment dicts.
    """
    syntax_in, occ_in = _build_inputs_from_fixture(repo_root)

    syntax = spans__syntax_nodes_v1(syntax_in, run_id=RUN_ID, repo_id=REPO_ID)
    occ = spans__scip_occurrences_v1(occ_in, run_id=RUN_ID, repo_id=REPO_ID)

    policy = SpanLabelPolicy(
        delimiter="|",
        include_syntax_goid=True,
        include_syntax_node=False,
        include_scip_symbol=True,
        include_scip_definitions_only=True,  # keep goldens compact + stable
    )

    seg = spans__normalized_segments_v1(
        syntax,
        occ,
        run_id=RUN_ID,
        repo_id=REPO_ID,
        label_policy=policy,
    )

    # canonicalize to JSONable per-file lists
    rows = seg.select(
        ["file_id", "segment_id", "begin_b", "end_b", "labels", "labels_hash", "primary_label"]
    ).to_pylist()

    by_file: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        rel = str(r["file_id"])
        by_file.setdefault(rel, []).append(
            {
                "segment_id": int(r["segment_id"]),
                "begin_b": int(r["begin_b"]),
                "end_b": int(r["end_b"]),
                "labels": list(r["labels"]),
                "labels_hash": int(r["labels_hash"]),
                "primary_label": r.get("primary_label"),
            }
        )

    for rel, segs in by_file.items():
        segs.sort(key=lambda s: (s["begin_b"], s["end_b"], s["labels_hash"]))
    return by_file


def _assert_disjoint_and_maximal(segs: list[dict[str, Any]]) -> None:
    # Disjoint, ordered; touching segments must differ in labels (maximality).
    segs = sorted(segs, key=lambda s: (s["begin_b"], s["end_b"]))
    prev = None
    for s in segs:
        b, e = s["begin_b"], s["end_b"]
        assert e > b, f"Invalid segment span [{b},{e})"
        if prev is None:
            prev = s
            continue
        assert b >= prev["end_b"], f"Overlapping segments: prev={prev} cur={s}"
        if b == prev["end_b"]:
            assert s["labels"] != prev["labels"], "Adjacent segments must differ in labels (maximality)"
        prev = s


def _apply_insert_bytes(path: Path, byte_offset: int, insert_bytes: bytes) -> None:
    data = path.read_bytes()
    if not (0 <= byte_offset <= len(data)):
        raise AssertionError(f"byte_offset {byte_offset} out of bounds for {path} len={len(data)}")
    out = data[:byte_offset] + insert_bytes + data[byte_offset:]
    path.write_bytes(out)


def _apply_delete_bytes(path: Path, byte_offset: int, delete_len: int) -> None:
    data = path.read_bytes()
    if not (0 <= byte_offset <= len(data)):
        raise AssertionError(f"byte_offset {byte_offset} out of bounds for {path} len={len(data)}")
    if not (0 <= byte_offset + delete_len <= len(data)):
        raise AssertionError(f"delete range out of bounds: off={byte_offset} len={delete_len} file={len(data)}")
    out = data[:byte_offset] + data[byte_offset + delete_len :]
    path.write_bytes(out)


def _byte_index_of_substring(path: Path, needle: str) -> int:
    text = path.read_text(encoding="utf-8")
    i = text.index(needle)
    # convert character index to byte offset
    return len(text[:i].encode("utf-8"))


def _shift_expected_in_gap(
    orig: list[dict[str, Any]],
    pos: int,
    delta: int,
) -> list[dict[str, Any]]:
    # Valid only when edit happens at a boundary / unlabeled gap (no segment overlaps pos).
    out: list[dict[str, Any]] = []
    for s in orig:
        b, e = s["begin_b"], s["end_b"]
        # No segment may straddle the insertion point for this comparator
        if b < pos < e:
            raise AssertionError(f"Edit position {pos} overlaps labeled segment [{b},{e})")
        if e <= pos:
            out.append(dict(s))
        else:
            out.append(
                {
                    **dict(s),
                    "begin_b": b + delta,
                    "end_b": e + delta,
                }
            )
    # segment_id expected to remain stable under pure shifts
    out.sort(key=lambda x: (x["segment_id"], x["begin_b"], x["end_b"]))
    return out


# -----------------------------
# Tests
# -----------------------------

def test_span_normalizer_golden_snapshots():
    """
    Golden snapshots per file under:
      tests/golden/spans/spans_tiny_repo/...

    Update with:
      CODEINTEL_UPDATE_GOLDENS=1 pytest -k span_normalizer_golden -q
    """
    by_file = _compute_segments(FIXTURE_REPO)

    # Always assert invariants even in update mode
    for rel, segs in by_file.items():
        _assert_disjoint_and_maximal(segs)
        golden_path = _golden_path_for(rel)
        _write_or_assert_golden(golden_path, segs)


def test_edit_insert_prefix_shifts_all_segments(tmp_path: Path):
    """
    Insert at byte 0; expect all labeled segments shift by +delta with identical labels/hashes.
    """
    repo = tmp_path / "repo"
    shutil.copytree(FIXTURE_REPO, repo)

    target_rel = "tiny_pkg/alpha.py"
    target_path = repo / target_rel

    before = _compute_segments(repo)[target_rel]
    _assert_disjoint_and_maximal(before)

    ins = b"# inserted header\n"
    _apply_insert_bytes(target_path, 0, ins)
    delta = len(ins)

    after = _compute_segments(repo)[target_rel]
    _assert_disjoint_and_maximal(after)

    expected = _shift_expected_in_gap(before, pos=0, delta=delta)
    assert after == expected


def test_edit_insert_then_delete_between_defs_roundtrip(tmp_path: Path):
    """
    Insert a comment line right before `class Greeter:` (a gap boundary),
    assert following segments shift, then delete the same bytes and assert we return to exact original.
    """
    repo = tmp_path / "repo"
    shutil.copytree(FIXTURE_REPO, repo)

    target_rel = "tiny_pkg/alpha.py"
    target_path = repo / target_rel

    before = _compute_segments(repo)[target_rel]
    _assert_disjoint_and_maximal(before)

    # insertion point: before the class keyword (stable, syntax-safe)
    pos = _byte_index_of_substring(target_path, "class Greeter:")
    ins = b"# inserted between defs\n"
    _apply_insert_bytes(target_path, pos, ins)
    delta = len(ins)

    mid = _compute_segments(repo)[target_rel]
    _assert_disjoint_and_maximal(mid)

    expected_mid = _shift_expected_in_gap(before, pos=pos, delta=delta)
    assert mid == expected_mid

    # delete exact inserted bytes; should return to original exactly
    _apply_delete_bytes(target_path, pos, len(ins))
    after = _compute_segments(repo)[target_rel]
    _assert_disjoint_and_maximal(after)

    assert after == before


def test_edit_insert_inside_function_preserves_label_universe(tmp_path: Path):
    """
    Insert an indented comment inside outer() body.
    Offsets will change in nontrivial ways, but the *label universe* should be unchanged,
    and normalization invariants must hold.
    """
    repo = tmp_path / "repo"
    shutil.copytree(FIXTURE_REPO, repo)

    target_rel = "tiny_pkg/alpha.py"
    target_path = repo / target_rel

    before = _compute_segments(repo)[target_rel]
    _assert_disjoint_and_maximal(before)

    before_labels = {lbl for s in before for lbl in s["labels"]}

    # Insert after the first line of `def outer(x):\n`
    text = target_path.read_text(encoding="utf-8")
    line_end = text.index("\n") + 1
    pos = len(text[:line_end].encode("utf-8"))

    _apply_insert_bytes(target_path, pos, b"    # inside outer\n")

    after = _compute_segments(repo)[target_rel]
    _assert_disjoint_and_maximal(after)

    after_labels = {lbl for s in after for lbl in s["labels"]}

    assert after_labels == before_labels
```

---

## What you get from this suite

* A **real-file** fixture (not synthetic spans) that still keeps extraction minimal and deterministic.
* Goldens that stabilize your normalization output **per-file** under `tests/golden/spans/spans_tiny_repo/...`.
* Three regression patterns:

  1. **prefix insert** (global shift)
  2. **insert+delete in a known “gap boundary”** (pure shift for downstream segments + roundtrip equality)
  3. **insert inside a labeled region** (label-universe preservation + invariants)

If you want the next increment after *this*: I can adapt the fixture extractor to optionally call your **actual** CodeIntel ingestion path (SCIP decode + LibCST spans) when an env var is set, so the same golden suite can run in **two modes**:

* fast AST-based “unit-ish” mode (always-on CI),
* full pipeline “integration” mode (nightly / pre-release).

Below is a **repo-ready “two-mode” golden suite** upgrade that keeps *one* test file but lets you flip between:

* **unit mode (default):** fast AST-based extractor (always-on CI)
* **integration mode:** calls your **real CodeIntel ingestion path** (SCIP decode + LibCST spans) via a pluggable adapter, intended for nightly / pre-release

It also **separates goldens by mode** so integration outputs don’t fight unit outputs.

---

# 1) Updated golden layout + workflow

## `tests/golden/spans/spans_tiny_repo/README.md` (replace/update)

```md
# span-normalizer goldens (spans_tiny_repo)

This suite runs in two modes:

## Mode A: unit (default, fast)
- Extracts spans from Python `ast` only (defs/classes) and synthetic “SCIP-like” def occurrences.
- Goldens live under: `tests/golden/spans/spans_tiny_repo/unit/`

Run:
  pytest -k span_normalizer_golden -q

Update goldens:
  CODEINTEL_UPDATE_GOLDENS=1 pytest -k span_normalizer_golden -q

## Mode B: integration (real pipeline)
- Calls your actual CodeIntel ingestion path (LibCST spans + SCIP decode).
- Goldens live under: `tests/golden/spans/spans_tiny_repo/integration/`

Run:
  CODEINTEL_SPANS_MODE=integration CODEINTEL_SPANS_ADAPTER=codeintel.testing.spans_integration_adapter:build_inputs \
  pytest -k span_normalizer_golden -q

Update goldens:
  CODEINTEL_SPANS_MODE=integration CODEINTEL_SPANS_ADAPTER=codeintel.testing.spans_integration_adapter:build_inputs \
  CODEINTEL_UPDATE_GOLDENS=1 pytest -k span_normalizer_golden -q

### Env vars
- `CODEINTEL_SPANS_MODE`: `unit` (default) or `integration`
- `CODEINTEL_SPANS_ADAPTER`: `module:function` returning `(syntax_nodes_table, scip_occurrences_table)`
- `CODEINTEL_UPDATE_GOLDENS=1`: rewrite snapshot files
```

---

# 2) Integration adapter hook (pluggable, repo-stable)

Create this **adapter stub** under `src/` so it’s importable without making `tests/` a package.

## `src/codeintel/testing/spans_integration_adapter.py` (new)

```python
from __future__ import annotations

from pathlib import Path
from typing import Tuple

import pyarrow as pa


def build_inputs(repo_root: Path, run_id: str, repo_id: str) -> tuple[pa.Table, pa.Table]:
    """
    Integration-mode adapter for golden span tests.

    Contract:
      Return (syntax_nodes_input, scip_occurrences_input) as PyArrow Tables.

    Required columns for syntax_nodes_input (minimum):
      - file_id (string): use repo-relative path or your canonical file_id
      - node_id (int64): stable per file
      - parent_node_id (int64, nullable) OR omit (projection will fill nulls if column exists)
      - kind (string)
      - depth (int32)
      - begin_b (int64)
      - end_b (int64)
      - goid (string, nullable)   [recommended if you want "goid" labels]
      - is_def (bool, nullable)   [optional]

    Required columns for scip_occurrences_input (minimum):
      - file_id (string)
      - begin_b (int64)
      - end_b (int64)
      - symbol (string)
      - role_flags (int64)
      - role_rank (int16)         [stable mapping]
      - is_definition (bool)

    Notes:
    - You may return your *raw* CodeIntel ingestion tables (e.g. syntax.nodes_v1 + scip.occurrences_v1)
      as long as they already include the required columns above (or you map/rename here).
    - This function is intentionally the only integration coupling point; the golden suite remains unchanged.

    Implementation options (choose one):
      A) Call your Hamilton driver directly for the fixture repo_root, requesting the needed upstream tables.
      B) Call your CodeIntel CLI/build runner to materialize datasets into a temp workspace, then load as Arrow.
      C) Call your internal ingestion APIs (LibCST span extractor + SCIP decode) and assemble Arrow tables here.

    Recommended: (A) Hamilton driver with minimal DAG targets (syntax + scip) for fixture repo.
    """
    raise NotImplementedError(
        "Implement build_inputs() to call your real CodeIntel ingestion path "
        "(LibCST spans + SCIP decode) and return the two Arrow tables required by the span golden suite."
    )
```

> In nightly CI you set:
> `CODEINTEL_SPANS_ADAPTER=codeintel.testing.spans_integration_adapter:build_inputs`
> after implementing it.

---

# 3) Updated golden test module (two modes, same suite)

Replace your existing `tests/golden/spans/test_span_normalizer_golden.py` with this version (it keeps the AST extractor, and adds the integration adapter path + mode-split goldens).

## `tests/golden/spans/test_span_normalizer_golden.py` (replace)

```python
from __future__ import annotations

import ast
import importlib
import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import pyarrow as pa

from codeintel.build.hamilton.native.spans.policy import SpanLabelPolicy
from codeintel.build.hamilton.native.spans.scip_occurrences import spans__scip_occurrences_v1
from codeintel.build.hamilton.native.spans.segments_normalize import spans__normalized_segments_v1
from codeintel.build.hamilton.native.spans.syntax_nodes import spans__syntax_nodes_v1


THIS_DIR = Path(__file__).resolve().parent
TESTS_DIR = THIS_DIR.parents[2]  # .../tests
FIXTURE_REPO = TESTS_DIR / "fixtures" / "spans_tiny_repo"

MODE = os.getenv("CODEINTEL_SPANS_MODE", "unit").strip().lower()
if MODE not in ("unit", "integration"):
    raise RuntimeError(f"Invalid CODEINTEL_SPANS_MODE={MODE!r}; expected 'unit' or 'integration'")

UPDATE_GOLDENS = os.getenv("CODEINTEL_UPDATE_GOLDENS") == "1"

# Default adapter path (importable from src/)
ADAPTER_SPEC = os.getenv(
    "CODEINTEL_SPANS_ADAPTER",
    "codeintel.testing.spans_integration_adapter:build_inputs",
)

RUN_ID = "golden"
REPO_ID = "spans_tiny_repo"

# Mode-split goldens to avoid conflicts
GOLDEN_DIR = THIS_DIR / "spans_tiny_repo" / MODE

PY_FILE_RE = re.compile(r".+\.py$", re.IGNORECASE)


# -----------------------------
# Integration adapter loader
# -----------------------------

def _load_callable(spec: str) -> Callable[..., Any]:
    """
    spec format: 'module.submodule:callable_name'
    """
    if ":" not in spec:
        raise RuntimeError(f"CODEINTEL_SPANS_ADAPTER must be 'module:callable', got {spec!r}")
    mod_name, fn_name = spec.split(":", 1)
    mod = importlib.import_module(mod_name)
    fn = getattr(mod, fn_name, None)
    if fn is None or not callable(fn):
        raise RuntimeError(f"Adapter {spec!r} not found/callable")
    return fn


def _build_inputs_integration(repo_root: Path) -> tuple[pa.Table, pa.Table]:
    build_inputs = _load_callable(ADAPTER_SPEC)
    out = build_inputs(repo_root=repo_root, run_id=RUN_ID, repo_id=REPO_ID)
    if not (isinstance(out, tuple) and len(out) == 2):
        raise RuntimeError("Integration adapter must return (syntax_table, scip_occ_table)")
    syntax_in, occ_in = out
    if not isinstance(syntax_in, pa.Table) or not isinstance(occ_in, pa.Table):
        raise RuntimeError("Integration adapter must return PyArrow Tables")
    return syntax_in, occ_in


# -----------------------------
# Unit-mode “real repo” extractor (AST-based)
# -----------------------------

@dataclass(frozen=True)
class _DefSpan:
    rel_path: str
    kind: str  # "FunctionDef" | "AsyncFunctionDef" | "ClassDef"
    qualname: str
    parent_qualname: str | None
    depth: int
    begin_b: int
    end_b: int
    name_begin_b: int
    name_end_b: int


def _iter_py_files(repo_root: Path) -> list[Path]:
    files: list[Path] = []
    for p in repo_root.rglob("*.py"):
        if "__pycache__" in p.parts:
            continue
        files.append(p)
    return sorted(files)


def _module_from_relpath(rel_path: str) -> str:
    p = Path(rel_path)
    parts = list(p.parts)
    if parts[-1] == "__init__.py":
        parts = parts[:-1]
    else:
        parts[-1] = parts[-1][:-3]
    return ".".join(parts)


def _build_line_index(text: str) -> tuple[list[str], list[int]]:
    lines = text.splitlines(keepends=True)
    starts: list[int] = []
    cur = 0
    for ln in lines:
        starts.append(cur)
        cur += len(ln.encode("utf-8"))
    return lines, starts


def _pos_to_byte(lines: list[str], starts: list[int], lineno: int, col: int) -> int:
    li = lineno - 1
    if li < 0:
        return 0
    if li >= len(lines):
        last = lines[-1] if lines else ""
        return (starts[-1] + len(last.encode("utf-8"))) if starts else 0
    prefix = starts[li]
    return prefix + len(lines[li][:col].encode("utf-8"))


def _find_def_name_cols(line: str, start_col: int, kind: str, fallback_name: str) -> tuple[int, int]:
    sub = line[start_col:]
    if kind in ("FunctionDef", "AsyncFunctionDef"):
        m = re.search(r"\bdef\s+([A-Za-z_]\w*)", sub)
    else:
        m = re.search(r"\bclass\s+([A-Za-z_]\w*)", sub)
    if m:
        return (start_col + m.start(1), start_col + m.end(1))
    idx = sub.find(fallback_name) if fallback_name else -1
    if idx >= 0:
        return (start_col + idx, start_col + idx + len(fallback_name))
    return (start_col, start_col)


class _DefCollector(ast.NodeVisitor):
    def __init__(self, rel_path: str, lines: list[str], starts: list[int]) -> None:
        self.rel_path = rel_path
        self.lines = lines
        self.starts = starts
        self.stack: list[str] = []
        self.out: list[_DefSpan] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> Any:
        self._handle_def(node, "FunctionDef")
        return None

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> Any:
        self._handle_def(node, "AsyncFunctionDef")
        return None

    def visit_ClassDef(self, node: ast.ClassDef) -> Any:
        self._handle_def(node, "ClassDef")
        return None

    def _handle_def(self, node: Any, kind: str) -> None:
        end_lineno = getattr(node, "end_lineno", None)
        end_col = getattr(node, "end_col_offset", None)
        if end_lineno is None or end_col is None:
            return

        qualname = ".".join(self.stack + [node.name])
        parent = ".".join(self.stack) if self.stack else None
        depth = len(self.stack)

        begin_b = _pos_to_byte(self.lines, self.starts, node.lineno, node.col_offset)
        end_b = _pos_to_byte(self.lines, self.starts, end_lineno, end_col)

        line = self.lines[node.lineno - 1]
        name_begin_col, name_end_col = _find_def_name_cols(
            line=line,
            start_col=node.col_offset,
            kind=kind,
            fallback_name=node.name,
        )
        name_begin_b = _pos_to_byte(self.lines, self.starts, node.lineno, name_begin_col)
        name_end_b = _pos_to_byte(self.lines, self.starts, node.lineno, name_end_col)

        self.out.append(
            _DefSpan(
                rel_path=self.rel_path,
                kind=kind,
                qualname=qualname,
                parent_qualname=parent,
                depth=depth,
                begin_b=begin_b,
                end_b=end_b,
                name_begin_b=name_begin_b,
                name_end_b=name_end_b,
            )
        )

        self.stack.append(node.name)
        self.generic_visit(node)
        self.stack.pop()


def _extract_defs(repo_root: Path) -> list[_DefSpan]:
    out: list[_DefSpan] = []
    for file_path in _iter_py_files(repo_root):
        rel = file_path.relative_to(repo_root).as_posix()
        text = file_path.read_text(encoding="utf-8")
        lines, starts = _build_line_index(text)

        tree = ast.parse(text, filename=str(file_path))
        collector = _DefCollector(rel_path=rel, lines=lines, starts=starts)
        collector.visit(tree)
        out.extend(collector.out)

    out.sort(key=lambda d: (d.rel_path, d.begin_b, d.end_b, d.kind, d.qualname))
    return out


def _build_inputs_unit(repo_root: Path) -> tuple[pa.Table, pa.Table]:
    defs = _extract_defs(repo_root)

    per_file: dict[str, list[_DefSpan]] = {}
    for d in defs:
        per_file.setdefault(d.rel_path, []).append(d)

    qual_to_id: dict[tuple[str, str], int] = {}
    syntax_rows: list[dict[str, Any]] = []
    occ_rows: list[dict[str, Any]] = []

    for rel_path, items in per_file.items():
        items.sort(key=lambda d: (d.begin_b, d.end_b, d.kind, d.qualname))
        for node_id, d in enumerate(items):
            qual_to_id[(rel_path, d.qualname)] = node_id

        module = _module_from_relpath(rel_path)

        for d in items:
            node_id = qual_to_id[(rel_path, d.qualname)]
            parent_id = qual_to_id.get((rel_path, d.parent_qualname)) if d.parent_qualname else None

            if d.kind in ("FunctionDef", "AsyncFunctionDef"):
                goid = f"F:{module}.{d.qualname}"
                sym = f"def:{module}.{d.qualname}"
                kind_norm = "FunctionDef" if d.kind == "FunctionDef" else "AsyncFunctionDef"
            else:
                goid = f"C:{module}.{d.qualname}"
                sym = f"class:{module}.{d.qualname}"
                kind_norm = "ClassDef"

            syntax_rows.append(
                {
                    "file_id": rel_path,
                    "node_id": node_id,
                    "parent_node_id": parent_id,
                    "kind": kind_norm,
                    "depth": d.depth,
                    "begin_b": d.begin_b,
                    "end_b": d.end_b,
                    "is_def": True,
                    "goid": goid,
                    "name": d.qualname.split(".")[-1],
                }
            )

            occ_rows.append(
                {
                    "file_id": rel_path,
                    "begin_b": d.name_begin_b,
                    "end_b": d.name_end_b,
                    "symbol": sym,
                    "role_flags": 1,
                    "role_rank": 0,
                    "is_definition": True,
                }
            )

    syntax_in = pa.Table.from_pylist(syntax_rows) if syntax_rows else pa.table({})
    occ_in = pa.Table.from_pylist(occ_rows) if occ_rows else pa.table({})
    return syntax_in, occ_in


# -----------------------------
# Golden snapshot helpers
# -----------------------------

def _golden_path_for(rel_path: str) -> Path:
    p = GOLDEN_DIR / rel_path
    return p.with_suffix(".segments.json")


def _write_or_assert_golden(path: Path, obj: Any) -> None:
    import json

    text = json.dumps(obj, indent=2, sort_keys=True) + "\n"

    if UPDATE_GOLDENS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return

    if not path.exists():
        raise AssertionError(
            f"Missing golden snapshot: {path}\n"
            f"Generate with: CODEINTEL_UPDATE_GOLDENS=1 pytest -k span_normalizer_golden -q\n"
            f"(integration: set CODEINTEL_SPANS_MODE=integration + CODEINTEL_SPANS_ADAPTER=...)"
        )

    expected = path.read_text(encoding="utf-8")
    assert expected == text


def _compute_segments(repo_root: Path) -> dict[str, list[dict[str, Any]]]:
    # Mode switch
    if MODE == "integration":
        syntax_in, occ_in = _build_inputs_integration(repo_root)
    else:
        syntax_in, occ_in = _build_inputs_unit(repo_root)

    # Project -> normalize using the same span pipeline code in both modes
    syntax = spans__syntax_nodes_v1(syntax_in, run_id=RUN_ID, repo_id=REPO_ID)
    occ = spans__scip_occurrences_v1(occ_in, run_id=RUN_ID, repo_id=REPO_ID)

    # Keep goldens compact + stable: GOIDs + def symbols only
    policy = SpanLabelPolicy(
        delimiter="|",
        include_syntax_goid=True,
        include_syntax_node=False,
        include_scip_symbol=True,
        include_scip_definitions_only=True,
    )

    seg = spans__normalized_segments_v1(
        syntax,
        occ,
        run_id=RUN_ID,
        repo_id=REPO_ID,
        label_policy=policy,
    )

    rows = seg.select(
        ["file_id", "segment_id", "begin_b", "end_b", "labels", "labels_hash", "primary_label"]
    ).to_pylist()

    by_file: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        rel = str(r["file_id"])
        by_file.setdefault(rel, []).append(
            {
                "segment_id": int(r["segment_id"]),
                "begin_b": int(r["begin_b"]),
                "end_b": int(r["end_b"]),
                "labels": list(r["labels"]),
                "labels_hash": int(r["labels_hash"]),
                "primary_label": r.get("primary_label"),
            }
        )

    for rel, segs in by_file.items():
        segs.sort(key=lambda s: (s["begin_b"], s["end_b"], s["labels_hash"]))
    return by_file


def _assert_disjoint_and_maximal(segs: list[dict[str, Any]]) -> None:
    segs = sorted(segs, key=lambda s: (s["begin_b"], s["end_b"]))
    prev = None
    for s in segs:
        b, e = s["begin_b"], s["end_b"]
        assert e > b, f"Invalid segment span [{b},{e})"
        if prev is None:
            prev = s
            continue
        assert b >= prev["end_b"], f"Overlapping segments: prev={prev} cur={s}"
        if b == prev["end_b"]:
            assert s["labels"] != prev["labels"], "Adjacent segments must differ in labels (maximality)"
        prev = s


def _apply_insert_bytes(path: Path, byte_offset: int, insert_bytes: bytes) -> None:
    data = path.read_bytes()
    assert 0 <= byte_offset <= len(data)
    path.write_bytes(data[:byte_offset] + insert_bytes + data[byte_offset:])


def _apply_delete_bytes(path: Path, byte_offset: int, delete_len: int) -> None:
    data = path.read_bytes()
    assert 0 <= byte_offset <= len(data)
    assert 0 <= byte_offset + delete_len <= len(data)
    path.write_bytes(data[:byte_offset] + data[byte_offset + delete_len :])


def _byte_index_of_substring(path: Path, needle: str) -> int:
    text = path.read_text(encoding="utf-8")
    i = text.index(needle)
    return len(text[:i].encode("utf-8"))


def _snap_pos_to_boundary(segs: list[dict[str, Any]], pos: int) -> int:
    # If pos is strictly inside any labeled segment, snap to that segment's end boundary (safe half-open boundary).
    for s in segs:
        b, e = s["begin_b"], s["end_b"]
        if b < pos < e:
            return e
    return pos


def _shift_expected_in_gap(orig: list[dict[str, Any]], pos: int, delta: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for s in orig:
        b, e = s["begin_b"], s["end_b"]
        if b < pos < e:
            raise AssertionError(f"Edit position {pos} overlaps labeled segment [{b},{e})")
        if e <= pos:
            out.append(dict(s))
        else:
            out.append({**dict(s), "begin_b": b + delta, "end_b": e + delta})
    out.sort(key=lambda x: (x["segment_id"], x["begin_b"], x["end_b"]))
    return out


# -----------------------------
# Tests
# -----------------------------

def test_span_normalizer_golden_snapshots():
    by_file = _compute_segments(FIXTURE_REPO)
    for rel, segs in by_file.items():
        _assert_disjoint_and_maximal(segs)
        _write_or_assert_golden(_golden_path_for(rel), segs)


def test_edit_insert_prefix_shifts_all_segments(tmp_path: Path):
    repo = tmp_path / "repo"
    shutil.copytree(FIXTURE_REPO, repo)

    target_rel = "tiny_pkg/alpha.py"
    target_path = repo / target_rel

    before = _compute_segments(repo)[target_rel]
    _assert_disjoint_and_maximal(before)

    ins = b"# inserted header\n"
    _apply_insert_bytes(target_path, 0, ins)
    delta = len(ins)

    after = _compute_segments(repo)[target_rel]
    _assert_disjoint_and_maximal(after)

    expected = _shift_expected_in_gap(before, pos=0, delta=delta)
    assert after == expected


def test_edit_insert_then_delete_boundary_roundtrip(tmp_path: Path):
    repo = tmp_path / "repo"
    shutil.copytree(FIXTURE_REPO, repo)

    target_rel = "tiny_pkg/alpha.py"
    target_path = repo / target_rel

    before = _compute_segments(repo)[target_rel]
    _assert_disjoint_and_maximal(before)

    pos = _byte_index_of_substring(target_path, "class Greeter:")
    pos = _snap_pos_to_boundary(before, pos)

    ins = b"# inserted between defs\n"
    _apply_insert_bytes(target_path, pos, ins)
    delta = len(ins)

    mid = _compute_segments(repo)[target_rel]
    _assert_disjoint_and_maximal(mid)

    expected_mid = _shift_expected_in_gap(before, pos=pos, delta=delta)
    assert mid == expected_mid

    _apply_delete_bytes(target_path, pos, len(ins))
    after = _compute_segments(repo)[target_rel]
    _assert_disjoint_and_maximal(after)

    assert after == before


def test_edit_insert_inside_function_preserves_label_universe(tmp_path: Path):
    repo = tmp_path / "repo"
    shutil.copytree(FIXTURE_REPO, repo)

    target_rel = "tiny_pkg/alpha.py"
    target_path = repo / target_rel

    before = _compute_segments(repo)[target_rel]
    _assert_disjoint_and_maximal(before)

    before_labels = {lbl for s in before for lbl in s["labels"]}

    text = target_path.read_text(encoding="utf-8")
    line_end = text.index("\n") + 1
    pos = len(text[:line_end].encode("utf-8"))

    _apply_insert_bytes(target_path, pos, b"    # inside outer\n")

    after = _compute_segments(repo)[target_rel]
    _assert_disjoint_and_maximal(after)

    after_labels = {lbl for s in after for lbl in s["labels"]}
    assert after_labels == before_labels
```

---

# 4) How you wire “real pipeline” ingestion (what the adapter should do)

In **integration mode**, your adapter should do *exactly one thing*:

> For the fixture repo root, run **LibCST span extraction** and **SCIP decode**, then return two Arrow tables shaped like the inputs expected by `spans__syntax_nodes_v1` and `spans__scip_occurrences_v1`.

Typical practical patterns:

### A) Hamilton-minidag (recommended)

* Build a minimal driver configured to:

  * scan `repo_root` → `core_files_v1`
  * LibCST parse spans → `syntax_nodes_v1`
  * SCIP index/decode → `scip_occurrences_v1`
* Return those two tables (or already-projected `spans.*` inputs if you prefer).

### B) CLI materialize then load

* Run your CodeIntel build CLI in a temp workspace:

  * materialize `syntax.nodes_v1` and `scip.occurrences_v1` (or directly your `spans.*` tables)
* Load as Arrow and return.

Either way, the golden suite stays unchanged; only `build_inputs()` is the integration seam.

---

If you want one more increment after this: I can provide a **concrete adapter implementation template** for the Hamilton-minidag approach that mirrors your “targets from DAG tags” pattern (driver construction + target execution + Arrow table extraction), written so you only need to fill in *your* actual driver/build entrypoint and dataset names.

Below is a **concrete integration adapter template** that implements the **Hamilton-minidag approach** in a way that mirrors your **“targets from DAG tags / `t__` anchors”** pattern:

* build a **minimal Hamilton Driver** from a configurable set of native modules,
* execute **exactly two anchor nodes** (or two dataset targets),
* coerce outputs into **PyArrow Tables**,
* return `(syntax_nodes_input, scip_occurrences_input)` for the golden suite.

It’s written so you typically only need to fill:

1. **your module list** (or set it via env var), and
2. **the two upstream dataset targets** (or node names) you want computed.

---

## `src/codeintel/testing/spans_integration_adapter.py` (replace the stub with this)

```python
from __future__ import annotations

import importlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Protocol, Sequence, Tuple

import pyarrow as pa


# ======================================================================================
# CONFIG SURFACE (only knobs you should need to set per-repo)
# ======================================================================================

# 1) Which DAG modules to load for the minidag
#
# Provide as env (recommended for CI):
#   CODEINTEL_SPANS_DAG_MODULES="codeintel.build.hamilton.native.core.files,codeintel.build.hamilton.native.syntax,..."
#
# Or hardcode below.
DEFAULT_DAG_MODULES: tuple[str, ...] = (
    # ---- Fill these with your actual Hamilton-native module paths ----
    # "codeintel.build.hamilton.native.core.files",
    # "codeintel.build.hamilton.native.syntax.nodes",
    # "codeintel.build.hamilton.native.scip.occurrences",
)

# 2) Which upstream datasets (targets) to compute
#
# If you follow the naming convention: t__<dataset with '.' -> '__'> (as in this project),
# then setting the DATASET names is enough.
#
# Examples:
#   CODEINTEL_SPANS_SYNTAX_SOURCE_DATASET="syntax.nodes_v1"
#   CODEINTEL_SPANS_SCIP_SOURCE_DATASET="scip.occurrences_v1"
SYNTAX_SOURCE_DATASET = os.getenv("CODEINTEL_SPANS_SYNTAX_SOURCE_DATASET", "syntax.nodes_v1")
SCIP_SOURCE_DATASET = os.getenv("CODEINTEL_SPANS_SCIP_SOURCE_DATASET", "scip.occurrences_v1")

# 3) Optional: override anchor node names directly (if your naming diverges)
#   CODEINTEL_SPANS_SYNTAX_SOURCE_NODE="t__syntax__nodes_v1"
#   CODEINTEL_SPANS_SCIP_SOURCE_NODE="t__scip__occurrences_v1"
SYNTAX_SOURCE_NODE = os.getenv("CODEINTEL_SPANS_SYNTAX_SOURCE_NODE", "")
SCIP_SOURCE_NODE = os.getenv("CODEINTEL_SPANS_SCIP_SOURCE_NODE", "")

# 4) Runtime inputs injection:
# Many CodeIntel DAGs accept repo_root/run_id/repo_id as inputs; you can add more via JSON:
#   CODEINTEL_SPANS_ADAPTER_INPUTS_JSON='{"workspace_root":"/tmp/codeintel-work","scip_cli":"/usr/local/bin/scip"}'
ADAPTER_INPUTS_JSON = os.getenv("CODEINTEL_SPANS_ADAPTER_INPUTS_JSON", "")

# 5) If your t__ anchors return DatasetRef-like objects rather than Arrow Tables,
# you can optionally wire a loader via env:
#   CODEINTEL_SPANS_DATASET_LOADER="codeintel.storage.gateway:load_arrow_for_test"
DATASET_LOADER_SPEC = os.getenv("CODEINTEL_SPANS_DATASET_LOADER", "")


# ======================================================================================
# OPTIONAL LOADER PROTOCOL (for t__ nodes that materialize + return refs)
# ======================================================================================

class DatasetLoader(Protocol):
    def __call__(self, ref_or_name: Any, *, run_id: str, repo_id: str) -> pa.Table: ...


def _load_callable(spec: str) -> Callable[..., Any]:
    """
    spec format: 'module.submodule:callable_name'
    """
    if ":" not in spec:
        raise RuntimeError(f"Expected 'module:callable' spec, got {spec!r}")
    mod_name, fn_name = spec.split(":", 1)
    mod = importlib.import_module(mod_name)
    fn = getattr(mod, fn_name, None)
    if fn is None or not callable(fn):
        raise RuntimeError(f"Callable not found for spec {spec!r}")
    return fn


def _maybe_load_dataset(obj: Any, *, run_id: str, repo_id: str) -> pa.Table | None:
    """
    If DATASET_LOADER_SPEC is provided, use it to load non-table outputs.
    This is how integration mode can stay faithful to your 't__ materialize returns DatasetRef' pattern.
    """
    if not DATASET_LOADER_SPEC:
        return None
    loader = _load_callable(DATASET_LOADER_SPEC)
    out = loader(obj, run_id=run_id, repo_id=repo_id)
    if not isinstance(out, pa.Table):
        raise TypeError(f"Dataset loader returned {type(out)}; expected pyarrow.Table")
    return out


# ======================================================================================
# HAMILTON MINIDAG WIRING
# ======================================================================================

def _dataset_to_anchor_node(dataset: str) -> str:
    """
    Convention used throughout this project:
      dataset 'spans.syntax_nodes_v1' -> anchor node 't__spans__syntax_nodes_v1'
      dataset 'syntax.nodes_v1'       -> anchor node 't__syntax__nodes_v1'
    """
    return "t__" + dataset.replace(".", "__")


def _import_modules(module_paths: Sequence[str]) -> list[Any]:
    mods: list[Any] = []
    for p in module_paths:
        p = p.strip()
        if not p:
            continue
        mods.append(importlib.import_module(p))
    if not mods:
        raise RuntimeError(
            "No DAG modules configured. Set CODEINTEL_SPANS_DAG_MODULES or update DEFAULT_DAG_MODULES."
        )
    return mods


def _build_driver(modules: Sequence[Any]) -> Any:
    """
    Minimal Hamilton driver construction.
    If you have an internal wrapper/builder, swap it in here.

    Default: vanilla Hamilton Driver(config, *modules).
    """
    try:
        from hamilton import driver as h_driver  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "Hamilton not importable in this environment. "
            "Install/enable Hamilton in integration jobs."
        ) from e

    config: dict[str, Any] = {}
    return h_driver.Driver(config, *modules)


def _coerce_arrow(obj: Any, *, run_id: str, repo_id: str) -> pa.Table:
    """
    Coerce common table-like objects into pyarrow.Table.
    Supports:
      - pyarrow.Table
      - objects with .to_arrow()
      - objects with .to_pyarrow()
      - dataset refs via optional DATASET_LOADER_SPEC
    """
    if isinstance(obj, pa.Table):
        return obj

    # dataset-ref loader hook (materialize-first pipelines)
    loaded = _maybe_load_dataset(obj, run_id=run_id, repo_id=repo_id)
    if loaded is not None:
        return loaded

    # common conversion hooks
    if hasattr(obj, "to_arrow") and callable(getattr(obj, "to_arrow")):
        out = obj.to_arrow()
        if isinstance(out, pa.Table):
            return out

    if hasattr(obj, "to_pyarrow") and callable(getattr(obj, "to_pyarrow")):
        out = obj.to_pyarrow()
        if isinstance(out, pa.Table):
            return out

    # polars fallback (optional)
    if hasattr(obj, "to_pandas") and callable(getattr(obj, "to_pandas")):
        import pandas as pd
        df = obj.to_pandas()
        return pa.Table.from_pandas(df, preserve_index=False)

    raise TypeError(
        f"Cannot coerce output of type {type(obj)} into pyarrow.Table. "
        "Either return Arrow Tables from the DAG outputs or configure CODEINTEL_SPANS_DATASET_LOADER."
    )


def _merge_runtime_inputs(repo_root: Path, run_id: str, repo_id: str) -> dict[str, Any]:
    inputs: dict[str, Any] = {
        # Most CodeIntel DAGs want these:
        "repo_root": repo_root,
        "run_id": run_id,
        "repo_id": repo_id,
    }
    if ADAPTER_INPUTS_JSON.strip():
        extra = json.loads(ADAPTER_INPUTS_JSON)
        if not isinstance(extra, dict):
            raise RuntimeError("CODEINTEL_SPANS_ADAPTER_INPUTS_JSON must be a JSON object")
        inputs.update(extra)
    return inputs


# ======================================================================================
# PUBLIC ENTRYPOINT (called by golden suite in integration mode)
# ======================================================================================

def build_inputs(repo_root: Path, run_id: str, repo_id: str) -> tuple[pa.Table, pa.Table]:
    """
    Integration-mode adapter for golden span tests.

    This runs a Hamilton *minidag* for the fixture repo_root and returns two Arrow tables:
      (syntax_nodes_input, scip_occurrences_input)

    You can drive it either by dataset names (preferred) or explicit node names.

    Env knobs:
      - CODEINTEL_SPANS_DAG_MODULES
      - CODEINTEL_SPANS_SYNTAX_SOURCE_DATASET / CODEINTEL_SPANS_SCIP_SOURCE_DATASET
      - CODEINTEL_SPANS_SYNTAX_SOURCE_NODE / CODEINTEL_SPANS_SCIP_SOURCE_NODE
      - CODEINTEL_SPANS_ADAPTER_INPUTS_JSON
      - CODEINTEL_SPANS_DATASET_LOADER (optional, if anchors return refs)
    """
    repo_root = Path(repo_root)

    # modules
    module_csv = os.getenv("CODEINTEL_SPANS_DAG_MODULES", "").strip()
    module_paths = (
        tuple(m.strip() for m in module_csv.split(",") if m.strip())
        if module_csv
        else DEFAULT_DAG_MODULES
    )
    modules = _import_modules(module_paths)

    # driver
    dr = _build_driver(modules)

    # outputs: prefer explicit node overrides, else derive from dataset names
    syntax_node = SYNTAX_SOURCE_NODE.strip() or _dataset_to_anchor_node(SYNTAX_SOURCE_DATASET)
    scip_node = SCIP_SOURCE_NODE.strip() or _dataset_to_anchor_node(SCIP_SOURCE_DATASET)
    outputs = [syntax_node, scip_node]

    # runtime inputs
    inputs = _merge_runtime_inputs(repo_root=repo_root, run_id=run_id, repo_id=repo_id)

    # execute minidag
    # NOTE: Hamilton returns a dict: {output_name: value}
    try:
        results: Mapping[str, Any] = dr.execute(outputs, inputs=inputs)
    except TypeError:
        # Some Hamilton versions call the kwarg "overrides" or omit "inputs".
        # If your Hamilton API differs, adjust only this block.
        results = dr.execute(outputs, **inputs)  # type: ignore[misc]

    if syntax_node not in results or scip_node not in results:
        raise RuntimeError(f"Driver did not return expected outputs: {outputs}. Got: {list(results.keys())}")

    syntax_tbl = _coerce_arrow(results[syntax_node], run_id=run_id, repo_id=repo_id)
    scip_tbl = _coerce_arrow(results[scip_node], run_id=run_id, repo_id=repo_id)

    return (syntax_tbl, scip_tbl)
```

---

## How this mirrors your “targets from DAG tags” pattern

* In your convention, a dataset `syntax.nodes_v1` is anchored by `t__syntax__nodes_v1`.
* This adapter defaults to executing those **anchor nodes** derived by:

```python
t__ + dataset.replace(".", "__")
```

So you can configure integration mode purely with dataset names:

```bash
CODEINTEL_SPANS_MODE=integration \
CODEINTEL_SPANS_DAG_MODULES="...your minimal modules..." \
CODEINTEL_SPANS_SYNTAX_SOURCE_DATASET="syntax.nodes_v1" \
CODEINTEL_SPANS_SCIP_SOURCE_DATASET="scip.occurrences_v1" \
pytest -k span_normalizer_golden -q
```

If your repo deviates from that anchor naming, set the explicit node env vars instead:

```bash
CODEINTEL_SPANS_SYNTAX_SOURCE_NODE="t__syntax__nodes_v1_realname" \
CODEINTEL_SPANS_SCIP_SOURCE_NODE="t__scip__occurrences_v1_realname"
```

---

## If your `t__` anchors return DatasetRefs (materialize-first)

If your anchor nodes *materialize* and return something like `DatasetRef` (not an Arrow table), set a loader hook:

1. Implement a loader callable somewhere stable, e.g.:

```python
# src/codeintel/storage/testing_loaders.py
import pyarrow as pa

def load_arrow_for_test(ref_or_name, *, run_id: str, repo_id: str) -> pa.Table:
    # ref_or_name might be DatasetRef or a dataset string
    # Use your storage gateway to load as Arrow.
    # return gateway.load_arrow(...)
    raise NotImplementedError
```

2. Then run integration mode with:

```bash
CODEINTEL_SPANS_DATASET_LOADER="codeintel.storage.testing_loaders:load_arrow_for_test"
```

This keeps the adapter faithful to: **“execute `t__` → materialize → load Arrow”**.

---

## What you (typically) need to fill in

1. **`CODEINTEL_SPANS_DAG_MODULES`**: the smallest module set that can produce:

   * `t__syntax__nodes_v1` (LibCST spans path)
   * `t__scip__occurrences_v1` (SCIP decode path)

2. If needed, **dataset/node names**:

   * `CODEINTEL_SPANS_SYNTAX_SOURCE_DATASET`
   * `CODEINTEL_SPANS_SCIP_SOURCE_DATASET`
   * or explicit `*_SOURCE_NODE`

3. If anchors return refs, wire **`CODEINTEL_SPANS_DATASET_LOADER`**.

---

If you want the next increment: tell me the *actual* anchor node names (or dataset targets) you use for “LibCST nodes table” and “SCIP occurrences table”, and the Hamilton-native module package root you’re using. I can then rewrite the defaults in this adapter to match your repo’s exact module paths + provide the minimal `CODEINTEL_SPANS_DAG_MODULES` set (so you don’t have to guess which modules are required).
