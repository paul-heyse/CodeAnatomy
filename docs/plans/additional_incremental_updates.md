Below is how I’d structure **PR17–PR18** to add the “symbol-level impact closure” precision upgrade on top of **PR16 (import-closure incremental)**, in the same **file/function-level checklist style** with **reviewable commit slices**, **exact signatures**, and the **minimal diffs** I’d actually apply.

I’m going to assume PR16 already gives you:

* a persistent “state store” of prior run datasets (partition-upserted),
* `inc_changed_files_v1` (or equivalent) computed from `repo_files`,
* an “impacted set” concept that can be threaded into extraction/normalization/CPG (filter-by-file_id / filter-by-path).

If any of those names differ in your branch, the code shapes below still apply 1:1.

---

## PR17 — Export delta + reverse callsite closure (foundation)

### Goal

Add a **symbol/qname-aware impact seed**:

1. **Compute exported API surface** per module (file) from `cst_defs_norm`
2. Diff exported surface for changed files: **`added|removed exports`**
3. Use the **prior run** call-resolution relations:

   * `rel_callsite_symbol` (SCIP symbol-based)
   * `rel_callsite_qname` (qname-id-based)

…to compute the **reverse call closure** = “which files *call* changed exports”.

> PR17 does *not* yet replace import-closure logic; it adds the new “callers closure” and the export delta artifacts so PR18 can combine them with reverse-import precision and then flip the strategy.

---

### New/updated artifacts introduced in PR17

* `dim_exported_defs_v1`
  Canonical “exported definitions” index (partition by `file_id`).
* `inc_changed_exports_v1`
  Delta of exported surface for changed files (added/removed).
* `inc_impacted_callers_v1`
  Caller set derived from prior `rel_callsite_symbol|qname`.

Optional (recommended, small, and useful beyond incremental):

* `rel_def_symbol_v1`
  Relationship table mapping `def_id` → SCIP `symbol` via span alignment.

---

## PR17 — Diff map (commits + reviewable slices)

### Commit 1 — Add `rel_def_symbol_v1` (defs → SCIP symbol) relationship

**Why:** To get “exported symbols” (SCIP) rather than only qnames. This makes caller lookup much more precise when SCIP is enabled.

#### Files

1. **`src/relspec/rules/relationship_specs.py`**

* Add a new interval-align rule definition:

```python
# add to relationship_rule_definitions()
_interval_align_rule(
    _IntervalAlignRuleSpec(
        name="cst_defs__name_to__scip_occurrences",
        output_dataset="rel_def_symbol",
        contract_name="rel_def_symbol_v1",
        inputs=("cst_defs", "scip_occurrences"),
        left_start_col="bstart",
        left_end_col="bend",
        select_left=("def_id", "path", "bstart", "bend"),
    )
),
```

2. **`src/hamilton_pipeline/pipeline_types.py`**

* Extend `CstRelspecInputs` to include `cst_defs_norm`:

```python
@dataclass(frozen=True)
class CstRelspecInputs:
    cst_name_refs: TableLike
    cst_imports_norm: TableLike
    cst_callsites: TableLike
    cst_defs_norm: TableLike  # NEW
```

3. **`src/hamilton_pipeline/modules/cpg_build.py`**

* Update `relspec_cst_inputs(...) -> CstRelspecInputs` signature + return
* Add `"cst_defs": relspec_cst_inputs.cst_defs_norm` to `relspec_input_datasets()`
* Register the new dataset schema + contract spec for `rel_def_symbol_v1` alongside existing relationship contracts.

**Schema shape (suggested minimal):**

* `def_id: string`
* `symbol: string`
* `symbol_roles: int32`
* `path: string`
* `bstart: int64`
* `bend: int64`
* plus confidence/score/rule fields if your relationship outputs standardize those

#### Definition of done

* Relationship compilation includes `rel_def_symbol_v1`
* When SCIP is enabled, `rel_def_symbol` materializes with non-null `symbol` for most top-level defs.

---

### Commit 2 — Add exported-def index builder (`dim_exported_defs_v1`)

**Why:** Your export delta becomes a cheap diff on a compact dataset.

#### Files

1. **`src/incremental/exports.py`** (new)

* Add a columnar export index builder that:

  * filters to **top-level defs** (`container_def_id is null`)
  * explodes `qnames` list (no Python loops)
  * computes `qname_id` with the same hash scheme used elsewhere

**Exact signature:**

```python
from __future__ import annotations
import pyarrow as pa
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import TableLike, pc
from arrowdsl.core.ids import prefixed_hash_id

def build_exported_defs_index(
    cst_defs_norm: TableLike,
    *,
    ctx: ExecutionContext,
    rel_def_symbol: TableLike | None = None,
) -> TableLike:
    ...
```

**Core implementation sketch:**

```python
def build_exported_defs_index(cst_defs_norm, *, ctx, rel_def_symbol=None):
    # top-level only
    top = cst_defs_norm.filter(pc.is_null(cst_defs_norm["container_def_id"]))

    # explode qnames with parent replication using list_parent_indices
    q = top["qnames"]
    parent_idx = pc.list_parent_indices(q)
    flat = pc.list_flatten(q)  # struct(name, source)

    def_id = pc.take(top["def_id"], parent_idx)
    file_id = pc.take(top["file_id"], parent_idx)
    path = pc.take(top["path"], parent_idx)
    def_kind = pc.take(top["def_kind_norm"], parent_idx)
    name = pc.take(top["name"], parent_idx)

    qname = pc.cast(pc.struct_field(flat, "name"), pa.string())
    qsrc = pc.cast(pc.struct_field(flat, "source"), pa.string())
    qname_id = prefixed_hash_id([qname], prefix="QNAME")

    out = pa.Table.from_arrays(
        [file_id, path, def_id, def_kind, name, qname_id, qname, qsrc],
        names=["file_id","path","def_id","def_kind_norm","name","qname_id","qname","qname_source"],
    )

    # optional: attach SCIP symbol via rel_def_symbol (path+def_id join is easiest if present)
    if rel_def_symbol is not None and rel_def_symbol.num_rows > 0:
        # join keys: def_id + path (or def_id alone if globally unique)
        # use your existing join infra (Ibis/DataFusion recommended here)
        out = _join_symbol(out, rel_def_symbol, ctx=ctx)

    return out
```

2. **`src/hamilton_pipeline/modules/incremental.py`** (new if PR16 didn’t create it; otherwise extend)

* Add the Hamilton node:

```python
from hamilton.function_modifiers import cache, tag
from arrowdsl.core.context import ExecutionContext
from arrowdsl.core.interop import TableLike
from incremental.exports import build_exported_defs_index

@cache(format="parquet")
@tag(layer="incremental", artifact="dim_exported_defs", kind="table")
def dim_exported_defs(
    cst_defs_norm: TableLike,
    rel_def_symbol: TableLike | None,
    ctx: ExecutionContext,
) -> TableLike:
    return build_exported_defs_index(cst_defs_norm, ctx=ctx, rel_def_symbol=rel_def_symbol)
```

3. **`src/hamilton_pipeline/modules/__init__.py`**

* Add the incremental module to `ALL_MODULES` (if not already).

#### Definition of done

* You can materialize `dim_exported_defs` for a full run and inspect (file_id/path/def/qname).

---

### Commit 3 — Export delta: `inc_changed_exports_v1`

**Why:** This is the “what changed in public API” set that drives impact closure.

#### Files

1. **`src/incremental/deltas.py`** (new)

**Exact signature:**

```python
from arrowdsl.core.interop import TableLike
from ibis.backends import BaseBackend

def compute_changed_exports(
    *,
    backend: BaseBackend,
    prev_exports: str,  # parquet dataset path
    curr_exports: str,  # parquet dataset path
    changed_files: TableLike,  # has file_id
) -> TableLike:
    ...
```

**Key behavior:**

* Restrict diff to `file_id ∈ changed_files`
* Use `qname_id` as primary identity (and `symbol` if available)
* Emit:

  * `delta_kind`: `"added" | "removed"`
  * `file_id`, `path`, `qname_id`, `qname`, optional `symbol`

**Minimal Ibis/DataFusion diff sketch:**

```python
prev = backend.read_parquet(prev_exports)
curr = backend.read_parquet(curr_exports)
chg = ibis.memtable(changed_files)

prev_f = prev.inner_join(chg, prev.file_id == chg.file_id)
curr_f = curr.inner_join(chg, curr.file_id == chg.file_id)

key = [prev_f.file_id, prev_f.qname_id]  # plus symbol if you want symbol-delta too

added = curr_f.anti_join(prev_f, predicates=[curr_f.file_id == prev_f.file_id,
                                            curr_f.qname_id == prev_f.qname_id]) \
              .mutate(delta_kind=ibis.literal("added"))
removed = prev_f.anti_join(curr_f, predicates=[prev_f.file_id == curr_f.file_id,
                                               prev_f.qname_id == curr_f.qname_id]) \
                .mutate(delta_kind=ibis.literal("removed"))

out = added.union_all(removed)
return out.to_pyarrow()
```

2. **`src/hamilton_pipeline/modules/incremental.py`**

* Add node `inc_changed_exports(...) -> TableLike`
* It reads prior/current export datasets from your PR16 state store abstraction.

#### Definition of done

* On a small edit that only changes a private helper inside a function, `inc_changed_exports` is empty.
* On a rename/add/remove of a top-level function/class, `inc_changed_exports` is non-empty.

---

### Commit 4 — Reverse call closure: `inc_impacted_callers_v1`

**Why:** This is the big win: shrink impacted set to callers of changed exports (rather than “everything that imports the file”).

#### Files

1. **`src/incremental/impact.py`** (new)

**Exact signature:**

```python
from arrowdsl.core.interop import TableLike
from ibis.backends import BaseBackend

def impacted_callers_from_changed_exports(
    *,
    backend: BaseBackend,
    changed_exports: TableLike,           # qname_id (+ optional symbol)
    prev_rel_callsite_qname: str,         # parquet dataset path
    prev_rel_callsite_symbol: str | None, # parquet dataset path (optional)
    prev_repo_files: str,                # parquet dataset path for path->file_id
) -> TableLike:
    ...
```

**Core query logic (two joins + union distinct):**

* **QNAME-based callers**:

  * join `prev_rel_callsite_qname` on `qname_id`
* **SYMBOL-based callers** (if symbol available):

  * join `prev_rel_callsite_symbol` on `symbol`

Then:

* project to `caller_path`
* join to `repo_files` to get `caller_file_id`
* distinct

2. **`src/hamilton_pipeline/modules/incremental.py`**

* Add node `inc_impacted_callers(...) -> TableLike`

#### Definition of done

* A change to exported symbol `pkg.mod.foo` yields impacted callers that previously called that symbol.
* A newly-added export yields impacted callers *if* some existing callsites already referenced it by qname.

---

### PR17 deprecations

None “hard” yet. But **mark as soft-deprecated** in docstrings/config:

* Any internal function like `compute_impacted_files_import_closure_only()`
  → keep it, but note PR18 will switch default strategy.

---

## PR18 — Reverse import precision + integrate symbol-level impacted set

### Goal

Complete the symbol-level closure:

* `impacted = changed_files ∪ reverse_call_references ∪ reverse_imports(of changed exports)`

And then wire it into your incremental driver so extraction/normalization/CPG only runs for impacted files (partition-upsert driven).

---

## PR18 — Diff map (commits + reviewable slices)

### Commit 1 — Reverse import closure on changed exports

**Why:** Call edges aren’t enough—`from X import y` importers must be rebuilt even if they don’t call `y` in a way that yields a callsite edge (or if the call graph is incomplete for that file).

#### Inputs assumed from PR16

You likely already have *some* import graph representation from PR16 import-closure. For this precision upgrade you need a table equivalent to:

`imports_resolved_v1` (or similar), with columns:

* `importer_file_id`, `importer_path`
* `imported_module_fqn` (absolute)
* `imported_name` (nullable; filled for `from ... import name`)
* `is_star` (bool)

If PR16 only stored raw `cst_imports_norm`, then PR18 should add a resolved layer first (but that’s a bigger detour). Since you said “after PR16 import-closure incremental is stable,” I’m assuming you already resolved imports.

#### Files

1. **`src/incremental/import_impact.py`** (new) or extend `incremental/impact.py`

**Exact signature:**

```python
from arrowdsl.core.interop import TableLike
from ibis.backends import BaseBackend

def impacted_importers_from_changed_exports(
    *,
    backend: BaseBackend,
    changed_exports: TableLike,     # must include export_module_fqn + export_name OR qname
    prev_imports_resolved: str,     # parquet dataset path
    prev_repo_files: str,           # parquet dataset path
) -> TableLike:
    ...
```

**Implementation sketch**

* If `changed_exports` has `qname`, derive:

  * `export_module_fqn = rsplit(qname, ".", 1)[0]`
  * `export_name = rsplit(qname, ".", 1)[1]`
* Join `imports_resolved`:

  * `from-import` importers:

    * `imports.imported_module_fqn == export_module_fqn`
    * `imports.imported_name == export_name`
  * `star-import` importers:

    * `imports.imported_module_fqn == export_module_fqn`
    * `imports.is_star == True`

Return importer file_ids (join through `repo_files` if needed).

#### Definition of done

* If `a.py` exports `foo`, and `b.py` has `from a import foo`, and `foo` is removed/renamed: `b.py` is impacted even if there are no call edges.

---

### Commit 2 — Replace impacted-set computation with symbol-level strategy (behind a flag first)

**Why:** This is where the pipeline actually *shrinks* work.

#### Files

1. **`src/hamilton_pipeline/execution.py`** and/or wherever PR16 picks the incremental strategy

* Add a config flag (or options field):

  * `incremental_impact_strategy: Literal["import_closure", "symbol_closure", "hybrid"]`
  * Default initially: `"hybrid"` for safety (then flip later)

2. **`src/hamilton_pipeline/modules/incremental.py`**

* Add node:

```python
@cache()
@tag(layer="incremental", artifact="inc_impacted_files_v2", kind="table")
def inc_impacted_files_v2(
    inc_changed_files: TableLike,
    inc_impacted_callers: TableLike,
    inc_impacted_importers: TableLike,
    impact_strategy: str,
    ctx: ExecutionContext,
) -> TableLike:
    ...
```

**Policy behavior**

* `symbol_closure`:

  * impacted = changed ∪ callers ∪ importers
* `hybrid`:

  * impacted = (symbol_closure) ∪ (import_closure_fallback for files with unresolved exports, or star-import modules, etc.)
* `import_closure`:

  * old PR16 behavior

3. Wherever PR16 threads impacted set into stages:

* Ensure the stage filters use **file_id** (not just path), and that partition-upsert uses file_id partitions.

#### Definition of done

* Running incremental with `symbol_closure` on a small internal edit does not rebuild large import closure sets.

---

### Commit 3 — Diagnostics + artifacts for incremental impact

**Why:** This is what makes it “LLM-agent usable”: you can tell an agent *why* a file was rebuilt.

#### Files

1. **`src/hamilton_pipeline/modules/outputs.py`**

* Add writers (parquet is ideal):

  * `write_inc_changed_exports_parquet(output_dir, inc_changed_exports)`
  * `write_inc_impacted_files_parquet(output_dir, inc_impacted_files_v2)`
  * optionally: `write_inc_impact_reasons_parquet` if you store reason codes

**Recommended minimal schema for reasons**
`inc_impacted_files_v2`:

* `file_id`
* `path`
* `reason_kind` (enum-like string): `"changed_file" | "caller_symbol" | "caller_qname" | "import_from" | "import_star"`
* `reason_ref` (string): symbol/qname/module involved (nullable)

This is *hugely* helpful for agents deciding what to do next.

---

### Commit 4 — Tests (unit + one integration)

**Why:** You’ll break this later without tests.

#### Files

1. **`tests/unit/test_incremental_exports.py`** (new)

* Build tiny `pyarrow.Table` fixtures for `cst_defs_norm` with `qnames` lists and verify:

  * export index only includes top-level defs
  * `qname_id` matches `prefixed_hash_id([qname], prefix="QNAME")`

2. **`tests/unit/test_incremental_export_delta.py`** (new)

* Two export tables (prev/curr) and a `changed_files` table → assert added/removed rows

3. **`tests/unit/test_incremental_call_closure.py`** (new)

* Tiny `rel_callsite_qname` table + changed_exports → impacted caller paths match expectation

4. Optional: **`tests/integration/test_incremental_symbol_closure_smoke.py`**

* Run pipeline twice on a tiny fixture repo:

  * edit internal helper → impacted set tiny
  * rename exported function → impacted includes callers/importers

---

## PR18 deprecations

After PR18 lands (and after you’ve validated in your own repo):

* **Deprecate** the PR16 default behavior of “module import-closure dominates impact”.
* Keep it available as `impact_strategy="import_closure"` for:

  * repos with heavy dynamic imports,
  * no SCIP indexing,
  * early debugging and regression triage.

---

## Notes that matter for correctness (and why this design stays conservative)

* **New exports**: This design still finds callers because `rel_callsite_qname` is driven by callsite qname candidates; even if the export didn’t exist previously, the qname *string* can exist in callsites, producing `qname_id` rows.
* **Removed exports**: Previous callers are captured by prior `rel_callsite_symbol` and/or `rel_callsite_qname`.
* **Star imports**: Always conservative—rebuild the star importers of a module whose exports changed.
* **Alias imports**: Covered primarily by the **callsite** closure (symbols/qnames), not by import-name matching alone. That’s why caller closure and import closure are unioned.

If you want an even more conservative but still small enhancement later: add a **1-iteration “fixpoint”** option:

* After rebuilding impacted files, recompute their export delta; if any *new* export deltas appear, run closure again (usually 1 extra iteration max on real repos).

---

## Quick “what I’d actually set as default”

* Start with **`impact_strategy="hybrid"`** for 1–2 weeks while you validate.
* Then flip to **`symbol_closure`** once you trust the diagnostics outputs and have a few real-world repos under test.

---

If you want, I can also write the **exact parquet partitioning conventions** I’d use for these new datasets (columns, directory layout, delete+rewrite semantics) so the partition-upsert part stays uniform with your PR16 state store and doesn’t introduce file-system-level footguns.
