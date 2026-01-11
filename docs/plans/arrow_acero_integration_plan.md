# Implementation Plan: Arrow/Acero Integration and Columnar Compute Consolidation

This plan integrates the Arrow/Acero DSL guidance with the repo findings, targeting a more
columnar, streaming-first pipeline and shared compute utilities. Decisions locked in:
canonical ordering only at finalize, error context captured as list<struct> in a unioned
errors dataset while good outputs drop bad rows, and vectorized IDs via Arrow hash64
(stable per-version + repo). Note: PyArrow 21 does not expose a built-in hash64 scalar
kernel, so we use a deterministic hash64 UDF in `arrowdsl.ids`. Each scope item includes
updated code patterns, target files, and an integration checklist with progress.

## Scope 1: Vectorized Stable IDs (Arrow Hash Kernels)

### Description
Replace row-wise SHA-1 ID generation with deterministic Arrow hash64 kernels. Prefer int64
IDs for joins and internal edges, and optionally derive string IDs for debug output only.

### Code patterns
```python
# src/arrowdsl/ids.py (hash64 UDF-backed)
import pyarrow as pa
import pyarrow.compute as pc

from arrowdsl.ids import (
    Hash64ColumnSpec,
    add_hash64_column,
    hash64_from_arrays,
    hash64_from_columns,
)


spec = Hash64ColumnSpec(cols=("path", "bstart", "bend"), out_col="span_id", as_string=True)
table = add_hash64_column(table, spec=spec)
hashes = hash64_from_columns(table, cols=["path", "bstart", "bend"], prefix="span")
span_id = pc.binary_join_element_wise(pa.scalar("span"), pc.cast(hashes, pa.string()), ":")
```

```python
# Example: repo file_id vectorized in repo_scan
hash_arrays = [pa.array([repo_id] * n, type=pa.string()), table["path"]]
file_hash = hash64_from_arrays(hash_arrays, prefix="file")
file_id = pc.binary_join_element_wise(pa.scalar("file"), pc.cast(file_hash, pa.string()), ":")
```

### Target files
- `src/normalize/ids.py` (add vectorized ID helpers)
- `src/extract/repo_scan.py` (replace `stable_id` file_id)
- `src/cpg/build_edges.py` (edge_id generation for join-friendly keys)
- `src/cpg/build_nodes.py` (node_id generation for anchored nodes)
- `src/normalize/spans.py` (optional vectorized span_id for joins)
- `src/normalize/diagnostics.py` (diag_id)
- `src/normalize/bytecode_dfg.py` (event_id)

### Integration checklist
### Implementation status
- [x] Introduced `arrowdsl/ids.py` hash64 UDF helpers and specs.
- [x] Vectorized `file_id` in `src/extract/repo_scan.py`.
- [x] Vectorized edge IDs in `src/cpg/build_edges.py`.
- [x] Vectorized `span_id` generation in `src/normalize/ids.py`.
- [x] Vectorized `diag_id` and `df_event` IDs in `src/normalize/diagnostics.py` and
  `src/normalize/bytecode_dfg.py`.
- [ ] Migrate remaining ID loops (e.g., `src/cpg/build_nodes.py`, `src/normalize/spans.py`).
- [ ] Decide whether any outputs should switch to int64 IDs (vs prefixed strings).

---

## Scope 2: Streaming Relspec Execution (Plan-Lane Union, Single Materialization)

### Description
Keep relspec execution in the Acero plan lane as long as possible, materializing once per
output. Use plan unions where supported, and only fall back to kernel-lane operations when
Acero cannot express the transform. This aligns with push-based ExecPlans and avoids
per-rule `to_table()` materialization.

### Code patterns
```python
# src/arrowdsl/plan.py (new helper)
from pyarrow import acero


def union_all_plans(plans: list[Plan], *, label: str = "") -> Plan:
    decls = [p.decl for p in plans if p.decl is not None]
    if len(decls) != len(plans):
        raise ValueError("All plans must be Acero-backed for union.")
    decl = acero.Declaration("union", None, inputs=decls)
    return Plan(decl=decl, label=label, ordering=Ordering.unordered())
```

```python
# src/relspec/compiler.py (execute output)
# Build plan union -> materialize once -> finalize (rule meta projected in plan-lane)
union = union_all_plans(rule_plans, label=output_name)
out = union.to_table(ctx=ctx)
```

### Target files
- `src/arrowdsl/plan.py` (add union helper + ordering metadata)
- `src/relspec/compiler.py` (compile outputs via plan union; avoid per-rule materialization)
- `src/arrowdsl/runtime.py` (optional: record pipeline breakers in Ordering metadata)

### Implementation status
- [x] Added `union_all_plans` and `Plan.aggregate` in `src/arrowdsl/plan.py`.
- [x] Updated relspec compiler to union plan-lane rules and materialize once when no kernels.
- [x] Preserved `rule_name`/`rule_priority` in plan-lane via projection.
- [ ] Expand plan-lane handling for rules with lightweight post-kernels (where feasible).
- [ ] Confirm ordering metadata after union/aggregate for any new plan-lane ops.

---

## Scope 3: Finalize-Only Canonical Ordering Policy

### Description
Canonical ordering is enforced only at finalize. Normalization outputs remain unordered to
avoid pipeline breakers, and any test or persistence that requires determinism should use
finalized outputs (or explicitly sort at the test boundary).

### Code patterns
```python
# src/arrowdsl/finalize.py
good = canonical_sort_if_canonical(good, sort_keys=contract.canonical_sort, ctx=ctx)
```

### Target files
- `src/arrowdsl/finalize.py`
- `src/normalize/bytecode_cfg.py` (remove canonical sort if present)

### Implementation status
- [x] Removed normalization canonical sort in `src/normalize/bytecode_cfg.py`.
- [x] Finalize remains the only canonical ordering boundary.
- [ ] Update any tests that assume row order in normalization outputs.

---

## Scope 4: Shared Schema Alignment and Casting Utilities

### Description
Consolidate schema alignment/casting logic into a shared module and reuse it in finalize and
normalization. This eliminates drift between `arrowdsl.finalize` and `normalize.schema_infer`.

### Code patterns
```python
# src/arrowdsl/schema.py
import pyarrow as pa
import pyarrow.compute as pc


# Example usage (normalize.schema_infer)
aligned, info = align_to_schema(
    table,
    schema=schema,
    safe_cast=opts.safe_cast,
    on_error="keep",
    keep_extra_columns=opts.keep_extra_columns,
)
```

### Target files
- `src/arrowdsl/finalize.py` (use shared aligner)
- `src/normalize/schema_infer.py` (reuse shared aligner or wrap it)
- `src/arrowdsl/schema.py` (new shared module)

### Implementation status
- [x] Added shared aligner in `src/arrowdsl/schema.py`.
- [x] Finalize and `normalize.schema_infer` use the shared aligner.
- [x] Safe cast and error policy options are centralized.

---

## Scope 5: Columnar Transform Helpers (Reduce `to_pylist` Loops)

### Description
Replace Python loops over `to_pylist()` with Arrow compute or Acero projections where
possible. Centralize column append/default/coalesce operations and reuse in all
normalization and CPG builders.

### Code patterns
```python
# src/arrowdsl/columns.py
import pyarrow as pa
import pyarrow.compute as pc


def add_const_column(table: pa.Table, name: str, value: object) -> pa.Table:
    scalar = pa.scalar(value)
    return table.append_column(name, pa.array([value] * table.num_rows, type=scalar.type))


def coalesce_string(table: pa.Table, cols: list[str], out: str) -> pa.Table:
    arr = pc.cast(table[cols[0]], pa.string())
    for col in cols[1:]:
        arr = pc.coalesce(arr, pc.cast(table[col], pa.string()))
    return table.append_column(out, arr)
```

### Target files
- `src/cpg/build_edges.py`
- `src/cpg/build_props.py`
- `src/normalize/diagnostics.py`
- `src/normalize/types.py`
- `src/normalize/bytecode_dfg.py`
- `src/normalize/bytecode_anchor.py`

### Implementation status
- [x] Added shared helpers in `src/arrowdsl/columns.py`.
- [x] Converted diagnostics/def-use to columnar compute (`src/normalize/diagnostics.py`,
  `src/normalize/bytecode_dfg.py`).
- [ ] Continue replacing `to_pylist()` in `src/cpg/build_edges.py`, `src/cpg/build_props.py`,
  and `src/normalize/bytecode_anchor.py` where practical.
- [ ] Validate no behavior drift in output schemas.

---

## Scope 6: Expand Plan DSL for Aggregation + Ordering Semantics

### Description
Add plan-lane aggregation and explicit ordering semantics so that more work stays in Acero,
and ordering guarantees are encoded in the DSL. Mark pipeline breakers (aggregate/order_by)
explicitly in ordering metadata.

### Code patterns
```python
# src/arrowdsl/plan.py
from pyarrow import acero


def aggregate(self, group_keys: list[str], aggs: list[tuple[str, str]], *, label: str = "") -> Plan:
    if self.decl is None:
        raise TypeError("aggregate() requires an Acero-backed Plan.")
    agg_specs = [(col, fn, None, f"{col}_{fn}") for col, fn in aggs]
    decl = acero.Declaration(
        "aggregate",
        acero.AggregateNodeOptions(agg_specs, keys=group_keys or None),
        inputs=[self.decl],
    )
    return Plan(decl=decl, label=label or self.label, ordering=Ordering.unordered())
```

### Target files
- `src/arrowdsl/plan.py`
- `src/arrowdsl/runtime.py` (optional: ordering metadata/pipeline breaker flags)
- `src/relspec/compiler.py` (use plan-lane aggregation when possible)

### Implementation status
- [x] Added aggregate helper with ordering metadata in `src/arrowdsl/plan.py`.
- [x] Union helper marks output unordered.
- [ ] Update relspec compiler to use plan-lane aggregation where appropriate.

---

## Scope 7: Shared Empty Table Utility

### Description
Replace repeated `pa.Table.from_arrays` empty table constructs with a shared helper to ensure
consistent schema handling and metadata.

### Code patterns
```python
# src/arrowdsl/empty.py
import pyarrow as pa


def empty_table(schema: pa.Schema) -> pa.Table:
    return pa.Table.from_arrays([pa.array([], type=f.type) for f in schema], schema=schema)
```

### Target files
- `src/extract/runtime_inspect_extract.py`
- `src/extract/tree_sitter_extract.py`
- `src/normalize/bytecode_cfg.py`
- `src/normalize/types.py`
- `src/cpg/build_props.py`

### Implementation status
- [x] Added `empty_table` helper in `src/arrowdsl/empty.py`.
- [x] Replaced local helpers in listed targets.
- [ ] Review other `_empty` helpers (e.g., `src/extract/cst_extract.py`,
  `src/extract/scip_extract.py`) for optional consolidation.

---

## Scope 8: Provenance Columns for Ordering and Determinism

### Description
Use Arrow scan provenance columns (`__filename`, fragment/batch indices) where available to
build tie-breakers for canonical ordering in deterministic outputs.

### Code patterns
```python
# src/arrowdsl/queryspec.py
if provenance:
    cols.update({
        "prov_filename": pc.field("__filename"),
        "prov_fragment_index": pc.field("__fragment_index"),
        "prov_batch_index": pc.field("__batch_index"),
        "prov_last_in_fragment": pc.field("__last_in_fragment"),
    })
```

```python
# src/arrowdsl/kernels.py
# canonical_sort_if_canonical appends provenance columns when present
```

### Target files
- `src/arrowdsl/queryspec.py` (already emits provenance columns)
- `src/arrowdsl/kernels.py` (canonical sort keys optionally include provenance columns)
- `src/relspec/compiler.py` (canonical sort tie-breakers when determinism is canonical)

### Implementation status
- [x] Provenance columns are emitted in `src/arrowdsl/queryspec.py`.
- [x] `canonical_sort_if_canonical` appends provenance columns when present.
- [ ] Decide which outputs require provenance tie-breakers beyond the default append.
- [ ] Verify scan profiles set `implicit_ordering` when required.

---

## Scope 9: Acero Scan/Project/Filter Consolidation

### Description
Ensure scan/project/filter steps are centralized and reused (no ad hoc compute outside
Acero when a scan plan can do it). This reduces duplication and keeps predicate pushdown
consistent.

### Code patterns
```python
# src/arrowdsl/dataset_io.py
scan_opts = acero.ScanNodeOptions(
    dataset,
    columns=spec.scan_columns(provenance=ctx.provenance),
    filter=spec.pushdown_predicate,
    **ctx.runtime.scan.scan_node_kwargs(),
)
```

### Target files
- `src/arrowdsl/dataset_io.py`
- `src/relspec/compiler.py`
- `src/arrowdsl/queryspec.py`

### Implementation status
- [x] `compile_to_acero_scan` centralizes scan/project/filter.
- [x] QuerySpec projection/predicate wiring is consistent.
- [ ] Audit any ad hoc scans outside the DSL (if new ones are added).

---

## Scope 10: Alignment with Determinism and Dedupe Policies

### Description
Use the DSL’s determinism tier and dedupe specs consistently across finalize and rule
outputs. Canonical sort and dedupe should be the standard policy gates, not ad hoc
per-module decisions.

### Code patterns
```python
# src/arrowdsl/finalize.py
if contract.dedupe is not None:
    good = apply_dedupe(good, spec=contract.dedupe, _ctx=ctx)

good = canonical_sort_if_canonical(good, sort_keys=contract.canonical_sort, ctx=ctx)
```

### Target files
- `src/arrowdsl/finalize.py`
- `src/arrowdsl/kernels.py`
- `src/relspec/compiler.py` (post-kernel dedupe if needed)

### Implementation status
- [x] Finalize applies dedupe and canonical sort gates.
- [x] Normalization canonical sorts removed where present.
- [ ] Consider deprecating `CanonicalSortKernelSpec` usage in relspec configs (if any).

---

## Scope 11: Finalized Error Context as list<struct> (Drop Bad Rows)

### Description
Keep the final output dataset clean (bad rows dropped), and capture per-row error context in
a separate finalized errors dataset using list<struct> details. This preserves error
observability without materializing per-rule outputs.

### Error detail struct layout
```
error_detail: list<struct<
  code: string,
  message: string,
  column: string,
  severity: string,
  rule_name: string,
  rule_priority: int32,
  source: string
>>
```

### Code patterns
```python
# src/arrowdsl/finalize.py
# Build a row_id for error grouping (prefer contract key fields when available)
row_id = hash64_from_columns(errors, cols=key_cols, prefix=f"{contract.name}:row", missing="null")

# Per-error instances
detail = pa.StructArray.from_arrays(
    [
        errors["error_code"],
        errors["error_message"],
        errors["error_column"],
        errors["error_severity"],
        errors["error_rule_name"],
        errors["error_rule_priority"],
        errors["error_source"],
    ],
    names=["code", "message", "column", "severity", "rule_name", "rule_priority", "source"],
)
errors = errors.append_column("error_detail", detail)

# Aggregate to list<struct> per row_id
errors_detailed = errors.group_by(["row_id", *key_cols]).aggregate([("error_detail", "list")])
```

### Target files
- `src/arrowdsl/finalize.py` (aggregate error details into list<struct>)
- `src/arrowdsl/contracts.py` (optional: declare error_detail schema for outputs)
- `src/arrowdsl/kernels.py` (optional: helper to build row_id and list<struct> aggregation)

### Implementation status
- [x] `row_id` computed via `hash64_from_columns` over contract key fields.
- [x] `error_detail` aggregated to list<struct> per row in finalize.
- [x] Good rows drop bad rows; errors emitted separately.
- [ ] Add tests for `error_detail` schema and aggregation behavior.

---

## Scope 12: PyArrow Typing Stubs (Pyrefly/Pyright Support)

### Description
Keep the local PyArrow type stubs aligned with the APIs used by Arrow 21 to prevent
type-checker false positives (compute kernels, Acero declarations, Table APIs).

### Target files
- `typings/pyarrow/__init__.pyi`
- `typings/pyarrow/compute.pyi`
- `typings/pyarrow/acero.pyi` (new)

### Implementation status
- [x] Updated compute and core stubs for kernels used in Arrow DSL utilities.
- [x] Added Acero stubs for plan helpers and declarations.
- [ ] Revisit stubs if PyArrow APIs change.

---

## Global Verification Steps
- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.14`
- Add or update targeted tests for:
  - Union/streaming relspec outputs
  - Canonical ordering with determinism tiers
  - Vectorized ID stability
  - Error details list<struct> aggregation
