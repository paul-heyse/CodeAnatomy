# Implementation Plan: Zero `to_pylist()` + Nested Array Builders

This plan removes all remaining `to_pylist()` usage and standardizes on Arrow-native
iteration and nested-array construction patterns. It introduces a shared iterator helper,
adopts list/struct builders for nested columns, and applies those patterns in extraction
and normalization to avoid future Python list materialization.

---

## Scope 1: Shared Iterator Helper + Final `to_pylist()` Removal

Status: Complete (guardrail deferred)

### Description
Create a single Arrow-centric iterator helper and replace the last two `to_pylist()` sites
with iterator-based logic. The helper also becomes the canonical way to iterate Arrow
arrays/tables across the codebase.

### Code patterns
```python
# src/arrowdsl/iter.py
from __future__ import annotations

from collections.abc import Iterator, Sequence

import pyarrow as pa

type ArrayLike = pa.Array | pa.ChunkedArray


def iter_array_values(array: ArrayLike) -> Iterator[object | None]:
    for value in array:
        if isinstance(value, pa.Scalar):
            yield value.as_py()
        else:
            yield value


def iter_arrays(arrays: Sequence[ArrayLike]) -> Iterator[tuple[object | None, ...]]:
    iters = [iter_array_values(array) for array in arrays]
    yield from zip(*iters, strict=True)


def iter_table_rows(table: pa.Table) -> Iterator[dict[str, object]]:
    columns = list(table.column_names)
    arrays = [table[col] for col in columns]
    iters = [iter_array_values(array) for array in arrays]
    for values in zip(*iters, strict=True):
        yield dict(zip(columns, values, strict=True))
```

```python
# src/arrowdsl/finalize.py (strict error message)
from arrowdsl.iter import iter_array_values

values = vc.field("values")
counts = vc.field("counts")
pairs = [
    (v, c)
    for v, c in zip(iter_array_values(values), iter_array_values(counts), strict=True)
]
msg = f"Finalize(strict) failed for contract={contract.name!r}: errors={pairs}"
```

```python
# src/arrowdsl/ids.py (hash64 UDF without to_pylist)
from arrowdsl.iter import iter_array_values

out = [_hash64_int(str(v)) if v is not None else None for v in iter_array_values(array)]
return pa.array(out, type=pa.int64())
```

### Target files
- `src/arrowdsl/iter.py` (new helper)
- `src/arrowdsl/ids.py` (hash64 UDF)
- `src/arrowdsl/finalize.py` (strict error summary)
- Replace local `_iter_array_values`/`_iter_arrays` helpers with the shared helper in:
  - `src/normalize/diagnostics.py`
  - `src/normalize/types.py`
  - `src/normalize/spans.py`
  - `src/normalize/bytecode_dfg.py`
  - `src/normalize/bytecode_anchor.py`
  - `src/cpg/build_nodes.py`
  - `src/cpg/build_props.py`
  - `src/extract/ast_extract.py`
  - `src/extract/bytecode_extract.py`
  - `src/extract/cst_extract.py`
  - `src/extract/scip_extract.py`
  - `src/extract/symtable_extract.py`
  - `src/extract/tree_sitter_extract.py`
  - `src/extract/runtime_inspect_extract.py`

### Implementation checklist
- [x] Add `src/arrowdsl/iter.py` with `iter_array_values`, `iter_arrays`, `iter_table_rows`.
- [x] Replace `to_pylist()` usage in `src/arrowdsl/ids.py` and `src/arrowdsl/finalize.py`.
- [x] Swap local iterator helpers to import from `arrowdsl.iter`.
- [x] Validate no regressions in strict-mode error messages.
- [ ] Add a lightweight CI check (`rg "to_pylist" src/`) to prevent reintroduction (optional).

---

## Scope 2: Nested Array Builder Utilities

Status: Complete (diagnostics list<struct> deferred)

### Description
Provide reusable builder utilities for list/struct arrays so nested columns are created
without Python list materialization. This uses `ListArray.from_arrays` and
`StructArray.from_arrays` (and optionally `ListViewArray` when offsets/sizes are
precomputed).

### Code patterns
```python
# src/arrowdsl/nested.py
from __future__ import annotations

import pyarrow as pa


def build_struct_array(
    fields: dict[str, pa.Array],
    *,
    mask: pa.Array | None = None,
) -> pa.StructArray:
    names = list(fields.keys())
    arrays = [fields[name] for name in names]
    return pa.StructArray.from_arrays(arrays, names=names, mask=mask)


def build_list_array(
    offsets: pa.Array,
    values: pa.Array,
) -> pa.ListArray:
    return pa.ListArray.from_arrays(offsets, values)


def build_list_of_structs(
    offsets: pa.Array,
    struct_fields: dict[str, pa.Array],
) -> pa.ListArray:
    struct_arr = build_struct_array(struct_fields)
    return build_list_array(offsets, struct_arr)
```

```python
# Example: list<struct{name, source}>
offsets = pa.array([0, 2, 2, 3], type=pa.int32())
names = pa.array(["a", "b", "c"], type=pa.string())
sources = pa.array(["x", "y", "z"], type=pa.string())
qnames = build_list_of_structs(offsets, {"name": names, "source": sources})
```

### Target files
- `src/arrowdsl/nested.py` (new utilities)
- `typings/pyarrow/__init__.pyi` (stubs for `ListArray.from_arrays` + struct mask)
- `src/extract/cst_extract.py` (qnames, callee_qnames list<struct>)
- `src/extract/scip_extract.py` (tags, documentation arrays, override documentation)
- `src/normalize/diagnostics.py` (details list<struct> when aggregating error detail rows; deferred)
- `src/hamilton_pipeline/modules/normalization.py` (list explode paths use Arrow list arrays)
- `src/cpg/build_props.py` (list<struct> qname props)

### Implementation checklist
- [x] Add `src/arrowdsl/nested.py` with list/struct builders.
- [x] Create offsets/values arrays while extracting (no Python list-of-dicts).
- [x] Replace list-of-dicts fields with `ListArray`/`StructArray` outputs.
- [x] Validate schemas match existing contracts (list<struct> field names/types).
- [ ] Migrate diagnostics `details_json` to list<struct> if/when that schema change is desired.

---

## Scope 3: Apply Nested Builders in Extraction (CST/SCIP)

Status: Complete

### Description
Refactor extraction to emit list/struct arrays using offsets + values buffers. This keeps
the extraction loop Pythonic but prevents nested Python lists from being embedded in
tables.

### Code patterns
```python
# Build offsets while iterating extraction rows
offsets = [0]
names_flat: list[str] = []
sources_flat: list[str] = []

for qnames in qualified_name_sets:
    for q in qnames:
        names_flat.append(q.name)
        sources_flat.append(str(q.source))
    offsets.append(len(names_flat))

offsets_arr = pa.array(offsets, type=pa.int32())
names_arr = pa.array(names_flat, type=pa.string())
sources_arr = pa.array(sources_flat, type=pa.string())
qnames_list = build_list_of_structs(offsets_arr, {"name": names_arr, "source": sources_arr})
```

### Target files
- `src/extract/cst_extract.py` (qnames / callee_qnames)
- `src/extract/scip_extract.py` (tags, documentation, override docs lists, signature docs)

### Implementation checklist
- [x] Track per-row offsets in extraction buffers.
- [x] Replace list-of-dicts fields with list<struct> arrays.
- [x] Ensure null/empty list semantics match current outputs.
- [ ] Add focused tests for list/struct schema equality.

---

## Scope 4: List Explode and Join Patterns (No List Materialization)

Status: Complete

### Description
Standardize list explode patterns using Arrow kernels (`pc.list_flatten`,
`pc.list_parent_indices`) so downstream joins never materialize nested lists in Python.

### Code patterns
```python
# explode list column with parent id
parent_ids = table["call_id"]
dst_lists = table["callee_qnames"]
parent_idx = pc.list_parent_indices(dst_lists)
dst_flat = pc.list_flatten(dst_lists)
parent_rep = pc.take(parent_ids, parent_idx)
exploded = pa.Table.from_arrays([parent_rep, dst_flat], names=["call_id", "qname"])
```

### Target files
- `src/arrowdsl/kernels.py` (explode_list_column already uses Arrow kernels)
- `src/hamilton_pipeline/modules/normalization.py` (callsite candidates)

### Implementation checklist
- [x] Ensure list<struct> outputs are compatible with `pc.list_flatten`.
- [x] Use `explode_list_column` whenever possible.
- [x] Remove any ad hoc list expansion in normalization modules.

---

## Scope 5: Guardrails + Migration Verification

Status: Not started (optional)

### Description
Ensure the codebase does not regress into `to_pylist()` and list materialization. Provide
one-time migration checks and lightweight CI guardrails.

### Code patterns
```bash
rg -n "to_pylist" src
```

### Target files
- `pyproject.toml` (optional: add a custom lint check via scripts)
- `scripts/` (optional: guardrail script)

### Implementation checklist
- [ ] Add a pre-commit/CI check that fails on `to_pylist` in `src/`.
- [ ] Document the preferred list/struct builder usage in `AGENTS.md`.
- [ ] Run `uv run ruff check --fix`, `uv run pyrefly check`, `uv run pyright --warnings --pythonversion=3.14`.
