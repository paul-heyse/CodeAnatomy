## Normalize Dedup + Acero/DSL Implementation Plan (src/normalize)

### Goals
- Maximize shared helpers/utilities across normalize modules.
- Shift row-wise Python loops into vectorized Arrow compute or Acero plans.
- Centralize determinism and dedupe in a canonical finalize boundary.
- Keep schema/spec definitions stable and reusable across normalizers.

---

### Scope 1: Shared Arrow column helpers
**Description**
Unify `column_or_null` and null-array creation into a single helper to remove duplication across
normalize modules.

**Code pattern**
```python
# src/normalize/arrow_utils.py
from arrowdsl.core.interop import ArrayLike, ChunkedArrayLike, DataTypeLike, TableLike
import pyarrow as pa

def column_or_null(
    table: TableLike,
    col: str,
    dtype: DataTypeLike,
) -> ArrayLike | ChunkedArrayLike:
    if col in table.column_names:
        return table[col]
    return pa.nulls(table.num_rows, type=dtype)
```

**Target files**
- New: `src/normalize/arrow_utils.py`
- Update: `src/normalize/spans.py`
- Update: `src/normalize/diagnostics.py`
- Update: `src/normalize/bytecode_anchor.py`
- Update: `src/normalize/bytecode_dfg.py`

**Implementation checklist**
- [ ] Replace local `_column_or_null` functions with `arrow_utils.column_or_null`.
- [ ] Ensure all callers pass explicit `dtype`.
- [ ] Remove now-unused local helpers.

---

### Scope 2: Text index + row coercion utilities
**Description**
Centralize `_row_value_int`, `_file_index`, and position-encoding normalization into a shared
module to remove drift between byte-span code paths.

**Code pattern**
```python
# src/normalize/text_index.py
from __future__ import annotations

from dataclasses import dataclass
import pyarrow as pa

from arrowdsl.core.interop import ArrayLike, TableLike

@dataclass(frozen=True)
class FileTextIndex:
    file_id: str
    path: str
    file_sha256: str | None
    encoding: str | None
    text: str
    lines: list[str]
    line_start_utf8: list[int]

@dataclass(frozen=True)
class RepoTextIndex:
    by_file_id: dict[str, FileTextIndex]
    by_path: dict[str, FileTextIndex]

def row_value_int(value: object | None) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        raw = value.strip()
        return int(raw) if raw.isdigit() else None
    return None

def file_index(repo_index: RepoTextIndex, file_id: object | None, path: object | None) -> FileTextIndex | None:
    if isinstance(file_id, str):
        return repo_index.by_file_id.get(file_id)
    if isinstance(path, str):
        return repo_index.by_path.get(path)
    return None
```

**Target files**
- New: `src/normalize/text_index.py`
- Update: `src/normalize/spans.py`
- Update: `src/normalize/diagnostics.py`
- Update: `src/normalize/bytecode_anchor.py`

**Implementation checklist**
- [ ] Move `FileTextIndex` and `RepoTextIndex` into `text_index.py`.
- [ ] Re-export from `src/normalize/spans.py` for backward compatibility.
- [ ] Replace local `_row_value_int` and `_file_index` with shared helpers.
- [ ] Move `normalize_position_encoding` into `text_index.py` and reuse in diagnostics/spans.

---

### Scope 3: Hash ID helper unification
**Description**
Standardize prefixed hash generation and masking into `normalize.ids` to avoid duplicate wrappers.

**Code pattern**
```python
# src/normalize/ids.py
from collections.abc import Sequence
import pyarrow as pa

from arrowdsl.core.ids import prefixed_hash_id
from arrowdsl.core.interop import ArrayLike, ChunkedArrayLike, pc

def prefixed_hash64(
    prefix: str,
    arrays: Sequence[ArrayLike | ChunkedArrayLike],
) -> ArrayLike | ChunkedArrayLike:
    return prefixed_hash_id(list(arrays), prefix=prefix)

def masked_prefixed_hash(
    prefix: str,
    arrays: Sequence[ArrayLike | ChunkedArrayLike],
    *,
    required: Sequence[ArrayLike | ChunkedArrayLike],
) -> ArrayLike | ChunkedArrayLike:
    hashed = prefixed_hash_id(list(arrays), prefix=prefix)
    mask = pc.is_valid(required[0])
    for arr in required[1:]:
        mask = pc.and_(mask, pc.is_valid(arr))
    return pc.if_else(mask, hashed, pa.scalar(None, type=pa.string()))
```

**Target files**
- Update: `src/normalize/ids.py`
- Update: `src/normalize/types.py`
- Update: `src/normalize/diagnostics.py`
- Update: `src/normalize/bytecode_dfg.py`

**Implementation checklist**
- [ ] Replace local `_prefixed_hash64` wrappers with `ids.prefixed_hash64`.
- [ ] Use `masked_prefixed_hash` where validity masks are required.
- [ ] Remove local duplicate hash wrappers.

---

### Scope 4: Normalize schema/spec boilerplate
**Description**
Move schema and dataset spec definitions into a shared `normalize/schemas.py` module to reduce
boilerplate and make schema reuse explicit.

**Code pattern**
```python
# src/normalize/schemas.py
from schema_spec.system import GLOBAL_SCHEMA_REGISTRY, make_dataset_spec, make_table_spec
from schema_spec.specs import ArrowFieldSpec, file_identity_bundle, span_bundle
import pyarrow as pa

SCHEMA_VERSION = 1

TYPE_EXPRS_NORM_SPEC = GLOBAL_SCHEMA_REGISTRY.register_dataset(
    make_dataset_spec(
        table_spec=make_table_spec(
            name="type_exprs_norm_v1",
            version=SCHEMA_VERSION,
            bundles=(file_identity_bundle(include_sha256=False), span_bundle()),
            fields=[
                ArrowFieldSpec(name="type_expr_id", dtype=pa.string()),
            ],
        )
    )
)
TYPE_EXPRS_NORM_SCHEMA = TYPE_EXPRS_NORM_SPEC.table_spec.to_arrow_schema()
```

**Target files**
- New: `src/normalize/schemas.py`
- Update: `src/normalize/types.py`
- Update: `src/normalize/diagnostics.py`
- Update: `src/normalize/bytecode_cfg.py`
- Update: `src/normalize/bytecode_dfg.py`
- Update: `src/normalize/spans.py`

**Implementation checklist**
- [ ] Move all `*_SPEC` and `*_SCHEMA` definitions into `schemas.py`.
- [ ] Keep public schema constants re-exported via `src/normalize/__init__.py` if needed.
- [ ] Ensure schema version constants stay centralized.

---

### Scope 5: Acero plan wrapper + finalize boundary
**Description**
Create a minimal plan runner that defaults to Acero `Declaration` and centralizes determinism,
canonical sort, and dedupe in a finalize step.

**Code pattern**
```python
# src/normalize/pipeline.py
from __future__ import annotations

from collections.abc import Sequence
import pyarrow as pa
import pyarrow.acero as acero
import pyarrow.compute as pc

def canonical_sort(table: pa.Table, sort_keys: Sequence[tuple[str, str]]) -> pa.Table:
    indices = pc.sort_indices(table, sort_keys=sort_keys)
    return table.take(indices)

def run_plan(decl: acero.Declaration, *, use_threads: bool) -> pa.Table:
    return decl.to_table(use_threads=use_threads)
```

**Target files**
- New: `src/normalize/pipeline.py`
- Update: `src/normalize/bytecode_dfg.py` (to route joins through plan runner)
- Update: `src/normalize/types.py` (for canonical sort and dedupe helpers)

**Implementation checklist**
- [ ] Introduce canonical sort helper and reuse in normalize outputs that require determinism.
- [ ] Ensure any dedupe policy is executed after a canonical sort.
- [ ] Keep Acero plans as the default for joins and projections.

---

### Scope 6: Vectorized filter/projection helpers
**Description**
Replace row-wise loops with Arrow compute filters and projections where the transform is
row-count preserving.

**Code pattern**
```python
# src/normalize/arrow_utils.py
import pyarrow.compute as pc
import pyarrow as pa

def filter_non_empty_utf8(table: pa.Table, column: str) -> pa.Table:
    trimmed = pc.utf8_trim_whitespace(table[column])
    ok = pc.and_(
        pc.is_valid(trimmed),
        pc.greater(pc.utf8_length(trimmed), 0),
    )
    return table.filter(ok)
```

**Target files**
- Update: `src/normalize/types.py`
- Update: `src/normalize/diagnostics.py`
- Update: `src/normalize/spans.py`

**Implementation checklist**
- [ ] Replace Python loops used only for filtering with `Table.filter`.
- [ ] Use `pc.*` expressions for computed columns instead of list buffers when possible.
- [ ] Preserve existing null-handling semantics.

---

### Scope 7: Rewrite `run_reaching_defs` with Acero join
**Description**
Compute defs/uses tables, join on `(code_unit_id, symbol)`, and build edge IDs using vectorized
hashing for deterministic, scalable output.

**Code pattern**
```python
# src/normalize/bytecode_dfg.py
from arrowdsl.plan.ops import JoinSpec
from arrowdsl.compute.kernels import apply_join
from normalize.ids import prefixed_hash64

def build_reaches(defs: TableLike, uses: TableLike) -> TableLike:
    joined = apply_join(
        defs,
        uses,
        spec=JoinSpec(
            join_type="inner",
            left_keys=("code_unit_id", "symbol"),
            right_keys=("code_unit_id", "symbol"),
            left_output=("code_unit_id", "symbol", "event_id"),
            right_output=("event_id", "path", "file_id"),
        ),
        use_threads=True,
    )
    edge_id = prefixed_hash64("df_reach", [joined["event_id"], joined["event_id_right"]])
    return joined.append_column("edge_id", edge_id)
```

**Target files**
- Update: `src/normalize/bytecode_dfg.py`
- Update: `src/normalize/pipeline.py` (canonical sort helper for deterministic outputs)

**Implementation checklist**
- [ ] Split def/use events into two tables.
- [ ] Join using `apply_join` (Acero) instead of Python loops.
- [ ] Compute `edge_id` via shared hash helper.
- [ ] Canonical-sort final table when determinism is required.

---

### Scope 8: Vectorize `normalize_type_exprs` and `normalize_types`
**Description**
Build normalized type expressions with compute kernels and dedupe types via `group_by` or
`Table.unique` with canonical sort.

**Code pattern**
```python
# src/normalize/types.py
import pyarrow.compute as pc

def normalize_type_exprs(table: TableLike) -> TableLike:
    trimmed = pc.utf8_trim_whitespace(table["expr_text"])
    ok = pc.and_(
        pc.is_valid(trimmed),
        pc.greater(pc.utf8_length(trimmed), 0),
    )
    filtered = table.filter(ok)
    type_repr = trimmed.filter(ok)
    type_id = prefixed_hash64("type", [type_repr])
    return filtered.append_column("type_repr", type_repr).append_column("type_id", type_id)
```

**Target files**
- Update: `src/normalize/types.py`
- Update: `src/normalize/arrow_utils.py` (filter helper)

**Implementation checklist**
- [ ] Replace list buffer accumulation with compute filters and appended columns.
- [ ] Use canonical sort before any order-dependent dedupe.
- [ ] For `normalize_types`, replace set-based dedupe with `group_by` or `unique`.

---

### Scope 9: Extract `join_code_unit_meta` helper
**Description**
Standardize the "join code unit metadata" pattern across CFG blocks/edges and any future
bytecode normalizers.

**Code pattern**
```python
# src/normalize/arrow_utils.py
from arrowdsl.compute.kernels import apply_join
from arrowdsl.plan.ops import JoinSpec

def join_code_unit_meta(table: TableLike, code_units: TableLike) -> TableLike:
    meta_cols = [col for col in ("code_unit_id", "file_id", "path") if col in code_units.column_names]
    meta = code_units.select(meta_cols)
    right_cols = tuple(col for col in meta_cols if col != "code_unit_id")
    return apply_join(
        table,
        meta,
        spec=JoinSpec(
            join_type="left outer",
            left_keys=("code_unit_id",),
            right_keys=("code_unit_id",),
            left_output=tuple(table.column_names),
            right_output=right_cols,
        ),
        use_threads=True,
    )
```

**Target files**
- Update: `src/normalize/arrow_utils.py`
- Update: `src/normalize/bytecode_cfg.py`
- Update: `src/normalize/bytecode_dfg.py` (if metadata join needed in the future)

**Implementation checklist**
- [ ] Replace inline metadata joins with `join_code_unit_meta`.
- [ ] Keep join keys and output ordering stable across callers.
- [ ] Add minimal validation (skip join if key missing).

---

### Scope 10: Unify span pipeline + diagnostics helpers
**Description**
Refactor span conversion and error table construction into shared helpers so CST/AST/SCIP
normalization uses a single pipeline.

**Code pattern**
```python
# src/normalize/span_pipeline.py
from arrowdsl.schema.arrays import set_or_append_column
import pyarrow as pa

def append_span_columns(
    table: TableLike,
    *,
    bstart: list[int | None],
    bend: list[int | None],
    ok: list[bool],
) -> TableLike:
    out = set_or_append_column(table, "bstart", pa.array(bstart, type=pa.int64()))
    out = set_or_append_column(out, "bend", pa.array(bend, type=pa.int64()))
    return set_or_append_column(out, "span_ok", pa.array(ok, type=pa.bool_()))
```

**Target files**
- New: `src/normalize/span_pipeline.py`
- Update: `src/normalize/spans.py`
- Update: `src/normalize/diagnostics.py`

**Implementation checklist**
- [ ] Move `_span_error_table` and alias helpers into `span_pipeline.py`.
- [ ] Use shared append helpers for AST, SCIP, and CST spans.
- [ ] Keep span error schema in `normalize/schemas.py`.

---

### Scope 11: Documentation and migration notes
**Description**
Capture the API moves and deprecations so downstream modules can migrate smoothly.

**Code pattern**
```text
# docs/plans/normalize_dedup_acero_plan.md
- Document old helper -> new helper mappings.
- Note any re-exports preserved for backward compatibility.
```

**Target files**
- Update: `docs/plans/normalize_dedup_acero_plan.md`
- Update: `src/normalize/__init__.py` (re-exports)

**Implementation checklist**
- [ ] Add a short migration section listing renamed helpers.
- [ ] Keep backwards-compatible imports for one release cycle.

