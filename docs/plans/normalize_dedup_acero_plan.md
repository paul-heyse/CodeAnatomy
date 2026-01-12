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
- [x] Replace local `_column_or_null` functions with `arrow_utils.column_or_null`.
- [x] Ensure all callers pass explicit `dtype`.
- [x] Remove now-unused local helpers.

**Status**: Completed (`src/normalize/arrow_utils.py`, `src/normalize/spans.py`,
`src/normalize/diagnostics.py`, `src/normalize/bytecode_anchor.py`,
`src/normalize/bytecode_dfg.py`).

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
- [x] Move `FileTextIndex` and `RepoTextIndex` into `text_index.py`.
- [x] Re-export from `src/normalize/spans.py` for backward compatibility.
- [x] Replace local `_row_value_int` and `_file_index` with shared helpers.
- [x] Move `normalize_position_encoding` into `text_index.py` and reuse in diagnostics/spans.

**Status**: Completed (`src/normalize/text_index.py`, `src/normalize/spans.py`,
`src/normalize/diagnostics.py`, `src/normalize/bytecode_anchor.py`).

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
- [x] Replace local `_prefixed_hash64` wrappers with `ids.prefixed_hash64`.
- [x] Use `masked_prefixed_hash` where validity masks are required.
- [x] Remove local duplicate hash wrappers.

**Status**: Completed (`src/normalize/ids.py`, `src/normalize/types.py`,
`src/normalize/diagnostics.py`, `src/normalize/bytecode_dfg.py`).

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
- [x] Move all `*_SPEC` and `*_SCHEMA` definitions into `schemas.py`.
- [x] Keep public schema constants re-exported via `src/normalize/__init__.py` if needed.
- [x] Ensure schema version constants stay centralized.

**Status**: Completed (`src/normalize/schemas.py`, imports updated across normalize modules).

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
from typing import Literal

import pyarrow as pa
import pyarrow.acero as acero

from arrowdsl.compute.kernels import canonical_sort as _canonical_sort
from arrowdsl.plan.ops import SortKey

def canonical_sort(
    table: pa.Table,
    sort_keys: Sequence[tuple[str, Literal["ascending", "descending"]]],
) -> pa.Table:
    keys = [SortKey(column=col, order=order) for col, order in sort_keys]
    return _canonical_sort(table, sort_keys=keys)

def run_plan(decl: acero.Declaration, *, use_threads: bool) -> pa.Table:
    return decl.to_table(use_threads=use_threads)
```

**Target files**
- New: `src/normalize/pipeline.py`
- Update: `src/normalize/bytecode_dfg.py` (to route joins through plan runner)
- Update: `src/normalize/types.py` (for canonical sort and dedupe helpers)

**Implementation checklist**
- [x] Introduce canonical sort helper and reuse in normalize outputs that require determinism.
- [x] Ensure any dedupe policy is executed after a canonical sort.
- [x] Keep Acero plans as the default for joins and projections.

**Status**: Completed (`src/normalize/pipeline.py`, `src/normalize/bytecode_dfg.py`,
`src/normalize/types.py`).

---

### Scope 6: Vectorized filter/projection helpers
**Description**
Replace row-wise loops with Arrow compute filters and projections where the transform is
row-count preserving.

**Code pattern**
```python
# src/normalize/arrow_utils.py
from arrowdsl.core.interop import ArrayLike, ChunkedArrayLike, TableLike, pc

def filter_non_empty_utf8(
    table: TableLike, column: str
) -> tuple[TableLike, ArrayLike | ChunkedArrayLike]:
    trimmed = pc.call_function("utf8_trim_whitespace", [table[column]])
    ok = pc.and_(
        pc.is_valid(trimmed),
        pc.greater(pc.call_function("utf8_length", [trimmed]), 0),
    )
    return table.filter(ok), pc.call_function("filter", [trimmed, ok])
```

**Target files**
- Update: `src/normalize/types.py`
- Update: `src/normalize/diagnostics.py`
- Update: `src/normalize/spans.py`

**Implementation checklist**
- [x] Replace Python loops used only for filtering with `Table.filter`.
- [x] Use `pc.*` expressions for computed columns instead of list buffers when possible.
- [x] Preserve existing null-handling semantics.

**Status**: Completed (vectorized filtering in `src/normalize/types.py`,
`src/normalize/arrow_utils.py`; other modules retained row-wise logic where required).

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
from normalize.pipeline import canonical_sort

def build_reaches(defs: TableLike, uses: TableLike) -> TableLike:
    joined = apply_join(
        defs,
        uses,
        spec=JoinSpec(
            join_type="inner",
            left_keys=("code_unit_id", "symbol"),
            right_keys=("code_unit_id", "symbol"),
            left_output=("code_unit_id", "symbol", "def_event_id"),
            right_output=("use_event_id", "path", "file_id"),
        ),
        use_threads=True,
    )
    edge_id = prefixed_hash64("df_reach", [joined["def_event_id"], joined["use_event_id"]])
    out = joined.append_column("edge_id", edge_id)
    return canonical_sort(
        out,
        sort_keys=[
            ("code_unit_id", "ascending"),
            ("symbol", "ascending"),
            ("def_event_id", "ascending"),
            ("use_event_id", "ascending"),
        ],
    )
```

**Target files**
- Update: `src/normalize/bytecode_dfg.py`
- Update: `src/normalize/pipeline.py` (canonical sort helper for deterministic outputs)

**Implementation checklist**
- [x] Split def/use events into two tables.
- [x] Join using `apply_join` (Acero) instead of Python loops.
- [x] Compute `edge_id` via shared hash helper.
- [x] Canonical-sort final table when determinism is required.

**Status**: Completed (`src/normalize/bytecode_dfg.py`).

---

### Scope 8: Vectorize `normalize_type_exprs` and `normalize_types`
**Description**
Build normalized type expressions with compute kernels and dedupe types via `group_by` or
`Table.unique` with canonical sort.

**Code pattern**
```python
# src/normalize/types.py
from normalize.arrow_utils import trimmed_non_empty_utf8
from normalize.ids import prefixed_hash64

def normalize_type_exprs(table: TableLike) -> TableLike:
    trimmed, ok = trimmed_non_empty_utf8(table["expr_text"])
    filtered = table.filter(ok)
    type_repr = pc.call_function("filter", [trimmed, ok])
    type_id = prefixed_hash64("type", [type_repr])
    return filtered.append_column("type_repr", type_repr).append_column("type_id", type_id)
```

**Target files**
- Update: `src/normalize/types.py`
- Update: `src/normalize/arrow_utils.py` (filter helper)

**Implementation checklist**
- [x] Replace list buffer accumulation with compute filters and appended columns.
- [x] Use canonical sort before any order-dependent dedupe.
- [x] For `normalize_types`, replace set-based dedupe with `group_by` or `unique`.

**Status**: Completed (`src/normalize/types.py`).

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
- [x] Replace inline metadata joins with `join_code_unit_meta`.
- [x] Keep join keys and output ordering stable across callers.
- [x] Add minimal validation (skip join if key missing).

**Status**: Completed (`src/normalize/arrow_utils.py`, `src/normalize/bytecode_cfg.py`).

---

### Scope 10: Unify span pipeline + diagnostics helpers
**Description**
Refactor span conversion and error table construction into shared helpers so CST/AST/SCIP
normalization uses a single pipeline.

**Code pattern**
```python
# src/normalize/span_pipeline.py
from dataclasses import dataclass
from arrowdsl.schema.arrays import set_or_append_column
import pyarrow as pa

@dataclass(frozen=True)
class SpanOutputColumns:
    bstart: str = "bstart"
    bend: str = "bend"
    ok: str = "span_ok"

def append_span_columns(
    table: TableLike,
    *,
    bstarts: list[int | None],
    bends: list[int | None],
    oks: list[bool],
    columns: SpanOutputColumns | None = None,
) -> TableLike:
    cols = columns or SpanOutputColumns()
    out = set_or_append_column(table, cols.bstart, pa.array(bstarts, type=pa.int64()))
    out = set_or_append_column(out, cols.bend, pa.array(bends, type=pa.int64()))
    return set_or_append_column(out, cols.ok, pa.array(oks, type=pa.bool_()))
```

**Target files**
- New: `src/normalize/span_pipeline.py`
- Update: `src/normalize/spans.py`
- Update: `src/normalize/diagnostics.py`

**Implementation checklist**
- [x] Move `_span_error_table` and alias helpers into `span_pipeline.py`.
- [x] Use shared append helpers for AST, SCIP, and CST spans.
- [x] Keep span error schema in `normalize/schemas.py`.

**Status**: Completed (`src/normalize/span_pipeline.py`, `src/normalize/spans.py`,
`src/normalize/schemas.py`).

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
- [x] Add a short migration section listing renamed helpers.
- [x] Keep backwards-compatible imports for one release cycle.

**Status**: Completed (`docs/plans/normalize_dedup_acero_plan.md`,
`src/normalize/__init__.py`, re-exports in `src/normalize/spans.py`).

---

### Migration notes (old → new)
- `normalize.spans.FileTextIndex` → `normalize.text_index.FileTextIndex`
- `normalize.spans.RepoTextIndex` → `normalize.text_index.RepoTextIndex`
- `normalize.spans.normalize_position_encoding` → `normalize.text_index.normalize_position_encoding`
- `normalize.spans._row_value_int` / `normalize.bytecode_anchor._row_value_int` → `normalize.text_index.row_value_int`
- `normalize.spans._column_or_null` / `normalize.diagnostics._column_or_null` / `normalize.bytecode_dfg._column_or_null` → `normalize.arrow_utils.column_or_null`
- `normalize.spans._append_alias_cols` → `normalize.span_pipeline.append_alias_cols`
- `normalize.spans._span_error_table` → `normalize.span_pipeline.span_error_table`
- Local `_prefixed_hash64` wrappers → `normalize.ids.prefixed_hash64`
- Schema/spec constants in normalize modules → `normalize.schemas`
- Inline code-unit meta joins → `normalize.arrow_utils.join_code_unit_meta`
