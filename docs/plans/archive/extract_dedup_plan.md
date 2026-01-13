## Extract Deduplication Plan (AST/CST/SCIP/Symtable/Bytecode/Tree-sitter)

### Goals
- Maximize shared helpers/utilities across extractors.
- Keep extractor-specific parsing/collection logic distinctive.
- Lean on ArrowDSL/pyarrow kernels for post-processing, hashing, and schema alignment.

---

### Scope 1: Shared file payload + context helpers
**Description**
Centralize file bytes/text decoding and standardized file identity mapping so every extractor uses the same logic.
**Status**
Completed with a small exception: typed context identity rows remain inline in
`src/extract/symtable_extract.py` and `src/extract/bytecode_extract.py`.

**Code pattern**
```python
# src/extract/common.py
from extract.file_context import FileContext

def file_identity_row(file_ctx: FileContext) -> dict[str, object]:
    return {
        "file_id": file_ctx.file_id,
        "path": file_ctx.path,
        "file_sha256": file_ctx.file_sha256,
    }

def text_from_file_ctx(file_ctx: FileContext) -> str | None:
    if file_ctx.text:
        return file_ctx.text
    if file_ctx.data is None:
        return None
    encoding = file_ctx.encoding or "utf-8"
    try:
        return file_ctx.data.decode(encoding, errors="replace")
    except UnicodeError:
        return None

def bytes_from_file_ctx(file_ctx: FileContext) -> bytes | None:
    if file_ctx.data is not None:
        return file_ctx.data
    if file_ctx.text is None:
        return None
    encoding = file_ctx.encoding or "utf-8"
    return file_ctx.text.encode(encoding, errors="replace")
```

**Target files**
- New: `src/extract/common.py`
- Update: `src/extract/ast_extract.py`
- Update: `src/extract/bytecode_extract.py`
- Update: `src/extract/symtable_extract.py`
- Update: `src/extract/cst_extract.py`
- Update: `src/extract/tree_sitter_extract.py`

**Integration checklist**
- [x] Replace `_row_text` / `_row_bytes` with `text_from_file_ctx` / `bytes_from_file_ctx`.
- [ ] Replace per-file identity dict literals with `file_identity_row` (still inline for typed context
  rows in `src/extract/symtable_extract.py` and `src/extract/bytecode_extract.py`).
- [x] Keep extractor-specific fields layered on top of the shared identity.

---

### Scope 2: Shared hash column + validity helpers
**Description**
Deduplicate `_valid_mask` + `_apply_hash_column` patterns in CST/symtable/bytecode/tree-sitter (and any future extractors).
**Status**
Completed.

**Code pattern**
```python
# src/extract/hashing.py
from collections.abc import Sequence

from arrowdsl.core.ids import HashSpec, hash_column_values
from arrowdsl.core.interop import TableLike, pc
from arrowdsl.schema.arrays import set_or_append_column

def valid_mask(table: TableLike, cols: Sequence[str]) -> object:
    mask = pc.is_valid(table[cols[0]])
    for col in cols[1:]:
        mask = pc.and_(mask, pc.is_valid(table[col]))
    return mask

def apply_hash_column(
    table: TableLike,
    *,
    spec: HashSpec,
    required: Sequence[str] | None = None,
) -> TableLike:
    hashed = hash_column_values(table, spec=spec)
    out_col = spec.out_col or f"{spec.prefix}_id"
    if required:
        mask = valid_mask(table, required)
        hashed = pc.if_else(mask, hashed, pa.scalar(None, type=hashed.type))
    return set_or_append_column(table, out_col, hashed)
```

**Target files**
- New: `src/extract/hashing.py`
- Update: `src/extract/cst_extract.py`
- Update: `src/extract/symtable_extract.py`
- Update: `src/extract/bytecode_extract.py`
- Update: `src/extract/tree_sitter_extract.py`

**Integration checklist**
- [x] Delete local `_valid_mask`/`_apply_hash_column` variants.
- [x] Route hash ID generation through `apply_hash_column`.
- [x] Add `required` fields when existing logic masked invalids.

---

### Scope 3: Rows → table + schema alignment helper
**Description**
Standardize table creation (row list to Arrow table), empty-table fallbacks, and schema alignment into a shared utility.
**Status**
Completed with explicit `rows_to_table` + `align_table` usage; `build_and_align` remains available but unused.

**Code pattern**
```python
# src/extract/tables.py
from collections.abc import Mapping, Sequence

from arrowdsl.core.interop import SchemaLike, TableLike
from arrowdsl.schema.schema import SchemaTransform, empty_table

def rows_to_table(rows: Sequence[Mapping[str, object]], schema: SchemaLike) -> TableLike:
    if not rows:
        return empty_table(schema)
    return pa.Table.from_pylist(list(rows), schema=schema)

def align_table(table: TableLike, schema: SchemaLike) -> TableLike:
    return SchemaTransform(schema=schema, safe_cast=True, on_error="unsafe").apply(table)

def build_and_align(
    rows: Sequence[Mapping[str, object]],
    schema: SchemaLike,
) -> TableLike:
    return align_table(rows_to_table(rows, schema), schema)
```

**Target files**
- New: `src/extract/tables.py`
- Update: `src/extract/ast_extract.py`
- Update: `src/extract/cst_extract.py`
- Update: `src/extract/symtable_extract.py`
- Update: `src/extract/bytecode_extract.py`
- Update: `src/extract/tree_sitter_extract.py`
- Update: `src/extract/runtime_inspect_extract.py`
- Update: `src/extract/scip_extract.py` (alignment helpers)

**Integration checklist**
- [x] Replace per-module “if rows else empty_table” blocks with `rows_to_table`.
- [x] Replace ad-hoc `SchemaTransform(...).apply` with `align_table`.
- [ ] Adopt `build_and_align` as the single helper (current usage keeps `rows_to_table` +
  `align_table` explicit for clarity).

---

### Scope 4: Nested list/struct accumulators
**Description**
Unify offsets/value accumulation for list columns and list-of-struct columns across CST, SCIP, and symtable.
**Status**
Mostly complete: list columns and CST QNames use shared accumulators; SCIP signature occurrences still build list<struct> manually.

**Code pattern**
```python
# src/extract/nested_lists.py
from dataclasses import dataclass, field
from collections.abc import Iterable, Sequence

@dataclass
class ListAccumulator:
    offsets: list[int] = field(default_factory=lambda: [0])
    values: list[object] = field(default_factory=list)

    def append(self, items: Iterable[object]) -> None:
        self.values.extend(items)
        self.offsets.append(len(self.values))

    def extend_from(self, other: "ListAccumulator") -> None:
        if len(other.offsets) <= 1:
            return
        base = self.offsets[-1]
        self.offsets.extend(base + offset for offset in other.offsets[1:])
        self.values.extend(other.values)

    def build(self, *, value_type: DataTypeLike) -> ArrayLike:
        return build_list_array(
            pa.array(self.offsets, type=pa.int32()),
            pa.array(self.values, type=value_type),
        )
```

**Target files**
- New: `src/extract/nested_lists.py`
- Update: `src/extract/cst_extract.py`
- Update: `src/extract/scip_extract.py`
- Update: `src/extract/symtable_extract.py`

**Integration checklist**
- [x] Replace `_offsets_start`, `_append_string_list`, `_extend_offsets`.
- [x] Replace per-module list builders with `ListAccumulator.build`.
- [ ] Convert SCIP signature occurrence list<struct> to `StructListAccumulator` (still manual in
  `src/extract/scip_extract.py`).

---

### Scope 5: Dataset spec registration helper
**Description**
Reduce boilerplate for `GLOBAL_SCHEMA_REGISTRY.register_dataset(make_dataset_spec(make_table_spec(...)))`.
**Status**
Completed.

**Code pattern**
```python
# src/extract/spec_helpers.py
from schema_spec.specs import ArrowFieldSpec, FieldBundle
from schema_spec.system import GLOBAL_SCHEMA_REGISTRY, make_dataset_spec, make_table_spec

def register_dataset(
    *,
    name: str,
    version: int,
    fields: Sequence[ArrowFieldSpec],
    bundles: Sequence[FieldBundle] = (),
) -> DatasetSpec:
    return GLOBAL_SCHEMA_REGISTRY.register_dataset(
        make_dataset_spec(
            table_spec=make_table_spec(
                name=name,
                version=version,
                bundles=bundles,
                fields=list(fields),
            )
        )
    )
```

**Target files**
- New: `src/extract/spec_helpers.py`
- Update: `src/extract/ast_extract.py`
- Update: `src/extract/cst_extract.py`
- Update: `src/extract/symtable_extract.py`
- Update: `src/extract/bytecode_extract.py`
- Update: `src/extract/repo_scan.py`
- Update: `src/extract/tree_sitter_extract.py`
- Update: `src/extract/runtime_inspect_extract.py`
- Update: `src/extract/scip_extract.py`

**Integration checklist**
- [x] Replace repeated registry boilerplate with `register_dataset`.
- [x] Keep bundles explicit where needed (file identity, span bundles).

---

### Scope 6: Shared derived-view helpers (filters/projections)
**Description**
Normalize "derived view" logic (e.g., AST defs) using ArrowDSL FilterSpec and predicate helpers.
**Status**
Completed.

**Code pattern**
```python
# src/extract/derived_views.py
from arrowdsl.compute.predicates import FilterSpec, InSet
from arrowdsl.core.interop import TableLike

def ast_def_nodes(nodes: TableLike) -> TableLike:
    if nodes.num_rows == 0:
        return nodes
    predicate = InSet(col="kind", values=("FunctionDef", "AsyncFunctionDef", "ClassDef"))
    return FilterSpec(predicate).apply_kernel(nodes)
```

**Target files**
- New: `src/extract/derived_views.py`
- Update: `src/extract/ast_extract.py` (replace in-function defs filter)

**Integration checklist**
- [x] Move derived-table logic into shared helpers.
- [x] Keep extractor API returning derived tables where expected.

---

### Scope 7: Shared join helper for kernel lane
**Description**
Wrap `apply_join` + `JoinSpec` boilerplate for repeated use (symtable/runtime inspect).
**Status**
Completed.

**Code pattern**
```python
# src/extract/join_helpers.py
from arrowdsl.compute.kernels import apply_join
from arrowdsl.plan.ops import JoinSpec
from arrowdsl.core.interop import TableLike

def left_join(
    left: TableLike,
    right: TableLike,
    *,
    left_keys: Sequence[str],
    right_keys: Sequence[str],
    left_output: Sequence[str],
    right_output: Sequence[str],
    use_threads: bool = True,
    output_suffix_for_right: str | None = None,
) -> TableLike:
    spec = JoinSpec(
        join_type="left outer",
        left_keys=tuple(left_keys),
        right_keys=tuple(right_keys),
        left_output=tuple(left_output),
        right_output=tuple(right_output),
        output_suffix_for_right=output_suffix_for_right or "",
    )
    return apply_join(left, right, spec=spec, use_threads=use_threads)
```

**Target files**
- New: `src/extract/join_helpers.py`
- Update: `src/extract/symtable_extract.py`
- Update: `src/extract/runtime_inspect_extract.py`

**Integration checklist**
- [x] Replace repeated `apply_join + JoinSpec` blocks with `left_join`.
- [x] Keep outputs explicit to preserve schema evolution.

---

### Scope 8: Post-processing pipeline helpers (hash → align → encode)
**Description**
Create a shared “postprocess” helper for extractors that consistently apply hash IDs, schema alignment, and optional dictionary encoding.
**Status**
Completed for SCIP; other extractors do not currently apply dictionary encoding.

**Code pattern**
```python
# src/extract/postprocess.py
from arrowdsl.schema.schema import EncodingSpec, encode_columns

def apply_encoding(table: TableLike, columns: Sequence[str]) -> TableLike:
    if not columns:
        return table
    return encode_columns(table, specs=tuple(EncodingSpec(column=col) for col in columns))
```

**Target files**
- New: `src/extract/postprocess.py`
- Update: `src/extract/scip_extract.py`
- Note: no dictionary encoding calls in other extractors at this time.

**Integration checklist**
- [x] Centralize dictionary-encoding usage via `apply_encoding` (currently used in
  `src/extract/scip_extract.py`).
- [x] Keep hash/align order consistent (hash first, align second, encode last).

---

### Scope 9: Extractor refactors to use helpers
**Description**
Apply the helper modules consistently to eliminate duplicated code paths and keep each extractor distinctive.
**Status**
Completed.

**Target files**
- `src/extract/ast_extract.py`
- `src/extract/cst_extract.py`
- `src/extract/scip_extract.py`
- `src/extract/symtable_extract.py`
- `src/extract/bytecode_extract.py`
- `src/extract/tree_sitter_extract.py`
- `src/extract/runtime_inspect_extract.py`
- `src/extract/repo_scan.py`

**Integration checklist**
- [x] Replace local row/text/bytes helpers with shared functions.
- [x] Replace nested list builders with `nested_lists` accumulators.
- [x] Replace hash column helpers with `extract/hashing.py`.
- [x] Replace schema transforms with `extract/tables.py`.
- [x] Keep extractor-specific row collection unchanged except for shared helpers.

---

### Scope 10: Documentation and naming cleanup
**Description**
Update docstrings and module comments to reflect shared utilities and remove stale per-module descriptions.
**Status**
Not started (docstring refresh still outstanding).

**Target files**
- All touched extractor modules above.

**Integration checklist**
- [ ] Update module docstrings to reference shared helpers.
- [x] Ensure helper modules have NumPy-style docstrings.
- [ ] Keep helper names and location aligned with extractor usage.
