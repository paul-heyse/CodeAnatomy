# Extraction Protocol Improvements Plan v1

## Goal
Improve accuracy and efficiency across tree-sitter, LibCST, AST, bytecode, and symtable extractors by:
- Adding consistent byte-span support (line/col -> byte offsets) where missing.
- Making encoding handling robust and consistent.
- Tightening query coverage and selective work to reduce parse/scan overhead.
- Keeping schemas stable where possible and implementing enhancements via derived views when needed.

---

## Scope 1 — Shared Encoding + Byte-Span Infrastructure

**Why**
AST, bytecode, and symtable currently emit line/col spans without byte offsets. A shared byte-span helper lets these extractors produce canonical spans aligned with semantic joins. At the same time, decoding should be consistent across extractors.

**Status:** Complete.

**Representative snippets**
```python
# src/extract/coordination/line_offsets.py (new)
@dataclass(frozen=True)
class LineOffsets:
    line_start_bytes: tuple[int, ...]
    total_len: int

    @classmethod
    def from_bytes(cls, data: bytes) -> "LineOffsets":
        starts = [0]
        for idx, value in enumerate(data):
            if value == NEWLINE_BYTE:
                starts.append(idx + 1)
        return cls(tuple(starts), len(data))

    def byte_offset(self, line0: int | None, col: int | None) -> int | None:
        if line0 is None or col is None:
            return None
        if line0 < 0 or line0 >= len(self.line_start_bytes):
            return None
        return self.line_start_bytes[line0] + col
```

```python
# src/utils/file_io.py (new helpers)
def detect_encoding(data: bytes, *, default: str = "utf-8") -> str:
    ...

def decode_bytes(
    data: bytes,
    *,
    encoding: str | None = None,
    default: str = "utf-8",
) -> tuple[str, str | None]:
    ...

# src/extract/coordination/context.py
_encoding, text = decode_bytes(data, encoding=file_ctx.encoding)
```

**Target files**
- `src/extract/coordination/line_offsets.py` (new)
- `src/extract/coordination/context.py`
- `src/utils/file_io.py`
- `src/extract/scanning/repo_scan.py` (optional: capture encoding in repo rows)

**Deprecate/remove after completion**
- `src/extract/extractors/tree_sitter/extract.py:_point_from_byte`
- `src/extract/extractors/tree_sitter/cache.py:_point_from_byte`

**Implementation checklist**
- [x] Add a shared `LineOffsets` helper for line->byte offset mapping.
- [x] Add shared `detect_encoding()` + `decode_bytes()` helpers.
- [x] Update `text_from_file_ctx` / decoding paths to use detected encoding when missing.
- [x] Replace ad-hoc byte->point helpers where possible with shared utilities.
- [x] Optional: capture detected encoding in repo scan rows for reuse.

---

## Scope 2 — AST Byte Spans + Encoding Consistency

**Why**
AST spans are currently line/col only. Adding byte spans makes AST joins compatible with bytecode, LibCST, and tree-sitter spans, improving downstream semantic joins.

**Status:** Complete.

**Representative snippets**
```python
# src/extract/extractors/ast_extract.py
line_offsets = LineOffsets.from_bytes(bytes_from_file_ctx(file_ctx) or b"")
start_offset = line_offsets.byte_offset(start_line0, start_col)
end_offset = line_offsets.byte_offset(end_line0, end_col)

span = SpanSpec(
    start_line0=start_line0,
    start_col=start_col,
    end_line0=end_line0,
    end_col=end_col,
    end_exclusive=True,
    col_unit="byte",
    byte_start=start_offset,
    byte_len=(
        max(0, end_offset - start_offset)
        if start_offset is not None and end_offset is not None
        else None
    ),
)
```

**Target files**
- `src/extract/extractors/ast_extract.py`
- `src/extract/coordination/line_offsets.py`

**Deprecate/remove after completion**
- No file deletions required.

**Implementation checklist**
- [x] Load file bytes once per file and build `LineOffsets`.
- [x] Add byte_start/byte_len to AST span specs.
- [x] Ensure encoding detection is used when decoding text for parsing.

---

## Scope 3 — Bytecode Spans + Line-Index Join View

**Why**
Bytecode spans are line/col only, and line tables lack byte offsets. Adding byte spans at instruction level and providing a derived view that joins to `file_line_index_v1` will enable exact byte-based mapping without changing the base schema.

**Status:** Complete.

**Representative snippets**
```python
# src/extract/extractors/bytecode_extract.py
line_offsets = LineOffsets.from_bytes(bytes_from_file_ctx(file_ctx) or b"")

byte_start = line_offsets.byte_offset(start_line0, start_col)
byte_end = line_offsets.byte_offset(end_line0, end_col)

span = SpanSpec(
    start_line0=start_line0,
    start_col=start_col,
    end_line0=end_line0,
    end_col=end_col,
    end_exclusive=True,
    col_unit="byte",
    byte_start=byte_start,
    byte_len=(byte_end - byte_start) if byte_start is not None and byte_end is not None else None,
)
```

```python
# src/semantics/bytecode_line_table.py
joined = bc_lines.join(
    line_index,
    join_keys=(["file_id", "line0"], ["file_id", "line_no"]),
    how="left",
    coalesce_duplicate_keys=True,
)
```

**Target files**
- `src/extract/extractors/bytecode_extract.py`
- `src/extract/coordination/line_offsets.py`
- `src/semantics/bytecode_line_table.py` (new)
- `src/semantics/ir_pipeline.py`
- `src/semantics/pipeline.py`
- `src/semantics/ir.py`
- `src/semantics/spec_registry.py`
- `src/datafusion_engine/schema/registry.py`

**Deprecate/remove after completion**
- None (schema remains stable; derived view only).

**Implementation checklist**
- [x] Add instruction-level byte spans using `LineOffsets`.
- [x] Ensure col_unit is explicitly set for bytecode spans.
- [x] Add derived view joining `py_bc_line_table` with `file_line_index_v1`.
- [x] Update downstream semantic joins to reference the derived view when byte offsets are required.

---

## Scope 4 — Symtable Span Hints With Byte Offsets

**Why**
Symtable currently emits only line-based hints. Adding byte offsets enables direct join with canonical byte spans while keeping the schema stable.

**Status:** Complete.

**Representative snippets**
```python
# src/extract/extractors/symtable_extract.py
line_offsets = LineOffsets.from_bytes(bytes_from_file_ctx(file_ctx) or b"")
byte_start = line_offsets.byte_offset(line0, 0)

span_hint = span_dict(
    SpanSpec(
        start_line0=line0,
        start_col=0,
        end_line0=None,
        end_col=None,
        end_exclusive=True,
        col_unit="byte",
        byte_start=byte_start,
        byte_len=None,
    )
)
```

**Target files**
- `src/extract/extractors/symtable_extract.py`
- `src/extract/coordination/line_offsets.py`

**Deprecate/remove after completion**
- None.

**Implementation checklist**
- [x] Build `LineOffsets` from file bytes once per file.
- [x] Add byte_start to `span_hint` for scopes.
- [x] Keep schema unchanged (still a span struct).

---

## Scope 5 — Tree-sitter Query Coverage + Targeted Query Execution

**Why**
Tree-sitter queries currently miss async defs and always scan the full tree. Adding async patterns and (optionally) scoping queries to changed ranges reduces missed coverage and improves performance on incremental runs.

**Status:** Complete.

**Representative snippets**
```scheme
; src/extract/extractors/tree_sitter/queries.py
(function_definition name: (identifier) @def.name) @def.node
(async_function_definition name: (identifier) @def.name) @def.node
```

```python
# src/extract/extractors/tree_sitter/extract.py
for pattern_index, capture_map in _iter_query_matches(
    cursor,
    root=context.root,
    ranges=context.ranges,
):
    collector.record_match(...)
```

**Target files**
- `src/extract/extractors/tree_sitter/queries.py`
- `src/extract/extractors/tree_sitter/extract.py`
- `src/extract/extractors/tree_sitter/cache.py` (reuse shared line helper)

**Deprecate/remove after completion**
- `_point_from_byte` helpers in tree-sitter modules (covered in Scope 1).

**Implementation checklist**
- [x] Add async defs (and docstrings if needed) to query pack.
- [x] Add optional per-range query execution when incremental parse is used.
- [x] Keep a fallback path for full-tree scans when no changed ranges exist.

---

## Scope 6 — LibCST Metadata Efficiency + Matcher Prefilter

**Why**
LibCST extraction already collects rich metadata, but matcher templates and metadata wrapper overhead can be reduced when features are disabled. This helps large repos and partial extraction modes.

**Status:** Complete.

**Representative snippets**
```python
# src/extract/extractors/cst_extract.py
if not _should_collect(options):
    return module
```

```python
# matcher prefilter
if not _prefilter(template):
    continue
```

**Target files**
- `src/extract/extractors/cst_extract.py`

**Deprecate/remove after completion**
- None.

**Implementation checklist**
- [x] Add short-circuit for metadata wrapper when nothing is requested.
- [x] Add a cheap prefilter for matcher templates.
- [x] Preserve existing behavior when options are fully enabled.

---

## Scope 7 — Validation + Regression Coverage

**Why**
Byte-span additions and query behavior changes should be validated with fixtures to ensure correctness and avoid regressions.

**Status:** Complete.

**Representative snippets**
```python
# tests/unit/extract/test_ast_spans.py (new)
assert row["span"]["byte_span"]["byte_start"] == expected
```

```python
# tests/unit/extract/test_tree_sitter_queries.py (new)
assert any(defn["kind"] == "async_function_definition" for defn in defs)
```

**Target files**
- `tests/unit/extract/test_ast_spans.py` (new)
- `tests/unit/extract/test_bytecode_spans.py` (new)
- `tests/unit/extract/test_symtable_spans.py` (new)
- `tests/unit/extract/test_tree_sitter_queries.py` (new)

**Deprecate/remove after completion**
- None.

**Implementation checklist**
- [x] Add focused fixture files for line/byte mapping.
- [x] Add tests for async defs in tree-sitter.
- [x] Validate byte_span output for AST/bytecode/symtable.

---

## Scope 8 — Canonical Byte-Span Normalization Helper

**Why**
Downstream view builders currently rely on per-view span assembly and mixed span sources. A single helper should canonicalize `bstart/bend/span` using byte-based fields when present and only fall back to line/col + `file_line_index_v1` when needed.

**Status:** Complete.

**Representative snippets**
```python
# src/semantics/span_normalize.py (new)
def normalize_byte_span_df(df: DataFrame, *, line_index: DataFrame | None) -> DataFrame:
    if "bstart" in df.schema().names and "bend" in df.schema().names:
        return df.with_column("span", span_make(col("bstart"), col("bend")))
    if "byte_start" in df.schema().names and "byte_len" in df.schema().names:
        return df.with_column("bstart", col("byte_start")).with_column(
            "bend", col("byte_start") + col("byte_len")
        ).with_column("span", span_make(col("bstart"), col("bend")))
    # fallback via line_index + col_to_byte
    return _normalize_via_line_index(df, line_index)
```

**Target files**
- `src/semantics/span_normalize.py` (new)
- `src/semantics/span_unnest.py` (new)
- `src/semantics/compiler.py`

**Deprecate/remove after completion**
- Ad-hoc span coalescing in per-view builders.

**Implementation checklist**
- [x] Add a shared `normalize_byte_span_df` helper for canonical `bstart/bend/span`.
- [x] Prefer byte-based fields; fallback to line/col + `file_line_index_v1` only when needed.
- [x] Use `span_make` + `col_to_byte` consistently.

---

## Scope 9 — Semantic Discovery for byte_len + Structured Span Inputs

**Why**
Join inference and normalization still expect `bstart/bend`. Teach the semantic layer to recognize `byte_len` (or nested `span.byte_span`) and derive `bend` consistently.

**Status:** Complete.

**Representative snippets**
```python
# src/semantics/column_types.py
(re.compile(r"^byte_len$|_byte_len$"), ColumnType.SPAN_END)

# src/semantics/schema.py
if span_start == "byte_start" and "byte_len" in schema.names:
    derived_end = span_start + byte_len
```

**Target files**
- `src/semantics/column_types.py`
- `src/semantics/schema.py`
- `src/semantics/compiler.py` (span derivation integration)

**Deprecate/remove after completion**
- Manual per-view `byte_start + byte_len` handling.

**Implementation checklist**
- [x] Treat `byte_len` as a span-end capability and derive `bend` when needed.
- [x] Allow structured `span.byte_span` inputs to serve as evidence.
- [x] Keep existing `bstart/bend` direct paths untouched.

---

## Scope 10 — Span Unnest Views for AST/Bytecode/Symtable/Tree-sitter

**Why**
CST already exposes span unnest views. Extending this pattern gives a uniform, byte-based surface for downstream joins and avoids repeated struct projections.

**Status:** Complete.

**Representative snippets**
```python
# src/semantics/ir_pipeline.py
SemanticIRView(
    name="ast_span_unnest",
    kind="span_unnest",
    inputs=("ast_nodes",),
    outputs=("ast_span_unnest",),
)
```

**Target files**
- `src/semantics/ir_pipeline.py`
- `src/semantics/pipeline.py`
- `src/semantics/span_unnest.py`
- `src/datafusion_engine/schema/registry.py`

**Deprecate/remove after completion**
- Ad-hoc span field projections in downstream views.

**Implementation checklist**
- [x] Add `*_span_unnest` views for AST, bytecode, symtable, tree-sitter.
- [x] Standardize output columns: `file_id`, `path`, `bstart`, `bend`, `span`, `col_unit`.
- [x] Register view names for schema validation.

---

## Scope 11 — Span Unit Normalization + Validation

**Why**
With byte spans unified, enforce and validate `col_unit="byte"` for byte-based views to prevent mismatched joins and hidden unit conversions.

**Status:** Complete.

**Representative snippets**
```python
# src/datafusion_engine/schema/registry.py
AST_SPAN_META = span_metadata(col_unit="byte")
BYTECODE_SPAN_META = span_metadata(col_unit="byte")
SYMTABLE_SPAN_META = span_metadata(col_unit="byte")
```

**Target files**
- `src/datafusion_engine/schema/registry.py`
- `src/semantics/span_normalize.py`

**Deprecate/remove after completion**
- Legacy `utf32` span metadata defaults on byte-based extractors.

**Implementation checklist**
- [x] Normalize span metadata to `col_unit="byte"` for byte-span outputs.
- [x] Add a validation rule to flag mixed `col_unit` in normalized views.

---

## Scope 12 — Prune Line/Col Columns from Normalized Views

**Why**
Once byte spans are canonical, line/col columns in normalized views add ambiguity and make downstream joins more complex. Keep them only in raw extractor outputs.

**Status:** Complete.

**Representative snippets**
```python
# src/semantics/compiler.py
if table_name.endswith("_norm_v1"):
    df = df.drop("start_line0", "start_col", "end_line0", "end_col")
```

**Target files**
- `src/semantics/compiler.py`
- `src/semantics/span_normalize.py`

**Deprecate/remove after completion**
- Line/col column usage in normalized semantic views.

**Implementation checklist**
- [x] Drop line/col columns from normalized views after `bstart/bend/span` are present.
- [x] Keep line/col only in raw extraction tables for debugging and auditing.

---

## Implementation Notes
- Prefer derived views for schema extensions to avoid rewriting existing extract schemas.
- Keep `SpanSpec` consistent: set `col_unit` explicitly and include `byte_start/byte_len` when available.
- Ensure SCIP remains on its existing ingestion path and is not folded into file-level extraction.
