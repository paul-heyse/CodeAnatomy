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

**Representative snippets**
```python
# src/extract/coordination/line_offsets.py (new)
@dataclass(frozen=True)
class LineOffsets:
    line_start_bytes: list[int]

    @classmethod
    def from_bytes(cls, data: bytes) -> "LineOffsets":
        starts = [0]
        for idx, b in enumerate(data):
            if b == 0x0A:  # '\n'
                starts.append(idx + 1)
        return cls(starts)

    def byte_offset(self, line0: int | None, col: int | None) -> int | None:
        if line0 is None or col is None:
            return None
        if line0 < 0 or line0 >= len(self.line_start_bytes):
            return None
        return self.line_start_bytes[line0] + col
```

```python
# src/extract/coordination/context.py (new helper)
def ensure_encoding(file_ctx: FileContext, data: bytes) -> str:
    if file_ctx.encoding:
        return file_ctx.encoding
    return detect_encoding(data)  # tokenize.detect_encoding wrapper
```

**Target files**
- `src/extract/coordination/line_offsets.py` (new)
- `src/extract/coordination/context.py`
- `src/utils/file_io.py` (or a new small helper module for encoding detection)
- `src/extract/scanning/repo_scan.py` (optional: capture encoding in repo rows)

**Deprecate/remove after completion**
- `src/extract/extractors/tree_sitter/extract.py:_point_from_byte`
- `src/extract/extractors/tree_sitter/cache.py:_point_from_byte`

**Implementation checklist**
- Add a shared `LineOffsets` helper for line->byte offset mapping.
- Add a shared `detect_encoding()` function (PEP-263 compliant).
- Update `text_from_file_ctx` / decoding paths to use detected encoding when missing.
- Replace ad-hoc byte->point helpers where possible with shared utilities.

---

## Scope 2 — AST Byte Spans + Encoding Consistency

**Why**
AST spans are currently line/col only. Adding byte spans makes AST joins compatible with bytecode, LibCST, and tree-sitter spans, improving downstream semantic joins.

**Representative snippets**
```python
# src/extract/extractors/ast_extract.py
line_offsets = LineOffsets.from_bytes(bytes_from_file_ctx(file_ctx) or b"")

span = SpanSpec(
    start_line0=start_line0,
    start_col=start_col,
    end_line0=end_line0,
    end_col=end_col,
    end_exclusive=True,
    col_unit="byte",
    byte_start=line_offsets.byte_offset(start_line0, start_col),
    byte_len=(
        line_offsets.byte_offset(end_line0, end_col) -
        line_offsets.byte_offset(start_line0, start_col)
    ) if start_line0 is not None and end_line0 is not None else None,
)
```

**Target files**
- `src/extract/extractors/ast_extract.py`
- `src/extract/coordination/line_offsets.py`

**Deprecate/remove after completion**
- No file deletions required.

**Implementation checklist**
- Load file bytes once per file and build `LineOffsets`.
- Add byte_start/byte_len to AST span specs.
- Ensure encoding detection is used when decoding text for parsing.

---

## Scope 3 — Bytecode Spans + Line-Index Join View

**Why**
Bytecode spans are line/col only, and line tables lack byte offsets. Adding byte spans at instruction level and providing a derived view that joins to `file_line_index_v1` will enable exact byte-based mapping without changing the base schema.

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

```sql
-- new derived view (DataFusion)
CREATE VIEW py_bc_line_table_with_bytes AS
SELECT
  bc.*, li.line_start_byte, li.line_end_byte, li.line_text
FROM py_bc_line_table bc
LEFT JOIN file_line_index_v1 li
  ON bc.file_id = li.file_id
 AND bc.line0 = li.line_no;
```

**Target files**
- `src/extract/extractors/bytecode_extract.py`
- `src/extract/coordination/line_offsets.py`
- `src/datafusion_engine/schema/registry.py` (add derived view registration if needed)
- `src/semantics/registry.py` or semantic view registry for the derived view

**Deprecate/remove after completion**
- None (schema remains stable; derived view only).

**Implementation checklist**
- Add instruction-level byte spans using `LineOffsets`.
- Ensure col_unit is explicitly set for bytecode spans.
- Add derived view joining `py_bc_line_table` with `file_line_index_v1`.
- Update semantic joins to reference the derived view when byte offsets are required.

---

## Scope 4 — Symtable Span Hints With Byte Offsets

**Why**
Symtable currently emits only line-based hints. Adding byte offsets enables direct join with canonical byte spans while keeping the schema stable.

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
- Build `LineOffsets` from file bytes once per file.
- Add byte_start to `span_hint` for scopes.
- Keep schema unchanged (still a span struct).

---

## Scope 5 — Tree-sitter Query Coverage + Targeted Query Execution

**Why**
Tree-sitter queries currently miss async defs and always scan the full tree. Adding async patterns and (optionally) scoping queries to changed ranges reduces missed coverage and improves performance on incremental runs.

**Representative snippets**
```scheme
; src/extract/extractors/tree_sitter/queries.py
(function_definition name: (identifier) @def.name) @def.node
(async_function_definition name: (identifier) @def.name) @def.node
```

```python
# src/extract/extractors/tree_sitter/extract.py
if parse_result.changed_ranges:
    cursor = QueryCursor(query, match_limit=options.query_match_limit)
    for r in parse_result.changed_ranges:
        cursor.set_byte_range(r.start_byte, r.end_byte)
        for _pattern_index, captures in cursor.matches(root):
            collector.record_match(...)
```

**Target files**
- `src/extract/extractors/tree_sitter/queries.py`
- `src/extract/extractors/tree_sitter/extract.py`
- `src/extract/extractors/tree_sitter/cache.py` (reuse shared line helper)

**Deprecate/remove after completion**
- `_point_from_byte` helpers in tree-sitter modules (covered in Scope 1).

**Implementation checklist**
- Add async defs (and docstrings if needed) to query pack.
- Add optional per-range query execution when incremental parse is used.
- Keep a fallback path for full-tree scans when no changed ranges exist.

---

## Scope 6 — LibCST Metadata Efficiency + Matcher Prefilter

**Why**
LibCST extraction already collects rich metadata, but matcher templates and metadata wrapper overhead can be reduced when features are disabled. This helps large repos and partial extraction modes.

**Representative snippets**
```python
# src/extract/extractors/cst_extract.py
if not any([
    options.include_refs,
    options.include_imports,
    options.include_callsites,
    options.include_defs,
    options.include_type_exprs,
    options.include_docstrings,
    options.include_decorators,
    options.include_call_args,
]):
    # Only parse_manifest requested; skip wrapper + traversal
    return module
```

```python
# matcher prefilter
if template not in data.decode("utf-8", errors="ignore"):
    continue
```

**Target files**
- `src/extract/extractors/cst_extract.py`

**Deprecate/remove after completion**
- None.

**Implementation checklist**
- Add short-circuit for metadata wrapper when nothing is requested.
- Add a cheap prefilter for matcher templates.
- Preserve existing behavior when options are fully enabled.

---

## Scope 7 — Validation + Regression Coverage

**Why**
Byte-span additions and query behavior changes should be validated with fixtures to ensure correctness and avoid regressions.

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
- Add focused fixture files for line/byte mapping.
- Add tests for async defs in tree-sitter.
- Validate byte_span output for AST/bytecode/symtable.

---

## Implementation Notes
- Prefer derived views for schema extensions to avoid rewriting existing extract schemas.
- Keep `SpanSpec` consistent: set `col_unit` explicitly and include `byte_start/byte_len` when available.
- Ensure SCIP remains on its existing ingestion path and is not folded into file-level extraction.
