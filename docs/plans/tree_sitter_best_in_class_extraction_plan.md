# Tree-sitter Best-in-Class Extraction Plan (Design-Phase)

## Goals
- Deliver high-fidelity Python-only tree-sitter extraction with byte-precise spans.
- Add semantic facts that cross-check LibCST/AST for consistency.
- Align DataFusion schemas, nested views, and validation with new outputs.
- Improve scalability with cursor-based traversal and bounded query execution.

## Non-goals
- Multi-language parsing or language injection.
- Replacing LibCST/AST as the primary Python source of truth.
- Changing coordinate standards away from byte-based spans.

## Decisions (approved)
- Tree-sitter is Python-only for now.
- Span coordinates are byte-based (`col_unit="byte"`).
- Tree-sitter semantics are used to cross-check LibCST/AST results.

## Constraints
- Keep `tree_sitter_files_v1` backward compatible (new fields nullable).
- SessionContext remains the schema authority for validation.
- No DataFusion schema changes without information_schema validation.

---

## Scope 1: Python-only parser configuration and file gating
Status: Completed

### Target file list
- `src/extract/tree_sitter_extract.py`
- `src/extract/helpers.py`

### Code pattern
```python
PY_LANGUAGE = Language(tree_sitter_python.language())

def _assert_language_abi(lang: Language) -> None:
    if not (MIN_COMPATIBLE_LANGUAGE_VERSION <= lang.abi_version <= LANGUAGE_VERSION):
        msg = f"Tree-sitter ABI mismatch: {lang.abi_version}"
        raise ValueError(msg)

def _parser() -> Parser:
    _assert_language_abi(PY_LANGUAGE)
    return Parser(PY_LANGUAGE)

def _should_parse(file_ctx: FileContext, options: TreeSitterExtractOptions) -> bool:
    if file_ctx.path is None:
        return False
    return file_ctx.path.endswith(options.extensions)
```

### Implementation checklist
- [x] Add `extensions: tuple[str, ...] = (".py", ".pyi")` to `TreeSitterExtractOptions`.
- [x] Skip non-Python paths early (`_should_parse`).
- [x] Record language metadata in file attrs (`language_name`, `abi_version`, `semantic_version`).
- [x] Add parser timeout options and call `parser.reset()` when timeouts occur.

---

## Scope 2: Byte-precise spans with line/column points
Status: Completed

### Target file list
- `src/extract/tree_sitter_extract.py`

### Code pattern
```python
def _span_spec(node: Node) -> SpanSpec:
    start = node.start_byte
    end = node.end_byte
    start_point = node.start_point
    end_point = node.end_point
    return SpanSpec(
        start_line0=start_point.row,
        start_col=start_point.column,
        end_line0=end_point.row,
        end_col=end_point.column,
        end_exclusive=True,
        col_unit="byte",
        byte_start=int(start),
        byte_len=int(end - start),
    )
```

### Implementation checklist
- [x] Populate `start_line0`, `start_col`, `end_line0`, `end_col` for nodes/errors/missing.
- [x] Preserve byte spans in `SpanSpec.byte_start/byte_len`.
- [x] Use `col_unit="byte"` and `end_exclusive=True` consistently.
- [x] Guard missing values when `start_point` or `end_point` is unavailable.

---

## Scope 3: Structural fidelity via edges and cursor traversal
Status: Completed

### Target file list
- `src/extract/tree_sitter_extract.py`
- `src/datafusion_engine/schema_registry.py`

### Code pattern
```python
def _iter_nodes_with_edges(
    root: Node,
) -> Iterator[tuple[Node, Node | None, str | None, int | None]]:
    cursor = root.walk()
    stack: list[tuple[str | None, int]] = []
    while True:
        parent_id = stack[-1][0] if stack else None
        child_index = stack[-1][1] if stack else None
        yield cursor.node, cursor.node.parent, cursor.field_name, child_index
        if cursor.goto_first_child():
            stack.append((cursor.node.parent.id if cursor.node.parent else None, 0))
            continue
        if cursor.goto_next_sibling():
            stack[-1] = (stack[-1][0], stack[-1][1] + 1)
            continue
        while stack and not cursor.goto_next_sibling():
            cursor.goto_parent()
            stack.pop()
        if not stack:
            break
```

### Implementation checklist
- [x] Add `TREE_SITTER_EDGE_T` with `parent_id`, `child_id`, `field_name`, `child_index`.
- [x] Populate `ts_edges` in `tree_sitter_files_v1`.
- [x] Use `TreeCursor` traversal to avoid child list allocations.
- [x] Keep `parent_id` and `child_index` stable for graph reconstruction.

---

## Scope 4: Node metadata enrichment and lightweight text capture
Status: Completed

### Target file list
- `src/extract/tree_sitter_extract.py`
- `src/datafusion_engine/schema_registry.py`

### Code pattern
```python
def _node_entry(node: Node, *, file_ctx: FileContext, parent: Node | None) -> Row:
    return {
        "node_id": _node_id(file_ctx, node),
        "node_uid": int(node.id),
        "kind": node.type,
        "kind_id": int(node.kind_id),
        "grammar_id": int(node.grammar_id),
        "grammar_name": node.grammar_name,
        "span": span_dict(_span_spec(node)),
        "flags": {
            "is_named": node.is_named,
            "has_error": node.has_error,
            "is_error": node.is_error,
            "is_missing": node.is_missing,
            "is_extra": node.is_extra,
            "has_changes": node.has_changes,
        },
        "attrs": attrs_map({"text": _extract_text(node, file_ctx)}),
    }
```

### Implementation checklist
- [x] Add new fields for `node_uid`, `kind_id`, `grammar_id`, `grammar_name`.
- [x] Add flags for `is_extra` and `has_changes`.
- [x] Extract bounded `text` for identifiers/literals (max length).
- [x] Keep `attrs` map as fallback for optional metadata.

---

## Scope 5: QueryCursor-based semantic extraction packs
Status: Completed

### Target file list
- `src/extract/tree_sitter_extract.py`
- `src/extract/tree_sitter_queries.py` (new)
- `docs/python_library_reference/tree-sitter.md`
- `docs/python_library_reference/tree-sitter_advanced.md`

### Code pattern
```python
TS_DEFS_QUERY = """
(function_definition
  name: (identifier) @def.name)
"""

query = Query(PY_LANGUAGE, TS_DEFS_QUERY)
cursor = QueryCursor(query, match_limit=10_000)
matches = cursor.matches(root)
if cursor.did_exceed_match_limit:
    raise ValueError("Tree-sitter query match_limit exceeded.")
```

### Implementation checklist
- [x] Store queries in a dedicated module or `.scm` files.
- [x] Compile queries once and lint for rooted/local patterns.
- [x] Add `ts_captures` + derived datasets (`ts_defs`, `ts_calls`, `ts_imports`, `ts_docstrings`).
- [x] Use `QueryCursor.match_limit` and `set_byte_range` for bounded execution.
- [x] Record query pack metadata in file-level attrs.

---

## Scope 6: Per-file stats and diagnostics
Status: Completed

### Target file list
- `src/extract/tree_sitter_extract.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/runtime.py`

### Code pattern
```python
stats = {
    "node_count": node_count,
    "named_count": named_count,
    "error_count": error_count,
    "missing_count": missing_count,
    "parse_ms": parse_ms,
    "match_limit_exceeded": cursor.did_exceed_match_limit,
}
file_attrs = attrs_map({**stats, "language_name": "python"})
```

### Implementation checklist
- [x] Add `ts_stats` nested list or embed stats into `tree_sitter_files_v1.attrs`.
- [x] Record parse time and counts for nodes/errors/missing.
- [x] Track query match-limit events for diagnostics.
- [x] Add runtime diagnostics sink hooks for tree-sitter stats.

---

## Scope 7: DataFusion schema and nested view updates
Status: Completed

### Target file list
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/runtime.py`
- `src/schema_spec/specs.py`
- `src/schema_spec/system.py`

### Code pattern
```python
TREE_SITTER_EDGE_T = pa.struct(
    [
        ("parent_id", pa.string()),
        ("child_id", pa.string()),
        ("field_name", pa.string()),
        ("child_index", pa.int32()),
        ("attrs", ATTRS_T),
    ]
)

TREE_SITTER_FILES_SCHEMA = pa.schema(
    [
        ("repo", pa.string()),
        ("path", pa.string()),
        ("file_id", pa.string()),
        ("nodes", pa.list_(TREE_SITTER_NODE_T)),
        ("edges", pa.list_(TREE_SITTER_EDGE_T)),
        ("errors", pa.list_(TREE_SITTER_ERROR_T)),
        ("missing", pa.list_(TREE_SITTER_MISSING_T)),
        ("attrs", ATTRS_T),
    ]
)
```

### Implementation checklist
- [x] Extend `TREE_SITTER_NODE_T` with new metadata fields.
- [x] Add `TREE_SITTER_EDGE_T` and `TREE_SITTER_CAPTURE_T` structs.
- [x] Register new nested datasets in `NESTED_DATASET_INDEX`.
- [x] Add ordering/identity metadata for tree-sitter schemas.
- [x] Create view specs for `ts_edges`, `ts_captures`, and derived datasets.

---

## Scope 8: DataFusion validation and AST/CST cross-check views
Status: Completed

### Target file list
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/runtime.py`

### Code pattern
```python
def validate_ts_views(ctx: SessionContext) -> None:
    errors: dict[str, str] = {}
    for name in TS_VIEW_NAMES:
        try:
            ctx.sql(f"DESCRIBE SELECT * FROM {name}").collect()
        except (RuntimeError, TypeError, ValueError) as exc:
            errors[name] = str(exc)
    if errors:
        raise ValueError(f"Tree-sitter view validation failed: {errors}.")
```

### Implementation checklist
- [x] Add `TS_VIEW_NAMES` and `validate_ts_views` (mirrors AST/CST patterns).
- [x] Create SQL views to compare `ts_defs/ts_calls/ts_imports` vs AST/CST equivalents.
- [x] Use byte spans for joins (`byte_start`, `byte_len`, `path`).
- [x] Record mismatch counts in diagnostics and schema validation artifacts.

---

## Scope 9: Incremental parsing and change-driven queries (opt-in)
Status: Completed

### Target file list
- `src/extract/tree_sitter_extract.py`
- `src/extract/tree_sitter_cache.py` (new)

### Code pattern
```python
old_tree.edit(
    edit.start_byte,
    edit.old_end_byte,
    edit.new_end_byte,
    edit.start_point,
    edit.old_end_point,
    edit.new_end_point,
)
new_tree = parser.parse(new_bytes, old_tree=old_tree)
for span in old_tree.changed_ranges(new_tree):
    cursor.set_byte_range(span.start_byte, span.end_byte)
    cursor.matches(new_tree.root_node)
```

### Implementation checklist
- [x] Cache trees by file id/path for incremental reparses.
- [x] Apply `changed_ranges` to limit query execution.
- [x] Gate this path behind an extraction option (`incremental=True`).
- [x] Keep the non-incremental path as the default for batch runs.

---

## Scope 10: Verification and signature snapshots
Status: Planned

### Target file list
- `tests/fixtures/rule_signatures.json`
- `tests/integration/test_rule_semantics.py`

### Implementation checklist
- [ ] Resolve the schema registry circular import so snapshots can regenerate.
- [ ] Refresh rule signatures snapshot after view updates.
- [ ] Run quality gates: `uv run ruff check --fix`, `uv run pyrefly check`,
      `uv run pyright --warnings --pythonversion=3.13`.
- [ ] Fix any new errors in files touched by this work.
