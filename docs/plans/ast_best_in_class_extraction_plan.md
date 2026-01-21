# AST Best-in-Class Extraction Plan (Design-Phase)

## Goals
- Expand Python AST extraction to capture higher-fidelity facts (types, docstrings, imports, defs, calls).
- Preserve raw byte-offset spans at extraction time; normalization aligns with CST/SCIP later.
- Keep DataFusion schemas, nested views, and validation aligned with new AST outputs.
- Improve robustness and performance for large or adversarial inputs.

## Non-goals
- Replacing CST/SCIP extraction or their normalization logic.
- Moving AST extraction to a non-stdlib parser.
- Rewriting CPG binding logic beyond AST-specific facts.

## Decisions (approved)
- Deduplicate AST nodes by object identity is intentional.
- Store AST spans as byte offsets (`col_unit="byte"`). No pre-normalization.

## Constraints
- Keep `ast_files_v1` backward-compatible (new fields nullable; no breaking renames).
- Use stdlib `ast` only (no private CPython imports).
- Validation must rely on DataFusion `information_schema` and `arrow_typeof`.

---

## Scope 1: Parse and span correctness (byte offsets, error handling)
Status: Completed

### Target file list
- `src/extract/ast_extract.py`
- `src/extract/helpers.py`
- `src/datafusion_engine/schema_registry.py`

### Code pattern
```python
AST_COL_UNIT = "byte"

def _parse_ast_text(text: str, *, filename: str, options: ASTExtractOptions) -> tuple[ast.AST | None, dict[str, object] | None]:
    try:
        return (
            ast.parse(
                text,
                filename=filename,
                mode=options.mode,
                type_comments=options.type_comments,
                feature_version=options.feature_version,
                optimize=options.optimize,
            ),
            None,
        )
    except (SyntaxError, RecursionError, MemoryError, TypeError, ValueError) as exc:
        return None, _exception_error_row(exc)
```

### Implementation checklist
- [x] Add `mode`, `optimize`, `allow_top_level_await`, and `dont_inherit` to `ASTExtractOptions`.
- [x] Use byte offsets for spans (`col_unit="byte"`); keep offsets unchanged.
- [x] Treat empty files as valid inputs (parse empty text into `Module`).
- [x] Catch `RecursionError` and `MemoryError` and emit error rows.
- [x] Route top-level await support through a `compile(..., flags=...)` code path when enabled.

---

## Scope 2: Node attribute enrichment (types, docstrings, scalar fields)
Status: Completed

### Target file list
- `src/extract/ast_extract.py`
- `src/datafusion_engine/schema_registry.py`

### Code pattern
```python
def _node_scalar_attrs(node: ast.AST) -> dict[str, str]:
    attrs: dict[str, str] = {}
    for field in getattr(node, "_fields", ()):
        value = getattr(node, field, None)
        if isinstance(value, ast.AST):
            continue
        if isinstance(value, list) and any(isinstance(item, ast.AST) for item in value):
            continue
        serialized = _stringify_attr_value(value)
        if serialized is not None:
            attrs[field] = serialized
    return attrs

def _docstring_row(node: ast.AST, *, ast_id: int, source: str) -> dict[str, object] | None:
    literal = _docstring_literal(node)
    if literal is None:
        return None
    doc = ast.get_docstring(node, clean=True)
    if doc is None:
        return None
    return {
        "owner_ast_id": ast_id,
        "owner_kind": type(node).__name__,
        "docstring": doc,
        "span": span_dict(_span_spec_from_node(literal)),
        "source": ast.get_source_segment(source, literal, padded=False),
        "attrs": attrs_map({}),
    }
```

### Implementation checklist
- [x] Record `type_comment` values for nodes that define them.
- [x] Extract `Module.type_ignores` into a dedicated nested list (see Scope 3).
- [x] Capture docstrings for module/class/function nodes with source segments.
- [x] Add scalar field capture via `_fields` (string/int/bool lists).
- [x] Preserve current `name` and `value` fields for compatibility; store extras in `attrs`.
- [x] Record parse settings in `ast_files_v1.attrs`
      (mode, feature_version, type_comments, optimize, allow_top_level_await, dont_inherit).

---

## Scope 3: Derived AST fact tables (imports, defs, calls, type ignores)
Status: Completed

### Target file list
- `src/extract/ast_extract.py`
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/query_fragments.py`

### Code pattern
```python
imports_rows.append(
    {
        "file_id": file_ctx.file_id,
        "path": file_ctx.path,
        "kind": type(node).__name__,
        "module": module_name,
        "name": alias_name,
        "asname": alias_asname,
        "span": span_dict(_span_spec_from_node(node)),
        "attrs": attrs_map({"level": level}),
    }
)

defs_rows.append(
    {
        "kind": type(node).__name__,
        "name": _node_name(node),
        "span": span_dict(_span_spec_from_node(node)),
        "attrs": attrs_map({"arg_count": len(node.args.args)}),
    }
)

type_ignores_rows = [
    {
        "tag": ignore.tag,
        "span": span_dict(_type_ignore_span(ignore)),
        "attrs": attrs_map({}),
    }
    for ignore in getattr(root, "type_ignores", [])
]
```

### Implementation checklist
- [x] Add new nested lists to `ast_files_v1` for `docstrings`, `imports`, `defs`,
      `calls`, and `type_ignores`.
- [x] Register derived datasets in `NESTED_DATASET_INDEX`
      (`ast_docstrings`, `ast_imports`, `ast_defs`, `ast_calls`, `ast_type_ignores`).
- [x] Define schema structs for each new list (keep fields minimal and nullable).
- [x] Add SQL fragments for the derived AST datasets in `query_fragments`.
- [x] Populate derived rows in a single AST walk to avoid extra passes.

---

## Scope 4: DataFusion integration and validation
Status: Completed

### Target file list
- `src/datafusion_engine/schema_registry.py`
- `src/datafusion_engine/runtime.py`

### Code pattern
```python
def validate_ast_views(ctx: SessionContext) -> None:
    errors: dict[str, str] = {}
    for name in AST_VIEW_NAMES:
        try:
            ctx.sql(f"DESCRIBE SELECT * FROM {name}").collect()
        except (RuntimeError, TypeError, ValueError) as exc:
            errors[name] = str(exc)
    if errors:
        raise ValueError(f"AST view validation failed: {errors}.")
```

### Implementation checklist
- [x] Add a `validate_ast_views` helper mirroring CST/symtable/bytecode.
- [x] Ensure `nested_view_specs` registers AST-derived views (nodes, edges, imports, docstrings).
- [x] Use `arrow_typeof` checks for nested list structs in AST datasets.
- [x] Use existing `information_schema` snapshots for view diagnostics when enabled.

---

## Scope 5: Performance and reproducibility
Status: Completed

### Target file list
- `src/extract/ast_extract.py`
- `src/extract/helpers.py`

### Code pattern
```python
def _collect_ast_rows(..., *, batch_size: int | None = None) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for file_ctx in iter_contexts(...):
        row = _extract_ast_for_context(file_ctx, options=options)
        if row is not None:
            rows.append(row)
            if batch_size is not None and len(rows) >= batch_size:
                yield rows
                rows = []
    if rows:
        yield rows
```

### Implementation checklist
- [x] Add optional batching for row collection to reduce peak memory.
- [x] Introduce file-hash caching (skip unchanged files).
- [x] Add defensive limits for very large inputs (max bytes and node caps).
- [x] Keep extraction single-pass where possible to avoid extra AST walks.

---

## Scope 6: Testing and validation gates
Status: Completed

### Target file list
- `tests/unit/test_ast_extract.py`
- `tests/unit/test_datafusion_schema_registry.py`

### Code pattern
```python
def test_ast_spans_are_byte_offsets() -> None:
    rows = extract_ast_tables(repo_files=table)["ast_files"].to_pylist()
    node = rows[0]["nodes"][0]
    assert node["span"]["col_unit"] == "byte"
```

### Implementation checklist
- [x] Add tests for byte-offset spans.
- [x] Add tests for empty-file parsing and error rows.
- [x] Add tests for `type_ignores` and docstring extraction.
- [x] Add tests for imports, defs, and calls extraction.
- [x] Add DataFusion view validation smoke tests for AST views.

---

## Deliverables
- Updated AST extraction options, span handling, and error resilience.
- Enriched AST facts (docstrings, imports, defs, calls, type ignores) stored as nested lists.
- AST-derived DataFusion views, SQL fragments, and validation hooks.
- Tests that lock in span units, schema shape, and enriched extraction behavior.
