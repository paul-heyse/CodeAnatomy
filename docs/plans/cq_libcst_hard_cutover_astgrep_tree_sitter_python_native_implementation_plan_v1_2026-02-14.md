# CQ LibCST Hard-Cutover to ast-grep/tree-sitter/Python-Native Implementation Plan v1 (2026-02-14)

**Status:** Proposed (Design Phase, Hard Cut)  
**Date:** 2026-02-14  
**Scope:** `tools/cq` Python enrichment pipeline, contracts, telemetry, docs, tests  
**Primary objective:** Fully remove `libcst` usage from `tools/cq` and replace it with `ast-grep-py` + `tree-sitter` + Python stdlib (`ast`, `symtable`) with no compatibility code and no legacy artifacts retained.

---

## 1. Design-Phase Hard-Cut Policy

This plan is intentionally aggressive.

1. No compatibility shims.
2. No dual-run old/new provider in production code paths.
3. No legacy `libcst` stage keys retained in output contracts.
4. No retained `libcst` modules under `tools/cq/search`.
5. Any wave that replaces a legacy module deletes that module in the same wave.

---

## 2. Target End State

Python enrichment in `tools/cq` is driven by five stages:

1. `ast_grep`
2. `python_ast`
3. `import_detail`
4. `python_resolution` (new; replaces `libcst`)
5. `tree_sitter`

`python_resolution` provides:

1. `symbol_role`
2. `qualified_name_candidates`
3. `binding_candidates`
4. `enclosing_callable`
5. `enclosing_class`
6. `import_alias_chain`

No `libcst` runtime import, wrapper, telemetry key, or stage metadata remains.

---

## 3. Scope Items

## Scope Item 1: Introduce `python_resolution` Native Provider

### Objective

Create a new provider module that reconstructs current LibCST output fields using only:

1. `ast-grep-py` anchor context
2. `tree-sitter` byte-range anchor + query captures
3. Python `ast` syntax tree
4. Python `symtable` scope table

### Representative Design Snippets

```python
# tools/cq/search/python_native_resolution.py
from __future__ import annotations

import ast
import symtable

from tools.cq.core.locations import byte_offset_to_line_col

def enrich_python_resolution_by_byte_range(
    source: str,
    *,
    source_bytes: bytes,
    file_path: str,
    byte_start: int,
    byte_end: int,
    session: object | None = None,
) -> dict[str, object]:
    line, col = byte_offset_to_line_col(source_bytes, byte_start)
    tree = ast.parse(source_bytes)
    st = symtable.symtable(source, file_path, "exec")

    anchor = _resolve_anchor_node(
        source_bytes=source_bytes,
        byte_start=byte_start,
        byte_end=byte_end,
        line=line,
        col=col,
        session=session,
    )
    if anchor is None:
        return {}

    payload: dict[str, object] = {}
    payload.update(_extract_symbol_role(anchor))
    payload.update(_extract_enclosing_context(anchor))
    payload.update(_extract_import_alias_chain(anchor, tree=tree))
    payload.update(_extract_binding_candidates(anchor, scope_table=st, line=line))
    payload.update(_extract_qualified_name_candidates(anchor, tree=tree, scope_table=st))
    return payload
```

```python
def _extract_symbol_role(node: ast.AST) -> dict[str, object]:
    ctx = getattr(node, "ctx", None)
    if isinstance(ctx, ast.Load):
        return {"symbol_role": "read"}
    if isinstance(ctx, ast.Store):
        return {"symbol_role": "write"}
    if isinstance(ctx, ast.Del):
        return {"symbol_role": "delete"}
    return {}
```

```python
def _binding_flags_for_name(table: symtable.SymbolTable, name: str) -> dict[str, bool] | None:
    try:
        sym = table.lookup(name)
    except KeyError:
        return None
    return {
        "is_imported": sym.is_imported(),
        "is_assigned": sym.is_assigned(),
        "is_parameter": sym.is_parameter(),
        "is_local": sym.is_local(),
        "is_free": sym.is_free(),
        "is_global": sym.is_global(),
        "is_nonlocal": sym.is_nonlocal(),
    }
```

### Key Library Functions

1. `ast.parse`
2. `symtable.symtable`
3. `SymbolTable.lookup`
4. `Symbol.is_imported`, `Symbol.is_assigned`, `Symbol.is_parameter`, `Symbol.is_local`, `Symbol.is_free`
5. `tree_sitter.Node.named_descendant_for_byte_range`
6. `ast_grep_py.SgNode.range`

### Target Files to Edit

1. `tools/cq/search/python_enrichment.py` (provider import switch)
2. `tools/cq/search/python_analysis_session.py` (session API integration)

### New Files to Create

1. `tools/cq/search/python_native_resolution.py`
2. `tests/unit/cq/search/test_python_native_resolution.py`

### Checklist

1. Implement byte-range entrypoint and deterministic payload builder.
2. Implement field extractors for all six resolution fields.
3. Ensure fail-open behavior (`{}` on parse/scope failures).
4. Add targeted unit coverage for each field extractor.

---

## Scope Item 2: Refactor `PythonAnalysisSession` for Native Resolution

### Objective

Remove LibCST wrapper state and add/canonicalize native analysis caches used by new resolution stage.

### Representative Design Snippet

```python
@dataclass(slots=True)
class PythonAnalysisSession:
    file_path: Path
    source: str
    source_bytes: bytes
    content_hash: str
    sg_root: SgRoot | None = None
    node_index: Any | None = None
    ast_tree: ast.Module | None = None
    symtable_table: symtable.SymbolTable | None = None
    tree_sitter_tree: Any | None = None
    resolution_index: dict[str, object] | None = None
    stage_timings_ms: dict[str, float] = field(default_factory=dict)
    stage_errors: dict[str, str] = field(default_factory=dict)
```

### Key Library Functions

1. `ast.parse`
2. `symtable.symtable`
3. `parse_python_tree` from `tools/cq/search/tree_sitter_python.py`

### Target Files to Edit

1. `tools/cq/search/python_analysis_session.py`

### New Files to Create

1. None (unless a separate index helper module is split during implementation)

### Checklist

1. Remove `libcst_wrapper` field.
2. Delete `ensure_libcst_wrapper`.
3. Add `ensure_resolution_index` (if required by provider internals).
4. Preserve session cache bounds and content-hash invalidation behavior.

---

## Scope Item 3: Replace LibCST Stage in `python_enrichment.py`

### Objective

Replace `_run_libcst_stage` with `_run_python_resolution_stage`, update state fields and agreement inputs, and remove all `libcst` stage mentions.

### Representative Design Snippets

```python
# old
from tools.cq.search.libcst_python import enrich_python_resolution_by_byte_range

# new
from tools.cq.search.python_native_resolution import enrich_python_resolution_by_byte_range
```

```python
def _run_python_resolution_stage(...) -> None:
    if byte_start is None or byte_end is None:
        state.stage_status["python_resolution"] = "skipped"
        state.stage_timings_ms["python_resolution"] = 0.0
        return

    started = perf_counter()
    try:
        state.python_resolution_fields = enrich_python_resolution_by_byte_range(
            source_text,
            source_bytes=source_bytes,
            file_path=cache_key,
            byte_start=byte_start,
            byte_end=byte_end,
            session=session,
        )
    except _ENRICHMENT_ERRORS as exc:
        state.python_resolution_fields = {}
        state.degrade_reasons.append(f"python_resolution: {type(exc).__name__}")

    state.stage_timings_ms["python_resolution"] = (perf_counter() - started) * 1000.0
    if state.python_resolution_fields:
        state.payload.update(state.python_resolution_fields)
        _append_source(state.payload, "python_resolution")
        state.stage_status["python_resolution"] = "applied"
    else:
        state.stage_status["python_resolution"] = "skipped"
```

```python
agreement = _build_agreement_section(
    ast_fields=state.ast_fields,
    python_resolution_fields=state.python_resolution_fields,
    tree_sitter_fields=state.tree_sitter_fields,
)
```

### Target Files to Edit

1. `tools/cq/search/python_enrichment.py`

### New Files to Create

1. None

### Checklist

1. Rename state field `libcst_fields` -> `python_resolution_fields`.
2. Rename stage keys `libcst` -> `python_resolution`.
3. Remove all `libcst` degrade reason paths.
4. Update agreement source names accordingly.
5. Update payload source list to include `python_resolution`.

---

## Scope Item 4: Expand tree-sitter Python Query Support for Gap Fill

### Objective

Strengthen `tree_sitter` supplemental coverage so it can contribute deterministic fallback signal for resolution fields when AST/symtable data is partial.

### Representative Design Snippets

```scheme
; tools/cq/search/queries/python/enrichment_resolution.scm
(call function: (identifier) @call.expression)
(call function: (attribute) @call.expression)
(function_definition name: (identifier) @def.function)
(class_definition name: (identifier) @class.definition)
(import_from_statement module_name: (_) @import.from)
(import_statement (aliased_import name: (_) @import.name alias: (identifier) @import.alias))
```

```python
query = _compile_query("enrichment_resolution.scm", source)
cursor = _TreeSitterQueryCursor(query, match_limit=match_limit)
cursor.set_byte_range(byte_start, byte_end)
captures = cursor.captures(root)
```

### Key Library Functions

1. `Query`
2. `QueryCursor.captures`
3. `QueryCursor.set_byte_range`
4. `Language.field_id_for_name`
5. `Node.child_by_field_name`

### Target Files to Edit

1. `tools/cq/search/tree_sitter_python.py`
2. `tools/cq/search/queries/python/enrichment_core.scm`
3. `tools/cq/search/queries/python/enrichment_imports.scm`
4. `tools/cq/search/queries/python/enrichment_locals.scm`

### New Files to Create

1. `tools/cq/search/queries/python/enrichment_resolution.scm`
2. `tools/cq/search/queries/python/enrichment_bindings.scm` (optional if split is preferred)

### Checklist

1. Add new query packs and integrate into `_query_sources`.
2. Keep compile-time lint via `lint_python_query_packs`.
3. Bound captures and text lengths to existing payload budgets.
4. Preserve fail-open behavior on query compile/runtime errors.

---

## Scope Item 5: Contract, Normalization, and Telemetry Renames

### Objective

Hard-rename all `libcst` stage nomenclature in structured payloads and telemetry.

### Representative Design Snippets

```python
# tools/cq/search/smart_search.py
"stages": {
    "ast_grep": {"applied": 0, "degraded": 0, "skipped": 0},
    "python_ast": {"applied": 0, "degraded": 0, "skipped": 0},
    "import_detail": {"applied": 0, "degraded": 0, "skipped": 0},
    "python_resolution": {"applied": 0, "degraded": 0, "skipped": 0},
    "tree_sitter": {"applied": 0, "degraded": 0, "skipped": 0},
}
```

```python
# tools/cq/search/enrichment/core.py
_PY_RESOLUTION_KEYS = frozenset({
    "symbol_role",
    "qualified_name_candidates",
    "binding_candidates",
    "enclosing_callable",
    "enclosing_class",
    "import_alias_chain",
    ...
})
```

### Target Files to Edit

1. `tools/cq/search/smart_search.py`
2. `tools/cq/search/enrichment/core.py`
3. `tools/cq/search/contracts.py` (if stage metadata shape is mirrored)
4. `tools/cq/core/enrichment_facts.py` (only if source labels are surfaced)

### New Files to Create

1. None

### Checklist

1. Remove `libcst` stage keys from telemetry defaults.
2. Ensure no residual `stage_status["libcst"]` reads/writes exist.
3. Ensure normalized payloads still partition into `resolution/behavior/structural`.
4. Keep backward-compatibility code out of scope by policy.

---

## Scope Item 6: Search Surface and Rendering Validation

### Objective

Ensure markdown/summary outputs remain coherent after stage replacement and source attribution changes.

### Representative Design Snippet

```python
if state.python_resolution_fields:
    state.payload.update(state.python_resolution_fields)
    _append_source(state.payload, "python_resolution")
```

### Target Files to Edit

1. `tools/cq/search/smart_search.py`
2. `tools/cq/core/report.py`
3. `tools/cq/core/enrichment_facts.py`
4. `tools/cq/search/semantic_planes_static.py`

### New Files to Create

1. None

### Checklist

1. Validate Code Facts rendering for resolution cluster fields.
2. Validate enrichment telemetry summary displays `python_resolution`.
3. Validate semantic planes `locals` preview still has stable data.

---

## Scope Item 7: Test and Golden Cutover

### Objective

Replace implicit LibCST assumptions in tests with native-resolution expectations and add explicit tests for new provider.

### Representative Design Snippets

```python
def test_python_resolution_stage_applies_for_identifier_anchor(...) -> None:
    payload = enrich_python_context(...)
    stage = payload.get("stage_status", {})
    assert stage.get("python_resolution") == "applied"
```

```python
def test_no_libcst_stage_key_in_payload(...) -> None:
    payload = enrich_python_context(...)
    assert "libcst" not in payload.get("stage_status", {})
```

### Target Files to Edit

1. `tests/unit/cq/search/test_python_enrichment.py`
2. `tests/unit/cq/search/test_smart_search.py`
3. `tests/unit/cq/test_report.py` (if output field names are asserted)

### New Files to Create

1. `tests/unit/cq/search/test_python_native_resolution.py`
2. `tests/unit/cq/search/test_python_resolution_agreement.py`

### Checklist

1. Add direct unit tests for new provider field extraction.
2. Update enrichment stage assertions from `libcst` to `python_resolution`.
3. Update goldens where telemetry/source names changed.
4. Run full quality gate after implementation wave completion.

---

## Scope Item 8: Docs and Dependency Cleanup

### Objective

Remove `libcst` references from CQ docs and dependency configuration for CQ runtime surface.

### Representative Design Snippet

```toml
# pyproject.toml
dependencies = [
  ...
  # "libcst",  # removed after hard cutover
]
```

### Target Files to Edit

1. `tools/cq/README.md`
2. `pyproject.toml`
3. `docs/plans/*` and CQ architecture docs that describe enrichment stages

### New Files to Create

1. None

### Checklist

1. Replace stage documentation row `libcst` -> `python_resolution`.
2. Remove CQ-specific LibCST references from operational docs.
3. Remove `libcst` dependency if no non-CQ code path requires it.

---

## 4. Full Decommission and Deletion Ledger (Final Wave)

This section is executed only after Scope Items 1-8 are implemented and tests are green.

## 4.1 Files to Fully Delete

1. `tools/cq/search/libcst_python.py`

## 4.2 In-File Legacy Code to Delete

1. `tools/cq/search/python_analysis_session.py`
   - Delete `libcst_wrapper` dataclass field.
   - Delete `ensure_libcst_wrapper`.
2. `tools/cq/search/python_enrichment.py`
   - Delete `_run_libcst_stage`.
   - Delete `libcst_fields` state storage.
   - Delete all `state.stage_status["libcst"]` and `state.stage_timings_ms["libcst"]`.
   - Delete `libcst` source names in agreement maps.
3. `tools/cq/search/smart_search.py`
   - Delete default telemetry buckets keyed by `libcst`.
4. `tools/cq/README.md`
   - Delete LibCST stage documentation row.

## 4.3 Legacy Strings and Symbol Names to Purge

1. `"libcst"` stage keys in Python enrichment telemetry and status maps.
2. `ensure_libcst_wrapper` symbol references.
3. `from tools.cq.search.libcst_python import ...` imports.

## 4.4 Deletion Validation Gates

1. `rg -n "libcst" tools/cq/search tools/cq/README.md` returns no runtime stage/module references.
2. No import references to deleted module remain.
3. Unit tests and goldens pass with `python_resolution` as canonical stage.

---

## 5. Implementation Checklist (End-to-End)

## 5.1 Planning and Contracts

1. [ ] Freeze target output contract (no `libcst` stage names).
2. [ ] Approve `python_resolution` stage naming and field set.

## 5.2 Provider and Session

1. [ ] Add `python_native_resolution.py`.
2. [ ] Refactor `PythonAnalysisSession` and remove LibCST wrapper code.
3. [ ] Implement byte-range native resolution pipeline.

## 5.3 Enrichment Pipeline Cutover

1. [ ] Replace provider import/use in `python_enrichment.py`.
2. [ ] Replace stage runner and state keys.
3. [ ] Update cross-source agreement logic.

## 5.4 Query Packs and Supplemental Signal

1. [ ] Add/expand tree-sitter Python query packs for resolution fallback.
2. [ ] Keep query pack lint and capture budgets enforced.

## 5.5 Contracts, Telemetry, and Rendering

1. [ ] Rename telemetry stages to include only `python_resolution`.
2. [ ] Verify normalized payload partitioning remains stable.
3. [ ] Validate markdown/report rendering quality.

## 5.6 Tests and Goldens

1. [ ] Add new provider-specific unit tests.
2. [ ] Update existing enrichment and smart-search tests.
3. [ ] Update goldens for stage/source renames.

## 5.7 Decommission and Cleanup

1. [ ] Delete `tools/cq/search/libcst_python.py`.
2. [ ] Remove all residual in-file LibCST code paths.
3. [ ] Remove CQ LibCST docs and dependency references.

## 5.8 Quality Gate

1. [ ] `uv run ruff format`
2. [ ] `uv run ruff check --fix`
3. [ ] `uv run pyrefly check`
4. [ ] `uv run pyright`
5. [ ] `uv run pytest -q`

---

## 6. Acceptance Criteria

1. `tools/cq` contains zero LibCST runtime imports or stage keys.
2. Python enrichment still emits complete `resolution` section fields.
3. Telemetry and summary surfaces contain `python_resolution`, not `libcst`.
4. Tests and goldens pass with no compatibility adapters.
5. Deletion ledger is fully completed in the same implementation wave.

