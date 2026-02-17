# CQ Design Improvements - Consolidated Implementation Plan v1

**Date**: 2026-02-16
**Scope**: tools/cq codebase (~78,500 LOC)
**Source**: 7 design review documents

## Scope Summary

This plan synthesizes design improvements across the tools/cq codebase, consolidating findings from 7 design review documents covering:

- **Core modules** (front_door_builders, render_summary, schema, cache infrastructure)
- **Query execution** (executor.py, execution_context, query cache)
- **Search pipeline** (smart_search, phase modules, Rust enrichment)
- **Orchestration** (runner, multilang_orchestrator, Q-step handling)
- **CLI interface** (result rendering, type converters, options)
- **Macro system** (calls analysis, impact tracing, target metadata)

The plan addresses **53 scope items** organized into 6 implementation waves, plus cross-scope decommission batches.
Wave 6 captures previously omitted high-signal findings from the 7 source reviews (CLI/run quick wins, search CQS fixes, query boundary cleanup, and Rust lane observability hardening).

## Design Principles

1. **Hard cutover contracts** - No compatibility shims, callers migrate in the same PR
2. **Single source of truth** - Constants, kind registries, error boundaries consolidated
3. **Typed boundaries first** - Replace `dict[str, object]` where fields are known
4. **Strict dependency direction** - Core/library modules must not depend on CLI or search-adapter internals
5. **Inject, do not resolve inline** - Remove global state, thread context explicitly
6. **Library-native implementation** - Prefer msgspec/diskcache/tree-sitter built-ins over bespoke patterns
7. **Compositional decomposition** - Split by responsibility, not file size
8. **Verified file manifests** - All file paths validated before implementation

## Current Baseline

**Codebase Statistics:**
- Total LOC: ~78,500
- Core modules: ~15,000 LOC
- Query system: ~12,000 LOC
- Search pipeline: ~18,000 LOC
- Macro system: ~8,000 LOC
- CLI interface: ~5,000 LOC

**Key Issues Identified:**
- **Correctness risks**: Triple-lock partial-failure leak in coordination.py
- **Duplication**: 15+ functions duplicated across modules
- **God modules**: smart_search.py (1981 LOC), executor.py (1223 LOC), runtime.py (1080 LOC)
- **Type safety gaps**: `dict[str, object]` enrichment payloads, `object | None` typed fields
- **Protocol bypasses**: 3+ sites using `getattr(backend, "cache", None)`
- **Dependency inversions**: Core→query imports, run→cli_app imports
- **Global state**: `_RENDER_ENRICHMENT_PORT_STATE`, cache context resolution fallbacks
- **CQS violations**: Mutations in query functions (assign_result_finding_ids, attach_target_metadata)

---

## Wave 1: Quick Wins (S1-S18)

### S1. Fix coordination correctness risk

**Goal**: Fix triple-lock partial-failure leak in coordination.py where manual acquire/release can leave locks held on exceptions.

**Representative Code Snippets**:

Current (incorrect):
```python
# tools/cq/core/cache/coordination.py:58-77
semaphore.acquire()
lock.acquire()
rlock.acquire()
try:
    yield
finally:
    with suppress(Exception):
        rlock.release()
    with suppress(Exception):
        lock.release()
    with suppress(Exception):
        semaphore.release()
```

Target (correct):
```python
# tools/cq/core/cache/coordination.py
with semaphore:
    with lock:
        with rlock:
            yield
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/cache/coordination.py` (lines 58-77)

**New Files to Create**:
- None (fix in place)

**Legacy Decommission/Delete Scope**:
- Delete manual `acquire()`/`release()` ceremony (lines 58-77)
- Delete `suppress(Exception)` wrappers in finally block

---

### S2. Consolidate _summary_value helper

**Goal**: Unify two different `_summary_value` implementations with conflicting type-check order into single canonical helper in render_utils.py.

**Representative Code Snippets**:

Current duplicates:
```python
# tools/cq/core/front_door_builders.py:46-49
def _summary_value(summary: CqSummary | Mapping[str, object], key: str) -> object:
    if isinstance(summary, CqSummary):
        return getattr(summary, key, None)
    return summary.get(key)

# tools/cq/core/render_summary.py:58-61
def _summary_value(summary: CqSummary | dict[str, object], key: str) -> object:
    if isinstance(summary, dict):
        return summary.get(key)
    return getattr(summary, key, None)
```

Target (canonical):
```python
# tools/cq/core/render_utils.py (add to existing module)
def summary_value(summary: CqSummary | Mapping[str, object], key: str) -> object:
    """Extract value from summary, supporting both struct and dict forms.

    Parameters
    ----------
    summary : CqSummary | Mapping[str, object]
        Summary in struct or dict form
    key : str
        Attribute/key name

    Returns
    -------
    object
        Value or None if not found
    """
    if isinstance(summary, CqSummary):
        return getattr(summary, key, None)
    return summary.get(key)
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/render_utils.py` (add function, update `__all__`)
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/front_door_builders.py` (import and use)
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/render_summary.py` (import and use)

**New Files to Create**:
- None (existing module)

**Legacy Decommission/Delete Scope**:
- Delete `_summary_value` from front_door_builders.py (line 46-49)
- Delete `_summary_value` from render_summary.py (line 58-61)

---

### S3. Consolidate _result_match_count

**Goal**: Unify two match-count extraction functions with different logic (one checks total_matches first, other checks matches first) into single canonical implementation.

**Representative Code Snippets**:

Current duplicates:
```python
# tools/cq/run/q_step_collapsing.py:79-88
def _result_match_count(result: CqResult | None) -> int:
    if result is None:
        return 0
    if result.summary.total_matches is not None:
        return result.summary.total_matches
    if result.summary.matches is not None:
        return result.summary.matches
    return len(result.key_findings)

# tools/cq/orchestration/multilang_orchestrator.py:152-159
def _result_match_count(result: CqResult | None) -> int:
    if result is None:
        return 0
    if result.summary.matches is not None:
        return result.summary.matches
    if result.summary.total_matches is not None:
        return result.summary.total_matches
    return 0
```

Target (canonical):
```python
# tools/cq/core/summary_contract.py (add to existing module)
def extract_match_count(result: CqResult | None) -> int:
    """Extract match count from result, with fallback chain.

    Canonical order: total_matches → matches → len(key_findings) → 0

    Parameters
    ----------
    result : CqResult | None
        Query result

    Returns
    -------
    int
        Match count (0 if None or no matches found)
    """
    if result is None:
        return 0
    if result.summary.total_matches is not None:
        return result.summary.total_matches
    if result.summary.matches is not None:
        return result.summary.matches
    return len(result.key_findings)
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/summary_contract.py` (add function)
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/q_step_collapsing.py` (import and use)
- `/Users/paulheyse/CodeAnatomy/tools/cq/orchestration/multilang_orchestrator.py` (import and use)

**New Files to Create**:
- None (existing module)

**Legacy Decommission/Delete Scope**:
- Delete `_result_match_count` from q_step_collapsing.py (line 79-88)
- Delete `_result_match_count` from multilang_orchestrator.py (line 152-159)

---

### S4. Replace inline boolean parsing with env_bool

**Goal**: Replace 3 sites of inline bool parsing with canonical `env_bool` helper from env_namespace.py.

**Representative Code Snippets**:

Current (inline parsing):
```python
# tools/cq/core/cache/cache_runtime_tuning.py:46-51
val = os.getenv("CQ_CACHE_DISABLE_WRITE", "").lower()
disable = val in ("1", "true", "yes")

# tools/cq/core/cache/diagnostics.py:68-72
val = os.getenv("CQ_CACHE_DIAGNOSTICS", "").lower()
enabled = val in ("1", "true", "yes")
```

Target (using env_bool):
```python
from tools.cq.core.runtime.env_namespace import env_bool

# tools/cq/core/cache/cache_runtime_tuning.py
disable = env_bool("CQ_CACHE_DISABLE_WRITE", default=False)

# tools/cq/core/cache/diagnostics.py
enabled = env_bool("CQ_CACHE_DIAGNOSTICS", default=False)
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/cache/cache_runtime_tuning.py` (lines 46-51)
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/cache/diagnostics.py` (lines 68-72)
- Search codebase for other inline `os.getenv(...).lower() in ("1", "true", ...)` patterns

**New Files to Create**:
- None (env_bool already exists at tools/cq/core/runtime/env_namespace.py:38-51)

**Legacy Decommission/Delete Scope**:
- Delete inline bool parsing logic
- Delete `.lower()` and `in ("1", "true", "yes")` patterns

---

### S5. Fix unnecessary touch() protocol bypass

**Goal**: Use `backend.touch()` directly instead of extracting raw cache via `getattr(backend, "cache", None)` to bypass the protocol.

**Representative Code Snippets**:

Current (protocol bypass):
```python
# tools/cq/core/cache/search_artifact_store.py:298-308
def _touch_cached_entry(backend: CqCacheBackend, key: str) -> None:
    raw_cache = getattr(backend, "cache", None)
    if raw_cache is not None and hasattr(raw_cache, "touch"):
        raw_cache.touch(key, expire=CACHE_TTL_SECONDS)
```

Target (protocol-compliant):
```python
# tools/cq/core/cache/search_artifact_store.py
def _touch_cached_entry(backend: CqCacheBackend, key: str) -> None:
    backend.touch(key, expire=CACHE_TTL_SECONDS)
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/cache/search_artifact_store.py` (lines 298-308)

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- Delete `getattr(backend, "cache", None)` pattern
- Delete `hasattr(raw_cache, "touch")` guard

---

### S6. Consolidate node_payload() and is_variadic_separator()

**Goal**: Deduplicate node_payload() and is_variadic_separator() functions between executor_ast_grep.py and sgpy_scanner.py.

**Representative Code Snippets**:

Current duplicates:
```python
# tools/cq/astgrep/sgpy_scanner.py:412-423
def _node_payload(node: SgNode) -> dict[str, object]:
    return {
        "kind": node.kind(),
        "text": node.text(),
        "range": node.range(),
    }

def _is_variadic_separator(node: SgNode) -> bool:
    return node.kind() == "list_splat_pattern"

# tools/cq/query/executor_ast_grep.py:810-831
def _node_payload(node: SgNode) -> dict[str, object]:
    # Identical implementation
    ...
def _is_variadic_separator(node: SgNode) -> bool:
    # Identical implementation
    ...
```

Target (canonical in sgpy_scanner.py):
```python
# tools/cq/astgrep/sgpy_scanner.py
def node_payload(node: SgNode) -> dict[str, object]:
    """Extract node payload for metavar capture.

    Parameters
    ----------
    node : SgNode
        AST grep node

    Returns
    -------
    dict[str, object]
        Node payload with kind, text, range
    """
    return {
        "kind": node.kind(),
        "text": node.text(),
        "range": node.range(),
    }

def is_variadic_separator(node: SgNode) -> bool:
    """Check if node is a variadic separator (list_splat_pattern).

    Parameters
    ----------
    node : SgNode
        AST grep node

    Returns
    -------
    bool
        True if variadic separator
    """
    return node.kind() == "list_splat_pattern"

__all__ = [
    # ... existing exports
    "node_payload",
    "is_variadic_separator",
]
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/astgrep/sgpy_scanner.py` (rename functions, update `__all__`)
- `/Users/paulheyse/CodeAnatomy/tools/cq/query/executor_ast_grep.py` (import from sgpy_scanner)

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- Delete `_node_payload` from executor_ast_grep.py (line ~810)
- Delete `_is_variadic_separator` from executor_ast_grep.py (line ~820)

---

### S7. Type cache_backend and symtable_enricher on ExecutionContext

**Goal**: Replace `object | None` with typed fields on ExecutionContext to improve type safety.

**Representative Code Snippets**:

Current (untyped):
```python
# tools/cq/query/execution_context.py:29-30
class ExecutionContext(msgspec.Struct):
    cache_backend: object | None = None
    symtable_enricher: object | None = None
```

Target (typed):
```python
# tools/cq/query/execution_context.py
from tools.cq.core.cache.interface import CqCacheBackend
from tools.cq.macros.symtable_enricher import SymtableEnricher

class ExecutionContext(msgspec.Struct):
    cache_backend: CqCacheBackend | None = None
    symtable_enricher: SymtableEnricher | None = None
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/query/execution_context.py` (lines 29-30, add imports)

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- Replace `object | None` with typed fields

---

### S8. Unify decorator kind checking

**Goal**: Replace hardcoded decorator kind set with canonical `ENTITY_KINDS.decorator_kinds` from entity_kinds.py.

**Representative Code Snippets**:

Current (hardcoded):
```python
# tools/cq/query/executor.py:1074-1080
if def_record.kind not in {
    "function",
    "async_function",
    "function_typeparams",
    "class",
    "class_bases",
}:
    continue
```

Target (canonical):
```python
# tools/cq/query/executor.py
from tools.cq.core.entity_kinds import ENTITY_KINDS

if def_record.kind not in ENTITY_KINDS.decorator_kinds:
    continue
```

Note: `ENTITY_KINDS.decorator_kinds` already exists as a property returning `function_kinds | class_kinds` (entity_kinds.py:41-44). The hardcoded set `{"function", "async_function", "function_typeparams", "class", "class_bases"}` is a subset of `decorator_kinds` (which also includes `class_typeparams`, `class_typeparams_bases`, `struct`, `enum`, `trait`). The scope expansion is intentional — Rust kinds are included.

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/query/executor.py` (lines 1074-1080)

**New Files to Create**:
- None (`decorator_kinds` property already exists at entity_kinds.py:41-44)

**Legacy Decommission/Delete Scope**:
- Delete inline `{"function", "async_function", "function_typeparams", "class", "class_bases"}` set literal from executor.py:1074-1080

---

### S9. Move QueryLanguage to core/types.py

**Goal**: Fix core→query dependency inversion by moving QueryLanguage type alias to core/types.py.

**Representative Code Snippets**:

Current (dependency inversion):
```python
# tools/cq/core/render_enrichment_orchestrator.py:15
from tools.cq.query.language import QueryLanguage  # core depends on query!
```

Target (correct direction):
```python
# tools/cq/core/types.py (add)
QueryLanguage = Literal["python", "rust"]
QueryLanguageScope = Literal["auto", "python", "rust"]

# tools/cq/query/language.py
from tools.cq.core.types import QueryLanguage, QueryLanguageScope

# tools/cq/core/render_enrichment_orchestrator.py
from tools.cq.core.types import QueryLanguage
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/types.py` (add QueryLanguage + QueryLanguageScope)
- `/Users/paulheyse/CodeAnatomy/tools/cq/query/language.py` (import from core/types and remove local alias definitions)
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/render_enrichment_orchestrator.py` (update import)
- Search codebase for all `from tools.cq.query.language import QueryLanguage` and update

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- Delete `QueryLanguage` alias definition from `tools/cq/query/language.py`
- Delete `QueryLanguageScope` alias definition from `tools/cq/query/language.py`
- Update all importers to use `tools/cq/core/types.py` in the same PR (hard cutover, no shim)

---

### S10. Fix RunOptions import in run/loader.py

**Goal**: Fix run→cli_app dependency inversion by defining minimal RunLoadInput struct in run/spec.py.

**Representative Code Snippets**:

Current (dependency inversion):
```python
# tools/cq/run/loader.py:11
from tools.cq.cli_app.options import RunOptions  # run depends on cli_app!
```

Target (correct direction):
```python
# tools/cq/run/spec.py (add)
class RunLoadInput(msgspec.Struct, frozen=True):
    """Input for run plan loading (minimal subset of RunOptions)."""
    plan_path: str | None = None
    root: str | None = None
    verbose: int = 0

# tools/cq/run/loader.py
from tools.cq.run.spec import RunLoadInput

def load_run_plan(input: RunLoadInput) -> RunPlan:
    ...

# tools/cq/cli_app/commands/run.py (adapter)
from tools.cq.cli_app.options import RunOptions
from tools.cq.run.spec import RunLoadInput

def run_command(options: RunOptions) -> None:
    load_input = RunLoadInput(
        plan_path=options.plan_path,
        root=options.root,
        verbose=options.verbose,
    )
    plan = load_run_plan(load_input)
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/spec.py` (add RunLoadInput)
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/loader.py` (use RunLoadInput)
- CLI commands that call loader (create adapter)

**New Files to Create**:
- None (spec.py likely exists)

**Legacy Decommission/Delete Scope**:
- Remove `from tools.cq.cli_app.options import RunOptions` from run/loader.py

---

### S11. Merge comma_separated_enum into comma_separated_list

**Goal**: Consolidate near-identical converter factories in cli_app/types.py while preserving current CQ behavior (both comma-delimited tokens and repeated option flags).

**Representative Code Snippets**:

Current (duplication):
```python
# tools/cq/cli_app/types.py:190-270
# comma_separated_list and comma_separated_enum are nearly identical functions
# that differ only in validation logic (enum validates against allowed values).
# Both parse comma-separated strings into lists.
```

Target (single converter factory, enum handled by constructor):
```python
# tools/cq/cli_app/types.py
def comma_separated_list[T](type_: Callable[[str], T]) -> Callable[..., list[T]]:
    """Cyclopts converter factory for comma-delimited and repeated values."""

    def convert(*args: object) -> list[T]:
        value = _converter_value(args)
        return [type_(item) for item in _iter_token_values(value)]

    convert.__dict__["__cyclopts_converter__"] = True
    return convert

# Example enum usage (no dedicated enum converter required):
# include = comma_separated_list(QueryLanguageScope)
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/types.py` (lines 190-270)
- Search for all uses of `comma_separated_enum` and replace with `comma_separated_list`

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- Delete `comma_separated_enum` function
- Keep comma-delimited parsing behavior (do not replace with cyclopts default list coercion)

---

### S12. Eliminate _find_ancestor duplication

**Goal**: Remove triplicated ancestor-walking code by making extractors_shared.find_ancestor canonical with RustNodeAccess.

**Representative Code Snippets**:

Current duplicates:
```python
# tools/cq/search/tree_sitter/rust_lane/runtime.py:272-296
def _find_ancestor(node, predicate):
    current = node.parent
    while current:
        if predicate(current):
            return current
        current = current.parent
    return None

# tools/cq/search/tree_sitter/rust_lane/role_classification.py:51-75
def _find_ancestor(node, predicate):
    # Identical implementation
    ...

# tools/cq/search/rust/extractors_shared.py:96-111
def _find_ancestor(node: RustNodeAccess, predicate):
    # Uses RustNodeAccess protocol
    ...
```

Target (canonical with adapter):
```python
# tools/cq/search/rust/extractors_shared.py (canonical)
def find_ancestor(node: RustNodeAccess, predicate: Callable[[RustNodeAccess], bool]) -> RustNodeAccess | None:
    """Find ancestor node matching predicate.

    Parameters
    ----------
    node : RustNodeAccess
        Starting node
    predicate : Callable[[RustNodeAccess], bool]
        Predicate function

    Returns
    -------
    RustNodeAccess | None
        Matching ancestor or None
    """
    current = node.parent
    while current:
        if predicate(current):
            return current
        current = current.parent
    return None

# tools/cq/search/tree_sitter/rust_lane/runtime.py (adapter)
from tools.cq.search.rust.extractors_shared import find_ancestor
from tools.cq.search.rust.node_access import TreeSitterRustNodeAccess

def some_function(node):
    wrapped = TreeSitterRustNodeAccess(node)
    ancestor = find_ancestor(wrapped, lambda n: n.kind == "function_item")
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/rust/extractors_shared.py` (rename to public, update `__all__`)
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/rust_lane/runtime.py` (import and adapt)
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/rust_lane/role_classification.py` (import and adapt)
- Create or verify `tools/cq/search/rust/node_access.py` for TreeSitterRustNodeAccess adapter

**New Files to Create**:
- `tools/cq/search/rust/node_access.py` (if not exists) - TreeSitterRustNodeAccess adapter
- `tests/unit/cq/search/rust/test_node_access.py` - Adapter tests

**Legacy Decommission/Delete Scope**:
- Delete `_find_ancestor` from runtime.py (line 272-296)
- Delete `_find_ancestor` from role_classification.py (line 51-75)

---

### S13. Remove underscore prefix from public exports

**Goal**: Fix naming convention violation by removing underscore prefix from 10 public exports in Rust enrichment modules.

**Representative Code Snippets**:

Current (private naming):
```python
# tools/cq/search/tree_sitter/rust_lane/enrichment_extractors.py
def _extract_visibility(node):
    ...

def _extract_trait_bounds(node):
    ...

__all__ = [
    "_extract_visibility",
    "_extract_trait_bounds",
    # ... 7 more _extract_* functions
]
```

Target (public naming):
```python
# tools/cq/search/tree_sitter/rust_lane/enrichment_extractors.py
def extract_visibility(node):
    """Extract visibility modifier from Rust node.

    Parameters
    ----------
    node : RustNodeAccess
        Rust AST node

    Returns
    -------
    str | None
        Visibility modifier or None
    """
    ...

def extract_trait_bounds(node):
    ...

__all__ = [
    "extract_visibility",
    "extract_trait_bounds",
    # ... 7 more extract_* functions
]
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/rust_lane/enrichment_extractors.py` (rename 9 functions, update `__all__`)
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/rust_lane/role_classification.py` (rename `_classify_item_role` → `classify_item_role`, update `__all__`)
- All importers of these functions

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- Update all importers to use new names

---

### S14. Make CallSite frozen

**Goal**: Enforce immutability on CallSite struct by adding `frozen=True`.

**Representative Code Snippets**:

Current (mutable):
```python
# tools/cq/macros/calls/analysis.py:49
class CallSite(msgspec.Struct):
    location: str
    caller: str
    context: str | None = None
```

Target (immutable):
```python
# tools/cq/macros/calls/analysis.py:49
class CallSite(msgspec.Struct, frozen=True):
    location: str
    caller: str
    context: str | None = None

    def __post_init__(self) -> None:
        """Normalize fields after construction."""
        # If normalization needed, use msgspec.structs.force_setattr
        if self.context and self.context.strip() == "":
            msgspec.structs.force_setattr(self, "context", None)
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/macros/calls/analysis.py` (line 49)

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- Any code that mutates CallSite instances must use `msgspec.structs.replace()` instead

---

### S15. Extract shared comprehension taint handler

**Goal**: Deduplicate 4 near-identical comprehension taint functions in impact.py.

**Representative Code Snippets**:

Current (4 duplicates):
```python
# tools/cq/macros/impact.py:355-394
def _handle_list_comprehension_taint(node, tainted_check):
    if isinstance(node.elt, cst.Name) and tainted_check(node.elt.value):
        return True
    return any(...)

def _handle_set_comprehension_taint(node, tainted_check):
    if isinstance(node.elt, cst.Name) and tainted_check(node.elt.value):
        return True
    return any(...)

def _handle_dict_comprehension_taint(node, tainted_check):
    # key/value instead of elt
    ...

def _handle_generator_exp_taint(node, tainted_check):
    # Identical to list comprehension
    ...
```

Target (unified):
```python
# tools/cq/macros/impact.py
def _tainted_comprehension(
    node: cst.BaseExpression,
    tainted_check: Callable[[str], bool],
    *,
    element_accessor: str = "elt",
) -> bool:
    """Check if comprehension element or generators are tainted.

    Parameters
    ----------
    node : cst.BaseExpression
        Comprehension node
    tainted_check : Callable[[str], bool]
        Function to check if name is tainted
    element_accessor : str
        Attribute name for element ("elt" for list/set/generator, "key"/"value" for dict)

    Returns
    -------
    bool
        True if tainted
    """
    element = getattr(node, element_accessor, None)
    if isinstance(element, cst.Name) and tainted_check(element.value):
        return True
    return any(
        _is_tainted_generator(gen, tainted_check)
        for gen in node.for_in.generators
    )

# Register for all types
_COMPREHENSION_HANDLERS = {
    cst.ListComp: lambda n, check: _tainted_comprehension(n, check, element_accessor="elt"),
    cst.SetComp: lambda n, check: _tainted_comprehension(n, check, element_accessor="elt"),
    cst.GeneratorExp: lambda n, check: _tainted_comprehension(n, check, element_accessor="elt"),
    cst.DictComp: lambda n, check: _tainted_comprehension(n, check, element_accessor="key"),
}
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/macros/impact.py` (lines 355-394)

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- Delete `_handle_list_comprehension_taint`
- Delete `_handle_set_comprehension_taint`
- Delete `_handle_dict_comprehension_taint`
- Delete `_handle_generator_exp_taint`

---

### S16. Fix list[object] typing in search pipeline

**Goal**: Replace `list[object]` with typed `list[LanguageSearchResult]` in smart_search_types.py.

**Representative Code Snippets**:

Current (untyped):
```python
# tools/cq/search/pipeline/smart_search_types.py:333-338
@dataclass(frozen=True, slots=True)
class SearchResultAssembly:
    """Assembly inputs for package-level orchestrators."""
    context: SearchConfig
    partition_results: list[object]  # Untyped!
```

Target (typed):
```python
# tools/cq/search/pipeline/smart_search_types.py
@dataclass(frozen=True, slots=True)
class SearchResultAssembly:
    """Assembly inputs for package-level orchestrators."""
    context: SearchConfig
    partition_results: list[LanguageSearchResult]
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search_types.py` (lines 333-338)

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- Replace `list[object]` with typed list

---

### S17. Add schema_version to CqSummary

**Goal**: Version the most-serialized contract to support future schema evolution.

**Representative Code Snippets**:

Current (no version):
```python
# tools/cq/core/summary_contract.py:45
class CqSummary(msgspec.Struct, omit_defaults=True):
    # 150+ fields
    total_matches: int | None = None
    matches: int | None = None
    ...
```

Target (versioned):
```python
# tools/cq/core/summary_contract.py:45
class CqSummary(msgspec.Struct, omit_defaults=True):
    """Summary statistics for CQ results.

    Schema version: 1
    """
    schema_version: int = 1
    total_matches: int | None = None
    matches: int | None = None
    ...
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/summary_contract.py` (line 45)

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- None (additive change)

---

### S18. Rename tree_sitter_blob_store.py

**Goal**: Fix misleading module name (not tree-sitter specific).

**Representative Code Snippets**:

Current:
```python
# tools/cq/core/cache/tree_sitter_blob_store.py
# Generic blob storage, not tree-sitter specific
```

Target:
```python
# tools/cq/core/cache/blob_store.py
# Generic blob storage
```

**Files to Edit**:
- Rename `/Users/paulheyse/CodeAnatomy/tools/cq/core/cache/tree_sitter_blob_store.py` → `blob_store.py`
- Update all importers

**New Files to Create**:
- None (rename)

**Legacy Decommission/Delete Scope**:
- Delete old filename after all importers updated

---

## Wave 2: Knowledge Consolidation (S19-S24)

### S19. Entity-kind registry consolidation

**Goal**: Extend EntityKindRegistry in entity_kinds.py with record-type mappings and matches() method to replace scattered hardcoded sets.

**Representative Code Snippets**:

Current (scattered):
```python
# tools/cq/query/executor_definitions.py:293-330
def matches_entity(entity: str, record_kind: str) -> bool:
    if entity == "function":
        return record_kind in ("function", "async_function", "method", ...)
    if entity == "class":
        return record_kind in ("class", "dataclass", ...)
    # 20+ more branches

# tools/cq/query/executor.py:1074-1080
DECORATOR_KINDS = {"decorator", "class_decorator", "function_decorator"}

# tools/cq/query/planner.py:241-266
RECORD_TYPE_MAPPINGS = {
    "function": ["function", "async_function", ...],
    "class": ["class", "dataclass", ...],
    ...
}
```

Target (extend existing frozen struct):
```python
# tools/cq/core/entity_kinds.py (extend existing)
# Current EntityKindRegistry is a frozen msgspec.Struct with 3 kind sets:
#   function_kinds: {"function", "async_function", "function_typeparams"}
#   class_kinds: {"class", "class_bases", "class_typeparams", "class_typeparams_bases",
#                  "struct", "enum", "trait"}
#   import_kinds: {"import", "import_as", "from_import", "from_import_as",
#                   "from_import_multi", "from_import_paren", "use_declaration"}
# And a decorator_kinds property: function_kinds | class_kinds

class EntityKindRegistry(msgspec.Struct, frozen=True):
    """Immutable registry of normalized entity kind sets."""

    function_kinds: frozenset[str] = frozenset({
        "function", "async_function", "function_typeparams",
    })
    class_kinds: frozenset[str] = frozenset({
        "class", "class_bases", "class_typeparams",
        "class_typeparams_bases", "struct", "enum", "trait",
    })
    import_kinds: frozenset[str] = frozenset({
        "import", "import_as", "from_import", "from_import_as",
        "from_import_multi", "from_import_paren", "use_declaration",
    })

    # NEW: Record-type mappings (from planner.py _ENTITY_RECORDS)
    _entity_records: dict[str, frozenset[str]] = {
        "function": frozenset({"def"}),
        "class": frozenset({"def"}),
        "method": frozenset({"def"}),
        "module": frozenset({"def"}),
        "decorator": frozenset({"def"}),
        "callsite": frozenset({"call"}),
        "import": frozenset({"import"}),
    }

    def matches(self, entity_type: str, record_kind: str) -> bool:
        """Check if record_kind matches entity_type.

        Replaces the if/elif chain in executor_definitions.py:293-330.

        Parameters
        ----------
        entity_type : str
            Entity type filter (function, class, import, etc.)
        record_kind : str
            Record kind from extraction

        Returns
        -------
        bool
            True if matches
        """
        if entity_type == "function":
            return record_kind in self.function_kinds
        if entity_type == "class":
            return record_kind in self.class_kinds
        if entity_type == "import":
            return record_kind in self.import_kinds
        if entity_type == "decorator":
            return record_kind in self.decorator_kinds
        return False

    @property
    def decorator_kinds(self) -> frozenset[str]:
        """Kinds that can carry decorators."""
        return self.function_kinds | self.class_kinds

ENTITY_KINDS = EntityKindRegistry()
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/entity_kinds.py` (add matches() method, record mappings)
- `/Users/paulheyse/CodeAnatomy/tools/cq/query/executor_definitions.py` (replace matches_entity() chain)
- `/Users/paulheyse/CodeAnatomy/tools/cq/query/executor.py` (use ENTITY_KINDS.decorator_kinds)
- `/Users/paulheyse/CodeAnatomy/tools/cq/query/planner.py` (remove RECORD_TYPE_MAPPINGS)

**New Files to Create**:
- `tests/unit/cq/core/test_entity_kind_registry.py` - Test matches() logic

**Legacy Decommission/Delete Scope**:
- Delete `matches_entity()` from executor_definitions.py (lines 293-330)
- Delete `DECORATOR_KINDS` from executor.py (lines 1074-1080)
- Delete `RECORD_TYPE_MAPPINGS` from planner.py (lines 241-266)

---

### S20. Unify Rust role classification

**Goal**: Create single canonical `classify_rust_item_role()` in extractors_shared.py by merging role maps from role_classification.py and enrichment.py.

**Representative Code Snippets**:

Current (scattered):
```python
# tools/cq/search/tree_sitter/rust_lane/role_classification.py:23-31
_ITEM_ROLE_SIMPLE: dict[str, str] = {
    "use_declaration": "use_import",
    "macro_invocation": "macro_call",
    "field_declaration": "struct_field",
    "enum_variant": "enum_variant",
    "const_item": "const_item",
    "type_item": "type_alias",
    "static_item": "static_item",
}

# tools/cq/search/rust/enrichment.py:198-202
_NON_FUNCTION_ROLE_BY_KIND: dict[str, str] = {
    "use_declaration": "use_import",        # DUPLICATED from role_classification.py
    "field_declaration": "struct_field",     # DUPLICATED from role_classification.py
    "enum_variant": "enum_variant",         # DUPLICATED from role_classification.py
}
```

Target (canonical):
```python
# tools/cq/search/rust/extractors_shared.py
_RUST_ITEM_ROLE_MAP: dict[str, str] = {
    # Merged from role_classification.py:_ITEM_ROLE_SIMPLE (7 entries)
    # + enrichment.py:_NON_FUNCTION_ROLE_BY_KIND (3 entries, all duplicates)
    "use_declaration": "use_import",
    "macro_invocation": "macro_call",
    "field_declaration": "struct_field",
    "enum_variant": "enum_variant",
    "const_item": "const_item",
    "type_item": "type_alias",
    "static_item": "static_item",
}

def classify_rust_item_role(node: RustNodeAccess) -> str | None:
    """Classify Rust item role from node kind.

    Parameters
    ----------
    node : RustNodeAccess
        Rust AST node

    Returns
    -------
    str | None
        Role classification or None
    """
    kind = node.kind
    return _RUST_ITEM_ROLE_MAP.get(kind)
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/rust/extractors_shared.py` (add classify_rust_item_role)
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/rust_lane/role_classification.py` (import canonical)
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/rust/enrichment.py` (import canonical)

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- Delete `_ITEM_ROLE_SIMPLE` from role_classification.py
- Delete `_NON_FUNCTION_ROLE_BY_KIND` from enrichment.py

---

### S21. Metavar extraction unification

**Goal**: Make executor_ast_grep.extract_match_metavars() canonical, have sgpy_scanner.py import it.

**Representative Code Snippets**:

Current (duplication):
```python
# tools/cq/query/executor_ast_grep.py
def extract_match_metavars(match: SgNode) -> dict[str, object]:
    metavars = {}
    for name in match.get_match_names():
        node = match.get_match(name)
        if node:
            metavars[name] = node_payload(node)
    return metavars

# tools/cq/astgrep/sgpy_scanner.py
def extract_metavars(match: SgNode) -> dict[str, object]:
    # Identical implementation
    ...
```

Target (canonical):
```python
# tools/cq/query/executor_ast_grep.py (keep as canonical)
def extract_match_metavars(match: SgNode) -> dict[str, object]:
    """Extract metavariables from ast-grep match.

    Parameters
    ----------
    match : SgNode
        AST grep match node

    Returns
    -------
    dict[str, object]
        Metavariable bindings
    """
    metavars = {}
    for name in match.get_match_names():
        node = match.get_match(name)
        if node:
            metavars[name] = node_payload(node)
    return metavars

# tools/cq/astgrep/sgpy_scanner.py (import)
from tools.cq.query.executor_ast_grep import extract_match_metavars
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/astgrep/sgpy_scanner.py` (import canonical, remove duplicate)
- `/Users/paulheyse/CodeAnatomy/tools/cq/query/executor_ast_grep.py` (ensure exported in `__all__`)

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- Delete `extract_metavars` from sgpy_scanner.py

---

### S22. Extract generic cache decode function

**Goal**: Consolidate identical msgspec.convert() patterns in cache modules into single decode_cached_payload[T]() function.

**Representative Code Snippets**:

Current (duplication):
```python
# tools/cq/core/cache/tree_sitter_cache_store.py
raw = backend.get(key)
if raw:
    payload = msgspec.json.decode(raw)
    return msgspec.convert(payload, TargetType, strict=False)

# tools/cq/core/cache/search_artifact_store.py
raw = backend.get(key)
if raw:
    payload = msgspec.json.decode(raw)
    return msgspec.convert(payload, OtherType, strict=False)
```

Target (canonical):
```python
# tools/cq/core/cache/cache_decode.py (new module)
from __future__ import annotations

from typing import TypeVar

import msgspec

T = TypeVar("T")

def decode_cached_payload(
    raw: bytes | None,
    target_type: type[T],
    *,
    strict: bool = True,
) -> T | None:
    """Decode cached payload to target type.

    Parameters
    ----------
    raw : bytes | None
        Raw cached bytes
    target_type : type[T]
        Target struct type
    strict : bool
        Strict conversion mode (default: True)

    Returns
    -------
    T | None
        Decoded payload or None
    """
    if raw is None:
        return None
    payload = msgspec.json.decode(raw)
    return msgspec.convert(payload, target_type, strict=strict)
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/cache/tree_sitter_cache_store.py` (use decode_cached_payload)
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/cache/search_artifact_store.py` (use decode_cached_payload)
- Search for other msgspec.convert patterns in cache modules

**New Files to Create**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/cache/cache_decode.py` - Generic decode helper
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/core/cache/test_cache_decode.py` - Decode tests

**Legacy Decommission/Delete Scope**:
- Replace inline decode patterns with canonical helper

---

### S23. Consolidate Rust def regex

**Goal**: Extract duplicated regex pattern for Rust definition parsing into module-level constant.

**Representative Code Snippets**:

Current (duplication):
```python
# tools/cq/macros/calls_target.py:30-33 (module-level compiled regex)
_RUST_DEF_RE = re.compile(
    r"^(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?(?:const\s+)?(?:unsafe\s+)?"
    r"(?:extern(?:\s+\"[^\"]+\")?\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)\b"
)

# tools/cq/macros/calls_target.py:129-132 (inline pattern in _find_rust_definition_call_site)
pattern = (
    rf"\b(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?(?:const\s+)?(?:unsafe\s+)?"
    rf"(?:extern(?:\s+\"[^\"]+\")?\s+)?fn\s+{base_name}\s*\("
)
```

The core Rust function signature regex (`pub? async? const? unsafe? extern? fn NAME`) is duplicated. The second instance is parameterized with `base_name` and adds `\s*\(` for call-site matching.

Target (shared base pattern):
```python
# tools/cq/macros/calls_target.py (top of module)
_RUST_FN_QUALIFIERS = (
    r"(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?(?:const\s+)?(?:unsafe\s+)?"
    r"(?:extern(?:\s+\"[^\"]+\")?\s+)?"
)

# For extracting function name from line start
_RUST_DEF_RE = re.compile(
    rf"^{_RUST_FN_QUALIFIERS}fn\s+([A-Za-z_][A-Za-z0-9_]*)\b"
)

# For finding call-site of a specific function
def _rust_call_site_pattern(base_name: str) -> str:
    return rf"\b{_RUST_FN_QUALIFIERS}fn\s+{base_name}\s*\("
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/macros/calls_target.py` (lines 30-33, 130-132)

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- Delete duplicate pattern definition

---

### S24. Group risk threshold constants into struct

**Goal**: Replace 9 module-level risk threshold constants with structured RiskThresholdPolicyV1.

**Representative Code Snippets**:

Current (scattered constants):
```python
# tools/cq/core/front_door_builders.py:35-43
_DEFAULT_TOP_CANDIDATES = 3
_DEFAULT_PREVIEW_PER_SLICE = 5
_HIGH_CALLER_THRESHOLD = 10
_HIGH_CALLER_STRICT_THRESHOLD = 10
_MEDIUM_CALLER_THRESHOLD = 4
_MEDIUM_CALLER_STRICT_THRESHOLD = 3
_ARG_VARIANCE_THRESHOLD = 3
_FILES_WITH_CALLS_THRESHOLD = 3
_DEFAULT_SEMANTIC_TARGETS = 1
```

Target (structured policy):
```python
# tools/cq/core/front_door_builders.py (or front_door_contracts.py)
class InsightThresholdPolicyV1(msgspec.Struct, frozen=True):
    """Insight card thresholds for front-door analysis.

    Controls candidate selection, caller risk levels, and semantic targets.
    """
    default_top_candidates: int = 3
    default_preview_per_slice: int = 5
    high_caller_threshold: int = 10
    high_caller_strict_threshold: int = 10
    medium_caller_threshold: int = 4
    medium_caller_strict_threshold: int = 3
    arg_variance_threshold: int = 3
    files_with_calls_threshold: int = 3
    default_semantic_targets: int = 1

DEFAULT_INSIGHT_THRESHOLDS = InsightThresholdPolicyV1()
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/front_door_builders.py` (lines 37-44)
- Update all references to use `DEFAULT_RISK_THRESHOLDS.critical_caller_threshold` etc.

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- Delete 9 module-level constants

---

## Wave 3: God Module Decomposition (S25-S32)

### S25. Complete front-door decomposition

**Goal**: Decompose front_door_builders.py by responsibility into contracts, render, assembly, and risk modules.

**Representative Code Snippets**:

Current (mixed responsibilities):
```python
# tools/cq/core/front_door_builders.py (1000+ LOC)
# Schema structs
class FrontDoorInsightV1(msgspec.Struct):
    ...

# Risk assessment
def assess_risk(callers, callees):
    ...

# Rendering logic
def render_insight_card(insight):
    ...

# Assembly logic
def build_front_door_insight(data):
    ...
```

Target (decomposed):
```python
# tools/cq/core/front_door_contracts.py (canonical schema)
class FrontDoorInsightV1(msgspec.Struct, frozen=True):
    """Front-door insight card contract."""
    target: TargetIdentity
    neighborhood: NeighborhoodSummary
    risk_assessment: RiskAssessment
    confidence: ConfidenceScore

# tools/cq/core/front_door_risk.py
def assess_risk(
    callers: int,
    callees: int,
    references: int,
    imports: int,
    scope_depth: int,
    *,
    thresholds: RiskThresholdPolicyV1 = DEFAULT_RISK_THRESHOLDS,
) -> RiskAssessment:
    """Assess risk level from metrics."""
    ...

# tools/cq/core/front_door_render.py
def render_insight_card(insight: FrontDoorInsightV1) -> str:
    """Render insight card to markdown."""
    ...

# tools/cq/core/front_door_assembly.py
def build_front_door_insight(
    target: TargetIdentity,
    neighborhood: NeighborhoodSummary,
    metrics: dict[str, int],
) -> FrontDoorInsightV1:
    """Assemble front-door insight from components."""
    risk = assess_risk(
        callers=metrics["callers"],
        callees=metrics["callees"],
        ...
    )
    confidence = calculate_confidence(...)
    return FrontDoorInsightV1(
        target=target,
        neighborhood=neighborhood,
        risk_assessment=risk,
        confidence=confidence,
    )
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/front_door_builders.py` (split into 4 modules)

**Existing Facade Files** (already exist as re-export shims):
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/front_door_contracts.py` (31 lines) — Re-exports 11 structs from front_door_builders.py
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/front_door_render.py` (15 lines) — Re-exports `coerce_front_door_insight`, `render_insight_card`, `to_public_front_door_insight_dict`

These files currently import FROM `front_door_builders.py`. After decomposition, the dependency direction reverses: `front_door_contracts.py` and `front_door_render.py` become canonical implementations, then `front_door_builders.py` is deleted after same-PR caller migration.

**New Files to Create**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/front_door_risk.py` - Risk assessment logic (extracted from builders)
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/front_door_assembly.py` - Assembly logic (extracted from builders)
- `tests/unit/cq/core/test_front_door_risk.py` - Risk assessment tests
- `tests/unit/cq/core/test_front_door_assembly.py` - Assembly tests

**Legacy Decommission/Delete Scope**:
- Reverse `front_door_contracts.py` from re-export shim to canonical struct definitions
- Reverse `front_door_render.py` from re-export shim to canonical rendering implementation
- Delete `front_door_builders.py` after callers migrate in the same PR (hard cutover, no facade)
- Update all importers to use new module structure

---

### S26. Extract classify_match to classification.py

**Goal**: Break circular import in search pipeline by extracting classify_match and 8 helper functions to new classification.py module.

**Representative Code Snippets**:

Current (circular dependency):
```python
# tools/cq/search/pipeline/smart_search.py (1200+ LOC)
def classify_match(match, context):
    """Classify match type (definition, callsite, import, etc.)."""
    ...

# 8 helper functions
def _is_definition(match, context):
    ...
def _is_callsite(match, context):
    ...
# ... 6 more

# tools/cq/search/pipeline/classify_phase.py
from tools.cq.search.pipeline.smart_search import classify_match  # Lazy import!
```

Target (extracted module):
```python
# tools/cq/search/pipeline/classification.py (new)
from __future__ import annotations

def classify_match(
    match: dict[str, object],
    context: ClassificationContext,
) -> str:
    """Classify match type from context.

    Returns one of: definition, callsite, import, reference, comment, string, unknown

    Parameters
    ----------
    match : dict[str, object]
        Match payload
    context : ClassificationContext
        Classification context

    Returns
    -------
    str
        Match classification
    """
    if _is_definition(match, context):
        return "definition"
    if _is_callsite(match, context):
        return "callsite"
    if _is_import(match, context):
        return "import"
    if _is_reference(match, context):
        return "reference"
    if _is_comment(match, context):
        return "comment"
    if _is_string(match, context):
        return "string"
    return "unknown"

def _is_definition(match, context):
    ...
def _is_callsite(match, context):
    ...
# ... 6 more helpers

# tools/cq/search/pipeline/classify_phase.py
from tools.cq.search.pipeline.classification import classify_match  # Direct import
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/classify_phase.py` (remove lazy import)
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/partition_pipeline.py` (update import)

**New Files to Create**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/classification.py` - Extracted classification logic
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/search/pipeline/test_classification.py` - Classification tests

**Legacy Decommission/Delete Scope**:
- Delete classify_match + 8 helpers from smart_search.py after extraction
- Remove lazy import workarounds

---

### S27. Complete search pipeline phase extraction

**Goal**: Make extracted phase modules (candidate_phase, classify_phase, enrichment_phase) authoritative, remove ~500 LOC of duplicate implementations from smart_search.py.

**Representative Code Snippets**:

Current (duplication):
```python
# tools/cq/search/pipeline/smart_search.py (1200+ LOC)
# Full implementations of candidate selection, classification, enrichment
def run_candidate_phase(matches):
    # 150 LOC
    ...

def run_classification_phase(candidates):
    # 200 LOC
    ...

def run_enrichment_phase(classified):
    # 150 LOC
    ...

# Also duplicated in:
# - tools/cq/search/pipeline/candidate_phase.py
# - tools/cq/search/pipeline/classify_phase.py
# - tools/cq/search/pipeline/enrichment_phase.py
```

Target (thin orchestrator):
```python
# tools/cq/search/pipeline/smart_search.py (~400 LOC after cleanup)
from tools.cq.search.pipeline.candidate_phase import run_candidate_phase
from tools.cq.search.pipeline.classify_phase import run_classification_phase
from tools.cq.search.pipeline.enrichment_phase import run_enrichment_phase
from tools.cq.search.pipeline.classification import classify_match

def smart_search(query: str, context: SearchContext) -> SearchResult:
    """Run smart search pipeline.

    Orchestrates: candidate selection → classification → enrichment
    """
    # 1. Candidate phase
    candidates = run_candidate_phase(query, context)

    # 2. Classification phase
    classified = run_classification_phase(candidates, context)

    # 3. Enrichment phase
    enriched = run_enrichment_phase(classified, context)

    return SearchResult(matches=enriched, summary=...)
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py` (remove ~500 LOC of duplicate implementations)
- Verify phase modules are complete and authoritative

**New Files to Create**:
- None (phase modules already exist)

**Legacy Decommission/Delete Scope**:
- Delete ~500 LOC of duplicate phase implementations from smart_search.py
- Delete internal helper functions now in phase modules

---

### S28. Extract query cache ceremony

**Goal**: Consolidate duplicated cache lifecycle code from executor.py and executor_ast_grep.py into new query_cache.py module.

**Representative Code Snippets**:

Current (duplication):
```python
# tools/cq/query/executor.py:340-538 (~200 LOC)
def scan_entity_fragments(repo_root, entity_filter, cache_backend):
    # Cache key computation
    cache_key = _compute_cache_key(repo_root, entity_filter)

    # Cache lookup
    if cache_backend:
        cached = cache_backend.get(cache_key)
        if cached:
            return decode_cached_payload(cached, FragmentSet)

    # Scan
    fragments = _scan_fragments(repo_root, entity_filter)

    # Cache write
    if cache_backend:
        encoded = msgspec.json.encode(fragments)
        cache_backend.set(cache_key, encoded)

    return fragments

# tools/cq/query/executor_ast_grep.py:155-393 (~240 LOC)
def scan_pattern_fragments(repo_root, pattern, cache_backend):
    # Identical cache ceremony with different scan function
    ...
```

Target (extracted ceremony):
```python
# tools/cq/query/query_cache.py (new)
from __future__ import annotations

from typing import Callable, TypeVar

import msgspec

from tools.cq.core.cache.interface import CqCacheBackend
from tools.cq.core.cache.cache_decode import decode_cached_payload

T = TypeVar("T")

def cached_scan(
    cache_key: str,
    scan_fn: Callable[[], T],
    result_type: type[T],
    cache_backend: CqCacheBackend | None,
) -> T:
    """Execute scan with cache read-through.

    Parameters
    ----------
    cache_key : str
        Cache key
    scan_fn : Callable[[], T]
        Scan function (called on cache miss)
    result_type : type[T]
        Result type for decoding
    cache_backend : CqCacheBackend | None
        Cache backend

    Returns
    -------
    T
        Scan result (from cache or fresh scan)
    """
    # Cache lookup
    if cache_backend:
        raw = cache_backend.get(cache_key)
        cached = decode_cached_payload(raw, result_type)
        if cached is not None:
            return cached

    # Fresh scan
    result = scan_fn()

    # Cache write
    if cache_backend:
        encoded = msgspec.json.encode(result)
        cache_backend.set(cache_key, encoded)

    return result

# tools/cq/query/executor.py (after extraction)
from tools.cq.query.query_cache import cached_scan

def scan_entity_fragments(repo_root, entity_filter, cache_backend):
    cache_key = _compute_cache_key(repo_root, entity_filter)
    return cached_scan(
        cache_key=cache_key,
        scan_fn=lambda: _scan_fragments(repo_root, entity_filter),
        result_type=FragmentSet,
        cache_backend=cache_backend,
    )
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/query/executor.py` (remove cache ceremony, use cached_scan)
- `/Users/paulheyse/CodeAnatomy/tools/cq/query/executor_ast_grep.py` (remove cache ceremony, use cached_scan)

**New Files to Create**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/query/query_cache.py` - Cache ceremony helpers
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/query/test_query_cache.py` - Cache ceremony tests

**Legacy Decommission/Delete Scope**:
- Delete ~200 LOC cache ceremony from executor.py (lines 340-538)
- Delete ~240 LOC cache ceremony from executor_ast_grep.py (lines 155-393)

---

### S29. Decompose executor.py

**Goal**: Split executor.py into executor_entity.py and executor_pattern.py after S28 extracts cache ceremony.

**Representative Code Snippets**:

Current (monolithic):
```python
# tools/cq/query/executor.py (~1800 LOC)
class QueryExecutor:
    def execute_entity_query(self, query):
        # 600 LOC
        ...

    def execute_pattern_query(self, query):
        # 400 LOC
        ...

    # Shared helpers
    def _build_result(self, matches):
        # 200 LOC
        ...
```

Target (decomposed):
```python
# tools/cq/query/executor_entity.py (new)
def execute_entity_query(
    query: EntityQuery,
    context: ExecutionContext,
) -> CqResult:
    """Execute entity-based query (function, class, import, etc.).

    Parameters
    ----------
    query : EntityQuery
        Entity query spec
    context : ExecutionContext
        Execution context

    Returns
    -------
    CqResult
        Query result
    """
    fragments = scan_entity_fragments(
        repo_root=context.repo_root,
        entity_filter=query.entity_filter,
        cache_backend=context.cache_backend,
    )
    matches = _filter_matches(fragments, query)
    return _build_result(matches, query)

# tools/cq/query/executor_pattern.py (new)
def execute_pattern_query(
    query: PatternQuery,
    context: ExecutionContext,
) -> CqResult:
    """Execute pattern-based query (ast-grep structural search).

    Parameters
    ----------
    query : PatternQuery
        Pattern query spec
    context : ExecutionContext
        Execution context

    Returns
    -------
    CqResult
        Query result
    """
    fragments = scan_pattern_fragments(
        repo_root=context.repo_root,
        pattern=query.pattern,
        cache_backend=context.cache_backend,
    )
    matches = _filter_matches(fragments, query)
    return _build_result(matches, query)

# tools/cq/query/executor.py (thin facade)
from tools.cq.query.executor_entity import execute_entity_query
from tools.cq.query.executor_pattern import execute_pattern_query

def execute_query(query: Query, context: ExecutionContext) -> CqResult:
    """Execute query (entity or pattern).

    Dispatches to appropriate executor.
    """
    if isinstance(query, EntityQuery):
        return execute_entity_query(query, context)
    if isinstance(query, PatternQuery):
        return execute_pattern_query(query, context)
    raise ValueError(f"Unknown query type: {type(query)}")
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/query/executor.py` (split into facade)

**New Files to Create**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/query/executor_entity.py` - Entity query execution
- `/Users/paulheyse/CodeAnatomy/tools/cq/query/executor_pattern.py` - Pattern query execution
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/query/test_executor_entity.py` - Entity executor tests
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/query/test_executor_pattern.py` - Pattern executor tests

**Legacy Decommission/Delete Scope**:
- Move entity query code to executor_entity.py
- Move pattern query code to executor_pattern.py
- Keep only dispatch logic in executor.py

---

### S30. Decompose rust_lane/runtime.py

**Goal**: Extract query pack orchestration and payload assembly from runtime.py into separate modules.

**Representative Code Snippets**:

Current (monolithic):
```python
# tools/cq/search/tree_sitter/rust_lane/runtime.py (1080 LOC)
def run_rust_lane_search(query, context):
    # Query pack orchestration (300 LOC)
    packs = _build_query_packs(query)
    results = []
    for pack in packs:
        result = _execute_pack(pack, context)
        results.append(result)

    # Payload assembly (400 LOC)
    assembled = _assemble_payloads(results, context)

    # Final result building (200 LOC)
    return _build_final_result(assembled)
```

Target (decomposed):
```python
# tools/cq/search/tree_sitter/rust_lane/query_orchestration.py (new)
def orchestrate_query_packs(
    query: RustQuery,
    context: RustSearchContext,
) -> list[QueryPackResult]:
    """Build and execute query packs.

    Parameters
    ----------
    query : RustQuery
        Rust query spec
    context : RustSearchContext
        Search context

    Returns
    -------
    list[QueryPackResult]
        Pack execution results
    """
    packs = _build_query_packs(query)
    results = []
    for pack in packs:
        result = _execute_pack(pack, context)
        results.append(result)
    return results

# tools/cq/search/tree_sitter/rust_lane/payload_assembly.py (new)
def assemble_payloads(
    pack_results: list[QueryPackResult],
    context: RustSearchContext,
) -> list[AssembledPayload]:
    """Assemble payloads from pack results.

    Parameters
    ----------
    pack_results : list[QueryPackResult]
        Pack execution results
    context : RustSearchContext
        Search context

    Returns
    -------
    list[AssembledPayload]
        Assembled payloads
    """
    assembled = []
    for pack_result in pack_results:
        payload = _assemble_from_pack(pack_result, context)
        assembled.append(payload)
    return assembled

# tools/cq/search/tree_sitter/rust_lane/runtime.py (thin entry point)
from tools.cq.search.tree_sitter.rust_lane.query_orchestration import orchestrate_query_packs
from tools.cq.search.tree_sitter.rust_lane.payload_assembly import assemble_payloads

def run_rust_lane_search(query, context):
    """Run Rust lane search (thin orchestrator)."""
    pack_results = orchestrate_query_packs(query, context)
    assembled = assemble_payloads(pack_results, context)
    return _build_final_result(assembled)
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/rust_lane/runtime.py` (split into thin orchestrator)

**New Files to Create**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/rust_lane/query_orchestration.py` - Query pack orchestration
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/rust_lane/payload_assembly.py` - Payload assembly
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/search/tree_sitter/rust_lane/test_query_orchestration.py` - Orchestration tests
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/search/tree_sitter/rust_lane/test_payload_assembly.py` - Assembly tests

**Legacy Decommission/Delete Scope**:
- Move orchestration logic to query_orchestration.py
- Move assembly logic to payload_assembly.py
- Keep only entry point in runtime.py

---

### S31. Decompose result.py

**Goal**: Split CLI result.py into result_filter.py, result_render.py, result_persist.py by responsibility.

**Representative Code Snippets**:

Current (mixed responsibilities):
```python
# tools/cq/cli_app/result.py (383 LOC)
def filter_results(results, options):
    # 100 LOC
    ...

def render_results(results, format):
    # 150 LOC
    ...

def persist_results(results, output_path):
    # 100 LOC
    ...
```

Target (decomposed):
```python
# tools/cq/cli_app/result_filter.py (new)
def filter_results(
    results: list[CqResult],
    options: FilterOptions,
) -> list[CqResult]:
    """Filter results by criteria.

    Parameters
    ----------
    results : list[CqResult]
        Results to filter
    options : FilterOptions
        Filter options

    Returns
    -------
    list[CqResult]
        Filtered results
    """
    filtered = results
    if options.min_confidence:
        filtered = [r for r in filtered if r.confidence >= options.min_confidence]
    if options.exclude_tests:
        filtered = [r for r in filtered if not _is_test_file(r.location)]
    return filtered

# tools/cq/cli_app/result_render.py (new)
def render_results(
    results: list[CqResult],
    format: str,
) -> str:
    """Render results to string.

    Parameters
    ----------
    results : list[CqResult]
        Results to render
    format : str
        Output format (md, json, summary, etc.)

    Returns
    -------
    str
        Rendered output
    """
    if format == "json":
        return _render_json(results)
    if format == "md":
        return _render_markdown(results)
    if format == "summary":
        return _render_summary(results)
    raise ValueError(f"Unknown format: {format}")

# tools/cq/cli_app/result_persist.py (new)
def persist_results(
    results: list[CqResult],
    output_path: str,
) -> None:
    """Persist results to file.

    Parameters
    ----------
    results : list[CqResult]
        Results to persist
    output_path : str
        Output file path
    """
    with open(output_path, "w") as f:
        f.write(render_results(results, format="json"))
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/result.py` (split into 3 modules)

**New Files to Create**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/result_filter.py` - Result filtering
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/result_render.py` - Result rendering
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/result_persist.py` - Result persistence
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/cli_app/test_result_filter.py` - Filter tests
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/cli_app/test_result_render.py` - Render tests
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/cli_app/test_result_persist.py` - Persist tests

**Legacy Decommission/Delete Scope**:
- Delete result.py after migration complete

---

### S32. Extract Q-step preparation

**Goal**: Move _prepare_q_step, _expand_q_step_by_scope, _partition_q_steps from runner.py to q_execution.py.

**Representative Code Snippets**:

Current (in runner.py):
```python
# tools/cq/run/runner.py:175-270
def _partition_q_steps(steps):
    # 45 LOC
    ...

def _prepare_q_step(step):
    # 45 LOC
    ...

def _expand_q_step_by_scope(step, scopes):
    # 50 LOC
    ...
```

Target (in q_execution.py):
```python
# tools/cq/run/q_execution.py (add to existing)
def partition_q_steps(
    steps: list[RunStep],
) -> dict[str, list[QStep]]:
    """Partition Q steps by language/scope.

    Parameters
    ----------
    steps : list[RunStep]
        All run steps

    Returns
    -------
    dict[str, list[QStep]]
        Partitioned Q steps by scope
    """
    q_steps = [s for s in steps if s.type == "q"]
    by_scope = {}
    for step in q_steps:
        scope = step.scope or "default"
        by_scope.setdefault(scope, []).append(step)
    return by_scope

def prepare_q_step(step: RunStep) -> QStep:
    """Prepare Q step for execution.

    Parameters
    ----------
    step : RunStep
        Raw run step

    Returns
    -------
    QStep
        Prepared Q step
    """
    # Normalization logic
    ...

def expand_q_step_by_scope(
    step: QStep,
    scopes: list[str],
) -> list[QStep]:
    """Expand Q step across multiple scopes.

    Parameters
    ----------
    step : QStep
        Base Q step
    scopes : list[str]
        Target scopes

    Returns
    -------
    list[QStep]
        Expanded steps
    """
    return [
        QStep(query=step.query, scope=scope, ...)
        for scope in scopes
    ]
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py` (remove 3 functions, import from q_execution)
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/q_execution.py` (add 3 functions)

**New Files to Create**:
- None (q_execution.py exists)

**Legacy Decommission/Delete Scope**:
- Delete `_partition_q_steps` from runner.py (line 175)
- Delete `_prepare_q_step` from runner.py (line 221)
- Delete `_expand_q_step_by_scope` from runner.py (line 251)

---

## Wave 4: Protocol & Boundary Improvements (S33-S38)

### S33. Extend CqCacheBackend protocol

**Goal**: Add explicit cache capability protocols (streaming + coordination) and remove all raw `backend.cache` reach-throughs in the same PR.

**Representative Code Snippets**:

Current (incomplete protocol):
```python
# tools/cq/core/cache/interface.py
class CqCacheBackend(Protocol):
    def get(self, key: str) -> bytes | None:
        ...
    def set(self, key: str, value: bytes) -> None:
        ...
    def touch(self, key: str, expire: int) -> None:
        ...
```

Target (capability-tiered protocol surface):
```python
# tools/cq/core/cache/interface.py
from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext
from typing import Protocol

class CqCacheBackend(Protocol):
    """Base CQ cache backend protocol (CRUD + lifecycle + batching)."""
    def get(self, key: str) -> object | None: ...
    def set(self, key: str, value: object, *, expire: int | None = None, tag: str | None = None) -> bool: ...
    def set_many(
        self,
        items: dict[str, object],
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> int: ...
    def touch(self, key: str, *, expire: int | None = None) -> bool: ...
    def transact(self) -> AbstractContextManager[None]:
        return nullcontext()

class CqCacheStreamingBackend(CqCacheBackend, Protocol):
    """Optional streaming/blob capability."""
    def read_streaming(self, key: str) -> bytes | None: ...
    def set_streaming(
        self,
        key: str,
        payload: bytes,
        *,
        expire: int | None = None,
        tag: str | None = None,
    ) -> bool: ...

class CqCacheCoordinationBackend(CqCacheBackend, Protocol):
    """Optional lock/semaphore/barrier capability."""
    def lock(self, key: str, *, expire: int | None = None) -> AbstractContextManager[None]: ...
    def rlock(self, key: str, *, expire: int | None = None) -> AbstractContextManager[None]: ...
    def semaphore(
        self,
        key: str,
        *,
        value: int,
        expire: int | None = None,
    ) -> AbstractContextManager[None]: ...
    def barrier(self, key: str, publish_fn: Callable[[], None]) -> None: ...
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/cache/interface.py` (extend protocol)
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/cache/diskcache_backend.py` (implement new methods)
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/cache/coordination.py` (replace raw cache extraction with coordination capability use)
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/cache/tree_sitter_blob_store.py` (use streaming capability methods)
- All sites using `getattr(backend, "cache", None)` (use capability protocols)

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- Delete all `getattr(backend, "cache", None)` protocol bypass patterns
- Delete raw `cache.read(...)`/`cache.set(..., read=True)` calls outside backend adapter
- Delete direct diskcache primitive construction outside cache backend

---

### S34. Type enrichment payloads

**Goal**: Replace `dict[str, object]` enrichment fields with typed msgspec structs and strict decode boundaries (no fallback union types).

**Representative Code Snippets**:

Current (untyped):
```python
# tools/cq/search/pipeline/smart_search_types.py:219-221
class EnrichedMatch(msgspec.Struct):
    match: dict[str, object]
    classification: str
    enrichment: dict[str, object]  # Untyped!
```

Target (typed):
```python
# tools/cq/search/pipeline/enrichment_contracts.py (new)
class RustTreeSitterEnrichmentV1(
    msgspec.Struct,
    frozen=True,
    forbid_unknown_fields=True,
):
    """Rust tree-sitter enrichment payload.

    Schema version: 1
    """
    schema_version: int = 1
    visibility: str | None = None
    trait_bounds: list[str] | None = None
    generics: list[str] | None = None
    return_type: str | None = None
    attributes: list[str] | None = None
    doc_comment: str | None = None

class PythonEnrichmentV1(
    msgspec.Struct,
    frozen=True,
    forbid_unknown_fields=True,
):
    """Python enrichment payload.

    Schema version: 1
    """
    schema_version: int = 1
    docstring: str | None = None
    decorators: list[str] | None = None
    parameters: list[str] | None = None
    return_annotation: str | None = None
    base_classes: list[str] | None = None

class PythonSemanticEnrichmentV1(
    msgspec.Struct,
    frozen=True,
    forbid_unknown_fields=True,
):
    """Python semantic enrichment payload."""
    schema_version: int = 1
    diagnostics: list[dict[str, object]] = msgspec.field(default_factory=list)
    planes: dict[str, object] = msgspec.field(default_factory=dict)

# tools/cq/search/pipeline/smart_search_types.py
from tools.cq.search.pipeline.enrichment_contracts import (
    RustTreeSitterEnrichmentV1,
    PythonEnrichmentV1,
    PythonSemanticEnrichmentV1,
)

class EnrichedMatch(msgspec.Struct):
    match: dict[str, object]
    classification: str
    rust_tree_sitter: RustTreeSitterEnrichmentV1 | None = None
    python_enrichment: PythonEnrichmentV1 | None = None
    python_semantic_enrichment: PythonSemanticEnrichmentV1 | None = None

# producer boundary
typed_rust = msgspec.convert(raw_payload, RustTreeSitterEnrichmentV1, strict=True)
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search_types.py` (type enrichment field)
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py` (typed construction at boundary)
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/search_semantic.py` (remove untyped traversal)
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/objects/resolve.py` (remove untyped traversal)
- Enrichment producers (parse to typed structs using `msgspec.convert(..., strict=True)`)
- Enrichment consumers (handle typed structs)

**New Files to Create**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/enrichment_contracts.py` - Typed enrichment schemas
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/search/pipeline/test_enrichment_contracts.py` - Schema tests

**Legacy Decommission/Delete Scope**:
- Delete `| dict[str, object]` fallback from enrichment field types
- Delete `isinstance(..., dict)` checks for known enrichment schemas
- Replace dict access with struct attribute access

---

### S35. Thread RenderEnrichmentPort through render_markdown()

**Goal**: Eliminate `_RENDER_ENRICHMENT_PORT_STATE` global in report.py by threading RenderEnrichmentPort through render_markdown().

**Representative Code Snippets**:

Current (global state):
```python
# tools/cq/core/report.py:51
_RENDER_ENRICHMENT_PORT_STATE: RenderEnrichmentPort | None = None

def render_markdown(result: CqResult) -> str:
    global _RENDER_ENRICHMENT_PORT_STATE
    port = _RENDER_ENRICHMENT_PORT_STATE or _default_port()
    return port.render(result)
```

Target (injected context):
```python
# tools/cq/core/report.py
def render_markdown(
    result: CqResult,
    *,
    render_context: RenderContext | None = None,
) -> str:
    """Render result to markdown.

    Parameters
    ----------
    result : CqResult
        Result to render
    render_context : RenderContext | None
        Rendering context (defaults to minimal context)

    Returns
    -------
    str
        Markdown output
    """
    ctx = render_context or RenderContext.minimal()
    port = ctx.enrichment_port
    return port.render(result)

# tools/cq/core/render_context.py (new)
class RenderContext(msgspec.Struct):
    """Rendering context for result output."""
    enrichment_port: RenderEnrichmentPort
    format_options: FormatOptions

    @classmethod
    def minimal(cls) -> RenderContext:
        """Create minimal render context."""
        return cls(
            enrichment_port=DefaultRenderEnrichmentPort(),
            format_options=FormatOptions(),
        )
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/report.py` (remove global, add parameter)
- All callers of `render_markdown()` (pass render_context)

**New Files to Create**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/render_context.py` - RenderContext struct
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/core/test_render_context.py` - Context tests

**Legacy Decommission/Delete Scope**:
- Delete `_RENDER_ENRICHMENT_PORT_STATE` global (line 51)
- Delete global state setters/getters

---

### S36. Thread ClassifierCacheContext through classification

**Goal**: Remove `_resolve_cache_context` fallback by making cache_context parameter required in 4 cache-aware functions.

**Representative Code Snippets**:

Current (fallback resolution):
```python
# tools/cq/search/pipeline/classifier_runtime.py:103-106
def classify_with_cache(match, cache_context=None):
    ctx = cache_context or _resolve_cache_context()  # Fallback!
    ...
```

Target (required parameter):
```python
# tools/cq/search/pipeline/classifier_runtime.py
def classify_with_cache(
    match: dict[str, object],
    cache_context: ClassifierCacheContext,
) -> str:
    """Classify match using cache.

    Parameters
    ----------
    match : dict[str, object]
        Match to classify
    cache_context : ClassifierCacheContext
        Cache context (required)

    Returns
    -------
    str
        Classification
    """
    cache_key = _build_cache_key(match)
    cached = cache_context.backend.get(cache_key)
    if cached:
        return msgspec.json.decode(cached)

    classification = _classify_match(match)

    encoded = msgspec.json.encode(classification)
    cache_context.backend.set(cache_key, encoded)

    return classification
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/classifier_runtime.py` (remove fallback, require parameter)
- 4 cache-aware functions (make cache_context required)
- All callers (pass cache_context explicitly)

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- Delete `_resolve_cache_context()` fallback function
- Delete optional cache_context parameters

---

### S37. Extract backend lifecycle from diskcache_backend.py

**Goal**: Move backend lifecycle management (_BackendState, get/set/close functions) from diskcache_backend.py to new backend_lifecycle.py.

**Representative Code Snippets**:

Current (mixed responsibilities):
```python
# tools/cq/core/cache/diskcache_backend.py:400-514
class _BackendState:
    backend: CqCacheBackend | None = None
    lock: threading.Lock = threading.Lock()

def get_cq_cache_backend() -> CqCacheBackend:
    # 40 LOC
    ...

def set_cq_cache_backend(backend: CqCacheBackend) -> None:
    # 20 LOC
    ...

def close_cq_cache_backend() -> None:
    # 30 LOC
    ...
```

Target (extracted lifecycle):
```python
# tools/cq/core/cache/backend_lifecycle.py (new)
from __future__ import annotations

import threading
from typing import TypeVar

from tools.cq.core.cache.interface import CqCacheBackend

T = TypeVar("T", bound=CqCacheBackend)

class _BackendState:
    """Global backend state (thread-safe singleton)."""
    backend: CqCacheBackend | None = None
    lock: threading.Lock = threading.Lock()

_STATE = _BackendState()

def get_cq_cache_backend() -> CqCacheBackend:
    """Get or create cache backend.

    Thread-safe singleton access.

    Returns
    -------
    CqCacheBackend
        Cache backend instance
    """
    with _STATE.lock:
        if _STATE.backend is None:
            from tools.cq.core.cache.diskcache_backend import DiskcacheBackend
            _STATE.backend = DiskcacheBackend.create_default()
        return _STATE.backend

def set_cq_cache_backend(backend: CqCacheBackend) -> None:
    """Set cache backend (for testing/configuration).

    Parameters
    ----------
    backend : CqCacheBackend
        Backend instance
    """
    with _STATE.lock:
        _STATE.backend = backend

def close_cq_cache_backend() -> None:
    """Close and clear cache backend."""
    with _STATE.lock:
        if _STATE.backend is not None:
            if hasattr(_STATE.backend, "close"):
                _STATE.backend.close()
            _STATE.backend = None
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/cache/diskcache_backend.py` (remove lifecycle code, lines 400-514)
- All backend lifecycle users (import from backend_lifecycle.py)

**New Files to Create**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/cache/backend_lifecycle.py` - Backend lifecycle management
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/core/cache/test_backend_lifecycle.py` - Lifecycle tests

**Legacy Decommission/Delete Scope**:
- Delete `_BackendState` from diskcache_backend.py
- Delete `get_cq_cache_backend` from diskcache_backend.py
- Delete `set_cq_cache_backend` from diskcache_backend.py
- Delete `close_cq_cache_backend` from diskcache_backend.py

---

### S38. Require explicit backend injection in cache public functions

**Goal**: Make cache backend a required explicit dependency at leaf functions (hard cutover; no fallback to ambient global backend).

**Representative Code Snippets**:

Current (hardcoded backend):
```python
# tools/cq/core/cache/search_artifact_store.py
def persist_tree_sitter_payload(key: str, payload: TreeSitterPayload) -> None:
    backend = get_cq_cache_backend()  # Hardcoded!
    encoded = msgspec.json.encode(payload)
    backend.set(key, encoded)
```

Target (explicit backend dependency):
```python
# tools/cq/core/cache/search_artifact_store.py
def persist_tree_sitter_payload(
    key: str,
    payload: TreeSitterPayload,
    *,
    backend: CqCacheBackend,
) -> None:
    """Persist tree-sitter payload to cache.

    Parameters
    ----------
    key : str
        Cache key
    payload : TreeSitterPayload
        Payload to persist
    backend : CqCacheBackend
        Cache backend (explicit dependency)
    """
    encoded = msgspec.json.encode(payload)
    backend.set(key, encoded)
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/cache/search_artifact_store.py` (add backend parameter)
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/cache/tree_sitter_cache_store.py` (add backend parameter)
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/cache/tree_sitter_blob_store.py` (add backend parameter)
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/cache/snapshot_fingerprint.py` (thread backend from caller)
- All cache public functions (explicit backend parameter)

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- Delete leaf-level `get_cq_cache_backend()` calls from cache modules
- Move backend resolution to composition boundaries only

---

## Wave 5: Structural Type Safety (S39-S43)

### S39. Return new Finding instances from assign_result_finding_ids()

**Goal**: Fix CQS violation by using msgspec.structs.replace() to produce new Finding instances with IDs instead of mutating.

**Representative Code Snippets**:

Current (mutation):
```python
# tools/cq/core/schema.py:448-467
def assign_result_finding_ids(result: CqResult) -> None:
    """Assign IDs to findings (mutates in place!)."""
    for idx, finding in enumerate(result.key_findings):
        finding.id = f"finding-{idx}"  # MUTATION!
```

Target (functional):
```python
# tools/cq/core/schema.py
import msgspec.structs

def assign_result_finding_ids(result: CqResult) -> CqResult:
    """Assign IDs to findings, returning new result.

    Parameters
    ----------
    result : CqResult
        Input result

    Returns
    -------
    CqResult
        Result with IDs assigned to findings
    """
    findings_with_ids = [
        msgspec.structs.replace(finding, id=f"finding-{idx}")
        for idx, finding in enumerate(result.key_findings)
    ]
    return msgspec.structs.replace(result, key_findings=findings_with_ids)
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/schema.py` (lines 448-467)
- All callers (use returned value instead of mutation)

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- Change return type from None to CqResult
- Update all callers to use returned value

---

### S40. Split attach_target_metadata into query + command

**Goal**: Fix CQS violation in `attach_target_metadata` by splitting resolution/caching query logic from result mutation logic.

**Representative Code Snippets**:

Current (mixed query + command):
```python
# tools/cq/macros/calls_target.py:458
def attach_target_metadata(
    result: CqResult,
    request: AttachTargetMetadataRequestV1,
) -> tuple[tuple[str, int] | None, Counter[str], QueryLanguage | None]:
    ...
    target_location = payload_state.target_location
    target_callees = payload_state.target_callees
    ...
    if target_location is not None:
        result.summary.target_file = target_location[0]
        result.summary.target_line = target_location[1]
    add_target_callees_section(result, target_callees, request.score, preview_limit=request.preview_limit)
    return target_location, target_callees, resolved_language
```

Target (split query/command):
```python
# tools/cq/macros/calls_target.py
class TargetMetadataResultV1(CqStruct, frozen=True):
    target_location: tuple[str, int] | None
    target_callees: Counter[str]
    resolved_language: QueryLanguage | None

def resolve_target_metadata(
    request: AttachTargetMetadataRequestV1,
) -> TargetMetadataResultV1:
    """Pure query: resolve target metadata + cache side effects only."""
    ...
    return TargetMetadataResultV1(
        target_location=target_location,
        target_callees=target_callees,
        resolved_language=resolved_language,
    )

def apply_target_metadata(
    result: CqResult,
    metadata: TargetMetadataResultV1,
    *,
    score: ScoreDetails | None,
    preview_limit: int,
) -> None:
    """Command: mutate result payload only."""
    if metadata.target_location is not None:
        result.summary.target_file = metadata.target_location[0]
        result.summary.target_line = metadata.target_location[1]
    add_target_callees_section(result, metadata.target_callees, score, preview_limit=preview_limit)
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/macros/calls_target.py` (split `attach_target_metadata`)
- `/Users/paulheyse/CodeAnatomy/tools/cq/macros/calls/entry.py` (call `resolve_target_metadata` then `apply_target_metadata`)
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/macros/test_calls.py` (update call flow assertions)

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- Delete `attach_target_metadata(...)` mixed-responsibility function from `tools/cq/macros/calls_target.py`
- Delete tuple-return coupling between metadata resolution and mutation call sites

---

### S41. CqSummary god struct improvements

**Goal**: Replace monolithic `CqSummary` with mode-tagged typed summary variants in this plan cycle (design-phase hard cutover, no interim doc-only phase).

**Representative Code Snippets**:

Current (undocumented god struct):
```python
# tools/cq/core/summary_contract.py:45
class CqSummary(msgspec.Struct, omit_defaults=True):
    # 150+ fields with no grouping documentation
    total_matches: int | None = None
    matches: int | None = None
    # ... 150 more fields
```

Target (typed variants + tagged envelope):
```python
# tools/cq/core/summary_contract.py:45
class SummaryEnvelopeV1(msgspec.Struct, frozen=True, tag=True):
    schema_version: int = 1
    mode: Literal["search", "calls", "impact", "run", "neighborhood"]

class SearchSummaryV1(SummaryEnvelopeV1, tag="search"):
    matches: int = 0
    files_scanned: int = 0
    total_matches: int = 0
    matched_files: int = 0
    scanned_files: int = 0
    query: str | None = None
    pattern: str | None = None
    lang: str | None = None
    mode_chain: list[str] = msgspec.field(default_factory=list)
    search_stage_timings_ms: dict[str, float] = msgspec.field(default_factory=dict)
    dropped_by_scope: object | None = None

class CallsSummaryV1(SummaryEnvelopeV1, tag="calls"):
    function: str | None = None
    total_sites: int = 0
    files_with_calls: int = 0
    callers_found: int = 0
    target_file: str | None = None
    target_line: int | None = None

class RunSummaryV1(SummaryEnvelopeV1, tag="run"):
    plan_version: int | None = None
    steps: list[str] = msgspec.field(default_factory=list)
    macro_summaries: dict[str, dict[str, object]] = msgspec.field(default_factory=dict)

# Legacy alias removed in same PR; call sites use SummaryEnvelopeV1 union directly.
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/summary_contract.py` (replace monolithic struct with typed variants)
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/schema.py` (update summary typing on `CqResult`)
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/render_summary.py` (variant-aware rendering)
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/run_summary.py` (construct `RunSummaryV1`)
- `/Users/paulheyse/CodeAnatomy/tools/cq/macros/` (construct mode-specific summary variants)

**New Files to Create**:
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/test_summary_variants.py` - Variant contract tests
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/test_summary_render_variants.py` - Rendering coverage per variant

**Legacy Decommission/Delete Scope**:
- Delete monolithic `CqSummary` struct from `tools/cq/core/summary_contract.py`
- Delete dict-like mutation helpers (`__setitem__`, `update`, `_set_known_field`) from summary contracts
- Delete call-site writes to fields not valid for the active summary mode

---

### S42. Define typed ContextWindow struct

**Goal**: Replace `dict[str, int]` with frozen ContextWindow(start_line, end_line) struct.

**Representative Code Snippets**:

Current (dict):
```python
# tools/cq/search/pipeline/smart_search_types.py:216
class EnrichedMatch(msgspec.Struct):
    context_window: dict[str, int]  # {"start_line": 10, "end_line": 20}

# tools/cq/macros/calls/analysis.py
context_window = {"start_line": match.start_line - 5, "end_line": match.end_line + 5}
```

Target (typed struct):
```python
# tools/cq/search/pipeline/context_window.py
class ContextWindow(msgspec.Struct, frozen=True):
    """Line-based context window.

    Immutable span of source lines around a match.
    """
    start_line: int = msgspec.field(metadata={"ge": 1})
    end_line: int = msgspec.field(metadata={"ge": 1})

    def __post_init__(self) -> None:
        """Validate window invariants."""
        if self.end_line < self.start_line:
            raise ValueError(f"Invalid window: end_line {self.end_line} < start_line {self.start_line}")

    @classmethod
    def around(cls, center_line: int, radius: int = 5) -> ContextWindow:
        """Create window around center line.

        Parameters
        ----------
        center_line : int
            Center line (1-indexed)
        radius : int
            Lines before/after center

        Returns
        -------
        ContextWindow
            Window spanning [center - radius, center + radius]
        """
        return cls(
            start_line=max(1, center_line - radius),
            end_line=center_line + radius,
        )

    def line_count(self) -> int:
        """Count of lines in window."""
        return self.end_line - self.start_line + 1

# tools/cq/search/pipeline/smart_search_types.py
from tools.cq.search.pipeline.context_window import ContextWindow

class EnrichedMatch(msgspec.Struct):
    context_window: ContextWindow

# tools/cq/macros/calls/analysis.py
from tools.cq.search.pipeline.context_window import ContextWindow

context_window = ContextWindow.around(center_line=match.line, radius=5)
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search_types.py` (line 216)
- `/Users/paulheyse/CodeAnatomy/tools/cq/macros/calls/analysis.py` (use ContextWindow struct)
- All sites using `dict[str, int]` for context windows

**New Files to Create**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/context_window.py` - ContextWindow struct
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/search/pipeline/test_context_window.py` - Window tests

**Legacy Decommission/Delete Scope**:
- Replace all `{"start_line": N, "end_line": M}` dicts with ContextWindow

---

### S43. Consolidate cache lifecycle management

**Goal**: Create per-language CacheRegistry for unified clear_all("rust") entry point used in test teardown.

**Representative Code Snippets**:

Current (scattered caches):
```python
# tools/cq/search/tree_sitter/rust_lane/runtime_cache.py
_TREE_CACHE: BoundedCache[str, None] = BoundedCache(...)

# tools/cq/search/tree_sitter/rust_lane/query_cache.py
@lru_cache(maxsize=1)
def _pack_source_rows() -> tuple[bytes, ...]:
    ...

# tools/cq/search/rust/enrichment.py
_AST_CACHE: BoundedCache[str, tuple[SgRoot, str]] = BoundedCache(...)

# tools/cq/search/python/extractors.py
_AST_CACHE: BoundedCache[str, tuple[ast.Module, str]] = BoundedCache(...)

# Test teardown (scattered clear calls)
def teardown():
    clear_tree_sitter_rust_cache()
    clear_rust_enrichment_cache()
    clear_python_enrichment_cache()
```

Target (unified registry):
```python
# tools/cq/search/cache/registry.py (new)
from __future__ import annotations

from typing import Callable

class CacheRegistry:
    """Registry of cache instances for lifecycle management."""

    def __init__(self) -> None:
        self._caches: dict[str, dict[str, object]] = {}
        self._clear_callbacks: dict[str, list[Callable[[], None]]] = {}

    def register_cache(
        self,
        language: str,
        name: str,
        cache: dict[str, object],
    ) -> None:
        """Register cache instance.

        Parameters
        ----------
        language : str
            Language scope (python, rust, auto)
        name : str
            Cache name
        cache : dict[str, object]
            Cache instance
        """
        key = f"{language}:{name}"
        self._caches[key] = cache

    def register_clear_callback(
        self,
        language: str,
        callback: Callable[[], None],
    ) -> None:
        """Register cache clear callback.

        Parameters
        ----------
        language : str
            Language scope
        callback : Callable[[], None]
            Clear callback
        """
        self._clear_callbacks.setdefault(language, []).append(callback)

    def clear_all(self, language: str | None = None) -> None:
        """Clear all caches for language.

        Parameters
        ----------
        language : str | None
            Language scope (None = all languages)
        """
        # Clear registered caches
        for key, cache in self._caches.items():
            cache_lang, _ = key.split(":", 1)
            if language is None or cache_lang == language:
                cache.clear()

        # Run clear callbacks
        if language:
            for callback in self._clear_callbacks.get(language, []):
                callback()
        else:
            for callbacks in self._clear_callbacks.values():
                for callback in callbacks:
                    callback()

CACHE_REGISTRY = CacheRegistry()

# tools/cq/search/tree_sitter/rust_lane/runtime_cache.py
from tools.cq.search.cache.registry import CACHE_REGISTRY

_TREE_CACHE: BoundedCache[str, None] = BoundedCache(...)
CACHE_REGISTRY.register_cache("rust", "runtime_cache:tree", _TREE_CACHE)
CACHE_REGISTRY.register_clear_callback("rust", clear_tree_sitter_rust_cache)

# tools/cq/search/rust/enrichment.py
_AST_CACHE: BoundedCache[str, tuple[SgRoot, str]] = BoundedCache(...)
CACHE_REGISTRY.register_cache("rust", "rust_enrichment:ast", _AST_CACHE)
CACHE_REGISTRY.register_clear_callback("rust", clear_rust_enrichment_cache)

# Test teardown (unified)
def teardown():
    from tools.cq.search.cache.registry import CACHE_REGISTRY
    CACHE_REGISTRY.clear_all("rust")
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/rust_lane/runtime_cache.py` (register cache)
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/rust_lane/query_cache.py` (register clear callback)
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/rust/enrichment.py` (register cache)
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/python/extractors.py` (register cache)
- Test teardown fixtures (use CACHE_REGISTRY.clear_all)

**New Files to Create**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/cache/` (directory)
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/cache/__init__.py`
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/cache/registry.py` - Unified cache registry
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/search/cache/test_registry.py` - Registry tests

**Legacy Decommission/Delete Scope**:
- Replace scattered cache.clear() calls with CACHE_REGISTRY.clear_all()

---

## Wave 6: Review-Driven Additions (S44-S53)

### S44. Deduplicate LDMD root alias resolution

**Goal**: Remove duplicated root-alias logic in the LDMD CLI command by using a single canonical section-id resolver.

**Representative Code Snippets**:

Current (duplicated in CLI command):
```python
# tools/cq/cli_app/commands/ldmd.py
if section_id == "root" and idx.sections:
    section_id = idx.sections[0].id
```

Target (single canonical resolver):
```python
# tools/cq/ldmd/format.py
def resolve_section_id(index: LdmdIndex, section_id: str) -> str:
    if section_id == "root" and index.sections:
        return index.sections[0].id
    return section_id

# tools/cq/cli_app/commands/ldmd.py
from tools.cq.ldmd.format import resolve_section_id
...
resolved_id = resolve_section_id(idx, section_id)
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/ldmd.py`
- `/Users/paulheyse/CodeAnatomy/tools/cq/ldmd/format.py`
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/ldmd/test_root_alias.py`

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- Delete inline root-alias branch from `tools/cq/cli_app/commands/ldmd.py`

---

### S45. Add warning logging for immediate Q-step errors

**Goal**: Emit warning-level telemetry when Q-step preparation yields immediate error results.

**Representative Code Snippets**:

Current (silent append):
```python
# tools/cq/run/runner.py
if isinstance(outcome, CqResult):
    immediate_results.append((step_id, outcome))
```

Target (explicit warning):
```python
# tools/cq/run/runner.py
if isinstance(outcome, CqResult):
    immediate_results.append((step_id, outcome))
    if outcome.summary.error:
        logger.warning(
            "Immediate q-step error step_id=%s error=%s",
            step_id,
            outcome.summary.error,
        )
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/run/runner.py`
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/test_runner_summary_telemetry.py`

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- None (additive observability)

---

### S46. Add CliContext.from_parts() for test construction

**Goal**: Provide explicit DI constructor for CLI context creation without filesystem probing.

**Representative Code Snippets**:

Target:
```python
# tools/cq/cli_app/context.py
@classmethod
def from_parts(
    cls,
    *,
    root: Path,
    toolchain: Toolchain,
    services: CqRuntimeServices,
) -> CliContext:
    return cls(root=root, toolchain=toolchain, services=services)

@classmethod
def build(...):
    ...
    return cls.from_parts(root=root, toolchain=toolchain, services=services)
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/context.py`
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/test_cli_context.py`

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- None (build path retained as convenience factory)

---

### S47. Move clear_caches() side effect out of _build_search_context()

**Goal**: Restore CQS by making `_build_search_context()` a pure constructor and moving cache clearing to orchestration.

**Representative Code Snippets**:

Current (hidden side effect):
```python
# tools/cq/search/pipeline/smart_search.py
def _build_search_context(request: SearchRequest) -> SearchConfig:
    clear_caches()
    ...
```

Target (orchestrator-visible side effect):
```python
# tools/cq/search/pipeline/smart_search.py
def smart_search(request: SearchRequest) -> CqResult:
    clear_caches()
    ctx = _build_search_context(request)
    ...

def _build_search_context(request: SearchRequest) -> SearchConfig:
    ...
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py`
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/search/test_smart_search.py`

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- Delete `clear_caches()` call from `_build_search_context()`

---

### S48. Move _ast_grep_prefilter_scope_paths out of assembly layer

**Goal**: Remove assembly→classifier dependency by relocating AST parsability prefiltering to candidate/enrichment preparation.

**Representative Code Snippets**:

Current (wrong layer):
```python
# tools/cq/search/pipeline/assembly.py
def _ast_grep_prefilter_scope_paths(...):
    from tools.cq.search.pipeline.classifier import get_sg_root
```

Target (candidate-phase prefilter):
```python
# tools/cq/search/pipeline/candidate_phase.py
def prefilter_scope_paths_for_ast(paths: list[Path], *, root: Path) -> list[Path]:
    ...

# tools/cq/search/pipeline/assembly.py
# receives prefiltered paths; no classifier imports
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/assembly.py`
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/candidate_phase.py`
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/pipeline/smart_search.py`
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/search/test_smart_search.py`

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- Delete `_ast_grep_prefilter_scope_paths` from `tools/cq/search/pipeline/assembly.py`

---

### S49. Move PYTHON_AST_FIELDS out of language-agnostic IR

**Goal**: Remove Python-specific knowledge from `query/ir.py` into language-scoped constants.

**Representative Code Snippets**:

Current:
```python
# tools/cq/query/ir.py
PYTHON_AST_FIELDS: frozenset[str] = frozenset({...})
```

Target:
```python
# tools/cq/query/python_constants.py (new)
PYTHON_AST_FIELDS: frozenset[str] = frozenset({...})

# tools/cq/query/ir.py
from tools.cq.query.python_constants import PYTHON_AST_FIELDS
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/query/ir.py`
- `/Users/paulheyse/CodeAnatomy/tools/cq/query/parser.py` (if direct constant imports exist)
- `/Users/paulheyse/CodeAnatomy/tools/cq/query/planner.py` (if direct constant imports exist)

**New Files to Create**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/query/python_constants.py`
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/query/test_python_constants.py`

**Legacy Decommission/Delete Scope**:
- Delete `PYTHON_AST_FIELDS` definition from `tools/cq/query/ir.py`

---

### S50. Make ParamInfo frozen and constructor-complete

**Goal**: Remove post-construction mutation of parameter kind in `DefIndex`.

**Representative Code Snippets**:

Current:
```python
# tools/cq/index/def_index.py
@dataclass
class ParamInfo:
    ...
    kind: str = "POSITIONAL_OR_KEYWORD"

param.kind = "POSITIONAL_ONLY"
```

Target:
```python
# tools/cq/index/def_index.py
@dataclass(frozen=True)
class ParamInfo:
    name: str
    annotation: str | None = None
    default: str | None = None
    kind: str = "POSITIONAL_OR_KEYWORD"

param = ParamInfo(name=name, annotation=ann, default=default, kind="POSITIONAL_ONLY")
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/index/def_index.py`

**New Files to Create**:
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/index/test_def_index.py`

**Legacy Decommission/Delete Scope**:
- Delete `param.kind = ...` post-construction mutation sites in `tools/cq/index/def_index.py`

---

### S51. Encapsulate TaintState mutation API

**Goal**: Replace direct mutable-container access (`visited.add`, list extends) with explicit `TaintState` methods.

**Representative Code Snippets**:

Target:
```python
# tools/cq/macros/impact.py
class TaintState(msgspec.Struct):
    ...
    def record_visit(self, key: str) -> None:
        self.visited.add(key)

    def add_sites(self, sites: list[TaintedSite]) -> None:
        self.tainted_sites.extend(sites)

    def mark_tainted(self, symbol: str) -> None:
        self.tainted.add(symbol)

# call sites:
context.state.record_visit(key)
context.state.add_sites(visitor.tainted_sites)
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/macros/impact.py`
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/macros/test_calls.py`
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/macros/test_scope_filtering.py`

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- Delete direct `context.state.visited.add(...)` and `context.state.tainted_sites.extend(...)` call sites

---

### S52. Add Rust lane stage timing telemetry and payload postconditions

**Goal**: Align Rust enrichment observability with Python lane stage timings and assert required payload metadata keys at build boundary.

**Representative Code Snippets**:

Target:
```python
# tools/cq/search/tree_sitter/rust_lane/runtime.py
stage_timings_ms: dict[str, float] = {
    "query_pack": query_pack_ms,
    "payload_build": payload_build_ms,
    "attachment": attach_ms,
}
payload["stage_timings_ms"] = stage_timings_ms

required_keys = ("language", "enrichment_status", "enrichment_sources")
missing = [k for k in required_keys if k not in payload]
if missing:
    raise ValueError(f"Rust enrichment payload missing required keys: {missing}")
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/rust_lane/runtime.py`
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/enrichment/rust_adapter.py`
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/search/tree_sitter/test_rust_lane_runtime.py`

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- Delete implicit/undocumented Rust lane stage timing aggregation paths

---

### S53. Canonicalize tree-sitter cancellation on progress_callback

**Goal**: Make `progress_callback` the canonical cancellation control plane and remove `timeout_micros` dependency from CQ query runtime contracts.

**Representative Code Snippets**:

Current:
```python
# tools/cq/search/tree_sitter/contracts/core_models.py
timeout_micros: int | None = None

# tools/cq/search/tree_sitter/core/runtime.py
if timeout_micros is not None and hasattr(cursor_any, "set_timeout_micros"):
    cursor_any.set_timeout_micros(timeout_micros)
```

Target:
```python
# tools/cq/search/tree_sitter/contracts/core_models.py
class QueryExecutionSettingsV1(CqStruct, frozen=True):
    match_limit: int = 4096
    max_start_depth: int | None = None
    budget_ms: int | None = None
    ...

# tools/cq/search/tree_sitter/core/runtime.py
def _progress_guard(start_ns: int, budget_ms: int | None) -> Callable[[object], bool] | None:
    ...

matches = cursor.matches(root, progress_callback=_progress_guard(...))
exceeded = bool(getattr(cursor, "did_exceed_match_limit", False))
```

**Files to Edit**:
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/contracts/core_models.py`
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/core/runtime.py`
- `/Users/paulheyse/CodeAnatomy/tools/cq/search/tree_sitter/core/runtime_support.py`
- `/Users/paulheyse/CodeAnatomy/tests/unit/cq/search/tree_sitter/test_core_runtime_support.py`

**New Files to Create**:
- None

**Legacy Decommission/Delete Scope**:
- Delete `timeout_micros` from `QueryExecutionSettingsV1`
- Delete cursor `set_timeout_micros` branches in runtime execution paths

---

## Cross-Scope Legacy Decommission

**D1 - After S2, S3**:
- Delete `_summary_value` from front_door_builders.py (line 46-49)
- Delete `_summary_value` from render_summary.py (line 58-61)
- Delete `_result_match_count` from q_step_collapsing.py (line 79-88)
- Delete `_result_match_count` from multilang_orchestrator.py (line 152-159)

**D2 - After S6, S12, S13**:
- Delete `_find_ancestor` from runtime.py (line 272-296)
- Delete `_find_ancestor` from role_classification.py (line 51-75)
- Delete `_node_payload` from executor_ast_grep.py (~line 810)
- Delete `_is_variadic_separator` from executor_ast_grep.py (~line 820)
- Update `__all__` in enrichment_extractors.py and role_classification.py

**D3 - After S19, S8**:
- Delete `DECORATOR_KINDS` from executor.py (lines 1074-1080)
- Delete `matches_entity()` from executor_definitions.py (lines 293-330)

**D4 - After S26, S27**:
- Delete ~500 LOC of duplicate implementations from smart_search.py
- Delete classify_match + 8 helpers from smart_search.py
- Remove lazy imports from classify_phase.py and partition_pipeline.py

**D5 - After S28, S29**:
- Delete cache ceremony code from executor.py (lines 340-538)
- Delete cache ceremony code from executor_ast_grep.py (lines 155-393)

**D6 - After S33, S38, S5, S4**:
- Delete all `getattr(backend, "cache", None)` bypass sites
- Delete inline boolean parsing (`.lower() in ("1", "true", "yes")` patterns)

**D7 - After S34**:
- Delete `dict[str, object]` enrichment field types from EnrichedMatch
- Delete isinstance checks on enrichment payloads

**D8 - After S47, S48**:
- Delete `clear_caches()` call from `_build_search_context()` in `smart_search.py`
- Delete `_ast_grep_prefilter_scope_paths` from `assembly.py`

**D9 - After S49, S53**:
- Delete `PYTHON_AST_FIELDS` definition from `query/ir.py`
- Delete `timeout_micros` contract/runtime usage in tree-sitter query execution

**D10 - After S41**:
- Delete monolithic `CqSummary` struct and dict-like mutation helpers

---

## Implementation Sequence

**Sequence Order** (53 items, dependency-aware):

1. **S1** - Coordination fix (standalone correctness)
2. **S2** - summary_value consolidation
3. **S3** - match count consolidation
4. **S4** - env_bool usage
5. **S5** - touch() bypass fix
6. **S6** - node_payload consolidation
7. **S7** - ExecutionContext typing
8. **S8** - decorator kinds unification
9. **S9** - QueryLanguage move
10. **S10** - RunOptions fix
11. **S11** - comma_separated merge
12. **S44** - LDMD root alias dedupe
13. **S45** - immediate q-step warning logs
14. **S46** - CliContext.from_parts
15. **S47** - move clear_caches side effect
16. **S49** - move PYTHON_AST_FIELDS out of IR
17. **S14** - CallSite frozen
18. **S15** - comprehension taint handler
19. **S50** - ParamInfo frozen + constructor-complete
20. **S51** - TaintState mutation API
21. **S16** - list[object] fix
22. **S17** - CqSummary schema/version groundwork
23. **S23** - Rust regex consolidation
24. **S12** - find_ancestor consolidation (requires adapter)
25. **S13** - underscore prefix removal (requires S12)
26. **S18** - blob_store rename
27. **S19** - entity-kind registry (foundational)
28. **S20** - Rust role classification (requires S12, S13)
29. **S21** - metavar extraction (requires S6)
30. **S22** - cache decode
31. **S24** - risk thresholds
32. **S25** - front-door decomposition
33. **S26** - classify_match extraction
34. **S27** - phase extraction completion (requires S26)
35. **S48** - assembly boundary fix for AST prefiltering
36. **S28** - query cache ceremony (requires S19)
37. **S29** - executor decomposition (requires S28)
38. **S30** - rust_lane/runtime decomposition (requires S12, S20)
39. **S31** - result.py decomposition
40. **S32** - Q-step extraction
41. **S33** - CqCacheBackend capability protocols (requires S5)
42. **S37** - backend lifecycle extraction
43. **S38** - explicit backend injection
44. **S34** - typed enrichment payloads (requires S26, S27)
45. **S35** - RenderEnrichmentPort threading
46. **S36** - ClassifierCacheContext threading
47. **S52** - Rust lane timing + payload assertions
48. **S53** - tree-sitter progress_callback cancellation model
49. **S39** - Finding immutable IDs
50. **S40** - attach_target_metadata split
51. **S41** - CqSummary typed variant migration
52. **S42** - ContextWindow struct
53. **S43** - cache lifecycle consolidation

---

## Implementation Checklist

Status key:
- `Complete` = `[x]`
- `Incomplete` = `[ ]`
- `Partial` = `[ ]` with `(partial)` note

### Wave 1: Quick Wins
- [x] S1 - Fix coordination.py triple-lock
- [x] S2 - Consolidate _summary_value
- [x] S3 - Consolidate _result_match_count
- [x] S4 - Replace inline bool parsing
- [x] S5 - Fix touch() bypass
- [x] S6 - Consolidate node_payload
- [x] S7 - Type ExecutionContext fields
- [x] S8 - Unify decorator kind checking
- [x] S9 - Move QueryLanguage
- [x] S10 - Fix RunOptions import
- [x] S11 - Merge comma_separated_enum into comma_separated_list
- [x] S12 - Eliminate _find_ancestor duplication
- [x] S13 - Remove underscore prefix from exports
- [x] S14 - Make CallSite frozen
- [x] S15 - Extract comprehension taint handler
- [x] S16 - Fix list[object] typing
- [x] S17 - CqSummary schema/version groundwork
- [x] S18 - Rename tree_sitter_blob_store.py
- [x] D1 - Delete _summary_value and _result_match_count duplicates
- [x] D2 - Delete _find_ancestor and node_payload duplicates

### Wave 2: Knowledge Consolidation
- [x] S19 - Entity-kind registry consolidation
- [x] S20 - Unify Rust role classification
- [x] S21 - Metavar extraction unification
- [x] S22 - Extract cache decode function
- [x] S23 - Consolidate Rust def regex
- [x] S24 - Group risk thresholds into struct
- [x] D3 - Delete hardcoded entity-kind sets

### Wave 3: God Module Decomposition
- [x] S25 - Complete front-door decomposition
- [x] S26 - Extract classify_match to classification.py
- [x] S27 - Complete search pipeline phase extraction
- [x] S28 - Extract query cache ceremony
- [x] S29 - Decompose executor.py
- [x] S30 - Decompose rust_lane/runtime.py
- [x] S31 - Decompose result.py
- [x] S32 - Extract Q-step preparation
- [x] D4 - Delete smart_search.py duplicates
- [x] D5 - Delete cache ceremony code

### Wave 4: Protocol & Boundary Improvements
- [x] S33 - Extend CqCacheBackend protocol
- [x] S34 - Type enrichment payloads
- [x] S35 - Thread RenderEnrichmentPort
- [x] S36 - Thread ClassifierCacheContext
- [x] S37 - Extract backend lifecycle
- [x] S38 - Require explicit backend injection
- [x] D6 - Delete protocol bypasses
- [x] D7 - Delete untyped enrichment handling

### Wave 5: Structural Type Safety
- [x] S39 - Return new Finding instances
- [x] S40 - Split attach_target_metadata
- [x] S41 - CqSummary typed variant migration
- [x] S42 - Define ContextWindow struct
- [x] S43 - Consolidate cache lifecycle

### Wave 6: Review-Driven Additions
- [x] S44 - Deduplicate LDMD root alias resolution
- [x] S45 - Add warning logging for immediate Q-step errors
- [x] S46 - Add CliContext.from_parts constructor
- [x] S47 - Move clear_caches side effect to smart_search orchestrator
- [x] S48 - Move _ast_grep_prefilter_scope_paths out of assembly layer
- [x] S49 - Move PYTHON_AST_FIELDS out of language-agnostic IR
- [x] S50 - Make ParamInfo frozen and constructor-complete
- [x] S51 - Encapsulate TaintState mutation API
- [x] S52 - Add Rust lane stage timing telemetry and payload postconditions
- [x] S53 - Canonicalize tree-sitter cancellation on progress_callback
- [x] D8 - Delete hidden clear_caches + assembly prefilter legacy
- [x] D9 - Delete IR Python field constant + timeout_micros plumbing
- [x] D10 - Delete monolithic CqSummary and dict-like mutation helpers

### Remaining Implementation Focus (Post-Audit)
Audit update (2026-02-17):

- No remaining scope items. S9, S28, and D5 are complete.

---

## Success Criteria

**Per-Wave Metrics**:

**Wave 1 (Quick Wins)**:
- 18 scope items complete
- ~1,500 LOC reduced
- 15+ duplication sites eliminated
- 0 correctness risks remaining
- Touch/protocol quick-fix bypasses eliminated
- Protocol bypass inventory complete for Wave 4 hard cutover

**Wave 2 (Knowledge Consolidation)**:
- 6 scope items complete
- Entity-kind registry authoritative
- Rust classification unified
- Cache decode canonical

**Wave 3 (God Module Decomposition)**:
- 8 scope items complete
- smart_search.py: 1981 LOC → 749 LOC orchestrator/support module
- executor.py: 1223 LOC → 37 LOC facade
- runtime.py: 1080 LOC → 21 LOC entrypoint (+ split `runtime_core.py` and helper modules)
- result.py: 382 LOC → 3 focused modules

**Wave 4 (Protocol & Boundary Improvements)**:
- 6 scope items complete
- CqCacheBackend protocol complete
- Enrichment payloads typed with strict decode boundaries
- All protocol bypasses eliminated
- Global state eliminated (2 globals removed)

**Wave 5 (Structural Type Safety)**:
- 5 scope items complete
- CQS violations fixed (2 sites)
- CqSummary converted to typed mode variants
- ContextWindow typed
- Cache lifecycle unified

**Wave 6 (Review-Driven Additions)**:
- 10 scope items complete
- All omitted high-signal findings from the seven design reviews incorporated
- Search pipeline CQS and boundary quick wins closed (`clear_caches`, assembly prefilter)
- CLI/run observability and testability gaps closed (`CliContext.from_parts`, immediate error logs)
- Tree-sitter cancellation model aligned to `progress_callback`

**Overall Success Criteria**:
- All 53 scope items complete
- ~5,500 LOC reduced
- 0 correctness risks
- 0 protocol bypasses
- 0 global state for render/cache context
- 0 CQS violations
- 100% typed boundaries (no dict[str, object] for known schemas)
- All tests passing (uv run pytest)
- All quality gates passing (ruff format, ruff check --fix, pyrefly check, pyright)

---

**End of Implementation Plan**
