# CQ Design Improvements Implementation Plan v1 (2026-02-15)

## Scope Summary

This plan covers 37 improvement items identified in the design review synthesis (`docs/reviews/design_review_synthesis_tools_cq_2026-02-15.md`) and consolidated plan (`docs/plans/cq_design_review_consolidated_implementation_plan_v1_2026-02-16.md`), organized into 4 implementation phases:

- **Phase 1 (Quick Wins):** 17 items (S1-S15, S33, S34) - small mechanical changes with immediate type-safety and DRY improvements
- **Phase 2 (Structural Prep):** 3 items - move vocabulary types and extract data models to enable safe decomposition
- **Phase 3 (Targeted Extractions + Guardrails):** 10 items (S19-S25, S35-S37) - medium-effort protocol/cache/payload extractions plus architecture/test harness enforcement
- **Phase 4 (God Module Decomposition):** 7 items - large-effort file decompositions

**Migration strategy:**
- **Strict hard cutover:** Type changes (Literal types, payload structs) are breaking changes and land atomically with all call sites updated in the same PR.
- **No compatibility shims:** Do not add re-export bridges, feature flags for compatibility, or long-lived deprecation aliases.
- **Contract reuse:** Reuse and extend existing contract modules when equivalent files already exist; do not introduce parallel near-duplicate contract files.
- **Legacy decommission:** Delete superseded code in the same scope item whenever possible; reserve D1-D5 batches for cross-scope coupled deletions.

**Timeline:** 9 weeks for full execution. Quick wins (Phase 1) deliver immediate value within 2 weeks.

---

## Design Principles

### Non-Negotiable Constraints

1. **Preserve composition-first design (3.0/3 score):** Zero class hierarchies. All behavior via function composition and frozen structs.
2. **Maintain idempotency + determinism (2.9/3, 2.6/3):** Read-only analysis, content-addressed caching, sorted results.
3. **Extend parse-don't-validate boundary enforcement (2.3/3):** All new types must use msgspec frozen structs with `convert_strict`.
4. **No duplicate tool configuration:** Do not replicate ruff/pyrefly rules in docstrings or comments.
5. **Fail-open on enrichment:** All enrichment planes must degrade gracefully without blocking core workflows.
6. **Test coverage parity:** Every extracted module must maintain coverage at or above original file's level via either direct module tests or explicitly mapped integration/e2e coverage.
7. **No upward imports:** Foundation, lane, and core modules must not import from orchestration modules. Dependency direction flows downward only.
8. **Explicit public API surfaces:** All modules must declare `__all__`; private (`_`-prefixed) imports across module boundaries are forbidden.
9. **Mutable state encapsulation:** Mutable process-global state must be encapsulated behind injectable abstractions (bounded caches, context objects).
10. **Strict hard cutover execution:** Scope items must land without compatibility shims (no transitional re-export or feature-flag compatibility mode).
11. **Contract file reuse over parallel contracts:** If a semantically equivalent contract module already exists, extend it in place rather than creating a sibling module with overlapping responsibility.

---

## Current Baseline

### Principle Alignment (System-Wide Averages)

| Strength | Score | Weakness | Score |
|----------|-------|----------|-------|
| Composition > Inheritance | 3.0/3 | SRP | 0.9/3 |
| Idempotency | 2.9/3 | Separation of Concerns | 1.1/3 |
| Determinism | 2.6/3 | DRY | 1.3/3 |
| Parse-Don't-Validate | 2.3/3 | Testability | 1.6/3 |

### God Modules (Root Cause of 68% of Violations)

| File | LOC | Violations |
|------|-----|------------|
| `smart_search.py` | 3,914 | 9 principles |
| `executor.py` | 3,457 | 8 principles |
| `calls.py` | 2,274 | 5 principles |
| `extractors.py` | 2,251 | 3 principles |
| `rust_lane/runtime.py` | 1,976 | 4 principles |
| `report.py` | 1,773 | 7 principles |
| `runner.py` | 1,297 | 6 principles |

**Total:** 16,942 LOC (23% of codebase)

### Additional Baseline Observations

- `CqResult.summary` and cache result summaries are untyped mappings (`dict[str, object]`) in `tools/cq/core/schema.py` and `tools/cq/core/cache/contracts.py`.
- Query batch modules import private executor internals (`tools/cq/query/batch.py`, `tools/cq/query/batch_spans.py` importing `_build_scan_context`, `_execute_rule_matches`, etc. from `tools/cq/query/executor.py`).
- `tools/cq/cli_app` command handlers currently use both `@require_ctx` decorator and `require_context()` function for redundant dual context validation.
- `tools/cq/search/pipeline/orchestration.py` currently imports a private symbol (`_assemble_smart_search_result`) from `smart_search.py`.
- `tools/cq/search/tree_sitter/query/drift.py` and `tools/cq/search/tree_sitter/query/registry.py` retain mutable process-global snapshot/report dictionaries.

---

## PHASE 1: QUICK WINS (Items 1-15)

---

## S1. Consolidate NodeLike Protocols

### Goal
Eliminate 5 duplicate `NodeLike` protocol definitions with inconsistent property sets. Define a single canonical protocol in `tools/cq/search/tree_sitter/contracts/core_models.py`.

### Representative Code Snippets

**Current state (5 definitions):**

```python
# tools/cq/search/tree_sitter/core/node_utils.py:13-39
class NodeLike(Protocol):
    start_byte: int
    end_byte: int
    start_point: tuple[int, int]
    end_point: tuple[int, int]

# tools/cq/search/tree_sitter/structural/exports.py:131-141
class NodeLike(Protocol):
    type: str
    start_byte: int
    end_byte: int

# tools/cq/search/tree_sitter/rust_lane/injections.py:18-31
class NodeLike(Protocol):
    start_byte: int
    end_byte: int
    start_point: tuple[int, int]
    end_point: tuple[int, int]

# tools/cq/search/tree_sitter/tags.py:30-41
class NodeLike(Protocol):
    start_byte: int
    end_byte: int

# tools/cq/search/tree_sitter/python_lane/locals_index.py:14-26
class NodeLike(Protocol):
    start_byte: int
    end_byte: int
    start_point: tuple[int, int]
    end_point: tuple[int, int]
    def child_by_field_name(self, name: str) -> NodeLike | None: ...
```

### Files to Edit

1. **Define canonical protocol:** `tools/cq/search/tree_sitter/contracts/core_models.py`
   - Add `NodeLike` protocol with superset of all properties (4 byte/point properties + `type` + `child_by_field_name`)
   - Add optional specialized protocols if needed: `NodeByteSpan` (just byte positions), `NodeWithType` (includes type field)

2. **Update imports in 5 files:**
   - `tools/cq/search/tree_sitter/core/node_utils.py:13` → delete local definition, import from contracts
   - `tools/cq/search/tree_sitter/structural/exports.py:131` → delete local definition, import from contracts
   - `tools/cq/search/tree_sitter/rust_lane/injections.py:18` → delete local definition, import from contracts
   - `tools/cq/search/tree_sitter/tags.py:30` → delete local definition, import from contracts
   - `tools/cq/search/tree_sitter/python_lane/locals_index.py:14` → delete local definition, import from contracts

### New Files to Create

None (use existing `contracts/core_models.py`).

### Legacy Decommission/Delete Scope

Delete 5 local `NodeLike` definitions after canonical import is stable.

---

## S2. Extract _normalize_semantic_version

### Goal
Consolidate 3 identical `_normalize_semantic_version` implementations into a single shared function in `tools/cq/search/tree_sitter/core/language_registry.py` (the module that owns the `Language` abstraction).

### Representative Code Snippets

**Current state (3 identical copies):**

```python
# tools/cq/search/tree_sitter/query/planner.py:21-26
_SEMANTIC_VERSION_PARTS = 3
def _normalize_semantic_version(version: str) -> tuple[int, ...]:
    parts = version.split(".")[:_SEMANTIC_VERSION_PARTS]
    return tuple(int(p) for p in parts)

# tools/cq/search/tree_sitter/schema/node_schema.py:93-98
_SEMANTIC_VERSION_PARTS = 3
def _normalize_semantic_version(version: str) -> tuple[int, ...]:
    parts = version.split(".")[:_SEMANTIC_VERSION_PARTS]
    return tuple(int(p) for p in parts)

# tools/cq/search/tree_sitter/core/language_registry.py:64-70
_SEMANTIC_VERSION_PARTS = 3
def _normalize_semantic_version(version: str) -> tuple[int, ...]:
    parts = version.split(".")[:_SEMANTIC_VERSION_PARTS]
    return tuple(int(p) for p in parts)
```

### Files to Edit

1. **Keep canonical implementation:** `tools/cq/search/tree_sitter/core/language_registry.py:64-70`
   - Keep the existing definition (this is the natural home for language version parsing)
   - Make it public: rename to `normalize_semantic_version` (drop leading underscore)
   - Add to `__all__` exports

2. **Update imports in 2 files:**
   - `tools/cq/search/tree_sitter/query/planner.py:21` → delete local definition, import `normalize_semantic_version` from `core.language_registry`
   - `tools/cq/search/tree_sitter/schema/node_schema.py:93` → delete local definition, import from language_registry

### New Files to Create

None.

### Legacy Decommission/Delete Scope

Delete 2 duplicate definitions and their local `_SEMANTIC_VERSION_PARTS` constants.

---

## S3. Extract _python_field_ids

### Goal
Consolidate 4 identical `@lru_cache(maxsize=1)` `_python_field_ids()` functions into a single shared utility.

### Representative Code Snippets

**Current state (4 identical copies):**

```python
# tools/cq/search/tree_sitter/python_lane/locals_index.py:38-40
@lru_cache(maxsize=1)
def _python_field_ids() -> dict[str, int]:
    return cached_field_ids("python")

# tools/cq/search/tree_sitter/python_lane/facts.py:113-115
@lru_cache(maxsize=1)
def _python_field_ids() -> dict[str, int]:
    return cached_field_ids("python")

# tools/cq/search/tree_sitter/python_lane/runtime.py:61-63
@lru_cache(maxsize=1)
def _python_field_ids() -> dict[str, int]:
    return cached_field_ids("python")

# tools/cq/search/tree_sitter/python_lane/fallback_support.py:18-20
@lru_cache(maxsize=1)
def _python_field_ids() -> dict[str, int]:
    return cached_field_ids("python")
```

### Files to Edit

1. **Create shared utility:** `tools/cq/search/tree_sitter/python_lane/runtime.py:61-63`
   - Keep the existing definition in `runtime.py` (it's the main orchestrator module for python_lane)
   - Make it public: rename to `get_python_field_ids` (drop leading underscore)
   - Add to `__all__` exports at top of file

2. **Update imports in 3 files:**
   - `tools/cq/search/tree_sitter/python_lane/locals_index.py:38` → delete local definition, import `get_python_field_ids` from `.runtime`
   - `tools/cq/search/tree_sitter/python_lane/facts.py:113` → delete local definition, import from `.runtime`
   - `tools/cq/search/tree_sitter/python_lane/fallback_support.py:18` → delete local definition, import from `.runtime`

### New Files to Create

None.

### Legacy Decommission/Delete Scope

Delete 3 duplicate definitions.

---

## S4. Define Literal Types for Categorical Fields

### Goal
Replace 6 bare `str` categorical fields with `Literal` unions to prevent invalid values at construction time.

### Representative Code Snippets

**Before:**

```python
# tools/cq/index/call_resolver.py:67
@frozen
class ResolvedCall:
    confidence: str  # Actually: "exact" | "likely" | "ambiguous" | "unresolved"

# tools/cq/macros/impact.py:53
@frozen
class TaintedSite:
    kind: str  # Actually: "source" | "call" | "return" | "assign"

# tools/cq/macros/calls.py:156
@frozen
class CallSite:
    binding: str  # Actually: "ok" | "ambiguous" | "would_break" | "unresolved"

# tools/cq/introspection/cfg_builder.py:72
@frozen
class CFGEdge:
    edge_type: str  # Actually: "fallthrough" | "jump" | "exception"

# tools/cq/search/enrichment/contracts.py:9
EnrichmentStatus = str  # Actually: "applied" | "degraded" | "skipped"

# tools/cq/cli_app/context.py:165
@frozen
class CliResult:
    result: Any  # Actually: CqResult | CliTextResult | int
```

**After:**

```python
# tools/cq/index/call_resolver.py
CallConfidence = Literal["exact", "likely", "ambiguous", "unresolved"]

@frozen
class ResolvedCall:
    confidence: CallConfidence

# tools/cq/macros/impact.py
TaintSiteKind = Literal["source", "call", "return", "assign"]

@frozen
class TaintedSite:
    kind: TaintSiteKind

# tools/cq/macros/calls.py
CallBinding = Literal["ok", "ambiguous", "would_break", "unresolved"]

@frozen
class CallSite:
    binding: CallBinding

# tools/cq/introspection/cfg_builder.py
CfgEdgeType = Literal["fallthrough", "jump", "exception"]

@frozen
class CFGEdge:
    edge_type: CfgEdgeType

# tools/cq/search/enrichment/contracts.py
EnrichmentStatus = Literal["applied", "degraded", "skipped"]

# tools/cq/cli_app/context.py
CliResultPayload = CqResult | CliTextResult | int

@frozen
class CliResult:
    result: CliResultPayload
```

### Files to Edit

1. `tools/cq/index/call_resolver.py:67` → Add `CallConfidence` type alias, update `ResolvedCall.confidence` field
2. `tools/cq/macros/impact.py:53` → Add `TaintSiteKind` type alias, update `TaintedSite.kind` field
3. `tools/cq/macros/calls.py:156` → Add `CallBinding` type alias, update `CallSite.binding` field
4. `tools/cq/introspection/cfg_builder.py:72` → Add `CfgEdgeType` type alias, update `CFGEdge.edge_type` field
5. `tools/cq/search/enrichment/contracts.py:9` → Update `EnrichmentStatus = str` to `EnrichmentStatus = Literal[...]`
6. `tools/cq/cli_app/context.py:165` → Add `CliResultPayload` type alias, update `CliResult.result: Any` to `result: CliResultPayload`

### New Files to Create

None.

### Legacy Decommission/Delete Scope

None (in-place type refinement).

---

## S5. Move _truncation_tracker into Per-Call State

### Goal
Eliminate module-level mutable `_truncation_tracker: list[str]` that leaks state between calls. Move into per-invocation context.

### Representative Code Snippets

**Current state:**

```python
# tools/cq/search/python/extractors.py:187
_truncation_tracker: list[str] = []

# Cleared at multiple locations:
# Line 2101
_truncation_tracker.clear()
# Line 2070
_truncation_tracker.clear()
# Line 2219
_truncation_tracker.clear()

# Appended at line 215
_truncation_tracker.append(f"Truncated {name} at {limit} items")
```

### Files to Edit

1. **Define context struct:** Add to `tools/cq/search/python/extractors.py` (near top after imports):

```python
@frozen
class EnrichmentContext:
    """Per-invocation context for enrichment operations."""
    truncations: list[str] = field(default_factory=list)
```

2. **Thread context through call stack:**
   - Update `_enrich_python_ast()` signature to accept `context: EnrichmentContext`
   - Update `_extract_class_structure()`, `_extract_import_details()`, etc. to accept context
   - Replace `_truncation_tracker.append(...)` with `context.truncations.append(...)`
   - Replace `_truncation_tracker.clear()` calls with fresh `EnrichmentContext()` construction

3. **Update callsites:** Thread context from top-level entry points (lines 2101, 2070, 2219)

### New Files to Create

None.

### Legacy Decommission/Delete Scope

Delete module-level `_truncation_tracker: list[str]` declaration at line 187.

---

## S6. Deduplicate Query Utility Functions

### Goal
Consolidate 3 sets of duplicated query utilities into single shared implementations.

### Representative Code Snippets

**Duplicate set 1: _count_result_matches**

```python
# tools/cq/query/executor.py:966-973
def _count_result_matches(result: CqResult) -> int:
    if result.findings is None:
        return 0
    return sum(len(f.matches) for f in result.findings)

# tools/cq/query/merge.py:29-36
def _count_result_matches(result: CqResult) -> int:
    if result.findings is None:
        return 0
    return sum(len(f.matches) for f in result.findings)
```

**Duplicate set 2: _missing_languages_from_summary**

```python
# tools/cq/query/entity_front_door.py:446-456
def _missing_languages_from_summary(summary: dict[str, object]) -> list[str]:
    langs = summary.get("missing_languages")
    if isinstance(langs, list):
        return [str(x) for x in langs]
    return []

# tools/cq/query/merge.py:41-51
def _missing_languages_from_summary(summary: dict[str, object]) -> list[str]:
    langs = summary.get("missing_languages")
    if isinstance(langs, list):
        return [str(x) for x in langs]
    return []
```

**Duplicate set 3: _extract_def_name (3 variants)**

```python
# tools/cq/query/executor.py:2744+
def _extract_def_name(node: ast.AST) -> str | None:
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return node.name
    return None

# tools/cq/query/symbol_resolver.py:204+
def _extract_def_name(node: ast.AST) -> str | None:
    # ... similar logic with slight variations

# tools/cq/query/enrichment.py:434+
def _extract_def_name(node: ast.AST) -> str | None:
    # ... similar logic with slight variations
```

### Files to Edit

1. **Create shared utilities module:** `tools/cq/query/shared_utils.py`

```python
"""Shared utility functions for query subsystem."""
from __future__ import annotations

import ast
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.core.contracts import CqResult

def count_result_matches(result: CqResult) -> int:
    """Count total matches across all findings in result."""
    if result.findings is None:
        return 0
    return sum(len(f.matches) for f in result.findings)

def extract_missing_languages(summary: dict[str, object]) -> list[str]:
    """Extract missing_languages list from summary dict."""
    langs = summary.get("missing_languages")
    if isinstance(langs, list):
        return [str(x) for x in langs]
    return []

def extract_def_name(node: ast.AST) -> str | None:
    """Extract name from function/class definition node."""
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return node.name
    return None
```

2. **Update imports in 5 files:**
   - `tools/cq/query/executor.py:966` → delete `_count_result_matches`, import `count_result_matches` from `.shared_utils`, update references
   - `tools/cq/query/merge.py:29` → delete `_count_result_matches`, import from `.shared_utils`
   - `tools/cq/query/entity_front_door.py:446` → delete `_missing_languages_from_summary`, import `extract_missing_languages` from `.shared_utils`
   - `tools/cq/query/merge.py:41` → delete `_missing_languages_from_summary`, import from `.shared_utils`
   - `tools/cq/query/executor.py:2744` + `symbol_resolver.py:204` + `enrichment.py:434` → delete local `_extract_def_name`, import `extract_def_name` from `.shared_utils`

### New Files to Create

- `tools/cq/query/shared_utils.py` (~50 LOC)

### Legacy Decommission/Delete Scope

Delete 7 duplicate function definitions across 5 files.

---

## S7. Add __all__ Exports

### Goal
Add missing `__all__` declarations to `enrichment.py` and `planner.py` to match existing pattern in `executor.py`.

### Representative Code Snippets

**Current state:**

```python
# tools/cq/query/executor.py:89 - HAS __all__
__all__ = [
    "execute_query",
    "execute_entity_query",
    "CqResult",
    # ... etc
]

# tools/cq/query/enrichment.py - MISSING __all__
# tools/cq/query/planner.py - MISSING __all__
```

### Files to Edit

1. **Add to `tools/cq/query/enrichment.py`** (after imports, before first function):

```python
__all__ = [
    "enrich_matches",
    "build_enrichment_payload",
    # ... list all public functions
]
```

2. **Add to `tools/cq/query/planner.py`** (after imports, before first function):

```python
__all__ = [
    "plan_query",
    "QueryPlan",
    # ... list all public types/functions
]
```

### New Files to Create

None.

### Legacy Decommission/Delete Scope

None.

---

## S8. Consolidate Dual Context Injection

### Goal
Eliminate redundant dual validation pattern: `@require_ctx` decorator + `require_context()` function both validate context presence. Consolidate to single mechanism.

### Representative Code Snippets

**Current state:**

```python
# tools/cq/cli_app/infrastructure.py:81-96
def require_ctx(f: Callable[P, T]) -> Callable[P, T]:
    """Decorator: validates ctx kwarg is present and non-None."""
    @wraps(f)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        if "ctx" not in kwargs or kwargs["ctx"] is None:
            raise TypeError(f"{f.__name__} requires ctx kwarg")
        return f(*args, **kwargs)
    return wrapper

# tools/cq/cli_app/infrastructure.py:99-108
def require_context(ctx: CqCliContext | None) -> CqCliContext:
    """Function: converts Optional[CqCliContext] to non-optional."""
    if ctx is None:
        raise RuntimeError("Context not available")
    return ctx

# Used in paired fashion across command files:
@require_ctx
def some_command(..., ctx: CqCliContext | None = None) -> ...:
    ctx = require_context(ctx)  # Redundant second validation
    ...
```

### Files to Edit

1. **Deprecate decorator approach:** `tools/cq/cli_app/infrastructure.py:81-96`
   - Remove `@require_ctx` decorator definition
   - Keep only `require_context()` function (lines 99-108)
   - Update docstring to clarify it's the single validation mechanism

2. **Update command modules under** `tools/cq/cli_app/commands/*.py`:
   - Verified affected modules include `admin.py`, `analysis.py`, `artifact.py`, `chain.py`, `ldmd.py`, `neighborhood.py`, `query.py`, `report.py`, `repl.py`, `run.py`, `search.py`
   - Remove `@require_ctx` decorator
   - Keep `ctx = require_context(ctx)` call inside function body
   - Change signature from `ctx: CqCliContext | None = None` to `ctx: CqCliContext | None = None` (no change needed)

### New Files to Create

None.

### Legacy Decommission/Delete Scope

Delete `@require_ctx` decorator definition and all decorator usages.

---

## S9. Move Semantic Contracts from search/ to core/

### Goal
Fix dependency inversion: `core/front_door_insight.py` should not import from `search/semantic/models.py`. Move `SemanticContractStateInputV1` and `SemanticStatus` to core.

### Representative Code Snippets

**Current state:**

```python
# tools/cq/search/semantic/models.py:18
SemanticStatus = Literal["ready", "degraded", "unavailable"]

# tools/cq/search/semantic/models.py:83-92
@frozen
class SemanticContractStateInputV1:
    status: SemanticStatus
    missing_capabilities: list[str]
    degraded_reasons: list[str]

# Imported by core (WRONG DIRECTION):
# tools/cq/core/front_door_insight.py imports from search/semantic/models.py
```

### Files to Edit

1. **Create new core module:** `tools/cq/core/semantic_contracts.py`

```python
"""Semantic enrichment contract types."""
from __future__ import annotations

from typing import Literal
from msgspec import frozen

SemanticProvider = Literal["python_static", "rust_static", "none"]
SemanticStatus = Literal["ready", "degraded", "unavailable"]

@frozen
class SemanticContractStateInputV1:
    """Input state for semantic enrichment contracts."""
    provider: SemanticProvider
    available: bool
    status: SemanticStatus
    missing_capabilities: list[str]
    degraded_reasons: list[str]
    attempted: int = 0
    applied: int = 0
    failed: int = 0
    timed_out: int = 0
    reasons: tuple[str, ...] = ()


def derive_semantic_contract_state(...) -> SemanticContractStateInputV1:
    """Derive semantic contract state from enrichment results.

    Moved from tools/cq/search/semantic/models.py to fix dependency inversion.
    """
    # ... implementation moved from search/semantic/models.py

__all__ = [
    "SemanticProvider",
    "SemanticStatus",
    "SemanticContractStateInputV1",
    "derive_semantic_contract_state",
]
```

2. **Update imports in 6 files:**
   - `tools/cq/core/front_door_insight.py` → import from `..core.semantic_contracts` instead of search
   - `tools/cq/query/entity_front_door.py` → import from `..core.semantic_contracts`
   - `tools/cq/query/merge.py` → import from `..core.semantic_contracts`
   - `tools/cq/macros/calls.py` → import from `..core.semantic_contracts`
   - `tools/cq/macros/sig_impact.py` → import from `..core.semantic_contracts`
   - `tools/cq/search/pipeline/smart_search.py` → import from `...core.semantic_contracts`

3. **Hard-cutover cleanup in old location:** `tools/cq/search/semantic/models.py`
   - Delete moved semantic contract definitions from the search-layer module.
   - Remove deprecated alias names (`fail_open`, `enrich_semantics`) in the same PR.
   - Do not add transitional re-exports.

### New Files to Create

- `tools/cq/core/semantic_contracts.py` (~50 LOC)

### Legacy Decommission/Delete Scope

Hard-cutover in this scope item: delete original definitions of `SemanticProvider`, `SemanticStatus`, `SemanticContractStateInputV1`, and `derive_semantic_contract_state` from `search/semantic/models.py` in the same PR that migrates imports. Also delete deprecated alias names (`fail_open`, `enrich_semantics`) with no compatibility shim period.

---

## S10. Extract Shared AST Helpers

### Goal
Consolidate identical `_node_byte_span` and `_ast_node_priority` functions duplicated across `analysis_session.py` and `resolution_support.py`.

### Representative Code Snippets

**Current state:**

```python
# tools/cq/search/python/analysis_session.py:36-53
def _node_byte_span(node: ast.AST, source_bytes: bytes) -> tuple[int, int]:
    """Extract byte span from AST node."""
    # ... implementation

# tools/cq/search/python/analysis_session.py:67-78
def _ast_node_priority(node: ast.AST) -> int:
    """Assign priority for node type selection."""
    # ... implementation

# tools/cq/search/python/resolution_support.py:62-87
def _node_byte_span(node: ast.AST, source_bytes: bytes) -> tuple[int, int]:
    """Extract byte span from AST node."""
    # ... identical implementation

# tools/cq/search/python/resolution_support.py:90-101
def _ast_node_priority(node: ast.AST) -> int:
    """Assign priority for node type selection."""
    # ... identical implementation
```

### Files to Edit

1. **Create shared utilities module:** `tools/cq/search/python/ast_utils.py`

```python
"""Shared AST utility functions for Python search."""
from __future__ import annotations

import ast

def node_byte_span(node: ast.AST, source_bytes: bytes) -> tuple[int, int]:
    """Extract byte span (bstart, bend) from AST node.

    Parameters
    ----------
    node : ast.AST
        AST node with lineno/col_offset attributes
    source_bytes : bytes
        Source code as bytes

    Returns
    -------
    tuple[int, int]
        (bstart, bend) byte offsets
    """
    # ... implementation from current _node_byte_span

def ast_node_priority(node: ast.AST) -> int:
    """Assign priority for node type selection.

    Higher priority wins when multiple nodes overlap.

    Parameters
    ----------
    node : ast.AST
        AST node

    Returns
    -------
    int
        Priority score (higher = more specific)
    """
    # ... implementation from current _ast_node_priority

__all__ = ["node_byte_span", "ast_node_priority"]
```

2. **Update imports in 2 files:**
   - `tools/cq/search/python/analysis_session.py:36` → delete both local functions, import from `.ast_utils`, update references to drop underscore prefix
   - `tools/cq/search/python/resolution_support.py:62` → delete both local functions, import from `.ast_utils`

### New Files to Create

- `tools/cq/search/python/ast_utils.py` (~80 LOC)

### Legacy Decommission/Delete Scope

Delete 4 duplicate function definitions (2 in each file).

---

## S11. Remove DefIndex.load_or_build Dead Code

### Goal
Delete `DefIndex.load_or_build()` method that always just calls `build()` with no actual caching logic.

### Representative Code Snippets

**Current state:**

```python
# tools/cq/index/def_index.py:478-513
@classmethod
def load_or_build(cls, root: Path) -> DefIndex:
    """Load from cache or build fresh index.

    NOTE: Currently always builds fresh. Caching removed.
    """
    return cls.build(root)  # Just delegates to build()
```

### Files to Edit

1. **Delete method:** `tools/cq/index/def_index.py:478-513`
   - Remove `load_or_build()` method entirely

2. **Callsite verification:**
   - Repository-wide search currently shows no callsites for `DefIndex.load_or_build`.
   - Add a regression assertion/test to keep callsite count at zero after method deletion.

### New Files to Create

None.

### Legacy Decommission/Delete Scope

Delete `load_or_build()` method definition (~35 LOC).

---

## S12. Extract Shared Coercion Helpers

> **Note:** In S31 (report.py decomposition), these coercion helpers will be further consolidated into a dedicated `tools/cq/core/type_coercion.py` module alongside other duplicated coercion helpers from `schema.py` and `scoring.py`.

### Goal
Move `_coerce_float` and `_coerce_str` static methods from `DetailPayload` class to module-level utilities for reuse.

### Representative Code Snippets

**Current state:**

```python
# tools/cq/core/schema.py:48-61
@frozen
class DetailPayload:
    data: dict[str, object]

    @staticmethod
    def _coerce_float(value: object) -> float:
        if isinstance(value, (int, float)):
            return float(value)
        raise TypeError(f"Expected numeric value, got {type(value)}")

    @staticmethod
    def _coerce_str(value: object) -> str:
        if isinstance(value, str):
            return value
        raise TypeError(f"Expected str, got {type(value)}")
```

### Files to Edit

1. **Extract to module level:** `tools/cq/core/schema.py` (move above `DetailPayload` class)

```python
def coerce_float(value: object) -> float:
    """Coerce value to float, raising TypeError if not numeric."""
    if isinstance(value, (int, float)):
        return float(value)
    raise TypeError(f"Expected numeric value, got {type(value)}")

def coerce_str(value: object) -> str:
    """Coerce value to str, raising TypeError if not string."""
    if isinstance(value, str):
        return value
    raise TypeError(f"Expected str, got {type(value)}")

# Add to __all__:
__all__ = [..., "coerce_float", "coerce_str"]
```

2. **Update references in same file:**
   - Change `DetailPayload._coerce_float(x)` to `coerce_float(x)`
   - Change `DetailPayload._coerce_str(x)` to `coerce_str(x)`

### New Files to Create

None.

### Legacy Decommission/Delete Scope

Delete static methods from `DetailPayload` class.

---

## S13. Fix canonicalize Mutations

### Goal
Make `canonicalize_python_lane_payload` and `canonicalize_rust_lane_payload` pure functions that return new dicts instead of mutating input.

### Representative Code Snippets

**Current state:**

```python
# tools/cq/search/tree_sitter/contracts/lane_payloads.py
def canonicalize_python_lane_payload(payload: dict[str, object]) -> dict[str, object]:
    """Canonicalize Python lane payload by removing unstable keys."""
    payload.pop("query_text", None)  # MUTATES INPUT
    payload.pop("timestamp", None)   # MUTATES INPUT
    return payload

def canonicalize_rust_lane_payload(payload: dict[str, object]) -> dict[str, object]:
    """Canonicalize Rust lane payload by removing unstable keys."""
    payload.pop("query_text", None)  # MUTATES INPUT
    payload.pop("timestamp", None)   # MUTATES INPUT
    return payload
```

**After (copy-first approach):**

```python
def canonicalize_python_lane_payload(payload: dict[str, object]) -> dict[str, object]:
    """Canonicalize Python lane payload by removing unstable keys.

    Returns a new dict; does not mutate input.
    """
    payload = dict(payload)  # shallow copy - preserves .pop() + conditional logic
    legacy = payload.pop("tree_sitter_diagnostics", None)
    payload.pop("query_text", None)
    payload.pop("timestamp", None)
    # ... remaining conditional logic preserved as-is
    return payload

def canonicalize_rust_lane_payload(payload: dict[str, object]) -> dict[str, object]:
    """Canonicalize Rust lane payload by removing unstable keys.

    Returns a new dict; does not mutate input.
    """
    payload = dict(payload)  # shallow copy
    payload.pop("query_text", None)
    payload.pop("timestamp", None)
    return payload
```

> **Note:** The copy-first approach (`payload = dict(payload)`) is preferred over dict comprehension because it preserves the existing `.pop()` + conditional logic (e.g., `tree_sitter_diagnostics` handling) while only adding one line at the top. More maintainable than rewriting as comprehension.

### Files to Edit

1. `tools/cq/search/tree_sitter/contracts/lane_payloads.py` → add `payload = dict(payload)` as first line in both functions to copy before mutating

### New Files to Create

None.

### Legacy Decommission/Delete Scope

None (in-place fix).

---

## S14. Rename no_semantic_enrichment to Positive Form

### Goal
Rename `NeighborhoodStep.no_semantic_enrichment` boolean field to positive form `semantic_enrichment` with inverted logic.

### Representative Code Snippets

**Current state:**

```python
# tools/cq/run/spec.py:111-114
@frozen
class NeighborhoodStep:
    no_semantic_enrichment: bool = False  # NEGATIVE BOOLEAN
```

**After:**

```python
@frozen
class NeighborhoodStep:
    semantic_enrichment: bool = True  # POSITIVE BOOLEAN (default True)
```

### Files to Edit

1. **Update struct definition:** `tools/cq/run/spec.py`
   - Rename field: `no_semantic_enrichment` → `semantic_enrichment`
   - Invert default: `False` → `True`

2. **Update all references:** `tools/cq/run/runner.py`
   - Invert condition checks at existing call sites:
     - `enable_semantic_enrichment=not step.no_semantic_enrichment`
     - `enable_semantic_enrichment=not step.no_semantic_enrichment`
   - Replace with `enable_semantic_enrichment=step.semantic_enrichment`

### New Files to Create

None.

### Legacy Decommission/Delete Scope

None (in-place rename).

---

## S15. Export is_section_collapsed

### Goal
Add and export `is_section_collapsed()` from neighborhood section-layout helpers so collapse policy is centralized and callable from downstream renderers/tests.

### Representative Code Snippets

**Current state:**

```python
# tools/cq/neighborhood/section_layout.py:318-324
collapsed = True
if slice_.kind in _UNCOLLAPSED_SECTIONS:
    collapsed = False
elif threshold is not None:
    collapsed = slice_.total > threshold
```

**After:**

```python
# tools/cq/neighborhood/section_layout.py
def is_section_collapsed(kind: str, total: int) -> bool:
    """Return whether a section kind should default to collapsed."""
    if kind in _UNCOLLAPSED_SECTIONS:
        return False
    threshold = _DYNAMIC_COLLAPSE_SECTIONS.get(kind)
    if threshold is None:
        return True
    return total > threshold
```

### Files to Edit

1. **Add helper and reuse it:** `tools/cq/neighborhood/section_layout.py`
   - Introduce `is_section_collapsed(kind: str, total: int) -> bool`
   - Replace inline collapse branching in `_slice_to_section` with helper call
   - Add `"is_section_collapsed"` to module `__all__`

2. **Expose from package surface:** `tools/cq/neighborhood/__init__.py`
   - Export `is_section_collapsed` from neighborhood package API

### New Files to Create

None.

### Legacy Decommission/Delete Scope

None.

---

## PHASE 2: STRUCTURAL PREPARATION (Items 16-18)

---

## S16. Move QueryMode + SearchLimits to _shared/types.py

### Goal
Fix dependency inversion: 8 modules in search lanes import upward from `pipeline/`. Move `QueryMode` enum and `SearchLimits` struct to foundation layer.

### Representative Code Snippets

**Current state:**

```python
# tools/cq/search/pipeline/classifier.py:36
class QueryMode(Enum):
    IDENTIFIER = "identifier"
    REGEX = "regex"
    LITERAL = "literal"

# tools/cq/search/pipeline/profiles.py:12
@frozen
class SearchLimits:
    max_files: int
    max_matches_per_file: int
    max_context_lines: int

# 8 modules import upward:
# tools/cq/search/_shared/core.py:26-27
from tools.cq.search.pipeline.classifier import QueryMode
from tools.cq.search.pipeline.profiles import SearchLimits

# Similar imports in:
# - pipeline/contracts.py
# - pipeline/partition_pipeline.py
# - rg/adapter.py
# - rg/collector.py
# - rg/prefilter.py
# - rg/runner.py
```

### Files to Edit

1. **Create foundation types module:** `tools/cq/search/_shared/types.py`

```python
"""Foundation vocabulary types for search subsystem."""
from __future__ import annotations

from enum import Enum
from msgspec import frozen

class QueryMode(Enum):
    """Query execution mode."""
    IDENTIFIER = "identifier"
    REGEX = "regex"
    LITERAL = "literal"

@frozen
class SearchLimits:
    """Resource limits for search operations."""
    max_files: int
    max_matches_per_file: int
    max_context_lines: int

__all__ = ["QueryMode", "SearchLimits"]
```

2. **Hard-cutover update in old locations:**
   - `tools/cq/search/pipeline/classifier.py` → delete local `QueryMode` definition and import from `.._shared.types`
   - `tools/cq/search/pipeline/profiles.py` → delete local `SearchLimits` definition and import from `.._shared.types`

3. **Update imports in 8 files:**
   - `tools/cq/search/_shared/core.py:26-27` → import from `.types` instead of `..pipeline`
   - `tools/cq/search/pipeline/contracts.py:16-17` → import from `.._shared.types`
   - `tools/cq/search/pipeline/partition_pipeline.py:38` → import from `.._shared.types`
   - `tools/cq/search/rg/adapter.py:17-18` → import from `.._shared.types`
   - `tools/cq/search/rg/collector.py:31` → import from `.._shared.types`
   - `tools/cq/search/rg/prefilter.py:11-12` → import from `.._shared.types`
   - `tools/cq/search/rg/runner.py:12-13` → import from `.._shared.types`

### New Files to Create

- `tools/cq/search/_shared/types.py` (~40 LOC)

### Legacy Decommission/Delete Scope

Hard-cutover in this scope item: delete original definitions from `classifier.py` and `profiles.py` in the same PR. No compatibility re-export phase.

---

## S17. Create core/lane_support.py for Shared Lane Utilities

### Goal
Consolidate 6+ functions duplicated across `python_lane/` and `rust_lane/` into shared utilities module.

### Representative Code Snippets

**Duplicated functions:**

```python
# _build_query_windows duplicated in:
# - tools/cq/search/tree_sitter/python_lane/runtime.py:162
# - tools/cq/search/tree_sitter/python_lane/facts.py:281
# - tools/cq/search/tree_sitter/rust_lane/runtime.py:1427

# _lift_anchor duplicated in:
# - tools/cq/search/tree_sitter/python_lane/runtime.py:487
# - tools/cq/search/tree_sitter/python_lane/facts.py:170

# _make_parser / _parse_tree duplicated in:
# - tools/cq/search/tree_sitter/python_lane/runtime.py:98-111
# - tools/cq/search/tree_sitter/rust_lane/runtime.py:376-389

# _ENRICHMENT_ERRORS duplicated in:
# - tools/cq/search/tree_sitter/python_lane/runtime.py:57
# - tools/cq/search/tree_sitter/rust_lane/runtime.py:95
```

### Files to Edit

1. **Create shared utilities module:** `tools/cq/search/tree_sitter/core/lane_support.py`

```python
"""Shared utilities for tree-sitter lane operations."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tree_sitter import Language, Parser, Tree, Node

# Shared error tuple for enrichment operations
ENRICHMENT_ERRORS = (
    ValueError,
    TypeError,
    KeyError,
    AttributeError,
    IndexError,
)

def build_query_windows(
    anchor: tuple[int, int],
    context_lines: int,
    total_lines: int,
) -> list[tuple[int, int]]:
    """Build line-based query windows around anchor span."""
    # ... consolidated implementation

def lift_anchor(
    node: Node,
    context_lines: int,
) -> tuple[int, int]:
    """Lift anchor span to include context lines."""
    # ... consolidated implementation

def make_parser(language: Language) -> Parser:
    """Create parser instance for language."""
    parser = Parser()
    parser.set_language(language)
    return parser

def parse_tree(source_bytes: bytes, parser: Parser) -> Tree:
    """Parse source bytes into tree."""
    return parser.parse(source_bytes)

__all__ = [
    "ENRICHMENT_ERRORS",
    "build_query_windows",
    "lift_anchor",
    "make_parser",
    "parse_tree",
]
```

2. **Update imports in 6+ files:**
   - `tools/cq/search/tree_sitter/python_lane/runtime.py` → delete local definitions, import from `..core.lane_support`
   - `tools/cq/search/tree_sitter/python_lane/facts.py` → delete local definitions, import from `..core.lane_support`
   - `tools/cq/search/tree_sitter/rust_lane/runtime.py` → delete local definitions, import from `..core.lane_support`

### New Files to Create

- `tools/cq/search/tree_sitter/core/lane_support.py` (~120 LOC)

### Legacy Decommission/Delete Scope

Delete 10+ duplicate function/constant definitions across python_lane and rust_lane modules.

---

## S18. Extract Data Types from smart_search.py

### Goal
Extract 16 type definitions (~800 LOC) from `smart_search.py` into dedicated `smart_search_types.py` module. This unblocks safe decomposition and eliminates the `Any`-typed `_smart_search_module()` hack in `partition_pipeline.py`.

### Representative Code Snippets

**Current state:**

```python
# tools/cq/search/pipeline/smart_search.py:207-3290
# 16 type definitions mixed with pipeline logic

@frozen
class RawMatch:
    """Raw search match before enrichment."""
    # ...

@frozen
class EnrichedMatch:
    """Match after enrichment pipeline."""
    # ...

# ... 14 more type definitions

# tools/cq/search/pipeline/partition_pipeline.py:129-132
def _smart_search_module() -> Any:
    """Lazy import hack to access smart_search types."""
    from . import smart_search
    return smart_search
```

### Files to Edit

1. **Create types module:** `tools/cq/search/pipeline/smart_search_types.py`

Move these 16 type definitions from `smart_search.py`:
- `RawMatch`
- `EnrichedMatch`
- `ClassifiedMatch`
- `GroupedMatches`
- `FindingContext`
- `CodeFact`
- `CodeFactsCluster`
- `EnrichmentTelemetry`
- `SearchDiagnostics`
- `MatchClassification`
- `RoleClassification`
- `AnchorResolution`
- `NeighborhoodSlice`
- `InsightCard`
- `SearchSummary`
- `SmartSearchResult`

Each with full imports and docstrings.

```python
"""Type definitions for smart search pipeline."""
from __future__ import annotations

from msgspec import frozen

@frozen
class RawMatch:
    """Raw search match before enrichment."""
    file: str
    line: int
    column: int
    match_text: str
    context: str

# ... 15 more type definitions

__all__ = [
    "RawMatch",
    "EnrichedMatch",
    # ... all 16 types
]
```

2. **Update `smart_search.py`:**
   - Delete the 16 type definitions
   - Add import: `from .smart_search_types import *`
   - Keep all pipeline logic (~3100 LOC → ~2300 LOC after extraction)

3. **Update `partition_pipeline.py`:**
   - Delete `_smart_search_module()` hack (lines 129-132)
   - Add direct import: `from .smart_search_types import RawMatch, EnrichedMatch, ...`
   - Replace `_smart_search_module().RawMatch` with `RawMatch`

4. **Importer verification:**
   - Verify no remaining type-only imports reference `from .smart_search import ...` after extraction.

### New Files to Create

- `tools/cq/search/pipeline/smart_search_types.py` (~800 LOC)

### Legacy Decommission/Delete Scope

Delete type definitions from `smart_search.py` and `_smart_search_module()` hack from `partition_pipeline.py`.

---

## PHASE 3: TARGETED EXTRACTIONS (Items 19-25) + GUARDRAILS (Items 35-37)

---

## S19. Extract AST-Grep Execution from executor.py

### Goal
Extract ~450 LOC of ast-grep match execution logic from `executor.py` (lines 1168-1620) into dedicated `executor_ast_grep.py` module.

### Representative Code Snippets

**Functions to extract:**

```python
# tools/cq/query/executor.py:1168-1620
def _execute_ast_grep_rules(...)
def _run_ast_grep(...)
def _process_ast_grep_file(...)
def _process_ast_grep_rule(...)
def _collect_ast_grep_match_spans(...)
# ... ~10 more ast-grep related functions
```

### Files to Edit

1. **Create extraction module:** `tools/cq/query/executor_ast_grep.py`

```python
"""AST-grep rule execution for query subsystem."""
from __future__ import annotations

import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.core.contracts import Finding, Match

def execute_ast_grep_rules(
    rules: list[str],
    root: Path,
    max_matches: int,
) -> list[Finding]:
    """Execute ast-grep rules and collect findings."""
    # ... move implementation from executor.py:1168

def run_ast_grep(rule: str, root: Path) -> str:
    """Run ast-grep subprocess for single rule."""
    # ... move implementation

def process_ast_grep_file(...):
    """Process ast-grep output for single file."""
    # ... move implementation

# ... move all ast-grep related functions

__all__ = ["execute_ast_grep_rules"]
```

2. **Update `executor.py`:**
   - Delete lines 1168-1620 (~450 LOC)
   - Add import: `from .executor_ast_grep import execute_ast_grep_rules`
   - Replace internal calls to `_execute_ast_grep_rules` with `execute_ast_grep_rules`

### New Files to Create

- `tools/cq/query/executor_ast_grep.py` (~450 LOC)

### Legacy Decommission/Delete Scope

Delete ast-grep functions from `executor.py` (lines 1168-1620).

---

## S20. Extract Enrichment Telemetry from smart_search.py

### Goal
Extract enrichment telemetry accumulation functions (~140 LOC, lines 2149-2290) from `smart_search.py` into `smart_search_telemetry.py`.

### Representative Code Snippets

**Functions to extract:**

```python
# tools/cq/search/pipeline/smart_search.py:2149-2290
def _empty_enrichment_telemetry() -> EnrichmentTelemetry:
    """Create empty telemetry struct."""
    # ...

def _accumulate_stage_status(...):
    """Accumulate status counts by stage."""
    # ...

def _accumulate_stage_timings(...):
    """Accumulate timing stats by stage."""
    # ...

def _accumulate_python_enrichment(...):
    """Accumulate Python enrichment metrics."""
    # ...

def _attach_enrichment_cache_stats(...):
    """Attach cache hit/miss stats."""
    # ...

def _accumulate_rust_enrichment(...):
    """Accumulate Rust enrichment metrics."""
    # ...
```

### Files to Edit

1. **Create telemetry module:** `tools/cq/search/pipeline/smart_search_telemetry.py`

```python
"""Enrichment telemetry accumulation for smart search."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .smart_search_types import EnrichmentTelemetry, EnrichedMatch

def empty_enrichment_telemetry() -> EnrichmentTelemetry:
    """Create empty telemetry struct."""
    # ... move implementation

def accumulate_stage_status(...) -> None:
    """Accumulate status counts by stage."""
    # ... move implementation

# ... move all 6 telemetry functions

__all__ = [
    "empty_enrichment_telemetry",
    "accumulate_stage_status",
    "accumulate_stage_timings",
    "accumulate_python_enrichment",
    "attach_enrichment_cache_stats",
    "accumulate_rust_enrichment",
]
```

2. **Update `smart_search.py`:**
   - Delete lines 2149-2290 (~140 LOC)
   - Add import: `from .smart_search_telemetry import *`
   - Update references to drop underscore prefix

### New Files to Create

- `tools/cq/search/pipeline/smart_search_telemetry.py` (~140 LOC)

### Legacy Decommission/Delete Scope

Delete telemetry functions from `smart_search.py`.

---

## S21. Extract Enrichment Rendering from report.py

### Goal
Extract enrichment rendering logic (~252 LOC) from `report.py` into dedicated `render_enrichment.py` module. Use callback injection to break the core→search dependency inversion.

### Representative Code Snippets

**Functions to extract:**

```python
# tools/cq/core/report.py
def _extract_enrichment_payload(...):  # line 236
    """Extract enrichment payload from match."""
    # ...

def _format_enrichment_facts(...):  # line 417
    """Format enrichment facts as markdown."""
    # ...

def _merge_enrichment_details(...):  # line 778
    """Merge enrichment details across matches."""
    # ...

def _compute_render_enrichment_payload_from_anchor(...):  # line 785
    """Compute enrichment payload for render."""
    # ...

def _maybe_attach_render_enrichment(...):  # line 1083
    """Conditionally attach enrichment to render output."""
    # ...
```

### Files to Edit

1. **Create rendering module:** `tools/cq/core/render_enrichment.py`

```python
"""Enrichment rendering for report generation."""
from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.core.schema import Finding

# Callback type alias for enrichment injection
EnrichmentCallback = Callable[[Finding, Path], dict[str, object]]

def maybe_attach_render_enrichment(
    finding: Finding,
    *,
    root: Path,
    enrich: EnrichmentCallback | None,
) -> None:
    """Conditionally attach enrichment via callback.

    Uses callback injection to decouple core rendering from search internals.
    The callback is provided by the search layer, breaking the dependency inversion.
    """
    if enrich is None:
        return
    payload = enrich(finding, root)
    for key, value in payload.items():
        finding.details.setdefault(key, value)

def extract_enrichment_payload(match: Match) -> dict[str, object] | None:
    """Extract enrichment payload from match."""
    # ... move implementation

def format_enrichment_facts(facts: dict[str, object]) -> str:
    """Format enrichment facts as markdown."""
    # ... move implementation

# ... move all 5 enrichment rendering functions

__all__ = [
    "EnrichmentCallback",
    "extract_enrichment_payload",
    "format_enrichment_facts",
    "merge_enrichment_details",
    "compute_render_enrichment_payload",
    "maybe_attach_render_enrichment",
]
```

2. **Update `report.py`:**
   - Delete the 5 enrichment functions (~252 LOC)
   - Add import: `from .render_enrichment import *`
   - Update references to drop underscore prefix
   - Replace direct import/call path into search internals with `EnrichmentCallback` injection

### New Files to Create

- `tools/cq/core/render_enrichment.py` (~252 LOC)

### Legacy Decommission/Delete Scope

Delete enrichment rendering functions from `report.py`. Delete direct import/call path from `tools/cq/core/report.py` into `tools/cq/search/pipeline/smart_search.py` internals (`RawMatch`, `classify_match`, `build_finding`) after callback injection.

---

## S22. Extract Shared BoundedCache Class

### Goal
Consolidate 5+ manual LRU/FIFO eviction patterns into single shared `BoundedCache[K, V]` class in `_shared/bounded_cache.py`.

### Representative Code Snippets

**Current patterns (5+ locations):**

```python
# tools/cq/search/rust/enrichment.py - FIFO, 64 max
_AST_CACHE: dict[str, ast.Module] = {}

def _cache_ast(file: str, tree: ast.Module) -> None:
    if len(_AST_CACHE) >= 64:
        _AST_CACHE.pop(next(iter(_AST_CACHE)))  # FIFO eviction
    _AST_CACHE[file] = tree

# tools/cq/search/python/analysis_session.py - FIFO, 64 max
_SESSION_CACHE: dict[str, PythonAnalysisSession] = {}
# ... similar FIFO eviction pattern

# tools/cq/search/tree_sitter/rust_lane/runtime.py - LRU, 128 max
_TREE_CACHE: OrderedDict[str, Tree] = OrderedDict()

def _cache_tree(key: str, tree: Tree) -> None:
    if key in _TREE_CACHE:
        _TREE_CACHE.move_to_end(key)  # LRU
    else:
        _TREE_CACHE[key] = tree
        if len(_TREE_CACHE) > 128:
            _TREE_CACHE.popitem(last=False)
```

### Files to Edit

1. **Create shared cache class:** `tools/cq/search/_shared/bounded_cache.py`

```python
"""Bounded cache with configurable eviction policy."""
from __future__ import annotations

from collections import OrderedDict
from typing import Generic, TypeVar, Literal

K = TypeVar("K")
V = TypeVar("V")

EvictionPolicy = Literal["fifo", "lru"]

class BoundedCache(Generic[K, V]):
    """Bounded cache with FIFO or LRU eviction.

    Thread-safe for single-threaded use. For multi-threaded access,
    wrap with threading.Lock.

    Parameters
    ----------
    max_size : int
        Maximum number of entries
    policy : EvictionPolicy
        Eviction policy ("fifo" or "lru")
    """

    def __init__(self, max_size: int, policy: EvictionPolicy = "fifo") -> None:
        self._max_size = max_size
        self._policy = policy
        self._cache: OrderedDict[K, V] = OrderedDict()

    def get(self, key: K) -> V | None:
        """Get value for key, updating LRU order if applicable."""
        if key not in self._cache:
            return None

        if self._policy == "lru":
            self._cache.move_to_end(key)

        return self._cache[key]

    def get_or_set(self, key: K, factory: Callable[[], V]) -> V:
        """Get value for key, or compute and store via factory.

        Cleaner API than separate get/put for cache-aside patterns.
        """
        if key in self._cache:
            value = self._cache.pop(key)
            self._cache[key] = value  # Move to end (LRU)
            return value
        value = factory()
        self._cache[key] = value
        while len(self._cache) > self._max_size:
            self._cache.popitem(last=False)
        return value

    def put(self, key: K, value: V) -> None:
        """Put value for key, evicting oldest if at capacity."""
        if key in self._cache:
            if self._policy == "lru":
                self._cache.move_to_end(key)
            self._cache[key] = value
            return

        if len(self._cache) >= self._max_size:
            self._cache.popitem(last=False)  # Remove oldest

        self._cache[key] = value

    def clear(self) -> None:
        """Clear all entries."""
        self._cache.clear()

    def __len__(self) -> int:
        return len(self._cache)

__all__ = ["BoundedCache", "EvictionPolicy"]
```

2. **Replace manual patterns in 5+ files:**

```python
# tools/cq/search/rust/enrichment.py
from .._shared.bounded_cache import BoundedCache

_AST_CACHE = BoundedCache[str, ast.Module](max_size=64, policy="fifo")

# Replace _cache_ast logic with:
_AST_CACHE.put(file, tree)

# Replace reads with:
cached = _AST_CACHE.get(file)
```

Similar replacements in:
- `tools/cq/search/python/analysis_session.py`
- `tools/cq/search/python/extractors.py`
- `tools/cq/search/tree_sitter/rust_lane/runtime.py`
- `tools/cq/search/pipeline/classifier_runtime.py` (for `_sg_cache`)
- `tools/cq/search/tree_sitter/core/parse.py` (tree cache)

3. **Create composition layer:** `tools/cq/search/pipeline/classifier_cache.py`

```python
"""Classifier cache manager composing BoundedCache instances."""
from __future__ import annotations

from dataclasses import dataclass

from tools.cq.search._shared.bounded_cache import BoundedCache


@dataclass(slots=True)
class ClassifierCacheManager:
    """Manages all classifier-level caches as a single injectable unit."""
    sg_cache: BoundedCache[tuple[str, str], object]
    source_cache: BoundedCache[str, str]
    def_lines_cache: BoundedCache[tuple[str, str], list[tuple[int, int]]]
```

4. **Make caches injectable** (for testability):

```python
def enrich_with_ast(
    file: str,
    cache: BoundedCache[str, ast.Module] | None = None,
) -> ...:
    cache = cache or _AST_CACHE  # Use default if not provided
    # ... use cache
```

### New Files to Create

- `tools/cq/search/_shared/bounded_cache.py` (~100 LOC)
- `tools/cq/search/pipeline/classifier_cache.py` (~30 LOC)

### Legacy Decommission/Delete Scope

Delete 5+ manual eviction patterns (replace with `BoundedCache` usage).

---

## S23. Define Typed Enrichment Payload Structs

### Goal
Replace `dict[str, object]` enrichment payloads with typed msgspec structs. This is the highest-ROI change for resolving principle violations (P7, P9, P10, P14, P15, P19).

### Representative Code Snippets

**Current state:**

```python
# tools/cq/search/pipeline/smart_search.py:405
@frozen
class EnrichedMatch:
    python_enrichment: dict[str, object] | None
    rust_tree_sitter: dict[str, object] | None

# tools/cq/search/enrichment/contracts.py:28
@frozen
class PythonEnrichmentPayload:
    data: dict[str, object]

# tools/cq/search/enrichment/contracts.py:35
@frozen
class RustEnrichmentPayload:
    data: dict[str, object]

# tools/cq/core/schema.py:45
@frozen
class DetailPayload:
    data: dict[str, object]
```

**After:**

```python
# tools/cq/search/enrichment/python_facts.py
@frozen
class PythonResolutionFacts:
    """Resolved names and scopes from Python enrichment."""
    resolved_name: str | None = None
    qualified_name: str | None = None
    scope_kind: Literal["module", "class", "function", "closure"] | None = None
    import_source: str | None = None

@frozen
class PythonBehaviorFacts:
    """Behavioral analysis from Python enrichment."""
    is_async: bool = False
    is_generator: bool = False
    has_side_effects: bool = False
    decorators: list[str] = field(default_factory=list)

@frozen
class PythonStructureFacts:
    """Structural metadata from Python enrichment."""
    node_type: str | None = None
    parent_type: str | None = None
    depth: int = 0

@frozen
class PythonEnrichmentFacts:
    """Complete Python enrichment payload."""
    resolution: PythonResolutionFacts | None = None
    behavior: PythonBehaviorFacts | None = None
    structure: PythonStructureFacts | None = None

# tools/cq/search/enrichment/rust_facts.py
@frozen
class RustEnrichmentFacts:
    """Rust tree-sitter enrichment payload."""
    node_kind: str | None = None
    is_macro: bool = False
    is_public: bool = False
    trait_bounds: list[str] = field(default_factory=list)
    # ... other typed fields

# Update usage:
@frozen
class EnrichedMatch:
    python_enrichment: PythonEnrichmentFacts | None
    rust_enrichment: RustEnrichmentFacts | None

@frozen
class PythonEnrichmentPayload:
    facts: PythonEnrichmentFacts

@frozen
class RustEnrichmentPayload:
    facts: RustEnrichmentFacts
```

### Files to Edit

1. **Create facts modules:**
   - `tools/cq/search/enrichment/python_facts.py` (~120 LOC) - define 4 fact structs
   - `tools/cq/search/enrichment/rust_facts.py` (~80 LOC) - define rust fact struct

2. **Update payload definitions:**
   - `tools/cq/search/enrichment/contracts.py:28` - replace `PythonEnrichmentPayload.data: dict` with `facts: PythonEnrichmentFacts`
   - `tools/cq/search/enrichment/contracts.py:35` - replace `RustEnrichmentPayload.data: dict` with `facts: RustEnrichmentFacts`
   - `tools/cq/search/pipeline/smart_search_types.py` - update `EnrichedMatch` fields

3. **Update producers (5+ files):**
   - `tools/cq/search/python/extractors.py` - build `PythonEnrichmentFacts` instead of dict
   - `tools/cq/search/rust/enrichment.py` - build `RustEnrichmentFacts` instead of dict
   - Replace key-set constants (`_PY_RESOLUTION_KEYS`, etc.) with struct field access

4. **Update consumers (10+ files):**
   - `tools/cq/core/report.py` - replace `.get("resolved_name")` with `.resolution.resolved_name if .resolution else None`
   - `tools/cq/search/objects/render.py` - replace dict navigation with struct field access
   - `tools/cq/query/enrichment.py` - replace `isinstance` + `.get()` patterns with struct field checks

### New Files to Create

- `tools/cq/search/enrichment/python_facts.py` (~120 LOC)
- `tools/cq/search/enrichment/rust_facts.py` (~80 LOC)

### Legacy Decommission/Delete Scope

Delete key-set constants and dict-navigation helper functions across 10+ files in batch D3.

---

## S24. Introduce SymbolIndex Protocol for DefIndex

### Goal
Define `SymbolIndex` protocol to enable dependency injection and testing. `DefIndex` becomes one implementation of the protocol.

### Representative Code Snippets

**Current state:**

```python
# tools/cq/index/def_index.py:405-669
class DefIndex:
    """Global symbol index built from AST extraction."""

    def __init__(self, ...):
        # ... internal state

    @classmethod
    def build(cls, root: Path) -> DefIndex:
        # ... always constructed inline at call sites

    def lookup_definition(self, name: str) -> list[Definition]:
        # ... 11 methods, non-injectable

# Call sites always do:
index = DefIndex.build(root)
```

**After:**

```python
# tools/cq/index/protocol.py
class SymbolIndex(Protocol):
    """Protocol for symbol lookup operations."""

    def lookup_definition(self, name: str) -> list[Definition]:
        """Lookup definitions by name."""
        ...

    def lookup_by_file(self, file: Path) -> list[Definition]:
        """Lookup all definitions in file."""
        ...

    # ... protocol methods for all 11 public methods

# tools/cq/index/def_index.py
class DefIndex:
    """Default SymbolIndex implementation using AST extraction."""
    # ... existing implementation (now typed as SymbolIndex)

# Call sites can inject:
def analyze_calls(
    root: Path,
    symbol_index: SymbolIndex | None = None,
) -> ...:
    index = symbol_index or DefIndex.build(root)
    # ... use index
```

### Files to Edit

1. **Define protocol:** `tools/cq/index/protocol.py`

```python
"""Protocol for symbol index implementations."""
from __future__ import annotations

from pathlib import Path
from typing import Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from .def_index import Definition

class SymbolIndex(Protocol):
    """Protocol for symbol lookup operations."""

    def lookup_definition(self, name: str) -> list[Definition]: ...
    def lookup_by_file(self, file: Path) -> list[Definition]: ...
    def lookup_by_kind(self, kind: str) -> list[Definition]: ...
    def lookup_references(self, name: str) -> list[tuple[Path, int, int]]: ...
    # ... 7 more method signatures

__all__ = ["SymbolIndex"]
```

2. **Update `def_index.py`:**
   - No changes needed to class implementation (structural typing)
   - Add comment: `# Implements SymbolIndex protocol`

3. **Update callsites (6+ files):**
   - `tools/cq/macros/calls.py` - add optional `symbol_index: SymbolIndex | None = None` parameter
   - `tools/cq/macros/impact.py` - same injection pattern
   - Update internal calls to thread injected index through

### New Files to Create

- `tools/cq/index/protocol.py` (~60 LOC)

### Legacy Decommission/Delete Scope

None (protocol addition is additive and does not require compatibility shims).

---

## S25. Resolve Runtime Services Once Per Run

### Goal
Ensure `CqRuntimeServices` is resolved once per run (not per step). Current implementation already caches per workspace root via dict + Lock.

### Representative Code Snippets

**Current state:**

```python
# tools/cq/core/bootstrap.py:16-24
@frozen
class CqRuntimeServices:
    """Runtime services singleton per workspace."""
    workspace_root: Path
    tree_sitter_cache: TreeSitterCache | None
    # ... other services

# tools/cq/core/bootstrap.py:46-59
_SERVICES_CACHE: dict[Path, CqRuntimeServices] = {}
_SERVICES_LOCK = Lock()

def resolve_runtime_services(workspace_root: Path) -> CqRuntimeServices:
    """Resolve runtime services, caching per workspace root."""
    with _SERVICES_LOCK:
        if workspace_root not in _SERVICES_CACHE:
            _SERVICES_CACHE[workspace_root] = _build_services(workspace_root)
        return _SERVICES_CACHE[workspace_root]
```

**Assessment:** Current implementation is already correct for service caching. However, the worker scheduler needs a `set_worker_scheduler()` injection hook for testability.

### Files to Edit

1. **Update docstring:** `tools/cq/core/bootstrap.py:46-59`

```python
def resolve_runtime_services(workspace_root: Path) -> CqRuntimeServices:
    """Resolve runtime services singleton for workspace.

    Services are cached per workspace root. Multiple run steps
    within the same workspace reuse the same service instance.
    This ensures shared cache state across steps.

    Thread-safe via lock-protected cache dictionary.

    Parameters
    ----------
    workspace_root : Path
        Workspace root directory

    Returns
    -------
    CqRuntimeServices
        Cached or newly created service instance
    """
```

2. **Add worker scheduler injection hook:** `tools/cq/core/runtime/worker_scheduler.py`

```python
"""Worker scheduler with injectable abstraction for testability."""
from __future__ import annotations

from threading import Lock
from dataclasses import dataclass, field

_SCHEDULER_LOCK = Lock()


@dataclass
class _SchedulerState:
    scheduler: WorkerScheduler | None = None


_SCHEDULER_STATE = _SchedulerState()


def get_worker_scheduler(policy: ParallelismPolicy | None = None) -> WorkerScheduler:
    """Get or create the worker scheduler singleton."""
    with _SCHEDULER_LOCK:
        if _SCHEDULER_STATE.scheduler is None:
            resolved = policy or default_runtime_execution_policy().parallelism
            _SCHEDULER_STATE.scheduler = WorkerScheduler(resolved)
        return _SCHEDULER_STATE.scheduler


def set_worker_scheduler(scheduler: WorkerScheduler | None) -> None:
    """Inject a worker scheduler (for testing) or reset to None."""
    with _SCHEDULER_LOCK:
        _SCHEDULER_STATE.scheduler = scheduler
```

### New Files to Create

- `tests/unit/tools/cq/core/runtime/test_worker_scheduler.py`

### Legacy Decommission/Delete Scope

None (additive change for testability).

---

## PHASE 4: GOD MODULE DECOMPOSITIONS (Items 26-32)

---

## S26. Decompose smart_search.py

### Goal
Decompose 3,914 LOC God Module into 7 focused modules (~100-900 LOC each). This addresses 9 principle violations.

### Decomposition Plan

**Target structure:**
- `smart_search.py` (~600 LOC) - Pipeline orchestration, main APIs
- `smart_search_types.py` (~800 LOC) - 16 data type definitions [DONE in S18]
- `smart_search_telemetry.py` (~140 LOC) - Enrichment telemetry [DONE in S20]
- `smart_search_assembly.py` / `assembly.py` (~900 LOC) - Result assembly from enriched matches
- `smart_search_context.py` (~400 LOC) - Context extraction and grouping
- `python_semantic.py` (~200 LOC) - Semantic prefetch logic
- `search_object_view_store.py` (~100 LOC) - Object view registry (replaces `_SEARCH_OBJECT_VIEW_REGISTRY` global dict)

### Files to Edit

1. **Extract assembly logic:** `tools/cq/search/pipeline/assembly.py`

Move from `smart_search.py` lines ~2800-3800:
- `_assemble_findings()`
- `_group_matches_by_context()`
- `_build_finding_from_group()`
- `_extract_code_snippet()`
- `_compute_confidence_score()`
- `_merge_enrichment_facts()`
- `_build_insight_card()`
- `_build_search_summary()`
- `_attach_semantic_contract_state()`
- ~12 more assembly-related functions

Public API: `assemble_smart_search_result(...) -> CqResult`

2. **Extract context logic:** `tools/cq/search/pipeline/smart_search_context.py`

Move from `smart_search.py` lines ~1800-2200:
- `_extract_match_context()`
- `_group_by_containing_function()`
- `_classify_context_kind()`
- `_merge_overlapping_contexts()`
- ~6 more context-related functions

3. **Extract semantic prefetch:** `tools/cq/search/pipeline/python_semantic.py`

Move from `smart_search.py`:
- `_prefetch_python_semantic()` and related helper functions
- Semantic state preparation for Python enrichment

Public API: `prefetch_python_semantic(...) -> SemanticContractStateInputV1`

4. **Extract object view store:** `tools/cq/search/pipeline/search_object_view_store.py`

Replace `_SEARCH_OBJECT_VIEW_REGISTRY` global dict with injectable store:
- Object view registration and lookup
- Clean lifecycle management (no `clear_caches()` side-effect)

5. **Update additional files:**
   - `tools/cq/search/pipeline/candidate_normalizer.py` - import from new assembly module
   - `tools/cq/search/_shared/search_contracts.py` - import from new types module

6. **Update main file:** `tools/cq/search/pipeline/smart_search.py`

Keep:
- Pipeline phase functions (`_classify_phase`, `_enrich_phase`, `_assemble_phase`)
- Main entry point: `smart_search()`
- Configuration and setup
- Re-exports from extracted modules

Delete lines 1800-3800 (~2000 LOC extracted).

Add imports:
```python
from .smart_search_types import *
from .smart_search_telemetry import *
from .assembly import assemble_smart_search_result
from .smart_search_context import *
from .python_semantic import prefetch_python_semantic
from .search_object_view_store import SearchObjectViewStore
```

### New Files to Create

- `assembly.py` (~900 LOC)
- `smart_search_context.py` (~400 LOC)
- `python_semantic.py` (~200 LOC)
- `search_object_view_store.py` (~100 LOC)

### Legacy Decommission/Delete Scope

After hard-cutover extraction lands, original definitions are deleted during extraction. Additionally:
- Delete `_SEARCH_OBJECT_VIEW_REGISTRY` global dict from `smart_search.py`
- Delete `clear_caches()` side-effect from `_build_search_context` path

**Verification:** Total LOC should match: 600 + 800 + 140 + 900 + 400 + 200 + 100 = 3,140 (original 3,914 minus dead code/comments).

---

## S27. Decompose executor.py

### Goal
Decompose 3,457 LOC God Module into 9 focused modules (~100-850 LOC each). This addresses 8 principle violations.

### Decomposition Plan

**Target structure:**
- `executor.py` (~850 LOC) - State models, entity execution, plan dispatch
- `executor_ast_grep.py` (~450 LOC) - AST-grep execution [DONE in S19]
- `executor_definitions.py` (~260 LOC) - Definition/import queries
- `executor_output.py` / `finding_builders.py` (~460 LOC) - Finding construction
- `section_builders.py` (~200 LOC) - Section construction
- `executor_bytecode.py` (~380 LOC) - Bytecode query execution
- `executor_metavars.py` (~250 LOC) - Metavariable extraction
- `scan.py` (~150 LOC) - Scan context management
- `executor_cache.py` (~100 LOC) - Executor-level cache

### Files to Edit

1. **Extract definition queries:** `tools/cq/query/executor_definitions.py`

Move from `executor.py` lines 2018-2280:
- `_execute_definition_query()`
- `_execute_import_query()`
- `_collect_definitions()`
- `_filter_by_name_pattern()`
- `_build_definition_finding()`
- ~8 more definition-related functions

2. **Extract finding builders:** `tools/cq/query/finding_builders.py`

Move from `executor.py` lines 2886-3350:
- `_build_findings_from_matches()`
- `_build_finding_section()`
- `_format_match_context()`
- `_group_matches_by_file()`
- `_compute_match_priority()`
- ~10 more output-building functions

3. **Extract section builders:** `tools/cq/query/section_builders.py`

Move section construction logic:
- `_build_section_header()`
- `_build_section_footer()`
- `_render_section_summary()`
- ~5 more section functions

4. **Extract scan context management:** `tools/cq/query/scan.py`

Move from `executor.py`:
- `_build_scan_context()` (currently imported privately by `batch.py` and `batch_spans.py`)
- `_execute_rule_matches()` (currently imported privately by `batch_spans.py`)
- Related scan setup functions

This replaces private cross-module imports with public APIs.

5. **Extract executor cache:** `tools/cq/query/executor_cache.py`

Move executor-level cache management:
- Cache initialization, lookup, and eviction for query execution state

6. **Extract bytecode execution:** `tools/cq/query/executor_bytecode.py`

Move from `executor.py` lines ~1650-2030:
- `_execute_bytecode_query()`
- `_match_opcode_pattern()`
- `_extract_bytecode_context()`
- `_build_bytecode_finding()`
- ~6 more bytecode functions

7. **Extract metavar handling:** `tools/cq/query/executor_metavars.py`

Move from `executor.py` lines scattered across file:
- `_extract_metavars()`
- `_resolve_metavar_constraints()`
- `_apply_metavar_filters()`
- `_build_metavar_bindings()`
- ~5 more metavar functions

8. **Update additional files:**
   - `tools/cq/query/sg_parser.py` - import from new modules
   - `tools/cq/query/parser.py` - import from new modules

9. **Update main file:** `tools/cq/query/executor.py`

Keep:
- State models (`QueryExecutionState`, `EntityQueryState`)
- Main entry points (`execute_query`, `execute_entity_query`)
- Plan dispatch logic
- Re-exports from extracted modules

### New Files to Create

- `executor_definitions.py` (~260 LOC)
- `finding_builders.py` (~460 LOC)
- `section_builders.py` (~200 LOC)
- `scan.py` (~150 LOC)
- `executor_cache.py` (~100 LOC)
- `executor_bytecode.py` (~380 LOC)
- `executor_metavars.py` (~250 LOC)

### Legacy Decommission/Delete Scope

Original definitions deleted during extraction. Additionally:
- Delete private imports from `tools/cq/query/batch.py` and `tools/cq/query/batch_spans.py` into `_`-prefixed executor functions (replaced by `scan.py` public API)
- Deprecate non-integrated `tools/cq/query/symbol_resolver.py` flow

---

## S28. Decompose calls.py into Package

### Goal
Decompose 2,274 LOC God Module into a `tools/cq/macros/calls/` package with 7 focused modules. This addresses 5 principle violations. Package split (not flat siblings) provides cleaner namespace and curated `__all__` exports.

### Decomposition Plan

**Target structure (package split):**
```
tools/cq/macros/calls/
├── __init__.py         # Public API export surface
├── entry.py            (~350 LOC) - Command entry, call discovery orchestration
├── scanning.py         (~200 LOC) - rg-based candidate finding
├── analysis.py         (~280 LOC) - AST call analysis
├── neighborhood.py     (~200 LOC) - Neighborhood/context extraction
├── semantic.py         (~200 LOC) - Semantic enrichment for calls
├── insight.py          (~200 LOC) - Insight card + result building
└── context_snippet.py  (~200 LOC) - Code snippet extraction
```

### Files to Edit

1. **Create package init:** `tools/cq/macros/calls/__init__.py`

```python
"""Calls analysis package - decomposed from monolithic calls.py."""
from __future__ import annotations

from tools.cq.macros.calls.entry import cmd_calls, collect_call_sites
from tools.cq.macros.calls.scanning import rg_find_candidates
from tools.cq.macros.calls.analysis import group_candidates

__all__ = ["cmd_calls", "collect_call_sites", "group_candidates", "rg_find_candidates"]
```

2. **Extract entry/orchestration:** `tools/cq/macros/calls/entry.py`

Main entry: `cmd_calls(target: str, root: Path) -> CqResult`
- Call discovery orchestration
- Top-level command logic

3. **Extract scanning:** `tools/cq/macros/calls/scanning.py`

- `rg_find_candidates()` - ripgrep-based candidate discovery
- Related scan/filter functions

4. **Extract AST analysis:** `tools/cq/macros/calls/analysis.py`

Move from `calls.py` lines 80-355:
- `_analyze_call_node()`
- `_extract_call_target()`
- `_classify_call_kind()`
- `_resolve_method_receiver()`
- `group_candidates()`
- ~8 more AST analysis functions

5. **Extract neighborhood context:** `tools/cq/macros/calls/neighborhood.py`

Move neighborhood/surrounding scope extraction:
- `_extract_call_context()`
- `_extract_surrounding_scope()`
- ~5 more context functions

6. **Extract semantic enrichment:** `tools/cq/macros/calls/semantic.py`

Move semantic-related call analysis:
- Semantic state attachment
- Cross-source agreement for calls

7. **Extract insight/result building:** `tools/cq/macros/calls/insight.py`

Move from `calls.py` lines 1505-2100:
- `_build_calls_result()`
- `_build_insight_card()`
- `_compute_call_summary()`
- ~10 more result-building functions

8. **Extract code snippets:** `tools/cq/macros/calls/context_snippet.py`

Move snippet extraction:
- `_build_code_snippet()`
- `_format_call_signature()`
- Related formatting functions

9. **Update additional files:**
   - `tools/cq/macros/__init__.py` - update imports to point to new package

### New Files to Create

- `tools/cq/macros/calls/__init__.py`
- `tools/cq/macros/calls/entry.py` (~350 LOC)
- `tools/cq/macros/calls/scanning.py` (~200 LOC)
- `tools/cq/macros/calls/analysis.py` (~280 LOC)
- `tools/cq/macros/calls/neighborhood.py` (~200 LOC)
- `tools/cq/macros/calls/semantic.py` (~200 LOC)
- `tools/cq/macros/calls/insight.py` (~200 LOC)
- `tools/cq/macros/calls/context_snippet.py` (~200 LOC)

### Legacy Decommission/Delete Scope

- Delete monolithic `tools/cq/macros/calls.py` once `tools/cq/macros/calls/` package is cut over
- Delete `_SELF_CLS` duplicate constants from `tools/cq/index/def_index.py` and `tools/cq/index/call_resolver.py`
- Delete `_STDLIB_PREFIXES` from `tools/cq/macros/imports.py`

---

## S29. Decompose extractors.py

### Goal
Decompose 2,251 LOC God Module into 4 focused modules (~500 LOC each). This addresses 3 principle violations.

### Decomposition Plan

**Target structure:**
- `extractors.py` (~800 LOC) - Enrichment pipeline, public APIs, utils
- `extractors_classification.py` (~260 LOC) - Role classification
- `extractors_analysis.py` (~290 LOC) - Behavior/imports analysis
- `extractors_structure.py` (~150 LOC) - Class structure extraction

### Files to Edit

1. **Extract classification:** `tools/cq/search/python/extractors_classification.py`

Move from `extractors.py` lines 480-740:
- `_classify_definition_role()`
- `_classify_usage_role()`
- `_detect_call_patterns()`
- `_classify_import_usage()`
- ~8 more classification functions

2. **Extract analysis:** `tools/cq/search/python/extractors_analysis.py`

Move from `extractors.py` lines 1041-1330:
- `_extract_behavior_facts()`
- `_analyze_control_flow()`
- `_extract_import_details()`
- `_analyze_decorator_usage()`
- ~10 more analysis functions

3. **Extract structure:** `tools/cq/search/python/extractors_structure.py`

Move from `extractors.py` lines 780-930:
- `_extract_class_structure()`
- `_extract_inheritance_chain()`
- `_extract_method_signatures()`
- `_build_class_hierarchy()`
- ~5 more structure functions

4. **Update main file:** `tools/cq/search/python/extractors.py`

Keep:
- Public APIs: `enrich_python_ast()`, `extract_python_facts()`
- Enrichment pipeline orchestration
- Utility functions (line index, span conversion)
- Re-exports

### New Files to Create

- `extractors_classification.py` (~260 LOC)
- `extractors_analysis.py` (~290 LOC)
- `extractors_structure.py` (~150 LOC)

### Legacy Decommission/Delete Scope

Original definitions deleted during extraction.

---

## S30. Decompose rust_lane/runtime.py

### Goal
Decompose 2,138 LOC (originally 1,976, updated after review) God Module into 4 focused modules (~400-500 LOC each). This addresses 4 principle violations.

### Decomposition Plan

**Target structure:**
- `runtime.py` (~400 LOC) - Query orchestration, public APIs
- `runtime_cache.py` (~256 LOC) - Cache/parser lifecycle
- `enrichment_extractors.py` (~185 LOC) - Enrichment extraction
- `role_classification.py` (~132 LOC) - Role classification
- `fact_extraction.py` (~245 LOC) - Fact extraction from nodes

### Files to Edit

1. **Extract cache management:** `tools/cq/search/tree_sitter/rust_lane/runtime_cache.py`

Move from `runtime.py` lines 188-444:
- `_get_cached_tree()`
- `_cache_tree()`
- `_evict_oldest_tree()`
- `_make_rust_parser()`
- `_parse_rust_tree()`
- ~8 more cache functions

2. **Extract enrichment:** `tools/cq/search/tree_sitter/rust_lane/enrichment_extractors.py`

Move from `runtime.py` lines 665-850:
- `_extract_rust_enrichment()`
- `_extract_macro_info()`
- `_extract_trait_bounds()`
- `_extract_visibility()`
- ~6 more enrichment functions

3. **Extract classification:** `tools/cq/search/tree_sitter/rust_lane/role_classification.py`

Move from `runtime.py` lines 1028-1160:
- `_classify_rust_role()`
- `_classify_definition_kind()`
- `_classify_usage_kind()`
- `_detect_rust_patterns()`
- ~5 more classification functions

4. **Extract fact extraction:** `tools/cq/search/tree_sitter/rust_lane/fact_extraction.py`

Move from `runtime.py` lines 1668-1913:
- `_extract_rust_facts()`
- `_extract_type_info()`
- `_extract_lifetime_info()`
- `_extract_generic_params()`
- ~8 more fact functions

5. **Update main file:** `tools/cq/search/tree_sitter/rust_lane/runtime.py`

Keep:
- Query orchestration: `execute_rust_lane_query()`
- Public APIs
- Re-exports

### New Files to Create

- `runtime_cache.py` (~256 LOC)
- `enrichment_extractors.py` (~185 LOC)
- `role_classification.py` (~132 LOC)
- `fact_extraction.py` (~245 LOC)

### Legacy Decommission/Delete Scope

Original definitions deleted during extraction.

---

## S31. Decompose report.py

### Goal
Decompose 1,773 LOC God Module into 4 focused modules (~450 LOC each). This addresses 7 principle violations.

### Decomposition Plan

**Target structure:**
- `report.py` (~700 LOC) - Finding format, section render, public APIs
- `render_enrichment.py` (~252 LOC) - Enrichment rendering [DONE in S21]
- `render_overview.py` (~83 LOC) - Code overview section
- `render_summary.py` (~246 LOC) - Summary/insight rendering
- `render_diagnostics.py` (~180 LOC) - Diagnostics section
- `type_coercion.py` (~60 LOC) - Shared coercion utilities (absorbs S12 coercion helpers)

### Files to Edit

1. **Extract overview:** `tools/cq/core/render_overview.py`

Move from `report.py` lines 617-700:
- `_render_code_overview()`
- `_extract_overview_stats()`
- `_format_top_symbols()`
- `_format_top_files()`
- ~3 more overview functions

2. **Extract summary:** `tools/cq/core/render_summary.py`

Move from `report.py` lines 1162-1408:
- `_render_search_summary()`
- `_render_insight_card()`
- `_compute_confidence_metrics()`
- `_format_suggested_followups()`
- `_render_scope_diagnostics()`
- ~8 more summary functions

3. **Extract diagnostics:** `tools/cq/core/render_diagnostics.py`

Move diagnostics-related functions:
- `_render_diagnostics_section()`
- `_format_enrichment_telemetry()`
- `_format_cache_stats()`
- `_format_timing_breakdown()`
- ~5 more diagnostic functions

4. **Extract type coercion:** `tools/cq/core/type_coercion.py`

Consolidate all coercion utilities (absorbs S12 helpers):

```python
"""Shared type coercion utilities for CQ core."""
from __future__ import annotations


def coerce_float(value: object) -> float:
    """Coerce value to float, raising TypeError if not numeric."""
    if isinstance(value, (int, float)):
        return float(value)
    raise TypeError(f"Expected numeric value, got {type(value)}")


def coerce_str(value: object) -> str:
    """Coerce value to str, raising TypeError if not string."""
    if isinstance(value, str):
        return value
    raise TypeError(f"Expected str, got {type(value)}")


__all__ = ["coerce_float", "coerce_str"]
```

Replaces:
- `DetailPayload._coerce_float` and `_coerce_str` from `schema.py` (S12)
- Duplicated coercion helpers from `scoring.py`

5. **Update main file:** `tools/cq/core/report.py`

Keep:
- Public API: `render_result()`, `format_finding()`
- Finding format logic
- Section render orchestration
- Explicit `__all__` export surface

### New Files to Create

- `render_overview.py` (~83 LOC)
- `render_summary.py` (~246 LOC)
- `render_diagnostics.py` (~180 LOC)
- `type_coercion.py` (~60 LOC)

### Legacy Decommission/Delete Scope

Original definitions deleted during extraction. Additionally:
- Delete duplicated coercion helpers from `tools/cq/core/schema.py` and `tools/cq/core/scoring.py` (replaced by `type_coercion.py`)
- Delete `DetailPayload.__setitem__` mapping-style mutation entry points once call sites are migrated

---

## S32. Decompose runner.py

### Goal
Decompose 1,297 LOC God Module into 4 focused modules (~300 LOC each). This addresses 6 principle violations.

### Decomposition Plan

**Target structure:**
- `runner.py` (~400 LOC) - Plan orchestration
- `step_executors.py` (~293 LOC) - All step execution (Q and non-Q)
- `q_step_collapsing.py` (~180 LOC) - Q-step result collapsing
- `run_summary.py` (~208 LOC) - Run summary population and metadata

> **Note:** Module names aligned with consolidated plan: `step_executors.py` (not `q_step_executor.py`) and `run_summary.py` (not `non_q_executor.py`) for clearer naming.

### Files to Edit

1. **Extract step executors:** `tools/cq/run/step_executors.py`

Move from `runner.py` lines 341-634 and 819-1027:
- `execute_non_q_step()` - Non-Q step dispatch (calls, impact, search, neighborhood)
- `_execute_q_step()`
- `_parse_q_step_query()`
- `_build_q_step_context()`
- `_merge_q_step_scans()`
- `_execute_calls_step()`
- `_execute_impact_step()`
- `_execute_search_step()`
- `_execute_neighborhood_step()`
- `_dispatch_step_by_type()`
- ~15 more step execution functions

2. **Extract Q collapsing:** `tools/cq/run/q_step_collapsing.py`

Move from `runner.py` lines 636-816:
- `_collapse_consecutive_q_steps()`
- `_can_merge_q_steps()`
- `_merge_q_step_results()`
- `_build_merged_finding()`
- ~6 more collapsing functions

3. **Extract run summary:** `tools/cq/run/run_summary.py`

Move run summary/metadata population:
- `populate_run_summary_metadata()`
- Related summary helper functions

4. **Update additional files:**
   - `tools/cq/cli_app/result.py` - import from new modules

5. **Update main file:** `tools/cq/run/runner.py`

```python
from tools.cq.run.run_summary import populate_run_summary_metadata
from tools.cq.run.step_executors import execute_non_q_step
```

Keep:
- Public API: `execute_run()`, `execute_steps()`
- Plan orchestration
- Explicit `__all__` export surface

### New Files to Create

- `step_executors.py` (~293 LOC)
- `q_step_collapsing.py` (~180 LOC)
- `run_summary.py` (~208 LOC)

### Legacy Decommission/Delete Scope

Original definitions deleted during extraction.

---

## S33. Define Typed Summary Models (RunSummaryV1)

### Goal
Replace `CqResult.summary: dict[str, object]` with typed `RunSummaryV1` struct so downstream logic stops re-validating ad hoc dicts. Reuse existing `tools/cq/core/summary_contracts.py` rather than creating a parallel contract module. Addresses P9 (Parse don't validate) and P10 (Make illegal states unrepresentable).

### Representative Code Snippets

**Current state:**

```python
# tools/cq/core/schema.py
@frozen
class CqResult:
    summary: dict[str, object] | None = None  # UNTYPED

# tools/cq/core/cache/contracts.py
@frozen
class CachedResult:
    summary: dict[str, object] | None = None  # UNTYPED
```

**After:**

```python
# tools/cq/core/summary_contracts.py
from __future__ import annotations

import msgspec

from tools.cq.core.structs import CqStrictOutputStruct


class RunSummaryV1(CqStrictOutputStruct, frozen=True):
    """Typed run summary replacing dict[str, object]."""
    query: str | None = None
    mode: str | None = None
    lang_scope: str | None = None
    total_matches: int = 0
    matched_files: int = 0
    scanned_files: int = 0
    step_summaries: dict[str, dict[str, object]] = msgspec.field(default_factory=dict)

# tools/cq/core/schema.py
@frozen
class CqResult:
    summary: RunSummaryV1 | None = None  # TYPED
```

### Files to Edit

1. `tools/cq/core/summary_contracts.py` - Add `RunSummaryV1` and conversion helpers
2. `tools/cq/core/schema.py` - Update `CqResult.summary` field type
3. `tools/cq/core/cache/contracts.py` - Update `CachedResult.summary` field type
4. `tools/cq/cli_app/context.py` - Update summary construction sites
5. `tools/cq/cli_app/result.py` - Update summary consumption sites
6. `tools/cq/run/runner.py` - Update run summary population

### New Files to Create

None required if existing CQ integration coverage already exercises summary production/consumption paths. If gaps are identified, add `tests/unit/tools/cq/core/test_summary_contracts.py`.

### Legacy Decommission/Delete Scope

Delete `summary: dict[str, object]` field type in `CqResult` and `CachedResult` (replaced by `RunSummaryV1`).

---

## S34. Define Macro Scoring Contracts (ScoringDetailsV1)

### Goal
Replace dict-return from `macro_scoring_details()` in `tools/cq/macros/shared.py` with typed `ScoringDetailsV1` struct. Reuse existing `tools/cq/macros/contracts.py` rather than creating a parallel contract module. Addresses P8 (Design by contract) and P10 (Make illegal states unrepresentable).

### Representative Code Snippets

**Current state:**

```python
# tools/cq/macros/shared.py
def macro_scoring_details(...) -> dict[str, object]:
    return {
        "impact_score": impact,
        "impact_bucket": bucket(impact),
        "confidence_score": confidence,
        "confidence_bucket": bucket(confidence),
        "evidence_kind": evidence_kind,
    }
```

**After:**

```python
# tools/cq/macros/contracts.py
from __future__ import annotations

from tools.cq.core.structs import CqStruct


class ScoringDetailsV1(CqStruct, frozen=True):
    """Typed scoring details for macro analysis."""
    impact_score: float
    impact_bucket: str
    confidence_score: float
    confidence_bucket: str
    evidence_kind: str


# tools/cq/macros/shared.py
from tools.cq.macros.contracts import ScoringDetailsV1


def macro_scoring_details(...) -> ScoringDetailsV1:
    return ScoringDetailsV1(
        impact_score=impact,
        impact_bucket=bucket(impact),
        confidence_score=confidence,
        confidence_bucket=bucket(confidence),
        evidence_kind=evidence_kind,
    )
```

### Files to Edit

1. `tools/cq/macros/contracts.py` - Add `ScoringDetailsV1`
2. `tools/cq/macros/shared.py` - Update return type and construction
3. `tools/cq/macros/calls.py` - Update scoring detail consumption
4. `tools/cq/macros/impact.py` - Update scoring detail consumption

### New Files to Create

None required if existing macro integration/unit tests already cover scoring payload shape. If gaps are identified, add `tests/unit/tools/cq/macros/test_scoring_contracts.py`.

### Legacy Decommission/Delete Scope

Delete dict-return shape for `macro_scoring_details` in `tools/cq/macros/shared.py` (replaced by `ScoringDetailsV1`).

---

## PHASE 3 EXTENSIONS: ARCHITECTURE AND RELIABILITY GUARDRAILS (Items 35-37)

---

## S35. Enforce Dependency Direction and Private Import Boundaries

### Goal
Make dependency direction and information-hiding rules enforceable in CI so architectural regressions fail fast. This scope turns Principle 5/8 constraints into executable checks rather than review-time convention.

### Representative Code Snippets

**Current state:**

```python
# tools/cq/search/pipeline/orchestration.py:26
from tools.cq.search.pipeline.smart_search import _assemble_smart_search_result
```

**After:**

```python
# tests/unit/cq/architecture/test_import_boundaries.py
import subprocess


def test_no_private_cross_module_imports() -> None:
    """Disallow 'from x import _private' across module boundaries."""
    result = subprocess.run(
        ["rg", "-n", r"from\\s+tools\\.cq\\..+\\s+import\\s+_[A-Za-z0-9_]+", "tools/cq"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode in (0, 1)
    assert result.stdout.strip() == ""
```

### Files to Edit

1. `tools/cq/search/pipeline/orchestration.py` - Replace private import with public assembly API
2. `tools/cq/search/pipeline/smart_search.py` - Expose required public assembly entrypoint
3. `tests/unit/cq` boundary test modules - Add/extend import-boundary checks for upward/private imports

### New Files to Create

- `tests/unit/cq/architecture/test_import_boundaries.py`

### Legacy Decommission/Delete Scope

- Delete private cross-module import usage patterns where `from ... import _private_symbol` is used across files.
- Delete temporary allowlists for dependency-direction exceptions once violations are fixed.

---

## S36. Consolidate Residual Mutable Process-Global State

### Goal
Move remaining process-global mutable registries to explicit injectable runtime state surfaces so behavior is deterministic, resettable, and testable.

### Representative Code Snippets

**Current state:**

```python
# tools/cq/search/tree_sitter/query/drift.py:75
_LAST_CONTRACT_SNAPSHOTS: dict[str, QueryContractSnapshotV1] = {}

# tools/cq/search/tree_sitter/query/registry.py:30
_LAST_DRIFT_REPORTS: dict[str, GrammarDriftReportV1] = {}
```

**After:**

```python
# tools/cq/search/tree_sitter/query/runtime_state.py
class QueryRuntimeStateV1(CqStruct, frozen=True):
    last_contract_snapshots: dict[str, QueryContractSnapshotV1]
    last_drift_reports: dict[str, GrammarDriftReportV1]
```

### Files to Edit

1. `tools/cq/search/tree_sitter/query/drift.py` - Read/write snapshot state via injected runtime state
2. `tools/cq/search/tree_sitter/query/registry.py` - Read/write drift reports via injected runtime state
3. `tools/cq/core/bootstrap.py` - Wire runtime-state provider through CQ runtime services

### New Files to Create

- `tools/cq/search/tree_sitter/query/runtime_state.py`
- `tests/unit/cq/search/test_query_runtime_state.py`

### Legacy Decommission/Delete Scope

- Delete `_LAST_CONTRACT_SNAPSHOTS` and `_LAST_DRIFT_REPORTS` module-level mutable globals.
- Delete direct module-global mutation helpers superseded by runtime-state APIs.

---

## S37. Add Determinism and Performance Regression Harness for CQ

### Goal
Codify deterministic-output and performance guardrails as CI-tested artifacts so hard-cutover refactors stay within documented behavior/performance budgets.

### Representative Code Snippets

**Current state:**

```python
# tests/unit/cq/search/test_performance_smoke.py
# benchmark checks are opt-in via environment flags
```

**After:**

```python
# tests/integration/cq/test_determinism_regression.py
from tools.cq.core.toolchain import Toolchain
from tools.cq.query.executor import ExecutePlanRequestV1, execute_plan
from tools.cq.query.parser import parse_query
from tools.cq.query.planner import compile_query


def test_repeated_runs_produce_identical_summary_payload() -> None:
    tc = Toolchain.detect()
    query_text = "entity=function name=execute_plan"
    query = parse_query(query_text)
    plan = compile_query(query)
    first = execute_plan(
        ExecutePlanRequestV1(plan=plan, query=query, root=".", argv=(), query_text=query_text),
        tc=tc,
    )
    second = execute_plan(
        ExecutePlanRequestV1(plan=plan, query=query, root=".", argv=(), query_text=query_text),
        tc=tc,
    )
    assert first.summary == second.summary
```

### Files to Edit

1. `tests/e2e/cq/test_query_performance.py` - Add explicit baseline assertions and report format
2. `tests/e2e/cq/test_query_regression.py` - Add deterministic summary/order assertions for representative commands
3. `docs/plans/cq_design_improvements_implementation_plan_v1_2026-02-15.md` quality-gate section - Tie phase gates to determinism/perf harness results

### New Files to Create

- `tests/integration/cq/test_determinism_regression.py`

### Legacy Decommission/Delete Scope

- Delete ad hoc/manual determinism verification checklist items superseded by automated deterministic regression tests.

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1: After Phase 1 Quick Wins (S1-S15, S33, S34)

**Timing:** After all Phase 1 PRs merged and stable (end of week 2).

**Files to clean:**

1. Delete duplicate `NodeLike` definitions (5 locations):
   - `tools/cq/search/tree_sitter/core/node_utils.py:13-39`
   - `tools/cq/search/tree_sitter/structural/exports.py:131-141`
   - `tools/cq/search/tree_sitter/rust_lane/injections.py:18-31`
   - `tools/cq/search/tree_sitter/tags.py:30-41`
   - `tools/cq/search/tree_sitter/python_lane/locals_index.py:14-26`

2. Delete duplicate `_normalize_semantic_version` (2 locations):
   - `tools/cq/search/tree_sitter/query/planner.py:21-26`
   - `tools/cq/search/tree_sitter/schema/node_schema.py:93-98`

3. Delete duplicate `_python_field_ids` (3 locations):
   - `tools/cq/search/tree_sitter/python_lane/locals_index.py:38-40`
   - `tools/cq/search/tree_sitter/python_lane/facts.py:113-115`
   - `tools/cq/search/tree_sitter/python_lane/fallback_support.py:18-20`

4. Delete `_truncation_tracker` module-level list:
   - `tools/cq/search/python/extractors.py:187`

5. Delete duplicate query utilities (7 functions):
   - `tools/cq/query/executor.py:966` (`_count_result_matches`)
   - `tools/cq/query/merge.py:29` (`_count_result_matches`)
   - `tools/cq/query/entity_front_door.py:446` (`_missing_languages_from_summary`)
   - `tools/cq/query/merge.py:41` (`_missing_languages_from_summary`)
   - `tools/cq/query/executor.py:2744` + `symbol_resolver.py:204` + `enrichment.py:434` (`_extract_def_name` variants)

6. Delete `@require_ctx` decorator:
   - `tools/cq/cli_app/infrastructure.py:81-96`

7. Delete legacy semantic contract definitions from search layer:
   - `tools/cq/search/semantic/models.py` (delete `SemanticProvider`, `SemanticStatus`, `SemanticContractStateInputV1`, `derive_semantic_contract_state`)

8. Delete duplicate AST helpers (4 functions):
   - `tools/cq/search/python/analysis_session.py:36-53` (`_node_byte_span`)
   - `tools/cq/search/python/analysis_session.py:67-78` (`_ast_node_priority`)
   - `tools/cq/search/python/resolution_support.py:62-87` (`_node_byte_span`)
   - `tools/cq/search/python/resolution_support.py:90-101` (`_ast_node_priority`)

9. Delete `DefIndex.load_or_build`:
   - `tools/cq/index/def_index.py:478-513`

10. Delete coercion static methods:
    - `tools/cq/core/schema.py:48-61` (`DetailPayload._coerce_float`, `_coerce_str`)

11. Delete `_SELF_CLS` duplicate constants:
    - `tools/cq/index/def_index.py` (`_SELF_CLS`)
    - `tools/cq/index/call_resolver.py` (`_SELF_CLS`)

12. Delete deprecated alias names from `tools/cq/search/semantic/models.py`:
    - `fail_open` alias
    - `enrich_semantics` alias

**Verification step:** Run full test suite + pyrefly check before and after batch deletion. Ensure zero behavioral change.

### Batch D2: After Phase 2 Structural Prep (S16-S18)

**Timing:** After vocabulary types moved and type extractions stable (end of week 3).

**Files to clean:**

1. Delete original `QueryMode`/`SearchLimits` definitions from pipeline (hard cutover):
   - `tools/cq/search/pipeline/classifier.py:36` (delete enum definition)
   - `tools/cq/search/pipeline/profiles.py:12` (delete struct definition)

2. Delete duplicate lane utilities (10+ functions across python_lane and rust_lane):
   - All functions listed in S17 from python_lane/runtime.py, python_lane/facts.py, rust_lane/runtime.py

3. Delete type definitions from `smart_search.py` (already moved to `smart_search_types.py` in S18)

4. Delete `_smart_search_module()` hack:
   - `tools/cq/search/pipeline/partition_pipeline.py:129-132`

### Batch D3: After Phase 3 Targeted Extractions + Guardrails (S19-S25, S35-S37)

**Timing:** After enrichment payloads/guardrails are migrated and enforced (end of week 6).

**Files to clean:**

1. Delete key-set constants (replaced by struct field access):
   - `tools/cq/search/enrichment/core.py` (`_PY_RESOLUTION_KEYS`)
   - `tools/cq/search/enrichment/core.py` (`_PY_BEHAVIOR_KEYS`)

2. Delete manual cache eviction patterns (replaced by `BoundedCache`):
   - `tools/cq/search/rust/enrichment.py` (`_AST_CACHE` FIFO eviction branch)
   - `tools/cq/search/python/extractors.py` (`_AST_CACHE` FIFO eviction branch)
   - `tools/cq/search/python/analysis_session.py` (`_SESSION_CACHE` FIFO eviction branch)
   - `tools/cq/search/pipeline/classifier_runtime.py` (manual `clear()` fan-out in classifier cache lifecycle)

3. Delete mutable snapshot/report globals superseded by runtime-state injection:
   - `tools/cq/search/tree_sitter/query/drift.py` (`_LAST_CONTRACT_SNAPSHOTS`)
   - `tools/cq/search/tree_sitter/query/registry.py` (`_LAST_DRIFT_REPORTS`)

4. Delete `_SEARCH_OBJECT_VIEW_REGISTRY` global dict from `tools/cq/search/pipeline/smart_search.py`

5. Delete `clear_caches()` side-effect from `_build_search_context` path in `tools/cq/search/pipeline/smart_search.py`

6. Delete `_STDLIB_PREFIXES` from `tools/cq/macros/imports.py`

### Batch D4: After Phase 4 God Module Decompositions (S26-S32)

**Timing:** After all 7 God Modules decomposed and hard-cutover imports stabilized (end of week 9).

**No deletions needed** - original definitions already deleted during extraction. Only verification:

1. Verify public API exports (`__all__` and package surfaces) are complete and stable
2. Verify test coverage maintained or improved
3. Verify no behavioral regressions
4. Update import paths in documentation/examples

### Batch D5: Final Typed-Summary and DetailPayload Mutation Deletions (after S33, S8, S9)

**Timing:** After typed summary models and expanded core contracts are stable.

**Files to clean:**

1. Delete legacy untyped summary-path code:
   - `tools/cq/core/schema.py` - remove `dict[str, object]` summary field remnants
   - `tools/cq/core/cache/contracts.py` - remove `dict[str, object]` summary field remnants
   - `tools/cq/cli_app/context.py` - remove untyped summary construction paths
   - `tools/cq/run/runner.py` - remove untyped summary population paths

2. Delete `DetailPayload.__setitem__` mutation API from `tools/cq/core/schema.py` (replaced by immutable construction-time payloads)

3. Delete or reclassify `plan_feasible_slices` from public CLI surface

---

## Implementation Sequence

Execute in this order to minimize risk and maximize incremental value:

### Week 1-2: Quick Wins Foundation
1. **S1** - Consolidate NodeLike protocols (tree-sitter foundation)
2. **S2** - Extract `_normalize_semantic_version` (tree-sitter DRY)
3. **S3** - Extract `_python_field_ids` (python_lane DRY)
4. **S4** - Define Literal types (type safety across 6 modules)
5. **S34** - Define `ScoringDetailsV1` (typed macro scoring, alongside S4 Literal refinements)
6. **S5** - Move `_truncation_tracker` to per-call state (determinism fix)
7. **S6** - Deduplicate query utilities (query subsystem DRY)
8. **S7** - Add `__all__` exports (query public API clarity)
9. **S8** - Consolidate dual context injection (CLI simplification)
10. **S9** - Move semantic contracts to core (fix dependency inversion, expanded scope)
11. **S33** - Define `RunSummaryV1` typed summary models (after S9, both are typed contract additions to core)
12. **S10** - Extract shared AST helpers (python search DRY)
13. **S11** - Remove `DefIndex.load_or_build` dead code (cleanup)
14. **S12** - Extract shared coercion helpers (core utilities, later absorbed into S31 `type_coercion.py`)
15. **S13** - Fix canonicalize mutations (copy-first pure function fix)
16. **S14** - Rename `no_semantic_enrichment` (positive boolean)
17. **S15** - Export `is_section_collapsed` (neighborhood API)
18. **D1** - Batch delete Phase 1 legacy code (12 deletion targets)

**Rationale:** Quick wins are independent, low-risk, and establish patterns for later work. S33 and S34 are quick typed contract additions that naturally fit alongside S4 and S9. Each can be a separate PR. D1 cleanup validates migration success before Phase 2.

### Week 2-3: Structural Preparation
19. **S16** - Move `QueryMode`/`SearchLimits` to `_shared/types.py` (fix upward imports)
20. **S17** - Create `core/lane_support.py` (shared lane utilities)
21. **S18** - Extract types from `smart_search.py` (enables decomposition)
22. **D2** - Batch delete Phase 2 legacy code

**Rationale:** These 3 changes create the foundation for safe God Module decomposition. S16 fixes dependency inversions. S17 consolidates lane duplication. S18 extracts types from the largest God Module. D2 validates structural changes.

### Week 3-6: Targeted Extractions + Guardrails
23. **S19** - Extract ast-grep from `executor.py` (targeted decomposition)
24. **S20** - Extract enrichment telemetry from `smart_search.py` (targeted decomposition)
25. **S21** - Extract enrichment rendering from `report.py` (callback injection, targeted decomposition)
26. **S22** - Extract `BoundedCache[K, V]` + `ClassifierCacheManager` (cross-cutting pattern)
27. **S23** - Define typed enrichment payloads (highest-ROI change)
28. **S24** - Introduce `SymbolIndex` protocol (testability improvement)
29. **S25** - Worker scheduler injection hook + runtime services verification
30. **S35** - Enforce dependency direction + private-import boundaries in CI
31. **S36** - Consolidate residual process-global mutable state
32. **S37** - Add determinism/performance regression harness for CQ
33. **D3** - Batch delete Phase 3 legacy code (key-sets, manual caches, view registry, mutable globals)

**Rationale:** S19-S21 are rehearsals for full God Module decomposition. S22-S23 address systemic patterns (caching, untyped dicts). S35-S37 convert architectural/testability principles into executable enforcement. D3 removes significant dead code.

### Week 6-9: God Module Decompositions
34. **S27** - Decompose `executor.py` into 7 modules (benefits most from S19)
35. **S26** - Decompose `smart_search.py` into 7 modules (benefits from S18, S20)
36. **S28** - Decompose `calls.py` into `calls/` package (benefits from S24)
37. **S30** - Decompose `rust_lane/runtime.py` (benefits from S17)
38. **S31** - Decompose `report.py` + `type_coercion.py` (benefits from S21)
39. **S32** - Decompose `runner.py` → `step_executors.py` + `run_summary.py` (smallest, can parallelize)
40. **S29** - Decompose `extractors.py` (last, benefits from all patterns)
41. **D4** - Verify API export surfaces and coverage
42. **D5** - Final typed-summary and DetailPayload mutation deletions

**Rationale:** Execute in order of dependency and impact. S27/S26 are the largest and benefit most from prior work. S28-S32 can partially overlap if separate engineers work on them. S29 runs last to apply established extraction patterns. D4 is verification-only (no deletions). D5 removes final legacy untyped paths after typed contracts stabilize.

---

## Implementation Checklist

### Phase 1: Quick Wins (Week 1-2)
- [ ] S1: Consolidate NodeLike protocols → `contracts/core_models.py`
- [ ] S2: Extract `_normalize_semantic_version` → `core/language_registry.py`
- [ ] S3: Extract `_python_field_ids` → `python_lane/runtime.py`
- [ ] S4: Define Literal types (6 categorical fields)
- [ ] S34: Define `ScoringDetailsV1` in `macros/contracts.py`
- [ ] S5: Move `_truncation_tracker` to per-call context
- [ ] S6: Deduplicate query utilities → `query/shared_utils.py`
- [ ] S7: Add `__all__` to `enrichment.py`, `planner.py`
- [ ] S8: Consolidate dual context injection
- [ ] S9: Move semantic contracts → `core/semantic_contracts.py` (expanded: `SemanticProvider`, `derive_semantic_contract_state`)
- [ ] S33: Define `RunSummaryV1` in `core/summary_contracts.py`
- [ ] S10: Extract AST helpers → `python/ast_utils.py`
- [ ] S11: Remove `DefIndex.load_or_build` dead code
- [ ] S12: Extract coercion helpers to module level (later absorbed into S31 `type_coercion.py`)
- [ ] S13: Fix `canonicalize_*_lane_payload` mutations (copy-first approach)
- [ ] S14: Rename `no_semantic_enrichment` to positive form
- [ ] S15: Export `is_section_collapsed` from neighborhood
- [ ] D1: Batch delete Phase 1 legacy code (including `_SELF_CLS`, deprecated aliases)

### Phase 2: Structural Preparation (Week 2-3)
- [ ] S16: Move `QueryMode`/`SearchLimits` → `_shared/types.py`
- [ ] S17: Create `core/lane_support.py` with shared utilities
- [ ] S18: Extract types → `smart_search_types.py` (~800 LOC)
- [ ] D2: Batch delete Phase 2 legacy code (4 deletion targets)

### Phase 3: Targeted Extractions + Guardrails (Week 3-6)
- [ ] S19: Extract ast-grep → `executor_ast_grep.py` (~450 LOC)
- [ ] S20: Extract telemetry → `smart_search_telemetry.py` (~140 LOC)
- [ ] S21: Extract rendering → `render_enrichment.py` (~252 LOC, with `EnrichmentCallback` injection)
- [ ] S22: Extract `BoundedCache[K, V]` + `ClassifierCacheManager` → `_shared/bounded_cache.py` + `classifier_cache.py`
- [ ] S23: Define typed enrichment structs → `enrichment/{python,rust}_facts.py`
- [ ] S24: Introduce `SymbolIndex` protocol → `index/protocol.py`
- [ ] S25: Worker scheduler injection hook → `core/runtime/worker_scheduler.py`
- [ ] S35: Enforce private-import and upward-import boundaries in CI
- [ ] S36: Consolidate mutable process-global runtime state
- [ ] S37: Add deterministic-output and performance regression harness
- [ ] D3: Batch delete Phase 3 legacy code (key-sets, manual caches, view registry, mutable globals)

### Phase 4: God Module Decompositions (Week 6-9)
- [ ] S27: Decompose `executor.py` → 7 modules (3457 LOC, includes `scan.py`, `finding_builders.py`, `section_builders.py`, `executor_cache.py`)
- [ ] S26: Decompose `smart_search.py` → 7 modules (3914 LOC, includes `assembly.py`, `python_semantic.py`, `search_object_view_store.py`)
- [ ] S28: Decompose `calls.py` → `calls/` package (2274 LOC, 7 modules: entry, scanning, analysis, neighborhood, semantic, insight, context_snippet)
- [ ] S30: Decompose `rust_lane/runtime.py` → 4 modules (2138 → 400 + 3x~500 LOC)
- [ ] S31: Decompose `report.py` → 5 modules (1773 LOC, includes `render_summary.py` + `type_coercion.py`)
- [ ] S32: Decompose `runner.py` → 3 modules (`step_executors.py` + `q_step_collapsing.py` + `run_summary.py`)
- [ ] S29: Decompose `extractors.py` → 4 modules (2251 → 800 + 3x~450 LOC)
- [ ] D4: Verify API export surfaces and test coverage (no deletions)
- [ ] D5: Final typed-summary and DetailPayload mutation deletions

### Quality Gates (After Each Phase)
- [ ] Phase 1: Run `uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright && uv run pytest -q`
- [ ] Phase 2: Run full gate + verify no import errors
- [ ] Phase 3: Run full gate + verify enrichment pipeline unchanged + determinism/perf harness pass
- [ ] Phase 4: Run full gate + verify God Module LOC targets met

---

## Success Criteria

### Quantitative Metrics

| Metric | Before | After | Target |
|--------|--------|-------|--------|
| Files > 800 LOC | 7 | 0 | 0 |
| Avg file LOC (God Modules) | 2,420 | ~500 | <800 |
| Duplicate code instances | 30+ | 0 | <5 |
| Untyped dict payloads | 8 | 0 | 0 |
| Module-level mutable state | 12+ | 0 | <3 |
| Upward imports (lanes → pipeline) | 8 | 0 | 0 |
| Categorical str fields | 6 | 0 | 0 |

### Principle Score Improvements (Target)

| Principle | Current | Target |
|-----------|---------|--------|
| P3: SRP | 0.9 | 2.5 |
| P2: Separation of Concerns | 1.1 | 2.5 |
| P7: DRY | 1.3 | 2.8 |
| P23: Testability | 1.6 | 2.5 |
| **Overall Average** | **1.9** | **2.5+** |

### Behavioral Invariants (Must Preserve)

1. All existing tests pass without modification
2. CLI command surface and semantics remain stable where possible; typed payload contracts transition via documented hard cutover (intentional breaking change)
3. Enrichment pipeline deterministic (same cache behavior)
4. Performance within 5% of baseline (no regression from decomposition)

---

## Risk Mitigation

### High-Risk Changes

| Change | Risk | Mitigation |
|--------|------|------------|
| S23 (Typed enrichment payloads) | Breaking change to 10+ modules | Implement as atomic hard-cutover PR, exhaustive integration + regression tests, immediate rollback via revert if needed |
| S26 (smart_search decomposition) | 3914 LOC, complex interdependencies | Extract types/telemetry first (S18, S20), validate with existing tests, add integration smoke tests |
| S27 (executor decomposition) | 3457 LOC, central query orchestrator | Extract ast-grep first (S19), preserve expected command semantics, comprehensive regression suite |

### Rollback Strategy

Each scope item (S1-S37) is a separate PR with:
- Atomic cutover (all call sites updated in the same PR)
- No compatibility shims (no transitional re-exports/feature-flag compatibility paths)
- Isolated test coverage or explicitly mapped integration/e2e coverage
- Git revert path if integration fails

For Phase 4 decompositions:
- No compatibility re-export grace period
- Monitor production metrics for performance regressions
- Complete import migration in the same PR that lands decomposition

---

**Document Status:** Updated with consolidated plan integration (2026-02-16)
**Next Action:** Begin Phase 1 Quick Wins (S1-S15, S33, S34)
**Owner:** CQ Subsystem Maintainers
**Review Cadence:** After each phase completion
**Source Plans:** Design review synthesis (2026-02-15) + Consolidated implementation plan (2026-02-16)
