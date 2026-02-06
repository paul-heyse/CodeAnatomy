# CQ Python Enrichment: Best-in-Class Recommendations

**Date**: 2026-02-06
**Revision**: Updated incorporating review feedback from
`docs/plans/cq_python_enrichment_proposal_review_2026-02-06.md`.

## Implementation Status Audit (2026-02-06, post-v2 codebase)

This section reflects the current implementation state in `tools/cq` and CQ tests.

### Snapshot

| Area | Status | Evidence |
|------|--------|----------|
| Core Python enrichment module and integration | Complete | `tools/cq/search/python_enrichment.py` + integration in `tools/cq/search/smart_search.py` (`_maybe_python_enrichment`) |
| Recommendation set (Recs 1-9) | Mostly complete | Most extractors and tests exist; specific field/contract gaps remain (see matrix) |
| Prerequisites P3-P4 (bounds + fail-open) | Complete | `_truncate`, constants, `_try_extract`, `enrichment_status/degrade_reason` paths are implemented |
| Prerequisites P1-P2 (byte-range correctness, node passthrough) | Partial/Pending | Byte-range entrypoint exists but is a stub; classifier does not pass resolved node through |
| Output trimming and rendering updates (Rec 10) | Mostly complete | `line_text` suppression and language-aware code fences are implemented; strict byte-range correctness still pending |

Focused verification: `uv run pytest -q tests/unit/cq/search/test_python_enrichment.py tests/unit/cq/search/test_tree_sitter_rust.py tests/unit/cq/search/test_smart_search.py` passed (`113 passed`).

### Detailed Status Matrix

| Scope Item | Status | Notes |
|------------|--------|-------|
| P1: Byte-range coordinate correctness | Partial | `enrich_python_context_by_byte_range()` is present but currently returns `None`; `smart_search` still uses line/col lookup and does not consume byte-range enrichment. |
| P2: Classifier node passthrough (A4) | Pending | `classify_from_node()` still returns only `NodeClassification`; enrichment does a second lookup via `get_node_index(...).find_containing(...)`. |
| P3: Payload bounds framework | Complete | Bounds constants and `_truncate()` framework are implemented in `tools/cq/search/python_enrichment.py`. |
| P4: Graceful degradation | Complete | Per-extractor `_try_extract` and degraded-status metadata are implemented. |
| Recommendation 1: Function signature extraction | Complete | `params`, `return_type`, `signature`, `is_async`, and generator signal support are implemented (ast-grep + Python `ast` tiers). |
| Recommendation 2: Decorators + item role | Complete | Decorator extraction and semantic `item_role` mapping are implemented and tested. |
| Recommendation 3: Class context extraction | Partial | `class_name`, `base_classes`, and `class_kind` are implemented; explicit `is_dataclass` boolean is not emitted as a dedicated field. |
| Recommendation 4: Call target enrichment | Complete | `call_target`, `call_receiver`, `call_method`, `call_args_count` are implemented. |
| Recommendation 5: Containing structure context | Complete | `scope_chain` and `structural_context` extraction are implemented. |
| Recommendation 6: Import detail extraction | Complete | Python `ast`-normalized import extraction is implemented, including `import_level` and `is_type_import`. |
| Recommendation 7: Function behavior summary | Complete | `returns_value`, `raises_exception`, `yields`, `awaits`, `has_context_manager` are implemented using scope-safe AST walking. |
| Recommendation 8: Expanded binding signals | Complete | `referenced`, `local`, and `nonlocal` flags are now surfaced in `_symtable_flags()`. |
| Recommendation 9: Class API shape summary | Complete | `method_count`, `property_names`, `abstract_member_count`, `class_markers` are implemented. |
| Recommendation 10: Output trimming (staged) | Mostly complete | `line_text` is suppressed when `context_snippet` exists, `node_kind` is retained, numeric confidence/evidence are retained, and `report.py` uses language-aware code fences. |
| O6: Payload budget metadata | Partial | `truncated_fields` is implemented; `payload_size_hint` and `dropped_fields` are not implemented. |
| Future tree-sitter Python augmentation | Deferred/Pending | `tools/cq/search/tree_sitter_python.py` is not present (consistent with deferred phase). |

### Remaining Work (Priority Order)

1. Implement `enrich_python_context_by_byte_range()` and route smart-search enrichment through byte offsets from ripgrep submatches.
2. Collapse duplicate node resolution by passing classifier-resolved `SgNode` directly into enrichment.
3. Finish payload budget metadata (`payload_size_hint`, `dropped_fields`) if output budget controls remain a target.
4. Decide whether to emit explicit `is_dataclass` (in addition to `class_kind`) to match contract wording.
5. Add Unicode-focused end-to-end tests proving byte-offset correctness from collector -> classifier -> enrichment.

## Problem Statement

The current Python query output from `/cq search` provides classification (definition, callsite, import, etc.), containing scope, and symtable binding flags. While this is more than many tools offer, an LLM agent reviewing Python code must still issue 2-5 follow-up queries for routine questions that a richer single-pass enrichment could answer immediately:

- "What are the function's parameters and return type annotation?"
- "Is this a method, classmethod, staticmethod, or free function?"
- "What decorators does this function/class have?"
- "Is this function async? Does it yield? Does it await?"
- "What class does this method belong to, and what does it inherit from?"
- "Is this inside a try/except, context manager, or comprehension?"
- "What does this function actually return (yield, raise, return)?"

Each of these questions costs a separate `/cq q` or file read. The recommendations below eliminate 2-4 follow-up queries per Python match.

## Current State Assessment

### What We Currently Emit Per Match (Python)

| Field | Source | Value for LLM Agents |
|-------|--------|---------------------|
| `category` | 3-stage classifier | **High** - defines/callsite/import/reference |
| `confidence` | Classifier | **Medium** - useful for triage |
| `evidence_kind` | Classifier | **Medium** - drives score bucketing; useful for classifier diagnostics |
| `node_kind` | ast-grep | **Medium** - raw CST kind (e.g., "function_definition"); useful for debugging/rules |
| `containing_scope` | ast-grep parent walk | **High** - enables grouping by function |
| `context_window` | Indent analysis | **High** - line range anchors the context_snippet, enables "Context (lines X-Y)" headers; avoids file reads |
| `context_snippet` | Source lines | **High** - actual code context |
| `binding_flags` | symtable | **Medium** - imported/assigned/parameter/closure/global (partial subset) |
| `match_text` | ripgrep | **High** - exact matched text |
| `line_text` | ripgrep | **Medium** - full line (often redundant with snippet) |

### Current Strengths

1. **Classification is solid** - The 3-stage pipeline (heuristic -> ast-grep node -> record-based) provides reliable category assignment. Confidence scoring is well-calibrated.

2. **Containing scope** - Grouping by containing function/class is the single most impactful enrichment for agent planning. This works well.

3. **Symtable binding flags** - Distinguishing imported/parameter/closure/global is uniquely valuable for understanding how a name arrived in scope. No other tool provides this.

4. **ast-grep integration** - Already parsed per-file. Zero incremental parse cost for new extractors that use `SgNode`.

### Current Weaknesses

1. **No function signature extraction** - The most frequently needed follow-up: "What parameters does this take?" Forces a file read.

2. **No decorator awareness** - Cannot distinguish `@classmethod`, `@staticmethod`, `@property`, `@pytest.fixture`, `@abstractmethod`, etc. Forces a file read.

3. **No class context** - For method matches, no base classes or class-level information. Forces a `/cq q` or file read.

4. **Code block language tag is hardcoded** - The markdown renderer (`report.py`) uses ` ```python` for all context code blocks regardless of match language. Rust matches render with Python syntax highlighting. (Fixed as part of this effort.)

5. **No async/generator/await awareness** - Cannot distinguish `def`, `async def`, generator functions, or whether async bodies contain `await`. Affects import decisions and call patterns.

6. **No return type / type annotation extraction** - For callsites, knowing return type eliminates "what type does this return?" follow-ups.

7. **No semantic `item_role`** - Raw `node_kind` values like `"function_definition"`, `"call"`, `"identifier"` are tree-sitter grammar internals. A semantic `item_role` (like `"method"`, `"classmethod"`, `"free_function"`, `"test_function"`) would be more actionable for agents.

8. **Partial symtable surface** - `is_referenced`, `is_local`, `is_nonlocal` flags are available from symtable but not currently surfaced. These improve refactor safety reasoning.

9. **No function behavior summary** - Cannot answer "does it raise?", "does it yield?", "does it await?" without reading the file.

10. **Coordinate correctness gap** - Current search ingestion uses ripgrep submatch byte offsets; classification and enrichment paths use line/column coordinates. Unicode lines can desynchronize byte offsets and character columns.

## Architecture

### Three-Tier Backend with Deterministic Precedence (A2)

Recommended extraction order:

1. **ast-grep/SgNode** (primary) - Already parsed per-file by the classifier. Zero incremental cost.
2. **Python `ast`** (semantic normalization) - For scope-safe facts (generator detection, import normalization) where ast-grep's tree structure is insufficient.
3. **tree-sitter-python QueryCursor** (gap-fill only) - Only for fields where both above are weak. Deferred to future.

Precedence rules:

1. Keep first non-empty value from the primary source.
2. Fill missing fields from lower tiers.
3. Never overwrite non-empty values unless explicitly marked as a canonical override.

### Mirror Rust Enrichment Architecture (A1)

Adopt the now-mature Rust enrichment module design:

1. Bounded, hash-aware cache (blake2b staleness detection, FIFO LRU).
2. Per-extractor bounds and truncation rules.
3. Grouped extraction helpers with `_try_extract()` pattern.
4. Explicit enrichment runtime metadata:
   - `enrichment_status`: `"applied"` | `"degraded"`,
   - `enrichment_sources`: `["ast_grep"]` / `["ast_grep", "python_ast"]`,
   - `degrade_reason`: Optional string when status is `"degraded"`.

### Parallel Python/Rust Payload Contracts (A3)

Define shared field groups across languages:

| Group | Python | Rust | Shared Semantics |
|-------|--------|------|-----------------|
| `identity` | `category`, `item_role`, `node_kind`, `language` | `node_kind`, `item_role`, `language` | Both emit `item_role` + `node_kind` |
| `signature` | `signature`, `params`, `return_type`, `is_async`, `is_generator` | `signature`, `params`, `return_type`, `generics`, `is_async`, `is_unsafe` | Same field names where applicable |
| `modifiers` | `decorators` | `visibility`, `attributes` | Language-specific modifier surface |
| `scope` | `containing_scope`, `scope_chain`, `structural_context` | `scope_chain`, `scope_kind`, `scope_name` | `scope_chain` shared |
| `call` | `call_target`, `call_receiver`, `call_method`, `call_args_count` | `call_target`, `call_receiver`, `call_method`, `macro_name` | Same field names |
| `import` | `import_module`, `import_names`, `import_alias`, `is_type_import`, `import_level` | (n/a) | Python-specific |
| `binding` | `binding_flags` | (n/a) | Python-specific (from symtable) |
| `behavior` | `returns_value`, `raises_exception`, `yields`, `awaits`, `has_context_manager` | (n/a) | Python-specific |
| `class_context` | `class_name`, `base_classes`, `is_dataclass`, `class_kind` | `impl_type`, `impl_trait`, `impl_kind` | Language-specific shape context |
| `runtime` | `enrichment_status`, `enrichment_sources`, `degrade_reason` | `enrichment_status`, `enrichment_sources`, `degrade_reason` | Identical contract |

Language-specific fields are allowed, but group names and runtime semantics match.

### Classifier Integration Without Redundant Traversal (A4)

The classifier already resolves a node candidate via `classify_from_node()`. The enrichment step must receive the resolved node metadata (or a per-match node locator) to avoid a second full-tree lookup.

### Enrichment Contract

> All fields produced by enrichment are strictly additive. They **never** affect: confidence scores, match counts, category classification, or relevance ranking. Numeric confidence is preserved in `ScoreDetails`; optional display buckets may be added without removing the numeric signal.

## Prerequisites (Blocking)

### P1: Byte-Range Coordinate Correctness (C5)

**Status: Blocking prerequisite.**

Current search ingestion uses ripgrep submatch byte offsets; classification uses line/column coordinates. Unicode lines can desynchronize these.

**Required changes**:

1. Add `enrich_python_context_by_byte_range(sg_root, source_bytes, byte_start, byte_end)` entrypoint from day one.
2. Convert ripgrep byte offsets to tree lookup by byte range first.
3. Use line/column only as fallback when byte range is unavailable.

### P2: Classifier Node Passthrough (A4)

Extend `classify_from_node()` to return the found `SgNode` alongside the `NodeClassification`, or cache it for the enrichment step. This avoids a second full-tree lookup for the same match.

### P3: Payload Bounds Framework

Add truncation constants and a `_truncate()` helper (same pattern as Rust enrichment):

```python
_MAX_SIGNATURE_LEN = 200
_MAX_PARAMS = 12
_MAX_RETURN_TYPE_LEN = 100
_MAX_DECORATORS = 8
_MAX_DECORATOR_LEN = 60
_MAX_BASE_CLASSES = 6
_MAX_BASE_CLASS_LEN = 60
_MAX_CALL_TARGET_LEN = 120
_MAX_CALL_RECEIVER_LEN = 80
_MAX_SCOPE_CHAIN = 8
_MAX_IMPORT_NAMES = 12
_MAX_METHODS_SHOWN = 8
_MAX_PROPERTIES_SHOWN = 8
```

### P4: Graceful Degradation

Any individual extractor failure must not prevent the rest from running. Use the same `_try_extract()` pattern from Rust enrichment: catch exceptions per-extractor, set `enrichment_status: "degraded"` with `degrade_reason`.

## Recommendations

### Recommendation 1: Function Signature Extraction

**Impact**: Eliminates the #1 most common follow-up query.

**New fields** (for `function_definition` / `decorated_definition` matches):

| Field | Type | Example |
|-------|------|---------|
| `params` | `list[str]` | `["self", "name: str", "*args", "**kwargs"]` |
| `return_type` | `str \| None` | `"dict[str, int]"`, `None` |
| `is_async` | `bool` | `true` / `false` |
| `is_generator` | `bool` | `true` if body contains `yield` at function scope |
| `signature` | `str` | Full signature line up to `:` |

**Extraction path**: `SgNode.field("parameters")` for params, `SgNode.field("return_type")` for return type. For `is_async`: check if node kind is `"function_definition"` and has `"async"` keyword child.

**Generator detection (C6)**: Use Python `ast` for scope-safe generator determination:
1. Walk function body AST nodes.
2. Ignore nested `FunctionDef`, `AsyncFunctionDef`, `ClassDef`, `Lambda` scopes.
3. Detect `Yield` / `YieldFrom` nodes at function scope only.
4. Keep ast-grep as fast first pass; confirm via `ast` when body contains nested scopes that could cause false positives.

**Bounds**: `_MAX_PARAMS = 12`, `_MAX_SIGNATURE_LEN = 200`, `_MAX_RETURN_TYPE_LEN = 100`.

### Recommendation 2: Decorator + Item Role Extraction

**Impact**: Distinguishes method types, test functions, abstract methods, property accessors.

**New fields**:

| Field | Type | Example |
|-------|------|---------|
| `decorators` | `list[str]` | `["classmethod", "lru_cache(maxsize=128)"]` |
| `item_role` | `str` | `"method"`, `"classmethod"`, `"staticmethod"`, `"property_getter"`, `"test_function"`, `"free_function"`, `"fixture"`, `"abstractmethod"`, `"class_def"`, `"dataclass"` |

**`item_role` is additive to `node_kind`, not a replacement (C7).** Both fields are emitted: `node_kind` for debugging/rule diagnostics, `item_role` as the agent-facing semantic abstraction.

**Extraction path**: For `decorated_definition`, walk `decorator` children. For `item_role`, classify based on decorators + context:

```
@classmethod -> "classmethod"
@staticmethod -> "staticmethod"
@property -> "property_getter"
@X.setter -> "property_setter"
@abstractmethod -> "abstractmethod"
@pytest.fixture -> "fixture"
@pytest.mark.parametrize -> "test_function"
def test_* -> "test_function"
inside class + has self param -> "method"
inside class + no self param -> "static_or_classmethod" (fallback)
at module level -> "free_function"
class at module level -> "class_def"
@dataclass on class -> "dataclass"
```

**Bounds**: `_MAX_DECORATORS = 8`, `_MAX_DECORATOR_LEN = 60`.

**Known test decorator patterns**:
```python
_TEST_DECORATOR_NAMES: frozenset[str] = frozenset({
    "pytest.fixture", "pytest.mark.parametrize",
    "pytest.mark.skipif", "pytest.mark.skip",
    "unittest.mock.patch", "mock.patch",
})
_TEST_FUNCTION_PREFIX = "test_"
```

### Recommendation 3: Class Context Extraction

**Impact**: For methods, provides containing class name + base classes without file read.

**New fields** (when match is inside a class):

| Field | Type | Example |
|-------|------|---------|
| `class_name` | `str` | `"GraphBuilder"` |
| `base_classes` | `list[str]` | `["Protocol", "Generic[T]"]` |
| `is_dataclass` | `bool` | `true` if `@dataclass` decorator present |
| `class_kind` | `str` | `"class"`, `"dataclass"`, `"protocol"`, `"enum"`, `"exception"` |

**Extraction path**: Walk up to `class_definition`, extract `field("name")` and `field("superclasses")` children. Classify based on base classes: `Protocol`/`ABC` -> protocol, `Enum`/`IntEnum` -> enum, `Exception`/`Error` suffix -> exception.

**Bounds**: `_MAX_BASE_CLASSES = 6`, `_MAX_BASE_CLASS_LEN = 60`.

### Recommendation 4: Call Target Enrichment

**Impact**: For callsite matches, distinguishes method calls from function calls, provides receiver.

**New fields** (for `call` node matches):

| Field | Type | Example |
|-------|------|---------|
| `call_target` | `str` | `"builder.build_graph"`, `"Path"` |
| `call_receiver` | `str \| None` | `"builder"`, `None` |
| `call_method` | `str \| None` | `"build_graph"`, `None` |
| `call_args_count` | `int` | `3` |

**Extraction path**: For `call` node, extract `field("function")`. If function is `attribute` (method call): split into receiver + method. Count `arguments` children.

**Bounds**: `_MAX_CALL_TARGET_LEN = 120`, `_MAX_CALL_RECEIVER_LEN = 80`.

### Recommendation 5: Containing Structure Context

**Impact**: Tells agents "this code is inside a try/except", "inside a with block", "inside a comprehension" without reading the file.

**New fields**:

| Field | Type | Example |
|-------|------|---------|
| `scope_chain` | `list[str]` | `["module", "GraphBuilder", "build_graph"]` |
| `structural_context` | `str \| None` | `"try_block"`, `"except_handler"`, `"with_block"`, `"for_loop"`, `"comprehension"`, `None` |

**Extraction path**: Walk parent chain from match node. Record first encountered structural context from: `try_statement`, `except_clause`, `with_statement`, `for_statement`, `while_statement`, `if_statement`, `list_comprehension`, `dict_comprehension`, `set_comprehension`, `generator_expression`.

**Bounds**: `_MAX_SCOPE_CHAIN = 8`.

### Recommendation 6: Import Detail Extraction (C8)

**Impact**: For import matches, provides structured import info without parsing the line.

**New fields** (for `import_statement` / `import_from_statement` matches):

| Field | Type | Example |
|-------|------|---------|
| `import_module` | `str` | `"collections.abc"` |
| `import_names` | `list[str]` | `["Callable", "Sequence"]` |
| `import_alias` | `str \| None` | `"abc"` (from `import X as abc`) |
| `import_level` | `int` | `0` (absolute), `1` (from . import), `2` (from .. import) |
| `is_type_import` | `bool` | `true` if inside `if TYPE_CHECKING:` |

**Two-pass extraction (C8)**:

1. **ast-grep structural capture**: Identify statement kind (`import_statement` vs `import_from_statement`), extract basic structure.
2. **Python `ast` normalization**: Canonical field extraction for: `module`, `names` (handling multi-import), `asname` (alias combinations), `level` (relative imports), `type_checking_guarded` (detecting `if TYPE_CHECKING:` parent context).

This two-pass approach handles edge cases that ast-grep alone cannot reliably resolve:
- Multi-import statements: `from os import path, getcwd`
- Alias combinations: `from collections import OrderedDict as OD, defaultdict as dd`
- Relative imports: `from ..utils import helper`
- `TYPE_CHECKING` guard detection via parent `if_statement` condition text.

**Bounds**: `_MAX_IMPORT_NAMES = 12`.

### Recommendation 7: Function Behavior Summary (O1)

**Impact**: Directly answers "does it raise?", "does it yield?", "does it await?" without file reads.

**New fields** (for `function_definition` matches):

| Field | Type | Example |
|-------|------|---------|
| `returns_value` | `bool` | `true` if has non-None `return` at function scope |
| `raises_exception` | `bool` | `true` if contains `raise` at function scope |
| `yields` | `bool` | `true` if generator (same as `is_generator`) |
| `awaits` | `bool` | `true` if async body contains `await` |
| `has_context_manager` | `bool` | `true` if contains `with`/`async with` |

**Extraction path**: Use Python `ast` for scope-safe analysis:
1. Walk function body AST nodes.
2. Ignore nested `FunctionDef`, `AsyncFunctionDef`, `ClassDef`, `Lambda` scopes.
3. Detect `Return` (with non-None value), `Raise`, `Yield`/`YieldFrom`, `Await`, `With`/`AsyncWith` at function scope only.

### Recommendation 8: Expanded Binding Signals (O2)

**Impact**: Improves refactor safety reasoning.

Expand the subset of symtable flags currently surfaced in `binding_flags`:

| Flag | Currently Surfaced | Action |
|------|-------------------|--------|
| `imported` | Yes | Keep |
| `assigned` | Yes | Keep |
| `parameter` | Yes | Keep |
| `closure_var` (free) | Yes | Keep |
| `global` | Yes | Keep |
| `is_referenced` | **No** | **Add** |
| `is_local` | **No** | **Add** |
| `is_nonlocal` | **No** | **Add** |

These are already available from `SymtableEnrichment` (the struct has all 8 flags). The change is in `_symtable_flags()` in `smart_search.py` which currently only surfaces 5 of 8.

### Recommendation 9: Class API Shape Summary (O3)

**Impact**: Fast architectural context for class definitions without file reads.

**New fields** (for `class_definition` matches):

| Field | Type | Example |
|-------|------|---------|
| `method_count` | `int` | `12` |
| `property_names` | `list[str]` | `["name", "value"]` |
| `abstract_member_count` | `int` | `3` |
| `class_markers` | `list[str]` | `["dataclass", "frozen", "slots"]` |

**Extraction path**: Walk class body children. Count `function_definition` nodes (methods), identify `@property` decorators, count `@abstractmethod` decorators, detect `@dataclass(frozen=True, slots=True)` arguments.

**Bounds**: `_MAX_METHODS_SHOWN = 8`, `_MAX_PROPERTIES_SHOWN = 8`.

### Recommendation 10: Output Trimming (Staged)

**Impact**: Frees output tokens for new enrichment fields.

| Field | Action | Rationale |
|-------|--------|-----------|
| `evidence_kind` | **Keep in `ScoreDetails`; drop from rendered prose only (C2)** | Drives score bucketing and test expectations. Useful for diagnosing classifier regressions. |
| `context_window` | **Always show alongside `context_snippet`** | High-value: provides line-range anchoring ("Context (lines X-Y)") that locates code without file reads. Always rendered for both Python and Rust matches. |
| `line_text` | **Remove when `context_snippet` present** | Redundant. Keep only when snippet is absent. |
| `node_kind` | **Keep alongside `item_role` (C7)** | `node_kind` retained for debugging/rule diagnostics. `item_role` added as agent-facing abstraction. |
| `confidence` | **Keep numeric in `ScoreDetails`; add optional display bucket (C1)** | Current ranking relies on numeric confidence. Display buckets (`"definite"`, `"high"`, `"medium"`, `"low"`) are additive, not replacement. |

**Net effect**: Modest token savings from `line_text` deduplication. `context_window` is always shown (it anchors the context_snippet with line ranges, avoiding file reads). New enrichment fields add ~80 tokens of high-value structured data per match.

## Per-Match Payload Budget Metadata (O6)

Expose truncation metadata in enrichment payloads:

| Field | Type | Example |
|-------|------|---------|
| `truncated_fields` | `list[str]` | `["signature", "base_classes"]` |
| `payload_size_hint` | `int` | `342` (approximate token count) |
| `dropped_fields` | `list[str] \| None` | `["class_markers"]` (when payload budget exceeded) |

This enables bounded outputs and explicit tradeoffs for context-window-constrained consumers.

## Compute Cost Analysis

| Extractor | Parse Required? | Per-Match Cost | Total Budget Impact |
|-----------|----------------|---------------|-------------------|
| Signature (Rec 1) | No (reuse SgRoot) | ~5 us field access | <1% |
| Decorators (Rec 2) | No (reuse SgRoot) | ~3 us child walk | <1% |
| Class context (Rec 3) | No (reuse SgRoot) | ~5 us parent walk | <1% |
| Call target (Rec 4) | No (reuse SgRoot) | ~3 us field access | <1% |
| Structural context (Rec 5) | No (reuse SgRoot) | ~3 us parent walk | <1% |
| Import detail (Rec 6) | ast-grep + Python `ast` | ~50 us (ast parse) | <5% |
| Behavior summary (Rec 7) | Python `ast` | ~30 us (ast walk) | <3% |
| Binding expansion (Rec 8) | No (already computed) | ~0 us | 0% |
| Class shape (Rec 9) | No (reuse SgRoot) | ~5 us child walk | <1% |

**Total incremental compute**: <10% increase. Well within the 3x budget constraint.

The Python `ast` tier (Recs 6-7) adds modest cost but provides scope-safe semantics that ast-grep cannot guarantee (generator detection across nested scopes, canonical import normalization). This cost is amortized: `ast.parse()` is cached per-file and reused across all matches in that file.

## Implementation Architecture

### Module Structure (C4)

**Primary module**: `tools/cq/search/python_enrichment.py`
- All Python-specific enrichment extractors.
- Single orchestrating entrypoint: `enrich_python_context(...)`.
- Merges sources by precedence (ast-grep primary, Python `ast` secondary).

**Optional gap-fill backend** (future): `tools/cq/search/tree_sitter_python.py`
- Only added when tree-sitter QueryCursor queries are needed for fields that both ast-grep and Python `ast` cannot provide.
- Not part of initial implementation.

```python
def enrich_python_context(
    sg_root: SgRoot,
    node: SgNode,
    source_bytes: bytes,
    *,
    line: int,
    col: int,
    cache_key: str,
) -> dict[str, object] | None:
    """Enrich a Python match with structured context fields.

    Parameters
    ----------
    sg_root
        Parsed ast-grep root for the file.
    node
        Resolved SgNode at the match position (from classifier).
    source_bytes
        Raw source bytes (for Python ast tier).
    line
        1-indexed line number.
    col
        0-indexed column offset.
    cache_key
        File path for cache keying.

    Returns
    -------
    dict[str, object] | None
        Enrichment payload, or None if enrichment not applicable.
    """
```

**Byte-range entrypoint (P1)**:

```python
def enrich_python_context_by_byte_range(
    sg_root: SgRoot,
    source_bytes: bytes,
    byte_start: int,
    byte_end: int,
    *,
    cache_key: str,
) -> dict[str, object] | None:
    """Enrich using byte-range anchor (preferred for ripgrep integration)."""
```

### Integration Point

```
smart_search.py:_classify_and_enrich()
  -> classifier.py:classify_from_node()  # existing: gets node + SgRoot
     -> CHANGED: also returns resolved SgNode (P2)
  -> python_enrichment.py:enrich_python_context()  # NEW: extracts from same SgRoot/node
     -> returns dict[str, object] stored as EnrichedMatch.python_enrichment
        -> merged into Finding.details.data
```

Key design: the enrichment function receives the **same `SgNode`** that the classifier already found. Zero redundant node lookup.

### Output Format

Enrichment fields merge directly into the `data` dict. Both `node_kind` (debugging) and `item_role` (agent-facing) are present. Numeric confidence preserved in `ScoreDetails`; display bucket optional.

```python
# Callsite match:
data = {
    "match_text": "build_graph",
    "language": "python",
    "node_kind": "call",
    "item_role": "callsite",
    "context_window": {"start_line": 42, "end_line": 48},
    "context_snippet": "def process(config):\n    result = build_graph(config, ctx)\n    ...",
    "containing_scope": "process",
    "scope_chain": ["module", "Pipeline", "process"],
    "call_target": "build_graph",
    "call_args_count": 2,
    "binding_flags": ["imported"],
    "enrichment_status": "applied",
    "enrichment_sources": ["ast_grep"],
}

# Definition match:
data = {
    "match_text": "build_graph",
    "language": "python",
    "node_kind": "function_definition",
    "item_role": "method",
    "context_window": {"start_line": 15, "end_line": 28},
    "context_snippet": "    async def build_graph(self, config: Config, ctx: Context) -> Graph:",
    "containing_scope": "Pipeline.build_graph",
    "scope_chain": ["module", "Pipeline", "build_graph"],
    "signature": "async def build_graph(self, config: Config, ctx: Context) -> Graph",
    "params": ["self", "config: Config", "ctx: Context"],
    "return_type": "Graph",
    "is_async": true,
    "is_generator": false,
    "decorators": ["lru_cache(maxsize=32)"],
    "class_name": "Pipeline",
    "base_classes": ["Protocol"],
    "class_kind": "protocol",
    "returns_value": true,
    "raises_exception": false,
    "yields": false,
    "awaits": true,
    "has_context_manager": false,
    "enrichment_status": "applied",
    "enrichment_sources": ["ast_grep", "python_ast"],
}

# Import match:
data = {
    "match_text": "Callable",
    "language": "python",
    "node_kind": "import_from_statement",
    "item_role": "from_import",
    "import_module": "collections.abc",
    "import_names": ["Callable", "Sequence"],
    "import_alias": null,
    "import_level": 0,
    "is_type_import": true,
    "enrichment_status": "applied",
    "enrichment_sources": ["ast_grep", "python_ast"],
}
```

## Execution Sequence

### Phase 1: Correctness and Contracts

**Scope**: P1 (byte-range correctness), P2 (classifier node passthrough), P3 (payload bounds), P4 (graceful degradation), Rec 10 (output trimming - staged), C1 (additive contract enforcement), C7 (`item_role` additive to `node_kind`).

1. Add `enrich_python_context_by_byte_range()` entrypoint.
2. Extend `classify_from_node()` to return resolved `SgNode`.
3. Add payload bounds constants and `_truncate()` helper.
4. Add `_try_extract()` pattern with `enrichment_status`/`degrade_reason`.
5. Wire `line_text` suppression when `context_snippet` present.
6. Ensure `context_window` + `context_snippet` always rendered for both Python and Rust.
7. Fix language-aware code block syntax highlighting in `report.py` (use match language instead of hardcoded `python`).

### Phase 2: Primary Enrichment Module + ast-grep Extraction

**Scope**: C4 (module naming), A1 (Rust pattern), Rec 1-5 (core extractors), Rec 2 (`item_role` wiring).

1. Create `tools/cq/search/python_enrichment.py` with bounded cache and grouped extractors.
2. Implement signature extraction (Rec 1).
3. Implement decorator + `item_role` extraction (Rec 2).
4. Implement class context extraction (Rec 3).
5. Implement call target extraction (Rec 4).
6. Implement scope chain + structural context (Rec 5).
7. Wire into `smart_search.py` via `MatchEnrichment`.

### Phase 3: Semantic Normalization Tier

**Scope**: C6 (scope-safe generator), C8 (import normalization), Rec 6-7 (import detail + behavior summary), Rec 8 (binding expansion).

1. Add Python `ast` cached parse tier.
2. Implement scope-safe generator detection (C6).
3. Implement two-pass import normalization (C8, Rec 6).
4. Implement function behavior summary (Rec 7).
5. Expand symtable binding flags surface (Rec 8).

### Phase 4: Payload Shaping + Class Shape

**Scope**: C2 (keep `evidence_kind`), Rec 9 (class API shape), O6 (payload budget metadata).

1. Implement class API shape summary (Rec 9).
2. Add per-match payload budget metadata (O6).
3. Verify `evidence_kind` preserved in `ScoreDetails`.
4. Verify `context_window` + `context_snippet` always present and rendered for both Python and Rust matches.

### Phase 5: Comprehensive Test Suite

**Scope**: All extractors, correctness edge cases, review-required categories.

1. Unicode coordinate tests for byte-range anchor correctness.
2. Snapshot parity tests ensuring enrichment does not alter ranking order.
3. Degradation tests with per-extractor failures (status stays fail-open).
4. Function behavior summary tests (`yield`, `raise`, `await`) with nested scopes.
5. Import normalization tests covering aliases, relative imports, and `TYPE_CHECKING`.
6. Payload bound/truncation tests.
7. Grammar schema-lint tests for node kinds/fields used by extractors.

### Future Phase: Optional Tree-sitter Augmentation

**Scope**: O4 (query-pack quality gates), O5 (schema-lint), O7 (incremental parse scaffolding).

Deferred to post-initial-implementation:
1. Add `tools/cq/search/tree_sitter_python.py` gap-fill backend.
2. Lint for rooted/local query patterns.
3. Enforce `match_limit` and range-scoped execution.
4. Wire progress callback cancellation.
5. Schema-lint tests validating node kinds/fields against grammar `node-types.json`.
6. Incremental parse/query scaffolding for repeated searches.

## Follow-Up Query Reduction Estimate

| Query Type | Before | After | Savings |
|------------|--------|-------|---------|
| "What params does X take?" | File read | Included | 1 query |
| "Is this async/generator?" | File read | Included | 1 query |
| "Does it raise/yield/await?" | File read | Included | 1 query |
| "What class is this method in?" | `/cq q` or file read | Included | 1 query |
| "Is this a test function?" | File read | `item_role` | 1 query |
| "What does this import?" | Already partial | Structured | 0.5 query |
| "What decorators?" | File read | Included | 1 query |
| "What's the class API shape?" | File read | Included | 1 query |
| **Typical routine review** | **3-5 follow-ups** | **0-1 follow-ups** | **70-80%** |

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Output bloat from new fields | Offset by Rec 10 trimming; payload budget metadata (O6) |
| ast-grep API surface changes | All extractors use stable `field()` / `kind()` / `parent()` / `children()` |
| Python `ast` version drift | Use only stable 3.13 AST node types; no deprecated visitors |
| Extractor failures on edge cases | Per-extractor `_try_extract()` with degradation metadata |
| Generator misclassification in nested scopes | Scope-safe `ast` walk ignoring nested function/class scopes (C6) |
| Import edge cases (multi, alias, relative) | Two-pass extraction with `ast` canonical normalization (C8) |
| Unicode coordinate desync | Byte-range entrypoint as primary path (P1/C5) |
| Breaking existing test expectations | Output changes staged; `context_window` retained as always-show (C2) |

## Files Modified (Estimated)

| File | Phase | Nature |
|------|-------|--------|
| `tools/cq/search/python_enrichment.py` | 2-4 | **New** (~500 lines) |
| `tools/cq/search/smart_search.py` | 1-2 | Minor integration (~40 lines) |
| `tools/cq/search/classifier.py` | 1 | Minor: expose SgNode from classify (~15 lines) |
| `tools/cq/core/report.py` | 1 | Minor: language-aware code block tags (~3 lines changed) |
| `tests/unit/cq/search/test_python_enrichment.py` | 5 | **New** (~500 lines) |
| `tests/unit/cq/search/test_smart_search.py` | 5 | Updates for output format changes |

## Acceptance Criteria

1. Enrichment remains additive and fail-open.
2. No changes to relevance ranking or match counts from enrichment fields.
3. Python enrichment returns deterministic, bounded payloads.
4. Byte/column correctness is guaranteed for non-ASCII source.
5. Follow-up query rate for common Python review tasks is materially reduced.
6. Python and Rust enrichment runtime contracts are structurally aligned.
7. `context_window` + `context_snippet` always rendered in materialized markdown for both Python and Rust matches.
8. Code blocks in markdown output use language-aware syntax highlighting (`python` / `rust`).

## Comparison: Before vs After

### Before: Agent searches for `build_graph`

```
Top Contexts:
  Pipeline.process (src/engine/pipeline.py)
    call, confidence=0.95, evidence=resolved_ast
    context: "    result = build_graph(config, ctx)"
    binding_flags: [imported]

Definitions:
  src/graph.py:42 definition
    context: "def build_graph(request: BuildRequest) -> GraphProduct:"
```

Agent must then:
1. Read `src/graph.py` to see full signature and parameters
2. Read `src/engine/pipeline.py` to understand if it's in a class
3. Check for decorators, async, return type
4. Check if function raises, yields, or uses context managers

### After: Same search, richer output

```
Top Contexts:
  Pipeline.process (src/engine/pipeline.py)
    call (node_kind=call), item_role=callsite
    scope: [module, Pipeline, process]
    call_target=build_graph, args=2
    binding: [imported]

Definitions:
  src/graph.py:42 free_function (node_kind=function_definition)
    signature: def build_graph(request: BuildRequest) -> GraphProduct
    params: [request: BuildRequest]
    return_type: GraphProduct
    decorators: [lru_cache(maxsize=64)]
    behavior: returns_value=true, raises=false, yields=false, awaits=false
```

Agent can now proceed directly to editing with full context. Zero follow-up queries needed.
