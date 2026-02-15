# CQ Search Restructuring Implementation Plan v1 (2026-02-15)

## Scope Summary

This plan restructures `tools/cq/search/` (21,284 LOC across 90+ files) into a well-factored codebase where every file is 250-750 LOC with clear semantic subfolder boundaries. The restructuring eliminates widespread duplication (7 helpers duplicated 5+ times each), consolidates 23 scattered contract files (879 LOC) into unified contract modules, enforces data object patterns (CqStruct/CqOutputStruct for serialization, dataclass for runtime-only), and adopts unused library capabilities (TreeCursor, msgspec.convert, ast-grep Config).

Design stance: **Hard cutover, no compatibility shims.** This is a design-stage restructuring with no external consumers. All legacy files are deleted in the same scope item that creates their replacements. No re-exports, no deprecated markers, no transitional wrappers. Every import is updated to its final path immediately.

## Design Principles

1. **File size discipline**: Every file 250-750 LOC. Split above 750, merge below 250.
2. **Semantic cohesion**: Group by responsibility domain (ts/, python/, rust/, pipeline/, resolution/).
3. **Eliminate duplication**: Consolidate all duplicated helpers into _helpers/ package before creating domain packages.
4. **Data object hygiene**: CqStruct/CqOutputStruct/CqSettingsStruct for serialization, plain dataclass for runtime-only objects containing AST/Tree handles.
5. **Contract consolidation**: Merge scattered contract files within each domain (23 tree-sitter contracts → ts/contracts.py).
6. **Hard cutover per scope item**: Each scope item creates new modules, updates all imports, and deletes legacy files atomically. No transitional coexistence.
7. **Test parity**: Every new file gets corresponding test file under tests/unit/cq/search/.
8. **Import discipline**: Absolute imports only, `from __future__ import annotations`, type-only imports in `if TYPE_CHECKING:`.
9. **Library adoption**: Use TreeCursor, msgspec.convert, ast-grep Config where applicable.
10. **No compatibility shims**: No re-exports, no deprecated wrappers, no `# removed` comments. All callers are updated to final import paths. `__init__.py` files contain only the go-forward public API.

## Current Baseline

- **Total LOC**: 21,284 across 90+ files in tools/cq/search/
- **Files over 750 LOC** (6 files, 10,413 LOC total): smart_search.py (3720), python_enrichment.py (2251), tree_sitter_rust.py (1765), classifier.py (1047), python_native_resolution.py (823), tree_sitter_python.py (807)
- **Files under 250 LOC** (50+ files): 23 tree-sitter contract files (879 LOC total), 15+ small utility files (30-190 LOC each), 12+ generated/init/leaf files
- **Confirmed duplication** (12 patterns): `_node_text()` (7 copies), `_line_col_to_byte_offset()` (4 copies), `_source_hash()` (3 copies), `_truncate()` (2 copies), `_scope_chain()` (3 variant copies), ENRICHMENT_ERRORS tuple (5 copies), tree-sitter language factories (4+ copies), import guards (8+ copies), `_compile_query()` pattern (3 copies), payload budget enforcement (2 copies), gap-fill merge logic (2 copies), source tracking append (2 copies)
- **Contract files to merge**: tree_sitter_*_contracts.py (23 files, 879 LOC), rust_*_contracts.py (2 files), object_*_contracts.py (2 files)
- **Data object issues**: HeuristicResult/NodeClassification/SymtableEnrichment (msgspec.Struct, should be CqOutputStruct), RgProcessResult (dataclass, should be CqOutputStruct), RuntimeBoundarySummary (plain msgspec.Struct, should be CqStruct), _PythonEnrichmentState (mixed serializable + runtime handles, must split)
- **Unused capabilities**: TreeCursor, Language.id_for_node_kind(), ast-grep Config with constraints, msgspec.convert(from_attributes=True), gc=False/cache_hash=True struct options, reusable Encoder/Decoder instances
- **Target structure**: 7 semantic packages (_helpers/, ts/, python/, rust/, rg/, pipeline/, resolution/) plus existing enrichment/, generated/, queries/

---

## S1. Shared Helpers Foundation (`_helpers/`)

### Goal

Establish the _helpers/ package as the single source of truth for all shared utility functions, eliminating 12 patterns of duplication. Consolidate node text extraction (7 copies), byte offset conversion (4 copies), source hashing (3 copies), truncation (2 copies), error tuples (5 copies), and constants (scattered across 10+ files) into a coherent helper library that all domain packages depend on.

### Representative Code Snippets

**_helpers/node_text.py** (unified text extraction for both tree-sitter Node and ast-grep SgNode):

```python
from __future__ import annotations

from hashlib import blake2b
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ast_grep_py import SgNode
    from tree_sitter import Node


def node_text(node: Node, source_bytes: bytes) -> str:
    """Extract text from a tree-sitter Node.

    Parameters
    ----------
    node : Node
        Tree-sitter node to extract text from.
    source_bytes : bytes
        Source file bytes.

    Returns
    -------
    str
        Extracted text, or empty string if node has no extent.
    """
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", start))
    if end <= start:
        return ""
    return source_bytes[start:end].decode("utf-8", errors="replace")


def node_text_optional(node: Node | None, source_bytes: bytes) -> str | None:
    """Extract text from optional tree-sitter Node, returning None if absent.

    Parameters
    ----------
    node : Node | None
        Optional tree-sitter node.
    source_bytes : bytes
        Source file bytes.

    Returns
    -------
    str | None
        Extracted text, or None if node is absent or has no text.
    """
    if node is None:
        return None
    result = node_text(node, source_bytes)
    return result if result else None


def sg_node_text(node: SgNode | None) -> str | None:
    """Extract text from optional ast-grep SgNode.

    Parameters
    ----------
    node : SgNode | None
        Optional ast-grep node.

    Returns
    -------
    str | None
        Extracted text, or None if node is absent or has no text.
    """
    if node is None:
        return None
    text = node.text().strip()
    return text if text else None


def truncate(text: str, max_len: int) -> str:
    """Truncate text to maximum length with ellipsis.

    Parameters
    ----------
    text : str
        Text to truncate.
    max_len : int
        Maximum length including ellipsis.

    Returns
    -------
    str
        Truncated text with "..." suffix if needed.
    """
    if len(text) <= max_len:
        return text
    return text[: max(1, max_len - 3)] + "..."


def source_hash(source_bytes: bytes) -> str:
    """Compute BLAKE2b hash of source bytes.

    Parameters
    ----------
    source_bytes : bytes
        Source file bytes.

    Returns
    -------
    str
        Hexadecimal hash digest (32 characters).
    """
    return blake2b(source_bytes, digest_size=16).hexdigest()
```

**_helpers/byte_offsets.py** (unified line/col to byte offset conversion):

```python
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tree_sitter import Node


def line_col_to_byte_offset(source_bytes: bytes, line: int, col: int) -> int | None:
    """Convert 1-indexed line + 0-indexed column to absolute byte offset.

    Parameters
    ----------
    source_bytes : bytes
        Source file bytes.
    line : int
        1-indexed line number.
    col : int
        0-indexed column offset.

    Returns
    -------
    int | None
        Byte offset, or None if coordinates are invalid.
    """
    if line < 1 or col < 0:
        return None
    lines = source_bytes.splitlines(keepends=True)
    if line > len(lines):
        return None
    offset = sum(len(l) for l in lines[: line - 1]) + col
    return min(offset, len(source_bytes))


def node_byte_span(node: Node) -> tuple[int, int]:
    """Extract byte span from tree-sitter Node.

    Parameters
    ----------
    node : Node
        Tree-sitter node.

    Returns
    -------
    tuple[int, int]
        (start_byte, end_byte) pair.
    """
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", start))
    return (start, end)
```

**_helpers/errors.py** (unified error handling):

```python
from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

# Canonical error tuple for enrichment operations
ENRICHMENT_ERRORS: tuple[type[BaseException], ...] = (
    RuntimeError,
    TypeError,
    ValueError,
    AttributeError,
    UnicodeError,
)

T = TypeVar("T")


def try_extract(
    label: str,
    extractor: Callable[..., T],
    *args: object,
) -> tuple[T | None, str | None]:
    """Execute extractor with enrichment error handling.

    Parameters
    ----------
    label : str
        Error context label.
    extractor : Callable
        Extraction function to execute.
    *args : object
        Arguments to pass to extractor.

    Returns
    -------
    tuple[T | None, str | None]
        (result, error) pair. Either result is populated or error is.
    """
    try:
        result = extractor(*args)
    except ENRICHMENT_ERRORS as exc:
        return None, f"{label}: {type(exc).__name__}"
    else:
        return result, None


def try_extract_dict(
    label: str,
    extractor: Callable[..., dict[str, object]],
    *args: object,
) -> tuple[dict[str, object], str | None]:
    """Execute dict extractor with enrichment error handling.

    Parameters
    ----------
    label : str
        Error context label.
    extractor : Callable
        Extraction function returning dict.
    *args : object
        Arguments to pass to extractor.

    Returns
    -------
    tuple[dict[str, object], str | None]
        (result_dict, error) pair. Returns empty dict on error.
    """
    try:
        result = extractor(*args)
    except ENRICHMENT_ERRORS as exc:
        return {}, f"{label}: {type(exc).__name__}"
    else:
        return result, None
```

**_helpers/constants.py** (consolidated constants):

```python
from __future__ import annotations

# Maximum match result limits
MAX_MATCH_LIMIT: int = 1000
MAX_CANDIDATE_LIMIT: int = 500
MAX_ENRICHMENT_LIMIT: int = 100

# Source file size limits
MAX_SOURCE_BYTES: int = 5_000_000  # 5 MB
MAX_DISPLAY_BYTES: int = 1000

# Payload budget limits
MAX_PAYLOAD_SIZE: int = 100_000  # 100 KB per enrichment payload
MAX_TOTAL_PAYLOAD_SIZE: int = 1_000_000  # 1 MB total

# Tree-sitter operational limits
MAX_PARSE_RETRIES: int = 3
MAX_QUERY_MATCHES: int = 10_000

# Classification scoring thresholds
MIN_CONFIDENCE_SCORE: float = 0.5
MIN_RELEVANCE_SCORE: float = 0.3
```

### Files to Edit

- None (new package)

### New Files to Create

**Source files:**
- `tools/cq/search/_helpers/__init__.py` - Package init
- `tools/cq/search/_helpers/node_text.py` - ~120 LOC (node_text, node_text_optional, sg_node_text, truncate, source_hash)
- `tools/cq/search/_helpers/byte_offsets.py` - ~50 LOC (line_col_to_byte_offset, node_byte_span)
- `tools/cq/search/_helpers/errors.py` - ~80 LOC (ENRICHMENT_ERRORS, try_extract, try_extract_dict)
- `tools/cq/search/_helpers/constants.py` - ~40 LOC (all MAX_* constants)

**Test files:**
- `tests/unit/cq/search/_helpers/test_node_text.py` - Test all text extraction variants
- `tests/unit/cq/search/_helpers/test_byte_offsets.py` - Test offset conversion edge cases
- `tests/unit/cq/search/_helpers/test_errors.py` - Test error handling wrapper behavior
- `tests/unit/cq/search/_helpers/test_constants.py` - Smoke test constants exist

### Legacy Decommission/Delete Scope

**Functions to delete:**
- `tree_sitter_match_rows.py:_node_text()` (line ~45)
- `tree_sitter_injections.py:_node_text()` (line ~120)
- `tree_sitter_python_facts.py:_node_text()` (line ~85)
- `tree_sitter_python.py:_node_text()` (line ~120)
- `tree_sitter_tags.py:_node_text()` (line ~60)
- `rust_enrichment.py:_node_text()` (line ~180, SgNode variant)
- `tree_sitter_rust.py:_node_text()` (line ~140, Node variant)
- `python_enrichment.py:_line_col_to_byte_offset()` (line 1771)
- `language_front_door_pipeline.py:_line_col_to_byte_offset()` (line 220)
- `python_native_resolution.py:_line_col_to_byte_offset()` (line 60)
- `python_analysis_session.py:_line_col_to_byte_offset()` (line 35)
- `python_enrichment.py:_source_hash()` (line 240)
- `rust_enrichment.py:_source_hash()` (line 63)
- `python_analysis_session.py:_source_hash()` (line 95)
- `python_enrichment.py:_truncate()` (line 187)
- `tree_sitter_rust.py:_truncate()` (line 163)
- `python_enrichment.py:ENRICHMENT_ERRORS` (line 84)
- `tree_sitter_python.py:ENRICHMENT_ERRORS` (line 55)
- `tree_sitter_rust.py:ENRICHMENT_ERRORS` (line 84)
- `rust_enrichment.py:ENRICHMENT_ERRORS` (line 28)
- `smart_search.py:ENRICHMENT_ERRORS` (line 193)

**Constants to consolidate** (delete originals):
- `smart_search.py:MAX_MATCH_LIMIT`, `MAX_SOURCE_BYTES`, `MAX_DISPLAY_BYTES` (lines 80-90)
- `python_enrichment.py:MAX_PAYLOAD_SIZE`, `MAX_ENRICHMENT_LIMIT` (lines 50-60)
- `classifier.py:MIN_CONFIDENCE_SCORE` (line ~120)

---

## S2. Tree-Sitter Infrastructure (`ts/`)

### Goal

Consolidate all tree-sitter infrastructure (parsing, query execution, language loading, contracts, diagnostics, injections, specialization) into a unified ts/ package. Merge 23 scattered contract files (879 LOC) into ts/contracts.py (~300 LOC after deduplication). Unify all language loading patterns (4+ cached factory copies) into ts/language_registry.py. Absorb small utility files (parser_controls, stream_source, query_pack_lint, budgeting, work_queue) into their natural parent modules.

### Representative Code Snippets

**ts/contracts.py** (consolidated tree-sitter contracts):

```python
from __future__ import annotations

from tools.cq.core.structs import CqOutputStruct, CqStruct

# Runtime contracts
class TreeSitterRuntimeConfig(CqStruct):
    """Tree-sitter runtime configuration."""

    timeout_ms: int = 5000
    max_depth: int = 512
    enable_logging: bool = False


class ParseResult(CqOutputStruct):
    """Result of tree-sitter parse operation."""

    tree_available: bool
    parse_error: str | None = None
    root_node_type: str | None = None
    byte_count: int = 0


# Query contracts
class QueryMatch(CqOutputStruct):
    """Single query match result."""

    pattern_index: int
    captures: dict[str, list[tuple[int, int]]]  # name -> [(start, end), ...]


class QueryExecutionResult(CqOutputStruct):
    """Result of query execution."""

    matches: list[QueryMatch]
    execution_time_ms: float
    timeout_exceeded: bool = False


# Injection contracts
class InjectionPoint(CqOutputStruct):
    """Tree-sitter injection point."""

    language: str
    byte_range: tuple[int, int]
    combined_text: str


class InjectionResult(CqOutputStruct):
    """Result of injection parsing."""

    injections: list[InjectionPoint]
    parse_errors: list[str]


# Node schema contracts
class NodeFieldSchema(CqOutputStruct):
    """Schema for a node field."""

    field_name: str
    node_types: list[str]
    is_multiple: bool
    is_required: bool


class NodeTypeSchema(CqOutputStruct):
    """Schema for a node type."""

    type_name: str
    fields: dict[str, NodeFieldSchema]
    subtypes: list[str]
    is_named: bool
    is_supertype: bool


# Diagnostic contracts
class ParseDiagnostic(CqOutputStruct):
    """Parse error diagnostic."""

    byte_range: tuple[int, int]
    message: str
    severity: str  # error, warning


class GrammarDriftReport(CqOutputStruct):
    """Grammar version drift report."""

    expected_abi: int
    actual_abi: int
    breaking_changes: list[str]
    warnings: list[str]


# Change window contracts
class ChangeWindow(CqOutputStruct):
    """Changed byte range for incremental parsing."""

    byte_range: tuple[int, int]
    priority: int
    edit_count: int


# Specialization contracts
class QuerySpecialization(CqOutputStruct):
    """Specialized query for specific context."""

    base_pattern: str
    specialized_pattern: str
    context_predicates: list[str]


# Adaptive runtime contracts
class LatencyProfile(CqOutputStruct):
    """Latency tracking for adaptive runtime."""

    operation: str
    p50_ms: float
    p95_ms: float
    p99_ms: float
    sample_count: int
```

**ts/language_registry.py** (unified language loading):

```python
from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tree_sitter import Language, Parser

_HAS_TREE_SITTER = False
try:
    import tree_sitter as _ts
    import tree_sitter_python as _ts_python
    import tree_sitter_rust as _ts_rust

    _HAS_TREE_SITTER = True
except ImportError:
    _ts = None
    _ts_python = None
    _ts_rust = None


def is_tree_sitter_available() -> bool:
    """Check if tree-sitter bindings are available.

    Returns
    -------
    bool
        True if tree-sitter and language bindings are importable.
    """
    return _HAS_TREE_SITTER


@lru_cache(maxsize=1)
def python_language() -> Language:
    """Get cached Python tree-sitter Language.

    Returns
    -------
    Language
        Python language object.

    Raises
    ------
    RuntimeError
        If tree-sitter bindings are unavailable.
    """
    if _ts_python is None or _ts is None:
        msg = "tree_sitter_python bindings are unavailable"
        raise RuntimeError(msg)
    return _ts.Language(_ts_python.language())


@lru_cache(maxsize=1)
def rust_language() -> Language:
    """Get cached Rust tree-sitter Language.

    Returns
    -------
    Language
        Rust language object.

    Raises
    ------
    RuntimeError
        If tree-sitter bindings are unavailable.
    """
    if _ts_rust is None or _ts is None:
        msg = "tree_sitter_rust bindings are unavailable"
        raise RuntimeError(msg)
    return _ts.Language(_ts_rust.language())


def make_parser(language: Language) -> Parser:
    """Create a tree-sitter Parser for the given language.

    Parameters
    ----------
    language : Language
        Language to parse.

    Returns
    -------
    Parser
        Configured parser instance.
    """
    if _ts is None:
        msg = "tree_sitter bindings are unavailable"
        raise RuntimeError(msg)
    parser = _ts.Parser(language)
    return parser


@lru_cache(maxsize=128)
def node_kind_id(language: Language, kind_name: str) -> int:
    """Get numeric ID for a node kind (cached).

    Parameters
    ----------
    language : Language
        Tree-sitter language.
    kind_name : str
        Node kind name (e.g., "function_definition").

    Returns
    -------
    int
        Numeric node kind ID for fast comparisons.
    """
    return language.id_for_node_kind(kind_name, named=True)


def check_abi_compatibility(language: Language, expected_abi: int) -> GrammarDriftReport:
    """Check ABI compatibility for grammar version drift.

    Parameters
    ----------
    language : Language
        Language to check.
    expected_abi : int
        Expected ABI version.

    Returns
    -------
    GrammarDriftReport
        Compatibility report.
    """
    from tools.cq.search.ts.contracts import GrammarDriftReport

    actual_abi = language.version
    breaking = []
    warnings = []

    if actual_abi != expected_abi:
        if actual_abi < expected_abi:
            breaking.append(f"Grammar ABI {actual_abi} < expected {expected_abi}")
        else:
            warnings.append(f"Grammar ABI {actual_abi} > expected {expected_abi}")

    return GrammarDriftReport(
        expected_abi=expected_abi,
        actual_abi=actual_abi,
        breaking_changes=breaking,
        warnings=warnings,
    )
```

**ts/runtime.py** (query execution, absorbs tree_sitter_runtime.py + parallel utilities):

```python
from __future__ import annotations

from typing import TYPE_CHECKING

from tools.cq.search._helpers.errors import ENRICHMENT_ERRORS
from tools.cq.search.ts.contracts import QueryExecutionResult, QueryMatch
from tools.cq.search.ts.language_registry import is_tree_sitter_available

if TYPE_CHECKING:
    from tree_sitter import Language, Node, Query

if is_tree_sitter_available():
    import tree_sitter as ts
else:
    ts = None


def execute_query(
    query: Query,
    root_node: Node,
    source_bytes: bytes,
    timeout_ms: int = 5000,
) -> QueryExecutionResult:
    """Execute tree-sitter query against a parse tree.

    Parameters
    ----------
    query : Query
        Compiled tree-sitter query.
    root_node : Node
        Root node to query.
    source_bytes : bytes
        Source file bytes.
    timeout_ms : int
        Query timeout in milliseconds.

    Returns
    -------
    QueryExecutionResult
        Execution result with matches.
    """
    import time

    start = time.perf_counter()
    matches = []
    timeout_exceeded = False

    try:
        for match in query.matches(root_node, timeout_micros=timeout_ms * 1000):
            captures = {}
            for capture_name, capture_nodes in match.captures.items():
                byte_ranges = [
                    (int(n.start_byte), int(n.end_byte)) for n in capture_nodes
                ]
                captures[capture_name] = byte_ranges

            matches.append(
                QueryMatch(pattern_index=match.pattern_index, captures=captures)
            )
    except ENRICHMENT_ERRORS:
        timeout_exceeded = True

    elapsed = (time.perf_counter() - start) * 1000

    return QueryExecutionResult(
        matches=matches,
        execution_time_ms=elapsed,
        timeout_exceeded=timeout_exceeded,
    )
```

### Files to Edit

- None (new package)

### New Files to Create

**Source files:**
- `tools/cq/search/ts/__init__.py` - Package init
- `tools/cq/search/ts/contracts.py` - ~300 LOC (merge 23 contract files)
- `tools/cq/search/ts/language_registry.py` - ~150 LOC (language loading, ABI checks, node kind IDs)
- `tools/cq/search/ts/runtime.py` - ~350 LOC (query execution, from tree_sitter_runtime.py)
- `tools/cq/search/ts/parse_session.py` - ~400 LOC (absorb tree_sitter_parse_session.py + parser_controls.py + stream_source.py)
- `tools/cq/search/ts/query_registry.py` - ~350 LOC (absorb tree_sitter_query_registry.py + query_pack_lint.py + pack_metadata.py)
- `tools/cq/search/ts/node_schema.py` - ~400 LOC (absorb tree_sitter_node_schema.py + tree_sitter_node_codegen.py)
- `tools/cq/search/ts/custom_predicates.py` - ~250 LOC (from tree_sitter_custom_predicates.py)
- `tools/cq/search/ts/diagnostics.py` - ~300 LOC (merge diagnostics + recovery_hints + grammar_drift)
- `tools/cq/search/ts/change_windows.py` - ~350 LOC (merge change_windows + work_queue + budgeting)
- `tools/cq/search/ts/injections.py` - ~400 LOC (merge injections + injection_runtime + rust_injection_profiles)
- `tools/cq/search/ts/structural_export.py` - ~350 LOC (merge structural_export + token_export + match_rows)
- `tools/cq/search/ts/specialization.py` - ~300 LOC (merge specialization + specialization_contracts)
- `tools/cq/search/ts/adaptive_runtime.py` - ~250 LOC (merge adaptive_runtime + adaptive_runtime_contracts)

**Test files:**
- `tests/unit/cq/search/ts/test_contracts.py` - Round-trip serialization tests
- `tests/unit/cq/search/ts/test_language_registry.py` - Language loading, ABI checks
- `tests/unit/cq/search/ts/test_runtime.py` - Query execution tests
- `tests/unit/cq/search/ts/test_parse_session.py` - Incremental parsing tests
- `tests/unit/cq/search/ts/test_query_registry.py` - Query pack discovery tests
- `tests/unit/cq/search/ts/test_node_schema.py` - Schema extraction tests
- `tests/unit/cq/search/ts/test_custom_predicates.py` - Predicate tests
- `tests/unit/cq/search/ts/test_diagnostics.py` - Parse diagnostic tests
- `tests/unit/cq/search/ts/test_change_windows.py` - Change window tests
- `tests/unit/cq/search/ts/test_injections.py` - Injection parsing tests
- `tests/unit/cq/search/ts/test_structural_export.py` - Export tests
- `tests/unit/cq/search/ts/test_specialization.py` - Query specialization tests
- `tests/unit/cq/search/ts/test_adaptive_runtime.py` - Latency tracking tests

### Legacy Decommission/Delete Scope

**Contract files to delete** (after merging into ts/contracts.py):
- `tree_sitter_runtime_contracts.py` (~30 LOC)
- `tree_sitter_parse_session_contracts.py` (~25 LOC)
- `tree_sitter_query_contracts.py` (~40 LOC)
- `tree_sitter_artifact_contracts.py` (~35 LOC)
- `tree_sitter_injection_contracts.py` (~50 LOC)
- `tree_sitter_structural_contracts.py` (~45 LOC)
- `tree_sitter_node_schema_contracts.py` (~40 LOC)
- `tree_sitter_custom_predicate_contracts.py` (~30 LOC)
- `tree_sitter_grammar_drift_contracts.py` (~35 LOC)
- `tree_sitter_change_windows_contracts.py` (~40 LOC)
- `tree_sitter_work_queue_contracts.py` (~30 LOC)
- `tree_sitter_query_planner_contracts.py` (~45 LOC)
- `tree_sitter_query_specialization_contracts.py` (~40 LOC)
- `tree_sitter_adaptive_runtime_contracts.py` (~35 LOC)
- Plus 9 more minor contract files (~360 LOC total)

**Implementation files to delete** (replaced by ts/):
- `tree_sitter_runtime.py` (move to ts/runtime.py)
- `tree_sitter_parse_session.py` (move to ts/parse_session.py)
- `tree_sitter_parser_controls.py` (absorb into ts/parse_session.py)
- `tree_sitter_stream_source.py` (absorb into ts/parse_session.py)
- `tree_sitter_query_registry.py` (move to ts/query_registry.py)
- `query_pack_lint.py` (absorb into ts/query_registry.py)
- `tree_sitter_node_schema.py` (move to ts/node_schema.py)
- `tree_sitter_node_codegen.py` (absorb into ts/node_schema.py)
- `tree_sitter_custom_predicates.py` (move to ts/custom_predicates.py)
- `tree_sitter_grammar_drift.py` (absorb into ts/diagnostics.py)
- `tree_sitter_change_windows.py` (move to ts/change_windows.py)
- `tree_sitter_budgeting.py` (absorb into ts/change_windows.py)
- `tree_sitter_work_queue.py` (absorb into ts/change_windows.py)
- `tree_sitter_injections.py` (move to ts/injections.py)
- `tree_sitter_injection_runtime.py` (absorb into ts/injections.py)
- `tree_sitter_rust_injection_profiles.py` (absorb into ts/injections.py)
- `tree_sitter_structural_export.py` (move to ts/structural_export.py)
- `tree_sitter_token_export.py` (absorb into ts/structural_export.py)
- `tree_sitter_match_rows.py` (absorb into ts/structural_export.py)
- `tree_sitter_query_specialization.py` (move to ts/specialization.py)
- `tree_sitter_adaptive_runtime.py` (move to ts/adaptive_runtime.py)

**Language loading functions to delete** (replaced by ts/language_registry.py):
- `tree_sitter_python.py:_python_language()` (line ~80)
- `tree_sitter_rust.py:_rust_language()` (line ~90)
- `tree_sitter_python_facts.py:_python_language()` (line ~60)
- `python_enrichment.py:_python_language()` (line ~150)

---

## S3. Ripgrep Infrastructure (`rg/`)

### Goal

Consolidate ripgrep subprocess orchestration (rg_native.py), event stream parsing (rg_events.py), and result collection (collector.py) into a unified rg/ package. Convert RgProcessResult from dataclass to CqOutputStruct for serialization consistency.

### Representative Code Snippets

**rg/native.py** (process runner with CqOutputStruct):

```python
from __future__ import annotations

import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.core.structs import CqOutputStruct

if TYPE_CHECKING:
    from collections.abc import Sequence


class RgProcessResult(CqOutputStruct):
    """Result of ripgrep subprocess execution.

    Converted from dataclass to CqOutputStruct for serialization.
    """

    returncode: int
    stdout_lines: list[str]
    stderr: str = ""
    timed_out: bool = False


def run_ripgrep(
    pattern: str,
    paths: Sequence[Path],
    include_glob: str | None = None,
    timeout_sec: float = 30.0,
) -> RgProcessResult:
    """Execute ripgrep subprocess with timeout.

    Parameters
    ----------
    pattern : str
        Pattern to search for.
    paths : Sequence[Path]
        Paths to search.
    include_glob : str | None
        Optional glob pattern to filter files.
    timeout_sec : float
        Subprocess timeout in seconds.

    Returns
    -------
    RgProcessResult
        Process execution result.
    """
    cmd = ["rg", "--json", "--", pattern]
    if include_glob:
        cmd.extend(["--glob", include_glob])
    cmd.extend(str(p) for p in paths)

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            check=False,
        )
        return RgProcessResult(
            returncode=result.returncode,
            stdout_lines=result.stdout.splitlines(),
            stderr=result.stderr,
        )
    except subprocess.TimeoutExpired as exc:
        stderr = exc.stderr.decode("utf-8") if exc.stderr else ""
        return RgProcessResult(
            returncode=-1,
            stdout_lines=[],
            stderr=stderr,
            timed_out=True,
        )
```

**rg/events.py** (event parsing, from rg_events.py):

```python
from __future__ import annotations

import json
from typing import TYPE_CHECKING

from tools.cq.core.structs import CqOutputStruct

if TYPE_CHECKING:
    pass


class RgMatch(CqOutputStruct):
    """Ripgrep match event."""

    path: str
    line_number: int
    column: int
    match_text: str


class RgSummary(CqOutputStruct):
    """Ripgrep summary event."""

    elapsed_total_ms: float
    stats_searches: int
    stats_searches_with_match: int


def parse_rg_event(line: str) -> RgMatch | RgSummary | None:
    """Parse single ripgrep JSON event.

    Parameters
    ----------
    line : str
        JSON event line.

    Returns
    -------
    RgMatch | RgSummary | None
        Parsed event, or None if not a relevant event type.
    """
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        return None

    event_type = event.get("type")

    if event_type == "match":
        data = event.get("data", {})
        path_data = data.get("path", {})
        line_data = data.get("lines", {})
        submatches = data.get("submatches", [])

        if not submatches:
            return None

        first_match = submatches[0]

        return RgMatch(
            path=path_data.get("text", ""),
            line_number=line_data.get("line_number", 0),
            column=first_match.get("start", 0),
            match_text=first_match.get("match", {}).get("text", ""),
        )

    if event_type == "summary":
        data = event.get("data", {})
        elapsed = data.get("elapsed_total", {})
        stats = data.get("stats", {})

        return RgSummary(
            elapsed_total_ms=elapsed.get("secs", 0.0) * 1000
            + elapsed.get("nanos", 0) / 1_000_000,
            stats_searches=stats.get("searches", 0),
            stats_searches_with_match=stats.get("searches_with_match", 0),
        )

    return None
```

### Files to Edit

- None (new package)

### New Files to Create

**Source files:**
- `tools/cq/search/rg/__init__.py` - Package init
- `tools/cq/search/rg/native.py` - ~200 LOC (from rg_native.py, RgProcessResult as CqOutputStruct)
- `tools/cq/search/rg/events.py` - ~250 LOC (from rg_events.py)
- `tools/cq/search/rg/collector.py` - ~200 LOC (from collector.py)

**Test files:**
- `tests/unit/cq/search/rg/test_native.py` - Subprocess execution tests
- `tests/unit/cq/search/rg/test_events.py` - Event parsing tests
- `tests/unit/cq/search/rg/test_collector.py` - Result collection tests

### Legacy Decommission/Delete Scope

**Files to delete** (replaced by rg/):
- `rg_native.py` (~190 LOC, move to rg/native.py)
- `rg_events.py` (~241 LOC, move to rg/events.py)
- `collector.py` (~193 LOC, move to rg/collector.py)

**Data structure migration**:
- Convert `RgProcessResult` from `@dataclass` to `CqOutputStruct` in rg/native.py

---

## S4. Rust-Specific (`rust/`)

### Goal

Consolidate all Rust-specific code analysis (tree-sitter feature extraction, ast-grep enrichment, macro expansion, module graph) into a unified rust/ package. Split tree_sitter_rust.py (1765 LOC) into 3 focused files. Split rust_enrichment.py (691 LOC) into 2 files. Merge scattered Rust contracts into rust/contracts.py.

### Representative Code Snippets

**rust/tree_sitter_features.py** (feature extraction from tree_sitter_rust.py lines 460-907):

```python
from __future__ import annotations

from typing import TYPE_CHECKING

from tools.cq.search._helpers.node_text import node_text, node_text_optional
from tools.cq.search.ts.language_registry import rust_language

if TYPE_CHECKING:
    from tree_sitter import Node


def extract_function_signature(node: Node, source_bytes: bytes) -> dict[str, object]:
    """Extract function signature features from Rust function_item node.

    Parameters
    ----------
    node : Node
        function_item node.
    source_bytes : bytes
        Source file bytes.

    Returns
    -------
    dict[str, object]
        Extracted features (name, params, return_type, modifiers).
    """
    name_node = node.child_by_field_name("name")
    params_node = node.child_by_field_name("parameters")
    return_node = node.child_by_field_name("return_type")

    modifiers = []
    for child in node.children:
        if child.type in ("visibility_modifier", "async", "unsafe", "const", "extern"):
            modifiers.append(node_text(child, source_bytes))

    return {
        "name": node_text_optional(name_node, source_bytes),
        "parameters": node_text_optional(params_node, source_bytes),
        "return_type": node_text_optional(return_node, source_bytes),
        "modifiers": modifiers,
    }


def extract_impl_block_features(node: Node, source_bytes: bytes) -> dict[str, object]:
    """Extract impl block features.

    Parameters
    ----------
    node : Node
        impl_item node.
    source_bytes : bytes
        Source file bytes.

    Returns
    -------
    dict[str, object]
        Extracted features (type, trait, methods).
    """
    type_node = node.child_by_field_name("type")
    trait_node = node.child_by_field_name("trait")
    body_node = node.child_by_field_name("body")

    methods = []
    if body_node:
        for child in body_node.children:
            if child.type == "function_item":
                name_node = child.child_by_field_name("name")
                if name_node:
                    methods.append(node_text(name_node, source_bytes))

    return {
        "impl_type": node_text_optional(type_node, source_bytes),
        "trait": node_text_optional(trait_node, source_bytes),
        "methods": methods,
    }
```

**rust/enrichment.py** (ast-grep enrichment from rust_enrichment.py):

```python
from __future__ import annotations

from typing import TYPE_CHECKING

from tools.cq.search._helpers.errors import try_extract_dict
from tools.cq.search._helpers.node_text import sg_node_text

if TYPE_CHECKING:
    from ast_grep_py import SgRoot


def enrich_rust_definition(sg_root: SgRoot, line: int, col: int) -> dict[str, object]:
    """Enrich Rust definition site with ast-grep.

    Parameters
    ----------
    sg_root : SgRoot
        ast-grep root for Rust file.
    line : int
        1-indexed line number.
    col : int
        0-indexed column.

    Returns
    -------
    dict[str, object]
        Enrichment payload (signature, docstring, attributes).
    """
    node = sg_root.root().find(line=line, column=col)
    if node is None:
        return {}

    payload = {}

    # Extract function signature
    if node.kind() == "function_item":
        name = node.field("name")
        params = node.field("parameters")
        ret = node.field("return_type")

        payload["signature"] = {
            "name": sg_node_text(name),
            "parameters": sg_node_text(params),
            "return_type": sg_node_text(ret),
        }

    # Extract struct signature
    elif node.kind() == "struct_item":
        name = node.field("name")
        payload["signature"] = {
            "name": sg_node_text(name),
            "kind": "struct",
        }

    # Extract doc comments
    doc_lines = []
    prev = node.prev()
    while prev and prev.kind() == "line_comment":
        text = sg_node_text(prev)
        if text and text.startswith("///"):
            doc_lines.insert(0, text[3:].strip())
        prev = prev.prev()

    if doc_lines:
        payload["docstring"] = "\n".join(doc_lines)

    return payload
```

**rust/contracts.py** (merged Rust contracts):

```python
from __future__ import annotations

from tools.cq.core.structs import CqOutputStruct


class RustMacroExpansion(CqOutputStruct):
    """Rust macro expansion result."""

    macro_name: str
    invocation_span: tuple[int, int]
    expanded_text: str
    expansion_error: str | None = None


class RustModuleGraphNode(CqOutputStruct):
    """Node in Rust module graph."""

    module_path: str
    file_path: str | None
    is_inline: bool
    children: list[str]


class RustEnrichmentPayload(CqOutputStruct):
    """Rust enrichment result."""

    signature: dict[str, object]
    docstring: str | None = None
    attributes: list[str]
    macro_expansions: list[RustMacroExpansion]
```

### Files to Edit

- None (new package)

### New Files to Create

**Source files:**
- `tools/cq/search/rust/__init__.py` - Package init
- `tools/cq/search/rust/contracts.py` - ~150 LOC (merge rust_macro_expansion_contracts + rust_module_graph_contracts)
- `tools/cq/search/rust/tree_sitter_features.py` - ~400 LOC (from tree_sitter_rust.py lines 460-907)
- `tools/cq/search/rust/tree_sitter_payload.py` - ~350 LOC (from tree_sitter_rust.py lines 907-1617)
- `tools/cq/search/rust/tree_sitter_queries.py` - ~300 LOC (from tree_sitter_rust.py lines 236-460 + 1617-1765)
- `tools/cq/search/rust/enrichment.py` - ~450 LOC (from rust_enrichment.py primary logic)
- `tools/cq/search/rust/enrichment_payload.py` - ~250 LOC (from rust_enrichment.py payload assembly)
- `tools/cq/search/rust/bundle.py` - ~150 LOC (from tree_sitter_rust_bundle.py)

**Test files:**
- `tests/unit/cq/search/rust/test_contracts.py` - Round-trip tests
- `tests/unit/cq/search/rust/test_tree_sitter_features.py` - Feature extraction tests
- `tests/unit/cq/search/rust/test_tree_sitter_payload.py` - Payload building tests
- `tests/unit/cq/search/rust/test_tree_sitter_queries.py` - Query execution tests
- `tests/unit/cq/search/rust/test_enrichment.py` - ast-grep enrichment tests
- `tests/unit/cq/search/rust/test_enrichment_payload.py` - Payload assembly tests
- `tests/unit/cq/search/rust/test_bundle.py` - Grammar bundle tests

### Legacy Decommission/Delete Scope

**Files to delete** (replaced by rust/):
- `tree_sitter_rust.py` (1765 LOC, split into 3 files)
- `rust_enrichment.py` (691 LOC, split into 2 files)
- `tree_sitter_rust_bundle.py` (146 LOC, move to rust/bundle.py)
- `rust_macro_expansion_bridge.py` (absorb into rust/enrichment.py)
- `rust_module_graph.py` (absorb into rust/enrichment.py)
- `rust_macro_expansion_contracts.py` (merge into rust/contracts.py)
- `rust_module_graph_contracts.py` (merge into rust/contracts.py)

---

## S5. Python-Specific (`python/`)

### Goal

Consolidate all Python-specific code analysis (ast-grep enrichment, Python AST enrichment, LibCST enrichment, tree-sitter enrichment, native resolution, symtable analysis, semantic planes) into a unified python/ package. Split python_enrichment.py (2251 LOC) into 3 focused files for the 5-stage enrichment pipeline. Split tree_sitter_python.py (807 LOC) into tree-sitter-specific enrichment. Split python_native_resolution.py (823 LOC) into focused resolution module.

### Representative Code Snippets

**python/enrichment_astgrep.py** (ast-grep tier from python_enrichment.py):

```python
from __future__ import annotations

from typing import TYPE_CHECKING

from tools.cq.search._helpers.errors import try_extract_dict
from tools.cq.search._helpers.node_text import sg_node_text

if TYPE_CHECKING:
    from ast_grep_py import SgRoot


def enrich_tier_astgrep(
    sg_root: SgRoot,
    line: int,
    col: int,
) -> tuple[dict[str, object], str | None]:
    """Extract ast-grep tier enrichment.

    Parameters
    ----------
    sg_root : SgRoot
        ast-grep root.
    line : int
        1-indexed line number.
    col : int
        0-indexed column.

    Returns
    -------
    tuple[dict[str, object], str | None]
        (payload, error) pair.
    """
    return try_extract_dict("astgrep", _extract_astgrep_payload, sg_root, line, col)


def _extract_astgrep_payload(
    sg_root: SgRoot,
    line: int,
    col: int,
) -> dict[str, object]:
    """Extract ast-grep payload internal implementation."""
    node = sg_root.root().find(line=line, column=col)
    if node is None:
        return {}

    payload: dict[str, object] = {
        "kind": node.kind(),
    }

    # Extract function/class signature
    if node.kind() in ("function_definition", "async_function_definition"):
        name = node.field("name")
        params = node.field("parameters")
        ret = node.field("return_type")

        payload["signature"] = {
            "name": sg_node_text(name),
            "parameters": sg_node_text(params),
            "return_type": sg_node_text(ret),
        }

    elif node.kind() == "class_definition":
        name = node.field("name")
        bases = node.field("superclasses")

        payload["signature"] = {
            "name": sg_node_text(name),
            "bases": sg_node_text(bases),
        }

    # Extract decorators
    decorators = []
    prev = node.prev()
    while prev and prev.kind() == "decorator":
        decorators.insert(0, sg_node_text(prev))
        prev = prev.prev()

    if decorators:
        payload["decorators"] = decorators

    return payload
```

**python/enrichment_ast.py** (Python AST tier from python_enrichment.py):

```python
from __future__ import annotations

import ast
from typing import TYPE_CHECKING

from tools.cq.search._helpers.byte_offsets import line_col_to_byte_offset
from tools.cq.search._helpers.errors import try_extract_dict

if TYPE_CHECKING:
    pass


def enrich_tier_python_ast(
    tree: ast.Module,
    source_bytes: bytes,
    line: int,
    col: int,
) -> tuple[dict[str, object], str | None]:
    """Extract Python AST tier enrichment.

    Parameters
    ----------
    tree : ast.Module
        Python AST tree.
    source_bytes : bytes
        Source file bytes.
    line : int
        1-indexed line number.
    col : int
        0-indexed column.

    Returns
    -------
    tuple[dict[str, object], str | None]
        (payload, error) pair.
    """
    return try_extract_dict(
        "python_ast", _extract_python_ast_payload, tree, source_bytes, line, col
    )


def _extract_python_ast_payload(
    tree: ast.Module,
    source_bytes: bytes,
    line: int,
    col: int,
) -> dict[str, object]:
    """Extract Python AST payload internal implementation."""
    target_offset = line_col_to_byte_offset(source_bytes, line, col)
    if target_offset is None:
        return {}

    # Find containing node by byte offset
    for node in ast.walk(tree):
        if not hasattr(node, "lineno"):
            continue

        node_start = line_col_to_byte_offset(source_bytes, node.lineno, node.col_offset)
        if node_start is None:
            continue

        node_end = node_start + 100  # Approximate

        if node_start <= target_offset < node_end:
            return _extract_ast_node_payload(node)

    return {}


def _extract_ast_node_payload(node: ast.AST) -> dict[str, object]:
    """Extract payload from AST node."""
    payload: dict[str, object] = {
        "ast_type": type(node).__name__,
    }

    if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
        payload["function_name"] = node.name
        payload["arg_count"] = len(node.args.args)
        payload["decorator_count"] = len(node.decorator_list)

    elif isinstance(node, ast.ClassDef):
        payload["class_name"] = node.name
        payload["base_count"] = len(node.bases)

    elif isinstance(node, ast.Import | ast.ImportFrom):
        payload["import_names"] = [alias.name for alias in node.names]

    return payload
```

**python/enrichment_pipeline.py** (5-stage orchestration from python_enrichment.py):

```python
from __future__ import annotations

import ast
from typing import TYPE_CHECKING

from tools.cq.search._helpers.source_hash import source_hash
from tools.cq.search.python.enrichment_astgrep import enrich_tier_astgrep
from tools.cq.search.python.enrichment_ast import enrich_tier_python_ast

if TYPE_CHECKING:
    from ast_grep_py import SgRoot


class PythonEnrichmentSettings:
    """Settings for Python enrichment pipeline (serializable).

    Uses plain class instead of CqSettingsStruct to avoid premature
    formalization. Will convert when settings stabilize.
    """

    def __init__(
        self,
        enable_astgrep: bool = True,
        enable_python_ast: bool = True,
        enable_import_detail: bool = True,
        enable_libcst: bool = True,
        enable_tree_sitter: bool = True,
    ) -> None:
        self.enable_astgrep = enable_astgrep
        self.enable_python_ast = enable_python_ast
        self.enable_import_detail = enable_import_detail
        self.enable_libcst = enable_libcst
        self.enable_tree_sitter = enable_tree_sitter


class PythonEnrichmentState:
    """Runtime state for Python enrichment (non-serializable).

    Contains runtime handles (SgRoot, ast.Module) that cannot be serialized.
    """

    def __init__(
        self,
        source_bytes: bytes,
        sg_root: SgRoot | None,
        ast_tree: ast.Module | None,
    ) -> None:
        self.source_bytes = source_bytes
        self.source_hash = source_hash(source_bytes)
        self.sg_root = sg_root
        self.ast_tree = ast_tree
        self.errors: list[str] = []


def run_python_enrichment_pipeline(
    state: PythonEnrichmentState,
    settings: PythonEnrichmentSettings,
    line: int,
    col: int,
) -> dict[str, object]:
    """Run 5-stage Python enrichment pipeline.

    Parameters
    ----------
    state : PythonEnrichmentState
        Runtime state with parsed handles.
    settings : PythonEnrichmentSettings
        Pipeline configuration.
    line : int
        1-indexed line number.
    col : int
        0-indexed column.

    Returns
    -------
    dict[str, object]
        Merged enrichment payload.
    """
    payload: dict[str, object] = {
        "source_hash": state.source_hash,
        "stages": [],
    }

    # Stage 1: ast-grep
    if settings.enable_astgrep and state.sg_root:
        stage_payload, error = enrich_tier_astgrep(state.sg_root, line, col)
        payload["astgrep"] = stage_payload
        payload["stages"].append("astgrep")
        if error:
            state.errors.append(error)

    # Stage 2: Python AST
    if settings.enable_python_ast and state.ast_tree:
        stage_payload, error = enrich_tier_python_ast(
            state.ast_tree, state.source_bytes, line, col
        )
        payload["python_ast"] = stage_payload
        payload["stages"].append("python_ast")
        if error:
            state.errors.append(error)

    # Stage 3-5: import_detail, libcst, tree_sitter (stubs for now)
    # Full implementation would follow same pattern

    if state.errors:
        payload["errors"] = state.errors

    return payload
```

### Files to Edit

- None (new package)

### New Files to Create

**Source files:**
- `tools/cq/search/python/__init__.py` - Package init
- `tools/cq/search/python/enrichment_astgrep.py` - ~600 LOC (ast-grep tier from python_enrichment.py)
- `tools/cq/search/python/enrichment_ast.py` - ~350 LOC (Python AST tier from python_enrichment.py)
- `tools/cq/search/python/enrichment_pipeline.py` - ~500 LOC (5-stage orchestration + state management)
- `tools/cq/search/python/native_resolution.py` - ~400 LOC (from python_native_resolution.py, keep dataclass for AST handles)
- `tools/cq/search/python/analysis_session.py` - ~350 LOC (from python_analysis_session.py, keep dataclass for runtime state)
- `tools/cq/search/python/tree_sitter_facts.py` - ~600 LOC (from tree_sitter_python_facts.py)
- `tools/cq/search/python/tree_sitter_enrichment.py` - ~400 LOC (tree-sitter gap-fill from tree_sitter_python.py)
- `tools/cq/search/python/semantic_planes.py` - ~300 LOC (merge semantic_signal + semantic_planes_static)

**Test files:**
- `tests/unit/cq/search/python/test_enrichment_astgrep.py` - ast-grep tier tests
- `tests/unit/cq/search/python/test_enrichment_ast.py` - Python AST tier tests
- `tests/unit/cq/search/python/test_enrichment_pipeline.py` - Pipeline orchestration tests
- `tests/unit/cq/search/python/test_native_resolution.py` - Native resolution tests
- `tests/unit/cq/search/python/test_analysis_session.py` - Analysis session tests
- `tests/unit/cq/search/python/test_tree_sitter_facts.py` - Fact extraction tests
- `tests/unit/cq/search/python/test_tree_sitter_enrichment.py` - Tree-sitter enrichment tests
- `tests/unit/cq/search/python/test_semantic_planes.py` - Semantic planes tests

### Legacy Decommission/Delete Scope

**Files to delete** (replaced by python/):
- `python_enrichment.py` (2251 LOC, split into 3 files)
- `python_native_resolution.py` (823 LOC, move to python/native_resolution.py)
- `python_analysis_session.py` (338 LOC, move to python/analysis_session.py)
- `tree_sitter_python_facts.py` (585 LOC, move to python/tree_sitter_facts.py)
- `tree_sitter_python.py` (807 LOC, move to python/tree_sitter_enrichment.py)
- `semantic_signal.py` (merge into python/semantic_planes.py)
- `semantic_planes_static.py` (merge into python/semantic_planes.py)

**Data structure notes**:
- Keep `_AstAnchor`, `_DefinitionSite` in python/native_resolution.py as dataclass (contain ast.AST handles)
- Keep `AstSpanEntry`, `PythonAnalysisSession` in python/analysis_session.py as dataclass (contain ast.Module handles)
- Split `_PythonEnrichmentState` into `PythonEnrichmentSettings` (will formalize later) + `PythonEnrichmentState` (runtime-only class)

---

## S6. Pipeline Orchestration (`pipeline/`)

### Goal

Consolidate all search pipeline orchestration (smart search, classification, partitioning, language dispatch) into a unified pipeline/ package. Split smart_search.py (3720 LOC) into 7 focused files representing the search pipeline stages. Split classifier.py (1047 LOC) into core classification engine and node indexing. Move supporting files (partition_pipeline, language_front_door_pipeline, adapter) into pipeline/.

### Representative Code Snippets

**pipeline/smart_search_core.py** (entry point + context building):

```python
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.search._helpers.constants import MAX_MATCH_LIMIT

if TYPE_CHECKING:
    from tools.cq.search.contracts import SearchSummary


class SearchConfig:
    """Search configuration (will convert to CqSettingsStruct when stable)."""

    def __init__(
        self,
        query: str,
        root: Path,
        include_glob: str | None = None,
        max_matches: int = MAX_MATCH_LIMIT,
        enable_enrichment: bool = True,
        enable_classification: bool = True,
    ) -> None:
        self.query = query
        self.root = root
        self.include_glob = include_glob
        self.max_matches = max_matches
        self.enable_enrichment = enable_enrichment
        self.enable_classification = enable_classification


class SmartSearchContext:
    """Runtime context for smart search execution."""

    def __init__(self, config: SearchConfig) -> None:
        self.config = config
        self.candidates: list[object] = []
        self.enriched: list[object] = []
        self.classified: list[object] = []
        self.errors: list[str] = []


def smart_search(
    query: str,
    root: Path,
    include_glob: str | None = None,
) -> SearchSummary:
    """Execute smart search pipeline.

    Parameters
    ----------
    query : str
        Search query.
    root : Path
        Repository root.
    include_glob : str | None
        Optional file glob filter.

    Returns
    -------
    SearchSummary
        Search result summary.
    """
    from tools.cq.search.pipeline.smart_search_candidates import run_candidate_phase
    from tools.cq.search.pipeline.smart_search_enrichment import run_enrichment_phase
    from tools.cq.search.pipeline.smart_search_classification import (
        run_classification_phase,
    )
    from tools.cq.search.pipeline.smart_search_output import assemble_output

    config = SearchConfig(query=query, root=root, include_glob=include_glob)
    context = SmartSearchContext(config)

    # Phase 1: Collect candidates
    run_candidate_phase(context)

    # Phase 2: Enrich candidates
    if config.enable_enrichment:
        run_enrichment_phase(context)

    # Phase 3: Classify matches
    if config.enable_classification:
        run_classification_phase(context)

    # Phase 4: Assemble output
    return assemble_output(context)
```

**pipeline/smart_search_candidates.py** (candidate collection phase):

```python
from __future__ import annotations

from typing import TYPE_CHECKING

from tools.cq.search.rg.native import run_ripgrep

if TYPE_CHECKING:
    from tools.cq.search.pipeline.smart_search_core import SmartSearchContext


def run_candidate_phase(context: SmartSearchContext) -> None:
    """Collect candidate matches via ripgrep.

    Parameters
    ----------
    context : SmartSearchContext
        Search context to populate with candidates.
    """
    config = context.config

    # Run ripgrep
    rg_result = run_ripgrep(
        pattern=config.query,
        paths=[config.root],
        include_glob=config.include_glob,
    )

    # Parse events (simplified)
    # Full implementation would parse rg events and build candidates
    context.candidates = []  # Stub
```

**pipeline/classifier_core.py** (classification engine from classifier.py):

```python
from __future__ import annotations

from typing import TYPE_CHECKING

from tools.cq.core.structs import CqOutputStruct

if TYPE_CHECKING:
    pass


class NodeClassification(CqOutputStruct):
    """Classification of a code node.

    Converted from msgspec.Struct to CqOutputStruct.
    """

    kind: str  # definition, callsite, import, reference, etc.
    confidence: float
    heuristics: list[str]


class HeuristicResult(CqOutputStruct):
    """Result of classification heuristic.

    Converted from msgspec.Struct to CqOutputStruct.
    """

    heuristic_name: str
    matched: bool
    confidence_boost: float
    evidence: dict[str, object]


def classify_node(
    node_type: str,
    context: dict[str, object],
) -> NodeClassification:
    """Classify a code node.

    Parameters
    ----------
    node_type : str
        AST node type.
    context : dict[str, object]
        Classification context.

    Returns
    -------
    NodeClassification
        Classification result.
    """
    # Simplified classification logic
    if "def" in node_type:
        kind = "definition"
        confidence = 0.9
    elif "call" in node_type:
        kind = "callsite"
        confidence = 0.8
    else:
        kind = "reference"
        confidence = 0.5

    return NodeClassification(
        kind=kind,
        confidence=confidence,
        heuristics=["node_type_heuristic"],
    )
```

### Files to Edit

- None (new package)

### New Files to Create

**Source files:**
- `tools/cq/search/pipeline/__init__.py` - Package init
- `tools/cq/search/pipeline/smart_search_core.py` - ~400 LOC (entry point, SearchConfig, SmartSearchContext)
- `tools/cq/search/pipeline/smart_search_candidates.py` - ~400 LOC (candidate collection phase)
- `tools/cq/search/pipeline/smart_search_enrichment.py` - ~400 LOC (enrichment orchestration)
- `tools/cq/search/pipeline/smart_search_classification.py` - ~400 LOC (classification + output assembly)
- `tools/cq/search/pipeline/smart_search_sections.py` - ~400 LOC (section/report building)
- `tools/cq/search/pipeline/smart_search_insight.py` - ~500 LOC (insight card + neighborhood)
- `tools/cq/search/pipeline/smart_search_output.py` - ~500 LOC (final assembly + telemetry)
- `tools/cq/search/pipeline/classifier_core.py` - ~500 LOC (classification engine from classifier.py)
- `tools/cq/search/pipeline/classifier_index.py` - ~350 LOC (node indexing + symtable from classifier.py)
- `tools/cq/search/pipeline/partition.py` - ~550 LOC (from partition_pipeline.py)
- `tools/cq/search/pipeline/language_front_door.py` - ~550 LOC (from language_front_door_pipeline.py + language_front_door_adapter.py)
- `tools/cq/search/pipeline/adapter.py` - ~250 LOC (from adapter.py)

**Test files:**
- `tests/unit/cq/search/pipeline/test_smart_search_core.py` - Entry point tests
- `tests/unit/cq/search/pipeline/test_smart_search_candidates.py` - Candidate phase tests
- `tests/unit/cq/search/pipeline/test_smart_search_enrichment.py` - Enrichment phase tests
- `tests/unit/cq/search/pipeline/test_smart_search_classification.py` - Classification tests
- `tests/unit/cq/search/pipeline/test_smart_search_sections.py` - Section building tests
- `tests/unit/cq/search/pipeline/test_smart_search_insight.py` - Insight card tests
- `tests/unit/cq/search/pipeline/test_smart_search_output.py` - Output assembly tests
- `tests/unit/cq/search/pipeline/test_classifier_core.py` - Classification engine tests
- `tests/unit/cq/search/pipeline/test_classifier_index.py` - Indexing tests
- `tests/unit/cq/search/pipeline/test_partition.py` - Partition tests
- `tests/unit/cq/search/pipeline/test_language_front_door.py` - Language dispatch tests
- `tests/unit/cq/search/pipeline/test_adapter.py` - Adapter tests

### Legacy Decommission/Delete Scope

**Files to delete** (replaced by pipeline/):
- `smart_search.py` (3720 LOC, split into 7 files)
- `classifier.py` (1047 LOC, split into 2 files)
- `partition_pipeline.py` (550 LOC, move to pipeline/partition.py)
- `language_front_door_pipeline.py` (509 LOC, move to pipeline/language_front_door.py)
- `language_front_door_adapter.py` (absorb into pipeline/language_front_door.py)
- `adapter.py` (244 LOC, move to pipeline/adapter.py)

**Data structure migrations**:
- Convert `HeuristicResult` from `msgspec.Struct` to `CqOutputStruct` in pipeline/classifier_core.py
- Convert `NodeClassification` from `msgspec.Struct` to `CqOutputStruct` in pipeline/classifier_core.py

---

## S7. Resolution (`resolution/`)

### Goal

Consolidate object resolution logic (name resolution, occurrence tracking, section building, fact attachment) into a unified resolution/ package. Split object_resolver.py (686 LOC) into core resolver and name resolution. Merge scattered resolution contracts into resolution/contracts.py.

### Representative Code Snippets

**resolution/object_resolver.py** (core resolution logic):

```python
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.search.resolution.contracts import ObjectResolution

if TYPE_CHECKING:
    pass


def resolve_object(
    identifier: str,
    root: Path,
) -> ObjectResolution:
    """Resolve identifier to object definition.

    Parameters
    ----------
    identifier : str
        Identifier to resolve.
    root : Path
        Repository root.

    Returns
    -------
    ObjectResolution
        Resolution result.
    """
    # Simplified resolution logic
    return ObjectResolution(
        identifier=identifier,
        definition_path=None,
        definition_line=None,
        resolution_confidence=0.0,
    )
```

**resolution/contracts.py** (merged resolution contracts):

```python
from __future__ import annotations

from tools.cq.core.structs import CqOutputStruct


class ObjectResolution(CqOutputStruct):
    """Object resolution result."""

    identifier: str
    definition_path: str | None
    definition_line: int | None
    resolution_confidence: float


class ObjectOccurrence(CqOutputStruct):
    """Object occurrence site."""

    path: str
    line: int
    col: int
    occurrence_kind: str  # definition, reference, import


class ObjectFact(CqOutputStruct):
    """Fact attached to object."""

    fact_type: str
    fact_value: object
    confidence: float
```

### Files to Edit

- None (new package)

### New Files to Create

**Source files:**
- `tools/cq/search/resolution/__init__.py` - Package init
- `tools/cq/search/resolution/contracts.py` - ~150 LOC (merge object_resolution_contracts + object_occurrence_contracts)
- `tools/cq/search/resolution/object_resolver.py` - ~400 LOC (core resolution from object_resolver.py)
- `tools/cq/search/resolution/object_names.py` - ~250 LOC (name resolution from object_resolver.py)
- `tools/cq/search/resolution/object_sections.py` - ~270 LOC (from object_sections.py)

**Test files:**
- `tests/unit/cq/search/resolution/test_contracts.py` - Round-trip tests
- `tests/unit/cq/search/resolution/test_object_resolver.py` - Resolution tests
- `tests/unit/cq/search/resolution/test_object_names.py` - Name resolution tests
- `tests/unit/cq/search/resolution/test_object_sections.py` - Section building tests

### Legacy Decommission/Delete Scope

**Files to delete** (replaced by resolution/):
- `object_resolver.py` (686 LOC, split into 2 files)
- `object_sections.py` (262 LOC, move to resolution/object_sections.py)
- `object_fact_attachment.py` (absorb into resolution/object_resolver.py)
- `object_resolution_contracts.py` (merge into resolution/contracts.py)
- `object_occurrence_contracts.py` (merge into resolution/contracts.py)

---

## S8. Remaining Root Consolidation

### Goal

Consolidate remaining root-level files that don't fit into the 6 primary domain packages. Merge small scattered files (models, profiles, requests, context, section_builder, runtime boundary) into appropriate existing modules or create focused modules in pipeline/.

### Representative Code Snippets

**Root contracts.py** (merge models + profiles + requests):

```python
from __future__ import annotations

from tools.cq.core.structs import CqOutputStruct, CqSettingsStruct


# From models.py
class SearchMode(CqSettingsStruct):
    """Search mode configuration."""

    mode: str  # identifier, regex, literal
    case_sensitive: bool = False


# From profiles.py
class SearchProfile(CqSettingsStruct):
    """Search execution profile."""

    enable_python: bool = True
    enable_rust: bool = True
    max_workers: int = 4


# From requests.py
class SearchRequest(CqSettingsStruct):
    """Search request parameters."""

    query: str
    root_path: str
    include_glob: str | None = None
    mode: SearchMode
    profile: SearchProfile
```

**pipeline/context.py** (merge context + context_window):

```python
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


class SearchContext:
    """Search execution context (runtime state)."""

    def __init__(self, root_path: str) -> None:
        self.root_path = root_path
        self.file_cache: dict[str, bytes] = {}
        self.parse_cache: dict[str, object] = {}


class ContextWindow:
    """Context window for progressive disclosure."""

    def __init__(self, max_size: int = 1000) -> None:
        self.max_size = max_size
        self.current_size = 0
```

### Files to Edit

- `tools/cq/search/contracts.py` - Merge models.py + profiles.py + requests.py contents

### New Files to Create

**Source files:**
- `tools/cq/search/pipeline/context.py` - ~250 LOC (merge context.py + context_window.py)
- `tools/cq/search/pipeline/sections.py` - ~300 LOC (merge section_builder.py + related logic)
- `tools/cq/search/pipeline/language_root_resolution.py` - ~200 LOC (from language_root_resolution.py)

**Test files:**
- `tests/unit/cq/search/pipeline/test_context.py` - Context tests
- `tests/unit/cq/search/pipeline/test_sections.py` - Section building tests
- `tests/unit/cq/search/pipeline/test_language_root_resolution.py` - Language root tests

### Legacy Decommission/Delete Scope

**Files to delete:**
- `models.py` (merge into contracts.py)
- `profiles.py` (merge into contracts.py)
- `requests.py` (merge into contracts.py)
- `context.py` (move to pipeline/context.py)
- `context_window.py` (merge into pipeline/context.py)
- `section_builder.py` (merge into pipeline/sections.py)
- `language_root_resolution.py` (move to pipeline/language_root_resolution.py)
- `contracts_runtime_boundary.py` (merge RuntimeBoundarySummary into enrichment/core.py as CqStruct)
- `timeout.py` (merge timeout helpers into _helpers/errors.py)
- `semantic_request_budget.py` (absorb into pipeline/smart_search_enrichment.py)
- `semantic_contract_state.py` (absorb into pipeline/smart_search_enrichment.py)
- `candidate_normalizer.py` (absorb into pipeline/smart_search_candidates.py)

---

## S9. Data Object Improvements

### Goal

Systematically fix data object inheritance patterns across the restructured codebase. Convert incorrectly-typed structs to use appropriate CqStruct base classes. Add msgspec performance optimizations (gc=False, cache_hash=True) where applicable. Split mixed runtime/serialization state into separate objects.

### Representative Code Snippets

**High-volume struct with gc=False** (in ts/contracts.py):

```python
from __future__ import annotations

from tools.cq.core.structs import CqOutputStruct


class QueryMatch(CqOutputStruct, gc=False):
    """Single query match result.

    Uses gc=False because matches are high-volume, short-lived objects.
    """

    pattern_index: int
    captures: dict[str, list[tuple[int, int]]]
```

**Frozen struct used as dict key with cache_hash** (in pipeline/classifier_core.py):

```python
from __future__ import annotations

from tools.cq.core.structs import CqOutputStruct


class ClassificationKey(CqOutputStruct, cache_hash=True):
    """Key for classification cache.

    Uses cache_hash=True because used as dict key in hot path.
    """

    node_type: str
    file_hash: str
```

**msgspec.convert for dataclass bridging** (_helpers/serialization.py):

```python
from __future__ import annotations

import msgspec
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.core.structs import CqOutputStruct


def dataclass_to_struct(obj: object, target_type: type[CqOutputStruct]) -> CqOutputStruct:
    """Convert dataclass to CqOutputStruct using msgspec.convert.

    Parameters
    ----------
    obj : object
        Dataclass instance.
    target_type : type[CqOutputStruct]
        Target struct type.

    Returns
    -------
    CqOutputStruct
        Converted struct instance.
    """
    return msgspec.convert(obj, target_type, from_attributes=True)
```

**Reusable encoder/decoder** (_helpers/serialization.py):

```python
from __future__ import annotations

import msgspec

# Reusable singleton encoders/decoders for hot paths
_JSON_ENCODER = msgspec.json.Encoder()
_JSON_DECODER = msgspec.json.Decoder()


def encode_json(obj: object) -> bytes:
    """Encode object to JSON using reusable encoder."""
    return _JSON_ENCODER.encode(obj)


def decode_json(data: bytes, target_type: type) -> object:
    """Decode JSON using reusable decoder."""
    return _JSON_DECODER.decode(data, type=target_type)
```

### Files to Edit

**Convert struct types:**
- `tools/cq/search/pipeline/classifier_core.py` - Convert HeuristicResult, NodeClassification from msgspec.Struct to CqOutputStruct
- `tools/cq/search/rg/native.py` - Convert RgProcessResult from dataclass to CqOutputStruct
- `tools/cq/search/enrichment/core.py` - Convert RuntimeBoundarySummary from msgspec.Struct to CqStruct

**Add gc=False:**
- `tools/cq/search/ts/contracts.py` - Add gc=False to QueryMatch, ParseResult (high-volume)

**Add cache_hash=True:**
- `tools/cq/search/pipeline/classifier_core.py` - Add cache_hash=True to classification key structs

**Split mixed state:**
- `tools/cq/search/python/enrichment_pipeline.py` - Split PythonEnrichmentState into settings (plain class for now) + runtime state (plain class)

### New Files to Create

**Source files:**
- `tools/cq/search/_helpers/serialization.py` - ~100 LOC (reusable encoders, dataclass_to_struct)

**Test files:**
- `tests/unit/cq/search/_helpers/test_serialization.py` - Serialization helper tests

### Legacy Decommission/Delete Scope

**Pattern changes** (no file deletions, just type changes):
- Change `HeuristicResult(msgspec.Struct, ...)` → `HeuristicResult(CqOutputStruct, ...)`
- Change `NodeClassification(msgspec.Struct, ...)` → `NodeClassification(CqOutputStruct, ...)`
- Change `@dataclass class RgProcessResult` → `class RgProcessResult(CqOutputStruct)`
- Change `RuntimeBoundarySummary(msgspec.Struct, ...)` → `RuntimeBoundarySummary(CqStruct, ...)`
- Split `_PythonEnrichmentState` payload dict into separate settings class + runtime state class

---

## S10. Library Capability Adoption

### Goal

Adopt unused library capabilities to improve performance and code quality. Add TreeCursor-based traversal for efficient parent walks. Use Language.id_for_node_kind() for fast node type comparisons. Adopt ast-grep Config-based search with constraints. Use msgspec.convert for dataclass bridging where needed.

### Representative Code Snippets

**TreeCursor-based parent traversal** (_helpers/tree_cursor.py):

```python
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tree_sitter import Node, TreeCursor


def find_parent_of_type(cursor: TreeCursor, node_type: str) -> Node | None:
    """Find parent node of given type using TreeCursor.

    TreeCursor provides O(1) parent traversal vs tree walking.

    Parameters
    ----------
    cursor : TreeCursor
        Tree cursor positioned at starting node.
    node_type : str
        Target parent node type.

    Returns
    -------
    Node | None
        Parent node, or None if not found.
    """
    while cursor.goto_parent():
        if cursor.node.type == node_type:
            return cursor.node
    return None


def collect_ancestors(cursor: TreeCursor, max_depth: int = 100) -> list[Node]:
    """Collect ancestor nodes using TreeCursor.

    Parameters
    ----------
    cursor : TreeCursor
        Tree cursor positioned at starting node.
    max_depth : int
        Maximum ancestor depth.

    Returns
    -------
    list[Node]
        Ancestor nodes from parent to root.
    """
    ancestors = []
    depth = 0
    while cursor.goto_parent() and depth < max_depth:
        ancestors.append(cursor.node)
        depth += 1
    return ancestors
```

**ast-grep Config-based search** (python/enrichment_astgrep.py):

```python
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ast_grep_py import SgRoot

try:
    from ast_grep_py import Config

    _HAS_ASTGREP_CONFIG = True
except ImportError:
    _HAS_ASTGREP_CONFIG = False


def search_with_constraints(
    sg_root: SgRoot,
    pattern: str,
    inside: str | None = None,
) -> list[object]:
    """Search using ast-grep Config with constraints.

    Parameters
    ----------
    sg_root : SgRoot
        ast-grep root.
    pattern : str
        Search pattern.
    inside : str | None
        Optional context constraint.

    Returns
    -------
    list[object]
        Match results.
    """
    if not _HAS_ASTGREP_CONFIG:
        # Fallback to basic search
        return []

    config_dict = {
        "rule": {
            "pattern": pattern,
        }
    }

    if inside:
        config_dict["rule"]["inside"] = {"pattern": inside}

    config = Config(config_dict)
    matches = sg_root.root().find_all(config)
    return list(matches)
```

**Language.id_for_node_kind() caching** (ts/language_registry.py, already shown in S2):

```python
@lru_cache(maxsize=128)
def node_kind_id(language: Language, kind_name: str) -> int:
    """Get numeric ID for a node kind (cached).

    Uses Language.id_for_node_kind() for O(1) comparisons.
    """
    return language.id_for_node_kind(kind_name, named=True)
```

### Files to Edit

- `tools/cq/search/ts/language_registry.py` - Add node_kind_id() helper (already in S2)
- `tools/cq/search/python/enrichment_astgrep.py` - Add Config-based search helper
- `tools/cq/search/_helpers/serialization.py` - Add msgspec.convert helper (already in S9)

### New Files to Create

**Source files:**
- `tools/cq/search/_helpers/tree_cursor.py` - ~150 LOC (TreeCursor helpers)

**Test files:**
- `tests/unit/cq/search/_helpers/test_tree_cursor.py` - TreeCursor helper tests

### Legacy Decommission/Delete Scope

**Pattern replacements** (no deletions, just adoption):
- Replace manual parent walks with TreeCursor-based traversal
- Replace string-based node type comparisons with node_kind_id() where applicable
- Use ast-grep Config for complex searches instead of manual pattern matching

---

## S11. Update Imports and Public API

### Goal

Update all imports across tools/cq/search/ and tests/ to use final go-forward paths. No backward-compatible re-exports — all callers import directly from the target module. `tools/cq/search/__init__.py` exports only the go-forward public API with no legacy aliases. Run full quality gate to validate the restructuring.

### Representative Code Snippets

**tools/cq/search/__init__.py** (go-forward public API only, no legacy re-exports):

```python
from __future__ import annotations

# Go-forward public API — all callers import directly from subpackages.
# This __init__.py exists only for package identity; no compatibility shims.
```

**Internal import update example** (pipeline/smart_search_enrichment.py):

```python
from __future__ import annotations

# OLD (before restructuring):
# from tools.cq.search.python_enrichment import enrich_python_match
# from tools.cq.search.rust_enrichment import enrich_rust_match

# NEW (after restructuring):
from tools.cq.search.python.enrichment_pipeline import run_python_enrichment_pipeline
from tools.cq.search.rust.enrichment import enrich_rust_definition
```

**Test import update example** (tests/unit/cq/search/test_smart_search.py):

```python
from __future__ import annotations

# All imports use final go-forward paths — no re-export aliases:
from tools.cq.search.pipeline.smart_search_core import smart_search
```

### Files to Edit

**Update all imports in:**
- `tools/cq/search/*.py` - Update all internal cross-references
- `tools/cq/cli_app/commands/*.py` - Update all imports from tools.cq.search
- `tools/cq/neighborhood/*.py` - Update any search imports
- `tests/unit/cq/search/**/*.py` - Update all test imports
- `tests/e2e/cq/**/*.py` - Update all E2E test imports

**Package init:**
- `tools/cq/search/__init__.py` - Minimal package init (no legacy re-exports)

### New Files to Create

**Migration verification script:**
- `scripts/verify_search_restructuring.py` - ~200 LOC (verify all imports resolve, no missing modules)

### Legacy Decommission/Delete Scope

**Files already deleted in S1-S8:**
- All legacy files are deleted within their owning scope item (no deferred batch)
- Total: ~73 legacy files deleted across S1-S8

**Verification checklist:**
1. All new test files passing
2. All imports resolved (no import errors)
3. Quality gate passing (ruff, pyrefly, pyright, pytest)
4. No references to deleted files in codebase
5. No compatibility shims, re-exports, or deprecated markers remain

---

## Cross-Scope Legacy Deletion Reference

### Files Deleted Per Scope Item

Legacy files are deleted atomically within their owning scope item — not deferred to a later batch. This section is a cross-reference for tracking purposes.

**Tree-sitter contract files** (23 files, 879 LOC total, deleted in S2):
- tree_sitter_runtime_contracts.py
- tree_sitter_parse_session_contracts.py
- tree_sitter_query_contracts.py
- tree_sitter_artifact_contracts.py
- tree_sitter_injection_contracts.py
- tree_sitter_structural_contracts.py
- tree_sitter_node_schema_contracts.py
- tree_sitter_custom_predicate_contracts.py
- tree_sitter_grammar_drift_contracts.py
- tree_sitter_change_windows_contracts.py
- tree_sitter_work_queue_contracts.py
- tree_sitter_query_planner_contracts.py
- tree_sitter_query_specialization_contracts.py
- tree_sitter_adaptive_runtime_contracts.py
- Plus 9 additional minor contract files

**Tree-sitter implementation files** (21 files, deleted in S2):
- tree_sitter_runtime.py
- tree_sitter_parse_session.py
- tree_sitter_parser_controls.py
- tree_sitter_stream_source.py
- tree_sitter_query_registry.py
- query_pack_lint.py
- tree_sitter_node_schema.py
- tree_sitter_node_codegen.py
- tree_sitter_custom_predicates.py
- tree_sitter_grammar_drift.py
- tree_sitter_change_windows.py
- tree_sitter_budgeting.py
- tree_sitter_work_queue.py
- tree_sitter_injections.py
- tree_sitter_injection_runtime.py
- tree_sitter_rust_injection_profiles.py
- tree_sitter_structural_export.py
- tree_sitter_token_export.py
- tree_sitter_match_rows.py
- tree_sitter_query_specialization.py
- tree_sitter_adaptive_runtime.py

**Python/Rust language files** (8 files, deleted in S4/S5):
- python_enrichment.py (2251 LOC)
- python_native_resolution.py (823 LOC)
- python_analysis_session.py (338 LOC)
- tree_sitter_python_facts.py (585 LOC)
- tree_sitter_python.py (807 LOC)
- tree_sitter_rust.py (1765 LOC)
- rust_enrichment.py (691 LOC)
- tree_sitter_rust_bundle.py (146 LOC)

**Pipeline files** (6 files, deleted in S6):
- smart_search.py (3720 LOC)
- classifier.py (1047 LOC)
- partition_pipeline.py (550 LOC)
- language_front_door_pipeline.py (509 LOC)
- language_front_door_adapter.py
- adapter.py (244 LOC)

**Resolution files** (5 files, deleted in S7):
- object_resolver.py (686 LOC)
- object_sections.py (262 LOC)
- object_fact_attachment.py
- object_resolution_contracts.py
- object_occurrence_contracts.py

**Root consolidation files** (10 files, deleted in S8):
- models.py
- profiles.py
- requests.py
- context.py
- context_window.py
- section_builder.py
- language_root_resolution.py
- contracts_runtime_boundary.py
- timeout.py
- semantic_request_budget.py
- semantic_contract_state.py
- candidate_normalizer.py

**Ripgrep files** (3 files, deleted in S3):
- rg_native.py (190 LOC)
- rg_events.py (241 LOC)
- collector.py (193 LOC)

**Total legacy files deleted**: ~73 files representing ~15,000 LOC (deleted atomically within their owning scope items, not deferred).

---

## Implementation Sequence

The restructuring follows a dependency-ordered sequence to minimize disruption:

1. **S1: Shared Helpers Foundation** - Create _helpers/ package first since all other packages depend on it. This eliminates duplication early and provides stable helpers for S2-S7.

2. **S2: Tree-Sitter Infrastructure** - Create ts/ package next since Python/Rust enrichment depend on it. Consolidates 23 contract files and establishes tree-sitter patterns.

3. **S3: Ripgrep Infrastructure** - Create rg/ package for subprocess orchestration. Small, independent, no dependencies on S4-S7.

4. **S4: Rust-Specific** - Create rust/ package after ts/ is stable. Depends on ts/ and _helpers/.

5. **S5: Python-Specific** - Create python/ package after ts/ is stable. Depends on ts/ and _helpers/.

6. **S6: Pipeline Orchestration** - Create pipeline/ package after S4/S5 complete. Depends on python/, rust/, rg/, _helpers/.

7. **S7: Resolution** - Create resolution/ package after S6. Depends on pipeline/.

8. **S8: Remaining Root Consolidation** - Merge small root files after S1-S7 packages exist. Depends on pipeline/.

9. **S9: Data Object Improvements** - Apply msgspec improvements across all packages. Can be done incrementally during S1-S8 or as final pass.

10. **S10: Library Capability Adoption** - Adopt unused library features across packages. Can be done incrementally or as final pass.

11. **S11: Update Imports and Verify** - Final pass to update all remaining imports to go-forward paths, delete all legacy files, and run full quality gate. No re-exports or compatibility shims.

**Rationale**: This sequence builds from leaf dependencies (helpers) to root orchestration (pipeline), ensuring each phase has stable dependencies. Each scope item is a hard cutover — new modules created, imports updated, legacy deleted atomically. No transitional coexistence between old and new paths.

---

## Implementation Checklist

### Phase 1: Foundation (S1-S3)

- [ ] S1: Create _helpers/ package with 4 modules (node_text, byte_offsets, errors, constants)
- [ ] S1: Write tests for all _helpers/ modules
- [ ] S1: Update 12 duplicated function call sites to use _helpers/
- [ ] S2: Create ts/ package with contracts.py (merge 23 files)
- [ ] S2: Create ts/language_registry.py (consolidate 4+ language factories)
- [ ] S2: Create remaining 12 ts/ modules (runtime, parse_session, query_registry, etc.)
- [ ] S2: Write tests for all ts/ modules
- [ ] S2: Update all tree-sitter call sites to use ts/
- [ ] S3: Create rg/ package with 3 modules (native, events, collector)
- [ ] S3: Convert RgProcessResult from dataclass to CqOutputStruct
- [ ] S3: Write tests for all rg/ modules
- [ ] S3: Update all ripgrep call sites to use rg/

### Phase 2: Language-Specific (S4-S5)

- [ ] S4: Create rust/ package with contracts.py
- [ ] S4: Split tree_sitter_rust.py into 3 files (features, payload, queries)
- [ ] S4: Split rust_enrichment.py into 2 files (enrichment, enrichment_payload)
- [ ] S4: Move tree_sitter_rust_bundle.py to rust/bundle.py
- [ ] S4: Write tests for all rust/ modules
- [ ] S4: Update all Rust enrichment call sites to use rust/
- [ ] S5: Create python/ package
- [ ] S5: Split python_enrichment.py into 3 files (astgrep, ast, pipeline)
- [ ] S5: Move python_native_resolution.py to python/native_resolution.py
- [ ] S5: Move python_analysis_session.py to python/analysis_session.py
- [ ] S5: Move tree_sitter_python_facts.py to python/tree_sitter_facts.py
- [ ] S5: Move tree_sitter_python.py to python/tree_sitter_enrichment.py
- [ ] S5: Merge semantic_signal + semantic_planes_static into python/semantic_planes.py
- [ ] S5: Write tests for all python/ modules
- [ ] S5: Update all Python enrichment call sites to use python/

### Phase 3: Orchestration (S6-S8)

- [ ] S6: Create pipeline/ package
- [ ] S6: Split smart_search.py into 7 files (core, candidates, enrichment, classification, sections, insight, output)
- [ ] S6: Split classifier.py into 2 files (classifier_core, classifier_index)
- [ ] S6: Convert HeuristicResult, NodeClassification to CqOutputStruct
- [ ] S6: Move partition_pipeline.py to pipeline/partition.py
- [ ] S6: Move language_front_door_pipeline.py + adapter to pipeline/language_front_door.py
- [ ] S6: Move adapter.py to pipeline/adapter.py
- [ ] S6: Write tests for all pipeline/ modules
- [ ] S6: Update all search pipeline call sites to use pipeline/
- [ ] S7: Create resolution/ package with contracts.py
- [ ] S7: Split object_resolver.py into 2 files (object_resolver, object_names)
- [ ] S7: Move object_sections.py to resolution/object_sections.py
- [ ] S7: Write tests for all resolution/ modules
- [ ] S7: Update all resolution call sites to use resolution/
- [ ] S8: Merge models + profiles + requests into root contracts.py
- [ ] S8: Merge context + context_window into pipeline/context.py
- [ ] S8: Merge section_builder into pipeline/sections.py
- [ ] S8: Move language_root_resolution.py to pipeline/
- [ ] S8: Merge contracts_runtime_boundary into enrichment/core.py
- [ ] S8: Merge timeout into _helpers/errors.py
- [ ] S8: Write tests for all consolidated modules

### Phase 4: Quality Improvements (S9-S10)

- [ ] S9: Create _helpers/serialization.py with reusable encoder/decoder
- [ ] S9: Add gc=False to high-volume structs in ts/contracts.py
- [ ] S9: Add cache_hash=True to key structs in pipeline/classifier_core.py
- [ ] S9: Split PythonEnrichmentState into settings + runtime state
- [ ] S9: Convert RuntimeBoundarySummary to CqStruct
- [ ] S9: Write tests for serialization helpers
- [ ] S10: Create _helpers/tree_cursor.py with TreeCursor helpers
- [ ] S10: Add Config-based search to python/enrichment_astgrep.py
- [ ] S10: Update relevant call sites to use TreeCursor/Config
- [ ] S10: Write tests for new library capability helpers

### Phase 5: Final Verification (S11)

- [ ] S11: Update tools/cq/search/__init__.py (minimal package init, no re-exports)
- [ ] S11: Update all remaining imports in tools/cq/search/*.py to go-forward paths
- [ ] S11: Update all remaining imports in tools/cq/cli_app/commands/*.py
- [ ] S11: Update all remaining imports in tools/cq/neighborhood/*.py
- [ ] S11: Update all remaining imports in tests/unit/cq/search/**/*.py
- [ ] S11: Update all remaining imports in tests/e2e/cq/**/*.py
- [ ] S11: Delete all legacy files (~73 files) — no coexistence with new paths
- [ ] S11: Create scripts/verify_search_restructuring.py
- [ ] S11: Run verification script (all imports resolve, no stale references)
- [ ] S11: Verify no compatibility shims, re-exports, or deprecated markers remain
- [ ] S11: Run full quality gate (ruff format && ruff check --fix && pyrefly check && pyright && pytest -q)
- [ ] S11: Update AGENTS.md and CLAUDE.md if needed


