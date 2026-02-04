# cq Smart Search — Comprehensive Design Document

## Executive Summary

This document specifies the implementation of a "Smart Search" feature for the cq tool that displaces plain ripgrep usage by providing semantically-enriched, grouped results. The design leverages:

- **rpygrep** for high-performance candidate generation with typed results
- **ast-grep-py** for AST-level classification and context extraction
- **symtable** for compile-time scope analysis and binding classification
- **msgspec** for schema-validated, high-performance serialization

The feature uses a three-phase pipeline: (1) rpygrep candidate generation, (2) ast-grep-py + symtable classification/enrichment, (3) result assembly with grouping and ranking.

---

## 0. Integration Constraints (Must Match Current cq Architecture)

Smart Search must align with existing cq conventions and infrastructure:

- The CLI is **Cyclopts**, not Typer. New commands must follow the `CliContext` → `CliResult` pipeline and be registered in `tools/cq/cli_app/app.py`.
- Repo root must remain the **true repo root** (`ctx.root`). The `--in` scope is a **scan filter**, not a root override, so anchors remain repo-relative.
- Result rendering and artifacts must flow through `tools/cq/cli_app/result.py` (`handle_result`) for consistent filtering and artifact saving.
- Summary keys should be **stable and ordered**, matching cq’s `Summary → Key Findings → Sections → Evidence` conventions.

These constraints avoid breaking existing macros and ensure Smart Search outputs interoperate with filters, artifacts, and renderers.

---

## 1. Command Interface

### 1.1 Primary Command: `/cq search`

```bash
# Identifier mode (default, auto-detected)
/cq search build_graph

# Regex mode (explicit)
/cq search "config.*path" --regex

# Literal mode (exact string match)
/cq search "hello world" --literal

# Include matches in strings/comments/docstrings
/cq search build_graph --include-strings

# Scoped search
/cq search CqResult --in tools/cq/core/

# With output format
/cq search Finding --format json
```

### 1.2 Query Fallback: `/cq q` for Plain Queries

```bash
# Falls back to Smart Search (no = in query)
/cq q build_graph

# Entity query (has =, uses existing query engine)
/cq q "entity=function name=foo"
```

**Detection heuristic**: Attempt to parse as a `q` query; if tokenization yields **no `key=value` pairs** or parsing fails with "missing entity", route to Smart Search. This keeps `/cq q` forgiving while preserving explicit `q` queries.

---

## 2. Module Structure

### 2.1 New Files

```
tools/cq/
├── search/
│   ├── smart_search.py      # Core Smart Search pipeline
│   ├── classifier.py        # Three-stage match classification
│   └── enricher.py          # symtable-based enrichment
├── cli_app/commands/
│   └── search.py            # CLI command handler (Cyclopts pattern)
```

### 2.2 Files to Modify

```
tools/cq/
├── cli_app/app.py           # Register search command
├── cli_app/result.py        # Rendering + artifact handling (reuse)
├── query/parser.py          # Add fallback detection for plain queries
```

### 2.3 Files to Reuse

| File | Reuse For | Reference |
|------|-----------|-----------|
| `tools/cq/search/adapter.py` | `search_content()`, rpygrep builder patterns | §3.2 |
| `tools/cq/search/profiles.py` | `SearchLimits` for safety bounds | §3.3 |
| `tools/cq/macros/calls.py` | Context window + snippet extraction (`_compute_context_window`, `_extract_context_snippet`) | §5.3 |
| `tools/cq/astgrep/sgpy_scanner.py` | SgRoot caching, node traversal patterns | §4.2 |
| `tools/cq/query/enrichment.py` | `SymtableEnricher` for binding analysis | §4.5 |
| `tools/cq/core/schema.py` | `CqResult`, `Finding`, `Section`, `Anchor`, `DetailPayload` | §6.1 |
| `tools/cq/core/scoring.py` | `ScoreDetails`, bucket computation | §6.2 |
| `tools/cq/core/report.py` | Markdown rendering with context snippets | §7.1 |

---

## 3. Phase 1: Candidate Generation (rpygrep)

### 3.1 rpygrep Integration Strategy

Per the rpygrep documentation, we use `RipGrepSearch` for content matching with typed results. The library provides:

- **Builder pattern** for command construction (`add_pattern`, `include_type`, `max_count`, etc.)
- **Typed results** via Pydantic-backed `RipGrepSearchResult` with `.path`, `.matches`, `.context`
- **Safety knobs** via `max_count`, `max_file_size`, `max_depth`, `add_safe_defaults()`

**Reference**: [rpygrep PyPI docs §F](docs/python_library_reference/rpygrep.md) — "Content search mode: RipGrepSearch"

### 3.2 Query Mode Detection (Identifier-Friendly)

```python
from enum import Enum
import re

class QueryMode(Enum):
    IDENTIFIER = "identifier"  # Word boundary match
    REGEX = "regex"            # User-provided regex
    LITERAL = "literal"        # Exact string match

def detect_query_mode(query: str, *, force_mode: QueryMode | None = None) -> QueryMode:
    """Detect query mode from query string.

    Parameters
    ----------
    query
        The search query string.
    force_mode
        Explicit mode override from CLI flag.

    Returns
    -------
    QueryMode
        Detected or forced query mode.

    Notes
    -----
    Auto-detection favors "identifier" for symbol-like input, including
    dotted names (e.g., `module.symbol`). Regex is only selected when
    explicit metacharacters are present.
    """
    if force_mode is not None:
        return force_mode

    # Identifier or dotted identifier (common symbol lookup)
    if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*$', query):
        return QueryMode.IDENTIFIER

    # Regex metacharacters (exclude '.' to allow dotted symbols)
    regex_chars = r'*+?[]{}()|^$\\'
    if any(c in query for c in regex_chars):
        return QueryMode.REGEX

    # Whitespace usually means literal string search
    if re.search(r"\s", query):
        return QueryMode.LITERAL

    return QueryMode.LITERAL
```

### 3.3 Search Profile Integration

Reuse existing `SearchLimits` from `tools/cq/search/profiles.py` and **derive** a Smart Search profile from the existing `INTERACTIVE` defaults (to preserve global tuning knobs):

```python
from dataclasses import replace
from tools.cq.search.profiles import INTERACTIVE, SearchLimits

SMART_SEARCH_LIMITS = replace(
    INTERACTIVE,
    max_files=200,            # Reasonable for interactive use
    max_matches_per_file=50,  # Sufficient for classification
    max_total_matches=500,    # Hard cap to prevent OOM
    max_file_size_bytes=2 * 1024 * 1024,  # 2MB max
    timeout_seconds=30.0,     # Interactive timeout
)
```

**Reference**: [rpygrep §F.3](docs/python_library_reference/rpygrep.md) — "Result limiting / safety"

### 3.4 rpygrep Builder Construction

```python
from pathlib import Path
from rpygrep import RipGrepSearch
from tools.cq.search.profiles import SearchLimits
from tools.cq.search.timeout import search_sync_with_timeout

def build_candidate_searcher(
    root: Path,
    query: str,
    mode: QueryMode,
    limits: SearchLimits,
    *,
    include_globs: list[str] | None = None,
    exclude_globs: list[str] | None = None,
) -> tuple[RipGrepSearch, str]:
    """Build rpygrep searcher for candidate generation.

    Parameters
    ----------
    root
        Repository root to search from.
    query
        Search query string.
    mode
        Query mode (identifier/regex/literal).
    limits
        Search safety limits.
    include_globs
        File patterns to include.
    exclude_globs
        File patterns to exclude.

    Returns
    -------
    tuple[RipGrepSearch, str]
        Configured searcher ready for execution, plus the effective pattern string.

    Notes
    -----
    Leverages rpygrep's builder pattern per documentation §D:
    - `add_pattern()` for regex patterns
    - `include_type("py")` for Python files
    - `before_context(1)` / `after_context(1)` for snippet extraction
    - `max_count()`, `max_file_size()` for safety bounds
    - `add_safe_defaults()` for agent-safe operation
    """
    # Build pattern based on mode
    searcher = (
        RipGrepSearch()
        .set_working_directory(root)
        .add_safe_defaults()  # Call first to avoid overriding explicit limits
        .include_type("py")
        .case_sensitive(True)
        .before_context(1)  # For snippet extraction
        .after_context(1)
        .max_count(limits.max_matches_per_file)
        .max_depth(limits.max_depth)
        .max_file_size(limits.max_file_size_bytes)
        .as_json()
    )

    if mode == QueryMode.IDENTIFIER:
        # Word boundary match for identifiers
        pattern = rf"\b{re.escape(query)}\b"
        searcher = searcher.add_pattern(pattern)
    elif mode == QueryMode.LITERAL:
        # Exact literal match (non-regex)
        pattern = query
        searcher = searcher.patterns_are_not_regex().add_pattern(query)
    else:
        # User-provided regex (pass through)
        pattern = query
        searcher = searcher.add_pattern(query)

    # Add include globs
    include_globs = include_globs or []
    for glob in include_globs:
        searcher = searcher.include_glob(glob)

    # Add exclude globs
    exclude_globs = exclude_globs or []
    for glob in exclude_globs:
        searcher = searcher.exclude_glob(glob)

    return searcher, pattern
```

**Important**: `include_globs` and `exclude_globs` must **affect scanning**, not just post-filtering. This ensures `--in`/`--exclude` reduce workload and improve result quality.

### 3.5 Raw Match Data Structure

```python
import msgspec
from typing import Annotated

class RawMatch(msgspec.Struct, frozen=True, omit_defaults=True):
    """Raw match from rpygrep candidate generation.

    Captures the essential data from rpygrep's typed results
    for downstream classification.

    Notes
    -----
    Uses msgspec.Struct with frozen=True for hashability (deduplication)
    and omit_defaults=True for compact JSON serialization.

    Reference: msgspec docs §1.1 — "frozen (hashable), omit_defaults"
    """

    file: str
    line: Annotated[int, msgspec.Meta(ge=1)]
    col: Annotated[int, msgspec.Meta(ge=0)]
    text: str  # Full line content
    match_text: str  # Exact matched substring
    match_start: int  # Column offset of match start
    match_end: int    # Column offset of match end
    submatch_index: int = 0  # Which submatch on this line

    # Optional context from rpygrep (line-only, not containing-def context)
    context_before: dict[int, str] | None = None
    context_after: dict[int, str] | None = None
```

### 3.6 Candidate Collection with Stats

```python
class SearchStats(msgspec.Struct, omit_defaults=True):
    """Statistics from candidate generation phase.

    Reference: msgspec docs §1.1 — "omit_defaults reduces payload size"
    """

    scanned_files: int
    matched_files: int
    total_matches: int
    truncated: bool = False
    timed_out: bool = False


def collect_candidates(
    searcher: RipGrepSearch,
    limits: SearchLimits,
) -> tuple[list[RawMatch], SearchStats]:
    """Execute rpygrep search and collect raw matches.

    Returns
    -------
    tuple[list[RawMatch], SearchStats]
        Raw matches and collection statistics.

    Notes
    -----
    Uses search_sync_with_timeout from existing adapter.py
    to enforce timeout bounds.

    Reference: rpygrep §G — "RipGrepSearchResult (per file)"
    """
    matches: list[RawMatch] = []
    seen_files: set[str] = set()
    truncated = False
    timed_out = False

    try:
        results = search_sync_with_timeout(
            lambda: list(searcher.run()),
            limits.timeout_seconds,
        )
    except TimeoutError:
        timed_out = True
        results = []

    # Optional: attach before/after context lines using rpygrep helpers
    context_map: dict[tuple[str, int], tuple[dict[int, str], dict[int, str]]] = {}
    try:
        from rpygrep.helpers import MatchedFile

        matched_files = MatchedFile.from_search_results(results, before_context=1, after_context=1)
        for mf in matched_files:
            for ml in mf.matched_lines:
                for line_no in ml.match:
                    context_map[(str(mf.path), line_no)] = (ml.before, ml.after)
    except Exception:
        context_map = {}

    for result in results:
        file_path = str(result.path)
        if file_path not in seen_files:
            if len(seen_files) >= limits.max_files:
                truncated = True
                break
            seen_files.add(file_path)

        for match in result.matches:
            if len(matches) >= limits.max_total_matches:
                truncated = True
                break

            line_text = match.data.lines.text or ""
            before, after = context_map.get((file_path, match.data.line_number), ({}, {}))

            # Emit one RawMatch per submatch so multiple hits on a line aren't dropped
            if match.data.submatches:
                for i, sub in enumerate(match.data.submatches):
                    if len(matches) >= limits.max_total_matches:
                        truncated = True
                        break
                    raw = RawMatch(
                        file=file_path,
                        line=match.data.line_number,
                        col=sub.start,
                        text=line_text,
                        match_text=line_text[sub.start:sub.end],
                        match_start=sub.start,
                        match_end=sub.end,
                        submatch_index=i,
                        context_before=before,
                        context_after=after,
                    )
                    matches.append(raw)
            else:
                raw = RawMatch(
                    file=file_path,
                    line=match.data.line_number,
                    col=0,
                    text=line_text,
                    match_text=line_text,
                    match_start=0,
                    match_end=len(line_text),
                    submatch_index=0,
                    context_before=before,
                    context_after=after,
                )
                matches.append(raw)

        if truncated:
            break

    stats = SearchStats(
        scanned_files=len(seen_files),  # rpygrep reports only matched files
        matched_files=len(seen_files),
        total_matches=len(matches),
        truncated=truncated,
        timed_out=timed_out,
    )

    return matches, stats
```

---

## 4. Phase 2: Semantic Enrichment (ast-grep-py + symtable)

### 4.1 Three-Stage Classification Architecture

The classification uses a three-stage pipeline that balances speed and accuracy:

| Stage | Cost | Confidence | When Used |
|-------|------|------------|-----------|
| 1. Fast Heuristic | O(1) | 0.60-0.80 | Always (first pass) |
| 2. AST Node Lookup | O(log n) | 0.85-0.95 | If heuristic uncertain |
| 3. Symtable Enrichment | O(parse) | 0.90-0.99 | For definitions/callsites only |

**Design rationale**: Most matches can be classified quickly via heuristics (comments, imports, obvious definitions). AST lookup provides higher confidence. Symtable is reserved for high-value matches where binding information adds signal.

### 4.2 ast-grep-py Integration

Per the ast-grep-py documentation, we use:

- `SgRoot(source, "python")` for parsing
- `SgNode.kind()` for node type classification
- `SgNode.range()` for position mapping
- `SgNode.parent()` for context traversal
- `SgNode.inside()` / `SgNode.has()` for refinement predicates

**Reference**: [ast-grep-py §1-2](docs/python_library_reference/ast-grep-py.md) — "Core object model", "Node inspection"

**Preferred classification path**: reuse existing ast-grep **rules** and **record types** in `tools/cq/astgrep/rules_py.py` to scan only matched files, then map matches to nearby `SgRecord` spans. This keeps Smart Search aligned with cq’s existing query semantics and reduces ad-hoc node-kind mapping drift.

### 4.3 Match Category Taxonomy

```python
from typing import Literal

MatchCategory = Literal[
    # Definitions (write sites)
    "definition",       # function/class/method/constant/variable def

    # Usage sites
    "callsite",         # function/method calls
    "import",           # import statement
    "from_import",      # from X import Y
    "reference",        # name/attribute access (not a call)
    "assignment",       # write site (x = ...)
    "annotation",       # type hints

    # Non-code matches
    "docstring_match",  # in docstring
    "comment_match",    # in comment
    "string_match",     # in string literal

    # Fallback
    "text_match",       # unclassified code match
]
```

By default, non-code categories (`docstring_match`, `comment_match`, `string_match`) are **collapsed** into a separate section unless `--include-strings` is provided.

### 4.4 Stage 1: Fast Heuristic Classification

```python
class HeuristicResult(msgspec.Struct, frozen=True):
    """Result from fast heuristic classification.

    Notes
    -----
    Uses frozen=True for immutability and potential caching.
    Reference: msgspec §A1 — "frozen: disables mutation, adds __hash__"
    """

    category: MatchCategory | None
    confidence: float
    skip_deeper: bool  # True if no further classification needed


def classify_heuristic(line: str, col: int, match_text: str) -> HeuristicResult:
    """Line-based pattern matching for fast classification.

    Parameters
    ----------
    line
        Full line content.
    col
        Column offset of match start.
    match_text
        The matched text.

    Returns
    -------
    HeuristicResult
        Classification result with confidence and skip flag.

    Notes
    -----
    O(1) complexity using simple string operations.
    Handles the common cases that don't need AST parsing.
    """
    stripped = line.lstrip()

    # Check comment (match position is after #)
    hash_pos = line.find("#")
    if hash_pos >= 0 and col > hash_pos:
        return HeuristicResult(
            category="comment_match",
            confidence=0.95,
            skip_deeper=True,  # No AST node for comments
        )

    # Check definition patterns (high confidence)
    if stripped.startswith(("def ", "async def ")):
        return HeuristicResult(
            category="definition",
            confidence=0.90,
            skip_deeper=False,  # AST can confirm
        )

    if stripped.startswith("class "):
        return HeuristicResult(
            category="definition",
            confidence=0.90,
            skip_deeper=False,
        )

    # Check import patterns (very high confidence)
    if stripped.startswith("import "):
        return HeuristicResult(
            category="import",
            confidence=0.95,
            skip_deeper=True,
        )

    if stripped.startswith("from ") and " import " in stripped:
        return HeuristicResult(
            category="from_import",
            confidence=0.95,
            skip_deeper=True,
        )

    # Check for call pattern (name followed by parenthesis)
    # This is a weaker signal, needs AST confirmation
    rest = line[col + len(match_text):]
    if rest.lstrip().startswith("("):
        return HeuristicResult(
            category="callsite",
            confidence=0.70,  # Needs AST confirmation
            skip_deeper=False,
        )

    # Check for docstring context (triple quotes)
    if '"""' in line or "'''" in line:
        return HeuristicResult(
            category="docstring_match",
            confidence=0.60,  # Uncertain without AST
            skip_deeper=False,
        )

    # No confident classification
    return HeuristicResult(
        category=None,
        confidence=0.0,
        skip_deeper=False,
    )
```

### 4.5 Stage 2: AST Node Classification

```python
from ast_grep_py import SgRoot, SgNode

# Node kind to category mapping based on tree-sitter-python grammar
# (Verify against actual ast-grep node kinds to avoid drift.)
NODE_KIND_MAP: dict[str, tuple[MatchCategory, float]] = {
    # Definitions
    "function_definition": ("definition", 0.95),
    "class_definition": ("definition", 0.95),
    "decorated_definition": ("definition", 0.95),

    # Calls
    "call": ("callsite", 0.95),

    # Imports
    "import_statement": ("import", 0.95),
    "import_from_statement": ("from_import", 0.95),

    # Assignments
    "assignment": ("assignment", 0.85),
    "augmented_assignment": ("assignment", 0.85),
    "named_expression": ("assignment", 0.85),  # walrus operator

    # Annotations
    "type": ("annotation", 0.90),

    # Strings (may be docstrings)
    "string": ("string_match", 0.85),
    "concatenated_string": ("string_match", 0.85),

    # Comments (shouldn't reach here via AST, but handle it)
    "comment": ("comment_match", 0.99),

    # Generic references
    "identifier": ("reference", 0.60),  # Low confidence - needs context
    "attribute": ("reference", 0.70),
}


class NodeClassification(msgspec.Struct, frozen=True, omit_defaults=True):
    """Result from AST node classification.

    Notes
    -----
    Reference: msgspec §1.1 — config keywords for Struct
    """

    category: MatchCategory
    confidence: float
    node_kind: str
    containing_scope: str | None = None
    evidence_kind: str = "resolved_ast"


def _find_node_at_position(root: SgNode, line: int, col: int) -> SgNode | None:
    """Find the most specific node containing position.

    Uses ast-grep's traversal primitives to locate the node.

    Reference: ast-grep-py §6 — "Traversal primitives (AST navigation)"
    """
    # Note: This naive traversal is O(n) in node count. For large files,
    # prefer building an IntervalIndex over matchable nodes (e.g., SgRecord spans)
    # to enable O(log n) lookups.
    # Use find_all with a range constraint
    # Note: ast-grep uses 0-indexed lines
    candidates = []

    def collect_at_position(node: SgNode) -> None:
        rng = node.range()
        # Check if position is within node range
        if (rng.start.line <= line - 1 <= rng.end.line):
            if rng.start.line == line - 1:
                if col < rng.start.column:
                    return
            if rng.end.line == line - 1:
                if col >= rng.end.column:
                    return
            candidates.append(node)
            # Recurse into children
            for child in node.children():
                collect_at_position(child)

    collect_at_position(root)

    # Return most specific (deepest) node
    if candidates:
        return candidates[-1]  # Last added is deepest
    return None


def _find_containing_scope(node: SgNode) -> str | None:
    """Walk up to find containing function/class scope.

    Reference: ast-grep-py §6 — "parent() -> Optional[SgNode]"
    """
    current = node.parent()
    while current:
        kind = current.kind()
        if kind in ("function_definition", "class_definition"):
            # Extract name from the definition
            name_node = current.field("name")
            if name_node:
                return name_node.text()
        current = current.parent()
    return None


def _is_docstring_context(node: SgNode) -> bool:
    """Check if string node is a docstring.

    Reference: ast-grep-py §5 — "inside(**rule) -> bool"
    """
    # A docstring is an expression_statement containing only a string
    # as the first statement of a function/class/module
    parent = node.parent()
    if parent and parent.kind() == "expression_statement":
        grandparent = parent.parent()
        if grandparent and grandparent.kind() in (
            "block", "module", "function_definition", "class_definition"
        ):
            # Check if it's the first statement
            siblings = list(grandparent.children())
            if siblings and siblings[0] == parent:
                return True
    return False


def classify_from_node(
    sg_root: SgRoot,
    line: int,
    col: int,
) -> NodeClassification | None:
    """Classify using ast-grep node lookup.

    Parameters
    ----------
    sg_root
        Parsed AST root.
    line
        1-indexed line number.
    col
        0-indexed column offset.

    Returns
    -------
    NodeClassification | None
        Classification result, or None if no classifiable node found.

    Notes
    -----
    Uses ast-grep's SgNode.kind() for classification.
    Reference: ast-grep-py §2.1 — "kind() -> str"
    """
    node = _find_node_at_position(sg_root.root(), line, col)
    if node is None:
        return None

    kind = node.kind()

    # Direct classification from node kind
    if kind in NODE_KIND_MAP:
        category, confidence = NODE_KIND_MAP[kind]

        # Special handling for strings that might be docstrings
        if category == "string_match" and _is_docstring_context(node):
            category = "docstring_match"
            confidence = 0.95

        return NodeClassification(
            category=category,
            confidence=confidence,
            node_kind=kind,
            containing_scope=_find_containing_scope(node),
        )

    # Walk up to find a classifiable parent
    parent = node.parent()
    depth = 0
    while parent and depth < 5:
        parent_kind = parent.kind()
        if parent_kind in NODE_KIND_MAP:
            category, confidence = NODE_KIND_MAP[parent_kind]
            # Reduce confidence for indirect classification
            return NodeClassification(
                category=category,
                confidence=confidence * 0.9,
                node_kind=parent_kind,
                containing_scope=_find_containing_scope(parent),
                evidence_kind="resolved_ast_heuristic",
            )
        parent = parent.parent()
        depth += 1

    return None
```

### 4.6 Stage 3: Symtable Enrichment

```python
import symtable
from typing import NamedTuple

class SymtableEnrichment(msgspec.Struct, frozen=True, omit_defaults=True):
    """Additional binding information from symtable.

    Uses symtable module to extract compile-time scope analysis.

    Reference: symtable docs §C6 — "Symbol per-identifier classification API"
    """

    is_imported: bool = False
    is_assigned: bool = False
    is_referenced: bool = False
    is_parameter: bool = False
    is_global: bool = False
    is_local: bool = False
    is_free: bool = False  # Closure variable
    is_nonlocal: bool = False


def enrich_with_symtable(
    source: str,
    filename: str,
    symbol_name: str,
    line: int,
) -> SymtableEnrichment | None:
    """Add scope/binding info for a symbol.

    Parameters
    ----------
    source
        Full source code of the file.
    filename
        Filename for error messages.
    symbol_name
        The symbol to look up.
    line
        Line number for scope resolution.

    Returns
    -------
    SymtableEnrichment | None
        Binding information, or None if lookup fails.

    Notes
    -----
    Uses symtable.symtable() to generate compile-time scope analysis.

    Reference: symtable docs §C1 — "symtable.symtable(code, filename, compile_type)"

    Symbol flags used (§C6):
    - is_referenced(): symbol is used
    - is_assigned(): symbol is assigned to
    - is_imported(): symbol is imported
    - is_parameter(): symbol is a function parameter
    - is_global(): symbol is declared global
    - is_local(): symbol is local to its scope
    - is_free(): symbol is a free variable (closure capture)
    - is_nonlocal(): symbol is declared nonlocal
    """
    try:
        table = symtable.symtable(source, filename, "exec")
    except SyntaxError:
        return None

    # Prefer caching per-file symtable results (or reuse SymtableEnricher from
    # tools/cq/query/enrichment.py) to avoid re-parsing for each match.

    def find_scope_for_line(
        st: symtable.SymbolTable,
        target_line: int,
    ) -> symtable.SymbolTable | None:
        """Find the innermost scope containing the line.

        Reference: symtable docs §C3 — "get_children() -> nested tables"
        """
        # Check if this scope starts at or before target line
        if st.get_lineno() <= target_line:
            # Check children first (deeper scopes)
            for child in st.get_children():
                result = find_scope_for_line(child, target_line)
                if result:
                    return result
            return st
        return None

    scope = find_scope_for_line(table, line) or table

    try:
        # Reference: symtable docs §C3 — "lookup(name) -> Symbol"
        sym = scope.lookup(symbol_name)
        return SymtableEnrichment(
            is_imported=sym.is_imported(),
            is_assigned=sym.is_assigned(),
            is_referenced=sym.is_referenced(),
            is_parameter=sym.is_parameter(),
            is_global=sym.is_global(),
            is_local=sym.is_local(),
            is_free=sym.is_free(),
            is_nonlocal=sym.is_nonlocal(),
        )
    except KeyError:
        return None
```

### 4.7 Unified Classification Pipeline

```python
class EnrichedMatch(msgspec.Struct, frozen=True, omit_defaults=True):
    """Fully enriched match with classification and context.

    Notes
    -----
    Uses msgspec Struct with:
    - frozen=True for hashability (deduplication, caching)
    - omit_defaults=True for compact serialization

    Reference: msgspec docs §1.1, §A8 — "omit_defaults reduces payload size"
    """

    # Core location
    file: str
    line: Annotated[int, msgspec.Meta(ge=1)]
    col: Annotated[int, msgspec.Meta(ge=0)]
    text: str
    match_text: str

    # Classification
    category: MatchCategory
    confidence: float
    evidence_kind: str

    # Context
    node_kind: str | None = None
    containing_scope: str | None = None

    # Context window for rendering
    context_window: dict[str, int] | None = None  # {start_line, end_line}
    context_snippet: str | None = None

    # Optional symtable enrichment
    symtable: SymtableEnrichment | None = None


# File-level AST cache to avoid re-parsing
_sg_cache: dict[str, SgRoot] = {}
_source_cache: dict[str, str] = {}


def _get_sg_root(file_path: Path) -> SgRoot | None:
    """Get or create cached SgRoot for a file.

    Reference: ast-grep-py §1.1 — "SgRoot(src: str, language: str)"
    """
    key = str(file_path)
    if key not in _sg_cache:
        try:
            source = _source_cache.get(key)
            if source is None:
                source = file_path.read_text(encoding="utf-8")
                _source_cache[key] = source
            _sg_cache[key] = SgRoot(source, "python")
        except (OSError, UnicodeDecodeError):
            return None
    return _sg_cache[key]


def classify_match(
    raw: RawMatch,
    root: Path,
    *,
    enable_symtable: bool = True,
) -> EnrichedMatch:
    """Run three-stage classification pipeline on a raw match.

    Parameters
    ----------
    raw
        Raw match from rpygrep.
    root
        Repository root for file resolution.
    enable_symtable
        Whether to run symtable enrichment (slower but higher quality).

    Returns
    -------
    EnrichedMatch
        Fully classified and enriched match.
    """
    # Extract match text from RawMatch (already computed per submatch)
    match_text = raw.match_text

    # Stage 1: Fast heuristic
    heuristic = classify_heuristic(raw.text, raw.col, match_text)

    if heuristic.skip_deeper and heuristic.category is not None:
        # Heuristic is confident enough, skip AST
        return EnrichedMatch(
            file=raw.file,
            line=raw.line,
            col=raw.col,
            text=raw.text,
            match_text=match_text,
            category=heuristic.category,
            confidence=heuristic.confidence,
            evidence_kind="heuristic",
        )

    # Stage 2: AST node classification
    file_path = root / raw.file
    if file_path.suffix != ".py":
        return EnrichedMatch(
            file=raw.file,
            line=raw.line,
            col=raw.col,
            text=raw.text,
            match_text=match_text,
            category=heuristic.category or "text_match",
            confidence=heuristic.confidence or 0.4,
            evidence_kind="rg_only",
        )
    sg_root = _get_sg_root(file_path)

    ast_result: NodeClassification | None = None
    if sg_root is not None:
        ast_result = classify_from_node(sg_root, raw.line, raw.col)

    # Determine best classification
    if ast_result is not None:
        category = ast_result.category
        confidence = ast_result.confidence
        evidence_kind = ast_result.evidence_kind
        node_kind = ast_result.node_kind
        containing_scope = ast_result.containing_scope
    elif heuristic.category is not None:
        category = heuristic.category
        confidence = heuristic.confidence
        evidence_kind = "heuristic"
        node_kind = None
        containing_scope = None
    else:
        # Fallback to text_match
        category = "text_match"
        confidence = 0.50
        evidence_kind = "rg_only"
        node_kind = None
        containing_scope = None

    # Stage 3: Symtable enrichment (only for high-value categories)
    symtable_enrichment: SymtableEnrichment | None = None
    if enable_symtable and category in ("definition", "callsite", "reference", "assignment"):
        try:
            source = _source_cache.get(str(file_path))
            if source is None:
                source = file_path.read_text(encoding="utf-8")
                _source_cache[str(file_path)] = source
            symtable_enrichment = enrich_with_symtable(
                source,
                raw.file,
                match_text,
                raw.line,
            )
        except (OSError, UnicodeDecodeError):
            pass

    return EnrichedMatch(
        file=raw.file,
        line=raw.line,
        col=raw.col,
        text=raw.text,
        match_text=match_text,
        category=category,
        confidence=confidence,
        evidence_kind=evidence_kind,
        node_kind=node_kind,
        containing_scope=containing_scope,
        symtable=symtable_enrichment,
    )
```

---

## 5. Context Window Extraction

### 5.1 Reuse from `macros/calls.py`

The existing `tools/cq/macros/calls.py` provides `_compute_context_window()` and `_extract_context_snippet()` utilities, which already match cq’s rendering expectations. Smart Search should reuse these to keep output consistent.

### 5.2 Context Window + Snippet (Reuse `macros/calls.py`)

Prefer the **existing** context window and snippet utilities to keep output consistent with other cq macros.

```python
from tools.cq.macros.calls import _compute_context_window, _extract_context_snippet
from tools.cq.search.adapter import find_def_lines

def compute_context(
    file_path: Path,
    match_line: int,
) -> tuple[dict[str, int] | None, str | None]:
    """Compute context window and snippet for a match.

    Notes
    -----
    - Uses `_compute_context_window` from calls.py (indent-based).
    - Uses `_extract_context_snippet` from calls.py (raw snippet, no line numbers).
    """
    try:
        source = file_path.read_text(encoding="utf-8")
        source_lines = source.splitlines()
    except (OSError, UnicodeDecodeError):
        return None, None

    def_lines = find_def_lines(file_path)
    if not def_lines:
        return None, None

    window = _compute_context_window(match_line, def_lines, len(source_lines))
    snippet = _extract_context_snippet(
        source_lines,
        window["start_line"],
        window["end_line"],
    )
    return window, snippet
```

---

## 6. Phase 3: Result Assembly

### 6.1 Ranking Algorithm

```python
# Kind weights for relevance scoring
KIND_WEIGHTS: dict[MatchCategory, float] = {
    "definition": 1.0,
    "callsite": 0.8,
    "import": 0.7,
    "from_import": 0.7,
    "reference": 0.6,
    "assignment": 0.5,
    "annotation": 0.5,
    "text_match": 0.3,
    "docstring_match": 0.2,
    "comment_match": 0.15,
    "string_match": 0.1,
}


def _classify_file_role(file_path: str) -> str:
    """Classify file role for ranking.

    Returns
    -------
    str
        One of "src", "test", "doc", "lib", "other"
    """
    path_lower = file_path.lower()

    if "/test" in path_lower or "test_" in path_lower or "_test.py" in path_lower:
        return "test"
    if "/doc" in path_lower or "/docs/" in path_lower:
        return "doc"
    if "/vendor/" in path_lower or "/third_party/" in path_lower:
        return "lib"
    if "/src/" in path_lower or "/lib/" in path_lower:
        return "src"

    return "other"


def compute_relevance_score(match: EnrichedMatch) -> float:
    """Compute relevance score for ranking.

    Parameters
    ----------
    match
        Enriched match to score.

    Returns
    -------
    float
        Relevance score (higher is better).

    Notes
    -----
    Scoring factors:
    1. Category weight (definitions most valuable)
    2. File role multiplier (src > lib > test > doc)
    3. Path depth penalty (prefer shallow paths)
    4. Confidence factor
    """
    # Base weight from category
    base = KIND_WEIGHTS.get(match.category, 0.3)

    # File role multiplier
    role = _classify_file_role(match.file)
    role_mult = {
        "src": 1.0,
        "lib": 0.9,
        "other": 0.7,
        "test": 0.5,
        "doc": 0.3,
    }.get(role, 0.7)

    # Path depth penalty (prefer shallow paths)
    depth = match.file.count("/")
    depth_penalty = min(0.2, depth * 0.02)

    # Confidence factor
    conf_factor = match.confidence

    return base * role_mult * conf_factor - depth_penalty
```

### 6.2 Summary Construction

```python
class SmartSearchSummary(msgspec.Struct, omit_defaults=True):
    """Summary for smart search results.

    Reference: msgspec §1.1 — omit_defaults for compact output
    """

    query: str
    mode: str  # "identifier", "regex", "literal"
    file_globs: list[str]
    include: list[str]
    exclude: list[str]
    context_lines: dict[str, int]  # {before, after}
    limit: int
    scanned_files: int
    matched_files: int
    total_matches: int
    returned_matches: int
    scan_method: str = "hybrid"  # rpygrep + ast-grep

    # Interpretation (make mode + pattern choices explicit)
    pattern: str | None = None
    case_sensitive: bool = True
    caps_hit: str = "none"

    # Conditional fields
    truncated: bool = False
    timed_out: bool = False


def build_summary(
    query: str,
    mode: QueryMode,
    stats: SearchStats,
    matches: list[EnrichedMatch],
    limits: SearchLimits,
    *,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
    file_globs: list[str] | None = None,
    limit: int | None = None,
    pattern: str | None = None,
) -> dict[str, object]:
    """Build summary dict for CqResult.

    Notes
    -----
    Returns dict for compatibility with existing CqResult.summary field.
    """
    return {
        "query": query,
        "mode": mode.value,
        "file_globs": file_globs or ["*.py", "*.pyi"],
        "include": include or [],
        "exclude": exclude or [],
        "context_lines": {"before": 1, "after": 1},
        "limit": limit if limit is not None else limits.max_total_matches,
        "scanned_files": stats.scanned_files,
        "matched_files": stats.matched_files,
        "total_matches": stats.total_matches,
        "returned_matches": len(matches),
        "scan_method": "hybrid",
        "pattern": pattern,
        "case_sensitive": True,
        "caps_hit": "timeout" if stats.timed_out else ("max_total_matches" if stats.truncated else "none"),
        "truncated": stats.truncated,
        "timed_out": stats.timed_out,
    }
```

**Explain interpretation**: the summary must always include the effective `pattern`, `case_sensitive`, `include`, `exclude`, and `context_lines`. This makes the mode decision transparent and helps users re-run with adjusted flags.

### 6.3 Section Construction

```python
from tools.cq.core.schema import (
    Finding, Section, Anchor, DetailPayload, ScoreDetails, CqResult, RunMeta, mk_runmeta, ms
)


def _evidence_to_bucket(evidence_kind: str) -> str:
    """Map evidence kind to confidence bucket."""
    return {
        "resolved_ast": "high",
        "resolved_ast_heuristic": "medium",
        "heuristic": "medium",
        "rg_only": "low",
    }.get(evidence_kind, "medium")


def build_finding(match: EnrichedMatch, root: Path) -> Finding:
    """Convert EnrichedMatch to Finding.

    Reference: tools/cq/core/schema.py Finding structure
    """
    # Compute context window + snippet (reuse calls.py helpers)
    file_path = root / match.file
    context_window, snippet = compute_context(file_path, match.line)

    # Build score details
    score = ScoreDetails(
        confidence_score=match.confidence,
        confidence_bucket=_evidence_to_bucket(match.evidence_kind),
        evidence_kind=match.evidence_kind,
    )

    # Build detail payload
    data: dict[str, object] = {
        "match_text": match.match_text,
        "line_text": match.text,
    }
    if context_window:
        data["context_window"] = context_window
    if snippet:
        data["context_snippet"] = snippet
    if match.containing_scope:
        data["containing_scope"] = match.containing_scope
    if match.node_kind:
        data["node_kind"] = match.node_kind
    if match.symtable:
        # Add symtable flags that are True
        symtable_flags = []
        if match.symtable.is_imported:
            symtable_flags.append("imported")
        if match.symtable.is_assigned:
            symtable_flags.append("assigned")
        if match.symtable.is_parameter:
            symtable_flags.append("parameter")
        if match.symtable.is_free:
            symtable_flags.append("closure_var")
        if match.symtable.is_global:
            symtable_flags.append("global")
        if symtable_flags:
            data["binding_flags"] = symtable_flags

    details = DetailPayload(
        kind=match.category,
        score=score,
        data=data,
    )

    return Finding(
        category=match.category,
        message=_category_message(match.category, match),
        anchor=Anchor(
            file=match.file,
            line=match.line,
            col=match.col,
        ),
        severity="info",
        details=details,
    )


def _category_message(category: MatchCategory, match: EnrichedMatch) -> str:
    """Generate human-readable message for category."""
    messages = {
        "definition": "Function/class definition",
        "callsite": "Function call",
        "import": "Import statement",
        "from_import": "From import",
        "reference": "Reference",
        "assignment": "Assignment",
        "annotation": "Type annotation",
        "docstring_match": "Match in docstring",
        "comment_match": "Match in comment",
        "string_match": "Match in string literal",
        "text_match": "Text match",
    }
    base = messages.get(category, "Match")
    if match.containing_scope:
        return f"{base} in {match.containing_scope}"
    return base
```

### 6.4 Section Grouping

```python
from collections import Counter

def build_sections(
    matches: list[EnrichedMatch],
    root: Path,
    query: str,
    mode: QueryMode,
    *,
    include_strings: bool = False,
) -> list[Section]:
    """Build organized sections for CqResult.

    Sections (in order):
    1. Top Contexts (grouped by containing function; fallback by file)
    2. Definitions / Imports / Calls (identifier mode only)
    3. Uses by Kind (counts, collapsed)
    4. Non-Code Matches (strings/comments/docstrings, collapsed)
    5. Hot Files (collapsed)
    6. Suggested Follow-ups
    """
    sections: list[Section] = []

    NON_CODE = {"docstring_match", "comment_match", "string_match"}

    # Sort by relevance
    sorted_matches = sorted(
        matches,
        key=lambda m: compute_relevance_score(m),
        reverse=True,
    )

    visible_matches = (
        sorted_matches if include_strings else [m for m in sorted_matches if m.category not in NON_CODE]
    )

    # Group by containing function/method (fallback to file)
    grouped: dict[str, list[EnrichedMatch]] = {}
    for match in visible_matches:
        if match.containing_scope:
            key = f"{match.containing_scope} ({match.file})"
        else:
            key = match.file
        grouped.setdefault(key, []).append(match)

    # Section 1: Top Contexts (representative match per group)
    group_scores = [
        (key, max(compute_relevance_score(m) for m in group), group)
        for key, group in grouped.items()
    ]
    group_scores.sort(key=lambda t: t[1], reverse=True)
    top_contexts: list[Finding] = []
    for key, _score, group in group_scores[:20]:
        rep = group[0]
        finding = build_finding(rep, root)
        finding.message = f"{key}"
        top_contexts.append(finding)
    sections.append(Section(title="Top Contexts", findings=top_contexts))

    # Section 2: Identifier panels (definitions, imports, calls)
    if mode == QueryMode.IDENTIFIER:
        defs = [m for m in visible_matches if m.category == "definition"]
        imps = [m for m in visible_matches if m.category in ("import", "from_import")]
        calls = [m for m in visible_matches if m.category == "callsite"]
        if defs:
            sections.append(Section(
                title="Definitions",
                findings=[build_finding(m, root) for m in defs[:5]],
            ))
        if imps:
            sections.append(Section(
                title="Imports",
                findings=[build_finding(m, root) for m in imps[:10]],
                collapsed=True,
            ))
        if calls:
            sections.append(Section(
                title="Callsites",
                findings=[build_finding(m, root) for m in calls[:10]],
                collapsed=True,
            ))

    # Section 3: Uses by Kind
    category_counts = Counter(m.category for m in matches)
    kind_findings = [
        Finding(
            category="count",
            message=f"{cat}: {count}",
            severity="info",
            details=DetailPayload(kind="count", data={"category": cat, "count": count}),
        )
        for cat, count in category_counts.most_common()
    ]
    sections.append(Section(
        title="Uses by Kind",
        findings=kind_findings,
        collapsed=True,
    ))

    # Section 4: Non-code matches (collapsed by default)
    non_code = [m for m in sorted_matches if m.category in NON_CODE]
    if non_code:
        sections.append(Section(
            title="Non-Code Matches (Strings / Comments / Docstrings)",
            findings=[build_finding(m, root) for m in non_code[:20]],
            collapsed=True,
        ))

    # Section 5: Hot Files
    file_counts = Counter(m.file for m in matches)
    hot_file_findings = [
        Finding(
            category="hot_file",
            message=f"{file}: {count} matches",
            anchor=Anchor(file=file, line=1),
            severity="info",
            details=DetailPayload(kind="hot_file", data={"count": count}),
        )
        for file, count in file_counts.most_common(10)
    ]
    sections.append(Section(
        title="Hot Files",
        findings=hot_file_findings,
        collapsed=True,
    ))

    # Section 6: Suggested Follow-ups
    followup_findings = build_followups(matches, query, mode)
    if followup_findings:
        sections.append(Section(
            title="Suggested Follow-ups",
            findings=followup_findings,
        ))

    return sections


def build_followups(
    matches: list[EnrichedMatch],
    query: str,
    mode: QueryMode,
) -> list[Finding]:
    """Generate actionable next commands."""
    findings: list[Finding] = []

    # If identifier mode and we found definitions, suggest callers
    if mode == QueryMode.IDENTIFIER:
        defs = [m for m in matches if m.category == "definition"]
        if defs:
            findings.append(Finding(
                category="next_step",
                message=f"Find callers: /cq calls {query}",
                severity="info",
                details=DetailPayload(
                    kind="next_step",
                    data={"cmd": f"/cq calls {query}"},
                ),
            ))
            findings.append(Finding(
                category="next_step",
                message=f"Find definitions: /cq q \"entity=function name={query}\"",
                severity="info",
                details=DetailPayload(
                    kind="next_step",
                    data={"cmd": f"/cq q \"entity=function name={query}\""},
                ),
            ))
            findings.append(Finding(
                category="next_step",
                message=f"Find callers (transitive): /cq q \"entity=function name={query} expand=callers(depth=2)\"",
                severity="info",
                details=DetailPayload(
                    kind="next_step",
                    data={"cmd": f"/cq q \"entity=function name={query} expand=callers(depth=2)\""},
                ),
            ))

        # If we found callsites, suggest impact analysis
        calls = [m for m in matches if m.category == "callsite"]
        if calls:
            findings.append(Finding(
                category="next_step",
                message=f"Analyze impact: /cq impact {query}",
                severity="info",
                details=DetailPayload(
                    kind="next_step",
                    data={"cmd": f"/cq impact {query}"},
                ),
            ))

    return findings
```

### 6.5 Progressive Disclosure Defaults

To keep context payloads high-signal:

- Show **top contexts only** (default 20).
- Collapse non-code matches and hot-file lists by default.
- Always emit full JSON artifacts (handled by existing artifact pipeline) for deep dives.

---

## 7. Full Pipeline Assembly

### 7.1 Main Entry Point

```python
from tools.cq.core.toolchain import Toolchain

def smart_search(
    root: Path,
    query: str,
    *,
    mode: QueryMode | None = None,
    include_globs: list[str] | None = None,
    exclude_globs: list[str] | None = None,
    include_strings: bool = False,
    limits: SearchLimits | None = None,
    tc: Toolchain | None = None,
    argv: list[str] | None = None,
) -> CqResult:
    """Execute Smart Search pipeline.

    Parameters
    ----------
    root
        Repository root path.
    query
        Search query string.
    mode
        Query mode override (auto-detected if None).
    include_globs
        File patterns to include.
    exclude_globs
        File patterns to exclude.
    include_strings
        Whether to include strings/comments/docstrings in primary ranking.
    limits
        Search safety limits.
    tc
        Toolchain info from CliContext (for RunMeta).
    argv
        Original command arguments for run metadata.

    Returns
    -------
    CqResult
        Complete search results.
    """
    started = ms()
    limits = limits or SMART_SEARCH_LIMITS
    argv = argv or ["search", query]

    # Detect query mode
    actual_mode = detect_query_mode(query, force_mode=mode)

    # Phase 1: Candidate generation
    searcher, pattern = build_candidate_searcher(
        root, query, actual_mode, limits,
        include_globs=include_globs,
        exclude_globs=exclude_globs,
    )
    raw_matches, stats = collect_candidates(searcher, limits)

    # Phase 2: Classification
    enriched_matches = [
        classify_match(raw, root)
        for raw in raw_matches
    ]

    # Phase 3: Assembly
    summary = build_summary(
        query,
        actual_mode,
        stats,
        enriched_matches,
        limits,
        include=include_globs,
        exclude=exclude_globs,
        file_globs=["*.py", "*.pyi"],
        limit=limits.max_total_matches,
        pattern=pattern,
    )
    sections = build_sections(enriched_matches, root, query, actual_mode, include_strings=include_strings)

    # Build CqResult
    run = mk_runmeta(
        macro="search",
        argv=argv,
        root=str(root),
        started_ms=started,
        toolchain=tc.to_dict() if tc else {},
    )

    return CqResult(
        run=run,
        summary=summary,
        sections=sections,
        key_findings=sections[0].findings[:5] if sections else [],  # Top 5 from Top Matches
        evidence=[build_finding(m, root) for m in enriched_matches],
    )
```

---

## 8. CLI Command Handler (Cyclopts)

### 8.1 Command Definition

```python
# tools/cq/cli_app/commands/search.py

from __future__ import annotations

from typing import Annotated
from cyclopts import Parameter

from tools.cq.cli_app.context import CliContext, CliResult, FilterConfig
from tools.cq.search.smart_search import smart_search, QueryMode, SMART_SEARCH_LIMITS


def search(
    query: Annotated[str, Parameter(help="Search query")],
    *,
    regex: Annotated[bool, Parameter(name="--regex", help="Treat query as regex")] = False,
    literal: Annotated[bool, Parameter(name="--literal", help="Treat query as literal")] = False,
    include_strings: Annotated[
        bool,
        Parameter(name="--include-strings", help="Include matches in strings/comments/docstrings")
    ] = False,
    in_dir: Annotated[str | None, Parameter(name="--in", help="Restrict to directory")] = None,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
    include: Annotated[list[str] | None, Parameter(help="Include patterns")] = None,
    exclude: Annotated[list[str] | None, Parameter(help="Exclude patterns")] = None,
    impact_filter: Annotated[str | None, Parameter(name="--impact", help="Impact filter")] = None,
    confidence: Annotated[str | None, Parameter(help="Confidence filter")] = None,
    severity: Annotated[str | None, Parameter(help="Severity filter")] = None,
    limit: Annotated[int | None, Parameter(help="Max findings")] = None,
) -> CliResult:
    """Search for code patterns with semantic enrichment."""
    if ctx is None:
        raise RuntimeError("Context not injected")

    # Determine mode
    mode: QueryMode | None = None
    if regex:
        mode = QueryMode.REGEX
    elif literal:
        mode = QueryMode.LITERAL

    # Treat --in as scan scope, not a root override
    include_globs = list(include) if include else []
    if in_dir:
        include_globs.append(f"{in_dir}/**")

    result = smart_search(
        ctx.root,
        query,
        mode=mode,
        include_globs=include_globs,
        exclude_globs=list(exclude) if exclude else None,
        include_strings=include_strings,
        limits=SMART_SEARCH_LIMITS,
        tc=ctx.toolchain,
        argv=ctx.argv,
    )

    filters = FilterConfig(
        include=list(include) if include else [],
        exclude=list(exclude) if exclude else [],
        impact=[p.strip() for p in impact_filter.split(",")] if impact_filter else [],
        confidence=[p.strip() for p in confidence.split(",")] if confidence else [],
        severity=[p.strip() for p in severity.split(",")] if severity else [],
        limit=limit,
    )
    return CliResult(result=result, context=ctx, filters=filters)
```

Register the command in `tools/cq/cli_app/app.py` under the Analysis group, consistent with existing macros.

---

## 9. Performance Considerations

### 9.1 Design Decisions for Performance

| Decision | Rationale | Reference |
|----------|-----------|-----------|
| rpygrep pre-filter | Reduces files by ~90% before parsing | rpygrep §A — "enumerate candidate files" |
| SgRoot caching | Parse each file at most once | ast-grep-py §1.1 — constructor |
| Three-stage classification | Skip expensive stages when possible | §4.1 |
| Lazy symtable | Only for high-value matches | symtable §C1 |
| Early termination | Stop at limits | rpygrep §F.3 |
| msgspec structs | C-backed, fast serialization | msgspec §1.1 |

### 9.2 Complexity Analysis

- **Phase 1** (rpygrep): O(total_files) but highly optimized native code
- **Phase 2** (classification): O(matched_files × matches_per_file)
  - Heuristic: O(1) per match
  - AST lookup: O(n) per match with naive traversal; O(log n) if an IntervalIndex is built per file
  - Symtable: O(parse) but only for ~10% of matches
- **Phase 3** (ranking): O(n log n) for sorting

### 9.3 Memory Management

- **SgRoot cache**: Bounded to prevent OOM; clear after pipeline completes
- **Source cache**: Cache file text per matched file to avoid repeated reads
- **Raw matches**: Use frozen msgspec Structs for deduplication
- **Results**: Stream to output when possible

---

## 10. Testing Strategy

### 10.1 Unit Tests

```
tests/cq/search/
├── test_classifier.py      # Classification stages
├── test_smart_search.py    # Full pipeline
└── test_query_mode.py      # Mode detection
```

### 10.2 Test Cases for Classification

```python
@pytest.mark.parametrize("line,col,expected_category", [
    # Heuristic-only (skip AST)
    ("# TODO: fix build_graph", 15, "comment_match"),
    ("import build_graph", 7, "import"),
    ("from foo import build_graph", 16, "from_import"),

    # AST confirmation needed
    ("def build_graph():", 4, "definition"),
    ("result = build_graph()", 9, "callsite"),
    ("self.build_graph = x", 5, "assignment"),
])
def test_classification(line: str, col: int, expected_category: str) -> None:
    ...
```

Additional unit cases:

- Multiple submatches on a single line (verify one RawMatch per submatch).
- Grouping by containing function (Top Contexts uses function scope).
- `--include-strings` promotes string/comment/docstring matches into Top Contexts.

### 10.3 Golden Tests

Add to `tests/cli_golden/`:

```bash
# Generate golden
uv run pytest tests/cli_golden/test_search_golden.py --update-golden

# Verify
uv run pytest tests/cli_golden/test_search_golden.py
```

### 10.4 Manual Verification

```bash
# Identifier mode
/cq search build_graph --format md

# Include string/comment matches
/cq search build_graph --include-strings

# Regex mode
/cq search "config.*path" --regex

# Scoped search
/cq search CqResult --in tools/cq/core/

# JSON output
/cq search Finding --format json | jq '.sections[].title'
```

---

## 11. Implementation Order

1. **Phase 1**: Create `classifier.py` with three-stage classification + per-file caches
2. **Phase 2**: Create `smart_search.py` with full pipeline (include/exclude scopes apply to scanning)
3. **Phase 3**: Add grouping by containing function + non-code collapse + `--include-strings`
4. **Phase 4**: Create Cyclopts CLI command (`cli_app/commands/search.py`) + register in `app.py`
5. **Phase 5**: Add `q` fallback detection for plain queries
6. **Phase 6**: Add tests and golden snapshots
7. **Phase 7**: Update AGENTS.md documentation

---

## 12. Follow-on Improvements (Aligned With Review)

These are high-ROI follow-ups that should be tracked explicitly:

1. **rg prefilter for `q entity=... name=...`**  
   Use rpygrep to narrow candidate files before ast-grep scanning for `q`, making entity queries feel as fast as `rg`.
2. **Lightweight call binding confidence** in `calls`  
   Add per-file import/local binding heuristics to reduce false positives and report confidence buckets.
3. **"Did you mean?" suggestions**  
   When no results are found, suggest close symbol names using fuzzy matching across defs/imports.

---

## Appendix A: Library Feature Mapping

### A.1 ast-grep-py Features Used

| Feature | Usage | Reference |
|---------|-------|-----------|
| `SgRoot(source, "python")` | Parse source to AST | §1.1 |
| `SgNode.kind()` | Node type classification | §2.1 |
| `SgNode.range()` | Position mapping | §2.2 |
| `SgNode.parent()` | Context traversal | §6 |
| `SgNode.children()` | Child enumeration | §6 |
| `SgNode.field(name)` | Named field access | §6 |
| `SgNode.inside(**rule)` | Refinement predicate | §5 |

### A.2 rpygrep Features Used

| Feature | Usage | Reference |
|---------|-------|-----------|
| `RipGrepSearch()` | Builder construction | §F |
| `.add_pattern()` | Pattern specification | §F.1 |
| `.patterns_are_not_regex()` | Literal string matching | §F.1 |
| `.include_type("py")` | File type filter | §D.3 |
| `.before_context() / .after_context()` | Context lines | §F.2 |
| `.max_count() / .max_file_size()` | Safety limits | §F.3 |
| `.add_safe_defaults()` | Agent-safe defaults | §F.3 |
| `.as_json()` | Typed output mode | §F.4 |
| `.run()` | Execution | §F.5 |
| `MatchedFile.from_search_results()` | Attach before/after context per match | §I |

### A.3 symtable Features Used

| Feature | Usage | Reference |
|---------|-------|-----------|
| `symtable.symtable()` | Table generation | §C1 |
| `SymbolTable.get_children()` | Nested scopes | §C3 |
| `SymbolTable.lookup()` | Symbol lookup | §C3 |
| `Symbol.is_imported()` | Import detection | §C6 |
| `Symbol.is_assigned()` | Assignment detection | §C6 |
| `Symbol.is_parameter()` | Parameter detection | §C6 |
| `Symbol.is_free()` | Closure detection | §C6 |
| `Symbol.is_global()` | Global detection | §C6 |

### A.4 msgspec Features Used

| Feature | Usage | Reference |
|---------|-------|-----------|
| `msgspec.Struct` | Schema definition | §1.1 |
| `frozen=True` | Immutability + hashability | §A1 |
| `omit_defaults=True` | Compact serialization | §A8 |
| `msgspec.Meta(ge=...)` | Value constraints | §2.1 |
| `msgspec.field(default_factory=...)` | Dynamic defaults | §A3 |
| `msgspec.structs.replace()` | Copy with changes | §1.5 |

---

## Appendix B: Evidence Kind Confidence Mapping

| Evidence Kind | Confidence Range | Description |
|---------------|-----------------|-------------|
| `resolved_ast` | 0.90-0.99 | Direct AST node classification |
| `resolved_ast_heuristic` | 0.80-0.90 | Parent node classification |
| `heuristic` | 0.60-0.80 | Line-based pattern matching |
| `rg_only` | 0.40-0.50 | No classification possible |

---

## Appendix C: Output Schema (JSON)

```json
{
  "run": {
    "macro": "search",
    "argv": ["search", "build_graph"],
    "root": "/path/to/repo",
    "started_ms": 1706812345678.0,
    "elapsed_ms": 234.5,
    "toolchain": {"rpygrep": "0.2.1", "ast-grep-py": "0.40.3"},
    "schema_version": "1.0.0"
  },
  "summary": {
    "query": "build_graph",
    "mode": "identifier",
    "file_globs": ["*.py", "*.pyi"],
    "include": [],
    "exclude": [],
    "context_lines": {"before": 1, "after": 1},
    "limit": 500,
    "scanned_files": 150,
    "matched_files": 12,
    "total_matches": 45,
    "returned_matches": 45,
    "scan_method": "hybrid",
    "pattern": "\\bbuild_graph\\b",
    "case_sensitive": true,
    "caps_hit": "none",
    "truncated": false,
    "timed_out": false
  },
  "key_findings": [...],
  "sections": [
    {
      "title": "Top Contexts",
      "findings": [
        {
          "category": "definition",
          "message": "build_graph (src/graph.py)",
          "anchor": {"file": "src/graph.py", "line": 42, "col": 4},
          "severity": "info",
          "details": {
            "kind": "definition",
            "score": {
              "confidence_score": 0.95,
              "confidence_bucket": "high",
              "evidence_kind": "resolved_ast"
            },
            "data": {
              "match_text": "build_graph",
              "line_text": "def build_graph(...):",
              "node_kind": "function_definition",
              "context_window": {"start_line": 38, "end_line": 65},
              "context_snippet": "..."
            }
          }
        }
      ]
    },
    {"title": "Uses by Kind", "findings": [...], "collapsed": true},
    {"title": "Non-Code Matches (Strings / Comments / Docstrings)", "findings": [...], "collapsed": true},
    {"title": "Hot Files", "findings": [...], "collapsed": true},
    {"title": "Definitions", "findings": [...]},
    {"title": "Suggested Follow-ups", "findings": [...]}
  ],
  "evidence": [...],
  "artifacts": []
}
```
