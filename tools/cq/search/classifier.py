"""Three-stage match classification pipeline for smart search.

Provides semantic enrichment of search matches through:
1. Fast heuristic classification (O(1) line patterns)
2. AST node classification (O(log n) with ast-grep)
3. Symtable enrichment (O(parse) for high-value matches)
"""

from __future__ import annotations

import re
import symtable
from enum import Enum
from pathlib import Path
from typing import Literal

import msgspec
from ast_grep_py import SgNode, SgRoot


class QueryMode(Enum):
    """Search query mode classification."""

    IDENTIFIER = "identifier"  # Word boundary match
    REGEX = "regex"  # User-provided regex
    LITERAL = "literal"  # Exact string match


MatchCategory = Literal[
    # Definitions (write sites)
    "definition",  # function/class/method/constant/variable def
    # Usage sites
    "callsite",  # function/method calls
    "import",  # import statement
    "from_import",  # from X import Y
    "reference",  # name/attribute access (not a call)
    "assignment",  # write site (x = ...)
    "annotation",  # type hints
    # Non-code matches
    "docstring_match",  # in docstring
    "comment_match",  # in comment
    "string_match",  # in string literal
    # Fallback
    "text_match",  # unclassified code match
]


class HeuristicResult(msgspec.Struct, frozen=True):
    """Result from fast heuristic classification.

    Parameters
    ----------
    category
        Detected match category, or None if uncertain.
    confidence
        Confidence score (0.0-1.0).
    skip_deeper
        True if no further classification needed.
    """

    category: MatchCategory | None
    confidence: float
    skip_deeper: bool


class NodeClassification(msgspec.Struct, frozen=True, omit_defaults=True):
    """Result from AST node classification.

    Parameters
    ----------
    category
        Classified match category.
    confidence
        Confidence score (0.0-1.0).
    node_kind
        AST node kind from tree-sitter.
    containing_scope
        Name of containing function/class, if any.
    evidence_kind
        Classification evidence source.
    """

    category: MatchCategory
    confidence: float
    node_kind: str
    containing_scope: str | None = None
    evidence_kind: str = "resolved_ast"


class SymtableEnrichment(msgspec.Struct, frozen=True, omit_defaults=True):
    """Additional binding information from symtable.

    Parameters
    ----------
    is_imported
        Symbol is imported.
    is_assigned
        Symbol is assigned to.
    is_referenced
        Symbol is used.
    is_parameter
        Symbol is a function parameter.
    is_global
        Symbol is declared global.
    is_local
        Symbol is local to its scope.
    is_free
        Symbol is a free variable (closure capture).
    is_nonlocal
        Symbol is declared nonlocal.
    """

    is_imported: bool = False
    is_assigned: bool = False
    is_referenced: bool = False
    is_parameter: bool = False
    is_global: bool = False
    is_local: bool = False
    is_free: bool = False
    is_nonlocal: bool = False


# Node kind to category mapping based on tree-sitter-python grammar
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
    "named_expression": ("assignment", 0.85),
    # Annotations
    "type": ("annotation", 0.90),
    # Strings (may be docstrings)
    "string": ("string_match", 0.85),
    "concatenated_string": ("string_match", 0.85),
    # Comments (shouldn't reach here via AST, but handle it)
    "comment": ("comment_match", 0.99),
    # Generic references
    "identifier": ("reference", 0.60),
    "attribute": ("reference", 0.70),
}


# Per-file caches to avoid re-parsing
_sg_cache: dict[str, SgRoot] = {}
_source_cache: dict[str, str] = {}


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
    if re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)*$", query):
        return QueryMode.IDENTIFIER

    # Regex metacharacters (exclude '.' to allow dotted symbols)
    regex_chars = r"*+?[]{}()|^$\\"
    if any(c in query for c in regex_chars):
        return QueryMode.REGEX

    # Whitespace usually means literal string search
    if re.search(r"\s", query):
        return QueryMode.LITERAL

    return QueryMode.LITERAL


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
    rest = line[col + len(match_text) :]
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


def _find_node_at_position(root: SgNode, line: int, col: int) -> SgNode | None:
    """Find the most specific node containing position.

    Parameters
    ----------
    root
        AST root node.
    line
        1-indexed line number.
    col
        0-indexed column offset.

    Returns
    -------
    SgNode | None
        Most specific node at position, or None.

    Notes
    -----
    Uses ast-grep's traversal primitives to locate the node.
    This naive traversal is O(n) in node count.
    """
    candidates: list[SgNode] = []

    def collect_at_position(node: SgNode) -> None:
        rng = node.range()
        # Check if position is within node range
        # ast-grep uses 0-indexed lines
        if rng.start.line <= line - 1 <= rng.end.line:
            if rng.start.line == line - 1 and col < rng.start.column:
                return
            if rng.end.line == line - 1 and col >= rng.end.column:
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

    Parameters
    ----------
    node
        Starting AST node.

    Returns
    -------
    str | None
        Name of containing scope, or None.
    """
    current = node.parent()
    while current:
        kind = current.kind()
        if kind in {"function_definition", "class_definition"}:
            # Extract name from the definition
            name_node = current.field("name")
            if name_node:
                return name_node.text()
        current = current.parent()
    return None


def _is_docstring_context(node: SgNode) -> bool:
    """Check if string node is a docstring.

    Parameters
    ----------
    node
        String AST node.

    Returns
    -------
    bool
        True if node is a docstring.
    """
    # A docstring is an expression_statement containing only a string
    # as the first statement of a function/class/module
    parent = node.parent()
    if parent and parent.kind() == "expression_statement":
        grandparent = parent.parent()
        if grandparent and grandparent.kind() in {
            "block",
            "module",
            "function_definition",
            "class_definition",
        }:
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
    max_parent_depth = 5
    parent = node.parent()
    depth = 0
    while parent and depth < max_parent_depth:
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
    """
    try:
        table = symtable.symtable(source, filename, "exec")
    except SyntaxError:
        return None

    def find_scope_for_line(
        st: symtable.SymbolTable,
        target_line: int,
    ) -> symtable.SymbolTable | None:
        """Find the innermost scope containing the line."""
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


def get_sg_root(file_path: Path) -> SgRoot | None:
    """Get or create cached SgRoot for a file.

    Parameters
    ----------
    file_path
        Path to the Python file.

    Returns
    -------
    SgRoot | None
        Parsed AST root, or None on error.
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


def get_cached_source(file_path: Path) -> str | None:
    """Get cached source for a file.

    Parameters
    ----------
    file_path
        Path to the file.

    Returns
    -------
    str | None
        File source, or None on error.
    """
    key = str(file_path)
    if key in _source_cache:
        return _source_cache[key]
    try:
        source = file_path.read_text(encoding="utf-8")
        _source_cache[key] = source
        return source
    except (OSError, UnicodeDecodeError):
        return None


def clear_caches() -> None:
    """Clear per-file caches."""
    _sg_cache.clear()
    _source_cache.clear()


__all__ = [
    "NODE_KIND_MAP",
    "HeuristicResult",
    "MatchCategory",
    "NodeClassification",
    "QueryMode",
    "SymtableEnrichment",
    "classify_from_node",
    "classify_heuristic",
    "clear_caches",
    "detect_query_mode",
    "enrich_with_symtable",
    "get_cached_source",
    "get_sg_root",
]
