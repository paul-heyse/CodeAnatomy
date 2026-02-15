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

from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.query.language import DEFAULT_QUERY_LANGUAGE, QueryLanguage
from tools.cq.search.pipeline.classifier_runtime import (
    _find_containing_scope,
    _find_node_at_position,
    _is_docstring_context,
    clear_classifier_caches,
    get_cached_source,
    get_def_lines_cached,
    get_node_index,
    get_record_context,
    get_sg_root,
    get_symtable_table,
)


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
    "call_expression": ("callsite", 0.95),
    "macro_invocation": ("callsite", 0.90),
    # Imports
    "import_statement": ("import", 0.95),
    "import_from_statement": ("from_import", 0.95),
    "use_declaration": ("import", 0.95),
    # Assignments
    "assignment": ("assignment", 0.85),
    "augmented_assignment": ("assignment", 0.85),
    "named_expression": ("assignment", 0.85),
    "let_declaration": ("assignment", 0.85),
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
    "scoped_identifier": ("reference", 0.70),
    # Rust definitions
    "function_item": ("definition", 0.95),
    "struct_item": ("definition", 0.95),
    "enum_item": ("definition", 0.95),
    "trait_item": ("definition", 0.95),
    "impl_item": ("definition", 0.85),
    "mod_item": ("definition", 0.95),
}


def _record_to_category(record: SgRecord) -> MatchCategory | None:
    """Map an ast-grep record to a match category.

    Used by record-based classification to label search evidence.

    Returns:
    -------
    MatchCategory | None
        Match category for the record, or None if not mapped.
    """
    if record.record == "def":
        return "definition"
    if record.record == "call":
        return "callsite"
    if record.record == "import":
        return "from_import" if record.kind.startswith("from_import") else "import"
    if record.record == "assign_ctor":
        return "assignment"
    return None


def _record_contains(record: SgRecord, line: int, col: int) -> bool:
    """Check whether a record contains the given (line, col) position.

    Used by record classification to filter matches by cursor position.

    Returns:
    -------
    bool
        True if the record span contains the provided position.
    """
    if record.start_line < line < record.end_line:
        return True
    if line == record.start_line and line == record.end_line:
        return record.start_col <= col < record.end_col
    if line == record.start_line:
        return col >= record.start_col
    if line == record.end_line:
        return col < record.end_col
    return False


def _extract_def_name_from_record(record: SgRecord) -> str | None:
    """Extract function/class name from a definition record.

    Used by classifier utilities to label definition records.

    Returns:
    -------
    str | None
        Extracted name, or None if not a definition.
    """
    if record.record != "def":
        return None
    patterns = (
        r"(?:async\s+)?(?:def|class)\s+([A-Za-z_][A-Za-z0-9_]*)",
        r"fn\s+([A-Za-z_][A-Za-z0-9_]*)",
        r"(?:struct|enum|trait|mod)\s+([A-Za-z_][A-Za-z0-9_]*)",
    )
    for pattern in patterns:
        match = re.search(pattern, record.text)
        if match:
            return match.group(1)
    return None


def _definition_name_span(record: SgRecord) -> tuple[int, int] | None:
    """Return absolute column span for a definition name.

    Returns:
    -------
    tuple[int, int] | None
        Start/end columns (0-indexed, end-exclusive) on ``record.start_line``.
    """
    if record.record != "def":
        return None
    first_line = record.text.splitlines()[0] if record.text else ""
    patterns = (
        r"(?:async\s+)?(?:def|class)\s+([A-Za-z_][A-Za-z0-9_]*)",
        r"fn\s+([A-Za-z_][A-Za-z0-9_]*)",
        r"(?:struct|enum|trait|mod)\s+([A-Za-z_][A-Za-z0-9_]*)",
    )
    for pattern in patterns:
        match = re.search(pattern, first_line)
        if match:
            start = record.start_col + match.start(1)
            end = record.start_col + match.end(1)
            return start, end
    return None


def _definition_name_contains(record: SgRecord, line: int, col: int) -> bool:
    """Return whether ``(line, col)`` overlaps the definition identifier token."""
    span = _definition_name_span(record)
    if span is None or line != record.start_line:
        return False
    start, end = span
    return start <= col < end


def detect_query_mode(query: str, *, force_mode: QueryMode | None = None) -> QueryMode:
    """Detect query mode from query string.

    Parameters
    ----------
    query
        The search query string.
    force_mode
        Explicit mode override from CLI flag.

    Returns:
    -------
    QueryMode
        Detected or forced query mode.

    Notes:
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

    Returns:
    -------
    HeuristicResult
        Classification result with confidence and skip flag.

    Notes:
    -----
    O(1) complexity using simple string operations.
    Handles the common cases that don't need AST parsing.
    """
    stripped = line.lstrip()
    result = HeuristicResult(category=None, confidence=0.0, skip_deeper=False)

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
        result = HeuristicResult(
            category="definition",
            confidence=0.90,
            skip_deeper=False,  # AST can confirm
        )
    elif stripped.startswith("class "):
        result = HeuristicResult(
            category="definition",
            confidence=0.90,
            skip_deeper=False,
        )
    # Check import patterns (very high confidence)
    elif stripped.startswith("import "):
        result = HeuristicResult(
            category="import",
            confidence=0.95,
            skip_deeper=True,
        )
    elif stripped.startswith("from ") and " import " in stripped:
        result = HeuristicResult(
            category="from_import",
            confidence=0.95,
            skip_deeper=True,
        )
    else:
        # Check for call pattern (name followed by parenthesis)
        # This is a weaker signal, needs AST confirmation
        rest = line[col + len(match_text) :]
        if rest.lstrip().startswith("("):
            result = HeuristicResult(
                category="callsite",
                confidence=0.70,  # Needs AST confirmation
                skip_deeper=False,
            )
        # Check for docstring context (triple quotes)
        elif '"""' in line or "'''" in line:
            result = HeuristicResult(
                category="docstring_match",
                confidence=0.60,  # Uncertain without AST
                skip_deeper=False,
            )

    return result


def classify_from_node(
    sg_root: SgRoot,
    line: int,
    col: int,
    *,
    lang: QueryLanguage = DEFAULT_QUERY_LANGUAGE,
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
    lang
        Query language used for parser/node-kind semantics.

    Returns:
    -------
    NodeClassification | None
        Classification result, or None if no classifiable node found.
    """
    node = _find_node_at_position(sg_root, line, col, lang=lang)
    if node is None:
        return None
    return classify_from_resolved_node(node)


def classify_from_resolved_node(node: SgNode) -> NodeClassification | None:
    """Classify from an already-resolved AST node.

    Returns:
    -------
    NodeClassification | None
        Classification result or ``None`` when no mapping applies.
    """
    kind = node.kind()
    if kind in NODE_KIND_MAP:
        category, confidence = NODE_KIND_MAP[kind]
        if category == "string_match" and _is_docstring_context(node):
            category = "docstring_match"
            confidence = 0.95
        return NodeClassification(
            category=category,
            confidence=confidence,
            node_kind=kind,
            containing_scope=_find_containing_scope(node),
        )

    max_parent_depth = 5
    parent = node.parent()
    depth = 0
    while parent and depth < max_parent_depth:
        parent_kind = parent.kind()
        if parent_kind in NODE_KIND_MAP:
            category, confidence = NODE_KIND_MAP[parent_kind]
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


def classify_from_records(
    file_path: Path,
    root: Path,
    line: int,
    col: int,
    *,
    lang: QueryLanguage = DEFAULT_QUERY_LANGUAGE,
) -> NodeClassification | None:
    """Classify using cached ast-grep records.

    Parameters
    ----------
    file_path
        Source file path.
    root
        Repository root.
    line
        1-indexed line number.
    col
        0-indexed column offset.
    lang
        Query language used for record extraction semantics.

    Returns:
    -------
    NodeClassification | None
        Classification result, or None if no record matches.
    """
    context = get_record_context(file_path, root, lang=lang)
    if not context.records:
        return None

    candidates = context.record_index.find_candidates(line)
    if not candidates:
        return None

    # Filter by column when possible
    scoped = [record for record in candidates if _record_contains(record, line, col)]
    candidates = scoped if scoped else candidates

    record = min(candidates, key=lambda r: r.end_line - r.start_line)
    category = _record_to_category(record)
    if category is None:
        return None
    if category == "definition" and not _definition_name_contains(record, line, col):
        return None

    containing_scope: str | None = None
    if record.record == "def":
        containing_scope = _extract_def_name_from_record(record)
    else:
        containing_def = context.def_index.find_containing(line)
        if containing_def is not None:
            containing_scope = _extract_def_name_from_record(containing_def)

    return NodeClassification(
        category=category,
        confidence=0.95,
        node_kind=record.kind,
        containing_scope=containing_scope,
        evidence_kind="resolved_ast_record",
    )


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

    Returns:
    -------
    SymtableEnrichment | None
        Binding information, or None if lookup fails.
    """
    try:
        table = symtable.symtable(source, filename, "exec")
    except SyntaxError:
        return None

    return enrich_with_symtable_from_table(table, symbol_name, line)


def enrich_with_symtable_from_table(
    table: symtable.SymbolTable,
    symbol_name: str,
    line: int,
) -> SymtableEnrichment | None:
    """Add scope/binding info for a symbol using a cached symtable.

    Parameters
    ----------
    table
        Precomputed symtable for the file.
    symbol_name
        The symbol to look up.
    line
        Line number for scope resolution.

    Returns:
    -------
    SymtableEnrichment | None
        Binding information, or None if lookup fails.
    """

    def find_scope_for_line(
        st: symtable.SymbolTable,
        target_line: int,
    ) -> symtable.SymbolTable | None:
        """Find the innermost scope containing the line.

        Used by ``enrich_with_symtable_from_table`` to locate the closest scope.

        Returns:
        -------
        symtable.SymbolTable | None
            Innermost scope containing ``target_line``.
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


def clear_caches() -> None:
    """Clear per-file caches and dependent enrichment caches."""
    clear_classifier_caches()
    from tools.cq.search.python.extractors import clear_python_enrichment_cache
    from tools.cq.search.tree_sitter.rust_lane.runtime import clear_tree_sitter_rust_cache

    clear_python_enrichment_cache()
    clear_tree_sitter_rust_cache()


__all__ = [
    "NODE_KIND_MAP",
    "HeuristicResult",
    "MatchCategory",
    "NodeClassification",
    "QueryMode",
    "SymtableEnrichment",
    "classify_from_node",
    "classify_from_records",
    "classify_from_resolved_node",
    "classify_heuristic",
    "clear_caches",
    "detect_query_mode",
    "enrich_with_symtable",
    "enrich_with_symtable_from_table",
    "get_cached_source",
    "get_def_lines_cached",
    "get_node_index",
    "get_sg_root",
    "get_symtable_table",
]
