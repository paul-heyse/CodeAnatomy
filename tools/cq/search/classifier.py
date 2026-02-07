"""Three-stage match classification pipeline for smart search.

Provides semantic enrichment of search matches through:
1. Fast heuristic classification (O(1) line patterns)
2. AST node classification (O(log n) with ast-grep)
3. Symtable enrichment (O(parse) for high-value matches)
"""

from __future__ import annotations

import re
import symtable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Literal

import msgspec
from ast_grep_py import SgNode, SgRoot

from tools.cq.astgrep.rules import get_rules_for_types
from tools.cq.astgrep.sgpy_scanner import SgRecord, scan_files
from tools.cq.query.language import DEFAULT_QUERY_LANGUAGE, QueryLanguage
from tools.cq.utils.interval_index import IntervalIndex


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


@dataclass(frozen=True)
class RecordContext:
    """Cached ast-grep record context for a file."""

    records: list[SgRecord]
    record_index: IntervalIndex[SgRecord]
    def_index: IntervalIndex[SgRecord]


@dataclass(frozen=True)
class NodeSpan:
    """Cached AST node span for fast position lookup."""

    start_line: int
    end_line: int
    start_col: int
    end_col: int
    node: SgNode


@dataclass(frozen=True)
class NodeIntervalIndex:
    """Interval index for AST node spans."""

    line_index: IntervalIndex[NodeSpan]

    def find_containing(self, line: int, col: int) -> SgNode | None:
        """Find the innermost node containing a position.

        Used by node-based classification to resolve a cursor location.

        Returns:
        -------
        SgNode | None
            Innermost node containing the position, or None if not found.
        """
        candidates = [
            span
            for span in self.line_index.find_candidates(line)
            if _span_contains(span, line, col)
        ]
        if not candidates:
            return None
        best = min(candidates, key=lambda s: (s.end_line - s.start_line, s.end_col - s.start_col))
        return best.node


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


# Per-file caches to avoid re-parsing
_sg_cache: dict[tuple[str, QueryLanguage], SgRoot] = {}
_source_cache: dict[str, str] = {}
_def_lines_cache: dict[tuple[str, QueryLanguage], list[tuple[int, int]]] = {}
_symtable_cache: dict[str, symtable.SymbolTable] = {}
_record_context_cache: dict[tuple[str, QueryLanguage], RecordContext] = {}
_node_index_cache: dict[tuple[str, QueryLanguage], NodeIntervalIndex] = {}


def _lang_cache_key(file_path: Path, lang: QueryLanguage) -> tuple[str, QueryLanguage]:
    return str(file_path), lang


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


def get_record_context(
    file_path: Path,
    root: Path,
    *,
    lang: QueryLanguage = DEFAULT_QUERY_LANGUAGE,
) -> RecordContext:
    """Get or build ast-grep record context for a file.

    Used by classification to cache parsed ast-grep records per file.

    Returns:
    -------
    RecordContext
        Cached record context for the file.
    """
    key = _lang_cache_key(file_path, lang)
    if key in _record_context_cache:
        return _record_context_cache[key]

    rules = get_rules_for_types(None, lang=lang)
    records = scan_files([file_path], rules, root, lang=lang)
    record_index = IntervalIndex.from_records(records)
    def_records = [record for record in records if record.record == "def"]
    def_index: IntervalIndex[SgRecord] = (
        IntervalIndex.from_records(def_records) if def_records else IntervalIndex([])
    )

    context = RecordContext(
        records=records,
        record_index=record_index,
        def_index=def_index,
    )
    _record_context_cache[key] = context
    return context


def _build_node_spans(root: SgNode) -> list[NodeSpan]:
    spans: list[NodeSpan] = []
    stack = [root]
    while stack:
        node = stack.pop()
        if node.is_named():
            rng = node.range()
            spans.append(
                NodeSpan(
                    start_line=rng.start.line + 1,
                    end_line=rng.end.line + 1,
                    start_col=rng.start.column,
                    end_col=rng.end.column,
                    node=node,
                )
            )
        stack.extend(node.children())
    # Sort for deterministic lookup (outer to inner)
    spans.sort(key=lambda s: (s.start_line, -s.end_line, s.start_col))
    return spans


def _build_node_interval_index(spans: list[NodeSpan]) -> IntervalIndex[NodeSpan]:
    intervals = [(span.start_line, span.end_line, span) for span in spans]
    return IntervalIndex.from_intervals(intervals)


def get_node_index(
    file_path: Path,
    sg_root: SgRoot,
    *,
    lang: QueryLanguage = DEFAULT_QUERY_LANGUAGE,
) -> NodeIntervalIndex:
    """Get or build cached node interval index for a file.

    Used by node-based classification to avoid repeated AST walks.

    Returns:
    -------
    NodeIntervalIndex
        Cached node interval index for the file.
    """
    key = _lang_cache_key(file_path, lang)
    if key in _node_index_cache:
        return _node_index_cache[key]
    spans = _build_node_spans(sg_root.root())
    index = NodeIntervalIndex(line_index=_build_node_interval_index(spans))
    _node_index_cache[key] = index
    return index


def _resolve_sg_root_path(sg_root: SgRoot) -> Path | None:
    """Resolve cached file path for a given SgRoot.

    Used by node classification to connect an in-memory tree to its file path.

    Returns:
    -------
    Path | None
        Cached path if available.
    """
    for (path_str, _lang), cached_root in _sg_cache.items():
        if cached_root is sg_root:
            return Path(path_str)
    return None


def _span_contains(span: NodeSpan, line: int, col: int) -> bool:
    if span.start_line < line < span.end_line:
        return True
    if line == span.start_line and line == span.end_line:
        return span.start_col <= col < span.end_col
    if line == span.start_line:
        return col >= span.start_col
    if line == span.end_line:
        return col < span.end_col
    return False


def _find_node_at_position(
    sg_root: SgRoot,
    line: int,
    col: int,
    *,
    file_path: Path | None = None,
    lang: QueryLanguage = DEFAULT_QUERY_LANGUAGE,
) -> SgNode | None:
    """Find the most specific node containing position.

    Uses a cached span index when possible to avoid full tree walks.

    Returns:
    -------
    SgNode | None
        Most specific node containing the position, if any.
    """
    resolved_path = file_path or _resolve_sg_root_path(sg_root)
    if resolved_path is not None:
        index = get_node_index(resolved_path, sg_root, lang=lang)
        return index.find_containing(line, col)

    spans = _build_node_spans(sg_root.root())
    index = NodeIntervalIndex(line_index=_build_node_interval_index(spans))
    return index.find_containing(line, col)


def _find_containing_scope(node: SgNode) -> str | None:
    """Walk up to find containing function/class scope.

    Parameters
    ----------
    node
        Starting AST node.

    Returns:
    -------
    str | None
        Name of containing scope, or None.
    """
    current = node.parent()
    while current:
        kind = current.kind()
        if kind in {
            "function_definition",
            "class_definition",
            "function_item",
            "struct_item",
            "enum_item",
            "trait_item",
            "impl_item",
            "mod_item",
        }:
            # Extract name from the definition
            name_node = current.field("name")
            if name_node:
                return name_node.text()
            if kind == "impl_item":
                type_node = current.field("type")
                if type_node:
                    return type_node.text()
        current = current.parent()
    return None


def _is_docstring_context(node: SgNode) -> bool:
    """Check if string node is a docstring.

    Parameters
    ----------
    node
        String AST node.

    Returns:
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


def get_symtable_table(file_path: Path, source: str) -> symtable.SymbolTable | None:
    """Get or create cached symtable for a file.

    Used by symtable enrichment to reuse parsed symbol tables.

    Returns:
    -------
    symtable.SymbolTable | None
        Cached or newly created symbol table, or None on syntax errors.
    """
    key = str(file_path)
    if key in _symtable_cache:
        return _symtable_cache[key]
    try:
        table = symtable.symtable(source, str(file_path), "exec")
    except SyntaxError:
        return None
    _symtable_cache[key] = table
    return table


def get_def_lines_cached(
    file_path: Path,
    *,
    lang: QueryLanguage = DEFAULT_QUERY_LANGUAGE,
) -> list[tuple[int, int]]:
    """Get or compute def/async def lines with indentation.

    Parameters
    ----------
    file_path
        Path to the file.
    lang
        Query language used to select definition prefixes.

    Returns:
    -------
    list[tuple[int, int]]
        (line_number, indent) tuples for def/async def lines.
    """
    key = _lang_cache_key(file_path, lang)
    if key in _def_lines_cache:
        return _def_lines_cache[key]

    source = get_cached_source(file_path)
    if source is None:
        _def_lines_cache[key] = []
        return _def_lines_cache[key]

    results: list[tuple[int, int]] = []
    for i, line in enumerate(source.splitlines(), 1):
        stripped = line.lstrip()
        if lang == "rust":
            prefixes = ("fn ", "pub fn ", "struct ", "enum ", "trait ", "impl ", "mod ", "pub mod ")
        else:
            prefixes = ("def ", "async def ", "class ")
        if stripped.startswith(prefixes):
            indent = len(line) - len(stripped)
            results.append((i, indent))
    _def_lines_cache[key] = results
    return results


def get_sg_root(
    file_path: Path,
    *,
    lang: QueryLanguage = DEFAULT_QUERY_LANGUAGE,
) -> SgRoot | None:
    """Get or create cached SgRoot for a file.

    Parameters
    ----------
    file_path
        Path to the Python file.
    lang
        Query language used by ast-grep parsing.

    Returns:
    -------
    SgRoot | None
        Parsed AST root, or None on error.
    """
    key = _lang_cache_key(file_path, lang)
    if key not in _sg_cache:
        try:
            source_key = str(file_path)
            source = _source_cache.get(source_key)
            if source is None:
                source = file_path.read_text(encoding="utf-8")
                _source_cache[source_key] = source
            _sg_cache[key] = SgRoot(source, lang)
        except (OSError, UnicodeDecodeError):
            return None
    return _sg_cache[key]


def get_cached_source(file_path: Path) -> str | None:
    """Get cached source for a file.

    Parameters
    ----------
    file_path
        Path to the file.

    Returns:
    -------
    str | None
        File source, or None on error.
    """
    key = str(file_path)
    if key in _source_cache:
        return _source_cache[key]
    try:
        source = file_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None
    else:
        _source_cache[key] = source
        return source


def clear_caches() -> None:
    """Clear per-file caches."""
    _sg_cache.clear()
    _source_cache.clear()
    _def_lines_cache.clear()
    _symtable_cache.clear()
    _record_context_cache.clear()
    _node_index_cache.clear()
    from tools.cq.search.python_enrichment import clear_python_enrichment_cache
    from tools.cq.search.tree_sitter_rust import clear_tree_sitter_rust_cache

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
