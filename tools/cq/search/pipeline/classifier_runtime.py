"""Runtime/cache helpers for search match classification."""

from __future__ import annotations

import symtable
from dataclasses import dataclass
from pathlib import Path

from ast_grep_py import SgNode, SgRoot

from tools.cq.astgrep.rules import get_rules_for_types
from tools.cq.astgrep.sgpy_scanner import SgRecord, scan_files
from tools.cq.query.language import DEFAULT_QUERY_LANGUAGE, QueryLanguage
from tools.cq.utils.interval_index import IntervalIndex


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


# Per-file caches to avoid re-parsing
_sg_cache: dict[tuple[str, QueryLanguage], SgRoot] = {}
_source_cache: dict[str, str] = {}
_def_lines_cache: dict[tuple[str, QueryLanguage], list[tuple[int, int]]] = {}
_symtable_cache: dict[str, symtable.SymbolTable] = {}
_record_context_cache: dict[tuple[str, QueryLanguage], RecordContext] = {}
_node_index_cache: dict[tuple[str, QueryLanguage], NodeIntervalIndex] = {}


def _lang_cache_key(file_path: Path, lang: QueryLanguage) -> tuple[str, QueryLanguage]:
    return str(file_path), lang


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
    records = scan_files([file_path], rules, root, lang=lang, prefilter=False)
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
    expr_stmt = node.parent()
    if expr_stmt is None or expr_stmt.kind() != "expression_statement":
        return False

    scope_body = expr_stmt.parent()
    if scope_body is None:
        return False

    # Module docstrings live directly under module; function/class docstrings live
    # under a block node whose first statement must be the docstring expression.
    if scope_body.kind() == "module":
        pass
    elif scope_body.kind() == "block":
        owner = scope_body.parent()
        if owner is None or owner.kind() not in {"function_definition", "class_definition"}:
            return False
    else:
        return False

    def _node_key(value: SgNode) -> tuple[str, int, int, int, int]:
        range_obj = value.range()
        return (
            value.kind(),
            range_obj.start.line,
            range_obj.start.column,
            range_obj.end.line,
            range_obj.end.column,
        )

    first_stmt = next((child for child in scope_body.children() if child.is_named()), None)
    if first_stmt is None or _node_key(first_stmt) != _node_key(expr_stmt):
        return False

    first_expr_child = next((child for child in expr_stmt.children() if child.is_named()), None)
    return first_expr_child is not None and _node_key(first_expr_child) == _node_key(node)


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


def clear_classifier_caches() -> None:
    """Clear internal classifier runtime caches."""
    _sg_cache.clear()
    _source_cache.clear()
    _def_lines_cache.clear()
    _symtable_cache.clear()
    _record_context_cache.clear()
    _node_index_cache.clear()


__all__ = [
    "RecordContext",
    "_find_containing_scope",
    "_find_node_at_position",
    "_is_docstring_context",
    "clear_classifier_caches",
    "get_cached_source",
    "get_def_lines_cached",
    "get_node_index",
    "get_record_context",
    "get_sg_root",
    "get_symtable_table",
]
