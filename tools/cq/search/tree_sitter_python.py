"""Optional Python context enrichment using tree-sitter query packs.

This module is best-effort only. Any parser/query failure must degrade to a
payload that never alters ranking, counts, or category classification.
"""

from __future__ import annotations

import re
from functools import lru_cache
from hashlib import blake2b
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tree_sitter import Language, Node, Parser, Query, QueryCursor, Tree

try:
    import tree_sitter_python as _tree_sitter_python
    from tree_sitter import Language as _TreeSitterLanguage
    from tree_sitter import Parser as _TreeSitterParser
    from tree_sitter import Query as _TreeSitterQuery
    from tree_sitter import QueryCursor as _TreeSitterQueryCursor
except ImportError:  # pragma: no cover - optional dependency
    _tree_sitter_python = None
    _TreeSitterLanguage = None
    _TreeSitterParser = None
    _TreeSitterQuery = None
    _TreeSitterQueryCursor = None

_QUERY_ROOT = Path(__file__).with_suffix("").parent / "queries" / "python"
_MAX_TREE_CACHE_ENTRIES = 64
_TREE_CACHE: dict[str, tuple[Tree, str]] = {}
_MAX_SOURCE_BYTES = 5 * 1024 * 1024
_MAX_CAPTURE_ITEMS = 8
_MAX_CAPTURE_TEXT_LEN = 120
_DEFAULT_MATCH_LIMIT = 4_096
_QUERY_TIMEOUT_SECONDS = 0.035
_ENRICHMENT_ERRORS = (RuntimeError, TypeError, ValueError, AttributeError, UnicodeError)
_COMPILE_ERRORS = (RuntimeError, TypeError, ValueError, AttributeError)


class _CacheStats:
    """Mutable cache counter holder."""

    def __init__(self) -> None:
        """Initialize zeroed cache counters."""
        self.hits = 0
        self.misses = 0
        self.evictions = 0


_CACHE_STATS = _CacheStats()


def _source_hash(source_bytes: bytes) -> str:
    return blake2b(source_bytes, digest_size=16).hexdigest()


def is_tree_sitter_python_available() -> bool:
    """Return whether tree-sitter Python runtime dependencies are available."""
    return all(
        obj is not None
        for obj in (
            _tree_sitter_python,
            _TreeSitterLanguage,
            _TreeSitterParser,
            _TreeSitterQuery,
            _TreeSitterQueryCursor,
        )
    )


@lru_cache(maxsize=1)
def _python_language() -> Language:
    if _tree_sitter_python is None or _TreeSitterLanguage is None:
        msg = "tree_sitter_python language bindings are unavailable"
        raise RuntimeError(msg)
    return _TreeSitterLanguage(_tree_sitter_python.language())


def _make_parser() -> Parser:
    if _TreeSitterParser is None:
        msg = "tree_sitter parser bindings are unavailable"
        raise RuntimeError(msg)
    return _TreeSitterParser(_python_language())


def _parse_tree(source_bytes: bytes) -> Tree:
    parser = _make_parser()
    tree = parser.parse(source_bytes)
    if tree is None:
        msg = "tree-sitter parser returned no tree"
        raise RuntimeError(msg)
    return tree


def _get_tree(source: str, *, cache_key: str | None) -> tuple[Tree, bytes]:
    source_bytes = source.encode("utf-8", errors="replace")
    if cache_key is None:
        _CACHE_STATS.misses += 1
        return _parse_tree(source_bytes), source_bytes

    content_hash = _source_hash(source_bytes)
    cached = _TREE_CACHE.get(cache_key)
    if cached is not None:
        cached_tree, cached_hash = cached
        if cached_hash == content_hash:
            _CACHE_STATS.hits += 1
            return cached_tree, source_bytes

    _CACHE_STATS.misses += 1
    tree = _parse_tree(source_bytes)
    if len(_TREE_CACHE) >= _MAX_TREE_CACHE_ENTRIES and cache_key not in _TREE_CACHE:
        oldest = next(iter(_TREE_CACHE))
        del _TREE_CACHE[oldest]
        _CACHE_STATS.evictions += 1
    _TREE_CACHE[cache_key] = (tree, content_hash)
    return tree, source_bytes


def parse_python_tree(source: str, *, cache_key: str | None = None) -> Tree | None:
    """Parse and optionally cache a tree-sitter Python tree.

    Returns:
    -------
    Tree | None
        Parsed syntax tree, or ``None`` when unavailable or parsing fails.
    """
    if not is_tree_sitter_python_available():
        return None
    try:
        tree, _ = _get_tree(source, cache_key=cache_key)
    except _ENRICHMENT_ERRORS:
        return None
    return tree


def clear_tree_sitter_python_cache() -> None:
    """Clear parser/query caches and reset observability counters."""
    _TREE_CACHE.clear()
    _python_language.cache_clear()
    _compile_query.cache_clear()
    _CACHE_STATS.hits = 0
    _CACHE_STATS.misses = 0
    _CACHE_STATS.evictions = 0


def get_tree_sitter_python_cache_stats() -> dict[str, int]:
    """Return tree-sitter Python cache counters."""
    return {
        "cache_hits": _CACHE_STATS.hits,
        "cache_misses": _CACHE_STATS.misses,
        "cache_evictions": _CACHE_STATS.evictions,
    }


def _query_sources() -> dict[str, str]:
    if not _QUERY_ROOT.exists():
        return {}
    sources: dict[str, str] = {}
    for path in sorted(_QUERY_ROOT.glob("*.scm")):
        sources[path.name] = path.read_text(encoding="utf-8")
    return sources


@lru_cache(maxsize=16)
def _compile_query(_filename: str, source: str) -> Query:
    if _TreeSitterQuery is None:
        msg = "tree_sitter query bindings are unavailable"
        raise RuntimeError(msg)
    return _TreeSitterQuery(_python_language(), source)


def _safe_cursor_captures(
    query: Query,
    root: Node,
    *,
    byte_start: int,
    byte_end: int,
    match_limit: int,
) -> dict[str, list[Node]]:
    if _TreeSitterQueryCursor is None:
        msg = "tree_sitter query cursor bindings are unavailable"
        raise RuntimeError(msg)
    cursor: QueryCursor = _TreeSitterQueryCursor(query, match_limit=match_limit)
    cursor.set_byte_range(byte_start, byte_end)
    captures = cursor.captures(root)
    if not isinstance(captures, dict):
        return {}
    result: dict[str, list[Node]] = {}
    for name, nodes in captures.items():
        if not isinstance(name, str):
            continue
        if isinstance(nodes, list):
            result[name] = [node for node in nodes if hasattr(node, "start_byte")]
    return result


def _node_text(node: Node, source_bytes: bytes) -> str:
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", start))
    if end <= start:
        return ""
    return source_bytes[start:end].decode("utf-8", errors="replace").strip()


def _extract_parse_quality(
    root: Node,
    source_bytes: bytes,
    *,
    byte_start: int,
    byte_end: int,
) -> dict[str, object]:
    quality: dict[str, object] = {"has_error": bool(getattr(root, "has_error", False))}
    captures: dict[str, list[Node]]
    try:
        query = _compile_query("__errors__.scm", "(ERROR) @error (MISSING) @missing")
        captures = _safe_cursor_captures(
            query,
            root,
            byte_start=byte_start,
            byte_end=byte_end,
            match_limit=1_024,
        )
    except _ENRICHMENT_ERRORS:
        captures = {}

    def _collect(name: str) -> list[str]:
        nodes = captures.get(name, [])
        rows: list[str] = []
        for node in nodes[:_MAX_CAPTURE_ITEMS]:
            text = _node_text(node, source_bytes)
            if text:
                rows.append(text[:_MAX_CAPTURE_TEXT_LEN])
            else:
                rows.append(str(getattr(node, "type", "unknown")))
        return rows

    quality["error_nodes"] = _collect("error")
    quality["missing_nodes"] = _collect("missing")
    return quality


def _capture_gap_fill_fields(
    root: Node,
    source_bytes: bytes,
    *,
    byte_start: int,
    byte_end: int,
    match_limit: int,
) -> dict[str, object]:
    payload: dict[str, object] = {}
    for filename, source in _query_sources().items():
        try:
            query = _compile_query(filename, source)
            captures = _safe_cursor_captures(
                query,
                root,
                byte_start=byte_start,
                byte_end=byte_end,
                match_limit=match_limit,
            )
        except _ENRICHMENT_ERRORS:
            continue
        _apply_gap_fill_from_captures(payload, captures, source_bytes)
    return payload


def _apply_gap_fill_from_captures(
    payload: dict[str, object],
    captures: dict[str, list[Node]],
    source_bytes: bytes,
) -> None:
    if "call_target" not in payload:
        call_target = _capture_call_target(captures.get("call.expression", []), source_bytes)
        if call_target is not None:
            payload["call_target"] = call_target
    if "enclosing_callable" not in payload:
        callable_name = _capture_named_definition(
            captures.get("def.function", []),
            source_bytes,
        )
        if callable_name is not None:
            payload["enclosing_callable"] = callable_name
    if "enclosing_class" not in payload:
        class_name = _capture_named_definition(
            captures.get("class.definition", []),
            source_bytes,
        )
        if class_name is not None:
            payload["enclosing_class"] = class_name


def _capture_call_target(nodes: list[Node], source_bytes: bytes) -> str | None:
    if not nodes:
        return None
    function_node = nodes[0].child_by_field_name("function")
    if function_node is None:
        return None
    text = _node_text(function_node, source_bytes)
    if not text:
        return None
    return text[:_MAX_CAPTURE_TEXT_LEN]


def _capture_named_definition(nodes: list[Node], source_bytes: bytes) -> str | None:
    if not nodes:
        return None
    name_node = nodes[0].child_by_field_name("name")
    if name_node is None:
        return None
    name = _node_text(name_node, source_bytes)
    if not name:
        return None
    return name


def _default_parse_quality() -> dict[str, object]:
    return {"has_error": False, "error_nodes": list[str](), "missing_nodes": list[str]()}


def enrich_python_context_by_byte_range(
    source: str,
    *,
    byte_start: int,
    byte_end: int,
    cache_key: str | None = None,
    match_limit: int = _DEFAULT_MATCH_LIMIT,
) -> dict[str, object] | None:
    """Best-effort tree-sitter Python enrichment for a byte-anchored match.

    Returns:
    -------
    dict[str, object] | None
        Enrichment payload, or ``None`` when enrichment is unavailable.
    """
    if byte_start < 0 or byte_end <= byte_start:
        return None
    if not is_tree_sitter_python_available():
        return None

    source_bytes = source.encode("utf-8", errors="replace")
    if byte_end > len(source_bytes):
        return None

    payload: dict[str, object] = {
        "language": "python",
        "enrichment_status": "applied",
        "enrichment_sources": ["tree_sitter"],
    }

    if len(source_bytes) > _MAX_SOURCE_BYTES:
        payload["enrichment_status"] = "skipped"
        payload["degrade_reason"] = "source_too_large"
        payload["parse_quality"] = _default_parse_quality()
        return payload

    try:
        tree, source_bytes = _get_tree(source, cache_key=cache_key)
    except _ENRICHMENT_ERRORS as exc:
        payload["enrichment_status"] = "degraded"
        payload["degrade_reason"] = type(exc).__name__
        payload["parse_quality"] = _default_parse_quality()
        return payload

    root = tree.root_node
    parse_quality = _extract_parse_quality(
        root,
        source_bytes,
        byte_start=byte_start,
        byte_end=byte_end,
    )
    payload["parse_quality"] = parse_quality

    try:
        payload.update(
            _capture_gap_fill_fields(
                root,
                source_bytes,
                byte_start=byte_start,
                byte_end=byte_end,
                match_limit=match_limit,
            )
        )
    except _ENRICHMENT_ERRORS as exc:
        payload["enrichment_status"] = "degraded"
        payload["degrade_reason"] = type(exc).__name__
    return payload


def lint_python_query_packs() -> list[str]:
    """Validate query packs by compile and by node/field references.

    Returns:
    -------
    list[str]
        Validation errors found in the query pack set.
    """
    errors: list[str] = []
    if not is_tree_sitter_python_available():
        return errors
    language = _python_language()
    node_pattern = re.compile(r"\(([A-Za-z_][A-Za-z0-9_]*)")
    field_pattern = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*:")
    named_node_kind = True
    unnamed_node_kind = False

    for filename, source in _query_sources().items():
        try:
            _compile_query(filename, source)
        except _COMPILE_ERRORS as exc:
            errors.append(f"{filename}: compile_error:{type(exc).__name__}")
            continue

        for node_kind in node_pattern.findall(source):
            if node_kind in {"ERROR", "MISSING", "_"}:
                continue
            if (
                language.id_for_node_kind(node_kind, named_node_kind) is None
                and language.id_for_node_kind(node_kind, unnamed_node_kind) is None
            ):
                errors.append(f"{filename}: unknown_node:{node_kind}")

        errors.extend(
            f"{filename}: unknown_field:{field_name}"
            for field_name in field_pattern.findall(source)
            if language.field_id_for_name(field_name) is None
        )
    return errors


__all__ = [
    "clear_tree_sitter_python_cache",
    "enrich_python_context_by_byte_range",
    "get_tree_sitter_python_cache_stats",
    "is_tree_sitter_python_available",
    "lint_python_query_packs",
    "parse_python_tree",
]
