"""Optional Rust context enrichment using ``tree-sitter-rust``.

This module is best-effort only. Any parser or runtime failure must degrade
to ``None`` so core search/query behavior remains unchanged.
"""

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tree_sitter import Language, Node, Parser, Tree

try:
    import tree_sitter_rust as _tree_sitter_rust
    from tree_sitter import Language as _TreeSitterLanguage
    from tree_sitter import Parser as _TreeSitterParser
    from tree_sitter import Point as _TreeSitterPoint
except ImportError:  # pragma: no cover - exercised via availability checks
    _tree_sitter_rust = None
    _TreeSitterLanguage = None
    _TreeSitterParser = None
    _TreeSitterPoint = None

_SCOPE_KINDS: tuple[str, ...] = (
    "function_item",
    "struct_item",
    "enum_item",
    "trait_item",
    "impl_item",
    "mod_item",
    "macro_invocation",
)

_TREE_CACHE: dict[str, Tree] = {}
_DEFAULT_SCOPE_DEPTH = 24
_ENRICHMENT_ERRORS = (RuntimeError, TypeError, ValueError, AttributeError, UnicodeError)


def is_tree_sitter_rust_available() -> bool:
    """Return whether tree-sitter Rust enrichment dependencies are available.

    Returns
    -------
    bool
        True when runtime dependencies for Rust tree-sitter enrichment exist.
    """
    return all(
        obj is not None
        for obj in (_tree_sitter_rust, _TreeSitterLanguage, _TreeSitterParser, _TreeSitterPoint)
    )


@lru_cache(maxsize=1)
def _rust_language() -> Language:
    if _tree_sitter_rust is None or _TreeSitterLanguage is None:
        msg = "tree_sitter_rust language bindings are unavailable"
        raise RuntimeError(msg)
    return _TreeSitterLanguage(_tree_sitter_rust.language())


def _make_parser() -> Parser:
    if _TreeSitterParser is None:
        msg = "tree_sitter parser bindings are unavailable"
        raise RuntimeError(msg)
    return _TreeSitterParser(_rust_language())


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
        return _parse_tree(source_bytes), source_bytes

    cached = _TREE_CACHE.get(cache_key)
    if cached is not None:
        return cached, source_bytes

    tree = _parse_tree(source_bytes)
    _TREE_CACHE[cache_key] = tree
    return tree, source_bytes


def clear_tree_sitter_rust_cache() -> None:
    """Clear per-process Rust parser caches."""
    _TREE_CACHE.clear()
    _rust_language.cache_clear()


def _node_text(node: Node | None, source_bytes: bytes) -> str | None:
    if node is None:
        return None
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", 0))
    if end <= start:
        return None
    text = source_bytes[start:end].decode("utf-8", errors="replace").strip()
    return text or None


def _scope_name(scope_node: Node, source_bytes: bytes) -> str | None:
    kind = scope_node.type
    if kind == "impl_item":
        return _node_text(scope_node.child_by_field_name("type"), source_bytes)
    if kind == "macro_invocation":
        return _node_text(scope_node.child_by_field_name("macro"), source_bytes) or _node_text(
            scope_node.child_by_field_name("name"),
            source_bytes,
        )
    return _node_text(scope_node.child_by_field_name("name"), source_bytes)


def _scope_chain(node: Node, source_bytes: bytes, *, max_depth: int) -> list[str]:
    chain: list[str] = []
    current: Node | None = node
    depth = 0
    while current is not None and depth < max_depth:
        if current.type in _SCOPE_KINDS:
            name = _scope_name(current, source_bytes)
            chain.append(f"{current.type}:{name}" if name else current.type)
        current = current.parent
        depth += 1
    return chain


def _find_scope(node: Node, *, max_depth: int) -> Node | None:
    current: Node | None = node
    depth = 0
    while current is not None and depth < max_depth:
        if current.type in _SCOPE_KINDS:
            return current
        current = current.parent
        depth += 1
    return None


def enrich_rust_context(
    source: str,
    *,
    line: int,
    col: int,
    cache_key: str | None = None,
    max_scope_depth: int = _DEFAULT_SCOPE_DEPTH,
) -> dict[str, object] | None:
    """Extract optional Rust context details for a match location.

    Parameters
    ----------
    source
        Full source text of the Rust file.
    line
        1-based line number of the match.
    col
        0-based column of the match.
    cache_key
        Stable file key for parse-tree cache reuse.
    max_scope_depth
        Maximum ancestor levels to inspect.

    Returns
    -------
    dict[str, object] | None
        Best-effort context payload, or ``None`` when unavailable.
    """
    if not is_tree_sitter_rust_available():
        return None
    if line < 1:
        return None

    try:
        tree, source_bytes = _get_tree(source, cache_key=cache_key)
        if _TreeSitterPoint is None:
            return None
        point = _TreeSitterPoint(max(0, line - 1), max(0, col))
        node = tree.root_node.named_descendant_for_point_range(point, point)
        if node is None:
            return None
        scope = _find_scope(node, max_depth=max_scope_depth)
        chain = _scope_chain(node, source_bytes, max_depth=max_scope_depth)
        payload: dict[str, object] = {
            "node_kind": node.type,
            "scope_chain": chain,
        }
        if scope is not None:
            payload["scope_kind"] = scope.type
            scope_name = _scope_name(scope, source_bytes)
            if scope_name:
                payload["scope_name"] = scope_name
    except _ENRICHMENT_ERRORS:
        return None
    return payload


__all__ = [
    "clear_tree_sitter_rust_cache",
    "enrich_rust_context",
    "is_tree_sitter_rust_available",
]
