"""Query-driven neighborhood extraction utilities."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.search.tree_sitter_runtime import run_bounded_query_matches
from tools.cq.search.tree_sitter_runtime_contracts import QueryExecutionSettingsV1, QueryWindowV1

if TYPE_CHECKING:
    from tree_sitter import Language, Node, Query

try:
    import tree_sitter_python as _tree_sitter_python
except ImportError:  # pragma: no cover - optional dependency
    _tree_sitter_python = None

try:
    import tree_sitter_rust as _tree_sitter_rust
except ImportError:  # pragma: no cover - optional dependency
    _tree_sitter_rust = None

try:
    from tree_sitter import Language as _TreeSitterLanguage
    from tree_sitter import Query as _TreeSitterQuery
except ImportError:  # pragma: no cover - optional dependency
    _TreeSitterLanguage = None
    _TreeSitterQuery = None


def _node_text(node: Node | None, source_bytes: bytes) -> str | None:
    if node is None:
        return None
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", start))
    if end <= start:
        return None
    text = source_bytes[start:end].decode("utf-8", errors="replace").strip()
    return text or None


def _query_path(language: str, pack_name: str) -> Path:
    return Path(__file__).with_suffix("").parent / "queries" / language / pack_name


@lru_cache(maxsize=2)
def _language(language: str) -> Language:
    if _TreeSitterLanguage is None:
        msg = "tree_sitter language bindings are unavailable"
        raise RuntimeError(msg)
    if language == "python":
        if _tree_sitter_python is None:
            msg = "tree_sitter_python bindings are unavailable"
            raise RuntimeError(msg)
        return _TreeSitterLanguage(_tree_sitter_python.language())
    if _tree_sitter_rust is None:
        msg = "tree_sitter_rust bindings are unavailable"
        raise RuntimeError(msg)
    return _TreeSitterLanguage(_tree_sitter_rust.language())


@lru_cache(maxsize=16)
def _compile_query(language: str, pack_name: str) -> Query | None:
    if _TreeSitterQuery is None:
        return None
    path = _query_path(language, pack_name)
    if not path.exists():
        return None
    try:
        source = path.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        return _TreeSitterQuery(_language(language), source)
    except (RuntimeError, TypeError, ValueError):
        return None


def _matches_name(callee_text: str, anchor_name: str) -> bool:
    if callee_text == anchor_name:
        return True
    return callee_text.endswith((f".{anchor_name}", f"::{anchor_name}"))


def collect_callers_callees(
    *,
    language: str,
    tree_root: Node,
    anchor: Node,
    source_bytes: bytes,
    anchor_name: str,
) -> tuple[list[Node], list[Node]]:
    """Collect caller and callee nodes using neighborhood query packs.

    Returns:
        tuple[list[Node], list[Node]]: A tuple of ``(callers, callees)`` nodes.
    """
    query = _compile_query(language, "10_calls.scm")
    if query is None or not anchor_name:
        return [], []

    global_window = QueryWindowV1(
        start_byte=int(getattr(tree_root, "start_byte", 0)),
        end_byte=int(getattr(tree_root, "end_byte", 0)),
    )
    subtree_window = QueryWindowV1(
        start_byte=int(getattr(anchor, "start_byte", 0)),
        end_byte=int(getattr(anchor, "end_byte", 0)),
    )
    settings = QueryExecutionSettingsV1(
        match_limit=4096,
        require_containment=True,
    )
    all_matches, _ = run_bounded_query_matches(
        query,
        tree_root,
        windows=(global_window,),
        settings=settings,
    )
    subtree_matches, _ = run_bounded_query_matches(
        query,
        tree_root,
        windows=(subtree_window,),
        settings=settings,
    )

    callers: list[Node] = []
    for _idx, capture_map in all_matches:
        callee_nodes = capture_map.get("call.callee")
        site_nodes = capture_map.get("call.site")
        if not isinstance(callee_nodes, list) or not callee_nodes:
            continue
        if not isinstance(site_nodes, list) or not site_nodes:
            continue
        callee_text = _node_text(callee_nodes[0], source_bytes)
        if not isinstance(callee_text, str):
            continue
        if _matches_name(callee_text, anchor_name):
            callers.append(site_nodes[0])

    callees: list[Node] = []
    for _idx, capture_map in subtree_matches:
        callee_nodes = capture_map.get("call.callee")
        if not isinstance(callee_nodes, list) or not callee_nodes:
            continue
        callees.append(callee_nodes[0])

    return callers, callees


__all__ = ["collect_callers_callees"]
