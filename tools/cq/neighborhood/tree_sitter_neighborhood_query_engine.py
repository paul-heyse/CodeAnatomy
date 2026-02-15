"""Query-driven neighborhood extraction utilities."""

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

from tools.cq.search.tree_sitter.contracts.core_models import (
    QueryExecutionSettingsV1,
    QueryWindowV1,
)
from tools.cq.search.tree_sitter.core.node_utils import node_text
from tools.cq.search.tree_sitter.core.runtime import run_bounded_query_matches
from tools.cq.search.tree_sitter.query.compiler import compile_query
from tools.cq.search.tree_sitter.query.support import query_pack_path

if TYPE_CHECKING:
    from tree_sitter import Node, Query


@lru_cache(maxsize=16)
def _load_query(language: str, pack_name: str) -> Query | None:
    """Load and compile a query pack from disk.

    Returns:
    -------
    Query | None
        Compiled query, or ``None`` when unavailable or invalid.
    """
    path = query_pack_path(language, pack_name)
    if not path.exists():
        return None
    try:
        source = path.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        return compile_query(
            language=language,
            pack_name=pack_name,
            source=source,
            request_surface="artifact",
            validate_rules=False,
        )
    except (RuntimeError, TypeError, ValueError):
        return None


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
    query = _load_query(language, "10_calls.scm")
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
        window_mode="containment_preferred",
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
        callee_text = node_text(callee_nodes[0], source_bytes)
        if not isinstance(callee_text, str):
            continue
        if callee_text == anchor_name:
            callers.append(site_nodes[0])

    callees: list[Node] = []
    for _idx, capture_map in subtree_matches:
        callee_nodes = capture_map.get("call.callee")
        if not isinstance(callee_nodes, list) or not callee_nodes:
            continue
        callees.append(callee_nodes[0])

    return callers, callees


__all__ = ["collect_callers_callees"]
