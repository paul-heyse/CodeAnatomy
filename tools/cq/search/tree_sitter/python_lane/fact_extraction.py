"""Extract structural Python lane facts from tree-sitter nodes."""

from __future__ import annotations

from tools.cq.search.tree_sitter.python_lane.fact_contracts import PythonScopeFactsV1

_SCOPE_KINDS = {"function_definition", "class_definition", "module"}


__all__ = ["extract_python_facts", "extract_scope_facts"]


def _node_name(node: object) -> str | None:
    text_fn = getattr(node, "text", None)
    if callable(text_fn):
        try:
            value = text_fn()
        except (RuntimeError, TypeError, ValueError):
            return None
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace").strip() or None
        if isinstance(value, str):
            return value.strip() or None
    return None


def extract_scope_facts(node: object, *, source_bytes: bytes) -> PythonScopeFactsV1:
    """Extract scope metadata by walking parent chain.

    Returns:
        PythonScopeFactsV1: Scope facts for the given node.
    """
    _ = source_bytes
    chain: list[str] = []
    current = node
    scope_kind: str | None = None
    scope_name: str | None = None
    while current is not None:
        kind = str(getattr(current, "type", ""))
        if kind in _SCOPE_KINDS:
            if scope_kind is None:
                scope_kind = kind
                scope_name = _node_name(current)
            chain.append(kind)
        current = getattr(current, "parent", None)
    return PythonScopeFactsV1(
        scope_kind=scope_kind,
        scope_name=scope_name,
        scope_chain=tuple(chain),
    )


def extract_python_facts(node: object, *, source_bytes: bytes) -> dict[str, object]:
    """Extract basic structural facts for one node.

    Returns:
        dict[str, object]: Render-friendly structural fact mapping.
    """
    scope = extract_scope_facts(node, source_bytes=source_bytes)
    return {
        "node_kind": str(getattr(node, "type", "unknown")),
        "scope_kind": scope.scope_kind,
        "scope_name": scope.scope_name,
        "scope_chain": list(scope.scope_chain),
    }
