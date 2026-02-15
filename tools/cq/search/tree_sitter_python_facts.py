"""Tree-sitter-first structural fact extraction for Python search enrichment."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable
from functools import lru_cache
from typing import TYPE_CHECKING

import msgspec

from tools.cq.search.tree_sitter_python import is_tree_sitter_python_available, parse_python_tree
from tools.cq.search.tree_sitter_query_registry import load_query_pack_sources
from tools.cq.search.tree_sitter_runtime import run_bounded_query_captures
from tools.cq.search.tree_sitter_runtime_contracts import (
    QueryExecutionSettingsV1,
    QueryWindowV1,
)

if TYPE_CHECKING:
    from tree_sitter import Language, Node, Query

try:
    import tree_sitter_python as _tree_sitter_python
    from tree_sitter import Language as _TreeSitterLanguage
    from tree_sitter import Query as _TreeSitterQuery
except ImportError:  # pragma: no cover - optional dependency
    _tree_sitter_python = None
    _TreeSitterLanguage = None
    _TreeSitterQuery = None

_MAX_CANDIDATES = 8
_DEFAULT_MATCH_LIMIT = 4_096
_STOP_CONTEXT_KINDS: frozenset[str] = frozenset({"module", "source_file"})


@lru_cache(maxsize=1)
def _python_language() -> Language:
    if _tree_sitter_python is None or _TreeSitterLanguage is None:
        msg = "tree_sitter_python language bindings are unavailable"
        raise RuntimeError(msg)
    return _TreeSitterLanguage(_tree_sitter_python.language())


@lru_cache(maxsize=64)
def _compile_query(pack_name: str, source: str) -> Query:
    if _TreeSitterQuery is None:
        msg = "tree_sitter query bindings are unavailable"
        raise RuntimeError(msg)
    _ = pack_name
    return _TreeSitterQuery(_python_language(), source)


def _node_text(node: Node, source_bytes: bytes) -> str:
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", start))
    if end <= start:
        return ""
    return source_bytes[start:end].decode("utf-8", errors="replace").strip()


def _lift_anchor(node: Node) -> Node:
    current = node
    while current.parent is not None:
        parent = current.parent
        if parent.type in {
            "call",
            "attribute",
            "assignment",
            "import_statement",
            "import_from_statement",
            "function_definition",
            "class_definition",
        }:
            return parent
        current = parent
    return node


def _anchor_call_target(anchor: Node, source_bytes: bytes) -> str | None:
    current = anchor
    while current is not None:
        if current.type == "call":
            function_node = current.child_by_field_name("function")
            if function_node is not None:
                text = _node_text(function_node, source_bytes)
                if text:
                    return text
        if current.type in _STOP_CONTEXT_KINDS or current.parent is None:
            break
        current = current.parent
    return None


def _unique_text(
    captures: dict[str, list[Node]],
    source_bytes: bytes,
    capture_names: Iterable[str],
    *,
    limit: int = _MAX_CANDIDATES,
) -> list[str]:
    seen: OrderedDict[str, None] = OrderedDict()
    for capture_name in capture_names:
        for node in captures.get(capture_name, []):
            text = _node_text(node, source_bytes)
            if not text or text in seen:
                continue
            seen[text] = None
            if len(seen) >= limit:
                return list(seen)
    return list(seen)


def _scope_chain(anchor: Node, source_bytes: bytes) -> list[str]:
    chain: list[str] = []
    current = anchor
    while current.parent is not None:
        current = current.parent
        if current.type not in {"function_definition", "class_definition", "module"}:
            continue
        if current.type == "module":
            chain.append("<module>")
            continue
        name = current.child_by_field_name("name")
        text = _node_text(name, source_bytes) if name is not None else current.type
        if text:
            chain.append(text)
    return list(reversed(chain))


def _find_enclosing(anchor: Node, source_bytes: bytes, kind: str) -> str | None:
    current = anchor
    while current.parent is not None:
        current = current.parent
        if current.type != kind:
            continue
        name = current.child_by_field_name("name")
        if name is None:
            return None
        text = _node_text(name, source_bytes)
        if text:
            return text
    return None


def _parse_quality(captures: dict[str, list[Node]], source_bytes: bytes) -> dict[str, object]:
    errors = _unique_text(captures, source_bytes, ("quality.error", "error"))
    missing = _unique_text(captures, source_bytes, ("quality.missing", "missing"))
    return {
        "has_error": bool(errors or missing),
        "error_nodes": errors,
        "missing_nodes": missing,
    }


def _pack_sources() -> tuple[tuple[str, str], ...]:
    sources = load_query_pack_sources("python", include_distribution=False)
    return tuple(
        (source.pack_name, source.source) for source in sources if source.pack_name.endswith(".scm")
    )


def _collect_query_pack_captures(
    *,
    root: Node,
    window: QueryWindowV1,
    settings: QueryExecutionSettingsV1,
) -> tuple[dict[str, list[Node]], dict[str, object]]:
    all_captures: dict[str, list[Node]] = {}
    telemetry: dict[str, object] = {}
    for pack_name, query_source in _pack_sources():
        try:
            query = _compile_query(pack_name, query_source)
            captures, run_telemetry = run_bounded_query_captures(
                query,
                root,
                windows=(window,),
                settings=settings,
            )
        except (RuntimeError, TypeError, ValueError, AttributeError):
            continue
        telemetry[pack_name] = msgspec.to_builtins(run_telemetry)
        for capture_name, nodes in captures.items():
            bucket = all_captures.setdefault(capture_name, [])
            bucket.extend(nodes)
    return all_captures, telemetry


def _extract_fact_lists(
    all_captures: dict[str, list[Node]],
    source_bytes: bytes,
) -> tuple[list[str], list[str], list[str], list[str], list[str], list[str], list[str]]:
    def_names = _unique_text(
        all_captures,
        source_bytes,
        ("def.function.name", "def.class.name", "class.definition.name"),
    )
    reference_names = _unique_text(
        all_captures,
        source_bytes,
        ("ref.identifier", "ref.attribute", "binding.identifier", "attribute.expr"),
    )
    call_targets = _unique_text(
        all_captures,
        source_bytes,
        (
            "call.target.identifier",
            "call.target.attribute",
            "call.function.identifier",
            "call.function.attribute",
        ),
    )
    import_modules = _unique_text(
        all_captures,
        source_bytes,
        ("import.module", "import.from.module", "import.from.name"),
    )
    import_aliases = _unique_text(
        all_captures,
        source_bytes,
        ("import.alias", "import.from.alias"),
    )
    local_defs = _unique_text(all_captures, source_bytes, ("local.definition", "assignment.target"))
    local_refs = _unique_text(all_captures, source_bytes, ("local.reference", "binding.identifier"))
    return (
        def_names,
        reference_names,
        call_targets,
        import_modules,
        import_aliases,
        local_defs,
        local_refs,
    )


def _build_candidate_rows(names: list[str], *, kind: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for name in names:
        rows.append({"name": name, "source": kind})
        if len(rows) >= _MAX_CANDIDATES:
            break
    return rows


def _build_binding_candidates(names: list[str], scope_chain: list[str]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for name in names:
        rows.append(
            {
                "name": name,
                "scope": scope_chain[-1] if scope_chain else None,
                "scope_type": "tree_sitter",
                "kind": "tree_sitter_binding",
            }
        )
        if len(rows) >= _MAX_CANDIDATES:
            break
    return rows


def _build_payload(
    *,
    anchor: Node,
    source_bytes: bytes,
    captures: dict[str, list[Node]],
    telemetry: dict[str, object],
    facts: tuple[list[str], list[str], list[str], list[str], list[str], list[str], list[str]],
) -> dict[str, object]:
    (
        def_names,
        reference_names,
        call_targets,
        import_modules,
        import_aliases,
        local_defs,
        local_refs,
    ) = facts
    scope_chain = _scope_chain(anchor, source_bytes)
    enclosing_callable = _find_enclosing(anchor, source_bytes, "function_definition")
    enclosing_class = _find_enclosing(anchor, source_bytes, "class_definition")
    parse_quality = _parse_quality(captures, source_bytes)

    qualified_candidates = _build_candidate_rows(
        [*def_names, *call_targets, *reference_names],
        kind="tree_sitter",
    )
    binding_candidates = _build_binding_candidates([*local_defs, *reference_names], scope_chain)
    resolved_call_target = call_targets[0] if call_targets else _anchor_call_target(anchor, source_bytes)
    payload: dict[str, object] = {
        "language": "python",
        "enrichment_status": "applied",
        "enrichment_sources": ["tree_sitter"],
        "node_kind": anchor.type,
        "scope_chain": scope_chain,
        "enclosing_callable": enclosing_callable,
        "class_name": enclosing_class,
        "call_target": resolved_call_target,
        "imports": {
            "modules": import_modules,
            "aliases": import_aliases,
        },
        "locals": {
            "definitions": local_defs,
            "references": local_refs,
        },
        "resolution": {
            "qualified_name_candidates": qualified_candidates,
            "binding_candidates": binding_candidates,
            "enclosing_class": enclosing_class,
            "import_alias_chain": [
                {"alias": alias, "source": "tree_sitter"} for alias in import_aliases
            ],
        },
        "parse_quality": parse_quality,
        "tree_sitter_query_telemetry": telemetry,
    }
    if parse_quality.get("has_error") is True:
        payload["degrade_reason"] = "parse_error"
        payload["enrichment_status"] = "degraded"
    return payload


def build_python_tree_sitter_facts(
    source: str,
    *,
    byte_start: int,
    byte_end: int,
    cache_key: str | None = None,
    match_limit: int = _DEFAULT_MATCH_LIMIT,
) -> dict[str, object] | None:
    """Build tree-sitter-first structural facts for one byte range.

    Returns:
    -------
    dict[str, object] | None
        Tree-sitter structural fact payload for the anchored byte range.
    """
    if not is_tree_sitter_python_available() or byte_start < 0 or byte_end <= byte_start:
        return None

    source_bytes = source.encode("utf-8", errors="replace")
    if byte_end > len(source_bytes):
        return None

    tree = parse_python_tree(source, cache_key=cache_key)
    if tree is None:
        return None

    root = tree.root_node
    anchor = root.named_descendant_for_byte_range(byte_start, byte_end)
    if anchor is None:
        return None
    anchor = _lift_anchor(anchor)

    settings = QueryExecutionSettingsV1(match_limit=match_limit)
    anchor_start = int(getattr(anchor, "start_byte", byte_start))
    anchor_end = int(getattr(anchor, "end_byte", byte_end))
    window = QueryWindowV1(
        start_byte=anchor_start if anchor_end > anchor_start else byte_start,
        end_byte=anchor_end if anchor_end > anchor_start else byte_end,
    )
    captures, telemetry = _collect_query_pack_captures(
        root=root,
        window=window,
        settings=settings,
    )
    facts = _extract_fact_lists(captures, source_bytes)
    return _build_payload(
        anchor=anchor,
        source_bytes=source_bytes,
        captures=captures,
        telemetry=telemetry,
        facts=facts,
    )


__all__ = [
    "build_python_tree_sitter_facts",
]
