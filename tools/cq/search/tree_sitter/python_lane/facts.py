"""Tree-sitter-first structural fact extraction for Python search enrichment."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable
from functools import lru_cache
from typing import TYPE_CHECKING

import msgspec

from tools.cq.search.tree_sitter.contracts.core_models import (
    ObjectEvidenceRowV1,
    QueryExecutionSettingsV1,
    QueryWindowV1,
    TreeSitterDiagnosticV1,
    TreeSitterQueryHitV1,
)
from tools.cq.search.tree_sitter.contracts.lane_payloads import canonicalize_python_lane_payload
from tools.cq.search.tree_sitter.contracts.query_models import QueryPackPlanV1, load_pack_rules
from tools.cq.search.tree_sitter.core.change_windows import (
    contains_window,
    ensure_query_windows,
    windows_from_changed_ranges,
)
from tools.cq.search.tree_sitter.core.query_pack_executor import execute_pack_rows
from tools.cq.search.tree_sitter.core.text_utils import node_text as _ts_node_text
from tools.cq.search.tree_sitter.core.work_queue import enqueue_windows
from tools.cq.search.tree_sitter.python_lane.locals_index import (
    build_locals_index,
    scope_chain_for_anchor,
)
from tools.cq.search.tree_sitter.python_lane.runtime import (
    is_tree_sitter_python_available,
    parse_python_tree_with_ranges,
)
from tools.cq.search.tree_sitter.query.planner import build_pack_plan, sort_pack_plans
from tools.cq.search.tree_sitter.query.predicates import (
    has_custom_predicates,
    make_query_predicate,
)
from tools.cq.search.tree_sitter.query.registry import load_query_pack_sources
from tools.cq.search.tree_sitter.query.specialization import specialize_query
from tools.cq.search.tree_sitter.structural.diagnostic_export import collect_diagnostic_rows

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
def _compile_query(
    pack_name: str,
    source: str,
    *,
    request_surface: str = "artifact",
) -> Query:
    if _TreeSitterQuery is None:
        msg = "tree_sitter query bindings are unavailable"
        raise RuntimeError(msg)
    _ = pack_name
    query = _TreeSitterQuery(_python_language(), source)
    rules = load_pack_rules("python")
    pattern_count = int(getattr(query, "pattern_count", 0))
    for pattern_idx in range(pattern_count):
        if rules.require_rooted and not bool(query.is_pattern_rooted(pattern_idx)):
            msg = f"python query pattern not rooted: {pattern_idx}"
            raise ValueError(msg)
        if rules.forbid_non_local and bool(query.is_pattern_non_local(pattern_idx)):
            msg = f"python query pattern non-local: {pattern_idx}"
            raise ValueError(msg)
    return specialize_query(query, request_surface=request_surface)


def _node_text(node: Node, source_bytes: bytes) -> str:
    return _ts_node_text(node, source_bytes)


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
    errors = _unique_text(captures, source_bytes, ("diag.error", "quality.error", "error"))
    missing = _unique_text(captures, source_bytes, ("diag.missing", "quality.missing", "missing"))
    return {
        "has_error": bool(errors or missing),
        "error_nodes": errors,
        "missing_nodes": missing,
    }


@lru_cache(maxsize=1)
def _pack_source_rows() -> tuple[tuple[str, str, QueryPackPlanV1], ...]:
    sources = load_query_pack_sources("python", include_distribution=False)
    rows = [
        (
            source.pack_name,
            source.source,
            build_pack_plan(
                pack_name=source.pack_name,
                query=_compile_query(
                    source.pack_name,
                    source.source,
                    request_surface="artifact",
                ),
                query_text=source.source,
            ),
        )
        for source in sources
        if source.pack_name.endswith(".scm")
    ]
    return tuple((pack_name, source, plan) for pack_name, source, plan in sort_pack_plans(rows))


def _pack_sources() -> tuple[tuple[str, str], ...]:
    return tuple((pack_name, source) for pack_name, source, _ in _pack_source_rows())


def _build_query_windows(
    *,
    anchor_window: QueryWindowV1,
    source_byte_len: int,
    changed_ranges: tuple[object, ...],
) -> tuple[QueryWindowV1, ...]:
    windows = windows_from_changed_ranges(
        changed_ranges,
        source_byte_len=source_byte_len,
        pad_bytes=96,
    )
    windows = ensure_query_windows(windows, fallback=anchor_window)
    if windows and not contains_window(
        windows,
        value=anchor_window.start_byte,
        width=anchor_window.end_byte - anchor_window.start_byte,
    ):
        return (*windows, anchor_window)
    return windows


def _collect_query_pack_captures(
    *,
    root: Node,
    source_bytes: bytes,
    windows: tuple[QueryWindowV1, ...],
    settings: QueryExecutionSettingsV1,
) -> tuple[
    dict[str, list[Node]],
    tuple[ObjectEvidenceRowV1, ...],
    dict[str, object],
    tuple[TreeSitterQueryHitV1, ...],
]:
    from tools.cq.search.tree_sitter.core.runtime import QueryExecutionCallbacksV1

    all_captures: dict[str, list[Node]] = {}
    all_rows: list[ObjectEvidenceRowV1] = []
    all_hits: list[TreeSitterQueryHitV1] = []
    telemetry: dict[str, object] = {}
    for pack_name, query_source in _pack_sources():
        try:
            query = _compile_query(pack_name, query_source, request_surface="artifact")
            predicate_callback = (
                make_query_predicate(source_bytes=source_bytes)
                if has_custom_predicates(query_source)
                else None
            )
            callbacks = (
                QueryExecutionCallbacksV1(predicate_callback=predicate_callback)
                if predicate_callback is not None
                else None
            )
            captures, pack_rows, pack_hits, run_telemetry, match_telemetry = execute_pack_rows(
                query=query,
                query_name=pack_name,
                root=root,
                source_bytes=source_bytes,
                windows=windows,
                settings=settings,
                callbacks=callbacks,
            )
        except (RuntimeError, TypeError, ValueError, AttributeError):
            continue
        telemetry[pack_name] = {
            "captures": msgspec.to_builtins(run_telemetry),
            "matches": msgspec.to_builtins(match_telemetry),
        }
        for capture_name, nodes in captures.items():
            bucket = all_captures.setdefault(capture_name, [])
            bucket.extend(nodes)
        all_rows.extend(pack_rows)
        all_hits.extend(pack_hits)
    return all_captures, tuple(all_rows), telemetry, tuple(all_hits)


def _extract_fact_lists(
    rows: tuple[ObjectEvidenceRowV1, ...],
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

    _extend_with_rows(
        target=def_names,
        values=_row_capture_values(
            rows=rows,
            emit="definitions",
            capture_keys=("def.function.name", "def.class.name"),
        ),
    )
    _extend_with_rows(
        target=reference_names,
        values=_row_capture_values(
            rows=rows,
            emit="references",
            capture_keys=("ref.identifier", "ref.attribute"),
        ),
    )
    _extend_with_rows(
        target=call_targets,
        values=_row_capture_values(
            rows=rows,
            emit="calls",
            capture_keys=("call.target.identifier", "call.target.attribute"),
        ),
    )
    _extend_with_rows(
        target=import_modules,
        values=_row_capture_values(
            rows=rows,
            emit="imports",
            capture_keys=("import.module", "import.from.module", "import.from.name"),
        ),
    )
    _extend_with_rows(
        target=import_aliases,
        values=_row_capture_values(
            rows=rows,
            emit="imports",
            capture_keys=("import.alias", "import.from.alias"),
        ),
    )
    _extend_with_rows(
        target=local_defs,
        values=_row_capture_values(
            rows=rows,
            emit="locals",
            row_kind="local_definition",
            capture_keys=("local.definition",),
        ),
    )
    _extend_with_rows(
        target=local_refs,
        values=_row_capture_values(
            rows=rows,
            emit="locals",
            row_kind="local_reference",
            capture_keys=("local.reference",),
        ),
    )
    return (
        def_names[:_MAX_CANDIDATES],
        reference_names[:_MAX_CANDIDATES],
        call_targets[:_MAX_CANDIDATES],
        import_modules[:_MAX_CANDIDATES],
        import_aliases[:_MAX_CANDIDATES],
        local_defs[:_MAX_CANDIDATES],
        local_refs[:_MAX_CANDIDATES],
    )


def _row_capture_values(
    *,
    rows: tuple[ObjectEvidenceRowV1, ...],
    emit: str,
    capture_keys: tuple[str, ...],
    row_kind: str | None = None,
) -> list[str]:
    values: list[str] = []
    for row in rows:
        if row.emit != emit:
            continue
        if row_kind is not None and row.kind != row_kind:
            continue
        for key in capture_keys:
            value = row.captures.get(key)
            if isinstance(value, str) and value:
                values.append(value)
                break
    return values


def _extend_with_rows(*, target: list[str], values: list[str]) -> None:
    for value in values:
        if value in target:
            continue
        target.append(value)


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
    query_hits: tuple[TreeSitterQueryHitV1, ...],
    diagnostics: tuple[TreeSitterDiagnosticV1, ...],
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
    local_scope_nodes: list[Node] = captures.get("local.scope") or []
    scope_chain = scope_chain_for_anchor(
        anchor=anchor,
        scopes=local_scope_nodes,
        source_bytes=source_bytes,
    )
    enclosing_callable = _find_enclosing(anchor, source_bytes, "function_definition")
    enclosing_class = _find_enclosing(anchor, source_bytes, "class_definition")
    parse_quality = _parse_quality(captures, source_bytes)
    local_definition_nodes: list[Node] = captures.get("local.definition") or []
    local_index_rows = build_locals_index(
        definitions=local_definition_nodes,
        scopes=local_scope_nodes,
        source_bytes=source_bytes,
    )

    qualified_candidates = _build_candidate_rows(
        [*def_names, *call_targets, *reference_names],
        kind="tree_sitter",
    )
    binding_candidates = _build_binding_candidates([*local_defs, *reference_names], scope_chain)
    resolved_call_target = (
        call_targets[0] if call_targets else _anchor_call_target(anchor, source_bytes)
    )
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
            "index": [msgspec.to_builtins(row) for row in local_index_rows],
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
        "cst_query_hits": [msgspec.to_builtins(row) for row in query_hits],
        "cst_diagnostics": [msgspec.to_builtins(row) for row in diagnostics],
    }
    if parse_quality.get("has_error") is True:
        payload["degrade_reason"] = "parse_error"
        payload["enrichment_status"] = "degraded"
    return canonicalize_python_lane_payload(payload)


def build_python_tree_sitter_facts(
    source: str,
    *,
    byte_start: int,
    byte_end: int,
    cache_key: str | None = None,
    match_limit: int = _DEFAULT_MATCH_LIMIT,
    query_budget_ms: int | None = None,
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

    tree, changed_ranges = parse_python_tree_with_ranges(source, cache_key=cache_key)
    if tree is None:
        return None

    anchor = tree.root_node.named_descendant_for_byte_range(byte_start, byte_end)
    if anchor is None:
        return None
    anchor = _lift_anchor(anchor)

    from tools.cq.search.tree_sitter.core.adaptive_runtime import adaptive_query_budget_ms

    effective_budget_ms = adaptive_query_budget_ms(
        language="python",
        fallback_budget_ms=query_budget_ms if query_budget_ms is not None else 200,
    )
    settings = QueryExecutionSettingsV1(
        match_limit=match_limit,
        budget_ms=effective_budget_ms,
        window_mode="containment_preferred",
    )
    anchor_start = int(getattr(anchor, "start_byte", byte_start))
    anchor_end = int(getattr(anchor, "end_byte", byte_end))
    window = QueryWindowV1(
        start_byte=anchor_start if anchor_end > anchor_start else byte_start,
        end_byte=anchor_end if anchor_end > anchor_start else byte_end,
    )
    query_windows = _build_query_windows(
        anchor_window=window,
        source_byte_len=len(source_bytes),
        changed_ranges=changed_ranges,
    )
    enqueue_windows(
        language="python",
        file_key=cache_key or "<memory>",
        windows=query_windows,
    )
    captures, rows, telemetry, query_hits = _collect_query_pack_captures(
        root=tree.root_node,
        source_bytes=source_bytes,
        windows=query_windows,
        settings=settings,
    )
    diagnostics = collect_diagnostic_rows(
        language="python",
        root=tree.root_node,
        windows=query_windows,
        match_limit=1024,
    )
    facts = _extract_fact_lists(rows, captures, source_bytes)
    return _build_payload(
        anchor=anchor,
        source_bytes=source_bytes,
        captures=captures,
        query_hits=query_hits,
        diagnostics=diagnostics,
        telemetry=telemetry,
        facts=facts,
    )


__all__ = [
    "build_python_tree_sitter_facts",
]
