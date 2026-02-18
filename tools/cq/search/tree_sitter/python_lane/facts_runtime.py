"""Tree-sitter-first structural fact extraction for Python search enrichment."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING, cast

import msgspec

from tools.cq.search.tree_sitter.contracts.core_models import (
    ObjectEvidenceRowV1,
    QueryExecutionSettingsV1,
    QueryWindowV1,
    TreeSitterDiagnosticV1,
    TreeSitterQueryHitV1,
)
from tools.cq.search.tree_sitter.contracts.lane_payloads import canonicalize_python_lane_payload
from tools.cq.search.tree_sitter.contracts.query_models import QueryPackPlanV1
from tools.cq.search.tree_sitter.core.infrastructure import child_by_field
from tools.cq.search.tree_sitter.core.lane_support import build_query_windows, lift_anchor
from tools.cq.search.tree_sitter.core.node_utils import node_text
from tools.cq.search.tree_sitter.core.query_pack_executor import (
    QueryPackExecutionContextV1,
    execute_pack_rows,
)
from tools.cq.search.tree_sitter.core.work_queue import enqueue_windows
from tools.cq.search.tree_sitter.python_lane.constants import (
    DEFAULT_MATCH_LIMIT as _DEFAULT_MATCH_LIMIT,
)
from tools.cq.search.tree_sitter.python_lane.constants import (
    PYTHON_LIFT_ANCHOR_TYPES as _PYTHON_LIFT_ANCHOR_TYPES,
)
from tools.cq.search.tree_sitter.python_lane.constants import (
    STOP_CONTEXT_KINDS as _STOP_CONTEXT_KINDS,
)
from tools.cq.search.tree_sitter.python_lane.constants import (
    get_python_field_ids,
)
from tools.cq.search.tree_sitter.python_lane.locals_index import (
    build_locals_index,
    scope_chain_for_anchor,
)
from tools.cq.search.tree_sitter.python_lane.runtime_engine import (
    is_tree_sitter_python_available,
    parse_python_tree_with_ranges,
)
from tools.cq.search.tree_sitter.query.compiler import compile_query
from tools.cq.search.tree_sitter.query.planner import (
    compile_pack_source_rows,
    normalize_pack_source_rows,
)
from tools.cq.search.tree_sitter.query.predicates import (
    has_custom_predicates,
    make_query_predicate,
)
from tools.cq.search.tree_sitter.query.registry import load_query_pack_sources
from tools.cq.search.tree_sitter.structural.exports import collect_diagnostic_rows

if TYPE_CHECKING:
    from tree_sitter import Node

_MAX_CANDIDATES = 8


@dataclass(frozen=True, slots=True)
class _PythonQueryArtifactsV1:
    query_hits: tuple[TreeSitterQueryHitV1, ...]
    diagnostics: tuple[TreeSitterDiagnosticV1, ...]
    telemetry: dict[str, object]


@dataclass(frozen=True, slots=True)
class _PythonFactListBundleV1:
    def_names: list[str]
    reference_names: list[str]
    call_targets: list[str]
    import_modules: list[str]
    import_aliases: list[str]
    local_defs: list[str]
    local_refs: list[str]

    @classmethod
    def from_tuple(
        cls,
        facts: tuple[list[str], list[str], list[str], list[str], list[str], list[str], list[str]],
    ) -> _PythonFactListBundleV1:
        return cls(
            def_names=facts[0],
            reference_names=facts[1],
            call_targets=facts[2],
            import_modules=facts[3],
            import_aliases=facts[4],
            local_defs=facts[5],
            local_refs=facts[6],
        )


@dataclass(frozen=True, slots=True)
class _PythonPayloadContextV1:
    scope_chain: list[str]
    enclosing_callable: str | None
    enclosing_class: str | None
    parse_quality: dict[str, object]
    local_index_rows: tuple[object, ...]
    qualified_candidates: list[dict[str, object]]
    binding_candidates: list[dict[str, object]]
    resolved_call_target: str | None


@dataclass(frozen=True, slots=True)
class _PreparedPythonFactInputV1:
    source_bytes: bytes
    root: Node
    anchor: Node
    windows: tuple[QueryWindowV1, ...]
    settings: QueryExecutionSettingsV1


def _find_child_at_byte(node: Node, byte_offset: int) -> Node | None:
    """Find a child covering a byte offset via byte-seek APIs with fallback.

    Returns:
        Node | None: Function return value.
    """
    first_named = getattr(node, "first_named_child_for_byte", None)
    if callable(first_named):
        try:
            child = first_named(byte_offset)
        except (TypeError, ValueError, RuntimeError, AttributeError):
            child = None
        if child is not None:
            return cast("Node", child)
    first_child = getattr(node, "first_child_for_byte", None)
    if callable(first_child):
        try:
            child = first_child(byte_offset)
        except (TypeError, ValueError, RuntimeError, AttributeError):
            child = None
        if child is not None:
            return cast("Node", child)
    for child in getattr(node, "named_children", ()):
        start_byte = int(getattr(child, "start_byte", 0))
        end_byte = int(getattr(child, "end_byte", start_byte))
        if start_byte <= byte_offset < end_byte:
            return cast("Node", child)
    return None


def _should_enrich_node(node: Node, *, has_change_context: bool) -> bool:
    if not has_change_context:
        return True
    has_changes = getattr(node, "has_changes", None)
    if has_changes is None:
        return True
    try:
        return bool(has_changes)
    except (RuntimeError, TypeError, ValueError, AttributeError):
        return True


def _filter_changed_nodes(
    nodes: list[Node],
    *,
    has_change_context: bool,
) -> list[Node]:
    if not has_change_context:
        return nodes
    return [node for node in nodes if _should_enrich_node(node, has_change_context=True)]


def _anchor_call_target(anchor: Node, source_bytes: bytes) -> str | None:
    current = anchor
    while current is not None:
        if current.type == "call":
            function_node = child_by_field(current, "function", get_python_field_ids())
            if function_node is None:
                function_node = _find_child_at_byte(
                    current,
                    int(getattr(current, "start_byte", 0)),
                )
            if function_node is not None:
                text = node_text(function_node, source_bytes)
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
            text = node_text(node, source_bytes)
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
        name = child_by_field(current, "name", get_python_field_ids())
        if name is None:
            return None
        text = node_text(name, source_bytes)
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
    source_rows = normalize_pack_source_rows(
        (source.pack_name, source.source)
        for source in load_query_pack_sources("python", include_distribution=False)
    )
    return compile_pack_source_rows(
        language="python",
        source_rows=source_rows,
        request_surface="artifact",
    )


def _pack_sources() -> tuple[tuple[str, str], ...]:
    return tuple((pack_name, source) for pack_name, source, _ in _pack_source_rows())


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
    from tools.cq.search.tree_sitter.core.runtime_engine import QueryExecutionCallbacksV1

    all_captures: dict[str, list[Node]] = {}
    all_rows: list[ObjectEvidenceRowV1] = []
    all_hits: list[TreeSitterQueryHitV1] = []
    telemetry: dict[str, object] = {}
    for pack_name, query_source in _pack_sources():
        try:
            query = compile_query(
                language="python",
                pack_name=pack_name,
                source=query_source,
                request_surface="artifact",
            )
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
                context=QueryPackExecutionContextV1(
                    windows=windows,
                    settings=settings,
                    callbacks=callbacks,
                ),
            )
        except (RuntimeError, TypeError, ValueError, AttributeError):
            continue
        telemetry[pack_name] = {
            "captures": msgspec.to_builtins(run_telemetry),
            "matches": msgspec.to_builtins(match_telemetry),
        }
        for capture_name, nodes in captures.items():
            bucket = all_captures.setdefault(capture_name, [])
            bucket.extend(
                _filter_changed_nodes(
                    nodes,
                    has_change_context=settings.has_change_context,
                )
            )
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


def _build_payload_context(
    *,
    anchor: Node,
    source_bytes: bytes,
    captures: dict[str, list[Node]],
    facts: _PythonFactListBundleV1,
) -> _PythonPayloadContextV1:
    local_scope_nodes: list[Node] = captures.get("local.scope") or []
    scope_chain = scope_chain_for_anchor(
        anchor=anchor,
        scopes=local_scope_nodes,
        source_bytes=source_bytes,
    )
    enclosing_class = _find_enclosing(anchor, source_bytes, "class_definition")
    local_definition_nodes: list[Node] = captures.get("local.definition") or []
    local_index_rows = tuple(
        build_locals_index(
            definitions=local_definition_nodes,
            scopes=local_scope_nodes,
            source_bytes=source_bytes,
        )
    )
    return _PythonPayloadContextV1(
        scope_chain=scope_chain,
        enclosing_callable=_find_enclosing(anchor, source_bytes, "function_definition"),
        enclosing_class=enclosing_class,
        parse_quality=_parse_quality(captures, source_bytes),
        local_index_rows=local_index_rows,
        qualified_candidates=_build_candidate_rows(
            [*facts.def_names, *facts.call_targets, *facts.reference_names],
            kind="tree_sitter",
        ),
        binding_candidates=_build_binding_candidates(
            [*facts.local_defs, *facts.reference_names],
            scope_chain,
        ),
        resolved_call_target=(
            facts.call_targets[0]
            if facts.call_targets
            else _anchor_call_target(anchor, source_bytes)
        ),
    )


def _build_python_payload(
    *,
    anchor: Node,
    facts: _PythonFactListBundleV1,
    context: _PythonPayloadContextV1,
    artifacts: _PythonQueryArtifactsV1,
) -> dict[str, object]:
    return {
        "language": "python",
        "enrichment_status": "applied",
        "enrichment_sources": ["tree_sitter"],
        "node_kind": anchor.type,
        "scope_chain": context.scope_chain,
        "enclosing_callable": context.enclosing_callable,
        "class_name": context.enclosing_class,
        "call_target": context.resolved_call_target,
        "imports": {
            "modules": facts.import_modules,
            "aliases": facts.import_aliases,
        },
        "locals": {
            "definitions": facts.local_defs,
            "references": facts.local_refs,
            "index": [msgspec.to_builtins(row) for row in context.local_index_rows],
        },
        "resolution": {
            "qualified_name_candidates": context.qualified_candidates,
            "binding_candidates": context.binding_candidates,
            "enclosing_class": context.enclosing_class,
            "import_alias_chain": [
                {"alias": alias, "source": "tree_sitter"} for alias in facts.import_aliases
            ],
        },
        "parse_quality": context.parse_quality,
        "tree_sitter_query_telemetry": artifacts.telemetry,
        "cst_query_hits": [msgspec.to_builtins(row) for row in artifacts.query_hits],
        "cst_diagnostics": [msgspec.to_builtins(row) for row in artifacts.diagnostics],
    }


def _build_payload(
    *,
    anchor: Node,
    source_bytes: bytes,
    captures: dict[str, list[Node]],
    artifacts: _PythonQueryArtifactsV1,
    facts: tuple[list[str], list[str], list[str], list[str], list[str], list[str], list[str]],
) -> dict[str, object]:
    fact_bundle = _PythonFactListBundleV1.from_tuple(facts)
    context = _build_payload_context(
        anchor=anchor,
        source_bytes=source_bytes,
        captures=captures,
        facts=fact_bundle,
    )
    payload = _build_python_payload(
        anchor=anchor,
        facts=fact_bundle,
        context=context,
        artifacts=artifacts,
    )
    if context.parse_quality.get("has_error") is True:
        payload["degrade_reason"] = "parse_error"
        payload["enrichment_status"] = "degraded"
    return canonicalize_python_lane_payload(payload)


def _build_query_settings(
    *,
    changed_ranges: tuple[object, ...],
    match_limit: int,
    query_budget_ms: int | None,
) -> QueryExecutionSettingsV1:
    from tools.cq.search.tree_sitter.core.adaptive_runtime import adaptive_query_budget_ms

    effective_budget_ms = adaptive_query_budget_ms(
        language="python",
        fallback_budget_ms=query_budget_ms if query_budget_ms is not None else 200,
    )
    return QueryExecutionSettingsV1(
        match_limit=match_limit,
        budget_ms=effective_budget_ms,
        has_change_context=bool(changed_ranges),
        window_mode="containment_preferred",
    )


def _resolve_anchor_window(
    *,
    anchor: Node,
    fallback_start: int,
    fallback_end: int,
) -> QueryWindowV1:
    anchor_start = int(getattr(anchor, "start_byte", fallback_start))
    anchor_end = int(getattr(anchor, "end_byte", fallback_end))
    return QueryWindowV1(
        start_byte=anchor_start if anchor_end > anchor_start else fallback_start,
        end_byte=anchor_end if anchor_end > anchor_start else fallback_end,
    )


def _prepare_python_fact_input(
    *,
    source: str,
    byte_start: int,
    byte_end: int,
    cache_key: str | None,
    match_limit: int,
    query_budget_ms: int | None,
) -> _PreparedPythonFactInputV1 | None:
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
    lifted_anchor = lift_anchor(anchor, parent_types=_PYTHON_LIFT_ANCHOR_TYPES)
    anchor_window = _resolve_anchor_window(
        anchor=lifted_anchor,
        fallback_start=byte_start,
        fallback_end=byte_end,
    )
    query_windows = build_query_windows(
        anchor_window=anchor_window,
        source_byte_len=len(source_bytes),
        changed_ranges=changed_ranges,
    )
    enqueue_windows(
        language="python",
        file_key=cache_key or "<memory>",
        windows=query_windows,
    )
    return _PreparedPythonFactInputV1(
        source_bytes=source_bytes,
        root=tree.root_node,
        anchor=lifted_anchor,
        windows=query_windows,
        settings=_build_query_settings(
            changed_ranges=changed_ranges,
            match_limit=match_limit,
            query_budget_ms=query_budget_ms,
        ),
    )


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
    prepared = _prepare_python_fact_input(
        source=source,
        byte_start=byte_start,
        byte_end=byte_end,
        cache_key=cache_key,
        match_limit=match_limit,
        query_budget_ms=query_budget_ms,
    )
    if prepared is None:
        return None
    captures, rows, telemetry, query_hits = _collect_query_pack_captures(
        root=prepared.root,
        source_bytes=prepared.source_bytes,
        windows=prepared.windows,
        settings=prepared.settings,
    )
    diagnostics = collect_diagnostic_rows(
        language="python",
        root=prepared.root,
        windows=prepared.windows,
        match_limit=1024,
    )
    facts = _extract_fact_lists(rows, captures, prepared.source_bytes)
    return _build_payload(
        anchor=prepared.anchor,
        source_bytes=prepared.source_bytes,
        captures=captures,
        artifacts=_PythonQueryArtifactsV1(
            query_hits=query_hits,
            diagnostics=diagnostics,
            telemetry=telemetry,
        ),
        facts=facts,
    )


__all__ = [
    "build_python_tree_sitter_facts",
]
