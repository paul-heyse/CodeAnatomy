"""Optional Python context enrichment using tree-sitter query packs.

This module is best-effort only. Any parser/query failure must degrade to a
payload that never alters ranking, counts, or category classification.
"""

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

import msgspec

from tools.cq.search.tree_sitter_diagnostics import collect_tree_sitter_diagnostics
from tools.cq.search.tree_sitter_pack_contracts import load_pack_rules
from tools.cq.search.tree_sitter_parse_session import clear_parse_session, get_parse_session
from tools.cq.search.tree_sitter_query_registry import load_query_pack_sources
from tools.cq.search.tree_sitter_runtime import run_bounded_query_captures
from tools.cq.search.tree_sitter_runtime_contracts import (
    QueryExecutionSettingsV1,
    QueryWindowV1,
)

if TYPE_CHECKING:
    from tree_sitter import Language, Node, Parser, Query, Tree

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

_MAX_SOURCE_BYTES = 5 * 1024 * 1024
_MAX_CAPTURE_ITEMS = 8
_MAX_CAPTURE_TEXT_LEN = 120
_DEFAULT_MATCH_LIMIT = 4_096
_ENRICHMENT_ERRORS = (RuntimeError, TypeError, ValueError, AttributeError, UnicodeError)
_STOP_CONTEXT_KINDS: frozenset[str] = frozenset({"module", "source_file"})


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


def _parse_with_session(source: str, *, cache_key: str | None) -> tuple[Tree | None, bytes]:
    source_bytes = source.encode("utf-8", errors="replace")
    if not is_tree_sitter_python_available():
        return None, source_bytes
    session = get_parse_session(language="python", parser_factory=_make_parser)
    tree, _changed_ranges, _reused = session.parse(file_key=cache_key, source_bytes=source_bytes)
    return tree, source_bytes


def parse_python_tree_with_ranges(
    source: str,
    *,
    cache_key: str | None = None,
) -> tuple[Tree | None, tuple[object, ...]]:
    """Parse source and return tree with changed ranges when incremental parse is used.

    Returns:
        tuple[Tree | None, tuple[object, ...]]: Parsed tree and changed ranges
            (empty when caching is unavailable or no changes are tracked).
    """
    source_bytes = source.encode("utf-8", errors="replace")
    if not is_tree_sitter_python_available():
        return None, ()
    session = get_parse_session(language="python", parser_factory=_make_parser)
    tree, changed_ranges, _reused = session.parse(file_key=cache_key, source_bytes=source_bytes)
    return tree, changed_ranges


def parse_python_tree(source: str, *, cache_key: str | None = None) -> Tree | None:
    """Parse and optionally cache a tree-sitter Python tree.

    Returns:
        Tree | None: Parsed syntax tree, or ``None`` when unavailable or parsing
            fails.
    """
    if not is_tree_sitter_python_available():
        return None
    try:
        tree, _ = _parse_with_session(source, cache_key=cache_key)
    except _ENRICHMENT_ERRORS:
        return None
    return tree


def clear_tree_sitter_python_cache() -> None:
    """Clear parser/query caches and reset observability counters."""
    clear_parse_session(language="python")
    _python_language.cache_clear()
    _compile_query.cache_clear()


def get_tree_sitter_python_cache_stats() -> dict[str, int]:
    """Return tree-sitter Python cache counters."""
    session = get_parse_session(language="python", parser_factory=_make_parser)
    stats = session.stats()
    return {
        "entries": stats.entries,
        "cache_hits": stats.cache_hits,
        "cache_misses": stats.cache_misses,
        "cache_evictions": 0,
        "parse_count": stats.parse_count,
        "reparse_count": stats.reparse_count,
        "edit_failures": stats.edit_failures,
    }


def _query_sources() -> dict[str, str]:
    sources: dict[str, str] = {}
    for source in load_query_pack_sources("python", include_distribution=False):
        if source.pack_name.endswith(".scm"):
            sources[source.pack_name] = source.source
    return sources


@lru_cache(maxsize=16)
def _compile_query(_filename: str, source: str) -> Query:
    if _TreeSitterQuery is None:
        msg = "tree_sitter query bindings are unavailable"
        raise RuntimeError(msg)
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
    return query


def _safe_cursor_captures(
    query: Query,
    root: Node,
    *,
    byte_start: int,
    byte_end: int,
    match_limit: int,
) -> tuple[dict[str, list[Node]], bool]:
    captures, telemetry = run_bounded_query_captures(
        query,
        root,
        windows=(QueryWindowV1(start_byte=byte_start, end_byte=byte_end),),
        settings=QueryExecutionSettingsV1(match_limit=match_limit),
    )
    return captures, telemetry.exceeded_match_limit


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
    did_exceed_match_limit = False
    try:
        query = _compile_query("__errors__.scm", "(ERROR) @error (MISSING) @missing")
        captures, did_exceed_match_limit = _safe_cursor_captures(
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
    quality["did_exceed_match_limit"] = did_exceed_match_limit
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
    did_exceed_match_limit = False
    for filename, source in _query_sources().items():
        try:
            query = _compile_query(filename, source)
            captures, exceeded = _safe_cursor_captures(
                query,
                root,
                byte_start=byte_start,
                byte_end=byte_end,
                match_limit=match_limit,
            )
            did_exceed_match_limit = did_exceed_match_limit or exceeded
        except _ENRICHMENT_ERRORS:
            continue
        _apply_gap_fill_from_captures(payload, captures, source_bytes)
    _finalize_gap_fill_payload(payload)
    payload["query_runtime"] = {
        "match_limit": match_limit,
        "did_exceed_match_limit": did_exceed_match_limit,
        "cancelled": False,
    }
    return payload


def _set_payload_field(
    payload: dict[str, object],
    *,
    key: str,
    value: object | None,
) -> None:
    if key in payload:
        return
    if value is None:
        return
    if isinstance(value, (list, dict)) and not value:
        return
    payload[key] = value


def _apply_gap_fill_from_captures(
    payload: dict[str, object],
    captures: dict[str, list[Node]],
    source_bytes: bytes,
) -> None:
    _set_payload_field(
        payload,
        key="call_target",
        value=_capture_call_target(captures, source_bytes),
    )
    _set_payload_field(
        payload,
        key="enclosing_callable",
        value=_capture_named_definition(
            captures.get("def.function.name", []),
            captures.get("def.function", []),
            source_bytes,
        ),
    )
    _set_payload_field(
        payload,
        key="enclosing_class",
        value=_capture_named_definition(
            captures.get("class.definition.name", []),
            captures.get("class.definition", []),
            source_bytes,
        ),
    )
    _set_payload_field(
        payload,
        key="import_alias_chain",
        value=_capture_import_alias_chain(captures, source_bytes),
    )
    _set_payload_field(
        payload,
        key="binding_candidates",
        value=_capture_binding_candidates(captures, source_bytes),
    )
    _set_payload_field(
        payload,
        key="qualified_name_candidates",
        value=_capture_qualified_name_candidates(captures, source_bytes),
    )


def _finalize_gap_fill_payload(payload: dict[str, object]) -> None:
    payload.setdefault("qualified_name_candidates", [])
    payload.setdefault("binding_candidates", [])
    payload.setdefault("import_alias_chain", [])
    call_target = payload.get("call_target")
    if not payload["qualified_name_candidates"] and isinstance(call_target, str) and call_target:
        payload["qualified_name_candidates"] = [{"name": call_target, "source": "tree_sitter"}]
    if not payload["binding_candidates"]:
        binding_candidate = _binding_candidate_from_call_target(call_target)
        if binding_candidate is not None:
            payload["binding_candidates"] = [binding_candidate]


def _binding_candidate_from_call_target(call_target: object) -> dict[str, object] | None:
    if not isinstance(call_target, str):
        return None
    head = call_target.split(".", maxsplit=1)[0].strip()
    if not head:
        return None
    return {
        "name": head[:_MAX_CAPTURE_TEXT_LEN],
        "kind": "tree_sitter_call_target",
        "byte_start": -1,
    }


def _capture_call_target(captures: dict[str, list[Node]], source_bytes: bytes) -> str | None:
    for capture_name in (
        "call.target.identifier",
        "call.target.attribute",
        "call.function.identifier",
        "call.function.attribute",
    ):
        nodes = captures.get(capture_name, [])
        if not nodes:
            continue
        text = _node_text(nodes[0], source_bytes)
        if text:
            return text[:_MAX_CAPTURE_TEXT_LEN]

    call_nodes = captures.get("call.expression", [])
    if not call_nodes:
        return None
    function_node = call_nodes[0].child_by_field_name("function")
    if function_node is None:
        return None
    text = _node_text(function_node, source_bytes)
    return text[:_MAX_CAPTURE_TEXT_LEN] if text else None


def _capture_named_definition(
    name_nodes: list[Node],
    fallback_nodes: list[Node],
    source_bytes: bytes,
) -> str | None:
    if name_nodes:
        name = _node_text(name_nodes[0], source_bytes)
        if name:
            return name
    if not fallback_nodes:
        return None
    name_node = fallback_nodes[0].child_by_field_name("name")
    if name_node is None:
        return None
    name = _node_text(name_node, source_bytes)
    return name or None


def _capture_text_rows(
    captures: dict[str, list[Node]],
    capture_names: tuple[str, ...],
    source_bytes: bytes,
) -> list[str]:
    rows: list[str] = []
    for name in capture_names:
        nodes = captures.get(name, [])
        for node in nodes:
            text = _node_text(node, source_bytes)
            if text:
                rows.append(text[:_MAX_CAPTURE_TEXT_LEN])
            if len(rows) >= _MAX_CAPTURE_ITEMS:
                return rows
    return rows


def _append_chain_row(chain: list[dict[str, object]], *, key: str, value: str) -> None:
    if not value or len(chain) >= _MAX_CAPTURE_ITEMS:
        return
    row: dict[str, object] = {key: value[:_MAX_CAPTURE_TEXT_LEN]}
    if row not in chain:
        chain.append(row)


def _append_named_import_chain(
    chain: list[dict[str, object]],
    name_node: Node,
    source_bytes: bytes,
) -> None:
    if getattr(name_node, "type", "") != "aliased_import":
        _append_chain_row(chain, key="module", value=_node_text(name_node, source_bytes))
        return

    module_node = name_node.child_by_field_name("name")
    alias_node = name_node.child_by_field_name("alias")
    if module_node is not None:
        _append_chain_row(chain, key="module", value=_node_text(module_node, source_bytes))
    if alias_node is not None:
        _append_chain_row(chain, key="alias", value=_node_text(alias_node, source_bytes))


def _append_import_statement_chain(
    chain: list[dict[str, object]],
    captures: dict[str, list[Node]],
    source_bytes: bytes,
) -> None:
    for statement in captures.get("import.statement", []):
        name_node = statement.child_by_field_name("name")
        if name_node is None:
            continue
        _append_named_import_chain(chain, name_node, source_bytes)
        if len(chain) >= _MAX_CAPTURE_ITEMS:
            return


def _append_import_from_statement_chain(
    chain: list[dict[str, object]],
    captures: dict[str, list[Node]],
    source_bytes: bytes,
) -> None:
    for statement in captures.get("import.from_statement", []):
        module_node = statement.child_by_field_name("module_name")
        if module_node is not None:
            _append_chain_row(chain, key="from", value=_node_text(module_node, source_bytes))
        name_node = statement.child_by_field_name("name")
        if name_node is not None:
            _append_named_import_chain(chain, name_node, source_bytes)
        if len(chain) >= _MAX_CAPTURE_ITEMS:
            return


def _append_module_alias_pairs(
    chain: list[dict[str, object]],
    *,
    modules: list[str],
    aliases: list[str],
) -> None:
    for index, module_name in enumerate(modules[:_MAX_CAPTURE_ITEMS]):
        _append_chain_row(chain, key="module", value=module_name)
        if index < len(aliases):
            _append_chain_row(chain, key="alias", value=aliases[index])
        if len(chain) >= _MAX_CAPTURE_ITEMS:
            return


def _append_alias_only_rows(
    chain: list[dict[str, object]],
    *,
    aliases: list[str],
) -> None:
    for alias in aliases:
        _append_chain_row(chain, key="alias", value=alias)
        if len(chain) >= _MAX_CAPTURE_ITEMS:
            return


def _capture_import_alias_chain(
    captures: dict[str, list[Node]],
    source_bytes: bytes,
) -> list[dict[str, object]]:
    chain: list[dict[str, object]] = []
    _append_import_statement_chain(chain, captures, source_bytes)
    _append_import_from_statement_chain(chain, captures, source_bytes)

    from_modules = _capture_text_rows(captures, ("import.from.module",), source_bytes)
    modules = _capture_text_rows(
        captures,
        ("import.module", "import.from.name"),
        source_bytes,
    )
    aliases = _capture_text_rows(
        captures,
        ("import.alias", "import.from.alias"),
        source_bytes,
    )
    if from_modules:
        _append_chain_row(chain, key="from", value=from_modules[0])

    _append_module_alias_pairs(chain, modules=modules, aliases=aliases)

    if not modules:
        _append_alias_only_rows(chain, aliases=aliases)
    return chain[:_MAX_CAPTURE_ITEMS]


def _capture_binding_candidates(
    captures: dict[str, list[Node]],
    source_bytes: bytes,
) -> list[dict[str, object]]:
    candidates: list[dict[str, object]] = []
    for capture_name, kind in (
        ("assignment.target", "assignment"),
        ("binding.identifier", "identifier"),
    ):
        nodes = captures.get(capture_name, [])
        for node in nodes:
            text = _node_text(node, source_bytes)
            if not text:
                continue
            row: dict[str, object] = {
                "name": text[:_MAX_CAPTURE_TEXT_LEN],
                "kind": f"tree_sitter_{kind}",
                "byte_start": int(getattr(node, "start_byte", -1)),
            }
            candidates.append(row)
            if len(candidates) >= _MAX_CAPTURE_ITEMS:
                return candidates
    return candidates


def _capture_qualified_name_candidates(
    captures: dict[str, list[Node]],
    source_bytes: bytes,
) -> list[dict[str, object]]:
    rows = _capture_text_rows(
        captures,
        (
            "call.target.identifier",
            "call.target.attribute",
            "call.function.identifier",
            "call.function.attribute",
            "ref.identifier",
            "ref.attribute",
            "import.module",
            "import.from.name",
            "def.function.name",
            "class.definition.name",
            "attribute.expr",
        ),
        source_bytes,
    )
    out: list[dict[str, object]] = []
    seen: set[str] = set()
    for text in rows:
        if text in seen:
            continue
        seen.add(text)
        out.append({"name": text, "source": "tree_sitter"})
        if len(out) >= _MAX_CAPTURE_ITEMS:
            break
    return out


def _default_parse_quality() -> dict[str, object]:
    return {
        "has_error": False,
        "error_nodes": list[str](),
        "missing_nodes": list[str](),
        "did_exceed_match_limit": False,
    }


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
        if current.type in _STOP_CONTEXT_KINDS:
            return current
        current = parent
    return node


def _effective_capture_window(root: Node, *, byte_start: int, byte_end: int) -> tuple[int, int]:
    anchor = root.named_descendant_for_byte_range(byte_start, byte_end)
    if anchor is None:
        return byte_start, byte_end
    lifted = _lift_anchor(anchor)
    start = int(getattr(lifted, "start_byte", byte_start))
    end = int(getattr(lifted, "end_byte", byte_end))
    if end <= start:
        return byte_start, byte_end
    return start, end


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
    if byte_start < 0 or byte_end <= byte_start or not is_tree_sitter_python_available():
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
        tree, source_bytes = _parse_with_session(source, cache_key=cache_key)
    except _ENRICHMENT_ERRORS as exc:
        payload["enrichment_status"] = "degraded"
        payload["degrade_reason"] = type(exc).__name__
        payload["parse_quality"] = _default_parse_quality()
        return payload
    if tree is None:
        payload["enrichment_status"] = "degraded"
        payload["degrade_reason"] = "parse_unavailable"
        payload["parse_quality"] = _default_parse_quality()
        return payload

    root = tree.root_node
    window_start, window_end = _effective_capture_window(
        root,
        byte_start=byte_start,
        byte_end=byte_end,
    )
    parse_quality = _extract_parse_quality(
        root,
        source_bytes,
        byte_start=window_start,
        byte_end=window_end,
    )
    payload["parse_quality"] = parse_quality
    diagnostics = collect_tree_sitter_diagnostics(
        language="python",
        root=root,
        windows=(QueryWindowV1(start_byte=window_start, end_byte=window_end),),
        match_limit=1024,
    )
    payload["tree_sitter_diagnostics"] = [msgspec.to_builtins(row) for row in diagnostics]

    try:
        payload.update(
            _capture_gap_fill_fields(
                root,
                source_bytes,
                byte_start=window_start,
                byte_end=window_end,
                match_limit=match_limit,
            )
        )
    except _ENRICHMENT_ERRORS as exc:
        payload["enrichment_status"] = "degraded"
        payload["degrade_reason"] = type(exc).__name__
    return payload


__all__ = [
    "clear_tree_sitter_python_cache",
    "enrich_python_context_by_byte_range",
    "get_tree_sitter_python_cache_stats",
    "is_tree_sitter_python_available",
    "parse_python_tree",
    "parse_python_tree_with_ranges",
]
