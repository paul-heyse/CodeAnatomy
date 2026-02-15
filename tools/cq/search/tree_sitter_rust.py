"""Optional Rust context enrichment using ``tree-sitter-rust``.

This module is best-effort only. Any parser or runtime failure must degrade
to ``None`` so core search/query behavior remains unchanged.

Enrichment Contract
-------------------
All fields produced by this module are strictly additive.
They never affect: confidence scores, match counts, category classification,
or relevance ranking.
They may affect: containing_scope display (used only for grouping in output).
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable, Mapping
from functools import lru_cache
from typing import TYPE_CHECKING

import msgspec

from tools.cq.search.tree_sitter_diagnostics import collect_tree_sitter_diagnostics
from tools.cq.search.tree_sitter_injection_runtime import parse_injected_ranges
from tools.cq.search.tree_sitter_injections import build_injection_plan
from tools.cq.search.tree_sitter_match_row_contracts import ObjectEvidenceRowV1
from tools.cq.search.tree_sitter_match_rows import build_match_rows
from tools.cq.search.tree_sitter_pack_contracts import load_pack_rules
from tools.cq.search.tree_sitter_parse_session import clear_parse_session, get_parse_session
from tools.cq.search.tree_sitter_runtime import (
    run_bounded_query_captures,
    run_bounded_query_matches,
)
from tools.cq.search.tree_sitter_runtime_contracts import (
    QueryExecutionSettingsV1,
    QueryWindowV1,
)
from tools.cq.search.tree_sitter_rust_bundle import (
    load_rust_grammar_bundle,
    load_rust_query_sources,
)
from tools.cq.search.tree_sitter_tags import build_tag_events

if TYPE_CHECKING:
    from tree_sitter import Language, Node, Parser, Query, Tree

try:
    import tree_sitter_rust as _tree_sitter_rust
    from tree_sitter import Language as _TreeSitterLanguage
    from tree_sitter import Parser as _TreeSitterParser
    from tree_sitter import Point as _TreeSitterPoint
    from tree_sitter import Query as _TreeSitterQuery
except ImportError:  # pragma: no cover - exercised via availability checks
    _tree_sitter_rust = None
    _TreeSitterLanguage = None
    _TreeSitterParser = None
    _TreeSitterPoint = None
    _TreeSitterQuery = None

_SCOPE_KINDS: tuple[str, ...] = (
    "function_item",
    "struct_item",
    "enum_item",
    "trait_item",
    "impl_item",
    "mod_item",
    "macro_invocation",
)

_DEFAULT_SCOPE_DEPTH = 24
_MAX_SCOPE_NODES = 256
MAX_SOURCE_BYTES = 5 * 1024 * 1024  # 5 MB
_ENRICHMENT_ERRORS = (RuntimeError, TypeError, ValueError, AttributeError, UnicodeError)
_QUERY_MATCH_LIMIT = 4_096

# ---------------------------------------------------------------------------
# Payload bounds constants
# ---------------------------------------------------------------------------

_MAX_SIGNATURE_LEN = 200
_MAX_PARAMS = 12
_MAX_RETURN_TYPE_LEN = 100
_MAX_GENERICS_LEN = 100
_MAX_ATTRIBUTES = 10
_MAX_ATTRIBUTE_LEN = 60
_MAX_FIELDS_SHOWN = 8
_MAX_VARIANTS_SHOWN = 12
_MAX_MEMBER_TEXT_LEN = 60
_MAX_CALL_TARGET_LEN = 120
_MAX_CALL_RECEIVER_LEN = 80
_MAX_TREE_CACHE_ENTRIES = 128
_TREE_CACHE: OrderedDict[str, None] = OrderedDict()
_TREE_CACHE_EVICTIONS = {"value": 0}

# ---------------------------------------------------------------------------
# Enrichment field groups (documentation / validation reference)
# ---------------------------------------------------------------------------

_FIELD_GROUPS: dict[str, list[str]] = {
    "core": [
        "node_kind",
        "scope_chain",
        "scope_kind",
        "scope_name",
        "language",
        "enrichment_status",
        "enrichment_sources",
        "degrade_reason",
    ],
    "signature": [
        "params",
        "return_type",
        "generics",
        "signature",
        "is_async",
        "is_unsafe",
    ],
    "visibility": [
        "visibility",
    ],
    "attributes": [
        "attributes",
    ],
    "impl_context": [
        "impl_type",
        "impl_trait",
        "impl_kind",
        "impl_generics",
    ],
    "call_target": [
        "call_target",
        "call_receiver",
        "call_method",
        "macro_name",
    ],
    "struct_shape": [
        "struct_field_count",
        "struct_fields",
    ],
    "enum_shape": [
        "enum_variant_count",
        "enum_variants",
    ],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _truncate(text: str, max_len: int) -> str:
    """Truncate *text* to *max_len* characters, appending ``...`` when needed.

    Parameters
    ----------
    text
        The string to truncate.
    max_len
        Maximum allowed length (must be >= 4 for the ellipsis to fit).

    Returns:
    -------
    str
        The original or truncated string.
    """
    if len(text) <= max_len:
        return text
    return text[: max(1, max_len - 3)] + "..."


def _byte_col_to_char_col(source_bytes: bytes, line_start_byte: int, byte_col: int) -> int:
    """Convert a byte-offset column to a character column within a line.

    Parameters
    ----------
    source_bytes
        Full source bytes of the file.
    line_start_byte
        Byte offset where the line begins.
    byte_col
        Byte-based column offset from the line start.

    Returns:
    -------
    int
        Character-based column offset (handles multi-byte UTF-8).
    """
    segment = source_bytes[line_start_byte : line_start_byte + byte_col]
    return len(segment.decode("utf-8", errors="replace"))


def _optional_field_text(
    parent: Node,
    field_name: str,
    source_bytes: bytes,
    *,
    max_len: int,
) -> str | None:
    """Extract and truncate text from an optional named field child.

    Parameters
    ----------
    parent
        Parent node to query.
    field_name
        Field name to look up via ``child_by_field_name``.
    source_bytes
        Full source bytes.
    max_len
        Truncation limit for the extracted text.

    Returns:
    -------
    str | None
        Truncated text, or ``None`` if the field is absent or empty.
    """
    child = parent.child_by_field_name(field_name)
    text = _node_text(child, source_bytes)
    if text is None:
        return None
    return _truncate(text, max_len)


# ---------------------------------------------------------------------------
# Runtime availability
# ---------------------------------------------------------------------------


def is_tree_sitter_rust_available() -> bool:
    """Return whether tree-sitter Rust enrichment dependencies are available.

    Returns:
    -------
    bool
        True when runtime dependencies for Rust tree-sitter enrichment exist.
    """
    return all(
        obj is not None
        for obj in (_tree_sitter_rust, _TreeSitterLanguage, _TreeSitterParser, _TreeSitterPoint)
    )


# ---------------------------------------------------------------------------
# Parser / tree management
# ---------------------------------------------------------------------------


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


def _touch_tree_cache(session: object, cache_key: str | None) -> None:
    if not cache_key:
        return
    if cache_key in _TREE_CACHE:
        _TREE_CACHE.move_to_end(cache_key)
        return
    _TREE_CACHE[cache_key] = None
    while len(_TREE_CACHE) > _MAX_TREE_CACHE_ENTRIES:
        stale_key, _ = _TREE_CACHE.popitem(last=False)
        entries = getattr(session, "_entries", None)
        if isinstance(entries, dict):
            entries.pop(stale_key, None)
        _TREE_CACHE_EVICTIONS["value"] += 1


def _parse_with_session(source: str, *, cache_key: str | None) -> tuple[Tree | None, bytes]:
    source_bytes = source.encode("utf-8", errors="replace")
    if not is_tree_sitter_rust_available():
        return None, source_bytes
    session = get_parse_session(language="rust", parser_factory=_make_parser)
    _touch_tree_cache(session=session, cache_key=cache_key)
    tree, _changed_ranges, _reused = session.parse(file_key=cache_key, source_bytes=source_bytes)
    return tree, source_bytes


def clear_tree_sitter_rust_cache() -> None:
    """Clear per-process Rust parser caches and reset debug counters."""
    clear_parse_session(language="rust")
    _TREE_CACHE.clear()
    _TREE_CACHE_EVICTIONS["value"] = 0
    _rust_language.cache_clear()
    _compile_query.cache_clear()


def get_tree_sitter_rust_cache_stats() -> dict[str, int]:
    """Return cache counters for observability/debugging."""
    session = get_parse_session(language="rust", parser_factory=_make_parser)
    stats = session.stats()
    return {
        "entries": stats.entries,
        "cache_hits": stats.cache_hits,
        "cache_misses": stats.cache_misses,
        "cache_evictions": _TREE_CACHE_EVICTIONS["value"],
        "parse_count": stats.parse_count,
        "reparse_count": stats.reparse_count,
        "edit_failures": stats.edit_failures,
    }


@lru_cache(maxsize=128)
def _compile_query(pack_name: str, source: str) -> Query:
    if _TreeSitterQuery is None:
        msg = "tree_sitter query bindings are unavailable"
        raise RuntimeError(msg)
    _ = pack_name
    query = _TreeSitterQuery(_rust_language(), source)
    rules = load_pack_rules("rust")
    pattern_count = int(getattr(query, "pattern_count", 0))
    for pattern_idx in range(pattern_count):
        if rules.require_rooted and not bool(query.is_pattern_rooted(pattern_idx)):
            msg = f"rust query pattern not rooted: {pattern_idx}"
            raise ValueError(msg)
        if rules.forbid_non_local and bool(query.is_pattern_non_local(pattern_idx)):
            msg = f"rust query pattern non-local: {pattern_idx}"
            raise ValueError(msg)
    return query


def _pack_sources() -> tuple[tuple[str, str], ...]:
    sources = load_rust_query_sources(include_distribution_queries=False)
    return tuple(
        (source.pack_name, source.source) for source in sources if source.pack_name.endswith(".scm")
    )


def _capture_texts_from_captures(
    captures: dict[str, list[Node]],
    source_bytes: bytes,
    *capture_names: str,
    limit: int = 8,
) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for capture_name in capture_names:
        for captured in captures.get(capture_name, []):
            text = _node_text(captured, source_bytes)
            if text is None or text in seen:
                continue
            seen.add(text)
            out.append(text)
            if len(out) >= limit:
                return out
    return out


def _collect_query_pack_captures(
    *,
    root: Node,
    source_bytes: bytes,
    window: QueryWindowV1,
    settings: QueryExecutionSettingsV1,
) -> tuple[dict[str, list[Node]], tuple[ObjectEvidenceRowV1, ...], dict[str, object]]:
    captures: dict[str, list[Node]] = {}
    rows: list[ObjectEvidenceRowV1] = []
    query_telemetry: dict[str, object] = {}
    for pack_name, query_source in _pack_sources():
        try:
            query = _compile_query(pack_name, query_source)
            pack_captures, capture_telemetry = run_bounded_query_captures(
                query,
                root,
                windows=(window,),
                settings=settings,
            )
            pack_matches, match_telemetry = run_bounded_query_matches(
                query,
                root,
                windows=(window,),
                settings=QueryExecutionSettingsV1(
                    match_limit=settings.match_limit,
                    max_start_depth=settings.max_start_depth,
                    budget_ms=settings.budget_ms,
                    require_containment=True,
                ),
            )
        except _ENRICHMENT_ERRORS:
            continue
        query_telemetry[pack_name] = {
            "captures": msgspec.to_builtins(capture_telemetry),
            "matches": msgspec.to_builtins(match_telemetry),
        }
        for capture_name, nodes in pack_captures.items():
            bucket = captures.setdefault(capture_name, [])
            bucket.extend(nodes)
        rows.extend(build_match_rows(query=query, matches=pack_matches, source_bytes=source_bytes))
    return captures, tuple(rows), query_telemetry


# ---------------------------------------------------------------------------
# Node / scope utilities
# ---------------------------------------------------------------------------


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
    nodes_visited = 0
    while current is not None and depth < max_depth:
        if nodes_visited >= _MAX_SCOPE_NODES:
            break
        if current.type in _SCOPE_KINDS:
            name = _scope_name(current, source_bytes)
            chain.append(f"{current.type}:{name}" if name else current.type)
        current = current.parent
        depth += 1
        nodes_visited += 1
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


def _find_ancestor(node: Node, kind: str, *, max_depth: int) -> Node | None:
    """Walk up from *node* to find the nearest ancestor of the given kind.

    Parameters
    ----------
    node
        Starting node.
    kind
        Target node ``type`` string.
    max_depth
        Maximum parent levels to inspect.

    Returns:
    -------
    Node | None
        The ancestor node, or ``None`` if not found within *max_depth*.
    """
    current: Node | None = node.parent
    depth = 0
    while current is not None and depth < max_depth:
        if current.type == kind:
            return current
        current = current.parent
        depth += 1
    return None


# ---------------------------------------------------------------------------
# Core enrichment extractors
# ---------------------------------------------------------------------------

_MODIFIER_STOP_KINDS: frozenset[str] = frozenset({"identifier", "parameters", "type_parameters"})


def _extract_fn_params(fn_node: Node, source_bytes: bytes) -> list[str]:
    """Extract parameter list from a function node.

    Parameters
    ----------
    fn_node
        A tree-sitter ``function_item`` node.
    source_bytes
        Full source bytes.

    Returns:
    -------
    list[str]
        Parameter text values, capped at ``_MAX_PARAMS``.
    """
    params_node = fn_node.child_by_field_name("parameters")
    if params_node is None:
        return []
    param_list: list[str] = []
    for child in params_node.named_children:
        text = _node_text(child, source_bytes)
        if text is not None:
            param_list.append(_truncate(text, _MAX_MEMBER_TEXT_LEN))
        if len(param_list) >= _MAX_PARAMS:
            break
    return param_list


def _extract_fn_modifiers(fn_node: Node, source_bytes: bytes) -> tuple[bool, bool]:
    """Detect ``async`` and ``unsafe`` modifiers on a function node.

    Parameters
    ----------
    fn_node
        A tree-sitter ``function_item`` node.
    source_bytes
        Full source bytes.

    Returns:
    -------
    tuple[bool, bool]
        ``(is_async, is_unsafe)`` flags.
    """
    is_async = False
    is_unsafe = False
    for child in fn_node.children:
        if child.type == "mutable_specifier":
            continue
        child_text = _node_text(child, source_bytes)
        if child_text == "async":
            is_async = True
        elif child_text == "unsafe":
            is_unsafe = True
        if child.type in _MODIFIER_STOP_KINDS:
            break
    return is_async, is_unsafe


def _extract_function_signature(fn_node: Node, source_bytes: bytes) -> dict[str, object]:
    """Extract function signature details from a ``function_item`` node.

    Parameters
    ----------
    fn_node
        A tree-sitter node of type ``function_item``.
    source_bytes
        Full source bytes.

    Returns:
    -------
    dict[str, object]
        Signature fields: params, return_type, generics, signature, is_async,
        is_unsafe.
    """
    result: dict[str, object] = {}
    result["params"] = _extract_fn_params(fn_node, source_bytes)

    ret_text = _optional_field_text(
        fn_node, "return_type", source_bytes, max_len=_MAX_RETURN_TYPE_LEN
    )
    if ret_text is not None:
        result["return_type"] = ret_text

    gen_text = _optional_field_text(
        fn_node, "type_parameters", source_bytes, max_len=_MAX_GENERICS_LEN
    )
    if gen_text is not None:
        result["generics"] = gen_text

    body_node = fn_node.child_by_field_name("body")
    if body_node is not None:
        sig_bytes = source_bytes[fn_node.start_byte : body_node.start_byte]
        sig_text = sig_bytes.decode("utf-8", errors="replace").strip()
        if sig_text:
            result["signature"] = _truncate(sig_text, _MAX_SIGNATURE_LEN)

    is_async, is_unsafe = _extract_fn_modifiers(fn_node, source_bytes)
    result["is_async"] = is_async
    result["is_unsafe"] = is_unsafe
    return result


def _extract_visibility(item_node: Node, source_bytes: bytes) -> str:
    """Extract visibility qualifier from a definition node.

    Parameters
    ----------
    item_node
        A tree-sitter definition node (function_item, struct_item, etc.).
    source_bytes
        Full source bytes.

    Returns:
    -------
    str
        One of ``"pub"``, ``"pub(crate)"``, ``"pub(super)"``, or ``"private"``.
    """
    vis_node = item_node.child_by_field_name("visibility_modifier")
    if vis_node is None:
        for child in item_node.children:
            if child.type == "visibility_modifier":
                vis_node = child
                break
    if vis_node is None:
        return "private"

    vis_text = _node_text(vis_node, source_bytes)
    if vis_text is None:
        return "private"

    normalized = vis_text.replace(" ", "")
    if normalized == "pub(crate)":
        return "pub(crate)"
    if normalized == "pub(super)":
        return "pub(super)"
    if normalized.startswith("pub"):
        return "pub"
    return "private"


_COMMENT_KINDS: frozenset[str] = frozenset({"line_comment", "block_comment"})


def _extract_attributes(item_node: Node, source_bytes: bytes) -> list[str]:
    """Extract attribute annotations preceding an item node.

    Walk the ``prev_named_sibling`` chain backwards, collecting
    ``attribute_item`` nodes and skipping comment nodes. Stop at any
    other node type.

    Parameters
    ----------
    item_node
        The definition node whose attributes to collect.
    source_bytes
        Full source bytes.

    Returns:
    -------
    list[str]
        Attribute text values in declaration order, stripped of ``#[...]``
        delimiters. Inner attributes (``#![...]``) are excluded.
    """
    attrs: list[str] = []
    current: Node | None = item_node.prev_named_sibling
    while current is not None:
        if current.type == "attribute_item":
            text = _node_text(current, source_bytes)
            if text is not None:
                if text.startswith("#!["):
                    current = current.prev_named_sibling
                    continue
                stripped = text
                if stripped.startswith("#[") and stripped.endswith("]"):
                    stripped = stripped[2:-1]
                attrs.append(_truncate(stripped, _MAX_ATTRIBUTE_LEN))
        elif current.type in _COMMENT_KINDS:
            current = current.prev_named_sibling
            continue
        else:
            break
        current = current.prev_named_sibling

    attrs.reverse()
    return attrs[:_MAX_ATTRIBUTES]


def _extract_impl_context(impl_node: Node, source_bytes: bytes) -> dict[str, object]:
    """Extract context from an ``impl_item`` node.

    Parameters
    ----------
    impl_node
        A tree-sitter node of type ``impl_item``.
    source_bytes
        Full source bytes.

    Returns:
    -------
    dict[str, object]
        Fields: impl_type, impl_trait (optional), impl_kind, impl_generics
        (optional).
    """
    result: dict[str, object] = {}

    type_text = _optional_field_text(impl_node, "type", source_bytes, max_len=_MAX_CALL_TARGET_LEN)
    if type_text is not None:
        result["impl_type"] = type_text

    trait_node = impl_node.child_by_field_name("trait")
    if trait_node is not None:
        trait_text = _node_text(trait_node, source_bytes)
        if trait_text is not None:
            result["impl_trait"] = trait_text
        result["impl_kind"] = "trait"
    else:
        result["impl_kind"] = "inherent"

    gen_text = _optional_field_text(
        impl_node, "type_parameters", source_bytes, max_len=_MAX_GENERICS_LEN
    )
    if gen_text is not None:
        result["impl_generics"] = gen_text

    return result


def _extract_field_expression(fn_node: Node, source_bytes: bytes) -> dict[str, object]:
    """Decompose a ``field_expression`` call target into receiver and method.

    Parameters
    ----------
    fn_node
        A tree-sitter ``field_expression`` node (the ``function`` child
        of a ``call_expression``).
    source_bytes
        Full source bytes.

    Returns:
    -------
    dict[str, object]
        Fields: call_receiver and call_method (when extractable).
    """
    result: dict[str, object] = {}
    recv_text = _optional_field_text(fn_node, "value", source_bytes, max_len=_MAX_CALL_RECEIVER_LEN)
    if recv_text is not None:
        result["call_receiver"] = recv_text
    method_text = _node_text(fn_node.child_by_field_name("field"), source_bytes)
    if method_text is not None:
        result["call_method"] = method_text
    return result


def _extract_call_target(node: Node, source_bytes: bytes) -> dict[str, object]:
    """Extract call target information from a call or macro invocation node.

    Parameters
    ----------
    node
        A tree-sitter node of type ``call_expression`` or ``macro_invocation``.
    source_bytes
        Full source bytes.

    Returns:
    -------
    dict[str, object]
        Fields: call_target (and optionally call_receiver, call_method) for
        calls; macro_name for macro invocations.
    """
    result: dict[str, object] = {}

    if node.type == "call_expression":
        fn_node = node.child_by_field_name("function")
        if fn_node is not None:
            target_text = _node_text(fn_node, source_bytes)
            if target_text is not None:
                result["call_target"] = _truncate(target_text, _MAX_CALL_TARGET_LEN)
            if fn_node.type == "field_expression":
                result.update(_extract_field_expression(fn_node, source_bytes))
    elif node.type == "macro_invocation":
        macro_text = _node_text(node.child_by_field_name("macro"), source_bytes)
        if macro_text is not None:
            result["macro_name"] = macro_text

    return result


def _extract_struct_shape(struct_node: Node, source_bytes: bytes) -> dict[str, object]:
    """Extract field shape from a ``struct_item`` node.

    Parameters
    ----------
    struct_node
        A tree-sitter node of type ``struct_item``.
    source_bytes
        Full source bytes.

    Returns:
    -------
    dict[str, object]
        Fields: struct_field_count, struct_fields (list of field text, capped).
    """
    result: dict[str, object] = {}
    body = struct_node.child_by_field_name("body")
    if body is None:
        return result

    field_nodes = [c for c in body.named_children if c.type == "field_declaration"]
    result["struct_field_count"] = len(field_nodes)

    fields: list[str] = []
    for field_node in field_nodes[:_MAX_FIELDS_SHOWN]:
        text = _node_text(field_node, source_bytes)
        if text is not None:
            fields.append(_truncate(text, _MAX_MEMBER_TEXT_LEN))

    remaining = len(field_nodes) - _MAX_FIELDS_SHOWN
    if remaining > 0:
        fields.append(f"... and {remaining} more")
    result["struct_fields"] = fields
    return result


def _extract_enum_shape(enum_node: Node, source_bytes: bytes) -> dict[str, object]:
    """Extract variant shape from an ``enum_item`` node.

    Parameters
    ----------
    enum_node
        A tree-sitter node of type ``enum_item``.
    source_bytes
        Full source bytes.

    Returns:
    -------
    dict[str, object]
        Fields: enum_variant_count, enum_variants (list of variant text, capped).
    """
    result: dict[str, object] = {}
    body = enum_node.child_by_field_name("body")
    if body is None:
        return result

    variant_nodes = [c for c in body.named_children if c.type == "enum_variant"]
    result["enum_variant_count"] = len(variant_nodes)

    variants: list[str] = []
    for variant_node in variant_nodes[:_MAX_VARIANTS_SHOWN]:
        text = _node_text(variant_node, source_bytes)
        if text is not None:
            variants.append(_truncate(text, _MAX_MEMBER_TEXT_LEN))

    remaining = len(variant_nodes) - _MAX_VARIANTS_SHOWN
    if remaining > 0:
        variants.append(f"... and {remaining} more")
    result["enum_variants"] = variants
    return result


# ---------------------------------------------------------------------------
# Item role classification
# ---------------------------------------------------------------------------

_ITEM_ROLE_SIMPLE: dict[str, str] = {
    "use_declaration": "use_import",
    "macro_invocation": "macro_call",
    "field_declaration": "struct_field",
    "enum_variant": "enum_variant",
    "const_item": "const_item",
    "type_item": "type_alias",
    "static_item": "static_item",
}

_TEST_ATTRIBUTE_NAMES: frozenset[str] = frozenset(
    {
        "test",
        "tokio::test",
        "rstest",
        "async_std::test",
    }
)


def _is_test_function(attributes: list[str]) -> bool:
    """Return whether any attribute marks this function as a test.

    Parameters
    ----------

    Attributes:
        Attribute strings (already stripped of ``#[...]`` delimiters).

    Returns:
    -------
    bool
        True when at least one attribute matches a known test marker.
    """
    return any(attr in _TEST_ATTRIBUTE_NAMES for attr in attributes)


def _classify_function_role(
    node: Node,
    attributes: list[str],
    *,
    max_scope_depth: int,
) -> str:
    """Classify a ``function_item`` into a specific role.

    Parameters
    ----------
    node
        A tree-sitter ``function_item`` node.

    Attributes:
        Pre-extracted attributes for the function.
    max_scope_depth
        Maximum ancestor search depth.

    Returns:
    -------
    str
        One of ``"test_function"``, ``"trait_method"``, ``"method"``,
        or ``"free_function"``.
    """
    if _is_test_function(attributes):
        return "test_function"
    impl_node = _find_ancestor(node, "impl_item", max_depth=max_scope_depth)
    if impl_node is not None:
        trait_node = impl_node.child_by_field_name("trait")
        if trait_node is not None:
            return "trait_method"
        return "method"
    return "free_function"


def _classify_call_role(node: Node) -> str:
    """Classify a ``call_expression`` node as method or function call.

    Parameters
    ----------
    node
        A tree-sitter ``call_expression`` node.

    Returns:
    -------
    str
        ``"method_call"`` when the target is a field expression, else
        ``"function_call"``.
    """
    fn_node = node.child_by_field_name("function")
    if fn_node is not None and fn_node.type == "field_expression":
        return "method_call"
    return "function_call"


def _classify_item_role(
    node: Node,
    scope: Node | None,
    attributes: list[str],
    *,
    max_scope_depth: int,
) -> str:
    """Classify the item role of a match node.

    Parameters
    ----------
    node
        The tree-sitter node at the match location.
    scope
        The nearest enclosing scope node.

    Attributes:
        Pre-extracted attribute strings.
    max_scope_depth
        Maximum ancestor search depth.

    Returns:
    -------
    str
        A semantic role string (e.g. ``"method"``, ``"free_function"``).
    """
    simple = _ITEM_ROLE_SIMPLE.get(node.type)
    if simple is not None:
        return simple

    if node.type == "call_expression":
        return _classify_call_role(node)

    fn_target = node if node.type == "function_item" else None
    if fn_target is None and scope is not None and scope.type == "function_item":
        fn_target = scope
    if fn_target is not None:
        return _classify_function_role(fn_target, attributes, max_scope_depth=max_scope_depth)

    return node.type


# ---------------------------------------------------------------------------
# Shared enrichment builder
# ---------------------------------------------------------------------------

_DEFINITION_SCOPE_KINDS: frozenset[str] = frozenset(
    {
        "function_item",
        "struct_item",
        "enum_item",
        "trait_item",
        "impl_item",
        "mod_item",
    }
)

_CALL_NODE_KINDS: frozenset[str] = frozenset({"call_expression", "macro_invocation"})


def _try_extract(
    label: str,
    extractor: Callable[[Node, bytes], dict[str, object]],
    target: Node,
    source_bytes: bytes,
) -> tuple[dict[str, object], str | None]:
    """Call *extractor* on *target*, returning results or a degrade reason.

    Parameters
    ----------
    label
        Human label for the extractor (used in degradation messages).
    extractor
        Callable ``(Node, bytes) -> dict[str, object]``.
    target
        Node argument forwarded to extractor.
    source_bytes
        Source bytes forwarded to extractor.

    Returns:
    -------
    tuple[dict[str, object], str | None]
        Extracted fields and an optional degrade reason on failure.
    """
    try:
        result = extractor(target, source_bytes)
    except _ENRICHMENT_ERRORS as exc:
        return {}, f"{label}: {exc}"
    else:
        return result, None


def _resolve_target(node: Node, scope: Node | None, kind: str) -> Node | None:
    """Return the node or scope matching *kind*, preferring node.

    Parameters
    ----------
    node
        The direct match node.
    scope
        The enclosing scope node (may be ``None``).
    kind
        The target node type string.

    Returns:
    -------
    Node | None
        The matching node, or ``None``.
    """
    if node.type == kind:
        return node
    if scope is not None and scope.type == kind:
        return scope
    return None


def _resolve_definition_target(node: Node, scope: Node | None) -> Node | None:
    """Find the best definition-category node for visibility/attribute extraction.

    Parameters
    ----------
    node
        The direct match node.
    scope
        The enclosing scope node (may be ``None``).

    Returns:
    -------
    Node | None
        The first matching definition-scope node, or ``None``.
    """
    for kind in _DEFINITION_SCOPE_KINDS:
        target = _resolve_target(node, scope, kind)
        if target is not None:
            return target
    return None


def _merge_result(
    payload: dict[str, object],
    reasons: list[str],
    result: tuple[dict[str, object], str | None],
) -> None:
    """Merge an extractor result into *payload* and record any degrade reason.

    Parameters
    ----------
    payload
        Target dict to update.
    reasons
        Accumulator for degradation reason strings.
    result
        Return value from ``_try_extract``.
    """
    fields, reason = result
    payload.update(fields)
    if reason is not None:
        reasons.append(reason)


def _apply_extractors(
    payload: dict[str, object],
    node: Node,
    scope: Node | None,
    source_bytes: bytes,
    *,
    max_scope_depth: int,
) -> None:
    """Dispatch all applicable extractors, updating *payload* in place.

    Parameters
    ----------
    payload
        Target dict to populate with extractor fields.
    node
        The tree-sitter node at the match location.
    scope
        The nearest enclosing scope node, or ``None``.
    source_bytes
        Full source bytes.
    max_scope_depth
        Maximum ancestor levels for ancestor searches.
    """
    reasons: list[str] = []

    sig_target = _resolve_target(node, scope, "function_item")
    if sig_target is not None:
        _merge_result(
            payload,
            reasons,
            _try_extract("signature", _extract_function_signature, sig_target, source_bytes),
        )

    vis_target = _resolve_definition_target(node, scope)
    if vis_target is not None:
        _enrich_visibility_and_attrs(payload, reasons, vis_target, source_bytes)

    impl_target = _resolve_impl_ancestor(node, scope, max_scope_depth=max_scope_depth)
    if impl_target is not None:
        _merge_result(
            payload,
            reasons,
            _try_extract("impl_context", _extract_impl_context, impl_target, source_bytes),
        )

    if scope is not None and scope.type == "struct_item":
        _merge_result(
            payload,
            reasons,
            _try_extract("struct_shape", _extract_struct_shape, scope, source_bytes),
        )
    if scope is not None and scope.type == "enum_item":
        _merge_result(
            payload, reasons, _try_extract("enum_shape", _extract_enum_shape, scope, source_bytes)
        )

    if node.type in _CALL_NODE_KINDS:
        _merge_result(
            payload, reasons, _try_extract("call_target", _extract_call_target, node, source_bytes)
        )

    # Classify item role using already-extracted attributes
    extracted_attrs = payload.get("attributes")
    attr_list: list[str] = extracted_attrs if isinstance(extracted_attrs, list) else []
    try:
        payload["item_role"] = _classify_item_role(
            node, scope, attr_list, max_scope_depth=max_scope_depth
        )
    except _ENRICHMENT_ERRORS as exc:
        reasons.append(f"item_role: {exc}")

    if reasons:
        payload["enrichment_status"] = "degraded"
        payload["degrade_reason"] = "; ".join(reasons)


def _enrich_visibility_and_attrs(
    payload: dict[str, object],
    reasons: list[str],
    target: Node,
    source_bytes: bytes,
) -> None:
    """Extract visibility and attributes for a definition node.

    Parameters
    ----------
    payload
        Target dict to update.
    reasons
        Accumulator for degradation reason strings.
    target
        A definition-category scope node.
    source_bytes
        Full source bytes.
    """
    fields, reason = _try_extract("visibility", _extract_visibility_dict, target, source_bytes)
    payload.update(fields)
    if reason is not None:
        reasons.append(reason)

    fields, reason = _try_extract("attributes", _extract_attributes_dict, target, source_bytes)
    payload.update(fields)
    if reason is not None:
        reasons.append(reason)


def _extract_visibility_dict(target: Node, source_bytes: bytes) -> dict[str, object]:
    """Wrap ``_extract_visibility`` to return a dict for ``_try_extract``.

    Parameters
    ----------
    target
        Definition node.
    source_bytes
        Full source bytes.

    Returns:
    -------
    dict[str, object]
        Single-key dict with ``visibility``.
    """
    return {"visibility": _extract_visibility(target, source_bytes)}


def _extract_attributes_dict(target: Node, source_bytes: bytes) -> dict[str, object]:
    """Wrap ``_extract_attributes`` to return a dict for ``_try_extract``.

    Parameters
    ----------
    target
        Definition node.
    source_bytes
        Full source bytes.

    Returns:
    -------
    dict[str, object]
        Dict with ``attributes`` key if any attributes found, else empty.
    """
    attrs = _extract_attributes(target, source_bytes)
    if attrs:
        return {"attributes": attrs}
    return {}


def _resolve_impl_ancestor(
    node: Node,
    scope: Node | None,
    *,
    max_scope_depth: int,
) -> Node | None:
    """Find the nearest ``impl_item`` from scope or ancestors.

    Parameters
    ----------
    node
        Match node.
    scope
        Enclosing scope node.
    max_scope_depth
        Maximum ancestor search depth.

    Returns:
    -------
    Node | None
        The impl node, or ``None``.
    """
    if scope is not None and scope.type == "impl_item":
        return scope
    return _find_ancestor(node, "impl_item", max_depth=max_scope_depth)


def _build_enrichment_payload(
    node: Node,
    scope: Node | None,
    source_bytes: bytes,
    *,
    max_scope_depth: int,
) -> dict[str, object]:
    """Build the full enrichment payload from resolved node and scope.

    This is the shared implementation called by both ``enrich_rust_context``
    and ``enrich_rust_context_by_byte_range``.

    Parameters
    ----------
    node
        The tree-sitter node at the match location.
    scope
        The nearest enclosing scope node, or ``None``.
    source_bytes
        Full source bytes.
    max_scope_depth
        Maximum ancestor levels for scope chain traversal.

    Returns:
    -------
    dict[str, object]
        The enrichment payload with all applicable fields.
    """
    chain = _scope_chain(node, source_bytes, max_depth=max_scope_depth)
    payload: dict[str, object] = {
        "node_kind": node.type,
        "scope_chain": chain,
        "language": "rust",
        "enrichment_status": "applied",
        "enrichment_sources": ["tree_sitter"],
    }

    if scope is not None:
        payload["scope_kind"] = scope.type
        scope_nm = _scope_name(scope, source_bytes)
        if scope_nm:
            payload["scope_name"] = scope_nm

    _apply_extractors(payload, node, scope, source_bytes, max_scope_depth=max_scope_depth)
    return payload


def _collect_query_pack_payload(
    *,
    root: Node,
    source_bytes: bytes,
    byte_start: int,
    byte_end: int,
) -> dict[str, object]:
    if byte_end <= byte_start:
        return {}

    window = QueryWindowV1(start_byte=byte_start, end_byte=byte_end)
    settings = QueryExecutionSettingsV1(match_limit=_QUERY_MATCH_LIMIT)
    captures, rows, query_telemetry = _collect_query_pack_captures(
        root=root,
        source_bytes=source_bytes,
        window=window,
        settings=settings,
    )

    payload: dict[str, object] = (
        {"query_pack_telemetry": query_telemetry} if query_telemetry else {}
    )
    definitions, references, calls, imports = _rust_fact_lists(captures, source_bytes)
    _extend_rust_fact_lists_from_rows(
        rows=rows,
        definitions=definitions,
        references=references,
        calls=calls,
        imports=imports,
    )
    payload["rust_tree_sitter_facts"] = _rust_fact_payload(
        definitions=definitions,
        references=references,
        calls=calls,
        imports=imports,
    )
    _attach_query_pack_payload(
        payload=payload,
        rows=rows,
        captures=captures,
        source_bytes=source_bytes,
    )
    payload["tree_sitter_diagnostics"] = [
        msgspec.to_builtins(row)
        for row in collect_tree_sitter_diagnostics(
            language="rust",
            root=root,
            windows=(window,),
            match_limit=1024,
        )
    ]
    return payload


def _rust_fact_lists(
    captures: dict[str, list[Node]],
    source_bytes: bytes,
) -> tuple[list[str], list[str], list[str], list[str]]:
    definitions = _capture_texts_from_captures(
        captures,
        source_bytes,
        "def.function.name",
        "def.struct.name",
        "def.enum.name",
        "def.trait.name",
        "def.module.name",
        "def.macro.name",
    )
    references = _capture_texts_from_captures(
        captures,
        source_bytes,
        "ref.identifier",
        "ref.scoped.name",
        "ref.use.path",
        "ref.macro.path",
    )
    calls = _capture_texts_from_captures(captures, source_bytes, "call.target", "call.macro.path")
    imports = _capture_texts_from_captures(
        captures,
        source_bytes,
        "import.path",
        "import.extern.name",
    )
    return definitions, references, calls, imports


def _extend_rust_fact_lists_from_rows(
    *,
    rows: tuple[ObjectEvidenceRowV1, ...],
    definitions: list[str],
    references: list[str],
    calls: list[str],
    imports: list[str],
) -> None:
    for row in rows:
        if row.emit == "definitions":
            _extend_fact_list(
                target=definitions,
                captures=row.captures,
                keys=(
                    "def.function.name",
                    "def.struct.name",
                    "def.enum.name",
                    "def.trait.name",
                    "def.module.name",
                    "def.macro.name",
                ),
            )
        elif row.emit == "references":
            _extend_fact_list(
                target=references,
                captures=row.captures,
                keys=("ref.identifier", "ref.scoped.name", "ref.use.path", "ref.macro.path"),
            )
        elif row.emit == "calls":
            _extend_fact_list(
                target=calls,
                captures=row.captures,
                keys=("call.target", "call.macro.path"),
            )
        elif row.emit in {"imports", "modules"}:
            _extend_fact_list(
                target=imports,
                captures=row.captures,
                keys=("import.path", "import.extern.name", "module.name"),
            )


def _extend_fact_list(
    *,
    target: list[str],
    captures: Mapping[str, object],
    keys: tuple[str, ...],
) -> None:
    for key in keys:
        value = captures.get(key)
        if isinstance(value, str) and value and value not in target:
            target.append(value)


def _rust_fact_payload(
    *,
    definitions: list[str],
    references: list[str],
    calls: list[str],
    imports: list[str],
) -> dict[str, list[str]]:
    return {
        "definitions": definitions[:_MAX_FIELDS_SHOWN],
        "references": references[:_MAX_FIELDS_SHOWN],
        "calls": calls[:_MAX_FIELDS_SHOWN],
        "imports": imports[:_MAX_FIELDS_SHOWN],
    }


def _attach_query_pack_payload(
    *,
    payload: dict[str, object],
    rows: tuple[ObjectEvidenceRowV1, ...],
    captures: dict[str, list[Node]],
    source_bytes: bytes,
) -> None:
    payload["query_match_rows"] = [msgspec.to_builtins(row) for row in rows]
    payload["query_pack_bundle"] = msgspec.to_builtins(
        load_rust_grammar_bundle(include_distribution_queries=False)
    )
    payload["query_pack_tags"] = msgspec.to_builtins(
        build_tag_events(source_bytes, captures=captures)
    )
    injection_plan = build_injection_plan(source_bytes, captures=captures, default_language="rust")
    payload["query_pack_injections"] = msgspec.to_builtins(injection_plan)
    rust_plan = tuple(row for row in injection_plan if row.language == "rust")
    if rust_plan:
        payload["query_pack_injection_runtime"] = msgspec.to_builtins(
            parse_injected_ranges(
                source_bytes=source_bytes,
                language=_rust_language(),
                plans=rust_plan,
            )
        )


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


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

    Returns:
    -------
    dict[str, object] | None
        Best-effort context payload, or ``None`` when unavailable.
    """
    if not is_tree_sitter_rust_available() or line < 1 or col < 0 or len(source) > MAX_SOURCE_BYTES:
        return None

    try:
        tree, source_bytes = _parse_with_session(source, cache_key=cache_key)
        if tree is None:
            return None
        if _TreeSitterPoint is None:
            return None
        point = _TreeSitterPoint(max(0, line - 1), max(0, col))
        node = tree.root_node.named_descendant_for_point_range(point, point)
        if node is None:
            return None
        scope = _find_scope(node, max_depth=max_scope_depth)
        payload = _build_enrichment_payload(
            node, scope, source_bytes, max_scope_depth=max_scope_depth
        )
        payload.update(
            _collect_query_pack_payload(
                root=tree.root_node,
                source_bytes=source_bytes,
                byte_start=int(getattr(node, "start_byte", 0)),
                byte_end=int(getattr(node, "end_byte", 0)),
            )
        )
    except _ENRICHMENT_ERRORS:
        return None
    else:
        return payload


def enrich_rust_context_by_byte_range(
    source: str,
    *,
    byte_start: int,
    byte_end: int,
    cache_key: str | None = None,
    max_scope_depth: int = _DEFAULT_SCOPE_DEPTH,
) -> dict[str, object] | None:
    """Extract optional Rust context using byte offsets instead of line/col.

    Use this entry point when the caller already has canonical byte offsets,
    avoiding line/column conversion inaccuracies with multi-byte characters.

    Parameters
    ----------
    source
        Full source text of the Rust file.
    byte_start
        0-based byte offset of the match start.
    byte_end
        0-based byte offset of the match end (exclusive).
    cache_key
        Stable file key for parse-tree cache reuse.
    max_scope_depth
        Maximum ancestor levels to inspect.

    Returns:
    -------
    dict[str, object] | None
        Best-effort context payload, or ``None`` when unavailable.
    """
    if not is_tree_sitter_rust_available() or byte_start < 0 or byte_end <= byte_start:
        return None

    source_byte_len = len(source.encode("utf-8", errors="replace"))
    if source_byte_len > MAX_SOURCE_BYTES or byte_end > source_byte_len:
        return None

    try:
        tree, source_bytes = _parse_with_session(source, cache_key=cache_key)
        if tree is None:
            return None
        node = tree.root_node.named_descendant_for_byte_range(byte_start, byte_end)
        if node is None:
            return None
        scope = _find_scope(node, max_depth=max_scope_depth)
        payload = _build_enrichment_payload(
            node, scope, source_bytes, max_scope_depth=max_scope_depth
        )
        payload.update(
            _collect_query_pack_payload(
                root=tree.root_node,
                source_bytes=source_bytes,
                byte_start=byte_start,
                byte_end=byte_end,
            )
        )
    except _ENRICHMENT_ERRORS:
        return None
    else:
        return payload


__all__ = [
    "MAX_SOURCE_BYTES",
    "clear_tree_sitter_rust_cache",
    "enrich_rust_context",
    "enrich_rust_context_by_byte_range",
    "get_tree_sitter_rust_cache_stats",
    "is_tree_sitter_rust_available",
]
