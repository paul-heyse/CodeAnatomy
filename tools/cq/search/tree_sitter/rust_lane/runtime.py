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
from dataclasses import dataclass, field
from functools import lru_cache
from typing import TYPE_CHECKING, cast

import msgspec

from tools.cq.core.locations import byte_offset_to_line_col
from tools.cq.search._shared.core import truncate as _shared_truncate
from tools.cq.search.rust.contracts import RustMacroExpansionRequestV1
from tools.cq.search.tree_sitter.contracts.core_models import (
    ObjectEvidenceRowV1,
    QueryExecutionSettingsV1,
    QueryWindowV1,
    TreeSitterDiagnosticV1,
    TreeSitterQueryHitV1,
)
from tools.cq.search.tree_sitter.contracts.lane_payloads import canonicalize_rust_lane_payload
from tools.cq.search.tree_sitter.contracts.query_models import QueryPackPlanV1
from tools.cq.search.tree_sitter.core.adaptive_runtime import adaptive_query_budget_ms
from tools.cq.search.tree_sitter.core.change_windows import (
    contains_window,
    ensure_query_windows,
    windows_from_changed_ranges,
)
from tools.cq.search.tree_sitter.core.infrastructure import cached_field_ids, child_by_field
from tools.cq.search.tree_sitter.core.language_registry import load_tree_sitter_language
from tools.cq.search.tree_sitter.core.node_utils import node_text
from tools.cq.search.tree_sitter.core.parse import clear_parse_session, get_parse_session
from tools.cq.search.tree_sitter.core.query_pack_executor import (
    QueryPackExecutionContextV1,
    execute_pack_rows_with_matches,
)
from tools.cq.search.tree_sitter.core.runtime import (
    QueryExecutionCallbacksV1,
)
from tools.cq.search.tree_sitter.core.work_queue import enqueue_windows
from tools.cq.search.tree_sitter.query.compiler import compile_query
from tools.cq.search.tree_sitter.query.planner import build_pack_plan, sort_pack_plans
from tools.cq.search.tree_sitter.query.predicates import (
    has_custom_predicates,
    make_query_predicate,
)
from tools.cq.search.tree_sitter.rust_lane.bundle import (
    load_rust_grammar_bundle,
    load_rust_query_sources,
)
from tools.cq.search.tree_sitter.rust_lane.injection_runtime import parse_injected_ranges
from tools.cq.search.tree_sitter.rust_lane.injections import (
    InjectionPlanV1,
    build_injection_plan_from_matches,
)
from tools.cq.search.tree_sitter.structural.exports import collect_diagnostic_rows
from tools.cq.search.tree_sitter.tags import RustTagEventV1, build_tag_events

if TYPE_CHECKING:
    from tree_sitter import Language, Node, Parser, Tree

try:
    from tree_sitter import Parser as _TreeSitterParser
    from tree_sitter import Point as _TreeSitterPoint
except ImportError:  # pragma: no cover - exercised via availability checks
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

_DEFAULT_SCOPE_DEPTH = 24
_MAX_SCOPE_NODES = 256
MAX_SOURCE_BYTES = 5 * 1024 * 1024  # 5 MB
_ENRICHMENT_ERRORS = (RuntimeError, TypeError, ValueError, AttributeError, UnicodeError)


@dataclass(frozen=True, slots=True)
class _RustPackRunResultV1:
    pack_name: str
    query: object
    captures: dict[str, list[Node]]
    matches: list[tuple[int, dict[str, list[Node]]]]
    rows: tuple[ObjectEvidenceRowV1, ...]
    query_hits: tuple[TreeSitterQueryHitV1, ...]
    capture_telemetry: object
    match_telemetry: object


@dataclass(slots=True)
class _RustPackAccumulatorV1:
    captures: dict[str, list[Node]] = field(default_factory=dict)
    rows: list[ObjectEvidenceRowV1] = field(default_factory=list)
    query_hits: list[TreeSitterQueryHitV1] = field(default_factory=list)
    injection_plans: list[InjectionPlanV1] = field(default_factory=list)
    tag_events: list[RustTagEventV1] = field(default_factory=list)
    query_telemetry: dict[str, object] = field(default_factory=dict)

    def merge(self, *, result: _RustPackRunResultV1, source_bytes: bytes) -> None:
        self.query_telemetry[result.pack_name] = {
            "captures": msgspec.to_builtins(result.capture_telemetry),
            "matches": msgspec.to_builtins(result.match_telemetry),
        }
        for capture_name, nodes in result.captures.items():
            self.captures.setdefault(capture_name, []).extend(nodes)
        if "injection.content" in result.captures:
            self.injection_plans.extend(
                build_injection_plan_from_matches(
                    query=result.query,
                    matches=result.matches,
                    source_bytes=source_bytes,
                    default_language="rust",
                )
            )
        self.tag_events.extend(build_tag_events(matches=result.matches, source_bytes=source_bytes))
        self.rows.extend(result.rows)
        self.query_hits.extend(result.query_hits)

    def finalize(
        self,
    ) -> tuple[
        dict[str, list[Node]],
        tuple[ObjectEvidenceRowV1, ...],
        tuple[TreeSitterQueryHitV1, ...],
        dict[str, object],
        tuple[InjectionPlanV1, ...],
        tuple[RustTagEventV1, ...],
    ]:
        return (
            self.captures,
            tuple(self.rows),
            tuple(self.query_hits),
            self.query_telemetry,
            tuple(self.injection_plans),
            tuple(self.tag_events),
        )


@dataclass(frozen=True, slots=True)
class _RustQueryPackArtifactsV1:
    rows: tuple[ObjectEvidenceRowV1, ...]
    query_hits: tuple[TreeSitterQueryHitV1, ...]
    diagnostics: tuple[TreeSitterDiagnosticV1, ...]
    injection_plan: tuple[InjectionPlanV1, ...]
    tag_events: tuple[RustTagEventV1, ...]
    source_bytes: bytes
    file_key: str | None


@dataclass(frozen=True, slots=True)
class _RustQueryCollectionV1:
    windows: tuple[QueryWindowV1, ...]
    captures: dict[str, list[Node]]
    rows: tuple[ObjectEvidenceRowV1, ...]
    query_hits: tuple[TreeSitterQueryHitV1, ...]
    query_telemetry: dict[str, object]
    injection_plan: tuple[InjectionPlanV1, ...]
    tag_events: tuple[RustTagEventV1, ...]
    diagnostics: tuple[TreeSitterDiagnosticV1, ...]


@dataclass(frozen=True, slots=True)
class _RustQueryExecutionPlanV1:
    windows: tuple[QueryWindowV1, ...]
    settings: QueryExecutionSettingsV1


@lru_cache(maxsize=1)
def _rust_field_ids() -> dict[str, int]:
    return cached_field_ids("rust")


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
    return _shared_truncate(text, max_len)


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
    child = child_by_field(parent, field_name, _rust_field_ids())
    text = node_text(child, source_bytes)
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
        for obj in (
            load_tree_sitter_language("rust"),
            _TreeSitterParser,
            _TreeSitterPoint,
        )
    )


# ---------------------------------------------------------------------------
# Parser / tree management
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def _rust_language() -> Language:
    resolved = load_tree_sitter_language("rust")
    if resolved is None:
        msg = "tree_sitter_rust language bindings are unavailable"
        raise RuntimeError(msg)
    return cast("Language", resolved)


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


def _parse_with_session(
    source: str,
    *,
    cache_key: str | None,
) -> tuple[Tree | None, bytes, tuple[object, ...]]:
    source_bytes = source.encode("utf-8", errors="replace")
    if not is_tree_sitter_rust_available():
        return None, source_bytes, ()
    session = get_parse_session(language="rust", parser_factory=_make_parser)
    _touch_tree_cache(session=session, cache_key=cache_key)
    tree, changed_ranges, _reused = session.parse(file_key=cache_key, source_bytes=source_bytes)
    return tree, source_bytes, tuple(changed_ranges) if changed_ranges is not None else ()


def clear_tree_sitter_rust_cache() -> None:
    """Clear per-process Rust parser caches and reset debug counters."""
    clear_parse_session(language="rust")
    _TREE_CACHE.clear()
    _TREE_CACHE_EVICTIONS["value"] = 0
    _rust_language.cache_clear()
    compile_query.cache_clear()
    _pack_source_rows.cache_clear()


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


@lru_cache(maxsize=1)
def _pack_source_rows() -> tuple[tuple[str, str, QueryPackPlanV1], ...]:
    sources = load_rust_query_sources(profile_name="rust_search_enriched")
    source_rows: list[tuple[str, str, QueryPackPlanV1]] = []
    for source in sources:
        if not source.pack_name.endswith(".scm"):
            continue
        try:
            source_rows.append(
                (
                    source.pack_name,
                    source.source,
                    build_pack_plan(
                        pack_name=source.pack_name,
                        query=compile_query(
                            language="rust",
                            pack_name=source.pack_name,
                            source=source.source,
                            request_surface="artifact",
                        ),
                        query_text=source.source,
                        language="rust",
                    ),
                )
            )
        except _ENRICHMENT_ERRORS:
            continue
    return tuple(
        (pack_name, source, plan) for pack_name, source, plan in sort_pack_plans(source_rows)
    )


def _pack_sources() -> tuple[tuple[str, str], ...]:
    return tuple((pack_name, source) for pack_name, source, _ in _pack_source_rows())


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
            text = node_text(captured, source_bytes)
            if text is None or text in seen:
                continue
            seen.add(text)
            out.append(text)
            if len(out) >= limit:
                return out
    return out


def _pack_callbacks(*, query_source: str, source_bytes: bytes) -> QueryExecutionCallbacksV1 | None:
    if not has_custom_predicates(query_source):
        return None
    return QueryExecutionCallbacksV1(predicate_callback=make_query_predicate(source_bytes=source_bytes))


def _run_query_pack(
    *,
    pack_name: str,
    query_source: str,
    root: Node,
    source_bytes: bytes,
    context: QueryPackExecutionContextV1,
) -> _RustPackRunResultV1 | None:
    try:
        query = compile_query(
            language="rust",
            pack_name=pack_name,
            source=query_source,
            request_surface="artifact",
        )
        (
            pack_captures,
            pack_matches,
            pack_rows,
            pack_hits,
            capture_telemetry,
            match_telemetry,
        ) = execute_pack_rows_with_matches(
            query=query,
            query_name=pack_name,
            root=root,
            source_bytes=source_bytes,
            context=context,
        )
    except _ENRICHMENT_ERRORS:
        return None
    return _RustPackRunResultV1(
        pack_name=pack_name,
        query=query,
        captures=pack_captures,
        matches=pack_matches,
        rows=pack_rows,
        query_hits=pack_hits,
        capture_telemetry=capture_telemetry,
        match_telemetry=match_telemetry,
    )


def _collect_query_pack_captures(
    *,
    root: Node,
    source_bytes: bytes,
    windows: tuple[QueryWindowV1, ...],
    settings: QueryExecutionSettingsV1,
) -> tuple[
    dict[str, list[Node]],
    tuple[ObjectEvidenceRowV1, ...],
    tuple[TreeSitterQueryHitV1, ...],
    dict[str, object],
    tuple[InjectionPlanV1, ...],
    tuple[RustTagEventV1, ...],
]:
    accumulator = _RustPackAccumulatorV1()
    for pack_name, query_source in _pack_sources():
        result = _run_query_pack(
            pack_name=pack_name,
            query_source=query_source,
            root=root,
            source_bytes=source_bytes,
            context=QueryPackExecutionContextV1(
                windows=windows,
                settings=settings,
                callbacks=_pack_callbacks(query_source=query_source, source_bytes=source_bytes),
            ),
        )
        if result is None:
            continue
        accumulator.merge(result=result, source_bytes=source_bytes)
    return accumulator.finalize()


# ---------------------------------------------------------------------------
# Node / scope utilities
# ---------------------------------------------------------------------------


def _scope_name(scope_node: Node, source_bytes: bytes) -> str | None:
    kind = scope_node.type
    if kind == "impl_item":
        return node_text(child_by_field(scope_node, "type", _rust_field_ids()), source_bytes)
    if kind == "macro_invocation":
        return node_text(
            child_by_field(scope_node, "macro", _rust_field_ids()), source_bytes
        ) or node_text(
            child_by_field(scope_node, "name", _rust_field_ids()),
            source_bytes,
        )
    return node_text(child_by_field(scope_node, "name", _rust_field_ids()), source_bytes)


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
    params_node = child_by_field(fn_node, "parameters", _rust_field_ids())
    if params_node is None:
        return []
    param_list: list[str] = []
    for child in params_node.named_children:
        text = node_text(child, source_bytes)
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
        child_text = node_text(child, source_bytes)
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

    body_node = child_by_field(fn_node, "body", _rust_field_ids())
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
    vis_node = child_by_field(item_node, "visibility_modifier", _rust_field_ids())
    if vis_node is None:
        for child in item_node.children:
            if child.type == "visibility_modifier":
                vis_node = child
                break
    if vis_node is None:
        return "private"

    vis_text = node_text(vis_node, source_bytes)
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
            text = node_text(current, source_bytes)
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

    trait_node = child_by_field(impl_node, "trait", _rust_field_ids())
    if trait_node is not None:
        trait_text = node_text(trait_node, source_bytes)
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
    method_text = node_text(child_by_field(fn_node, "field", _rust_field_ids()), source_bytes)
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
        fn_node = child_by_field(node, "function", _rust_field_ids())
        if fn_node is not None:
            target_text = node_text(fn_node, source_bytes)
            if target_text is not None:
                result["call_target"] = _truncate(target_text, _MAX_CALL_TARGET_LEN)
            if fn_node.type == "field_expression":
                result.update(_extract_field_expression(fn_node, source_bytes))
    elif node.type == "macro_invocation":
        macro_text = node_text(child_by_field(node, "macro", _rust_field_ids()), source_bytes)
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
    body = child_by_field(struct_node, "body", _rust_field_ids())
    if body is None:
        return result

    field_nodes = [c for c in body.named_children if c.type == "field_declaration"]
    result["struct_field_count"] = len(field_nodes)

    fields: list[str] = []
    for field_node in field_nodes[:_MAX_FIELDS_SHOWN]:
        text = node_text(field_node, source_bytes)
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
    body = child_by_field(enum_node, "body", _rust_field_ids())
    if body is None:
        return result

    variant_nodes = [c for c in body.named_children if c.type == "enum_variant"]
    result["enum_variant_count"] = len(variant_nodes)

    variants: list[str] = []
    for variant_node in variant_nodes[:_MAX_VARIANTS_SHOWN]:
        text = node_text(variant_node, source_bytes)
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
        trait_node = child_by_field(impl_node, "trait", _rust_field_ids())
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
    fn_node = child_by_field(node, "function", _rust_field_ids())
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


def _query_windows_for_span(
    *,
    byte_start: int,
    byte_end: int,
    source_byte_len: int,
    changed_ranges: tuple[object, ...],
) -> tuple[QueryWindowV1, ...]:
    anchor_window = QueryWindowV1(start_byte=byte_start, end_byte=byte_end)
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


def _query_execution_plan(
    *,
    byte_start: int,
    byte_end: int,
    source_bytes: bytes,
    changed_ranges: tuple[object, ...],
    query_budget_ms: int | None,
    file_key: str | None,
) -> _RustQueryExecutionPlanV1:
    windows = _query_windows_for_span(
        byte_start=byte_start,
        byte_end=byte_end,
        source_byte_len=len(source_bytes),
        changed_ranges=changed_ranges,
    )
    enqueue_windows(
        language="rust",
        file_key=file_key or "<memory>",
        windows=windows,
    )
    effective_budget_ms = adaptive_query_budget_ms(
        language="rust",
        fallback_budget_ms=query_budget_ms if query_budget_ms is not None else 200,
    )
    return _RustQueryExecutionPlanV1(
        windows=windows,
        settings=QueryExecutionSettingsV1(
            budget_ms=effective_budget_ms,
            has_change_context=bool(changed_ranges),
            window_mode="containment_preferred",
        ),
    )


def _collect_query_bundle(
    *,
    root: Node,
    source_bytes: bytes,
    plan: _RustQueryExecutionPlanV1,
) -> _RustQueryCollectionV1:
    captures, rows, query_hits, query_telemetry, injection_plan, tag_events = _collect_query_pack_captures(
        root=root,
        source_bytes=source_bytes,
        windows=plan.windows,
        settings=plan.settings,
    )
    diagnostics = collect_diagnostic_rows(
        language="rust",
        root=root,
        windows=plan.windows,
        match_limit=1024,
    )
    return _RustQueryCollectionV1(
        windows=plan.windows,
        captures=captures,
        rows=rows,
        query_hits=query_hits,
        query_telemetry=query_telemetry,
        injection_plan=injection_plan,
        tag_events=tag_events,
        diagnostics=diagnostics,
    )


def _base_query_pack_payload(query_telemetry: dict[str, object]) -> dict[str, object]:
    payload: dict[str, object] = {"query_pack_telemetry": query_telemetry} if query_telemetry else {}
    if query_telemetry:
        payload["query_runtime"] = _aggregate_query_runtime(query_telemetry)
    return payload


def _fact_payload_from_collection(
    *,
    captures: dict[str, list[Node]],
    rows: tuple[ObjectEvidenceRowV1, ...],
    source_bytes: bytes,
) -> dict[str, list[str]]:
    definitions, references, calls, imports, modules = _rust_fact_lists(captures, source_bytes)
    _extend_rust_fact_lists_from_rows(
        rows=rows,
        definitions=definitions,
        references=references,
        calls=calls,
        imports=imports,
        modules=modules,
    )
    return _rust_fact_payload(
        definitions=definitions,
        references=references,
        calls=calls,
        imports=imports,
        modules=modules,
    )


def _collect_query_pack_payload(
    *,
    root: Node,
    source_bytes: bytes,
    byte_span: tuple[int, int],
    changed_ranges: tuple[object, ...] = (),
    query_budget_ms: int | None = None,
    file_key: str | None = None,
) -> dict[str, object]:
    byte_start, byte_end = byte_span
    if byte_end <= byte_start:
        return {}

    plan = _query_execution_plan(
        byte_start=byte_start,
        byte_end=byte_end,
        source_bytes=source_bytes,
        changed_ranges=changed_ranges,
        query_budget_ms=query_budget_ms,
        file_key=file_key,
    )
    collection = _collect_query_bundle(
        root=root,
        source_bytes=source_bytes,
        plan=plan,
    )
    payload = _base_query_pack_payload(collection.query_telemetry)
    payload["rust_tree_sitter_facts"] = _fact_payload_from_collection(
        captures=collection.captures,
        rows=collection.rows,
        source_bytes=source_bytes,
    )
    module_rows = _module_rows_from_matches(rows=collection.rows, file_key=file_key)
    import_rows = _import_rows_from_matches(rows=collection.rows, module_rows=module_rows)
    payload["rust_module_rows"] = module_rows
    payload["rust_import_rows"] = import_rows
    _attach_query_pack_payload(
        payload=payload,
        artifacts=_RustQueryPackArtifactsV1(
            rows=collection.rows,
            query_hits=collection.query_hits,
            diagnostics=collection.diagnostics,
            injection_plan=collection.injection_plan,
            tag_events=collection.tag_events,
            source_bytes=source_bytes,
            file_key=file_key,
        ),
    )
    return payload


def _rust_fact_lists(
    captures: dict[str, list[Node]],
    source_bytes: bytes,
) -> tuple[list[str], list[str], list[str], list[str], list[str]]:
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
    macro_calls = _capture_texts_from_captures(captures, source_bytes, "call.macro.path")
    for macro_name in macro_calls:
        normalized = macro_name if macro_name.endswith("!") else f"{macro_name}!"
        if normalized not in calls:
            calls.append(normalized)
    imports = _capture_texts_from_captures(
        captures,
        source_bytes,
        "import.path",
        "import.extern.name",
    )
    modules = _capture_texts_from_captures(
        captures,
        source_bytes,
        "module.name",
        "def.module.name",
    )
    return definitions, references, calls, imports, modules


def _extend_rust_fact_lists_from_rows(
    *,
    rows: tuple[ObjectEvidenceRowV1, ...],
    definitions: list[str],
    references: list[str],
    calls: list[str],
    imports: list[str],
    modules: list[str],
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
                keys=("call.target",),
            )
            _extend_macro_fact_list(
                target=calls,
                captures=row.captures,
                key="call.macro.path",
            )
        elif row.emit == "imports":
            _extend_fact_list(
                target=imports,
                captures=row.captures,
                keys=("import.path", "import.extern.name"),
            )
        elif row.emit == "modules":
            _extend_fact_list(
                target=modules,
                captures=row.captures,
                keys=("module.name", "def.module.name"),
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


def _extend_macro_fact_list(
    *,
    target: list[str],
    captures: Mapping[str, object],
    key: str,
) -> None:
    value = captures.get(key)
    if not isinstance(value, str) or not value:
        return
    normalized = value if value.endswith("!") else f"{value}!"
    if normalized not in target:
        target.append(normalized)


def _rust_fact_payload(
    *,
    definitions: list[str],
    references: list[str],
    calls: list[str],
    imports: list[str],
    modules: list[str],
) -> dict[str, list[str]]:
    return {
        "definitions": definitions[:_MAX_FIELDS_SHOWN],
        "references": references[:_MAX_FIELDS_SHOWN],
        "calls": calls[:_MAX_FIELDS_SHOWN],
        "imports": imports[:_MAX_FIELDS_SHOWN],
        "modules": modules[:_MAX_FIELDS_SHOWN],
    }


def _module_rows_from_matches(
    *,
    rows: tuple[ObjectEvidenceRowV1, ...],
    file_key: str | None,
) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    seen: set[str] = set()
    for row in rows:
        if row.emit != "modules":
            continue
        module_name = None
        for key in ("module.name", "def.module.name"):
            value = row.captures.get(key)
            if isinstance(value, str) and value:
                module_name = value
                break
        if module_name is None or module_name in seen:
            continue
        seen.add(module_name)
        out.append(
            {
                "module_id": f"module:{module_name}",
                "module_name": module_name,
                "file_path": file_key,
            }
        )
    return out


def _import_rows_from_matches(
    *,
    rows: tuple[ObjectEvidenceRowV1, ...],
    module_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    module_lookup = {
        str(row["module_name"]): str(row["module_id"])
        for row in module_rows
        if isinstance(row.get("module_name"), str) and isinstance(row.get("module_id"), str)
    }
    default_source = module_rows[0]["module_id"] if module_rows else "module:<root>"
    out: list[dict[str, object]] = []
    seen: set[tuple[str, str, str, bool]] = set()
    for row in rows:
        if row.emit != "imports":
            continue
        target_path = None
        for key in ("import.path", "import.extern.name"):
            value = row.captures.get(key)
            if isinstance(value, str) and value:
                target_path = value
                break
        if target_path is None:
            continue
        visibility_capture = row.captures.get("import.visibility")
        visibility_text = visibility_capture if isinstance(visibility_capture, str) else ""
        visibility = "public" if visibility_text.startswith("pub") else "private"
        is_reexport = visibility == "public"
        source_module_id = str(default_source)
        for module_name, module_id in module_lookup.items():
            if target_path.startswith(f"{module_name}::"):
                source_module_id = module_id
                break
        key = (source_module_id, target_path, visibility, is_reexport)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "source_module_id": source_module_id,
                "target_path": target_path,
                "visibility": visibility,
                "is_reexport": is_reexport,
            }
        )
    return out


def _macro_expansion_requests(
    *,
    rows: tuple[ObjectEvidenceRowV1, ...],
    source_bytes: bytes,
    file_key: str | None,
) -> tuple[RustMacroExpansionRequestV1, ...]:
    file_path = file_key if isinstance(file_key, str) and file_key else "<memory>.rs"
    out: list[RustMacroExpansionRequestV1] = []
    seen: set[str] = set()
    for row in rows:
        if row.emit != "calls":
            continue
        macro_name = row.captures.get("call.macro.path")
        if not isinstance(macro_name, str) or not macro_name:
            continue
        line, col = byte_offset_to_line_col(source_bytes, row.anchor_start_byte)
        macro_call_id = f"{file_path}:{row.anchor_start_byte}:{row.anchor_end_byte}:{macro_name}"
        if macro_call_id in seen:
            continue
        seen.add(macro_call_id)
        out.append(
            RustMacroExpansionRequestV1(
                file_path=file_path,
                line=max(0, int(line) - 1),
                col=max(0, int(col)),
                macro_call_id=macro_call_id,
            )
        )
    return tuple(out)


def _aggregate_query_runtime(query_telemetry: dict[str, object]) -> dict[str, object]:
    did_exceed_match_limit = False
    cancelled = False
    window_split_count = 0
    degrade_reasons: set[str] = set()
    for telemetry_row in query_telemetry.values():
        if not isinstance(telemetry_row, dict):
            continue
        for phase in ("captures", "matches"):
            phase_row = telemetry_row.get(phase)
            if not isinstance(phase_row, dict):
                continue
            did_exceed_match_limit = did_exceed_match_limit or bool(
                phase_row.get("exceeded_match_limit")
            )
            cancelled = cancelled or bool(phase_row.get("cancelled"))
            split = phase_row.get("window_split_count")
            if isinstance(split, int) and not isinstance(split, bool):
                window_split_count += split
            reason = phase_row.get("degrade_reason")
            if isinstance(reason, str) and reason:
                degrade_reasons.add(reason)
    return {
        "did_exceed_match_limit": did_exceed_match_limit,
        "cancelled": cancelled,
        "window_split_count": window_split_count,
        "degrade_reasons": sorted(degrade_reasons),
    }


def _attach_query_pack_payload(
    *,
    payload: dict[str, object],
    artifacts: _RustQueryPackArtifactsV1,
) -> None:
    payload["query_pack_bundle"] = msgspec.to_builtins(
        load_rust_grammar_bundle(profile_name="rust_search_enriched")
    )
    payload["cst_query_hits"] = [msgspec.to_builtins(row) for row in artifacts.query_hits]
    payload["cst_diagnostics"] = [msgspec.to_builtins(row) for row in artifacts.diagnostics]
    payload["query_pack_injections"] = [
        msgspec.to_builtins(row) for row in artifacts.injection_plan
    ]
    payload["query_pack_tags"] = [msgspec.to_builtins(row) for row in artifacts.tag_events]
    payload["query_pack_tag_summary"] = {
        "definitions": sum(1 for row in artifacts.tag_events if row.role == "definition"),
        "references": sum(1 for row in artifacts.tag_events if row.role == "reference"),
    }
    macro_requests = _macro_expansion_requests(
        rows=artifacts.rows,
        source_bytes=artifacts.source_bytes,
        file_key=artifacts.file_key,
    )
    if macro_requests:
        payload["macro_expansion_requests"] = [msgspec.to_builtins(row) for row in macro_requests]
    payload["query_pack_injection_profiles"] = sorted(
        {
            str(row.profile_name)
            for row in artifacts.injection_plan
            if isinstance(getattr(row, "profile_name", None), str)
        }
    )
    rust_plan = tuple(row for row in artifacts.injection_plan if row.language == "rust")
    if rust_plan:
        payload["query_pack_injection_runtime"] = msgspec.to_builtins(
            parse_injected_ranges(
                source_bytes=artifacts.source_bytes,
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
    query_budget_ms: int | None = None,
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
        tree, source_bytes, changed_ranges = _parse_with_session(
            source,
            cache_key=cache_key,
        )
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
                byte_span=(
                    int(getattr(node, "start_byte", 0)),
                    int(getattr(node, "end_byte", 0)),
                ),
                changed_ranges=changed_ranges,
                query_budget_ms=query_budget_ms,
                file_key=cache_key,
            )
        )
    except _ENRICHMENT_ERRORS:
        return None
    else:
        return canonicalize_rust_lane_payload(payload)


def enrich_rust_context_by_byte_range(
    source: str,
    *,
    byte_start: int,
    byte_end: int,
    cache_key: str | None = None,
    max_scope_depth: int = _DEFAULT_SCOPE_DEPTH,
    query_budget_ms: int | None = None,
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
        tree, source_bytes, changed_ranges = _parse_with_session(
            source,
            cache_key=cache_key,
        )
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
                byte_span=(byte_start, byte_end),
                changed_ranges=changed_ranges,
                query_budget_ms=query_budget_ms,
                file_key=cache_key,
            )
        )
    except _ENRICHMENT_ERRORS:
        return None
    else:
        return canonicalize_rust_lane_payload(payload)


__all__ = [
    "MAX_SOURCE_BYTES",
    "clear_tree_sitter_rust_cache",
    "enrich_rust_context",
    "enrich_rust_context_by_byte_range",
    "get_tree_sitter_rust_cache_stats",
    "is_tree_sitter_rust_available",
]
