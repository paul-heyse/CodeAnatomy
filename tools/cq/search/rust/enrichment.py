"""Rust enrichment orchestration (ast-grep first, tree-sitter gap-fill)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec
from ast_grep_py import SgRoot

from tools.cq.core.locations import byte_offset_to_line_col
from tools.cq.core.typed_boundary import BoundaryDecodeError, convert_lax
from tools.cq.search._shared.core import RustEnrichmentRequest
from tools.cq.search._shared.core import sg_node_text as _shared_sg_node_text
from tools.cq.search._shared.core import source_hash as _shared_source_hash
from tools.cq.search.enrichment.core import (
    append_source,
    has_value,
    merge_gap_fill_payload,
    normalize_rust_payload,
    set_degraded,
)
from tools.cq.search.pipeline.classifier import get_node_index
from tools.cq.search.rust.evidence import attach_rust_evidence
from tools.cq.search.rust.macro_expansion_bridge import expand_macros
from tools.cq.search.rust.macro_expansion_contracts import RustMacroExpansionRequestV1
from tools.cq.search.tree_sitter.rust_lane.runtime import (
    enrich_rust_context_by_byte_range as _ts_enrich,
)
from tools.cq.search.tree_sitter.rust_lane.runtime import (
    is_tree_sitter_rust_available,
)

if TYPE_CHECKING:
    from ast_grep_py import SgNode

_ENRICHMENT_ERRORS = (RuntimeError, TypeError, ValueError, AttributeError, UnicodeError)
_MAX_AST_CACHE_ENTRIES = 64
_AST_CACHE: dict[str, tuple[SgRoot, str]] = {}

_DEFAULT_SCOPE_DEPTH = 24
_CROSSCHECK_ENV = "CQ_RUST_ENRICHMENT_CROSSCHECK"
_MAX_FUNCTION_PARAMS = 12
_MAX_ATTRIBUTES = 10

_SCOPE_KINDS: frozenset[str] = frozenset(
    {
        "function_item",
        "struct_item",
        "enum_item",
        "trait_item",
        "impl_item",
        "mod_item",
        "macro_invocation",
    }
)

_RUST_TEST_ATTRS: frozenset[str] = frozenset(
    {
        "test",
        "tokio::test",
        "rstest",
        "async_std::test",
    }
)

_CROSSCHECK_METADATA_KEYS: frozenset[str] = frozenset(
    {"enrichment_status", "enrichment_sources", "degrade_reason", "language"}
)


def _source_hash(source_bytes: bytes) -> str:
    return _shared_source_hash(source_bytes)


def _get_sg_root(source: str, *, cache_key: str | None) -> SgRoot:
    source_bytes = source.encode("utf-8", errors="replace")
    if cache_key is None:
        return SgRoot(source, "rust")

    content_hash = _source_hash(source_bytes)
    cached = _AST_CACHE.get(cache_key)
    if cached is not None:
        root, cached_hash = cached
        if cached_hash == content_hash:
            return root

    root = SgRoot(source, "rust")
    if len(_AST_CACHE) >= _MAX_AST_CACHE_ENTRIES and cache_key not in _AST_CACHE:
        oldest_key = next(iter(_AST_CACHE))
        del _AST_CACHE[oldest_key]
    _AST_CACHE[cache_key] = (root, content_hash)
    return root


def clear_rust_enrichment_cache() -> None:
    """Clear ast-grep parse cache for Rust enrichment."""
    _AST_CACHE.clear()


def _node_text(node: SgNode | None) -> str | None:
    return _shared_sg_node_text(node)


def _scope_name(scope_node: SgNode) -> str | None:
    kind = scope_node.kind()
    if kind == "impl_item":
        return _node_text(scope_node.field("type"))
    if kind == "macro_invocation":
        return _node_text(scope_node.field("macro")) or _node_text(scope_node.field("name"))
    return _node_text(scope_node.field("name"))


def _find_scope(node: SgNode, *, max_depth: int) -> SgNode | None:
    current: SgNode | None = node
    depth = 0
    while current is not None and depth < max_depth:
        if current.kind() in _SCOPE_KINDS:
            return current
        current = current.parent()
        depth += 1
    return None


def _find_ancestor(node: SgNode, kind: str, *, max_depth: int) -> SgNode | None:
    current: SgNode | None = node.parent()
    depth = 0
    while current is not None and depth < max_depth:
        if current.kind() == kind:
            return current
        current = current.parent()
        depth += 1
    return None


def _scope_chain(node: SgNode, *, max_depth: int) -> list[str]:
    chain: list[str] = []
    current: SgNode | None = node
    depth = 0
    while current is not None and depth < max_depth:
        kind = current.kind()
        if kind in _SCOPE_KINDS:
            name = _scope_name(current)
            chain.append(f"{kind}:{name}" if name else kind)
        current = current.parent()
        depth += 1
    return chain


def _extract_visibility(item: SgNode) -> str:
    text = item.text().lstrip()
    if text.startswith("pub(crate)"):
        return "pub(crate)"
    if text.startswith("pub(super)"):
        return "pub(super)"
    if text.startswith("pub"):
        return "pub"
    return "private"


def _extract_function_signature(node: SgNode) -> dict[str, object]:
    if node.kind() != "function_item":
        return {}

    result = _extract_function_params(node)
    result.update(_extract_function_type_details(node))
    signature_fields = _extract_signature_flags(node)
    result.update(signature_fields)
    return result


def _extract_function_params(node: SgNode) -> dict[str, object]:
    params_node = node.field("parameters")
    if params_node is None:
        return {"params": []}
    params: list[str] = []
    for child in params_node.children():
        if not child.is_named():
            continue
        text = child.text().strip()
        if text:
            params.append(text)
        if len(params) >= _MAX_FUNCTION_PARAMS:
            break
    return {"params": params}


def _extract_function_type_details(node: SgNode) -> dict[str, object]:
    result: dict[str, object] = {}

    return_type = _node_text(node.field("return_type"))
    if return_type is not None:
        result["return_type"] = return_type

    type_params = _node_text(node.field("type_parameters"))
    if type_params is not None:
        result["generics"] = type_params
    return result


def _extract_signature_flags(node: SgNode) -> dict[str, object]:
    sig_text = node.text()
    body_idx = sig_text.find("{")
    if body_idx > 0:
        sig_text = sig_text[:body_idx].strip()

    if sig_text:
        return {
            "signature": sig_text[:200],
            "is_async": " async " in f" {sig_text} " or sig_text.startswith("async "),
            "is_unsafe": " unsafe " in f" {sig_text} " or sig_text.startswith("unsafe "),
        }
    return {"is_async": False, "is_unsafe": False}


def _extract_struct_shape(node: SgNode) -> dict[str, object]:
    if node.kind() != "struct_item":
        return {}
    body = node.field("body")
    if body is None:
        return {}
    fields: list[str] = []
    field_nodes = [
        child
        for child in body.children()
        if child.is_named() and child.kind() == "field_declaration"
    ]
    for member in field_nodes[:8]:
        text = member.text().strip()
        if text:
            fields.append(text[:60])
    remaining = len(field_nodes) - 8
    if remaining > 0:
        fields.append(f"... and {remaining} more")
    return {
        "struct_field_count": len(field_nodes),
        "struct_fields": fields,
    }


def _extract_enum_shape(node: SgNode) -> dict[str, object]:
    if node.kind() != "enum_item":
        return {}
    body = node.field("body")
    if body is None:
        return {}
    variants: list[str] = []
    variant_nodes = [
        child for child in body.children() if child.is_named() and child.kind() == "enum_variant"
    ]
    for member in variant_nodes[:12]:
        text = member.text().strip()
        if text:
            variants.append(text[:60])
    remaining = len(variant_nodes) - 12
    if remaining > 0:
        variants.append(f"... and {remaining} more")
    return {
        "enum_variant_count": len(variant_nodes),
        "enum_variants": variants,
    }


def _extract_call_target(node: SgNode) -> dict[str, object]:
    if node.kind() == "call_expression":
        function = node.field("function")
        if function is None:
            return {}
        target = function.text().strip()
        data: dict[str, object] = {"call_target": target[:120]} if target else {}
        if function.kind() == "field_expression":
            receiver = _node_text(function.field("value"))
            method = _node_text(function.field("field"))
            if receiver:
                data["call_receiver"] = receiver[:80]
            if method:
                data["call_method"] = method
        return data
    if node.kind() == "macro_invocation":
        macro = _node_text(node.field("macro")) or _node_text(node.field("name"))
        if macro:
            return {"macro_name": macro}
    return {}


def _extract_attributes(node: SgNode) -> list[str]:
    attrs: list[str] = []
    for child in node.children():
        if child.kind() != "attribute_item":
            continue
        text = child.text().strip()
        if text.startswith("#!["):
            continue
        if text.startswith("#[") and text.endswith("]"):
            text = text[2:-1].strip()
        if text:
            attrs.append(text[:60])
        if len(attrs) >= _MAX_ATTRIBUTES:
            break
    return attrs


def _classify_item_role(
    node: SgNode,
    scope: SgNode | None,
    *,
    max_scope_depth: int,
    attrs: list[str],
) -> str:
    kind = node.kind()
    role = _classify_call_like_role(node, kind)
    if role is not None:
        return role

    fn_target = _resolve_function_target(node, scope)
    if fn_target is not None:
        return _classify_function_role(fn_target, attrs=attrs, max_scope_depth=max_scope_depth)

    return _NON_FUNCTION_ROLE_BY_KIND.get(kind, kind)


_NON_FUNCTION_ROLE_BY_KIND: dict[str, str] = {
    "use_declaration": "use_import",
    "field_declaration": "struct_field",
    "enum_variant": "enum_variant",
}


def _classify_call_like_role(node: SgNode, kind: str) -> str | None:
    if kind == "macro_invocation":
        return "macro_call"
    if kind != "call_expression":
        return None
    function = node.field("function")
    if function is not None and function.kind() == "field_expression":
        return "method_call"
    return "function_call"


def _resolve_function_target(node: SgNode, scope: SgNode | None) -> SgNode | None:
    if node.kind() == "function_item":
        return node
    if scope is not None and scope.kind() == "function_item":
        return scope
    return None


def _classify_function_role(
    fn_target: SgNode,
    *,
    attrs: list[str],
    max_scope_depth: int,
) -> str:
    if any(attr in _RUST_TEST_ATTRS for attr in attrs):
        return "test_function"
    impl_node = _find_ancestor(fn_target, "impl_item", max_depth=max_scope_depth)
    if impl_node is None:
        return "free_function"
    return "trait_method" if impl_node.field("trait") is not None else "method"


def _canonicalize_tree_sitter_payload(payload: dict[str, object] | None) -> dict[str, object]:
    if not payload:
        return {}
    data = dict(payload)
    if "field_count" in data and "struct_field_count" not in data:
        data["struct_field_count"] = data.pop("field_count")
    if "fields" in data and "struct_fields" not in data:
        data["struct_fields"] = data.pop("fields")
    if "variant_count" in data and "enum_variant_count" not in data:
        data["enum_variant_count"] = data.pop("variant_count")
    if "variants" in data and "enum_variants" not in data:
        data["enum_variants"] = data.pop("variants")
    return data


def _merge_gap_fill(primary: dict[str, object], secondary: dict[str, object]) -> dict[str, object]:
    return merge_gap_fill_payload(primary, secondary)


def _build_ast_grep_payload(
    source: str,
    *,
    byte_start: int,
    byte_end: int,
    cache_key: str | None,
    max_scope_depth: int,
) -> dict[str, object] | None:
    source_bytes = source.encode("utf-8", errors="replace")
    sg_root = _get_sg_root(source, cache_key=cache_key)

    node = _resolve_rust_node(
        sg_root=sg_root,
        source_bytes=source_bytes,
        byte_start=byte_start,
        byte_end=byte_end,
        cache_key=cache_key,
    )
    if node is None:
        return None

    scope = _find_scope(node, max_depth=max_scope_depth)
    payload = _base_rust_payload(node=node, scope=scope, max_scope_depth=max_scope_depth)
    attrs = _apply_definition_metadata(payload=payload, node=node, scope=scope)
    _apply_kind_extractors(payload=payload, node=node, scope=scope)
    payload["item_role"] = _classify_item_role(
        node, scope, max_scope_depth=max_scope_depth, attrs=attrs
    )
    return payload


def _resolve_rust_node(
    *,
    sg_root: SgRoot,
    source_bytes: bytes,
    byte_start: int,
    byte_end: int,
    cache_key: str | None,
) -> SgNode | None:
    line, col = byte_offset_to_line_col(source_bytes, byte_start)
    index = get_node_index(Path(cache_key or "<memory>.rs"), sg_root, lang="rust")
    node = index.find_containing(line, col)
    if node is not None:
        return node
    end_line, end_col = byte_offset_to_line_col(source_bytes, max(byte_start, byte_end - 1))
    return index.find_containing(end_line, end_col)


def _base_rust_payload(
    *,
    node: SgNode,
    scope: SgNode | None,
    max_scope_depth: int,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "language": "rust",
        "node_kind": node.kind(),
        "scope_chain": _scope_chain(node, max_depth=max_scope_depth),
        "enrichment_status": "applied",
        "enrichment_sources": ["ast_grep"],
    }
    if scope is not None:
        payload["scope_kind"] = scope.kind()
        scope_name = _scope_name(scope)
        if scope_name:
            payload["scope_name"] = scope_name
    return payload


def _resolve_definition_target(node: SgNode, scope: SgNode | None) -> SgNode | None:
    if node.kind() in {
        "function_item",
        "struct_item",
        "enum_item",
        "trait_item",
        "impl_item",
        "mod_item",
    }:
        return node
    return scope


def _apply_definition_metadata(
    *,
    payload: dict[str, object],
    node: SgNode,
    scope: SgNode | None,
) -> list[str]:
    def_target = _resolve_definition_target(node, scope)
    if def_target is None:
        return []
    payload["visibility"] = _extract_visibility(def_target)
    attrs = _extract_attributes(def_target)
    if attrs:
        payload["attributes"] = attrs
    return attrs


def _resolve_shape_target(node: SgNode, scope: SgNode | None, kind: str) -> SgNode | None:
    if scope is not None and scope.kind() == kind:
        return scope
    if node.kind() == kind:
        return node
    return None


def _apply_kind_extractors(
    *,
    payload: dict[str, object],
    node: SgNode,
    scope: SgNode | None,
) -> None:
    fn_target = _resolve_function_target(node, scope)
    if fn_target is not None:
        payload.update(_extract_function_signature(fn_target))

    struct_target = _resolve_shape_target(node, scope, "struct_item")
    if struct_target is not None:
        payload.update(_extract_struct_shape(struct_target))

    enum_target = _resolve_shape_target(node, scope, "enum_item")
    if enum_target is not None:
        payload.update(_extract_enum_shape(enum_target))

    if node.kind() in {"call_expression", "macro_invocation"}:
        payload.update(_extract_call_target(node))


def _crosscheck_mismatches(
    ast_payload: dict[str, object],
    ts_payload: dict[str, object],
) -> list[dict[str, object]]:
    mismatches: list[dict[str, object]] = []
    for key in sorted(set(ast_payload).intersection(ts_payload)):
        if key in _CROSSCHECK_METADATA_KEYS:
            continue
        left = ast_payload.get(key)
        right = ts_payload.get(key)
        if has_value(left) and has_value(right) and left != right:
            mismatches.append({"field": key, "ast_grep": left, "tree_sitter": right})
    return mismatches


def _macro_requests(payload: dict[str, object]) -> tuple[RustMacroExpansionRequestV1, ...]:
    raw_requests = payload.get("macro_expansion_requests")
    if not isinstance(raw_requests, list):
        return ()
    rows: list[RustMacroExpansionRequestV1] = []
    for item in raw_requests:
        if not isinstance(item, dict):
            continue
        try:
            rows.append(convert_lax(item, type_=RustMacroExpansionRequestV1))
        except BoundaryDecodeError:
            continue
    return tuple(rows)


def _attach_macro_expansions(
    payload: dict[str, object],
    *,
    macro_client: object | None,
) -> None:
    if macro_client is None:
        return
    requests = _macro_requests(payload)
    if not requests:
        return
    results = expand_macros(client=macro_client, requests=requests)
    payload["macro_expansion_results"] = [msgspec.to_builtins(row) for row in results]
    if any(bool(row.applied) for row in results):
        append_source(payload, "rust_analyzer")


def enrich_rust_context_by_byte_range(
    source: str,
    *,
    byte_start: int,
    byte_end: int,
    cache_key: str | None = None,
    max_scope_depth: int = _DEFAULT_SCOPE_DEPTH,
    query_budget_ms: int | None = None,
) -> dict[str, object] | None:
    """Best-effort Rust enrichment with deterministic ast-grep-first merging.

    Returns:
    -------
    dict[str, object] | None
        Enrichment payload or ``None`` when no context could be resolved.
    """
    if byte_start < 0 or byte_end <= byte_start:
        return None
    source_bytes = source.encode("utf-8", errors="replace")
    if byte_end > len(source_bytes):
        return None

    ast_payload = _safe_ast_grep_payload(
        source=source,
        byte_start=byte_start,
        byte_end=byte_end,
        cache_key=cache_key,
        max_scope_depth=max_scope_depth,
    )
    ts_payload = _safe_tree_sitter_payload(
        source=source,
        byte_start=byte_start,
        byte_end=byte_end,
        cache_key=cache_key,
        max_scope_depth=max_scope_depth,
        query_budget_ms=query_budget_ms,
    )

    if ast_payload is None and not ts_payload:
        return None

    merged = _merge_enrichment_payloads(ast_payload=ast_payload, ts_payload=ts_payload)
    macro_client = merged.pop("_macro_client", None)
    _attach_macro_expansions(merged, macro_client=macro_client)
    attach_rust_evidence(merged)

    if os.getenv(_CROSSCHECK_ENV) == "1" and ts_payload and ast_payload is not None:
        mismatches = _crosscheck_mismatches(ast_payload, ts_payload)
        if mismatches:
            merged["crosscheck_mismatches"] = mismatches
            set_degraded(merged, "crosscheck mismatch")

    return normalize_rust_payload(merged)


def _safe_ast_grep_payload(
    *,
    source: str,
    byte_start: int,
    byte_end: int,
    cache_key: str | None,
    max_scope_depth: int,
) -> dict[str, object] | None:
    try:
        return _build_ast_grep_payload(
            source,
            byte_start=byte_start,
            byte_end=byte_end,
            cache_key=cache_key,
            max_scope_depth=max_scope_depth,
        )
    except _ENRICHMENT_ERRORS:
        return None


def _safe_tree_sitter_payload(
    *,
    source: str,
    byte_start: int,
    byte_end: int,
    cache_key: str | None,
    max_scope_depth: int,
    query_budget_ms: int | None,
) -> dict[str, object]:
    try:
        return _canonicalize_tree_sitter_payload(
            _ts_enrich(
                source,
                byte_start=byte_start,
                byte_end=byte_end,
                cache_key=cache_key,
                max_scope_depth=max_scope_depth,
                query_budget_ms=query_budget_ms,
            )
        )
    except _ENRICHMENT_ERRORS:
        return {}


def _merge_enrichment_payloads(
    *,
    ast_payload: dict[str, object] | None,
    ts_payload: dict[str, object],
) -> dict[str, object]:
    if ast_payload is None:
        merged = dict(ts_payload)
        append_source(merged, "tree_sitter")
        merged.setdefault("enrichment_status", "applied")
        merged.setdefault("language", "rust")
        return merged

    merged = _merge_gap_fill(ast_payload, ts_payload)
    if ts_payload:
        append_source(merged, "tree_sitter")
    _apply_upstream_degradation(merged, ast_payload=ast_payload, ts_payload=ts_payload)
    return merged


def _apply_upstream_degradation(
    merged: dict[str, object],
    *,
    ast_payload: dict[str, object],
    ts_payload: dict[str, object],
) -> None:
    ast_status = str(ast_payload.get("enrichment_status", "applied"))
    ts_status = str(ts_payload.get("enrichment_status", "applied"))
    if ast_status != "degraded" and ts_status != "degraded":
        return

    set_degraded(merged, "upstream degraded")
    reasons: list[str] = []
    ast_reason = ast_payload.get("degrade_reason")
    ts_reason = ts_payload.get("degrade_reason")
    if isinstance(ast_reason, str) and ast_reason:
        reasons.append(f"ast_grep:{ast_reason}")
    if isinstance(ts_reason, str) and ts_reason:
        reasons.append(f"tree_sitter:{ts_reason}")
    if reasons:
        merged["degrade_reason"] = "; ".join(reasons)


def lint_rust_enrichment_schema() -> list[str]:
    """Run lightweight schema consistency checks for Rust enrichment keys.

    Returns:
    -------
    list[str]
        Schema consistency errors discovered in the sample payload.
    """
    errors: list[str] = []
    source = 'pub struct S { a: i32 }\nenum E { A, B }\nfn f() { println!("x"); }\n'
    payload = enrich_rust_context_by_byte_range(
        source,
        byte_start=0,
        byte_end=3,
        cache_key="__schema_lint__.rs",
    )
    if payload is None:
        errors.append("enrichment payload unavailable for schema lint sample")
        return errors
    forbidden = {"field_count", "fields", "variant_count", "variants"}
    errors.extend(f"deprecated key emitted: {key}" for key in forbidden if key in payload)
    errors.extend(
        f"missing required key: {required}"
        for required in ("language", "enrichment_status", "enrichment_sources")
        if required not in payload
    )
    return errors


def runtime_available() -> bool:
    """Return whether Rust tree-sitter runtime dependencies are available."""
    return is_tree_sitter_rust_available()


def enrich_context_by_byte_range(
    source: str,
    *,
    byte_start: int,
    byte_end: int,
) -> dict[str, object]:
    """Run Rust byte-range enrichment through the consolidated lane."""
    payload = enrich_rust_context_by_byte_range(
        source,
        byte_start=byte_start,
        byte_end=byte_end,
    )
    return payload if isinstance(payload, dict) else {}


def extract_rust_context(request: RustEnrichmentRequest) -> dict[str, object]:
    """Extract Rust enrichment payload for a byte-range request."""
    payload = enrich_rust_context_by_byte_range(
        request.source,
        byte_start=request.byte_start,
        byte_end=request.byte_end,
        cache_key=request.cache_key,
        max_scope_depth=request.max_scope_depth,
        query_budget_ms=request.query_budget_ms,
    )
    return payload if isinstance(payload, dict) else {}


__all__ = [
    "clear_rust_enrichment_cache",
    "enrich_context_by_byte_range",
    "enrich_rust_context_by_byte_range",
    "extract_rust_context",
    "lint_rust_enrichment_schema",
    "runtime_available",
]
