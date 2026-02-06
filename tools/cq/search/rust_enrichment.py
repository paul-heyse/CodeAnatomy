"""Rust enrichment orchestration (ast-grep first, tree-sitter gap-fill)."""

from __future__ import annotations

import os
from hashlib import blake2b
from pathlib import Path
from typing import TYPE_CHECKING

from ast_grep_py import SgRoot

from tools.cq.core.locations import byte_offset_to_line_col
from tools.cq.search.classifier import get_node_index
from tools.cq.search.tree_sitter_rust import enrich_rust_context_by_byte_range as _ts_enrich

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

_METADATA_KEYS: frozenset[str] = frozenset(
    {
        "enrichment_status",
        "enrichment_sources",
        "degrade_reason",
        "language",
    }
)


def _source_hash(source_bytes: bytes) -> str:
    return blake2b(source_bytes, digest_size=16).hexdigest()


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


def _has_value(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


def _node_text(node: SgNode | None) -> str | None:
    if node is None:
        return None
    text = node.text().strip()
    return text if text else None


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
    result: dict[str, object] = {}
    if node.kind() != "function_item":
        return result

    params_node = node.field("parameters")
    params: list[str] = []
    if params_node is not None:
        for child in params_node.children():
            if not child.is_named():
                continue
            text = child.text().strip()
            if text:
                params.append(text)
            if len(params) >= _MAX_FUNCTION_PARAMS:
                break
    result["params"] = params

    return_type = _node_text(node.field("return_type"))
    if return_type is not None:
        result["return_type"] = return_type

    type_params = _node_text(node.field("type_parameters"))
    if type_params is not None:
        result["generics"] = type_params

    sig_text = node.text()
    body_idx = sig_text.find("{")
    if body_idx > 0:
        sig_text = sig_text[:body_idx].strip()
    if sig_text:
        result["signature"] = sig_text[:200]
        result["is_async"] = " async " in f" {sig_text} " or sig_text.startswith("async ")
        result["is_unsafe"] = " unsafe " in f" {sig_text} " or sig_text.startswith("unsafe ")
    else:
        result["is_async"] = False
        result["is_unsafe"] = False
    return result


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
    if kind == "macro_invocation":
        return "macro_call"
    if kind == "call_expression":
        function = node.field("function")
        if function is not None and function.kind() == "field_expression":
            return "method_call"
        return "function_call"
    fn_target = (
        node
        if kind == "function_item"
        else scope
        if scope and scope.kind() == "function_item"
        else None
    )
    if fn_target is not None:
        if any(attr in _RUST_TEST_ATTRS for attr in attrs):
            return "test_function"
        impl_node = _find_ancestor(fn_target, "impl_item", max_depth=max_scope_depth)
        if impl_node is not None:
            if impl_node.field("trait") is not None:
                return "trait_method"
            return "method"
        return "free_function"
    if kind == "use_declaration":
        return "use_import"
    if kind == "field_declaration":
        return "struct_field"
    if kind == "enum_variant":
        return "enum_variant"
    return kind


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
    merged = dict(primary)
    for key, value in secondary.items():
        if key in _METADATA_KEYS:
            continue
        if key not in merged or not _has_value(merged.get(key)):
            merged[key] = value
    return merged


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

    line, col = byte_offset_to_line_col(source_bytes, byte_start)
    index = get_node_index(Path(cache_key or "<memory>.rs"), sg_root, lang="rust")
    node = index.find_containing(line, col)
    if node is None:
        end_line, end_col = byte_offset_to_line_col(source_bytes, max(byte_start, byte_end - 1))
        node = index.find_containing(end_line, end_col)
    if node is None:
        return None

    scope = _find_scope(node, max_depth=max_scope_depth)
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

    def_target: SgNode | None = node
    if node.kind() not in {
        "function_item",
        "struct_item",
        "enum_item",
        "trait_item",
        "impl_item",
        "mod_item",
    }:
        def_target = scope
    if def_target is not None:
        payload["visibility"] = _extract_visibility(def_target)
        attrs = _extract_attributes(def_target)
        if attrs:
            payload["attributes"] = attrs
    else:
        attrs = []

    fn_target = (
        node
        if node.kind() == "function_item"
        else scope
        if scope and scope.kind() == "function_item"
        else None
    )
    if fn_target is not None:
        payload.update(_extract_function_signature(fn_target))

    struct_target = (
        scope
        if scope and scope.kind() == "struct_item"
        else node
        if node.kind() == "struct_item"
        else None
    )
    if struct_target is not None:
        payload.update(_extract_struct_shape(struct_target))

    enum_target = (
        scope
        if scope and scope.kind() == "enum_item"
        else node
        if node.kind() == "enum_item"
        else None
    )
    if enum_target is not None:
        payload.update(_extract_enum_shape(enum_target))

    if node.kind() in {"call_expression", "macro_invocation"}:
        payload.update(_extract_call_target(node))

    payload["item_role"] = _classify_item_role(
        node,
        scope,
        max_scope_depth=max_scope_depth,
        attrs=attrs,
    )
    return payload


def _crosscheck_mismatches(
    ast_payload: dict[str, object],
    ts_payload: dict[str, object],
) -> list[dict[str, object]]:
    mismatches: list[dict[str, object]] = []
    for key in sorted(set(ast_payload).intersection(ts_payload)):
        if key in _METADATA_KEYS:
            continue
        left = ast_payload.get(key)
        right = ts_payload.get(key)
        if _has_value(left) and _has_value(right) and left != right:
            mismatches.append({"field": key, "ast_grep": left, "tree_sitter": right})
    return mismatches


def enrich_rust_context_by_byte_range(
    source: str,
    *,
    byte_start: int,
    byte_end: int,
    cache_key: str | None = None,
    max_scope_depth: int = _DEFAULT_SCOPE_DEPTH,
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

    try:
        ast_payload = _build_ast_grep_payload(
            source,
            byte_start=byte_start,
            byte_end=byte_end,
            cache_key=cache_key,
            max_scope_depth=max_scope_depth,
        )
    except _ENRICHMENT_ERRORS:
        ast_payload = None

    try:
        ts_payload = _canonicalize_tree_sitter_payload(
            _ts_enrich(
                source,
                byte_start=byte_start,
                byte_end=byte_end,
                cache_key=cache_key,
                max_scope_depth=max_scope_depth,
            )
        )
    except _ENRICHMENT_ERRORS:
        ts_payload = {}

    if ast_payload is None and not ts_payload:
        return None

    if ast_payload is None:
        merged = dict(ts_payload)
        sources = merged.get("enrichment_sources")
        source_list = list(sources) if isinstance(sources, list) else []
        if "tree_sitter" not in source_list:
            source_list.append("tree_sitter")
        merged["enrichment_sources"] = source_list
        merged.setdefault("enrichment_status", "applied")
        merged.setdefault("language", "rust")
        return merged

    merged = _merge_gap_fill(ast_payload, ts_payload)
    raw_sources = merged.get("enrichment_sources")
    source_list: list[str] = (
        [source for source in raw_sources if isinstance(source, str)]
        if isinstance(raw_sources, list)
        else []
    )
    if ts_payload and "tree_sitter" not in source_list:
        source_list.append("tree_sitter")
    merged["enrichment_sources"] = source_list

    ast_status = str(ast_payload.get("enrichment_status", "applied"))
    ts_status = str(ts_payload.get("enrichment_status", "applied"))
    if ast_status == "degraded" or ts_status == "degraded":
        merged["enrichment_status"] = "degraded"
        reasons: list[str] = []
        ast_reason = ast_payload.get("degrade_reason")
        ts_reason = ts_payload.get("degrade_reason")
        if isinstance(ast_reason, str) and ast_reason:
            reasons.append(f"ast_grep:{ast_reason}")
        if isinstance(ts_reason, str) and ts_reason:
            reasons.append(f"tree_sitter:{ts_reason}")
        if reasons:
            merged["degrade_reason"] = "; ".join(reasons)

    if os.getenv(_CROSSCHECK_ENV) == "1" and ts_payload:
        mismatches = _crosscheck_mismatches(ast_payload, ts_payload)
        if mismatches:
            merged["crosscheck_mismatches"] = mismatches
            merged["enrichment_status"] = "degraded"
            reason = merged.get("degrade_reason")
            suffix = "crosscheck mismatch"
            if isinstance(reason, str) and reason:
                merged["degrade_reason"] = f"{reason}; {suffix}"
            else:
                merged["degrade_reason"] = suffix

    return merged


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


__all__ = [
    "clear_rust_enrichment_cache",
    "enrich_rust_context_by_byte_range",
    "lint_rust_enrichment_schema",
]
