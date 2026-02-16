"""Shared Rust enrichment extractors used by ast-grep and tree-sitter lanes."""

from __future__ import annotations

from tools.cq.search.rust.node_access import RustNodeAccess

RUST_SCOPE_KINDS: frozenset[str] = frozenset(
    {
        "function_item",
        "struct_item",
        "enum_item",
        "trait_item",
        "impl_item",
        "mod_item",
        "macro_invocation",
        "block",
    }
)

RUST_TEST_ATTRS: frozenset[str] = frozenset(
    {
        "test",
        "tokio::test",
        "rstest",
        "async_std::test",
    }
)

_MAX_FUNCTION_PARAMS = 12
_MAX_ATTRIBUTES = 10


def _extract_params(node: RustNodeAccess) -> list[str]:
    params_node = node.child_by_field_name("parameters")
    if params_node is None:
        return []
    params: list[str] = []
    for child in params_node.children():
        text = child.text().strip()
        if text:
            params.append(text)
        if len(params) >= _MAX_FUNCTION_PARAMS:
            break
    return params


def _extract_optional_text(node: RustNodeAccess, field_name: str) -> str | None:
    field = node.child_by_field_name(field_name)
    if field is None:
        return None
    value = field.text().strip()
    return value or None


def _trim_signature_text(text: str) -> str:
    body_idx = text.find("{")
    if body_idx > 0:
        return text[:body_idx].strip()
    return text


def scope_name(scope_node: RustNodeAccess) -> str | None:
    """Return a best-effort name for a Rust scope node."""
    kind = scope_node.kind()
    if kind == "impl_item":
        type_node = scope_node.child_by_field_name("type")
        return type_node.text().strip() if type_node is not None else None
    if kind == "macro_invocation":
        macro = scope_node.child_by_field_name("macro")
        if macro is not None and macro.text().strip():
            return macro.text().strip()
        name = scope_node.child_by_field_name("name")
        return name.text().strip() if name is not None else None
    name = scope_node.child_by_field_name("name")
    return name.text().strip() if name is not None else None


def find_scope(node: RustNodeAccess, *, max_depth: int) -> RustNodeAccess | None:
    """Find the nearest enclosing Rust scope within ``max_depth``.

    Returns:
    -------
    RustNodeAccess | None
        The nearest scope node, or ``None`` when no scope is found.
    """
    current: RustNodeAccess | None = node
    depth = 0
    while current is not None and depth < max_depth:
        if current.kind() in RUST_SCOPE_KINDS:
            return current
        current = current.parent()
        depth += 1
    return None


def find_ancestor(node: RustNodeAccess, kind: str, *, max_depth: int) -> RustNodeAccess | None:
    """Find the nearest ancestor whose node kind matches ``kind``.

    Returns:
    -------
    RustNodeAccess | None
        Matching ancestor node, or ``None`` when absent.
    """
    current = node.parent()
    depth = 0
    while current is not None and depth < max_depth:
        if current.kind() == kind:
            return current
        current = current.parent()
        depth += 1
    return None


def scope_chain(node: RustNodeAccess, *, max_depth: int) -> list[str]:
    """Build a scope chain label list from node to outer scopes.

    Returns:
    -------
    list[str]
        Scope labels from inner to outer context.
    """
    chain: list[str] = []
    current: RustNodeAccess | None = node
    depth = 0
    while current is not None and depth < max_depth:
        kind = current.kind()
        if kind in RUST_SCOPE_KINDS:
            name = scope_name(current)
            chain.append(f"{kind}:{name}" if name else kind)
        current = current.parent()
        depth += 1
    return chain


def extract_visibility(item: RustNodeAccess) -> str:
    """Extract Rust visibility token from item text.

    Returns:
    -------
    str
        Visibility token such as ``pub`` or ``private``.
    """
    text = item.text().lstrip()
    if text.startswith("pub(crate)"):
        return "pub(crate)"
    if text.startswith("pub(super)"):
        return "pub(super)"
    if text.startswith("pub"):
        return "pub"
    return "private"


def extract_function_signature(node: RustNodeAccess) -> dict[str, object]:
    """Extract function signature metadata for a Rust function item.

    Returns:
    -------
    dict[str, object]
        Function signature fields and flags.
    """
    if node.kind() != "function_item":
        return {}

    result: dict[str, object] = {"params": _extract_params(node)}

    return_type = _extract_optional_text(node, "return_type")
    if return_type:
        result["return_type"] = return_type

    generics = _extract_optional_text(node, "type_parameters")
    if generics:
        result["generics"] = generics

    sig_text = _trim_signature_text(node.text().strip())
    if sig_text:
        result["signature"] = sig_text[:200]
    wrapped = f" {sig_text} "
    result["is_async"] = " async " in wrapped or sig_text.startswith("async ")
    result["is_unsafe"] = " unsafe " in wrapped or sig_text.startswith("unsafe ")
    return result


def extract_struct_shape(node: RustNodeAccess) -> dict[str, object]:
    """Extract struct field shape metadata for a Rust struct item.

    Returns:
    -------
    dict[str, object]
        Struct field count and sample field renderings.
    """
    if node.kind() != "struct_item":
        return {}
    body = node.child_by_field_name("body")
    if body is None:
        return {}
    field_nodes = [child for child in body.children() if child.kind() == "field_declaration"]
    fields: list[str] = []
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


def extract_enum_shape(node: RustNodeAccess) -> dict[str, object]:
    """Extract enum variant shape metadata for a Rust enum item.

    Returns:
    -------
    dict[str, object]
        Enum variant count and sample variant renderings.
    """
    if node.kind() != "enum_item":
        return {}
    body = node.child_by_field_name("body")
    if body is None:
        return {}
    variant_nodes = [child for child in body.children() if child.kind() == "enum_variant"]
    variants: list[str] = []
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


def extract_call_target(node: RustNodeAccess) -> dict[str, object]:
    """Extract call target metadata for a Rust call expression.

    Returns:
    -------
    dict[str, object]
        Call target payload, optionally including receiver/method details.
    """
    if node.kind() != "call_expression":
        return {}
    target = node.child_by_field_name("function")
    if target is None:
        return {}
    target_text = target.text().strip()
    if not target_text:
        return {}
    payload: dict[str, object] = {"call_target": target_text[:120]}
    if target.kind() == "field_expression":
        receiver = target.child_by_field_name("value")
        field = target.child_by_field_name("field")
        if receiver is not None:
            payload["call_receiver"] = receiver.text().strip()[:80]
        if field is not None:
            payload["call_method"] = field.text().strip()[:80]
    return payload


def extract_impl_context(node: RustNodeAccess) -> dict[str, object]:
    """Extract implementation context for Rust ``impl_item`` nodes.

    Returns:
    -------
    dict[str, object]
        Impl metadata fields (impl_type, impl_trait, impl_kind, impl_generics).
    """
    if node.kind() != "impl_item":
        return {}

    result: dict[str, object] = {}
    impl_type = node.child_by_field_name("type")
    if impl_type is not None:
        value = impl_type.text().strip()
        if value:
            result["impl_type"] = value

    impl_trait = node.child_by_field_name("trait")
    if impl_trait is not None:
        value = impl_trait.text().strip()
        if value:
            result["impl_trait"] = value
        result["impl_kind"] = "trait"
    else:
        result["impl_kind"] = "inherent"

    type_parameters = node.child_by_field_name("type_parameters")
    if type_parameters is not None:
        value = type_parameters.text().strip()
        if value:
            result["impl_generics"] = value

    return result


def extract_attributes(node: RustNodeAccess) -> list[str]:
    """Extract nearby Rust attribute markers for a node.

    Returns:
    -------
    list[str]
        Normalized attribute labels near the node.
    """
    attrs: list[str] = []
    parent = node.parent()
    if parent is None:
        return attrs
    for child in parent.children():
        if child.kind() != "attribute_item":
            continue
        text = child.text().strip()
        if text.startswith("#"):
            text = text.lstrip("#![").rstrip("]").strip()
        if text:
            attrs.append(text)
        if len(attrs) >= _MAX_ATTRIBUTES:
            break
    return attrs


__all__ = [
    "RUST_SCOPE_KINDS",
    "RUST_TEST_ATTRS",
    "extract_attributes",
    "extract_call_target",
    "extract_enum_shape",
    "extract_function_signature",
    "extract_impl_context",
    "extract_struct_shape",
    "extract_visibility",
    "find_ancestor",
    "find_scope",
    "scope_chain",
    "scope_name",
]
