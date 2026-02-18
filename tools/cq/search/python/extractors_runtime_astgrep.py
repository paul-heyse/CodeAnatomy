"""Ast-grep tier extraction runtime for Python enrichment."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from tools.cq.search._shared.error_boundaries import ENRICHMENT_ERRORS
from tools.cq.search._shared.helpers import truncate as shared_truncate
from tools.cq.search.enrichment.core import append_source as _append_enrichment_source
from tools.cq.search.python.constants import MAX_PARENT_DEPTH
from tools.cq.search.python.extractors_classification import (
    _is_class_node,
    _is_function_node,
    _unwrap_decorated,
)
from tools.cq.search.python.extractors_classification import (
    classify_item_role as _classify_item_role,
)
from tools.cq.search.python.extractors_structure import (
    classify_class_kind as _classify_class_kind,
)
from tools.cq.search.python.extractors_structure import (
    extract_base_classes as _extract_base_classes,
)
from tools.cq.search.python.extractors_structure import (
    extract_class_shape as _extract_class_shape,
)
from tools.cq.search.python.extractors_structure import (
    find_enclosing_class as _find_enclosing_class,
)

if TYPE_CHECKING:
    from ast_grep_py import SgNode

MAX_SIGNATURE_LEN = 200
MAX_PARAMS = 12
MAX_RETURN_TYPE_LEN = 100
MAX_DECORATORS = 8
MAX_DECORATOR_LEN = 60
MAX_CALL_TARGET_LEN = 120
MAX_CALL_RECEIVER_LEN = 80
MAX_SCOPE_CHAIN = 8
MAX_PAYLOAD_BYTES = 4096
PYTHON_ENRICHMENT_CROSSCHECK_ENV = "CQ_PY_ENRICHMENT_CROSSCHECK"

ENRICHABLE_KINDS: frozenset[str] = frozenset(
    {
        "function_definition",
        "decorated_definition",
        "class_definition",
        "call",
        "import_statement",
        "import_from_statement",
        "assignment",
        "augmented_assignment",
        "identifier",
        "attribute",
    }
)

_PREFERRED_ENRICHMENT_KINDS: frozenset[str] = frozenset(
    {
        "function_definition",
        "decorated_definition",
        "class_definition",
        "call",
        "import_statement",
        "import_from_statement",
        "assignment",
        "augmented_assignment",
    }
)

_STRUCTURAL_CONTEXT_MAP: dict[str, str] = {
    "try_statement": "try_block",
    "except_clause": "except_handler",
    "with_statement": "with_block",
    "for_statement": "for_loop",
    "while_statement": "while_loop",
    "if_statement": "if_block",
    "list_comprehension": "comprehension",
    "dict_comprehension": "comprehension",
    "set_comprehension": "comprehension",
    "generator_expression": "comprehension",
}

_SCOPE_BOUNDARY_NODE_KINDS: frozenset[str] = frozenset(
    {
        "function_definition",
        "class_definition",
        "decorated_definition",
    }
)

logger = logging.getLogger(__name__)


@dataclass
class EnrichmentContext:
    """Per-invocation context for enrichment operations."""

    truncations: list[str] = field(default_factory=list)


def truncate(
    text: str,
    max_len: int,
    field_name: str | None = None,
    *,
    context: EnrichmentContext | None = None,
) -> str:
    """Truncate text to max length and track truncated field names.

    Returns:
        str: Original or truncated text value.
    """
    truncated = shared_truncate(text, max_len)
    if truncated == text:
        return text
    if field_name and context is not None:
        context.truncations.append(field_name)
    return truncated


def try_extract(
    label: str,
    extractor: Callable[..., dict[str, object]],
    *args: object,
) -> tuple[dict[str, object], str | None]:
    """Call extractor, returning payload and optional degrade reason.

    Returns:
        tuple[dict[str, object], str | None]: Extracted payload and failure reason.
    """
    try:
        result = extractor(*args)
    except ENRICHMENT_ERRORS as exc:
        logger.warning("Python extractor degraded (%s): %s", label, type(exc).__name__)
        return {}, f"{label}: {type(exc).__name__}"
    return result, None


def _extract_params(func_node: SgNode) -> list[str] | None:
    params_node = func_node.field("parameters")
    if params_node is None:
        return None
    param_list: list[str] = []
    for child in params_node.children():
        if child.is_named() and child.kind() not in {"(", ")", ","}:
            text = child.text().strip()
            if text:
                param_list.append(text)
            if len(param_list) >= MAX_PARAMS:
                break
    return param_list


def _extract_return_type(func_node: SgNode) -> str | None:
    ret_node = func_node.field("return_type")
    if ret_node is None:
        return None
    ret_text = ret_node.text().strip()
    if ret_text.startswith("->"):
        ret_text = ret_text[2:].strip()
    return ret_text if ret_text else None


def _extract_sig_text(full_text: str) -> str:
    depth = 0
    for i, ch in enumerate(full_text):
        if ch in {"(", "["}:
            depth += 1
        elif ch in {")", "]"}:
            depth -= 1
        elif ch == ":" and depth == 0 and i > 0:
            return full_text[:i].strip()
    return ""


def extract_signature(
    node: SgNode,
    _source_bytes: bytes,
    *,
    context: EnrichmentContext | None = None,
) -> dict[str, object]:
    """Extract function signature details.

    Returns:
        dict[str, object]: Signature-related enrichment fields.
    """
    result: dict[str, object] = {}
    func_node = _unwrap_decorated(node)
    if func_node.kind() != "function_definition":
        return result

    params = _extract_params(func_node)
    if params is not None:
        result["params"] = params

    ret_text = _extract_return_type(func_node)
    if ret_text is not None:
        result["return_type"] = truncate(
            ret_text,
            MAX_RETURN_TYPE_LEN,
            "return_type",
            context=context,
        )

    func_text = func_node.text()
    result["is_async"] = func_text.lstrip().startswith("async ")

    sig_text = _extract_sig_text(node.text())
    if sig_text:
        result["signature"] = truncate(
            sig_text,
            MAX_SIGNATURE_LEN,
            "signature",
            context=context,
        )

    return result


def extract_decorators(
    node: SgNode,
    *,
    context: EnrichmentContext | None = None,
) -> dict[str, object]:
    """Extract decorators from decorated definition node.

    Returns:
        dict[str, object]: Decorator fields for the target node.
    """
    if node.kind() != "decorated_definition":
        return {}
    decorators: list[str] = []
    for child in node.children():
        if child.kind() != "decorator":
            continue
        text = child.text().strip()
        if text.startswith("@"):
            text = text[1:]
        if text:
            decorators.append(
                truncate(text, MAX_DECORATOR_LEN, "decorator", context=context)
            )
        if len(decorators) >= MAX_DECORATORS:
            break
    return {"decorators": decorators} if decorators else {}


def extract_class_context(
    node: SgNode,
    *,
    context: EnrichmentContext | None = None,
) -> dict[str, object]:
    """Extract class context for a node inside class body.

    Returns:
        dict[str, object]: Class context fields when available.
    """
    class_node = _find_enclosing_class(node)
    if class_node is None:
        return {}

    result: dict[str, object] = {}
    name_node = class_node.field("name")
    if name_node is not None:
        result["class_name"] = name_node.text()

    def truncate_wrapper(text: str, max_len: int, field_name: str | None) -> str:
        return truncate(text, max_len, field_name, context=context)

    bases = _extract_base_classes(class_node, truncate=truncate_wrapper)
    if bases:
        result["base_classes"] = bases
    result["class_kind"] = _classify_class_kind(class_node, bases)
    return result


def extract_call_target(
    node: SgNode,
    *,
    context: EnrichmentContext | None = None,
) -> dict[str, object]:
    """Extract call target metadata from a call node.

    Returns:
        dict[str, object]: Call target/arity fields for call nodes.
    """
    if node.kind() != "call":
        return {}

    result: dict[str, object] = {}
    func_node = node.field("function")
    if func_node is None:
        return result

    func_kind = func_node.kind()
    if func_kind == "attribute":
        obj_node = func_node.field("object")
        attr_node = func_node.field("attribute")
        receiver = obj_node.text().strip() if obj_node is not None else ""
        method = attr_node.text().strip() if attr_node is not None else ""
        if receiver and method:
            result["call_target"] = truncate(
                f"{receiver}.{method}",
                MAX_CALL_TARGET_LEN,
                "call_target",
                context=context,
            )
            result["call_receiver"] = truncate(
                receiver,
                MAX_CALL_RECEIVER_LEN,
                "call_receiver",
                context=context,
            )
            result["call_method"] = method
        elif method:
            result["call_target"] = method
    elif func_kind == "identifier":
        result["call_target"] = truncate(
            func_node.text().strip(),
            MAX_CALL_TARGET_LEN,
            "call_target",
            context=context,
        )
    else:
        text = func_node.text().strip()
        if text:
            result["call_target"] = truncate(
                text,
                MAX_CALL_TARGET_LEN,
                "call_target",
                context=context,
            )

    args_node = node.field("arguments")
    if args_node is not None:
        arg_count = sum(
            1
            for child in args_node.children()
            if child.is_named() and child.kind() not in {"(", ")", ","}
        )
        result["call_args_count"] = arg_count

    return result


def extract_scope_chain(node: SgNode) -> dict[str, object]:
    """Extract scope chain from module to enclosing definitions.

    Returns:
        dict[str, object]: Scope chain payload rooted at ``module``.
    """
    chain: list[str] = []
    current = node.parent()
    depth = 0
    while current is not None and depth < MAX_PARENT_DEPTH:
        kind = current.kind()
        if kind in {"function_definition", "class_definition"}:
            name_node = current.field("name")
            if name_node is not None:
                chain.append(name_node.text())
        elif kind == "decorated_definition":
            inner = _unwrap_decorated(current)
            inner_kind = inner.kind()
            if inner_kind in {"function_definition", "class_definition"}:
                name_node = inner.field("name")
                if name_node is not None:
                    chain.append(name_node.text())
        depth += 1
        current = current.parent()

    chain.reverse()
    chain.insert(0, "module")
    return {"scope_chain": chain[:MAX_SCOPE_CHAIN]}


def extract_structural_context(node: SgNode) -> dict[str, object]:
    """Extract nearest structural context label for a node.

    Returns:
        dict[str, object]: Structural context label payload, when found.
    """
    current = node.parent()
    depth = 0
    while current is not None and depth < MAX_PARENT_DEPTH:
        kind = current.kind()
        if kind in _SCOPE_BOUNDARY_NODE_KINDS:
            break
        ctx = _STRUCTURAL_CONTEXT_MAP.get(kind)
        if ctx is not None:
            return {"structural_context": ctx}
        depth += 1
        current = current.parent()
    return {}


def _collect_extract(
    payload: dict[str, object],
    degrade_reasons: list[str],
    label: str,
    extractor: Callable[..., dict[str, object]],
    *args: object,
) -> dict[str, object]:
    result, reason = try_extract(label, extractor, *args)
    payload.update(result)
    if reason:
        degrade_reasons.append(reason)
    return result


def _coerce_str_list(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [item for item in value if isinstance(item, str)]


def _enrich_ast_grep_core(
    node: SgNode,
    node_kind: str,
    source_bytes: bytes,
    payload: dict[str, object],
    degrade_reasons: list[str],
    *,
    context: EnrichmentContext | None = None,
) -> None:
    def extract_with_context(
        label: str,
        extractor: Callable[..., dict[str, object]],
        *args: object,
    ) -> dict[str, object]:
        try:
            result = extractor(*args, context=context)
        except ENRICHMENT_ERRORS as exc:
            result: dict[str, object] = {}
            degrade_reasons.append(f"{label}: {type(exc).__name__}")
            logger.warning("Python extractor degraded (%s): %s", label, type(exc).__name__)
        else:
            payload.update(result)
        return result

    dec_result = extract_with_context("decorators", extract_decorators, node)
    decorators = _coerce_str_list(dec_result.get("decorators"))

    role_result = _collect_extract(
        payload,
        degrade_reasons,
        "item_role",
        _classify_item_role,
        node,
        decorators,
    )
    if role_result.get("item_role") == "dataclass":
        payload["is_dataclass"] = True

    if _is_function_node(node):
        extract_with_context("signature", extract_signature, node, source_bytes)

    extract_with_context("class_context", extract_class_context, node)
    class_kind = payload.get("class_kind")
    if isinstance(class_kind, str):
        payload["is_dataclass"] = class_kind == "dataclass"

    if node_kind == "call":
        extract_with_context("call_target", extract_call_target, node)

    _collect_extract(payload, degrade_reasons, "scope_chain", extract_scope_chain, node)
    _collect_extract(
        payload,
        degrade_reasons,
        "structural_context",
        extract_structural_context,
        node,
    )

    if _is_class_node(node):
        _collect_extract(payload, degrade_reasons, "class_shape", _extract_class_shape, node)


def enrich_ast_grep_tier(
    node: SgNode,
    node_kind: str,
    source_bytes: bytes,
    *,
    context: EnrichmentContext | None = None,
) -> tuple[dict[str, object], list[str]]:
    """Run ast-grep tier extractors and return payload + degrade reasons.

    Returns:
        tuple[dict[str, object], list[str]]: Ast-grep payload plus degrade reasons.
    """
    payload: dict[str, object] = {"node_kind": node_kind}
    degrade_reasons: list[str] = []
    _enrich_ast_grep_core(node, node_kind, source_bytes, payload, degrade_reasons, context=context)
    return payload, degrade_reasons


def append_source(payload: dict[str, object], source_name: str) -> None:
    """Append source label to enrichment_sources payload key."""
    _append_enrichment_source(payload, source_name)


def promote_enrichment_node(node: SgNode) -> SgNode:
    """Promote low-signal nodes to preferred enclosing enrichment kinds.

    Returns:
        SgNode: Best candidate node for enrichment extraction.
    """
    current = node
    depth = 0
    while current is not None and depth < MAX_PARENT_DEPTH:
        if current.kind() in _PREFERRED_ENRICHMENT_KINDS:
            return current
        parent = current.parent()
        if parent is None:
            break
        current = parent
        depth += 1
    return node


__all__ = [
    "ENRICHABLE_KINDS",
    "MAX_PAYLOAD_BYTES",
    "PYTHON_ENRICHMENT_CROSSCHECK_ENV",
    "EnrichmentContext",
    "append_source",
    "enrich_ast_grep_tier",
    "extract_call_target",
    "extract_class_context",
    "extract_decorators",
    "extract_scope_chain",
    "extract_signature",
    "extract_structural_context",
    "promote_enrichment_node",
    "truncate",
    "try_extract",
]
