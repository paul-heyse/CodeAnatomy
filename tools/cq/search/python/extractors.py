"""Python context enrichment using ast-grep and Python ``ast`` module.

Enrichment Contract
-------------------
All fields produced by this module are strictly additive.
They never affect: confidence scores, match counts, category classification,
or relevance ranking.
They may affect: output detail payload for agent consumption.

Two enrichment tiers:
1. **ast-grep tier** (zero incremental cost - reuses cached ``SgRoot``):
   signature, decorators, item_role, class context, call target, scope chain,
   structural context.
2. **Python ``ast`` tier** (per-file ``ast.parse``, cached):
   generator detection, behavior summary, import normalization.
"""

from __future__ import annotations

import ast
import logging
import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, cast

from tools.cq.core.locations import byte_offset_to_line_col
from tools.cq.search._shared.bounded_cache import BoundedCache
from tools.cq.search._shared.core import (
    PythonByteRangeEnrichmentRequest,
    PythonNodeEnrichmentRequest,
)
from tools.cq.search._shared.core import (
    line_col_to_byte_offset as _shared_line_col_to_byte_offset,
)
from tools.cq.search._shared.core import source_hash as _shared_source_hash
from tools.cq.search._shared.core import truncate as _shared_truncate
from tools.cq.search.enrichment.core import (
    append_source as _append_enrichment_source,
)
from tools.cq.search.enrichment.core import (
    enforce_payload_budget as _enforce_shared_payload_budget,
)
from tools.cq.search.enrichment.core import (
    payload_size_hint as _shared_payload_size_hint,
)
from tools.cq.search.python.analysis_session import PythonAnalysisSession
from tools.cq.search.python.extractors_analysis import (
    extract_behavior_summary as _extract_behavior_summary,
)
from tools.cq.search.python.extractors_analysis import (
    extract_generator_flag as _extract_generator_flag,
)
from tools.cq.search.python.extractors_analysis import (
    extract_import_detail as _extract_import_detail,
)
from tools.cq.search.python.extractors_analysis import find_ast_function as _find_ast_function
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
from tools.cq.search.python.resolution_index import enrich_python_resolution_by_byte_range
from tools.cq.search.tree_sitter.python_lane.facts import build_python_tree_sitter_facts

if TYPE_CHECKING:
    from ast_grep_py import SgNode, SgRoot

# ---------------------------------------------------------------------------
# Cache infrastructure
# ---------------------------------------------------------------------------

_MAX_TREE_CACHE_ENTRIES = 64

# ---------------------------------------------------------------------------
# Payload bounds (mirror Rust pattern)
# ---------------------------------------------------------------------------

_MAX_SIGNATURE_LEN = 200
_MAX_PARAMS = 12
_MAX_RETURN_TYPE_LEN = 100
_MAX_DECORATORS = 8
_MAX_DECORATOR_LEN = 60
_MAX_CALL_TARGET_LEN = 120
_MAX_CALL_RECEIVER_LEN = 80
_MAX_SCOPE_CHAIN = 8
_MAX_PAYLOAD_BYTES = 4096
_FULL_AGREEMENT_SOURCE_COUNT = 3
_PYTHON_ENRICHMENT_CROSSCHECK_ENV = "CQ_PY_ENRICHMENT_CROSSCHECK"

# ---------------------------------------------------------------------------
# Parent-chain traversal depth limit
# ---------------------------------------------------------------------------

_MAX_PARENT_DEPTH = 20
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Error tuple (fail-open boundary)
# ---------------------------------------------------------------------------

_ENRICHMENT_ERRORS = (
    RuntimeError,
    TypeError,
    ValueError,
    AttributeError,
    UnicodeError,
    SyntaxError,
)

# ---------------------------------------------------------------------------
# Enrichable node kinds
# ---------------------------------------------------------------------------

_ENRICHABLE_KINDS: frozenset[str] = frozenset(
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

# ---------------------------------------------------------------------------
# Structural context mapping
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Enrichment context (per-invocation state)
# ---------------------------------------------------------------------------


@dataclass
class EnrichmentContext:
    """Per-invocation context for enrichment operations."""

    truncations: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _truncate(
    text: str,
    max_len: int,
    field_name: str | None = None,
    *,
    context: EnrichmentContext | None = None,
) -> str:
    """Truncate *text* to *max_len* characters, appending ``...`` when needed.

    Parameters
    ----------
    text
        The string to truncate.
    max_len
        Maximum allowed length.
    field_name
        Optional field name for truncation tracking.
    context
        Per-invocation enrichment context for tracking truncations.

    Returns:
    -------
    str
        The original or truncated string.
    """
    truncated = _shared_truncate(text, max_len)
    if truncated == text:
        return text
    if field_name and context is not None:
        context.truncations.append(field_name)
    return truncated


def _try_extract(
    label: str,
    extractor: Callable[..., dict[str, object]],
    *args: object,
) -> tuple[dict[str, object], str | None]:
    """Call *extractor* with *args*, returning results or a degrade reason.

    Parameters
    ----------
    label
        Human label for the extractor (used in degradation messages).
    extractor
        Callable returning a dict.
    *args
        Positional arguments forwarded to *extractor*.

    Returns:
    -------
    tuple[dict[str, object], str | None]
        Extracted fields and an optional degrade reason on failure.
    """
    try:
        result = extractor(*args)
    except _ENRICHMENT_ERRORS as exc:
        logger.warning("Python extractor degraded (%s): %s", label, type(exc).__name__)
        return {}, f"{label}: {type(exc).__name__}"
    else:
        return result, None


# ---------------------------------------------------------------------------
# Python ast cache (per-file, hash-verified)
# ---------------------------------------------------------------------------

_AST_CACHE: BoundedCache[str, tuple[ast.Module, str]] = BoundedCache(
    max_size=_MAX_TREE_CACHE_ENTRIES, policy="fifo"
)


def _get_ast(source_bytes: bytes, *, cache_key: str) -> ast.Module | None:
    """Get or parse a cached Python AST module.

    Parameters
    ----------
    source_bytes
        Raw source bytes.
    cache_key
        File path for cache keying.

    Returns:
    -------
    ast.Module | None
        Parsed AST, or None on syntax error.
    """
    content_hash = _shared_source_hash(source_bytes)
    cached = _AST_CACHE.get(cache_key)
    if cached is not None:
        cached_tree, cached_hash = cached
        if cached_hash == content_hash:
            return cached_tree
    try:
        tree = ast.parse(source_bytes)
    except SyntaxError:
        return None
    _AST_CACHE.put(cache_key, (tree, content_hash))
    return tree


# ---------------------------------------------------------------------------
# ast-grep tier: Signature extraction (Rec 1)
# ---------------------------------------------------------------------------


def _extract_params(func_node: SgNode) -> list[str] | None:
    """Extract parameter names from a function node.

    Parameters
    ----------
    func_node
        A function_definition node.

    Returns:
    -------
    list[str] | None
        Parameter list, or None if no parameters node exists.
    """
    params_node = func_node.field("parameters")
    if params_node is None:
        return None
    param_list: list[str] = []
    for child in params_node.children():
        if child.is_named() and child.kind() not in {"(", ")", ","}:
            text = child.text().strip()
            if text:
                param_list.append(text)
            if len(param_list) >= _MAX_PARAMS:
                break
    return param_list


def _extract_return_type(func_node: SgNode) -> str | None:
    """Extract the return type annotation from a function node.

    Parameters
    ----------
    func_node
        A function_definition node.

    Returns:
    -------
    str | None
        The return type text, or None if absent.
    """
    ret_node = func_node.field("return_type")
    if ret_node is None:
        return None
    ret_text = ret_node.text().strip()
    if ret_text.startswith("->"):
        ret_text = ret_text[2:].strip()
    return ret_text if ret_text else None


def _extract_signature(
    node: SgNode, _source_bytes: bytes, *, context: EnrichmentContext | None = None
) -> dict[str, object]:
    """Extract function signature details.

    Parameters
    ----------
    node
        A function_definition or decorated_definition node.
    _source_bytes
        Full source bytes (unused, kept for _try_extract compat).
    context
        Per-invocation enrichment context for tracking truncations.

    Returns:
    -------
    dict[str, object]
        Signature fields: params, return_type, is_async, signature.
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
        result["return_type"] = _truncate(
            ret_text, _MAX_RETURN_TYPE_LEN, "return_type", context=context
        )

    func_text = func_node.text()
    result["is_async"] = func_text.lstrip().startswith("async ")

    sig_text = _extract_sig_text(node.text())
    if sig_text:
        result["signature"] = _truncate(sig_text, _MAX_SIGNATURE_LEN, "signature", context=context)

    return result


def _extract_sig_text(full_text: str) -> str:
    """Extract signature text from a function definition.

    Find the colon that starts the body (after params close and optional return type).

    Parameters
    ----------
    full_text
        Full text of the function definition node.

    Returns:
    -------
    str
        The signature text up to the colon.
    """
    # Find the first newline or the body colon
    depth = 0
    for i, ch in enumerate(full_text):
        if ch in {"(", "["}:
            depth += 1
        elif ch in {")", "]"}:
            depth -= 1
        elif ch == ":" and depth == 0 and i > 0:
            return full_text[:i].strip()
    return ""


# ---------------------------------------------------------------------------
# ast-grep tier: Decorator extraction (Rec 2)
# ---------------------------------------------------------------------------


def _extract_decorators(
    node: SgNode, *, context: EnrichmentContext | None = None
) -> dict[str, object]:
    """Extract decorator list from a decorated definition.

    Parameters
    ----------
    node
        A decorated_definition or function_definition/class_definition node.
    context
        Per-invocation enrichment context for tracking truncations.

    Returns:
    -------
    dict[str, object]
        Dict with ``decorators`` key if any decorators found, else empty.
    """
    if node.kind() != "decorated_definition":
        return {}
    decorators: list[str] = []
    for child in node.children():
        if child.kind() == "decorator":
            text = child.text().strip()
            if text.startswith("@"):
                text = text[1:]
            if text:
                decorators.append(_truncate(text, _MAX_DECORATOR_LEN, "decorator", context=context))
            if len(decorators) >= _MAX_DECORATORS:
                break
    if decorators:
        return {"decorators": decorators}
    return {}


# ---------------------------------------------------------------------------
# ast-grep tier: Class context extraction (Rec 3)
# ---------------------------------------------------------------------------


def _extract_class_context(
    node: SgNode, *, context: EnrichmentContext | None = None
) -> dict[str, object]:
    """Extract class context for a node inside a class.

    Parameters
    ----------
    node
        An ast-grep node.
    context
        Per-invocation enrichment context for tracking truncations.

    Returns:
    -------
    dict[str, object]
        Class context fields, or empty dict if not inside a class.
    """
    class_node = _find_enclosing_class(node)
    if class_node is None:
        return {}

    result: dict[str, object] = {}

    # Class name
    name_node = class_node.field("name")
    if name_node is not None:
        result["class_name"] = name_node.text()

    # Base classes
    def truncate_wrapper(text: str, max_len: int, field_name: str | None) -> str:
        return _truncate(text, max_len, field_name, context=context)

    bases = _extract_base_classes(class_node, truncate=truncate_wrapper)
    if bases:
        result["base_classes"] = bases

    # Class kind
    result["class_kind"] = _classify_class_kind(class_node, bases)

    return result


# ---------------------------------------------------------------------------
# ast-grep tier: Call target extraction (Rec 4)
# ---------------------------------------------------------------------------


def _extract_call_target(
    node: SgNode, *, context: EnrichmentContext | None = None
) -> dict[str, object]:
    """Extract call target information from a call node.

    Parameters
    ----------
    node
        A call node.
    context
        Per-invocation enrichment context for tracking truncations.

    Returns:
    -------
    dict[str, object]
        Call target fields.
    """
    if node.kind() != "call":
        return {}

    result: dict[str, object] = {}
    func_node = node.field("function")
    if func_node is None:
        return result

    func_kind = func_node.kind()

    if func_kind == "attribute":
        # Method call: receiver.method(...)
        obj_node = func_node.field("object")
        attr_node = func_node.field("attribute")
        receiver = obj_node.text().strip() if obj_node is not None else ""
        method = attr_node.text().strip() if attr_node is not None else ""
        if receiver and method:
            result["call_target"] = _truncate(
                f"{receiver}.{method}",
                _MAX_CALL_TARGET_LEN,
                "call_target",
                context=context,
            )
            result["call_receiver"] = _truncate(
                receiver, _MAX_CALL_RECEIVER_LEN, "call_receiver", context=context
            )
            result["call_method"] = method
        elif method:
            result["call_target"] = method
    elif func_kind == "identifier":
        result["call_target"] = _truncate(
            func_node.text().strip(),
            _MAX_CALL_TARGET_LEN,
            "call_target",
            context=context,
        )
    else:
        # Complex expression (e.g., subscript call)
        text = func_node.text().strip()
        if text:
            result["call_target"] = _truncate(
                text, _MAX_CALL_TARGET_LEN, "call_target", context=context
            )

    # Count arguments
    args_node = node.field("arguments")
    if args_node is not None:
        arg_count = sum(
            1
            for child in args_node.children()
            if child.is_named() and child.kind() not in {"(", ")", ","}
        )
        result["call_args_count"] = arg_count

    return result


# ---------------------------------------------------------------------------
# ast-grep tier: Scope chain (Rec 5)
# ---------------------------------------------------------------------------


def _extract_scope_chain(node: SgNode) -> dict[str, object]:
    """Extract the scope chain from module down to the node.

    Parameters
    ----------
    node
        An ast-grep node.

    Returns:
    -------
    dict[str, object]
        Dict with ``scope_chain`` key.
    """
    chain: list[str] = []
    current = node.parent()
    depth = 0
    while current is not None and depth < _MAX_PARENT_DEPTH:
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

    return {"scope_chain": chain[:_MAX_SCOPE_CHAIN]}


# ---------------------------------------------------------------------------
# ast-grep tier: Structural context (Rec 5)
# ---------------------------------------------------------------------------


def _extract_structural_context(node: SgNode) -> dict[str, object]:
    """Extract the nearest structural context for a node.

    Parameters
    ----------
    node
        An ast-grep node.

    Returns:
    -------
    dict[str, object]
        Dict with ``structural_context`` key if inside a structural block,
        else empty dict.
    """
    current = node.parent()
    depth = 0
    while current is not None and depth < _MAX_PARENT_DEPTH:
        kind = current.kind()
        # Stop at function/class boundaries
        if kind in _SCOPE_BOUNDARY_NODE_KINDS:
            break
        ctx = _STRUCTURAL_CONTEXT_MAP.get(kind)
        if ctx is not None:
            return {"structural_context": ctx}
        depth += 1
        current = current.parent()
    return {}


# ---------------------------------------------------------------------------
# Public entrypoints - enrichment sub-extractors
# ---------------------------------------------------------------------------


def _collect_extract(
    payload: dict[str, object],
    degrade_reasons: list[str],
    label: str,
    extractor: Callable[..., dict[str, object]],
    *args: object,
) -> dict[str, object]:
    """Run an extractor, merge results into *payload*, track degrade reasons.

    Parameters
    ----------
    payload
        Accumulator dict (mutated in place).
    degrade_reasons
        Accumulator list (mutated in place).
    label
        Human label for the extractor.
    extractor
        Callable returning a dict.
    *args
        Positional arguments forwarded to *extractor*.

    Returns:
    -------
    dict[str, object]
        The raw result dict from the extractor (for callers that inspect it).
    """
    result, reason = _try_extract(label, extractor, *args)
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
    """Run the core ast-grep tier extractors, mutating *payload* in place.

    Parameters
    ----------
    node
        Resolved SgNode at the match position.
    node_kind
        The ``kind()`` of *node*.
    source_bytes
        Raw source bytes (for signature extraction compat).
    payload
        Accumulator dict (mutated in place).
    degrade_reasons
        Accumulator list (mutated in place).
    context
        Per-invocation enrichment context for tracking truncations.
    """

    # Create wrapper to pass context to extractors
    def extract_with_context(
        label: str, extractor: Callable[..., dict[str, object]], *args: object
    ) -> dict[str, object]:
        # Call extractor with context keyword argument
        try:
            result = extractor(*args, context=context)
        except _ENRICHMENT_ERRORS as exc:
            result: dict[str, object] = {}
            reason = f"{label}: {type(exc).__name__}"
            logger.warning("Python extractor degraded (%s): %s", label, type(exc).__name__)
            degrade_reasons.append(reason)
        else:
            payload.update(result)
        return result

    dec_result = extract_with_context("decorators", _extract_decorators, node)
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
        extract_with_context("signature", _extract_signature, node, source_bytes)

    extract_with_context("class_context", _extract_class_context, node)
    class_kind = payload.get("class_kind")
    if isinstance(class_kind, str):
        payload["is_dataclass"] = class_kind == "dataclass"

    if node_kind == "call":
        extract_with_context("call_target", _extract_call_target, node)

    _collect_extract(payload, degrade_reasons, "scope_chain", _extract_scope_chain, node)
    _collect_extract(
        payload, degrade_reasons, "structural_context", _extract_structural_context, node
    )

    if _is_class_node(node):
        _collect_extract(payload, degrade_reasons, "class_shape", _extract_class_shape, node)


def _enrich_ast_grep_tier(
    node: SgNode,
    node_kind: str,
    source_bytes: bytes,
    *,
    context: EnrichmentContext | None = None,
) -> tuple[dict[str, object], list[str]]:
    """Run the ast-grep tier extractors for a match node.

    Parameters
    ----------
    node
        Resolved SgNode at the match position.
    node_kind
        The ``kind()`` of *node*.
    source_bytes
        Raw source bytes (for signature extraction compat).
    context
        Per-invocation enrichment context for tracking truncations.

    Returns:
    -------
    tuple[dict[str, object], list[str]]
        Extracted payload fields and any degrade reasons.
    """
    payload: dict[str, object] = {"node_kind": node_kind}
    degrade_reasons: list[str] = []
    _enrich_ast_grep_core(node, node_kind, source_bytes, payload, degrade_reasons, context=context)
    return payload, degrade_reasons


def _enrich_python_ast_tier(
    node: SgNode,
    source_bytes: bytes,
    cache_key: str,
) -> tuple[dict[str, object], list[str]]:
    """Run the Python ast tier extractors for function nodes.

    Parameters
    ----------
    node
        A function_definition or decorated_definition node.
    source_bytes
        Raw source bytes.
    cache_key
        File path for AST cache keying.

    Returns:
    -------
    tuple[dict[str, object], list[str]]
        Extracted payload fields and any degrade reasons.
    """
    payload: dict[str, object] = {}
    degrade_reasons: list[str] = []

    func_node = _unwrap_decorated(node)
    func_line = func_node.range().start.line + 1  # 0-indexed to 1-indexed
    ast_tree = _get_ast(source_bytes, cache_key=cache_key)
    if ast_tree is None:
        return payload, degrade_reasons

    func_ast = _find_ast_function(ast_tree, func_line)
    if func_ast is not None:
        gen_result, gen_reason = _try_extract("generator", _extract_generator_flag, func_ast)
        if gen_result:
            payload["is_generator"] = gen_result.get("is_generator", False)
        if gen_reason:
            degrade_reasons.append(gen_reason)

        beh_result, beh_reason = _try_extract("behavior", _extract_behavior_summary, func_ast)
        payload.update(beh_result)
        if beh_reason:
            degrade_reasons.append(beh_reason)

    return payload, degrade_reasons


def _enrich_import_tier(
    node: SgNode,
    source_bytes: bytes,
    cache_key: str,
    line: int,
) -> tuple[dict[str, object], list[str]]:
    """Run import detail extraction for import nodes.

    Parameters
    ----------
    node
        An import_statement or import_from_statement node.
    source_bytes
        Raw source bytes.
    cache_key
        File path for AST cache keying.
    line
        1-indexed line number.

    Returns:
    -------
    tuple[dict[str, object], list[str]]
        Extracted payload fields and any degrade reasons.
    """
    degrade_reasons: list[str] = []
    ast_tree = _get_ast(source_bytes, cache_key=cache_key)
    if ast_tree is None:
        return {}, degrade_reasons

    imp_result, imp_reason = _try_extract(
        "import",
        _extract_import_detail,
        node,
        source_bytes,
        ast_tree,
        line,
    )
    if imp_reason:
        degrade_reasons.append(imp_reason)
    return imp_result, degrade_reasons


def _append_source(payload: dict[str, object], source_name: str) -> None:
    """Append *source_name* to the enrichment_sources list in *payload*.

    Parameters
    ----------
    payload
        The enrichment payload dict (mutated in place).
    source_name
        The source name to add if not already present.
    """
    _append_enrichment_source(payload, source_name)


def _promote_enrichment_node(node: SgNode) -> SgNode:
    """Promote low-signal nodes (like identifiers) to richer enclosing nodes.

    Returns:
    -------
    SgNode
        Preferred enclosing node for enrichment extraction.
    """
    current = node
    depth = 0
    while current is not None and depth < _MAX_PARENT_DEPTH:
        if current.kind() in _PREFERRED_ENRICHMENT_KINDS:
            return current
        parent = current.parent()
        if parent is None:
            break
        current = parent
        depth += 1
    return node


# ---------------------------------------------------------------------------
# Payload budgeting / metadata
# ---------------------------------------------------------------------------


def _payload_size_hint(payload: dict[str, object]) -> int:
    """Estimate payload size in bytes.

    Returns:
    -------
    int
        Encoded payload size in bytes.
    """
    return _shared_payload_size_hint(payload)


def _enforce_payload_budget(payload: dict[str, object]) -> tuple[list[str], int]:
    """Prune optional fields when payload exceeds the configured budget.

    Returns:
    -------
    tuple[list[str], int]
        Removed keys and final payload size.
    """
    drop_order = (
        "scope_chain",
        "decorators",
        "base_classes",
        "property_names",
        "import_names",
        "signature",
        "call_target",
        "structural_context",
    )
    return _enforce_shared_payload_budget(
        payload,
        max_payload_bytes=_MAX_PAYLOAD_BYTES,
        drop_order=drop_order,
    )


def _extract_ast_grep_stage_fields(payload: dict[str, object]) -> dict[str, object]:
    """Return ast-grep structural fields used for stage agreement checks."""
    keys = {
        "node_kind",
        "item_role",
        "call_target",
        "scope_chain",
        "class_name",
        "class_kind",
        "structural_context",
    }
    return {key: payload[key] for key in keys if key in payload}


def _build_agreement_section(
    *,
    ast_fields: dict[str, object],
    python_resolution_fields: dict[str, object],
    tree_sitter_fields: dict[str, object],
) -> dict[str, object]:
    """Build deterministic cross-source agreement metadata.

    Returns:
    -------
    dict[str, object]
        Agreement status, source presence, and conflict details.
    """
    conflicts: list[dict[str, object]] = []
    comparison_sources = {
        "ast_grep": ast_fields,
        "python_resolution": python_resolution_fields,
        "tree_sitter": tree_sitter_fields,
    }
    present_sources = [name for name, values in comparison_sources.items() if values]

    for key in sorted(set(ast_fields).intersection(python_resolution_fields)):
        left = ast_fields.get(key)
        right = python_resolution_fields.get(key)
        if left != right:
            conflicts.append({"field": key, "ast_grep": left, "python_resolution": right})

    for key in sorted(set(python_resolution_fields).intersection(tree_sitter_fields)):
        left = python_resolution_fields.get(key)
        right = tree_sitter_fields.get(key)
        if left != right:
            conflicts.append({"field": key, "python_resolution": left, "tree_sitter": right})

    if conflicts:
        status = "conflict"
    elif len(present_sources) >= _FULL_AGREEMENT_SOURCE_COUNT:
        status = "full"
    else:
        status = "partial"

    return {
        "status": status,
        "sources": present_sources,
        "conflicts": conflicts,
    }


@dataclass(slots=True)
class _PythonEnrichmentState:
    payload: dict[str, object]
    context: EnrichmentContext = field(default_factory=EnrichmentContext)
    stage_status: dict[str, str] = field(default_factory=dict)
    stage_timings_ms: dict[str, float] = field(default_factory=dict)
    degrade_reasons: list[str] = field(default_factory=list)
    ast_fields: dict[str, object] = field(default_factory=dict)
    python_resolution_fields: dict[str, object] = field(default_factory=dict)
    tree_sitter_fields: dict[str, object] = field(default_factory=dict)


def _resolve_python_enrichment_range(
    *,
    node: SgNode,
    source_bytes: bytes,
    line: int,
    col: int,
    byte_start: int | None,
    byte_end: int | None,
) -> tuple[int | None, int | None]:
    resolved_start = byte_start
    if resolved_start is None:
        resolved_start = _shared_line_col_to_byte_offset(source_bytes, line, col)
    resolved_end = byte_end
    if resolved_end is None and resolved_start is not None:
        resolved_end = min(
            len(source_bytes),
            resolved_start + max(1, len(node.text().encode("utf-8"))),
        )
    return resolved_start, resolved_end


def _run_ast_grep_stage(
    state: _PythonEnrichmentState,
    *,
    node: SgNode,
    node_kind: str,
    source_bytes: bytes,
) -> None:
    ast_started = perf_counter()
    sg_fields, degrade_reasons = _enrich_ast_grep_tier(
        node, node_kind, source_bytes, context=state.context
    )
    state.stage_timings_ms["ast_grep"] = (perf_counter() - ast_started) * 1000.0
    state.stage_status["ast_grep"] = "degraded" if degrade_reasons else "applied"
    state.payload.update(sg_fields)
    state.ast_fields = _extract_ast_grep_stage_fields(sg_fields)
    state.degrade_reasons.extend(degrade_reasons)


def _run_python_ast_stage(
    state: _PythonEnrichmentState,
    *,
    node: SgNode,
    source_bytes: bytes,
    cache_key: str,
) -> None:
    if not _is_function_node(node):
        state.stage_status["python_ast"] = "skipped"
        state.stage_timings_ms["python_ast"] = 0.0
        return

    py_ast_started = perf_counter()
    ast_extra_fields, ast_extra_reasons = _enrich_python_ast_tier(node, source_bytes, cache_key)
    state.stage_timings_ms["python_ast"] = (perf_counter() - py_ast_started) * 1000.0
    state.payload.update(ast_extra_fields)
    state.degrade_reasons.extend(ast_extra_reasons)
    state.stage_status["python_ast"] = "degraded" if ast_extra_reasons else "applied"
    _append_source(state.payload, "python_ast")


def _run_import_stage(
    state: _PythonEnrichmentState,
    *,
    node: SgNode,
    node_kind: str,
    source_bytes: bytes,
    cache_key: str,
    line: int,
) -> None:
    if node_kind not in {"import_statement", "import_from_statement"}:
        state.stage_status["import_detail"] = "skipped"
        state.stage_timings_ms["import_detail"] = 0.0
        return

    import_started = perf_counter()
    imp_fields, imp_reasons = _enrich_import_tier(node, source_bytes, cache_key, line)
    state.stage_timings_ms["import_detail"] = (perf_counter() - import_started) * 1000.0
    state.payload.update(imp_fields)
    state.degrade_reasons.extend(imp_reasons)
    state.stage_status["import_detail"] = "degraded" if imp_reasons else "applied"
    _append_source(state.payload, "python_ast")


def _decode_python_source_text(
    *,
    source_bytes: bytes,
    session: PythonAnalysisSession | None,
) -> str:
    return session.source if session is not None else source_bytes.decode("utf-8", errors="replace")


def _run_python_resolution_stage(
    state: _PythonEnrichmentState,
    *,
    source_bytes: bytes,
    byte_start: int | None,
    byte_end: int | None,
    cache_key: str,
    session: PythonAnalysisSession | None,
) -> None:
    if byte_start is None or byte_end is None:
        state.stage_status["python_resolution"] = "skipped"
        state.stage_timings_ms["python_resolution"] = 0.0
        return

    resolution_started = perf_counter()
    resolution_reasons: list[str] = []
    try:
        source_text = _decode_python_source_text(source_bytes=source_bytes, session=session)
        state.python_resolution_fields = enrich_python_resolution_by_byte_range(
            source_text,
            source_bytes=source_bytes,
            file_path=cache_key,
            byte_start=byte_start,
            byte_end=byte_end,
            session=session,
        )
    except _ENRICHMENT_ERRORS as exc:
        logger.warning("Python resolution enrichment failed: %s", type(exc).__name__)
        state.python_resolution_fields = {}
        resolution_reasons.append(f"python_resolution: {type(exc).__name__}")
    state.stage_timings_ms["python_resolution"] = (perf_counter() - resolution_started) * 1000.0
    state.degrade_reasons.extend(resolution_reasons)
    if state.python_resolution_fields:
        state.payload.update(state.python_resolution_fields)
        _append_source(state.payload, "python_resolution")
        state.stage_status["python_resolution"] = "applied"
        return
    state.stage_status["python_resolution"] = "degraded" if resolution_reasons else "skipped"


def _run_tree_sitter_stage(
    state: _PythonEnrichmentState,
    *,
    source_bytes: bytes,
    byte_span: tuple[int | None, int | None],
    cache_key: str,
    query_budget_ms: int | None,
    session: PythonAnalysisSession | None,
) -> None:
    byte_start, byte_end = byte_span
    if byte_start is None or byte_end is None:
        state.stage_status["tree_sitter"] = "skipped"
        state.stage_timings_ms["tree_sitter"] = 0.0
        return

    ts_started = perf_counter()
    tree_sitter_reasons: list[str] = []
    try:
        source_text = _decode_python_source_text(source_bytes=source_bytes, session=session)
        ts_payload = build_python_tree_sitter_facts(
            source_text,
            byte_start=byte_start,
            byte_end=byte_end,
            cache_key=cache_key,
            query_budget_ms=query_budget_ms,
        )
        if ts_payload:
            state.tree_sitter_fields.update(ts_payload)
            # Tree-sitter is the primary structural plane in this cutover.
            for key, value in ts_payload.items():
                if key == "enrichment_sources":
                    continue
                state.payload[key] = value
            _append_source(state.payload, "tree_sitter")
            ts_status = ts_payload.get("enrichment_status")
            state.stage_status["tree_sitter"] = (
                ts_status if isinstance(ts_status, str) else "applied"
            )
            ts_reason = ts_payload.get("degrade_reason")
            if isinstance(ts_reason, str) and ts_reason:
                tree_sitter_reasons.append(f"tree_sitter: {ts_reason}")
        else:
            state.stage_status["tree_sitter"] = "skipped"
    except _ENRICHMENT_ERRORS as exc:
        logger.warning("Python tree-sitter enrichment stage failed: %s", type(exc).__name__)
        state.stage_status["tree_sitter"] = "degraded"
        tree_sitter_reasons.append(f"tree_sitter: {type(exc).__name__}")
    state.stage_timings_ms["tree_sitter"] = (perf_counter() - ts_started) * 1000.0
    state.degrade_reasons.extend(tree_sitter_reasons)


def _finalize_python_enrichment_payload(state: _PythonEnrichmentState) -> None:
    agreement = _build_agreement_section(
        ast_fields=state.ast_fields,
        python_resolution_fields=state.python_resolution_fields,
        tree_sitter_fields=state.tree_sitter_fields,
    )
    state.payload["agreement"] = agreement
    if (
        os.getenv(_PYTHON_ENRICHMENT_CROSSCHECK_ENV) == "1"
        and agreement.get("status") == "conflict"
    ):
        conflicts = agreement.get("conflicts")
        if isinstance(conflicts, list):
            state.payload["crosscheck_mismatches"] = conflicts
        state.degrade_reasons.append("crosscheck mismatch")

    if state.degrade_reasons:
        state.payload["enrichment_status"] = "degraded"
        state.payload["degrade_reason"] = "; ".join(state.degrade_reasons)

    state.payload["stage_status"] = state.stage_status
    state.payload["stage_timings_ms"] = state.stage_timings_ms

    if state.context.truncations:
        state.payload["truncated_fields"] = list(state.context.truncations)

    dropped_fields, size_hint = _enforce_payload_budget(state.payload)
    state.payload["payload_size_hint"] = size_hint
    if dropped_fields:
        state.payload["dropped_fields"] = dropped_fields


# ---------------------------------------------------------------------------
# Public entrypoints
# ---------------------------------------------------------------------------


def enrich_python_context(request: PythonNodeEnrichmentRequest) -> dict[str, object] | None:
    """Enrich a Python match with structured context fields.

    Parameters
    ----------
    request
        Typed request object containing node anchor and per-file context.

    Returns:
    -------
    dict[str, object] | None
        Enrichment payload, or None if enrichment not applicable.
    """
    node = cast("SgNode", request.node)
    node_kind = node.kind()
    if node_kind not in _ENRICHABLE_KINDS:
        return None

    state = _PythonEnrichmentState(
        payload={
            "enrichment_status": "applied",
            "enrichment_sources": ["ast_grep"],
        }
    )

    byte_start, byte_end = _resolve_python_enrichment_range(
        node=node,
        source_bytes=request.source_bytes,
        line=request.line,
        col=request.col,
        byte_start=request.byte_start,
        byte_end=request.byte_end,
    )

    _run_ast_grep_stage(state, node=node, node_kind=node_kind, source_bytes=request.source_bytes)
    _run_python_ast_stage(
        state,
        node=node,
        source_bytes=request.source_bytes,
        cache_key=request.cache_key,
    )
    _run_import_stage(
        state,
        node=node,
        node_kind=node_kind,
        source_bytes=request.source_bytes,
        cache_key=request.cache_key,
        line=request.line,
    )
    _run_python_resolution_stage(
        state,
        source_bytes=request.source_bytes,
        byte_start=byte_start,
        byte_end=byte_end,
        cache_key=request.cache_key,
        session=cast("PythonAnalysisSession | None", request.session),
    )
    _run_tree_sitter_stage(
        state,
        source_bytes=request.source_bytes,
        byte_span=(byte_start, byte_end),
        cache_key=request.cache_key,
        query_budget_ms=request.query_budget_ms,
        session=cast("PythonAnalysisSession | None", request.session),
    )
    _finalize_python_enrichment_payload(state)
    return state.payload


def enrich_python_context_by_byte_range(
    request: PythonByteRangeEnrichmentRequest,
) -> dict[str, object] | None:
    """Enrich using byte-range anchor (preferred for ripgrep integration).

    Parameters
    ----------
    request
        Typed request object containing byte-range anchor and optional resolved node.

    Returns:
    -------
    dict[str, object] | None
        Enrichment payload, or None if not applicable.
    """
    if (
        request.byte_start < 0
        or request.byte_end <= request.byte_start
        or request.byte_end > len(request.source_bytes)
    ):
        return None

    from tools.cq.search.pipeline.classifier import get_node_index

    if request.resolved_node is None:
        line, col = byte_offset_to_line_col(request.source_bytes, request.byte_start)
        index = get_node_index(
            Path(request.cache_key), cast("SgRoot", request.sg_root), lang="python"
        )
        node = index.find_containing(line, col)
        if node is None:
            line, col = byte_offset_to_line_col(
                request.source_bytes,
                max(request.byte_start, request.byte_end - 1),
            )
            node = index.find_containing(line, col)
        if node is None:
            return None
    else:
        node = _promote_enrichment_node(cast("SgNode", request.resolved_node))
        if request.resolved_line is None or request.resolved_col is None:
            line, col = byte_offset_to_line_col(request.source_bytes, request.byte_start)
        else:
            line, col = request.resolved_line, request.resolved_col

    node = _promote_enrichment_node(node)

    return enrich_python_context(
        PythonNodeEnrichmentRequest(
            sg_root=request.sg_root,
            node=node,
            source_bytes=request.source_bytes,
            line=line,
            col=col,
            cache_key=request.cache_key,
            byte_start=request.byte_start,
            byte_end=request.byte_end,
            session=request.session,
        )
    )


def clear_python_enrichment_cache() -> None:
    """Clear per-process Python enrichment caches."""
    _AST_CACHE.clear()


def extract_python_node(request: PythonNodeEnrichmentRequest) -> dict[str, object]:
    """Compatibility wrapper for node-anchored extraction.

    Returns:
        dict[str, object]: Function return value.
    """
    payload = enrich_python_context(request)
    return payload if isinstance(payload, dict) else {}


def extract_python_byte_range(
    request: PythonByteRangeEnrichmentRequest,
) -> dict[str, object]:
    """Compatibility wrapper for byte-range extraction.

    Returns:
        dict[str, object]: Function return value.
    """
    payload = enrich_python_context_by_byte_range(request)
    return payload if isinstance(payload, dict) else {}


__all__ = [
    "clear_python_enrichment_cache",
    "enrich_python_context",
    "enrich_python_context_by_byte_range",
    "extract_python_byte_range",
    "extract_python_node",
]
