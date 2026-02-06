"""LibCST-based semantic enrichment helpers for Python search results."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from hashlib import blake2b
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from libcst.metadata import MetadataWrapper

try:
    import libcst as cst
    from libcst.metadata import (
        ByteSpanPositionProvider,
        ExpressionContextProvider,
        ParentNodeProvider,
        QualifiedNameProvider,
        ScopeProvider,
    )
except ImportError:  # pragma: no cover - optional dependency
    cst = None
    ByteSpanPositionProvider = None
    ExpressionContextProvider = None
    ParentNodeProvider = None
    QualifiedNameProvider = None
    ScopeProvider = None

_MAX_WRAPPER_CACHE = 64
_WRAPPER_CACHE: dict[str, tuple[MetadataWrapper, str]] = {}
_MAX_BINDINGS = 8
_MAX_ENCLOSING_CONTEXT_DEPTH = 32
_MAX_IMPORT_ALIAS_CHAIN_DEPTH = 16


def _source_hash(source: str) -> str:
    return blake2b(source.encode("utf-8", errors="replace"), digest_size=16).hexdigest()


@dataclass(slots=True, frozen=True)
class _NodeSpan:
    node: Any
    start: int
    end: int


def is_libcst_available() -> bool:
    """Return ``True`` when LibCST metadata providers are importable."""
    return all(
        obj is not None
        for obj in (
            cst,
            ByteSpanPositionProvider,
            ExpressionContextProvider,
            ParentNodeProvider,
            QualifiedNameProvider,
            ScopeProvider,
        )
    )


def _get_wrapper(source: str, *, cache_key: str | None) -> MetadataWrapper | None:
    if not is_libcst_available():
        return None
    assert cst is not None  # narrowed by is_libcst_available
    from libcst.metadata import MetadataWrapper

    if cache_key is None:
        return MetadataWrapper(cst.parse_module(source))

    content_hash = _source_hash(source)
    cached = _WRAPPER_CACHE.get(cache_key)
    if cached is not None:
        wrapper, old_hash = cached
        if old_hash == content_hash:
            return wrapper

    wrapper = MetadataWrapper(cst.parse_module(source))
    if len(_WRAPPER_CACHE) >= _MAX_WRAPPER_CACHE and cache_key not in _WRAPPER_CACHE:
        oldest = next(iter(_WRAPPER_CACHE))
        del _WRAPPER_CACHE[oldest]
    _WRAPPER_CACHE[cache_key] = (wrapper, content_hash)
    return wrapper


def clear_libcst_python_cache() -> None:
    """Clear cached metadata wrappers."""
    _WRAPPER_CACHE.clear()


def _iter_spans(wrapper: MetadataWrapper) -> list[_NodeSpan]:
    assert ByteSpanPositionProvider is not None
    spans = wrapper.resolve(ByteSpanPositionProvider)
    rows: list[_NodeSpan] = []
    for node, span in spans.items():
        start = int(getattr(span, "start", -1))
        length = int(getattr(span, "length", -1))
        if start < 0 or length <= 0:
            continue
        rows.append(_NodeSpan(node=node, start=start, end=start + length))
    return rows


def _find_target_node(
    wrapper: MetadataWrapper,
    *,
    byte_start: int,
    byte_end: int,
) -> Any | None:
    best: _NodeSpan | None = None
    for row in _iter_spans(wrapper):
        if (
            row.start <= byte_start
            and byte_end <= row.end
            and (best is None or (row.end - row.start) < (best.end - best.start))
        ):
            if cst is not None and isinstance(row.node, cst.Module):
                continue
            best = row
    return None if best is None else best.node


def _resolve_lazy(value: object) -> object:
    try:
        if callable(value):
            return value()
    except (RuntimeError, TypeError, ValueError, AttributeError):
        return value
    return value


def _extract_symbol_role(
    target: Any,
    *,
    ctx_map: Mapping[Any, object],
) -> str | None:
    ctx = _resolve_lazy(ctx_map.get(target))
    name = str(ctx).upper()
    if name.endswith("LOAD"):
        return "read"
    if name.endswith("STORE"):
        return "write"
    if name.endswith("DEL"):
        return "delete"
    return None


def _extract_qnames(
    target: Any,
    *,
    qn_map: Mapping[Any, object],
) -> list[dict[str, object]]:
    raw = _resolve_lazy(qn_map.get(target))
    if raw is None:
        return []
    rows: list[dict[str, object]] = []
    iterator = _coerce_qname_iterator(raw)
    if iterator is None:
        return rows
    for item in iterator:
        name = getattr(item, "name", None)
        source = getattr(item, "source", None)
        if not isinstance(name, str):
            continue
        rows.append(
            {
                "name": name,
                "source": str(source).split(".")[-1].lower() if source is not None else "unknown",
            }
        )
    rows.sort(key=lambda item: str(item.get("name", "")))
    return rows


def _coerce_qname_iterator(value: object) -> Iterable[object] | None:
    if isinstance(value, (str, bytes, bytearray)):
        return None
    if isinstance(value, Mapping):
        return value.values()
    if isinstance(value, Iterable):
        return value
    return None


def _extract_binding_candidates(
    target: Any,
    *,
    scope_map: Mapping[Any, object],
    span_map: Mapping[Any, object],
) -> list[dict[str, object]]:
    if cst is None or not isinstance(target, cst.Name):
        return []
    scope = scope_map.get(target)
    if scope is None:
        return []
    assignments = getattr(scope, "assignments", None)
    if assignments is None:
        return []
    try:
        bound = assignments[target.value]
    except (KeyError, TypeError):
        return []
    rows: list[dict[str, object]] = []
    for item in list(bound)[:_MAX_BINDINGS]:
        node = getattr(item, "node", None)
        span = span_map.get(node)
        start = getattr(span, "start", None)
        rows.append(
            {
                "name": getattr(item, "name", target.value),
                "kind": type(node).__name__.lower() if node is not None else "unknown",
                "byte_start": int(start) if isinstance(start, int) else None,
            }
        )
    return rows


def _extract_enclosing_context(
    target: Any,
    *,
    parent_map: Mapping[Any, object],
) -> tuple[str | None, str | None]:
    if cst is None:
        return None, None
    current = target
    enclosing_callable: str | None = None
    enclosing_class: str | None = None
    depth = 0
    while current is not None and depth < _MAX_ENCLOSING_CONTEXT_DEPTH:
        parent = parent_map.get(current)
        if parent is None:
            break
        if enclosing_callable is None and isinstance(parent, cst.FunctionDef):
            name_node = getattr(parent, "name", None)
            name_value = getattr(name_node, "value", None)
            if isinstance(name_value, str):
                enclosing_callable = name_value
        if enclosing_class is None and isinstance(parent, cst.ClassDef):
            name_node = getattr(parent, "name", None)
            name_value = getattr(name_node, "value", None)
            if isinstance(name_value, str):
                enclosing_class = name_value
        if enclosing_callable is not None and enclosing_class is not None:
            break
        current = parent
        depth += 1
    return enclosing_callable, enclosing_class


def _extract_import_alias_chain(
    target: Any,
    *,
    parent_map: Mapping[Any, object],
) -> list[dict[str, object]]:
    if cst is None:
        return []
    chain: list[dict[str, object]] = []
    current = target
    depth = 0
    while current is not None and depth < _MAX_IMPORT_ALIAS_CHAIN_DEPTH:
        parent = parent_map.get(current)
        if parent is None:
            break
        if isinstance(parent, cst.AsName):
            alias_name = parent.name.value if isinstance(parent.name, cst.Name) else None
            chain.append({"alias": alias_name})
        if isinstance(parent, cst.ImportAlias):
            module_name = getattr(parent.name, "value", None)
            if isinstance(module_name, str):
                chain.append({"module": module_name})
        if isinstance(parent, cst.ImportFrom):
            module_name = getattr(parent.module, "value", None)
            if isinstance(module_name, str):
                chain.append({"from": module_name})
            break
        if isinstance(parent, cst.Import):
            break
        current = parent
        depth += 1
    return chain


def _resolve_target_for_byte_range(
    wrapper: MetadataWrapper,
    *,
    byte_start: int,
    byte_end: int,
) -> Any | None:
    try:
        return _find_target_node(wrapper, byte_start=byte_start, byte_end=byte_end)
    except (RuntimeError, TypeError, ValueError, AttributeError):
        return None


def _resolve_provider_maps(
    wrapper: MetadataWrapper,
) -> (
    tuple[
        Mapping[Any, object],
        Mapping[Any, object],
        Mapping[Any, object],
        Mapping[Any, object],
        Mapping[Any, object],
    ]
    | None
):
    assert ByteSpanPositionProvider is not None
    assert ParentNodeProvider is not None
    assert ExpressionContextProvider is not None
    assert QualifiedNameProvider is not None
    assert ScopeProvider is not None
    try:
        return (
            wrapper.resolve(ByteSpanPositionProvider),
            wrapper.resolve(ParentNodeProvider),
            wrapper.resolve(ExpressionContextProvider),
            wrapper.resolve(QualifiedNameProvider),
            wrapper.resolve(ScopeProvider),
        )
    except (RuntimeError, TypeError, ValueError, AttributeError):
        return None


def _build_resolution_payload(
    target: Any,
    *,
    span_map: Mapping[Any, object],
    parent_map: Mapping[Any, object],
    ctx_map: Mapping[Any, object],
    qn_map: Mapping[Any, object],
    scope_map: Mapping[Any, object],
) -> dict[str, object]:
    payload: dict[str, object] = {}
    symbol_role = _extract_symbol_role(target, ctx_map=ctx_map)
    if symbol_role is not None:
        payload["symbol_role"] = symbol_role

    qnames = _extract_qnames(target, qn_map=qn_map)
    if qnames:
        payload["qualified_name_candidates"] = qnames

    binding_candidates = _extract_binding_candidates(
        target,
        scope_map=scope_map,
        span_map=span_map,
    )
    if binding_candidates:
        payload["binding_candidates"] = binding_candidates

    enclosing_callable, enclosing_class = _extract_enclosing_context(
        target,
        parent_map=parent_map,
    )
    if enclosing_callable is not None:
        payload["enclosing_callable"] = enclosing_callable
    if enclosing_class is not None:
        payload["enclosing_class"] = enclosing_class

    import_alias_chain = _extract_import_alias_chain(target, parent_map=parent_map)
    if import_alias_chain:
        payload["import_alias_chain"] = import_alias_chain
    return payload


def enrich_python_resolution_by_byte_range(
    source: str,
    *,
    byte_start: int,
    byte_end: int,
    cache_key: str | None = None,
    wrapper: MetadataWrapper | None = None,
) -> dict[str, object]:
    """Extract LibCST semantic context for a byte-anchored match.

    Returns:
    -------
    dict[str, object]
        Extracted LibCST semantic payload fields.
    """
    payload: dict[str, object] = {}
    if byte_start < 0 or byte_end <= byte_start or not is_libcst_available():
        return payload

    local_wrapper = wrapper if wrapper is not None else _get_wrapper(source, cache_key=cache_key)
    if local_wrapper is None:
        return payload

    target = _resolve_target_for_byte_range(local_wrapper, byte_start=byte_start, byte_end=byte_end)
    if target is None:
        return payload

    provider_maps = _resolve_provider_maps(local_wrapper)
    if provider_maps is None:
        return payload

    span_map, parent_map, ctx_map, qn_map, scope_map = provider_maps
    return _build_resolution_payload(
        target,
        span_map=span_map,
        parent_map=parent_map,
        ctx_map=ctx_map,
        qn_map=qn_map,
        scope_map=scope_map,
    )


__all__ = [
    "clear_libcst_python_cache",
    "enrich_python_resolution_by_byte_range",
    "is_libcst_available",
]
