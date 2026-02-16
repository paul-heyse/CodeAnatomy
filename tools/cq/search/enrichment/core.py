"""Shared enrichment helpers for payload normalization and merge policy."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import cast

import msgspec

from tools.cq.search._shared.core import encode_mapping
from tools.cq.search.enrichment.contracts import (
    EnrichmentMeta,
    EnrichmentStatus,
    PythonEnrichmentPayload,
    RustEnrichmentPayload,
)
from tools.cq.search.enrichment.python_facts import PythonEnrichmentFacts
from tools.cq.search.enrichment.rust_facts import RustEnrichmentFacts

_DEFAULT_METADATA_KEYS: frozenset[str] = frozenset(
    {"enrichment_status", "enrichment_sources", "degrade_reason", "language"}
)


def payload_size_hint(payload: Mapping[str, object]) -> int:
    """Estimate payload size using msgspec JSON encoding.

    Returns:
        Estimated serialized payload size in bytes.
    """
    return len(encode_mapping(dict(payload)))


def has_value(value: object) -> bool:
    """Return whether a payload value is considered non-empty.

    Returns:
        `True` when value is non-empty for merge purposes.
    """
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


def string_or_none(value: object) -> str | None:
    """Return stripped string when value is a non-empty string."""
    if isinstance(value, str):
        text = value.strip()
        return text if text else None
    return None


def parse_python_enrichment(raw: dict[str, object] | None) -> PythonEnrichmentFacts | None:
    """Parse Python enrichment payload into typed facts.

    Returns:
        PythonEnrichmentFacts | None: Parsed Python enrichment facts when valid.
    """
    if raw is None:
        return None
    try:
        return msgspec.convert(raw, type=PythonEnrichmentFacts, strict=False)
    except (msgspec.ValidationError, TypeError, ValueError, RuntimeError):
        return None


def parse_rust_enrichment(raw: dict[str, object] | None) -> RustEnrichmentFacts | None:
    """Parse Rust enrichment payload into typed facts.

    Returns:
        RustEnrichmentFacts | None: Parsed Rust enrichment facts when valid.
    """
    if raw is None:
        return None
    try:
        return msgspec.convert(raw, type=RustEnrichmentFacts, strict=False)
    except (msgspec.ValidationError, TypeError, ValueError, RuntimeError):
        return None


def append_source(payload: dict[str, object], source: str) -> None:
    """Append an enrichment source name if it is not already present."""
    raw = payload.get("enrichment_sources")
    sources: list[str] = (
        [item for item in raw if isinstance(item, str)] if isinstance(raw, list) else []
    )
    if source not in sources:
        sources.append(source)
    payload["enrichment_sources"] = sources


def set_degraded(payload: dict[str, object], reason: str) -> None:
    """Mark payload as degraded while preserving earlier degradation reason."""
    payload["enrichment_status"] = "degraded"
    existing = payload.get("degrade_reason")
    if isinstance(existing, str) and existing:
        payload["degrade_reason"] = f"{existing}; {reason}"
    else:
        payload["degrade_reason"] = reason


def merge_gap_fill_payload(
    primary: Mapping[str, object],
    secondary: Mapping[str, object],
    *,
    metadata_keys: Sequence[str] | None = None,
) -> dict[str, object]:
    """Merge secondary payload values only when primary is missing/empty.

    Returns:
        Merged payload preserving existing populated primary values.
    """
    merged = dict(primary)
    protected = set(metadata_keys) if metadata_keys is not None else set(_DEFAULT_METADATA_KEYS)
    for key, value in secondary.items():
        if key in protected:
            continue
        if key not in merged or not has_value(merged.get(key)):
            merged[key] = value
    return merged


def enforce_payload_budget(
    payload: dict[str, object],
    *,
    max_payload_bytes: int,
    drop_order: Sequence[str],
) -> tuple[list[str], int]:
    """Prune optional keys until payload fits the size budget.

    Returns:
        Dropped keys and resulting payload size in bytes.
    """
    dropped: list[str] = []
    size = payload_size_hint(payload)
    if size <= max_payload_bytes:
        return dropped, size

    for key in drop_order:
        if key not in payload:
            continue
        payload.pop(key, None)
        dropped.append(key)
        size = payload_size_hint(payload)
        if size <= max_payload_bytes:
            break
    return dropped, size


def _meta_from_flat(payload: Mapping[str, object], *, language: str) -> EnrichmentMeta:
    status_raw = payload.get("enrichment_status")
    status: EnrichmentStatus = (
        cast("EnrichmentStatus", status_raw)
        if isinstance(status_raw, str) and status_raw in {"applied", "degraded", "skipped"}
        else "applied"
    )
    sources_raw = payload.get("enrichment_sources")
    sources = (
        [item for item in sources_raw if isinstance(item, str)]
        if isinstance(sources_raw, list)
        else []
    )
    degrade_reason = payload.get("degrade_reason")
    payload_size = payload.get("payload_size_hint")
    dropped = payload.get("dropped_fields")
    truncated = payload.get("truncated_fields")
    return EnrichmentMeta(
        language=language,
        enrichment_status=status,
        enrichment_sources=sources,
        degrade_reason=degrade_reason if isinstance(degrade_reason, str) else None,
        payload_size_hint=payload_size if isinstance(payload_size, int) else None,
        dropped_fields=(list(dropped) if isinstance(dropped, list) else None),
        truncated_fields=(list(truncated) if isinstance(truncated, list) else None),
    )


_PY_RESOLUTION_KEYS: frozenset[str] = frozenset(
    {
        "symbol_role",
        "qualified_name_candidates",
        "binding_candidates",
        "enclosing_callable",
        "enclosing_class",
        "import_alias_chain",
        "call_target",
        "call_receiver",
        "call_method",
        "call_args_count",
        "import_module",
        "import_alias",
        "import_names",
        "import_level",
        "is_type_import",
        "item_role",
    }
)
_PY_BEHAVIOR_KEYS: frozenset[str] = frozenset(
    {
        "is_async",
        "is_generator",
        "returns_value",
        "raises_exception",
        "yields",
        "awaits",
        "has_context_manager",
        "has_await",
        "has_yield",
        "has_raise",
        "in_try",
        "in_except",
        "in_with",
        "in_loop",
    }
)
_PY_STRUCTURAL_KEYS: frozenset[str] = frozenset(
    {
        "node_kind",
        "signature",
        "params",
        "return_type",
        "decorators",
        "class_name",
        "class_kind",
        "base_classes",
        "scope_chain",
        "structural_context",
        "method_count",
        "property_names",
        "abstract_member_count",
        "class_markers",
        "is_dataclass",
    }
)
_PY_FLAT_EXCLUDED_KEYS: frozenset[str] = frozenset(
    {
        "language",
        "enrichment_status",
        "enrichment_sources",
        "degrade_reason",
        "payload_size_hint",
        "dropped_fields",
        "truncated_fields",
        "stage_status",
        "stage_timings_ms",
    }
)


def _to_python_wrapper_data(payload: dict[str, object]) -> dict[str, object]:
    return {key: value for key, value in payload.items() if key not in _PY_FLAT_EXCLUDED_KEYS}


def _partition_python_payload_fields(
    flat_data: dict[str, object],
) -> tuple[
    dict[str, object], dict[str, object], dict[str, object], dict[str, object], dict[str, object]
]:
    resolution: dict[str, object] = {}
    behavior: dict[str, object] = {}
    structural: dict[str, object] = {}
    parse_quality: dict[str, object] = {}
    agreement: dict[str, object] = {"status": "partial", "conflicts": []}

    for key, value in flat_data.items():
        if key in _PY_RESOLUTION_KEYS:
            resolution[key] = value
            continue
        if key in _PY_BEHAVIOR_KEYS:
            behavior[key] = value
            continue
        if key in _PY_STRUCTURAL_KEYS:
            structural[key] = value
            continue
        if key == "parse_quality" and isinstance(value, dict):
            parse_quality = dict(value)
            continue
        if key == "agreement" and isinstance(value, dict):
            agreement = dict(value)
            continue
        structural[key] = value

    return resolution, behavior, structural, parse_quality, agreement


def _derive_behavior_flags(behavior: dict[str, object], structural: dict[str, object]) -> None:
    if "awaits" in behavior and "has_await" not in behavior:
        behavior["has_await"] = bool(behavior["awaits"])
    if "yields" in behavior and "has_yield" not in behavior:
        behavior["has_yield"] = bool(behavior["yields"])
    if "raises_exception" in behavior and "has_raise" not in behavior:
        behavior["has_raise"] = bool(behavior["raises_exception"])

    ctx = structural.get("structural_context")
    if not isinstance(ctx, str):
        return
    behavior.setdefault("in_try", ctx == "try_block")
    behavior.setdefault("in_except", ctx == "except_handler")
    behavior.setdefault("in_with", ctx == "with_block")
    behavior.setdefault("in_loop", ctx in {"for_loop", "while_loop", "comprehension"})


def _build_python_meta(
    *,
    payload: dict[str, object],
    meta: EnrichmentMeta,
) -> dict[str, object]:
    out: dict[str, object] = {
        "language": meta.language,
        "enrichment_status": meta.enrichment_status,
        "enrichment_sources": meta.enrichment_sources,
    }
    stage_status = payload.get("stage_status")
    if isinstance(stage_status, dict):
        out["stage_status"] = stage_status
    stage_timings = payload.get("stage_timings_ms")
    if isinstance(stage_timings, dict):
        out["stage_timings_ms"] = stage_timings
    if meta.degrade_reason:
        out["degrade_reason"] = meta.degrade_reason
    if meta.payload_size_hint is not None:
        out["payload_size_hint"] = meta.payload_size_hint
    if meta.dropped_fields:
        out["dropped_fields"] = meta.dropped_fields
    if meta.truncated_fields:
        out["truncated_fields"] = meta.truncated_fields
    return out


def normalize_python_payload(payload: dict[str, object] | None) -> dict[str, object] | None:
    """Normalize Python enrichment payload via typed wrapper contract.

    Returns:
        Structured Python payload envelope, or `None` when input is `None`.
    """
    if payload is None:
        return None
    wrapper = PythonEnrichmentPayload(
        meta=_meta_from_flat(payload, language="python"),
        data=_to_python_wrapper_data(payload),
    )
    flat_data = dict(wrapper.data)
    resolution, behavior, structural, parse_quality, agreement = _partition_python_payload_fields(
        flat_data
    )
    _derive_behavior_flags(behavior, structural)
    meta = _build_python_meta(payload=payload, meta=wrapper.meta)

    return {
        "meta": meta,
        "resolution": resolution,
        "behavior": behavior,
        "structural": structural,
        "parse_quality": parse_quality,
        "agreement": agreement,
    }


def normalize_rust_payload(payload: dict[str, object] | None) -> dict[str, object] | None:
    """Normalize Rust enrichment payload via typed wrapper contract.

    Returns:
        Structured Rust payload envelope, or `None` when input is `None`.
    """
    if payload is None:
        return None
    wrapper = RustEnrichmentPayload(
        meta=_meta_from_flat(payload, language="rust"),
        data={
            key: value
            for key, value in payload.items()
            if key
            not in {
                "language",
                "enrichment_status",
                "enrichment_sources",
                "degrade_reason",
                "payload_size_hint",
                "dropped_fields",
                "truncated_fields",
            }
        },
    )
    out = dict(wrapper.data)
    out["language"] = wrapper.meta.language
    out["enrichment_status"] = wrapper.meta.enrichment_status
    out["enrichment_sources"] = wrapper.meta.enrichment_sources
    if wrapper.meta.degrade_reason:
        out["degrade_reason"] = wrapper.meta.degrade_reason
    if wrapper.meta.payload_size_hint is not None:
        out["payload_size_hint"] = wrapper.meta.payload_size_hint
    if wrapper.meta.dropped_fields:
        out["dropped_fields"] = wrapper.meta.dropped_fields
    if wrapper.meta.truncated_fields:
        out["truncated_fields"] = wrapper.meta.truncated_fields
    return out


__all__ = [
    "append_source",
    "enforce_payload_budget",
    "has_value",
    "merge_gap_fill_payload",
    "normalize_python_payload",
    "normalize_rust_payload",
    "parse_python_enrichment",
    "parse_rust_enrichment",
    "payload_size_hint",
    "set_degraded",
    "string_or_none",
]
