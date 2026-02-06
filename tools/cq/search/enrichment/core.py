"""Shared enrichment helpers for payload normalization and merge policy."""

from __future__ import annotations

# ruff: noqa: DOC201
from collections.abc import Mapping, Sequence
from typing import cast

import msgspec

from tools.cq.core.serialization import to_builtins
from tools.cq.search.enrichment.contracts import (
    EnrichmentMeta,
    PythonEnrichmentPayload,
    RustEnrichmentPayload,
)

_DEFAULT_METADATA_KEYS: frozenset[str] = frozenset(
    {"enrichment_status", "enrichment_sources", "degrade_reason", "language"}
)


def payload_size_hint(payload: Mapping[str, object]) -> int:
    """Estimate payload size using msgspec JSON encoding."""
    return len(msgspec.json.encode(payload))


def has_value(value: object) -> bool:
    """Return whether a payload value is considered non-empty."""
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


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
    """Merge secondary payload values only when primary is missing/empty."""
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
    """Prune optional keys until payload fits the size budget."""
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
    status = status_raw if isinstance(status_raw, str) else "applied"
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


def normalize_python_payload(payload: dict[str, object] | None) -> dict[str, object] | None:
    """Normalize Python enrichment payload via typed wrapper contract."""
    if payload is None:
        return None
    wrapper = PythonEnrichmentPayload(
        meta=_meta_from_flat(payload, language="python"),
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
    out = cast("dict[str, object]", to_builtins(wrapper.data))
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


def normalize_rust_payload(payload: dict[str, object] | None) -> dict[str, object] | None:
    """Normalize Rust enrichment payload via typed wrapper contract."""
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
    out = cast("dict[str, object]", to_builtins(wrapper.data))
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
    "payload_size_hint",
    "set_degraded",
]
