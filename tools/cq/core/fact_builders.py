"""Fact payload builder helpers."""

from __future__ import annotations

from tools.cq.core.enrichment_facts import (
    resolve_fact_clusters as _resolve_fact_clusters,
)
from tools.cq.core.enrichment_facts import resolve_fact_context, resolve_primary_language_payload

__all__ = ["build_fact_clusters", "build_primary_language_payload"]


def build_primary_language_payload(details: dict[str, object]) -> dict[str, object]:
    """Resolve primary-language enrichment payload.

    Returns:
        dict[str, object]: Language-specific primary payload mapping.
    """
    _language, payload = resolve_primary_language_payload(details)
    return payload if isinstance(payload, dict) else {}


def build_fact_clusters(details: dict[str, object]) -> tuple[object, ...]:
    """Resolve fact clusters for render consumption.

    Returns:
        tuple[object, ...]: Ordered fact clusters for rendering.
    """
    language, payload = resolve_primary_language_payload(details)
    context = resolve_fact_context(language=language, language_payload=payload)
    return tuple(
        _resolve_fact_clusters(
            context=context,
            language_payload=payload,
        )
    )
