"""Semantic front-door provider helpers for concrete language lanes."""

from __future__ import annotations

from pathlib import Path

from tools.cq.search._shared.requests import PythonByteRangeEnrichmentRequest
from tools.cq.search.pipeline.classifier import get_sg_root


def run_python_byte_range_enrichment(
    *,
    target_file_path: Path,
    source_bytes: bytes,
    byte_start: int,
    byte_end: int,
    query_budget_ms: int,
) -> dict[str, object] | None:
    """Execute Python byte-range enrichment for front-door semantic flow.

    Returns:
        Enrichment payload when available; otherwise `None`.
    """
    from tools.cq.search.pipeline.classifier_runtime import ClassifierCacheContext
    from tools.cq.search.python.extractors_orchestrator import (
        enrich_python_context_by_byte_range,
    )

    sg_root = get_sg_root(
        target_file_path,
        lang="python",
        cache_context=ClassifierCacheContext(),
    )
    payload = enrich_python_context_by_byte_range(
        PythonByteRangeEnrichmentRequest(
            sg_root=sg_root,
            source_bytes=source_bytes,
            byte_start=byte_start,
            byte_end=byte_end,
            cache_key=str(target_file_path),
            query_budget_ms=query_budget_ms,
        )
    )
    return dict(payload) if isinstance(payload, dict) else None


def run_rust_byte_range_enrichment(
    *,
    source: str,
    target_file_path: Path,
    byte_start: int,
    byte_end: int,
    query_budget_ms: int,
) -> dict[str, object] | None:
    """Execute Rust byte-range enrichment for front-door semantic flow.

    Returns:
        Enrichment payload when available; otherwise `None`.
    """
    from tools.cq.search.rust.enrichment import enrich_rust_context_by_byte_range

    payload = enrich_rust_context_by_byte_range(
        source,
        byte_start=byte_start,
        byte_end=byte_end,
        cache_key=str(target_file_path),
        query_budget_ms=query_budget_ms,
    )
    return dict(payload) if isinstance(payload, dict) else None


__all__ = [
    "run_python_byte_range_enrichment",
    "run_rust_byte_range_enrichment",
]
