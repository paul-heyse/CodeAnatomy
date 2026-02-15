"""Python-lane orchestration helpers."""

from __future__ import annotations

from collections.abc import Mapping

from tools.cq.search._shared.core import PythonByteRangeEnrichmentRequest
from tools.cq.search.python.extractors import extract_python_byte_range
from tools.cq.search.python.resolution_index import build_resolution_index
from tools.cq.search.python.semantic_signal import evaluate_python_semantic_signal_from_mapping


def run_python_enrichment_pipeline(
    request: PythonByteRangeEnrichmentRequest,
) -> dict[str, object]:
    """Run enrichment + native-resolution merge for one byte-range request."""
    payload = extract_python_byte_range(request)
    resolution = build_resolution_index(
        source_bytes=request.source_bytes,
        byte_start=request.byte_start,
        byte_end=request.byte_end,
        cache_key=request.cache_key,
        session=request.session,
    )
    if resolution:
        payload.setdefault("resolution", resolution)
    has_signal, reasons = evaluate_python_semantic_signal_from_mapping(
        payload if isinstance(payload, Mapping) else {}
    )
    payload["semantic_signal"] = {"has_signal": has_signal, "reasons": list(reasons)}
    return payload


__all__ = ["run_python_enrichment_pipeline"]
