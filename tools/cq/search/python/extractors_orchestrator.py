"""Public orchestration entry points for Python enrichment pipeline."""

from __future__ import annotations

from tools.cq.search._shared.requests import (
    PythonByteRangeEnrichmentRequest,
    PythonNodeEnrichmentRequest,
)
from tools.cq.search.python.extractors_entrypoints import (
    enrich_python_context as _enrich_python_context,
)
from tools.cq.search.python.extractors_entrypoints import (
    enrich_python_context_by_byte_range as _enrich_python_context_by_byte_range,
)

__all__ = ["enrich_python_context", "enrich_python_context_by_byte_range"]


def enrich_python_context(request: PythonNodeEnrichmentRequest) -> dict[str, object] | None:
    """Orchestrate full Python enrichment for one ast-grep node request.

    Returns:
        dict[str, object] | None: Enrichment payload, or `None` when unavailable.
    """
    return _enrich_python_context(request)


def enrich_python_context_by_byte_range(
    request: PythonByteRangeEnrichmentRequest,
) -> dict[str, object] | None:
    """Orchestrate full Python enrichment for one byte-range request.

    Returns:
        dict[str, object] | None: Enrichment payload for the requested source range.
    """
    return _enrich_python_context_by_byte_range(request)
