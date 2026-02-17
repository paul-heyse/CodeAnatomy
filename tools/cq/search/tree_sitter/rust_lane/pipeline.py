"""Rust lane enrichment pipeline entry points.

Keeps runtime orchestration callables in a dedicated module for reuse.
"""

from __future__ import annotations

__all__ = ["collect_payload_with_timings", "run_rust_enrichment_pipeline"]


def collect_payload_with_timings(request: object) -> dict[str, object]:
    """Delegate to runtime payload+timing collection implementation.

    Returns:
        dict[str, object]: Payload and timing envelope from runtime implementation.
    """
    from tools.cq.search.tree_sitter.rust_lane.runtime_core import (
        collect_payload_with_timings as collect_payload_with_timings_impl,
    )

    return collect_payload_with_timings_impl(request)


def run_rust_enrichment_pipeline(request: object) -> dict[str, object] | None:
    """Delegate to runtime pipeline execution implementation.

    Returns:
        dict[str, object] | None: Enrichment payload, or None on fail-open paths.
    """
    from tools.cq.search.tree_sitter.rust_lane.runtime_core import (
        run_rust_enrichment_pipeline as run_rust_enrichment_pipeline_impl,
    )

    return run_rust_enrichment_pipeline_impl(request)
