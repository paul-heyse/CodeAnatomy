"""Rust lane pipeline-stage execution surface."""

from __future__ import annotations


def collect_payload_with_timings(request: object) -> dict[str, object]:
    """Collect Rust enrichment payload with stage timing metadata.

    Returns:
        Rust enrichment payload annotated with per-stage timing fields.
    """
    from tools.cq.search.tree_sitter.rust_lane.runtime_engine import (
        collect_payload_with_timings as collect_payload_with_timings_impl,
    )

    return collect_payload_with_timings_impl(request)


def run_rust_enrichment_pipeline(request: object) -> dict[str, object] | None:
    """Run full Rust enrichment pipeline.

    Returns:
        Rust enrichment payload when enrichment succeeds; otherwise `None`.
    """
    from tools.cq.search.tree_sitter.rust_lane.runtime_engine import (
        run_rust_enrichment_pipeline as run_rust_enrichment_pipeline_impl,
    )

    return run_rust_enrichment_pipeline_impl(request)


__all__ = ["collect_payload_with_timings", "run_rust_enrichment_pipeline"]
