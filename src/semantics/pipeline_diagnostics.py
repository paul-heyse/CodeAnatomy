"""Semantic pipeline diagnostics helpers."""

from __future__ import annotations

from semantics.diagnostics_emission import (
    SemanticQualityDiagnosticsRequest,
    build_semantic_diagnostics_context,
    emit_semantic_quality_views,
    run_context_guard,
)


def emit_semantic_quality_diagnostics(request: SemanticQualityDiagnosticsRequest) -> None:
    """Emit semantic quality diagnostics through canonical emission module."""
    if not request.runtime_profile.diagnostics.emit_semantic_quality_diagnostics:
        return
    context = build_semantic_diagnostics_context(
        request.ctx,
        runtime_profile=request.runtime_profile,
        dataset_resolver=request.dataset_resolver,
        schema_policy=request.schema_policy,
    )
    if context is None:
        return
    with run_context_guard():
        emit_semantic_quality_views(
            context,
            requested_outputs=request.requested_outputs,
            manifest=request.manifest,
            finalize_builder=request.finalize_builder,
        )


__all__ = [
    "SemanticQualityDiagnosticsRequest",
    "emit_semantic_quality_diagnostics",
]
