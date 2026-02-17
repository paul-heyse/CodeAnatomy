"""Semantic build execution helpers extracted from pipeline_build."""

from __future__ import annotations

from typing import TYPE_CHECKING

from semantics.pipeline_build import build_cpg

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from semantics.compile_context import SemanticExecutionContext
    from semantics.pipeline_build import CpgBuildOptions


def execute_semantic_build(
    ctx: SessionContext,
    *,
    runtime_profile: DataFusionRuntimeProfile,
    execution_context: SemanticExecutionContext,
    options: CpgBuildOptions | None = None,
) -> object | None:
    """Execute semantic CPG build using canonical pipeline entrypoint.

    Returns:
        object: Return payload from the semantic build pipeline.
    """
    return build_cpg(
        ctx,
        runtime_profile=runtime_profile,
        options=options,
        execution_context=execution_context,
    )


__all__ = ["execute_semantic_build"]
