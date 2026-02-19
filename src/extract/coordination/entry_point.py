"""Shared entry-point helpers for extractor wrappers."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, TypedDict

from datafusion_engine.arrow.interop import TableLike
from extract.coordination.context import ExtractExecutionContext
from extract.coordination.materialization import (
    ExtractMaterializeOptions,
    materialize_extract_plan,
)
from extract.coordination.schema_ops import ExtractNormalizeOptions
from extract.infrastructure.result_types import ExtractResult

if TYPE_CHECKING:
    from collections.abc import Callable

    from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from extract.session import ExtractSession


class _NormalizeKwargs(TypedDict, total=False):
    repo_id: str | None
    enable_encoding: bool


def run_extract_entry_point[T_Options](  # noqa: PLR0913
    extractor_name: str,
    dataset_name: str,
    repo_files: TableLike,
    options: T_Options,
    *,
    context: ExtractExecutionContext | None = None,
    plan_builder: Callable[
        [
            TableLike,
            T_Options,
            ExtractExecutionContext,
            ExtractSession,
            DataFusionRuntimeProfile,
        ],
        DataFusionPlanArtifact,
    ],
    normalize_kwargs: _NormalizeKwargs | None = None,
    apply_post_kernels: bool = True,
) -> ExtractResult[TableLike]:
    """Run canonical extractor materialization flow for table entry points.

    Returns:
        ExtractResult[TableLike]: Materialized extractor output for ``dataset_name``.
    """
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    determinism_tier = exec_context.determinism_tier()
    normalize = ExtractNormalizeOptions(options=options, **(normalize_kwargs or {}))

    plan = plan_builder(repo_files, options, exec_context, session, runtime_profile)

    table = materialize_extract_plan(
        dataset_name,
        plan,
        runtime_profile=runtime_profile,
        determinism_tier=determinism_tier,
        options=ExtractMaterializeOptions(
            normalize=normalize,
            apply_post_kernels=apply_post_kernels,
        ),
    )
    return ExtractResult(table=table, extractor_name=extractor_name)


def run_extract_plan_entry_point[T_Options](
    repo_files: TableLike,
    options: T_Options,
    *,
    context: ExtractExecutionContext | None = None,
    plan_builder: Callable[
        [
            TableLike,
            T_Options,
            ExtractExecutionContext,
            ExtractSession,
            DataFusionRuntimeProfile,
        ],
        dict[str, DataFusionPlanArtifact],
    ],
) -> dict[str, DataFusionPlanArtifact]:
    """Run canonical extractor setup flow for plan-only entry points.

    Returns:
        dict[str, DataFusionPlanArtifact]: Named plan artifacts returned by the builder.
    """
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    return plan_builder(repo_files, options, exec_context, session, runtime_profile)


__all__ = ["run_extract_entry_point", "run_extract_plan_entry_point"]
