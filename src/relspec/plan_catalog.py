"""Plan compilation and catalog helpers for inference-driven scheduling."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from relspec.inferred_deps import (
    InferredDeps,
    InferredDepsRequest,
    infer_deps_from_ibis_plan,
)
from relspec.task_catalog import TaskBuildContext, TaskCatalog, TaskSpec
from sqlglot_tools.bridge import ibis_to_sqlglot
from sqlglot_tools.optimizer import plan_fingerprint

if TYPE_CHECKING:
    from ibis.backends import BaseBackend

    from arrowdsl.core.execution_context import ExecutionContext
    from ibis_engine.plan import IbisPlan
    from sqlglot_tools.bridge import IbisCompilerBackend
    from sqlglot_tools.compat import Expression


@dataclass(frozen=True)
class PlanArtifact:
    """Compiled plan plus lineage metadata.

    Attributes
    ----------
    task : TaskSpec
        Task specification used for compilation.
    plan : IbisPlan
        Ibis plan for execution.
    sqlglot_ast : Expression
        SQLGlot AST for lineage analysis.
    deps : InferredDeps
        Inferred dependencies for scheduling.
    plan_fingerprint : str
        Stable plan fingerprint for caching/incremental use.
    """

    task: TaskSpec
    plan: IbisPlan
    sqlglot_ast: Expression
    deps: InferredDeps
    plan_fingerprint: str


@dataclass(frozen=True)
class PlanCatalog:
    """Immutable catalog of compiled plans."""

    artifacts: tuple[PlanArtifact, ...]

    def by_task(self) -> Mapping[str, PlanArtifact]:
        """Return plan artifacts keyed by task name.

        Returns
        -------
        Mapping[str, PlanArtifact]
            Mapping of task name to plan artifact.
        """
        return {artifact.task.name: artifact for artifact in self.artifacts}

    def by_output(self) -> Mapping[str, PlanArtifact]:
        """Return plan artifacts keyed by output name.

        Returns
        -------
        Mapping[str, PlanArtifact]
            Mapping of output dataset name to plan artifact.
        """
        return {artifact.task.output: artifact for artifact in self.artifacts}


def compile_task_plan(
    task: TaskSpec,
    *,
    backend: BaseBackend,
    ctx: ExecutionContext,
    build_context: TaskBuildContext | None = None,
) -> PlanArtifact:
    """Compile a task into an Ibis plan with lineage metadata.

    Parameters
    ----------
    task : TaskSpec
        Task specification to compile.
    backend : BaseBackend
        Backend for Ibis/SQLGlot compilation.
    ctx : ExecutionContext
        Execution context passed to the task builder.
    build_context : TaskBuildContext | None
        Optional pre-built task context overrides.

    Returns
    -------
    PlanArtifact
        Compiled plan with SQLGlot lineage metadata.
    """
    resolved_context = build_context or TaskBuildContext(ctx=ctx, backend=backend)
    plan = task.build(resolved_context)
    compiler_backend = cast("IbisCompilerBackend", backend)
    sg_expr = ibis_to_sqlglot(plan.expr, backend=compiler_backend, params=None)
    deps = infer_deps_from_ibis_plan(
        plan,
        backend=compiler_backend,
        request=InferredDepsRequest(rule_name=task.name, output=task.output),
        sqlglot_expr=sg_expr,
    )
    fingerprint = deps.plan_fingerprint or plan_fingerprint(sg_expr)
    return PlanArtifact(
        task=task,
        plan=plan,
        sqlglot_ast=sg_expr,
        deps=deps,
        plan_fingerprint=fingerprint,
    )


def compile_task_catalog(
    catalog: TaskCatalog,
    *,
    backend: BaseBackend,
    ctx: ExecutionContext,
    build_context: TaskBuildContext | None = None,
) -> PlanCatalog:
    """Compile all tasks in a catalog.

    Parameters
    ----------
    catalog : TaskCatalog
        Task catalog to compile.
    backend : BaseBackend
        Backend for Ibis/SQLGlot compilation.
    ctx : ExecutionContext
        Execution context passed to task builders.
    build_context : TaskBuildContext | None
        Optional task build context override.

    Returns
    -------
    PlanCatalog
        Catalog of compiled plans.
    """
    artifacts = tuple(
        compile_task_plan(
            task,
            backend=backend,
            ctx=ctx,
            build_context=build_context,
        )
        for task in catalog.tasks
    )
    return PlanCatalog(artifacts=artifacts)


__all__ = [
    "PlanArtifact",
    "PlanCatalog",
    "compile_task_catalog",
    "compile_task_plan",
]
