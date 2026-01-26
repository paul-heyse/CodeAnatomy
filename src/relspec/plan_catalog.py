"""Plan compilation and catalog helpers for inference-driven scheduling."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from ibis.expr.types import Scalar

from arrowdsl.core.ordering import Ordering
from datafusion_engine.diagnostics import record_artifact
from datafusion_engine.parameterized_execution import ParameterizedRulepack, ParameterSpec
from ibis_engine.execution_factory import datafusion_facade_from_ctx
from ibis_engine.plan import IbisPlan
from relspec.context import ensure_task_build_context
from relspec.inferred_deps import InferredDeps, infer_deps_from_sqlglot_expr
from relspec.task_catalog import TaskBuildContext, TaskCatalog, TaskSpec

if TYPE_CHECKING:
    from ibis.backends import BaseBackend

    from arrowdsl.core.execution_context import ExecutionContext
    from datafusion_engine.diagnostics import DiagnosticsRecorder
    from datafusion_engine.execution_facade import DataFusionExecutionFacade
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
    parameterization : PlanParameterization | None
        Parameterization metadata for parameterized tasks when configured.
    """

    task: TaskSpec
    plan: IbisPlan
    sqlglot_ast: Expression
    deps: InferredDeps
    plan_fingerprint: str
    parameterization: PlanParameterization | None = None


@dataclass(frozen=True)
class PlanParameterization:
    """Parameterized execution metadata for a compiled plan."""

    param_specs: Mapping[str, ParameterSpec]
    params: Mapping[str, Scalar]

    def resolve_values(self, values: Mapping[str, object]) -> dict[str, object]:
        """Resolve parameter values using defaults and validation.

        Returns
        -------
        dict[str, object]
            Resolved parameter values.

        Raises
        ------
        ValueError
            Raised when required parameters are missing or unknown parameters are supplied.
        """
        resolved: dict[str, object] = {}
        for name, spec in self.param_specs.items():
            if name in values:
                spec.validate_value(values[name])
                resolved[name] = values[name]
                continue
            if spec.default is not None:
                resolved[name] = spec.default
                continue
            msg = f"Missing required parameter: {name}"
            raise ValueError(msg)
        unknown = set(values) - set(self.param_specs)
        if unknown:
            msg = f"Unknown parameters: {sorted(unknown)}"
            raise ValueError(msg)
        return resolved

    def param_mapping(self, values: Mapping[str, object]) -> Mapping[Scalar, object]:
        """Return Ibis parameter mapping for execution.

        Returns
        -------
        Mapping[ibis.expr.types.Scalar, object]
            Mapping from Ibis scalar parameters to resolved values.
        """
        resolved = self.resolve_values(values)
        return {self.params[name]: resolved[name] for name in resolved}


@dataclass(frozen=True)
class _CompileContext:
    task: TaskSpec
    ctx: ExecutionContext
    plan: IbisPlan
    parameterization: PlanParameterization | None
    backend: BaseBackend
    facade: DataFusionExecutionFacade
    sqlglot_override: Expression | None
    compile_recorder: DiagnosticsRecorder | None


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

    Raises
    ------
    TypeError
        Raised when the task builder returns an unsupported plan type.
    """
    resolved_context = ensure_task_build_context(
        ctx,
        backend,
        build_context=build_context,
    )
    resolved_context = _attach_build_diagnostics(task, resolved_context)
    built = task.build(resolved_context)
    if not isinstance(built, (IbisPlan, ParameterizedRulepack)):
        msg = f"Task builder must return IbisPlan or ParameterizedRulepack, got {type(built)}."
        raise TypeError(msg)
    plan, parameterization = _resolve_built_plan(built)
    facade = resolved_context.facade or datafusion_facade_from_ctx(ctx, backend=backend)
    sqlglot_override = (
        task.sqlglot_builder(resolved_context) if task.sqlglot_builder is not None else None
    )
    compile_recorder = _resolve_compile_recorder(task, resolved_context, facade)
    compile_context = _CompileContext(
        task=task,
        ctx=ctx,
        plan=plan,
        parameterization=parameterization,
        backend=backend,
        facade=facade,
        sqlglot_override=sqlglot_override,
        compile_recorder=compile_recorder,
    )
    return _compile_plan_artifact(compile_context)


def _attach_build_diagnostics(
    task: TaskSpec,
    context: TaskBuildContext,
) -> TaskBuildContext:
    if context.diagnostics is not None:
        return context
    recorder = context.facade.diagnostics_recorder(
        operation_id=f"relspec.build.{task.name}",
    )
    if recorder is None:
        return context
    return replace(context, diagnostics=recorder)


def _resolve_built_plan(
    built: IbisPlan | ParameterizedRulepack,
) -> tuple[IbisPlan, PlanParameterization | None]:
    if isinstance(built, ParameterizedRulepack):
        plan = IbisPlan(expr=built.expr, ordering=Ordering.unordered())
        parameterization = PlanParameterization(
            param_specs=built.param_specs,
            params=built.param_objects(),
        )
        return plan, parameterization
    return built, None


def _resolve_compile_recorder(
    task: TaskSpec,
    context: TaskBuildContext,
    facade: DataFusionExecutionFacade,
) -> DiagnosticsRecorder | None:
    if context.diagnostics is not None:
        return context.diagnostics
    return facade.diagnostics_recorder(operation_id=f"relspec.compile.{task.name}")


def _compile_plan_artifact(context: _CompileContext) -> PlanArtifact:
    return _compile_with_facade(context)


def _compile_with_facade(context: _CompileContext) -> PlanArtifact:
    facade = context.facade
    compiled = facade.compile(context.sqlglot_override or context.plan.expr)
    sqlglot_ast = compiled.compiled.sqlglot_ast
    deps = infer_deps_from_sqlglot_expr(
        sqlglot_ast,
        task_name=context.task.name,
        output=context.task.output,
        dialect="datafusion",
    )
    fingerprint = compiled.compiled.fingerprint
    deps = replace(deps, plan_fingerprint=fingerprint)
    _record_compile_artifact(
        context.ctx,
        recorder=context.compile_recorder,
        task=context.task,
        sqlglot_ast=sqlglot_ast,
        plan_fingerprint=fingerprint,
    )
    return PlanArtifact(
        task=context.task,
        plan=context.plan,
        sqlglot_ast=sqlglot_ast,
        deps=deps,
        plan_fingerprint=fingerprint,
        parameterization=context.parameterization,
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
    resolved_context = ensure_task_build_context(
        ctx,
        backend,
        build_context=build_context,
    )
    artifacts = tuple(
        compile_task_plan(
            task,
            backend=backend,
            ctx=ctx,
            build_context=resolved_context,
        )
        for task in catalog.tasks
    )
    return PlanCatalog(artifacts=artifacts)


def _record_compile_artifact(
    ctx: ExecutionContext,
    *,
    recorder: DiagnosticsRecorder | None,
    task: TaskSpec,
    sqlglot_ast: Expression,
    plan_fingerprint: str,
) -> None:
    payload = {
        "task_name": task.name,
        "output": task.output,
        "plan_fingerprint": plan_fingerprint,
        "ast_type": sqlglot_ast.__class__.__name__,
    }
    if recorder is not None:
        recorder.record_artifact("relspec_plan_compile_v1", payload)
        return
    profile = ctx.runtime.datafusion
    if profile is None:
        msg = "DataFusion runtime profile is required for plan diagnostics."
        raise ValueError(msg)
    record_artifact(profile, "relspec_plan_compile_v1", payload)


__all__ = [
    "PlanArtifact",
    "PlanCatalog",
    "PlanParameterization",
    "compile_task_catalog",
    "compile_task_plan",
]
