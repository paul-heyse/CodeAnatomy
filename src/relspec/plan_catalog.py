"""Plan compilation and catalog helpers for inference-driven scheduling."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from ibis.expr.types import Scalar

from arrowdsl.core.ordering import Ordering
from datafusion_engine.parameterized_execution import ParameterizedRulepack, ParameterSpec
from ibis_engine.compiler_checkpoint import compile_checkpoint
from ibis_engine.plan import IbisPlan
from relspec.inferred_deps import InferredDeps, infer_deps_from_sqlglot_expr
from relspec.task_catalog import TaskBuildContext, TaskCatalog, TaskSpec

if TYPE_CHECKING:
    from ibis.backends import BaseBackend

    from arrowdsl.core.execution_context import ExecutionContext
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
    resolved_context = build_context or TaskBuildContext(ctx=ctx, backend=backend)
    built = task.build(resolved_context)
    parameterization: PlanParameterization | None = None
    if isinstance(built, ParameterizedRulepack):
        plan = IbisPlan(expr=built.expr, ordering=Ordering.unordered())
        parameterization = PlanParameterization(
            param_specs=built.param_specs,
            params=built.param_objects(),
        )
    else:
        if not isinstance(built, IbisPlan):
            msg = f"Task builder must return IbisPlan or ParameterizedRulepack, got {type(built)}."
            raise TypeError(msg)
        plan = built
    compiler_backend = cast("IbisCompilerBackend", backend)
    checkpoint = compile_checkpoint(
        plan.expr,
        backend=compiler_backend,
        schema_map=None,
        dialect="datafusion",
    )
    deps = infer_deps_from_sqlglot_expr(
        checkpoint.normalized,
        task_name=task.name,
        output=task.output,
        dialect="datafusion",
    )
    fingerprint = checkpoint.plan_hash
    return PlanArtifact(
        task=task,
        plan=plan,
        sqlglot_ast=checkpoint.normalized,
        deps=deps,
        plan_fingerprint=fingerprint,
        parameterization=parameterization,
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
    "PlanParameterization",
    "compile_task_catalog",
    "compile_task_plan",
]
