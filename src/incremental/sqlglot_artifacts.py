"""View artifact helpers for incremental plans."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.execution_facade import DataFusionExecutionFacade
from datafusion_engine.runtime import record_view_definition
from datafusion_engine.sql_policy_engine import SQLPolicyProfile
from datafusion_engine.view_artifacts import ViewArtifact, ViewArtifactInputs, build_view_artifact
from incremental.runtime import IncrementalRuntime

if TYPE_CHECKING:
    from ibis.expr.types import Table as IbisTable


def record_view_artifact(
    runtime: IncrementalRuntime,
    *,
    name: str,
    expr: IbisTable,
    schema: pa.Schema,
) -> ViewArtifact:
    """Record a ViewArtifact for an incremental Ibis expression.

    Parameters
    ----------
    runtime
        Incremental runtime context.
    name
        Logical name for the artifact (typically the view/dataset name).
    expr
        Ibis expression for compilation.
    schema
        Resolved Arrow schema for the output.

    Returns
    -------
    ViewArtifact
        Recorded view artifact instance.
    """
    ctx = runtime.session_context()
    policy_profile = SQLPolicyProfile(policy=runtime.sqlglot_policy)
    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=runtime.profile)
    from datafusion_engine.compile_options import DataFusionCompileOptions

    plan = facade.compile(
        expr,
        options=DataFusionCompileOptions(
            sql_policy_profile=policy_profile,
            runtime_profile=runtime.profile,
        ),
    )
    artifact = build_view_artifact(
        ViewArtifactInputs(
            ctx=ctx,
            name=name,
            ast=plan.compiled.sqlglot_ast,
            schema=schema,
            policy_profile=policy_profile,
            sql=plan.compiled.rendered_sql,
        )
    )
    record_view_definition(runtime.profile, artifact=artifact)
    return artifact


__all__ = ["record_view_artifact"]
