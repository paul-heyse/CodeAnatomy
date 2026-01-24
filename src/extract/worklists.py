"""DataFusion-backed worklist helpers for extraction."""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING

from datafusion import SessionContext

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import TableLike
from datafusion_engine.bridge import datafusion_from_arrow
from datafusion_engine.df_builder import df_from_sqlglot
from datafusion_engine.registry_bridge import register_dataset_df
from datafusion_engine.schema_introspection import table_names_snapshot
from extract.helpers import FileContext, iter_file_contexts
from sqlglot_tools.compat import Expression, exp
from sqlglot_tools.optimizer import NormalizeExprOptions, normalize_expr, resolve_sqlglot_policy

if TYPE_CHECKING:
    from datafusion_engine.runtime import DataFusionRuntimeProfile
    from ibis_engine.registry import DatasetLocation


def _is_null(expr: Expression) -> Expression:
    return exp.Is(this=expr, expression=exp.null())


def _is_not_null(expr: Expression) -> Expression:
    return exp.Not(this=_is_null(expr))


def _is_distinct(left: Expression, right: Expression) -> Expression:
    return exp.or_(
        exp.and_(_is_null(left), _is_not_null(right)),
        exp.and_(_is_not_null(left), _is_null(right)),
        exp.NEQ(this=left, expression=right),
    )


def worklist_expr(
    output_table: str | None,
    *,
    repo_table: str,
    output_table_name: str | None = None,
) -> Expression:
    """Return a SQLGlot expression for a dataset worklist.

    Returns
    -------
    sqlglot.expressions.Expression
        SQLGlot expression representing the worklist query.
    """
    repo_tbl = exp.to_table(repo_table)
    if output_table is None:
        return exp.select("*").from_(repo_tbl)
    output_name = output_table_name or output_table
    output_tbl = exp.to_table(output_name)
    repo_alias = repo_tbl.alias_or_name
    output_alias = output_tbl.alias_or_name
    join_on = exp.EQ(
        this=exp.column("file_id", table=repo_alias),
        expression=exp.column("file_id", table=output_alias),
    )
    output_file_id = exp.column("file_id", table=output_alias)
    output_sha = exp.column("file_sha256", table=output_alias)
    repo_sha = exp.column("file_sha256", table=repo_alias)
    return (
        exp.select("*")
        .from_(repo_tbl)
        .join(output_tbl, on=join_on, join_type="left")
        .where(exp.or_(_is_null(output_file_id), _is_distinct(output_sha, repo_sha)))
    )


def iter_worklist_contexts(
    repo_files: TableLike,
    *,
    output_table: str,
    ctx: ExecutionContext | None,
    file_contexts: Iterable[FileContext] | None = None,
) -> Iterable[FileContext]:
    """Yield worklist file contexts with DataFusion fallback.

    Yields
    ------
    FileContext
        File contexts matching the worklist query.
    """
    if file_contexts is not None:
        yield from file_contexts
        return
    if ctx is None or ctx.runtime.datafusion is None:
        yield from iter_file_contexts(repo_files)
        return
    yield from _worklist_stream(ctx, repo_files=repo_files, output_table=output_table)


def _worklist_stream(
    ctx: ExecutionContext,
    *,
    repo_files: TableLike,
    output_table: str,
) -> Iterator[FileContext]:
    runtime_profile = ctx.runtime.datafusion
    if runtime_profile is None:
        yield from iter_file_contexts(repo_files)
        return
    df_ctx = runtime_profile.session_context()
    repo_name = f"__repo_files_{uuid.uuid4().hex}"
    output_exists = _table_exists(df_ctx, output_table)
    output_location = None if output_exists else runtime_profile.dataset_location(output_table)
    use_output = output_exists or output_location is not None
    output_name = output_table if use_output else None
    expr = worklist_expr(
        output_table if use_output else None,
        repo_table=repo_name,
        output_table_name=output_name,
    )
    expr = _normalize_worklist_expr(expr)
    with _registered_table(df_ctx, name=repo_name, table=repo_files), _registered_output_table(
        df_ctx,
        name=output_table,
        location=output_location,
        runtime_profile=runtime_profile,
    ):
        stream = df_from_sqlglot(df_ctx, expr).execute_stream()
        for batch in stream:
            arrow_batch = batch.to_pyarrow()
            rows = arrow_batch.to_pylist()
            for row in rows:
                yield FileContext.from_repo_row(row)


def _table_exists(ctx: SessionContext, name: str) -> bool:
    try:
        return name in table_names_snapshot(ctx)
    except (RuntimeError, TypeError, ValueError):
        pass
    tables_expr = (
        exp.select("table_name")
        .from_(exp.to_table("tables", db="information_schema"))
        .where(exp.EQ(this=exp.column("table_name"), expression=exp.Literal.string(name)))
        .limit(1)
    )
    try:
        stream = df_from_sqlglot(ctx, tables_expr).execute_stream()
    except (KeyError, RuntimeError, TypeError, ValueError):
        try:
            ctx.table(name)
        except KeyError:
            return False
        return True
    return any(batch.to_pyarrow().num_rows for batch in stream)


def _normalize_worklist_expr(expr: Expression) -> Expression:
    policy = resolve_sqlglot_policy(name="datafusion_compile")
    options = NormalizeExprOptions(policy=policy)
    return normalize_expr(expr, options=options)


@contextlib.contextmanager
def _registered_table(
    ctx: SessionContext,
    *,
    name: str,
    table: TableLike,
) -> Iterator[None]:
    datafusion_from_arrow(ctx, name=name, value=table)
    try:
        yield None
    finally:
        deregister = getattr(ctx, "deregister_table", None)
        if callable(deregister):
            with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                deregister(name)


@contextlib.contextmanager
def _registered_output_table(
    ctx: SessionContext,
    *,
    name: str,
    location: DatasetLocation | None,
    runtime_profile: DataFusionRuntimeProfile,
) -> Iterator[None]:
    if location is None:
        yield None
        return
    register_dataset_df(
        ctx,
        name=name,
        location=location,
        runtime_profile=runtime_profile,
    )
    try:
        yield None
    finally:
        deregister = getattr(ctx, "deregister_table", None)
        if callable(deregister):
            with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                deregister(name)


__all__ = ["iter_worklist_contexts", "worklist_expr"]
