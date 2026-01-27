"""DataFusion-backed worklist helpers for extraction."""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Protocol

from datafusion import SessionContext

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import TableLike
from cache.diskcache_factory import build_deque, build_index
from datafusion_engine.arrow_ingest import datafusion_from_arrow
from datafusion_engine.schema_introspection import table_names_snapshot
from extract.cache_utils import diskcache_profile_from_ctx, stable_cache_label
from extract.helpers import FileContext
from sqlglot_tools.compat import Expression, exp

if TYPE_CHECKING:
    import pyarrow as pa
    from diskcache import Deque, Index

    from datafusion_engine.dataset_registry import DatasetLocation
    from datafusion_engine.runtime import DataFusionRuntimeProfile


class _ArrowBatch(Protocol):
    def to_pyarrow(self) -> pa.RecordBatch:
        """Return the underlying PyArrow record batch."""
        ...


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
    queue_name: str | None = None,
) -> Iterable[FileContext]:
    """Yield worklist file contexts with DataFusion fallback.

    Parameters
    ----------
    repo_files:
        Repo manifest table.
    output_table:
        Output dataset name used for worklist computation.
    ctx:
        Execution context used for DataFusion integration.
    file_contexts:
        Optional precomputed file contexts.
    queue_name:
        Optional DiskCache queue name for persistent worklists.

    Yields
    ------
    FileContext
        File contexts matching the worklist query.

    Raises
    ------
    ValueError
        Raised when the execution context lacks a DataFusion runtime profile.
    """
    if file_contexts is not None:
        yield from file_contexts
        return
    if ctx is None or ctx.runtime.datafusion is None:
        msg = "Worklist execution requires a DataFusion runtime profile."
        raise ValueError(msg)
    if queue_name is None:
        yield from _worklist_stream(ctx, repo_files=repo_files, output_table=output_table)
        return
    queue_bundle = _worklist_queue(ctx, queue_name=queue_name)
    if queue_bundle is None:
        yield from _worklist_stream(ctx, repo_files=repo_files, output_table=output_table)
        return
    queue, index = queue_bundle
    if len(queue) > 0:
        yield from _drain_worklist_queue(queue, index=index)
        return
    for file_ctx in _worklist_stream(ctx, repo_files=repo_files, output_table=output_table):
        file_id = file_ctx.file_id
        if not file_id:
            continue
        sha = file_ctx.file_sha256 or ""
        existing = index.get(file_id)
        if isinstance(existing, str) and existing == sha:
            continue
        index[file_id] = sha
        queue.append(file_ctx)
    yield from _drain_worklist_queue(queue, index=index)


def _worklist_stream(
    ctx: ExecutionContext,
    *,
    repo_files: TableLike,
    output_table: str,
) -> Iterator[FileContext]:
    runtime_profile = ctx.runtime.datafusion
    if runtime_profile is None:
        msg = "Worklist streaming requires a DataFusion runtime profile."
        raise ValueError(msg)
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
    with (
        _registered_table(df_ctx, name=repo_name, table=repo_files),
        _registered_output_table(
            df_ctx,
            name=output_table,
            location=output_location,
            runtime_profile=runtime_profile,
        ),
    ):
        stream = _execute_expr_stream(df_ctx, expr, runtime_profile=runtime_profile)
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
    try:
        ctx.table(name)
    except (KeyError, RuntimeError, TypeError, ValueError):
        return False
    return True


def _execute_expr_stream(
    ctx: SessionContext,
    expr: Expression,
    *,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> Iterator[_ArrowBatch]:
    from datafusion_engine.execution_facade import DataFusionExecutionFacade

    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=runtime_profile)
    plan = facade.compile(expr)
    result = facade.execute(plan)
    if result.dataframe is None:
        msg = "Worklist execution did not return a DataFusion DataFrame."
        raise ValueError(msg)
    return result.dataframe.execute_stream()


def worklist_queue_name(*, output_table: str, repo_id: str | None) -> str:
    """Return a stable queue name for persistent worklists.

    Returns
    -------
    str
        Stable queue name derived from the repo and output table.
    """
    return stable_cache_label(
        "worklist",
        {"output_table": output_table, "repo_id": repo_id},
    )


def _worklist_queue(
    ctx: ExecutionContext,
    *,
    queue_name: str,
) -> tuple[Deque, Index] | None:
    profile = diskcache_profile_from_ctx(ctx)
    if profile is None:
        return None
    base_name = f"worklist_{queue_name}"
    queue = build_deque(profile, name=base_name)
    index = build_index(profile, name=f"{base_name}_index")
    return queue, index


def _drain_worklist_queue(queue: Deque, *, index: Index) -> Iterator[FileContext]:
    while len(queue) > 0:
        item = queue.popleft()
        if not isinstance(item, FileContext):
            continue
        file_id = item.file_id
        if file_id:
            with contextlib.suppress(KeyError):
                index.pop(file_id)
        yield item


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
    from datafusion_engine.execution_facade import DataFusionExecutionFacade

    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=runtime_profile)
    facade.register_dataset(name=name, location=location)
    try:
        yield None
    finally:
        deregister = getattr(ctx, "deregister_table", None)
        if callable(deregister):
            with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                deregister(name)


__all__ = ["iter_worklist_contexts", "worklist_expr", "worklist_queue_name"]
