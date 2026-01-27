"""DataFusion-backed worklist helpers for extraction."""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Callable, Iterable, Iterator
from typing import TYPE_CHECKING, Protocol

from datafusion import DataFrame, SessionContext, col

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.interop import TableLike
from cache.diskcache_factory import build_deque, build_index
from datafusion_engine.arrow_ingest import datafusion_from_arrow
from datafusion_engine.schema_introspection import table_names_snapshot
from extract.cache_utils import diskcache_profile_from_ctx, stable_cache_label
from extract.helpers import FileContext

if TYPE_CHECKING:
    import pyarrow as pa
    from diskcache import Deque, Index

    from datafusion_engine.dataset_registry import DatasetLocation
    from datafusion_engine.runtime import DataFusionRuntimeProfile


class _ArrowBatch(Protocol):
    def to_pyarrow(self) -> pa.RecordBatch:
        """Return the underlying PyArrow record batch."""
        ...


def worklist_builder(
    output_table: str | None,
    *,
    repo_table: str,
    output_table_name: str | None = None,
) -> Callable[[SessionContext], DataFrame]:
    """Return a DataFusion DataFrame builder for a dataset worklist.

    Returns
    -------
    Callable[[SessionContext], datafusion.DataFrame]
        DataFrame builder representing the worklist query.
    """

    def _builder(ctx: SessionContext) -> DataFrame:
        repo_df = ctx.table(repo_table)
        if output_table is None:
            return repo_df
        output_name = output_table_name or output_table
        output_df = ctx.table(output_name).select(
            col("file_id").alias("output_file_id"),
            col("file_sha256").alias("output_sha"),
        )
        joined = repo_df.join(
            output_df,
            join_keys=(["file_id"], ["output_file_id"]),
            how="left",
            coalesce_duplicate_keys=True,
        )
        output_missing = col("output_file_id").is_null()
        output_sha = col("output_sha")
        repo_sha = col("file_sha256")
        sha_distinct = (
            (output_sha.is_null() & repo_sha.is_not_null())
            | (output_sha.is_not_null() & repo_sha.is_null())
            | (output_sha != repo_sha)
        )
        return joined.filter(output_missing | sha_distinct)

    return _builder


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
    session_runtime = runtime_profile.session_runtime()
    df_ctx = session_runtime.ctx
    repo_name = f"__repo_files_{uuid.uuid4().hex}"
    output_exists = _table_exists(df_ctx, output_table)
    output_location = runtime_profile.dataset_location(output_table)
    use_output = output_exists or output_location is not None
    output_name = output_table if use_output else None
    builder = worklist_builder(
        output_table if use_output else None,
        repo_table=repo_name,
        output_table_name=output_name,
    )
    if output_location is not None and output_location.format == "delta":
        from datafusion_engine.scan_overrides import apply_scan_unit_overrides
        from datafusion_engine.scan_planner import ScanLineage, plan_scan_unit

        scan_unit = plan_scan_unit(
            df_ctx,
            dataset_name=output_table,
            location=output_location,
            lineage=ScanLineage(
                dataset_name=output_table,
                projected_columns=(),
                pushed_filters=(),
            ),
        )
        apply_scan_unit_overrides(
            df_ctx,
            scan_units=(scan_unit,),
            runtime_profile=runtime_profile,
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
        stream = _execute_expr_stream(df_ctx, builder, runtime_profile=runtime_profile)
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
    builder: Callable[[SessionContext], DataFrame],
    *,
    runtime_profile: DataFusionRuntimeProfile | None = None,
) -> Iterator[_ArrowBatch]:
    from datafusion_engine.execution_facade import DataFusionExecutionFacade

    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=runtime_profile)
    bundle = facade.compile_to_bundle(builder)
    result = facade.execute_plan_bundle(bundle)
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


__all__ = ["iter_worklist_contexts", "worklist_builder", "worklist_queue_name"]
