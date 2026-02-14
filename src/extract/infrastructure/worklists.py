"""DataFusion-backed worklist helpers for extraction."""

from __future__ import annotations

import contextlib
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from datafusion import DataFrame, SessionContext, col

from cache.diskcache_factory import build_deque, build_index
from datafusion_engine.arrow.interop import TableLike
from datafusion_engine.io.ingest import datafusion_from_arrow
from datafusion_engine.schema.introspection import table_names_snapshot
from extract.coordination.context import FileContext
from extract.infrastructure.cache_utils import diskcache_profile_from_ctx, stable_cache_label
from utils.uuid_factory import uuid7_hex

if TYPE_CHECKING:
    import pyarrow as pa
    from diskcache import Deque, Index

    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from extract.scanning.scope_manifest import ScopeManifest
    from semantics.program_manifest import ManifestDatasetResolver


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

    Returns:
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


@dataclass(frozen=True)
class WorklistRequest:
    """Inputs needed to build a worklist iterator."""

    repo_files: TableLike
    output_table: str
    runtime_profile: DataFusionRuntimeProfile | None
    file_contexts: Iterable[FileContext] | None = None
    queue_name: str | None = None
    scope_manifest: ScopeManifest | None = None


def iter_worklist_contexts(request: WorklistRequest) -> Iterable[FileContext]:
    """Yield worklist file contexts with DataFusion fallback.

    Parameters
    ----------
    request:
        Worklist request inputs including repo files, output dataset, and options.

    Yields:
    ------
    FileContext
        File contexts matching the worklist query.

    """
    allowed_paths = (
        request.scope_manifest.allowed_paths() if request.scope_manifest is not None else None
    )
    for file_ctx in _iter_worklist_contexts_raw(request):
        if allowed_paths is not None and file_ctx.path not in allowed_paths:
            continue
        yield file_ctx


def _iter_worklist_contexts_raw(request: WorklistRequest) -> Iterable[FileContext]:
    if request.file_contexts is not None:
        yield from request.file_contexts
        return
    runtime_profile = _resolve_runtime_profile(request.runtime_profile)
    if request.queue_name is None:
        yield from _worklist_stream(
            runtime_profile,
            repo_files=request.repo_files,
            output_table=request.output_table,
        )
        return
    queue_bundle = _worklist_queue(runtime_profile, queue_name=request.queue_name)
    if queue_bundle is None:
        yield from _worklist_stream(
            runtime_profile,
            repo_files=request.repo_files,
            output_table=request.output_table,
        )
        return
    queue, index = queue_bundle
    if len(queue) > 0:
        drained = False
        for file_ctx in _drain_worklist_queue(queue, index=index):
            drained = True
            yield file_ctx
        if drained:
            return
    yield from _stream_with_queue(
        runtime_profile,
        repo_files=request.repo_files,
        output_table=request.output_table,
        queue=queue,
        index=index,
    )


def _stream_with_queue(
    runtime_profile: DataFusionRuntimeProfile,
    *,
    repo_files: TableLike,
    output_table: str,
    queue: Deque,
    index: Index,
) -> Iterable[FileContext]:
    for file_ctx in _worklist_stream(
        runtime_profile,
        repo_files=repo_files,
        output_table=output_table,
    ):
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


def _resolve_runtime_profile(
    runtime_profile: DataFusionRuntimeProfile | None,
) -> DataFusionRuntimeProfile:
    if runtime_profile is not None:
        return runtime_profile
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

    return DataFusionRuntimeProfile()


def _worklist_stream(
    runtime_profile: DataFusionRuntimeProfile,
    *,
    repo_files: TableLike,
    output_table: str,
    dataset_resolver: ManifestDatasetResolver | None = None,
) -> Iterator[FileContext]:
    session_runtime = runtime_profile.session_runtime()
    df_ctx = session_runtime.ctx
    repo_name = f"__repo_files_{uuid7_hex()}"
    output_exists = _table_exists(df_ctx, output_table)
    output_location = (
        dataset_resolver.location(output_table) if dataset_resolver is not None else None
    )
    use_output = output_exists or output_location is not None
    output_name = output_table if use_output else None
    builder = worklist_builder(
        output_table if use_output else None,
        repo_table=repo_name,
        output_table_name=output_name,
    )
    if output_location is not None and output_location.format == "delta":
        from datafusion_engine.dataset.resolution import apply_scan_unit_overrides
        from datafusion_engine.lineage.scheduling import ScanLineage, plan_scan_unit

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
            dataset_resolver=dataset_resolver,
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
    from datafusion_engine.plan.result_types import (
        PlanExecutionOptions,
        PlanScanOverrides,
    )
    from datafusion_engine.plan.result_types import (
        execute_plan_artifact as execute_plan_artifact_helper,
    )
    from datafusion_engine.session.facade import DataFusionExecutionFacade

    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=runtime_profile)
    bundle = facade.compile_to_bundle(builder)
    execution = execute_plan_artifact_helper(
        ctx,
        bundle,
        options=PlanExecutionOptions(
            runtime_profile=runtime_profile,
            scan=PlanScanOverrides(apply_scan_overrides=False),
        ),
    )
    result = execution.execution_result
    if result.dataframe is None:
        msg = "Worklist execution did not return a DataFusion DataFrame."
        raise ValueError(msg)
    return result.dataframe.execute_stream()


def worklist_queue_name(*, output_table: str, repo_id: str | None) -> str | None:
    """Return a stable queue name for persistent worklists.

    Returns:
    -------
    str | None
        Stable queue name derived from the repo and output table, or ``None``
        when a repo identifier is not available.
    """
    if repo_id is None or not str(repo_id).strip():
        return None
    return stable_cache_label(
        "worklist",
        {"output_table": output_table, "repo_id": repo_id},
    )


def _worklist_queue(
    runtime_profile: DataFusionRuntimeProfile,
    *,
    queue_name: str,
) -> tuple[Deque, Index] | None:
    profile = diskcache_profile_from_ctx(runtime_profile)
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
    from datafusion_engine.session.facade import DataFusionExecutionFacade

    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=runtime_profile)
    facade.register_dataset(name=name, location=location)
    try:
        yield None
    finally:
        deregister = getattr(ctx, "deregister_table", None)
        if callable(deregister):
            with contextlib.suppress(KeyError, RuntimeError, TypeError, ValueError):
                deregister(name)


__all__ = [
    "WorklistRequest",
    "iter_worklist_contexts",
    "worklist_builder",
    "worklist_queue_name",
]
