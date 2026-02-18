"""Shared runtime loop helpers for extractor orchestration."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from datafusion_engine.arrow.interop import TableLike
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from extract.coordination.context import FileContext
from extract.infrastructure.parallel import parallel_map, resolve_max_workers
from extract.infrastructure.worklists import (
    WorklistRequest,
    iter_worklist_contexts,
    worklist_queue_name,
)

if TYPE_CHECKING:
    from extract.scanning.scope_manifest import ScopeManifest


@dataclass(frozen=True)
class ExtractionRuntimeWorklist:
    """Worklist inputs for shared extraction runtime loops."""

    repo_files: TableLike
    output_table: str
    runtime_profile: DataFusionRuntimeProfile | None
    file_contexts: Iterable[FileContext] | None
    scope_manifest: ScopeManifest | None
    use_worklist_queue: bool
    repo_id: str | None


def runtime_worklist_contexts(request: ExtractionRuntimeWorklist) -> list[FileContext]:
    """Resolve extraction contexts from the configured worklist source.

    Returns:
    -------
    list[FileContext]
        Ordered extraction contexts yielded by worklist resolution.
    """
    queue_name = (
        worklist_queue_name(output_table=request.output_table, repo_id=request.repo_id)
        if request.use_worklist_queue
        else None
    )
    return list(
        iter_worklist_contexts(
            WorklistRequest(
                repo_files=request.repo_files,
                output_table=request.output_table,
                runtime_profile=request.runtime_profile,
                file_contexts=request.file_contexts,
                queue_name=queue_name,
                scope_manifest=request.scope_manifest,
            )
        )
    )


def iter_runtime_rows[RowT](
    contexts: Sequence[FileContext],
    *,
    worker: Callable[[FileContext], RowT | None],
    parallel: bool,
    max_workers: int | None,
) -> Iterator[RowT]:
    """Iterate extractor rows across contexts using shared parallel policy.

    Yields:
        RowT: Non-null rows produced by ``worker`` across all contexts.
    """
    if not parallel:
        for file_ctx in contexts:
            row = worker(file_ctx)
            if row is not None:
                yield row
        return
    resolved_workers = resolve_max_workers(max_workers, kind="cpu")
    for row in parallel_map(contexts, worker, max_workers=resolved_workers):
        if row is not None:
            yield row


def collect_runtime_rows[RowT](
    contexts: Sequence[FileContext],
    *,
    worker: Callable[[FileContext], RowT | None],
    parallel: bool,
    max_workers: int | None,
) -> list[RowT]:
    """Collect extractor rows across contexts into a list.

    Returns:
    -------
    list[RowT]
        Materialized row collection from ``iter_runtime_rows``.
    """
    return list(
        iter_runtime_rows(
            contexts,
            worker=worker,
            parallel=parallel,
            max_workers=max_workers,
        )
    )


def iter_runtime_row_batches[RowT](
    rows: Iterable[RowT],
    *,
    batch_size: int,
) -> Iterator[list[RowT]]:
    """Group row iterables into deterministic fixed-size batches.

    Yields:
        list[RowT]: Consecutive row batches of size ``batch_size`` (final batch may be smaller).

    Raises:
        ValueError: If ``batch_size`` is not a positive integer.
    """
    if batch_size <= 0:
        msg = "batch_size must be a positive integer."
        raise ValueError(msg)
    batch: list[RowT] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


__all__ = [
    "ExtractionRuntimeWorklist",
    "collect_runtime_rows",
    "iter_runtime_row_batches",
    "iter_runtime_rows",
    "runtime_worklist_contexts",
]
