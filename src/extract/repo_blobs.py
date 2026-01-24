"""Build repo blob tables with bytes/text payloads."""

from __future__ import annotations

import io
import tokenize
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Literal, overload

from arrowdsl.core.array_iter import iter_table_rows
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.schema.serialization import schema_fingerprint
from datafusion_engine.extract_registry import dataset_query, dataset_schema, normalize_options
from extract.cache_utils import (
    cache_for_extract,
    cache_get,
    cache_set,
    cache_ttl_seconds,
    diskcache_profile_from_ctx,
    stable_cache_key,
)
from extract.helpers import (
    ExtractExecutionContext,
    ExtractMaterializeOptions,
    ExtractPlanOptions,
    FileContext,
    bytes_from_file_ctx,
    extract_plan_from_rows,
    materialize_extract_plan,
)
from extract.repo_blobs_git import open_repo_for_path, read_blob_at_ref
from extract.schema_ops import ExtractNormalizeOptions
from extract.session import ExtractSession
from ibis_engine.plan import IbisPlan
from ibis_engine.query_compiler import IbisQuerySpec
from serde_msgspec import to_builtins

if TYPE_CHECKING:
    import pygit2


@dataclass(frozen=True)
class RepoBlobOptions:
    """Configure repo blob extraction behavior."""

    repo_id: str | None = None
    include_bytes: bool = True
    include_text: bool = True
    max_file_bytes: int | None = None
    max_files: int | None = None
    source_ref: str | None = None


def default_repo_blob_options() -> RepoBlobOptions:
    """Return default RepoBlobOptions for repo blob extraction.

    Returns
    -------
    RepoBlobOptions
        Default repo blob options.
    """
    return RepoBlobOptions()


def repo_file_blobs_query(repo_id: str | None) -> IbisQuerySpec:
    """Return the IbisQuerySpec for repo blob extraction.

    Returns
    -------
    IbisQuerySpec
        IbisQuerySpec for repo blob projection.
    """
    return dataset_query("repo_file_blobs_v1", repo_id=repo_id)


def _detect_encoding_and_decode(data: bytes) -> tuple[str, str | None]:
    """Detect encoding using tokenize rules and decode best-effort.

    Parameters
    ----------
    data:
        Raw file bytes.

    Returns
    -------
    tuple[str, str | None]
        Detected encoding and decoded text (if available).
    """
    try:
        encoding, _ = tokenize.detect_encoding(io.BytesIO(data).readline)
    except (LookupError, SyntaxError):
        encoding = "utf-8"
    try:
        text = data.decode(encoding, errors="replace")
    except (LookupError, UnicodeError):
        return encoding, None
    else:
        return encoding, text


def _detect_encoding(data: bytes) -> str:
    try:
        encoding, _ = tokenize.detect_encoding(io.BytesIO(data).readline)
    except (LookupError, SyntaxError):
        return "utf-8"
    return encoding


@cache
def _repo_blob_schema_fingerprint() -> str:
    return schema_fingerprint(dataset_schema("repo_file_blobs_v1"))


def _repo_blob_cache_key(
    file_ctx: FileContext,
    *,
    options: RepoBlobOptions,
) -> str | None:
    if not file_ctx.file_id or file_ctx.file_sha256 is None:
        return None
    return stable_cache_key(
        "repo_blob",
        {
            "file_id": file_ctx.file_id,
            "file_sha256": file_ctx.file_sha256,
            "schema_fingerprint": _repo_blob_schema_fingerprint(),
            "options": to_builtins(options),
        },
    )


def _build_repo_blob_row(
    row: Mapping[str, object],
    *,
    options: RepoBlobOptions,
    data_override: bytes | None,
) -> dict[str, object] | None:
    file_ctx = FileContext.from_repo_row(row)
    if not file_ctx.file_id or not file_ctx.path:
        return None
    if not options.include_bytes and not options.include_text:
        return None
    data = data_override
    if data is None and (options.include_bytes or options.include_text):
        data = bytes_from_file_ctx(file_ctx)
    if data is None:
        return None
    if options.max_file_bytes is not None and len(data) > options.max_file_bytes:
        return None
    size_bytes = row.get("size_bytes")
    mtime_ns = row.get("mtime_ns")
    encoding = None
    text: str | None = None
    if options.include_text:
        encoding, text = _detect_encoding_and_decode(data)
    elif options.include_bytes:
        encoding = _detect_encoding(data)
    return {
        "file_id": file_ctx.file_id,
        "path": file_ctx.path,
        "file_sha256": file_ctx.file_sha256,
        "abs_path": file_ctx.abs_path,
        "size_bytes": size_bytes if isinstance(size_bytes, int) else len(data),
        "mtime_ns": mtime_ns if isinstance(mtime_ns, int) else None,
        "encoding": encoding,
        "text": text if options.include_text else None,
        "bytes": data if options.include_bytes else None,
    }


@overload
def scan_repo_blobs(
    repo_files: TableLike,
    options: RepoBlobOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
    prefer_reader: Literal[False] = False,
) -> TableLike: ...


@overload
def scan_repo_blobs(
    repo_files: TableLike,
    options: RepoBlobOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
    prefer_reader: Literal[True],
) -> TableLike | RecordBatchReaderLike: ...


def scan_repo_blobs(
    repo_files: TableLike,
    options: RepoBlobOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
    prefer_reader: bool = False,
) -> TableLike | RecordBatchReaderLike:
    """Read repo file blobs and return a repo_file_blobs table.

    Parameters
    ----------
    repo_files:
        Repo manifest table.
    options:
        Blob extraction options.
    context:
        Optional extract execution context for session and profile resolution.
    prefer_reader:
        When True, return a streaming reader when possible.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Repo file blob output.
    """
    normalized_options = normalize_options("repo_blobs", options, RepoBlobOptions)
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    ctx = session.exec_ctx
    normalize = ExtractNormalizeOptions(
        options=normalized_options,
        repo_id=normalized_options.repo_id,
    )
    if not normalized_options.include_bytes and not normalized_options.include_text:
        empty_plan = extract_plan_from_rows(
            "repo_file_blobs_v1",
            [],
            session=session,
            options=ExtractPlanOptions(normalize=normalize),
        )
        return materialize_extract_plan(
            "repo_file_blobs_v1",
            empty_plan,
            ctx=ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                prefer_reader=prefer_reader,
                apply_post_kernels=True,
            ),
        )
    plan = scan_repo_blobs_plan(repo_files, options=normalized_options, session=session)
    return materialize_extract_plan(
        "repo_file_blobs_v1",
        plan,
        ctx=ctx,
        options=ExtractMaterializeOptions(
            normalize=normalize,
            prefer_reader=prefer_reader,
            apply_post_kernels=True,
        ),
    )


def scan_repo_blobs_plan(
    repo_files: TableLike,
    *,
    options: RepoBlobOptions,
    session: ExtractSession,
) -> IbisPlan:
    """Build the plan for repo blob extraction.

    Returns
    -------
    IbisPlan
        Ibis plan emitting repo file blobs.
    """
    normalize = ExtractNormalizeOptions(options=options, repo_id=options.repo_id)

    cache_profile = diskcache_profile_from_ctx(session.exec_ctx)
    cache = cache_for_extract(cache_profile)
    cache_ttl = cache_ttl_seconds(cache_profile, "extract")

    def iter_rows() -> Iterator[dict[str, object]]:
        count = 0
        repo: pygit2.Repository | None = None
        for row in iter_table_rows(repo_files):
            file_ctx = FileContext.from_repo_row(row)
            cache_key = _repo_blob_cache_key(file_ctx, options=options)
            if cache is not None and cache_key is not None:
                cached = cache_get(cache, key=cache_key, default=None)
                if isinstance(cached, dict):
                    yield cached
                    count += 1
                    if options.max_files is not None and count >= options.max_files:
                        break
                    continue
            data_override: bytes | None = None
            if options.source_ref:
                if file_ctx.abs_path and file_ctx.path:
                    if repo is None:
                        repo = open_repo_for_path(Path(file_ctx.abs_path))
                    if repo is not None:
                        data_override = read_blob_at_ref(
                            repo,
                            ref=options.source_ref,
                            path_posix=file_ctx.path,
                        )
            blob_row = _build_repo_blob_row(
                row,
                options=options,
                data_override=data_override,
            )
            if blob_row is None:
                continue
            if cache is not None and cache_key is not None:
                cache_set(
                    cache,
                    key=cache_key,
                    value=blob_row,
                    expire=cache_ttl,
                    tag=options.repo_id,
                    read=options.include_bytes,
                )
            yield blob_row
            count += 1
            if options.max_files is not None and count >= options.max_files:
                break

    return extract_plan_from_rows(
        "repo_file_blobs_v1",
        iter_rows(),
        session=session,
        options=ExtractPlanOptions(normalize=normalize),
    )


__all__ = [
    "RepoBlobOptions",
    "default_repo_blob_options",
    "repo_file_blobs_query",
    "scan_repo_blobs",
    "scan_repo_blobs_plan",
]
