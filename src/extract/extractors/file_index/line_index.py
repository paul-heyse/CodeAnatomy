"""Extract line index from source files for byte offset conversion.

This module provides line-level byte offset mappings needed by the SCIP
normalization layer to convert line/column positions to canonical byte spans.

The line index is derived from file blob content and produces one row per line,
capturing line boundaries and newline conventions.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Literal, overload

import pyarrow as pa

from arrow_utils.core.array_iter import iter_table_rows
from datafusion_engine.arrow.interop import RecordBatchReaderLike, TableLike
from datafusion_engine.expr.query_spec import QuerySpec
from datafusion_engine.extract.registry import dataset_query, normalize_options
from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
from extract.coordination.context import (
    ExtractExecutionContext,
    FileContext,
)
from extract.coordination.materialization import (
    ExtractMaterializeOptions,
    ExtractPlanOptions,
    extract_plan_from_rows,
    materialize_extract_plan,
)
from extract.coordination.schema_ops import ExtractNormalizeOptions
from extract.infrastructure.cache_utils import (
    CacheSetOptions,
    cache_for_extract,
    cache_get,
    cache_set,
    cache_ttl_seconds,
    diskcache_profile_from_ctx,
    stable_cache_key,
)
from extract.infrastructure.schema_cache import file_line_index_fingerprint
from extract.session import ExtractSession

if TYPE_CHECKING:
    from diskcache import Cache, FanoutCache


# Constants for newline detection
_CRLF_BYTES = 2

FILE_LINE_INDEX_SCHEMA = pa.schema(
    [
        pa.field("file_id", pa.string(), nullable=False),
        pa.field("path", pa.string(), nullable=False),
        pa.field("line_no", pa.int64(), nullable=False),
        pa.field("line_start_byte", pa.int64(), nullable=False),
        pa.field("line_end_byte", pa.int64(), nullable=False),
        pa.field("line_text", pa.string(), nullable=True),
        pa.field("newline_kind", pa.string(), nullable=False),
    ]
)


@dataclass(frozen=True)
class LineIndexOptions:
    """Configure line index extraction behavior."""

    repo_id: str | None = None
    max_files: int | None = None
    include_text: bool = True
    max_line_text_bytes: int = 10_000


@dataclass(frozen=True)
class _LineIndexConfig:
    """Internal configuration for line extraction."""

    file_id: str
    path: str
    encoding: str = "utf-8"
    include_text: bool = True
    max_line_text_bytes: int = 10_000


def default_line_index_options() -> LineIndexOptions:
    """Return default LineIndexOptions for line index extraction.

    Returns:
    -------
    LineIndexOptions
        Default line index options.
    """
    return LineIndexOptions()


def file_line_index_query(repo_id: str | None) -> QuerySpec:
    """Return the QuerySpec for line index extraction.

    Returns:
    -------
    QuerySpec
        QuerySpec for line index projection.
    """
    return dataset_query("file_line_index_v1", repo_id=repo_id)


def extract_file_line_index(file_path: Path, config: _LineIndexConfig) -> pa.Table:
    """Extract line index from a source file.

    Build a table with one row per line containing line boundaries
    and text content. Used for SCIP byte offset conversion.

    Parameters
    ----------
    file_path
        Absolute path to the source file.
    config
        Extraction configuration with file identity and options.

    Returns:
    -------
    pa.Table
        Line index table with columns: file_id, path, line_no,
        line_start_byte, line_end_byte, line_text, newline_kind.
    """
    content = file_path.read_bytes()
    rows = list(extract_line_index_rows(content, config=config))
    return pa.Table.from_pylist(rows, schema=FILE_LINE_INDEX_SCHEMA)


def extract_line_index_rows(
    content: bytes,
    *,
    config: _LineIndexConfig,
) -> Iterator[dict[str, object]]:
    """Yield line index rows from file content.

    Parameters
    ----------
    content
        Raw file bytes.
    config
        Extraction configuration with file identity and options.

    Yields:
    ------
    dict[str, object]
        Line index row dictionaries.
    """
    byte_offset = 0
    line_no = 0
    start = 0
    size = len(content)

    while start < size:
        # Find the next newline character
        newline_idx = content.find(b"\n", start)

        if newline_idx == -1:
            # No more newlines - rest of content is final line
            line_bytes = content[start:]
            newline_kind = "none"
            end = size
            next_start = size
            line_text_bytes = line_bytes
        else:
            # Found newline at newline_idx
            end = newline_idx + 1
            line_bytes = content[start:end]

            # Check for CRLF vs LF
            if end >= _CRLF_BYTES and content[end - _CRLF_BYTES : end] == b"\r\n":
                newline_kind = "crlf"
                line_text_bytes = line_bytes[:-_CRLF_BYTES]
            else:
                newline_kind = "lf"
                line_text_bytes = line_bytes[:-1]

            next_start = end

        # Truncate if too long and decode
        if len(line_text_bytes) > config.max_line_text_bytes:
            line_text_bytes = line_text_bytes[: config.max_line_text_bytes]

        line_text: str | None = None
        if config.include_text:
            line_text = line_text_bytes.decode(config.encoding, errors="replace")

        yield {
            "file_id": config.file_id,
            "path": config.path,
            "line_no": line_no,
            "line_start_byte": byte_offset,
            "line_end_byte": byte_offset + len(line_bytes),
            "line_text": line_text,
            "newline_kind": newline_kind,
        }

        byte_offset += len(line_bytes)
        line_no += 1
        start = next_start


def _line_index_cache_key(
    file_ctx: FileContext,
    *,
    options: LineIndexOptions,
) -> str | None:
    """Compute stable cache key for line index extraction.

    Returns:
    -------
    str | None
        Cache key when identity fields are available.
    """
    if not file_ctx.file_id or file_ctx.file_sha256 is None:
        return None
    return stable_cache_key(
        "line_index",
        {
            "file_id": file_ctx.file_id,
            "file_sha256": file_ctx.file_sha256,
            "schema_identity_hash": file_line_index_fingerprint(),
            "include_text": options.include_text,
            "max_line_text_bytes": options.max_line_text_bytes,
        },
    )


def _get_file_content(file_ctx: FileContext) -> bytes | None:
    """Resolve file content from context or filesystem.

    Returns:
    -------
    bytes | None
        File content when available.
    """
    # Try pre-loaded data first
    if file_ctx.data is not None:
        return file_ctx.data

    # Try text content (encode back to bytes)
    if file_ctx.text is not None:
        encoding = file_ctx.encoding or "utf-8"
        try:
            return file_ctx.text.encode(encoding)
        except (LookupError, UnicodeError):
            return file_ctx.text.encode("utf-8", errors="replace")

    # Fall back to reading from filesystem
    if file_ctx.abs_path is not None:
        path = Path(file_ctx.abs_path)
        if path.is_file():
            try:
                return path.read_bytes()
            except OSError:
                return None

    return None


def _build_line_index_rows(
    row: Mapping[str, object],
    *,
    options: LineIndexOptions,
) -> list[dict[str, object]] | None:
    """Build line index rows from a repo file row.

    Returns:
    -------
    list[dict[str, object]] | None
        Line index rows for the file, or None if unavailable.
    """
    file_ctx = FileContext.from_repo_row(row)
    if not file_ctx.file_id or not file_ctx.path:
        return None

    content = _get_file_content(file_ctx)
    if content is None:
        return None

    config = _LineIndexConfig(
        file_id=file_ctx.file_id,
        path=file_ctx.path,
        encoding=file_ctx.encoding or "utf-8",
        include_text=options.include_text,
        max_line_text_bytes=options.max_line_text_bytes,
    )
    return list(extract_line_index_rows(content, config=config))


@overload
def scan_file_line_index(
    repo_files: TableLike,
    options: LineIndexOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
    prefer_reader: Literal[False] = False,
) -> TableLike: ...


@overload
def scan_file_line_index(
    repo_files: TableLike,
    options: LineIndexOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
    prefer_reader: Literal[True],
) -> TableLike | RecordBatchReaderLike: ...


def scan_file_line_index(
    repo_files: TableLike,
    options: LineIndexOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
    prefer_reader: bool = False,
) -> TableLike | RecordBatchReaderLike:
    """Extract line index from repo files and return a file_line_index table.

    Parameters
    ----------
    repo_files
        Repo manifest table or file blobs table with file content.
    options
        Line index extraction options.
    context
        Optional extract execution context for session and profile resolution.
    prefer_reader
        When True, return a streaming reader when possible.

    Returns:
    -------
    TableLike | RecordBatchReaderLike
        Line index output table.
    """
    normalized_options = normalize_options("file_line_index_v1", options, LineIndexOptions)
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    determinism_tier = exec_context.determinism_tier()
    normalize = ExtractNormalizeOptions(
        options=normalized_options,
        repo_id=normalized_options.repo_id,
    )

    plan = scan_file_line_index_plan(repo_files, options=normalized_options, session=session)
    return materialize_extract_plan(
        "file_line_index_v1",
        plan,
        runtime_profile=runtime_profile,
        determinism_tier=determinism_tier,
        options=ExtractMaterializeOptions(
            normalize=normalize,
            prefer_reader=prefer_reader,
            apply_post_kernels=True,
        ),
    )


def scan_file_line_index_plan(
    repo_files: TableLike,
    *,
    options: LineIndexOptions,
    session: ExtractSession,
) -> DataFusionPlanArtifact:
    """Build the plan for line index extraction.

    Returns:
    -------
    DataFusionPlanArtifact
        DataFusion plan bundle emitting line index rows.
    """
    normalize = ExtractNormalizeOptions(options=options, repo_id=options.repo_id)

    cache_profile = diskcache_profile_from_ctx(session.engine_session.datafusion_profile)
    extract_cache = cache_for_extract(cache_profile)
    cache_ttl = cache_ttl_seconds(cache_profile, "extract")

    def iter_rows() -> Iterator[dict[str, object]]:
        yield from _iter_line_index_rows(
            repo_files,
            options=options,
            cache=extract_cache,
            cache_ttl=cache_ttl,
        )

    return extract_plan_from_rows(
        "file_line_index_v1",
        iter_rows(),
        session=session,
        options=ExtractPlanOptions(normalize=normalize),
    )


def _iter_line_index_rows(
    repo_files: TableLike,
    *,
    options: LineIndexOptions,
    cache: Cache | FanoutCache | None,
    cache_ttl: float | None,
) -> Iterator[dict[str, object]]:
    """Yield line index rows from repo files with caching.

    Yields:
    ------
    dict[str, object]
        Line index row dictionaries.
    """
    count = 0
    for row in iter_table_rows(repo_files):
        if options.max_files is not None and count >= options.max_files:
            break

        file_ctx = FileContext.from_repo_row(row)
        cache_key = _line_index_cache_key(file_ctx, options=options)

        # Check cache
        cached = _load_cached_line_rows(cache, cache_key)
        if cached is not None:
            yield from cached
            count += 1
            continue

        # Build rows
        line_rows = _build_line_index_rows(row, options=options)
        if line_rows is None:
            continue

        # Store in cache
        _store_cached_line_rows(
            cache,
            cache_key=cache_key,
            line_rows=line_rows,
            options=options,
            cache_ttl=cache_ttl,
        )

        yield from line_rows
        count += 1


def _load_cached_line_rows(
    cache: Cache | FanoutCache | None,
    cache_key: str | None,
) -> list[dict[str, object]] | None:
    """Load cached line index rows.

    Returns:
    -------
    list[dict[str, object]] | None
        Cached rows when available.
    """
    if cache is None or cache_key is None:
        return None
    cached = cache_get(cache, key=cache_key, default=None)
    if isinstance(cached, list):
        return cached
    return None


def _store_cached_line_rows(
    cache: Cache | FanoutCache | None,
    *,
    cache_key: str | None,
    line_rows: list[dict[str, object]],
    options: LineIndexOptions,
    cache_ttl: float | None,
) -> None:
    """Store line index rows in cache."""
    if cache is None or cache_key is None:
        return
    cache_set(
        cache,
        key=cache_key,
        value=line_rows,
        options=CacheSetOptions(
            expire=cache_ttl,
            tag=options.repo_id,
            read=False,
        ),
    )


__all__ = [
    "FILE_LINE_INDEX_SCHEMA",
    "LineIndexOptions",
    "default_line_index_options",
    "extract_file_line_index",
    "extract_line_index_rows",
    "file_line_index_query",
    "scan_file_line_index",
    "scan_file_line_index_plan",
]
