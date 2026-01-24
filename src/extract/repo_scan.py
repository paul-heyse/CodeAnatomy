"""Scan a repository for source files and capture file metadata using shared helpers."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, overload

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from core_types import PathLike, ensure_path
from datafusion_engine.extract_registry import dataset_query, normalize_options
from extract.helpers import (
    ExtractExecutionContext,
    ExtractMaterializeOptions,
    ExtractPlanOptions,
    empty_ibis_plan,
    extract_plan_from_rows,
    materialize_extract_plan,
)
from extract.repo_scan_fs import iter_repo_files_fs
from extract.repo_scan_git import iter_repo_files_git
from extract.schema_ops import ExtractNormalizeOptions
from extract.session import ExtractSession
from ibis_engine.plan import IbisPlan
from ibis_engine.query_compiler import IbisQuerySpec

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class RepoFileFingerprint:
    """Stable fingerprint metadata for cached repo files."""

    size_bytes: int
    mtime_ns: int
    file_sha256: str | None


@dataclass(frozen=True)
class RepoScanOptions:
    """Configure repository scanning behavior."""

    repo_id: str | None = None
    include_globs: Sequence[str] = ("**/*.py",)
    exclude_dirs: Sequence[str] = (
        ".git",
        "__pycache__",
        ".venv",
        "venv",
        "node_modules",
        "dist",
        "build",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
    )
    exclude_globs: Sequence[str] = ()
    follow_symlinks: bool = False
    include_sha256: bool = True
    max_file_bytes: int | None = None
    max_files: int | None = 200_000
    hash_index: Mapping[str, RepoFileFingerprint] | None = None


def default_repo_scan_options() -> RepoScanOptions:
    """Return default RepoScanOptions for repo scanning.

    Returns
    -------
    RepoScanOptions
        Default repo scan options.
    """
    return RepoScanOptions()


def repo_scan_globs_from_options(options: RepoScanOptions) -> tuple[list[str], list[str]]:
    """Return include/exclude globs derived from RepoScanOptions.

    Returns
    -------
    tuple[list[str], list[str]]
        Include and exclude globs.
    """
    include_globs = list(options.include_globs)
    exclude_globs = [f"**/{name}/**" for name in options.exclude_dirs]
    exclude_globs.extend(options.exclude_globs)
    return include_globs, exclude_globs


def repo_files_query(repo_id: str | None) -> IbisQuerySpec:
    """Return the IbisQuerySpec for repo file scanning.

    Returns
    -------
    IbisQuerySpec
        IbisQuerySpec for repo file projection.
    """
    return dataset_query("repo_files_v1", repo_id=repo_id)


def _sha256_path(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def iter_repo_files(repo_root: Path, options: RepoScanOptions) -> Iterator[Path]:
    """Iterate over repo files that match include/exclude rules.

    Parameters
    ----------
    repo_root:
        Repository root path.
    options:
        Scan options.

    Yields
    ------
    pathlib.Path
        Relative paths for matching files.
    """
    repo_root = repo_root.resolve()
    include_globs, exclude_globs = repo_scan_globs_from_options(options)
    git_files = iter_repo_files_git(
        repo_root,
        include_globs=include_globs,
        exclude_globs=exclude_globs,
        exclude_dirs=options.exclude_dirs,
        follow_symlinks=options.follow_symlinks,
    )
    if git_files is not None:
        yield from git_files
        return
    yield from iter_repo_files_fs(
        repo_root,
        include_globs=include_globs,
        exclude_globs=exclude_globs,
        exclude_dirs=options.exclude_dirs,
        follow_symlinks=options.follow_symlinks,
    )


def _build_repo_file_row(
    *,
    rel: Path,
    repo_root: Path,
    options: RepoScanOptions,
) -> dict[str, object] | None:
    abs_path = (repo_root / rel).resolve()
    rel_posix = rel.as_posix()
    try:
        stat = abs_path.stat()
    except OSError:
        return None
    size_bytes = int(stat.st_size)
    if options.max_file_bytes is not None and size_bytes > options.max_file_bytes:
        return None
    mtime_ns = int(stat.st_mtime_ns)
    file_sha256: str | None = None
    if options.include_sha256:
        file_sha256 = _reuse_sha256(rel_posix, size_bytes, mtime_ns, options)
        if file_sha256 is None:
            try:
                file_sha256 = _sha256_path(abs_path)
            except OSError:
                return None

    return {
        "file_id": None,
        "path": rel_posix,
        "abs_path": str(abs_path),
        "size_bytes": size_bytes,
        "mtime_ns": mtime_ns,
        "file_sha256": file_sha256,
    }


def _reuse_sha256(
    path: str,
    size_bytes: int,
    mtime_ns: int,
    options: RepoScanOptions,
) -> str | None:
    if options.hash_index is None:
        return None
    fingerprint = options.hash_index.get(path)
    if fingerprint is None:
        return None
    if fingerprint.size_bytes != size_bytes or fingerprint.mtime_ns != mtime_ns:
        return None
    return fingerprint.file_sha256


def repo_scan_hash_index(repo_files: TableLike) -> dict[str, RepoFileFingerprint]:
    """Build a path keyed hash index from a repo_files table.

    Returns
    -------
    dict[str, RepoFileFingerprint]
        Mapping from repo-relative path to cached hash fingerprint.
    """
    from arrowdsl.core.array_iter import iter_table_rows

    index: dict[str, RepoFileFingerprint] = {}
    for row in iter_table_rows(repo_files):
        path = row.get("path")
        size_bytes = row.get("size_bytes")
        mtime_ns = row.get("mtime_ns")
        file_sha256 = row.get("file_sha256")
        if not isinstance(path, str):
            continue
        if not isinstance(size_bytes, int) or not isinstance(mtime_ns, int):
            continue
        index[path] = RepoFileFingerprint(
            size_bytes=size_bytes,
            mtime_ns=mtime_ns,
            file_sha256=file_sha256 if isinstance(file_sha256, str) else None,
        )
    return index


@overload
def scan_repo(
    repo_root: PathLike,
    options: RepoScanOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
    prefer_reader: Literal[False] = False,
) -> TableLike: ...


@overload
def scan_repo(
    repo_root: PathLike,
    options: RepoScanOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
    prefer_reader: Literal[True],
) -> TableLike | RecordBatchReaderLike: ...


def scan_repo(
    repo_root: PathLike,
    options: RepoScanOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
    prefer_reader: bool = False,
) -> TableLike | RecordBatchReaderLike:
    """Scan the repo for Python files and return a repo_files table.

    Parameters
    ----------
    repo_root:
        Repository root path.
    options:
        Scan options.
    context:
        Optional extract execution context for session and profile resolution.
    prefer_reader:
        When True, return a streaming reader when possible.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Repo file metadata output.
    """
    normalized_options = normalize_options("repo_scan", options, RepoScanOptions)
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    ctx = session.exec_ctx
    normalize = ExtractNormalizeOptions(
        options=normalized_options,
        repo_id=normalized_options.repo_id,
    )
    max_files = normalized_options.max_files
    if max_files is not None and max_files <= 0:
        empty_plan = empty_ibis_plan("repo_files_v1", session=session)
        return materialize_extract_plan(
            "repo_files_v1",
            empty_plan,
            ctx=ctx,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                prefer_reader=prefer_reader,
                apply_post_kernels=True,
            ),
        )

    plan = scan_repo_plan(repo_root, options=normalized_options, session=session)
    return materialize_extract_plan(
        "repo_files_v1",
        plan,
        ctx=ctx,
        options=ExtractMaterializeOptions(
            normalize=normalize,
            prefer_reader=prefer_reader,
            apply_post_kernels=True,
        ),
    )


def scan_repo_plan(
    repo_root: PathLike,
    *,
    options: RepoScanOptions,
    session: ExtractSession,
) -> IbisPlan:
    """Build the plan for repository scanning.

    Returns
    -------
    IbisPlan
        Ibis plan emitting repo file metadata.
    """
    repo_root_path = ensure_path(repo_root).resolve()
    normalize = ExtractNormalizeOptions(options=options, repo_id=options.repo_id)

    def iter_rows() -> Iterator[dict[str, object]]:
        count = 0
        for rel in sorted(iter_repo_files(repo_root_path, options), key=lambda p: p.as_posix()):
            row = _build_repo_file_row(rel=rel, repo_root=repo_root_path, options=options)
            if row is None:
                continue
            yield row
            count += 1
            if options.max_files is not None and count >= options.max_files:
                break

    return extract_plan_from_rows(
        "repo_files_v1",
        iter_rows(),
        session=session,
        options=ExtractPlanOptions(normalize=normalize),
    )
