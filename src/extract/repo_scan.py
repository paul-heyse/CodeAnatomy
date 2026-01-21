"""Scan a repository for source files and capture file metadata using shared helpers."""

from __future__ import annotations

import fnmatch
import hashlib
import io
import tokenize
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, overload

from arrowdsl.core.execution_context import ExecutionContext, execution_context_factory
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from core_types import PathLike, ensure_path
from datafusion_engine.extract_registry import dataset_query, normalize_options
from extract.helpers import (
    ExtractMaterializeOptions,
    apply_query_and_project,
    empty_ibis_plan,
    ibis_plan_from_rows,
    materialize_extract_plan,
)
from extract.schema_ops import ExtractNormalizeOptions
from ibis_engine.plan import IbisPlan
from ibis_engine.query_compiler import IbisQuerySpec

SCHEMA_VERSION = 1


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
    include_bytes: bool = True
    include_text: bool = True
    max_file_bytes: int | None = None
    max_files: int | None = 200_000


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


def _is_excluded_dir(rel_path: Path, exclude_dirs: Sequence[str]) -> bool:
    parts = set(rel_path.parts)
    return any(d in parts for d in exclude_dirs)


def _matches_any_glob(path_posix: str, globs: Sequence[str]) -> bool:
    return any(fnmatch.fnmatch(path_posix, g) for g in globs)


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


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
    seen: set[str] = set()

    for pat in options.include_globs:
        for path in repo_root.glob(pat):
            if path.is_dir():
                continue
            if not options.follow_symlinks and path.is_symlink():
                continue

            rel = path.relative_to(repo_root)
            if _is_excluded_dir(rel, options.exclude_dirs):
                continue

            rel_posix = rel.as_posix()
            if options.exclude_globs and _matches_any_glob(rel_posix, options.exclude_globs):
                continue

            if rel_posix not in seen:
                seen.add(rel_posix)
                yield rel


def _build_repo_file_row(
    *,
    rel: Path,
    repo_root: Path,
    options: RepoScanOptions,
) -> dict[str, object] | None:
    abs_path = (repo_root / rel).resolve()
    rel_posix = rel.as_posix()

    try:
        data = abs_path.read_bytes()
    except OSError:
        return None

    if options.max_file_bytes is not None and len(data) > options.max_file_bytes:
        return None

    file_sha256 = _sha256_hex(data)
    encoding = "utf-8"
    text: str | None = None
    if options.include_text:
        encoding, text = _detect_encoding_and_decode(data)

    return {
        "file_id": None,
        "path": rel_posix,
        "abs_path": str(abs_path),
        "size_bytes": len(data),
        "file_sha256": file_sha256,
        "encoding": encoding,
        "text": text,
        "bytes": data if options.include_bytes else None,
    }


@overload
def scan_repo(
    repo_root: PathLike,
    options: RepoScanOptions | None = None,
    ctx: ExecutionContext | None = None,
    profile: str = "default",
    *,
    prefer_reader: Literal[False] = False,
) -> TableLike: ...


@overload
def scan_repo(
    repo_root: PathLike,
    options: RepoScanOptions | None = None,
    ctx: ExecutionContext | None = None,
    profile: str = "default",
    *,
    prefer_reader: Literal[True],
) -> TableLike | RecordBatchReaderLike: ...


def scan_repo(
    repo_root: PathLike,
    options: RepoScanOptions | None = None,
    ctx: ExecutionContext | None = None,
    profile: str = "default",
    *,
    prefer_reader: bool = False,
) -> TableLike | RecordBatchReaderLike:
    """Scan the repo for Python files and return a repo_files table.

    Parameters
    ----------
    repo_root:
        Repository root path.
    options:
        Scan options.
    ctx:
        Execution context for plan execution.
    profile:
        Execution profile name used when ``ctx`` is not provided.
    prefer_reader:
        When True, return a streaming reader when possible.

    Returns
    -------
    TableLike | RecordBatchReaderLike
        Repo file metadata output.
    """
    normalized_options = normalize_options("repo_scan", options, RepoScanOptions)
    ctx = ctx or execution_context_factory(profile)
    normalize = ExtractNormalizeOptions(
        options=normalized_options,
        repo_id=normalized_options.repo_id,
    )
    max_files = normalized_options.max_files
    if max_files is not None and max_files <= 0:
        empty_plan = empty_ibis_plan("repo_files_v1")
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

    plan = scan_repo_plan(repo_root, options=normalized_options)
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

    raw_plan = ibis_plan_from_rows("repo_files_v1", iter_rows())
    return apply_query_and_project(
        "repo_files_v1",
        raw_plan.expr,
        normalize=normalize,
        evidence_plan=None,
        repo_id=normalize.repo_id,
    )
