"""Scan a repository for source files and capture file metadata using shared helpers."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Literal, overload

from diskcache import memoize_stampede, throttle

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.schema.abi import schema_fingerprint
from core_types import PathLike, ensure_path
from datafusion_engine.extract_registry import dataset_query, dataset_schema, normalize_options
from extract.cache_utils import (
    cache_for_kind_optional,
    cache_ttl_seconds,
    diskcache_profile_from_ctx,
    stable_cache_key,
)
from extract.git_authorship import blame_hunks
from extract.git_context import open_git_context
from extract.git_delta import diff_paths
from extract.git_remotes import remote_callbacks_from_env
from extract.git_submodules import submodule_roots, update_submodules, worktree_roots
from extract.helpers import (
    ExtractExecutionContext,
    ExtractMaterializeOptions,
    ExtractPlanOptions,
    extract_plan_from_rows,
    materialize_extract_plan,
)
from extract.repo_scan_fs import iter_repo_files_fs
from extract.repo_scan_pygit2 import iter_repo_files_pygit2
from extract.schema_ops import ExtractNormalizeOptions
from extract.session import ExtractSession
from ibis_engine.plan import IbisPlan
from ibis_engine.query_compiler import IbisQuerySpec
from serde_msgspec import to_builtins

if TYPE_CHECKING:
    from diskcache import Cache, FanoutCache

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
    include_sha256: bool = True
    max_file_bytes: int | None = None
    max_files: int | None = 200_000
    diff_base_ref: str | None = None
    diff_head_ref: str | None = None
    changed_only: bool = False
    include_submodules: bool = False
    include_worktrees: bool = False
    update_submodules: bool = False
    submodule_update_init: bool = True
    submodule_update_depth: int | None = None
    submodule_use_remote_auth: bool = False
    record_blame: bool = False
    blame_max_files: int | None = None
    blame_ref: str | None = None


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


def _diff_filter_paths(
    repo_root: Path,
    *,
    options: RepoScanOptions,
) -> frozenset[str] | None:
    if not options.changed_only:
        return None
    if options.diff_base_ref is None or options.diff_head_ref is None:
        return None
    diff_result = diff_paths(
        repo_root,
        base_ref=options.diff_base_ref,
        head_ref=options.diff_head_ref,
    )
    if diff_result is None:
        return None
    return diff_result.changed_paths


def _iter_repo_root_files(
    root: Path,
    *,
    include_globs: Sequence[str],
    exclude_globs: Sequence[str],
    options: RepoScanOptions,
    diff_filter: frozenset[str] | None,
) -> Iterator[Path]:
    git_files = iter_repo_files_pygit2(
        root,
        include_globs=include_globs,
        exclude_globs=exclude_globs,
        exclude_dirs=options.exclude_dirs,
        follow_symlinks=options.follow_symlinks,
    )
    repo_iter = (
        iter(git_files)
        if git_files is not None
        else iter_repo_files_fs(
            root,
            include_globs=include_globs,
            exclude_globs=exclude_globs,
            exclude_dirs=options.exclude_dirs,
            follow_symlinks=options.follow_symlinks,
        )
    )
    for rel in repo_iter:
        if diff_filter is not None and rel.as_posix() not in diff_filter:
            continue
        yield rel


def _record_if_new(seen: set[str], rel: Path) -> bool:
    rel_posix = rel.as_posix()
    if rel_posix in seen:
        return False
    seen.add(rel_posix)
    return True


def _maybe_update_submodules(repo_root: Path, options: RepoScanOptions) -> None:
    if not options.include_submodules or not options.update_submodules or options.changed_only:
        return
    callbacks = remote_callbacks_from_env() if options.submodule_use_remote_auth else None
    update_submodules(
        repo_root,
        init=options.submodule_update_init,
        depth=options.submodule_update_depth,
        callbacks=callbacks,
    )


@dataclass
class _RepoIterContext:
    include_globs: Sequence[str]
    exclude_globs: Sequence[str]
    options: RepoScanOptions
    seen: set[str]
    diff_filter: frozenset[str] | None


def _iter_prefixed_roots(
    roots: Iterable[tuple[Path, Path]],
    *,
    context: _RepoIterContext,
) -> Iterator[Path]:
    for root, prefix in roots:
        for rel in _iter_repo_root_files(
            root,
            include_globs=context.include_globs,
            exclude_globs=context.exclude_globs,
            options=context.options,
            diff_filter=context.diff_filter,
        ):
            prefixed = prefix / rel if prefix.parts else rel
            if _record_if_new(context.seen, prefixed):
                yield prefixed


def _submodule_prefixes(repo_root: Path, options: RepoScanOptions) -> list[tuple[Path, Path]]:
    if not options.include_submodules or options.changed_only:
        return []
    return [(submodule.repo_root, submodule.prefix) for submodule in submodule_roots(repo_root)]


def _worktree_prefixes(repo_root: Path, options: RepoScanOptions) -> list[tuple[Path, Path]]:
    if not options.include_worktrees:
        return []
    prefixes: list[tuple[Path, Path]] = []
    for worktree in worktree_roots(repo_root):
        if worktree.repo_root == repo_root:
            continue
        prefixes.append((worktree.repo_root, Path(".worktrees") / worktree.name))
    return prefixes


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
    diff_paths_set = _diff_filter_paths(repo_root, options=options)
    _maybe_update_submodules(repo_root, options)
    seen: set[str] = set()
    base_context = _RepoIterContext(
        include_globs=include_globs,
        exclude_globs=exclude_globs,
        options=options,
        seen=seen,
        diff_filter=diff_paths_set,
    )
    yield from _iter_prefixed_roots([(repo_root, Path())], context=base_context)
    if options.include_submodules:
        submodule_context = _RepoIterContext(
            include_globs=include_globs,
            exclude_globs=exclude_globs,
            options=options,
            seen=seen,
            diff_filter=None,
        )
        yield from _iter_prefixed_roots(
            _submodule_prefixes(repo_root, options),
            context=submodule_context,
        )
    if options.include_worktrees:
        worktree_context = _RepoIterContext(
            include_globs=include_globs,
            exclude_globs=exclude_globs,
            options=options,
            seen=seen,
            diff_filter=None,
        )
        yield from _iter_prefixed_roots(
            _worktree_prefixes(repo_root, options),
            context=worktree_context,
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
        empty_plan = extract_plan_from_rows(
            "repo_files_v1",
            [],
            session=session,
            options=ExtractPlanOptions(normalize=normalize),
        )
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
    cache_profile = diskcache_profile_from_ctx(session.exec_ctx)
    cache_options = RepoScanCacheOptions(
        cache=cache_for_kind_optional(cache_profile, "repo_scan"),
        coord_cache=cache_for_kind_optional(cache_profile, "coordination"),
        cache_key=None,
        cache_ttl=cache_ttl_seconds(cache_profile, "repo_scan"),
    )
    cache_options = _with_repo_scan_cache_key(
        cache_options,
        repo_root_path,
        options=options,
    )
    rows = _load_repo_scan_rows(
        repo_root_path,
        options=options,
        cache_options=cache_options,
    )
    _record_repo_blame(
        repo_root_path,
        rows,
        options=options,
        session=session,
    )
    return extract_plan_from_rows(
        "repo_files_v1",
        rows,
        session=session,
        options=ExtractPlanOptions(normalize=normalize),
    )


@dataclass(frozen=True)
class RepoScanCacheOptions:
    """Cache options for repository scanning."""

    cache: Cache | FanoutCache | None
    coord_cache: Cache | FanoutCache | None
    cache_key: str | None
    cache_ttl: float | None


def _with_repo_scan_cache_key(
    cache_options: RepoScanCacheOptions,
    repo_root_path: Path,
    *,
    options: RepoScanOptions,
) -> RepoScanCacheOptions:
    if cache_options.cache is None:
        return cache_options
    cache_key = stable_cache_key(
        "repo_scan",
        {
            "repo_root": str(repo_root_path),
            "schema_fingerprint": schema_fingerprint(dataset_schema("repo_files_v1")),
            "options": to_builtins(options),
        },
    )
    return replace(cache_options, cache_key=cache_key)


def _load_repo_scan_rows(
    repo_root_path: Path,
    *,
    options: RepoScanOptions,
    cache_options: RepoScanCacheOptions,
) -> list[dict[str, object]]:
    compute_rows = _select_row_loader(
        repo_root_path,
        options=options,
        cache_options=cache_options,
    )
    if cache_options.cache is None or cache_options.cache_key is None:
        return compute_rows()

    @memoize_stampede(
        cache_options.cache,
        expire=cache_options.cache_ttl,
        tag=options.repo_id,
        name="repo_scan",
    )
    def _cached_scan(key: str) -> list[dict[str, object]]:
        _ = key
        return compute_rows()

    return _cached_scan(cache_options.cache_key)


def _select_row_loader(
    repo_root_path: Path,
    *,
    options: RepoScanOptions,
    cache_options: RepoScanCacheOptions,
) -> Callable[[], list[dict[str, object]]]:
    def _compute_rows() -> list[dict[str, object]]:
        return list(_iter_repo_scan_rows(repo_root_path, options=options))

    if cache_options.coord_cache is None or cache_options.cache_key is None:
        return _compute_rows

    @throttle(
        cache_options.coord_cache,
        count=1,
        seconds=5,
        name=f"repo_scan:{cache_options.cache_key}",
        expire=cache_options.cache_ttl,
    )
    def _throttled_rows() -> list[dict[str, object]]:
        return _compute_rows()

    return _throttled_rows


def _iter_repo_scan_rows(
    repo_root_path: Path,
    *,
    options: RepoScanOptions,
) -> Iterator[dict[str, object]]:
    count = 0
    for rel in sorted(iter_repo_files(repo_root_path, options), key=lambda p: p.as_posix()):
        row = _build_repo_file_row(rel=rel, repo_root=repo_root_path, options=options)
        if row is None:
            continue
        yield row
        count += 1
        if options.max_files is not None and count >= options.max_files:
            break


@dataclass
class _AuthorStat:
    name: str
    email: str
    lines: int
    files: set[str]


def _record_repo_blame(
    repo_root: Path,
    entries: Iterable[Path | Mapping[str, object]],
    *,
    options: RepoScanOptions,
    session: ExtractSession,
) -> None:
    if not options.record_blame:
        return
    runtime_profile = session.exec_ctx.runtime.datafusion
    if runtime_profile is None or runtime_profile.diagnostics_sink is None:
        return
    git_ctx = open_git_context(repo_root)
    if git_ctx is None:
        return
    paths = _blame_paths(entries, limit=options.blame_max_files)
    if not paths:
        return
    author_stats: dict[str, _AuthorStat] = {}
    for path_posix in paths:
        for hunk in blame_hunks(git_ctx.repo, path_posix=path_posix, ref=options.blame_ref):
            key = hunk.author_email or hunk.author_name
            if not key:
                continue
            stat = author_stats.get(key)
            if stat is None:
                stat = _AuthorStat(
                    name=hunk.author_name,
                    email=hunk.author_email,
                    lines=0,
                    files=set(),
                )
                author_stats[key] = stat
            stat.lines += hunk.lines
            stat.files.add(path_posix)
    payload = {
        "blame_ref": options.blame_ref,
        "total_files": len(paths),
        "authors": [
            {
                "author_name": stat.name,
                "author_email": stat.email,
                "lines": stat.lines,
                "files": len(stat.files),
            }
            for stat in sorted(author_stats.values(), key=lambda item: (-item.lines, item.email))
        ],
    }
    from datafusion_engine.diagnostics import record_artifact

    record_artifact(runtime_profile, "repo_scan_blame_v1", payload)


def _blame_paths(
    entries: Iterable[Path | Mapping[str, object]],
    *,
    limit: int | None,
) -> list[str]:
    paths: list[str] = []
    for entry in entries:
        path = _path_from_entry(entry)
        if not path:
            continue
        paths.append(path)
        if limit is not None and len(paths) >= limit:
            break
    return paths


def _path_from_entry(entry: Path | Mapping[str, object]) -> str | None:
    if isinstance(entry, Path):
        return entry.as_posix()
    path_value = entry.get("path")
    if isinstance(path_value, str):
        return path_value
    return None
