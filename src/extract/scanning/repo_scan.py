"""Scan a repository for source files and capture file metadata using shared helpers."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast, overload

from diskcache import memoize_stampede, throttle

from core_types import PathLike, ensure_path
from datafusion_engine.arrow.interop import RecordBatchReaderLike, TableLike
from datafusion_engine.expr.query_spec import QuerySpec
from datafusion_engine.extract.registry import dataset_query, normalize_options
from datafusion_engine.hashing import stable_id
from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
from extract.coordination.context import ExtractExecutionContext
from extract.coordination.materialization import (
    ExtractMaterializeOptions,
    ExtractPlanOptions,
    extract_plan_from_rows,
    materialize_extract_plan,
)
from extract.coordination.schema_ops import ExtractNormalizeOptions
from extract.git.history import blame_hunks, diff_paths
from extract.git.pygit2_scan import repo_status_paths
from extract.git.remotes import remote_callbacks_from_env
from extract.git.submodules import submodule_roots, update_submodules, worktree_roots
from extract.infrastructure.cache_utils import (
    cache_for_kind_optional,
    cache_ttl_seconds,
    diskcache_profile_from_ctx,
    stable_cache_key,
)
from extract.infrastructure.options import RepoOptions
from extract.infrastructure.schema_cache import repo_files_fingerprint
from extract.scanning.repo_scope import (
    RepoScope,
    RepoScopeOptions,
    default_repo_scope_options,
    resolve_repo_scope,
)
from extract.scanning.scope_manifest import (
    ScopeManifest,
    ScopeManifestOptions,
    build_scope_manifest,
)
from extract.scanning.scope_rules import ScopeRuleSet, build_scope_rules, explain_scope_paths
from extract.session import ExtractSession
from serde_msgspec import to_builtins
from utils.file_io import detect_encoding
from utils.hashing import hash_file_sha256

if TYPE_CHECKING:
    from diskcache import Cache, FanoutCache


SCHEMA_VERSION = 2


@dataclass(frozen=True)
class RepoScanOptions(RepoOptions):
    """Configure repository scanning behavior."""

    scope_policy: RepoScopeOptions | Mapping[str, object] = field(
        default_factory=default_repo_scope_options
    )
    include_sha256: bool = True
    include_encoding: bool = True
    encoding_sample_bytes: int = 8192
    max_file_bytes: int | None = None
    max_files: int | None = 200_000
    diff_base_ref: str | None = None
    diff_head_ref: str | None = None
    changed_only: bool = False
    update_submodules: bool = False
    submodule_update_init: bool = True
    submodule_update_depth: int | None = None
    submodule_use_remote_auth: bool = False
    record_blame: bool = False
    blame_max_files: int | None = None
    blame_ref: str | None = None
    record_pathspec_trace: bool = False
    pathspec_trace_limit: int | None = 200
    pathspec_trace_pattern_limit: int | None = 50


@dataclass(frozen=True)
class RepoScanBundle:
    """Bundle of repo scan outputs."""

    repo_rows: tuple[dict[str, object], ...]
    scope_manifest_rows: tuple[dict[str, str | None], ...]
    python_extension_rows: tuple[dict[str, str], ...]
    scope_hash: str


def default_repo_scan_options() -> RepoScanOptions:
    """Return default RepoScanOptions for repo scanning.

    Returns:
    -------
    RepoScanOptions
        Default repo scan options.
    """
    return RepoScanOptions()


def repo_files_query(repo_id: str | None) -> QuerySpec:
    """Return the QuerySpec for repo file scanning.

    Returns:
    -------
    QuerySpec
        QuerySpec for repo file projection.
    """
    return dataset_query("repo_files_v1", repo_id=repo_id)


def _sha256_path(path: Path) -> str:
    return hash_file_sha256(path)


def _detect_file_encoding(path: Path, *, sample_bytes: int, default: str = "utf-8") -> str | None:
    if sample_bytes <= 0:
        return default
    try:
        with path.open("rb") as handle:
            data = handle.read(sample_bytes)
    except OSError:
        return None
    return detect_encoding(data, default=default)


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


def _resolve_scope_policy(options: RepoScanOptions) -> RepoScopeOptions:
    policy = options.scope_policy
    if isinstance(policy, RepoScopeOptions):
        return policy
    if policy is None:
        return default_repo_scope_options()
    if isinstance(policy, Mapping):
        return _parse_scope_policy(policy)
    msg = "scope_policy must be RepoScopeOptions or a mapping"
    raise TypeError(msg)


def _parse_scope_policy(values: Mapping[str, object]) -> RepoScopeOptions:
    from extract.python.scope import PythonScopePolicy

    python_scope = values.get("python_scope")
    if python_scope is None:
        python_scope_value = PythonScopePolicy()
    elif isinstance(python_scope, Mapping):
        python_scope_value = PythonScopePolicy(**dict(python_scope))
    elif isinstance(python_scope, PythonScopePolicy):
        python_scope_value = python_scope
    else:
        python_scope_value = PythonScopePolicy()
    include_globs = values.get("include_globs")
    if include_globs is None:
        include_globs_value = ()
    elif isinstance(include_globs, str):
        include_globs_value = (include_globs,)
    elif isinstance(include_globs, Sequence) and not isinstance(
        include_globs, (str, bytes, bytearray)
    ):
        include_globs_value = tuple(str(item) for item in include_globs)
    else:
        include_globs_value = ()
    exclude_globs = values.get("exclude_globs")
    if exclude_globs is None:
        exclude_globs_value = ()
    elif isinstance(exclude_globs, str):
        exclude_globs_value = (exclude_globs,)
    elif isinstance(exclude_globs, Sequence) and not isinstance(
        exclude_globs, (str, bytes, bytearray)
    ):
        exclude_globs_value = tuple(str(item) for item in exclude_globs)
    else:
        exclude_globs_value = ()
    include_untracked = values.get("include_untracked")
    include_untracked_value = bool(include_untracked) if include_untracked is not None else True
    include_submodules = values.get("include_submodules")
    include_submodules_value = bool(include_submodules) if include_submodules is not None else False
    include_worktrees = values.get("include_worktrees")
    include_worktrees_value = bool(include_worktrees) if include_worktrees is not None else False
    follow_symlinks = values.get("follow_symlinks")
    follow_symlinks_value = bool(follow_symlinks) if follow_symlinks is not None else False
    return RepoScopeOptions(
        python_scope=python_scope_value,
        include_globs=include_globs_value,
        exclude_globs=exclude_globs_value,
        include_untracked=include_untracked_value,
        include_submodules=include_submodules_value,
        include_worktrees=include_worktrees_value,
        follow_symlinks=follow_symlinks_value,
    )


def _maybe_update_submodules(repo_root: Path, options: RepoScanOptions) -> None:
    scope_policy = _resolve_scope_policy(options)
    if not scope_policy.include_submodules or not options.update_submodules or options.changed_only:
        return
    callbacks = remote_callbacks_from_env() if options.submodule_use_remote_auth else None
    update_submodules(
        repo_root,
        init=options.submodule_update_init,
        depth=options.submodule_update_depth,
        callbacks=callbacks,
    )


@dataclass(frozen=True)
class _ScopedRoot:
    scope: RepoScope
    prefix: Path
    diff_filter: frozenset[str] | None
    rules: ScopeRuleSet
    status_flags: Mapping[str, int]


def _build_scope_rules(scope: RepoScope, options: RepoScopeOptions) -> ScopeRuleSet:
    include_lines, exclude_lines = _scope_rule_lines(scope, options)
    return build_scope_rules(include_lines=include_lines, exclude_lines=exclude_lines)


def _build_scoped_root(
    scope: RepoScope,
    *,
    prefix: Path,
    diff_filter: frozenset[str] | None,
    rules: ScopeRuleSet,
) -> _ScopedRoot:
    return _ScopedRoot(
        scope=scope,
        prefix=prefix,
        diff_filter=diff_filter,
        rules=rules,
        status_flags=repo_status_paths(scope),
    )


def _candidate_paths_for_root(scoped_root: _ScopedRoot) -> list[str]:
    paths: set[str] = set(scoped_root.status_flags)
    paths.update(entry.path for entry in scoped_root.scope.repo.index)
    return sorted(paths)


def _scoped_roots(repo_root: Path, *, options: RepoScanOptions) -> list[_ScopedRoot]:
    scope_policy = _resolve_scope_policy(options)
    base_scope = resolve_repo_scope(repo_root, scope_policy)
    rules = _build_scope_rules(base_scope, scope_policy)
    diff_filter = _diff_filter_paths(base_scope.repo_root, options=options)
    scopes: list[_ScopedRoot] = [
        _build_scoped_root(
            base_scope,
            prefix=Path(),
            diff_filter=diff_filter,
            rules=rules,
        )
    ]
    if scope_policy.include_submodules:
        sub_policy = replace(scope_policy, include_submodules=False, include_worktrees=False)
        for submodule in submodule_roots(base_scope.repo_root):
            sub_scope = resolve_repo_scope(submodule.repo_root, sub_policy)
            scopes.append(
                _build_scoped_root(
                    sub_scope,
                    prefix=submodule.prefix,
                    diff_filter=None,
                    rules=_build_scope_rules(sub_scope, sub_policy),
                )
            )
    if scope_policy.include_worktrees:
        worktree_policy = replace(scope_policy, include_submodules=False, include_worktrees=False)
        for worktree in worktree_roots(base_scope.repo_root):
            if worktree.repo_root == base_scope.repo_root:
                continue
            work_scope = resolve_repo_scope(worktree.repo_root, worktree_policy)
            scopes.append(
                _build_scoped_root(
                    work_scope,
                    prefix=Path(".worktrees") / worktree.name,
                    diff_filter=None,
                    rules=_build_scope_rules(work_scope, worktree_policy),
                )
            )
    return scopes


def _scope_rule_lines(
    scope: RepoScope, scope_policy: RepoScopeOptions
) -> tuple[list[str], list[str]]:
    from extract.scanning.repo_scope import scope_rule_lines

    return scope_rule_lines(scope, scope_policy)


def _build_repo_file_row(
    *,
    rel: Path,
    abs_path: Path,
    options: RepoScanOptions,
    follow_symlinks: bool,
) -> dict[str, object] | None:
    if abs_path.is_dir():
        return None
    if not follow_symlinks and abs_path.is_symlink():
        return None
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
    file_id = stable_id("file", options.repo_id, rel_posix)
    encoding: str | None = None
    if options.include_encoding:
        encoding = _detect_file_encoding(
            abs_path,
            sample_bytes=options.encoding_sample_bytes,
        )
    return {
        "file_id": file_id,
        "path": rel_posix,
        "abs_path": str(abs_path),
        "size_bytes": size_bytes,
        "mtime_ns": mtime_ns,
        "file_sha256": file_sha256,
        "encoding": encoding,
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

    Returns:
    -------
    TableLike | RecordBatchReaderLike
        Repo file metadata output.
    """
    normalized_options = normalize_options("repo_scan", options, RepoScanOptions)
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    determinism_tier = exec_context.determinism_tier()
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
            runtime_profile=runtime_profile,
            determinism_tier=determinism_tier,
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
        runtime_profile=runtime_profile,
        determinism_tier=determinism_tier,
        options=ExtractMaterializeOptions(
            normalize=normalize,
            prefer_reader=prefer_reader,
            apply_post_kernels=True,
        ),
    )


def scan_repo_tables(
    repo_root: PathLike,
    options: RepoScanOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
    prefer_reader: bool = False,
) -> Mapping[str, TableLike | RecordBatchReaderLike]:
    """Scan the repo and return repo scope tables.

    Returns:
    -------
    Mapping[str, TableLike | RecordBatchReaderLike]
        Mapping of output table names to data.
    """
    normalized_options = normalize_options("repo_scan", options, RepoScanOptions)
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    determinism_tier = exec_context.determinism_tier()
    normalize = ExtractNormalizeOptions(
        options=normalized_options,
        repo_id=normalized_options.repo_id,
    )
    plans = scan_repo_plans(repo_root, options=normalized_options, session=session)
    outputs: dict[str, TableLike | RecordBatchReaderLike] = {}
    repo_files_table: TableLike | None = None
    for name, plan in plans.items():
        plan_prefer_reader = prefer_reader and name != "repo_files_v1"
        outputs[name] = materialize_extract_plan(
            name,
            plan,
            runtime_profile=runtime_profile,
            determinism_tier=determinism_tier,
            options=ExtractMaterializeOptions(
                normalize=normalize,
                prefer_reader=plan_prefer_reader,
                apply_post_kernels=True,
            ),
        )
        if name == "repo_files_v1":
            repo_files_table = cast("TableLike", outputs[name])
    if repo_files_table is not None and "file_line_index_v1" not in outputs:
        from extract.extractors.file_index.line_index import LineIndexOptions, scan_file_line_index

        line_index_options = LineIndexOptions(
            repo_id=normalized_options.repo_id,
            max_files=normalized_options.max_files,
        )
        outputs["file_line_index_v1"] = scan_file_line_index(
            repo_files_table,
            options=line_index_options,
            context=exec_context,
            prefer_reader=prefer_reader,
        )
    return outputs


def scan_repo_plan(
    repo_root: PathLike,
    *,
    options: RepoScanOptions,
    session: ExtractSession,
) -> DataFusionPlanArtifact:
    """Build the plan for repository scanning.

    Returns:
    -------
    DataFusionPlanArtifact
        DataFusion plan bundle emitting repo file metadata.
    """
    repo_root_path = ensure_path(repo_root).resolve()
    normalize = ExtractNormalizeOptions(options=options, repo_id=options.repo_id)
    cache_profile = diskcache_profile_from_ctx(session.runtime_profile)
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
    bundle = _load_repo_scan_bundle(
        repo_root_path,
        options=options,
        cache_options=cache_options,
    )
    _record_repo_scope_stats(
        repo_root_path,
        bundle,
        options=options,
        session=session,
    )
    _record_repo_scope_trace(
        repo_root_path,
        bundle,
        options=options,
        session=session,
    )
    _record_repo_blame(
        repo_root_path,
        bundle.repo_rows,
        options=options,
        session=session,
    )
    return extract_plan_from_rows(
        "repo_files_v1",
        bundle.repo_rows,
        session=session,
        options=ExtractPlanOptions(normalize=normalize),
    )


def scan_repo_plans(
    repo_root: PathLike,
    *,
    options: RepoScanOptions,
    session: ExtractSession,
) -> Mapping[str, DataFusionPlanArtifact]:
    """Build plan bundles for repo scope datasets.

    Returns:
    -------
    Mapping[str, DataFusionPlanArtifact]
        Mapping of dataset name to plan bundle.
    """
    repo_root_path = ensure_path(repo_root).resolve()
    normalize = ExtractNormalizeOptions(options=options, repo_id=options.repo_id)
    cache_profile = diskcache_profile_from_ctx(session.runtime_profile)
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
    bundle = _load_repo_scan_bundle(
        repo_root_path,
        options=options,
        cache_options=cache_options,
    )
    plans: dict[str, DataFusionPlanArtifact] = {
        "repo_files_v1": extract_plan_from_rows(
            "repo_files_v1",
            bundle.repo_rows,
            session=session,
            options=ExtractPlanOptions(normalize=normalize),
        ),
        "scope_manifest_v1": extract_plan_from_rows(
            "scope_manifest_v1",
            bundle.scope_manifest_rows,
            session=session,
            options=ExtractPlanOptions(normalize=normalize),
        ),
        "python_extensions_v1": extract_plan_from_rows(
            "python_extensions_v1",
            bundle.python_extension_rows,
            session=session,
            options=ExtractPlanOptions(normalize=normalize),
        ),
    }
    if bundle.repo_rows:
        from datafusion_engine.arrow.build import table_from_rows
        from extract.extractors.file_index.line_index import (
            LineIndexOptions,
            scan_file_line_index_plan,
        )

        repo_table = table_from_rows(bundle.repo_rows)
        line_index_options = LineIndexOptions(
            repo_id=options.repo_id,
            max_files=options.max_files,
        )
        plans["file_line_index_v1"] = scan_file_line_index_plan(
            repo_table,
            options=line_index_options,
            session=session,
        )
    return plans


@dataclass(frozen=True)
class RepoScanCacheOptions:
    """Cache options for repository scanning."""

    cache: Cache | FanoutCache | None
    coord_cache: Cache | FanoutCache | None
    cache_key: str | None
    cache_ttl: float | None


def _collect_included_paths(
    scoped_roots: Sequence[_ScopedRoot],
    *,
    max_files: int | None,
) -> list[str]:
    included_paths: list[str] = []
    seen: set[str] = set()
    for scoped_root in scoped_roots:
        candidate_paths = _candidate_paths_for_root(scoped_root)
        manifest = build_scope_manifest(
            candidate_paths,
            options=ScopeManifestOptions(
                repo=scoped_root.scope.repo,
                rules=scoped_root.rules,
                scope_kind="repo",
                prefix=scoped_root.prefix,
                repo_root=scoped_root.scope.repo_root,
                status_flags=scoped_root.status_flags,
            ),
        )
        for entry in manifest.entries:
            if not entry.included:
                continue
            if scoped_root.diff_filter is not None and entry.path not in scoped_root.diff_filter:
                continue
            if entry.path in seen:
                continue
            included_paths.append(entry.path)
            seen.add(entry.path)
            if max_files is not None and len(included_paths) >= max_files:
                break
        if max_files is not None and len(included_paths) >= max_files:
            break
    return included_paths


def _scope_signature(scoped_roots: Sequence[_ScopedRoot]) -> list[dict[str, object]]:
    return [
        {
            "repo_root": str(root.scope.repo_root),
            "prefix": root.prefix.as_posix(),
            "extensions": sorted(root.scope.python_extensions),
            "rules": root.rules.signature(),
        }
        for root in scoped_roots
    ]


def _with_repo_scan_cache_key(
    cache_options: RepoScanCacheOptions,
    repo_root_path: Path,
    *,
    options: RepoScanOptions,
) -> RepoScanCacheOptions:
    if cache_options.cache is None:
        return cache_options
    scoped_roots = _scoped_roots(repo_root_path, options=options)
    included_paths = _collect_included_paths(scoped_roots, max_files=options.max_files)
    scope_hash = _scope_hash(scoped_roots, included_paths)
    scope_signature = _scope_signature(scoped_roots)
    cache_key = stable_cache_key(
        "repo_scan",
        {
            "repo_root": str(repo_root_path),
            "schema_version": SCHEMA_VERSION,
            "schema_identity_hash": repo_files_fingerprint(),
            "options": to_builtins(options),
            "scope_signature": scope_signature,
            "scope_hash": scope_hash,
        },
    )
    return replace(cache_options, cache_key=cache_key)


def _load_repo_scan_bundle(
    repo_root_path: Path,
    *,
    options: RepoScanOptions,
    cache_options: RepoScanCacheOptions,
) -> RepoScanBundle:
    compute_bundle = _select_bundle_loader(
        repo_root_path,
        options=options,
        cache_options=cache_options,
    )
    if cache_options.cache is None or cache_options.cache_key is None:
        return compute_bundle()

    @memoize_stampede(
        cache_options.cache,
        expire=cache_options.cache_ttl,
        tag=options.repo_id,
        name="repo_scan",
    )
    def _cached_scan(key: str) -> RepoScanBundle:
        _ = key
        return compute_bundle()

    return _cached_scan(cache_options.cache_key)


def _select_bundle_loader(
    repo_root_path: Path,
    *,
    options: RepoScanOptions,
    cache_options: RepoScanCacheOptions,
) -> Callable[[], RepoScanBundle]:
    def _compute_bundle() -> RepoScanBundle:
        return _build_repo_scope_bundle(repo_root_path, options=options)

    if cache_options.coord_cache is None or cache_options.cache_key is None:
        return _compute_bundle

    @throttle(
        cache_options.coord_cache,
        count=1,
        seconds=5,
        name=f"repo_scan:{cache_options.cache_key}",
        expire=cache_options.cache_ttl,
    )
    def _throttled_bundle() -> RepoScanBundle:
        return _compute_bundle()

    return _throttled_bundle


def _build_repo_scope_bundle(repo_root_path: Path, *, options: RepoScanOptions) -> RepoScanBundle:
    _maybe_update_submodules(repo_root_path, options)
    scoped_roots = _scoped_roots(repo_root_path, options=options)
    repo_rows: list[dict[str, object]] = []
    manifest_entries: list[ScopeManifest] = []
    extension_rows: list[dict[str, str]] = []
    seen_paths: set[str] = set()
    max_files = options.max_files
    for scoped_root in scoped_roots:
        extension_rows.extend(_extension_rows_for_scope(scoped_root.scope))
        candidate_paths = _candidate_paths_for_root(scoped_root)
        manifest = build_scope_manifest(
            candidate_paths,
            options=ScopeManifestOptions(
                repo=scoped_root.scope.repo,
                rules=scoped_root.rules,
                scope_kind="repo",
                prefix=scoped_root.prefix,
                repo_root=scoped_root.scope.repo_root,
                status_flags=scoped_root.status_flags,
            ),
        )
        manifest_entries.append(manifest)
        for entry in manifest.entries:
            if not entry.included:
                continue
            if scoped_root.diff_filter is not None and entry.path not in scoped_root.diff_filter:
                continue
            if entry.path in seen_paths:
                continue
            rel_in_scope = _rel_path_for_entry(entry.path, scoped_root.prefix)
            abs_path = scoped_root.scope.repo_root / rel_in_scope
            row = _build_repo_file_row(
                rel=Path(entry.path),
                abs_path=abs_path,
                options=options,
                follow_symlinks=scoped_root.scope.follow_symlinks,
            )
            if row is None:
                continue
            repo_rows.append(row)
            seen_paths.add(entry.path)
            if max_files is not None and len(repo_rows) >= max_files:
                break
        if max_files is not None and len(repo_rows) >= max_files:
            break
    manifest_rows: list[dict[str, str | None]] = []
    for manifest in manifest_entries:
        manifest_rows.extend(manifest.rows())
    scope_hash = _scope_hash(scoped_roots, _repo_row_paths(repo_rows))
    return RepoScanBundle(
        repo_rows=tuple(repo_rows),
        scope_manifest_rows=tuple(manifest_rows),
        python_extension_rows=tuple(extension_rows),
        scope_hash=scope_hash,
    )


def _rel_path_for_entry(path: str, prefix: Path) -> Path:
    rel = Path(path)
    if not prefix.parts:
        return rel
    try:
        return rel.relative_to(prefix)
    except ValueError:
        return rel


def _extension_rows_for_scope(scope: RepoScope) -> list[dict[str, str]]:
    rows = scope.extension_catalog.rows()
    for row in rows:
        row["repo_root"] = str(scope.repo_root)
    return rows


def _repo_row_paths(rows: Sequence[Mapping[str, object]]) -> list[str]:
    paths: list[str] = []
    for row in rows:
        path = row.get("path")
        if isinstance(path, str):
            paths.append(path)
    return paths


def _scope_hash(scoped_roots: Sequence[_ScopedRoot], paths: Sequence[str]) -> str:
    roots_payload = [
        {
            "repo_root": str(root.scope.repo_root),
            "prefix": root.prefix.as_posix(),
            "extensions": sorted(root.scope.python_extensions),
            "rules": root.rules.signature(),
        }
        for root in scoped_roots
    ]
    return stable_cache_key(
        "repo_scope",
        {
            "roots": roots_payload,
            "paths": sorted(path for path in paths if path),
        },
    )


def _parse_optional_int(value: object) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _pattern_counts_payload(
    counts: Mapping[int, int],
    patterns: Sequence[str],
    *,
    top_limit: int,
) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for idx, count in counts.items():
        if idx < 0 or idx >= len(patterns):
            continue
        items.append({"index": idx, "pattern": patterns[idx], "count": count})
    return sorted(items, key=_pattern_sort_key)[:top_limit]


def _pattern_sort_key(item: Mapping[str, object]) -> tuple[int, int]:
    count = item.get("count")
    index = item.get("index")
    count_value = count if isinstance(count, int) else 0
    index_value = index if isinstance(index, int) else 0
    return (-count_value, index_value)


def _pathspec_stats(
    manifest_rows: Sequence[Mapping[str, object]],
    rules_by_root: Mapping[str, ScopeRuleSet],
    *,
    top_limit: int = 10,
) -> list[dict[str, object]]:
    rows_by_root: dict[str, list[Mapping[str, object]]] = {}
    for row in manifest_rows:
        repo_root = row.get("repo_root")
        if not isinstance(repo_root, str) or not repo_root:
            continue
        rows_by_root.setdefault(repo_root, []).append(row)
    stats: list[dict[str, object]] = []
    for repo_root, rows in rows_by_root.items():
        rules = rules_by_root.get(repo_root)
        if rules is None:
            continue
        include_counts: dict[int, int] = {}
        exclude_counts: dict[int, int] = {}
        include_unmatched = 0
        exclude_unmatched = 0
        for row in rows:
            include_idx = _parse_optional_int(row.get("include_index"))
            exclude_idx = _parse_optional_int(row.get("exclude_index"))
            if rules.include_spec is not None:
                if include_idx is None:
                    include_unmatched += 1
                else:
                    include_counts[include_idx] = include_counts.get(include_idx, 0) + 1
            if rules.exclude_spec is not None:
                if exclude_idx is None:
                    exclude_unmatched += 1
                else:
                    exclude_counts[exclude_idx] = exclude_counts.get(exclude_idx, 0) + 1
        stats.append(
            {
                "repo_root": repo_root,
                "include_patterns": _pattern_counts_payload(
                    include_counts,
                    rules.include_lines,
                    top_limit=top_limit,
                ),
                "exclude_patterns": _pattern_counts_payload(
                    exclude_counts,
                    rules.exclude_lines,
                    top_limit=top_limit,
                ),
                "include_unmatched": include_unmatched,
                "exclude_unmatched": exclude_unmatched,
                "include_pattern_total": len(rules.include_lines),
                "exclude_pattern_total": len(rules.exclude_lines),
            }
        )
    return stats


def _rules_by_repo_root(
    repo_roots: Iterable[str],
    *,
    scope_policy: RepoScopeOptions,
) -> dict[str, ScopeRuleSet]:
    rules_by_root: dict[str, ScopeRuleSet] = {}
    for root in repo_roots:
        try:
            scope = resolve_repo_scope(Path(root), scope_policy)
        except ValueError:
            continue
        include_lines, exclude_lines = _scope_rule_lines(scope, scope_policy)
        rules_by_root[root] = build_scope_rules(
            include_lines=include_lines,
            exclude_lines=exclude_lines,
        )
    return rules_by_root


def _record_repo_scope_stats(
    repo_root: Path,
    bundle: RepoScanBundle,
    *,
    options: RepoScanOptions,
    session: ExtractSession,
) -> None:
    runtime_profile = session.runtime_profile
    if runtime_profile is None or runtime_profile.diagnostics.diagnostics_sink is None:
        return
    scope_policy = _resolve_scope_policy(options)
    repo_roots = {
        row.get("repo_root")
        for row in bundle.scope_manifest_rows
        if isinstance(row.get("repo_root"), str)
    }
    rules_by_root = _rules_by_repo_root(
        {root for root in repo_roots if isinstance(root, str)},
        scope_policy=scope_policy,
    )
    pathspec_stats = _pathspec_stats(bundle.scope_manifest_rows, rules_by_root)
    total = len(bundle.scope_manifest_rows)
    included = sum(1 for row in bundle.scope_manifest_rows if str(row.get("included")) == "True")
    ignored = sum(
        1 for row in bundle.scope_manifest_rows if str(row.get("ignored_by_git")) == "True"
    )
    untracked = sum(
        1 for row in bundle.scope_manifest_rows if str(row.get("is_untracked")) == "True"
    )
    payload = {
        "repo_root": str(repo_root),
        "scope_hash": bundle.scope_hash,
        "total_candidates": total,
        "included": included,
        "ignored_by_git": ignored,
        "untracked": untracked,
        "repo_rows": len(bundle.repo_rows),
        "python_extensions": len(bundle.python_extension_rows),
        "repo_id": options.repo_id,
        "pathspec_stats": pathspec_stats,
    }
    from datafusion_engine.lineage.diagnostics import record_artifact
    from serde_artifact_specs import REPO_SCOPE_STATS_SPEC

    record_artifact(runtime_profile, REPO_SCOPE_STATS_SPEC, payload)


def _trace_sample_paths(paths: Sequence[str], limit: int | None) -> list[str]:
    if limit is None:
        return list(paths)
    if limit <= 0:
        return []
    return list(paths[:limit])


def _match_detail_payload(detail: object, *, limit: int | None) -> dict[str, object]:
    patterns = getattr(detail, "patterns", None)
    if isinstance(patterns, Sequence) and not isinstance(patterns, (str, bytes, bytearray)):
        return {"patterns": _truncate_patterns(patterns, limit)}
    return {"patterns": []}


def _truncate_patterns(patterns: Sequence[object], limit: int | None) -> list[str]:
    if limit is None:
        return [str(pattern) for pattern in patterns]
    if limit <= 0:
        return []
    return [str(pattern) for pattern in patterns[:limit]]


def _trace_payload(
    paths: Sequence[str],
    rules: ScopeRuleSet,
    *,
    pattern_limit: int | None,
) -> Mapping[str, object]:
    raw = explain_scope_paths(paths, rules)
    include_raw = raw.get("include")
    exclude_raw = raw.get("exclude")
    include_payload: dict[str, object] = {}
    exclude_payload: dict[str, object] = {}
    if isinstance(include_raw, Mapping):
        for key, value in include_raw.items():
            include_payload[str(key)] = _match_detail_payload(value, limit=pattern_limit)
    if isinstance(exclude_raw, Mapping):
        for key, value in exclude_raw.items():
            exclude_payload[str(key)] = _match_detail_payload(value, limit=pattern_limit)
    return {"include": include_payload, "exclude": exclude_payload}


def _record_repo_scope_trace(
    repo_root: Path,
    bundle: RepoScanBundle,
    *,
    options: RepoScanOptions,
    session: ExtractSession,
) -> None:
    if not options.record_pathspec_trace:
        return
    runtime_profile = session.runtime_profile
    if runtime_profile is None or runtime_profile.diagnostics.diagnostics_sink is None:
        return
    scoped_roots = _scoped_roots(repo_root, options=options)
    traces: list[dict[str, object]] = []
    trace_limit = options.pathspec_trace_limit
    pattern_limit = options.pathspec_trace_pattern_limit
    for scoped_root in scoped_roots:
        candidate_paths = _candidate_paths_for_root(scoped_root)
        sample_paths = _trace_sample_paths(candidate_paths, trace_limit)
        if not sample_paths:
            continue
        trace = _trace_payload(sample_paths, scoped_root.rules, pattern_limit=pattern_limit)
        traces.append(
            {
                "repo_root": str(scoped_root.scope.repo_root),
                "prefix": scoped_root.prefix.as_posix(),
                "sample_size": len(sample_paths),
                "sample_paths": sample_paths,
                "include_pattern_total": len(scoped_root.rules.include_lines),
                "exclude_pattern_total": len(scoped_root.rules.exclude_lines),
                "pattern_limit": pattern_limit,
                "trace": trace,
            }
        )
    if not traces:
        return
    payload = {
        "repo_root": str(repo_root),
        "scope_hash": bundle.scope_hash,
        "trace_limit": trace_limit,
        "traces": traces,
    }
    from datafusion_engine.lineage.diagnostics import record_artifact
    from serde_artifact_specs import REPO_SCOPE_TRACE_SPEC

    record_artifact(runtime_profile, REPO_SCOPE_TRACE_SPEC, payload)


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
    runtime_profile = session.runtime_profile
    if runtime_profile is None or runtime_profile.diagnostics.diagnostics_sink is None:
        return
    paths = _blame_paths(entries, limit=options.blame_max_files)
    if not paths:
        return
    try:
        git_scope = resolve_repo_scope(repo_root, _resolve_scope_policy(options))
    except ValueError:
        return
    author_stats: dict[str, _AuthorStat] = {}
    for path_posix in paths:
        for hunk in blame_hunks(git_scope.repo, path_posix=path_posix, ref=options.blame_ref):
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
    from datafusion_engine.lineage.diagnostics import record_artifact
    from serde_artifact_specs import REPO_SCAN_BLAME_SPEC

    record_artifact(runtime_profile, REPO_SCAN_BLAME_SPEC, payload)


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


__all__ = [
    "RepoScanBundle",
    "RepoScanOptions",
    "default_repo_scan_options",
    "repo_files_query",
    "scan_repo",
    "scan_repo_plan",
    "scan_repo_plans",
    "scan_repo_tables",
]
