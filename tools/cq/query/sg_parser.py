"""ast-grep parser using ast-grep-py native bindings."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import msgspec

from tools.cq.astgrep.rules import get_rules_for_types
from tools.cq.astgrep.sgpy_scanner import (
    RecordType,
    SgRecord,
    filter_records_by_type,
    group_records_by_file,
    scan_files,
)
from tools.cq.core.cache import (
    CqCacheBackend,
    CqCachePolicyV1,
    ScopePlanV1,
    ScopeResolutionV1,
    build_cache_key,
    build_namespace_cache_tag,
    default_cache_policy,
    get_cq_cache_backend,
    is_namespace_cache_enabled,
    record_cache_decode_failure,
    record_cache_get,
    record_cache_set,
    resolve_namespace_ttl_seconds,
    resolve_scope,
)
from tools.cq.core.structs import CqStruct
from tools.cq.index.files import build_repo_file_index, tabulate_files
from tools.cq.index.repo import RepoContext, resolve_repo_context
from tools.cq.query.language import (
    DEFAULT_QUERY_LANGUAGE,
    QueryLanguage,
    file_extensions_for_language,
)
from tools.cq.query.parser import QueryParseError

# Record types from ast-grep rules
ALL_RECORD_TYPES: set[RecordType] = {"def", "call", "import", "raise", "except", "assign_ctor"}


class FileInventoryCacheV1(CqStruct, frozen=True):
    """Cached file inventory payload for ast-grep scans."""

    files: list[str]
    snapshot_digest: str = ""
    inventory_token: dict[str, int] = msgspec.field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class FileInventoryWriteRequestV1:
    """Request envelope for persisting file-inventory cache payloads."""

    cache: CqCacheBackend
    policy: CqCachePolicyV1
    namespace: str
    cache_key: str
    root: Path
    scope: ScopeResolutionV1
    files: list[Path]


# Re-export SgRecord from sgpy_scanner for backward compatibility
__all__ = [
    "ALL_RECORD_TYPES",
    "RecordType",
    "SgRecord",
    "filter_records_by_kind",
    "group_records_by_file",
    "list_scan_files",
    "normalize_record_types",
    "sg_scan",
]


def sg_scan(
    paths: list[Path],
    record_types: Iterable[str] | Iterable[RecordType] | None = None,
    root: Path | None = None,
    globs: list[str] | None = None,
    lang: QueryLanguage = DEFAULT_QUERY_LANGUAGE,
) -> list[SgRecord]:
    """Run ast-grep-py scan and return parsed records.

    Parameters
    ----------
    paths
        Paths to scan (files or directories)
    record_types
        Filter to specific record types (def, call, import, etc.)
        If None, returns all record types.
    root
        Root directory for relative paths.
    globs
        Glob filters for file selection (supports ! excludes).
    lang
        Query language for scanner parsing and rule dispatch.

    Returns:
    -------
    list[SgRecord]
        Parsed scan records.
    """
    if root is None:
        root = Path.cwd()

    files = _tabulate_scan_files(paths, root, globs, lang=lang)
    if not files:
        return []

    normalized_record_types = normalize_record_types(record_types)
    rules = get_rules_for_types(normalized_record_types, lang=lang)
    if not rules:
        return []

    records = scan_files(files, rules, root, lang=lang)
    return filter_records_by_type(records, normalized_record_types)


def list_scan_files(
    paths: list[Path],
    root: Path | None = None,
    globs: list[str] | None = None,
    lang: QueryLanguage = DEFAULT_QUERY_LANGUAGE,
) -> list[Path]:
    """Return the list of files that would be scanned.

    Parameters
    ----------
    paths
        Paths to scan (files or directories)
    root
        Root directory for relative paths
    globs
        Glob filters for file selection (supports ! excludes)
    lang
        Query language for file extension selection.

    Returns:
    -------
    list[Path]
        Files selected for scanning
    """
    if root is None:
        root = Path.cwd()
    return _tabulate_scan_files(paths, root, globs, lang=lang)


def _repo_inventory_token(*, root: Path, repo_context: RepoContext) -> dict[str, int]:
    try:
        root_mtime_ns = max(0, int(root.stat().st_mtime_ns))
    except (OSError, RuntimeError, ValueError):
        root_mtime_ns = 0

    git_index_mtime_ns = 0
    if repo_context.git_dir is not None:
        index_path = repo_context.git_dir / "index"
        try:
            git_index_mtime_ns = max(0, int(index_path.stat().st_mtime_ns))
        except (OSError, RuntimeError, ValueError):
            git_index_mtime_ns = 0

    return {
        "root_mtime_ns": root_mtime_ns,
        "git_index_mtime_ns": git_index_mtime_ns,
    }


def _tabulate_scan_files(
    paths: list[Path],
    root: Path,
    globs: list[str] | None,
    *,
    lang: QueryLanguage,
) -> list[Path]:
    """Tabulate files to scan using cache-backed inventory and snapshots.

    Returns:
        list[Path]: Sorted files that match scan inputs.
    """
    namespace = "file_inventory"
    resolved_root = root.resolve()
    scope_plan = ScopePlanV1(
        root=str(resolved_root),
        paths=tuple(str(path) for path in paths),
        globs=tuple(globs or ()),
        language=lang,
    )
    scope = resolve_scope(
        scope_plan,
        list_files=_list_files_for_inventory,
        inventory_token_fn=_inventory_token_for_root,
    )
    cache_key = build_cache_key(
        namespace,
        version="v1",
        workspace=scope.root,
        language=scope.language,
        target=scope.scope_hash or scope.language,
        extras={
            "scope_globs": scope_plan.globs,
            "inventory_token": scope.inventory_token,
        },
    )
    policy = default_cache_policy(root=resolved_root)
    cache = get_cq_cache_backend(root=resolved_root)
    cache_enabled = is_namespace_cache_enabled(policy=policy, namespace=namespace)
    if cache_enabled:
        cached_files = _read_cached_file_inventory(
            cache=cache,
            cache_key=cache_key,
            root=resolved_root,
            namespace=namespace,
        )
        if cached_files is not None:
            return cached_files
    files = [resolved_root / rel for rel in scope.files]
    if cache_enabled:
        _write_cached_file_inventory(
            FileInventoryWriteRequestV1(
                cache=cache,
                namespace=namespace,
                cache_key=cache_key,
                policy=policy,
                root=resolved_root,
                scope=scope,
                files=files,
            )
        )
    return files


def _read_cached_file_inventory(
    *,
    cache: CqCacheBackend,
    cache_key: str,
    root: Path,
    namespace: str,
) -> list[Path] | None:
    cached = cache.get(cache_key)
    record_cache_get(namespace=namespace, hit=isinstance(cached, dict), key=cache_key)
    if not isinstance(cached, dict):
        return None
    try:
        payload = msgspec.convert(cached, type=FileInventoryCacheV1)
    except (RuntimeError, TypeError, ValueError):
        record_cache_decode_failure(namespace=namespace)
        return None
    files = [root / rel for rel in payload.files]
    existing = [path for path in files if path.exists()]
    return existing if len(existing) == len(files) else None


def _write_cached_file_inventory(request: FileInventoryWriteRequestV1) -> None:
    ttl_seconds = resolve_namespace_ttl_seconds(
        policy=request.policy,
        namespace=request.namespace,
    )
    rel_files = [_normalize_match_file(str(path), request.root) for path in request.files]
    payload = FileInventoryCacheV1(
        files=rel_files,
        snapshot_digest=request.scope.snapshot_digest,
        inventory_token={
            str(k): int(v) if isinstance(v, int) else 0
            for k, v in request.scope.inventory_token.items()
        },
    )
    ok = request.cache.set(
        request.cache_key,
        msgspec.to_builtins(payload),
        expire=ttl_seconds,
        tag=build_namespace_cache_tag(
            workspace=request.scope.root,
            language=request.scope.language,
            namespace=request.namespace,
            scope_hash=request.scope.scope_hash,
            snapshot=request.scope.snapshot_digest,
        ),
    )
    record_cache_set(namespace=request.namespace, ok=ok, key=request.cache_key)


def _list_files_for_inventory(
    paths: list[Path],
    root: Path,
    globs: list[str] | None,
    lang: str,
) -> list[Path]:
    query_lang: QueryLanguage = "rust" if lang == "rust" else "python"
    repo_context = resolve_repo_context(root)
    repo_index = build_repo_file_index(repo_context)
    result = tabulate_files(
        repo_index,
        paths,
        globs,
        extensions=tuple(file_extensions_for_language(query_lang)),
    )
    return sorted(result.files, key=lambda path: path.as_posix())


def _inventory_token_for_root(root: Path) -> Mapping[str, object]:
    return _repo_inventory_token(root=root, repo_context=resolve_repo_context(root))


def normalize_record_types(
    record_types: Iterable[str] | Iterable[RecordType] | None,
) -> set[RecordType] | None:
    """Normalize and validate record types.

    Args:
        record_types: Requested record types, if any.

    Returns:
        set[RecordType] | None: Normalized record types, empty set, or `None`.

    Raises:
        QueryParseError: If any provided record type is invalid.
    """
    if record_types is None:
        return None
    record_set: set[RecordType] = set()
    invalid: list[str] = []
    for value in record_types:
        if value in ALL_RECORD_TYPES:
            record_set.add(cast("RecordType", value))
        else:
            invalid.append(str(value))
    if not record_set and not invalid:
        return set()
    invalid = sorted(invalid)
    if invalid:
        msg = (
            "Invalid record types: "
            f"{', '.join(invalid)}. Valid types: {', '.join(sorted(ALL_RECORD_TYPES))}"
        )
        raise QueryParseError(msg)
    return record_set


def _normalize_match_file(file_path: str, root: Path) -> str:
    """Normalize file paths to repo-relative POSIX paths.

    Returns:
        str: Normalized file path string.
    """
    path = Path(file_path)
    if path.is_absolute():
        try:
            return path.relative_to(root).as_posix()
        except ValueError:
            return file_path
    return path.as_posix()


def _parse_rule_id(rule_id: str) -> tuple[RecordType | None, str]:
    """Parse rule ID to extract record type and kind.

    Returns:
        tuple[RecordType | None, str]: Parsed record type and record kind.
    """
    if not rule_id.startswith("py_"):
        return None, ""

    suffix = rule_id[3:]

    prefix_map: dict[str, RecordType] = {
        "def_": "def",
        "call_": "call",
        "import": "import",
        "from_import": "import",
        "raise": "raise",
        "except": "except",
        "ctor_assign": "assign_ctor",
    }

    for prefix, record_type in prefix_map.items():
        if suffix.startswith(prefix):
            kind = suffix[len(prefix) :] if prefix.endswith("_") else suffix
            return record_type, kind

    return None, ""


def filter_records_by_kind(
    records: list[SgRecord],
    record_type: RecordType,
    kinds: set[str] | None = None,
) -> list[SgRecord]:
    """Filter records by type and optionally by kind.

    Returns:
        list[SgRecord]: Matching records by record type and kind.
    """
    return [r for r in records if r.record == record_type and (kinds is None or r.kind in kinds)]
