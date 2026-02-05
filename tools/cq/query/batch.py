"""Batch query execution helpers for shared ast-grep scans."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.astgrep.rules_py import get_rules_for_types
from tools.cq.astgrep.sgpy_scanner import RecordType, SgRecord, filter_records_by_type, scan_files
from tools.cq.index.files import build_repo_file_index, tabulate_files
from tools.cq.index.repo import resolve_repo_context
from tools.cq.query.enrichment import SymtableEnricher
from tools.cq.query.executor import (
    EntityCandidates,
    ScanContext,
    _build_entity_candidates,
    _build_scan_context,
)
from tools.cq.query.planner import scope_to_globs, scope_to_paths
from tools.cq.query.sg_parser import normalize_record_types

if TYPE_CHECKING:
    from tools.cq.core.toolchain import Toolchain
    from tools.cq.query.ir import Scope


@dataclass(frozen=True)
class BatchEntityQuerySession:
    """Shared scan session for multiple entity queries."""

    root: Path
    tc: Toolchain
    files: list[Path]
    files_by_rel: dict[str, Path]
    records: list[SgRecord]
    scan: ScanContext
    candidates: EntityCandidates
    symtable: SymtableEnricher


def build_batch_session(
    *,
    root: Path,
    tc: Toolchain,
    paths: list[Path],
    record_types: Iterable[str] | Iterable[RecordType],
) -> BatchEntityQuerySession:
    """Build a shared scan session for multiple entity queries.

    Returns
    -------
    BatchEntityQuerySession
        Session containing shared scan artifacts.
    """
    repo_context = resolve_repo_context(root)
    repo_index = build_repo_file_index(repo_context)
    file_result = tabulate_files(
        repo_index,
        paths,
        None,
        extensions=(".py",),
    )
    files = file_result.files
    files_by_rel = _index_files_by_rel(root, files)

    normalized_record_types = normalize_record_types(record_types)
    rules = get_rules_for_types(normalized_record_types)
    if not files or not rules:
        records: list[SgRecord] = []
    else:
        records = scan_files(files, rules, root)
        records = filter_records_by_type(records, normalized_record_types)

    scan_ctx = _build_scan_context(records)
    candidates = _build_entity_candidates(scan_ctx, records)
    return BatchEntityQuerySession(
        root=root,
        tc=tc,
        files=files,
        files_by_rel=files_by_rel,
        records=records,
        scan=scan_ctx,
        candidates=candidates,
        symtable=SymtableEnricher(root),
    )


def filter_files_for_scope(files: list[Path], root: Path, scope: Scope) -> set[str]:
    """Filter a pre-tabulated file list by scope constraints.

    Returns
    -------
    set[str]
        Relative paths that satisfy the scope constraints.
    """
    scope_paths = scope_to_paths(scope, root)
    if not scope_paths:
        return set()

    globs = scope_to_globs(scope)
    allowed: set[str] = set()
    for file_path in files:
        if not _is_within_scope(file_path, scope_paths):
            continue
        rel_path = _rel_path(root, file_path)
        if globs and not _matches_globs(rel_path, globs):
            continue
        allowed.add(rel_path)
    return allowed


def select_files_by_rel(files_by_rel: dict[str, Path], rel_paths: set[str]) -> list[Path]:
    """Return file paths matching a set of relative paths.

    Returns
    -------
    list[pathlib.Path]
        File paths resolved from relative path keys.
    """
    return [files_by_rel[rel] for rel in rel_paths if rel in files_by_rel]


def _index_files_by_rel(root: Path, files: list[Path]) -> dict[str, Path]:
    indexed: dict[str, Path] = {}
    for file_path in files:
        rel = _rel_path(root, file_path)
        indexed[rel] = file_path
    return indexed


def _rel_path(root: Path, file_path: Path) -> str:
    try:
        return file_path.relative_to(root).as_posix()
    except ValueError:
        return file_path.as_posix()


def _matches_globs(rel_path: str, globs: list[str]) -> bool:
    if not globs:
        return True
    has_includes = any(not glob.startswith("!") for glob in globs)
    include = not has_includes
    for glob in globs:
        negated = glob.startswith("!")
        pattern = glob[1:] if negated else glob
        if Path(rel_path).match(pattern):
            include = not negated
    return include


def _is_within_scope(path: Path, scope_roots: list[Path]) -> bool:
    return any(_is_relative_to(path, scope_root) for scope_root in scope_roots)


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    else:
        return True


__all__ = [
    "BatchEntityQuerySession",
    "build_batch_session",
    "filter_files_for_scope",
    "select_files_by_rel",
]
