"""Batch query execution helpers for shared ast-grep scans."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.astgrep.rules import get_rules_for_types
from tools.cq.astgrep.sgpy_scanner import RecordType, SgRecord, filter_records_by_type, scan_files
from tools.cq.core.pathing import is_relative_to, match_ordered_globs, normalize_repo_relative_path
from tools.cq.core.run_context import SymtableEnricherPort
from tools.cq.core.types import DEFAULT_QUERY_LANGUAGE, QueryLanguage
from tools.cq.query.planner import scope_to_globs, scope_to_paths
from tools.cq.query.scan import (
    EntityCandidates,
    ScanContext,
    build_entity_candidates,
    build_scan_context,
)
from tools.cq.query.sg_parser import list_scan_files, normalize_record_types

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
    symtable: SymtableEnricherPort


def build_batch_session(
    *,
    root: Path,
    tc: Toolchain,
    paths: list[Path],
    record_types: Iterable[str] | Iterable[RecordType],
    lang: QueryLanguage = DEFAULT_QUERY_LANGUAGE,
    symtable: SymtableEnricherPort,
) -> BatchEntityQuerySession:
    """Build a shared scan session for multiple entity queries.

    Returns:
    -------
    BatchEntityQuerySession
        Session containing shared scan artifacts.
    """
    files = list_scan_files(paths=paths, root=root, globs=None, lang=lang)
    files_by_rel = _index_files_by_rel(root, files)

    normalized_record_types = normalize_record_types(record_types)
    rules = get_rules_for_types(normalized_record_types, lang=lang)
    if not files or not rules:
        records: list[SgRecord] = []
    else:
        if lang == DEFAULT_QUERY_LANGUAGE:
            records = scan_files(files, rules, root, prefilter=True)
        else:
            records = scan_files(files, rules, root, lang=lang, prefilter=True)
        records = filter_records_by_type(records, normalized_record_types)

    scan_ctx = build_scan_context(records)
    candidates = build_entity_candidates(scan_ctx, records)
    return BatchEntityQuerySession(
        root=root,
        tc=tc,
        files=files,
        files_by_rel=files_by_rel,
        records=records,
        scan=scan_ctx,
        candidates=candidates,
        symtable=symtable,
    )


def filter_files_for_scope(files: list[Path], root: Path, scope: Scope) -> set[str]:
    """Filter a pre-tabulated file list by scope constraints.

    Returns:
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
        rel_path = normalize_repo_relative_path(file_path, root=root)
        if globs and not match_ordered_globs(rel_path, globs):
            continue
        allowed.add(rel_path)
    return allowed


def select_files_by_rel(files_by_rel: dict[str, Path], rel_paths: set[str]) -> list[Path]:
    """Return file paths matching a set of relative paths.

    Returns:
    -------
    list[pathlib.Path]
        File paths resolved from relative path keys.
    """
    return [files_by_rel[rel] for rel in rel_paths if rel in files_by_rel]


def _index_files_by_rel(root: Path, files: list[Path]) -> dict[str, Path]:
    indexed: dict[str, Path] = {}
    for file_path in files:
        rel = normalize_repo_relative_path(file_path, root=root)
        indexed[rel] = file_path
    return indexed


def _is_within_scope(path: Path, scope_roots: list[Path]) -> bool:
    return any(is_relative_to(path, scope_root) for scope_root in scope_roots)


__all__ = [
    "BatchEntityQuerySession",
    "build_batch_session",
    "filter_files_for_scope",
    "select_files_by_rel",
]
