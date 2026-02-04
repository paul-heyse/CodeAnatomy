"""ast-grep parser using ast-grep-py native bindings.

Parses Python source code into typed records using ast-grep-py.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from tools.cq.astgrep.rules_py import get_rules_for_types
from tools.cq.astgrep.sgpy_scanner import (
    SgRecord,
    filter_records_by_type,
    group_records_by_file,
    scan_files,
)
from tools.cq.index.files import build_repo_file_index, tabulate_files
from tools.cq.index.repo import resolve_repo_context

# Record types from ast-grep rules
RecordType = Literal["def", "call", "import", "raise", "except", "assign_ctor"]
ALL_RECORD_TYPES: set[str] = {"def", "call", "import", "raise", "except", "assign_ctor"}

# Re-export SgRecord from sgpy_scanner for backward compatibility
__all__ = [
    "ALL_RECORD_TYPES",
    "RecordType",
    "SgRecord",
    "filter_records_by_kind",
    "group_records_by_file",
    "list_scan_files",
    "sg_scan",
]


def sg_scan(
    paths: list[Path],
    record_types: set[str] | None = None,
    root: Path | None = None,
    globs: list[str] | None = None,
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

    Returns
    -------
    list[SgRecord]
        Parsed scan records.
    """
    if root is None:
        root = Path.cwd()

    files = _tabulate_scan_files(paths, root, globs)
    if not files:
        return []

    rules = get_rules_for_types(record_types)
    if not rules:
        return []

    records = scan_files(files, rules, root)
    return filter_records_by_type(records, record_types)


def list_scan_files(
    paths: list[Path],
    root: Path | None = None,
    globs: list[str] | None = None,
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

    Returns
    -------
    list[Path]
        Files selected for scanning
    """
    if root is None:
        root = Path.cwd()
    return _tabulate_scan_files(paths, root, globs)


    # No caching: scan all files directly.


def _tabulate_scan_files(
    paths: list[Path],
    root: Path,
    globs: list[str] | None,
) -> list[Path]:
    """Tabulate files to scan using the file index."""
    repo_context = resolve_repo_context(root)
    repo_index = build_repo_file_index(repo_context)
    result = tabulate_files(
        repo_index,
        paths,
        globs,
        extensions=(".py",),
    )
    return result.files


def _filter_records(
    records: list[SgRecord],
    record_types: set[str] | None,
) -> list[SgRecord]:
    """Filter records by record type."""
    if record_types is None:
        return records
    return [record for record in records if record.record in record_types]


def _normalize_file_path(file_path: str, root: Path) -> str:
    """Normalize file paths to repo-relative POSIX paths."""
    path = Path(file_path)
    if path.is_absolute():
        try:
            return path.relative_to(root).as_posix()
        except ValueError:
            return file_path
    return path.as_posix()


def _parse_rule_id(rule_id: str) -> tuple[RecordType | None, str]:
    """Parse rule ID to extract record type and kind.

    Rule IDs follow patterns:
    - py_def_function -> (def, function)
    - py_def_class_bases -> (def, class_bases)
    - py_call_name -> (call, name)
    - py_import -> (import, import)
    - py_from_import_as -> (import, from_import_as)
    - py_raise -> (raise, raise)
    - py_except_as -> (except, except_as)
    - py_ctor_assign_name -> (assign_ctor, ctor_assign_name)
    """
    if not rule_id.startswith("py_"):
        return None, ""

    # Remove py_ prefix
    suffix = rule_id[3:]

    # Map rule prefixes to record types
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
            # Kind is the full suffix (e.g., "def_function" -> kind="function")
            if prefix.endswith("_"):
                kind = suffix[len(prefix) :]
            else:
                kind = suffix
            return record_type, kind

    return None, ""


def filter_records_by_kind(
    records: list[SgRecord],
    record_type: RecordType,
    kinds: set[str] | None = None,
) -> list[SgRecord]:
    """Filter records by type and optionally by kind.

    Parameters
    ----------
    records
        Records to filter
    record_type
        Record type to match
    kinds
        Optional set of kinds to match. If None, matches all kinds.

    Returns
    -------
    list[SgRecord]
        Filtered records
    """
    return [r for r in records if r.record == record_type and (kinds is None or r.kind in kinds)]
