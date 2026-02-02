"""ast-grep parser using ast-grep-py native bindings.

Parses Python source code into typed records using ast-grep-py.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

from tools.cq.astgrep.rules_py import get_rules_for_types
from tools.cq.astgrep.sgpy_scanner import (
    SgRecord,
    filter_records_by_type,
    group_records_by_file,
    scan_files,
)
from tools.cq.index.files import build_repo_file_index, tabulate_files
from tools.cq.index.repo import resolve_repo_context

if TYPE_CHECKING:
    from tools.cq.index.sqlite_cache import IndexCache

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
    "sg_scan",
]


def sg_scan(
    paths: list[Path],
    record_types: set[str] | None = None,
    root: Path | None = None,
    globs: list[str] | None = None,
    index_cache: IndexCache | None = None,
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
    index_cache
        Optional index cache for incremental scanning.

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

    if index_cache is not None:
        return _scan_with_cache(
            files=files,
            root=root,
            record_types=record_types,
            index_cache=index_cache,
        )

    records = scan_files(files, rules, root)
    return filter_records_by_type(records, record_types)


def _scan_with_cache(
    files: list[Path],
    root: Path,
    record_types: set[str] | None,
    index_cache: IndexCache,
) -> list[SgRecord]:
    """Run ast-grep-py scan with index cache support."""
    record_types_set = set(record_types) if record_types is not None else ALL_RECORD_TYPES
    if not files:
        return []

    cached_records: list[SgRecord] = []
    files_to_scan: list[Path] = []

    for file_path in files:
        if index_cache.needs_rescan(file_path, record_types_set):
            files_to_scan.append(file_path)
            continue
        cached = index_cache.retrieve(file_path, record_types_set)
        if cached is None:
            files_to_scan.append(file_path)
            continue
        cached_records.extend(_records_from_cache(cached, root))

    scanned_records: list[SgRecord] = []
    if files_to_scan:
        rules = get_rules_for_types(record_types)
        scanned_records = scan_files(files_to_scan, rules, root)
        records_by_file = group_records_by_file(scanned_records)
        recorded_paths = set()
        for file_path in records_by_file:
            path_obj = Path(file_path)
            if path_obj.is_absolute():
                resolved = path_obj.resolve()
            else:
                resolved = (root / path_obj).resolve()
            if resolved.is_relative_to(root):
                recorded_paths.add(str(resolved.relative_to(root)))
        for file_path_str, file_records in records_by_file.items():
            file_path_obj = root / file_path_str
            records_data = [_record_to_cache_dict(record) for record in file_records]
            if file_path_obj.exists():
                index_cache.store(file_path_obj, records_data, record_types_set)
        for file_path in files_to_scan:
            if not file_path.exists():
                continue
            if str(file_path.relative_to(root)) not in recorded_paths:
                index_cache.store(file_path, [], record_types_set)

    return _filter_records(cached_records + scanned_records, record_types)


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


def _record_to_cache_dict(record: SgRecord) -> dict[str, object]:
    """Convert a record to cache-serializable dict."""
    return {
        "record": record.record,
        "kind": record.kind,
        "file": record.file,
        "start_line": record.start_line,
        "start_col": record.start_col,
        "end_line": record.end_line,
        "end_col": record.end_col,
        "text": record.text,
        "rule_id": record.rule_id,
    }


def _records_from_cache(
    records: list[dict[str, object]],
    root: Path,
) -> list[SgRecord]:
    """Convert cached record dicts to SgRecord instances."""
    parsed: list[SgRecord] = []
    for record in records:
        record_type = str(record.get("record", ""))
        if record_type not in {"def", "call", "import", "raise", "except", "assign_ctor"}:
            continue
        file_path = _normalize_file_path(str(record.get("file", "")), root)
        parsed.append(
            SgRecord(
                record=cast("RecordType", record_type),
                kind=str(record.get("kind", "")),
                file=file_path,
                start_line=int(record.get("start_line", 0)),
                start_col=int(record.get("start_col", 0)),
                end_line=int(record.get("end_line", 0)),
                end_col=int(record.get("end_col", 0)),
                text=str(record.get("text", "")),
                rule_id=str(record.get("rule_id", "")),
            )
        )
    return parsed


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
    prefix_map = {
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
            return record_type, kind  # type: ignore[return-value]

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
