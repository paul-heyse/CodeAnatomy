"""ast-grep JSON stream parser.

Parses ast-grep scan output (JSON stream format) into typed records.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

if TYPE_CHECKING:
    from tools.cq.index.sqlite_cache import IndexCache

# Record types from ast-grep rules
RecordType = Literal["def", "call", "import", "raise", "except", "assign_ctor"]
ALL_RECORD_TYPES: set[str] = {"def", "call", "import", "raise", "except", "assign_ctor"}


@dataclass(frozen=True)
class SgRecord:
    """Parsed record from ast-grep scan output.

    Attributes
    ----------
    record
        Record type (def, call, import, raise, except, assign_ctor)
    kind
        Specific kind within record type (function, class, name_call, etc.)
    file
        File path (relative to scan root)
    start_line
        Start line (1-indexed for human output)
    start_col
        Start column (0-indexed)
    end_line
        End line (1-indexed)
    end_col
        End column (0-indexed)
    text
        Matched source text
    rule_id
        Full rule ID from ast-grep
    """

    record: RecordType
    kind: str
    file: str
    start_line: int
    start_col: int
    end_line: int
    end_col: int
    text: str
    rule_id: str

    @property
    def location(self) -> str:
        """Human-readable location string."""
        return f"{self.file}:{self.start_line}:{self.start_col}"


def sg_scan(
    paths: list[Path],
    record_types: set[str] | None = None,
    config_path: Path | None = None,
    root: Path | None = None,
    globs: list[str] | None = None,
    index_cache: IndexCache | None = None,
) -> list[SgRecord]:
    """Run ast-grep scan and parse JSON stream output.

    Parameters
    ----------
    paths
        Paths to scan (files or directories)
    record_types
        Filter to specific record types (def, call, import, etc.)
        If None, returns all record types.
    config_path
        Path to sgconfig.yml. If None, uses default location.
    root
        Root directory for relative paths.
    globs
        Glob filters for ast-grep (supports ! excludes).
    index_cache
        Optional index cache for incremental scanning.

    Returns
    -------
    list[SgRecord]
        Parsed scan records.

    Raises
    ------
    RuntimeError
        If ast-grep is not available or scan fails.
    """
    if root is None:
        root = Path.cwd()

    if config_path is None:
        config_path = root / "tools" / "cq" / "astgrep" / "sgconfig.yml"

    if not config_path.exists():
        msg = f"ast-grep config not found: {config_path}"
        raise RuntimeError(msg)

    records: list[SgRecord] = []

    if index_cache is not None:
        records.extend(
            _scan_with_cache(
                paths=paths,
                root=root,
                config_path=config_path,
                record_types=record_types,
                globs=globs,
                index_cache=index_cache,
            )
        )
        return records

    return _run_scan(paths, root, config_path, record_types, globs)


def _run_scan(
    paths: list[Path],
    root: Path,
    config_path: Path,
    record_types: set[str] | None,
    globs: list[str] | None,
) -> list[SgRecord]:
    """Run ast-grep scan and return parsed records."""
    cmd = [
        "ast-grep",
        "scan",
        "-c",
        str(config_path),
        "--json=stream",
    ]
    if globs:
        for glob in globs:
            cmd.extend(["--globs", glob])
    cmd.extend(str(p) for p in paths)

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=root,
    )

    if result.returncode != 0:
        msg = f"ast-grep scan failed: {result.stderr}"
        raise RuntimeError(msg)

    return _parse_records_from_output(result.stdout, record_types)


def _scan_with_cache(
    paths: list[Path],
    root: Path,
    config_path: Path,
    record_types: set[str] | None,
    globs: list[str] | None,
    index_cache: IndexCache,
) -> list[SgRecord]:
    """Run ast-grep scan with index cache support."""
    record_types_set = set(record_types) if record_types is not None else ALL_RECORD_TYPES
    files = _expand_paths(paths, root)
    files = _filter_files_by_globs(files, root, globs)

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
        cached_records.extend(_records_from_cache(cached))

    scanned_records: list[SgRecord] = []
    if files_to_scan:
        scanned_records = _run_scan(files_to_scan, root, config_path, record_types, None)
        records_by_file = group_records_by_file(scanned_records)
        for file_path_str, file_records in records_by_file.items():
            file_path_obj = root / file_path_str
            records_data = [_record_to_cache_dict(record) for record in file_records]
            if file_path_obj.exists():
                index_cache.store(file_path_obj, records_data, record_types_set)
        for file_path in files_to_scan:
            if str(file_path.relative_to(root)) not in records_by_file:
                index_cache.store(file_path, [], record_types_set)

    return _filter_records(cached_records + scanned_records, record_types)


def _parse_records_from_output(output: str, record_types: set[str] | None) -> list[SgRecord]:
    """Parse JSON stream output into records."""
    records: list[SgRecord] = []
    for line in output.strip().split("\n"):
        if not line:
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue
        record = _parse_record(data)
        if record is None:
            continue
        if record_types and record.record not in record_types:
            continue
        records.append(record)
    return records


def _filter_records(
    records: list[SgRecord],
    record_types: set[str] | None,
) -> list[SgRecord]:
    """Filter records by record type."""
    if record_types is None:
        return records
    return [record for record in records if record.record in record_types]


def _expand_paths(paths: list[Path], root: Path) -> list[Path]:
    """Expand input paths to a list of Python files."""
    files: list[Path] = []
    for path in paths:
        full_path = path if path.is_absolute() else root / path
        if full_path.is_file():
            files.append(full_path)
        elif full_path.is_dir():
            files.extend(full_path.rglob("*.py"))
    return files


def _filter_files_by_globs(
    files: list[Path],
    root: Path,
    globs: list[str] | None,
) -> list[Path]:
    """Filter files by glob patterns (supports ! excludes)."""
    if not globs:
        return files

    has_includes = any(not glob.startswith("!") for glob in globs)
    filtered: list[Path] = []

    for file_path in files:
        rel_path = file_path.relative_to(root).as_posix()
        include = not has_includes
        for glob in globs:
            negated = glob.startswith("!")
            pattern = glob[1:] if negated else glob
            if Path(rel_path).match(pattern):
                include = not negated
        if include:
            filtered.append(file_path)
    return filtered


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


def _records_from_cache(records: list[dict[str, object]]) -> list[SgRecord]:
    """Convert cached record dicts to SgRecord instances."""
    parsed: list[SgRecord] = []
    for record in records:
        record_type = str(record.get("record", ""))
        if record_type not in {"def", "call", "import", "raise", "except", "assign_ctor"}:
            continue
        parsed.append(
            SgRecord(
                record=cast(RecordType, record_type),
                kind=str(record.get("kind", "")),
                file=str(record.get("file", "")),
                start_line=int(record.get("start_line", 0)),
                start_col=int(record.get("start_col", 0)),
                end_line=int(record.get("end_line", 0)),
                end_col=int(record.get("end_col", 0)),
                text=str(record.get("text", "")),
                rule_id=str(record.get("rule_id", "")),
            )
        )
    return parsed


def _parse_record(data: dict) -> SgRecord | None:
    """Parse a single JSON record into SgRecord."""
    rule_id = data.get("ruleId", "")
    if not rule_id:
        return None

    # Extract record type and kind from rule_id
    # Rule IDs follow pattern: py_{record}_{kind} or py_{record}
    record_type, kind = _parse_rule_id(rule_id)
    if record_type is None:
        return None

    # Extract position (ast-grep uses 0-indexed lines, convert to 1-indexed)
    range_data = data.get("range", {})
    start = range_data.get("start", {})
    end = range_data.get("end", {})

    return SgRecord(
        record=record_type,
        kind=kind,
        file=data.get("file", ""),
        start_line=start.get("line", 0) + 1,  # Convert to 1-indexed
        start_col=start.get("column", 0),
        end_line=end.get("line", 0) + 1,
        end_col=end.get("column", 0),
        text=data.get("text", ""),
        rule_id=rule_id,
    )


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
    return [
        r
        for r in records
        if r.record == record_type and (kinds is None or r.kind in kinds)
    ]


def group_records_by_file(records: list[SgRecord]) -> dict[str, list[SgRecord]]:
    """Group records by file path.

    Returns
    -------
    dict[str, list[SgRecord]]
        Records grouped by file path
    """
    groups: dict[str, list[SgRecord]] = {}
    for record in records:
        if record.file not in groups:
            groups[record.file] = []
        groups[record.file].append(record)
    return groups
