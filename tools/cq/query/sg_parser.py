"""ast-grep JSON stream parser.

Parses ast-grep scan output (JSON stream format) into typed records.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

# Record types from ast-grep rules
RecordType = Literal["def", "call", "import", "raise", "except", "assign_ctor"]


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

    # Build command
    cmd = [
        "ast-grep",
        "scan",
        "-c",
        str(config_path),
        "--json=stream",
    ]
    cmd.extend(str(p) for p in paths)

    # Run ast-grep
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=root,
    )

    if result.returncode != 0:
        msg = f"ast-grep scan failed: {result.stderr}"
        raise RuntimeError(msg)

    # Parse JSON stream
    records: list[SgRecord] = []
    for line in result.stdout.strip().split("\n"):
        if not line:
            continue

        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue

        record = _parse_record(data)
        if record is None:
            continue

        # Filter by record type if specified
        if record_types and record.record not in record_types:
            continue

        records.append(record)

    return records


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
