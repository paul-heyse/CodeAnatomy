"""ast-grep-py native scanner.

Provides Python-native scanning using ast-grep-py bindings, replacing
the subprocess-based CLI integration.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

from ast_grep_py import Config, Rule, SgNode, SgRoot

from tools.cq.core.locations import SourceSpan
from tools.cq.query.language import DEFAULT_QUERY_LANGUAGE, QueryLanguage

# Record types from ast-grep rules
RecordType = Literal["def", "call", "import", "raise", "except", "assign_ctor"]


@dataclass(frozen=True)
class RuleSpec:
    """Python-native rule specification for ast-grep-py.

    Parameters
    ----------
    rule_id
        Unique identifier for the rule (e.g., "py_def_function").
    record_type
        Record type category (def, call, import, raise, except, assign_ctor).
    kind
        Specific kind within the record type (function, class, name_call, etc.).
    config
        ast-grep-py rule configuration dict. Should contain a "rule" key
        with the matching criteria.
    """

    rule_id: str
    record_type: RecordType
    kind: str
    config: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Normalize config to a concrete mapping.

        Raises
        ------
        TypeError
            Raised when config is not a mapping.
        """
        if not isinstance(self.config, Mapping):
            msg = f"RuleSpec.config must be a mapping, got {type(self.config)!r}"
            raise TypeError(msg)
        if not isinstance(self.config, dict):
            object.__setattr__(self, "config", dict(self.config))

    def to_config(self) -> Rule:
        """Convert to ast-grep-py Rule mapping.

        Returns
        -------
        dict[str, Any]
            Configuration dict suitable for SgNode.find_all().
        """
        if _is_full_config(self.config):
            inner = self.config.get("rule", {})
            if isinstance(inner, Mapping):
                return cast("Rule", dict(inner))
            return cast("Rule", {})
        return cast("Rule", self.config)


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

    @property
    def span(self) -> SourceSpan:
        """Return a SourceSpan for this record."""
        return SourceSpan(
            file=self.file,
            start_line=self.start_line,
            start_col=self.start_col,
            end_line=self.end_line,
            end_col=self.end_col,
        )


def _is_full_config(config: Mapping[str, Any]) -> bool:
    """Check if config needs full dict form (has rule/utils/constraints/transform).

    Parameters
    ----------
    config
        Rule configuration dict.

    Returns
    -------
    bool
        True if config has top-level wrapper keys.
    """
    return any(k in config for k in ("rule", "utils", "constraints", "transform"))


def _has_complex_rule_keys(config: Mapping[str, Any]) -> bool:
    """Check if inner rule has complex keys needing full config.

    Parameters
    ----------
    config
        Inner rule configuration dict.

    Returns
    -------
    bool
        True if config has complex keys requiring {"rule": config} wrapper.
    """
    complex_keys = {
        "regex",
        "not",
        "has",
        "all",
        "any",
        "inside",
        "follows",
        "precedes",
        "nthChild",
    }
    return any(k in config for k in complex_keys)


def scan_files(
    files: list[Path],
    rules: tuple[RuleSpec, ...],
    root: Path,
    lang: QueryLanguage = DEFAULT_QUERY_LANGUAGE,
) -> list[SgRecord]:
    """Scan files using ast-grep-py native bindings.

    Parameters
    ----------
    files
        List of Python files to scan.
    rules
        Tuple of rule specifications to apply.
    root
        Repository root for relative path normalization.
    lang
        Language used by ast-grep for parsing source files.

    Returns
    -------
    list[SgRecord]
        Parsed scan records.
    """
    records: list[SgRecord] = []

    for file_path in files:
        try:
            src = file_path.read_text(encoding="utf-8")
        except OSError:
            continue

        sg_root = SgRoot(src, lang)
        node = sg_root.root()

        for rule in rules:
            inner_config = rule.to_config()

            # Route based on config complexity
            if _is_full_config(rule.config):
                # Already has rule/utils/constraints wrapper
                matches = node.find_all(config=cast("Config", rule.config))
            elif _has_complex_rule_keys(inner_config):
                # Wrap in full config for complex rules
                matches = node.find_all(config=cast("Config", {"rule": inner_config}))
            elif "pattern" in inner_config and len(inner_config) == 1:
                # Simple pattern-only
                matches = node.find_all(pattern=inner_config["pattern"])
            elif "kind" in inner_config and len(inner_config) == 1:
                # Simple kind-only
                matches = node.find_all(kind=inner_config["kind"])
            else:
                # Kwargs for simple multi-key rules without complex constraints
                matches = node.find_all(**inner_config)

            for match in matches:
                record = _match_to_record(match, file_path, rule, root)
                if record is not None:
                    records.append(record)

    return records


def scan_with_pattern(
    files: list[Path],
    pattern: str,
    root: Path,
    rule_id: str = "pattern_query",
    lang: QueryLanguage = DEFAULT_QUERY_LANGUAGE,
) -> list[dict[str, Any]]:
    """Scan files with a pattern.

    Parameters
    ----------
    files
        List of Python files to scan.
    pattern
        ast-grep pattern string to match.
    root
        Repository root for relative path normalization.
    rule_id
        Rule ID to use in results.
    lang
        Language used by ast-grep for parsing source files.

    Returns
    -------
    list[dict[str, Any]]
        Raw match dictionaries with position and metavar info.
    """
    matches: list[dict[str, Any]] = []

    for file_path in files:
        try:
            src = file_path.read_text(encoding="utf-8")
        except OSError:
            continue

        sg_root = SgRoot(src, lang)
        node = sg_root.root()

        for match in node.find_all(pattern=pattern):
            match_dict = _node_to_match_dict(match, file_path, root, rule_id)
            matches.append(match_dict)

    return matches


def _match_to_record(
    match: SgNode,
    file_path: Path,
    rule: RuleSpec,
    root: Path,
) -> SgRecord | None:
    """Convert an ast-grep-py match to an SgRecord.

    Parameters
    ----------
    match
        ast-grep-py SgNode match.
    file_path
        Path to the file being scanned.
    rule
        Rule specification that produced the match.
    root
        Repository root for relative path normalization.

    Returns
    -------
    SgRecord | None
        Parsed record, or None if match is invalid.
    """
    range_obj = match.range()
    text = match.text()

    # Apply regex filters if present in rule config
    rule_config_value = rule.config.get("rule", {})
    if isinstance(rule_config_value, Mapping):
        rule_config: Mapping[str, Any] = rule_config_value
    else:
        rule_config = {}
    if not _apply_regex_filters(text, rule_config):
        return None

    # Normalize file path to repo-relative POSIX string
    rel_path = _normalize_file_path(file_path, root)

    # ast-grep-py uses 0-indexed lines, convert to 1-indexed
    # Range has .start and .end which are Pos objects with .line, .column, .index
    return SgRecord(
        record=rule.record_type,
        kind=rule.kind,
        file=rel_path,
        start_line=range_obj.start.line + 1,
        start_col=range_obj.start.column,
        end_line=range_obj.end.line + 1,
        end_col=range_obj.end.column,
        text=text,
        rule_id=rule.rule_id,
    )


def _node_to_match_dict(
    match: SgNode,
    file_path: Path,
    root: Path,
    rule_id: str,
) -> dict[str, Any]:
    """Convert an ast-grep-py match to a dict matching CLI JSON output.

    Parameters
    ----------
    match
        ast-grep-py SgNode match.
    file_path
        Path to the file being scanned.
    root
        Repository root for relative path normalization.
    rule_id
        Rule ID to include in the result.

    Returns
    -------
    dict[str, Any]
        Match dictionary compatible with CLI JSON output format.
    """
    range_obj = match.range()
    rel_path = _normalize_file_path(file_path, root)

    # Convert Range to dict format for compatibility with CLI JSON output
    range_dict = {
        "start": {"line": range_obj.start.line, "column": range_obj.start.column},
        "end": {"line": range_obj.end.line, "column": range_obj.end.column},
    }

    # Extract metavariables
    metavars = _extract_metavars(match)

    return {
        "ruleId": rule_id,
        "file": rel_path,
        "text": match.text(),
        "range": range_dict,
        "metaVariables": metavars,
    }


def _extract_metavars(match: SgNode) -> dict[str, dict[str, Any]]:
    """Extract metavariable captures from a match.

    Parameters
    ----------
    match
        ast-grep-py SgNode match.

    Returns
    -------
    dict[str, dict[str, Any]]
        Dictionary of metavariable name to capture info.
    """
    metavars: dict[str, dict[str, Any]] = {}

    # Common metavariable names to try
    common_names = [
        "FUNC",
        "F",
        "CLASS",
        "METHOD",
        "M",
        "X",
        "Y",
        "Z",
        "A",
        "B",
        "OBJ",
        "ATTR",
        "VAL",
        "ARGS",
        "KWARGS",
        "E",
        "NAME",
        "MODULE",
        "EXCEPT",
        "COND",
        "VAR",
        "P",
        "L",
        "DECORATOR",
        "SQL",
        "CURSOR",
    ]

    for name in common_names:
        captured = match.get_match(name)
        if captured is not None:
            range_obj = captured.range()
            payload = {
                "text": captured.text(),
                "start": {"line": range_obj.start.line, "column": range_obj.start.column},
                "end": {"line": range_obj.end.line, "column": range_obj.end.column},
            }
            metavars[name] = payload
            metavars[f"${name}"] = payload

    return metavars


def _apply_regex_filters(text: str, rule_config: Mapping[str, Any]) -> bool:
    """Apply regex filters from rule configuration.

    Parameters
    ----------
    text
        Matched text to filter.
    rule_config
        Rule configuration that may contain regex/not.regex.

    Returns
    -------
    bool
        True if text passes all regex filters.
    """
    # Check positive regex match
    if "regex" in rule_config:
        pattern = rule_config["regex"]
        if not re.search(pattern, text):
            return False

    # Check negative regex (not.regex)
    not_config = rule_config.get("not", {})
    if isinstance(not_config, Mapping) and "regex" in not_config:
        pattern = not_config["regex"]
        if re.search(pattern, text):
            return False

    return True


def _normalize_file_path(file_path: Path, root: Path) -> str:
    """Normalize file path to repo-relative POSIX string.

    Parameters
    ----------
    file_path
        Absolute or relative file path.
    root
        Repository root.

    Returns
    -------
    str
        Repo-relative POSIX path string.
    """
    if file_path.is_absolute():
        try:
            return file_path.relative_to(root).as_posix()
        except ValueError:
            return file_path.as_posix()
    return file_path.as_posix()


def filter_records_by_type(
    records: list[SgRecord],
    record_types: set[RecordType] | None,
) -> list[SgRecord]:
    """Filter records by record type.

    Parameters
    ----------
    records
        Records to filter.
    record_types
        Set of record types to keep. If None, returns all records.

    Returns
    -------
    list[SgRecord]
        Filtered records.
    """
    if record_types is None:
        return records
    return [r for r in records if r.record in record_types]


def group_records_by_file(records: list[SgRecord]) -> dict[str, list[SgRecord]]:
    """Group records by file path.

    Parameters
    ----------
    records
        Records to group.

    Returns
    -------
    dict[str, list[SgRecord]]
        Records grouped by file path.
    """
    groups: dict[str, list[SgRecord]] = {}
    for record in records:
        if record.file not in groups:
            groups[record.file] = []
        groups[record.file].append(record)
    return groups
