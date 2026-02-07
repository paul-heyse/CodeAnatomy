"""Generate a migration report for record_artifact() callsite typing.

Scan ``src/`` for ``record_artifact()`` callsites that pass raw string
artifact names and match each against the typed ``ArtifactSpec`` constants
defined in ``serde_artifact_specs``.  Produce a markdown migration report
(or JSON with ``--json``) organized by subsystem phase.

Usage
-----
    uv run python scripts/migrate_artifact_callsites.py
    uv run python scripts/migrate_artifact_callsites.py --json
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MIN_POSITIONAL_ARGS_FOR_NAME = 2

_MIGRATION_PHASES: list[tuple[str, str, tuple[str, ...]]] = [
    (
        "Phase 1",
        "Planning artifacts",
        (
            "src/datafusion_engine/plan/",
            "src/relspec/",
        ),
    ),
    (
        "Phase 2",
        "Write path artifacts",
        (
            "src/datafusion_engine/io/",
            "src/datafusion_engine/delta/",
        ),
    ),
    (
        "Phase 3",
        "Hamilton lifecycle artifacts",
        ("src/hamilton_pipeline/",),
    ),
    (
        "Phase 4",
        "Diagnostics artifacts",
        (
            "src/datafusion_engine/views/",
            "src/datafusion_engine/session/",
        ),
    ),
    (
        "Phase 5",
        "Remaining",
        (),
    ),
]

_VERSION_SUFFIXES: tuple[str, ...] = ("_v10", "_v5", "_v4", "_v3", "_v2", "_v1")

_DEFINITION_FILES: frozenset[str] = frozenset(
    {
        "datafusion_engine/lineage/diagnostics.py",
        "obs/otel/logs.py",
    }
)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SpecMapping:
    """Map a canonical artifact name to its Python constant."""

    canonical_name: str
    constant_name: str
    has_payload_type: bool


@dataclass(frozen=True)
class CallsiteRecord:
    """A single record_artifact() callsite."""

    file_path: Path
    line_number: int
    kind: str  # "typed", "string", "unknown"
    artifact_name: str
    is_method_call: bool


@dataclass
class MigrationEntry:
    """A callsite paired with its migration suggestion."""

    callsite: CallsiteRecord
    matching_spec: SpecMapping | None = None
    suggested_constant: str = ""
    needs_new_spec: bool = False


@dataclass
class _PhaseBucket:
    """Bucket of migration entries for a single phase."""

    phase_id: str
    phase_label: str
    entries: list[MigrationEntry] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Output buffer
# ---------------------------------------------------------------------------


class _OutputBuffer:
    """Accumulate output lines and flush to stdout at the end."""

    def __init__(self) -> None:
        self._lines: list[str] = []

    def line(self, text: str = "") -> None:
        """Append a line of text."""
        self._lines.append(text)

    def flush(self) -> None:
        """Write all accumulated lines to stdout."""
        sys.stdout.write("\n".join(self._lines))
        sys.stdout.write("\n")


# ---------------------------------------------------------------------------
# Spec registry parser
# ---------------------------------------------------------------------------


def _parse_spec_registry(repo_root: Path) -> dict[str, SpecMapping]:
    """Parse serde_artifact_specs.py to extract canonical_name -> constant mappings.

    Parameters
    ----------
    repo_root
        Repository root directory.

    Returns:
    -------
    dict[str, SpecMapping]
        Mapping from canonical name to spec metadata.
    """
    specs_file = repo_root / "src" / "serde_artifact_specs.py"
    if not specs_file.exists():
        return {}

    source = specs_file.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(specs_file))
    mappings: dict[str, SpecMapping] = {}

    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name):
            continue
        constant_name = target.id
        if not constant_name.endswith("_SPEC"):
            continue
        canonical_name = _extract_canonical_name(node.value)
        if canonical_name is None:
            continue
        has_payload = _extract_has_payload_type(node.value)
        mappings[canonical_name] = SpecMapping(
            canonical_name=canonical_name,
            constant_name=constant_name,
            has_payload_type=has_payload,
        )

    return mappings


def _extract_canonical_name(node: ast.expr) -> str | None:
    """Extract the canonical_name string from a register_artifact_spec() call.

    Returns:
    -------
    str | None
        Canonical name when found.
    """
    if not isinstance(node, ast.Call) or not node.args:
        return None
    inner = node.args[0]
    if not isinstance(inner, ast.Call):
        return None
    for kw in inner.keywords:
        if kw.arg == "canonical_name" and isinstance(kw.value, ast.Constant):
            value = kw.value.value
            if isinstance(value, str):
                return value
    return None


def _extract_has_payload_type(node: ast.expr) -> bool:
    """Check whether the ArtifactSpec constructor includes a payload_type.

    Returns:
    -------
    bool
        True when a non-None payload_type keyword is present.
    """
    if not isinstance(node, ast.Call) or not node.args:
        return False
    inner = node.args[0]
    if not isinstance(inner, ast.Call):
        return False
    for kw in inner.keywords:
        if kw.arg == "payload_type":
            return not (isinstance(kw.value, ast.Constant) and kw.value.value is None)
    return False


# ---------------------------------------------------------------------------
# AST visitor for callsites
# ---------------------------------------------------------------------------


def _make_record(
    path: Path,
    lineno: int,
    kind: str,
    name: str,
    *,
    is_method: bool,
) -> CallsiteRecord:
    """Create a CallsiteRecord.

    Returns:
    -------
    CallsiteRecord
        Constructed callsite record.
    """
    return CallsiteRecord(
        file_path=path,
        line_number=lineno,
        kind=kind,
        artifact_name=name,
        is_method_call=is_method,
    )


def _classify_name_arg(
    name_arg: ast.expr,
    path: Path,
    lineno: int,
    *,
    is_method: bool,
) -> CallsiteRecord:
    """Classify a name argument AST node into a callsite record.

    Returns:
    -------
    CallsiteRecord
        Classified callsite record.
    """
    if isinstance(name_arg, ast.Constant) and isinstance(name_arg.value, str):
        return _make_record(path, lineno, "string", name_arg.value, is_method=is_method)
    if isinstance(name_arg, ast.Name):
        return _make_record(path, lineno, "typed", name_arg.id, is_method=is_method)
    if isinstance(name_arg, ast.Attribute):
        return _make_record(
            path,
            lineno,
            "typed",
            _dotted_name(name_arg),
            is_method=is_method,
        )
    return _make_record(path, lineno, "unknown", ast.dump(name_arg), is_method=is_method)


def _dotted_name(node: ast.Attribute) -> str:
    """Reconstruct a dotted name from an ast.Attribute chain.

    Returns:
    -------
    str
        Dotted attribute name.
    """
    parts: list[str] = []
    current: ast.expr = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
    parts.reverse()
    return ".".join(parts)


def _resolve_name_arg(node: ast.Call, *, is_method: bool) -> ast.expr | None:
    """Resolve the name argument from positional args or keywords.

    Returns:
    -------
    ast.expr | None
        The AST node for the name argument.
    """
    if is_method:
        name_arg = node.args[0] if node.args else None
    else:
        name_arg = node.args[1] if len(node.args) >= _MIN_POSITIONAL_ARGS_FOR_NAME else None
    if name_arg is not None:
        return name_arg
    for kw in node.keywords:
        if kw.arg == "name":
            return kw.value
    return None


class _ArtifactCallVisitor(ast.NodeVisitor):
    """Collect record_artifact() callsites from an AST."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.records: list[CallsiteRecord] = []

    def visit_Call(self, node: ast.Call) -> None:
        """Visit function call nodes to find record_artifact() calls."""
        func = node.func
        is_method = False
        is_record = False
        if isinstance(func, ast.Attribute) and func.attr == "record_artifact":
            is_record = True
            is_method = True
        elif isinstance(func, ast.Name) and func.id == "record_artifact":
            is_record = True
        if is_record:
            self._extract_call(node, is_method=is_method)
        self.generic_visit(node)

    def _extract_call(self, node: ast.Call, *, is_method: bool) -> None:
        """Extract the artifact name argument from a record_artifact() call."""
        name_arg = _resolve_name_arg(node, is_method=is_method)
        if name_arg is None:
            self.records.append(
                _make_record(
                    self.path, node.lineno, "unknown", "<no_name_arg>", is_method=is_method
                )
            )
            return
        self.records.append(
            _classify_name_arg(name_arg, self.path, node.lineno, is_method=is_method)
        )


# ---------------------------------------------------------------------------
# File scanner
# ---------------------------------------------------------------------------


def _scan_file(path: Path) -> list[CallsiteRecord]:
    """Parse a single file and return callsite records.

    Returns:
    -------
    list[CallsiteRecord]
        Callsite records found in the file.
    """
    try:
        source = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return []
    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError:
        return []
    visitor = _ArtifactCallVisitor(path)
    visitor.visit(tree)
    return visitor.records


def _scan_directory(root: Path) -> list[CallsiteRecord]:
    """Scan all Python files under *root* for record_artifact() calls.

    Returns:
    -------
    list[CallsiteRecord]
        All callsite records found.
    """
    results: list[CallsiteRecord] = []
    for py_file in sorted(root.rglob("*.py")):
        results.extend(_scan_file(py_file))
    return results


# ---------------------------------------------------------------------------
# Callsite filtering
# ---------------------------------------------------------------------------


def _is_definition_callsite(record: CallsiteRecord, src_root: Path) -> bool:
    """Return True when a record is a protocol/class definition, not a real callsite.

    Returns:
    -------
    bool
        True when the file is a known definition file.
    """
    try:
        rel = record.file_path.relative_to(src_root)
    except ValueError:
        return False
    return str(rel) in _DEFINITION_FILES


# ---------------------------------------------------------------------------
# Migration matching
# ---------------------------------------------------------------------------


def _suggest_constant_name(artifact_name: str) -> str:
    """Suggest a Python constant name from a raw artifact name string.

    Strip trailing version suffixes and upper-case the result.

    Returns:
    -------
    str
        Suggested constant name.
    """
    base = artifact_name
    for suffix in _VERSION_SUFFIXES:
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    return base.upper() + "_SPEC"


def _build_migration_entries(
    callsites: list[CallsiteRecord],
    spec_map: dict[str, SpecMapping],
    src_root: Path,
) -> list[MigrationEntry]:
    """Match each callsite against the spec registry and build migration entries.

    Parameters
    ----------
    callsites
        All discovered callsite records.
    spec_map
        Canonical name to spec mapping.
    src_root
        Source root for relative path computation.

    Returns:
    -------
    list[MigrationEntry]
        Migration entries for all non-definition callsites.
    """
    entries: list[MigrationEntry] = []
    for record in callsites:
        if _is_definition_callsite(record, src_root):
            continue
        if record.kind == "typed":
            entries.append(MigrationEntry(callsite=record))
            continue
        if record.kind == "string":
            spec = spec_map.get(record.artifact_name)
            if spec is not None:
                entries.append(
                    MigrationEntry(
                        callsite=record,
                        matching_spec=spec,
                        suggested_constant=spec.constant_name,
                    )
                )
            else:
                entries.append(
                    MigrationEntry(
                        callsite=record,
                        suggested_constant=_suggest_constant_name(record.artifact_name),
                        needs_new_spec=True,
                    )
                )
        else:
            entries.append(MigrationEntry(callsite=record))
    return entries


# ---------------------------------------------------------------------------
# Phase classification
# ---------------------------------------------------------------------------


def _classify_phase(file_path: Path, src_root: Path) -> tuple[str, str]:
    """Return (phase_id, phase_label) for a file based on its path prefix.

    Parameters
    ----------
    file_path
        Absolute path to the source file.
    src_root
        Source root directory.

    Returns:
    -------
    tuple[str, str]
        Phase identifier and human label.
    """
    try:
        rel = str(file_path.relative_to(src_root.parent))
    except ValueError:
        rel = str(file_path)

    for phase_id, phase_label, prefixes in _MIGRATION_PHASES:
        if not prefixes:
            continue
        for prefix in prefixes:
            if rel.startswith(prefix):
                return phase_id, phase_label

    return "Phase 5", "Remaining"


def _group_by_phase(
    entries: list[MigrationEntry],
    src_root: Path,
) -> list[_PhaseBucket]:
    """Group migration entries by subsystem phase.

    Returns:
    -------
    list[_PhaseBucket]
        Phase buckets with entries classified.
    """
    buckets: dict[str, _PhaseBucket] = {}
    for phase_id, phase_label, _ in _MIGRATION_PHASES:
        buckets[phase_id] = _PhaseBucket(phase_id=phase_id, phase_label=phase_label)

    for entry in entries:
        phase_id, _ = _classify_phase(entry.callsite.file_path, src_root)
        buckets[phase_id].entries.append(entry)

    return [buckets[pid] for pid, _, _ in _MIGRATION_PHASES]


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------


def _rel_path(file_path: Path, src_root: Path) -> str:
    """Return a display-friendly relative path.

    Returns:
    -------
    str
        Relative path string.
    """
    try:
        return str(file_path.relative_to(src_root))
    except ValueError:
        return str(file_path)


# ---------------------------------------------------------------------------
# Markdown report (decomposed into sub-sections)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _SummaryCounts:
    """Aggregate counts for the migration summary."""

    total: int
    typed: int
    string: int
    match: int
    needs: int
    unknown: int
    specs: int


def _md_summary(out: _OutputBuffer, counts: _SummaryCounts) -> None:
    """Emit the summary table."""
    out.line("# Artifact Callsite Migration Report")
    out.line()
    out.line("## Summary")
    out.line()
    out.line("| Metric | Count |")
    out.line("|--------|-------|")
    out.line(f"| Total callsites (excluding definitions) | {counts.total} |")
    out.line(f"| Already typed (ArtifactSpec constants) | {counts.typed} |")
    out.line(f"| Raw string callsites | {counts.string} |")
    out.line(f"| - With matching registered spec | {counts.match} |")
    out.line(f"| - Need new spec created | {counts.needs} |")
    out.line(f"| Unknown / dynamic | {counts.unknown} |")
    out.line(f"| Registered specs available | {counts.specs} |")
    if counts.total > 0:
        typed_pct = 100.0 * counts.typed / counts.total
        out.line(f"| Current typed coverage | {typed_pct:.1f}% |")
    out.line()


def _md_typed_callsites(
    out: _OutputBuffer,
    typed_entries: list[MigrationEntry],
    src_root: Path,
) -> None:
    """Emit the already-typed callsites section."""
    if not typed_entries:
        return
    out.line("## Already Typed Callsites")
    out.line()
    out.line("These callsites already use `ArtifactSpec` constants. No action needed.")
    out.line()
    out.line("| File | Line | Constant |")
    out.line("|------|------|----------|")
    for entry in sorted(
        typed_entries, key=lambda e: (str(e.callsite.file_path), e.callsite.line_number)
    ):
        rel = _rel_path(entry.callsite.file_path, src_root)
        out.line(f"| `{rel}` | {entry.callsite.line_number} | `{entry.callsite.artifact_name}` |")
    out.line()


def _md_matchable_callsites(
    out: _OutputBuffer,
    has_match: list[MigrationEntry],
    src_root: Path,
) -> None:
    """Emit the section for callsites with matching specs."""
    if not has_match:
        return
    out.line("## Callsites Ready to Migrate (Matching Spec Exists)")
    out.line()
    out.line("These callsites use raw strings but have a corresponding `ArtifactSpec` constant.")
    out.line()
    out.line("| File | Line | Current String | Import Constant |")
    out.line("|------|------|----------------|-----------------|")
    for entry in sorted(
        has_match, key=lambda e: (str(e.callsite.file_path), e.callsite.line_number)
    ):
        rel = _rel_path(entry.callsite.file_path, src_root)
        spec = entry.matching_spec
        typed_label = "typed" if spec is not None and spec.has_payload_type else "untyped"
        out.line(
            f"| `{rel}` | {entry.callsite.line_number} "
            f'| `"{entry.callsite.artifact_name}"` '
            f"| `{entry.suggested_constant}` ({typed_label}) |"
        )
    out.line()


def _md_needs_spec_callsites(
    out: _OutputBuffer,
    needs_spec: list[MigrationEntry],
    src_root: Path,
) -> None:
    """Emit the section for callsites needing new specs."""
    if not needs_spec:
        return
    out.line("## Callsites Needing New Specs")
    out.line()
    out.line("These callsites use artifact names with no registered `ArtifactSpec`.")
    out.line("New specs must be created in `serde_artifact_specs.py` before migration.")
    out.line()
    name_counter: Counter[str] = Counter()
    for entry in needs_spec:
        name_counter[entry.callsite.artifact_name] += 1
    unique_names = sorted(name_counter.keys())

    out.line("### Unique Artifact Names Requiring New Specs")
    out.line()
    out.line("| Artifact Name | Callsite Count | Suggested Constant |")
    out.line("|---------------|----------------|--------------------|")
    for name in unique_names:
        count = name_counter[name]
        suggested = _suggest_constant_name(name)
        out.line(f'| `"{name}"` | {count} | `{suggested}` |')
    out.line()

    out.line("### Callsite Details")
    out.line()
    out.line("| File | Line | Current String | Suggested Constant |")
    out.line("|------|------|----------------|--------------------|")
    for entry in sorted(
        needs_spec, key=lambda e: (str(e.callsite.file_path), e.callsite.line_number)
    ):
        rel = _rel_path(entry.callsite.file_path, src_root)
        out.line(
            f"| `{rel}` | {entry.callsite.line_number} "
            f'| `"{entry.callsite.artifact_name}"` '
            f"| `{entry.suggested_constant}` |"
        )
    out.line()


def _md_phase_bucket(
    out: _OutputBuffer,
    bucket: _PhaseBucket,
    src_root: Path,
) -> None:
    """Emit a single phase bucket in the migration checklist."""
    string_in_phase = [e for e in bucket.entries if e.callsite.kind == "string"]
    typed_in_phase = [e for e in bucket.entries if e.callsite.kind == "typed"]

    out.line(f"### {bucket.phase_id}: {bucket.phase_label}")
    out.line()

    if not bucket.entries:
        out.line("No callsites in this phase.")
        out.line()
        return

    matchable = [e for e in string_in_phase if e.matching_spec is not None]
    unmatchable = [e for e in string_in_phase if e.needs_new_spec]

    out.line(f"- Total callsites: {len(bucket.entries)}")
    out.line(f"- Already typed: {len(typed_in_phase)}")
    out.line(f"- Raw strings with matching spec: {len(matchable)}")
    out.line(f"- Raw strings needing new spec: {len(unmatchable)}")
    out.line()

    if not matchable and not unmatchable:
        return

    file_groups: dict[str, list[MigrationEntry]] = {}
    for entry in string_in_phase:
        rel = _rel_path(entry.callsite.file_path, src_root)
        file_groups.setdefault(rel, []).append(entry)

    for rel_file in sorted(file_groups.keys()):
        file_entries = file_groups[rel_file]
        out.line(f"**`{rel_file}`**")
        out.line()
        for entry in sorted(file_entries, key=lambda e: e.callsite.line_number):
            status = "HAS SPEC" if entry.matching_spec is not None else "NEEDS SPEC"
            out.line(
                f"- [ ] L{entry.callsite.line_number}: "
                f'`"{entry.callsite.artifact_name}"` '
                f"-> `{entry.suggested_constant}` [{status}]"
            )
        out.line()


def _md_file_summary(
    out: _OutputBuffer,
    entries: list[MigrationEntry],
    src_root: Path,
) -> None:
    """Emit the file-level summary table."""
    out.line("## File-Level Summary")
    out.line()
    file_counts: Counter[str] = Counter()
    file_string_counts: Counter[str] = Counter()
    file_typed_counts: Counter[str] = Counter()
    for entry in entries:
        rel = _rel_path(entry.callsite.file_path, src_root)
        file_counts[rel] += 1
        if entry.callsite.kind == "string":
            file_string_counts[rel] += 1
        elif entry.callsite.kind == "typed":
            file_typed_counts[rel] += 1

    out.line("| File | Total | Typed | String | Progress |")
    out.line("|------|-------|-------|--------|----------|")
    for rel, total_count in file_counts.most_common():
        typed_count = file_typed_counts.get(rel, 0)
        string_count = file_string_counts.get(rel, 0)
        pct = f"{100.0 * typed_count / total_count:.0f}%" if total_count > 0 else "N/A"
        out.line(f"| `{rel}` | {total_count} | {typed_count} | {string_count} | {pct} |")
    out.line()


def _print_markdown_report(
    entries: list[MigrationEntry],
    spec_map: dict[str, SpecMapping],
    src_root: Path,
) -> None:
    """Print the migration report in markdown format."""
    out = _OutputBuffer()

    typed_entries = [e for e in entries if e.callsite.kind == "typed"]
    string_entries = [e for e in entries if e.callsite.kind == "string"]
    unknown_entries = [e for e in entries if e.callsite.kind == "unknown"]
    has_match = [e for e in string_entries if e.matching_spec is not None]
    needs_spec = [e for e in string_entries if e.needs_new_spec]

    counts = _SummaryCounts(
        total=len(entries),
        typed=len(typed_entries),
        string=len(string_entries),
        match=len(has_match),
        needs=len(needs_spec),
        unknown=len(unknown_entries),
        specs=len(spec_map),
    )
    _md_summary(out, counts)
    _md_typed_callsites(out, typed_entries, src_root)
    _md_matchable_callsites(out, has_match, src_root)
    _md_needs_spec_callsites(out, needs_spec, src_root)

    phases = _group_by_phase(entries, src_root)
    out.line("## Migration Checklist by Subsystem Phase")
    out.line()
    for bucket in phases:
        _md_phase_bucket(out, bucket, src_root)

    _md_file_summary(out, entries, src_root)
    out.flush()


# ---------------------------------------------------------------------------
# JSON report
# ---------------------------------------------------------------------------


def _entry_to_dict(entry: MigrationEntry, src_root: Path) -> dict[str, object]:
    """Convert a migration entry to a JSON-serializable dict.

    Returns:
    -------
    dict[str, object]
        Serializable entry payload.
    """
    rel = _rel_path(entry.callsite.file_path, src_root)
    result: dict[str, object] = {
        "file": rel,
        "line": entry.callsite.line_number,
        "kind": entry.callsite.kind,
        "artifact_name": entry.callsite.artifact_name,
        "is_method_call": entry.callsite.is_method_call,
    }
    if entry.matching_spec is not None:
        result["matching_spec"] = {
            "canonical_name": entry.matching_spec.canonical_name,
            "constant_name": entry.matching_spec.constant_name,
            "has_payload_type": entry.matching_spec.has_payload_type,
        }
    if entry.suggested_constant:
        result["suggested_constant"] = entry.suggested_constant
    result["needs_new_spec"] = entry.needs_new_spec
    return result


def _build_json_report(
    entries: list[MigrationEntry],
    spec_map: dict[str, SpecMapping],
    src_root: Path,
) -> dict[str, object]:
    """Build a JSON-serializable migration report.

    Parameters
    ----------
    entries
        All migration entries.
    spec_map
        Canonical name to spec mapping.
    src_root
        Source root for relative path computation.

    Returns:
    -------
    dict[str, object]
        JSON-serializable report payload.
    """
    total = len(entries)
    typed_entries = [e for e in entries if e.callsite.kind == "typed"]
    string_entries = [e for e in entries if e.callsite.kind == "string"]
    has_match = [e for e in string_entries if e.matching_spec is not None]
    needs_spec = [e for e in string_entries if e.needs_new_spec]

    phases = _group_by_phase(entries, src_root)
    phase_data = [_phase_to_dict(bucket, src_root) for bucket in phases]

    names_needing_specs = _names_needing_specs_list(needs_spec)

    return {
        "summary": {
            "total_callsites": total,
            "typed_callsites": len(typed_entries),
            "string_callsites": len(string_entries),
            "string_with_matching_spec": len(has_match),
            "string_needs_new_spec": len(needs_spec),
            "registered_specs": len(spec_map),
            "typed_coverage_pct": round(100.0 * len(typed_entries) / total, 1)
            if total > 0
            else 0.0,
        },
        "registered_specs": [
            {
                "canonical_name": s.canonical_name,
                "constant_name": s.constant_name,
                "has_payload_type": s.has_payload_type,
            }
            for s in sorted(spec_map.values(), key=lambda s: s.canonical_name)
        ],
        "names_needing_new_specs": names_needing_specs,
        "phases": phase_data,
        "callsites": [_entry_to_dict(e, src_root) for e in entries],
    }


def _phase_to_dict(bucket: _PhaseBucket, src_root: Path) -> dict[str, object]:
    """Convert a phase bucket to a JSON-serializable dict.

    Returns:
    -------
    dict[str, object]
        Phase data payload.
    """
    return {
        "phase_id": bucket.phase_id,
        "phase_label": bucket.phase_label,
        "total": len(bucket.entries),
        "typed": sum(1 for e in bucket.entries if e.callsite.kind == "typed"),
        "string_with_spec": sum(
            1 for e in bucket.entries if e.callsite.kind == "string" and e.matching_spec is not None
        ),
        "string_needs_spec": sum(1 for e in bucket.entries if e.needs_new_spec),
        "entries": [_entry_to_dict(e, src_root) for e in bucket.entries],
    }


def _names_needing_specs_list(
    needs_spec: list[MigrationEntry],
) -> list[dict[str, object]]:
    """Build a list of unique artifact names needing new specs.

    Returns:
    -------
    list[dict[str, object]]
        Sorted list of name entries with counts.
    """
    name_counter: Counter[str] = Counter()
    for entry in needs_spec:
        name_counter[entry.callsite.artifact_name] += 1
    return [
        {
            "artifact_name": name,
            "callsite_count": name_counter[name],
            "suggested_constant": _suggest_constant_name(name),
        }
        for name in sorted(name_counter.keys())
    ]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the migration analysis and produce a report."""
    parser = argparse.ArgumentParser(
        description="Generate a migration report for record_artifact() callsite typing.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        help="Output machine-readable JSON instead of markdown.",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    src_root = repo_root / "src"
    if not src_root.is_dir():
        sys.stderr.write(f"ERROR: {src_root} is not a directory.\n")
        sys.exit(1)

    spec_map = _parse_spec_registry(repo_root)
    callsites = _scan_directory(src_root)
    entries = _build_migration_entries(callsites, spec_map, src_root)

    if args.json_output:
        report = _build_json_report(entries, spec_map, src_root)
        sys.stdout.write(json.dumps(report, indent=2, sort_keys=False))
        sys.stdout.write("\n")
    else:
        _print_markdown_report(entries, spec_map, src_root)


if __name__ == "__main__":
    main()
