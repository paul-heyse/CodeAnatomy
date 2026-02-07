"""Audit callsites of record_artifact() for typed vs untyped usage.

Scan all Python files under ``src/`` for ``record_artifact()`` calls and
classify each as *typed* (uses an ArtifactSpec constant) or *string*
(uses a raw string literal).  Produces a plain-text report suitable for CI.

Usage
-----
    uv run python scripts/audit_artifact_callsites.py
"""

from __future__ import annotations

import ast
import sys
from collections import Counter
from pathlib import Path

_MIN_STANDALONE_ARGS = 2
_SEPARATOR_WIDTH = 72
_TOP_UNTYPED_LIMIT = 20


def _write_line(message: str = "") -> None:
    """Write a single line to stdout."""
    sys.stdout.write(message + "\n")


# ---------------------------------------------------------------------------
# AST helpers
# ---------------------------------------------------------------------------


def _resolve_attribute_chain(node: ast.Attribute) -> str:
    """Walk an ``ast.Attribute`` chain and return a dotted name string.

    Parameters
    ----------
    node
        The starting attribute node.

    Returns:
    -------
    str
        Dotted name such as ``"ArtifactSpecs.PLAN_SCHEDULE_SPEC"``.
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


def _classify_name_arg(name_arg: ast.expr) -> tuple[str, str]:
    """Classify a name argument node as typed, string, or unknown.

    Parameters
    ----------
    name_arg
        The AST expression node for the artifact name argument.

    Returns:
    -------
    tuple[str, str]
        ``(kind, value)`` where *kind* is one of ``"string"``, ``"typed"``,
        or ``"unknown"``.
    """
    if isinstance(name_arg, ast.Constant) and isinstance(name_arg.value, str):
        return ("string", name_arg.value)
    if isinstance(name_arg, ast.Name):
        return ("typed", name_arg.id)
    if isinstance(name_arg, ast.Attribute):
        return ("typed", _resolve_attribute_chain(name_arg))
    return ("unknown", ast.dump(name_arg))


# ---------------------------------------------------------------------------
# AST visitor
# ---------------------------------------------------------------------------


class _ArtifactCallVisitor(ast.NodeVisitor):
    """Collect record_artifact() callsites from an AST."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.calls: list[tuple[str, str, int]] = []

    def visit_Call(self, node: ast.Call) -> None:
        """Visit a call node and check for record_artifact() usage."""
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
        """Extract the artifact name argument from a record_artifact() call.

        For method calls (``self.record_artifact(name, payload)`` or
        ``profile.record_artifact(name, payload)``), the name is ``args[0]``.

        For standalone calls (``record_artifact(profile, name, payload)``),
        the name is ``args[1]``.
        """
        name_arg = self._find_name_arg(node, is_method=is_method)
        if name_arg is None:
            self.calls.append(("unknown", "<no_name_arg>", node.lineno))
            return
        kind, value = _classify_name_arg(name_arg)
        self.calls.append((kind, value, node.lineno))

    @staticmethod
    def _find_name_arg(node: ast.Call, *, is_method: bool) -> ast.expr | None:
        """Locate the name argument in a record_artifact() call.

        Parameters
        ----------
        node
            The call AST node.
        is_method
            Whether the call is a method invocation (name at ``args[0]``)
            or a standalone function (name at ``args[1]``).

        Returns:
        -------
        ast.expr | None
            The AST node for the name argument, or ``None`` if not found.
        """
        if is_method:
            name_arg = node.args[0] if node.args else None
        else:
            name_arg = node.args[1] if len(node.args) >= _MIN_STANDALONE_ARGS else None
        if name_arg is not None:
            return name_arg
        # Fall back to keyword "name" if present.
        for kw in node.keywords:
            if kw.arg == "name":
                return kw.value
        return None


# ---------------------------------------------------------------------------
# File scanner
# ---------------------------------------------------------------------------


def _scan_file(path: Path) -> list[tuple[Path, str, str, int]]:
    """Parse a single file and return callsite records.

    Returns:
    -------
    list[tuple[Path, str, str, int]]
        Each tuple is ``(path, kind, name, lineno)``.
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
    return [(path, kind, name, lineno) for kind, name, lineno in visitor.calls]


def _scan_directory(root: Path) -> list[tuple[Path, str, str, int]]:
    """Scan all Python files under *root* for record_artifact() calls.

    Returns:
    -------
    list[tuple[Path, str, str, int]]
        Aggregated callsite records.
    """
    results: list[tuple[Path, str, str, int]] = []
    for py_file in sorted(root.rglob("*.py")):
        results.extend(_scan_file(py_file))
    return results


# ---------------------------------------------------------------------------
# Suggested spec name
# ---------------------------------------------------------------------------


def _suggest_spec_name(artifact_name: str) -> str:
    """Suggest a Python constant name for an artifact name.

    Convert e.g. ``delta_maintenance_v1`` to ``DELTA_MAINTENANCE_SPEC``.

    Returns:
    -------
    str
        Suggested constant name.
    """
    base = artifact_name
    # Strip trailing version suffix for the constant name
    for suffix in ("_v1", "_v2", "_v3", "_v4", "_v5", "_v10"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    return base.upper() + "_SPEC"


# ---------------------------------------------------------------------------
# Report sections
# ---------------------------------------------------------------------------


def _print_header(
    src_root: Path,
    total: int,
    typed_count: int,
    string_count: int,
    unknown_count: int,
) -> None:
    """Write the report header with summary counts."""
    _write_line("=" * _SEPARATOR_WIDTH)
    _write_line("  record_artifact() Callsite Audit Report")
    _write_line("=" * _SEPARATOR_WIDTH)
    _write_line()
    _write_line(f"Source root : {src_root}")
    _write_line(f"Total callsites : {total}")
    _write_line(f"  Typed (spec)  : {typed_count}")
    _write_line(f"  String (raw)  : {string_count}")
    _write_line(f"  Unknown       : {unknown_count}")
    if total > 0:
        typed_pct = 100.0 * typed_count / total
        _write_line(f"  Coverage      : {typed_pct:.1f}% typed")
    _write_line()


def _format_table_header() -> tuple[str, str]:
    """Return the header and underline rows for the untyped names table.

    Returns:
    -------
    tuple[str, str]
        ``(header_row, underline_row)``.
    """
    name_label = "Name"
    count_label = "Count"
    header = f"  {name_label:<50s} {count_label:>5s}  Suggested Spec"
    underline = f"  {'----':<50s} {'-----':>5s}  --------------"
    return header, underline


def _print_top_untyped(
    string_records: list[tuple[Path, str, str, int]],
) -> None:
    """Write the top untyped artifact names section."""
    if not string_records:
        return
    name_counts: Counter[str] = Counter()
    for _, _, name, _ in string_records:
        name_counts[name] += 1
    top_names = name_counts.most_common(_TOP_UNTYPED_LIMIT)
    _write_line("-" * _SEPARATOR_WIDTH)
    _write_line("  Top Untyped Artifact Names (by callsite count)")
    _write_line("-" * _SEPARATOR_WIDTH)
    _write_line()
    header, underline = _format_table_header()
    _write_line(header)
    _write_line(underline)
    for name, count in top_names:
        suggested = _suggest_spec_name(name)
        _write_line(f"  {name:<50s} {count:>5d}  {suggested}")
    _write_line()


def _print_unknown(
    unknown_records: list[tuple[Path, str, str, int]],
    src_root: Path,
) -> None:
    """Write the unknown/dynamic artifact names section."""
    if not unknown_records:
        return
    _write_line("-" * _SEPARATOR_WIDTH)
    _write_line("  Unknown / Dynamic Artifact Names")
    _write_line("-" * _SEPARATOR_WIDTH)
    _write_line()
    for path, _, name, lineno in unknown_records:
        rel = path.relative_to(src_root) if path.is_relative_to(src_root) else path
        _write_line(f"  {rel}:{lineno}  {name}")
    _write_line()


def _print_file_summary(
    records: list[tuple[Path, str, str, int]],
    src_root: Path,
) -> None:
    """Write the per-file summary section."""
    _write_line("-" * _SEPARATOR_WIDTH)
    _write_line("  File-Level Summary")
    _write_line("-" * _SEPARATOR_WIDTH)
    _write_line()
    file_counts: Counter[Path] = Counter()
    file_string: Counter[Path] = Counter()
    for path, kind, _, _ in records:
        file_counts[path] += 1
        if kind == "string":
            file_string[path] += 1
    for path, total_count in file_counts.most_common():
        rel = path.relative_to(src_root) if path.is_relative_to(src_root) else path
        string_count = file_string.get(path, 0)
        rel_str = str(rel)
        _write_line(f"  {rel_str:<65s}  {total_count:>3d} total  {string_count:>3d} string")
    _write_line()
    _write_line("=" * _SEPARATOR_WIDTH)


# ---------------------------------------------------------------------------
# Report orchestration
# ---------------------------------------------------------------------------


def _print_report(records: list[tuple[Path, str, str, int]], src_root: Path) -> int:
    """Print a plain-text audit report.

    Returns:
    -------
    int
        The number of untyped (string) callsites.
    """
    typed = [(p, k, n, ln) for p, k, n, ln in records if k == "typed"]
    string = [(p, k, n, ln) for p, k, n, ln in records if k == "string"]
    unknown = [(p, k, n, ln) for p, k, n, ln in records if k == "unknown"]

    _print_header(src_root, len(records), len(typed), len(string), len(unknown))
    _print_top_untyped(string)
    _print_unknown(unknown, src_root)
    _print_file_summary(records, src_root)

    return len(string)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the audit and exit with code 0."""
    repo_root = Path(__file__).resolve().parents[1]
    src_root = repo_root / "src"
    if not src_root.is_dir():
        sys.stderr.write(f"ERROR: {src_root} is not a directory.\n")
        sys.exit(1)
    records = _scan_directory(src_root)
    _print_report(records, src_root)


if __name__ == "__main__":
    main()
