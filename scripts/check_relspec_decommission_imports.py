#!/usr/bin/env python3
"""Guard against importing decommissioned relspec modules.

This script scans Python files under selected roots and fails if any import
references relspec modules that were intentionally removed during the Rust
planning/scheduling cutover.
"""

from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path

BANNED_MODULES = {
    "relspec.incremental",
    "relspec.counterfactual_replay",
    "relspec.decision_recorder",
    "relspec.fallback_quarantine",
    "relspec.policy_validation",
    "relspec.runtime_artifacts",
    "relspec.execution_authority",
    "relspec.execution_planning_runtime",
    "relspec.rustworkx_graph",
    "relspec.rustworkx_schedule",
    "relspec.schedule_events",
    "relspec.graph_edge_validation",
    "relspec.evidence",
    "relspec.extract_plan",
}


def _iter_py_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(path for path in root.rglob("*.py") if path.is_file())


def _violations_from_import_from(node: ast.ImportFrom) -> list[tuple[int, str]]:
    module = node.module
    if module is None:
        return []

    violations: list[tuple[int, str]] = []
    if module in BANNED_MODULES:
        violations.append((node.lineno, f"from {module} import ..."))

    if module != "relspec":
        return violations

    for alias in node.names:
        candidate = f"relspec.{alias.name}"
        if candidate in BANNED_MODULES:
            violations.append((node.lineno, f"from relspec import {alias.name}"))

    return violations


def _violations_from_import(node: ast.Import) -> list[tuple[int, str]]:
    return [
        (node.lineno, f"import {alias.name}")
        for alias in node.names
        if alias.name in BANNED_MODULES
    ]


def _find_violations(path: Path) -> list[tuple[int, str]]:
    try:
        source = path.read_text(encoding="utf-8")
    except OSError as exc:
        return [(0, f"unable to read file: {exc}")]

    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        return [(exc.lineno or 0, f"syntax error while parsing: {exc.msg}")]

    violations: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            violations.extend(_violations_from_import_from(node))
            continue
        if isinstance(node, ast.Import):
            violations.extend(_violations_from_import(node))

    return violations


def main() -> int:
    """Run the decommissioned relspec import guard.

    Returns:
        int: Exit code (0 when no violations are found, 1 otherwise).
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        action="append",
        default=["src", "tests"],
        help="Root directory to scan. May be specified multiple times.",
    )
    args = parser.parse_args()

    roots = [Path(value) for value in args.root]
    files: list[Path] = []
    for root in roots:
        files.extend(_iter_py_files(root))

    failures: list[tuple[Path, int, str]] = []
    for path in files:
        for lineno, text in _find_violations(path):
            failures.append((path, lineno, text))

    if failures:
        sys.stdout.write("Found imports of decommissioned relspec modules:\n")
        for path, lineno, text in failures:
            sys.stdout.write(f"  - {path}:{lineno}: {text}\n")
        return 1

    sys.stdout.write("No imports of decommissioned relspec modules found.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
